import os
import re
import io
import time
import math
import shutil
import sqlite3
import asyncio
import logging
import subprocess
import requests
import vk_api
from collections import deque
from pyrogram import Client, filters, enums
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BotCommand, ChatPrivileges

from rich.console import Console
from rich.live import Live
from rich.progress import (
    Progress, TextColumn, BarColumn, DownloadColumn,
    TransferSpeedColumn, TimeRemainingColumn
)

import config  # Ensure your config.py has API_ID, API_HASH, VK_BOT (token), and VK_TOKEN

# ============================================================
# CHAPTER 1: EDITABLE CONFIGURATIONS & UI STRINGS
# ============================================================

# --- Core Engine Limits ---
MAX_RELAY_WORKERS = 3           # Concurrent Zero-Disk stream jobs
DL_WORKERS = 2                  # Concurrent Disk Fallback download jobs
UP_WORKERS = 2                  # Concurrent Disk Fallback upload jobs
MAX_STAGED_FILES = 4            # Max files waiting on disk before pausing DLs
MIN_FREE_GB = 2.0               # Minimum disk space required for Fallback jobs

# --- Zero-Disk / Network Limits ---
CHUNK_SIZE = 1024 * 1024        # 1 MB chunk size for Telegram MTProto
QUEUE_MAX_CHUNKS = 8            # Max chunks held in RAM per active Zero-Disk job (~8MB)
SEGMENT_STAGGER_SECONDS = 1.2   # Delay between starting concurrent chunks

# --- Storage Paths ---
DB_PATH = "SysCache/vk_queue.db"
DOWNLOAD_DIR = "SysCache/vk_downloads"

# --- UI Text Strings (Edit these to change bot messages) ---
UI_STRINGS = {
    "dashboard_header": "📊 **GLOBAL TRANSFER ENGINE**",
    "vk_search": "🔍 VK Search: *{query}*\n📌 Send the **Group Chat ID** to fetch from.",
    "found_preview": "📊 **Found** `{query}`\n🆕 Total Found: *{new}* | ⏭️ Skipped from VK: *{skipped}*",
    "booting": "⚙️ Booting Master Dashboard...\n(Pinning message...)",
    "monitor_setup": "👁️ **MONITORING CONFIGURATION**\nSend the Group IDs or usernames to monitor.",
    "auto_sync_setup": "📡 **CONTINUOUS AUTO-SYNC CONFIGURATION**\nSend the Group ID/Username to mirror.",
    "refresh_step_1": "🔄 **Initiating System & VK Database Refresh...**\n`[1/5]` Pausing transfer engine...",
    "topic_set": "✅ **Master Forum ID successfully set to:** `{forum_id}`",
    "scanning_group": "🔎 Scanning media group...",
    "error_resolution": "❌ Couldn't resolve `{target}`: {error}",
    "zero_disk_active": "🚀 Zero-Disk Stream Active",
    "disk_fallback": "💾 Disk Fallback Active"
}

# ============================================================
# SYSTEM INITIALIZATION (Do not edit below this line)
# ============================================================
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Mute noisy Pyrogram logs to keep the console clean during Backpressure pauses
logging.getLogger("pyrogram.session.session").setLevel(logging.CRITICAL)
logging.getLogger("pyrogram.client").setLevel(logging.WARNING)

console = Console()
progress_ui = Progress(
    TextColumn("[bold blue]{task.fields[name]}", justify="right"),
    BarColumn(bar_width=None),
    "[progress.percentage]{task.percentage:>3.1f}%",
    "•", DownloadColumn(),
    "•", TransferSpeedColumn(),
    "•", TimeRemainingColumn(),
    console=console,
    expand=True
)

console.print("[bold yellow]Connecting to VK...[/bold yellow]")
try:
    vk_session = vk_api.VkApi(token=config.VK_TOKEN)
    vk = vk_session.get_api()
    my_vk_id = vk_session.method('users.get')[0]['id']
    console.print(f"[bold green]✅ VK Connected: {my_vk_id}[/bold green]")
except Exception as e:
    console.print(f"[bold red]❌ Failed to connect to VK: {e}[/bold red]")
    exit(1)

bot_app = Client("bot_session", api_id=config.API_ID, api_hash=config.API_HASH, bot_token=config.VK_BOT)
user_app = Client("p_session", api_id=config.API_ID, api_hash=config.API_HASH, max_concurrent_transmissions=5, workers=10)

# --- Global State Tracking ---
download_queue_t1 = asyncio.Queue()
download_queue_t2 = asyncio.Queue()
upload_queue = asyncio.Queue(maxsize=MAX_STAGED_FILES)
relay_queue = asyncio.Queue() # NEW: Dedicated queue for Zero-Disk jobs

active_jobs = {}
cancelled_jobs = set()
user_states = {}
ui_state = "MAIN"
monitor_page = 0
vk_reupload_attempts = {}

playlist_queues = {}
playlist_order = deque()
vk_video_title_cache = {}
vk_album_name_cache = {}

ENGINE_RUNNING = "RUNNING"
ENGINE_PAUSE_REQUESTED = "PAUSE_REQUESTED"
ENGINE_PAUSED = "PAUSED"
engine_state = ENGINE_RUNNING

pause_event = asyncio.Event()
pause_event.set()