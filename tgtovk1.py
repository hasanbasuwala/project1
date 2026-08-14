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

# ============================================================
# CHAPTER 2: DATABASE INITIALIZATION & STATE PERSISTENCE
# ============================================================

def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def _init_db():
    conn = _connect()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            chat_id INTEGER,
            msg_chat_id INTEGER,
            msg_id INTEGER,
            album_id INTEGER,
            album_name TEXT,
            query TEXT,
            idx INTEGER,
            status TEXT,
            file_path TEXT,
            caption TEXT,
            updated_at REAL,
            tier INTEGER DEFAULT 1,
            is_zero_disk INTEGER DEFAULT 0
        )
    """)
    for stmt in (
        "ALTER TABLE jobs ADD COLUMN caption TEXT",
        "ALTER TABLE jobs ADD COLUMN playlist_id TEXT",
        "ALTER TABLE jobs ADD COLUMN is_pilot INTEGER DEFAULT 0",
        "ALTER TABLE jobs ADD COLUMN tier INTEGER DEFAULT 1",
        "ALTER TABLE jobs ADD COLUMN is_zero_disk INTEGER DEFAULT 0",
    ):
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError:
            pass

    conn.execute("""
        CREATE TABLE IF NOT EXISTS playlists (
            playlist_id TEXT PRIMARY KEY,
            chat_id INTEGER,
            query TEXT,
            album_name TEXT,
            album_id INTEGER,
            status TEXT,
            total INTEGER,
            completed INTEGER,
            failed INTEGER,
            skipped_dupes INTEGER,
            created_at REAL,
            updated_at REAL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS monitored_chats (
            chat_identifier TEXT PRIMARY KEY,
            resolved_id INTEGER,
            added_at REAL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS monitored_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            msg_id INTEGER,
            tag TEXT,
            file_unique_id TEXT,
            caption TEXT,
            is_queued INTEGER DEFAULT 0,
            discovered_at REAL,
            UNIQUE(chat_id, msg_id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_mon_tag ON monitored_messages(tag, is_queued)")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS monitored_tags_meta (
            tag TEXT PRIMARY KEY,
            last_seen_count INTEGER DEFAULT 0,
            last_checked_at REAL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS always_monitors (
            chat_id INTEGER PRIMARY KEY,
            chat_title TEXT,
            status TEXT,
            last_msg_id INTEGER DEFAULT 0,
            added_at REAL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS selected_monitors (
            chat_id INTEGER PRIMARY KEY,
            chat_title TEXT,
            status TEXT,
            last_msg_id INTEGER DEFAULT 0,
            added_at REAL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS selected_tags (
            tag TEXT PRIMARY KEY,
            added_at REAL
        )
    """)

    conn.execute("CREATE TABLE IF NOT EXISTS control (key TEXT PRIMARY KEY, value TEXT)")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS tg_routing_destinations (
            tag TEXT PRIMARY KEY,
            topic_id INTEGER,
            topic_title TEXT,
            created_at REAL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS tg_copied_messages (
            chat_id INTEGER,
            msg_id INTEGER,
            tag TEXT,
            dest_topic_id INTEGER,
            copied_at REAL,
            PRIMARY KEY (chat_id, msg_id, dest_topic_id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS autotransfer_monitors (
            chat_id INTEGER PRIMARY KEY,
            chat_title TEXT,
            status TEXT,
            delete_originals INTEGER DEFAULT 0,
            owner_user_id INTEGER,
            last_msg_id INTEGER DEFAULT 0,
            added_at REAL,
            mode TEXT DEFAULT 'ALL',
            tags TEXT DEFAULT ''
        )
    """)

    conn.commit()
    conn.close()

_init_db()

async def db_execute(query, params=(), fetch=None):
    def run():
        conn = _connect()
        try:
            cur = conn.execute(query, params)
            res = cur.fetchone() if fetch == "one" else cur.fetchall() if fetch == "all" else None
            conn.commit()
            return res
        finally:
            conn.close()
    return await asyncio.to_thread(run)

async def save_job(job):
    await db_execute(
        """INSERT INTO jobs (job_id, playlist_id, chat_id, msg_chat_id, msg_id, album_id, album_name, query, idx, is_pilot, status, file_path, caption, updated_at, tier, is_zero_disk)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(job_id) DO UPDATE SET status=excluded.status, file_path=excluded.file_path,
               is_pilot=excluded.is_pilot, updated_at=excluded.updated_at, tier=excluded.tier, is_zero_disk=excluded.is_zero_disk""",
        (job['job_id'], job.get('playlist_id'), job['chat_id'], job['msg_chat_id'], job['msg_id'],
         job['album_id'], job['album_name'], job['query'], job['idx'], int(job.get('is_pilot', False)),
         job['status'], job.get('file_path'), job.get('caption', ''), time.time(), job.get('tier', 1), int(job.get('is_zero_disk', False)))
    )

async def update_job_status(job_id, status, file_path=None):
    if file_path is not None:
        await db_execute("UPDATE jobs SET status=?, file_path=?, updated_at=? WHERE job_id=?", (status, file_path, time.time(), job_id))
    else:
        await db_execute("UPDATE jobs SET status=?, updated_at=? WHERE job_id=?", (status, time.time(), job_id))

async def delete_job_row(job_id):
    await db_execute("DELETE FROM jobs WHERE job_id=?", (job_id,))

async def set_control(key, value):
    await db_execute("INSERT INTO control (key, value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))

async def get_control(key, default=None):
    row = await db_execute("SELECT value FROM control WHERE key=?", (key,), fetch="one")
    return row[0] if row else default

async def is_msg_in_db(msg_chat_id, msg_id):
    row = await db_execute(
        """SELECT status FROM jobs 
           WHERE (msg_id=? OR job_id=?) AND status IN ('done', 'downloading', 'uploading', 'relaying', 'queued')""", 
        (msg_id, f"{msg_chat_id}_{msg_id}"), 
        fetch="one"
    )
    return bool(row)

async def create_playlist(chat_id, query, album_name, album_id, total):
    playlist_id = f"pl_{int(time.time() * 1000)}_{chat_id}"
    await db_execute(
        """INSERT INTO playlists (playlist_id, chat_id, query, album_name, album_id, status, total, completed, failed, skipped_dupes, created_at, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (playlist_id, chat_id, query, album_name, album_id, "WAITING", total, 0, 0, 0, time.time(), time.time())
    )
    return playlist_id

async def set_playlist_status(playlist_id, status):
    await db_execute("UPDATE playlists SET status=?, updated_at=? WHERE playlist_id=?", (status, time.time(), playlist_id))

async def bump_playlist(playlist_id, completed_delta=0, failed_delta=0):
    await db_execute(
        "UPDATE playlists SET completed = completed + ?, failed = failed + ?, updated_at=? WHERE playlist_id=?",
        (completed_delta, failed_delta, time.time(), playlist_id)
    )

async def get_playlist(playlist_id):
    return await db_execute(
        "SELECT playlist_id, chat_id, query, album_name, album_id, status, total, completed, failed, skipped_dupes FROM playlists WHERE playlist_id=?",
        (playlist_id,), fetch="one"
    )

async def list_playlists(limit=15):
    return await db_execute(
        "SELECT playlist_id, album_name, status, total, completed, failed, skipped_dupes FROM playlists WHERE status != 'KILLED' ORDER BY updated_at DESC LIMIT ?",
        (limit,), fetch="all"
    )

async def sync_vk_to_local_db():
    console.print("[bold cyan]🔄 Syncing state from VK and cleaning duplicate/broken uploads...[/bold cyan]")
    synced_count = 0
    deleted_duplicates_count = 0
    deleted_broken_count = 0
    seen_msg_ids = {} 

    try:
        albums_resp = await asyncio.to_thread(vk.video.getAlbums, owner_id=my_vk_id, count=100)
        albums = albums_resp.get('items', [])
        album_ids = [alb['id'] for alb in albums if alb.get('id')]
        
        if None not in album_ids and 0 not in album_ids:
            album_ids.append(None)

        for album_id in album_ids:
            offset = 0
            count = 200
            while True:
                try:
                    kwargs = {'owner_id': my_vk_id, 'count': count, 'offset': offset}
                    if album_id is not None and album_id > 0:
                        kwargs['album_id'] = album_id
                    items = await asyncio.to_thread(vk.video.get, **kwargs)
                except Exception:
                    break

                video_list = items.get('items', [])
                if not video_list:
                    break

                for v in video_list:
                    title = v.get('title', '')
                    video_id = v.get('id')
                    duration = v.get('duration', 0)
                    is_processing = v.get('processing', 0)

                    if duration == 0 and not is_processing:
                        try:
                            await asyncio.to_thread(vk.video.delete, owner_id=my_vk_id, video_id=video_id)
                            deleted_broken_count += 1
                            console.print(f"[bold red]🗑️ Purged broken/unavailable VK video: {title} (ID: {video_id})[/bold red]")
                        except Exception:
                            pass
                        continue

                    match = re.search(r'\[TG_(\d+)\]', title)
                    if match:
                        msg_id = int(match.group(1))
                        
                        if msg_id in seen_msg_ids:
                            if seen_msg_ids[msg_id] != video_id:
                                try:
                                    await asyncio.to_thread(vk.video.delete, owner_id=my_vk_id, video_id=video_id)
                                    deleted_duplicates_count += 1
                                    console.print(f"[bold yellow]🗑️ Purged duplicate VK video ID {video_id} (TG_{msg_id})[/bold yellow]")
                                except Exception:
                                    pass
                            continue
                        
                        seen_msg_ids[msg_id] = video_id
                        job_id = f"vk_recovered_{msg_id}"
                        
                        await db_execute(
                            """INSERT INTO jobs (job_id, chat_id, msg_chat_id, msg_id, album_id, album_name, query, idx, status, updated_at, tier)
                               VALUES (?,0,0,?,?,?,'vk_sync',1,'done',?,1)
                               ON CONFLICT(job_id) DO UPDATE SET status='done'""",
                            (job_id, msg_id, album_id if album_id else 0, title.split(' - ')[0], time.time())
                        )
                        synced_count += 1

                if len(video_list) < count:
                    break
                offset += count

        if seen_msg_ids:
            placeholders = ','.join('?' for _ in seen_msg_ids.keys())
            await db_execute(f"UPDATE monitored_messages SET is_queued=1 WHERE msg_id IN ({placeholders})", tuple(seen_msg_ids.keys()))

        console.print(f"[bold green]✅ VK Sync Complete: {synced_count} indexed, {deleted_duplicates_count} dupes purged, {deleted_broken_count} broken videos purged.[/bold green]")
        return synced_count, deleted_duplicates_count, deleted_broken_count
    except Exception as e:
        console.print(f"[bold red]⚠️ VK Sync failed on boot: {e}[/bold red]")
        return 0, 0, 0
        
