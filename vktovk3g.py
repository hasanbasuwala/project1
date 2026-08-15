"""
vk_bot.py — VK-to-VK Playlist Deduplicator, Community Scanner & Deep Scanner
═══════════════════════════════════════════════════════════════════════════
v5.0

This file is organized into numbered CHAPTERS with big banner comments so
you can jump around with Ctrl+F / your editor's "go to symbol":

  CH 00  — TXT: all user-facing strings (dashboard headers, job card labels,
           button captions, message templates). Edit THIS to reskin the bot.
  CH 01  — Imports & logging setup
  CH 02  — Config / constants / paths
  CH 03  — Small utility functions (bar renderer, title cleaner, etc)
  CH 04  — VK API helpers (resolve community, albums, tags, caption parsing)
  CH 05  — Database layer (JobScheduler / SQLite)
  CH 06  — Drip-feed orchestrator (playlist scheduling loop)
  CH 07  — Pipeline engines (Downloader / Encoder / Uploader)
  CH 08  — Dashboard renderer + worker panel renderer
  CH 09  — /scan UI renderer (community scan results)
  CH 10  — /scan #tag + /deepscan #tag session state & renderers
  CH 10B — Autoscan engine (persistent hashtag/community watchlist:
           one-time historical scan + continuous background monitoring)
  CH 11  — Router: all command handlers & callback handlers
           (includes /autoscan setup wizard + dashboard Autoscan panel)
  CH 12  — Worker pool + pipeline workers
  CH 13  — Bootstrap: dashboard refresher, crash report, main()

Search for "CH 0" through "CH 13" to jump to a chapter.
═══════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations
import asyncio
import json
import logging
import os
import re
import secrets
import shutil
import socket
import subprocess
import time
import urllib.request
import uuid
import sys
import sqlite3
from pathlib import Path
from collections import defaultdict

import aiohttp
import aiohttp.payload
import yt_dlp
from logging.handlers import RotatingFileHandler
from rapidfuzz import process, fuzz

from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait, MessageNotModified
import config


# ═══════════════════════════════════════════════════════════════════════
# CH 00 — TXT : ALL EDITABLE USER-FACING STRINGS LIVE HERE
# ═══════════════════════════════════════════════════════════════════════
# Change headings, labels, emoji, and button captions here. Nothing below
# this block should contain a hardcoded UI string — everything routes
# through TXT.* so you have exactly one place to reskin the bot.
class TXT:
    # ---- shared chrome ----
    DIVIDER = "`━━━━━━━━━━━━━━━━━━━━━━━━━━`"

    # ---- Dashboard (main mainframe view) ----
    DASH_TITLE          = "💻 **VK PLAYLIST MAINFRAME**"
    DASH_STATUS_LABEL    = "DRIP-FEED ACTIVE"
    DASH_ROW_STATUS      = "`[⚡] STAT :`"
    DASH_ROW_USED        = "`[💾] USED :`"
    DASH_ROW_FREE        = "`[🆓] FREE :`"
    DASH_ROW_ACTIVE      = "`[🔄] ACT  :`"
    DASH_ROW_LAST        = "`[🏁] LAST :`"
    DASH_IDLE            = "`SYSTEM IDLE`"
    DASH_PLAYLISTS_HDR   = "🎵 ACTIVE PLAYLISTS"
    DASH_PAUSE_ALL_BTN   = "⏸ PAUSE ALL PLAYLISTS"
    DASH_RESUME_ALL_BTN  = "▶️ RESUME ALL PLAYLISTS"
    DASH_REFRESH_BTN     = "🔄 REFRESH SYSTEM"
    DASH_ALBUM_PREFIX    = "Alb:"
    DASH_CANCELLING_TAG  = " [CANCELLING]"
    DASH_SYSTEM_IDLE_ROW = "└ System Idle"

    # ---- Job bucket labels (inside an expanded playlist) ----
    BUCKET_DOWNLOADING  = ("📥", "DOWNLOADING")
    BUCKET_WAIT_PREP    = ("⏳", "WAITING PREP")
    BUCKET_PREPARING    = ("⚙️", "PREPARING")
    BUCKET_WAIT_UPLOAD  = ("⏳", "WAITING UP")
    BUCKET_UPLOADING    = ("📤", "UPLOADING")
    BUCKET_EMPTY_ROW    = "└ Empty"

    # ---- Job card (expanded single job) ----
    JOB_CARD_ID_LABEL    = "🪪 JOB"
    JOB_CARD_FILE_ICON   = "📁"
    JOB_CARD_SPEED_ICON  = "⚡"
    JOB_CARD_ETA_ICON    = "⏳"
    JOB_CARD_PROG_ICON   = "📊"
    JOB_LOGS_BTN         = "📄 LOGS"
    JOB_KILL_BTN         = "❌ KILL"
    JOB_CLOSE_CARD_BTN   = "🔙 CLOSE CARD"

    # ---- Playlist control buttons ----
    PL_RESUME_BTN         = "▶️ RESUME"
    PL_PAUSE_BTN          = "⏸ PAUSE"
    PL_GRACEFUL_CANCEL_BTN = "🧹 GRACEFUL CANCEL"
    PL_PURGE_BTN          = "❌ PURGE INSTANTLY"
    PL_CANCELLING_ROW     = "⚠️ CANCELLING (Waiting on Uploads)"

    # ---- Worker pool panel ----
    WK_TITLE       = "🛠 **WORKER POOLS**"
    WK_ROW_DL      = "📥 DOWNLOAD"
    WK_ROW_PREP    = "⚙️ PREPARE"
    WK_ROW_UP      = "📤 UPLOAD"
    WK_ACTIVE_SUFFIX = "active"
    WK_DONE_BTN    = "✅ Done"

    # ---- Live stack card (compact always-on status) ----
    STACK_TITLE       = "📦 **ACTIVE JOBS**"
    STACK_IDLE        = "`  System idle.`"
    STACK_LAST_LABEL  = "🏁 LAST :"
    STACK_LABELS = {
        "dl":  "📥 DOWNLOADING",
        "enc": "⚙️ PREPARING",
        "up":  "📤 UPLOADING",
    }

    # ---- /scan (community scan) ----
    SCAN_USAGE           = "❌ Usage: `/scan https://vk.com/community_link`"
    SCAN_RUNNING          = "🔍 `Scanning community (this may take a moment for large groups)...`"
    SCAN_EXPIRED          = "❌ Scan session expired."
    SCAN_RESULTS_TITLE    = "🔍 **COMMUNITY SCAN RESULTS**"
    SCAN_VIDEOS_HDR        = "📁 **Community Videos**"
    SCAN_WALL_HDR           = "📰 **Wall Post Videos**"
    SCAN_NONE_FOUND         = "└ None found.\n"
    SCAN_UPLOAD_ALL_VIDS_BTN = "📥 UPLOAD ALL VIDEOS"
    SCAN_UPLOAD_ALL_WALL_BTN = "📥 UPLOAD ALL WALL POSTS"
    SCAN_CANCEL_BTN          = "❌ CANCEL SCAN"
    SCAN_NO_VIDEOS_FOUND     = "❌ No videos found in that community."
    SCAN_FAILED              = "❌ Scan failed: `{err}`"
    SCAN_WAITING_ALBUM_PROMPT = "\n\n⚠️ **WAITING FOR TARGET ALBUM**\nSend the album name (e.g. `#MyArchive`) to begin upload."
    SCAN_SEND_ALBUM_TOAST     = "Send the Album Name tag in chat."
    SCAN_RESOLVING_ALBUM      = "🔍 Resolving album '{album}' & checking for duplicates..."
    SCAN_ALL_PRESENT          = "✅ All selected videos are already in album '{album}'. Skipped."
    SCAN_LOCKED_TO_ALBUM      = "✅ **LOCKED TO ALBUM**\nSelected: `{selected}`\nDuplicates Skipped: `{skipped}`\nQueued: `{queued}`"
    SCAN_QUEUE_ERROR          = "❌ Error queueing videos: `{err}`"
    SCAN_CLOSED_TOAST         = "Scan closed."

    # ---- /scan #tag (hashtag wall scan, now with progress bar) ----
    HSCAN_USAGE            = "❌ Usage: `/scan #HashtagName` (e.g., `/scan #HasanBasu`)"
    HSCAN_INIT              = "🔍 **Hashtag Scan Initiated for:** `#{tag}`\n\nPlease send the **VK Community Link** you want to search across."
    HSCAN_RESOLVING         = "🔍 `Resolving community...`"
    HSCAN_PROGRESS_TITLE    = "🔍 **SCANNING WALL FOR** `#{tag}`"
    HSCAN_PROGRESS_POSTS    = "📄 Posts scanned:"
    HSCAN_PROGRESS_MATCHES  = "🎬 Videos matched:"
    HSCAN_NONE_FOUND        = "❌ No native videos found with `#{tag}` or `{tag}` on the wall."
    HSCAN_COMPLETE_TITLE    = "🎯 **WALL SEARCH COMPLETE**"
    HSCAN_ROW_TERM          = "🏷 **Search Term:**"
    HSCAN_ROW_MATCHES       = "📹 **Matching Videos Found:**"
    HSCAN_ROW_ALBUM         = "📁 **Target Album:**"
    HSCAN_CONFIRM_PROMPT    = "\nQueue all **{count}** to album **#{tag}**?"
    HSCAN_DOWNLOAD_BTN      = "📥 Download & Upload ({count})"
    HSCAN_CANCEL_BTN        = "❌ Cancel"
    HSCAN_CANCELLED         = "❌ Hashtag scan cancelled."
    HSCAN_RESOLVING_ALBUM   = "⏳ Resolving/creating album and deduplicating items..."
    HSCAN_ALL_PRESENT       = "✅ All `{count}` videos are already present in album **{album}**."
    HSCAN_BATCH_TITLE       = "✅ **HASHTAG BATCH QUEUED**"
    HSCAN_ROW_FOUND         = "🔍 **Found:**"
    HSCAN_ROW_SKIPPED       = "⏭ **Duplicates Skipped:**"
    HSCAN_ROW_QUEUED        = "📥 **Queued for Processing:**"
    HSCAN_QUEUE_ERROR       = "❌ Error queueing batch: `{err}`"
    HSCAN_SESSION_EXPIRED   = "Session expired or no videos found."
    HSCAN_ERROR             = "❌ Error during hashtag wall search: `{err}`"

    # ---- /deepscan #tag (scan every community you're in) ----
    DSCAN_USAGE            = "❌ Usage: `/deepscan #HashtagName`"
    DSCAN_FETCHING_GROUPS   = "🔍 `Fetching your community list...`"
    DSCAN_NO_COMMUNITIES    = "❌ No communities found on this account."
    DSCAN_PROGRESS_TITLE    = "🔍 **DEEP SCAN IN PROGRESS**"
    DSCAN_ROW_TAG           = "🏷 **Tag:**"
    DSCAN_ROW_SCANNED       = "📡 Scanned:"
    DSCAN_ROW_HITS          = "🎯 Hits:"
    DSCAN_ROW_HITS_UNIT     = "communities"
    DSCAN_ROW_HITS_VIDEO_UNIT = "videos"
    DSCAN_SCANNING_NOTE_PREFIX = "🔄 Scanning..."
    DSCAN_NONE_FOUND        = "❌ No videos tagged `#{tag}` found across {total} communities."
    DSCAN_FAILED            = "❌ Deep scan failed: `{err}`"
    DSCAN_RESULTS_TITLE     = "🎯 **DEEP SCAN RESULTS**"
    DSCAN_ROW_COMMUNITIES   = "📡 Communities with hits:"
    DSCAN_ROW_TOTAL_VIDEOS  = "🎬 Total videos:"
    DSCAN_QUEUE_ALL_COMM_BTN = "📥 QUEUE ALL FROM THIS COMMUNITY ({count})"
    DSCAN_QUEUE_EVERYTHING_BTN = "📥📤 QUEUE EVERYTHING ({count})"
    DSCAN_CLOSE_BTN         = "❌ CLOSE"
    DSCAN_MORE_ROW          = "…and {n} more"
    DSCAN_SESSION_EXPIRED   = "❌ Deep scan session expired."
    DSCAN_QUEUEING_VIDEO    = "Queueing video..."
    DSCAN_QUEUEING_N        = "Queueing {n} videos..."
    DSCAN_VIDEO_RESULT      = "📥 Queued: `{queued}`  ⏭ Already present: `{skipped}`"
    DSCAN_COMMUNITY_RESULT  = "📥 **{name}**\nQueued: `{queued}`  ⏭ Duplicates: `{skipped}`"
    DSCAN_ALL_TITLE         = "📥 **DEEP SCAN BATCH QUEUED**"
    DSCAN_ALL_ROW_TOTAL     = "Total:"
    DSCAN_ALL_ROW_QUEUED    = "Queued:"
    DSCAN_ALL_ROW_DUPES     = "⏭ Duplicates:"

    # ---- /autoscan (persistent hashtag+community watchlist) ----
    ASCAN_INIT_TAGS_PROMPT   = "🤖 **AUTOSCAN SETUP — STEP 1/2: HASHTAGS**\n\nSend the hashtag(s) you want to track (space-separated for multiple, e.g. `#Tag1 #Tag2`). Send more messages to add more. Tap **Done** when finished."
    ASCAN_TAGS_LIST_TITLE    = "🏷 **Tracked Hashtags** ({n})"
    ASCAN_TAGS_EMPTY_ROW     = "└ None added yet — send one above."
    ASCAN_DONE_TAGS_BTN      = "✅ Done — Next: Communities"
    ASCAN_CANCEL_BTN         = "❌ Cancel Setup"
    ASCAN_NEED_ONE_TAG       = "⚠️ Add at least one hashtag before continuing."
    ASCAN_INIT_COMMS_PROMPT  = "🤖 **AUTOSCAN SETUP — STEP 2/2: COMMUNITIES**\n\nSend the VK community link(s) to monitor (space/newline-separated for multiple). Send more messages to add more. Tap **Done** when finished."
    ASCAN_COMMS_LIST_TITLE   = "📡 **Tracked Communities** ({n})"
    ASCAN_COMMS_EMPTY_ROW    = "└ None added yet — send a link above."
    ASCAN_DONE_COMMS_BTN     = "✅ Done — Review & Start"
    ASCAN_NEED_ONE_COMM      = "⚠️ Add at least one community before continuing."
    ASCAN_RESOLVE_FAILED     = "⚠️ Couldn't resolve `{url}`: `{err}` — skipped."
    ASCAN_CONFIRM_TITLE      = "🤖 **AUTOSCAN — READY TO START**"
    ASCAN_CONFIRM_TAGS_ROW   = "🏷 Hashtags:"
    ASCAN_CONFIRM_COMMS_ROW  = "📡 Communities:"
    ASCAN_CONFIRM_NOTE       = "\nThis will scan full wall history for these communities/tags once, then keep watching for new posts automatically."
    ASCAN_START_BTN          = "🚀 START AUTOSCAN"
    ASCAN_CANCELLED          = "❌ Autoscan setup cancelled."
    ASCAN_STARTING           = "⏳ `Starting autoscan — {tags} hashtag(s) × {comms} communit{y}. Scanning historical wall posts...`"
    ASCAN_HIST_SWITCHING     = "📡 **Now scanning...**"
    ASCAN_HIST_COMM_TIMEOUT  = "⏱ Timed out scanning `{name}` — skipping it for now, will retry on the next monitoring cycle."
    ASCAN_HIST_COMM_ERROR    = "⚠️ Error scanning `{name}`: `{err}` — skipping it for now, will retry on the next monitoring cycle."
    ASCAN_HIST_PROGRESS_TITLE = "🔍 **AUTOSCAN HISTORICAL SCAN**"
    ASCAN_HIST_ROW_COMM      = "📡 Community:"
    ASCAN_HIST_ROW_PROGRESS  = "📄 Posts scanned:"
    ASCAN_HIST_ROW_QUEUED    = "📥 Queued so far:"
    ASCAN_HIST_DONE_TITLE    = "✅ **AUTOSCAN LIVE**"
    ASCAN_HIST_DONE_ROW_COMMS = "📡 Communities:"
    ASCAN_HIST_DONE_ROW_TAGS  = "🏷 Hashtags:"
    ASCAN_HIST_DONE_ROW_QUEUED = "📥 Queued from history:"
    ASCAN_HIST_DONE_NOTE     = "\n🤖 Now watching for new posts automatically. Manage anytime from the dashboard's AUTOSCAN panel."

    # ---- Dashboard AUTOSCAN panel ----
    ASCAN_PANEL_HDR          = "🤖 AUTOSCAN"
    ASCAN_PANEL_ON           = "🟢 ON"
    ASCAN_PANEL_OFF          = "🔴 OFF"
    ASCAN_PANEL_TAGS_HDR     = "    🏷 Hashtags"
    ASCAN_PANEL_COMMS_HDR    = "    📡 Communities"
    ASCAN_PANEL_ADD_TAG_BTN  = "    ➕ Add Hashtag"
    ASCAN_PANEL_ADD_COMM_BTN = "    ➕ Add Community"
    ASCAN_PANEL_EMPTY        = "      └ None"
    ASCAN_PANEL_TOGGLE_ON_BTN  = "▶️ Resume Monitoring"
    ASCAN_PANEL_TOGGLE_OFF_BTN = "⏸ Pause Monitoring"
    ASCAN_PANEL_RESCAN_BTN   = "🔍 Force Rescan Now"
    ASCAN_PANEL_LAST_POLL    = "    🕐 Last poll:"
    ASCAN_PANEL_TOTAL_QUEUED = "    📥 Total queued:"
    ASCAN_PANEL_NEVER        = "never"
    ASCAN_ADD_TAG_PROMPT     = "🏷 Send the hashtag to add to Autoscan."
    ASCAN_ADD_COMM_PROMPT    = "📡 Send the VK community link to add to Autoscan."
    ASCAN_TAG_ADDED          = "✅ Added `#{tag}` to Autoscan."
    ASCAN_TAG_REMOVED        = "🗑 Removed `#{tag}` from Autoscan."
    ASCAN_COMM_ADDED         = "✅ Added **{name}** to Autoscan."
    ASCAN_COMM_REMOVED       = "🗑 Removed **{name}** from Autoscan."
    ASCAN_COMM_ADD_FAILED    = "❌ Couldn't resolve that link: `{err}`"
    ASCAN_RESCAN_STARTED     = "⏳ Rescanning full history for all tracked hashtags/communities..."
    ASCAN_NEW_HIT_NOTIFY     = "🤖 **AUTOSCAN HIT**\n📡 {comm}\n🏷 `#{tag}`\n📥 Queued: `{queued}`" + ("" if True else "")

    # ---- /transfer (bookmark + fuzzy tag) ----
    TRANSFER_USAGE          = "❌ Usage: `/transfer https://vk.com/community_link`"
    TRANSFER_INIT            = "⏳ `Resolving target community and initializing Bookmark engine...`"
    TRANSFER_PROGRESS_TITLE  = "🔄 **SCANNING WALL**"
    TRANSFER_ROW_SCANNED     = "🔍 **Scanned:**"
    TRANSFER_ROW_BOOKMARKED  = "🔖 **Bookmarked & Tagged:**"
    TRANSFER_ROW_SKIPPED     = "⏭ **Skipped (No Match/Dupes/Queued):**"
    TRANSFER_COMPLETE_TITLE  = "✅ **TRANSFER COMPLETE**"
    TRANSFER_ROW_TARGET      = "🎯 **Target:**"
    TRANSFER_ROW_TOTAL       = "🔍 **Total Posts Checked:**"
    TRANSFER_ROW_SUCCESS     = "🔖 **Successfully Bookmarked:**"
    TRANSFER_ROW_SKIPPED2    = "⏭ **Skipped/Sent to Downloader:**"
    TRANSFER_CRITICAL_ERROR  = "❌ Transfer Critical Error: `{err}`"

    # ---- auto-catch (plain link + #Album) ----
    AUTO_NEEDS_ALBUM   = "❌ Please provide a target playlist name using `#Name`\nExample: `https://vk.com/video... #My Archive`"
    AUTO_RESOLVING      = "🔍 `Querying VK API & resolving album '{album}'...`"
    AUTO_ALL_PRESENT    = "✅ **All {total} videos are already uploaded** to album '{album}'."
    AUTO_NONE_FOUND     = "❌ No videos found in the provided link."
    AUTO_LOCKED         = "✅ **PLAYLIST LOCKED**\nFound `{total}` videos.\nSkipped `{skipped}` duplicates.\nQueued `{queued}` for drip-feed."
    AUTO_ERROR          = "❌ Extraction error: `{err}`"

    # ---- pause/resume/kill toasts ----
    TOAST_PAUSED_ALL      = "⏸ ALL PLAYLISTS PAUSED"
    TOAST_RESUMED_ALL     = "▶️ ALL PLAYLISTS RESUMED"
    TOAST_JOB_KILLED      = "Task terminated."
    TOAST_NO_LOGS         = "No logs found."
    TOAST_GRACEFUL_CANCEL = "🧹 GRACEFUL CANCEL INITIATED\n\n• Pending downloads wiped.\n• In-progress downloads stopped.\n• Downloaded videos will finish uploading to VK."
    TOAST_PL_PAUSED       = "⏸ PLAYLIST PAUSED"
    TOAST_PL_RESUMED      = "▶️ Playlist Resumed."
    TOAST_PL_PURGED       = "❌ Playlist Terminated & Wiped."
    TOAST_WORKERS_CLOSED  = "Closed."
    MSG_PAUSED_N          = "⏸ Paused {n} playlist(s)."
    MSG_RESUMED_N         = "▶️ Resumed {n} playlist(s)."
    WORKERS_USAGE         = "Usage: `/vk_workers dl=5 enc=3 up=2`"

    # ---- reboot crash report ----
    CRASH_TITLE       = "⚠️ **CRASH RECOVERY / REBOOT REPORT**"
    CRASH_ROW_URL     = "🔗 **URL:**"
    CRASH_ROW_DEST    = "📁 **Destination:**"
    CRASH_ROW_STATE   = "⚙️ **Current State:**"
    CRASH_STATE_VAL   = "`HELD ON BOOT`"
    CRASH_BREAKDOWN_HDR = "📊 **DETAILED BREAKDOWN:**"
    CRASH_ROW_TOTAL   = "🌐 Total Videos:"
    CRASH_ROW_UPLOADED = "✅ Uploaded to VK:"
    CRASH_ROW_READY    = "💾 Ready for Upload:"
    CRASH_ROW_DOWNLOADING = "📥 Currently Downloading:"
    CRASH_ROW_REMAINING = "⏳ Remaining:"
    CRASH_ROW_FAILED    = "❌ Perm Failures:"
    CRASH_RESUME_BTN    = "▶️ RESUME PLAYLIST"
    CRASH_FLUSH_BTN     = "🧹 FLUSH DOWNLOADED & CANCEL"
    CRASH_PURGE_BTN     = "❌ PURGE ALL NOW"

    # ---- terminal (console) mode ----
    TERM_HEADER   = "=== VK MAINFRAME [LIVE] ==="
    TERM_QUEUES   = "QUEUES | DL: {dl} | PREP: {enc} | UP: {up}"
    TERM_IDLE     = "System Idle. Awaiting playlist vectors."


# ═══════════════════════════════════════════════════════════════════════
# CH 01 — LOGGING SETUP
# ═══════════════════════════════════════════════════════════════════════
try:
    loop = asyncio.get_running_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

BASE_DIR = Path("SysCache_VK")
LOG_DIR = BASE_DIR / "logs"
DB_PATH = BASE_DIR / "vk_scheduler.db"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(name)s – %(message)s",
    handlers=[
        RotatingFileHandler(LOG_DIR / "vk_engine.log", maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logging.getLogger().handlers[1].setLevel(logging.CRITICAL)
log = logging.getLogger("vk_stealth_bot")
logging.getLogger("pyrogram").setLevel(logging.ERROR)


# ═══════════════════════════════════════════════════════════════════════
# CH 02 — CONFIG / CONSTANTS / PATHS
# ═══════════════════════════════════════════════════════════════════════
API_ID = config.API_ID
API_HASH = config.API_HASH
BOT_TOKEN = getattr(config, "VK_BOT_TOKEN", config.BOT_TOKEN)
CHANNEL_ID = config.CHANNEL_ID
OWNER_ID = int(config.OWNER_ID) if hasattr(config, "OWNER_ID") else 0

VK_COOKIES = None
if os.path.exists("extracted_cookies.txt"):
    with open("extracted_cookies.txt", "r", encoding="utf-8") as f:
        VK_COOKIES = f.read().strip()

JOBS_DIR = BASE_DIR / "jobs"
JOBS_DIR.mkdir(parents=True, exist_ok=True)

MAX_RETRIES = 3
MAX_GLOBAL_CONCURRENT = 10
MIN_FREE_GB = 3.0
EST_JOB_FOOTPRINT_GB = 1.5

# Deep scan tuning — how deep to go per-community and how fast to page.
DEEPSCAN_MAX_POSTS_PER_COMMUNITY = 3000     # 30 pages @ 100/page
DEEPSCAN_PAGE_DELAY_SEC = 0.34               # ~3 req/s, stays under VK rate limits
HASHTAG_SCAN_PAGE_DELAY_SEC = 0.3
PROGRESS_EDIT_MIN_INTERVAL_SEC = 1.2

# Autoscan tuning — how often the background watcher polls, and how deep
# a "light" steady-state poll checks (vs. the one-time full historical scan
# which uses DEEPSCAN_MAX_POSTS_PER_COMMUNITY above).
AUTOSCAN_POLL_INTERVAL_SEC = 300     # 5 minutes between monitoring cycles
AUTOSCAN_MONITOR_MAX_POSTS = 100     # only the newest page per cycle — cheap

# --- UI State Constants (console colors) ---
C_CYAN, C_YELLOW, C_RED, C_GREEN, C_RESET, C_BOLD = "\033[36m", "\033[33m", "\033[31m", "\033[32m", "\033[0m", "\033[1m"
_live_ui_text = {}
_last_completed = "—"
_dash_msg_id, _dash_chat_id = 0, 0
_stack_msg_id, _stack_chat_id = 0, 0
_dash_tab = "playlists"
_expanded_pl = None
_expanded_bucket = None
_expanded_jid = None

# --- In-Memory Session State ---
_scan_sessions = {}          # /scan community scan
_hashtag_scan_pending = {}   # Chat ID -> List of pending tags waiting for a URL
_hashtag_scan_results = {}   # Bot Message ID -> Session data for concurrent scanning
_deepscan_sessions = {}      # /deepscan #Tag all-communities search
_autoscan_wizard_sessions = {}  # /autoscan setup wizard: chat_id -> {'stage','tags','comms','setup_msg_id'}
_autoscan_pending = {}          # dashboard quick add: chat_id -> 'add_tag' | 'add_comm' (plain string, single-shot)
_autoscan_panel_expanded = False  # collapsed by default — dashboard shows summary only until tapped


# ═══════════════════════════════════════════════════════════════════════
# CH 03 — SMALL UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════
def make_bar(percent: float, width: int = 10) -> str:
    filled = int(max(0.0, min(percent, 100.0)) / (100.0 / width))
    return "█" * filled + "░" * (width - filled)


def get_free_space_gb() -> float:
    total, used, free = shutil.disk_usage(BASE_DIR)
    return free / (1024 ** 3)


def clean_title(title_str: str) -> str:
    return title_str.split("|||")[0] if "|||" in title_str else title_str


def parse_album_caption(caption: str) -> tuple[int | None, str]:
    """
    Playlist `caption` stores the target VK album as 'album_id|||Album Title'
    (same '|||' convention job titles use to carry a hidden unique id).
    Older/legacy playlists (e.g. the /transfer bookmark-fallback path) may
    store a bare album_id or a non-numeric label like 'Bookmarks' instead —
    both are handled here so every caller gets (album_id_or_None, display_title).
    """
    if not caption:
        return None, "Default"
    if "|||" in caption:
        aid_str, title = caption.split("|||", 1)
        aid = int(aid_str) if aid_str.lstrip('-').isdigit() else None
        return aid, title
    if caption.lstrip('-').isdigit():
        return int(caption), caption
    return None, caption


def _stack_bucket(job: dict) -> str:
    stage = (job.get('stage') or '').lower()
    if stage.startswith('uploading') or stage.startswith('uploaded'): return 'up'
    if stage in ('encoding', 'encoded', 'process'): return 'enc'
    return 'dl'


def render_stack_card(jobs: list[dict], max_per_bucket: int = 6) -> str:
    groups: dict[str, list[dict]] = {'dl': [], 'enc': [], 'up': []}
    for j in jobs:
        groups[_stack_bucket(j)].append(j)

    def fmt_job(j: dict) -> str:
        pct = float(j.get('pct') or 0.0)
        title = clean_title(str(j.get('title') or '?'))[:14]
        bar = make_bar(pct, 8)
        speed = ""
        stage_val = j.get('stage') or ""
        if "|" in stage_val:
            parts = [p.strip() for p in stage_val.split("|")]
            if len(parts) >= 2 and parts[1] not in ("~", ""): speed = f" {parts[1]}"
        return f"`  ├ {title:<14} [{bar}] {pct:>3.0f}%{speed}`"

    lines = [f"{TXT.STACK_TITLE} ({len(jobs)})", TXT.DIVIDER]
    if not jobs:
        lines.append(TXT.STACK_IDLE)
    else:
        labels = (('dl', TXT.STACK_LABELS['dl']), ('enc', TXT.STACK_LABELS['enc']), ('up', TXT.STACK_LABELS['up']))
        shown = 0
        for key, label in labels:
            bucket_jobs = groups[key]
            if not bucket_jobs: continue
            lines.append(f"`{label} ({len(bucket_jobs)})`")
            lines.extend(fmt_job(j) for j in bucket_jobs[:max_per_bucket])
            shown += min(len(bucket_jobs), max_per_bucket)
        extra = len(jobs) - shown
        if extra > 0: lines.append(f"`  …and {extra} more`")
    lines.append(f"`{TXT.STACK_LAST_LABEL} {clean_title(_last_completed)[:20]}`")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════
# CH 04 — VK API HELPERS
# ═══════════════════════════════════════════════════════════════════════
def get_vk_api():
    VK_TOKEN = getattr(config, "VK_TOKEN", None)
    if not VK_TOKEN: raise ValueError("VK_TOKEN missing in config.")
    import vk_api
    return vk_api.VkApi(token=VK_TOKEN).get_api()


def resolve_vk_community_id(vk, url: str) -> int:
    """Helper to cleanly extract a community/owner ID from a VK URL."""
    clean_url = re.sub(r'(m\.vk\.com|vk\.ru)', 'vk.com', url, flags=re.IGNORECASE)

    comm_match = re.search(r'vk\.com/(?:club|public|event|groups/|video/owner/-)(\d+)', clean_url)
    if comm_match: return -int(comm_match.group(1))

    vid_match = re.search(r'video(-?\d+)_', clean_url)
    if vid_match: return int(vid_match.group(1))

    screen_match = re.search(r'vk\.com/(?:@)?([^/#?\s]+)', clean_url)
    if screen_match:
        screen_name = screen_match.group(1)
        res = vk.utils.resolveScreenName(screen_name=screen_name)
        if res and res.get('type') in ('group', 'page', 'event'):
            return -int(res['object_id'])

    raise ValueError(f"Could not resolve community ID from: {clean_url}")


def get_or_create_vk_album(vk, album_name: str) -> int:
    albums = vk.video.getAlbums(count=100).get('items', [])
    for album in albums:
        if album.get('title', '').strip().lower() == album_name.strip().lower():
            return album['id']
    return vk.video.addAlbum(title=album_name)['album_id']


def get_existing_vk_db_ids(vk, album_id: int) -> set:
    existing_ids = set()
    offset, count = 0, 100
    while True:
        res = vk.video.get(album_id=album_id, count=count, offset=offset)
        items = res.get('items', [])
        if not items: break
        for v in items:
            desc = v.get('description', '')
            match = re.search(r'\[VK_DB_ID:\s*(.+?)\]', desc)
            if match: existing_ids.add(match.group(1))
        offset += count
        if offset >= res.get('count', 0): break
    return existing_ids


def parse_caption(caption: str):
    """Parses a VK caption for: [Production] Name x Name (Description optional)"""
    if not caption: return None

    pattern = r'\[([^\]]+)\]\s*([^-]+)(?:-\s*(.*))?'
    match = re.search(pattern, caption, flags=re.DOTALL)

    if not match: return None

    production = match.group(1).strip()
    names_raw = match.group(2).strip()
    description = match.group(3).strip() if match.group(3) else ""

    names_list = [
        name.strip() for name in
        re.split(r'\s*(?:x|&|,|\band\b)\s*', names_raw, flags=re.IGNORECASE)
        if name.strip()
    ]
    return production, names_list, description


def extract_videos_from_post(post: dict, depth: int = 0, max_depth: int = 4) -> list[dict]:
    """
    Collects video attachments from a wall post AND any reposts nested inside it.
    VK stores a repost's original post under 'copy_history' — if a community
    reposts another community's post that contains a video, that video lives
    inside copy_history, not in the outer post's own 'attachments'. Without this,
    reposted/bundled videos from other communities are silently skipped.
    """
    videos = []
    if depth > max_depth or not post:
        return videos
    for att in post.get('attachments', []) or []:
        if att.get('type') == 'video':
            v = att['video']
            videos.append({'owner_id': v['owner_id'], 'id': v['id'], 'title': v.get('title', 'VK Video')})
    for repost in post.get('copy_history', []) or []:
        videos.extend(extract_videos_from_post(repost, depth + 1, max_depth))
    return videos


def extract_text_chain(post: dict, depth: int = 0, max_depth: int = 4) -> str:
    """
    Concatenates a post's own caption with the captions of any reposts nested
    inside it, so a hashtag placed on the outer repost OR the original inner
    post is still found by pattern matching.
    """
    if depth > max_depth or not post:
        return ""
    parts = [post.get('text', '') or '']
    for repost in post.get('copy_history', []) or []:
        parts.append(extract_text_chain(repost, depth + 1, max_depth))
    return "\n".join(parts)


class FuzzyTagManager:
    """Manages VK Fave Tags with fuzzy matching to avoid duplicate tag creation."""
    def __init__(self, vk):
        self.vk = vk
        self.tags = {}  # "Tag Name": tag_id
        self._load_tags()

    def _load_tags(self):
        try:
            res = self.vk.fave.getTags()
            for t in res.get('items', []):
                self.tags[t['name'].strip()] = t['id']
        except Exception as e:
            log.error(f"Error loading fave tags: {e}")

    def get_or_create_tag_id(self, tag_name: str) -> int:
        tag_name = tag_name.strip()
        if not tag_name:
            return None

        existing_names = list(self.tags.keys())
        if existing_names:
            match = process.extractOne(tag_name, existing_names, scorer=fuzz.WRatio)
            if match and match[1] > 85:
                return self.tags[match[0]]

        try:
            new_tag = self.vk.fave.addTag(name=tag_name)
            tag_id = new_tag['id']
            self.tags[tag_name] = tag_id
            return tag_id
        except Exception as e:
            log.error(f"Failed to create tag '{tag_name}': {e}")
            return None


# ═══════════════════════════════════════════════════════════════════════
# CH 05 — DATABASE LAYER (JobScheduler / SQLite)
# ═══════════════════════════════════════════════════════════════════════
class JobScheduler:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.lock = asyncio.Lock()
        self.conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute('''CREATE TABLE IF NOT EXISTS playlists (
            id TEXT PRIMARY KEY, url TEXT, caption TEXT, total INTEGER,
            downloaded INTEGER, status TEXT, chat_id INTEGER
        )''')
        self.conn.execute('''CREATE TABLE IF NOT EXISTS playlist_items (
            id TEXT PRIMARY KEY, playlist_id TEXT, url TEXT, title TEXT,
            status TEXT, retries INTEGER
        )''')
        self.conn.execute('''CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY, url TEXT, title TEXT, playlist_id TEXT, item_id TEXT,
            stage TEXT, pct REAL, retries INTEGER, chat_id INTEGER, tracker_id INTEGER,
            held INTEGER DEFAULT 0
        )''')
        self.conn.execute('''CREATE TABLE IF NOT EXISTS processed_transfers (
            video_uid TEXT PRIMARY KEY
        )''')
        # Autoscan: persistent watchlist + monitoring state (survives restarts)
        self.conn.execute('''CREATE TABLE IF NOT EXISTS autoscan_tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT, tag TEXT UNIQUE
        )''')
        self.conn.execute('''CREATE TABLE IF NOT EXISTS autoscan_communities (
            id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT UNIQUE,
            resolved_id INTEGER, display_name TEXT, historical_done INTEGER DEFAULT 0
        )''')
        self.conn.execute('''CREATE TABLE IF NOT EXISTS autoscan_seen (
            unique_id TEXT PRIMARY KEY, tag TEXT
        )''')
        self.conn.execute('''CREATE TABLE IF NOT EXISTS autoscan_state (
            key TEXT PRIMARY KEY, value TEXT
        )''')

        for stmt in (
            'ALTER TABLE jobs ADD COLUMN tracker_id INTEGER',
            'ALTER TABLE jobs ADD COLUMN item_id TEXT',
            'ALTER TABLE jobs ADD COLUMN held INTEGER DEFAULT 0',
            'ALTER TABLE playlist_items ADD COLUMN retries INTEGER',
        ):
            try: self.conn.execute(stmt)
            except sqlite3.OperationalError: pass
        self.conn.commit()

    def log_trace(self, jid: str, msg: str):
        job_dir = JOBS_DIR / f"JOB_{jid}"
        job_dir.mkdir(parents=True, exist_ok=True)
        with open(job_dir / "trace.log", "a", encoding="utf-8") as f:
            f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")

    # --- /transfer dedupe ---
    async def is_transferred(self, uid: str) -> bool:
        async with self.lock:
            row = self.conn.execute('SELECT 1 FROM processed_transfers WHERE video_uid = ?', (uid,)).fetchone()
            return bool(row)

    async def mark_transferred(self, uid: str):
        async with self.lock:
            self.conn.execute('INSERT OR IGNORE INTO processed_transfers (video_uid) VALUES (?)', (uid,))
            self.conn.commit()

    # --- Autoscan: tags ---
    async def add_autoscan_tag(self, tag: str) -> bool:
        async with self.lock:
            try:
                self.conn.execute('INSERT INTO autoscan_tags (tag) VALUES (?)', (tag,))
                self.conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    async def remove_autoscan_tag(self, tag_id: int):
        async with self.lock:
            self.conn.execute('DELETE FROM autoscan_tags WHERE id = ?', (tag_id,))
            self.conn.commit()

    async def get_autoscan_tags(self) -> list[dict]:
        async with self.lock:
            return [dict(r) for r in self.conn.execute('SELECT * FROM autoscan_tags ORDER BY id').fetchall()]

    # --- Autoscan: communities ---
    async def add_autoscan_community(self, url: str) -> bool:
        async with self.lock:
            try:
                self.conn.execute('INSERT INTO autoscan_communities (url, historical_done) VALUES (?, 0)', (url,))
                self.conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    async def remove_autoscan_community(self, comm_id: int):
        async with self.lock:
            self.conn.execute('DELETE FROM autoscan_communities WHERE id = ?', (comm_id,))
            self.conn.commit()

    async def get_autoscan_communities(self) -> list[dict]:
        async with self.lock:
            return [dict(r) for r in self.conn.execute('SELECT * FROM autoscan_communities ORDER BY id').fetchall()]

    async def update_autoscan_community(self, comm_id: int, **kwargs):
        async with self.lock:
            for k, v in kwargs.items(): self.conn.execute(f'UPDATE autoscan_communities SET {k} = ? WHERE id = ?', (v, comm_id))
            self.conn.commit()

    async def mark_all_communities_need_rescan(self):
        async with self.lock:
            self.conn.execute('UPDATE autoscan_communities SET historical_done = 0')
            self.conn.commit()

    # --- Autoscan: seen-video cache (avoids re-hitting VK album API every poll) ---
    async def is_autoscan_seen(self, uid: str) -> bool:
        async with self.lock:
            row = self.conn.execute('SELECT 1 FROM autoscan_seen WHERE unique_id = ?', (uid,)).fetchone()
            return bool(row)

    async def mark_autoscan_seen(self, uid: str, tag: str):
        async with self.lock:
            self.conn.execute('INSERT OR IGNORE INTO autoscan_seen (unique_id, tag) VALUES (?, ?)', (uid, tag))
            self.conn.commit()

    # --- Autoscan: small persistent key/value state ---
    async def get_autoscan_state(self, key: str, default: str = None) -> str:
        async with self.lock:
            row = self.conn.execute('SELECT value FROM autoscan_state WHERE key = ?', (key,)).fetchone()
            return row[0] if row else default

    async def set_autoscan_state(self, key: str, value: str):
        async with self.lock:
            self.conn.execute(
                'INSERT INTO autoscan_state (key, value) VALUES (?, ?) '
                'ON CONFLICT(key) DO UPDATE SET value = excluded.value', (key, value))
            self.conn.commit()

    async def incr_autoscan_total_queued(self, n: int):
        async with self.lock:
            row = self.conn.execute('SELECT value FROM autoscan_state WHERE key = "total_queued"').fetchone()
            current = int(row[0]) if row else 0
            self.conn.execute(
                'INSERT INTO autoscan_state (key, value) VALUES ("total_queued", ?) '
                'ON CONFLICT(key) DO UPDATE SET value = excluded.value', (str(current + n),))
            self.conn.commit()

    # --- Playlist methods ---
    async def create_playlist(self, pl_id: str, url: str, caption: str, total: int, chat_id: int):
        async with self.lock:
            self.conn.execute('INSERT INTO playlists VALUES (?, ?, ?, ?, 0, "active", ?)', (pl_id, url, caption, total, chat_id))
            self.conn.commit()

    async def add_playlist_items(self, items: list[tuple]):
        async with self.lock:
            self.conn.executemany('INSERT INTO playlist_items VALUES (?, ?, ?, ?, "pending", 0)', items)
            self.conn.commit()

    async def get_active_playlists(self) -> list[dict]:
        async with self.lock:
            return [dict(r) for r in self.conn.execute('SELECT * FROM playlists WHERE status != "completed" AND status != "cancelled"').fetchall()]

    async def get_playlist(self, pl_id: str) -> dict:
        async with self.lock:
            row = self.conn.execute('SELECT * FROM playlists WHERE id = ?', (pl_id,)).fetchone()
            return dict(row) if row else {}

    async def get_playlist_failed_count(self, pl_id: str) -> int:
        async with self.lock:
            row = self.conn.execute('SELECT COUNT(*) FROM playlist_items WHERE playlist_id = ? AND status = "failed"', (pl_id,)).fetchone()
            return row[0] if row else 0

    async def update_playlist(self, pl_id: str, **kwargs):
        async with self.lock:
            for k, v in kwargs.items(): self.conn.execute(f'UPDATE playlists SET {k} = ? WHERE id = ?', (v, pl_id))
            self.conn.commit()

    async def pause_all_playlists(self) -> int:
        async with self.lock:
            cur = self.conn.execute('UPDATE playlists SET status = "paused" WHERE status = "active"')
            self.conn.commit()
            return cur.rowcount

    async def resume_all_playlists(self) -> int:
        async with self.lock:
            cur = self.conn.execute('UPDATE playlists SET status = "active" WHERE status = "paused"')
            self.conn.commit()
            return cur.rowcount

    async def cancel_playlist(self, pl_id: str):
        async with self.lock:
            self.conn.execute('UPDATE playlists SET status = "cancelled" WHERE id = ?', (pl_id,))
            self.conn.execute('DELETE FROM playlist_items WHERE playlist_id = ? AND status = "pending"', (pl_id,))
            active_jobs = [dict(r) for r in self.conn.execute('SELECT id FROM jobs WHERE playlist_id = ?', (pl_id,)).fetchall()]
            self.conn.execute('DELETE FROM jobs WHERE playlist_id = ?', (pl_id,))
            self.conn.commit()
        for j in active_jobs: shutil.rmtree(JOBS_DIR / f"JOB_{j['id']}", ignore_errors=True)

    async def graceful_cancel_playlist(self, pl_id: str):
        async with self.lock:
            self.conn.execute('DELETE FROM playlist_items WHERE playlist_id = ? AND status = "pending"', (pl_id,))
            cancel_jobs = [dict(r) for r in self.conn.execute('SELECT id FROM jobs WHERE playlist_id = ? AND stage IN ("queued", "downloading")', (pl_id,)).fetchall()]
            for j in cancel_jobs:
                self.conn.execute('DELETE FROM jobs WHERE id = ?', (j['id'],))
                shutil.rmtree(JOBS_DIR / f"JOB_{j['id']}", ignore_errors=True)

            row = self.conn.execute('SELECT COUNT(*) FROM jobs WHERE playlist_id = ?', (pl_id,)).fetchone()
            remaining = row[0] if row else 0
            if remaining == 0: self.conn.execute('UPDATE playlists SET status = "cancelled" WHERE id = ?', (pl_id,))
            else: self.conn.execute('UPDATE playlists SET status = "cancelling" WHERE id = ?', (pl_id,))
            self.conn.commit()

    async def get_pending_items(self, pl_id: str, limit: int = 2) -> list[dict]:
        # ORDER BY rowid guarantees we always claim items in the exact
        # order they were inserted — critical for oldest-first queuing
        # (see CH 04 helper `oldest_first` / call sites in CH 11), since
        # VK's own list endpoints return newest-first by default.
        async with self.lock:
            return [dict(r) for r in self.conn.execute('SELECT * FROM playlist_items WHERE playlist_id = ? AND status = "pending" ORDER BY rowid ASC LIMIT ?', (pl_id, limit)).fetchall()]

    async def get_pending_count(self, pl_id: str) -> int:
        async with self.lock:
            row = self.conn.execute('SELECT COUNT(*) FROM playlist_items WHERE playlist_id = ? AND status = "pending"', (pl_id,)).fetchone()
            return row[0] if row else 0

    async def update_item_status(self, item_id: str, status: str):
        async with self.lock:
            self.conn.execute('UPDATE playlist_items SET status = ? WHERE id = ?', (status, item_id))
            self.conn.commit()

    async def claim_item_as_job(self, item: dict, chat_id: int):
        jid = item['id']
        async with self.lock:
            self.conn.execute('UPDATE playlist_items SET status = "processing" WHERE id = ?', (jid,))
            self.conn.execute('INSERT OR IGNORE INTO jobs (id, url, title, playlist_id, item_id, stage, pct, retries, chat_id, tracker_id) VALUES (?, ?, ?, ?, ?, "queued", 0.0, 0, ?, NULL)',
                              (jid, item['url'], item['title'], item['playlist_id'], jid, chat_id))
            self.conn.commit()
        root = JOBS_DIR / f"JOB_{jid}"
        for d in (root, root / "dl", root / "enc"): d.mkdir(parents=True, exist_ok=True)

    async def update_job(self, jid: str, **kwargs):
        async with self.lock:
            for k, v in kwargs.items(): self.conn.execute(f'UPDATE jobs SET {k} = ? WHERE id = ?', (v, jid))
            self.conn.commit()

    async def delete_job(self, jid: str):
        async with self.lock:
            self.conn.execute('DELETE FROM jobs WHERE id = ?', (jid,))
            self.conn.commit()

    async def get_active_jobs(self) -> list[dict]:
        async with self.lock:
            return [dict(r) for r in self.conn.execute('SELECT * FROM jobs').fetchall()]

    async def get_held_jobs(self, pl_id: str) -> list[dict]:
        async with self.lock:
            return [dict(r) for r in self.conn.execute('SELECT * FROM jobs WHERE playlist_id = ? AND held = 1', (pl_id,)).fetchall()]

    async def clear_held(self, jid: str):
        async with self.lock:
            self.conn.execute('UPDATE jobs SET held = 0 WHERE id = ?', (jid,))
            self.conn.commit()

    async def get_job(self, jid: str) -> dict:
        async with self.lock:
            row = self.conn.execute('SELECT * FROM jobs WHERE id = ?', (jid,)).fetchone()
            return dict(row) if row else {}

    async def get_item_status(self, item_id: str) -> str | None:
        async with self.lock:
            row = self.conn.execute('SELECT status FROM playlist_items WHERE id = ?', (item_id,)).fetchone()
            return row[0] if row else None

    async def global_in_flight_count(self) -> int:
        async with self.lock:
            row = self.conn.execute('SELECT COUNT(*) FROM jobs').fetchone()
            return row[0] if row else 0

    async def fail_or_retry(self, job: dict, reason: str):
        jid, item_id = job['id'], job.get('item_id') or job['id']
        retries = int(job.get('retries') or 0) + 1
        self.log_trace(jid, f"FAILURE (attempt {retries}/{MAX_RETRIES}): {reason}")

        async with self.lock:
            if retries < MAX_RETRIES:
                self.conn.execute('UPDATE playlist_items SET status = "pending", retries = ? WHERE id = ?', (retries, item_id))
                self.conn.execute('UPDATE jobs SET stage = "queued", pct = 0.0, retries = ? WHERE id = ?', (retries, jid))
                self.conn.commit()
                return

            self.conn.execute('UPDATE playlist_items SET status = "failed", retries = ? WHERE id = ?', (retries, item_id))
            pl = self.conn.execute('SELECT * FROM playlists WHERE id = ?', (job['playlist_id'],)).fetchone()
            if pl:
                new_done = pl['downloaded'] + 1
                status = "completed" if new_done >= pl['total'] else pl['status']
                self.conn.execute('UPDATE playlists SET downloaded = ?, status = ? WHERE id = ?', (new_done, status, pl['id']))
            self.conn.execute('DELETE FROM jobs WHERE id = ?', (jid,))
            self.conn.commit()
        shutil.rmtree(JOBS_DIR / f"JOB_{jid}", ignore_errors=True)

    async def force_fail_job(self, jid: str):
        async with self.lock:
            row = self.conn.execute('SELECT * FROM jobs WHERE id = ?', (jid,)).fetchone()
            job = dict(row) if row else None
            if job:
                item_id = job.get('item_id') or jid
                self.conn.execute('UPDATE playlist_items SET status = "failed" WHERE id = ?', (item_id,))
                pl = self.conn.execute('SELECT * FROM playlists WHERE id = ?', (job['playlist_id'],)).fetchone()
                if pl:
                    new_done = pl['downloaded'] + 1
                    status = "completed" if new_done >= pl['total'] else pl['status']
                    self.conn.execute('UPDATE playlists SET downloaded = ?, status = ? WHERE id = ?', (new_done, status, pl['id']))
                self.conn.execute('DELETE FROM jobs WHERE id = ?', (jid,))
                self.conn.commit()
        shutil.rmtree(JOBS_DIR / f"JOB_{jid}", ignore_errors=True)

    async def reconcile_items(self):
        async with self.lock:
            job_item_ids = {r[0] for r in self.conn.execute('SELECT item_id FROM jobs').fetchall()}
            stuck = self.conn.execute('SELECT id FROM playlist_items WHERE status = "processing"').fetchall()
            for (iid,) in stuck:
                if iid not in job_item_ids: self.conn.execute('UPDATE playlist_items SET status = "pending" WHERE id = ?', (iid,))
            self.conn.commit()

    async def reconcile_on_startup(self) -> dict:
        result = {"dl": [], "enc": [], "up": [], "held": []}
        async with self.lock:
            jobs = [dict(r) for r in self.conn.execute('SELECT * FROM jobs').fetchall()]
            playlist_status = {r[0]: r[1] for r in self.conn.execute('SELECT id, status FROM playlists').fetchall()}

        known_ids = {j['id'] for j in jobs}
        if JOBS_DIR.exists():
            for folder in JOBS_DIR.glob("JOB_*"):
                jid = folder.name.replace("JOB_", "", 1)
                if jid not in known_ids: shutil.rmtree(folder, ignore_errors=True)

        for j in jobs:
            jid = j['id']
            root = JOBS_DIR / f"JOB_{jid}"
            dl_dir, enc_dir = root / "dl", root / "enc"
            for d in (root, dl_dir, enc_dir): d.mkdir(parents=True, exist_ok=True)

            enc_file_exists = any(f.is_file() for f in enc_dir.rglob("*"))
            in_progress_markers = list(dl_dir.glob("*.aria2")) + list(dl_dir.glob("*.part"))
            complete_media_files = [f for f in dl_dir.rglob("*") if f.is_file() and f.suffix.lower() in (".mp4", ".mkv", ".ts", ".webm") and not f.name.endswith(".part")]
            stage = (j.get('stage') or "").lower()

            if stage.startswith("uploaded"): bucket, new_stage = "up", None
            elif enc_file_exists: bucket, new_stage = "up", "encoded"
            elif complete_media_files and not in_progress_markers: bucket, new_stage = "enc", "downloaded"
            else: bucket, new_stage = "dl", "queued"

            if new_stage: await self.update_job(jid, stage=new_stage, pct=0.0)

            is_paused = playlist_status.get(j.get('playlist_id')) in ("paused", "cancelling")
            if is_paused:
                async with self.lock:
                    self.conn.execute('UPDATE jobs SET held = 1 WHERE id = ?', (jid,))
                    self.conn.commit()
                result["held"].append(jid)
            else: result[bucket].append(jid)
        return result


# ═══════════════════════════════════════════════════════════════════════
# CH 06 — DRIP-FEED ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════
async def playlist_drip_feed_loop(db: JobScheduler, dl_q: asyncio.Queue, enc_q: asyncio.Queue, up_q: asyncio.Queue,
                                   dl_pool: "WorkerPool", enc_pool: "WorkerPool", up_pool: "WorkerPool"):
    import random  # Imported locally to ensure lottery system works immediately
    
    while True:
        await asyncio.sleep(3)
        try:
            active_playlists = await db.get_active_playlists()
            for pl in active_playlists:
                if pl['status'] in ('paused', 'cancelled', 'completed', 'cancelling'): continue
                held_jobs = await db.get_held_jobs(pl['id'])
                for hj in held_jobs:
                    await db.clear_held(hj['id'])
                    stage = (hj.get('stage') or '').lower()
                    if stage.startswith('uploaded') or stage == 'encoded': await up_q.put(hj['id'])
                    elif stage == 'downloaded': await enc_q.put(hj['id'])
                    else: await dl_q.put(hj['id'])

            free_gb = get_free_space_gb()
            if free_gb < MIN_FREE_GB: continue

            worker_capacity = dl_pool.target + enc_pool.target + up_pool.target
            effective_cap = max(MAX_GLOBAL_CONCURRENT, worker_capacity)
            total_in_flight = await db.global_in_flight_count()
            if total_in_flight >= effective_cap: continue

            slots_by_space = max(1, int((free_gb - MIN_FREE_GB) / EST_JOB_FOOTPRINT_GB))
            global_slots_free = min(effective_cap - total_in_flight, slots_by_space)
            if global_slots_free <= 0: continue

            # Weighted allocation system
            eligible_playlists = [pl for pl in active_playlists if pl['status'] not in ('paused', 'cancelled', 'completed', 'cancelling')]
            valid_pls, weights = [], []
            
            # Evaluate weights based on how many pending items exist
            for pl in eligible_playlists:
                p_count = await db.get_pending_count(pl['id'])
                if p_count > 0:
                    valid_pls.append(pl)
                    weights.append(p_count)

            # Assign available worker slots
            while global_slots_free > 0 and valid_pls:
                # Weighted random selection (larger queue = more likely to claim the slot)
                chosen_pl = random.choices(valid_pls, weights=weights, k=1)[0]
                idx = valid_pls.index(chosen_pl)

                pending_items = await db.get_pending_items(chosen_pl['id'], limit=1)
                if pending_items:
                    item = pending_items[0]
                    await db.claim_item_as_job(item, chosen_pl['chat_id'])
                    await dl_q.put(item['id'])
                    global_slots_free -= 1
                    
                    # Decrement its weight so it scales dynamically
                    weights[idx] -= 1
                    if weights[idx] <= 0:
                        valid_pls.pop(idx)
                        weights.pop(idx)
                else:
                    valid_pls.pop(idx)
                    weights.pop(idx)

        except Exception as e:
            log.exception(f"Drip Feed Loop Error: {e}")


# ═══════════════════════════════════════════════════════════════════════
# CH 07 — PIPELINE ENGINES (Downloader / Encoder / Uploader)
# ═══════════════════════════════════════════════════════════════════════
class DownloaderEngine:
    def __init__(self, db: JobScheduler, app: Client):
        self.db = db
        self.app = app

    def _extract_vk_api(self, url: str, jid: str) -> str | None:
        VK_TOKEN = getattr(config, "VK_TOKEN", None)
        if not VK_TOKEN: return None
        try:
            import vk_api
            vk_session = vk_api.VkApi(token=VK_TOKEN)
            vk = vk_session.get_api()
            video_id = None
            video_match = re.search(r'video(-?\d+_\d+)', url)
            if video_match: video_id = video_match.group(1)
            if video_id:
                vid_details = vk.video.get(videos=video_id)
                if vid_details and vid_details.get('items'):
                    files = vid_details['items'][0].get('files', {})
                    for q in ['mp4_1080', 'mp4_720', 'mp4_480', 'mp4_360', 'hls']:
                        if q in files:
                            self.db.log_trace(jid, f"[vk_api] Direct {q.upper()} CDN link extracted.")
                            return files[q]
        except Exception as e:
            self.db.log_trace(jid, f"[vk_api] Ghost Protocol Failed: {e}")
        return None

    @staticmethod
    def _get_free_port() -> int:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()
        return port

    @staticmethod
    def _aria2_rpc_call(port: int, secret: str, method: str, params: list | None = None) -> dict:
        payload = {"jsonrpc": "2.0", "id": "poll", "method": method, "params": [f"token:{secret}"] + (params or [])}
        req = urllib.request.Request(f"http://127.0.0.1:{port}/jsonrpc", data=json.dumps(payload).encode("utf-8"), headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=3) as resp:
            return json.loads(resp.read())

    async def _poll_aria2_progress(self, jid: str, port: int, secret: str, stop_event: asyncio.Event):
        for _ in range(30):
            if stop_event.is_set(): return
            try:
                await asyncio.to_thread(self._aria2_rpc_call, port, secret, "aria2.tellActive")
                break
            except Exception: await asyncio.sleep(0.5)

        last_db_update = 0.0
        seen_active = False
        idle_ticks = 0
        MAX_IDLE_TICKS = 8

        while not stop_event.is_set():
            try:
                resp = await asyncio.to_thread(self._aria2_rpc_call, port, secret, "aria2.tellActive")
                active = resp.get("result", [])
            except Exception:
                active = None

            if active:
                seen_active = True
                idle_ticks = 0
                completed = sum(int(d.get("completedLength", 0)) for d in active)
                total = sum(int(d.get("totalLength", 0)) for d in active)
                speed_bps = sum(int(d.get("downloadSpeed", 0)) for d in active)

                pct = (completed / total * 100.0) if total else 0.0
                speed_str = f"{speed_bps / (1024 * 1024):.2f}MiB/s" if speed_bps else "~"
                if speed_bps > 0 and total > completed:
                    eta_sec = int((total - completed) / speed_bps)
                    eta_str = f"{eta_sec // 60}m{eta_sec % 60}s"
                else: eta_str = "~"

                global _live_ui_text
                _live_ui_text[jid] = f"[aria2] {pct:.1f}% at {speed_str} ETA {eta_str}"

                now = time.time()
                if now - last_db_update >= 1.0:
                    await self.db.update_job(jid, pct=pct, stage=f"downloading | {speed_str} | {eta_str}")
                    last_db_update = now
            elif seen_active:
                try: await asyncio.to_thread(self._aria2_rpc_call, port, secret, "aria2.shutdown")
                except Exception: pass
                idle_ticks += 1
                if idle_ticks >= MAX_IDLE_TICKS: return
                seen_active = False
            await asyncio.sleep(1.0)

    async def execute(self, job: dict):
        jid, original_url = job['id'], job['url']
        dl_dir = JOBS_DIR / f"JOB_{jid}" / "dl"

        await self.db.update_job(jid, stage="downloading | ~ | ~")
        extracted_cdn = await asyncio.to_thread(self._extract_vk_api, original_url, jid)
        target_url = extracted_cdn if extracted_cdn else original_url

        rpc_port = self._get_free_port()
        rpc_secret = secrets.token_hex(8)

        opts = {
            "outtmpl": str(dl_dir / f"{jid}.%(ext)s"),
            "format": "bestvideo[height<=1080]+bestaudio/best",
            "merge_output_format": "mkv",
            "quiet": False,
            "noprogress": True,
            "no_warnings": True,
            "compat_opts": {"allow-unsafe-ext"},
            "max_filesize": getattr(config, "VK_MAX_FILESIZE_BYTES", 2 * 1024 * 1024 * 1024),
            "external_downloader": "aria2c",
            "external_downloader_args": [
                "-c", "-j", "16", "-x", "16", "-s", "16", "-k", "5M",
                "--connect-timeout=15", "--timeout=15", "--max-tries=5",
                "--summary-interval=0",
                "--enable-rpc=true", f"--rpc-listen-port={rpc_port}",
                f"--rpc-secret={rpc_secret}", "--rpc-listen-all=false",
            ],
        }

        custom_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        if "srcAg=GECKO" in target_url:
            custom_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"
        elif "srcAg=SAFARI" in target_url:
            custom_ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15"

        opts.setdefault("http_headers", {})
        opts["http_headers"]["User-Agent"] = custom_ua
        if "impersonate" in opts and ("srcAg=" in target_url):
            del opts["impersonate"]

        stop_event = asyncio.Event()
        poller_task = asyncio.create_task(self._poll_aria2_progress(jid, rpc_port, rpc_secret, stop_event))
        try: await asyncio.to_thread(self._run_ytdlp, target_url, jid, opts)
        finally:
            stop_event.set()
            poller_task.cancel()
            try: await poller_task
            except asyncio.CancelledError: pass

    def _run_ytdlp(self, url: str, jid: str, base_opts: dict):
        opts = base_opts.copy()
        opts["quiet"] = True
        opts["noprogress"] = True
        self.db.log_trace(jid, "Executing aria2c-backed downloader...")
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.extract_info(url, download=True)
        except Exception as e:
            self.db.log_trace(jid, f"Download Error: {e}")
            raise e


class EncoderEngine:
    async def execute(self, job: dict, db: JobScheduler):
        jid = job['id']
        dl_dir, enc_dir = JOBS_DIR / f"JOB_{jid}" / "dl", JOBS_DIR / f"JOB_{jid}" / "enc"

        if any(f.is_file() and f.stat().st_size > 0 for f in enc_dir.rglob("*")): return

        files = [f for f in dl_dir.rglob("*") if f.is_file() and f.suffix.lower() in [".mp4", ".mkv", ".ts", ".webm"]]
        if not files: raise RuntimeError("No downloaded media found.")

        src = max(files, key=lambda p: p.stat().st_size)
        dst = enc_dir / f"{jid}{src.suffix}"
        src.rename(dst)

        for f in dl_dir.rglob("*"):
            if f.is_file():
                try: f.unlink()
                except Exception: pass


class _ProgressFilePayload(aiohttp.payload.IOBasePayload):
    def __init__(self, value, progress_cb=None, *args, **kwargs):
        super().__init__(value, *args, **kwargs)
        self._progress_cb = progress_cb

    async def write(self, writer):
        loop = asyncio.get_event_loop()
        sent = 0
        try:
            chunk = await loop.run_in_executor(None, self._value.read, 1024 * 1024)
            while chunk:
                await writer.write(chunk)
                sent += len(chunk)
                if self._progress_cb:
                    await self._progress_cb(sent)
                chunk = await loop.run_in_executor(None, self._value.read, 1024 * 1024)
        finally:
            await loop.run_in_executor(None, self._value.close)


class UploaderEngine:
    def __init__(self, db: JobScheduler, app: Client):
        self.db = db; self.app = app

    async def execute(self, job: dict):
        jid, stage = job['id'], (job.get('stage') or "").lower()
        enc_dir = JOBS_DIR / f"JOB_{jid}" / "enc"

        if not stage.startswith("uploaded"):
            files = [f for f in enc_dir.rglob("*") if f.is_file()]
            if not files: raise RuntimeError("Payload missing from upload queue.")
            enc_file = files[0]
            file_size = enc_file.stat().st_size
            if file_size <= 0: raise RuntimeError("Encoded file is empty (0 bytes) — refusing to upload.")

            pl = await self.db.get_playlist(job['playlist_id'])
            target_album_id, _ = parse_album_caption(pl.get('caption')) if pl else (None, "Default")

            raw_title = job['title']
            clean_title_str, unique_id = raw_title.split("|||", 1) if "|||" in raw_title else (raw_title, "UNKNOWN")
            db_signature = f"\n\n[VK_DB_ID: {unique_id}]"

            def get_upload_server():
                vk = get_vk_api()
                params = {"name": clean_title_str, "description": f"Archived via Stealth Bot{db_signature}"}
                if target_album_id: params["album_id"] = target_album_id
                return vk.video.save(**params)

            upload_data = await asyncio.to_thread(get_upload_server)
            upload_url = upload_data['upload_url']

            custom_timeout = aiohttp.ClientTimeout(total=3600, sock_connect=60, sock_read=300)
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }

            last_update = {"t": 0.0, "bytes": 0}

            async def progress_cb(sent: int):
                now = time.time()
                if now - last_update["t"] < 1.0 and sent < file_size:
                    return
                elapsed = now - last_update["t"] or 1.0
                speed_bps = (sent - last_update["bytes"]) / elapsed
                speed_str = f"{speed_bps / (1024*1024):.2f}MiB/s" if speed_bps > 0 else "~"
                pct = 50.0 + min(sent / file_size, 1.0) * 49.0
                last_update["t"], last_update["bytes"] = now, sent
                await self.db.update_job(jid, pct=pct, stage=f"uploading | {speed_str} | {sent}/{file_size}B")

            await self.db.update_job(jid, pct=50.0, stage="uploading | ~ | ~")

            try:
                async with aiohttp.ClientSession(timeout=custom_timeout, headers=headers) as session:
                    with open(enc_file, 'rb') as f:
                        payload = _ProgressFilePayload(
                            f, progress_cb=progress_cb,
                            filename=enc_file.name, content_type='video/mp4'
                        )
                        form = aiohttp.FormData()
                        form.add_field('video_file', payload, filename=enc_file.name, content_type='video/mp4')

                        async with session.post(upload_url, data=form) as resp:
                            response_data = await resp.json(content_type=None)
                            if 'video_hash' not in response_data:
                                raise RuntimeError(f"VK API Rejected Upload: {response_data}")
            except asyncio.TimeoutError:
                raise RuntimeError("Upload hit the 3600s hard timeout — connection was stalled/dead.")
            except (aiohttp.ClientError, OSError) as e:
                raise RuntimeError(f"Network/Timeout error during upload: {e}")

            await self.db.update_job(jid, stage="uploaded", pct=100.0)
            job['stage'] = "uploaded"

        await self.finalize(job)

    async def finalize(self, job: dict):
        jid, item_id, pl_id = job['id'], job.get('item_id') or job['id'], job['playlist_id']
        if (await self.db.get_item_status(item_id)) != "done":
            global _last_completed
            _last_completed = clean_title(job['title'])
            pl = await self.db.get_playlist(pl_id)
            if pl:
                new_count = pl['downloaded'] + 1
                active_jobs_left = len([j for j in await self.db.get_active_jobs() if j['playlist_id'] == pl_id])
                status = "cancelled" if pl['status'] == "cancelling" and active_jobs_left <= 1 else ("completed" if new_count >= pl['total'] else pl['status'])
                await self.db.update_playlist(pl['id'], downloaded=new_count, status=status)
            await self.db.update_item_status(item_id, "done")
        await self.db.delete_job(jid)
        shutil.rmtree(JOBS_DIR / f"JOB_{jid}", ignore_errors=True)


# ═══════════════════════════════════════════════════════════════════════
# CH 08 — DASHBOARD RENDERER + WORKER PANEL RENDERER
# ═══════════════════════════════════════════════════════════════════════
async def safe_edit(app: Client, chat_id: int, msg_id: int, text: str, kb: InlineKeyboardMarkup | None = None):
    try: await app.edit_message_text(chat_id, msg_id, text, reply_markup=kb)
    except MessageNotModified: pass
    except FloodWait as e: await asyncio.sleep(e.value)
    except Exception: pass


def render_worker_panel(dl_pool: "WorkerPool", enc_pool: "WorkerPool", up_pool: "WorkerPool") -> tuple[str, InlineKeyboardMarkup]:
    text = (
        f"{TXT.WK_TITLE}\n{TXT.DIVIDER}\n"
        f"`{TXT.WK_ROW_DL} : {dl_pool.current_count()}/{dl_pool.target} {TXT.WK_ACTIVE_SUFFIX}`\n"
        f"`{TXT.WK_ROW_PREP} : {enc_pool.current_count()}/{enc_pool.target} {TXT.WK_ACTIVE_SUFFIX}`\n"
        f"`{TXT.WK_ROW_UP} : {up_pool.current_count()}/{up_pool.target} {TXT.WK_ACTIVE_SUFFIX}`\n"
        f"{TXT.DIVIDER}\n"
    )
    kb = [
        [InlineKeyboardButton("−", callback_data="wk|dl|-1"), InlineKeyboardButton(f"{TXT.WK_ROW_DL}: {dl_pool.target}", callback_data="noop"), InlineKeyboardButton("+", callback_data="wk|dl|1")],
        [InlineKeyboardButton("−", callback_data="wk|enc|-1"), InlineKeyboardButton(f"{TXT.WK_ROW_PREP}: {enc_pool.target}", callback_data="noop"), InlineKeyboardButton("+", callback_data="wk|enc|1")],
        [InlineKeyboardButton("−", callback_data="wk|up|-1"), InlineKeyboardButton(f"{TXT.WK_ROW_UP}: {up_pool.target}", callback_data="noop"), InlineKeyboardButton("+", callback_data="wk|up|1")],
        [InlineKeyboardButton(TXT.WK_DONE_BTN, callback_data="wk|close|0")],
    ]
    return text, InlineKeyboardMarkup(kb)


async def render_dashboard(db: JobScheduler, tab: str = "playlists", exp_pl: str = None, exp_bucket: str = None, exp_jid: str = None) -> tuple[str, InlineKeyboardMarkup]:
    playlists, active_jobs = await db.get_active_playlists(), await db.get_active_jobs()
    total_storage, free_gb = sum(f.stat().st_size for f in JOBS_DIR.rglob("*") if f.is_file()) / (1024 ** 3), get_free_space_gb()

    act_text_blocks = [f"{TXT.DASH_ROW_ACTIVE}"]
    if not active_jobs: act_text_blocks = [f"{TXT.DASH_ROW_ACTIVE}` {TXT.DASH_IDLE.strip('`')}`"]
    else:
        for i, j in enumerate(active_jobs[:7]):
            pct, stage_short = float(j.get('pct', 0.0) or 0.0), (j.get('stage') or '').split('|')[0].strip()[:4].upper()
            act_text_blocks.append(f"`  {chr(97+i)}. [{stage_short}] {clean_title(j['title'])[:12]}.. [{make_bar(pct, 8)}] {pct:.0f}%`")

    text = (f"{TXT.DASH_TITLE}\n{TXT.DIVIDER}\n"
            f"{TXT.DASH_ROW_STATUS} `{TXT.DASH_STATUS_LABEL}`\n"
            f"{TXT.DASH_ROW_USED} `{total_storage:.2f} GB`  {TXT.DASH_ROW_FREE} `{free_gb:.2f} GB`\n"
            f"{chr(10).join(act_text_blocks)}\n"
            f"{TXT.DASH_ROW_LAST} `{clean_title(_last_completed)[:12]}`\n{TXT.DIVIDER}")

    kb = []
    is_root_open = (tab == "playlists")
    kb.append([InlineKeyboardButton(f"{'[-]' if is_root_open else '[+]'} {TXT.DASH_PLAYLISTS_HDR} ({len(playlists)})", callback_data=f"dash|{'root' if is_root_open else 'playlists'}")])

    if playlists:
        if any(p['status'] == 'active' for p in playlists): kb.append([InlineKeyboardButton(TXT.DASH_PAUSE_ALL_BTN, callback_data="pause_all")])
        else: kb.append([InlineKeyboardButton(TXT.DASH_RESUME_ALL_BTN, callback_data="resume_all")])

    def _base(stage_str): return stage_str.split("|")[0].strip().lower() if stage_str else "queued"

    if is_root_open:
        if not playlists: kb.append([InlineKeyboardButton(TXT.DASH_SYSTEM_IDLE_ROW, callback_data="noop")])
        else:
            for pl in playlists:
                pl_id, is_this_pl_exp = pl['id'], (exp_pl == pl['id'])
                pl_status_icon = "⏸" if pl['status'] in ("paused", "cancelling") else "▶️"
                status_txt = TXT.DASH_CANCELLING_TAG if pl['status'] == "cancelling" else ""

                _, pl_album_title = parse_album_caption(pl.get('caption'))
                kb.append([InlineKeyboardButton(f" {'[-]' if is_this_pl_exp else '[+]'} {pl_status_icon} {TXT.DASH_ALBUM_PREFIX} {pl_album_title[:16]}{status_txt} [{pl['downloaded']}/{pl['total']}]", callback_data=f"dash|playlists:{pl_id}" if not is_this_pl_exp else "dash|playlists")])

                if is_this_pl_exp:
                    pl_jobs = [j for j in active_jobs if j.get('playlist_id') == pl_id]
                    buckets = {
                        "dl": [j for j in pl_jobs if _base(j['stage']) in ["queued", "downloading"]],
                        "dl_done": [j for j in pl_jobs if _base(j['stage']) == "downloaded"],
                        "enc": [j for j in pl_jobs if _base(j['stage']) in ["encoding", "process"]],
                        "enc_done": [j for j in pl_jobs if _base(j['stage']) == "encoded"],
                        "up": [j for j in pl_jobs if _base(j['stage']) in ["uploading", "uploaded"]]
                    }

                    def build_bucket(bucket_id, icon_label, job_list):
                        icon, label = icon_label
                        is_b_open = (exp_bucket == bucket_id)
                        kb.append([InlineKeyboardButton(f"    ├ {'[-]' if is_b_open else '[+]'} {icon} {label} ({len(job_list)})", callback_data=f"dash|playlists:{pl_id}:{bucket_id}" if not is_b_open else f"dash|playlists:{pl_id}")])
                        if is_b_open:
                            if not job_list: kb.append([InlineKeyboardButton(f"      {TXT.BUCKET_EMPTY_ROW}", callback_data="noop")])
                            for j in job_list:
                                jid, is_j_open, clean_t = j['id'], (exp_jid == j['id']), clean_title(j['title'])
                                if is_j_open:
                                    speed, eta, p = "—", "—", [x.strip() for x in (j.get('stage') or "").split("|")]
                                    if len(p) >= 3: speed, eta = p[1], p[2]
                                    elif len(p) == 2: speed = p[1]
                                    pct = float(j.get('pct', 0.0) or 0.0)
                                    kb.extend([
                                        [InlineKeyboardButton(f"{TXT.JOB_CARD_ID_LABEL}: {jid}", callback_data="noop")],
                                        [InlineKeyboardButton(f"{TXT.JOB_CARD_FILE_ICON} {clean_t[:15]}...", callback_data="noop")],
                                        [InlineKeyboardButton(f"{TXT.JOB_CARD_SPEED_ICON} {speed}  |  {TXT.JOB_CARD_ETA_ICON} {eta}", callback_data="noop")],
                                        [InlineKeyboardButton(f"{TXT.JOB_CARD_PROG_ICON} [{make_bar(pct, 8)}] {pct:.1f}%", callback_data="noop")],
                                        [InlineKeyboardButton(TXT.JOB_LOGS_BTN, callback_data=f"joblog|{jid}"), InlineKeyboardButton(TXT.JOB_KILL_BTN, callback_data=f"kill_job|{jid}")],
                                        [InlineKeyboardButton(TXT.JOB_CLOSE_CARD_BTN, callback_data=f"dash|playlists:{pl_id}:{bucket_id}")]
                                    ])
                                else: kb.append([InlineKeyboardButton(f"      ├ ⚡ {clean_t[:10]}.. | {float(j.get('pct', 0.0) or 0.0):.0f}%", callback_data=f"dash|playlists:{pl_id}:{bucket_id}:{jid}")])

                    build_bucket("dl", TXT.BUCKET_DOWNLOADING, buckets["dl"])
                    build_bucket("dl_done", TXT.BUCKET_WAIT_PREP, buckets["dl_done"])
                    build_bucket("enc", TXT.BUCKET_PREPARING, buckets["enc"])
                    build_bucket("enc_done", TXT.BUCKET_WAIT_UPLOAD, buckets["enc_done"])
                    build_bucket("up", TXT.BUCKET_UPLOADING, buckets["up"])

                    if pl['status'] == "cancelling": kb.append([InlineKeyboardButton(TXT.PL_CANCELLING_ROW, callback_data="noop")])
                    elif pl['status'] == "paused": kb.append([InlineKeyboardButton(TXT.PL_RESUME_BTN, callback_data=f"res|{pl['id']}"), InlineKeyboardButton(TXT.PL_GRACEFUL_CANCEL_BTN, callback_data=f"graceful_cancel|{pl['id']}")])
                    else: kb.append([InlineKeyboardButton(TXT.PL_PAUSE_BTN, callback_data=f"pause|{pl['id']}"), InlineKeyboardButton(TXT.PL_GRACEFUL_CANCEL_BTN, callback_data=f"graceful_cancel|{pl['id']}")])
                    kb.append([InlineKeyboardButton(TXT.PL_PURGE_BTN, callback_data=f"kill|{pl['id']}")])
                    kb.append([InlineKeyboardButton("───────────────────", callback_data="noop")])

    # ---- AUTOSCAN dedicated section — collapsed by default, tap to expand ----
    ascan_tags = await db.get_autoscan_tags()
    ascan_comms = await db.get_autoscan_communities()
    ascan_enabled = (await db.get_autoscan_state('enabled', '0')) == '1'
    ascan_last_poll = await db.get_autoscan_state('last_poll_ts')
    ascan_total_q = await db.get_autoscan_state('total_queued', '0')
    ascan_last_poll_str = TXT.ASCAN_PANEL_NEVER if not ascan_last_poll else time.strftime('%Y-%m-%d %H:%M', time.localtime(int(ascan_last_poll)))

    kb.append([InlineKeyboardButton("───────────────────", callback_data="noop")])
    kb.append([InlineKeyboardButton(
        f"{'[-]' if _autoscan_panel_expanded else '[+]'} {TXT.ASCAN_PANEL_HDR} (🏷{len(ascan_tags)} 📡{len(ascan_comms)}) {TXT.ASCAN_PANEL_ON if ascan_enabled else TXT.ASCAN_PANEL_OFF}",
        callback_data="ascan_ui|toggle"
    )])

    if _autoscan_panel_expanded:
        kb.append([InlineKeyboardButton(TXT.ASCAN_PANEL_TAGS_HDR, callback_data="noop")])
        if not ascan_tags:
            kb.append([InlineKeyboardButton(TXT.ASCAN_PANEL_EMPTY, callback_data="noop")])
        else:
            for t in ascan_tags[:10]:
                kb.append([InlineKeyboardButton(f"      🏷 #{t['tag']}", callback_data="noop"), InlineKeyboardButton("❌", callback_data=f"ascan|rmtag|{t['id']}")])
            if len(ascan_tags) > 10:
                kb.append([InlineKeyboardButton(f"      …and {len(ascan_tags) - 10} more", callback_data="noop")])
        kb.append([InlineKeyboardButton(TXT.ASCAN_PANEL_ADD_TAG_BTN, callback_data="ascan|addtag")])

        kb.append([InlineKeyboardButton(TXT.ASCAN_PANEL_COMMS_HDR, callback_data="noop")])
        if not ascan_comms:
            kb.append([InlineKeyboardButton(TXT.ASCAN_PANEL_EMPTY, callback_data="noop")])
        else:
            for c in ascan_comms[:10]:
                label = (c.get('display_name') or c['url'])[:26]
                status_dot = "🟢" if c.get('historical_done') else "🟡"
                kb.append([InlineKeyboardButton(f"      📡 {status_dot} {label}", callback_data="noop"), InlineKeyboardButton("❌", callback_data=f"ascan|rmcomm|{c['id']}")])
            if len(ascan_comms) > 10:
                kb.append([InlineKeyboardButton(f"      …and {len(ascan_comms) - 10} more", callback_data="noop")])
        kb.append([InlineKeyboardButton(TXT.ASCAN_PANEL_ADD_COMM_BTN, callback_data="ascan|addcomm")])

        kb.append([
            InlineKeyboardButton(TXT.ASCAN_PANEL_TOGGLE_OFF_BTN if ascan_enabled else TXT.ASCAN_PANEL_TOGGLE_ON_BTN, callback_data="ascan|togglepause"),
            InlineKeyboardButton(TXT.ASCAN_PANEL_RESCAN_BTN, callback_data="ascan|rescan")
        ])
        kb.append([InlineKeyboardButton(f"{TXT.ASCAN_PANEL_LAST_POLL} {ascan_last_poll_str}", callback_data="noop")])
        kb.append([InlineKeyboardButton(f"{TXT.ASCAN_PANEL_TOTAL_QUEUED} {ascan_total_q}", callback_data="noop")])

    kb.append([InlineKeyboardButton(TXT.DASH_REFRESH_BTN, callback_data="refresh")])
    return text, InlineKeyboardMarkup(kb)


async def refresh_dashboard(app: Client, db: JobScheduler):
    """Re-renders and pushes the live dashboard message, if one is open. Shared
    by the router (after any state-changing action) and by the Autoscan engine
    (after a background poll finds something new)."""
    global _dash_msg_id, _dash_chat_id, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid
    if _dash_msg_id and _dash_chat_id:
        text, kb = await render_dashboard(db, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid)
        await safe_edit(app, _dash_chat_id, _dash_msg_id, text, kb)


# ═══════════════════════════════════════════════════════════════════════
# CH 09 — /scan UI RENDERER (community scan results)
# ═══════════════════════════════════════════════════════════════════════
def render_scan_ui(chat_id: int):
    session = _scan_sessions.get(chat_id)
    if not session: return TXT.SCAN_EXPIRED, None

    text = f"{TXT.SCAN_RESULTS_TITLE}\n🔗 {session['url']}\n{TXT.DIVIDER}\n"
    kb = []

    vids, vp = session['videos'], session['vid_page']
    text += f"{TXT.SCAN_VIDEOS_HDR} (Total: {len(vids)})\n"
    if vids:
        start_v = vp * 5
        for i, v in enumerate(vids[start_v:start_v+5]):
            kb.append([InlineKeyboardButton(f"📥 {v['title'][:30]}...", callback_data=f"scan|up|vid|{start_v + i}")])
        nav_v = []
        if vp > 0: nav_v.append(InlineKeyboardButton("◀ Prev Vids", callback_data="scan|page|vid|-1"))
        if start_v + 5 < len(vids): nav_v.append(InlineKeyboardButton("Next Vids ▶", callback_data="scan|page|vid|1"))
        if nav_v: kb.append(nav_v)
        kb.append([InlineKeyboardButton(TXT.SCAN_UPLOAD_ALL_VIDS_BTN, callback_data="scan|up|vid|all")])
    else: text += TXT.SCAN_NONE_FOUND

    text += f"{TXT.DIVIDER}\n"

    wall, wp = session['wall'], session['wall_page']
    text += f"{TXT.SCAN_WALL_HDR} (Total: {len(wall)})\n"
    if wall:
        start_w = wp * 5
        for i, v in enumerate(wall[start_w:start_w+5]):
            kb.append([InlineKeyboardButton(f"📥 {v['title'][:30]}...", callback_data=f"scan|up|wall|{start_w + i}")])
        nav_w = []
        if wp > 0: nav_w.append(InlineKeyboardButton("◀ Prev Wall", callback_data="scan|page|wall|-1"))
        if start_w + 5 < len(wall): nav_w.append(InlineKeyboardButton("Next Wall ▶", callback_data="scan|page|wall|1"))
        if nav_w: kb.append(nav_w)
        kb.append([InlineKeyboardButton(TXT.SCAN_UPLOAD_ALL_WALL_BTN, callback_data="scan|up|wall|all")])
    else: text += TXT.SCAN_NONE_FOUND

    kb.append([InlineKeyboardButton(TXT.SCAN_CANCEL_BTN, callback_data="scan|close")])
    return text, InlineKeyboardMarkup(kb)


# ═══════════════════════════════════════════════════════════════════════
# CH 10 — /scan #tag & /deepscan #tag SESSION STATE + RENDERERS
# ═══════════════════════════════════════════════════════════════════════
def render_deepscan_ui(chat_id: int, progress_note: str = None):
    session = _deepscan_sessions.get(chat_id)
    if not session: return TXT.DSCAN_SESSION_EXPIRED, None

    tag, communities = session['tag'], session['communities']
    total_videos = sum(len(c['videos']) for c in communities)

    text = f"{TXT.DSCAN_RESULTS_TITLE}\n🏷 Tag: `#{tag}`\n"
    if progress_note:
        text += f"{progress_note}\n"
    text += (f"{TXT.DSCAN_ROW_COMMUNITIES} `{len(communities)}`  |  {TXT.DSCAN_ROW_TOTAL_VIDEOS} `{total_videos}`\n"
             f"{TXT.DIVIDER}")

    kb = []
    for ci, c in enumerate(communities):
        is_exp = ci in session['expanded']
        kb.append([InlineKeyboardButton(f"{'[-]' if is_exp else '[+]'} {c['name'][:28]} ({len(c['videos'])})", callback_data=f"dscan|t|{ci}")])
        if is_exp:
            for vi, v in enumerate(c['videos'][:15]):
                kb.append([InlineKeyboardButton(f"    📥 {v['title'][:32]}", callback_data=f"dscan|v|{ci}|{vi}")])
            if len(c['videos']) > 15:
                kb.append([InlineKeyboardButton(TXT.DSCAN_MORE_ROW.format(n=len(c['videos']) - 15), callback_data="noop")])
            kb.append([InlineKeyboardButton(TXT.DSCAN_QUEUE_ALL_COMM_BTN.format(count=len(c['videos'])), callback_data=f"dscan|c|{ci}")])

    kb.append([InlineKeyboardButton(TXT.DSCAN_QUEUE_EVERYTHING_BTN.format(count=total_videos), callback_data="dscan|all")])
    kb.append([InlineKeyboardButton(TXT.DSCAN_CLOSE_BTN, callback_data="dscan|close")])
    return text, InlineKeyboardMarkup(kb)


def _hashtag_pattern(tag: str) -> re.Pattern:
    """
    Builds a flexible matcher for a tag so it catches every real-world
    caption style we've seen, not just '#TagName' verbatim:
      #HasanBasu, #Hasan Basu, #Hasan_Basu, #Hasan-Basu,
      xxxxx.Hasan.Basu.xxxxx, hasan.basu, HASAN BASU, etc.

    Strategy: split the tag into "word" chunks — first on any existing
    separator (space/dot/underscore/hyphen), then further on camelCase
    boundaries — then rejoin the chunks allowing ANY run of separator
    characters (or none at all) between them. Word-boundary anchors on
    each end mean "xxxxx.Hasan.Basu.xxxxx" matches (dot is a boundary)
    while "HasanBasuExtra" as one solid run of letters won't falsely
    match a totally different tag.
    """
    raw_chunks = [c for c in re.split(r'[\s._\-]+', tag) if c]
    words = []
    for chunk in raw_chunks:
        found = re.findall(r'[A-Z]?[a-z0-9]+|[A-Z]+(?![a-z])', chunk)
        words.extend(found if found else [chunk])
    if not words:
        words = [tag]
    sep = r'[\s._\-]*'
    body = sep.join(re.escape(w) for w in words)
    return re.compile(rf"#?\b{body}\b", re.IGNORECASE)


def _iter_post_texts_and_videos(post: dict, max_depth: int = 5) -> tuple[list[str], list[dict]]:
    """
    Recursively walks a wall post AND any reposts nested under it via
    VK's 'copy_history' field. A post that bundles/reposts content from
    another community stores the original post (with its own text and
    attachments) inside copy_history rather than at the top level — so
    without this, hashtag matching and video collection both silently
    skip anything that was reposted in.

    Returns (all_text_strings_found, all_video_attachment_dicts_found)
    across the post and every nested repost, deepest-first order.
    """
    texts, videos = [], []

    def walk(p: dict, depth: int):
        if not p or depth > max_depth:
            return
        texts.append(p.get('text', '') or '')
        for att in (p.get('attachments') or []):
            if att.get('type') == 'video':
                videos.append(att['video'])
        for nested in (p.get('copy_history') or []):
            walk(nested, depth + 1)

    walk(post, 0)
    return texts, videos


def oldest_first(videos: list[dict]) -> list[dict]:
    """
    VK's video.get / wall.get endpoints return items newest-first by
    default. Since VK albums display newest-*added* on top, uploading
    in that same newest-first order actually puts the OLDEST video on
    top (each later upload pushes earlier ones down). Reversing here
    before we queue means we upload oldest -> newest, so the newest
    video naturally ends up on top of the album once the batch finishes.
    """
    return list(reversed(videos))


VK_CALL_TIMEOUT_SEC = 30.0  # guards against a single stuck VK API call hanging autoscan/deepscan forever


async def _vk_call(fn, *args, timeout: float = VK_CALL_TIMEOUT_SEC, **kwargs):
    """Runs a blocking vk_api call in a thread with a hard timeout. Without
    this, one slow/stuck VK request (rate limiting, network hiccup) makes the
    whole scan look permanently frozen with zero feedback — this turns that
    into a clear, catchable error instead."""
    return await asyncio.wait_for(asyncio.to_thread(fn, *args, **kwargs), timeout=timeout)


async def deepscan_search_community(vk, comm_id: int, pattern: re.Pattern, max_posts: int = None) -> list[dict]:
    """Shared wall-walker used by /deepscan AND the Autoscan engine (CH 10B).
    max_posts caps how deep to page — full historical scans use
    DEEPSCAN_MAX_POSTS_PER_COMMUNITY, Autoscan's light monitoring pass uses
    the much smaller AUTOSCAN_MONITOR_MAX_POSTS."""
    if max_posts is None:
        max_posts = DEEPSCAN_MAX_POSTS_PER_COMMUNITY
    videos, seen = [], set()
    offset, count = 0, 100
    while True:
        res = await _vk_call(vk.wall.get, owner_id=comm_id, count=count, offset=offset)
        posts = res.get('items', [])
        if not posts: break
        for post in posts:
            texts, post_videos = _iter_post_texts_and_videos(post)
            if any(pattern.search(t) for t in texts):
                for v in post_videos:
                    uid = f"{v['owner_id']}_{v['id']}"
                    if uid not in seen:
                        seen.add(uid)
                        videos.append({'url': f"https://vk.com/video{uid}", 'title': v.get('title', 'VK Video'), 'unique_id': uid})
        offset += count
        if offset >= res.get('count', 0) or offset >= max_posts: break
        await asyncio.sleep(DEEPSCAN_PAGE_DELAY_SEC)
    return oldest_first(videos)


async def queue_tagged_videos(db: JobScheduler, chat_id: int, tag: str, videos: list[dict], source_label: str = None) -> tuple[int, int]:
    """Dedupe against target album + queue as a playlist. Returns (queued, skipped)."""
    def dedupe_and_prepare():
        vk = get_vk_api()
        target_album_id = get_or_create_vk_album(vk, tag)
        existing_ids = get_existing_vk_db_ids(vk, target_album_id)
        missing = [v for v in videos if v['unique_id'] not in existing_ids]
        return target_album_id, missing

    target_album_id, missing = await asyncio.to_thread(dedupe_and_prepare)
    if not missing: return 0, len(videos)
    pl_id = str(uuid.uuid4())[:8]
    await db.create_playlist(pl_id, source_label or f"deepscan:#{tag}", f"{target_album_id}|||{tag}", len(missing), chat_id)
    db_items = [(str(uuid.uuid4())[:8], pl_id, it['url'], f"{it['title']}|||{it['unique_id']}") for it in missing]
    await db.add_playlist_items(db_items)
    return len(missing), len(videos) - len(missing)


# ═══════════════════════════════════════════════════════════════════════
# CH 10B — AUTOSCAN ENGINE (background historical scan + continuous watch)
# ═══════════════════════════════════════════════════════════════════════
def parse_multi_tags(text: str) -> list[str]:
    """Splits free-form text into hashtag words: '#Tag1 #Tag2', 'Tag1, Tag2',
    newline-separated, etc. Strips a leading '#', dedupes case-insensitively."""
    raw = re.split(r'[,\s]+', text.strip())
    seen, out = set(), []
    for r in raw:
        t = r.strip().lstrip('#').strip()
        if not t: continue
        key = t.lower()
        if key in seen: continue
        seen.add(key)
        out.append(t)
    return out


def parse_multi_links(text: str) -> list[str]:
    """Splits free-form text into VK links: space/comma/newline separated,
    ignores anything that doesn't look like a URL."""
    raw = re.split(r'[,\s]+', text.strip())
    seen, out = set(), []
    for r in raw:
        u = r.strip()
        if not u.startswith('http'): continue
        if u in seen: continue
        seen.add(u)
        out.append(u)
    return out


async def _resolve_autoscan_community_name(vk, comm_id: int, fallback: str) -> str:
    try:
        info = await _vk_call(vk.groups.getById, group_id=str(-comm_id))
        if info: return info[0].get('name', fallback)
    except Exception:
        pass
    return fallback


async def _autoscan_full_scan_community(db: JobScheduler, vk, comm: dict, tag_rows: list[dict], progress_cb=None) -> int:
    """
    One-time (per community) FULL wall-history scan across every tracked
    hashtag. This is what /autoscan runs during initial setup (and what the
    engine loop falls back to for any community still flagged
    historical_done=0, e.g. one added later via the dashboard).

    Groups matches by tag and queues each tag's batch in one shot (single
    album dedupe lookup per tag instead of per-video) — oldest-first so the
    newest match ends up on top of the VK album once uploads finish.

    progress_cb(offset, total_posts, queued_so_far), if given, is awaited
    periodically (throttled) so a caller can stream live UI updates.
    """
    patterns = {t['tag']: _hashtag_pattern(t['tag']) for t in tag_rows}
    matched_by_tag: dict[str, list[dict]] = {t['tag']: [] for t in tag_rows}
    seen_uids = set()
    offset, count, total_count, last_cb = 0, 100, None, 0.0

    while True:
        res = await _vk_call(vk.wall.get, owner_id=comm['resolved_id'], count=count, offset=offset)
        posts = res.get('items', [])
        if total_count is None: total_count = res.get('count', 0)
        if not posts: break

        for post in posts:
            texts, post_videos = _iter_post_texts_and_videos(post)
            for tag, pattern in patterns.items():
                if any(pattern.search(t) for t in texts):
                    for v in post_videos:
                        uid = f"{v['owner_id']}_{v['id']}"
                        if uid in seen_uids: continue
                        seen_uids.add(uid)
                        matched_by_tag[tag].append({'url': f"https://vk.com/video{uid}", 'title': v.get('title', 'VK Video'), 'unique_id': uid})

        offset += count
        now = time.time()
        if progress_cb and (now - last_cb >= PROGRESS_EDIT_MIN_INTERVAL_SEC or offset >= (total_count or 0)):
            queued_so_far = sum(len(v) for v in matched_by_tag.values())
            await progress_cb(offset, total_count or offset, queued_so_far)
            last_cb = now
        if offset >= (total_count or 0) or offset >= DEEPSCAN_MAX_POSTS_PER_COMMUNITY: break
        await asyncio.sleep(HASHTAG_SCAN_PAGE_DELAY_SEC)

    total_queued = 0
    for tag, vids in matched_by_tag.items():
        if not vids: continue
        queued, _ = await queue_tagged_videos(db, OWNER_ID, tag, oldest_first(vids), source_label=f"autoscan:{comm['url']}")
        for v in vids: await db.mark_autoscan_seen(v['unique_id'], tag)
        total_queued += queued
    return total_queued


async def _autoscan_light_scan_community(db: JobScheduler, app: Client, vk, comm: dict, tag_rows: list[dict]) -> int:
    """Cheap steady-state monitoring pass — only the newest page of wall
    posts (AUTOSCAN_MONITOR_MAX_POSTS), since new content always appears at
    the top. Sends a Telegram notification for anything it queues."""
    patterns = {t['tag']: _hashtag_pattern(t['tag']) for t in tag_rows}
    matched_by_tag: dict[str, list[dict]] = {t['tag']: [] for t in tag_rows}
    seen_uids = set()

    res = await _vk_call(vk.wall.get, owner_id=comm['resolved_id'], count=AUTOSCAN_MONITOR_MAX_POSTS, offset=0)
    for post in res.get('items', []):
        texts, post_videos = _iter_post_texts_and_videos(post)
        for tag, pattern in patterns.items():
            if any(pattern.search(t) for t in texts):
                for v in post_videos:
                    uid = f"{v['owner_id']}_{v['id']}"
                    if uid in seen_uids: continue
                    seen_uids.add(uid)
                    if await db.is_autoscan_seen(uid): continue
                    matched_by_tag[tag].append({'url': f"https://vk.com/video{uid}", 'title': v.get('title', 'VK Video'), 'unique_id': uid})

    total_queued = 0
    for tag, vids in matched_by_tag.items():
        if not vids: continue
        queued, _ = await queue_tagged_videos(db, OWNER_ID, tag, oldest_first(vids), source_label=f"autoscan:{comm['url']}")
        for v in vids: await db.mark_autoscan_seen(v['unique_id'], tag)
        if queued and OWNER_ID:
            try:
                await app.send_message(OWNER_ID, TXT.ASCAN_NEW_HIT_NOTIFY.format(
                    comm=comm.get('display_name') or comm['url'], tag=tag, queued=queued))
            except Exception:
                pass
        total_queued += queued
    return total_queued


async def autoscan_cycle(db: JobScheduler, app: Client):
    """One full pass over every tracked community: resolves any community
    that hasn't been resolved yet, does a full historical scan for anything
    still flagged historical_done=0, and a light monitoring pass for
    everything else. Called both by the periodic loop and by 'Force Rescan'."""
    tags = await db.get_autoscan_tags()
    communities = await db.get_autoscan_communities()
    if not tags or not communities:
        return

    vk = await asyncio.to_thread(get_vk_api)
    total_queued_this_cycle = 0

    for comm in communities:
        if not comm.get('resolved_id'):
            try:
                comm_id = await _vk_call(resolve_vk_community_id, vk, comm['url'])
                display_name = await _resolve_autoscan_community_name(vk, comm_id, comm['url'])
                await db.update_autoscan_community(comm['id'], resolved_id=comm_id, display_name=display_name)
                comm['resolved_id'], comm['display_name'] = comm_id, display_name
            except Exception as e:
                log.warning(f"Autoscan: couldn't resolve {comm['url']}: {e}")
                continue

        try:
            if not comm.get('historical_done'):
                queued = await _autoscan_full_scan_community(db, vk, comm, tags)
                await db.update_autoscan_community(comm['id'], historical_done=1)
            else:
                queued = await _autoscan_light_scan_community(db, app, vk, comm, tags)
            total_queued_this_cycle += queued
        except Exception as e:
            log.warning(f"Autoscan cycle error on {comm.get('url')}: {e}")

    if total_queued_this_cycle:
        await db.incr_autoscan_total_queued(total_queued_this_cycle)
    await db.set_autoscan_state('last_poll_ts', str(int(time.time())))
    await refresh_dashboard(app, db)


async def autoscan_engine_loop(db: JobScheduler, app: Client):
    """Background task started once at bot startup (CH 13 main()). Sleeps
    AUTOSCAN_POLL_INTERVAL_SEC between cycles; skips entirely if monitoring
    is paused or nothing is configured yet."""
    while True:
        await asyncio.sleep(AUTOSCAN_POLL_INTERVAL_SEC)
        try:
            enabled = (await db.get_autoscan_state('enabled', '0')) == '1'
            if not enabled: continue
            await autoscan_cycle(db, app)
        except Exception as e:
            log.exception(f"Autoscan engine loop error: {e}")


# ═══════════════════════════════════════════════════════════════════════
# CH 11 — ROUTER: COMMAND HANDLERS & CALLBACK HANDLERS
# ═══════════════════════════════════════════════════════════════════════
def setup_router(app: Client, db: JobScheduler, dl_q: asyncio.Queue, enc_q: asyncio.Queue, up_q: asyncio.Queue, dl_pool: "WorkerPool", enc_pool: "WorkerPool", up_pool: "WorkerPool"):

    async def refresh_dashboard_if_open():
        await refresh_dashboard(app, db)

    # ──────────────────────────────────────────────
    # 11.0 — Autoscan wizard/quick-add text interceptor.
    # MUST be registered before any other text/link handler (catch_album_target,
    # handle_hashtag_community_link, auto_catch_playlist all match similar
    # patterns) so it gets first refusal while a wizard or quick-add prompt is
    # active, and transparently steps aside (continue_propagation) otherwise.
    # ──────────────────────────────────────────────
    def _ascan_tags_wizard_view(session: dict) -> tuple[str, InlineKeyboardMarkup]:
        lines = [TXT.ASCAN_INIT_TAGS_PROMPT, "", TXT.ASCAN_TAGS_LIST_TITLE.format(n=len(session['tags']))]
        lines.append(TXT.ASCAN_TAGS_EMPTY_ROW if not session['tags'] else "\n".join(f"  🏷 #{t}" for t in session['tags']))
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(TXT.ASCAN_DONE_TAGS_BTN, callback_data="ascanwiz|done_tags")],
            [InlineKeyboardButton(TXT.ASCAN_CANCEL_BTN, callback_data="ascanwiz|cancel")]
        ])
        return "\n".join(lines), kb

    def _ascan_comms_wizard_view(session: dict) -> tuple[str, InlineKeyboardMarkup]:
        lines = [TXT.ASCAN_INIT_COMMS_PROMPT, "", TXT.ASCAN_COMMS_LIST_TITLE.format(n=len(session['comms']))]
        lines.append(TXT.ASCAN_COMMS_EMPTY_ROW if not session['comms'] else "\n".join(f"  📡 {u}" for u in session['comms']))
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(TXT.ASCAN_DONE_COMMS_BTN, callback_data="ascanwiz|done_comms")],
            [InlineKeyboardButton(TXT.ASCAN_CANCEL_BTN, callback_data="ascanwiz|cancel")]
        ])
        return "\n".join(lines), kb

    def _ascan_confirm_view(session: dict) -> tuple[str, InlineKeyboardMarkup]:
        text = (f"{TXT.ASCAN_CONFIRM_TITLE}\n{TXT.DIVIDER}\n"
                f"{TXT.ASCAN_CONFIRM_TAGS_ROW} " + ", ".join(f"#{t}" for t in session['tags']) + "\n"
                f"{TXT.ASCAN_CONFIRM_COMMS_ROW} `{len(session['comms'])}`\n"
                f"{TXT.ASCAN_CONFIRM_NOTE}")
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(TXT.ASCAN_START_BTN, callback_data="ascanwiz|start")],
            [InlineKeyboardButton(TXT.ASCAN_CANCEL_BTN, callback_data="ascanwiz|cancel")]
        ])
        return text, kb

    async def run_autoscan_historical(chat_id: int, msg_id: int, tags: list[str], comm_urls: list[str]):
        """The wizard's initial batch scan — resolves each new community, runs
        a full historical scan with live progress against ALL currently
        tracked tags (not just the ones just added), then flips monitoring on.

        Always posts SOMETHING on every community transition (even instant/
        empty ones) and shows overall community i/N progress, not just
        within-community post-paging — otherwise a large batch (many tags
        and/or many communities) looks frozen with zero feedback."""
        for t in tags:
            await db.add_autoscan_tag(t)

        await safe_edit(app, chat_id, msg_id, TXT.ASCAN_STARTING.format(
            tags=len(tags), comms=len(comm_urls), y="y" if len(comm_urls) == 1 else "ies"))

        vk = await asyncio.to_thread(get_vk_api)
        resolved_comms = []
        for url in comm_urls:
            try:
                comm_id = await _vk_call(resolve_vk_community_id, vk, url)
                display_name = await _resolve_autoscan_community_name(vk, comm_id, url)
                await db.add_autoscan_community(url)
                rows = await db.get_autoscan_communities()
                row = next((c for c in rows if c['url'] == url), None)
                if row:
                    await db.update_autoscan_community(row['id'], resolved_id=comm_id, display_name=display_name)
                    row['resolved_id'], row['display_name'] = comm_id, display_name
                    resolved_comms.append(row)
            except Exception as e:
                await safe_edit(app, chat_id, msg_id, TXT.ASCAN_RESOLVE_FAILED.format(url=url, err=e))
                await asyncio.sleep(1.2)

        tag_rows = await db.get_autoscan_tags()
        total_queued = 0
        total_comms = len(resolved_comms)

        for ci, comm in enumerate(resolved_comms):
            comm_label = comm.get('display_name') or comm['url']
            # Fires immediately, before the first vk.wall.get for this
            # community even happens — so a fast/empty community still
            # produces a visible "now on X of Y" update.
            await safe_edit(app, chat_id, msg_id,
                f"{TXT.ASCAN_HIST_SWITCHING}\n{TXT.DIVIDER}\n"
                f"{TXT.ASCAN_HIST_ROW_COMM} `{comm_label}`  ({ci+1}/{total_comms})")

            async def progress_cb(offset, total, queued_so_far, _comm=comm, _ci=ci):
                pct = min(offset / total * 100.0, 100.0) if total else 0.0
                await safe_edit(app, chat_id, msg_id,
                    f"{TXT.ASCAN_HIST_PROGRESS_TITLE}\n{TXT.DIVIDER}\n"
                    f"{TXT.ASCAN_HIST_ROW_COMM} `{_comm.get('display_name') or _comm['url']}`  ({_ci+1}/{total_comms})\n"
                    f"`[{make_bar(pct, 16)}] {pct:.0f}%`\n"
                    f"{TXT.ASCAN_HIST_ROW_PROGRESS} `{min(offset, total)}/{total}`\n"
                    f"{TXT.ASCAN_HIST_ROW_QUEUED} `{queued_so_far}`")

            try:
                queued = await _autoscan_full_scan_community(db, vk, comm, tag_rows, progress_cb=progress_cb)
                await db.update_autoscan_community(comm['id'], historical_done=1)
                total_queued += queued
            except asyncio.TimeoutError:
                log.warning(f"Autoscan historical scan timed out on {comm.get('url')}")
                await safe_edit(app, chat_id, msg_id, TXT.ASCAN_HIST_COMM_TIMEOUT.format(name=comm_label))
                await asyncio.sleep(1.5)
            except Exception as e:
                log.warning(f"Autoscan historical scan failed on {comm.get('url')}: {e}")
                await safe_edit(app, chat_id, msg_id, TXT.ASCAN_HIST_COMM_ERROR.format(name=comm_label, err=e))
                await asyncio.sleep(1.5)

        await db.set_autoscan_state('enabled', '1')
        await db.set_autoscan_state('last_poll_ts', str(int(time.time())))

        final_tags = await db.get_autoscan_tags()
        final_comms = await db.get_autoscan_communities()
        await safe_edit(app, chat_id, msg_id,
            f"{TXT.ASCAN_HIST_DONE_TITLE}\n{TXT.DIVIDER}\n"
            f"{TXT.ASCAN_HIST_DONE_ROW_TAGS} `{len(final_tags)}`\n"
            f"{TXT.ASCAN_HIST_DONE_ROW_COMMS} `{len(final_comms)}`\n"
            f"{TXT.ASCAN_HIST_DONE_ROW_QUEUED} `{total_queued}`"
            f"{TXT.ASCAN_HIST_DONE_NOTE}")
        await refresh_dashboard_if_open()

    @app.on_message(filters.command(["autoscan"]) & filters.user(OWNER_ID))
    async def cmd_autoscan(_, msg: Message):
        _autoscan_wizard_sessions[msg.chat.id] = {'stage': 'tags', 'tags': [], 'comms': []}
        text, kb = _ascan_tags_wizard_view(_autoscan_wizard_sessions[msg.chat.id])
        m = await msg.reply(text, reply_markup=kb)
        _autoscan_wizard_sessions[msg.chat.id]['setup_msg_id'] = m.id

    @app.on_message(filters.text & filters.user(OWNER_ID))
    async def intercept_autoscan_input(_, msg: Message):
        chat_id, text = msg.chat.id, (msg.text or "").strip()
        wiz, pending = _autoscan_wizard_sessions.get(chat_id), _autoscan_pending.get(chat_id)

        if (not wiz and not pending) or text.startswith('/'):
            return msg.continue_propagation()

        if text.lower() == "cancel":
            _autoscan_wizard_sessions.pop(chat_id, None)
            _autoscan_pending.pop(chat_id, None)
            return await msg.reply(TXT.ASCAN_CANCELLED)

        if wiz:
            if wiz['stage'] == 'tags':
                new_tags = parse_multi_tags(text)
                if not new_tags: return msg.continue_propagation()
                for t in new_tags:
                    if t.lower() not in [x.lower() for x in wiz['tags']]: wiz['tags'].append(t)
                view_text, kb = _ascan_tags_wizard_view(wiz)
                await safe_edit(app, chat_id, wiz['setup_msg_id'], view_text, kb)
                try: await msg.delete()
                except Exception: pass
                return

            elif wiz['stage'] == 'comms':
                new_links = parse_multi_links(text)
                if not new_links: return msg.continue_propagation()
                for u in new_links:
                    if u not in wiz['comms']: wiz['comms'].append(u)
                view_text, kb = _ascan_comms_wizard_view(wiz)
                await safe_edit(app, chat_id, wiz['setup_msg_id'], view_text, kb)
                try: await msg.delete()
                except Exception: pass
                return

            return msg.continue_propagation()

        if pending == 'add_tag':
            new_tags = parse_multi_tags(text)
            if not new_tags: return msg.continue_propagation()
            added = 0
            for t in new_tags:
                if await db.add_autoscan_tag(t): added += 1
            _autoscan_pending.pop(chat_id, None)
            await msg.reply(TXT.ASCAN_TAG_ADDED.format(tag=", ".join(new_tags)) if added else TXT.ASCAN_TAG_ADDED.format(tag="(already tracked)"))
            if await db.get_autoscan_tags() and await db.get_autoscan_communities():
                await db.set_autoscan_state('enabled', '1')
            await refresh_dashboard_if_open()
            return

        if pending == 'add_comm':
            links = parse_multi_links(text)
            if not links: return msg.continue_propagation()
            _autoscan_pending.pop(chat_id, None)
            vk = await asyncio.to_thread(get_vk_api)
            added_names = []
            for url in links:
                try:
                    comm_id = await _vk_call(resolve_vk_community_id, vk, url)
                    display_name = await _resolve_autoscan_community_name(vk, comm_id, url)
                    if await db.add_autoscan_community(url):
                        rows = await db.get_autoscan_communities()
                        row = next((c for c in rows if c['url'] == url), None)
                        if row: await db.update_autoscan_community(row['id'], resolved_id=comm_id, display_name=display_name)
                        added_names.append(display_name)
                except Exception as e:
                    await msg.reply(TXT.ASCAN_COMM_ADD_FAILED.format(err=e))
            if added_names:
                await msg.reply(TXT.ASCAN_COMM_ADDED.format(name=", ".join(added_names)))
            if await db.get_autoscan_tags() and await db.get_autoscan_communities():
                await db.set_autoscan_state('enabled', '1')
            await refresh_dashboard_if_open()
            return

        return msg.continue_propagation()

    # ──────────────────────────────────────────────
    # 11.1 — /transfer (bookmarks + fuzzy tag)
    # ──────────────────────────────────────────────
    @app.on_message(filters.command(["transfer"]) & filters.user(OWNER_ID))
    async def cmd_transfer(_, msg: Message):
        args = msg.text.split(maxsplit=1)
        if len(args) < 2:
            return await msg.reply(TXT.TRANSFER_USAGE)

        url = args[1].strip()
        m = await msg.reply(TXT.TRANSFER_INIT)

        async def run_transfer_job(chat_id, msg_id, link, db_instance):
            try:
                vk = await asyncio.to_thread(get_vk_api)
                comm_id = await _vk_call(resolve_vk_community_id, vk, link)
                tag_manager = await asyncio.to_thread(FuzzyTagManager, vk)

                offset, processed, added, skipped = 0, 0, 0, 0

                while True:
                    res = await _vk_call(vk.wall.get, owner_id=comm_id, count=100, offset=offset)
                    posts = res.get('items', [])
                    if not posts: break

                    for post in posts:
                        processed += 1
                        texts, video_atts = _iter_post_texts_and_videos(post)

                        if not video_atts:
                            log.info(f"⏭ SKIPPED Post {post.get('id')}: No native video attachments found (incl. reposts).")
                            skipped += 1
                            continue

                        # Try parsing every text found (top post + any nested
                        # reposts) — the caption with the [Production] Name x Name
                        # format may live on the original post being reposted in,
                        # not on the top-level wrapper post.
                        parsed_data = None
                        for candidate_text in texts:
                            parsed_data = parse_caption(candidate_text)
                            if parsed_data:
                                caption = candidate_text
                                break
                        if not parsed_data:
                            clean_cap = (texts[0] if texts else '').replace('\n', ' ')[:50]
                            log.info(f"⏭ SKIPPED Post {post.get('id')}: No caption (incl. reposts) matched format. Capt: '{clean_cap}...'")
                            skipped += 1
                            continue

                        production, names_list, _ = parsed_data
                        target_names = [production] + names_list

                        for v in video_atts:
                            uid = f"{v['owner_id']}_{v['id']}"
                            if await db_instance.is_transferred(uid):
                                log.info(f"⏭ SKIPPED Video {uid}: Already marked as transferred in database.")
                                skipped += 1
                                continue

                            def do_bookmark():
                                vk.fave.addVideo(owner_id=v['owner_id'], id=v['id'])
                                resolved_tag_ids = [tag_manager.get_or_create_tag_id(n) for n in target_names if n]
                                valid_ids = [str(t_id) for t_id in resolved_tag_ids if t_id]
                                if valid_ids:
                                    vk.fave.setTags(item_type="video", item_owner_id=v['owner_id'], item_id=v['id'], tag_ids=",".join(valid_ids))

                            try:
                                await asyncio.to_thread(do_bookmark)
                                await db_instance.mark_transferred(uid)
                                added += 1
                            except Exception as e:
                                error_msg = str(e).lower()
                                if "access denied" in error_msg or "code 15" in error_msg or "code 204" in error_msg:
                                    pl_id = f"trans_{uid[:8]}"
                                    await db_instance.create_playlist(pl_id, f"https://vk.com/video{uid}", "Bookmarks", 1, chat_id)
                                    item_id = str(uuid.uuid4())[:8]
                                    db_item = [(item_id, pl_id, f"https://vk.com/video{uid}", f"{v.get('title', 'Private Video')}|||{uid}")]
                                    await db_instance.add_playlist_items(db_item)
                                    log.warning(f"Video {uid} bookmark denied. Queued for physical download.")
                                else:
                                    log.error(f"Failed to bookmark {uid}: {e}")
                                skipped += 1

                        if processed % 50 == 0:
                            report = (f"{TXT.TRANSFER_PROGRESS_TITLE}\n{TXT.DIVIDER}\n"
                                      f"{TXT.TRANSFER_ROW_SCANNED} `{processed}` posts\n"
                                      f"{TXT.TRANSFER_ROW_BOOKMARKED} `{added}` videos\n"
                                      f"{TXT.TRANSFER_ROW_SKIPPED} `{skipped}`")
                            await safe_edit(app, chat_id, msg_id, report)

                    offset += 100
                    await asyncio.sleep(0.5)

                final_report = (f"{TXT.TRANSFER_COMPLETE_TITLE}\n{TXT.DIVIDER}\n"
                                f"{TXT.TRANSFER_ROW_TARGET} `{link}`\n"
                                f"{TXT.TRANSFER_ROW_TOTAL} `{processed}`\n"
                                f"{TXT.TRANSFER_ROW_SUCCESS} `{added}`\n"
                                f"{TXT.TRANSFER_ROW_SKIPPED2} `{skipped}`")
                await safe_edit(app, chat_id, msg_id, final_report)

            except Exception as e:
                await safe_edit(app, chat_id, msg_id, TXT.TRANSFER_CRITICAL_ERROR.format(err=e))

        asyncio.create_task(run_transfer_job(msg.chat.id, m.id, url, db))

    # ──────────────────────────────────────────────
    # 11.2 — /scan (community-wide scan)
    # ──────────────────────────────────────────────
    @app.on_message(filters.command(["scan"]) & filters.user(OWNER_ID))
    async def cmd_scan(_, msg: Message):
        args = msg.text.split(maxsplit=1)
        if len(args) < 2:
            return await msg.reply(TXT.SCAN_USAGE)

        if args[1].strip().startswith("#"):
            return msg.continue_propagation()

        url = args[1].strip()
        m = await msg.reply(TXT.SCAN_RUNNING)

        def perform_scan():
            vk = get_vk_api()
            comm_id = resolve_vk_community_id(vk, url)
            videos, wall = [], []

            try:
                offset, count = 0, 100
                while True:
                    res = vk.video.get(owner_id=comm_id, count=count, offset=offset)
                    for v in res.get('items', []):
                        uid = f"{v['owner_id']}_{v['id']}"
                        videos.append({'url': f"https://vk.com/video{uid}", 'title': v.get('title', 'VK Video'), 'unique_id': uid})
                    offset += count
                    if offset >= res.get('count', 0) or offset >= 1000: break
            except Exception as e: print(f"Vid fetch err: {e}")

            try:
                offset, count = 0, 100
                while True:
                    res = vk.wall.get(owner_id=comm_id, count=count, offset=offset)
                    posts = res.get('items', [])
                    if not posts: break
                    for post in posts:
                        _, post_videos = _iter_post_texts_and_videos(post)
                        for v in post_videos:
                            uid = f"{v['owner_id']}_{v['id']}"
                            wall.append({'url': f"https://vk.com/video{uid}", 'title': v.get('title', 'VK Video'), 'unique_id': uid})
                    offset += count
                    if offset >= res.get('count', 0) or offset >= 1000: break
            except Exception as e: print(f"Wall fetch err: {e}")

            return oldest_first(videos), oldest_first(wall)

        try:
            videos, wall = await asyncio.to_thread(perform_scan)
            if not videos and not wall: return await m.edit(TXT.SCAN_NO_VIDEOS_FOUND)

            _scan_sessions[msg.chat.id] = {
                'url': url, 'videos': videos, 'wall': wall,
                'vid_page': 0, 'wall_page': 0, 'pending_upload': None
            }

            text, kb = render_scan_ui(msg.chat.id)
            await m.edit(text, reply_markup=kb)
        except Exception as e:
            await m.edit(TXT.SCAN_FAILED.format(err=e))

    @app.on_message(filters.regex(r"^#") & filters.user(OWNER_ID))
    async def catch_album_target(_, msg: Message):
        session = _scan_sessions.get(msg.chat.id)
        if not session or not session.get('pending_upload'):
            return msg.continue_propagation()

        album_name = msg.text.strip().lstrip('#').strip()
        pending = session['pending_upload']
        m = await msg.reply(TXT.SCAN_RESOLVING_ALBUM.format(album=album_name))

        items_to_process = []
        source_list = session['videos'] if pending['type'] == 'vid' else session['wall']
        if pending['mode'] == 'single': items_to_process.append(source_list[pending['index']])
        else: items_to_process.extend(source_list)

        def dedupe_and_prepare():
            vk = get_vk_api()
            target_album_id = get_or_create_vk_album(vk, album_name)
            existing_ids = get_existing_vk_db_ids(vk, target_album_id)
            missing = [vid for vid in items_to_process if vid['unique_id'] not in existing_ids]
            return target_album_id, missing

        try:
            target_album_id, missing_videos = await asyncio.to_thread(dedupe_and_prepare)

            if not missing_videos:
                session['pending_upload'] = None
                return await m.edit(TXT.SCAN_ALL_PRESENT.format(album=album_name))

            pl_id = str(uuid.uuid4())[:8]
            await db.create_playlist(pl_id, session['url'], f"{target_album_id}|||{album_name}", len(missing_videos), msg.chat.id)
            db_items = [(str(uuid.uuid4())[:8], pl_id, item['url'], f"{item['title']}|||{item['unique_id']}") for item in missing_videos]
            await db.add_playlist_items(db_items)

            skipped = len(items_to_process) - len(missing_videos)
            session['pending_upload'] = None

            await m.edit(TXT.SCAN_LOCKED_TO_ALBUM.format(selected=len(items_to_process), skipped=skipped, queued=len(missing_videos)))
            await refresh_dashboard_if_open()

            text, kb = render_scan_ui(msg.chat.id)
            if 'msg_id' in session: await safe_edit(app, msg.chat.id, session['msg_id'], text, kb)

        except Exception as e:
            await m.edit(TXT.SCAN_QUEUE_ERROR.format(err=e))

    # ──────────────────────────────────────────────
    # 11.3 — /scan #Hashtag (wall search by tag, WITH progress bar)
    # ──────────────────────────────────────────────
    async def run_hashtag_scan(chat_id: int, tag: str, url: str):
        """Runs independently for each tag, spawning its own UI card."""
        m = await app.send_message(chat_id, TXT.HSCAN_RESOLVING)
        try:
            vk = await asyncio.to_thread(get_vk_api)
            comm_id = await _vk_call(resolve_vk_community_id, vk, url)
            pattern = _hashtag_pattern(tag)

            matching_videos, seen_uids = [], set()
            offset, count, total_count, last_edit = 0, 100, None, 0.0

            while True:
                res = await _vk_call(vk.wall.get, owner_id=comm_id, count=count, offset=offset)
                posts = res.get('items', [])
                if total_count is None: total_count = res.get('count', 0)
                if not posts: break

                for post in posts:
                    texts, post_videos = _iter_post_texts_and_videos(post)
                    if any(pattern.search(t) for t in texts):
                        for v in post_videos:
                            uid = f"{v['owner_id']}_{v['id']}"
                            if uid not in seen_uids:
                                seen_uids.add(uid)
                                matching_videos.append({'url': f"https://vk.com/video{uid}", 'title': v.get('title', 'VK Video'), 'unique_id': uid})

                offset += count
                now = time.time()
                if now - last_edit >= PROGRESS_EDIT_MIN_INTERVAL_SEC or offset >= (total_count or 0):
                    pct = min(offset / total_count * 100.0, 100.0) if total_count else 0.0
                    await safe_edit(app, chat_id, m.id,
                        f"{TXT.HSCAN_PROGRESS_TITLE.format(tag=tag)}\n{TXT.DIVIDER}\n"
                        f"`[{make_bar(pct, 16)}] {pct:.0f}%`\n"
                        f"{TXT.HSCAN_PROGRESS_POSTS} `{min(offset, total_count or offset)}/{total_count or '?'}`\n"
                        f"{TXT.HSCAN_PROGRESS_MATCHES} `{len(matching_videos)}`")
                    last_edit = now

                if offset >= (total_count or 0): break
                await asyncio.sleep(HASHTAG_SCAN_PAGE_DELAY_SEC)

            videos = oldest_first(matching_videos)
            if not videos:
                return await safe_edit(app, chat_id, m.id, TXT.HSCAN_NONE_FOUND.format(tag=tag))

            # Key the session state to this specific bot message ID! (Allows concurrent sessions)
            _hashtag_scan_results[m.id] = {'tag': tag, 'url': url, 'videos': videos}

            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton(TXT.HSCAN_DOWNLOAD_BTN.format(count=len(videos)), callback_data="ht_confirm_download"),
                InlineKeyboardButton(TXT.HSCAN_CANCEL_BTN, callback_data="ht_cancel")
            ]])
            await safe_edit(app, chat_id, m.id,
                f"{TXT.HSCAN_COMPLETE_TITLE}\n{TXT.DIVIDER}\n"
                f"{TXT.HSCAN_ROW_TERM} `#{tag}`\n{TXT.HSCAN_ROW_MATCHES} `{len(videos)}`\n"
                f"{TXT.HSCAN_ROW_ALBUM} `{tag}`\n{TXT.HSCAN_CONFIRM_PROMPT.format(count=len(videos), tag=tag)}", kb)
        except Exception as e:
            await safe_edit(app, chat_id, m.id, TXT.HSCAN_ERROR.format(err=e))

    @app.on_message(filters.regex(r"^/scan\s+(#[^\s]+.*)") & filters.user(OWNER_ID))
    async def cmd_scan_hashtag(_, msg: Message):
        # Extract all hashtags
        tags = re.findall(r'#([A-Za-z0-9_]+)', msg.text)
        if not tags: return await msg.reply(TXT.HSCAN_USAGE)
            
        # Check if they also provided a URL in the same command
        urls = re.findall(r'(https?://[^\s]+)', msg.text)
        if urls:
            url = urls[0]
            # Immediately spawn a background task for EACH hashtag simultaneously
            for tag in tags:
                asyncio.create_task(run_hashtag_scan(msg.chat.id, tag, url))
            return

        if msg.chat.id not in _hashtag_scan_pending:
            _hashtag_scan_pending[msg.chat.id] = []
            
        _hashtag_scan_pending[msg.chat.id].append({'tags': tags, 'stage': 'awaiting_link'})
        tags_formatted = ", ".join(f"#{t}" for t in tags)
        await msg.reply(TXT.HSCAN_INIT.format(tag=tags_formatted))

    @app.on_message(filters.regex(r"https?://") & filters.user(OWNER_ID))
    async def intercept_urls_for_hashtags(_, msg: Message):
        pending = _hashtag_scan_pending.get(msg.chat.id, [])
        if not pending:
            return msg.continue_propagation()
            
        session = pending.pop(0)  # FIFO: Take the oldest waiting request
        url = msg.text.strip()
        
        # Spawn a separate UI card and search process for EVERY tag requested
        for tag in session['tags']:
            asyncio.create_task(run_hashtag_scan(msg.chat.id, tag, url))

    # ──────────────────────────────────────────────
    # 11.4 — /deepscan #Hashtag (scan every joined community, WITH progress bar)
    # ──────────────────────────────────────────────
    async def run_deepscan(chat_id: int, msg_id: int, tag: str):
        try:
            vk = await asyncio.to_thread(get_vk_api)
            groups, offset = [], 0
            while True:
                res = await _vk_call(vk.groups.get, extended=1, count=1000, offset=offset)
                items = res.get('items', [])
                groups.extend(items)
                offset += 1000
                if offset >= res.get('count', 0): break

            if not groups:
                return await safe_edit(app, chat_id, msg_id, TXT.DSCAN_NO_COMMUNITIES)

            pattern = _hashtag_pattern(tag)
            total_groups = len(groups)
            # Session is created up front and grown as each community finishes,
            # so the interactive (tappable) results are live from the very
            # first hit — no need to wait for the whole sweep to complete.
            _deepscan_sessions[chat_id] = {'tag': tag, 'communities': [], 'expanded': set()}
            last_edit = 0.0

            for i, g in enumerate(groups):
                comm_id = -int(g['id'])
                try:
                    vids = await deepscan_search_community(vk, comm_id, pattern)
                except Exception as e:
                    log.warning(f"Deepscan failed on {g.get('name')}: {e}")
                    vids = []
                if vids:
                    _deepscan_sessions[chat_id]['communities'].append({'id': comm_id, 'name': g.get('name', f"Community {g['id']}"), 'videos': vids})

                now = time.time()
                if now - last_edit >= PROGRESS_EDIT_MIN_INTERVAL_SEC or i == total_groups - 1:
                    pct = (i + 1) / total_groups * 100.0
                    note = f"{TXT.DSCAN_SCANNING_NOTE_PREFIX} `[{make_bar(pct, 16)}] {pct:.0f}%` ({i+1}/{total_groups})"
                    text, kb = render_deepscan_ui(chat_id, progress_note=note if i < total_groups - 1 else None)
                    await safe_edit(app, chat_id, msg_id, text, kb)
                    last_edit = now

            if not _deepscan_sessions[chat_id]['communities']:
                _deepscan_sessions.pop(chat_id, None)
                return await safe_edit(app, chat_id, msg_id, TXT.DSCAN_NONE_FOUND.format(tag=tag, total=total_groups))
        except Exception as e:
            await safe_edit(app, chat_id, msg_id, TXT.DSCAN_FAILED.format(err=e))

    @app.on_message(filters.regex(r"^/deepscan\s+#(\w+)") & filters.user(OWNER_ID))
    async def cmd_deepscan(_, msg: Message):
        match = re.search(r"^/deepscan\s+#(\w+)", msg.text.strip())
        if not match:
            return await msg.reply(TXT.DSCAN_USAGE)
        tag = match.group(1).strip()
        m = await msg.reply(TXT.DSCAN_FETCHING_GROUPS)
        asyncio.create_task(run_deepscan(msg.chat.id, m.id, tag))

    # ──────────────────────────────────────────────
    # 11.5 — Standard playlist catcher (plain link + #Album)
    # ──────────────────────────────────────────────
    @app.on_message(filters.regex(r"https?://") & filters.user(OWNER_ID) & ~filters.command(["scan", "transfer", "vk_dash", "vk_workers", "vk_pause_all", "vk_resume_all", "deepscan"]))
    async def auto_catch_playlist(_, msg: Message):
        text = msg.text.strip()
        parts = text.split("#", 1)
        url = parts[0].strip()

        if len(parts) < 2 or not parts[1].strip():
            return await msg.reply(TXT.AUTO_NEEDS_ALBUM)

        target_album_name = parts[1].strip()
        url = re.sub(r'vk\.ru', 'vk.com', url, flags=re.IGNORECASE)
        m = await msg.reply(TXT.AUTO_RESOLVING.format(album=target_album_name))

        def extract_and_dedupe(playlist_url: str, album_name: str):
            vk = get_vk_api()
            target_album_id = get_or_create_vk_album(vk, album_name)
            existing_ids = get_existing_vk_db_ids(vk, target_album_id)
            all_videos = []

            match = re.search(r'playlist/(-?\d+)_(\d+)', playlist_url)
            if not match: match = re.search(r'album_(-?\d+)_(\d+)', playlist_url)

            if match:
                owner_id, album_id = int(match.group(1)), int(match.group(2))
                offset, count = 0, 100
                while True:
                    res = vk.video.get(owner_id=owner_id, album_id=album_id, count=count, offset=offset)
                    items = res.get('items', [])
                    if not items: break
                    for v in items:
                        unique_id = f"{v['owner_id']}_{v['id']}"
                        all_videos.append({'url': f"https://vk.com/video{unique_id}", 'title': v.get('title', 'VK Video'), 'unique_id': unique_id})
                    offset += count
                    if offset >= res.get('count', 0): break
            else:
                match_wall = re.search(r'wall(-?\d+)_(\d+)', playlist_url)
                if match_wall:
                    posts = vk.wall.getById(posts=f"{match_wall.group(1)}_{match_wall.group(2)}")
                    if posts:
                        for att in posts[0].get('attachments', []):
                            if att.get('type') == 'video':
                                v = att['video']
                                uid = f"{v['owner_id']}_{v['id']}"
                                all_videos.append({'url': f"https://vk.com/video{uid}", 'title': v.get('title', 'VK Video'), 'unique_id': uid})
                else:
                    cookie_path = "vk_temp_cookies.txt"
                    if VK_COOKIES:
                        with open(cookie_path, "w", encoding="utf-8") as f:
                            f.write("# Netscape HTTP Cookie File\n")
                            for item in VK_COOKIES.strip().split(';'):
                                if '=' in item:
                                    k, v = item.strip().split('=', 1)
                                    f.write(f".vk.com\tTRUE\t/\tTRUE\t2147483647\t{k}\t{v}\n")
                    opts = {'extract_flat': True, 'quiet': True, 'cookiefile': cookie_path if VK_COOKIES else None}
                    with yt_dlp.YoutubeDL(opts) as ydl:
                        data = ydl.extract_info(playlist_url, download=False)
                        for e in data.get('entries', []):
                            v_url = e.get('url') or e.get('webpage_url')
                            if v_url: all_videos.append({'url': v_url, 'title': e.get('title', 'VK Video'), 'unique_id': str(e.get('id', uuid.uuid4().hex[:10]))})

            all_videos = oldest_first(all_videos)
            missing_videos = [v for v in all_videos if v['unique_id'] not in existing_ids]
            return target_album_id, len(all_videos), missing_videos

        try:
            target_album_id, total_found, items_to_upload = await asyncio.to_thread(extract_and_dedupe, url, target_album_name)

            if not items_to_upload:
                if total_found > 0: return await m.edit(TXT.AUTO_ALL_PRESENT.format(total=total_found, album=target_album_name))
                else: return await m.edit(TXT.AUTO_NONE_FOUND)

            pl_id = str(uuid.uuid4())[:8]
            await db.create_playlist(pl_id, url, f"{target_album_id}|||{target_album_name}", len(items_to_upload), msg.chat.id)
            db_items = [(str(uuid.uuid4())[:8], pl_id, item['url'], f"{item['title']}|||{item['unique_id']}") for item in items_to_upload]
            await db.add_playlist_items(db_items)

            await m.edit(TXT.AUTO_LOCKED.format(total=total_found, skipped=total_found - len(items_to_upload), queued=len(items_to_upload)))
            await refresh_dashboard_if_open()

        except Exception as e: await m.edit(TXT.AUTO_ERROR.format(err=e))

    # ──────────────────────────────────────────────
    # 11.6 — Dashboard / worker / pause-resume commands
    # ──────────────────────────────────────────────
    @app.on_message(filters.command(["vk_dash"]) & filters.user(OWNER_ID))
    async def cmd_dash(_, msg: Message):
        global _dash_msg_id, _dash_chat_id, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid
        global _stack_msg_id, _stack_chat_id
        
        # 1. Spawn the Active Jobs (Stack) card first
        jobs = await db.get_active_jobs()
        stack_msg = await msg.reply(render_stack_card(jobs))
        _stack_msg_id, _stack_chat_id = stack_msg.id, stack_msg.chat.id

        # 2. Spawn the Main Dashboard card right under it
        text, kb = await render_dashboard(db, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid)
        m = await msg.reply(text, reply_markup=kb)
        _dash_msg_id, _dash_chat_id = m.id, m.chat.id

    @app.on_message(filters.command(["vk_pause_all"]) & filters.user(OWNER_ID))
    async def cmd_pause_all(_, msg: Message):
        count = await db.pause_all_playlists()
        await msg.reply(TXT.MSG_PAUSED_N.format(n=count))
        await refresh_dashboard_if_open()

    @app.on_message(filters.command(["vk_resume_all"]) & filters.user(OWNER_ID))
    async def cmd_resume_all(_, msg: Message):
        count = await db.resume_all_playlists()
        await msg.reply(TXT.MSG_RESUMED_N.format(n=count))
        await refresh_dashboard_if_open()

    @app.on_message(filters.command(["vk_workers"]) & filters.user(OWNER_ID))
    async def cmd_workers(_, msg: Message):
        args = msg.command[1:]
        if not args:
            text, kb = render_worker_panel(dl_pool, enc_pool, up_pool)
            return await msg.reply(text, reply_markup=kb)

        changes = {}
        for arg in args:
            if "=" in arg:
                k, v = arg.split("=", 1)
                if k.lower() in ("dl", "enc", "up") and v.lstrip("-").isdigit(): changes[k.lower()] = int(v)

        if not changes: return await msg.reply(TXT.WORKERS_USAGE)

        lines = []
        pools = {"dl": dl_pool, "enc": enc_pool, "up": up_pool}
        for key, new_target in changes.items():
            pool = pools[key]
            before = pool.current_count()
            await pool.adjust(new_target)
            lines.append(f"{key.upper()} {before} → {new_target}")

        await msg.reply("✅ " + " | ".join(lines))

    # ──────────────────────────────────────────────
    # 11.7 — Callback query router
    # ──────────────────────────────────────────────
    @app.on_callback_query()
    async def handle_callbacks(_, cb: CallbackQuery):
        global _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid
        if cb.data == "noop": return await cb.answer()

        # --- Autoscan setup wizard callbacks ---
        if cb.data.startswith("ascanwiz|"):
            action = cb.data.split("|")[1]
            chat_id = cb.message.chat.id
            wiz = _autoscan_wizard_sessions.get(chat_id)
            if not wiz: return await cb.answer("Setup session expired.", show_alert=True)
            wiz['setup_msg_id'] = cb.message.id

            if action == "cancel":
                _autoscan_wizard_sessions.pop(chat_id, None)
                await cb.answer()
                return await safe_edit(app, chat_id, cb.message.id, TXT.ASCAN_CANCELLED, None)

            elif action == "done_tags":
                if not wiz['tags']:
                    return await cb.answer(TXT.ASCAN_NEED_ONE_TAG, show_alert=True)
                wiz['stage'] = 'comms'
                await cb.answer()
                text, kb = _ascan_comms_wizard_view(wiz)
                return await safe_edit(app, chat_id, cb.message.id, text, kb)

            elif action == "done_comms":
                if not wiz['comms']:
                    return await cb.answer(TXT.ASCAN_NEED_ONE_COMM, show_alert=True)
                wiz['stage'] = 'confirm'
                await cb.answer()
                text, kb = _ascan_confirm_view(wiz)
                return await safe_edit(app, chat_id, cb.message.id, text, kb)

            elif action == "start":
                tags, comms = wiz['tags'], wiz['comms']
                _autoscan_wizard_sessions.pop(chat_id, None)
                await cb.answer()
                asyncio.create_task(run_autoscan_historical(chat_id, cb.message.id, tags, comms))
                return

        # --- Autoscan panel collapse/expand ---
        if cb.data == "ascan_ui|toggle":
            global _autoscan_panel_expanded
            _autoscan_panel_expanded = not _autoscan_panel_expanded
            await cb.answer()
            text, kb = await render_dashboard(db, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid)
            return await safe_edit(app, cb.message.chat.id, cb.message.id, text, kb)

        # --- Autoscan dashboard panel callbacks ---
        if cb.data.startswith("ascan|"):
            parts = cb.data.split("|")
            action = parts[1]
            chat_id = cb.message.chat.id

            if action == "addtag":
                _autoscan_pending[chat_id] = 'add_tag'
                return await cb.answer(TXT.ASCAN_ADD_TAG_PROMPT, show_alert=True)

            elif action == "addcomm":
                _autoscan_pending[chat_id] = 'add_comm'
                return await cb.answer(TXT.ASCAN_ADD_COMM_PROMPT, show_alert=True)

            elif action == "rmtag":
                tag_id = int(parts[2])
                rows = await db.get_autoscan_tags()
                row = next((t for t in rows if t['id'] == tag_id), None)
                await db.remove_autoscan_tag(tag_id)
                await cb.answer(TXT.ASCAN_TAG_REMOVED.format(tag=row['tag'] if row else ""))

            elif action == "rmcomm":
                comm_id = int(parts[2])
                rows = await db.get_autoscan_communities()
                row = next((c for c in rows if c['id'] == comm_id), None)
                await db.remove_autoscan_community(comm_id)
                await cb.answer(TXT.ASCAN_COMM_REMOVED.format(name=(row.get('display_name') or row.get('url')) if row else ""))

            elif action == "togglepause":
                enabled_now = (await db.get_autoscan_state('enabled', '0')) == '1'
                await db.set_autoscan_state('enabled', '0' if enabled_now else '1')
                await cb.answer(TXT.ASCAN_PANEL_TOGGLE_OFF_BTN if enabled_now else TXT.ASCAN_PANEL_TOGGLE_ON_BTN)

            elif action == "rescan":
                await db.mark_all_communities_need_rescan()
                await cb.answer(TXT.ASCAN_RESCAN_STARTED, show_alert=True)
                asyncio.create_task(autoscan_cycle(db, app))

            text, kb = await render_dashboard(db, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid)
            return await safe_edit(app, chat_id, cb.message.id, text, kb)

        # --- Scan Callbacks ---
        if cb.data.startswith("scan|"):
            parts = cb.data.split("|")
            action = parts[1]
            session = _scan_sessions.get(cb.message.chat.id)
            if not session: return await cb.answer("Scan expired.", show_alert=True)
            session['msg_id'] = cb.message.id

            if action == "close":
                del _scan_sessions[cb.message.chat.id]
                await cb.message.delete()
                return await cb.answer(TXT.SCAN_CLOSED_TOAST)

            elif action == "page":
                sec, delta = parts[2], int(parts[3])
                if sec == 'vid': session['vid_page'] = max(0, session['vid_page'] + delta)
                else: session['wall_page'] = max(0, session['wall_page'] + delta)
                text, kb = render_scan_ui(cb.message.chat.id)
                return await safe_edit(app, cb.message.chat.id, cb.message.id, text, kb)

            elif action == "up":
                sec, val = parts[2], parts[3]
                mode = 'single' if val.isdigit() else 'all'
                idx = int(val) if val.isdigit() else 0
                session['pending_upload'] = {'type': sec, 'mode': mode, 'index': idx}

                text, kb = render_scan_ui(cb.message.chat.id)
                await safe_edit(app, cb.message.chat.id, cb.message.id, text + TXT.SCAN_WAITING_ALBUM_PROMPT, kb)
                return await cb.answer(TXT.SCAN_SEND_ALBUM_TOAST)

        # --- Hashtag Scan Confirmation Callbacks ---
        if cb.data == "ht_cancel":
            _hashtag_scan_results.pop(cb.message.id, None)
            await cb.answer("Cancelled.")
            return await cb.message.edit(TXT.HSCAN_CANCELLED)

        elif cb.data == "ht_confirm_download":
            session = _hashtag_scan_results.get(cb.message.id)
            if not session or not session.get('videos'):
                return await cb.answer(TXT.HSCAN_SESSION_EXPIRED, show_alert=True)

            videos, album_name, comm_url = session['videos'], session['tag'], session['url']
            await cb.answer("Queueing videos for download and upload...")
            await cb.message.edit(TXT.HSCAN_RESOLVING_ALBUM)

            try:
                queued, skipped = await queue_tagged_videos(db, cb.message.chat.id, album_name, videos, source_label=comm_url)

                if queued == 0:
                    _hashtag_scan_results.pop(cb.message.id, None)
                    return await cb.message.edit(TXT.HSCAN_ALL_PRESENT.format(count=len(videos), album=album_name))

                _hashtag_scan_results.pop(cb.message.id, None)
                await cb.message.edit(
                    f"{TXT.HSCAN_BATCH_TITLE}\n{TXT.DIVIDER}\n"
                    f"{TXT.HSCAN_ROW_ALBUM} `{album_name}`\n"
                    f"{TXT.HSCAN_ROW_FOUND} `{len(videos)}`\n"
                    f"{TXT.HSCAN_ROW_SKIPPED} `{skipped}`\n"
                    f"{TXT.HSCAN_ROW_QUEUED} `{queued}`")
                await refresh_dashboard_if_open()
            except Exception as e:
                await cb.message.edit(TXT.HSCAN_QUEUE_ERROR.format(err=e))

        # --- Deep Scan Callbacks ---
        if cb.data.startswith("dscan|"):
            parts = cb.data.split("|")
            action = parts[1]
            session = _deepscan_sessions.get(cb.message.chat.id)
            if action != "close" and not session:
                return await cb.answer("Session expired.", show_alert=True)

            if action == "close":
                _deepscan_sessions.pop(cb.message.chat.id, None)
                await cb.answer(TXT.TOAST_WORKERS_CLOSED)
                try: await cb.message.delete()
                except Exception: pass
                return

            elif action == "t":
                ci = int(parts[2])
                session['expanded'].symmetric_difference_update({ci})
                text, kb = render_deepscan_ui(cb.message.chat.id)
                await cb.answer()
                return await safe_edit(app, cb.message.chat.id, cb.message.id, text, kb)

            elif action == "v":
                ci, vi = int(parts[2]), int(parts[3])
                video = session['communities'][ci]['videos'][vi]
                await cb.answer(TXT.DSCAN_QUEUEING_VIDEO)
                queued, skipped = await queue_tagged_videos(db, cb.message.chat.id, session['tag'], [video])
                await cb.message.reply(TXT.DSCAN_VIDEO_RESULT.format(queued=queued, skipped=skipped))

            elif action == "c":
                ci = int(parts[2])
                comm = session['communities'][ci]
                await cb.answer(TXT.DSCAN_QUEUEING_N.format(n=len(comm['videos'])))
                queued, skipped = await queue_tagged_videos(db, cb.message.chat.id, session['tag'], comm['videos'])
                await cb.message.reply(TXT.DSCAN_COMMUNITY_RESULT.format(name=comm['name'], queued=queued, skipped=skipped))

            elif action == "all":
                all_videos = [v for c in session['communities'] for v in c['videos']]
                await cb.answer(TXT.DSCAN_QUEUEING_N.format(n=len(all_videos)))
                queued, skipped = await queue_tagged_videos(db, cb.message.chat.id, session['tag'], all_videos)
                await cb.message.reply(
                    f"{TXT.DSCAN_ALL_TITLE}\n"
                    f"{TXT.DSCAN_ALL_ROW_TOTAL} `{len(all_videos)}`\n"
                    f"{TXT.DSCAN_ALL_ROW_QUEUED} `{queued}`  {TXT.DSCAN_ALL_ROW_DUPES} `{skipped}`")

            await refresh_dashboard_if_open()
            return

        # --- Worker Callbacks ---
        if cb.data.startswith("wk|"):
            _, pool_key, delta_str = cb.data.split("|")
            pools = {"dl": dl_pool, "enc": enc_pool, "up": up_pool}
            if pool_key == "close":
                await cb.answer(TXT.TOAST_WORKERS_CLOSED)
                try: await cb.message.delete()
                except Exception: pass
                return
            pool = pools[pool_key]
            new_target = max(0, pool.target + int(delta_str))
            await pool.adjust(new_target)
            await cb.answer(f"{pool_key.upper()} workers: {new_target}")
            text, kb = render_worker_panel(dl_pool, enc_pool, up_pool)
            return await safe_edit(app, cb.message.chat.id, cb.message.id, text, kb)

        # --- Dashboard Callbacks ---
        elif cb.data.startswith("dash|"):
            parts = cb.data.split("|")[1].split(":")
            _dash_tab = parts[0]
            _expanded_pl = parts[1] if len(parts) > 1 else None
            _expanded_bucket = parts[2] if len(parts) > 2 else None
            _expanded_jid = parts[3] if len(parts) > 3 else None
            await cb.answer()

        elif cb.data == "refresh": await cb.answer("Refreshed.")
        elif cb.data == "pause_all":
            await db.pause_all_playlists()
            await cb.answer(TXT.TOAST_PAUSED_ALL, show_alert=True)
        elif cb.data == "resume_all":
            await db.resume_all_playlists()
            await cb.answer(TXT.TOAST_RESUMED_ALL, show_alert=True)

        elif cb.data.startswith("joblog|"):
            jid = cb.data.split("|")[1]
            log_path = JOBS_DIR / f"JOB_{jid}" / "trace.log"
            if not log_path.exists(): return await cb.answer(TXT.TOAST_NO_LOGS, show_alert=True)
            with open(log_path, "r", encoding="utf-8") as f:
                lines = f.read().splitlines()
                recent = "\n".join(lines[-15:]) if lines else "No data."
            return await cb.answer(f"--- TRACE LOGS ---\n{recent}", show_alert=True)

        elif cb.data.startswith("kill_job|"):
            jid = cb.data.split("|")[1]
            await db.force_fail_job(jid)
            _expanded_jid = None
            await cb.answer(TXT.TOAST_JOB_KILLED, show_alert=True)

        elif cb.data.startswith("graceful_cancel|"):
            pl_id = cb.data.split("|")[1]
            await db.graceful_cancel_playlist(pl_id)
            await cb.answer(TXT.TOAST_GRACEFUL_CANCEL, show_alert=True)

        elif cb.data.startswith("pause|"):
            pl_id = cb.data.split("|")[1]
            await db.update_playlist(pl_id, status="paused")
            _expanded_pl = pl_id
            await cb.answer(TXT.TOAST_PL_PAUSED, show_alert=True)

        elif cb.data.startswith("res|"):
            pl_id = cb.data.split("|")[1]
            await db.update_playlist(pl_id, status="active")
            _expanded_pl = pl_id
            await cb.answer(TXT.TOAST_PL_RESUMED, show_alert=True)

        elif cb.data.startswith("kill|"):
            pl_id = cb.data.split("|")[1]
            await db.cancel_playlist(pl_id)
            _expanded_pl = None
            await cb.answer(TXT.TOAST_PL_PURGED, show_alert=True)

        if cb.data.startswith("dash") or cb.data in ["refresh", "pause_all", "resume_all", "kill_job", "graceful_cancel", "pause", "res", "kill"]:
            text, kb = await render_dashboard(db, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid)
            await safe_edit(app, cb.message.chat.id, cb.message.id, text, kb)


# ═══════════════════════════════════════════════════════════════════════
# CH 12 — WORKER POOL + PIPELINE WORKER LOOPS
# ═══════════════════════════════════════════════════════════════════════
async def terminal_loop(db: JobScheduler, dl_q: asyncio.Queue, enc_q: asyncio.Queue, up_q: asyncio.Queue):
    sys.stdout.write("\033[2J")
    while True:
        await asyncio.sleep(2)
        sys.stdout.write("\033[H")
        sys.stdout.write(f"{C_CYAN}{C_BOLD}{TXT.TERM_HEADER}{C_RESET}\n")
        sys.stdout.write(f"{TXT.TERM_QUEUES.format(dl=dl_q.qsize(), enc=enc_q.qsize(), up=up_q.qsize())}\n{'─' * 40}\n")

        jobs = await db.get_active_jobs()
        if not jobs: sys.stdout.write(f"{C_GREEN}{TXT.TERM_IDLE}{C_RESET}\033[K\n")
        else:
            for j in jobs[:5]:
                stage_val = j.get('stage') or ""
                col = C_YELLOW if "download" in stage_val else C_CYAN if "enc" in stage_val else C_GREEN
                pct = float(j.get('pct', 0.0) or 0.0)
                sys.stdout.write(f"{C_BOLD}[{clean_title(j['title'])[:15]}]{C_RESET} {col}{stage_val}{C_RESET} | [{make_bar(pct, 10)}] {pct:.1f}%\033[K\n")

        sys.stdout.write("\033[J")
        sys.stdout.flush()


class WorkerPool:
    def __init__(self, name: str, worker_factory):
        self.name, self._factory = name, worker_factory
        self.tasks: list[asyncio.Task] = []
        self.target, self._retire_count = 0, 0

    def current_count(self) -> int:
        self.tasks = [t for t in self.tasks if not t.done()]
        return len(self.tasks)

    async def adjust(self, new_target: int):
        new_target = max(0, new_target)
        current = self.current_count()
        if new_target > current:
            for _ in range(new_target - current): self.tasks.append(asyncio.create_task(self._factory(self)))
        elif new_target < current: self._retire_count += (current - new_target)
        self.target = new_target

    def should_retire(self) -> bool:
        if self._retire_count > 0:
            self._retire_count -= 1
            return True
        return False


async def worker_pipeline(db: JobScheduler, dl_q: asyncio.Queue, enc_q: asyncio.Queue, up_q: asyncio.Queue, app: Client):
    dl_engine, enc_engine, up_engine = DownloaderEngine(db, app), EncoderEngine(), UploaderEngine(db, app)

    async def dl_worker(pool: WorkerPool):
        while True:
            jid = await dl_q.get()
            try:
                jobs = await db.get_active_jobs()
                j_data = next((j for j in jobs if j['id'] == jid), None)
                if j_data:
                    try:
                        await dl_engine.execute(j_data)
                        await db.update_job(jid, stage="downloaded")
                        await enc_q.put(jid)
                    except Exception as e:
                        db.log_trace(jid, f"DL Error: {e}")
                        await db.fail_or_retry(j_data, str(e))
            except Exception: pass
            finally: dl_q.task_done()
            if pool.should_retire(): return

    async def enc_worker(pool: WorkerPool):
        while True:
            jid = await enc_q.get()
            try:
                jobs = await db.get_active_jobs()
                j_data = next((j for j in jobs if j['id'] == jid), None)
                if j_data:
                    try:
                        await db.update_job(jid, stage="encoding")
                        await enc_engine.execute(j_data, db)
                        await db.update_job(jid, stage="encoded")
                        await up_q.put(jid)
                    except Exception as e:
                        db.log_trace(jid, f"Enc Error: {e}")
                        await db.fail_or_retry(j_data, str(e))
            except Exception: pass
            finally: enc_q.task_done()
            if pool.should_retire(): return

    async def up_worker(pool: WorkerPool):
        while True:
            jid = await up_q.get()
            try:
                jobs = await db.get_active_jobs()
                j_data = next((j for j in jobs if j['id'] == jid), None)
                if j_data:
                    try:
                        if not (j_data.get('stage') or "").lower().startswith("uploaded"):
                            await db.update_job(jid, stage="uploading")
                            j_data['stage'] = "uploading"
                        await up_engine.execute(j_data)
                    except Exception as e:
                        db.log_trace(jid, f"UP Error: {e}")
                        latest = await db.get_job(jid)
                        if latest and (latest.get('stage') or "").lower().startswith("uploaded"):
                            retries = int(latest.get('retries') or 0) + 1
                            if retries < MAX_RETRIES:
                                await db.update_job(jid, retries=retries)
                                await asyncio.sleep(2)
                                await up_q.put(jid)
                        else: await db.fail_or_retry(j_data, str(e))
            except Exception: pass
            finally: up_q.task_done()
            if pool.should_retire(): return

    dl_pool, enc_pool, up_pool = WorkerPool("dl", dl_worker), WorkerPool("enc", enc_worker), WorkerPool("up", up_worker)
    await dl_pool.adjust(3); await enc_pool.adjust(2); await up_pool.adjust(2)
    return dl_pool, enc_pool, up_pool


# ═══════════════════════════════════════════════════════════════════════
# CH 13 — BOOTSTRAP: DASHBOARD REFRESHER, CRASH REPORT, MAIN()
# ═══════════════════════════════════════════════════════════════════════
async def dashboard_refresher(app: Client, db: JobScheduler):
    global _dash_msg_id, _dash_chat_id, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid, _stack_msg_id, _stack_chat_id
    last_state_hash = {}

    while True:
        await asyncio.sleep(2)
        try:
            jobs, playlists = await db.get_active_jobs(), await db.get_active_playlists()
            needs_update, current_hash = False, {}

            pl_summary = str([(p['id'], p['status'], p['downloaded']) for p in playlists])
            if last_state_hash.get("playlists") != pl_summary:
                needs_update = True; last_state_hash["playlists"] = pl_summary

            for j in jobs:
                jid = j['id']
                stage_base = (j.get('stage') or "").split('|')[0].strip()
                pct_bucket = int(float(j.get('pct', 0.0) or 0.0) // 10) * 10
                state_str = f"{stage_base}_{pct_bucket}"
                current_hash[jid] = state_str
                if last_state_hash.get(jid) != state_str: needs_update = True

            if set(current_hash.keys()) != set(k for k in last_state_hash.keys() if k != "playlists"): needs_update = True

            if needs_update:
                if _stack_msg_id and _stack_chat_id:
                    try: await safe_edit(app, _stack_chat_id, _stack_msg_id, render_stack_card(jobs), None)
                    except Exception: pass
                for k in list(last_state_hash.keys()):
                    if k != "playlists" and k not in current_hash: del last_state_hash[k]
                for k, v in current_hash.items(): last_state_hash[k] = v

            if _dash_msg_id and _dash_chat_id and needs_update:
                text, kb = await render_dashboard(db, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid)
                await safe_edit(app, _dash_chat_id, _dash_msg_id, text, kb)
        except Exception: pass


async def send_reboot_crash_report(app: Client, db: JobScheduler):
    playlists = await db.get_active_playlists()
    if not playlists: return

    active_jobs = await db.get_active_jobs()
    for pl in playlists:
        pl_id = pl['id']
        pl_jobs = [j for j in active_jobs if j.get('playlist_id') == pl_id]

        uploaded_count = pl['downloaded']
        failed_count = await db.get_playlist_failed_count(pl_id)

        dl_wait_up = len([j for j in pl_jobs if (j.get('stage') or '').lower().startswith(('downloaded', 'encoding', 'encoded', 'uploading'))])
        dl_now = len([j for j in pl_jobs if (j.get('stage') or '').lower().startswith(('queued', 'downloading'))])
        pending_items = len(await db.get_pending_items(pl_id, limit=99999))

        _, album_info = parse_album_caption(pl.get('caption'))

        report_text = (
            f"{TXT.CRASH_TITLE}\n{TXT.DIVIDER}\n"
            f"{TXT.CRASH_ROW_URL} {pl['url']}\n{TXT.CRASH_ROW_DEST} `{album_info}`\n"
            f"{TXT.CRASH_ROW_STATE} {TXT.CRASH_STATE_VAL}\n\n"
            f"{TXT.CRASH_BREAKDOWN_HDR}\n"
            f"  ├ {TXT.CRASH_ROW_TOTAL} `{pl['total']}`\n"
            f"  ├ {TXT.CRASH_ROW_UPLOADED} `{uploaded_count}`\n"
            f"  ├ {TXT.CRASH_ROW_READY} `{dl_wait_up}`\n"
            f"  ├ {TXT.CRASH_ROW_DOWNLOADING} `{dl_now}`\n"
            f"  ├ {TXT.CRASH_ROW_REMAINING} `{pending_items}`\n"
            f"  └ {TXT.CRASH_ROW_FAILED} `{failed_count}`\n{TXT.DIVIDER}\n"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(TXT.CRASH_RESUME_BTN, callback_data=f"res|{pl_id}")],
            [InlineKeyboardButton(TXT.CRASH_FLUSH_BTN, callback_data=f"graceful_cancel|{pl_id}"), InlineKeyboardButton(TXT.CRASH_PURGE_BTN, callback_data=f"kill|{pl_id}")]
        ])
        await app.send_message(OWNER_ID, report_text, reply_markup=kb)


async def main():
    app = Client("vk_stealth_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
    db = JobScheduler(DB_PATH)
    dl_q, enc_q, up_q = asyncio.Queue(), asyncio.Queue(), asyncio.Queue()

    await db.reconcile_items()
    recovered = await db.reconcile_on_startup()
    for jid in recovered["dl"]: await dl_q.put(jid)
    for jid in recovered["enc"]: await enc_q.put(jid)
    for jid in recovered["up"]: await up_q.put(jid)

    async with app:
        log.info("VK Playlist Bot Online via MTProto.")
        dl_pool, enc_pool, up_pool = await worker_pipeline(db, dl_q, enc_q, up_q, app)
        setup_router(app, db, dl_q, enc_q, up_q, dl_pool, enc_pool, up_pool)
        asyncio.create_task(playlist_drip_feed_loop(db, dl_q, enc_q, up_q, dl_pool, enc_pool, up_pool))
        asyncio.create_task(terminal_loop(db, dl_q, enc_q, up_q))
        asyncio.create_task(dashboard_refresher(app, db))
        asyncio.create_task(autoscan_engine_loop(db, app))

        if OWNER_ID:
            await send_reboot_crash_report(app, db)
            global _dash_msg_id, _dash_chat_id, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid, _stack_msg_id, _stack_chat_id
            text, kb = await render_dashboard(db, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid)
            m = await app.send_message(OWNER_ID, text, reply_markup=kb)
            _dash_msg_id, _dash_chat_id = m.id, m.chat.id

            stack_msg = await app.send_message(OWNER_ID, render_stack_card(await db.get_active_jobs()))
            _stack_msg_id, _stack_chat_id = stack_msg.id, stack_msg.chat.id

            try:
                await app.unpin_all_chat_messages(m.chat.id)
                await m.pin(disable_notification=True, both_sides=True)
            except Exception: pass

        while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    try: loop.run_until_complete(main())
    except KeyboardInterrupt: sys.exit(0)
