"""
bot.py - v9.0.0 "MAINFRAME UNIFIED"
───────────────────────────────────────────────────────────────
ARCHITECTURE:
  • Single-file, chapter-organized (search "CHAPTER" to jump around).
  • Everything editable in ONE place per concern:
      - CHAPTER 1  : Global config / constants
      - CHAPTER 2  : TXT — every user-facing string / template lives here
      - CHAPTER 3  : Database schema (JobScheduler)
      - CHAPTER 4  : Link classifier + link/caption/playlist parsing
      - CHAPTER 5  : VK Playlist & Upload Manager (NEW)
      - CHAPTER 6  : Downloader Engine (unchanged battle-tested logic)
      - CHAPTER 7  : Encoder Engine
      - CHAPTER 8  : Uploader Engine (branches: Telegram / VK)
      - CHAPTER 9  : Recovery & Crash Courier
      - CHAPTER 10 : Pipeline Manager (workers/queues)
      - CHAPTER 11 : Telegram Dispatcher + UI Accumulator (rate-limited edits)
      - CHAPTER 12 : Dashboard / Job Card rendering
      - CHAPTER 13 : Destination Selection (NEW — VK/Telegram picker)
      - CHAPTER 14 : Command Router (message + callback handlers)
      - CHAPTER 15 : Event loops (terminal UI)
      - CHAPTER 16 : Bootstrap

NEW IN v9.0.0:
  • Every pasted link (and every /go...​/end batch) now asks you to pick a
    destination: 📤 Telegram or 🎬 VK, via inline buttons.
  • VK destination: paste `<url> #PlaylistName optional caption text`.
    The `#PlaylistName` becomes the VK playlist (album) name (case-insensitive
    match against your existing playlists; created if it doesn't exist; the
    literal `#` is stripped before creating). Everything after the tag becomes
    the video's VK description/caption.
  • Batch mode: the name you gave `/go <Name>` is reused as the playlist name
    for every item in that batch when VK is chosen at `/end`.
  • Playlists are created under the VK_TOKEN owner's own account (no group_id).

KNOWN LIMITATION (flagged, not hidden):
  • VK's simple upload wrapper (vk_api.VkUpload.video) does a single blocking
    multipart POST — there is no native per-chunk progress callback like the
    Pyrogram streaming path has. We do NOT fake a percentage bar for it (that
    caused real problems before — see PASS 7.5 fix history). The job card
    shows "uploading | vk | processing..." and jumps to 100% on completion.
    If you want a real progress bar for VK later, it needs a custom streaming
    multipart implementation similar to `_ProgressFilePayload`.
───────────────────────────────────────────────────────────────
"""

from __future__ import annotations
import asyncio
import json
import logging
import os
import re
import shutil
import subprocess
import time
import uuid
import sys
import traceback
import sqlite3
import zipfile
import urllib.parse as urlparse
from enum import Enum
from pathlib import Path
import yt_dlp
import aiohttp
import random
from yt_dlp.networking.impersonate import ImpersonateTarget

try:
    loop = asyncio.get_running_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, ForceReply
from pyrogram.errors import FloodWait, MessageNotModified
from logging.handlers import RotatingFileHandler
import config
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

# ═══════════════════════════════════════════════════════════════════════
# CHAPTER 1 — GLOBAL CONFIG / CONSTANTS
# Edit paths, worker counts, credentials wiring, and logging here.
# ═══════════════════════════════════════════════════════════════════════

BASE_DIR = Path("SysCache")
LOG_DIR = BASE_DIR / "logs"
DB_PATH = BASE_DIR / "scheduler.db"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(name)s – %(message)s",
    handlers=[
        RotatingFileHandler(LOG_DIR / "engine.log", maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("stealth_bot")
logging.getLogger().handlers[1].setLevel(logging.CRITICAL)

API_ID, API_HASH, BOT_TOKEN, CHANNEL_ID = config.API_ID, config.API_HASH, config.BOT_TOKEN, config.CHANNEL_ID
OWNER_ID = int(config.OWNER_ID) if hasattr(config, "OWNER_ID") else 0

# --- VK API & SESSION CONFIGURATION ---
try:
    import vk_api
except ImportError:
    vk_api = None

VK_COOKIES = None
if os.path.exists("extracted_cookies.txt"):
    with open("extracted_cookies.txt", "r", encoding="utf-8") as f:
        VK_COOKIES = f.read().strip()

VK_USERNAME = getattr(config, "VK_USERNAME", None)
VK_PASSWORD = getattr(config, "VK_PASSWORD", None)
VK_TOKEN = getattr(config, "VK_TOKEN", None)
# --- DUMP-ALBUM AUTO-SORTER CONFIGURATION ---
# A "dump" playlist that videos land in continuously (by hand or another
# process). DumpSorter scans it on a loop and links each video into its
# matching performer playlist(s), parsed from the caption — without ever
# removing it from the dump album itself.
DUMP_ALBUM_OWNER_ID = getattr(config, "DUMP_ALBUM_OWNER_ID", 257484939)
DUMP_ALBUM_ID = getattr(config, "DUMP_ALBUM_ID", 283)
DUMP_SWEEP_INTERVAL = getattr(config, "DUMP_SWEEP_INTERVAL", 600)  # seconds

# --- RSS-ONLY DOWNLOAD FAILURE HANDLING ---
# RSS-sourced jobs never escalate to the Playwright extraction fallback (that
# fallback is reserved for manually-submitted links). If the assigned
# download engine fails, the normal MAX_RETRIES immediate retries apply; if
# those are exhausted, the job is marked failed and parked, then brought
# back once for a second full round of attempts after this delay. If that
# second round also exhausts its retries, it's marked permanently failed.
RSS_RETRY_DELAY_SECONDS = getattr(config, "RSS_RETRY_DELAY_SECONDS", 10800)  # 3 hours
# --- RSS CONFIGURATION ---
# Uses getattr so the bot doesn't crash if the variable is missing from config.py
from config import RSS_FEEDS, SITE_CONFIGS
# --------------------------------------

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

JOBS_DIR, DONE_DIR = BASE_DIR / "jobs", BASE_DIR / "completed"
for d in (JOBS_DIR, DONE_DIR): d.mkdir(parents=True, exist_ok=True)

MAX_DL_WORKERS, MAX_RETRIES = 3, 3

# ──────────────────────────── BATCH CONFIGURATION ─────────────────────
_batch_mode = False
_batch_collection = []
_current_batch_name = None
_pending_batches = asyncio.Queue()

# ──────────────────────────── DESTINATION SELECTION STATE (NEW) ───────
# Links / batches sit here waiting for a VK/Telegram tap before a job is created.
PENDING_LINKS: dict[str, dict] = {}
PENDING_BATCH_SELECTIONS: dict[str, dict] = {}

C_CYAN, C_YELLOW, C_RED, C_GREEN, C_RESET, C_BOLD = "\033[36m", "\033[33m", "\033[31m", "\033[32m", "\033[0m", "\033[1m"

def make_bar(percent: float, width: int = 10) -> str:
    filled = int(max(0.0, min(percent, 100.0)) / (100.0 / width))
    return "█" * filled + "░" * (width - filled)

async def extract_video_metadata(file_path: Path) -> tuple[int, int, int]:
    """Extracts (width, height, duration) using ffprobe."""
    cmd = [
        "ffprobe", "-v", "error", "-select_streams", "v:0",
        "-show_entries", "stream=width,height,duration",
        "-of", "json", str(file_path)
    ]
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, _ = await proc.communicate()
    try:
        data = json.loads(stdout)
        stream = data['streams'][0]
        return int(stream.get('width', 0)), int(stream.get('height', 0)), int(float(stream.get('duration', 0)))
    except Exception:
        return 0, 0, 0

# ═══════════════════════════════════════════════════════════════════════
# CHAPTER 2 — TXT: ALL USER-FACING STRINGS / TEMPLATES
# Edit anything the user sees here. Nothing else in the file should
# contain a raw UI string — if you're adding a new message, add it here.
# ═══════════════════════════════════════════════════════════════════════

class TXT:
    MAINFRAME_TITLE = "💻 **MAINFRAME v9.0.0**"

    BATCH_MODE_START = "🟢 **BATCH MODE INITIATED**\n🏷️ **Name:** `{name}`\nPaste your URLs one by one. Send `/end` when finished."
    BATCH_NOT_ACTIVE = "⚠️ Not in batch mode. Use `/go` first."
    BATCH_EMPTY = "⚠️ No links were collected. Batch cancelled."
    BATCH_ADDED = "✅ Added to batch. Total: {count}. Send `/end` to process."
    BATCH_AWAITING_DEST = "🚀 **BATCH COLLECTED** — {count} tasks.\n📍 Choose a destination for the whole batch:"
    BATCH_SUBMITTED = "🚀 **BATCH SUBMITTED**\nSent {count} tasks to the Orchestrator via **{dest}**."
    BATCH_PROCESSED = "✅ **BATCH PROCESSED**\nAll downloads and encodings complete. Initiating mass upload sequence..."

    LINK_AWAITING_DEST = "📍 **Choose destination for:**\n`{title}`"
    LINK_QUEUED = "`[ ⚡ ] ＴＡＳＫ :` `{title}`\n`[ ⚙️ ] ＳＴＡＴ :` `QUEUED ({dest})`"
    LINK_PENDING_EXPIRED = "⚠️ This link selection expired or was already handled."

    DEST_BTN_TELEGRAM = "📤 Telegram"
    DEST_BTN_VK = "🎬 VK"

    VK_UNAVAILABLE = "❌ VK isn't configured (missing `vk_api` package or `VK_TOKEN` in config). Falling back to Telegram."
    VK_PLAYLIST_RESOLVING = "[VK] Resolving playlist '{name}'..."
    VK_PLAYLIST_CREATED = "[VK] Created new playlist '{name}' (album_id={album_id})"
    VK_PLAYLIST_FOUND = "[VK] Matched existing playlist '{name}' (album_id={album_id})"
    VK_PLAYLIST_FAILED = "[VK] Playlist resolve/create failed: {err}"
    VK_UPLOAD_START = "[VK] Uploading video via VkUpload..."
    VK_UPLOAD_DONE = "[VK] Upload complete: {result}"

    JOB_TRACKER_TEMPLATE = (
        "`[❖] ＴＡＳＫ :` `{title}..`\n"
        "`━━━━━━━━━━━━━━━━━━━━━━━━━━`\n"
        "`⚙️ PHASE :` `{status}`\n"
        "`⚡ SPEED :` `{speed}`\n"
        "`⏳ ETA   :` `{eta}`\n"
        "`📊 PROG  :` `[{bar}] {pct:.1f}%`\n"
        "`📍 ROUTE :` `{route}`"
    )

    JOB_COMPLETE_TEMPLATE = (
        "`[❖] ＴＡＳＫ :` `{title}..`\n"
        "`━━━━━━━━━━━━━━━━━━━━━━━━━━`\n"
        "`✅ PHASE : COMPLETED`\n"
        "`📤 ROUTE : {route}`\n"
        "`━━━━━━━━━━━━━━━━━━━━━━━━━━`"
    )

    RENAME_PROMPT = "✏️ **RENAME TASK:** `{jid}`\nReply to this exact message with the new file name."
    UPDATE_PROMPT = "🔄 **UPDATE SEQUENCE**\nEnter the script name (e.g., `testui5.1`):"
    UPDATE_FILE_MISSING = "❌ File `{script}` not found in the Debian directory."
    UPDATE_IN_PROGRESS = "🔄 Updating to `{script}`... Suspending current processes."
    UPDATE_FAILED = "❌ **Update Failed! Rolled back to previous version.**\n**Target:** `{script}` | **Exit Code:** `{code}`\n\n**Error Logs:**\n```{log}```"
    UPDATE_SUCCESS = "✅ **Updated to `{script}`.** The new mainframe instance is now online."
    UPDATE_CRITICAL = "🚨 **Critical Execution Error during Update:**\n`{err}`"

    TASK_TERMINATED = "💀 **TASK TERMINATED:** `JOB_{jid}`"
    DIAG_SUCCESS_CAPTION = "🕵️ **SUCCESS DEBUG**\nPayload captured for `{jid}`.\n(Video files excluded to save space)."
    DIAG_FAULT_CAPTION = "🚨 **MAINFRAME FAULT**\n`{jid}` collapsed.\nError: `{err}`"

    @staticmethod
    def dest_keyboard(token: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([[
            InlineKeyboardButton(TXT.DEST_BTN_TELEGRAM, callback_data=f"dest|{token}|telegram"),
            InlineKeyboardButton(TXT.DEST_BTN_VK, callback_data=f"dest|{token}|vk"),
        ]])

    @staticmethod
    def route_label(destination: str, playlist_name: str | None = None) -> str:
        if destination == "vk":
            return f"VK · {playlist_name}" if playlist_name else "VK"
        return "TELEGRAM · CHANNEL"

# ═══════════════════════════════════════════════════════════════════════
# CHAPTER 3 — DATABASE (JobScheduler)
# ═══════════════════════════════════════════════════════════════════════

class Stage(str, Enum):
    QUEUED, DOWNLOADING, DOWNLOADED, ENCODING, ENCODED, UPLOADING, COMPLETED, FAILED, CANCELLED = (
        "queued", "downloading", "downloaded", "encoding", "encoded", "uploading", "completed", "failed", "cancelled"
    )

class JobScheduler:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.lock = asyncio.Lock()
        self._init_db()

    def _init_db(self):
        """Creates or patches the database tables if they do not exist."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY, url TEXT, title TEXT, source TEXT, quality TEXT, strategy TEXT,
                stage TEXT, pct REAL, last_ui_pct REAL, retries INTEGER, chat_id INTEGER, tracker_id INTEGER,
                recovered_at_stage TEXT DEFAULT NULL,
                destination TEXT DEFAULT 'telegram',
                playlist_name TEXT DEFAULT NULL,
                caption TEXT DEFAULT ''
            )''')

            # Patch existing DBs missing newer columns
            for ddl in (
                'ALTER TABLE jobs ADD COLUMN recovered_at_stage TEXT DEFAULT NULL',
                "ALTER TABLE jobs ADD COLUMN destination TEXT DEFAULT 'telegram'",
                'ALTER TABLE jobs ADD COLUMN playlist_name TEXT DEFAULT NULL',
                "ALTER TABLE jobs ADD COLUMN caption TEXT DEFAULT ''",
                'ALTER TABLE jobs ADD COLUMN is_rss INTEGER DEFAULT 0',
                'ALTER TABLE jobs ADD COLUMN rss_deferred_attempts INTEGER DEFAULT 0',
            ):
                try:
                    conn.execute(ddl)
                except sqlite3.OperationalError:
                    pass

    def _safe_execute(self, query: str, params: tuple = (), fetchone=False, fetchall=False):
        """Executes DB operations with automatic self-healing if the DB file is deleted mid-run."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.execute(query, params)
                if fetchone: return cur.fetchone()
                if fetchall: return cur.fetchall()
                conn.commit()
        except sqlite3.OperationalError as e:
            if "no such table" in str(e).lower():
                self._init_db()  # Rebuild the missing tables
                with sqlite3.connect(self.db_path) as conn:
                    conn.row_factory = sqlite3.Row
                    cur = conn.execute(query, params)
                    if fetchone: return cur.fetchone()
                    if fetchall: return cur.fetchall()
                    conn.commit()
            else:
                raise e

    async def create_job(self, data: dict):
        async with self.lock:
            self._safe_execute(
                '''INSERT INTO jobs (id, url, title, source, quality, strategy, stage, pct, last_ui_pct, retries, chat_id, tracker_id, destination, playlist_name, caption)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (data['id'], data['url'], data['title'], data['source'], data.get('quality', 'auto'), data.get('strategy', 'GENERIC'),
                 Stage.QUEUED.value, 0.0, -10.0, 0, data['chat_id'], data['tracker_id'],
                 data.get('destination', 'telegram'), data.get('playlist_name'), data.get('caption', ''))
            )

        root = JOBS_DIR / f"JOB_{data['id']}"
        for d in (root, root / "dl", root / "enc", root / "thumb"): d.mkdir(parents=True, exist_ok=True)

    async def update_job(self, jid: str, **kwargs):
        async with self.lock:
            for k, v in kwargs.items():
                self._safe_execute(f'UPDATE jobs SET {k} = ? WHERE id = ?', (v, jid))

    async def get_job(self, jid: str) -> dict:
        async with self.lock:
            row = self._safe_execute('SELECT * FROM jobs WHERE id = ?', (jid,), fetchone=True)
            return dict(row) if row else {}

    async def get_active_jobs(self) -> list[dict]:
        async with self.lock:
            rows = self._safe_execute('SELECT * FROM jobs WHERE stage NOT IN ("completed", "failed", "cancelled")', fetchall=True)
            return [dict(row) for row in (rows or [])]

    async def delete_job(self, jid: str):
        async with self.lock:
            self._safe_execute('DELETE FROM jobs WHERE id = ?', (jid,))

    def log_trace(self, jid: str, msg: str):
        # 1. Keep writing to the job's trace.log file
        try:
            with open(JOBS_DIR / f"JOB_{jid}" / "trace.log", "a", encoding="utf-8") as f:
                f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")
        except Exception:
            pass
        
        # 2. ALSO push it to the standard Python logger so it prints in the terminal
        logging.getLogger("stealth_bot").info(f"[{jid}] {msg}")

# ═══════════════════════════════════════════════════════════════════════
# CHAPTER 4 — LINK CLASSIFIER + MESSAGE PARSING (url / #playlist / caption)
# ═══════════════════════════════════════════════════════════════════════

class LinkClassifier:
    @staticmethod
    def classify(url: str) -> str:
        u = url.lower()
        if u == "telegram_bridge": return "TELEGRAM"
        if "magnet:?" in u: return "MAGNET"
        if ".m3u8" in u: return "HLS_STREAM"
        if "youtube.com" in u or "youtu.be" in u: return "YOUTUBE"
        if ".mp4" in u or "direct-mp4" in u: return "DIRECT_MP4"
        return "GENERIC_FALLBACK"

def parse_link_message(text: str, url: str) -> dict:
    remainder = text.replace(url, "").strip()
    tags = re.findall(r"#(\S+)", remainder)

    if tags:
        # Combine all found tags into a comma-separated string
        playlist_name = ",".join(tags)
        
        # Title is everything before the FIRST tag
        first_tag_match = re.search(r"#\S+", remainder)
        title = remainder[:first_tag_match.start()].strip()
        
        # Caption is everything after the LAST tag
        last_tag_match = list(re.finditer(r"#\S+", remainder))[-1]
        caption = remainder[last_tag_match.end():].strip()
    else:
        playlist_name = None
        title = remainder
        caption = ""

    if not title:
        title = playlist_name or url[:40]

    return {"title": title, "playlist_name": playlist_name, "caption": caption}
  
# ═══════════════════════════════════════════════════════════════════════
# CHAPTER 5 — VK PLAYLIST & UPLOAD MANAGER 
# ═══════════════════════════════════════════════════════════════════════
import asyncio
import vk_api
import time
from pathlib import Path

class VKPlaylistManager:
    def __init__(self, token: str | None):
        self.available = bool(vk_api and token)
        self._session = vk_api.VkApi(token=token) if self.available else None
        self._vk = self._session.get_api() if self._session else None
        
        self._album_cache: dict[str, int] = {}          # normalized playlist name -> canonical album_id
        self._video_cache: set = set()                    # normalized video title -> membership
        self._video_owner: dict[str, tuple[int, int]] = {}   # normalized title -> (owner_id, video_id), first-seen
        self._album_videos: dict[int, dict[str, list[tuple[int, int]]]] = {}  # album_id -> {title -> [(owner_id, video_id), ...]}
        self._db_loaded = False
        self._lock = asyncio.Lock()

    async def load_vk_database(self, jid: str, db):
        """Pulls all albums, video titles, and per-album membership from VK into
        memory for instant deduplication, then runs a one-time maintenance sweep
        that consolidates duplicate playlists and removes in-playlist duplicate
        videos (see _run_maintenance_sweep)."""
        if self._db_loaded or not self._vk: return
        
        async with self._lock:
            if self._db_loaded: return
            try:
                db.log_trace(jid, "[VK] Initiating full sync with VK Ultimate Database...")
                
                # 1. Sync Albums — group by normalized name so real VK-side
                #    duplicate playlists (same name, different album_id) are
                #    detected instead of silently overwriting each other.
                album_groups: dict[str, list[tuple[int, int]]] = {}  # key -> [(album_id, video_count), ...]
                offset = 0
                while True:
                    albums = await asyncio.to_thread(self._vk.video.getAlbums, count=100, offset=offset)
                    items = albums.get('items', [])
                    if not items: break
                    for item in items:
                        key = item.get('title', '').replace('#', '').strip().lower()
                        if not key: continue
                        album_groups.setdefault(key, []).append((item['id'], item.get('count', 0)))
                    offset += 100
                    if offset >= albums.get('count', 0): break

                duplicate_album_ids: list[tuple[str, int, int]] = []  # (key, canonical_id, dup_id)
                for key, entries in album_groups.items():
                    entries.sort(key=lambda e: (-e[1], e[0]))  # most videos first, then oldest (lowest id)
                    canonical_id = entries[0][0]
                    self._album_cache[key] = canonical_id
                    for dup_id, _ in entries[1:]:
                        duplicate_album_ids.append((key, canonical_id, dup_id))

                # 2. Sync All Videos (For Deduplication) — first-seen owner/id kept
                #    per title for later playlist-linking of existing videos.
                offset = 0
                while True:
                    videos = await asyncio.to_thread(self._vk.video.get, count=200, offset=offset)
                    items = videos.get('items', [])
                    if not items: break
                    for item in items:
                        v_title = item.get('title', '').strip().lower()
                        if not v_title: continue
                        self._video_cache.add(v_title)
                        if v_title not in self._video_owner:
                            self._video_owner[v_title] = (item.get('owner_id'), item.get('id'))
                    offset += 200
                    if offset >= videos.get('count', 0): break

                # 3. Sync per-album membership (every album, including duplicates —
                #    duplicates need their membership known so it can be moved
                #    over before the empty album is deleted).
                all_album_ids = [aid for entries in album_groups.values() for aid, _ in entries]
                for album_id in all_album_ids:
                    members: dict[str, list[tuple[int, int]]] = {}
                    offset = 0
                    while True:
                        try:
                            vids = await asyncio.to_thread(self._vk.video.get, album_id=album_id, count=200, offset=offset)
                        except Exception as e:
                            db.log_trace(jid, f"[VK] Failed to list playlist {album_id}: {e}")
                            break
                        items = vids.get('items', [])
                        if not items: break
                        for item in items:
                            t = item.get('title', '').strip().lower()
                            members.setdefault(t, []).append((item.get('owner_id'), item.get('id')))
                        offset += 200
                        if offset >= vids.get('count', 0): break
                    self._album_videos[album_id] = members

                self._db_loaded = True
                db.log_trace(jid, f"[VK] Sync Complete: {len(self._album_cache)} Playlists, {len(self._video_cache)} Videos loaded.")

                await self._run_maintenance_sweep(duplicate_album_ids, jid, db)
            except Exception as e:
                db.log_trace(jid, f"[VK] CRITICAL: Ultimate Database sync failed: {e}")

    async def _run_maintenance_sweep(self, duplicate_album_ids: list[tuple[str, int, int]], jid: str, db):
        """Self-healing pass, run once per sync:
        1) Merge every duplicate playlist's videos into the canonical playlist,
           then delete the now-redundant duplicate playlist.
        2) Remove same-video duplicates found sitting twice inside one playlist,
           keeping a single copy. A video legitimately belonging to several
           different playlists is left untouched — only in-playlist repeats
           are removed."""
        # 1) Consolidate duplicate playlists
        for key, canonical_id, dup_id in duplicate_album_ids:
            dup_members = self._album_videos.get(dup_id, {})
            canon_members = self._album_videos.setdefault(canonical_id, {})
            for title, entries in dup_members.items():
                for owner_id, vid_id in entries:
                    already = any(v == vid_id and o == owner_id for o, v in canon_members.get(title, []))
                    if already: continue
                    try:
                        await asyncio.to_thread(self._vk.video.addToAlbum, owner_id=owner_id, video_id=vid_id, album_id=canonical_id)
                        canon_members.setdefault(title, []).append((owner_id, vid_id))
                    except Exception as e:
                        db.log_trace(jid, f"[VK] Consolidate: failed to move video into playlist {canonical_id}: {e}")
            try:
                await asyncio.to_thread(self._vk.video.deleteAlbum, album_id=dup_id)
                db.log_trace(jid, f"[VK] Consolidated duplicate playlist '{key}' ({dup_id} -> {canonical_id}) and deleted the empty duplicate.")
                self._album_videos.pop(dup_id, None)
            except Exception as e:
                db.log_trace(jid, f"[VK] Consolidate: failed to delete duplicate playlist {dup_id}: {e}")

        # 2) Remove in-playlist duplicate videos (keep the lowest video_id per title)
        for album_id, members in self._album_videos.items():
            for title, entries in members.items():
                if len(entries) <= 1: continue
                entries.sort(key=lambda e: e[1])  # keep the oldest/lowest video_id
                keep, extras = entries[0], entries[1:]
                for owner_id, vid_id in extras:
                    try:
                        await asyncio.to_thread(self._vk.video.removeFromAlbum, owner_id=owner_id, video_id=vid_id, album_id=album_id)
                        db.log_trace(jid, f"[VK] Removed duplicate video '{title[:30]}' from playlist {album_id} (kept {keep[1]}).")
                    except Exception as e:
                        db.log_trace(jid, f"[VK] Failed to remove duplicate video from playlist {album_id}: {e}")
                members[title] = [keep]

    async def is_duplicate(self, title: str) -> bool:
        """Checks if a video already exists in the VK library."""
        if not self._vk or not title: return False
        clean_title = title.strip().lower()
        return clean_title in self._video_cache

    async def list_album_videos(self, album_id: int, offset: int = 0, count: int = 200) -> dict:
        """Raw passthrough to video.get scoped to one album — for external sweepers
        (e.g. DumpSorter) that need a live view rather than the cached snapshot."""
        if not self._vk: return {}
        return await asyncio.to_thread(self._vk.video.get, album_id=album_id, count=count, offset=offset)

    async def link_video(self, owner_id: int, video_id: int, title: str, album_id: int, jid: str, db) -> bool:
        """Adds an existing video to a playlist without touching its other
        memberships, and keeps the membership cache in sync."""
        if not self._vk: return False
        key = (title or "").strip().lower()
        members = self._album_videos.setdefault(album_id, {})
        if key and any(v == video_id and o == owner_id for o, v in members.get(key, [])):
            return True  # already linked
        try:
            await asyncio.to_thread(self._vk.video.addToAlbum, owner_id=owner_id, video_id=video_id, album_id=album_id)
            if key:
                members.setdefault(key, []).append((owner_id, video_id))
            return True
        except Exception as e:
            db.log_trace(jid, f"[VK] link_video failed ({owner_id}_{video_id} -> {album_id}): {e}")
            return False

    async def ensure_video_in_albums(self, title: str, album_ids: list[int], jid: str, db):
        """For a video that already exists in the library (a duplicate-caption
        match caught before download), make sure it's a member of every target
        playlist — adding it to whichever ones it's missing from. A video is
        allowed to belong to several playlists at once; this never removes it
        from one to add it to another, and never adds it twice to the same one."""
        if not self._vk or not title or not album_ids: return
        key = title.strip().lower()
        owner_vid = self._video_owner.get(key)
        if not owner_vid or not owner_vid[1]:
            return
        owner_id, vid_id = owner_vid
        for album_id in album_ids:
            if not album_id: continue
            members = self._album_videos.setdefault(album_id, {})
            existing = members.get(key, [])
            if any(v == vid_id and o == owner_id for o, v in existing):
                continue  # already in this playlist
            try:
                await asyncio.to_thread(self._vk.video.addToAlbum, owner_id=owner_id, video_id=vid_id, album_id=album_id)
                members.setdefault(key, []).append((owner_id, vid_id))
                db.log_trace(jid, f"[VK] Linked existing video '{title[:30]}' -> Playlist {album_id}")
            except Exception as e:
                db.log_trace(jid, f"[VK] Failed to link existing video '{title[:30]}' to playlist {album_id}: {e}")

    async def playlist_exists(self, raw_name: str, jid: str, db) -> bool:
        """Check-only — does a playlist with this name already exist? Never
        creates one. Used to disambiguate a 1-word vs 2-word performer name
        against what's already been created."""
        if not self._vk or not raw_name: return False
        await self.load_vk_database(jid, db)
        key = raw_name.replace('#', '').strip().lower()
        return key in self._album_cache

    async def resolve_playlist(self, raw_name: str, jid: str, db) -> int | None:
        if not self._vk: return None
        clean_name = raw_name.replace('#', '').strip()
        if not clean_name: return None
        
        await self.load_vk_database(jid, db)
        key = clean_name.lower()

        if key in self._album_cache:
            return self._album_cache[key]

        try:
            db.log_trace(jid, f"[VK] Playlist '{clean_name}' not found. Creating...")
            created = await asyncio.to_thread(self._vk.video.addAlbum, title=clean_name)
            album_id = created.get('album_id')
            if album_id:
                self._album_cache[key] = album_id
                return album_id
        except Exception as e:
            db.log_trace(jid, f"[VK] Failed to create playlist '{clean_name}': {e}")
        return None

    async def upload_video(self, file_path: Path, title: str, description: str, album_ids: list[int] | None, jid: str, db) -> dict:
        if not self._session:
            raise RuntimeError("VK upload unavailable: vk_api not installed or VK_TOKEN missing.")

        def _do_upload():
            import requests
            
            clean_title = (title or "").strip()
            kwargs = {'name': clean_title[:200]} if clean_title else {}
            if description: kwargs['description'] = description.strip()

            try:
                save_resp = self._vk.video.save(**kwargs)
            except vk_api.exceptions.ApiError as e:
                if e.code == 10:
                    save_resp = self._vk.video.save() 
                else: raise e

            upload_url = save_resp.get('upload_url')
            vid_id = save_resp.get('video_id')
            own_id = save_resp.get('owner_id')

            if not upload_url:
                raise RuntimeError("Failed to retrieve upload URL from VK.")

            db.log_trace(jid, "[VK] Streaming payload to VK servers...")
            with open(file_path, 'rb') as f:
                upload_result = requests.post(upload_url, files={'video_file': f}).json()

            if 'video_hash' not in upload_result and 'size' not in upload_result:
                raise RuntimeError(f"VK File stream rejected: {upload_result}")

            db.log_trace(jid, "[VK] Payload accepted. Waiting for VK Backend Registration...")
            
            # ── STATE-VERIFIED ALLOCATION LOOP ──
            video_registered = False
            for attempt in range(1, 16):  
                try:
                    check = self._vk.video.get(videos=f"{own_id}_{vid_id}")
                    if check.get("items"):
                        video_registered = True
                        db.log_trace(jid, f"[VK] Backend Registration Confirmed (Attempt {attempt}).")
                        break
                except Exception: pass
                time.sleep(3) # Wait 3s before polling again
                
            mapped_albums: list[int] = []
            if video_registered and album_ids:
                for a_id in album_ids:
                    mapped = False
                    for map_attempt in range(1, 6):
                        try:
                            self._vk.video.addToAlbum(owner_id=own_id, video_id=vid_id, album_id=a_id)
                            db.log_trace(jid, f"[VK] [+] Successfully mapped to Playlist {a_id}")
                            mapped = True
                            mapped_albums.append(a_id)
                            break
                        except Exception as e:
                            db.log_trace(jid, f"[VK] Map Retry {map_attempt}/5 for Playlist {a_id}: {e}")
                            time.sleep(2)
                    if not mapped:
                        db.log_trace(jid, f"[VK] ❌ CRITICAL: Failed to map Playlist {a_id} entirely.")
            elif not video_registered:
                db.log_trace(jid, "[VK] ❌ Timeout: Video never registered in API. Playlists skipped.")

            # Append to local dedup/membership caches instantly so the next RSS
            # sweep sees this video and its playlist membership without a resync
            if clean_title:
                key = clean_title.lower()
                self._video_cache.add(key)
                if vid_id is not None:
                    self._video_owner.setdefault(key, (own_id, vid_id))
                    for a_id in mapped_albums:
                        members = self._album_videos.setdefault(a_id, {})
                        members.setdefault(key, []).append((own_id, vid_id))

            return save_resp

        result = await asyncio.to_thread(_do_upload)
        return result
        
# ═══════════════════════════════════════════════════════════════════════
# CHAPTER 6 — DOWNLOADER ENGINE
# (Kept intentionally as-is — this is the debugged multi-pass extraction
#  waterfall: yt-dlp variants → Playwright → PASS 7.5 browser-native →
#  N_m3u8DL-RE → ffmpeg capture → cookie bypass → aria2c. Don't simplify
#  this without re-testing against VK CDN 403s / curl_cffi pinning.)
# ═══════════════════════════════════════════════════════════════════════

from aiohttp import web
import base64
import urllib.parse as urlparse

class PlaywrightHLSProxy:
    def __init__(self, page, initial_manifest_url):
        self.page = page
        self.base_url = initial_manifest_url
        self.app = web.Application()
        self.app.router.add_get('/manifest.m3u8', self.serve_manifest)
        self.app.router.add_get('/segment', self.serve_segment)
        self.runner = None
        self.site = None

    async def start(self) -> str:
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, '127.0.0.1', 0) # 0 assigns a random free port
        await self.site.start()
        port = self.site._server.sockets[0].getsockname()[1]
        return f"http://127.0.0.1:{port}/manifest.m3u8"

    async def stop(self):
        if self.runner:
            await self.runner.cleanup()

    async def serve_manifest(self, request):
        target_url = request.query.get('url', self.base_url)
        js = "async (u) => { const r = await fetch(u, {credentials: 'omit'}); return await r.text(); }"
        
        try:
            manifest_text = await self.page.evaluate(js, target_url)
        except Exception as e:
            return web.Response(status=500, text=str(e))
        
        new_lines = []
        for line in manifest_text.splitlines():
            if line.startswith("#"):
                new_lines.append(line)
            elif line.strip():
                abs_uri = urlparse.urljoin(target_url, line.strip())
                encoded = urlparse.quote_plus(abs_uri)
                if ".m3u8" in abs_uri:
                    new_lines.append(f"/manifest.m3u8?url={encoded}")
                else:
                    new_lines.append(f"/segment?url={encoded}")
        
        return web.Response(text="\n".join(new_lines), content_type="application/vnd.apple.mpegurl")

    async def serve_segment(self, request):
        target_url = request.query.get('url')
        if not target_url: return web.Response(status=400)
            
        # ── FAST PATH: Direct fetch at full network speed ──
        try:
            from curl_cffi.requests import AsyncSession
            async with AsyncSession(impersonate="chrome") as session:
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "*/*",
                    "Connection": "keep-alive"
                }
                # Request the segment directly, bypassing Playwright's JS engine
                resp = await session.get(target_url, headers=headers, timeout=30)
                
                if resp.status_code in [200, 206]:
                    return web.Response(body=resp.content, content_type="video/MP2T")
        except Exception:
            pass # WAF blocked the fast path or it timed out; fall back to slow path
            
        # ── SLOW PATH: Playwright Base64 Bridge Fallback ──
        js = """
        async (u) => {
            const r = await fetch(u, {credentials: 'omit'});
            if (!r.ok) return null;
            const buf = await r.arrayBuffer();
            const bytes = new Uint8Array(buf);
            let binary = '';
            for (let i = 0; i < bytes.length; i += 8192) {
                binary += String.fromCharCode.apply(null, bytes.subarray(i, i + 8192));
            }
            return btoa(binary);
        }
        """
        try:
            b64 = await self.page.evaluate(js, target_url)
            if not b64: return web.Response(status=403)
            import base64
            return web.Response(body=base64.b64decode(b64), content_type="video/MP2T")
        except Exception:
            return web.Response(status=500)

class DownloaderEngine:
    def __init__(self, scheduler: JobScheduler, app: Client):
        self.db = scheduler
        self.app = app
        self.procs = {}

    # ─── PAYLOAD CACHING HELPERS ───
    def _get_payload_cache_path(self, dl_dir: Path) -> Path:
        return dl_dir / "playwright_payload.json"

    def _load_cached_payload(self, dl_dir: Path) -> dict | None:
        cache_file = self._get_payload_cache_path(dl_dir)
        if cache_file.exists():
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return None
        return None

    def _save_cached_payload(self, dl_dir: Path, payload: dict):
        cache_file = self._get_payload_cache_path(dl_dir)
        with open(cache_file, "w", encoding="utf-8") as f:
            safe_payload = {
                "url": payload.get("url"),
                "headers": payload.get("headers", {}),
                "cookie_str": payload.get("cookie_str", ""),
                "raw_cookies": payload.get("raw_cookies", [])
            }
            json.dump(safe_payload, f)

    def _extract_vk_api(self, url: str, jid: str) -> str | None:
        """Ghost Protocol: Extract direct CDN links bypassing web frontend entirely."""
        if not vk_api or not VK_TOKEN:
            return None

        try:
            vk_session = vk_api.VkApi(token=VK_TOKEN)
            vk = vk_session.get_api()

            video_id = None

            wall_match = re.search(r'wall(-?\d+_\d+)', url)
            if wall_match:
                post_id = wall_match.group(1)
                response = vk.wall.getById(posts=post_id)
                if response:
                    for item in response[0].get('attachments', []):
                        if item.get('type') == 'video':
                            v = item['video']
                            video_id = f"{v['owner_id']}_{v['id']}_{v.get('access_key', '')}".strip('_')
            else:
                video_match = re.search(r'video(-?\d+_\d+)', url)
                if video_match:
                    video_id = video_match.group(1)

            if video_id:
                vid_details = vk.video.get(videos=video_id)
                if vid_details and vid_details.get('items'):
                    files = vid_details['items'][0].get('files', {})
                    for q in ['mp4_1080', 'mp4_720', 'mp4_480', 'mp4_360', 'mp4_240', 'hls']:
                        if q in files:
                            direct_link = files[q]
                            self.db.log_trace(jid, f"[vk_api] Direct {q.upper()} link extracted.")
                            return direct_link

        except Exception as e:
            self.db.log_trace(jid, f"[vk_api] Ghost Protocol Failed: {e}")
        return None

    async def _pre_download_validation(self, url: str, jid: str, headers: dict, cookie_str: str) -> bool:
        from curl_cffi.requests import AsyncSession
        self.db.log_trace(jid, "Performing pre-download hardened TLS validation via curl_cffi...")

        req_headers = headers.copy() if headers else {}
        if cookie_str:
            req_headers['Cookie'] = cookie_str

        try:
            async with AsyncSession(impersonate="chrome") as session:
                response = await session.get(url, headers=req_headers, allow_redirects=True, stream=True)

                status = response.status_code
                content_type = response.headers.get("Content-Type", "").lower()
                content_length = int(response.headers.get("Content-Length", 0))
                final_url = str(response.url).lower()

                self.db.log_trace(jid, f"Pre-check: HTTP {status} | Type: {content_type} | Size: {content_length}")

                if status == 403:
                    self.db.log_trace(jid, "Pre-check warning: HTTP 403 detected despite TLS impersonation. Overriding gate for downstream engines.")
                    return True

                if status >= 400 and status != 403:
                    self.db.log_trace(jid, f"Pre-check failed: HTTP {status}")
                    return False

                if "login" in final_url or "captcha" in final_url:
                    self.db.log_trace(jid, "Pre-check failed: Redirected to login/captcha page.")
                    return False

                invalid_types = ["text/html", "application/json", "text/plain"]
                if any(bad in content_type for bad in invalid_types):
                    self.db.log_trace(jid, f"Pre-check failed: Invalid Content-Type '{content_type}'")
                    return False

                if status == 206:
                    self.db.log_trace(jid, "Pre-check: HTTP 206 Partial Content chunk detected. Stream is valid.")
                    return True

                if content_length > 0 and content_length < 100000:
                    self.db.log_trace(jid, "Pre-check failed: Content-Length suspiciously small (<100KB).")
                    return False

                return True
        except Exception as e:
            self.db.log_trace(jid, f"Pre-check warning: Hardened ping encounter ({e}). Passing downstream to engine.")
            return True
            
    async def _extract_perverzija_stream(self, main_url: str, jid: str) -> tuple[str | None, str | None]:
        """Custom dedicated extractor for Perverzija/Xtremestream."""
        from curl_cffi.requests import AsyncSession
        import re

        try:
            async with AsyncSession(impersonate="chrome120") as session:
                iframe_url = main_url
                
                # 1. If it's the main wrapper page, extract the iframe
                if "perverzija.com" in main_url.lower():
                    self.db.log_trace(jid, "[*] Fetching main page to locate iframe...")
                    main_res = await session.get(main_url, timeout=30)
                    iframe_match = re.search(r'["\'](https?://[^"\']+xtremestream[^"\']+/player/index\.php\?data=[^"\']+)["\']', main_res.text, re.IGNORECASE)
                    
                    if iframe_match:
                        iframe_url = iframe_match.group(1)
                        self.db.log_trace(jid, f"[+] Found Player URL: {iframe_url}")
                    else:
                        self.db.log_trace(jid, "[-] Could not find the player iframe on main page.")
                        return None, None

                # 2. Fetch the player HTML
                self.db.log_trace(jid, "[*] Fetching player HTML...")
                player_res = await session.get(iframe_url, headers={"Referer": main_url if "perverzija" in main_url else iframe_url}, timeout=30)
                html = player_res.text

                # 3. Extract the JS variables
                loader_match = re.search(r'm3u8_loader_url\s*=\s*["\'\`]([^"\'\`]+)["\'\`]', html)
                video_id_match = re.search(r'video_id\s*=\s*["\'\`]([^"\'\`]+)["\'\`]', html)

                if not loader_match or not video_id_match:
                    self.db.log_trace(jid, "[-] Could not parse variable definitions. Regex failed.")
                    return None, None

                loader_url = loader_match.group(1)
                vid = video_id_match.group(1)
                stream_url = loader_url + vid
                
                # 4. Construct final URL
                if not stream_url.startswith("http"):
                    base_domain = re.match(r"https?://[^/]+", iframe_url).group(0)
                    stream_url = base_domain + stream_url

                self.db.log_trace(jid, f"[+] SUCCESS! Constructed Stream URL: {stream_url}")
                return stream_url, iframe_url

        except Exception as e:
            self.db.log_trace(jid, f"Perverzija extraction failed: {e}")
            return None, None
            
    @staticmethod
    def _decode_packed_js(packed_script: str) -> str:
        """Mathematically unpacks obfuscated JavaScript (used by Lulustream)."""
        match = re.search(r"}\s*\(\s*'(.*?)',\s*(\d+),\s*(\d+),\s*'(.*?)'\.split\('\|'\)", packed_script, re.DOTALL)
        if not match: return ""
        p = match.group(1).replace("\\'", "'").replace('\\"', '"').replace('\\\\', '\\')
        a = int(match.group(2))
        c = int(match.group(3))
        k = match.group(4).split('|')

        def replace_func(m):
            word = m.group(0)
            try: index = int(word, a)
            except ValueError: return word
            if index < c and k[index]: return k[index]
            return word

        return re.sub(r'\b\w+\b', replace_func, p)

    async def _extract_and_download_direct_mp4(self, url: str, jid: str, dl_dir: Path) -> bool:
        """Handles FPO and Porneec via native HTTP/2 chunked streaming with 3-strike WAF retries."""
        from curl_cffi.requests import AsyncSession
        import time
        import asyncio
        
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Upgrade-Insecure-Requests": "1"
        }
        impersonations = ["safari15_5", "chrome116", "edge101", "safari_ios", "chrome120"]

        # ── 3-STRIKE RETRY LOOP ──
        for attempt in range(1, 4):
            self.db.log_trace(jid, f"[*] Direct MP4 Extraction Attempt {attempt}/3: {url}")
            successful_session, html = None, ""

            for browser in impersonations:
                try:
                    temp_session = AsyncSession(impersonate=browser, headers=headers)
                    temp_res = await temp_session.get(url, timeout=15)
                    if temp_res.status_code == 200:
                        self.db.log_trace(jid, f"[+] SUCCESS! Connection secured using {browser}")
                        successful_session = temp_session
                        html = temp_res.text
                        break
                except Exception:
                    pass

            if not successful_session:
                self.db.log_trace(jid, "[-] WAF bypass failed on this attempt.")
                await asyncio.sleep(3)
                continue

            mp4_matches = re.findall(r'["\'](https?://[^"\']+\.mp4[^"\']*)["\']', html)
            if not mp4_matches:
                self.db.log_trace(jid, "[-] No MP4 link found in the page source.")
                await asyncio.sleep(3)
                continue

            video_url = mp4_matches[0]
            self.db.log_trace(jid, f"[+] Extracted Direct Stream: {video_url}")

            out_file = dl_dir / f"{jid}.mp4"
            dl_headers = {"Referer": url}

            self.db.log_trace(jid, "[*] Launching native chunked download...")
            try:
                res = await successful_session.get(video_url, headers=dl_headers, stream=True, http_version=2, timeout=20)
                if res.status_code in [200, 206]:
                    total_size = int(res.headers.get("Content-Length", 0))
                    downloaded = 0
                    last_ui_update = time.time()

                    with open(out_file, 'wb') as f:
                        async for chunk in res.aiter_content(chunk_size=1024*1024):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)
                                now = time.time()
                                if total_size and (now - last_ui_update > 2.0):
                                    pct = (downloaded / total_size) * 100
                                    await self.db.update_job(jid, pct=pct, stage=f"downloading | native | {downloaded//(1024*1024)}MB")
                                    global _live_ui_text
                                    _live_ui_text[jid] = f"[Native] {downloaded//(1024*1024)}MB / {total_size//(1024*1024)}MB ({pct:.1f}%)"
                                    last_ui_update = now
                    self.db.log_trace(jid, f"[+] Download complete! Saved to {out_file.name}.")
                    return True
                else:
                    self.db.log_trace(jid, f"[-] CDN rejected the download with Status: {res.status_code}. Retrying...")
                    await asyncio.sleep(3)
                    
            except Exception as e:
                self.db.log_trace(jid, f"[-] Connection dropped during download: {e}. Retrying...")
                await asyncio.sleep(3)

        self.db.log_trace(jid, "[-] All 3 Direct MP4 extraction attempts failed.")
        return False
            
    async def _extract_hornysimp_target(self, main_url: str, jid: str) -> str | None:
        from curl_cffi.requests import AsyncSession
        import re
        try:
            async with AsyncSession(impersonate="chrome") as session:
                res = await session.get(main_url, timeout=30)
                # ── FIXED REGEX ──
                # Allows immediate domain matching and checks data-src for lazy-loaded iframes
                m = re.search(r'(?:href|src|data-src)=["\'](https?://(?:www\.)?(?:hrnyvid|lulustream)[^"\']+)["\']', res.text, re.IGNORECASE)
                if m:
                    return m.group(1)
        except Exception as e:
            self.db.log_trace(jid, f"HornySimp wrapper extraction failed: {e}")
        return None

    async def _extract_and_download_lulu(self, url: str, jid: str, dl_dir: Path) -> bool:
        """Handles Lulustream and Hrnyvid via JS unpacking and fragment compilation."""
        from curl_cffi.requests import AsyncSession
        from urllib.parse import urljoin
        import subprocess

        self.db.log_trace(jid, f"[*] Accessing Lulustream/Hrnyvid page: {url}")
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Upgrade-Insecure-Requests": "1"
        }
        impersonations = ["safari15_5", "chrome116", "edge101", "safari_ios", "chrome120"]
        successful_session, html = None, ""

        for browser in impersonations:
            self.db.log_trace(jid, f"[*] Attempting WAF bypass with: {browser}...")
            try:
                temp_session = AsyncSession(impersonate=browser, headers=headers)
                temp_res = await temp_session.get(url, timeout=15)
                if temp_res.status_code == 200:
                    self.db.log_trace(jid, f"[+] SUCCESS! Connection secured using {browser}")
                    successful_session = temp_session
                    html = temp_res.text
                    break
            except Exception:
                pass

        if not successful_session:
            self.db.log_trace(jid, "[-] All WAF bypass attempts failed.")
            return False

        packed_match = re.search(r'eval\(function\(p,a,c,k,e,d\).*?split\([^)]+\)\)\)', html)
        if not packed_match:
            self.db.log_trace(jid, "[-] Could not find the packed JavaScript.")
            return False

        self.db.log_trace(jid, "[*] Mathematically unpacking JavaScript...")
        unpacked_js = self._decode_packed_js(packed_match.group(0))

        m3u8_match = re.search(r'["\'](https?://[^"\']+\.m3u8[^"\']*)["\']', unpacked_js)
        if not m3u8_match:
            self.db.log_trace(jid, "[-] No M3U8 link found inside the payload.")
            return False

        master_url = m3u8_match.group(1)
        self.db.log_trace(jid, f"[+] Extracted Master Manifest: {master_url}")
        dl_headers = {"Referer": "https://lulustream.com/"}

        self.db.log_trace(jid, "[*] Fetching Master M3U8...")
        master_req = await successful_session.get(master_url, headers=dl_headers)
        if master_req.status_code != 200:
            self.db.log_trace(jid, f"[-] CDN Blocked Master Manifest. HTTP {master_req.status_code}")
            return False

        playlists = [line.strip() for line in master_req.text.splitlines() if line.strip() and not line.startswith('#')]
        if not playlists:
            self.db.log_trace(jid, "[-] No video streams found in the master manifest.")
            return False

        video_playlist_url = urljoin(master_url, playlists[-1])
        self.db.log_trace(jid, "[*] Fetching Video Fragments Manifest...")
        playlist_req = await successful_session.get(video_playlist_url, headers=dl_headers)

        segments = [line.strip() for line in playlist_req.text.splitlines() if line.strip() and not line.startswith('#')]
        self.db.log_trace(jid, f"[+] Found {len(segments)} video fragments. Commencing native secure download...")

        raw_ts_file = dl_dir / f"{jid}_raw.ts"
        out_file = dl_dir / f"{jid}.mp4"

        # Download segments natively (async loop)
        with open(raw_ts_file, "wb") as f:
            for i, seg in enumerate(segments):
                seg_url = urljoin(video_playlist_url, seg)
                for _ in range(3): # Light retry mechanism for dropped fragments
                    try:
                        seg_req = await successful_session.get(seg_url, headers=dl_headers, timeout=20)
                        if seg_req.status_code == 200:
                            f.write(seg_req.content)
                            break
                    except Exception:
                        await asyncio.sleep(1)
                
                if i % 5 == 0 or i == len(segments) - 1:
                    pct = ((i + 1) / len(segments)) * 100
                    await self.db.update_job(jid, pct=pct, stage=f"downloading | fragments | {i+1}/{len(segments)}")
                    global _live_ui_text
                    _live_ui_text[jid] = f"[Lulu Native] Fragment {i+1}/{len(segments)} ({pct:.1f}%)"

        self.db.log_trace(jid, f"[+] Stream downloaded successfully! Remuxing via FFmpeg...")
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-v", "warning", "-i", str(raw_ts_file), "-c", "copy", str(out_file),
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await proc.communicate()
        
        if proc.returncode == 0 and out_file.exists() and out_file.stat().st_size > 1024:
            self.db.log_trace(jid, f"[+] Remux complete. Final video saved.")
            raw_ts_file.unlink(missing_ok=True)
            return True
        else:
            self.db.log_trace(jid, f"[-] FFmpeg remux failed: {stderr.decode(errors='ignore')}")
            return False

    async def execute(self, job_data: dict):
        jid, url, strategy, quality = job_data['id'], job_data['url'], job_data['strategy'], job_data['quality']

        # ── DOMAIN-AGNOSTIC ROUTER ──
        dl_type = "default"
        # <--- Direct reference
        for site_key, site_data in SITE_CONFIGS.items():
            if any(d in url.lower() for d in site_data.get("domains", [])):
                dl_type = site_data.get("dl_type", "default")
                break

        self.db.log_trace(jid, f"Download Orchestrator engaged. Mainframe Route: {dl_type.upper()}")

        if strategy == "TELEGRAM":
            async def tg_prog(c, t):
                if t: await self.db.update_job(jid, pct=(c * 100 / t))
            await self.app.download_media(url, file_name=str(JOBS_DIR / f"JOB_{jid}" / "dl" / f"{jid}.mp4"), progress=tg_prog)
            return

        dl_dir = JOBS_DIR / f"JOB_{jid}" / "dl"
        if strategy in ["MAGNET", "DIRECT_MP4"]:
            await self._run_aria(url, jid, dl_dir)
            return

        # ── MODULAR EXTRACTOR EXECUTION ──
        if dl_type == "vk_api" and VK_TOKEN:
            self.db.log_trace(jid, "Target detected. Querying API backend with token...")
            extracted_player = await asyncio.to_thread(self._extract_vk_api, url, jid)
            if extracted_player:
                self.db.log_trace(jid, f"API Token bypass successful! Rerouting payload URL.")
                url = extracted_player
                
        elif dl_type == "perverzija_iframe":
            stream_url, referer = await self._extract_perverzija_stream(url, jid)
            if stream_url:
                custom_opts = {"http_headers": {"Referer": referer}, "concurrent_fragment_downloads": 4}
                try:
                    await asyncio.to_thread(self._execute_ytdlp, stream_url, jid, dl_dir, custom_opts)
                    valid_files = [f for f in dl_dir.rglob("*") if f.is_file() and f.suffix.lower() in [".mp4", ".mkv", ".ts"]]
                    if valid_files: return
                except Exception as e:
                    self.db.log_trace(jid, f"Dedicated extractor yt-dlp failed: {e}")
                    
        elif dl_type == "hornysimp_wrapper":
            lulu_url = await self._extract_hornysimp_target(url, jid)
            if lulu_url:
                if await self._extract_and_download_lulu(lulu_url, jid, dl_dir): return
                
        elif dl_type == "lulu_unpack":
            if await self._extract_and_download_lulu(url, jid, dl_dir): return
            
        elif dl_type == "direct_mp4":
            if await self._extract_and_download_direct_mp4(url, jid, dl_dir): return

        # ... The rest of the execute function stays the same (Playwright Fallback, etc.) ...
        playwright_data = self._load_cached_payload(dl_dir)

        if not playwright_data:
            variant_success = await self._attempt_ytdlp_variants(url, jid, dl_dir)
            if variant_success:
                return

            if job_data.get('is_rss'):
                # RSS-sourced jobs never escalate to Playwright — the assigned
                # engine either works or this attempt fails outright, and the
                # worker loop's retry/park/retry-once-more logic takes over.
                raise RuntimeError("RSS_ENGINE_FAILED: assigned download engine failed; "
                                    "Playwright extraction is disabled for RSS jobs.")

            self.db.log_trace(jid, "yt-dlp variants failed. Escalating to Playwright extraction...")

            try:
                playwright_data = await self._run_playwright_extraction(url, jid, dl_dir)
            except Exception as e:
                raise RuntimeError(f"PASS 11 FAILED: Extraction failed: {e}")

            if not playwright_data or not playwright_data.get('url'):
                raise RuntimeError("PASS 11 FAILED: All extraction methods exhausted. Target is highly protected.")

            self._save_cached_payload(dl_dir, playwright_data)
            self.db.log_trace(jid, "Playwright extraction successful and payload state cached.")

            if playwright_data.get("browser_downloaded"):
                self.db.log_trace(jid, "Media already downloaded via in-browser native fetch (PASS 7.5). Skipping downstream engines.")
                return
        else:
            self.db.log_trace(jid, "Loaded cached Playwright payload. Bypassing browser extraction phases.")

        extracted_url = playwright_data['url']
        headers = playwright_data['headers']
        raw_cookies = playwright_data['raw_cookies']
        cookie_str = playwright_data['cookie_str']

        self.db.log_trace(jid, "Delegating authorized payload downstream...")

        is_valid_url = await self._pre_download_validation(extracted_url, jid, headers, cookie_str)

        if not is_valid_url:
            cache_file = self._get_payload_cache_path(dl_dir)
            if cache_file and cache_file.exists():
                try:
                    os.remove(cache_file)
                    self.db.log_trace(jid, "Toxic payload cache purged.")
                except Exception:
                    pass
            raise RuntimeError("Pre-Download Validation Failed: Target URL points to HTML/Text, not a media file.")

        if ".m3u8" in extracted_url:
            self.db.log_trace(jid, "PASS 7.8: Attempting N_m3u8DL-RE stream capture...")
            if await self._run_nm3u8dlre_capture(extracted_url, jid, dl_dir, headers, cookie_str):
                self.db.log_trace(jid, "PASS 7.8 SUCCESS: Payload captured via N_m3u8DL-RE.")
                return
            self.db.log_trace(jid, "PASS 7.8 FAILED: N_m3u8DL-RE stream capture dropped.")

            self.db.log_trace(jid, "PASS 8: Attempting FFmpeg direct capture fallback...")
            if await self._run_ffmpeg_capture(extracted_url, jid, dl_dir, headers, cookie_str):
                return
            self.db.log_trace(jid, "PASS 8 FAILED: FFmpeg stream capture dropped.")

        self.db.log_trace(jid, "PASS 9: Attempting yt-dlp cookie bypass...")
        if await self._run_ytdlp_with_cookies(extracted_url, jid, dl_dir, headers, raw_cookies):
            return

        self.db.log_trace(jid, "PASS 10: Attempting Aria2c full header replay bypass locally...")
        try:
            full_headers = headers.copy()
            if cookie_str:
                full_headers["Cookie"] = cookie_str
            await self._run_aria(extracted_url, jid, dl_dir, headers=full_headers)
            return
        except Exception as e:
            self.db.log_trace(jid, f"PASS 10 FAILED: Aria2c local bypass failed. Error: {e}")

        raise RuntimeError("PASS 11 FAILED: CDNs are blocking signatures or dropping connections on all interface vectors.")

    async def _attempt_ytdlp_variants(self, url: str, jid: str, dl_dir: Path) -> bool:
        variants = [
            ("PASS 1 Standard", {}),
            ("PASS 2 Force Generic", {"force_generic_extractor": True}),
            ("PASS 3 Impersonate Chrome", {"impersonate": ImpersonateTarget(client="chrome")}),
            ("PASS 4 Mobile UA", {"http_headers": {"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1"}})
        ]

        for pass_name, custom_opts in variants:
            self.db.log_trace(jid, f"Attempting {pass_name}...")
            try:
                await asyncio.to_thread(self._execute_ytdlp, url, jid, dl_dir, custom_opts)

                valid_files = [f for f in dl_dir.rglob("*") if f.is_file() and f.suffix.lower() in [".mp4", ".mkv", ".avi", ".ts", ".webm", ".flv", ".php"]]
                if valid_files:
                    self.db.log_trace(jid, f"{pass_name} SUCCESS.")
                    return True
                else:
                    self.db.log_trace(jid, f"{pass_name} FAILED: yt-dlp exited cleanly but wrote no payload.")
            except Exception as e:
                self.db.log_trace(jid, f"{pass_name} FAILED: {str(e)[:100]}")
        return False

    async def _run_playwright_extraction(self, url: str, jid: str, dl_dir: Path) -> dict:
        from playwright.async_api import async_playwright
        from playwright_stealth import Stealth

        path_to_extension = "/home/ubuntu/stealth_mainframe/ublock/uBlock0.chromium"
        user_data_dir = f"/tmp/pw_data_{jid}"

        extracted_payload = {"url": None, "headers": {}, "cookie_str": "", "raw_cookies": [], "browser_downloaded": False}
        found_urls = []
        capture_headers = {}

        auth_state_path = Path("vk_state.json")
        storage_state = str(auth_state_path) if auth_state_path.exists() else None

        async with async_playwright() as p:
            context = await p.chromium.launch_persistent_context(
                user_data_dir,
                headless=True,
                channel="chromium",
                user_agent=USER_AGENT,
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-site-isolation-trials",
                    "--disable-web-security",
                    "--ignore-certificate-errors",
                    f"--disable-extensions-except={path_to_extension}",
                    f"--load-extension={path_to_extension}"
                ]
            )

            if any(d in url.lower() for d in ["vk.com", "vk.ru", "vkvideo.ru"]) and VK_COOKIES:
                pw_cookies = []
                for item in VK_COOKIES.strip().split(';'):
                    if '=' in item:
                        k, v = item.strip().split('=', 1)
                        pw_cookies.append({"name": k, "value": v, "domain": ".vk.com", "path": "/"})
                        pw_cookies.append({"name": k, "value": v, "domain": ".vk.ru", "path": "/"})
                        pw_cookies.append({"name": k, "value": v, "domain": ".vkvideo.ru", "path": "/"})
                if pw_cookies:
                    try:
                        await context.add_cookies(pw_cookies)
                        self.db.log_trace(jid, "Injected VIP session cookies directly into Playwright browser context.")
                    except Exception as e:
                        self.db.log_trace(jid, f"Failed to inject cookies into Playwright: {e}")

            page = context.pages[0]
            await Stealth().apply_stealth_async(page)
            await page.wait_for_timeout(2000)

            async def handle_response(response):
                try:
                    req = response.request
                    url_lower = req.url.lower()
                    content_type = response.headers.get("content-type", "").lower()

                    bad_keywords = ["google", "analytics", "ad", "beacon", "vast", "blank", "trailer", "promo", ".mp3", "audio"]
                    if any(bad in url_lower for bad in bad_keywords): return

                    is_media = False
                    vtype = "mp4"

                    if "mpegurl" in content_type or "application/x-mpegurl" in content_type or ".m3u8" in url_lower:
                        is_media = True
                        vtype = "m3u8"
                    elif "video/" in content_type or ".mp4" in url_lower or ".ts" in url_lower:
                        is_media = True
                        vtype = "mp4"
                    elif "application/octet-stream" in content_type and req.resource_type in ["media", "xhr", "fetch"]:
                        is_media = True
                        vtype = "mp4"

                    if is_media:
                        found_urls.append({"type": vtype, "url": req.url})
                        headers = await req.all_headers()
                        capture_headers.update(headers)
                except Exception:
                    pass

            page.on("response", handle_response)

            async def handle_route(route):
                if route.request.resource_type == "image":
                    await route.abort()
                else:
                    try: await route.continue_()
                    except Exception: pass

            await page.route("**/*", handle_route)

            try:
                self.db.log_trace(jid, "Navigating to main target URL...")
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(8000)

                if "vk.com" in url or "vkvideo.ru" in url:
                    try:
                        sign_in_btn = page.locator("text='Sign in', text='Войти'")
                        if await sign_in_btn.count() > 0 and await sign_in_btn.first.is_visible():
                            self.db.log_trace(jid, "Guest wall detected. Clicking Sign In to spawn auth form...")
                            await sign_in_btn.first.click()
                            await page.wait_for_timeout(3500)

                        login_input = page.locator("input[name='login']")
                        if await login_input.count() > 0 and await login_input.first.is_visible():
                            self.db.log_trace(jid, "VK Auth Form detected. Injecting Playwright credentials...")

                            if VK_USERNAME:
                                await login_input.first.fill(VK_USERNAME)
                                await page.keyboard.press("Enter")
                                await page.wait_for_timeout(3500)

                            pass_input = page.locator("input[name='password']")
                            if await pass_input.count() > 0 and await pass_input.first.is_visible() and VK_PASSWORD:
                                await pass_input.first.fill(VK_PASSWORD)
                                await page.keyboard.press("Enter")
                                await page.wait_for_timeout(6000)

                            self.db.log_trace(jid, "Playwright auth sequence executed. Reloading target wall...")
                            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                            await page.wait_for_timeout(8000)
                    except Exception as e:
                        self.db.log_trace(jid, f"VK Auth automation bypassed or failed: {e}")

                try:
                    await page.screenshot(path=str(dl_dir / f"{jid}_01_initial_load.png"))
                except Exception: pass

                try:
                    age_gate = await page.wait_for_selector("a.av_btn.av_go[rel='yes']", state="visible", timeout=10000)
                    if age_gate:
                        self.db.log_trace(jid, "Age-gate detected. Clicking 'Yes'...")
                        await age_gate.click()
                        await page.wait_for_timeout(2000)
                except Exception:
                    pass

                try:
                    await page.screenshot(path=str(dl_dir / f"{jid}_02_post_age_gate.png"))
                except Exception: pass

                try:
                    fake_player = await page.wait_for_selector("div.vi-on, div.play", state="visible", timeout=5000)
                    if fake_player:
                        self.db.log_trace(jid, "Clicking fake player overlay to spawn iframe...")
                        await fake_player.click()
                        await page.wait_for_timeout(4000)
                except Exception:
                    self.db.log_trace(jid, "No fake player overlay found. Proceeding...")

                try:
                    await page.screenshot(path=str(dl_dir / f"{jid}_03_pre_click_bomb.png"))
                except Exception: pass

                try:
                    self.db.log_trace(jid, f"Scanning main page and {len(page.frames)} child frames for video players...")
                    jw_url = None

                    viewport = page.viewport_size
                    cx = viewport['width'] / 2
                    cy = viewport['height'] / 2

                    click_targets = [
                        (cx, cy),
                        (cx, cy + 100),
                        (50, viewport['height'] - 50)
                    ]

                    for x, y in click_targets:
                        await page.mouse.move(x, y)
                        await page.mouse.down()
                        await page.mouse.up()
                        await page.wait_for_timeout(800)

                    await page.wait_for_timeout(6000)

                    try:
                        await page.screenshot(path=str(dl_dir / f"{jid}_04_post_click_bomb.png"))
                    except Exception: pass

                    frames_to_search = [page.main_frame] + page.frames

                    for frame in frames_to_search:
                        if "google" in frame.url or "blank" in frame.url or "magsrv" in frame.url: continue

                        try:
                            await frame.evaluate("document.querySelectorAll('.play-button, .vjs-big-play-button, video').forEach(b => b.click());")
                            await frame.evaluate("document.querySelectorAll('video').forEach(v => { v.muted = true; v.playbackRate = 16.0; });")

                            res = await frame.evaluate('''() => {
                                try {
                                    const isBad = (url) => url.match(/trailer|promo|ad|blank|teaser/i);
                                    if (typeof jwplayer === 'function') {
                                        let pl = jwplayer().getPlaylist();
                                        if (pl) {
                                            for (let i = 0; i < pl.length; i++) {
                                                if (pl[i].file && !isBad(pl[i].file) && pl[i].file.includes('.m3u8')) return pl[i].file;
                                            }
                                        }
                                    }
                                    let v = document.querySelector('video');
                                    if (v && v.src && !v.src.startsWith('blob:') && !isBad(v.src)) return v.src;
                                } catch(e) {}
                                return null;
                            }''')
                            if res:
                                jw_url = res
                                break
                        except Exception:
                            pass

                    if jw_url:
                        self.db.log_trace(jid, "RAM Ripper successful!")
                        extracted_payload["url"] = jw_url

                except Exception as e:
                    self.db.log_trace(jid, f"Simulation warning: {e}")

                if not extracted_payload.get("url"):
                    self.db.log_trace(jid, "RAM Ripper missed. Checking Network Sniffer logs...")
                    m3u8s = [u["url"] for u in found_urls if u["type"] == "m3u8"]

                    if m3u8s:
                        extracted_payload["url"] = m3u8s[-1]
                        self.db.log_trace(jid, "Sniffer successfully locked onto HLS Stream.")
                    else:
                        mp4s = [u["url"] for u in found_urls if u["type"] == "mp4"]
                        if mp4s:
                            extracted_payload["url"] = mp4s[-1]
                            self.db.log_trace(jid, "Sniffer successfully locked onto MP4 Stream.")
                        else:
                            extracted_payload["url"] = url

            except Exception as e:
                self.db.log_trace(jid, f"Playwright critical failure: {e}")
                try:
                    await page.screenshot(path=str(dl_dir / f"{jid}_crash_screenshot.png"))
                    html_content = await page.content()
                    with open(dl_dir / f"{jid}_crash_dump.html", "w", encoding="utf-8") as f:
                        f.write(html_content)
                    self.db.log_trace(jid, "Crash screenshot and HTML saved.")
                except Exception:
                    pass

            extracted_payload["headers"] = {
                "Referer": url,
                "Origin": "/".join(url.split("/")[:3]),
                "User-Agent": USER_AGENT,
                "Accept": "*/*",
                "Connection": "keep-alive"
            }

            bad_headers = ["host", "accept-encoding", "sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform", "user-agent", "accept", "referer", "origin"]

            for k, v in capture_headers.items():
                if k.lower() not in bad_headers:
                    extracted_payload["headers"][k] = v

            extracted_payload["headers"]["sec-ch-ua"] = '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"'
            extracted_payload["headers"]["sec-ch-ua-mobile"] = "?0"
            extracted_payload["headers"]["sec-ch-ua-platform"] = '"Windows"'

            cookies = await context.cookies()
            extracted_payload["raw_cookies"] = cookies
            extracted_payload["cookie_str"] = "; ".join([f"{c['name']}={c['value']}" for c in cookies])

            if extracted_payload.get("url"):
                try:
                    self.db.log_trace(jid, "PASS 7.5: Booting Localhost Playwright Proxy...")
                    proxy = PlaywrightHLSProxy(page, extracted_payload["url"])
                    local_m3u8_url = await proxy.start()
                    
                    self.db.log_trace(jid, f"PASS 7.5: Proxy online. Handing off to N_m3u8DL-RE for multi-threaded capture...")
                    
                    out_file = dl_dir / f"{jid}.mp4"
                    
                    # ── Swap FFmpeg for multi-threaded N_m3u8DL-RE ──
                    cmd = [
                        "N_m3u8DL-RE", local_m3u8_url,
                        "--save-dir", str(dl_dir),
                        "--save-name", jid,
                        "--thread-count", "16",
                        "--auto-subtitle-fix", "True"
                    ]
                    
                    # Increase buffer limit to 5MB to prevent the StreamReader from crashing
                    proc = await asyncio.create_subprocess_exec(
                        *cmd, 
                        stdout=asyncio.subprocess.PIPE, 
                        stderr=asyncio.subprocess.STDOUT,
                        limit=1024 * 1024 * 5
                    )
                    
                    # ── NEW PROGRESS PARSER FOR THE UI ──
                    try:
                        while True:
                            line = await proc.stdout.readline()
                            if not line: break
                            
                            line_str = line.decode('utf-8', errors='ignore').strip()
                            clean_str = re.sub(r"\x1b[^m]*m", "", line_str)
                            
                            if "%" in clean_str or "Mbps" in clean_str:
                                global _live_ui_text
                                _live_ui_text[jid] = f"[N_m3u8DL-RE] {clean_str[:50]}"
                                
                                m_pct = re.search(r"(\d{1,3}\.\d{1,2})%", clean_str)
                                if m_pct:
                                    try:
                                        val = float(m_pct.group(1))
                                        asyncio.create_task(self.db.update_job(jid, pct=val, stage="downloading | proxy"))
                                    except Exception: pass
                    except Exception as e:
                        self.db.log_trace(jid, f"Output Reader Warning: {e}")
                    
                    await proc.wait()
                    await proxy.stop()
                    
                    # Check if N_m3u8DL-RE successfully created the media file
                    valid_files = [f for f in dl_dir.rglob(f"{jid}.*") if f.is_file() and f.suffix.lower() in [".mp4", ".ts", ".mkv"]]
                    
                    if proc.returncode == 0 and valid_files:
                        self.db.log_trace(jid, "PASS 7.5 SUCCESS: N_m3u8DL-RE proxy stream capture complete.")
                        extracted_payload["browser_downloaded"] = True
                    else:
                        self.db.log_trace(jid, "PASS 7.5 FAILED: N_m3u8DL-RE proxy capture failed or wrote no payload.")
                        extracted_payload["browser_downloaded"] = False

                except Exception as e:
                    self.db.log_trace(jid, f"PASS 7.5 CRITICAL PROXY ERROR: {e}")
                    extracted_payload["browser_downloaded"] = False

            await context.close()
            try:
                if os.path.exists(user_data_dir):
                    shutil.rmtree(user_data_dir, ignore_errors=True)
            except Exception as e:
                self.db.log_trace(jid, f"Disk cleanup warning: {e}")

            return extracted_payload

    async def _try_browser_native_download(self, page, jid: str, dl_dir: Path, media_url: str) -> bool:
        """
        Executes genuine in-DOM fetching via page.evaluate().
        Inherits real Chrome TLS session, active cookies, and Origin/Referer bindings.
        """
        out_file = dl_dir / f"{jid}.mp4"

        try:
            self.db.log_trace(jid, "PASS 7.5: Executing in-DOM browser native fetch...")

            # 1. Fetch manifest or header bytes directly inside the browser renderer
            fetch_probe_js = """
            async (targetUrl) => {
                try {
                    const resp = await fetch(targetUrl, {
                        method: 'GET',
                        credentials: 'include',
                        mode: 'cors'
                    });
                    if (!resp.ok) return { status: resp.status, ok: false };
                    
                    const text = await resp.text();
                    return { status: resp.status, ok: true, isM3u8: text.startsWith("#EXTM3U") || targetUrl.includes(".m3u8"), content: text };
                } catch (e) {
                    return { ok: false, error: e.toString() };
                }
            }
            """
            probe_result = await page.evaluate(fetch_probe_js, media_url)

            if not probe_result.get("ok"):
                self.db.log_trace(jid, f"PASS 7.5 FAILED: In-DOM fetch returned status {probe_result.get('status')} / err: {probe_result.get('error')}")
                return False

            if probe_result.get("isM3u8"):
                manifest_text = probe_result.get("content", "")
                return await self._download_hls_via_browser(page, jid, dl_dir, media_url, manifest_text, out_file)

            # Direct file download in-DOM (for MP4 streams)
            self.db.log_trace(jid, "PASS 7.5: Streaming direct binary payload from DOM...")
            stream_blob_js = """
            async (targetUrl) => {
                const resp = await fetch(targetUrl, { credentials: 'include' });
                const blob = await resp.blob();
                return new Promise((resolve) => {
                    const reader = new FileReader();
                    reader.onloadend = () => resolve(reader.result.split(',')[1]);
                    reader.readAsDataURL(blob);
                });
            }
            """
            b64_data = await page.evaluate(stream_blob_js, media_url)
            if not b64_data or len(b64_data) < 1000:
                return False

            import base64
            with open(out_file, "wb") as f:
                f.write(base64.b64decode(b64_data))

            self.db.log_trace(jid, f"PASS 7.5 SUCCESS: Saved media payload ({out_file.stat().st_size} bytes).")
            return True

        except Exception as e:
            self.db.log_trace(jid, f"PASS 7.5 FAILED: In-DOM fetch error: {e}")
            return False

    async def _download_hls_via_browser(self, page, jid: str, dl_dir: Path, manifest_url: str, manifest_text: str, out_file: Path) -> bool:
        """
        Recursively resolves Master playlists, downloads .ts segments concurrently inside the DOM,
        and remuxes them cleanly using ffmpeg concat.
        """
        lines = manifest_text.splitlines()

        # ── 1. Resolve Master Playlist Variants ──
        if any(l.startswith("#EXT-X-STREAM-INF") for l in lines):
            best_bw = -1
            variant_uri = None
            for i, line in enumerate(lines):
                if line.startswith("#EXT-X-STREAM-INF"):
                    bw_match = re.search(r"BANDWIDTH=(\d+)", line)
                    bw = int(bw_match.group(1)) if bw_match else 0
                    if i + 1 < len(lines) and not lines[i + 1].startswith("#"):
                        if bw >= best_bw:
                            best_bw = bw
                            variant_uri = lines[i + 1].strip()

            if not variant_uri:
                self.db.log_trace(jid, "PASS 7.5 FAILED: Master playlist had no usable variant streams.")
                return False

            variant_url = urlparse.urljoin(manifest_url, variant_uri)
            self.db.log_trace(jid, f"PASS 7.5: Master playlist resolved. Fetching highest variant ({best_bw} bps)...")

            sub_manifest_js = "async (u) => { const r = await fetch(u, {credentials: 'include'}); return await r.text(); }"
            variant_text = await page.evaluate(sub_manifest_js, variant_url)
            return await self._download_hls_via_browser(page, jid, dl_dir, variant_url, variant_text, out_file)

        # ── 2. Collect Segment URIs ──
        segment_uris = [l.strip() for l in lines if l.strip() and not l.startswith("#")]
        if not segment_uris:
            self.db.log_trace(jid, "PASS 7.5 FAILED: Media playlist contained no segments.")
            return False

        seg_dir = dl_dir / f"{jid}_segments"
        seg_dir.mkdir(parents=True, exist_ok=True)
        concat_list_path = seg_dir / "concat.txt"
        seg_paths = []

        self.db.log_trace(jid, f"PASS 7.5: Downloading {len(segment_uris)} HLS segments in-DOM...")

        # JS snippet: No-iframe fetch with CORS fallback
        fetch_seg_b64_js = """
        async (segUrl) => {
            try {
                const controller = new AbortController();
                const timeoutId = setTimeout(() => controller.abort(), 45000); 
                
                // 1. Fetch using 'omit' to prevent CORS preflight hangs
                // 2. NO IFRAMES: Avoids the 'frame detached' crash caused by site security scripts
                let resp;
                try {
                    resp = await fetch(segUrl, { 
                        credentials: 'omit', 
                        signal: controller.signal 
                    });
                } catch (err) {
                    // Fallback if 'omit' is rejected
                    resp = await fetch(segUrl, { signal: controller.signal });
                }
                
                clearTimeout(timeoutId);
                
                if (!resp.ok) {
                    return { error: `HTTP ${resp.status} ${resp.statusText}` };
                }
                
                // 3. Directly stream the payload in the main DOM context
                const buffer = await resp.arrayBuffer();
                const bytes = new Uint8Array(buffer);
                
                if (bytes.length === 0) {
                    return { error: "Received 0 bytes from CDN" };
                }
                
                let binary = '';
                const chunkSize = 8192; 
                for (let i = 0; i < bytes.length; i += chunkSize) {
                    binary += String.fromCharCode.apply(null, bytes.subarray(i, i + chunkSize));
                }
                
                return { b64: btoa(binary) };
            } catch (e) {
                return { error: `JS Exception: ${e.message || e.toString()}` };
            }
        }
        """

        import base64
        import asyncio
        
        for idx, seg_uri in enumerate(segment_uris):
            seg_url = urlparse.urljoin(manifest_url, seg_uri)
            seg_path = seg_dir / f"seg_{idx:05d}.ts"

            try:
                # Bumped to 60 seconds to ensure the JS timeout fires first
                result = await asyncio.wait_for(
                    page.evaluate(fetch_seg_b64_js, seg_url), 
                    timeout=60.0
                )

                if result and result.get("b64"):
                    with open(seg_path, "wb") as f:
                        f.write(base64.b64decode(result["b64"]))
                    seg_paths.append(seg_path)
                else:
                    err_msg = result.get("error", "Empty/Null response") if result else "No result returned"
                    self.db.log_trace(jid, f"PASS 7.5 FAILED: Segment {idx} rejected by CDN. Error: {err_msg}")
                    # Print directly to our new standard logger
                    log.error(f"Segment {idx} failed: {err_msg}") 
                    shutil.rmtree(seg_dir, ignore_errors=True)
                    return False

            except asyncio.TimeoutError:
                self.db.log_trace(jid, f"PASS 7.5 FAILED: Segment {idx} hard-timed out after 60 seconds.")
                log.error(f"Segment {idx} hard-timed out after 60 seconds.")
                shutil.rmtree(seg_dir, ignore_errors=True)
                return False
            except Exception as e:
                self.db.log_trace(jid, f"PASS 7.5 FAILED: Segment {idx} critical fetch error: {e}")
                shutil.rmtree(seg_dir, ignore_errors=True)
                return False

            # Update live UI every 10 segments or on the last segment
            if idx % 10 == 0 or idx == len(segment_uris) - 1:
                pct = ((idx + 1) / len(segment_uris)) * 100
                await self.db.update_job(jid, pct=pct, stage=f"downloading | in-DOM HLS | seg {idx + 1}/{len(segment_uris)}")
                global _live_ui_text
                _live_ui_text[jid] = f"[DOM-HLS] seg {idx + 1}/{len(segment_uris)} ({pct:.1f}%)"

        # ── 3. Write concat file and remux via ffmpeg ──
        with open(concat_list_path, "w", encoding="utf-8") as f:
            for p in seg_paths:
                f.write(f"file '{p.name}'\n")

        self.db.log_trace(jid, "PASS 7.5: Remuxing segment stream via FFmpeg...")
        cmd = [
            "ffmpeg", "-y", "-f", "concat", "-safe", "0",
            "-i", str(concat_list_path),
            "-c", "copy", "-bsf:a", "aac_adtstoasc",
            str(out_file)
        ]
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        _, stderr = await proc.communicate()

        shutil.rmtree(seg_dir, ignore_errors=True)

        if proc.returncode == 0 and out_file.exists() and out_file.stat().st_size > 1024:
            self.db.log_trace(jid, f"PASS 7.5 SUCCESS: Remux complete ({out_file.stat().st_size} bytes).")
            return True
        else:
            self.db.log_trace(jid, f"PASS 7.5 FAILED: FFmpeg remux error: {stderr.decode(errors='ignore')[:300]}")
            return False

    async def _run_nm3u8dlre_capture(self, url: str, jid: str, dl_dir: Path, headers: dict, cookie_str: str) -> bool:
        out_file = dl_dir / f"{jid}.mp4"

        cmd = [
            "N_m3u8DL-RE", url,
            "--save-dir", str(dl_dir),
            "--save-name", jid,
            "--thread-count", "16",
            "--auto-subtitle-fix", "True",
            "--log-level", "INFO"
        ]

        if headers:
            for k, v in headers.items():
                cmd.extend(["-H", f"{k}: {v}"])

        if cookie_str:
            cmd.extend(["-H", f"Cookie: {cookie_str}"])

        self.db.log_trace(jid, f"N_m3u8DL-RE command initialized with {len(headers)} headers.")

        # Increase the asyncio StreamReader limit to 5MB to handle N_m3u8DL-RE's heavy ANSI output
        proc = await asyncio.create_subprocess_exec(
            *cmd, 
            stdout=asyncio.subprocess.PIPE, 
            stderr=asyncio.subprocess.STDOUT,
            limit=1024 * 1024 * 5
        )

        try:
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break

                line_str = line.decode('utf-8', errors='ignore').strip()
                clean_str = re.sub(r"\x1b[^m]*m", "", line_str)

                if "%" in clean_str or "Mbps" in clean_str:
                    global _live_ui_text
                    _live_ui_text[jid] = f"[N_m3u8DL-RE] {clean_str[:50]}"

                    m_pct = re.search(r"(\d{1,3}\.\d{1,2})%", clean_str)
                    if m_pct:
                        try:
                            val = float(m_pct.group(1))
                            await self.db.update_job(jid, pct=val, stage="downloading | RE-Engine")
                        except Exception: pass

        except Exception as e:
            self.db.log_trace(jid, f"N_m3u8DL-RE Output Reader Error: {e}")

        await proc.wait()

        valid_files = [f for f in dl_dir.rglob(f"{jid}.*") if f.is_file() and f.suffix.lower() in [".mp4", ".ts", ".mkv"]]

        if proc.returncode == 0 and valid_files:
            return True

        return False

    async def _run_ffmpeg_capture(self, url: str, jid: str, dl_dir: Path, headers: dict, cookie_str: str) -> bool:
        out_file = dl_dir / f"{jid}.mp4"
        debug_log_file = dl_dir / f"{jid}_ffmpeg_debug.log"

        header_arg = "".join([f"{k}: {v}\r\n" for k, v in headers.items()])
        if cookie_str:
            header_arg += f"Cookie: {cookie_str}\r\n"

        cmd = [
            "ffmpeg", "-y",
            "-loglevel", "debug",
            "-headers", header_arg,
            "-i", url, "-c", "copy", "-bsf:a", "aac_adtstoasc", str(out_file)
        ]

        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE
        )

        total_duration_sec = 0.0

        def parse_time_to_sec(time_str: str) -> float:
            try:
                h, m, s = time_str.split(':')
                return float(h) * 3600 + float(m) * 60 + float(s)
            except Exception:
                return 0.0

        with open(debug_log_file, "wb") as f:
            while True:
                line = await proc.stderr.readline()
                if not line:
                    break
                f.write(line)

                line_str = line.decode('utf-8', errors='ignore')

                if total_duration_sec == 0.0 and "Duration:" in line_str:
                    m_dur = re.search(r"Duration:\s*([0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+)", line_str)
                    if m_dur:
                        total_duration_sec = parse_time_to_sec(m_dur.group(1))

                if "size=" in line_str and "time=" in line_str:
                    try:
                        m_size = re.search(r"size=\s*([0-9A-Za-z]+)", line_str)
                        m_time = re.search(r"time=\s*([0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+)", line_str)
                        m_speed = re.search(r"speed=\s*([0-9\.]+x)", line_str)

                        size_val = m_size.group(1) if m_size else ""
                        time_str = m_time.group(1) if m_time else ""
                        speed_val = m_speed.group(1) if m_speed else "1.0x"

                        if size_val:
                            current_pct = 0.0
                            if total_duration_sec > 0 and time_str:
                                current_sec = parse_time_to_sec(time_str)
                                current_pct = min((current_sec / total_duration_sec) * 100, 100.0)

                            stage_str = f"downloading | {speed_val} speed | {size_val} DL"
                            await self.db.update_job(jid, stage=stage_str, pct=current_pct)

                            global _live_ui_text
                            _live_ui_text[jid] = f"[ffmpeg] {size_val} at {speed_val} ({current_pct:.1f}%)"
                    except Exception:
                        pass

        await proc.wait()

        if proc.returncode == 0 and out_file.exists() and out_file.stat().st_size > 1024:
            return True

        return False

    async def _run_ytdlp_with_cookies(self, url: str, jid: str, dl_dir: Path, headers: dict, raw_cookies: list) -> bool:
        cookie_path = dl_dir / f"{jid}_cookies.txt"

        with open(cookie_path, "w", encoding="utf-8") as f:
            f.write("# Netscape HTTP Cookie File\n")
            for c in raw_cookies:
                domain = c.get("domain", "")
                inc_sub = "TRUE" if domain.startswith(".") else "FALSE"
                path = c.get("path", "/")
                secure = "TRUE" if c.get("secure", False) else "FALSE"
                expires = str(int(c.get("expires", 0))) if c.get("expires", -1) != -1 else "0"
                name = c.get("name", "")
                value = c.get("value", "")
                f.write(f"{domain}\t{inc_sub}\t{path}\t{secure}\t{expires}\t{name}\t{value}\n")

        opts = {
            "http_headers": headers,
            "cookiefile": str(cookie_path),
            "impersonate": ImpersonateTarget(client="chrome"),
            "extractor_args": {"generic": ["impersonate"]}
        }

        try:
            await asyncio.to_thread(self._execute_ytdlp, url, jid, dl_dir, opts)

            valid_files = [f for f in dl_dir.rglob("*") if f.is_file() and f.suffix.lower() in [".mp4", ".mkv", ".avi", ".ts", ".webm", ".flv", ".php"]]
            if valid_files:
                return True
            else:
                self.db.log_trace(jid, "PASS 9 FAILED: yt-dlp cookie bypass exited cleanly but wrote no payload.")
                return False
        except Exception as e:
            self.db.log_trace(jid, f"PASS 9 FAILED: yt-dlp cookie bypass error: {e}")
            return False

    def _execute_ytdlp(self, url: str, jid: str, dl_dir: Path, custom_opts: dict = None):
        class SilentLogger:
            def debug(self, msg): pass
            def warning(self, msg): pass
            def error(self, msg): pass

        def prog_hook(d):
            if d.get("status") == "downloading":
                try:
                    pct_str = re.sub(r"\x1b[^m]*m", "", d.get("_percent_str", "0.0%")).strip()
                    speed = re.sub(r"\x1b[^m]*m", "", d.get("_speed_str", "~")).strip()
                    eta = re.sub(r"\x1b[^m]*m", "", d.get("_eta_str", "~")).strip()
                    tot_str = re.sub(r"\x1b[^m]*m", "", d.get("_total_bytes_str", d.get("_total_bytes_estimate_str", "~"))).strip()

                    val = float(re.search(r"[\d.]+", pct_str).group()) if re.search(r"[\d.]+", pct_str) else 0.0

                    global _live_ui_text
                    _live_ui_text[jid] = f"[yt-dlp] {pct_str} of {tot_str} at {speed} ETA {eta}"

                    stage_str = f"downloading | {speed} | {eta}"
                    asyncio.run_coroutine_threadsafe(self.db.update_job(jid, pct=val, stage=stage_str), loop)
                except Exception: pass

        opts = {
            "outtmpl": str(dl_dir / f"{jid}.%(ext)s"),
            "format": "bestvideo[height<=1080]+bestaudio/best",
            "merge_output_format": "mp4",
            "progress_hooks": [prog_hook],
            "quiet": True, "noprogress": True, "no_warnings": True,
            "logger": SilentLogger(),
            "compat_opts": {"allow-unsafe-ext"}
        }

        custom_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

        if "srcAg=GECKO" in url:
            custom_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"
            if hasattr(self, 'db'):
                self.db.log_trace(jid, "Ghost Protocol: Gecko CDN signature detected. Spoofing Firefox User-Agent.")
        elif "srcAg=SAFARI" in url:
            custom_ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15"
            if hasattr(self, 'db'):
                self.db.log_trace(jid, "Ghost Protocol: Safari CDN signature detected. Spoofing Apple User-Agent.")
        elif "srcAg=CHROMIUM" in url:
            if hasattr(self, 'db'):
                self.db.log_trace(jid, "Ghost Protocol: Chromium CDN signature detected. Using standard Chrome User-Agent.")

        if "http_headers" not in opts:
            opts["http_headers"] = {}
        opts["http_headers"]["User-Agent"] = custom_ua

        if "impersonate" in opts and ("srcAg=" in url):
            del opts["impersonate"]
            if hasattr(self, 'db'):
                self.db.log_trace(jid, "Ghost Protocol: Disabled curl_cffi impersonation to prevent header collisions.")

        if ("vk.com" in url.lower() or "vkvideo.ru" in url.lower()) and VK_COOKIES:
            cookie_path = dl_dir / f"{jid}_vk_cookies.txt"
            try:
                with open(cookie_path, "w", encoding="utf-8") as f:
                    f.write("# Netscape HTTP Cookie File\n")
                    for item in VK_COOKIES.strip().split(';'):
                        if '=' in item:
                            k, v = item.strip().split('=', 1)
                            f.write(f".vk.com\tTRUE\t/\tTRUE\t2147483647\t{k}\t{v}\n")
                            f.write(f".vkvideo.ru\tTRUE\t/\tTRUE\t2147483647\t{k}\t{v}\n")
                opts["cookiefile"] = str(cookie_path)
            except Exception:
                pass

        if custom_opts: opts.update(custom_opts)

        base_opts = opts.copy()

        opts["external_downloader"] = "aria2c"

        opts["noprogress"] = False
        opts["quiet"] = False

        opts["external_downloader_args"] = {
            "aria2c": [
                "-c",
                "-j", "10",
                "-x", "10",
                "-s", "10",
                "-k", "5M",
                "--summary-interval=1",
                "--console-log-level=notice"
            ]
        }

        if hasattr(self, 'db'):
            self.db.log_trace(jid, "Executing yt-dlp via Aria2c multi-connection mode...")

        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.extract_info(url, download=True)

        except Exception as e:
            if hasattr(self, 'db'):
                self.db.log_trace(jid, f"Aria2c download failed/rejected: {str(e)[:100]}. Falling back to boosted native yt-dlp...")

            fallback_opts = base_opts.copy()

            fallback_opts["concurrent_fragment_downloads"] = 10
            fallback_opts["http_chunk_size"] = 10485760
            fallback_opts["buffersize"] = 32768
            fallback_opts["source_address"] = "0.0.0.0"

            if hasattr(self, 'db'):
                self.db.log_trace(jid, "Executing fallback via Native yt-dlp (Boosted HTTP Settings)...")

            with yt_dlp.YoutubeDL(fallback_opts) as ydl_fallback:
                ydl_fallback.extract_info(url, download=True)

    async def _run_aria(self, url: str, jid: str, dl_dir: Path, headers: dict = None):
        out_name = f"{jid}.mp4"
        cmd = ["aria2c", "-d", str(dl_dir), "-o", out_name, "-c", "-x", "16", "-s", "10", "--file-allocation=none"]
        if headers:
            for k, v in headers.items(): cmd.append(f"--header={k}: {v}")
        cmd.append(url)

        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=subprocess.DEVNULL
        )
        self.procs[jid] = proc
        try:
            while True:
                chunk = await proc.stdout.readline()
                if not chunk: break
                chunk_str = chunk.decode("utf-8", errors="ignore").strip()

                if chunk_str:
                    clean_str = re.sub(r"\x1b[^m]*m", "", chunk_str)

                    if "DL:" in clean_str or "%" in clean_str:
                        global _live_ui_text
                        _live_ui_text[jid] = f"[aria2c] {clean_str}"

                    m = re.search(r"\(([\d.]+)%\).*?DL:([^\s]+).*?ETA:([^\s\]]+)", chunk_str)
                    if m:
                        val = float(m.group(1))
                        stage_str = f"downloading | {m.group(2)} | {m.group(3)}"
                        await self.db.update_job(jid, pct=val, stage=stage_str)
                    else:
                        m2 = re.search(r"\((\d+)%\)", chunk_str)
                        if m2: await self.db.update_job(jid, pct=float(m2.group(1)))
        finally:
            await proc.wait(); self.procs.pop(jid, None)

        valid_files = [f for f in dl_dir.rglob("*") if f.is_file() and f.suffix.lower() in [".mp4", ".mkv", ".avi", ".ts", ".webm", ".flv", ".php"]]
        if not valid_files:
            raise RuntimeError("Aria2c failed: No media payloads found in output directory. The link might be dead or geo-blocked.")

# ═══════════════════════════════════════════════════════════════════════
# CHAPTER 7 — ENCODER ENGINE
# ═══════════════════════════════════════════════════════════════════════

class EncoderEngine:
    def __init__(self, scheduler: JobScheduler):
        self.db = scheduler

    async def validate_media_file(self, file_path: Path, jid: str) -> bool:
        if not file_path.exists():
            self.db.log_trace(jid, f"Validation failed: File does not exist at {file_path}")
            return False

        size_bytes = file_path.stat().st_size
        size_mb = size_bytes / (1024 * 1024)
        self.db.log_trace(jid, f"Validation: File size is {size_mb:.2f} MB")

        if size_bytes < 100000:
            self.db.log_trace(jid, "Validation failed: File is suspiciously small. Likely an HTML error page.")
            return False

        try:
            with open(file_path, 'rb') as f:
                header = f.read(256).lower()
                if b'<!doctype html' in header or b'<html' in header:
                    self.db.log_trace(jid, "Validation failed: File contains HTML magic bytes. Download aborted.")
                    return False
        except Exception as e:
            self.db.log_trace(jid, f"Validation warning: Could not read magic bytes: {e}")

        self.db.log_trace(jid, "Validation: Running ffprobe stream analysis...")
        try:
            process = await asyncio.create_subprocess_exec(
                'ffprobe',
                '-v', 'error',
                '-show_entries', 'format=duration:stream=codec_type',
                '-of', 'json',
                str(file_path),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                self.db.log_trace(jid, f"Validation failed: ffprobe rejected the file. Error: {stderr.decode().strip()}")
                return False

            probe_data = json.loads(stdout.decode())

            probe_dump_path = file_path.with_suffix('.probe.json')
            with open(probe_dump_path, 'w') as f:
                json.dump(probe_data, f, indent=4)

            streams = probe_data.get('streams', [])
            has_video = any(s.get('codec_type') == 'video' for s in streams)

            if not has_video:
                self.db.log_trace(jid, "Validation failed: ffprobe found no video streams.")
                return False

            self.db.log_trace(jid, "Validation passed: Valid video stream confirmed.")
            return True

        except Exception as e:
            self.db.log_trace(jid, f"Validation critical failure during ffprobe execution: {e}")
            return False
            
    async def check_vk_compliance(self, file_path: Path, jid: str) -> bool:
        self.db.log_trace(jid, "Analyzing internal codecs for VK standard compliance...")
        try:
            process = await asyncio.create_subprocess_exec(
                'ffprobe', '-v', 'error',
                '-show_entries', 'format=format_name:stream=codec_name,codec_type',
                '-of', 'json',
                str(file_path),
                stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            stdout, _ = await process.communicate()
            data = json.loads(stdout.decode())
            
            # 1. Check Container Format
            format_name = data.get('format', {}).get('format_name', '')
            if 'mp4' not in format_name.lower() and 'mov' not in format_name.lower():
                return False
                
            has_valid_video = False
            has_valid_audio = False
            
            # 2. Check Codecs
            for stream in data.get('streams', []):
                ctype = stream.get('codec_type')
                cname = stream.get('codec_name', '').lower()
                
                if ctype == 'video' and cname in ['h264', 'hevc', 'av1', 'vp9']:
                    has_valid_video = True
                if ctype == 'audio' and cname in ['aac', 'opus', 'mp3']:
                    has_valid_audio = True
                    
            if has_valid_video and has_valid_audio:
                return True
                
            return False
        except Exception as e:
            self.db.log_trace(jid, f"VK Compliance check failed: {e}")
            return False

    async def execute(self, job_data: dict):
        jid = job_data['id']
        dl_dir, enc_dir, thumb_dir = JOBS_DIR / f"JOB_{jid}" / "dl", JOBS_DIR / f"JOB_{jid}" / "enc", JOBS_DIR / f"JOB_{jid}" / "thumb"

        dl_files = [f for f in dl_dir.rglob("*") if f.is_file() and f.suffix.lower() in [".mp4", ".mkv", ".avi", ".ts", ".webm", ".flv", ".php"]]

        if not dl_files:
            raise RuntimeError("Encoder failed: No downloaded files found to process.")

        dl_file = max(dl_files, key=lambda p: p.stat().st_size)
        enc_file, thumb_file = enc_dir / f"{jid}.mp4", thumb_dir / f"{jid}.jpg"

        self.db.log_trace(jid, "Running Pre-FFmpeg Validation Gate...")
        is_valid = await self.validate_media_file(dl_file, jid)
        if not is_valid:
            raise RuntimeError("Validation Gate Failed: The downloaded payload is not a valid media file. Discarding payload.")

        # ── 1. Always extract a thumbnail ──
        await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-i", str(dl_file), "-ss", "00:00:02", "-vframes", "1", str(thumb_file), 
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )

        # ── 2. Run the Fast-Track Compliance Check ──
        is_vk_ready = await self.check_vk_compliance(dl_file, jid)
        
        if is_vk_ready:
            self.db.log_trace(jid, "✅ Payload is already VK-compliant (Native MP4/H.264/AAC). Bypassing FFmpeg remux.")
            
            # Instantly move the file to the encoded directory to skip processing
            shutil.move(str(dl_file), str(enc_file))
            return

        # ── 3. Fallback to FFmpeg Remuxing (Only for non-compliant files) ──
        self.db.log_trace(jid, "Entering FFmpeg Sandbox to sanitize headers and encode AAC audio...")

        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-nostdin",
            "-fflags", "+genpts",
            "-i", str(dl_file),
            "-c:v", "copy",
            "-c:a", "aac",
            "-avoid_negative_ts", "make_zero",
            "-movflags", "+faststart",
            str(enc_file),
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        try:
            await asyncio.wait_for(proc.wait(), timeout=900)
        except asyncio.TimeoutError:
            proc.kill()
            raise TimeoutError("FFmpeg Zombie Sandbox Timeout: Corrupted video headers caused process hang.")

# ═══════════════════════════════════════════════════════════════════════
# CHAPTER 8 — UPLOADER ENGINE (branches: Telegram / VK)
# ═══════════════════════════════════════════════════════════════════════

class UploaderEngine:
    def __init__(self, db: JobScheduler, app: Client, vk_manager: VKPlaylistManager):
        self.db = db
        self.app = app
        self.vk = vk_manager

    async def execute(self, job_data: dict):
        jid = job_data['id']
        job_dir = JOBS_DIR / f"JOB_{jid}"
        enc_dir, dl_dir, thumb_dir = job_dir / "enc", job_dir / "dl", job_dir / "thumb"

        self.db.log_trace(jid, "Uploader Engine initialized.")
        await self.db.update_job(jid, stage="uploading", pct=0.0)

        target_file = None
        for d in [enc_dir, dl_dir]:
            if d.exists():
                files = [f for f in d.rglob("*") if f.is_file() and not f.name.endswith('.part')]
                if files:
                    target_file = sorted(files, key=lambda x: x.stat().st_size, reverse=True)[0]
                    break

        if not target_file:
            raise RuntimeError("Uploader failed: No media payload found in job directories.")

        self.db.log_trace(jid, f"Target locked: {target_file.name}. Extracting metadata...")

        width, height, duration = await extract_video_metadata(target_file)
        thumb_file = thumb_dir / f"{jid}.jpg"
        thumb_path = str(thumb_file) if thumb_file.exists() else None

        destination = (job_data.get('destination') or 'telegram').lower()

        if destination == 'vk' and not self.vk.available:
            self.db.log_trace(jid, "Requested destination was VK but VK isn't configured. Falling back to Telegram.")
            destination = 'telegram'

        if destination == 'vk':
            route_label = await self._upload_to_vk(job_data, target_file)
        else:
            route_label = await self._upload_to_telegram(job_data, target_file, thumb_path, width, height, duration)

        self.db.log_trace(jid, "Upload sequence complete. Running final UI cleanup...")

        try:
            latest_job = await self.db.get_job(jid)
            if latest_job and latest_job.get('tracker_id'):
                final_text = TXT.JOB_COMPLETE_TEMPLATE.format(title=latest_job['title'][:18], route=route_label)
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("🗑️ DISMISS", callback_data=f"delmsg|{latest_job['tracker_id']}")]])
                await self.app.edit_message_text(latest_job['chat_id'], latest_job['tracker_id'], final_text, reply_markup=kb)
        except Exception as e:
            self.db.log_trace(jid, f"Failed to push final completion card: {e}")

        # ─── LIGHTWEIGHT DIAGNOSTIC DUMP ───
        try:
            self.db.log_trace(jid, "Zipping lightweight diagnostic data before cleanup...")
            zip_target = str(JOBS_DIR / f"JOB_{jid}_diagnostic_success.zip")

            if os.path.exists(zip_target):
                os.remove(zip_target)

            with zipfile.ZipFile(zip_target, 'w', zipfile.ZIP_DEFLATED) as zf:
                for root, dirs, files in os.walk(job_dir):
                    for file in files:
                        if file.lower().endswith(('.log', '.html', '.json', '.png', '.txt')):
                            file_path = os.path.join(root, file)
                            arcname = os.path.relpath(file_path, job_dir)
                            zf.write(file_path, arcname)

            cap = TXT.DIAG_SUCCESS_CAPTION.format(jid=jid)
            await self.app.send_document(job_data['chat_id'], document=zip_target, caption=cap)

            if os.path.exists(zip_target):
                os.remove(zip_target)
        except Exception as e:
            self.db.log_trace(jid, f"Failed to send success debug zip: {e}")

        global _last_completed
        _last_completed = job_data['title']
        await self.db.delete_job(jid)
        shutil.rmtree(job_dir, ignore_errors=True)

    async def _upload_to_telegram(self, job_data: dict, target_file: Path, thumb_path, width, height, duration) -> str:
        jid = job_data['id']
        start_time = time.time()

        async def _up_prog(current, total):
            if not total: return
            pct = (current / total) * 100
            elapsed = time.time() - start_time
            speed = current / elapsed if elapsed > 0 else 0
            eta = (total - current) / speed if speed > 0 else 0

            speed_str = f"{speed / (1024*1024):.2f} MiB/s"
            eta_str = f"{int(eta // 60):02d}:{int(eta % 60):02d}"
            await self.db.update_job(jid, pct=pct, stage=f"uploading | {speed_str} | {eta_str}")

        caption = f"**{job_data['title']}**"
        if job_data.get('caption'):
            caption += f"\n{job_data['caption']}"

        await self.app.send_video(
            chat_id=CHANNEL_ID,
            video=str(target_file),
            caption=caption,
            thumb=thumb_path,
            width=width,
            height=height,
            duration=duration,
            supports_streaming=True,
            progress=_up_prog
        )
        return TXT.route_label('telegram')

    async def _upload_to_vk(self, job_data: dict, target_file: Path) -> str:
        jid = job_data['id']
        raw_playlists = job_data.get('playlist_name')
        caption = job_data.get('caption') or ""

        await self.db.update_job(jid, stage="uploading | vk | resolving playlist(s)", pct=5.0)

        album_ids = []
        if raw_playlists:
            # Split the comma-separated tags we generated in parse_link_message
            playlist_names = [p.strip() for p in raw_playlists.split(",")]
            for p_name in playlist_names:
                a_id = await self.vk.resolve_playlist(p_name, jid, self.db)
                if a_id:
                    album_ids.append(a_id)
        else:
            # Fallback: if no tags were provided, use the title as a single playlist
            fallback_title = job_data.get('title') or "Untitled"
            a_id = await self.vk.resolve_playlist(fallback_title, jid, self.db)
            if a_id:
                album_ids.append(a_id)

        await self.db.update_job(jid, stage="uploading | vk | processing...", pct=10.0)
        
        await self.vk.upload_video(
            file_path=target_file,
            title=job_data['title'],
            description=caption,
            album_ids=album_ids,  # Passing the list of IDs
            jid=jid,
            db=self.db,
        )
        await self.db.update_job(jid, stage="uploading | vk | done", pct=100.0)
        
        # Display the joined list of playlists in the UI card
        display_name = raw_playlists.replace(",", ", ") if raw_playlists else job_data.get('title')
        return TXT.route_label('vk', display_name)
  
# ═══════════════════════════════════════════════════════════════════════
# CHAPTER 9 — RECOVERY & CRASH COURIER
# ═══════════════════════════════════════════════════════════════════════

class CrashCourier:
    @staticmethod
    async def push_fault(app: Client, db: JobScheduler, jid: str, exc: Exception):
        await db.update_job(jid, stage=Stage.FAILED.value)
        tb_str = traceback.format_exc()
        db.log_trace(jid, f"CRITICAL FAULT:\n{tb_str}")

        job = await db.get_job(jid)
        chat_id = job.get('chat_id', OWNER_ID)

        cap = TXT.DIAG_FAULT_CAPTION.format(jid=jid, err=str(exc)[:100])
        job_dir = JOBS_DIR / f"JOB_{jid}"

        if job_dir.exists():
            zip_target = str(JOBS_DIR / f"JOB_{jid}_diagnostic.zip")

            if os.path.exists(zip_target):
                try: os.remove(zip_target)
                except Exception: pass

            try:
                with zipfile.ZipFile(zip_target, 'w', zipfile.ZIP_DEFLATED) as zf:
                    for root, dirs, files in os.walk(job_dir):
                        for file in files:
                            if file.lower().endswith(('.log', '.html', '.json', '.png', '.txt')):
                                file_path = os.path.join(root, file)
                                arcname = os.path.relpath(file_path, job_dir)
                                zf.write(file_path, arcname)

                await app.send_document(chat_id, document=zip_target, caption=cap)

            except Exception as e:
                log_path = job_dir / "trace.log"
                if log_path.exists():
                    try: await app.send_document(chat_id, document=str(log_path), caption=f"{cap}\n*(Failed to zip dir: {e})*")
                    except Exception: pass
            finally:
                if os.path.exists(zip_target):
                    try: os.remove(zip_target)
                    except Exception: pass

class RecoveryManager:
    @staticmethod
    async def scan_and_requeue(db: JobScheduler, dl_q: asyncio.Queue, enc_q: asyncio.Queue, up_q: asyncio.Queue, app: Client):
        active = await db.get_active_jobs()
        resumed = []
        recovering_batch_jids = []

        for job in active:
            jid, stage, title = job['id'], job['stage'], job['title'][:25]
            is_batch = str(job.get('source', '')).startswith('Batch_')

            if stage in [Stage.QUEUED.value, Stage.DOWNLOADING.value] or "download" in stage:
                await db.update_job(jid, stage=Stage.QUEUED.value, recovered_at_stage=stage)
                if is_batch:
                    recovering_batch_jids.append(jid)
                    resumed.append(f"  ├ `[BATCH DL HOLD]` `{title}`")
                else:
                    dl_q.put_nowait(jid)
                    resumed.append(f"  ├ `[DL]` `{title}`")

            elif stage in [Stage.DOWNLOADED.value, Stage.ENCODING.value] or "enc" in stage:
                await db.update_job(jid, stage=Stage.DOWNLOADED.value, recovered_at_stage=stage)
                enc_q.put_nowait(jid)
                resumed.append(f"  ├ `[ENC]` `{title}`")
                if is_batch: recovering_batch_jids.append(jid)

            elif stage in [Stage.ENCODED.value, Stage.UPLOADING.value] or "upload" in stage:
                await db.update_job(jid, stage=Stage.ENCODED.value, recovered_at_stage=stage)
                if is_batch:
                    recovering_batch_jids.append(jid)
                    resumed.append(f"  ├ `[BATCH UP HOLD]` `{title}`")
                else:
                    up_q.put_nowait(jid)
                    resumed.append(f"  ├ `[UP]` `{title}`")

        if resumed and OWNER_ID:
            try: await app.send_message(OWNER_ID, "🔄 **RESUME AUDITOR**\n" + "\n".join(resumed))
            except Exception: pass

        return recovering_batch_jids

# ═══════════════════════════════════════════════════════════════════════
# CHAPTER 10 — PIPELINE MANAGER
# ═══════════════════════════════════════════════════════════════════════

class PipelineManager:
    _DL_STOP = object()  # sentinel: a worker that dequeues this exits gracefully

    def __init__(self, app: Client, db: JobScheduler, vk_manager: VKPlaylistManager):
        self.app, self.db = app, db
        self.dl_q, self.enc_q, self.up_q = asyncio.Queue(), asyncio.Queue(), asyncio.Queue()
        self.dl_engine, self.enc_engine, self.up_engine = DownloaderEngine(db, app), EncoderEngine(db), UploaderEngine(db, app, vk_manager)
        self.dl_worker_tasks: list[asyncio.Task] = []

    async def _worker_loop(self, queue: asyncio.Queue, engine, start_stage: Stage, success_stage: Stage, next_q: asyncio.Queue = None):
        while True:
            jid = await queue.get()

            if jid is self._DL_STOP:
                # Graceful scale-down signal — only ever placed in dl_q, and
                # only ever reached once this worker has finished whatever it
                # was doing and drained every real job queued ahead of it.
                queue.task_done()
                current_task = asyncio.current_task()
                if current_task in self.dl_worker_tasks:
                    self.dl_worker_tasks.remove(current_task)
                logging.getLogger("stealth_bot").info("📉 Download worker stopped gracefully (scaled down).")
                return

            job = await self.db.get_job(jid)
            retry = job.get('retries', 0)

            if job.get('stage') == Stage.CANCELLED.value:
                queue.task_done()
                continue

            try:
                await self.db.update_job(jid, stage=start_stage.value, retries=retry)
                await engine.execute(job)
                await self.db.update_job(jid, stage=success_stage.value, retries=0, recovered_at_stage=None)

                if next_q:
                    updated_job = await self.db.get_job(jid)
                    if next_q == self.up_q and updated_job and str(updated_job.get('source', '')).startswith('Batch_'):
                        pass  # The Batch Orchestrator will manually release this later
                    else:
                        await next_q.put(jid)

            except Exception as e:
                retry += 1
                if retry >= MAX_RETRIES:
                    is_rss_download = bool(job.get('is_rss')) and queue is self.dl_q
                    deferred = job.get('rss_deferred_attempts', 0) if is_rss_download else 0
                    if is_rss_download and deferred < 1:
                        # First round of retries exhausted — park it, keep the
                        # record (the job row itself, stage=failed), and bring
                        # it back once for a second full round after a delay.
                        await self.db.update_job(jid, stage=Stage.FAILED.value, retries=0, rss_deferred_attempts=deferred + 1)
                        self.db.log_trace(jid, f"[RSS] Download failed after {MAX_RETRIES} attempts — parked. "
                                                f"Will retry once more in {RSS_RETRY_DELAY_SECONDS}s.")
                        asyncio.create_task(self._deferred_rss_requeue(jid))
                    else:
                        await CrashCourier.push_fault(self.app, self.db, jid, e)
                        if is_rss_download:
                            self.db.log_trace(jid, "[RSS] Second round also failed — permanently marked failed.")
                else:
                    await self.db.update_job(jid, stage=job['stage'], retries=retry)
                    await queue.put(jid)
            finally:
                queue.task_done()

    async def _deferred_rss_requeue(self, jid: str):
        """Brings a parked RSS download back for one more full round of
        MAX_RETRIES attempts, after RSS_RETRY_DELAY_SECONDS. If that round
        also fails, _worker_loop marks it permanently failed (rss_deferred_
        attempts is already at 1, so it won't be parked again)."""
        await asyncio.sleep(RSS_RETRY_DELAY_SECONDS)
        job = await self.db.get_job(jid)
        if not job or job.get('stage') != Stage.FAILED.value:
            return  # already handled/changed by something else in the meantime
        await self.db.update_job(jid, stage=Stage.QUEUED.value, retries=0)
        self.db.log_trace(jid, "[RSS] Bringing parked download back for a second round of attempts.")
        await self.dl_q.put(jid)

    def start_workers(self):
        for _ in range(MAX_DL_WORKERS):
            t = asyncio.create_task(self._worker_loop(self.dl_q, self.dl_engine, Stage.DOWNLOADING, Stage.DOWNLOADED, self.enc_q))
            self.dl_worker_tasks.append(t)
        asyncio.create_task(self._worker_loop(self.enc_q, self.enc_engine, Stage.ENCODING, Stage.ENCODED, self.up_q))
        asyncio.create_task(self._worker_loop(self.up_q, self.up_engine, Stage.UPLOADING, Stage.COMPLETED, None))

    async def set_dl_worker_count(self, target: int) -> str:
        """Scales the download worker pool to `target`. Scaling up spawns new
        workers immediately. Scaling down never kills an in-flight download —
        it drops one _DL_STOP sentinel into dl_q per worker to remove; each
        exits only once it dequeues that sentinel, i.e. after finishing its
        current job and draining every real job queued ahead of the signal."""
        target = max(1, target)
        current = len(self.dl_worker_tasks)
        if target == current:
            return f"Already at {current} download worker(s)."
        if target > current:
            added = target - current
            for _ in range(added):
                t = asyncio.create_task(self._worker_loop(self.dl_q, self.dl_engine, Stage.DOWNLOADING, Stage.DOWNLOADED, self.enc_q))
                self.dl_worker_tasks.append(t)
            return f"Scaled UP: {current} → {target} download worker(s)."
        else:
            removed = current - target
            for _ in range(removed):
                await self.dl_q.put(self._DL_STOP)
            return (f"Scaling DOWN: {current} → {target} download worker(s). "
                    f"{removed} worker(s) will stop after finishing their current "
                    f"job and draining anything already queued ahead of them — "
                    f"nothing in-flight gets killed.")

# ═══════════════════════════════════════════════════════════════════════
# CHAPTER 11 — TELEGRAM DISPATCHER + UI ACCUMULATOR
# ═══════════════════════════════════════════════════════════════════════

_dashboard_msg_id, _dashboard_chat_id, _dashboard_tab = 0, 0, "root"
_last_completed = "—"
_live_ui_text = {}

class TelegramDispatcher:
    def __init__(self, app: Client):
        self.app = app
        self.edit_queue = asyncio.Queue()
        self.pending_edits = {}
        self.lock = asyncio.Lock()

        self.tokens = 25.0
        self.last_refill = time.time()
        self.rate = 25.0

    async def _consume_token(self):
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(30.0, self.tokens + elapsed * self.rate)
        self.last_refill = now

        if self.tokens < 1.0:
            await asyncio.sleep(1.0 / self.rate)
            await self._consume_token()
        else:
            self.tokens -= 1.0

    async def safe_edit_queued(self, chat_id: int, msg_id: int, text: str, kb: InlineKeyboardMarkup):
        async with self.lock:
            key = (chat_id, msg_id)
            is_new = key not in self.pending_edits
            self.pending_edits[key] = (text, kb)

        if is_new:
            await self.edit_queue.put(key)

    async def sender_loop(self):
        while True:
            key = await self.edit_queue.get()

            async with self.lock:
                if key not in self.pending_edits:
                    self.edit_queue.task_done()
                    continue
                text, kb = self.pending_edits.pop(key)

            retries = 0
            backoff = 1
            while retries < 5:
                await self._consume_token()
                try:
                    await self.app.edit_message_text(key[0], key[1], text, reply_markup=kb)
                    await asyncio.sleep(1.0)
                    break
                except MessageNotModified:
                    break
                except FloodWait as e:
                    sleep_time = e.value + backoff
                    await asyncio.sleep(sleep_time)
                    backoff *= 2
                    retries += 1
                except Exception:
                    break

            self.edit_queue.task_done()

class UIAccumulator:
    def __init__(self, db: JobScheduler, dispatcher: TelegramDispatcher, pipeline: PipelineManager):
        self.db = db
        self.dispatcher = dispatcher
        self.pipeline = pipeline

        self.last_stages = {}
        self.last_pcts = {}
        self.known_jids = set()
        self.job_stats_history = {}

    async def run_loop(self):
        global _dashboard_msg_id, _dashboard_chat_id, _dashboard_tab
        while True:
            await asyncio.sleep(4)

            try:
                jobs = await self.db.get_active_jobs()
                current_jids = {j['id'] for j in jobs}

                dashboard_needs_update = False

                if current_jids != self.known_jids:
                    dashboard_needs_update = True
                    self.known_jids = current_jids

                for job in jobs:
                    jid = job['id']
                    if not job.get('tracker_id'): continue

                    raw_stage = job['stage']
                    base_phase = raw_stage.split("|")[0].strip().lower() if "|" in raw_stage else raw_stage.strip().lower()

                    last_phase = self.last_stages.get(jid, "")
                    last_pct = self.last_pcts.get(jid, -10.0)
                    current_pct = float(job.get('pct', 0.0) or 0.0)

                    if jid not in self.job_stats_history:
                        self.job_stats_history[jid] = {'speeds': [], 'etas': []}
                    if "|" in raw_stage:
                        parts = [p.strip() for p in raw_stage.split("|")]
                        if len(parts) >= 3:
                            self.job_stats_history[jid]['speeds'].append(_parse_speed(parts[1]))
                            self.job_stats_history[jid]['etas'].append(_parse_eta(parts[2]))

                    stage_changed = (base_phase != last_phase)
                    progression_jump = (current_pct - last_pct) >= 10.0

                    if stage_changed or progression_jump:
                        dashboard_needs_update = True

                        hist = self.job_stats_history[jid]
                        avg_s = _format_speed(sum(hist['speeds']) / len(hist['speeds'])) if hist['speeds'] else None
                        avg_e = _format_eta(sum(hist['etas']) / len(hist['etas'])) if hist['etas'] else None

                        kb = InlineKeyboardMarkup([
                            [InlineKeyboardButton("📄 LOGS", callback_data=f"joblog|{jid}"),
                             InlineKeyboardButton("❌ KILL", callback_data=f"kill|{jid}")]
                        ])

                        await self.dispatcher.safe_edit_queued(job['chat_id'], job['tracker_id'], _job_tracker_text(job, avg_s, avg_e), kb)

                        self.last_stages[jid] = base_phase
                        self.last_pcts[jid] = current_pct
                        self.job_stats_history[jid] = {'speeds': [], 'etas': []}

                        await self.db.update_job(jid, last_ui_pct=current_pct)

                if dashboard_needs_update and _dashboard_msg_id and _dashboard_chat_id:
                    text, kb = await _get_dashboard_components(_dashboard_tab, self.db, self.pipeline)
                    await self.dispatcher.safe_edit_queued(_dashboard_chat_id, _dashboard_msg_id, text, kb)

            except Exception:
                pass

# ═══════════════════════════════════════════════════════════════════════
# CHAPTER 12 — DASHBOARD / JOB CARD RENDERING
# ═══════════════════════════════════════════════════════════════════════

def _job_tracker_text(job: dict, avg_speed: str = None, avg_eta: str = None) -> str:
    title = str(job.get('title', 'Unknown'))[:18]
    status_raw = str(job.get('stage', 'PROCESSING')).upper()

    speed, eta = "—", "—"
    if "|" in status_raw:
        parts = [p.strip() for p in status_raw.split("|")]
        status_raw = parts[0]
        if len(parts) >= 3:
            speed = parts[1]
            eta = parts[2]

    if avg_speed: speed = avg_speed
    if avg_eta: eta = avg_eta

    pct = job.get('pct')
    pct_float = float(pct) if pct is not None else 0.0
    bar = make_bar(pct_float, 10)

    route = TXT.route_label(job.get('destination', 'telegram'), job.get('playlist_name'))

    return TXT.JOB_TRACKER_TEMPLATE.format(title=title, status=status_raw, speed=speed, eta=eta, bar=bar, pct=pct_float, route=route)

async def _get_dashboard_components(tab: str, db: JobScheduler, pipeline: PipelineManager) -> tuple[str, InlineKeyboardMarkup]:
    global _last_completed

    parts = tab.split(":")
    stage_tab = parts[0]

    expanded_batch = None
    expanded_jid = None

    if len(parts) == 2:
        if parts[1].startswith("Batch_"):
            expanded_batch = parts[1]
        else:
            expanded_jid = parts[1]
    elif len(parts) == 3:
        expanded_batch = parts[1]
        expanded_jid = parts[2]

    total_storage = sum(f.stat().st_size for f in JOBS_DIR.rglob("*") if f.is_file()) / (1024 ** 3)
    jobs = await db.get_active_jobs()

    recovery_pool = [j for j in jobs if j.get('recovered_at_stage') is not None]
    standard_jobs = [j for j in jobs if j.get('recovered_at_stage') is None]

    def _base(stage_str):
        if not stage_str: return ""
        return stage_str.split("|")[0].strip().lower() if "|" in stage_str else stage_str.strip().lower()

    buckets = {
        "dl": [j for j in standard_jobs if _base(j['stage']) in ["queued", "downloading"]],
        "dl_done": [j for j in standard_jobs if _base(j['stage']) == "downloaded"],
        "enc": [j for j in standard_jobs if _base(j['stage']) in ["encoding", "process"]],
        "enc_done": [j for j in standard_jobs if _base(j['stage']) == "encoded"],
        "up": [j for j in standard_jobs if _base(j['stage']) == "uploading"]
    }

    act_text_blocks = []
    if not buckets['dl'] and not buckets['enc'] and not buckets['up']:
        act_text_blocks.append("`[🔄] ACT  :` `0 DL | 0 PR | 0 UP`")
    else:
        act_text_blocks.append("`[🔄] ACT  :`")
        counter = 1

        if buckets['dl']:
            act_text_blocks.append(f"`  {counter}. DL ({len(buckets['dl'])})`")
            for i, j in enumerate(buckets['dl'][:5]):
                pct = float(j.get('pct', 0.0) or 0.0)
                act_text_blocks.append(f"`     {chr(97+i)}. {j['title'][:12]}.. [{make_bar(pct, 8)}] {pct:.1f}%`")
            counter += 1

        if buckets['enc']:
            act_text_blocks.append(f"`  {counter}. PR ({len(buckets['enc'])})`")
            for i, j in enumerate(buckets['enc'][:5]):
                pct = float(j.get('pct', 0.0) or 0.0)
                act_text_blocks.append(f"`     {chr(97+i)}. {j['title'][:12]}.. [{make_bar(pct, 8)}] {pct:.1f}%`")
            counter += 1

        if buckets['up']:
            act_text_blocks.append(f"`  {counter}. UP ({len(buckets['up'])})`")
            for i, j in enumerate(buckets['up'][:5]):
                pct = float(j.get('pct', 0.0) or 0.0)
                act_text_blocks.append(f"`     {chr(97+i)}. {j['title'][:12]}.. [{make_bar(pct, 8)}] {pct:.1f}%`")

    act_string = "\n".join(act_text_blocks)

    sync_stat = "`RECOVERY AUDIT ACTIVE`" if recovery_pool else "`SYSTEM NORMAL`"

    global _batch_mode, _batch_collection
    batch_active = any(str(j.get('source', '')).startswith('Batch_') for j in standard_jobs)

    if _batch_mode:
        stat_str = f"🟡 BATCH COLLECTION ({len(_batch_collection)} ITEMS QUEUED)"
    elif batch_active:
        stat_str = "🔵 BATCH PROCESSING ACTIVE"
    else:
        stat_str = "ONLINE & SECURE"

    text = (
        f"{TXT.MAINFRAME_TITLE}\n"
        f"`━━━━━━━━━━━━━━━━━━━━━━━━━━`\n"
        f"`[⚡] STAT :` `{stat_str}`\n"
        f"`[⚠️] SYNC :` {sync_stat}\n"
        f"`[💾] DISK :` `{total_storage:.2f} GB`\n"
        f"{act_string}\n"
        f"`[🏁] LAST :` `{_last_completed[:12]}`\n"
        f"`━━━━━━━━━━━━━━━━━━━━━━━━━━`\n"
        f"**Select a subsystem:**"
    )

    kb_lines = []

    def build_batch_dropdown(target_stage: str, label: str, icon: str, job_list: list, parent_tab: str = "root"):
        batch_jobs = [j for j in job_list if str(j.get('source', '')).startswith('Batch_')]
        if not batch_jobs: return

        batches = {}
        for j in batch_jobs:
            src = j['source']
            if src not in batches: batches[src] = []
            batches[src].append(j)

        is_stage_open = (stage_tab == target_stage)
        prefix = "[-]" if is_stage_open else "[+]"
        parent_nav = parent_tab if is_stage_open and target_stage != "root" else ("root" if is_stage_open else target_stage)

        kb_lines.append([InlineKeyboardButton(f"{prefix} {icon} {label} ({len(batches)} BATCHES)", callback_data=f"dash|{parent_nav}")])

        if is_stage_open:
            for b_name, b_jobs in batches.items():
                is_this_batch_open = (expanded_batch == b_name)
                b_prefix = "[-]" if is_this_batch_open else "[+]"

                next_cb = f"dash|{target_stage}" if is_this_batch_open else f"dash|{target_stage}:{b_name}"
                kb_lines.append([InlineKeyboardButton(f" └ {b_prefix} 📦 {b_name} ({len(b_jobs)} Active)", callback_data=next_cb)])

                if is_this_batch_open:
                    for j in b_jobs[:10]:
                        jid = j['id']
                        title = j['title'][:10]
                        is_job_expanded = (expanded_jid == jid)

                        if is_job_expanded:
                            raw_stage = j.get('stage', '')
                            speed, eta = "—", "—"
                            if "|" in raw_stage:
                                p = [p.strip() for p in raw_stage.split("|")]
                                if len(p) >= 3: speed, eta = p[1], p[2]

                            pct = float(j.get('pct', 0.0) or 0.0)
                            bar = make_bar(pct, 8)

                            kb_lines.append([InlineKeyboardButton(f"🪪 ISOLATED JOB CARD: {jid}", callback_data="noop")])
                            kb_lines.append([InlineKeyboardButton(f"📁 {title}...", callback_data="noop")])
                            kb_lines.append([InlineKeyboardButton(f"⚡ {speed}  |  ⏳ {eta}", callback_data="noop")])
                            kb_lines.append([InlineKeyboardButton(f"📊 [{bar}] {pct:.1f}%", callback_data="noop")])
                            kb_lines.append([
                                InlineKeyboardButton("📄 LOGS", callback_data=f"joblog|{jid}"),
                                InlineKeyboardButton("❌ KILL", callback_data=f"kill|{jid}")
                            ])
                            kb_lines.append([
                                InlineKeyboardButton("✏️ RENAME", callback_data=f"rename|{jid}"),
                                InlineKeyboardButton("⏭ FORCE UP", callback_data=f"forceup|{jid}")
                            ])
                            kb_lines.append([InlineKeyboardButton("🔙 CLOSE CARD", callback_data=f"dash|{target_stage}:{b_name}")])
                        else:
                            pct = float(j.get('pct', 0.0) or 0.0)
                            stage_short = _base(j.get('stage', ''))[:4].upper()
                            kb_lines.append([
                                InlineKeyboardButton(f"      ├ [{stage_short}] {title}.. | {pct:.1f}%", callback_data=f"dash|{target_stage}:{b_name}:{jid}"),
                                InlineKeyboardButton("❌", callback_data=f"kill|{j['id']}")
                            ])

    def build_dropdown(target_stage: str, label: str, icon: str, job_list: list, parent_tab: str = "root"):
        is_stage_open = (stage_tab == target_stage)
        prefix = "[-]" if is_stage_open else "[+]"

        kb_lines.append([InlineKeyboardButton(f"{prefix} {icon} {label} ({len(job_list)})", callback_data=f"dash|{parent_tab if is_stage_open else target_stage}")])

        if is_stage_open:
            if not job_list:
                kb_lines.append([InlineKeyboardButton("└ No active tasks", callback_data="noop")])
            for j in job_list[:10]:
                jid = j['id']
                title = j['title'][:10]
                is_job_expanded = (expanded_jid == jid)

                if is_job_expanded:
                    raw_stage = j.get('stage', '')
                    speed, eta = "—", "—"
                    if "|" in raw_stage:
                        parts = [p.strip() for p in raw_stage.split("|")]
                        if len(parts) >= 3: speed, eta = parts[1], parts[2]

                    pct = j.get('pct', 0.0)
                    bar = make_bar(pct, 8)

                    kb_lines.append([InlineKeyboardButton(f"🪪 ISOLATED JOB CARD: {jid}", callback_data="noop")])
                    kb_lines.append([InlineKeyboardButton(f"📁 {title}...", callback_data="noop")])
                    kb_lines.append([InlineKeyboardButton(f"⚡ {speed}  |  ⏳ {eta}", callback_data="noop")])
                    kb_lines.append([InlineKeyboardButton(f"📊 [{bar}] {pct:.1f}%", callback_data="noop")])
                    kb_lines.append([
                        InlineKeyboardButton("📄 LOGS", callback_data=f"joblog|{jid}"),
                        InlineKeyboardButton("❌ KILL", callback_data=f"kill|{jid}")
                    ])
                    kb_lines.append([
                        InlineKeyboardButton("✏️ RENAME", callback_data=f"rename|{jid}"),
                        InlineKeyboardButton("⏭ FORCE UP", callback_data=f"forceup|{jid}")
                    ])
                    kb_lines.append([InlineKeyboardButton("🔙 CLOSE CARD", callback_data=f"dash|{target_stage}")])
                else:
                    pct = j.get('pct', 0.0)
                    kb_lines.append([
                        InlineKeyboardButton(f" ├ ⚡ {title}.. | {pct:.1f}%", callback_data=f"dash|{target_stage}:{jid}"),
                        InlineKeyboardButton("❌", callback_data=f"kill|{jid}")
                    ])

    if recovery_pool:
        is_rec_open = stage_tab in ["recovery", "rec_dl", "rec_enc", "rec_up", "rec_batches"]
        kb_lines.append([InlineKeyboardButton(f"{'[-]' if is_rec_open else '[+]'} 🚨 RECOVERY POOL ({len(recovery_pool)})", callback_data=f"dash|{'root' if is_rec_open else 'recovery'}")])

        if is_rec_open:
            rec_dl = [j for j in recovery_pool if _base(j['recovered_at_stage']) in ["queued", "downloading", "downloaded"]]
            rec_enc = [j for j in recovery_pool if _base(j['recovered_at_stage']) in ["encoding", "encoded"]]
            rec_up = [j for j in recovery_pool if _base(j['recovered_at_stage']) == "uploading"]

            build_batch_dropdown("rec_batches", "STALLED BATCHES", "📦", recovery_pool, parent_tab="recovery")

            build_dropdown("rec_dl", "STALLED DOWNLOADS", "📥", rec_dl, parent_tab="recovery")
            build_dropdown("rec_enc", "STALLED PROCESSING", "⚙️", rec_enc, parent_tab="recovery")
            build_dropdown("rec_up", "STALLED UPLOADS", "📤", rec_up, parent_tab="recovery")

            kb_lines.append([InlineKeyboardButton("🗑️ PURGE ALL RECOVERED", callback_data="purge_recovery")])

    build_batch_dropdown("main_batches", "ACTIVE BATCHES", "📦", standard_jobs, parent_tab="root")

    build_dropdown("dl", "DOWNLOADING", "📥", buckets["dl"])
    build_dropdown("dl_done", "WAITING PROC", "⏳", buckets["dl_done"])
    build_dropdown("enc", "PROCESSING", "⚙️", buckets["enc"])
    build_dropdown("enc_done", "WAITING UP", "⏳", buckets["enc_done"])
    build_dropdown("up", "UPLOADING", "📤", buckets["up"])

    is_storage_open = (stage_tab == "storage")
    kb_lines.append([InlineKeyboardButton(f"{'[-]' if is_storage_open else '[+]'} 💾 STORAGE MANAGER", callback_data=f"dash|{'root' if is_storage_open else 'storage'}")])

    if is_storage_open:
        if not jobs:
            kb_lines.append([InlineKeyboardButton("└ Storage empty", callback_data="noop")])
        else:
            for j in jobs[:10]:
                title = j['title'][:10]
                j_dir = JOBS_DIR / f"JOB_{j['id']}"
                size_mb = sum(f.stat().st_size for f in j_dir.rglob("*") if f.is_file()) / (1024 ** 2) if j_dir.exists() else 0

                kb_lines.append([
                    InlineKeyboardButton(f" ├ 📁 {title}.. | {size_mb:.1f} MB", callback_data="noop"),
                    InlineKeyboardButton("🗑️", callback_data=f"kill|{j['id']}")
                ])

    kb_lines.append([InlineKeyboardButton("🔄 REFRESH SYSTEM", callback_data=f"dash|{tab}")])

    return text, InlineKeyboardMarkup(kb_lines)

async def safe_edit(app: Client, chat_id: int, msg_id: int, text: str, kb: InlineKeyboardMarkup):
    try: await app.edit_message_text(chat_id, msg_id, text, reply_markup=kb)
    except MessageNotModified: pass
    except FloodWait as e: await asyncio.sleep(e.value)
    except Exception: pass

pipeline_ref = None
vk_manager_ref: VKPlaylistManager | None = None

# ── Dump sorter runtime handle, so it can be restarted on its own (cancel +
#    relaunch its background task) without touching the rest of the bot.
#    Populated once main() actually creates the sorter.
_dump_sorter_ref: "DumpSorter | None" = None
_dump_sorter_task: asyncio.Task | None = None

async def restart_dump_sorter(db, jid: str = "DUMP_SYS", full: bool = False) -> str:
    """Cancels the current dump-sorter task (if running) and starts a fresh
    one. Re-reads config.py so a changed DUMP_ALBUM_ID / DUMP_ALBUM_OWNER_ID /
    DUMP_SWEEP_INTERVAL takes effect immediately, without a full bot restart.
    NOTE: this does NOT reload bot.py's own code — DumpSorter.extract_performers
    and the rest of the class body are whatever was already loaded when this
    process started. A source-code fix still needs a full process restart.
    If full=True, also wipes the history file so every video in the dump
    album gets freshly re-checked."""
    global _dump_sorter_ref, _dump_sorter_task

    if _dump_sorter_task and not _dump_sorter_task.done():
        _dump_sorter_task.cancel()
        try:
            await _dump_sorter_task
        except asyncio.CancelledError:
            pass

    import importlib
    importlib.reload(config)
    dump_album_id = getattr(config, "DUMP_ALBUM_ID", DUMP_ALBUM_ID)
    dump_owner_id = getattr(config, "DUMP_ALBUM_OWNER_ID", DUMP_ALBUM_OWNER_ID)
    poll_interval = getattr(config, "DUMP_SWEEP_INTERVAL", DUMP_SWEEP_INTERVAL)

    new_sorter = DumpSorter(vk_manager_ref, dump_album_id, dump_owner_id, poll_interval)

    if full:
        try:
            new_sorter.history_file.unlink(missing_ok=True)
        except Exception:
            pass

    _dump_sorter_ref = new_sorter
    _dump_sorter_task = asyncio.create_task(new_sorter.run_loop(db))

    db.log_trace(jid, f"[DUMP] Restarted (album={dump_album_id}, owner={dump_owner_id}, "
                       f"interval={poll_interval}s, full_rescan={full})")
    return (f"album `{dump_album_id}`, owner `{dump_owner_id}`, "
            f"every `{poll_interval}s`"
            + (", history cleared — full rescan" if full else ""))

# ═══════════════════════════════════════════════════════════════════════
# CHAPTER 13 — DESTINATION SELECTION (NEW)
# One place that owns "ask VK or Telegram" and the follow-up handling for
# both single links and whole batches.
# ═══════════════════════════════════════════════════════════════════════

async def prompt_link_destination(app: Client, msg: Message, url: str, parsed: dict):
    """Stores the parsed link and shows the VK/Telegram picker. The job is
    only created once the person taps a button (see callback router)."""
    token = str(uuid.uuid4())[:10]
    PENDING_LINKS[token] = {
        "url": url,
        "title": parsed["title"],
        "playlist_name": parsed["playlist_name"],
        "caption": parsed["caption"],
        "chat_id": msg.chat.id,
    }
    await msg.reply(
        TXT.LINK_AWAITING_DEST.format(title=parsed["title"][:40]),
        reply_markup=TXT.dest_keyboard(token),
        quote=True,
    )

async def finalize_link_destination(app: Client, db: JobScheduler, pipeline: PipelineManager, cb: CallbackQuery, token: str, destination: str):
    pending = PENDING_LINKS.pop(token, None)
    if not pending:
        await cb.answer(TXT.LINK_PENDING_EXPIRED, show_alert=True)
        return

    if destination == "vk" and not vk_manager_ref.available:
        await cb.answer(TXT.VK_UNAVAILABLE, show_alert=True)
        destination = "telegram"

    jid = str(uuid.uuid4())[:8]
    title = pending["title"]
    tracker_text = TXT.LINK_QUEUED.format(title=title[:30], dest=destination.upper())
    tracker = await app.send_message(
        pending["chat_id"],
        tracker_text,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ CANCEL", callback_data=f"kill|{jid}")]])
    )

    await db.create_job({
        "id": jid, "url": pending["url"], "title": title, "source": "Direct", "quality": "auto",
        "strategy": LinkClassifier.classify(pending["url"]), "chat_id": pending["chat_id"], "tracker_id": tracker.id,
        "destination": destination, "playlist_name": pending["playlist_name"], "caption": pending["caption"],
    })
    await pipeline.dl_q.put(jid)

    try:
        await cb.message.delete()
    except Exception:
        pass
    await cb.answer()

async def prompt_batch_destination(app: Client, chat_id: int, name: str | None, items: list):
    token = str(uuid.uuid4())[:10]
    PENDING_BATCH_SELECTIONS[token] = {"name": name, "items": items}
    await app.send_message(
        chat_id,
        TXT.BATCH_AWAITING_DEST.format(count=len(items)),
        reply_markup=TXT.dest_keyboard(f"batch:{token}"),
    )

async def finalize_batch_destination(app: Client, cb: CallbackQuery, token: str, destination: str):
    pending = PENDING_BATCH_SELECTIONS.pop(token, None)
    if not pending:
        await cb.answer(TXT.LINK_PENDING_EXPIRED, show_alert=True)
        return

    if destination == "vk" and not vk_manager_ref.available:
        await cb.answer(TXT.VK_UNAVAILABLE, show_alert=True)
        destination = "telegram"

    await _pending_batches.put((pending["name"], list(pending["items"]), destination))

    try:
        await cb.message.edit_text(TXT.BATCH_SUBMITTED.format(count=len(pending["items"]), dest=destination.upper()))
    except Exception:
        pass
    await cb.answer()

# ═══════════════════════════════════════════════════════════════════════
# CHAPTER 14 — COMMAND ROUTER
# ═══════════════════════════════════════════════════════════════════════

async def _monitor_batch_completion(db: JobScheduler, chat_id: int, app: Client):
    global _hold_uploads, _mass_upload_active
    while _hold_uploads:
        await asyncio.sleep(5)
        active_jobs = await db.get_active_jobs()

        pending_stages = ["queued", "downloading", "downloaded", "encoding", "process"]
        is_processing = any(
            any(stage in j.get('stage', '').lower() for stage in pending_stages)
            for j in active_jobs
        )

        if not is_processing and active_jobs:
            _hold_uploads = False
            _mass_upload_active = True
            try:
                await app.send_message(chat_id, TXT.BATCH_PROCESSED)
            except Exception:
                pass

            while _mass_upload_active:
                await asyncio.sleep(5)
                jobs = await db.get_active_jobs()
                if not jobs:
                    _mass_upload_active = False
            break

async def _process_single_batch(batch_items: list, batch_counter: int, custom_name: str, destination: str, db: JobScheduler, pipeline: PipelineManager, app: Client):
    actual_name = custom_name if custom_name else str(batch_counter)
    batch_source = f"Batch_{actual_name}"
    batch_jids = []

    for url, title, chat_id in batch_items:
        jid = str(uuid.uuid4())[:8]
        batch_jids.append(jid)

        await db.create_job({
            "id": jid, "url": url, "title": title, "source": batch_source,
            "quality": "auto", "strategy": LinkClassifier.classify(url),
            "chat_id": chat_id, "tracker_id": None,
            "destination": destination, "playlist_name": actual_name, "caption": "",
        })

    for jid in batch_jids:
        job = await db.get_job(jid)
        if not job:
            continue

        tracker = await app.send_message(
            job['chat_id'],
            f"`[ ⚡ ] ＴＡＳＫ :` `{job['title'][:30]}`\n`[ ⚙️ ] ＳＴＡＴ :` `PROCESSING (BATCH {actual_name} · {destination.upper()})`",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ CANCEL", callback_data=f"kill|{jid}")]])
        )

        await db.update_job(jid, tracker_id=tracker.id)
        await pipeline.dl_q.put(jid)

        while True:
            await asyncio.sleep(2)
            current_job = await db.get_job(jid)
            if not current_job:
                break

            base_stage = current_job.get('stage', '').split('|')[0].strip().lower()
            if base_stage in ["encoded", "completed", "failed", "cancelled"]:
                break

        updated_job = await db.get_job(jid)
        if updated_job and updated_job.get('stage') == "encoded":
            await pipeline.up_q.put(jid)

    _pending_batches.task_done()

async def _batch_runner(db: JobScheduler, pipeline: PipelineManager, app: Client):
    batch_counter = 0
    while True:
        custom_name, batch_items, destination = await _pending_batches.get()
        batch_counter += 1

        asyncio.create_task(_process_single_batch(batch_items, batch_counter, custom_name, destination, db, pipeline, app))

async def _resume_interrupted_batches(db: JobScheduler, pipeline: PipelineManager, batch_jids: list):
    dl_jids = []
    for jid in batch_jids:
        job = await db.get_job(jid)
        if job and job.get('stage') == "queued":
            dl_jids.append(jid)

    for jid in dl_jids:
        await pipeline.dl_q.put(jid)
        while True:
            await asyncio.sleep(2)
            job = await db.get_job(jid)
            if not job:
                break
            base_stage = job.get('stage', '').split('|')[0].strip().lower()
            if base_stage not in ["queued", "downloading"]:
                break

    while True:
        await asyncio.sleep(3)
        all_done = True
        for jid in batch_jids:
            job = await db.get_job(jid)
            if job:
                base_stage = job.get('stage', '').split('|')[0].strip().lower()
                if base_stage in ["queued", "downloading", "downloaded", "encoding", "process"]:
                    all_done = False
                    break
        if all_done:
            break

    for jid in batch_jids:
        job = await db.get_job(jid)
        if job:
            base_stage = job.get('stage', '').split('|')[0].strip().lower()
            if base_stage == "encoded":
                await pipeline.up_q.put(jid)

# ── RSS STATE HELPERS FOR TELEGRAM UI ──
RSS_STATE_FILE = BASE_DIR / "rss_state.json" 
RSS_PRIORITY_FILE = BASE_DIR / "rss_priority.json"

def get_rss_state():
    try:
        with open(RSS_STATE_FILE, "r") as f:
            return json.load(f)
    except Exception: return {}

def save_rss_state(state):
    with open(RSS_STATE_FILE, "w") as f:
        json.dump(state, f, indent=4)

def get_rss_priority() -> set:
    try:
        with open(RSS_PRIORITY_FILE, "r") as f:
            return set(json.load(f))
    except Exception:
        return set()

def save_rss_priority(priority_set: set):
    with open(RSS_PRIORITY_FILE, "w") as f:
        json.dump(sorted(priority_set), f, indent=4)

def build_rss_keyboard(state_dict, priority_set=None):
    priority_set = priority_set or set()
    buttons = []
    for i, feed in enumerate(RSS_FEEDS):
        fid = feed.get("id", f"feed_{i}")
        display_name = feed.get("name", feed.get("url", "Unknown Feed"))
        is_active = state_dict.get(fid, False)
        is_priority = fid in priority_set
        status_icon = "🟢" if is_active else "🔴"
        priority_icon = "⭐" if is_priority else "☆"
        buttons.append([
            InlineKeyboardButton(f"{status_icon} | {display_name}", callback_data=f"rss_toggle|{i}"),
            InlineKeyboardButton(priority_icon, callback_data=f"rss_priority_toggle|{i}"),
        ])

    buttons.append([InlineKeyboardButton("🔄 Refresh Status", callback_data="rss_refresh")])
    return InlineKeyboardMarkup(buttons)

def setup_router(app: Client, db: JobScheduler, pipeline: PipelineManager, vk_manager: VKPlaylistManager):
    global pipeline_ref, vk_manager_ref
    pipeline_ref = pipeline
    vk_manager_ref = vk_manager

    @app.on_message(filters.command(["start", "dashboard"]) & filters.user(OWNER_ID))
    async def init_dashboard(_, msg: Message):
        global _dashboard_msg_id, _dashboard_chat_id, _dashboard_tab
        m = await msg.reply("🟢 Booting Mainframe...")
        _dashboard_msg_id, _dashboard_chat_id = m.id, m.chat.id
        try:
            await app.unpin_all_chat_messages(m.chat.id)
            await m.pin(disable_notification=True, both_sides=True)
        except Exception:
            pass
        text, kb = await _get_dashboard_components(_dashboard_tab, db, pipeline)
        await safe_edit(app, _dashboard_chat_id, _dashboard_msg_id, text, kb)

    # ── COMMAND: /rss ──
    @app.on_message(filters.command(["rss"]) & filters.user(OWNER_ID))
    async def cmd_rss_menu(_, msg: Message):
        state = get_rss_state()
        kb = build_rss_keyboard(state, get_rss_priority())
        await msg.reply_text(
            "📡 **RSS Engine Control Panel**\n"
            "Tap a feed to toggle it on/off. Tap ⭐ to mark it priority — "
            "priority feeds get rechecked almost immediately whenever any "
            "other feed finishes its cycle, instead of waiting for their own timer.",
            reply_markup=kb
        )

    # ── COMMAND: /dl [number] ──
    @app.on_message(filters.command(["dl"]) & filters.user(OWNER_ID))
    async def cmd_dl_workers(_, msg: Message):
        args = msg.text.split(maxsplit=1)
        current = len(pipeline_ref.dl_worker_tasks) if pipeline_ref else 0

        if len(args) < 2 or not args[1].strip().lstrip('-').isdigit():
            await msg.reply(f"📥 Current download workers: **{current}**\nUsage: `/dl <number>` to scale.")
            return

        target = int(args[1].strip())
        if target < 1:
            await msg.reply("⚠️ Must keep at least 1 download worker.")
            return
        if not pipeline_ref:
            await msg.reply("❌ Pipeline not ready yet.")
            return

        result = await pipeline_ref.set_dl_worker_count(target)
        await msg.reply(f"📥 {result}")

    # ── COMMAND: /restartdump [full] ──
    @app.on_message(filters.command(["restartdump"]) & filters.user(OWNER_ID))
    async def cmd_restart_dump(_, msg: Message):
        args = msg.text.split(maxsplit=1)
        full = len(args) > 1 and args[1].strip().lower() == "full"
        note = await msg.reply("🗂️ Restarting dump sorter...")
        try:
            summary = await restart_dump_sorter(db, full=full)
            await note.edit_text(f"✅ Dump sorter restarted.\n{summary}\n\n"
                                  f"⚠️ This only restarts the background task — it does NOT reload "
                                  f"bot.py's own code. If you changed the extraction logic itself, "
                                  f"that still needs a full bot restart.")
        except Exception as e:
            await note.edit_text(f"❌ Restart failed: {e}")

    @app.on_message(filters.command(["go"]) & filters.user(OWNER_ID))
    async def batch_go(_, msg: Message):
        global _batch_mode, _batch_collection, _current_batch_name
        _batch_mode = True
        _batch_collection = []

        args = msg.text.split(maxsplit=1)
        _current_batch_name = args[1].strip() if len(args) > 1 else None

        display_name = _current_batch_name or "Auto-generated Number"
        await msg.reply(TXT.BATCH_MODE_START.format(name=display_name))

    @app.on_message(filters.command(["end"]) & filters.user(OWNER_ID))
    async def batch_end(_, msg: Message):
        global _batch_mode, _batch_collection, _current_batch_name
        if not _batch_mode:
            return await msg.reply(TXT.BATCH_NOT_ACTIVE)

        _batch_mode = False
        if not _batch_collection:
            return await msg.reply(TXT.BATCH_EMPTY)

        await prompt_batch_destination(app, msg.chat.id, _current_batch_name, list(_batch_collection))
        _batch_collection.clear()
        _current_batch_name = None

    @app.on_message((filters.video | filters.document) & filters.user(OWNER_ID))
    async def auto_catch_media(_, msg: Message):
        if msg.document and msg.document.mime_type and not msg.document.mime_type.startswith("video/"): return
        jid = str(uuid.uuid4())[:8]
        title = msg.caption.strip() if msg.caption else "Direct Media Upload"
        file_id = msg.video.file_id if msg.video else msg.document.file_id
        tracker = await msg.reply(f"`[ ⚡ ] ＴＡＳＫ :` `{title[:30]}`\n`[ ⚙️ ] ＳＴＡＴ :` `QUEUED`", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ CANCEL", callback_data=f"kill|{jid}")]]))
        await db.create_job({"id": jid, "url": file_id, "title": title, "source": "telegram", "strategy": "TELEGRAM", "chat_id": msg.chat.id, "tracker_id": tracker.id, "destination": "telegram"})
        await pipeline.dl_q.put(jid)
        try: await msg.delete()
        except: pass

    @app.on_message(filters.command(["update"]) & filters.user(OWNER_ID))
    async def update_command(_, msg: Message):
        await msg.reply(TXT.UPDATE_PROMPT, reply_markup=ForceReply(selective=True))

    @app.on_message(filters.text & filters.user(OWNER_ID) & filters.reply)
    async def update_catcher(_, msg: Message):
        if msg.reply_to_message and "UPDATE SEQUENCE" in msg.reply_to_message.text:
            input_name = msg.text.strip()
            script_name = f"{input_name}.py" if not input_name.endswith(".py") else input_name

            if not os.path.exists(script_name):
                return await msg.reply(TXT.UPDATE_FILE_MISSING.format(script=script_name))

            progress = await msg.reply(TXT.UPDATE_IN_PROGRESS.format(script=script_name))

            await app.stop()

            try:
                proc = subprocess.Popen(
                    ["python3.13", script_name],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )

                try:
                    outs, errs = proc.communicate(timeout=6)
                    crashed = True
                    exit_code = proc.returncode
                except subprocess.TimeoutExpired:
                    crashed = False

                if crashed:
                    await app.start()
                    error_log = errs.strip()[-3500:] if errs else "No traceback available (Immediate Exit)."

                    await progress.edit_text(TXT.UPDATE_FAILED.format(script=script_name, code=exit_code, log=error_log))
                else:
                    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
                    payload = {
                        "chat_id": msg.chat.id,
                        "message_id": progress.id,
                        "text": TXT.UPDATE_SUCCESS.format(script=script_name)
                    }
                    async with aiohttp.ClientSession() as session:
                        await session.post(url, json=payload)

                    os._exit(0)

            except Exception as e:
                await app.start()
                await progress.edit_text(TXT.UPDATE_CRITICAL.format(err=str(e)))

            return

    @app.on_message(filters.text & filters.user(OWNER_ID) & ~filters.command(["start", "dashboard", "go", "end", "rss"]))
    async def url_catcher(_, msg: Message):
        if msg.reply_to_message and msg.reply_to_message.text and "RENAME TASK" in msg.reply_to_message.text:
            try:
                jid = re.search(r"`([a-zA-Z0-9_]+)`", msg.reply_to_message.text).group(1)
                new_title = msg.text.strip()
                await db.update_job(jid, title=new_title)
                await msg.reply_to_message.delete()
                await msg.delete()
                if _dashboard_msg_id:
                    text, kb = await _get_dashboard_components(_dashboard_tab, db, pipeline)
                    await safe_edit(app, _dashboard_chat_id, _dashboard_msg_id, text, kb)
            except Exception: pass
            return

        url = next((w for w in msg.text.split() if w.startswith("http") or w.startswith("magnet:?")), None)
        if url:
            global _batch_mode, _batch_collection
            if _batch_mode:
                parsed = parse_link_message(msg.text, url)
                _batch_collection.append((url, parsed["title"], msg.chat.id))
                await msg.reply(TXT.BATCH_ADDED.format(count=len(_batch_collection)), quote=True)
            else:
                parsed = parse_link_message(msg.text, url)
                await prompt_link_destination(app, msg, url, parsed)

    @app.on_callback_query()
    async def _router(client: Client, cb: CallbackQuery):
        global _dashboard_tab, _dashboard_msg_id, _dashboard_chat_id

        if cb.data == "noop":
            await cb.answer()
            return

        # ── FIXED RSS TOGGLE HANDLING ──
        if cb.data.startswith("rss_"):
            action = cb.data.split("|")[0]
            state = get_rss_state()
            
            if action == "rss_toggle":
                try:
                    index = int(cb.data.split("|")[1])
                    feed = RSS_FEEDS[index]
                    fid = feed.get("id")
                    state[fid] = not state.get(fid, False)
                    save_rss_state(state)
                except IndexError:
                    pass

            elif action == "rss_priority_toggle":
                try:
                    index = int(cb.data.split("|")[1])
                    feed = RSS_FEEDS[index]
                    fid = feed.get("id")
                    priority_set = get_rss_priority()
                    if fid in priority_set:
                        priority_set.discard(fid)
                    else:
                        priority_set.add(fid)
                    save_rss_priority(priority_set)
                except IndexError:
                    pass

            kb = build_rss_keyboard(state, get_rss_priority())
            try:
                await cb.message.edit_text(
                    "📡 **RSS Engine Control Panel**\n"
                    "Tap a feed to toggle it on/off. Tap ⭐ to mark it priority — "
                    "priority feeds get rechecked almost immediately whenever any "
                    "other feed finishes its cycle, instead of waiting for their own timer.",
                    reply_markup=kb
                )
                await cb.answer("Status Updated!")
            except Exception:
                await cb.answer("No changes made.")
            return

        if cb.data.startswith("dest|"):
            _, token, destination = cb.data.split("|", 2)
            if token.startswith("batch:"):
                await finalize_batch_destination(app, cb, token.split("batch:", 1)[1], destination)
            else:
                await finalize_link_destination(app, db, pipeline, cb, token, destination)
            return

        if cb.data == "force_release":
            global _hold_uploads, _mass_upload_active
            _hold_uploads = False
            _mass_upload_active = True
            await cb.answer("🔓 Uploads released! Processing mass upload...", show_alert=True)
            try:
                text, kb = await _get_dashboard_components(_dashboard_tab, pipeline_ref.db, pipeline_ref)
                await cb.message.edit_text(text, reply_markup=kb)
            except Exception: pass
            return

        if cb.data.startswith("delmsg|"):
            _, msg_id = cb.data.split("|")
            try:
                await client.delete_messages(cb.message.chat.id, int(msg_id))
                await cb.answer("Cleared from terminal.")
            except Exception:
                await cb.answer("Failed to clear.", show_alert=True)
            return

        if cb.data.startswith("dash|"):
            new_tab = cb.data.split("|")[1]
            if new_tab != _dashboard_tab:
                _dashboard_tab = new_tab
                try:
                    text, kb = await _get_dashboard_components(_dashboard_tab, pipeline_ref.db, pipeline_ref)
                    await cb.message.edit_text(text, reply_markup=kb)
                except MessageNotModified: pass
            await cb.answer()
            return

        if cb.data.startswith("joblog|"):
            jid = cb.data.split("|")[1]
            log_path = JOBS_DIR / f"JOB_{jid}" / "trace.log"
            if not log_path.exists():
                await cb.answer("No logs found.", show_alert=True)
                return
            with open(log_path, "r", encoding="utf-8") as f:
                lines = f.read().splitlines()
                recent_logs = "\n".join(lines[-15:]) if lines else "No data."
            await cb.answer(f"--- TRACE LOGS ---\n{recent_logs}", show_alert=True)
            return

        if cb.data.startswith("rename|"):
            jid = cb.data.split("|")[1]
            await cb.message.reply(
                TXT.RENAME_PROMPT.format(jid=jid),
                reply_markup=ForceReply(selective=True)
            )
            await cb.answer()
            return

        if cb.data.startswith("forceup|"):
            jid = cb.data.split("|")[1]
            await pipeline_ref.db.update_job(jid, stage="downloaded")
            await pipeline_ref.enc_q.put(jid)
            await pipeline_ref.db.log_trace(jid, "SYS_OP OVERRIDE: FORCE UPLOAD INITIATED.")

            await cb.answer("Download interrupted. Pushing payload to encoder/uploader pipeline.", show_alert=True)

            try:
                text, kb = await _get_dashboard_components(_dashboard_tab, pipeline_ref.db, pipeline_ref)
                await cb.message.edit_text(text, reply_markup=kb)
            except Exception: pass
            return

        if cb.data == "purge_recovery":
            jobs = await pipeline_ref.db.get_active_jobs()
            recovery_pool = [j for j in jobs if j.get('recovered_at_stage') is not None]
            for j in recovery_pool:
                jid = j['id']
                await pipeline_ref.db.log_trace(jid, "SYS_OP INITIATED MANUAL OVERRIDE: PURGED FROM RECOVERY.")
                await pipeline_ref.db.delete_job(jid)
                shutil.rmtree(JOBS_DIR / f"JOB_{jid}", ignore_errors=True)
            await cb.answer(f"Purged {len(recovery_pool)} stalled vectors.", show_alert=True)
            try:
                text, kb = await _get_dashboard_components("root", pipeline_ref.db, pipeline_ref)
                await cb.message.edit_text(text, reply_markup=kb)
            except Exception: pass
            return

        if cb.data.startswith("kill|"):
            jid = cb.data.split("|")[1]
            await pipeline_ref.db.log_trace(jid, "SYS_OP INITIATED MANUAL OVERRIDE: KILL COMMAND RECEIVED.")
            await pipeline_ref.db.delete_job(jid)

            job_dir = JOBS_DIR / f"JOB_{jid}"
            shutil.rmtree(job_dir, ignore_errors=True)

            if _dashboard_msg_id != cb.message.id:
                try: await cb.message.edit_text(TXT.TASK_TERMINATED.format(jid=jid), reply_markup=None)
                except Exception: pass
            else:
                try:
                    text, kb = await _get_dashboard_components(_dashboard_tab, pipeline_ref.db, pipeline_ref)
                    await cb.message.edit_text(text, reply_markup=kb)
                except Exception: pass
            await cb.answer("Process terminated and payload destroyed.", show_alert=True)
            return
# ═══════════════════════════════════════════════════════════════════════
# CHAPTER 15 — EVENT LOOPS (terminal UI + helpers)
# ═══════════════════════════════════════════════════════════════════════

def _parse_speed(s: str) -> float:
    try:
        m = re.search(r"([\d\.]+)\s*([KMG]?i?B/s)", str(s).upper().replace(" ", ""))
        if not m: return 0.0
        v, u = float(m.group(1)), m.group(2)
        return v * 1024**3 if "G" in u else v * 1024**2 if "M" in u else v * 1024 if "K" in u else v
    except: return 0.0

def _format_speed(b: float) -> str:
    if b <= 0: return "—"
    for u in ["B/s", "KiB/s", "MiB/s", "GiB/s"]:
        if b < 1024.0: return f"{b:.2f} {u}"
        b /= 1024.0
    return f"{b:.2f} TiB/s"

def _parse_eta(s: str) -> int:
    try:
        parts = re.findall(r"\d+", str(s))
        if len(parts) == 3: return int(parts[0])*3600 + int(parts[1])*60 + int(parts[2])
        if len(parts) == 2: return int(parts[0])*60 + int(parts[1])
    except: pass
    return 0

def _format_eta(s: int) -> str:
    if s <= 0: return "—"
    h, s = divmod(int(s), 3600)
    m, s = divmod(s, 60)
    return f"{h:02d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"

async def terminal_loop(db: JobScheduler, pipeline: PipelineManager):
    sys.stdout.write("\033[2J") # Wipe screen only ONCE at startup
    while True:
        await asyncio.sleep(1)
        # \033[H moves cursor to top-left. \033[K clears the old text on that line.
        sys.stdout.write("\033[H")
        sys.stdout.write(f"{C_CYAN}{C_BOLD}=== STEALTH MAINFRAME [LIVE] ==={C_RESET}\033[K\n")
        sys.stdout.write(f"QUEUES | DL: {pipeline.dl_q.qsize()} | ENC: {pipeline.enc_q.qsize()} | UP: {pipeline.up_q.qsize()}\033[K\n{'─' * 40}\033[K\n")

        jobs = await db.get_active_jobs()
        if not jobs:
            sys.stdout.write(f"{C_GREEN}System Idle. Awaiting vectors.{C_RESET}\033[K\n")
        else:
            for j in jobs[:5]:
                col = C_YELLOW if "download" in j['stage'] else C_CYAN if "enc" in j['stage'] else C_GREEN
                sys.stdout.write(f"{C_BOLD}[{j['title'][:15]}]{C_RESET} {col}{j['stage']}{C_RESET} | [{make_bar(j['pct'], 10)}] {j['pct']:.1f}%\033[K\n")

                log_path = JOBS_DIR / f"JOB_{j['id']}" / "trace.log"
                last_log = "Initializing..."
                if log_path.exists():
                    try:
                        with open(log_path, "r", encoding="utf-8") as f:
                            lines = [ln.strip() for ln in f.read().splitlines() if ln.strip()]
                            if lines: last_log = re.sub(r"^\[.*?\]\s*", "", lines[-1])
                    except Exception: pass
                sys.stdout.write(f"  ├ 📄 \033[2m{last_log[:70]}\033[0m\033[K\n")

                live_text = _live_ui_text.get(j['id'], "Awaiting data stream...")
                sys.stdout.write(f"  └ 📡 \033[36m{live_text[:75]}\033[0m\033[K\n")

        sys.stdout.write("\033[J") # Clear any ghost lines below the UI
        sys.stdout.flush()

# ═══════════════════════════════════════════════════════════════════════
# CHAPTER 15.5 — AUTONOMOUS RSS ENGINE
# ═══════════════════════════════════════════════════════════════════════
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession
import re
import uuid
import asyncio
import logging
import json
import urllib.parse as urlparse
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from config import RSS_FEEDS, SITE_CONFIGS

class RSSFeeder:
    def __init__(self, db, pipeline, app, owner_id: int, vk_manager: VKPlaylistManager): # <-- Added vk_manager
        self.db = db
        self.pipeline = pipeline
        self.app = app
        self.owner_id = owner_id
        self.vk_manager = vk_manager # <-- Saved reference
        
        self.target_feeds = RSS_FEEDS
        self.history_file = BASE_DIR / "rss_history.txt"
        # ... [keep the rest of __init__ the same] ...
        self.state_file = BASE_DIR / "rss_state.json" 
        self.poll_interval = 1800  
        self.global_feed_lock = asyncio.Lock() 

        # ── Priority feeds: each feed_id gets its own wake-up Event. A
        # priority feed sleeps on its Event (bounded by poll_interval as a
        # fallback) instead of a plain sleep; every OTHER feed sets every
        # priority feed's Event right after finishing its own cycle, so
        # priority feeds get rechecked almost immediately instead of waiting
        # for their own timer.
        self._feed_events: dict[str, asyncio.Event] = {
            feed.get("id"): asyncio.Event() for feed in self.target_feeds if feed.get("id")
        }

        self._init_state()

    def _get_priority_set(self) -> set:
        try:
            with open(BASE_DIR / "rss_priority.json", "r") as f:
                return set(json.load(f))
        except Exception:
            return set()

    async def _wake_priority_feeds(self, exclude_feed_id: str | None = None):
        """Called after any feed finishes a scan cycle — nudges every
        priority feed (other than itself, if it happens to be one) to run
        again right now instead of waiting out its own timer."""
        priority_set = self._get_priority_set()
        for fid in priority_set:
            if fid == exclude_feed_id:
                continue
            ev = self._feed_events.get(fid)
            if ev:
                ev.set()

    def _init_state(self):
        state = self._get_state()
        changed = False
        for feed in self.target_feeds:
            fid = feed.get("id")
            if fid and fid not in state:
                state[fid] = False 
                changed = True
        if changed:
            with open(self.state_file, "w") as f:
                json.dump(state, f, indent=4)

    def _get_state(self) -> dict:
        if not self.state_file.exists(): return {}
        try:
            with open(self.state_file, "r") as f:
                return json.load(f)
        except Exception: 
            return {}

    def _load_history(self) -> set:
        if not self.history_file.exists(): return set()
        with open(self.history_file, "r", encoding="utf-8") as f:
            return set(f.read().splitlines())

    def _save_history(self, history_set: set):
        # Atomic write: never leave rss_history.txt truncated/corrupt if the
        # process dies mid-write (e.g. bot restart).
        tmp_path = self.history_file.with_suffix(".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write("\n".join(history_set))
        os.replace(tmp_path, self.history_file)

    def _mark_processed(self, link: str):
        """Read-modify-write against the CURRENT on-disk history — never
        against a cached/stale in-memory set. Each feed runs as its own
        concurrent asyncio task; a stale local copy would silently wipe out
        links other feeds just added when it overwrites the file. This must
        only be called while holding global_feed_lock so two feeds can't
        race each other between the read and the write."""
        current = self._load_history()
        current.add(link)
        self._save_history(current)

    def _parse_metadata(self, raw_title: str, models: list, site_key: str) -> tuple[str, str]:
        tags = []
        clean_title = raw_title.strip()
        
        if site_key == "site_porneec":
            m = re.match(r'^\[(.*?)\]\s*(.*)', clean_title)
            if m:
                tags.append(m.group(1).replace(" ", ""))
                clean_title = m.group(2)
            parts = re.split(r'\s*[-–—]\s*', clean_title)
            if len(parts) > 1:
                raw_names = re.split(r'\s*&\s*|\s*,\s*|\s+and\s+', parts[0], flags=re.IGNORECASE)
                tags.extend([n.replace(" ", "") for n in raw_names if n.strip() and len(n.split()) <= 3])
                
        elif site_key == "site_perverzija":
            parts = re.split(r'\s*[-–—]\s*', clean_title)
            if len(parts) >= 3:
                tags.append(parts[0].strip().replace(" ", "")) 
                raw_names = re.split(r'\s*&\s*|\s*,\s*|\s+and\s+', parts[1], flags=re.IGNORECASE)
                tags.extend([n.replace(" ", "") for n in raw_names if n.strip()])
            elif len(parts) == 2:
                raw_names = re.split(r'\s*&\s*|\s*,\s*|\s+and\s+', parts[0], flags=re.IGNORECASE)
                tags.extend([n.replace(" ", "") for n in raw_names if n.strip()])

        elif site_key == "site_hornysimp":
            tags.append("HornySimp") 
            
        elif site_key == "site_fpv":
            if models: tags.extend([n.replace(" ", "") for n in models if n.strip()])

        if models and site_key != "site_fpv": 
            tags.extend([n.replace(" ", "") for n in models if n.strip()])
            
        clean_tags = list(dict.fromkeys(tags))
        playlist_string = ",".join(clean_tags) if clean_tags else "AutoRSS"
        
        return clean_title, playlist_string

    async def _get_last_page(self, url: str, rss_type: str) -> int:
        try:
            async with AsyncSession(impersonate="chrome") as session:
                headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
                resp = await session.get(url, headers=headers, timeout=30)
                if resp.status_code != 200: return 20
                
                soup = BeautifulSoup(resp.text, "html.parser")
                max_page = 1
                
                if rss_type == "fpv_style":
                    last_btn = soup.select_one(".pagination .last a")
                    if last_btn:
                        m = re.search(r'/(\d+)/?$', last_btn.get("href", ""))
                        if m: max_page = int(m.group(1))
                        
                elif rss_type == "wp_grid":
                    page_links = soup.select("a.page-numbers")
                    for link in page_links:
                        txt = link.get_text(strip=True).replace(',', '')
                        if txt.isdigit(): max_page = max(max_page, int(txt))
                            
                elif rss_type in ["fpo_style", "wp_article", "pt_cv"]:
                    page_links = soup.select(".pagination a, .page-numbers, .pt-cv-pagination a")
                    for link in page_links:
                        href = link.get("href", "")
                        m = re.search(r'(?:/page/|from_videos=)(\d+)/?', href)
                        if m: max_page = max(max_page, int(m.group(1)))
                        
                return max_page if max_page > 1 else 20
        except Exception as e:
            logging.getLogger("stealth_bot").error(f"Auto-page detection failed for {url}: {e}")
            return 20

    async def _fetch_profile(self, url: str, rss_type: str) -> list:
        try:
            async with AsyncSession(impersonate="chrome") as session:
                headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
                resp = await session.get(url, headers=headers, timeout=30)
                if resp.status_code != 200: return []
                
                soup = BeautifulSoup(resp.text, "html.parser")
                entries = []
                
                if rss_type == "fpv_style":
                    for item in soup.select("#list_videos_latest_videos_list_items .item"):
                        a_tag = item.select_one("a.thumb_title")
                        if not a_tag: continue
                        title = a_tag.get("title", "").strip()
                        raw_link = a_tag.get("href", "").strip()
                        link = urlparse.urljoin(url, raw_link)
                        models = [tag.text.strip() for tag in item.select(".models .thumb_model span")]
                        if title and link: entries.append({"title": title, "link": link, "models": models})
                            
                elif rss_type == "fpo_style":
                    for item in soup.select("#list_videos_uploaded_videos_items .item"):
                        a_tag = item.select_one("a")
                        if not a_tag: continue
                        title = a_tag.get("title", "").strip()
                        raw_link = a_tag.get("href", "").strip()
                        link = urlparse.urljoin(url, raw_link)
                        if title and link: entries.append({"title": title, "link": link, "models": []})

                elif rss_type == "wp_grid":
                    for block in soup.find_all("div", class_="col-md-3 col-sm-6 col-xs-6"):
                        head_tag = block.find(class_="item-head")
                        if not head_tag: continue
                        a_tag = head_tag.find("a")
                        if not a_tag: continue
                        title = a_tag.get_text(strip=True)
                        raw_link = a_tag.get("href", "").strip()
                        link = urlparse.urljoin(url, raw_link)
                        if title and link: entries.append({"title": title, "link": link, "models": []})

                elif rss_type == "wp_article":
                    for article in soup.find_all("article", class_="video-preview-item"):
                        a_tag = article.find("a")
                        if not a_tag: continue
                        title = a_tag.get("title", "").strip() 
                        raw_link = a_tag.get("href", "").strip()
                        link = urlparse.urljoin(url, raw_link)
                        if title and link: entries.append({"title": title, "link": link, "models": []})

                elif rss_type == "pt_cv":
                    for block in soup.find_all("div", class_="pt-cv-content-item"):
                        title_tag = block.find(class_="pt-cv-title")
                        if not title_tag: continue
                        a_tag = title_tag.find("a")
                        if not a_tag: continue
                        title = a_tag.get_text(strip=True)
                        raw_link = a_tag.get("href", "").strip()
                        link = urlparse.urljoin(url, raw_link)
                        if title and link: entries.append({"title": title, "link": link, "models": []})
                            
                return entries
        except Exception as e:
            logging.getLogger("stealth_bot").error(f"RSS Fetch Error for {url}: {e}")
            return []

    async def _monitor_feed(self, feed_config: dict):
        base_url = feed_config.get("url")
        feed_id = feed_config.get("id")
        backfill_setting = feed_config.get("backfill_pages", 20)
        
        site_cfg = SITE_CONFIGS.get(feed_config.get("config_key"), {})
        rss_type = site_cfg.get("rss_type", "default")
        
        if rss_type == "fpv_style": page_builder = lambda b, p: f"{b.rstrip('/')}/{p}/"
        elif rss_type in ["wp_grid", "wp_article", "pt_cv"]: page_builder = lambda b, p: f"{b.rstrip('/')}/page/{p}/"
        elif rss_type == "fpo_style": page_builder = lambda b, p: f"{b}{'&' if '?' in b else '?'}from_videos={p}"
        else: page_builder = lambda b, p: f"{b}"

        while True:
            try:
                state = self._get_state()
                if not state.get(feed_id, False):
                    await asyncio.sleep(30)
                    continue

                history = self._load_history()
                marker = f"__BACKFILL_DONE__{base_url}"
                needs_backfill = (marker not in history)
                
                if needs_backfill:
                    if str(backfill_setting).lower() == "auto":
                        logging.getLogger("stealth_bot").info(f"🕵️ Auto-detecting total pages for {base_url}...")
                        total_pages = await self._get_last_page(base_url, rss_type)
                    else:
                        total_pages = int(backfill_setting)
                        
                    logging.getLogger("stealth_bot").info(f"🕰️ Backfill initiated for {base_url}. Scanning {total_pages} pages deep...")
                    urls_to_scan = [page_builder(base_url, p) for p in range(total_pages, 1, -1)]
                    urls_to_scan.append(base_url)
                else:
                    urls_to_scan = [base_url]
                
                for url in urls_to_scan:
                    # ── LIVE PAUSE CHECK 1: Break out of pagination ──
                    if not self._get_state().get(feed_id, False):
                        logging.getLogger("stealth_bot").info(f"⏸️ Scan paused mid-run for {feed_id}.")
                        break 
                        
                    logging.getLogger("stealth_bot").info(f"📡 Scanning RSS target: {url}")
                    entries = await self._fetch_profile(url, rss_type)
                    
                    if not entries:
                        await asyncio.sleep(2)
                        continue
                        
                    for entry in reversed(entries):
                        # ── LIVE PAUSE CHECK 2: Break out of entry parsing ──
                        if not self._get_state().get(feed_id, False):
                            break 
                            
                        link = entry['link']
                        title = entry['title']
                        models = entry['models']
                        
                        if link not in self._load_history():
                            async with self.global_feed_lock:
                                while True:
                                    # ── LIVE PAUSE CHECK 3: Break the queue-waiting lock ──
                                    if not self._get_state().get(feed_id, False):
                                        break
                                        
                                    active_jobs = await self.db.get_active_jobs()
                                    if len(active_jobs) == 0: break 
                                    await asyncio.sleep(5)
                                
                                # Safety catch: if we aborted the wait because of a pause, stop the injection!
                                if not self._get_state().get(feed_id, False):
                                    break
                                
                                jid = str(uuid.uuid4())[:8]
                                site_key = feed_config.get("config_key", "default")
                                clean_caption, playlists = self._parse_metadata(title, models, site_key)
                                tracker_id = None
                                
                                # ── ULTIMATE VK DEDUPLICATION CHECK ──
                                await self.vk_manager.load_vk_database("RSS_SYS", self.db)
                                if await self.vk_manager.is_duplicate(clean_caption):
                                    logging.getLogger("stealth_bot").info(f"⏩ SKIP: '{clean_caption[:30]}' already exists in VK Database.")
                                    # It's already uploaded — just make sure it's linked into
                                    # every playlist this item's tags imply (a video can sit in
                                    # more than one playlist; this only adds missing links, it
                                    # never removes it from ones it's already in).
                                    target_album_ids = []
                                    for p_name in [p.strip() for p in playlists.split(",") if p.strip()]:
                                        a_id = await self.vk_manager.resolve_playlist(p_name, jid, self.db)
                                        if a_id: target_album_ids.append(a_id)
                                    if target_album_ids:
                                        await self.vk_manager.ensure_video_in_albums(clean_caption, target_album_ids, jid, self.db)
                                    self._mark_processed(link)
                                    continue
                                # ─────────────────────────────────────
                                
                                try:
                                    tracker_text = f"`[ ⚡ ] ＲＳＳ ＴＡＳＫ :` `{clean_caption[:30]}...`\n`[ ⚙️ ] ＳＴＡＴ :` `QUEUED (VK)`"
                                    # ... [keep the rest of the job queuing logic exactly the same] ...
                                    tracker = await self.app.send_message(self.owner_id, tracker_text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ CANCEL", callback_data=f"kill|{jid}")]]))
                                    tracker_id = tracker.id
                                except Exception: pass
                                
                                await self.db.create_job({
                                    "id": jid, "url": link, "title": title[:128], "source": base_url, 
                                    "quality": "auto", "strategy": "GENERIC_FALLBACK", "chat_id": self.owner_id, 
                                    "tracker_id": tracker_id, "destination": "vk", "playlist_name": playlists, "caption": clean_caption
                                })
                                await self.db.update_job(jid, is_rss=1)
                                await self.pipeline.dl_q.put(jid)
                                logging.getLogger("stealth_bot").info(f"✨ RSS Injected [{feed_id}]: {clean_caption[:30]} -> Playlists: {playlists}")
                                
                                self._mark_processed(link)
                                
                            await asyncio.sleep(10)
                
                # Only write the backfill marker if the loop actually finished without being paused
                if needs_backfill and self._get_state().get(feed_id, False):
                    async with self.global_feed_lock:
                        history = self._load_history()
                        history.add(marker)
                        self._save_history(history)
                    logging.getLogger("stealth_bot").info(f"✅ Historical Backfill locked in for {base_url}")
                    
            except Exception as e:
                logging.getLogger("stealth_bot").error(f"Error in {feed_id} lane: {e}")

            # Every feed nudges every OTHER priority feed to run again right
            # away, regardless of how this cycle went.
            await self._wake_priority_feeds(exclude_feed_id=feed_id)

            if feed_id in self._get_priority_set():
                # Priority feed: wake early if any other feed's cycle just
                # finished (Event set), otherwise fall back to the normal
                # interval so it's never starved if nothing else triggers it.
                ev = self._feed_events.get(feed_id)
                if ev:
                    try:
                        await asyncio.wait_for(ev.wait(), timeout=self.poll_interval)
                    except asyncio.TimeoutError:
                        pass
                    ev.clear()
                else:
                    await asyncio.sleep(self.poll_interval)
            else:
                await asyncio.sleep(self.poll_interval)

    async def run_loop(self):
        logging.getLogger("stealth_bot").info("🛰️ Autonomous RSS Engine initialized (Domain-Free Mode).")
        await asyncio.sleep(10) 
        tasks = [asyncio.create_task(self._monitor_feed(feed)) for feed in self.target_feeds]
        await asyncio.gather(*tasks)

# ═══════════════════════════════════════════════════════════════════════
# CHAPTER 15.5 — DUMP ALBUM AUTO-SORTER
# Continuously watches one "dump" playlist (videos added by hand or by
# another process) and, for each video, parses performer name(s) out of its
# caption/title, then links the video into the matching performer playlist —
# creating it if it doesn't exist yet. The video is NEVER removed from the
# dump album; this only ever adds it to additional playlists.
# ═══════════════════════════════════════════════════════════════════════

class DumpSorter:
    def __init__(self, vk_manager: VKPlaylistManager, dump_album_id: int, dump_owner_id: int, poll_interval: int = 600):
        self.vk = vk_manager
        self.dump_album_id = dump_album_id
        self.dump_owner_id = dump_owner_id
        self.poll_interval = poll_interval
        self.history_file = BASE_DIR / "dump_sort_history.txt"

    def _load_history(self, path: Path) -> set:
        if not path.exists(): return set()
        with open(path, "r", encoding="utf-8") as f:
            return set(f.read().splitlines())

    def _save_history(self, path: Path, history_set: set):
        tmp_path = path.with_suffix(".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write("\n".join(history_set))
        os.replace(tmp_path, path)

    @staticmethod
    def _clean_word(word: str) -> str:
        return word.strip(".,!?:;\"'()[]{}—-")

    SEPARATORS = {"and", "x"}

    async def extract_performers(self, caption: str, jid: str, db) -> list[str]:
        """A name is normally the next two words joined (no space) — e.g.
        "hasan basu" -> "hasanbasu". But if a separator ('and' or 'x') shows
        up right after only ONE word, that single word is the whole name
        instead — e.g. in "hasan and hussain basu ...", "hasan" alone is a
        name. After a name (either length), if the very next word is a
        separator and at least one more word follows it, parsing continues
        with the next name; otherwise that name was the LAST one, and its
        length (1 vs 2 words) is ambiguous from position alone — resolved by
        checking which version (if either) already exists as a playlist; if
        neither exists yet, defaults to 2 words.
        e.g. "hasan basu and hussain basu going to party"
             -> ["hasanbasu", "hussainbasu"]
             "hasan basu and hussain basu and haider basu are going to gym"
             -> ["hasanbasu", "hussainbasu", "haiderbasu"]
             "hasan and hussain basu are currently enjoying"
             -> ["hasan", "hussainbasu"]
             "hussain basu and hasan went for swimming"  (once "hasan" already
             exists as a playlist from some prior video)
             -> ["hussainbasu", "hasan"]
             "hasan basu x hussain x haider basu went clubbing"
             -> ["hasanbasu", "hussain", "haiderbasu"]"""
        words = (caption or "").strip().split()
        if not words:
            return []
        performers = []
        i = 0
        while i < len(words):
            remaining = len(words) - i

            # Non-last single-word segment: separator right after this one
            # word, with something left to continue parsing after it.
            if remaining >= 2 and self._clean_word(words[i + 1]).lower() in self.SEPARATORS and i + 2 < len(words):
                performers.append(self._clean_word(words[i]))
                i += 2
                continue

            # Non-last two-word segment: separator right after these two
            # words, with something left to continue parsing after it.
            if remaining >= 3 and self._clean_word(words[i + 2]).lower() in self.SEPARATORS and i + 3 < len(words):
                performers.append(self._clean_word(words[i]) + self._clean_word(words[i + 1]))
                i += 3
                continue

            # LAST segment — no following separator to signal its length.
            # Check which version (if either) is an existing playlist.
            c1 = self._clean_word(words[i]) if remaining >= 1 else None
            c2 = (self._clean_word(words[i]) + self._clean_word(words[i + 1])) if remaining >= 2 else None
            c1_exists = bool(c1) and await self.vk.playlist_exists(c1, jid, db)
            c2_exists = bool(c2) and await self.vk.playlist_exists(c2, jid, db)
            if c1_exists and c2_exists:
                chosen = c2  # both exist — tie-break to the 2-word version
            elif c1_exists:
                chosen = c1
            elif c2_exists:
                chosen = c2
            else:
                chosen = c2 or c1  # neither exists yet — default to 2 words
            if chosen:
                performers.append(chosen)
            break
        return performers

    async def _process_video(self, item: dict, default_owner_id: int, jid: str, db) -> bool:
        """Extract performer(s) from one video's title and link it into the
        matching playlist(s), creating any that don't exist yet.
        True if at least one performer was extracted and (attempted to be) linked."""
        v_owner = item.get('owner_id', default_owner_id)
        v_id = item.get('id')
        if v_id is None: return False
        title = item.get('title', '').strip()
        performers = await self.extract_performers(title, jid, db)
        if not performers:
            return False
        for name in performers:
            a_id = await self.vk.resolve_playlist(name, jid, db)
            if not a_id: continue
            linked = await self.vk.link_video(v_owner, v_id, title, a_id, jid, db)
            if linked:
                logging.getLogger("stealth_bot").info(f"🗂️ SORT: '{title[:40]}' -> Playlist '{name}'")
        return True

    async def _sweep_dump_album(self, jid: str, db):
        history = self._load_history(self.history_file)
        new_entries = 0
        offset = 0
        while True:
            result = await self.vk.list_album_videos(self.dump_album_id, offset=offset, count=200)
            items = result.get('items', [])
            if not items: break
            for item in items:
                v_owner = item.get('owner_id', self.dump_owner_id)
                v_id = item.get('id')
                if v_id is None: continue
                key = f"{v_owner}_{v_id}"
                if key in history: continue

                await self._process_video(item, self.dump_owner_id, jid, db)
                # Doesn't clearly match/create a playlist name — leave it in
                # the dump only, but remember we looked so we don't re-scan
                # it forever (edit the caption + delete this history line to
                # force a re-check later).
                history.add(key)
                new_entries += 1

            offset += 200
            if offset >= result.get('count', 0): break

        if new_entries:
            self._save_history(self.history_file, history)
            db.log_trace(jid, f"[DUMP] Sweep processed {new_entries} new video(s) from album {self.dump_album_id}.")

    async def run_loop(self, db):
        logging.getLogger("stealth_bot").info(f"🗂️ Dump Album Auto-Sorter initialized (album {self.dump_album_id}).")
        await asyncio.sleep(20)
        while True:
            try:
                await self.vk.load_vk_database("DUMP_SYS", db)
                await self._sweep_dump_album("DUMP_SYS", db)
            except Exception as e:
                logging.getLogger("stealth_bot").error(f"[SORT] Sweep error: {e}")
            await asyncio.sleep(self.poll_interval)

# ═══════════════════════════════════════════════════════════════════════
# CHAPTER 16 — BOOTSTRAP
# ═══════════════════════════════════════════════════════════════════════

async def main():
    log.info("Booting Stealth Mainframe...")
    app = Client("stealth_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
    db = JobScheduler(DB_PATH)
    vk_manager = VKPlaylistManager(VK_TOKEN)
    pipeline = PipelineManager(app, db, vk_manager)
    dispatcher = TelegramDispatcher(app)

    ui_accumulator = UIAccumulator(db, dispatcher, pipeline)
    
    # Pass vk_manager directly into the RSS Feeder
    rss_engine = RSSFeeder(db, pipeline, app, OWNER_ID, vk_manager)
    dump_sorter = DumpSorter(vk_manager, DUMP_ALBUM_ID, DUMP_ALBUM_OWNER_ID, DUMP_SWEEP_INTERVAL)
    
    # ... [keep the rest of main() the same] ...

    setup_router(app, db, pipeline, vk_manager)

    async with app:
        log.info("Running recovery audits...")
        recovering_batch_jids = await RecoveryManager.scan_and_requeue(db, pipeline.dl_q, pipeline.enc_q, pipeline.up_q, app)
        pipeline.start_workers()

        asyncio.create_task(dispatcher.sender_loop())
        asyncio.create_task(ui_accumulator.run_loop())
        
        # ── BOOT RSS ENGINE BACKGROUND LOOP ──
        asyncio.create_task(rss_engine.run_loop())

        # ── BOOT DUMP-ALBUM AUTO-SORTER BACKGROUND LOOP ──
        global _dump_sorter_ref, _dump_sorter_task
        _dump_sorter_ref = dump_sorter
        _dump_sorter_task = asyncio.create_task(dump_sorter.run_loop(db))
        
        # ... Rest of your code (batch runner, terminal loop, etc.) ...
        asyncio.create_task(terminal_loop(db, pipeline)) # <--- ADD THIS LINE BACK
        
        asyncio.create_task(_batch_runner(db, pipeline, app))
        if recovering_batch_jids:
            asyncio.create_task(_resume_interrupted_batches(db, pipeline, recovering_batch_jids))

        log.info("🟢 System Online. Listening for Telegram links...")

        if OWNER_ID:
            try:
                await app.send_message(OWNER_ID, "🟢 Mainframe Systems Online. Terminal logging active.")
            except Exception:
                pass

        while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    try: loop.run_until_complete(main())
    except KeyboardInterrupt: sys.exit(0)
