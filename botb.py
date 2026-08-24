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
logging.getLogger("pyrogram").setLevel(logging.ERROR)

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
# --------------------------------------

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

JOBS_DIR, DONE_DIR = BASE_DIR / "jobs", BASE_DIR / "completed"
for d in (JOBS_DIR, DONE_DIR): d.mkdir(parents=True, exist_ok=True)

MAX_DL_WORKERS, MAX_RETRIES = 20, 3

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
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY, url TEXT, title TEXT, source TEXT, quality TEXT, strategy TEXT,
                stage TEXT, pct REAL, last_ui_pct REAL, retries INTEGER, chat_id INTEGER, tracker_id INTEGER,
                recovered_at_stage TEXT DEFAULT NULL,
                destination TEXT DEFAULT 'telegram',
                playlist_name TEXT DEFAULT NULL,
                caption TEXT DEFAULT ''
            )''')

            # Patch existing DBs missing newer columns. SQLite throws if a column
            # already exists — that's expected and safely ignored.
            for ddl in (
                'ALTER TABLE jobs ADD COLUMN recovered_at_stage TEXT DEFAULT NULL',
                "ALTER TABLE jobs ADD COLUMN destination TEXT DEFAULT 'telegram'",
                'ALTER TABLE jobs ADD COLUMN playlist_name TEXT DEFAULT NULL',
                "ALTER TABLE jobs ADD COLUMN caption TEXT DEFAULT ''",
            ):
                try:
                    conn.execute(ddl)
                except sqlite3.OperationalError:
                    pass

    async def create_job(self, data: dict):
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''INSERT INTO jobs (id, url, title, source, quality, strategy, stage, pct, last_ui_pct, retries, chat_id, tracker_id, destination, playlist_name, caption)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                             (data['id'], data['url'], data['title'], data['source'], data.get('quality', 'auto'), data.get('strategy', 'GENERIC'),
                              Stage.QUEUED.value, 0.0, -10.0, 0, data['chat_id'], data['tracker_id'],
                              data.get('destination', 'telegram'), data.get('playlist_name'), data.get('caption', '')))

        root = JOBS_DIR / f"JOB_{data['id']}"
        for d in (root, root / "dl", root / "enc", root / "thumb"): d.mkdir(parents=True, exist_ok=True)

    async def update_job(self, jid: str, **kwargs):
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                for k, v in kwargs.items():
                    conn.execute(f'UPDATE jobs SET {k} = ? WHERE id = ?', (v, jid))

    async def get_job(self, jid: str) -> dict:
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute('SELECT * FROM jobs WHERE id = ?', (jid,)).fetchone()
                return dict(row) if row else {}

    async def get_active_jobs(self) -> list[dict]:
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                return [dict(row) for row in conn.execute('SELECT * FROM jobs WHERE stage NOT IN ("completed", "failed", "cancelled")').fetchall()]

    async def delete_job(self, jid: str):
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('DELETE FROM jobs WHERE id = ?', (jid,))

    def log_trace(self, jid: str, msg: str):
        # 1. Keep writing to the job's trace.log file
        try:
            with open(JOBS_DIR / f"JOB_{jid}" / "trace.log", "a", encoding="utf-8") as f:
                f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")
        except Exception:
            pass
        
        # 2. ALSO push it to the standard Python logger so it prints in Termux
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
# CHAPTER 5 — VK PLAYLIST & UPLOAD MANAGER (NEW)
# Playlists are created under the VK_TOKEN owner's own account (no group_id).
# ═══════════════════════════════════════════════════════════════════════

class VKPlaylistManager:
    def __init__(self, token: str | None):
        self.available = bool(vk_api and token)
        self._session = vk_api.VkApi(token=token) if self.available else None
        self._vk = self._session.get_api() if self._session else None
        self._album_cache: dict[str, int] = {}  # lowercase playlist name -> album_id
        self._cache_loaded = False

    async def _load_album_cache(self, jid: str, db: JobScheduler):
        if self._cache_loaded or not self._vk:
            return
        try:
            albums = await asyncio.to_thread(self._vk.video.getAlbums, count=200)
            for item in albums.get('items', []):
                self._album_cache[item.get('title', '').strip().lower()] = item['id']
            self._cache_loaded = True
        except Exception as e:
            db.log_trace(jid, TXT.VK_PLAYLIST_FAILED.format(err=f"cache load: {e}"))

    async def resolve_playlist(self, raw_name: str, jid: str, db: JobScheduler) -> int | None:
        """Case-insensitive lookup of an existing playlist by name; creates it
        (with the '#' stripped) if no match is found. Returns the album_id,
        or None if VK isn't configured / the call fails."""
        if not self._vk:
            return None

        clean_name = raw_name.lstrip('#').strip()
        key = clean_name.lower()

        db.log_trace(jid, TXT.VK_PLAYLIST_RESOLVING.format(name=clean_name))
        await self._load_album_cache(jid, db)

        if key in self._album_cache:
            album_id = self._album_cache[key]
            db.log_trace(jid, TXT.VK_PLAYLIST_FOUND.format(name=clean_name, album_id=album_id))
            return album_id

        try:
            created = await asyncio.to_thread(self._vk.video.addAlbum, title=clean_name)
            album_id = created.get('album_id')
            self._album_cache[key] = album_id
            db.log_trace(jid, TXT.VK_PLAYLIST_CREATED.format(name=clean_name, album_id=album_id))
            return album_id
        except Exception as e:
            db.log_trace(jid, TXT.VK_PLAYLIST_FAILED.format(err=str(e)[:200]))
            return None

    async def upload_video(self, file_path: Path, title: str, description: str, album_ids: list[int] | None, jid: str, db: JobScheduler) -> dict:
        """Bypasses vk_api.VkUpload to manually orchestrate the upload sequence..."""
        if not self._session:
            raise RuntimeError("VK upload unavailable: vk_api not installed or VK_TOKEN missing.")

        def _do_upload():
            import requests
            import time # Added for the processing delay

            # 1. Sanitize metadata and request the upload URL directly
            kwargs = {}
            clean_title = (title or "").strip()
            if clean_title:
                kwargs['name'] = clean_title[:200]
            
            clean_desc = (description or "").strip()
            if clean_desc:
                kwargs['description'] = clean_desc

            # NATIVE FIX: Tell VK to put it in the first playlist automatically!
            if album_ids and len(album_ids) > 0:
                kwargs['album_id'] = album_ids[0]

            try:
                # Ask VK for the upload server
                save_resp = self._vk.video.save(**kwargs)
            except vk_api.exceptions.ApiError as e:
                if e.code == 10:
                    db.log_trace(jid, "[VK] API Error 10 on metadata. Retrying with bare-minimum payload...")
                    save_resp = self._vk.video.save() 
                else:
                    raise e

            upload_url = save_resp.get('upload_url')
            vid_id = save_resp.get('video_id')
            own_id = save_resp.get('owner_id')

            if not upload_url:
                raise RuntimeError(f"Failed to retrieve upload URL from VK. Response: {save_resp}")

            # 2. Push the payload using standard requests
            db.log_trace(jid, "[VK] Upload URL acquired. Streaming payload to VK servers...")
            with open(file_path, 'rb') as f:
                upload_result = requests.post(upload_url, files={'video_file': f}).json()

            if 'video_hash' not in upload_result and 'size' not in upload_result:
                raise RuntimeError(f"VK File stream rejected: {upload_result}")

            # 3. Post-upload Album Assignment (For ANY EXTRA playlists)
            if album_ids and len(album_ids) > 1 and vid_id and own_id:
                db.log_trace(jid, "[VK] Waiting 3 seconds for VK to process before assigning extra playlists...")
                time.sleep(3) # Give VK a moment to finalize the video
                
                # Loop through any remaining albums
                for a_id in album_ids[1:]:
                    try:
                        self._vk.video.addToAlbum(
                            owner_id=own_id,
                            video_id=vid_id,
                            album_id=a_id
                        )
                    except Exception as e:
                        db.log_trace(jid, f"[VK] Album assignment warning for extra album {a_id}: {e}")

            return save_resp

        db.log_trace(jid, TXT.VK_UPLOAD_START)
        result = await asyncio.to_thread(_do_upload)
        db.log_trace(jid, TXT.VK_UPLOAD_DONE.format(result="Success"))
        return result
        
# ═══════════════════════════════════════════════════════════════════════
# CHAPTER 6 — DOWNLOADER ENGINE
# (Kept intentionally as-is — this is the debugged multi-pass extraction
#  waterfall: yt-dlp variants → Playwright → PASS 7.5 browser-native →
#  N_m3u8DL-RE → ffmpeg capture → cookie bypass → aria2c. Don't simplify
#  this without re-testing against VK CDN 403s / curl_cffi pinning.)
# ═══════════════════════════════════════════════════════════════════════

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

    async def execute(self, job_data: dict):
        jid, url, strategy, quality = job_data['id'], job_data['url'], job_data['strategy'], job_data['quality']

        if "vk.ru" in url.lower():
            url = re.sub(r'vk\.ru', 'vk.com', url, flags=re.IGNORECASE)
            self.db.log_trace(jid, f"Normalized vk.ru alias to {url}")

        if any(domain in url.lower() for domain in ["vk.com", "vkvideo.ru"]) and VK_TOKEN:
            self.db.log_trace(jid, "VK target detected. Querying VK API backend with token...")
            extracted_player = await asyncio.to_thread(self._extract_vk_api, url, jid)
            if extracted_player:
                self.db.log_trace(jid, f"VK API Token bypass successful! Rerouting payload URL to: {extracted_player}")
                url = extracted_player

        dl_dir = JOBS_DIR / f"JOB_{jid}" / "dl"

        self.db.log_trace(jid, f"Download Orchestrator engaged. Strategy: {strategy}")

        if strategy == "TELEGRAM":
            async def tg_prog(c, t):
                if t: await self.db.update_job(jid, pct=(c * 100 / t))
            await self.app.download_media(url, file_name=str(dl_dir / f"{jid}.mp4"), progress=tg_prog)
            return

        if strategy in ["MAGNET", "DIRECT_MP4"]:
            await self._run_aria(url, jid, dl_dir)
            return

        playwright_data = self._load_cached_payload(dl_dir)

        if not playwright_data:
            variant_success = await self._attempt_ytdlp_variants(url, jid, dl_dir)
            if variant_success:
                return

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
                    # Pass the active page object rather than context
                    downloaded = await self._try_browser_native_download(
                        page, jid, dl_dir, extracted_payload["url"]
                    )
                    extracted_payload["browser_downloaded"] = downloaded
                except Exception as e:
                    self.db.log_trace(jid, f"PASS 7.5 FAILED: Unexpected error: {e}")
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

        # JS snippet: Extended 45s internal timeout with aggressive error tracing
        fetch_seg_b64_js = """
        async (segUrl) => {
            try {
                const controller = new AbortController();
                const timeoutId = setTimeout(() => controller.abort(), 45000); 
                
                const resp = await fetch(segUrl, { 
                    credentials: 'include', 
                    signal: controller.signal 
                });
                
                clearTimeout(timeoutId);
                
                if (!resp.ok) {
                    return { error: `HTTP ${resp.status} ${resp.statusText}` };
                }
                
                const buffer = await resp.arrayBuffer();
                const bytes = new Uint8Array(buffer);
                
                if (bytes.length === 0) return { error: "Received 0 bytes from CDN" };
                
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

        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
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

        self.db.log_trace(jid, "Entering FFmpeg Sandbox...")

        await asyncio.create_subprocess_exec("ffmpeg", "-y", "-i", str(dl_file), "-ss", "00:00:02", "-vframes", "1", str(thumb_file), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

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
    def __init__(self, app: Client, db: JobScheduler, vk_manager: VKPlaylistManager):
        self.app, self.db = app, db
        self.dl_q, self.enc_q, self.up_q = asyncio.Queue(), asyncio.Queue(), asyncio.Queue()
        self.dl_engine, self.enc_engine, self.up_engine = DownloaderEngine(db, app), EncoderEngine(db), UploaderEngine(db, app, vk_manager)

    async def _worker_loop(self, queue: asyncio.Queue, engine, start_stage: Stage, success_stage: Stage, next_q: asyncio.Queue = None):
        while True:
            jid = await queue.get()
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
                    await CrashCourier.push_fault(self.app, self.db, jid, e)
                else:
                    await self.db.update_job(jid, stage=job['stage'], retries=retry)
                    await queue.put(jid)
            finally:
                queue.task_done()

    def start_workers(self):
        for _ in range(MAX_DL_WORKERS): asyncio.create_task(self._worker_loop(self.dl_q, self.dl_engine, Stage.DOWNLOADING, Stage.DOWNLOADED, self.enc_q))
        asyncio.create_task(self._worker_loop(self.enc_q, self.enc_engine, Stage.ENCODING, Stage.ENCODED, self.up_q))
        asyncio.create_task(self._worker_loop(self.up_q, self.up_engine, Stage.UPLOADING, Stage.COMPLETED, None))

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
    """Handles the lifecycle of a single batch independently, showing only one active job card at a time."""
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
    """Dispatches incoming batches (name, items, destination) to independent workers."""
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

        # Ask VK/Telegram for the whole batch before dispatching.
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

    @app.on_message(filters.text & filters.user(OWNER_ID) & ~filters.command(["start", "dashboard", "go", "end"]))
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
                # Batch collection still just gathers (url, title, chat_id); the
                # #tag/caption syntax is only parsed for single-link mode. In
                # batch mode the /go name is the playlist name for everyone.
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

    setup_router(app, db, pipeline, vk_manager)

    async with app:
        log.info("Running recovery audits...")
        recovering_batch_jids = await RecoveryManager.scan_and_requeue(db, pipeline.dl_q, pipeline.enc_q, pipeline.up_q, app)
        pipeline.start_workers()

        asyncio.create_task(dispatcher.sender_loop())
        asyncio.create_task(ui_accumulator.run_loop())
        
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
