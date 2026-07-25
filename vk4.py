"""
vk_bot.py - Dedicated VK Playlist Downloader Microservice
───────────────────────────────────────────────────────────────
FEATURES & ARCHITECTURE:
  • MTProto Pyrogram Engine (API_ID + API_HASH + BOT_TOKEN) for >1GB Uploads
  • Fixed Pause/Resume & Cancel Playlist State Management
  • Native yt-dlp Multi-threaded Engine (Silent & Fast, Termux safe)
  • Live Upload Progress Tracking (Up to 2 GB per file)
  • Throttled Dual UI: Termux (2s) / Telegram Dashboard (10% or Stage change)
  • Dynamic Job Cards per download with live tracking & cleanup
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
import sqlite3
from pathlib import Path
import yt_dlp
from logging.handlers import RotatingFileHandler

from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait, MessageNotModified
import config

try:
    loop = asyncio.get_running_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

# ──────────────────────────── CONFIGURATION & CONSTANTS ──────────────────

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

# --- Termux UI & Dashboard State Constants ---
C_CYAN, C_YELLOW, C_RED, C_GREEN, C_RESET, C_BOLD = "\033[36m", "\033[33m", "\033[31m", "\033[32m", "\033[0m", "\033[1m"
_live_ui_text = {}
_last_completed = "—"
_dash_msg_id, _dash_chat_id = 0, 0
_dash_tab = "playlists"
_expanded_pl = None
_expanded_bucket = None
_expanded_jid = None

def make_bar(percent: float, width: int = 10) -> str:
    filled = int(max(0.0, min(percent, 100.0)) / (100.0 / width))
    return "█" * filled + "░" * (width - filled)

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
    
    return (
        f"`[❖] ＴＡＳＫ :` `{title}..`\n"
        f"`━━━━━━━━━━━━━━━━━━━━━━━━━━`\n"
        f"`⚙️ PHASE :` `{status_raw}`\n"
        f"`⚡ SPEED :` `{speed}`\n"
        f"`⏳ ETA   :` `{eta}`\n"
        f"`📊 PROG  :` `[{bar}] {pct_float:.1f}%`"
    )

# ──────────────────────────── SUBSYSTEM 1: DATABASE ─────────────────────

class JobScheduler:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.lock = asyncio.Lock()
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''CREATE TABLE IF NOT EXISTS playlists (
                id TEXT PRIMARY KEY, url TEXT, caption TEXT, total INTEGER,
                downloaded INTEGER, status TEXT, chat_id INTEGER
            )''')
            conn.execute('''CREATE TABLE IF NOT EXISTS playlist_items (
                id TEXT PRIMARY KEY, playlist_id TEXT, url TEXT, title TEXT, status TEXT
            )''')
            conn.execute('''CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY, url TEXT, title TEXT, playlist_id TEXT,
                stage TEXT, pct REAL, retries INTEGER, chat_id INTEGER, tracker_id INTEGER
            )''')
            
            # Safe migration if upgrading existing database
            try:
                conn.execute('ALTER TABLE jobs ADD COLUMN tracker_id INTEGER')
            except sqlite3.OperationalError:
                pass

    def log_trace(self, jid: str, msg: str):
        job_dir = JOBS_DIR / f"JOB_{jid}"
        job_dir.mkdir(parents=True, exist_ok=True)
        with open(job_dir / "trace.log", "a", encoding="utf-8") as f:
            f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")

    async def create_playlist(self, pl_id: str, url: str, caption: str, total: int, chat_id: int):
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('INSERT INTO playlists VALUES (?, ?, ?, ?, 0, "active", ?)', (pl_id, url, caption, total, chat_id))

    async def add_playlist_items(self, items: list[tuple]):
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.executemany('INSERT INTO playlist_items VALUES (?, ?, ?, ?, "pending")', items)

    async def get_active_playlists(self) -> list[dict]:
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                return [dict(r) for r in conn.execute('SELECT * FROM playlists WHERE status != "completed" AND status != "cancelled"').fetchall()]

    async def get_playlist(self, pl_id: str) -> dict:
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute('SELECT * FROM playlists WHERE id = ?', (pl_id,)).fetchone()
                return dict(row) if row else {}

    async def update_playlist(self, pl_id: str, **kwargs):
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                for k, v in kwargs.items():
                    conn.execute(f'UPDATE playlists SET {k} = ? WHERE id = ?', (v, pl_id))

    async def cancel_playlist(self, pl_id: str):
        """Full purge fix: Marks playlist cancelled, wipes pending items & active jobs, cleans disk."""
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('UPDATE playlists SET status = "cancelled" WHERE id = ?', (pl_id,))
                conn.execute('DELETE FROM playlist_items WHERE playlist_id = ? AND status = "pending"', (pl_id,))
                conn.row_factory = sqlite3.Row
                active_jobs = [dict(r) for r in conn.execute('SELECT id FROM jobs WHERE playlist_id = ?', (pl_id,)).fetchall()]
                conn.execute('DELETE FROM jobs WHERE playlist_id = ?', (pl_id,))

        for j in active_jobs:
            shutil.rmtree(JOBS_DIR / f"JOB_{j['id']}", ignore_errors=True)

    async def get_pending_items(self, pl_id: str, limit: int = 2) -> list[dict]:
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                return [dict(r) for r in conn.execute('SELECT * FROM playlist_items WHERE playlist_id = ? AND status = "pending" LIMIT ?', (pl_id, limit)).fetchall()]

    async def update_item_status(self, item_id: str, status: str):
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('UPDATE playlist_items SET status = ? WHERE id = ?', (status, item_id))

    async def create_job(self, data: dict):
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('INSERT INTO jobs VALUES (?, ?, ?, ?, "queued", 0.0, 0, ?, NULL)',
                             (data['id'], data['url'], data['title'], data['playlist_id'], data['chat_id']))
        root = JOBS_DIR / f"JOB_{data['id']}"
        for d in (root, root / "dl", root / "enc"): d.mkdir(parents=True, exist_ok=True)

    async def update_job(self, jid: str, **kwargs):
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                for k, v in kwargs.items():
                    conn.execute(f'UPDATE jobs SET {k} = ? WHERE id = ?', (v, jid))

    async def delete_job(self, jid: str):
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('DELETE FROM jobs WHERE id = ?', (jid,))

    async def get_active_jobs(self) -> list[dict]:
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                return [dict(r) for r in conn.execute('SELECT * FROM jobs').fetchall()]

    async def get_job(self, jid: str) -> dict:
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute('SELECT * FROM jobs WHERE id = ?', (jid,)).fetchone()
                return dict(row) if row else {}

# ──────────────────────────── DRIP-FEED ORCHESTRATOR ───────────────────

async def playlist_drip_feed_loop(db: JobScheduler, dl_q: asyncio.Queue):
    """Monitors active playlists and maintains 3 concurrent downloads at all times."""
    while True:
        await asyncio.sleep(3)
        try:
            active_playlists = await db.get_active_playlists()
            active_jobs = await db.get_active_jobs()

            for pl in active_playlists:
                if pl['status'] in ['paused', 'cancelled', 'completed']:
                    continue

                pl_id = pl['id']
                
                # Count ONLY jobs currently in queued or downloading stage for this playlist
                downloading_jobs = [
                    j for j in active_jobs 
                    if j.get('playlist_id') == pl_id and (
                        not j.get('stage') or 
                        'queued' in j.get('stage', '').lower() or 
                        'download' in j.get('stage', '').lower()
                    )
                ]
                current_dl_count = len(downloading_jobs)

                TARGET_CONCURRENT_DOWNLOADS = 3
                if current_dl_count < TARGET_CONCURRENT_DOWNLOADS:
                    slots_free = TARGET_CONCURRENT_DOWNLOADS - current_dl_count
                    pending_items = await db.get_pending_items(pl_id, limit=slots_free)

                    for item in pending_items:
                        jid = item['id']
                        await db.create_job({
                            'id': jid, 'url': item['url'], 'title': item['title'],
                            'playlist_id': pl_id, 'chat_id': pl['chat_id']
                        })
                        await db.update_item_status(jid, "processing")
                        await dl_q.put(jid)
        except Exception as e:
            log.error(f"Drip Feed Loop Error: {e}")
            
class AriaLogger:
    def __init__(self, jid: str, db: JobScheduler):
        self.jid = jid
        self.db = db
        self.last_up = 0

    def debug(self, msg): self._process(msg)
    def info(self, msg): self._process(msg)
    def warning(self, msg): pass
    def error(self, msg): pass

    def _process(self, msg):
        # Strip ANSI codes & clean formatting
        clean_msg = re.sub(r"\x1b[^m]*m", "", str(msg)).strip()
        
        if "DL:" in clean_msg and "ETA:" in clean_msg and "%" in clean_msg:
            try:
                pct_m = re.search(r"\((\d+)%\)", clean_msg)
                speed_m = re.search(r"DL:([^\s]+)", clean_msg)
                eta_m = re.search(r"ETA:([^\]\s]+)", clean_msg)
                
                if pct_m and speed_m and eta_m:
                    pct = float(pct_m.group(1))
                    speed = speed_m.group(1)
                    if not speed.endswith("/s"): speed += "/s"
                    eta = eta_m.group(1)
                    
                    now = time.time()
                    if now - self.last_up >= 1.0:
                        global _live_ui_text
                        _live_ui_text[self.jid] = f"[aria2] {pct:.1f}% at {speed} ETA {eta}"
                        
                        stage_str = f"downloading | {speed} | {eta}"
                        
                        # Dynamically acquire active running event loop
                        try:
                            active_loop = asyncio.get_running_loop()
                        except RuntimeError:
                            active_loop = loop
                            
                        asyncio.run_coroutine_threadsafe(
                            self.db.update_job(self.jid, pct=pct, stage=stage_str), active_loop
                        )
                        self.last_up = now
            except Exception:
                pass

# ──────────────────────────── PIPELINE ENGINES ─────────────────────────

class DownloaderEngine:
    def __init__(self, db: JobScheduler, app: Client):
        self.db = db
        self.app = app

    def _extract_vk_api(self, url: str, jid: str) -> str | None:
        """Ghost Protocol: Direct CDN extraction without cookies."""
        VK_TOKEN = getattr(config, "VK_TOKEN", None)
        if not VK_TOKEN: return None

        try:
            import vk_api
            vk_session = vk_api.VkApi(token=VK_TOKEN)
            vk = vk_session.get_api()
            
            video_id = None
            video_match = re.search(r'video(-?\d+_\d+)', url)
            if video_match:
                video_id = video_match.group(1)

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

    async def execute(self, job: dict):
        jid, original_url = job['id'], job['url']
        chat_id = job.get('chat_id')
        dl_dir = JOBS_DIR / f"JOB_{jid}" / "dl"

        # Immediately update status to downloading so UI doesn't linger on 'queued'
        await self.db.update_job(jid, stage="downloading | ~ | ~")

        # Initialize Individual Job Card for tracking
        if chat_id:
            try:
                card_text = _job_tracker_text(job)
                m = await self.app.send_message(chat_id, card_text)
                await self.db.update_job(jid, tracker_id=m.id)
                job['tracker_id'] = m.id
            except Exception as e:
                self.db.log_trace(jid, f"Failed to deploy tracker card: {e}")

        self.db.log_trace(jid, "Querying VK API backend for direct CDN payload...")
        extracted_cdn = await asyncio.to_thread(self._extract_vk_api, original_url, jid)
        
        target_url = extracted_cdn if extracted_cdn else original_url
        if extracted_cdn:
            self.db.log_trace(jid, "Target bypassed! Proceeding with cookieless direct CDN stream.")

        last_db_update = 0

        def prog_hook(d):
            nonlocal last_db_update
            if d.get("status") == "downloading":
                try:
                    pct_str = re.sub(r"\x1b[^m]*m", "", d.get("_percent_str", "0.0%")).strip()
                    speed = re.sub(r"\x1b[^m]*m", "", d.get("_speed_str", "~")).strip()
                    eta = re.sub(r"\x1b[^m]*m", "", d.get("_eta_str", "~")).strip()
                    tot_str = re.sub(r"\x1b[^m]*m", "", d.get("_total_bytes_str", d.get("_total_bytes_estimate_str", "~"))).strip()
                    
                    val = float(re.search(r"[\d.]+", pct_str).group()) if re.search(r"[\d.]+", pct_str) else 0.0
                    
                    global _live_ui_text
                    _live_ui_text[jid] = f"[native] {pct_str} of {tot_str} at {speed} ETA {eta}"

                    current_time = time.time()
                    if current_time - last_db_update >= 1.0:
                        stage_str = f"downloading | {speed} | {eta}"
                        try:
                            active_loop = asyncio.get_running_loop()
                        except RuntimeError:
                            active_loop = loop
                        asyncio.run_coroutine_threadsafe(self.db.update_job(jid, pct=val, stage=stage_str), active_loop)
                        last_db_update = current_time
                except Exception: pass

        opts = {
            "outtmpl": str(dl_dir / f"{jid}.%(ext)s"),
            "format": "bestvideo[height<=1080]+bestaudio/best",
            "merge_output_format": "mp4",
            "progress_hooks": [prog_hook],
            "quiet": False,
            "noprogress": True,
            "no_warnings": True,
            "compat_opts": {"allow-unsafe-ext"},
            
            "external_downloader": "aria2c",
            "external_downloader_args": [
                "-c", "-j", "16", "-x", "16", "-s", "16", "-k", "5M",
                "--summary-interval=1", 
                "--connect-timeout=15", "--timeout=15", "--max-tries=5"
            ],
            "logger": AriaLogger(jid, self.db)
        }

        # Dynamic CDN Signature Spoofing
        custom_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        if "srcAg=GECKO" in target_url:
            custom_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"
        elif "srcAg=SAFARI" in target_url:
            custom_ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15"

        if "http_headers" not in opts:
            opts["http_headers"] = {}
        opts["http_headers"]["User-Agent"] = custom_ua

        if "impersonate" in opts and ("srcAg=" in target_url):
            del opts["impersonate"]

        await asyncio.to_thread(self._run_ytdlp, target_url, jid, opts)

    def _run_ytdlp(self, url: str, jid: str, base_opts: dict):
        opts = base_opts.copy()
        opts["quiet"] = True
        opts["noprogress"] = True
        
        # Increase fragments to 15-20 for faster segmented streaming (HLS)
        opts["concurrent_fragment_downloads"] = 15 
        
        # Optionally, remove or increase chunk size to let yt-dlp optimize itself
        # 10485760 is 10MB. We can increase to 20MB (20971520)
        opts["http_chunk_size"] = 20971520

        self.db.log_trace(jid, "Executing native multi-threaded downloader...")
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.extract_info(url, download=True)
        except Exception as e:
            self.db.log_trace(jid, f"Native Download Error: {e}")
            raise e

class EncoderEngine:
    async def execute(self, job: dict):
        jid = job['id']
        dl_dir, enc_dir = JOBS_DIR / f"JOB_{jid}" / "dl", JOBS_DIR / f"JOB_{jid}" / "enc"
        files = [f for f in dl_dir.rglob("*") if f.is_file() and f.suffix.lower() in [".mp4", ".mkv", ".ts", ".webm"]]
        if not files: raise RuntimeError("No downloaded media found.")

        src = max(files, key=lambda p: p.stat().st_size)
        dst = enc_dir / f"{jid}.mp4"

        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-i", str(src), "-c", "copy", "-movflags", "+faststart", str(dst),
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        await proc.wait()

class UploaderEngine:
    def __init__(self, db: JobScheduler, app: Client):
        self.db = db
        self.app = app

    async def execute(self, job: dict):
        jid = job['id']
        enc_file = JOBS_DIR / f"JOB_{jid}" / "enc" / f"{jid}.mp4"
        if not enc_file.exists(): raise RuntimeError("Encoded payload missing.")

        pl = await self.db.get_playlist(job['playlist_id'])
        caption = f"{pl['caption']}\n\n**{job['title']}**" if pl and pl.get('caption') else f"**{job['title']}**"

        last_db_up = 0

        async def upload_progress(current, total):
            nonlocal last_db_up
            now = time.time()
            if now - last_db_up >= 1.5:
                pct = (current / total) * 100 if total else 0.0
                curr_mb = current / (1024 * 1024)
                tot_mb = total / (1024 * 1024)
                stage_str = f"uploading | {curr_mb:.1f}MB/{tot_mb:.1f}MB | —"
                
                global _live_ui_text
                _live_ui_text[jid] = f"[upload] {curr_mb:.1f}/{tot_mb:.1f} MB ({pct:.1f}%)"
                
                await self.db.update_job(jid, pct=pct, stage=stage_str)
                last_db_up = now

        # MTProto Pyrogram client handles files > 1 GB smoothly via API_ID + API_HASH
        await self.app.send_video(
            chat_id=CHANNEL_ID,
            video=str(enc_file),
            caption=caption,
            supports_streaming=True,
            progress=upload_progress
        )

        global _last_completed
        _last_completed = job['title']

        if pl:
            new_count = pl['downloaded'] + 1
            status = "completed" if new_count >= pl['total'] else pl['status']
            await self.db.update_playlist(pl['id'], downloaded=new_count, status=status)
            await self.db.update_item_status(jid, "done")

        # 5. Final UI Freeze & Cleanup in the tracking chat
        try:
            latest_job = await self.db.get_job(jid)
            if latest_job and latest_job.get('tracker_id'):
                final_text = (
                    f"`[❖] ＴＡＳＫ :` `{latest_job['title'][:18]}..`\n"
                    f"`━━━━━━━━━━━━━━━━━━━━━━━━━━`\n"
                    f"`✅ PHASE : COMPLETED`\n"
                    f"`📤 ROUTE : {CHANNEL_ID}`\n"
                    f"`━━━━━━━━━━━━━━━━━━━━━━━━━━`"
                )
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("🗑️ DISMISS", callback_data=f"delmsg|{latest_job['tracker_id']}")]])
                await safe_edit(self.app, latest_job['chat_id'], latest_job['tracker_id'], final_text, kb)
        except Exception as e:
            self.db.log_trace(jid, f"Failed to push final completion card: {e}")

        await self.db.delete_job(jid)
        shutil.rmtree(JOBS_DIR / f"JOB_{jid}", ignore_errors=True)

# ──────────────────────────── DASHBOARD & ROUTER ───────────────────────

async def safe_edit(app: Client, chat_id: int, msg_id: int, text: str, kb: InlineKeyboardMarkup | None):
    try: await app.edit_message_text(chat_id, msg_id, text, reply_markup=kb)
    except MessageNotModified: pass
    except FloodWait as e: await asyncio.sleep(e.value)
    except Exception: pass

async def render_dashboard(db: JobScheduler, tab: str = "playlists", exp_pl: str = None, exp_bucket: str = None, exp_jid: str = None) -> tuple[str, InlineKeyboardMarkup]:
    playlists = await db.get_active_playlists()
    active_jobs = await db.get_active_jobs()
    total_storage = sum(f.stat().st_size for f in JOBS_DIR.rglob("*") if f.is_file()) / (1024 ** 3)

    act_text_blocks = ["`[🔄] ACT  :`"]
    if not active_jobs:
        act_text_blocks = ["`[🔄] ACT  :` `SYSTEM IDLE`"]
    else:
        for i, j in enumerate(active_jobs[:7]):
            pct = float(j.get('pct', 0.0) or 0.0)
            stage_short = j.get('stage', '').split('|')[0].strip()[:4].upper()
            act_text_blocks.append(f"`  {chr(97+i)}. [{stage_short}] {j['title'][:12]}.. [{make_bar(pct, 8)}] {pct:.0f}%`")

    act_string = "\n".join(act_text_blocks)

    text = (
        f"💻 **VK PLAYLIST MAINFRAME**\n"
        f"`━━━━━━━━━━━━━━━━━━━━━━━━━━`\n"
        f"`[⚡] STAT :` `DRIP-FEED ACTIVE`\n"
        f"`[💾] DISK :` `{total_storage:.2f} GB`\n"
        f"{act_string}\n"
        f"`[🏁] LAST :` `{_last_completed[:12]}`\n"
        f"`━━━━━━━━━━━━━━━━━━━━━━━━━━`"
    )

    kb = []
    is_root_open = (tab == "playlists")
    kb.append([InlineKeyboardButton(f"{'[-]' if is_root_open else '[+]'} 🎵 ACTIVE PLAYLISTS ({len(playlists)})", callback_data=f"dash|{'root' if is_root_open else 'playlists'}")])

    def _base(stage_str): 
        if not stage_str: return "queued"
        return stage_str.split("|")[0].strip().lower()

    if is_root_open:
        if not playlists:
            kb.append([InlineKeyboardButton("└ System Idle (Send Link)", callback_data="noop")])
        else:
            for pl in playlists:
                pl_id = pl['id']
                is_this_pl_exp = (exp_pl == pl_id)
                pl_status_icon = "⏸" if pl['status'] == "paused" else "▶️"
                
                kb.append([InlineKeyboardButton(
                    f" {'[-]' if is_this_pl_exp else '[+]'} {pl_status_icon} {pl['caption'][:15] or 'Playlist'} [{pl['downloaded']}/{pl['total']}]", 
                    callback_data=f"dash|playlists:{pl_id}" if not is_this_pl_exp else "dash|playlists"
                )])
                
                if is_this_pl_exp:
                    pl_jobs = [j for j in active_jobs if j.get('playlist_id') == pl_id]
                    buckets = {
                        "dl": [j for j in pl_jobs if _base(j['stage']) in ["queued", "downloading"]],
                        "dl_done": [j for j in pl_jobs if _base(j['stage']) == "downloaded"],
                        "enc": [j for j in pl_jobs if _base(j['stage']) in ["encoding", "process"]],
                        "enc_done": [j for j in pl_jobs if _base(j['stage']) == "encoded"],
                        "up": [j for j in pl_jobs if _base(j['stage']) == "uploading"]
                    }

                    def build_bucket(bucket_id, label, icon, job_list):
                        is_b_open = (exp_bucket == bucket_id)
                        b_pref = "[-]" if is_b_open else "[+]"
                        kb.append([InlineKeyboardButton(f"    ├ {b_pref} {icon} {label} ({len(job_list)})", callback_data=f"dash|playlists:{pl_id}:{bucket_id}" if not is_b_open else f"dash|playlists:{pl_id}")])
                        
                        if is_b_open:
                            if not job_list: kb.append([InlineKeyboardButton("      └ Empty", callback_data="noop")])
                            for j in job_list:
                                jid = j['id']
                                is_j_open = (exp_jid == jid)
                                
                                if is_j_open:
                                    speed, eta = "—", "—"
                                    if "|" in j['stage']:
                                        p = [x.strip() for x in j['stage'].split("|")]
                                        if len(p) >= 3: speed, eta = p[1], p[2]
                                        elif len(p) == 2: speed = p[1]
                                    pct = float(j.get('pct', 0.0))
                                    bar = make_bar(pct, 8)
                                    
                                    kb.append([InlineKeyboardButton(f"🪪 ISOLATED JOB CARD: {jid}", callback_data="noop")])
                                    kb.append([InlineKeyboardButton(f"📁 {j['title'][:15]}...", callback_data="noop")])
                                    kb.append([InlineKeyboardButton(f"⚡ {speed}  |  ⏳ {eta}", callback_data="noop")])
                                    kb.append([InlineKeyboardButton(f"📊 [{bar}] {pct:.1f}%", callback_data="noop")])
                                    kb.append([
                                        InlineKeyboardButton("📄 LOGS", callback_data=f"joblog|{jid}"),
                                        InlineKeyboardButton("❌ KILL", callback_data=f"kill_job|{jid}")
                                    ])
                                    kb.append([InlineKeyboardButton("🔙 CLOSE CARD", callback_data=f"dash|playlists:{pl_id}:{bucket_id}")])
                                else:
                                    pct = float(j.get('pct', 0.0))
                                    kb.append([InlineKeyboardButton(f"      ├ ⚡ {j['title'][:10]}.. | {pct:.0f}%", callback_data=f"dash|playlists:{pl_id}:{bucket_id}:{jid}")])

                    build_bucket("dl", "DOWNLOADING", "📥", buckets["dl"])
                    build_bucket("dl_done", "WAITING PROC", "⏳", buckets["dl_done"])
                    build_bucket("enc", "PROCESSING", "⚙️", buckets["enc"])
                    build_bucket("enc_done", "WAITING UP", "⏳", buckets["enc_done"])
                    build_bucket("up", "UPLOADING", "📤", buckets["up"])

                    if pl['status'] == "paused":
                        kb.append([
                            InlineKeyboardButton("▶️ RESUME PLAYLIST", callback_data=f"res|{pl['id']}"),
                            InlineKeyboardButton("❌ CANCEL PLAYLIST", callback_data=f"kill|{pl['id']}")
                        ])
                    else:
                        kb.append([
                            InlineKeyboardButton("⏸ PAUSE PLAYLIST", callback_data=f"pause|{pl['id']}"),
                            InlineKeyboardButton("❌ CANCEL PLAYLIST", callback_data=f"kill|{pl['id']}")
                        ])
                    kb.append([InlineKeyboardButton("───────────────────", callback_data="noop")])

    kb.append([InlineKeyboardButton("🔄 REFRESH SYSTEM", callback_data="refresh")])
    return text, InlineKeyboardMarkup(kb)

def setup_router(app: Client, db: JobScheduler, dl_q: asyncio.Queue, enc_q: asyncio.Queue, up_q: asyncio.Queue):

    @app.on_message(filters.text & filters.user(OWNER_ID) & ~filters.command(["vk_dash"]))
    async def auto_catch_playlist(_, msg: Message):
        text = msg.text.strip()
        if not ("http://" in text or "https://" in text): return
            
        parts = text.split("#", 1)
        url = parts[0].strip()
        caption = f"#{parts[1].strip()}" if len(parts) > 1 else ""
        
        url = re.sub(r'vk\.ru', 'vk.com', url, flags=re.IGNORECASE)
        m = await msg.reply("🔍 `Querying VK API for Playlist items...`")

        def extract_via_vk_api(playlist_url: str):
            VK_TOKEN = getattr(config, "VK_TOKEN", None)
            if not VK_TOKEN: return None

            try:
                import vk_api
                vk_session = vk_api.VkApi(token=VK_TOKEN)
                vk = vk_session.get_api()

                match = re.search(r'playlist/(-?\d+)_(\d+)', playlist_url)
                if not match: match = re.search(r'album_(-?\d+)_(\d+)', playlist_url)

                if match:
                    owner_id, album_id = int(match.group(1)), int(match.group(2))
                    all_videos = []
                    offset, count = 0, 100

                    while True:
                        res = vk.video.get(owner_id=owner_id, album_id=album_id, count=count, offset=offset)
                        items = res.get('items', [])
                        if not items: break

                        for v in items:
                            v_url = f"https://vk.com/video{v['owner_id']}_{v['id']}"
                            all_videos.append({'url': v_url, 'title': v.get('title', 'VK Video')})

                        offset += count
                        if offset >= res.get('count', 0): break
                    return all_videos
            except Exception as e:
                log.error(f"VK API Extraction failed: {e}")
            return None

        def extract_via_ytdlp(playlist_url: str):
            cookie_path = "vk_temp_cookies.txt"
            if VK_COOKIES:
                with open(cookie_path, "w", encoding="utf-8") as f:
                    f.write("# Netscape HTTP Cookie File\n")
                    for item in VK_COOKIES.strip().split(';'):
                        if '=' in item:
                            k, v = item.strip().split('=', 1)
                            f.write(f".vk.com\tTRUE\t/\tTRUE\t2147483647\t{k}\t{v}\n")
                            f.write(f".vkvideo.ru\tTRUE\t/\tTRUE\t2147483647\t{k}\t{v}\n")

            opts = {'extract_flat': True, 'quiet': True, 'cookiefile': cookie_path if VK_COOKIES else None}
            with yt_dlp.YoutubeDL(opts) as ydl:
                data = ydl.extract_info(playlist_url, download=False)
                entries = data.get('entries', [])
                items = []
                for e in entries:
                    v_url = e.get('url') or e.get('webpage_url')
                    if v_url: items.append({'url': v_url, 'title': e.get('title', 'VK Video')})
                return items

        try:
            entries = await asyncio.to_thread(extract_via_vk_api, url)
            if entries is None:
                entries = await asyncio.to_thread(extract_via_ytdlp, url)

            if not entries:
                return await m.edit("❌ No videos found in playlist link.")

            pl_id = str(uuid.uuid4())[:8]
            await db.create_playlist(pl_id, url, caption, len(entries), msg.chat.id)

            items = [(str(uuid.uuid4())[:8], pl_id, item['url'], item['title']) for item in entries]
            await db.add_playlist_items(items)
            await m.edit(f"✅ **PLAYLIST LOCKED**\nFound `{len(items)}` videos.\nDrip-feeding queued.")
            
            global _dash_msg_id, _dash_chat_id, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid
            if _dash_msg_id and _dash_chat_id:
                dash_text, kb = await render_dashboard(db, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid)
                await safe_edit(app, _dash_chat_id, _dash_msg_id, dash_text, kb)
                
        except Exception as e:
            await m.edit(f"❌ Extraction error: `{e}`")

    @app.on_message(filters.command(["vk_dash"]) & filters.user(OWNER_ID))
    async def cmd_dash(_, msg: Message):
        global _dash_msg_id, _dash_chat_id, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid
        text, kb = await render_dashboard(db, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid)
        m = await msg.reply(text, reply_markup=kb)
        _dash_msg_id, _dash_chat_id = m.id, m.chat.id

    @app.on_callback_query()
    async def handle_callbacks(_, cb: CallbackQuery):
        global _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid
        
        if cb.data == "noop": return await cb.answer()

        if cb.data.startswith("delmsg|"):
            msg_id = int(cb.data.split("|")[1])
            try:
                await app.delete_messages(cb.message.chat.id, msg_id)
                await cb.answer("Task card dismissed.")
            except Exception:
                await cb.answer("Failed to delete.", show_alert=True)
            return

        elif cb.data.startswith("dash|"):
            parts = cb.data.split("|")[1].split(":")
            _dash_tab = parts[0]
            _expanded_pl = parts[1] if len(parts) > 1 else None
            _expanded_bucket = parts[2] if len(parts) > 2 else None
            _expanded_jid = parts[3] if len(parts) > 3 else None
            await cb.answer()

        elif cb.data == "refresh":
            await cb.answer("Refreshed.")

        elif cb.data.startswith("joblog|"):
            jid = cb.data.split("|")[1]
            log_path = JOBS_DIR / f"JOB_{jid}" / "trace.log"
            if not log_path.exists(): return await cb.answer("No logs found.", show_alert=True)
            with open(log_path, "r", encoding="utf-8") as f:
                lines = f.read().splitlines()
                recent = "\n".join(lines[-15:]) if lines else "No data."
            return await cb.answer(f"--- TRACE LOGS ---\n{recent}", show_alert=True)

        elif cb.data.startswith("kill_job|"):
            jid = cb.data.split("|")[1]
            await db.delete_job(jid)
            shutil.rmtree(JOBS_DIR / f"JOB_{jid}", ignore_errors=True)
            _expanded_jid = None 
            await cb.answer("Task terminated.", show_alert=True)

        elif cb.data.startswith("pause|"):
            pl_id = cb.data.split("|")[1]
            await db.update_playlist(pl_id, status="paused")
            _expanded_pl = pl_id
            await cb.answer("⏸ Playlist Paused. Queue held.", show_alert=True)

        elif cb.data.startswith("res|"):
            pl_id = cb.data.split("|")[1]
            await db.update_playlist(pl_id, status="active")
            _expanded_pl = pl_id
            await cb.answer("▶️ Playlist Resumed.", show_alert=True)

        elif cb.data.startswith("kill|"):
            pl_id = cb.data.split("|")[1]
            await db.cancel_playlist(pl_id)
            _expanded_pl = None
            await cb.answer("❌ Playlist Terminated & Wiped.", show_alert=True)

        text, kb = await render_dashboard(db, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid)
        await safe_edit(app, cb.message.chat.id, cb.message.id, text, kb)

# ──────────────────────────── WORKER & TERMUX LOOPS ────────────────────

async def terminal_loop(db: JobScheduler, dl_q: asyncio.Queue, enc_q: asyncio.Queue, up_q: asyncio.Queue):
    sys.stdout.write("\033[2J") 
    while True:
        await asyncio.sleep(2) 
        sys.stdout.write("\033[H") 
        sys.stdout.write(f"{C_CYAN}{C_BOLD}=== VK MAINFRAME [LIVE] ==={C_RESET}\n")
        sys.stdout.write(f"QUEUES | DL: {dl_q.qsize()} | ENC: {enc_q.qsize()} | UP: {up_q.qsize()}\n{'─' * 40}\n")
        
        jobs = await db.get_active_jobs()
        if not jobs: 
            sys.stdout.write(f"{C_GREEN}System Idle. Awaiting playlist vectors.{C_RESET}\033[K\n")
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
        
        sys.stdout.write("\033[J") 
        sys.stdout.flush()

async def worker_pipeline(db: JobScheduler, dl_q: asyncio.Queue, enc_q: asyncio.Queue, up_q: asyncio.Queue, app: Client):
    dl_engine = DownloaderEngine(db, app)
    enc_engine = EncoderEngine()
    up_engine = UploaderEngine(db, app)

    async def dl_worker():
        while True:
            jid = await dl_q.get()
            jobs = await db.get_active_jobs()
            j_data = next((j for j in jobs if j['id'] == jid), None)
            if j_data:
                try:
                    await dl_engine.execute(j_data)
                    await db.update_job(jid, stage="downloaded")
                    await enc_q.put(jid)
                except Exception as e:
                    db.log_trace(jid, f"DL Error: {e}")
                    await db.delete_job(jid)
            dl_q.task_done()

    async def enc_worker():
        while True:
            jid = await enc_q.get()
            jobs = await db.get_active_jobs()
            j_data = next((j for j in jobs if j['id'] == jid), None)
            if j_data:
                try:
                    await db.update_job(jid, stage="encoding")
                    await enc_engine.execute(j_data)
                    await db.update_job(jid, stage="encoded")
                    await up_q.put(jid)
                except Exception as e:
                    db.log_trace(jid, f"Enc Error: {e}")
                    await db.delete_job(jid)
            enc_q.task_done()

    async def up_worker():
        while True:
            jid = await up_q.get()
            jobs = await db.get_active_jobs()
            j_data = next((j for j in jobs if j['id'] == jid), None)
            if j_data:
                try:
                    await db.update_job(jid, stage="uploading")
                    await up_engine.execute(j_data)
                except Exception as e:
                    db.log_trace(jid, f"UP Error: {e}")
                    await db.delete_job(jid)
            up_q.task_done()

    for _ in range(3): asyncio.create_task(dl_worker())
    asyncio.create_task(enc_worker())
    asyncio.create_task(up_worker())

# ──────────────────────────── BOOTSTRAP & REFRESHER ────────────────────

async def dashboard_refresher(app: Client, db: JobScheduler):
    global _dash_msg_id, _dash_chat_id, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid
    last_state_hash = {}

    while True:
        await asyncio.sleep(2)
        try:
            jobs = await db.get_active_jobs()
            playlists = await db.get_active_playlists()
            
            needs_update = False
            current_hash = {}
            jobs_to_update = []

            pl_summary = str([(p['id'], p['status'], p['downloaded']) for p in playlists])
            if last_state_hash.get("playlists") != pl_summary:
                needs_update = True
                last_state_hash["playlists"] = pl_summary

            for j in jobs:
                jid = j['id']
                stage_base = j['stage'].split('|')[0].strip() if j['stage'] else ""
                pct = float(j.get('pct', 0.0))
                pct_bucket = int(pct // 10) * 10 

                state_str = f"{stage_base}_{pct_bucket}"
                current_hash[jid] = state_str

                if last_state_hash.get(jid) != state_str:
                    needs_update = True
                    jobs_to_update.append(j)

            active_jids = set(current_hash.keys())
            known_jids = set(k for k in last_state_hash.keys() if k != "playlists")
            if active_jids != known_jids:
                needs_update = True

            # Update individual tracker cards for active jobs
            for j in jobs_to_update:
                if j.get('tracker_id') and j.get('chat_id'):
                    try:
                        card_text = _job_tracker_text(j)
                        await safe_edit(app, j['chat_id'], j['tracker_id'], card_text, None)
                    except Exception:
                        pass

            if _dash_msg_id and _dash_chat_id and needs_update:
                text, kb = await render_dashboard(db, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid)
                await safe_edit(app, _dash_chat_id, _dash_msg_id, text, kb)
                
                for k in list(last_state_hash.keys()):
                    if k != "playlists" and k not in current_hash:
                        del last_state_hash[k]
                for k, v in current_hash.items():
                    last_state_hash[k] = v

        except Exception:
            pass

async def main():
    app = Client("vk_stealth_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
    db = JobScheduler(DB_PATH)

    dl_q, enc_q, up_q = asyncio.Queue(), asyncio.Queue(), asyncio.Queue()
    setup_router(app, db, dl_q, enc_q, up_q)

    async with app:
        log.info("VK Playlist Bot Online via MTProto.")
        asyncio.create_task(playlist_drip_feed_loop(db, dl_q))
        asyncio.create_task(worker_pipeline(db, dl_q, enc_q, up_q, app))
        asyncio.create_task(terminal_loop(db, dl_q, enc_q, up_q))
        asyncio.create_task(dashboard_refresher(app, db))

        if OWNER_ID:
            global _dash_msg_id, _dash_chat_id, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid
            text, kb = await render_dashboard(db, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid)
            m = await app.send_message(OWNER_ID, text, reply_markup=kb)
            _dash_msg_id, _dash_chat_id = m.id, m.chat.id
            
            try:
                await app.unpin_all_chat_messages(m.chat.id)
                await m.pin(disable_notification=True, both_sides=True)
            except Exception as e:
                log.error(f"Failed to pin dashboard: {e}")

        while True:
            await asyncio.sleep(3600)

if __name__ == "__main__":
    try: loop.run_until_complete(main())
    except KeyboardInterrupt: sys.exit(0)