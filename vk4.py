"""
vk_bot.py - ULTIMATE MAINFRAME EDITION
───────────────────────────────────────────────────────────────
ARCHITECTURE:
  • Single-file Micro-Orchestration (Classes).
  • JobScheduler (SQLite + asyncio.Lock) with Playlist Tracking.
  • Aria2c (16-conn) + Dynamic CDN Spoofing (Ghost Protocol).
  • High-Speed Memory Bridge for Termux UI.
  • Full Accordion Dashboard & ANSI Logger.
  • Auto-Ejecting Recovery Pool + Disk-Aware Backpressure.
  • FFprobe Metadata & Channel Uploader (Streaming Supported).
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
from enum import Enum
from pathlib import Path
import yt_dlp
import aiohttp
import random
from yt_dlp.networking.impersonate import ImpersonateTarget
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait, MessageNotModified
from logging.handlers import RotatingFileHandler
import config

try:
    loop = asyncio.get_running_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

# ──────────────────────────── CONFIGURATION ─────────────────────────────

BASE_DIR = Path("SysCache_VK")
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
logging.getLogger().handlers[1].setLevel(logging.CRITICAL)
log = logging.getLogger("stealth_bot")
logging.getLogger("pyrogram").setLevel(logging.ERROR)

API_ID, API_HASH, BOT_TOKEN, CHANNEL_ID = config.API_ID, config.API_HASH, config.BOT_TOKEN, config.CHANNEL_ID
OWNER_ID = int(config.OWNER_ID) if hasattr(config, "OWNER_ID") else 0

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

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

JOBS_DIR, DONE_DIR = BASE_DIR / "jobs", BASE_DIR / "completed"
for d in (JOBS_DIR, DONE_DIR): d.mkdir(parents=True, exist_ok=True)

MAX_DL_WORKERS, MAX_RETRIES = 3, 3
MIN_FREE_DISK_GB = 8.0  
MAX_ACTIVE_PHYSICAL_JOBS = 5  

_batch_mode = False
_batch_collection = []
_current_batch_name = None
_pending_batches = asyncio.Queue()

C_CYAN, C_YELLOW, C_RED, C_GREEN, C_RESET, C_BOLD = "\033[36m", "\033[33m", "\033[31m", "\033[32m", "\033[0m", "\033[1m"
_live_ui_text = {}

def make_bar(percent: float, width: int = 10) -> str:
    filled = int(max(0.0, min(percent, 100.0)) / (100.0 / width))
    return "█" * filled + "░" * (width - filled)

def get_video_metadata(video_path: str):
    """Extracts width, height, duration using ffprobe and generates a thumbnail frame."""
    cmd = ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_streams", "-show_format", video_path]
    res = subprocess.run(cmd, capture_output=True, text=True)
    data = json.loads(res.stdout) if res.returncode == 0 else {}

    width, height, duration = 1280, 720, 0
    for stream in data.get("streams", []):
        if stream.get("codec_type") == "video":
            width = int(stream.get("width", 1280))
            height = int(stream.get("height", 720))
            break
            
    if "format" in data and "duration" in data["format"]:
        duration = int(float(data["format"]["duration"]))

    thumb_path = f"{video_path}_thumb.jpg"
    thumb_cmd = ["ffmpeg", "-y", "-ss", "00:00:02", "-i", video_path, "-vframes", "1", "-q:v", "2", "-vf", "scale=1280:-1", thumb_path]
    subprocess.run(thumb_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    return {"width": width, "height": height, "duration": duration, "thumb": thumb_path if Path(thumb_path).exists() else None}

# ──────────────────────────── SUBSYSTEM 1: DATABASE ─────────────────────

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
                recovered_at_stage TEXT DEFAULT NULL, playlist_id TEXT DEFAULT NULL, item_num INTEGER DEFAULT 0
            )''')
            conn.execute('''CREATE TABLE IF NOT EXISTS playlists (
                id TEXT PRIMARY KEY, title TEXT, total_items INTEGER, completed_items INTEGER DEFAULT 0, status TEXT, chat_id INTEGER
            )''')
            try: conn.execute('ALTER TABLE jobs ADD COLUMN recovered_at_stage TEXT DEFAULT NULL')
            except sqlite3.OperationalError: pass
            try: conn.execute('ALTER TABLE jobs ADD COLUMN playlist_id TEXT DEFAULT NULL')
            except sqlite3.OperationalError: pass
            try: conn.execute('ALTER TABLE jobs ADD COLUMN item_num INTEGER DEFAULT 0')
            except sqlite3.OperationalError: pass

    async def create_playlist(self, data: dict):
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''INSERT OR REPLACE INTO playlists (id, title, total_items, status, chat_id)
                                VALUES (?, ?, ?, ?, ?)''', (data['id'], data['title'], data['total_items'], 'active', data['chat_id']))

    async def get_active_playlists(self) -> list[dict]:
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                return [dict(r) for r in conn.execute("SELECT * FROM playlists WHERE status = 'active'").fetchall()]

    async def increment_playlist_progress(self, pl_id: str):
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("UPDATE playlists SET completed_items = completed_items + 1 WHERE id = ?", (pl_id,))

    async def create_job(self, data: dict):
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''INSERT INTO jobs (id, url, title, source, quality, strategy, stage, pct, last_ui_pct, retries, chat_id, tracker_id, playlist_id, item_num)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
                             (data['id'], data['url'], data['title'], data.get('source', 'Direct'), data.get('quality', 'auto'), data.get('strategy', 'GENERIC'), 
                              Stage.QUEUED.value, 0.0, -10.0, 0, data['chat_id'], data.get('tracker_id'), data.get('playlist_id'), data.get('item_num', 0)))
                
        root = JOBS_DIR / f"JOB_{data['id']}"
        for d in (root, root / "dl", root / "enc", root / "thumb"): d.mkdir(parents=True, exist_ok=True)

    async def update_job(self, jid: str, **kwargs):
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                for k, v in kwargs.items(): conn.execute(f'UPDATE jobs SET {k} = ? WHERE id = ?', (v, jid))

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

    async def get_pending_items(self, pl_id: str, limit: int) -> list[dict]:
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                return [dict(r) for r in conn.execute("SELECT * FROM jobs WHERE playlist_id = ? AND stage = 'queued' ORDER BY item_num ASC LIMIT ?", (pl_id, limit)).fetchall()]

    async def delete_job(self, jid: str):
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('DELETE FROM jobs WHERE id = ?', (jid,))

    def log_trace(self, jid: str, msg: str):
        with open(JOBS_DIR / f"JOB_{jid}" / "trace.log", "a", encoding="utf-8") as f:
            f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")

# ──────────────────────────── SUBSYSTEM 2: RESOLVER ─────────────────────

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

async def is_proxy_working(proxy_url: str) -> bool:
    test_url = "http://httpbin.org/ip"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(test_url, proxy=proxy_url, timeout=5) as response:
                return response.status == 200
    except Exception: return False

async def get_random_free_proxy() -> str:
    url = "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    text = await response.text()
                    proxies = [line.strip() for line in text.split('\n') if line.strip()]
                    if proxies:
                        random.shuffle(proxies)
                        for p in proxies[:10]:
                            formatted_proxy = p if p.startswith("http") else f"http://{p}"
                            if await is_proxy_working(formatted_proxy): return formatted_proxy
    except Exception: pass
    return ""

# ──────────────────────────── SUBSYSTEM 3: ENGINES ──────────────────────

class AriaLogger:
    def __init__(self, jid: str, db):
        self.jid = jid
        self.db = db
        self.last_up = 0

    def debug(self, msg): self._process(msg)
    def info(self, msg): self._process(msg)
    def warning(self, msg): pass
    def error(self, msg): pass

    def _process(self, msg):
        clean_msg = re.sub(r"\x1b[^m]*m", "", str(msg)).strip()
        pattern = r"([\d\.]+[KMG]iB)/([\d\.]+[KMG]iB)\((\d+)%\).*?DL:([\d\.]+[KMG]iB)(?:/s)?.*?ETA:(.*)"
        match = re.search(pattern, clean_msg)
        
        if match:
            downloaded, total, pct, speed, eta = match.groups()
            eta = eta.split("]")[0].strip()
            val = float(pct)
            speed_str = f"{speed}/s" if not speed.endswith("/s") else speed
            
            now = time.time()
            if now - self.last_up >= 1.0:
                global _live_ui_text
                _live_ui_text[self.jid] = f"[aria2] {val:.1f}% of {total} at {speed_str} ETA {eta}"
                stage_str = f"downloading | {speed_str} | {eta}"
                try: active_loop = asyncio.get_running_loop()
                except RuntimeError: active_loop = loop
                asyncio.run_coroutine_threadsafe(self.db.update_job(self.jid, pct=val, stage=stage_str), active_loop)
                self.last_up = now

class DownloaderEngine:
    def __init__(self, scheduler: JobScheduler, app: Client):
        self.db = scheduler
        self.app = app
        self.procs = {}

    def _extract_vk_api(self, url: str, jid: str) -> str | None:
        if not vk_api or not VK_TOKEN: return None
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
                if video_match: video_id = video_match.group(1)

            if video_id:
                vid_details = vk.video.get(videos=video_id)
                if vid_details and vid_details.get('items'):
                    files = vid_details['items'][0].get('files', {})
                    for q in ['mp4_1080', 'mp4_720', 'mp4_480', 'mp4_360', 'mp4_240', 'hls']:
                        if q in files: return files[q]
        except Exception as e:
            self.db.log_trace(jid, f"[vk_api] Ghost Protocol Failed: {e}")
        return None
            
    async def execute(self, job_data: dict):
        jid, original_url = job_data['id'], job_data['url']
        chat_id = job_data.get('chat_id')
        dl_dir = JOBS_DIR / f"JOB_{jid}" / "dl"

        await self.db.update_job(jid, stage="downloading | ~ | ~")
        
        extracted_cdn = await asyncio.to_thread(self._extract_vk_api, original_url, jid)
        target_url = extracted_cdn if extracted_cdn else original_url

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
                        try: active_loop = asyncio.get_running_loop()
                        except RuntimeError: active_loop = loop
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
            "external_downloader_args": {"aria2c": ["-c", "-j", "16", "-x", "16", "-s", "16", "-k", "5M", "--summary-interval=1"]},
            "logger": AriaLogger(jid, self.db)
        }

        custom_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        if "srcAg=GECKO" in target_url:
            custom_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"
        elif "srcAg=SAFARI" in target_url:
            custom_ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15"

        if "http_headers" not in opts: opts["http_headers"] = {}
        opts["http_headers"]["User-Agent"] = custom_ua

        if "impersonate" in opts and ("srcAg=" in target_url): del opts["impersonate"]

        await asyncio.to_thread(self._run_ytdlp, target_url, jid, opts, dl_dir)

    def _run_ytdlp(self, url, jid, opts, dl_dir):
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.extract_info(url, download=True)
            valid_files = [f for f in dl_dir.rglob("*") if f.is_file() and not f.name.endswith('.part')]
            if not valid_files: raise RuntimeError("yt-dlp completed but wrote no valid payload.")

class EncoderEngine:
    def __init__(self, scheduler: JobScheduler):
        self.db = scheduler

    async def execute(self, job_data: dict):
        jid = job_data['id']
        dl_dir, enc_dir, thumb_dir = JOBS_DIR / f"JOB_{jid}" / "dl", JOBS_DIR / f"JOB_{jid}" / "enc", JOBS_DIR / f"JOB_{jid}" / "thumb"
        
        dl_files = [f for f in dl_dir.rglob("*") if f.is_file() and not f.name.endswith('.part')]
        if not dl_files: raise RuntimeError("Encoder failed: No downloaded files found.")
            
        dl_file = max(dl_files, key=lambda p: p.stat().st_size)
        enc_file = enc_dir / f"{jid}.mp4"

        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-nostdin", "-fflags", "+genpts", "-i", str(dl_file), 
            "-c:v", "copy", "-c:a", "aac", "-avoid_negative_ts", "make_zero", "-movflags", "+faststart", 
            str(enc_file), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        try: await asyncio.wait_for(proc.wait(), timeout=900)
        except asyncio.TimeoutError:
            proc.kill()
            raise TimeoutError("FFmpeg Zombie Timeout")

class UploaderEngine:
    def __init__(self, db: JobScheduler, app: Client):
        self.db, self.app = db, app

    async def execute(self, job_data: dict):
        jid = job_data['id']
        job_dir = JOBS_DIR / f"JOB_{jid}"
        enc_dir, dl_dir = job_dir / "enc", job_dir / "dl"
        
        await self.db.update_job(jid, stage="uploading | metadata extraction", pct=0.0)

        target_file = None
        for d in [enc_dir, dl_dir]:
            if d.exists():
                files = [f for f in d.rglob("*") if f.is_file() and not f.name.endswith('.part')]
                if files:
                    target_file = sorted(files, key=lambda x: x.stat().st_size, reverse=True)[0]
                    break
                    
        if not target_file: raise RuntimeError("Uploader failed: No payload.")

        meta = await asyncio.to_thread(get_video_metadata, str(target_file))
        file_size_mb = target_file.stat().st_size / (1024 * 1024)
        duration_str = f"{meta['duration'] // 60}m {meta['duration'] % 60}s"

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

        title = job_data['title']
        playlist_id = job_data.get('playlist_id')
        
        caption = (f"🎬 **{title}**\n📏 **Resolution:** {meta['width']}x{meta['height']}\n"
                   f"⏱️ **Duration:** {duration_str}\n💾 **Size:** {file_size_mb:.2f} MB")

        await self.db.update_job(jid, stage="uploading | streaming payload")

        await self.app.send_video(
            chat_id=CHANNEL_ID, video=str(target_file), caption=caption,
            thumb=meta["thumb"], width=meta["width"], height=meta["height"],
            duration=meta["duration"], supports_streaming=True, progress=_up_prog
        )
        
        if meta["thumb"] and Path(meta["thumb"]).exists(): Path(meta["thumb"]).unlink()

        if playlist_id: await self.db.increment_playlist_progress(playlist_id)
        
        global _last_completed, _live_ui_text
        _last_completed = title
        _live_ui_text.pop(jid, None)
        await self.db.delete_job(jid)
        shutil.rmtree(job_dir, ignore_errors=True)

# ──────────────────────────── SUBSYSTEM 4: RECOVERY & LOGGING ─────────

class CrashCourier:
    @staticmethod
    async def push_fault(app: Client, db: JobScheduler, jid: str, exc: Exception):
        await db.update_job(jid, stage=Stage.FAILED.value)
        db.log_trace(jid, f"CRITICAL FAULT:\n{traceback.format_exc()}")
        global _live_ui_text
        _live_ui_text.pop(jid, None)

class RecoveryManager:
    @staticmethod
    async def recover_orphaned_jobs(db: JobScheduler, dl_q: asyncio.Queue, up_q: asyncio.Queue):
        print("🔄 [RECOVERY] Checking for orphaned or interrupted jobs...")
        active_jobs = await db.get_active_jobs()
        recovered_count = 0
        for job in active_jobs:
            jid, stage = job['id'], str(job.get('stage', '')).lower()
            if any(term in stage for term in ['completed', 'failed']): continue
            dl_dir = Path(f"SysCache_VK/jobs/JOB_{jid}/dl")
            enc_dir = Path(f"SysCache_VK/jobs/JOB_{jid}/enc")
            encoded_files = list(enc_dir.glob("*.mp4")) if enc_dir.exists() else []

            if 'upload' in stage or 'encode' in stage:
                if encoded_files and encoded_files[0].exists():
                    await db.update_job(jid, stage="encoded")
                    await up_q.put(jid)
                else:
                    await db.update_job(jid, stage="queued", pct=0.0)
                    await dl_q.put(jid)
                recovered_count += 1
            elif 'download' in stage or 'queued' in stage:
                if dl_dir.exists():
                    for partial in dl_dir.glob("*"):
                        try: partial.unlink()
                        except Exception: pass
                await db.update_job(jid, stage="queued", pct=0.0)
                await dl_q.put(jid)
                recovered_count += 1
        print(f"✅ [RECOVERY] Recovered {recovered_count} active jobs.")

# ──────────────────────────── PIPELINE MANAGER (Orchestrator) ───────────

class TelegramDispatcher:
    def __init__(self, app: Client):
        self.app = app
        self.edit_queue = asyncio.Queue()
        self.pending_edits = {}  
        self.lock = asyncio.Lock()
        self.tokens, self.last_refill, self.rate = 25.0, time.time(), 25.0

    async def _consume_token(self):
        now = time.time()
        self.tokens = min(30.0, self.tokens + (now - self.last_refill) * self.rate)
        self.last_refill = now
        if self.tokens < 1.0:
            await asyncio.sleep(1.0 / self.rate)
            await self._consume_token()
        else: self.tokens -= 1.0

    async def safe_edit_queued(self, chat_id: int, msg_id: int, text: str, kb: InlineKeyboardMarkup):
        async with self.lock:
            key = (chat_id, msg_id)
            is_new = key not in self.pending_edits
            self.pending_edits[key] = (text, kb)
        if is_new: await self.edit_queue.put(key)

    async def sender_loop(self):
        while True:
            key = await self.edit_queue.get()
            async with self.lock:
                if key not in self.pending_edits:
                    self.edit_queue.task_done()
                    continue
                text, kb = self.pending_edits.pop(key)
            retries, backoff = 0, 1
            while retries < 5:
                await self._consume_token()
                try:
                    await self.app.edit_message_text(key[0], key[1], text, reply_markup=kb)
                    await asyncio.sleep(1.0)
                    break
                except MessageNotModified: break
                except FloodWait as e:
                    await asyncio.sleep(e.value + backoff)
                    backoff *= 2; retries += 1
                except Exception: break
            self.edit_queue.task_done()

class PipelineManager:
    def __init__(self, app: Client, db: JobScheduler):
        self.app, self.db = app, db
        self.dl_q, self.enc_q, self.up_q = asyncio.Queue(), asyncio.Queue(), asyncio.Queue()
        self.dl_engine, self.enc_engine, self.up_engine = DownloaderEngine(db, app), EncoderEngine(db), UploaderEngine(db, app)

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
                await self.db.update_job(jid, stage=success_stage.value, retries=0)
                if next_q: await next_q.put(jid)
            except Exception as e:
                retry += 1
                if retry >= MAX_RETRIES: await CrashCourier.push_fault(self.app, self.db, jid, e)
                else: 
                    await self.db.update_job(jid, stage=job['stage'], retries=retry)
                    await queue.put(jid)
            finally: queue.task_done()

    def start_workers(self):
        for _ in range(MAX_DL_WORKERS): asyncio.create_task(self._worker_loop(self.dl_q, self.dl_engine, Stage.DOWNLOADING, Stage.DOWNLOADED, self.enc_q))
        asyncio.create_task(self._worker_loop(self.enc_q, self.enc_engine, Stage.ENCODING, Stage.ENCODED, self.up_q))
        asyncio.create_task(self._worker_loop(self.up_q, self.up_engine, Stage.UPLOADING, Stage.COMPLETED, None))

# ──────────────────────────── DISK-AWARE ORCHESTRATOR ───────────────────

async def playlist_drip_feed_loop(db: JobScheduler, dl_q: asyncio.Queue):
    """Monitors active playlists, maintains 3 concurrent DLs, and throttles disk space."""
    while True:
        await asyncio.sleep(3)
        try:
            total, used, free = shutil.disk_usage(BASE_DIR)
            free_gb = free / (1024 ** 3)
            active_jobs = await db.get_active_jobs()

            if free_gb < MIN_FREE_DISK_GB:
                global _live_ui_text
                _live_ui_text["SYSTEM"] = f"⚠️ DISK LOW ({free_gb:.1f} GB Free) - Throttled"
                continue

            if len(active_jobs) >= MAX_ACTIVE_PHYSICAL_JOBS:
                _live_ui_text["SYSTEM"] = f"⚠️ MAX JOBS ({len(active_jobs)}) - Waiting for Uploads"
                continue
            
            _live_ui_text.pop("SYSTEM", None)

            active_playlists = await db.get_active_playlists()
            for pl in active_playlists:
                pl_id, total_items, completed_items = pl['id'], pl['total_items'], pl.get('completed_items', 0)

                dl_count = len([j for j in active_jobs if j.get('playlist_id') == pl_id and ('download' in j.get('stage', '').lower() or 'queued' in j.get('stage', '').lower())])

                if dl_count < MAX_DL_WORKERS:
                    slots_free = MAX_DL_WORKERS - dl_count
                    pending_items = await db.get_pending_items(pl_id, limit=slots_free)

                    for item in pending_items:
                        jid = item['id']
                        await db.update_job(jid, stage="downloading") # Immediately un-queue to prevent duplicate pulls
                        await dl_q.put(jid)

        except Exception as e:
            log.error(f"Drip Feed Loop Error: {e}")

# ──────────────────────────── UI & DASHBOARD ──────────────────────────

_dashboard_msg_id, _dashboard_chat_id, _dashboard_tab = 0, 0, "root"
_last_completed = "—"

def _job_tracker_text(job: dict) -> str:
    title, status_raw = str(job.get('title', 'Unknown'))[:18], str(job.get('stage', 'PROCESSING')).upper()
    speed, eta = "—", "—"
    if "|" in status_raw:
        parts = [p.strip() for p in status_raw.split("|")]
        status_raw = parts[0]
        if len(parts) >= 3: speed, eta = parts[1], parts[2]
    pct = float(job.get('pct', 0.0) or 0.0)
    return f"`[❖] ＴＡＳＫ :` `{title}..`\n`⚙️ PHASE :` `{status_raw}`\n`⚡ SPEED :` `{speed}`\n`⏳ ETA   :` `{eta}`\n`📊 PROG  :` `[{make_bar(pct, 10)}] {pct:.1f}%`"

async def _get_dashboard_components(tab: str, db: JobScheduler, pipeline: PipelineManager):
    total_storage = sum(f.stat().st_size for f in JOBS_DIR.rglob("*") if f.is_file()) / (1024 ** 3)
    jobs = await db.get_active_jobs()
    active_pls = await db.get_active_playlists()
    
    stat_str = f"ONLINE & SECURE (Batches: {len(active_pls)})"
    if "SYSTEM" in _live_ui_text: stat_str = _live_ui_text["SYSTEM"]

    text = (f"💻 **MAINFRAME (VK EDITION)**\n`━━━━━━━━━━━━━━━━━━━━━━━━━━`\n"
            f"`[⚡] STAT :` `{stat_str}`\n`[💾] DISK :` `{total_storage:.2f} GB / 30 GB`\n"
            f"`[🏁] LAST :` `{_last_completed[:12]}`\n`━━━━━━━━━━━━━━━━━━━━━━━━━━`\n")

    kb_lines = []
    
    # Render Playlists/Batches dynamically
    if active_pls:
        kb_lines.append([InlineKeyboardButton("📦 ACTIVE PLAYLISTS", callback_data="noop")])
        for pl in active_pls:
            pl_title = pl['title'][:15]
            comp, tot = pl['completed_items'], pl['total_items']
            kb_lines.append([InlineKeyboardButton(f" └ 📁 {pl_title} [{comp}/{tot} Complete]", callback_data="noop")])
            pl_jobs = [j for j in jobs if j.get('playlist_id') == pl['id']]
            for j in pl_jobs[:3]:
                kb_lines.append([InlineKeyboardButton(f"   ├ ⚡ {j['title'][:15]}.. | {j.get('pct',0):.1f}%", callback_data=f"kill|{j['id']}")])

    kb_lines.append([InlineKeyboardButton("🔄 REFRESH SYSTEM", callback_data=f"dash|{tab}")])
    return text, InlineKeyboardMarkup(kb_lines)

class UIAccumulator:
    def __init__(self, db: JobScheduler, dispatcher: TelegramDispatcher, pipeline: PipelineManager):
        self.db, self.dispatcher, self.pipeline = db, dispatcher, pipeline
        self.last_stages, self.last_pcts, self.known_jids = {}, {}, set()

    async def run_loop(self):
        global _dashboard_msg_id, _dashboard_chat_id, _dashboard_tab
        while True:
            await asyncio.sleep(4)
            try:
                jobs = await self.db.get_active_jobs()
                current_jids = {j['id'] for j in jobs}
                dashboard_needs_update = (current_jids != self.known_jids) or ("SYSTEM" in _live_ui_text)
                self.known_jids = current_jids
                
                for job in jobs:
                    jid, raw_stage = job['id'], job['stage']
                    if not job.get('tracker_id'): continue
                    base_phase = raw_stage.split("|")[0].strip().lower() if "|" in raw_stage else raw_stage.strip().lower()
                    last_phase, last_pct = self.last_stages.get(jid, ""), self.last_pcts.get(jid, -10.0)
                    current_pct = float(job.get('pct', 0.0) or 0.0)
                    
                    if (base_phase != last_phase) or ((current_pct - last_pct) >= 10.0):
                        dashboard_needs_update = True
                        kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ KILL", callback_data=f"kill|{jid}")]])
                        await self.dispatcher.safe_edit_queued(job['chat_id'], job['tracker_id'], _job_tracker_text(job), kb)
                        self.last_stages[jid], self.last_pcts[jid] = base_phase, current_pct
                        
                if dashboard_needs_update and _dashboard_msg_id:
                    text, kb = await _get_dashboard_components(_dashboard_tab, self.db, self.pipeline)
                    await self.dispatcher.safe_edit_queued(_dashboard_chat_id, _dashboard_msg_id, text, kb)
            except Exception: pass

async def terminal_loop(db: JobScheduler, pipeline: PipelineManager):
    sys.stdout.write("\033[2J") 
    while True:
        await asyncio.sleep(1)
        sys.stdout.write("\033[H")
        sys.stdout.write(f"{C_CYAN}{C_BOLD}=== MAINFRAME [LIVE] ==={C_RESET}\n")
        jobs = [j for j in await db.get_active_jobs() if j.get("stage") and "completed" not in j["stage"]]
        for j in jobs:
            title = j['title'][:20]
            stage = _live_ui_text.get(j['id'], j.get("stage", "processing"))
            sys.stdout.write(f"[[{title}]] {stage}\033[K\n")
        sys.stdout.write("\033[J")
        sys.stdout.flush()

# ──────────────────────────── ROUTER & BOOTSTRAP ──────────────────────

def setup_router(app: Client, db: JobScheduler, pipeline: PipelineManager):
    @app.on_message(filters.command(["start", "dashboard"]) & filters.user(OWNER_ID))
    async def init_dashboard(_, msg: Message):
        global _dashboard_msg_id, _dashboard_chat_id
        m = await msg.reply("🟢 Booting VK Mainframe...")
        _dashboard_msg_id, _dashboard_chat_id = m.id, m.chat.id
        text, kb = await _get_dashboard_components("root", db, pipeline)
        await pipeline.app.edit_message_text(_dashboard_chat_id, _dashboard_msg_id, text, reply_markup=kb)

    @app.on_message(filters.command(["go"]) & filters.user(OWNER_ID))
    async def batch_go(_, msg: Message):
        global _batch_mode, _batch_collection, _current_batch_name
        _batch_mode = True
        _batch_collection = []
        args = msg.text.split(maxsplit=1)
        _current_batch_name = args[1].strip() if len(args) > 1 else f"PL_{str(uuid.uuid4())[:4]}"
        await msg.reply(f"🟢 **PLAYLIST MODE INITIATED**\n🏷️ Name: `{_current_batch_name}`\nPaste URLs one by one. Send `/end` to start drip-feed.")

    @app.on_message(filters.command(["end"]) & filters.user(OWNER_ID))
    async def batch_end(_, msg: Message):
        global _batch_mode, _batch_collection, _current_batch_name
        if not _batch_mode or not _batch_collection: return await msg.reply("⚠️ No links collected.")
        _batch_mode = False
        
        pl_id = str(uuid.uuid4())[:8]
        await db.create_playlist({'id': pl_id, 'title': _current_batch_name, 'total_items': len(_batch_collection), 'chat_id': msg.chat.id})
        
        for idx, (url, title, chat_id) in enumerate(_batch_collection, 1):
            jid = str(uuid.uuid4())[:8]
            tracker = await msg.reply(f"`[ ⚡ ] ＴＡＳＫ :` `[{idx}/{len(_batch_collection)}] {title[:20]}`\n`[ ⚙️ ] ＳＴＡＴ :` `QUEUED`", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ CANCEL", callback_data=f"kill|{jid}")]]))
            await db.create_job({"id": jid, "url": url, "title": f"[{idx}/{len(_batch_collection)}] {title}", "playlist_id": pl_id, "item_num": idx, "chat_id": chat_id, "tracker_id": tracker.id})
        
        await msg.reply(f"🚀 **PLAYLIST LOCKED**\n{len(_batch_collection)} items added to Drip Feeder limiters.")
        _batch_collection.clear()

    @app.on_message(filters.text & filters.user(OWNER_ID) & ~filters.command(["start", "dashboard", "go", "end"]))
    async def url_catcher(_, msg: Message):
        url = next((w for w in msg.text.split() if w.startswith("http")), None)
        if url:
            title = msg.text.replace(url, "").strip() or url[:40]
            global _batch_mode, _batch_collection
            if _batch_mode:
                _batch_collection.append((url, title, msg.chat.id))
                await msg.reply(f"✅ Added to Playlist. Total: {len(_batch_collection)}", quote=True)
            else:
                jid = str(uuid.uuid4())[:8]
                tracker = await msg.reply(f"`[ ⚡ ] ＴＡＳＫ :` `{title[:30]}`\n`[ ⚙️ ] ＳＴＡＴ :` `QUEUED`")
                await db.create_job({"id": jid, "url": url, "title": title, "chat_id": msg.chat.id, "tracker_id": tracker.id})
                await pipeline.dl_q.put(jid)

    @app.on_callback_query(filters.regex(r"^kill\|"))
    async def kill_job(_, cb: CallbackQuery):
        jid = cb.data.split("|")[1]
        await db.delete_job(jid)
        shutil.rmtree(JOBS_DIR / f"JOB_{jid}", ignore_errors=True)
        global _live_ui_text
        _live_ui_text.pop(jid, None)
        try: await cb.message.edit_text("💀 TERMINATED")
        except Exception: pass

async def main():
    app = Client("vk_bot_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
    db = JobScheduler(DB_PATH)
    pipeline = PipelineManager(app, db)
    dispatcher = TelegramDispatcher(app)
    ui_accumulator = UIAccumulator(db, dispatcher, pipeline)
    setup_router(app, db, pipeline)

    async with app:
        await RecoveryManager.recover_orphaned_jobs(db, pipeline.dl_q, pipeline.up_q)
        pipeline.start_workers()
        asyncio.create_task(dispatcher.sender_loop())
        asyncio.create_task(ui_accumulator.run_loop())
        asyncio.create_task(playlist_drip_feed_loop(db, pipeline.dl_q))
        asyncio.create_task(terminal_loop(db, pipeline))
        
        if OWNER_ID:
            m = await app.send_message(OWNER_ID, "🟢 VK Mainframe Systems Online.")
            global _dashboard_msg_id, _dashboard_chat_id
            _dashboard_msg_id, _dashboard_chat_id = m.id, m.chat.id
            text, kb = await _get_dashboard_components("root", db, pipeline)
            await dispatcher.safe_edit_queued(_dashboard_chat_id, _dashboard_msg_id, text, kb)

        while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    try: loop.run_until_complete(main())
    except KeyboardInterrupt: sys.exit(0)