"""
vk_bot.py - v9.0.1 (Ultimate VK Edition)
───────────────────────────────────────────────────────────────
ARCHITECTURE:
  • Single-file Micro-Orchestration for VK Links ONLY.
  • Dynamic CDN Spoofing & Aria2c 16-Connection Aggressive Pull.
  • FFprobe Metadata & High-Res Thumbnail Extraction.
  • Disk-Aware Backpressure (8GB limit / Max 6 physical jobs).
  • Full Accordion Dashboard & Batch Tracking [X/Y Complete].
  • Crash Recovery Subsystem.
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

# ──────────────────────────── CONFIGURATION ─────────────────────────────

BASE_DIR = Path("SysCache_VK")
LOG_DIR = BASE_DIR / "logs"
DB_PATH = BASE_DIR / "vk_scheduler.db"
for d in (LOG_DIR, BASE_DIR): d.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(name)s – %(message)s",
    handlers=[
        RotatingFileHandler(LOG_DIR / "engine.log", maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8"), 
        logging.StreamHandler()
    ]
)
logging.getLogger().handlers[1].setLevel(logging.CRITICAL)
log = logging.getLogger("vk_bot")
logging.getLogger("pyrogram").setLevel(logging.ERROR)

API_ID, API_HASH, CHANNEL_ID = config.API_ID, config.API_HASH, config.CHANNEL_ID
BOT_TOKEN = getattr(config, "VK_BOT_TOKEN", getattr(config, "BOT_TOKEN", None))
OWNER_ID = int(config.OWNER_ID) if hasattr(config, "OWNER_ID") else 0
VK_TOKEN = getattr(config, "VK_TOKEN", None)
VK_COOKIES = None
if os.path.exists("extracted_cookies.txt"):
    with open("extracted_cookies.txt", "r", encoding="utf-8") as f:
        VK_COOKIES = f.read().strip()

JOBS_DIR = BASE_DIR / "jobs"
JOBS_DIR.mkdir(parents=True, exist_ok=True)

# Architecture Limits
MAX_DL_WORKERS = 3         # Always keep 3 concurrent downloads running
MIN_FREE_DISK_GB = 8.0     # Pause downloads if disk space < 8GB
MAX_ACTIVE_PHYS_JOBS = 6   # Max jobs allowed on disk across DL/ENC/UP
MAX_RETRIES = 3

_batch_mode = False
_batch_collection = []
_current_batch_name = None
_live_ui_text = {}
_last_completed = "—"
_dashboard_msg_id, _dashboard_chat_id, _dashboard_tab = 0, 0, "root"

C_CYAN, C_YELLOW, C_RED, C_GREEN, C_RESET, C_BOLD = "\033[36m", "\033[33m", "\033[31m", "\033[32m", "\033[0m", "\033[1m"

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
                id TEXT PRIMARY KEY, url TEXT, title TEXT, source TEXT, 
                stage TEXT, pct REAL, last_ui_pct REAL, retries INTEGER, 
                chat_id INTEGER, tracker_id INTEGER, recovered_at_stage TEXT DEFAULT NULL
            )''')
            # Table to track batches
            conn.execute('''CREATE TABLE IF NOT EXISTS batches (
                name TEXT PRIMARY KEY, total_items INTEGER, completed_items INTEGER DEFAULT 0
            )''')

    async def create_job(self, data: dict):
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''INSERT INTO jobs (id, url, title, source, stage, pct, last_ui_pct, retries, chat_id, tracker_id)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
                             (data['id'], data['url'], data['title'], data['source'], 
                              Stage.QUEUED.value, 0.0, -10.0, 0, data['chat_id'], data.get('tracker_id')))
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

    async def register_batch(self, name: str, total: int):
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('INSERT OR IGNORE INTO batches (name, total_items, completed_items) VALUES (?, ?, ?)', (name, total, 0))

    async def increment_batch(self, name: str):
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('UPDATE batches SET completed_items = completed_items + 1 WHERE name = ?', (name,))

    async def get_batch_stats(self, name: str) -> tuple[int, int]:
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                row = conn.execute('SELECT total_items, completed_items FROM batches WHERE name = ?', (name,)).fetchone()
                return (row[0], row[1]) if row else (0, 0)

    def log_trace(self, jid: str, msg: str):
        with open(JOBS_DIR / f"JOB_{jid}" / "trace.log", "a", encoding="utf-8") as f:
            f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")

# ──────────────────────────── LOGGER & ENGINES ────────────────────────

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
    def __init__(self, scheduler: JobScheduler):
        self.db = scheduler

    def _extract_vk_api(self, url: str, jid: str) -> str | None:
        if not VK_TOKEN: return None
        try:
            import vk_api
            vk_session = vk_api.VkApi(token=VK_TOKEN)
            vk = vk_session.get_api()
            video_id = None
            wall_match = re.search(r'wall(-?\d+_\d+)', url)
            if wall_match:
                response = vk.wall.getById(posts=wall_match.group(1))
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
                    for q in ['mp4_1080', 'mp4_720', 'mp4_480', 'mp4_360', 'hls']:
                        if q in files:
                            self.db.log_trace(jid, f"VK API Bypass successful. Extracted {q} CDN.")
                            return files[q]
        except Exception as e:
            self.db.log_trace(jid, f"[vk_api] Ghost Protocol Failed: {e}")
        return None

    async def execute(self, job_data: dict):
        jid, original_url = job_data['id'], job_data['url']
        dl_dir = JOBS_DIR / f"JOB_{jid}" / "dl"

        await self.db.update_job(jid, stage="downloading | ~ | ~")

        self.db.log_trace(jid, "Analyzing VK Link...")
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
                    val = float(re.search(r"[\d.]+", pct_str).group()) if re.search(r"[\d.]+", pct_str) else 0.0
                    
                    global _live_ui_text
                    _live_ui_text[jid] = f"[yt-dlp native] {pct_str} at {speed} ETA {eta}"

                    current_time = time.time()
                    if current_time - last_db_update >= 1.0:
                        try: active_loop = asyncio.get_running_loop()
                        except RuntimeError: active_loop = loop
                        asyncio.run_coroutine_threadsafe(self.db.update_job(jid, pct=val, stage=f"downloading | {speed} | {eta}"), active_loop)
                        last_db_update = current_time
                except Exception: pass

        opts = {
            "outtmpl": str(dl_dir / f"{jid}.%(ext)s"),
            "format": "bestvideo[height<=1080]+bestaudio/best",
            "merge_output_format": "mp4",
            "progress_hooks": [prog_hook],
            "quiet": False, "noprogress": True, "no_warnings": True,
            "compat_opts": {"allow-unsafe-ext"},
            "external_downloader": "aria2c",
            "external_downloader_args": {"aria2c": ["-c", "-j", "16", "-x", "16", "-s", "16", "-k", "5M", "--summary-interval=1", "--console-log-level=notice"]},
            "logger": AriaLogger(jid, self.db)
        }

        # Dynamic CDN Spoofing
        custom_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        if "srcAg=GECKO" in target_url:
            custom_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"
        elif "srcAg=SAFARI" in target_url:
            custom_ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15"

        opts["http_headers"] = {"User-Agent": custom_ua}
        
        if VK_COOKIES and "vk" in target_url.lower():
            cookie_path = dl_dir / f"{jid}_vk_cookies.txt"
            with open(cookie_path, "w", encoding="utf-8") as f:
                f.write("# Netscape HTTP Cookie File\n")
                for item in VK_COOKIES.strip().split(';'):
                    if '=' in item:
                        k, v = item.strip().split('=', 1)
                        f.write(f".vk.com\tTRUE\t/\tTRUE\t2147483647\t{k}\t{v}\n")
                        f.write(f".vkvideo.ru\tTRUE\t/\tTRUE\t2147483647\t{k}\t{v}\n")
            opts["cookiefile"] = str(cookie_path)

        self.db.log_trace(jid, f"Engaging aria2c quad-core pull with UA: {custom_ua[:30]}...")
        await asyncio.to_thread(self._run_ytdlp, target_url, opts)

    def _run_ytdlp(self, url, opts):
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.extract_info(url, download=True)

class EncoderEngine:
    def __init__(self, scheduler: JobScheduler):
        self.db = scheduler

    async def execute(self, job_data: dict):
        jid = job_data['id']
        dl_dir, enc_dir = JOBS_DIR / f"JOB_{jid}" / "dl", JOBS_DIR / f"JOB_{jid}" / "enc"
        
        dl_files = [f for f in dl_dir.rglob("*") if f.is_file() and not f.name.endswith('.part')]
        if not dl_files: raise RuntimeError("Encoder failed: No completed files found.")
            
        dl_file = max(dl_files, key=lambda p: p.stat().st_size)
        enc_file = enc_dir / f"{jid}.mp4"

        self.db.log_trace(jid, "Entering FFmpeg FastStart Sandbox...")
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-nostdin", "-i", str(dl_file), 
            "-c:v", "copy", "-c:a", "aac", "-movflags", "+faststart", 
            str(enc_file), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        await asyncio.wait_for(proc.wait(), timeout=900)

class UploaderEngine:
    def __init__(self, db: JobScheduler, app: Client):
        self.db = db; self.app = app

    async def execute(self, job_data: dict):
        jid = job_data['id']
        job_dir = JOBS_DIR / f"JOB_{jid}"
        enc_dir = job_dir / "enc"
        
        target_file = next((f for f in enc_dir.rglob("*") if f.is_file()), None)
        if not target_file: raise RuntimeError("Uploader failed: No encoded payload found.")

        await self.db.update_job(jid, stage="uploading | extracting metadata")
        meta = await asyncio.to_thread(get_video_metadata, str(target_file))
        
        file_mb = target_file.stat().st_size / (1024 * 1024)
        duration_str = f"{meta['duration'] // 60}m {meta['duration'] % 60}s"

        caption = (
            f"🎬 **{job_data['title']}**\n"
            f"📏 **Resolution:** {meta['width']}x{meta['height']}\n"
            f"⏱️ **Duration:** {duration_str}\n"
            f"💾 **Size:** {file_mb:.2f} MB"
        )

        start_time = time.time()
        async def _up_prog(current, total):
            if not total: return
            pct = (current / total) * 100
            elapsed = time.time() - start_time
            speed = current / elapsed if elapsed > 0 else 0
            eta = (total - current) / speed if speed > 0 else 0
            await self.db.update_job(jid, pct=pct, stage=f"uploading | {speed / (1024*1024):.2f} MB/s | {int(eta//60):02d}:{int(eta%60):02d}")

        await self.db.update_job(jid, stage="uploading | streaming payload")
        await self.app.send_video(
            chat_id=CHANNEL_ID, video=str(target_file), caption=caption,
            width=meta["width"], height=meta["height"], duration=meta["duration"],
            thumb=meta["thumb"], supports_streaming=True, progress=_up_prog
        )

        if meta["thumb"] and Path(meta["thumb"]).exists(): Path(meta["thumb"]).unlink()

        # Update Batch Stats
        batch_source = job_data.get('source', '')
        if batch_source.startswith("Batch_"):
            await self.db.increment_batch(batch_source)

        # Update Final Tracker Card
        if job_data.get('tracker_id'):
            try:
                final_text = f"`[❖] ＴＡＳＫ :` `{job_data['title'][:18]}..`\n`✅ COMPLETED`"
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("🗑️ DISMISS", callback_data=f"delmsg|{job_data['tracker_id']}")]])
                await self.app.edit_message_text(job_data['chat_id'], job_data['tracker_id'], final_text, reply_markup=kb)
            except Exception: pass

        global _last_completed
        _last_completed = job_data['title']
        _live_ui_text.pop(jid, None)
        await self.db.delete_job(jid)
        shutil.rmtree(job_dir, ignore_errors=True)

# ──────────────────────────── PIPELINE & ORCHESTRATOR ─────────────────

class PipelineManager:
    def __init__(self, app: Client, db: JobScheduler):
        self.app, self.db = app, db
        self.dl_q, self.enc_q, self.up_q = asyncio.Queue(), asyncio.Queue(), asyncio.Queue()
        self.dl_engine, self.enc_engine, self.up_engine = DownloaderEngine(db), EncoderEngine(db), UploaderEngine(db, app)

    async def _worker_loop(self, queue: asyncio.Queue, engine, start_stage: Stage, success_stage: Stage, next_q: asyncio.Queue = None):
        while True:
            jid = await queue.get()
            job = await self.db.get_job(jid)
            retry = job.get('retries', 0)

            if job.get('stage') == Stage.CANCELLED.value: 
                queue.task_done(); continue

            try:
                await self.db.update_job(jid, stage=start_stage.value, retries=retry)
                await engine.execute(job)
                await self.db.update_job(jid, stage=success_stage.value, retries=0, recovered_at_stage=None)
                if next_q: await next_q.put(jid)
            except Exception as e:
                retry += 1
                if retry >= MAX_RETRIES:
                    await self.db.update_job(jid, stage=Stage.FAILED.value)
                    self.db.log_trace(jid, f"CRITICAL FAULT: {e}\n{traceback.format_exc()}")
                else: 
                    await self.db.update_job(jid, stage=job['stage'], retries=retry)
                    await queue.put(jid)
            finally:
                queue.task_done()

    def start_workers(self):
        for _ in range(MAX_DL_WORKERS): asyncio.create_task(self._worker_loop(self.dl_q, self.dl_engine, Stage.DOWNLOADING, Stage.DOWNLOADED, self.enc_q))
        asyncio.create_task(self._worker_loop(self.enc_q, self.enc_engine, Stage.ENCODING, Stage.ENCODED, self.up_q))
        asyncio.create_task(self._worker_loop(self.up_q, self.up_engine, Stage.UPLOADING, Stage.COMPLETED, None))

# --- CRASH RECOVERY ---
async def recover_orphaned_jobs(db: JobScheduler, dl_q: asyncio.Queue, enc_q: asyncio.Queue, up_q: asyncio.Queue):
    active_jobs = await db.get_active_jobs()
    for job in active_jobs:
        jid, stage = job['id'], str(job.get('stage', '')).lower()
        if any(t in stage for t in ['completed', 'failed']): continue
        
        await db.update_job(jid, recovered_at_stage=stage)
        enc_dir = JOBS_DIR / f"JOB_{jid}/enc"
        encoded_files = list(enc_dir.glob("*.mp4")) if enc_dir.exists() else []

        if 'upload' in stage or 'encode' in stage:
            if encoded_files and encoded_files[0].exists():
                await db.update_job(jid, stage="queued for upload", pct=0.0)
                await up_q.put(jid)
            else:
                await db.update_job(jid, stage="queued", pct=0.0)
                await dl_q.put(jid)
        else:
            dl_dir = JOBS_DIR / f"JOB_{jid}/dl"
            if dl_dir.exists():
                for p in dl_dir.glob("*"):
                    try: p.unlink()
                    except: pass
            await db.update_job(jid, stage="queued", pct=0.0)
            await dl_q.put(jid)

# --- DISK-AWARE DRIP FEEDER ---
async def batch_drip_feed_loop(db: JobScheduler, pipeline: PipelineManager):
    """Monitors batches, checks Disk Space, and maintains 3 concurrent downloads."""
    while True:
        await asyncio.sleep(3)
        try:
            total, used, free = shutil.disk_usage(BASE_DIR)
            free_gb = free / (1024 ** 3)
            
            global _live_ui_text
            if free_gb < MIN_FREE_DISK_GB:
                _live_ui_text["SYSTEM"] = f"⚠️ DISK LOW ({free_gb:.1f} GB Free) - DLs Paused"
                continue
            else:
                _live_ui_text.pop("SYSTEM", None)

            active_jobs = await db.get_active_jobs()
            if len(active_jobs) >= MAX_ACTIVE_PHYS_JOBS: continue

            # Count jobs specifically in download phase
            dl_jobs = [j for j in active_jobs if j.get('stage', '').lower() in ['queued', 'downloading']]
            
            if len(dl_jobs) < MAX_DL_WORKERS:
                slots = MAX_DL_WORKERS - len(dl_jobs)
                
                # Fetch pending jobs that haven't been queued yet
                pending_jobs = [j for j in active_jobs if j.get('stage') == 'pending_batch']
                for j in pending_jobs[:slots]:
                    tracker = await pipeline.app.send_message(
                        j['chat_id'], 
                        f"`[ ⚡ ] ＴＡＳＫ :` `{j['title'][:30]}`\n`[ ⚙️ ] ＳＴＡＴ :` `QUEUED`", 
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ CANCEL", callback_data=f"kill|{j['id']}")]])
                    )
                    await db.update_job(j['id'], stage="queued", tracker_id=tracker.id)
                    await pipeline.dl_q.put(j['id'])
        except Exception as e:
            log.error(f"Drip Feeder Error: {e}")

# ──────────────────────────── UI DASHBOARD ────────────────────────────

class TelegramDispatcher:
    def __init__(self, app: Client):
        self.app, self.edit_queue, self.pending_edits, self.lock = app, asyncio.Queue(), {}, asyncio.Lock()
        self.tokens, self.rate, self.last_refill = 25.0, 25.0, time.time()

    async def _consume(self):
        now = time.time(); elapsed = now - self.last_refill
        self.tokens = min(30.0, self.tokens + elapsed * self.rate); self.last_refill = now
        if self.tokens < 1.0: await asyncio.sleep(1.0 / self.rate); await self._consume()
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
                if key not in self.pending_edits: self.edit_queue.task_done(); continue
                text, kb = self.pending_edits.pop(key)
            
            for _ in range(5):
                await self._consume()
                try:
                    await self.app.edit_message_text(key[0], key[1], text, reply_markup=kb)
                    break
                except MessageNotModified: break
                except FloodWait as e: await asyncio.sleep(e.value + 1)
                except Exception: break
            self.edit_queue.task_done()

async def _get_dashboard_components(tab: str, db: JobScheduler) -> tuple[str, InlineKeyboardMarkup]:
    global _last_completed
    total, used, free = shutil.disk_usage(BASE_DIR)
    free_gb = free / (1024 ** 3)
    total_gb = total / (1024 ** 3)
    
    disk_stat = f"`{free_gb:.1f} GB Free / {total_gb:.1f} GB (HEALTHY)`"
    if free_gb < MIN_FREE_DISK_GB:
        disk_stat = f"`⚠️ {free_gb:.1f} GB Free (THROTTLED - WAITING)`"

    jobs = await db.get_active_jobs()
    std_jobs = [j for j in jobs if j.get('stage') != 'pending_batch']

    # Text Block
    text = (
        f"💻 **VK MAINFRAME v9.0.1**\n"
        f"`━━━━━━━━━━━━━━━━━━━━━━━━━━`\n"
        f"`[💾] DISK :` {disk_stat}\n"
        f"`[⚡] ACT  :` `{len(std_jobs)} JOBS ACTIVE`\n"
        f"`[🏁] LAST :` `{_last_completed[:12]}`\n"
        f"`━━━━━━━━━━━━━━━━━━━━━━━━━━`\n"
    )

    kb_lines = []
    
    # Batches Accordion
    batches = {}
    for j in jobs:
        if str(j.get('source', '')).startswith('Batch_'):
            batches.setdefault(j['source'], []).append(j)

    is_batch_open = (tab == "batches")
    kb_lines.append([InlineKeyboardButton(f"{'[-]' if is_batch_open else '[+]'} 📦 ACTIVE BATCHES ({len(batches)})", callback_data=f"dash|{'root' if is_batch_open else 'batches'}")])
    
    if is_batch_open:
        for b_name, b_jobs in batches.items():
            tot, cmp = await db.get_batch_stats(b_name)
            kb_lines.append([InlineKeyboardButton(f" └ 📁 {b_name} [{cmp}/{tot} Complete]", callback_data="noop")])
            for j in b_jobs[:5]:
                kb_lines.append([InlineKeyboardButton(f"      ├ {j['title'][:15]} | {j.get('stage', '').split('|')[0]}", callback_data="noop")])

    # Standard Dropdowns
    def build_dropdown(target: str, label: str, icon: str, filt_stage: list):
        j_list = [j for j in std_jobs if any(x in j.get('stage', '').lower() for x in filt_stage)]
        is_open = (tab == target)
        kb_lines.append([InlineKeyboardButton(f"{'[-]' if is_open else '[+]'} {icon} {label} ({len(j_list)})", callback_data=f"dash|{'root' if is_open else target}")])
        if is_open:
            for j in j_list[:10]:
                pct = float(j.get('pct', 0.0) or 0.0)
                kb_lines.append([
                    InlineKeyboardButton(f" ├ ⚡ {j['title'][:15]} | {pct:.1f}%", callback_data="noop"),
                    InlineKeyboardButton("❌", callback_data=f"kill|{j['id']}")
                ])

    build_dropdown("dl", "DOWNLOADING", "📥", ["queued", "downloading"])
    build_dropdown("enc", "PROCESSING", "⚙️", ["downloaded", "encoding", "process"])
    build_dropdown("up", "UPLOADING", "📤", ["encoded", "uploading"])
    
    kb_lines.append([InlineKeyboardButton("🔄 REFRESH SYSTEM", callback_data=f"dash|{tab}")])
    return text, InlineKeyboardMarkup(kb_lines)

# ──────────────────────────── ROUTER & ENTRY ──────────────────────────

def setup_router(app: Client, db: JobScheduler, pipeline: PipelineManager, dispatcher: TelegramDispatcher):
    
    @app.on_message(filters.command(["start", "dashboard"]) & filters.user(OWNER_ID))
    async def init_dashboard(_, msg: Message):
        global _dashboard_msg_id, _dashboard_chat_id, _dashboard_tab
        m = await msg.reply("🟢 Booting VK Mainframe...")
        _dashboard_msg_id, _dashboard_chat_id = m.id, m.chat.id
        text, kb = await _get_dashboard_components(_dashboard_tab, db)
        await dispatcher.safe_edit_queued(_dashboard_chat_id, _dashboard_msg_id, text, kb)

    @app.on_message(filters.command(["go"]) & filters.user(OWNER_ID))
    async def batch_go(_, msg: Message):
        global _batch_mode, _batch_collection, _current_batch_name
        _batch_mode = True
        _batch_collection = []
        args = msg.text.split(maxsplit=1)
        _current_batch_name = args[1].strip() if len(args) > 1 else f"VK_{str(uuid.uuid4())[:4]}"
        await msg.reply(f"🟢 **BATCH MODE**\n🏷️ `{_current_batch_name}`\nPaste VK URLs. Send `/end` when finished.")

    @app.on_message(filters.command(["end"]) & filters.user(OWNER_ID))
    async def batch_end(_, msg: Message):
        global _batch_mode, _batch_collection, _current_batch_name
        if not _batch_mode: return
        _batch_mode = False
        
        batch_src = f"Batch_{_current_batch_name}"
        total_count = len(_batch_collection)
        await db.register_batch(batch_src, total_count)
        
        for idx, (url, title, chat_id) in enumerate(_batch_collection, 1):
            jid = str(uuid.uuid4())[:8]
            await db.create_job({
                "id": jid, "url": url, "title": f"[{idx}/{total_count}] {title[:30]}", 
                "source": batch_src, "chat_id": chat_id, "tracker_id": None
            })
            # Kept as pending_batch, the drip feeder will push it to dl_q
            await db.update_job(jid, stage="pending_batch")
            
        await msg.reply(f"🚀 **BATCH SUBMITTED**\nSent {total_count} tasks. Disk backpressure will regulate flow.")
        _batch_collection.clear()

    @app.on_message(filters.text & filters.user(OWNER_ID) & ~filters.command(["start", "dashboard", "go", "end"]))
    async def url_catcher(_, msg: Message):
        url = next((w for w in msg.text.split() if w.startswith("http")), None)
        if url and "vk" in url.lower():
            title = msg.text.replace(url, "").strip() or "VK_Video"
            global _batch_mode, _batch_collection
            if _batch_mode:
                _batch_collection.append((url, title, msg.chat.id))
                await msg.reply(f"✅ Added to batch. Total: {len(_batch_collection)}", quote=True)
            else:
                jid = str(uuid.uuid4())[:8]
                tracker = await msg.reply(f"`[ ⚡ ] ＴＡＳＫ :` `{title[:30]}`\n`[ ⚙️ ] ＳＴＡＴ :` `QUEUED`", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ CANCEL", callback_data=f"kill|{jid}")]]))
                await db.create_job({"id": jid, "url": url, "title": title, "source": "Direct", "chat_id": msg.chat.id, "tracker_id": tracker.id})
                await pipeline.dl_q.put(jid)

    @app.on_callback_query()
    async def _router(client: Client, cb: CallbackQuery):
        global _dashboard_tab
        if cb.data == "noop": return await cb.answer()
        
        if cb.data.startswith("dash|"):
            _dashboard_tab = cb.data.split("|")[1]
            text, kb = await _get_dashboard_components(_dashboard_tab, db)
            try: await cb.message.edit_text(text, reply_markup=kb)
            except MessageNotModified: pass
            return await cb.answer()

        if cb.data.startswith("kill|"):
            jid = cb.data.split("|")[1]
            await db.delete_job(jid)
            shutil.rmtree(JOBS_DIR / f"JOB_{jid}", ignore_errors=True)
            await cb.answer("Process terminated.", show_alert=True)

        if cb.data.startswith("delmsg|"):
            _, msg_id = cb.data.split("|")
            try: await client.delete_messages(cb.message.chat.id, int(msg_id))
            except Exception: pass
            await cb.answer()

# ──────────────────────────── EVENT LOOPS ─────────────────────────────

async def ui_accumulator_loop(db: JobScheduler, dispatcher: TelegramDispatcher):
    global _dashboard_msg_id, _dashboard_chat_id, _dashboard_tab
    last_known = set()
    while True:
        await asyncio.sleep(4)
        try:
            jobs = await db.get_active_jobs()
            current = {j['id'] for j in jobs if j.get('stage') != 'pending_batch'}
            needs_update = (current != last_known)
            last_known = current
            
            for j in jobs:
                if j.get('tracker_id'):
                    pct = float(j.get('pct', 0.0) or 0.0)
                    last_pct = float(j.get('last_ui_pct', -10.0))
                    if (pct - last_pct) >= 10.0 or needs_update:
                        needs_update = True
                        bar = make_bar(pct, 10)
                        txt = f"`[❖] ＴＡＳＫ :` `{j['title'][:18]}..`\n`⚙️ PHASE :` `{j['stage']}`\n`📊 PROG  :` `[{bar}] {pct:.1f}%`"
                        kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ KILL", callback_data=f"kill|{j['id']}")]])
                        await dispatcher.safe_edit_queued(j['chat_id'], j['tracker_id'], txt, kb)
                        await db.update_job(j['id'], last_ui_pct=pct)

            if needs_update and _dashboard_msg_id:
                text, kb = await _get_dashboard_components(_dashboard_tab, db)
                await dispatcher.safe_edit_queued(_dashboard_chat_id, _dashboard_msg_id, text, kb)
        except Exception: pass

async def terminal_loop(db: JobScheduler, pipeline: PipelineManager):
    sys.stdout.write("\033[2J") 
    while True:
        await asyncio.sleep(1) 
        sys.stdout.write("\033[H") 
        sys.stdout.write(f"{C_CYAN}{C_BOLD}=== VK MAINFRAME [LIVE] ==={C_RESET}\n")
        
        sys.msg = _live_ui_text.get("SYSTEM", "")
        if sys.msg: sys.stdout.write(f"{C_RED}{C_BOLD}{sys.msg}{C_RESET}\n")

        jobs = await db.get_active_jobs()
        active = [j for j in jobs if j.get('stage') != 'pending_batch']
        if not active: sys.stdout.write(f"{C_GREEN}System Idle.{C_RESET}\033[K\n")
        else:
            for j in active[:5]:
                col = C_YELLOW if "down" in j['stage'] else C_CYAN if "enc" in j['stage'] else C_GREEN
                sys.stdout.write(f"{C_BOLD}[{j['title'][:15]}]{C_RESET} {col}{j['stage']}{C_RESET} | {j.get('pct', 0.0):.1f}%\033[K\n")
                sys.stdout.write(f"  └ 📡 \033[36m{_live_ui_text.get(j['id'], '...')[:75]}\033[0m\033[K\n")
        sys.stdout.write("\033[J"); sys.stdout.flush()

# ──────────────────────────── BOOTSTRAP ───────────────────────────────

async def main():
    app = Client("vk_bot_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
    db = JobScheduler(DB_PATH)
    pipeline = PipelineManager(app, db)
    dispatcher = TelegramDispatcher(app) 
    
    setup_router(app, db, pipeline, dispatcher)

    async with app:
        print("🔄 Running Crash Recovery...")
        await recover_orphaned_jobs(db, pipeline.dl_q, pipeline.enc_q, pipeline.up_q)
        
        pipeline.start_workers()
        asyncio.create_task(dispatcher.sender_loop()) 
        asyncio.create_task(ui_accumulator_loop(db, dispatcher)) 
        asyncio.create_task(terminal_loop(db, pipeline))
        asyncio.create_task(batch_drip_feed_loop(db, pipeline))
        
        print("🟢 VK Mainframe Systems Online.")
        while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    try: loop.run_until_complete(main())
    except KeyboardInterrupt: sys.exit(0)