"""
stealth_bot.py – v13.2 (The Complete Structured Monolith)
───────────────────────────────────────────────────────────────
ARCHITECTURE:
  • Single-file Micro-Orchestration (Classes).
  • JobScheduler (SQLite + asyncio.Lock) replacing state.json.
  • FFmpeg Sandbox (Strict timeouts prevent zombie processes).
  • Full 6-Stage Waterfall (including Playwright interceptor).
  • Full Accordion Dashboard & ANSI Termux Logger.
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
import urllib.parse
import primp
import yt_dlp
from yt_dlp.networking.impersonate import ImpersonateTarget

# Pre-initialize event loop for Pyrogram compatibility
try:
    loop = asyncio.get_running_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait, MessageNotModified
from logging.handlers import RotatingFileHandler
import config

# ──────────────────────────── CONFIGURATION ─────────────────────────────

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
logging.getLogger().handlers[1].setLevel(logging.CRITICAL) # Preserve ANSI UI
log = logging.getLogger("stealth_bot")
logging.getLogger("pyrogram").setLevel(logging.ERROR)

API_ID, API_HASH, BOT_TOKEN, CHANNEL_ID = config.API_ID, config.API_HASH, config.BOT_TOKEN, config.CHANNEL_ID
OWNER_ID = int(config.OWNER_ID) if hasattr(config, "OWNER_ID") else 0
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

JOBS_DIR, DONE_DIR = BASE_DIR / "jobs", BASE_DIR / "completed"
for d in (JOBS_DIR, DONE_DIR): d.mkdir(parents=True, exist_ok=True)

MAX_DL_WORKERS, MAX_RETRIES = 3, 3

# ANSI Colors
C_CYAN, C_YELLOW, C_RED, C_GREEN, C_RESET, C_BOLD = "\033[36m", "\033[33m", "\033[31m", "\033[32m", "\033[0m", "\033[1m"

def make_bar(percent: float, width: int = 10) -> str:
    filled = int(max(0.0, min(percent, 100.0)) / (100.0 / width))
    return "█" * filled + "░" * (width - filled)

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
                stage TEXT, pct REAL, last_ui_pct REAL, retries INTEGER, chat_id INTEGER, tracker_id INTEGER
            )''')

    async def create_job(self, data: dict):
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''INSERT INTO jobs (id, url, title, source, quality, strategy, stage, pct, last_ui_pct, retries, chat_id, tracker_id)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
                             (data['id'], data['url'], data['title'], data['source'], data.get('quality', 'auto'), data.get('strategy', 'GENERIC'), 
                              Stage.QUEUED.value, 0.0, -10.0, 0, data['chat_id'], data['tracker_id']))
                
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

    def log_trace(self, jid: str, msg: str):
        with open(JOBS_DIR / f"JOB_{jid}" / "trace.log", "a", encoding="utf-8") as f:
            f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")

# ──────────────────────────── SUBSYSTEM 2: RESOLVER ─────────────────────

class LinkClassifier:
    @staticmethod
    def classify(url: str) -> str:
        if url == "telegram_bridge": return "TELEGRAM"
        if "magnet:?" in url: return "MAGNET"
        if ".m3u8" in url or "m3u8" in url: return "HLS_STREAM"
        if "youtube.com" in url or "youtu.be" in url: return "YOUTUBE"
        if url.endswith(".mp4") or "direct-mp4" in url: return "DIRECT_MP4"
        return "GENERIC_FALLBACK"

# ──────────────────────────── SUBSYSTEM 3: ENGINES ──────────────────────

class DownloaderEngine:
    def __init__(self, scheduler: JobScheduler, app: Client):
        self.db = scheduler
        self.app = app
        self.procs = {}

    async def execute(self, job_data: dict):
        jid, url, strategy, quality = job_data['id'], job_data['url'], job_data['strategy'], job_data['quality']
        dl_dir = JOBS_DIR / f"JOB_{jid}" / "dl"
        
        self.db.log_trace(jid, f"Download Orchestrator engaged. Strategy: {strategy}")

        if strategy == "TELEGRAM":
            async def tg_prog(c, t):
                if t: await self.db.update_job(jid, pct=(c * 100 / t))
            await self.app.download_media(url, file_name=str(dl_dir / f"{jid}.mp4"), progress=tg_prog)
            return

        if strategy == "MAGNET" or strategy == "DIRECT_MP4":
            await self._run_aria(url, jid, dl_dir)
        elif strategy == "HLS_STREAM":
            self.db.log_trace(jid, "Attempting HLS extraction via yt-dlp...")
            await asyncio.to_thread(self._run_ytdlp, url, jid, dl_dir, url, "")
        else:
            # 6-Stage Waterfall for Generic Links
            actual_url = url
            referer = url
            cookie = ""
            
            try:
                client = primp.Client(impersonate="chrome_120")
                resp = client.get(url, headers={"User-Agent": USER_AGENT})
                match = re.search(r"(https?://[^\"']+(?:\.m3u8|\.mp4)[^\"']*)", resp.text)
                if match: actual_url = match.group(1).replace(r"\/", "/")
            except Exception: pass

            try:
                await asyncio.to_thread(self._run_ytdlp, actual_url, jid, dl_dir, referer, cookie)
            except Exception as e:
                self.db.log_trace(jid, f"yt-dlp failed, escalating to Playwright. Error: {e}")
                await self._run_playwright(url, jid, dl_dir)

    async def _run_aria(self, url: str, jid: str, dl_dir: Path, headers: dict = None):
        cmd = ["aria2c", "-d", str(dl_dir), "-c", "-x", "16", "-s", "10", "--file-allocation=none"]
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
                chunk_str = chunk.decode("utf-8", errors="ignore")
                
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
        if not valid_files: raise RuntimeError("Aria2c failed: No media payloads found in output directory.")

    def _run_ytdlp(self, url: str, jid: str, dl_dir: Path, referer: str, cookie: str):
        class SilentLogger:
            def debug(self, msg): pass
            def warning(self, msg): pass
            def error(self, msg): pass

        def prog_hook(d):
            if d.get("status") == "downloading":
                try: 
                    total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
                    down = d.get("downloaded_bytes", 0)
                    if total > 0:
                        val = (down / total) * 100
                        speed = re.sub(r"\x1b[^m]*m", "", d.get("_speed_str", "~")).strip()
                        eta = re.sub(r"\x1b[^m]*m", "", d.get("_eta_str", "~")).strip()
                        stage_str = f"downloading | {speed} | {eta}"
                        asyncio.run_coroutine_threadsafe(self.db.update_job(jid, pct=val, stage=stage_str), loop)
                except Exception: pass
        
        fmt = "bestvideo[height<=1080]+bestaudio/best"
        opts = {
            "outtmpl": str(dl_dir / f"{jid}.%(ext)s"), 
            "format": fmt, 
            "http_headers": {"Referer": referer, "User-Agent": USER_AGENT},
            "impersonate": ImpersonateTarget(client="chrome"),
            "progress_hooks": [prog_hook], 
            "quiet": True,
            "noprogress": True,
            "no_warnings": True,
            "logger": SilentLogger(),
            "compat_opts": {"allow-unsafe-ext"}
        }
        with yt_dlp.YoutubeDL(opts) as ydl: ydl.extract_info(url, download=True)

    async def _run_playwright(self, url: str, jid: str, dl_dir: Path):
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-setuid-sandbox"])
            context = await browser.new_context(user_agent=USER_AGENT)
            page = await context.new_page()

            async def block_bloat(route):
                if route.request.resource_type in ["image", "font", "stylesheet"] or any(x in route.request.url for x in ["ads", "tracking"]):
                    await route.abort()
                else: await route.continue_()

            await page.route("**/*", block_bloat)
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)

            video_src = await page.evaluate('''() => {
                let v = document.querySelector('video'); if (v && v.src && !v.src.startsWith('blob:')) return v.src;
                let s = document.querySelector('video source'); if (s && s.src && !s.src.startsWith('blob:')) return s.src;
                return null;
            }''')

            cookies = await context.cookies()
            cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
            referer = page.url
            await browser.close()

            if video_src:
                try:
                    await asyncio.to_thread(self._run_ytdlp, video_src, jid, dl_dir, referer, cookie_str)
                except Exception as e:
                    self.db.log_trace(jid, "yt-dlp panicked on file extension. Forcing Aria2c bypass.")
                    await self._run_aria(video_src, jid, dl_dir, headers={"Cookie": cookie_str, "Referer": referer})
            else:
                raise RuntimeError("Playwright headless interceptor failed to find raw video source.")

class EncoderEngine:
    def __init__(self, scheduler: JobScheduler):
        self.db = scheduler

    async def execute(self, job_data: dict):
        jid = job_data['id']
        dl_dir, enc_dir, thumb_dir = JOBS_DIR / f"JOB_{jid}" / "dl", JOBS_DIR / f"JOB_{jid}" / "enc", JOBS_DIR / f"JOB_{jid}" / "thumb"
        
        # Note: Added .php to valid extensions here so FFmpeg can convert the bypassed files
        dl_files = [f for f in dl_dir.rglob("*") if f.is_file() and f.suffix.lower() in [".mp4", ".mkv", ".avi", ".ts", ".webm", ".flv", ".php"]]
        dl_file = max(dl_files, key=lambda p: p.stat().st_size)
        enc_file, thumb_file = enc_dir / f"{jid}.mp4", thumb_dir / f"{jid}.jpg"

        self.db.log_trace(jid, "Entering FFmpeg Sandbox...")
        
        await asyncio.create_subprocess_exec("ffmpeg", "-y", "-i", str(dl_file), "-ss", "00:00:02", "-vframes", "1", str(thumb_file), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        proc = await asyncio.create_subprocess_exec("ffmpeg", "-y", "-nostdin", "-i", str(dl_file), "-c:v", "copy", "-c:a", "aac", "-movflags", "+faststart", str(enc_file), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            await asyncio.wait_for(proc.wait(), timeout=900)
        except asyncio.TimeoutError:
            proc.kill()
            raise TimeoutError("FFmpeg Zombie Sandbox Timeout: Corrupted video headers caused process hang.")

class UploadEngine:
    def __init__(self, scheduler: JobScheduler, app: Client):
        self.db = scheduler
        self.app = app

    async def execute(self, job_data: dict):
        jid, title = job_data['id'], job_data['title']
        enc_file, thumb_file = JOBS_DIR / f"JOB_{jid}" / "enc" / f"{jid}.mp4", JOBS_DIR / f"JOB_{jid}" / "thumb" / f"{jid}.jpg"

        w, h, dur = 1280, 720, 100
        try:
            proc = await asyncio.create_subprocess_exec("ffprobe", "-v", "error", "-show_entries", "stream=width,height:format=duration", "-of", "json", str(enc_file), stdout=subprocess.PIPE)
            stdout, _ = await proc.communicate()
            probe = json.loads(stdout.decode())
            dur = int(float(probe.get("format", {}).get("duration", 100)))
            for s in probe.get("streams", []):
                if s.get("width"): w, h = s["width"], s["height"]
        except Exception: pass

        async def up_prog(c, t):
            if t: await self.db.update_job(jid, pct=(c * 100 / t))

        await self.app.send_video(CHANNEL_ID, video=str(enc_file), thumb=str(thumb_file) if thumb_file.exists() else None, caption=title, width=w, height=h, duration=dur, supports_streaming=True, progress=up_prog)
        
        shutil.move(str(JOBS_DIR / f"JOB_{jid}"), str(DONE_DIR / f"JOB_{jid}"))

# ──────────────────────────── SUBSYSTEM 4: RECOVERY & LOGGING ─────────

class CrashCourier:
    @staticmethod
    async def push_fault(app: Client, db: JobScheduler, jid: str, exc: Exception):
        await db.update_job(jid, stage=Stage.FAILED.value)
        tb_str = traceback.format_exc()
        db.log_trace(jid, f"CRITICAL FAULT:\n{tb_str}")

        job = await db.get_job(jid)
        chat_id = job.get('chat_id', OWNER_ID)
        
        cap = f"🚨 **MAINFRAME FAULT**\n`{jid}` collapsed.\nError: `{str(exc)[:100]}`"
        log_path = JOBS_DIR / f"JOB_{jid}" / "trace.log"
        if log_path.exists():
            try: await app.send_document(chat_id, document=str(log_path), caption=cap)
            except Exception: pass

class RecoveryManager:
    @staticmethod
    async def scan_and_requeue(db: JobScheduler, dl_q: asyncio.Queue, enc_q: asyncio.Queue, up_q: asyncio.Queue, app: Client):
        active = await db.get_active_jobs()
        resumed = []
        for job in active:
            jid, stage, title = job['id'], job['stage'], job['title'][:25]
            if stage in [Stage.QUEUED.value, Stage.DOWNLOADING.value]:
                await db.update_job(jid, stage=Stage.QUEUED.value); dl_q.put_nowait(jid); resumed.append(f"  ├ `[DL]` `{title}`")
            elif stage in [Stage.DOWNLOADED.value, Stage.ENCODING.value]:
                await db.update_job(jid, stage=Stage.DOWNLOADED.value); enc_q.put_nowait(jid); resumed.append(f"  ├ `[ENC]` `{title}`")
            elif stage in [Stage.ENCODED.value, Stage.UPLOADING.value]:
                await db.update_job(jid, stage=Stage.ENCODED.value); up_q.put_nowait(jid); resumed.append(f"  ├ `[UP]` `{title}`")

        if resumed and OWNER_ID:
            try: await app.send_message(OWNER_ID, "🔄 **RESUME AUDITOR**\n" + "\n".join(resumed))
            except Exception: pass

# ──────────────────────────── PIPELINE MANAGER (Orchestrator) ───────────

class PipelineManager:
    def __init__(self, app: Client, db: JobScheduler):
        self.app, self.db = app, db
        self.dl_q, self.enc_q, self.up_q = asyncio.Queue(), asyncio.Queue(), asyncio.Queue()
        self.dl_engine, self.enc_engine, self.up_engine = DownloaderEngine(db, app), EncoderEngine(db), UploadEngine(db, app)

    async def _worker_loop(self, queue: asyncio.Queue, engine, start_stage: Stage, success_stage: Stage, next_q: asyncio.Queue = None):
        while True:
            jid = await queue.get()
            job = await self.db.get_job(jid)
            retry = job.get('retries', 0)

            if job.get('stage') == Stage.CANCELLED.value: queue.task_done(); continue

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
            finally:
                queue.task_done()

    def start_workers(self):
        for _ in range(MAX_DL_WORKERS): asyncio.create_task(self._worker_loop(self.dl_q, self.dl_engine, Stage.DOWNLOADING, Stage.DOWNLOADED, self.enc_q))
        asyncio.create_task(self._worker_loop(self.enc_q, self.enc_engine, Stage.ENCODING, Stage.ENCODED, self.up_q))
        asyncio.create_task(self._worker_loop(self.up_q, self.up_engine, Stage.UPLOADING, Stage.COMPLETED, None))

# ──────────────────────────── UI & COMMAND ROUTER ───────────────────────

_dashboard_msg_id, _dashboard_chat_id, _dashboard_tab = 0, 0, "root"
_last_completed = "—"

def _job_tracker_text(job: dict) -> str:
    bar = make_bar(job['pct'], 12)
    return f"`[ ⚡ ] ＴＡＳＫ :` `{job['title'][:30]}`\n`[ ⚙️ ] ＳＴＡＴ :` `{job['stage'].upper()}`\n`[ 📊 ] ＰＲＯＧ :` `[{bar}] {job['pct']:.1f}%`"

async def _get_dashboard_components(tab: str, db: JobScheduler, pipeline: PipelineManager) -> tuple[str, InlineKeyboardMarkup]:
    global _last_completed
    wait_dl, wait_enc, wait_up = pipeline.dl_q.qsize(), pipeline.enc_q.qsize(), pipeline.up_q.qsize()
    
    text = f"🖥 **STEALTH MAINFRAME**\n{'═' * 28}\n\n"
    if tab == "root":
        # Calculate storage mapping
        storage_mb = sum(f.stat().st_size for f in JOBS_DIR.rglob("*") if f.is_file()) / (1024 ** 2)
        text += (
            f"**[ 📡 TELEMETRY ]**\n"
            f"  ├ Wait Download  : `{wait_dl}`\n"
            f"  ├ Wait Encode    : `{wait_enc}`\n"
            f"  ├ Wait Upload    : `{wait_up}`\n"
            f"  └ Storage Use    : `{storage_mb:.1f} MB`\n\n"
            f"**[ 🏁 LATEST ]**\n  └ `{_last_completed[:35]}`"
        )
    else:
        text += f"**[ 📂 VIEW: {tab.upper()} ]**\n"
        jobs = await db.get_active_jobs()
        found = False
        for job in jobs:
            if (tab == "dl" and "download" in job['stage']) or \
               (tab == "enc" and ("process" in job['stage'] or "enc" in job['stage'])) or \
               (tab == "up" and "upload" in job['stage']):
                found = True
                pct = job.get('pct', 0.0)
                text += f"  ├ `{job['title'][:20]}`\n  └ `[{make_bar(pct, 8)}] {pct:.1f}%`\n"
        if not found:
            text += "  └ _Empty_"

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 SUMMARY" if tab != "root" else "🔘 SUMMARY", callback_data="dash|root")],
        [
            InlineKeyboardButton(f"📥 DL", callback_data="dash|dl"),
            InlineKeyboardButton(f"⚙️ ENC", callback_data="dash|enc"),
            InlineKeyboardButton(f"📤 UP", callback_data="dash|up"),
        ]
    ])
    return text, kb

async def safe_edit(app: Client, chat_id: int, msg_id: int, text: str, kb: InlineKeyboardMarkup):
    try: await app.edit_message_text(chat_id, msg_id, text, reply_markup=kb)
    except MessageNotModified: pass
    except FloodWait as e: await asyncio.sleep(e.value)
    except Exception: pass

pipeline_ref = None

def setup_router(app: Client, db: JobScheduler, pipeline: PipelineManager):
    global pipeline_ref
    pipeline_ref = pipeline
    
    @app.on_message(filters.command(["start", "dashboard"]) & filters.user(OWNER_ID))
    async def init_dashboard(_, msg: Message):
        global _dashboard_msg_id, _dashboard_chat_id, _dashboard_tab
        m = await msg.reply("🟢 Booting Mainframe...")
        _dashboard_msg_id, _dashboard_chat_id = m.id, m.chat.id
        try: await m.pin(disable_notification=True)
        except Exception: pass
        text, kb = await _get_dashboard_components(_dashboard_tab, db, pipeline)
        await safe_edit(app, _dashboard_chat_id, _dashboard_msg_id, text, kb)

    @app.on_message((filters.video | filters.document) & filters.user(OWNER_ID))
    async def auto_catch_media(_, msg: Message):
        if msg.document and msg.document.mime_type and not msg.document.mime_type.startswith("video/"): return
        
        jid = str(uuid.uuid4())[:8]
        title = msg.caption.strip() if msg.caption else "Direct Media Upload"
        file_id = msg.video.file_id if msg.video else msg.document.file_id
        
        tracker = await msg.reply(f"`[ ⚡ ] ＴＡＳＫ :` `{title[:30]}`\n`[ ⚙️ ] ＳＴＡＴ :` `QUEUED`", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ CANCEL", callback_data=f"kill|{jid}")]]))
        await db.create_job({"id": jid, "url": file_id, "title": title, "source": "telegram", "strategy": "TELEGRAM", "chat_id": msg.chat.id, "tracker_id": tracker.id})
        await pipeline.dl_q.put(jid)
        try: await msg.delete()
        except: pass

    @app.on_message(filters.text & filters.user(OWNER_ID) & ~filters.command(["start", "dashboard"]))
    async def url_catcher(_, msg: Message):
        url = next((w for w in msg.text.split() if w.startswith("http") or w.startswith("magnet:?")), None)
        if url:
            jid = str(uuid.uuid4())[:8]
            title = msg.text.replace(url, "").strip() or url[:40]
            tracker = await msg.reply(f"`[ ⚡ ] ＴＡＳＫ :` `{title[:30]}`\n`[ ⚙️ ] ＳＴＡＴ :` `QUEUED`", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ CANCEL", callback_data=f"kill|{jid}")]]))
            
            await db.create_job({"id": jid, "url": url, "title": title, "source": "Direct", "quality": "auto", "strategy": LinkClassifier.classify(url), "chat_id": msg.chat.id, "tracker_id": tracker.id})
            await pipeline.dl_q.put(jid)

    @app.on_callback_query(filters.user(OWNER_ID))
    async def cb_router(_, cb):
        parts = cb.data.split("|"); action = parts[0]
        
        if action == "dash":
            global _dashboard_tab; _dashboard_tab = parts[1]
            text, kb = await _get_dashboard_components(_dashboard_tab, db, pipeline)
            await safe_edit(app, _dashboard_chat_id, _dashboard_msg_id, text, kb)
            await cb.answer()
            
        elif action == "kill":
            await db.update_job(parts[1], stage=Stage.CANCELLED.value)
            if parts[1] in pipeline.dl_engine.procs:
                try: pipeline.dl_engine.procs[parts[1]].kill()
                except Exception: pass
            shutil.rmtree(JOBS_DIR / f"JOB_{parts[1]}", ignore_errors=True)
            await cb.answer("Killed.")
            
        elif action == "joblog":
            log_path = JOBS_DIR / f"JOB_{parts[1]}" / "trace.log"
            if not log_path.exists(): log_path = DONE_DIR / f"JOB_{parts[1]}" / "trace.log"
            if log_path.exists(): await cb.message.reply_document(str(log_path))
            else: await cb.answer("No logs found.", show_alert=True)

# ──────────────────────────── EVENT LOOPS ─────────────────────────────

async def ui_throttle_loop(app: Client, db: JobScheduler):
    while True:
        await asyncio.sleep(3)
        for job in await db.get_active_jobs():
            if not job['tracker_id']: continue
            if job['stage'] != job['last_ui_pct'] or (job['pct'] - job['last_ui_pct']) >= 10.0: 
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("📄 LOGS", callback_data=f"joblog|{job['id']}"), InlineKeyboardButton("❌ CANCEL", callback_data=f"kill|{job['id']}")]])
                await safe_edit(app, job['chat_id'], job['tracker_id'], _job_tracker_text(job), kb)
                await db.update_job(job['id'], last_ui_pct=job['pct'])
                
        global _dashboard_msg_id, _dashboard_chat_id, _dashboard_tab
        if _dashboard_msg_id and _dashboard_chat_id:
            text, kb = await _get_dashboard_components(_dashboard_tab, db, pipeline_ref)
            await safe_edit(app, _dashboard_chat_id, _dashboard_msg_id, text, kb)

async def terminal_loop(db: JobScheduler, pipeline: PipelineManager):
    sys.stdout.write("\033[2J") 
    while True:
        await asyncio.sleep(2)
        sys.stdout.write("\033[H") 
        sys.stdout.write(f"{C_CYAN}{C_BOLD}=== STEALTH MAINFRAME [LIVE] ==={C_RESET}\n")
        sys.stdout.write(f"QUEUES | DL: {pipeline.dl_q.qsize()} | ENC: {pipeline.enc_q.qsize()} | UP: {pipeline.up_q.qsize()}\n{'─' * 40}\n")
        
        jobs = await db.get_active_jobs()
        if not jobs: sys.stdout.write(f"{C_GREEN}System Idle. Awaiting vectors.{C_RESET}\033[K\n")
        for j in jobs[:5]:
            col = C_YELLOW if "download" in j['stage'] else C_CYAN if "enc" in j['stage'] else C_GREEN
            sys.stdout.write(f"{C_BOLD}[{j['title'][:15]}]{C_RESET} {col}{j['stage']}{C_RESET} | [{make_bar(j['pct'], 10)}] {j['pct']:.1f}%\033[K\n")
        for _ in range(5): sys.stdout.write("\033[K\n")
        sys.stdout.flush()

# ──────────────────────────── BOOTSTRAP ───────────────────────────────

async def main():
    app = Client("stealth_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
    db = JobScheduler(DB_PATH)
    pipeline = PipelineManager(app, db)
    setup_router(app, db, pipeline)

    async with app:
        await RecoveryManager.scan_and_requeue(db, pipeline.dl_q, pipeline.enc_q, pipeline.up_q, app)
        pipeline.start_workers()
        asyncio.create_task(ui_throttle_loop(app, db))
        asyncio.create_task(terminal_loop(db, pipeline))
        
        if OWNER_ID:
            m = await app.send_message(OWNER_ID, "🟢 Mainframe Systems Online.")
            global _dashboard_msg_id, _dashboard_chat_id, _dashboard_tab
            _dashboard_msg_id, _dashboard_chat_id = m.id, m.chat.id
            text, kb = await _get_dashboard_components(_dashboard_tab, db, pipeline)
            await safe_edit(app, _dashboard_chat_id, _dashboard_msg_id, text, kb)

        while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: sys.exit(0)