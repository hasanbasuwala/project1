"""
stealth_bot.py – v13.0 (Mainframe Edition)
───────────────────────────────────────────────────────────────
UI/UX & ARCHITECTURE UPGRADES:
  • High-Fidelity Diagnostics: Full traceback logged & couriered.
  • Mainframe UI: Accordion Dashboard + Monospaced Minimalist Job Cards.
  • Auto-Pin & Auditor: Boot sequence pins dashboard and audits resumed jobs.
  • Event-Driven Throttling: UI updates strictly on stage transitions or 10% jumps.
  • Stealth Termux Logger: ANSI-powered, flicker-free static command center.
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
from enum import Enum
from pathlib import Path
import urllib.parse
import primp
import yt_dlp
from yt_dlp.networking.impersonate import ImpersonateTarget

try:
    loop = asyncio.get_running_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, ForceReply
from pyrogram.errors import FloodWait, MessageNotModified
from logging.handlers import RotatingFileHandler
import config

# ──────────────────────────── CONFIG & LOGGING ─────────────────────────────

BASE_DIR = Path("SysCache")
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "engine.log"

BOT_DOWNLOAD_DIR = Path("Bot_Download")
BOT_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(name)s – %(message)s",
    handlers=[
        RotatingFileHandler(LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
# Suppress standard logging to stdout to preserve the ANSI UI
logging.getLogger().handlers[1].setLevel(logging.CRITICAL) 
log = logging.getLogger("stealth_bot")
logging.getLogger("pyrogram").setLevel(logging.ERROR)

API_ID   = config.API_ID
API_HASH = config.API_HASH
BOT_TOKEN = config.BOT_TOKEN
CHANNEL_ID = config.CHANNEL_ID
OWNER_ID = int(config.OWNER_ID) if hasattr(config, "OWNER_ID") else 0
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

JOBS_DIR  = BASE_DIR / "jobs"
VAULT_DIR = BASE_DIR / "pending_uploads"
DONE_DIR  = BASE_DIR / "completed"
FAIL_DIR  = BASE_DIR / "failed"
for d in (JOBS_DIR, VAULT_DIR, DONE_DIR, FAIL_DIR):
    d.mkdir(parents=True, exist_ok=True)

MAX_DL_WORKERS = 3
MAX_RETRIES    = 3

# ──────────────────────────── GLOBAL STATE ─────────────────────────────────

_live_progress: dict[str, dict] = {}
_active_procs:  dict[str, subprocess.Process] = {}

_dashboard_msg_id:  int = 0
_dashboard_chat_id: int = 0
_dashboard_tab: str = "root" # "root", "dl", "enc", "up"
_last_completed: str = "—"
_pending_confirmations: dict[str, dict] = {}

dl_queue  = asyncio.Queue()
enc_queue = asyncio.Queue()
up_queue  = asyncio.Queue()

# ANSI Colors
C_CYAN   = "\033[36m"
C_YELLOW = "\033[33m"
C_RED    = "\033[31m"
C_GREEN  = "\033[32m"
C_RESET  = "\033[0m"
C_BOLD   = "\033[1m"

def make_bar(percent: float, width: int = 10) -> str:
    filled = int(max(0.0, min(percent, 100.0)) / (100.0 / width))
    return "█" * filled + "░" * (width - filled)

# ──────────────────────────── STAGE & JOB ──────────────────────────────────

class Stage(str, Enum):
    QUEUED      = "queued"
    RESOLVING   = "resolving"
    DOWNLOADING = "downloading"
    DOWNLOADED  = "downloaded"
    ENCODING    = "encoding"
    ENCODED     = "encoded"
    UPLOADING   = "uploading"
    COMPLETED   = "completed"
    FAILED      = "failed"
    CANCELLED   = "cancelled"

class Job:
    def __init__(self, job_id: str):
        self.job_id   = job_id
        self.root     = JOBS_DIR / f"JOB_{job_id}"
        self.dl_dir   = self.root / "dl"
        self.enc_dir  = self.root / "enc"
        self.thumb_dir = self.root / "thumb"

    def init_dirs(self):
        for d in (self.root, self.dl_dir, self.enc_dir, self.thumb_dir):
            d.mkdir(parents=True, exist_ok=True)

    @property
    def meta_path(self)  -> Path: return self.root / "meta.json"
    @property
    def state_path(self) -> Path: return self.root / "state.json"
    @property
    def log_path(self)   -> Path: return self.root / "trace.log"

    def update_state(self, stage: Stage, data: dict = None, retries: int = 0):
        d = json.loads(self.state_path.read_text()) if self.state_path.exists() else {"stage": Stage.QUEUED.value, "retries": 0, "data": {}}
        d["stage"]   = stage.value
        d["retries"] = retries
        if data: d["data"] = data
        self.state_path.write_text(json.dumps(d, indent=2))

    def get_state(self) -> dict:
        if self.state_path.exists():
            try: return json.loads(self.state_path.read_text())
            except Exception: pass
        return {"stage": "unknown", "retries": 0, "data": {}}

    def write_log(self, msg: str):
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")

    def check_cancelled(self) -> bool:
        if not self.state_path.exists(): return False
        try: return json.loads(self.state_path.read_text()).get("stage") == Stage.CANCELLED.value
        except Exception: return False

def ensure_progress(job_id: str, default_stage: str, default_status: str):
    if job_id not in _live_progress:
        j = Job(job_id)
        meta = json.loads(j.meta_path.read_text()) if j.meta_path.exists() else {}
        _live_progress[job_id] = {
            "stage": default_stage, "pct": 0.0, "last_ui_pct": -10.0, 
            "last_ui_stage": "", "title": meta.get("title", "Media Asset"),
            "status": default_status, "tracker_id": meta.get("tracker_id"), 
            "chat_id": meta.get("chat_id", OWNER_ID)
        }

# ──────────────────────────── UI HELPERS ───────────────────────────────────

def _job_tracker_text(job_id: str) -> str:
    """Minimalist Monospaced Job Card."""
    data  = _live_progress.get(job_id, {})
    stage = data.get("stage", "working").upper()
    pct   = data.get("pct", 0.0)
    title = data.get("title", job_id)[:30]
    bar   = make_bar(pct, 12)
    return (
        f"`[ ⚡ ] ＴＡＳＫ :` `{title}`\n"
        f"`[ ⚙️ ] ＳＴＡＴ :` `{stage}`\n"
        f"`[ 📊 ] ＰＲＯＧ :` `[{bar}] {pct:.1f}%`"
    )

def _job_tracker_kb(job_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("📄 LOGS", callback_data=f"joblog|{job_id}"),
        InlineKeyboardButton("❌ CANCEL", callback_data=f"kill|{job_id}"),
        InlineKeyboardButton("🗑️ DELETE", callback_data=f"del|{job_id}"),
    ]])

async def _safe_edit(app: Client, chat_id: int, msg_id: int, text: str, kb: InlineKeyboardMarkup = None):
    try: await app.edit_message_text(chat_id, msg_id, text, reply_markup=kb)
    except MessageNotModified: pass
    except FloodWait as e: await asyncio.sleep(e.value)
    except Exception: pass

# ──────────────────────────── ACCORDION DASHBOARD ────────────────────────────

def _build_dashboard_text(tab: str) -> str:
    active_count = len(_live_progress)
    wait_dl = dl_queue.qsize()
    wait_enc = enc_queue.qsize()
    wait_up = up_queue.qsize()
    
    text = f"🖥 **STEALTH MAINFRAME**\n{'═' * 28}\n\n"
    
    if tab == "root":
        text += (
            f"**[ 📡 TELEMETRY ]**\n"
            f"  ├ Active Workers : `{active_count}`\n"
            f"  ├ Wait Download  : `{wait_dl}`\n"
            f"  ├ Wait Encode    : `{wait_enc}`\n"
            f"  └ Wait Upload    : `{wait_up}`\n\n"
            f"**[ 🏁 LATEST ]**\n  └ `{_last_completed[:35]}`"
        )
    else:
        text += f"**[ 📂 VIEW: {tab.upper()} ]**\n"
        found = False
        for jid, data in list(_live_progress.items()):
            stage = data.get("stage", "").lower()
            if (tab == "dl" and "download" in stage) or (tab == "enc" and "process" in stage or "enc" in stage) or (tab == "up" and "upload" in stage):
                found = True
                pct = data.get('pct', 0.0)
                text += f"  ├ `{data.get('title')[:20]}`\n  └ `[{make_bar(pct, 8)}] {pct:.1f}%`\n"
        if not found: text += "  └ _Empty_"

    return text

def _build_dashboard_kb(tab: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 SUMMARY" if tab != "root" else "🔘 SUMMARY", callback_data="dash|root")
        ],
        [
            InlineKeyboardButton(f"📥 DL ({dl_queue.qsize()})", callback_data="dash|dl"),
            InlineKeyboardButton(f"⚙️ ENC ({enc_queue.qsize()})", callback_data="dash|enc"),
            InlineKeyboardButton(f"📤 UP ({up_queue.qsize()})", callback_data="dash|up"),
        ],
        [
            InlineKeyboardButton("🔗 New URL", callback_data="ui|download"),
            InlineKeyboardButton("📄 Sys Logs", callback_data="ui|log"),
        ]
    ])

# ──────────────────────────── URL CONFIRM CARD ─────────────────────────────

async def _send_confirm_card(app: Client, chat_id: int, url: str, title_hint: str):
    token = str(uuid.uuid4())[:8]
    _pending_confirmations[token] = {"url": url, "chat_id": chat_id, "title": title_hint}
    domain = urllib.parse.urlparse(url).netloc or url[:30]
    text = f"🔗 **NEW VECTOR INTERCEPTED**\n{'─' * 28}\n**SRC:** `{domain}`\n**TAG:** `{title_hint[:40]}`"
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📺 1080p", callback_data=f"confirm|{token}|1080"), InlineKeyboardButton("📺 720p", callback_data=f"confirm|{token}|720")],
        [InlineKeyboardButton("✖ ABORT", callback_data=f"confirm|{token}|cancel")]
    ])
    await app.send_message(chat_id, text, reply_markup=kb)

# ──────────────────────────── CORE ENGINES ─────────────────────────────────

async def download_aria2c(url: str, job_id: str, job: Job) -> Path:
    job.write_log("Spawning Aria2c Engine...")
    cmd = ["aria2c", "-d", str(job.dl_dir), "-c", "-x", "16", "-s", "10", "--seed-time=0", "--summary-interval=0", "--file-allocation=none", url]
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=subprocess.STDOUT)
    _active_procs[job_id] = proc
    try:
        while True:
            chunk = await proc.stdout.readline()
            if not chunk: break
            line = chunk.decode("utf-8", errors="ignore")
            if "%" in line and job_id in _live_progress:
                m = re.search(r"\((\d+)%\)", line)
                if m: _live_progress[job_id]["pct"] = float(m.group(1))
    finally:
        await proc.wait()
        _active_procs.pop(job_id, None)

    if job.check_cancelled(): raise ValueError("KILL_SWITCH")
    files = [f for f in job.dl_dir.rglob("*") if f.is_file() and f.suffix.lower() in [".mp4", ".mkv", ".avi", ".ts", ".webm"]]
    if not files: raise RuntimeError("Aria2c wrote zero fragments.")
    return max(files, key=lambda p: p.stat().st_size)

async def run_custom_workflow(url: str, job_id: str) -> tuple[str, str, str]:
    job = Job(job_id)
    try:
        client = primp.Client(impersonate="chrome_120")
        resp = client.get(url, headers={"User-Agent": USER_AGENT})
        match = re.search(r"(https?://[^\"']+(?:\.m3u8|\.mp4)[^\"']*)", resp.text)
        if match: return match.group(1).replace(r"\/", "/"), url, ""
    except Exception as e:
        job.write_log(f"Native parser fault:\n{traceback.format_exc()}")
    return url, url, ""

def download_waterfall_fallback(target_url: str, job_id: str, referer: str, cookie_str: str, quality: str = "best") -> Path:
    job = Job(job_id)
    out_tmpl = str(job.dl_dir / f"{job_id}.%(ext)s")
    def prog_hook(d):
        if job.check_cancelled(): raise ValueError("KILL_SWITCH")
        if d.get("status") == "downloading" and job_id in _live_progress:
            try: _live_progress[job_id]["pct"] = float(re.sub(r"\x1b[^m]*m", "", d.get("_percent_str", "0.0%")).replace("%", "").strip())
            except Exception: pass

    fmt = "bestvideo[height<=1080]+bestaudio/best[height<=1080]" if quality == "1080" else "bestvideo[height<=720]+bestaudio/best[height<=720]" if quality == "720" else "bestvideo+bestaudio/best"
    opts = {"outtmpl": out_tmpl, "format": fmt, "http_headers": {"Referer": referer, "User-Agent": USER_AGENT}, "progress_hooks": [prog_hook], "quiet": True}
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(target_url, download=True)
        return Path(info.get("requested_downloads", [{}])[0].get("filepath") or ydl.prepare_filename(info))

# ──────────────────────────── PIPELINE FAILURE ─────────────────────────────

async def handle_pipeline_failure(app: Client, job: Job, exc: Exception):
    job.update_state(Stage.FAILED, retries=MAX_RETRIES)
    tb_str = traceback.format_exc()
    job.write_log(f"CRITICAL FAULT (3 Strikes):\n{tb_str}")

    meta = json.loads(job.meta_path.read_text()) if job.meta_path.exists() else {}
    chat_id = meta.get("chat_id", OWNER_ID)
    
    caption = f"🚨 **SYSTEM FAULT**\n`{job.job_id}` collapsed after 3 retries.\nError: `{str(exc)[:100]}`"
    if job.log_path.exists():
        try: await app.send_document(chat_id, document=str(job.log_path), caption=caption)
        except Exception: pass
    
    if meta.get("tracker_id"):
        await _safe_edit(app, chat_id, meta["tracker_id"], f"❌ **FAILED**\n`{str(exc)[:50]}`", InlineKeyboardMarkup([[InlineKeyboardButton("📄 LOGS", callback_data=f"joblog|{job.job_id}")]]))

# ──────────────────────────── WORKERS ──────────────────────────────────────

async def dl_worker(app: Client):
    while True:
        job_id = await dl_queue.get()
        job = Job(job_id)
        retry = job.get_state().get("retries", 0)

        try:
            if job.check_cancelled(): raise InterruptedError("KILL_SWITCH")
            ensure_progress(job_id, "Downloading", "Routing")
            job.update_state(Stage.DOWNLOADING, retries=retry)
            meta = json.loads(job.meta_path.read_text())
            url, source = meta.get("url"), meta.get("source")

            if not [f for f in job.dl_dir.glob("*") if f.is_file() and f.stat().st_size > 1024 * 1024]:
                if source == "telegram":
                    async def tg_prog(c, t): 
                        if job_id in _live_progress: _live_progress[job_id]["pct"] = (c * 100 / t) if t else 0
                    await app.download_media(meta.get("file_id"), file_name=str(job.dl_dir / f"{job_id}.mp4"), progress=tg_prog)
                else:
                    _live_progress[job_id]["stage"] = "Resolving"
                    actual_url, referer, cookie = await run_custom_workflow(url, job_id)
                    _live_progress[job_id]["stage"] = "Downloading"
                    
                    if actual_url.endswith(".mp4"): await download_aria2c(actual_url, job_id, job)
                    else: await asyncio.to_thread(download_waterfall_fallback, actual_url, job_id, referer, cookie, meta.get("quality", "best"))

            job.update_state(Stage.DOWNLOADED, retries=0)
            if job_id in _live_progress: _live_progress[job_id]["stage"] = "Wait Process"
            await enc_queue.put(job_id)

        except Exception as e:
            if "KILL_SWITCH" not in str(e):
                retry += 1
                job.write_log(f"DL Strike {retry}:\n{traceback.format_exc()}")
                if retry >= MAX_RETRIES: await handle_pipeline_failure(app, job, e)
                else: job.update_state(Stage.QUEUED, retries=retry); await dl_queue.put(job_id)
        finally: dl_queue.task_done()

async def enc_worker(app: Client):
    while True:
        job_id = await enc_queue.get()
        job = Job(job_id)
        retry = job.get_state().get("retries", 0)

        try:
            if job.check_cancelled(): raise InterruptedError("KILL_SWITCH")
            ensure_progress(job_id, "Encoding", "Remuxing")
            job.update_state(Stage.ENCODING, retries=retry)

            dl_file = max([f for f in job.dl_dir.glob("*") if f.is_file()], key=lambda p: p.stat().st_size)
            enc_file, thumb_file = job.enc_dir / f"{job_id}.mp4", job.thumb_dir / f"{job_id}.jpg"

            if not enc_file.exists() or enc_file.stat().st_size < 1024:
                await asyncio.create_subprocess_exec("ffmpeg", "-y", "-i", str(dl_file), "-ss", "00:00:02", "-vframes", "1", str(thumb_file), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                proc = await asyncio.create_subprocess_exec("ffmpeg", "-y", "-nostdin", "-i", str(dl_file), "-c:v", "copy", "-c:a", "aac", "-movflags", "+faststart", str(enc_file), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                _active_procs[job_id] = proc; await proc.wait(); _active_procs.pop(job_id, None)

                if job.check_cancelled(): raise ValueError("KILL_SWITCH")

            job.update_state(Stage.ENCODED, retries=0)
            if job_id in _live_progress: _live_progress[job_id]["stage"] = "Wait Upload"
            await up_queue.put(job_id)

        except Exception as e:
            if "KILL_SWITCH" not in str(e):
                retry += 1
                job.write_log(f"ENC Strike {retry}:\n{traceback.format_exc()}")
                if retry >= MAX_RETRIES: await handle_pipeline_failure(app, job, e)
                else: job.update_state(Stage.DOWNLOADED, retries=retry); await enc_queue.put(job_id)
        finally: enc_queue.task_done()

async def up_worker(app: Client):
    global _last_completed
    while True:
        job_id = await up_queue.get()
        job = Job(job_id)
        retry = job.get_state().get("retries", 0)

        try:
            if job.check_cancelled(): raise InterruptedError("KILL_SWITCH")
            ensure_progress(job_id, "Uploading", "Deploying")
            job.update_state(Stage.UPLOADING, retries=retry)

            enc_file, thumb_file = job.enc_dir / f"{job_id}.mp4", job.thumb_dir / f"{job_id}.jpg"
            meta = json.loads(job.meta_path.read_text())
            w, h, dur = 1280, 720, 100

            async def up_prog(c, t): 
                if job_id in _live_progress: _live_progress[job_id]["pct"] = (c * 100 / t) if t else 0
            await app.send_video(CHANNEL_ID, video=str(enc_file), thumb=str(thumb_file) if thumb_file.exists() else None, caption=meta.get("title", "Asset"), width=w, height=h, duration=dur, supports_streaming=True, progress=up_prog)

            _last_completed = meta.get("title", job_id)[:40]
            job.update_state(Stage.COMPLETED)

            if meta.get("tracker_id"):
                await _safe_edit(app, meta.get("chat_id", OWNER_ID), meta["tracker_id"], f"`[ ⚡ ] ＴＡＳＫ :` `{meta.get('title', '')[:30]}`\n`[ ✅ ] ＳＴＡＴ :` `COMPLETED`", InlineKeyboardMarkup([[InlineKeyboardButton("🗑️ DELETE CACHE", callback_data=f"del|{job_id}")]]))
            _live_progress.pop(job_id, None)
            try: shutil.move(str(job.root), str(DONE_DIR / job.root.name))
            except Exception: pass

        except Exception as e:
            if "KILL_SWITCH" not in str(e):
                retry += 1
                job.write_log(f"UP Strike {retry}:\n{traceback.format_exc()}")
                if retry >= MAX_RETRIES: await handle_pipeline_failure(app, job, e)
                else: job.update_state(Stage.ENCODED, retries=retry); await up_queue.put(job_id)
        finally: up_queue.task_done()

# ──────────────────────────── BOOT & RECOVERY ───────────────────────────────

async def recover_pending_jobs(app: Client):
    resumed = []
    if not JOBS_DIR.exists(): return
    for folder in JOBS_DIR.iterdir():
        if not (folder.is_dir() and folder.name.startswith("JOB_")): continue
        jid = folder.name.replace("JOB_", ""); job = Job(jid)
        phase = job.get_state().get("stage", "")
        if phase in [Stage.COMPLETED.value, Stage.FAILED.value, Stage.CANCELLED.value]: continue
        
        meta = json.loads(job.meta_path.read_text()) if job.meta_path.exists() else {}
        title = meta.get("title", jid)[:25]
        has_temp = any(f.suffix.lower() in [".part", ".aria2"] for f in (list(job.dl_dir.glob("*")) if job.dl_dir.exists() else []))

        if has_temp or phase in [Stage.QUEUED.value, Stage.RESOLVING.value, Stage.DOWNLOADING.value]:
            job.update_state(Stage.QUEUED, retries=0); dl_queue.put_nowait(jid); resumed.append((jid, title, "DL"))
        elif phase in [Stage.DOWNLOADED.value, Stage.ENCODING.value]:
            job.update_state(Stage.DOWNLOADED, retries=0); enc_queue.put_nowait(jid); resumed.append((jid, title, "ENC"))
        elif phase in [Stage.ENCODED.value, Stage.UPLOADING.value]:
            job.update_state(Stage.ENCODED, retries=0); up_queue.put_nowait(jid); resumed.append((jid, title, "UP"))

    if resumed and OWNER_ID:
        report = "🔄 **RESUME AUDITOR**\n" + "\n".join(f"  ├ `[{stage}]` `{title}`" for jid, title, stage in resumed)
        try: await app.send_message(OWNER_ID, report)
        except Exception: pass

# ──────────────────────────── LOOPS ────────────────────────────────────────

async def dashboard_loop(app: Client):
    global _dashboard_msg_id, _dashboard_chat_id
    while True:
        await asyncio.sleep(3)
        
        # 10% Throttled UI Updates for Job Cards
        for jid, data in list(_live_progress.items()):
            tid = data.get("tracker_id")
            if not tid: continue
            
            curr_pct, curr_stage = data.get("pct", 0.0), data.get("stage", "")
            last_pct, last_stage = data.get("last_ui_pct", 0.0), data.get("last_ui_stage", "")
            
            if curr_stage != last_stage or (curr_pct - last_pct) >= 10.0:
                await _safe_edit(app, data.get("chat_id", OWNER_ID), tid, _job_tracker_text(jid), _job_tracker_kb(jid))
                _live_progress[jid]["last_ui_pct"] = curr_pct
                _live_progress[jid]["last_ui_stage"] = curr_stage

        if _dashboard_msg_id and _dashboard_chat_id:
            await _safe_edit(app, _dashboard_chat_id, _dashboard_msg_id, _build_dashboard_text(_dashboard_tab), _build_dashboard_kb(_dashboard_tab))

async def terminal_dashboard_loop():
    sys.stdout.write("\033[2J") # Initial clear
    while True:
        await asyncio.sleep(2)
        sys.stdout.write("\033[H") # Cursor home (no flicker)
        
        sys.stdout.write(f"{C_CYAN}{C_BOLD}=== STEALTH MAINFRAME [LIVE] ==={C_RESET}\n")
        sys.stdout.write(f"CPU: {C_YELLOW}Active{C_RESET} | DL: {dl_queue.qsize()} | ENC: {enc_queue.qsize()} | UP: {up_queue.qsize()}\n")
        sys.stdout.write(f"{'─' * 40}\n")
        
        if not _live_progress:
            sys.stdout.write(f"{C_GREEN}System Idle. Awaiting vectors.{C_RESET}\033[K\n")
        else:
            for jid, data in list(_live_progress.items())[:5]:
                stage, pct = data.get('stage', '?'), data.get('pct', 0.0)
                color = C_YELLOW if "download" in stage.lower() else C_CYAN if "enc" in stage.lower() else C_GREEN
                sys.stdout.write(f"{C_BOLD}[{data.get('title', jid)[:15]}]{C_RESET} {color}{stage}{C_RESET} | [{make_bar(pct, 10)}] {pct:.1f}%\033[K\n")
        
        # Clear remaining old lines
        for _ in range(5): sys.stdout.write("\033[K\n")
        sys.stdout.flush()

# ──────────────────────────── HANDLERS ─────────────────────────────────────

def _setup_handlers(app: Client):
    @app.on_message(filters.command(["start", "dashboard"]) & filters.user(OWNER_ID))
    async def init_dashboard(_, msg: Message):
        global _dashboard_msg_id, _dashboard_chat_id, _dashboard_tab
        _dashboard_chat_id, _dashboard_tab = msg.chat.id, "root"
        m = await msg.reply(_build_dashboard_text(_dashboard_tab), reply_markup=_build_dashboard_kb(_dashboard_tab))
        _dashboard_msg_id = m.id
        try: await m.pin(disable_notification=True)
        except Exception: pass

    @app.on_message((filters.video | filters.document) & filters.user(OWNER_ID))
    async def auto_catch_media(_, msg: Message):
        if msg.document and msg.document.mime_type and not msg.document.mime_type.startswith("video/"): return
        title, job_id = (msg.caption.strip() if msg.caption else "Direct Media Upload"), str(uuid.uuid4())[:8]
        
        tracker = await msg.reply(f"`[ ⚡ ] ＴＡＳＫ :` `{title[:30]}`\n`[ ⚙️ ] ＳＴＡＴ :` `QUEUED`\n`[ 📊 ] ＰＲＯＧ :` `[░░░░░░░░░░░░] 0.0%`", reply_markup=_job_tracker_kb(job_id))
        j = Job(job_id); j.init_dirs(); j.update_state(Stage.QUEUED)
        j.meta_path.write_text(json.dumps({"url": "telegram_bridge", "title": title, "tracker_id": tracker.id, "chat_id": msg.chat.id, "source": "telegram", "file_id": msg.video.file_id if msg.video else msg.document.file_id}))
        await dl_queue.put(job_id)
        try: await msg.delete()
        except: pass

    @app.on_message(filters.text & filters.user(OWNER_ID) & ~filters.command(["start", "dashboard"]))
    async def native_link_catcher(_, msg: Message):
        url = next((w for w in msg.text.split() if w.startswith("http") or w.startswith("magnet:?")), None)
        if url: await _send_confirm_card(app, msg.chat.id, url, msg.text.replace(url, "").strip() or url[:40])

    @app.on_callback_query(filters.user(OWNER_ID))
    async def callback_router(_, cb):
        parts = cb.data.split("|"); action = parts[0]

        if action == "dash":
            global _dashboard_tab; _dashboard_tab = parts[1]
            await _safe_edit(app, _dashboard_chat_id, _dashboard_msg_id, _build_dashboard_text(_dashboard_tab), _build_dashboard_kb(_dashboard_tab))
            await cb.answer()

        elif action == "confirm":
            token, quality = parts[1], parts[2]
            pending = _pending_confirmations.pop(token, None)
            if not pending: return await cb.answer("Expired.", show_alert=True)
            if quality == "cancel": return await cb.message.delete()
            
            job_id, title_hint = str(uuid.uuid4())[:8], pending.get("title", "Asset")
            tracker = await cb.message.edit_text(f"`[ ⚡ ] ＴＡＳＫ :` `{title_hint[:30]}`\n`[ ⚙️ ] ＳＴＡＴ :` `QUEUED`\n`[ 📊 ] ＰＲＯＧ :` `[░░░░░░░░░░░░] 0.0%`", reply_markup=_job_tracker_kb(job_id))
            
            j = Job(job_id); j.init_dirs(); j.update_state(Stage.QUEUED)
            j.meta_path.write_text(json.dumps({"url": pending["url"], "title": title_hint, "tracker_id": tracker.id, "chat_id": pending["chat_id"], "source": "Direct", "quality": quality}))
            await dl_queue.put(job_id); await cb.answer("Queued.")

        elif action == "joblog":
            log_path = JOBS_DIR / f"JOB_{parts[1]}" / "trace.log"
            if not log_path.exists(): log_path = DONE_DIR / f"JOB_{parts[1]}" / "trace.log"
            if log_path.exists(): await cb.message.reply_document(str(log_path))
            else: await cb.answer("No logs.", show_alert=True)

        elif action == "kill":
            jid = parts[1]
            Job(jid).update_state(Stage.CANCELLED)
            if jid in _active_procs:
                try: _active_procs[jid].kill()
                except Exception: pass
            shutil.rmtree(JOBS_DIR / f"JOB_{jid}", ignore_errors=True)
            _live_progress.pop(jid, None)
            try: await cb.message.edit_text(f"`[ ❌ ] ＡＢＯＲＴＥＤ :` `{jid}`")
            except Exception: pass
            await cb.answer("Killed.")

        elif action == "del":
            shutil.rmtree(JOBS_DIR / f"JOB_{parts[1]}", ignore_errors=True)
            shutil.rmtree(DONE_DIR / f"JOB_{parts[1]}", ignore_errors=True)
            await cb.answer("Cache Cleared.")

# ──────────────────────────── BOOTSTRAP ────────────────────────────────────

async def main():
    app = Client("stealth_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
    _setup_handlers(app)

    async with app:
        # Boot Sequence & Pin
        global _dashboard_msg_id, _dashboard_chat_id
        if OWNER_ID:
            m = await app.send_message(OWNER_ID, "🟢 Booting Stealth Mainframe...")
            _dashboard_chat_id, _dashboard_msg_id = m.chat.id, m.id
            try: await m.pin(disable_notification=True)
            except Exception: pass
            await _safe_edit(app, _dashboard_chat_id, _dashboard_msg_id, _build_dashboard_text(_dashboard_tab), _build_dashboard_kb(_dashboard_tab))

        await recover_pending_jobs(app)
        
        for _ in range(MAX_DL_WORKERS): asyncio.create_task(dl_worker(app))
        asyncio.create_task(enc_worker(app))
        asyncio.create_task(up_worker(app))
        asyncio.create_task(dashboard_loop(app))
        asyncio.create_task(terminal_dashboard_loop())

        while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: sys.exit(0)