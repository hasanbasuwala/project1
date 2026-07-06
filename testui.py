"""
stealth_bot.py - v13.3 (The Complete Structured Monolith)
───────────────────────────────────────────────────────────────
ARCHITECTURE:
  • Single-file Micro-Orchestration (Classes).
  • JobScheduler (SQLite + asyncio.Lock).
  • High-Speed Memory Bridge for Termux UI.
  • Full Accordion Dashboard & ANSI Logger.
  • Auto-Ejecting Recovery Pool Dashboard.
  • FFprobe Metadata & Channel Uploader.
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
from yt_dlp.networking.impersonate import ImpersonateTarget

try:
    loop = asyncio.get_running_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
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
logging.getLogger().handlers[1].setLevel(logging.CRITICAL)
log = logging.getLogger("stealth_bot")
logging.getLogger("pyrogram").setLevel(logging.ERROR)

API_ID, API_HASH, BOT_TOKEN, CHANNEL_ID = config.API_ID, config.API_HASH, config.BOT_TOKEN, config.CHANNEL_ID
OWNER_ID = int(config.OWNER_ID) if hasattr(config, "OWNER_ID") else 0
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

JOBS_DIR, DONE_DIR = BASE_DIR / "jobs", BASE_DIR / "completed"
for d in (JOBS_DIR, DONE_DIR): d.mkdir(parents=True, exist_ok=True)

MAX_DL_WORKERS, MAX_RETRIES = 20, 3

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
            # 1. Create the table if it's a completely fresh install
            conn.execute('''CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY, url TEXT, title TEXT, source TEXT, quality TEXT, strategy TEXT,
                stage TEXT, pct REAL, last_ui_pct REAL, retries INTEGER, chat_id INTEGER, tracker_id INTEGER,
                recovered_at_stage TEXT DEFAULT NULL
            )''')
            
            # 2. Patch existing databases that are missing the new column
            try:
                conn.execute('ALTER TABLE jobs ADD COLUMN recovered_at_stage TEXT DEFAULT NULL')
            except sqlite3.OperationalError:
                # If the column already exists, SQLite throws an error. We just ignore it.
                pass

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

        if strategy in ["MAGNET", "DIRECT_MP4"]:
            await self._run_aria(url, jid, dl_dir)
            return
            
        # ─── 11-PASS WATERFALL ESCALATION FOR HLS & GENERIC ───
        
        # PASS 1-4: yt-dlp Standard & Variants
        variant_success = await self._attempt_ytdlp_variants(url, jid, dl_dir)
        if variant_success:
            return

        # PASS 5-7: Playwright Deep Extraction (DOM, Network, HAR) & Cookie Export
        self.db.log_trace(jid, "yt-dlp variants failed. Escalating to Playwright extraction...")
        playwright_data = await self._run_playwright_extraction(url, jid, dl_dir)
        
        if not playwright_data or not playwright_data.get('url'):
            raise RuntimeError("PASS 11 FAILED: All extraction methods exhausted. Target is highly protected.")

        extracted_url = playwright_data['url']
        headers = playwright_data['headers']
        raw_cookies = playwright_data['raw_cookies']
        cookie_str = playwright_data['cookie_str']

        self.db.log_trace(jid, "Playwright extraction successful. Delegating authorized payload downstream...")

        # PASS 8: FFmpeg Direct Stream Capture
        if ".m3u8" in extracted_url:
            self.db.log_trace(jid, "PASS 8: Attempting FFmpeg direct capture with exported cookies...")
            if await self._run_ffmpeg_capture(extracted_url, jid, dl_dir, headers, cookie_str):
                return
            self.db.log_trace(jid, "PASS 8 FAILED: FFmpeg direct stream capture aborted.")

        # PASS 9: yt-dlp with Exported Session Cookies (Netscape Format Bypass)
        self.db.log_trace(jid, "PASS 9: Attempting yt-dlp with exported Netscape cookiefile...")
        if await self._run_ytdlp_with_cookies(extracted_url, jid, dl_dir, headers, raw_cookies):
            return
        self.db.log_trace(jid, "PASS 9 FAILED: yt-dlp cookie authentication rejected.")

        # PASS 10: Aria2c Full Header Replay
        self.db.log_trace(jid, "PASS 10: Attempting Aria2c full header replay bypass...")
        try:
            full_headers = headers.copy()
            if cookie_str:
                full_headers["Cookie"] = cookie_str
            await self._run_aria(extracted_url, jid, dl_dir, headers=full_headers)
            return
        except Exception as e:
            self.db.log_trace(jid, f"PASS 10 FAILED: Aria2c bypass failed. Error: {e}")
            
        # PASS 11: Final Fail Handler
        raise RuntimeError("PASS 11 FAILED: CDNs are blocking TLS signatures on all vectors.")

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
        
        har_path = dl_dir / f"{jid}_intercept.har"
        extracted_payload = {"url": None, "headers": {}, "cookie_str": "", "raw_cookies": []}
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True, 
                args=[
                    "--no-sandbox", 
                    "--disable-setuid-sandbox",
                    "--disable-blink-features=AutomationControlled" 
                ]
            )
            
            context = await browser.new_context(
                user_agent=USER_AGENT, 
                record_har_path=str(har_path),
                viewport={"width": 1920, "height": 1080},
                locale="en-US"
            )
            page = await context.new_page()
            
            await Stealth().apply_stealth_async(page)

            found_urls = []
            capture_headers = {}

            # ─── UPGRADED: NETWORK RESPONSE SNIFFER ───
            async def handle_response(response):
                try:
                    req_url = response.url
                    # Actively sniff the network for media streams
                    if any(ext in req_url for ext in [".m3u8", ".mp4", ".ts", "video/mp4"]):
                        if "ads" not in req_url and "tracking" not in req_url:
                            found_urls.append(req_url)
                            # Capture the exact request headers that succeeded
                            req_headers = await response.request.all_headers()
                            capture_headers.update(req_headers)
                except Exception:
                    pass
            
            page.on("response", handle_response)
            # ──────────────────────────────────────────

            # Block useless assets to speed up extraction and prevent Cloudflare tracking
            async def handle_route(route):
                req = route.request
                if req.resource_type in ["image", "font", "stylesheet"] or any(x in req.url for x in ["ads", "tracking", "analytics"]):
                    await route.abort()
                else: 
                    await route.continue_()

            await page.route("**/*", handle_route)
            
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=45000)
                await page.wait_for_timeout(8000) 
                
                # SIMULATE HUMAN INTERACTION
                await page.evaluate('''() => {
                    window.scrollBy(0, 500);
                    let v = document.querySelector('video');
                    if(v) { 
                        v.muted = true; 
                        v.play().catch(()=>{}); 
                    } else {
                        // Click exactly in the center to trigger overlays/popunders
                        document.elementFromPoint(window.innerWidth/2, window.innerHeight/2)?.click();
                    }
                }''')
                # Wait longer for the video chunk to actually start streaming into the network tab
                await page.wait_for_timeout(6000) 
                
            except Exception as e:
                self.db.log_trace(jid, f"Playwright page load warning: {e}")

            # Grab the first valid media URL sniffed from the network
            if found_urls:
                m3u8s = [u for u in found_urls if ".m3u8" in u]
                extracted_payload["url"] = m3u8s[0] if m3u8s else found_urls[0]
            
            # Fallback to DOM extraction if network sniffing missed it
            if not extracted_payload["url"]:
                extracted_payload["url"] = await page.evaluate('''() => {
                    let v = document.querySelector('video'); if (v && v.src && !v.src.startsWith('blob:')) return v.src;
                    let s = document.querySelector('video source'); if (s && s.src && !s.src.startsWith('blob:')) return s.src;
                    let vdata = document.querySelector('video[data-src]'); if (vdata) return vdata.getAttribute('data-src');
                    return null;
                }''')

            if extracted_payload["url"]:
                raw_url = extracted_payload["url"]
                if raw_url.startswith("//"):
                    extracted_payload["url"] = "https:" + raw_url
                elif raw_url.startswith("/"):
                    from urllib.parse import urlparse
                    parsed = urlparse(page.url)
                    extracted_payload["url"] = f"{parsed.scheme}://{parsed.netloc}{raw_url}"
            else:
                self.db.log_trace(jid, "Playwright yielded no direct media. Passing original URL and cleared cookies to Downstream Extractors.")
                extracted_payload["url"] = url 

            cookies = await context.cookies()
            extracted_payload["raw_cookies"] = cookies
            extracted_payload["cookie_str"] = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
            
            extracted_payload["headers"] = {
                "Referer": page.url,
                "Origin": "/".join(page.url.split("/")[:3]),
                "User-Agent": USER_AGENT,
                "Accept": "*/*",
                "Connection": "keep-alive"
            }
            # Add the exact successful request headers we sniffed
            for k, v in capture_headers.items():
                if k.lower() not in ["host", "accept-encoding"]:
                    extracted_payload["headers"][k] = v

            await browser.close()
            return extracted_payload

    async def _run_ffmpeg_capture(self, url: str, jid: str, dl_dir: Path, headers: dict, cookie_str: str) -> bool:
        out_file = dl_dir / f"{jid}.mp4"
        header_arg = "".join([f"{k}: {v}\r\n" for k, v in headers.items()])
        if cookie_str: header_arg += f"Cookie: {cookie_str}\r\n"
        
        cmd = [
            "ffmpeg", "-y", "-headers", header_arg,
            "-i", url, "-c", "copy", "-bsf:a", "aac_adtstoasc", str(out_file)
        ]
        
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        _, stderr = await proc.communicate()
        
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

        # ─── FIX: RE-ARMING CHROME TLS IMPERSONATION ───
        opts = {
            "http_headers": headers,
            "cookiefile": str(cookie_path),
            "impersonate": ImpersonateTarget(client="chrome"), 
            "extractor_args": {"generic": ["impersonate"]} # Forces the generic extractor to mask its TLS
        }
        # ───────────────────────────────────────────────
            
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
        if custom_opts: opts.update(custom_opts)
            
        with yt_dlp.YoutubeDL(opts) as ydl: ydl.extract_info(url, download=True)

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

class EncoderEngine:
    def __init__(self, scheduler: JobScheduler):
        self.db = scheduler

    async def execute(self, job_data: dict):
        jid = job_data['id']
        dl_dir, enc_dir, thumb_dir = JOBS_DIR / f"JOB_{jid}" / "dl", JOBS_DIR / f"JOB_{jid}" / "enc", JOBS_DIR / f"JOB_{jid}" / "thumb"
        
        dl_files = [f for f in dl_dir.rglob("*") if f.is_file() and f.suffix.lower() in [".mp4", ".mkv", ".avi", ".ts", ".webm", ".flv", ".php"]]
        dl_file = max(dl_files, key=lambda p: p.stat().st_size)
        enc_file, thumb_file = enc_dir / f"{jid}.mp4", thumb_dir / f"{jid}.jpg"

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

class UploaderEngine:
    def __init__(self, db: JobScheduler, app: Client):
        self.db = db
        self.app = app

    async def execute(self, job_data: dict):
        jid = job_data['id']
        job_dir = JOBS_DIR / f"JOB_{jid}"
        enc_dir, dl_dir, thumb_dir = job_dir / "enc", job_dir / "dl", job_dir / "thumb"
        
        self.db.log_trace(jid, "Uploader Engine initialized.")
        await self.db.update_job(jid, stage="uploading", pct=0.0)

        # 1. Find the target file
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
        
        # 2. Extract Metadata and Thumbnail
        width, height, duration = await extract_video_metadata(target_file)
        thumb_file = thumb_dir / f"{jid}.jpg"
        thumb_path = str(thumb_file) if thumb_file.exists() else None

        # 3. Pyrogram Progress Hook
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

        # 4. Execute the Upload (Targeting CHANNEL_ID with full metadata)
        caption = f"**{job_data['title']}**"
        
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
        
        self.db.log_trace(jid, "Upload sequence complete. Running final UI cleanup...")

        # 5. Final UI Freeze & Cleanup in the tracking chat
        try:
            latest_job = await self.db.get_job(jid)
            if latest_job and latest_job.get('tracker_id'):
                final_text = (
                    f"`[❖] ＴＡＳＫ :` `{latest_job['title'][:18]}..`\n"
                    f"`━━━━━━━━━━━━━━━━━━━━━━━━━━`\n"
                    f"`✅ PHASE : COMPLETED`\n"
                    f"`📤 ROUTE : CHANNEL_ID`\n"
                    f"`━━━━━━━━━━━━━━━━━━━━━━━━━━`"
                )
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("🗑️ DISMISS", callback_data=f"delmsg|{latest_job['tracker_id']}")]])
                await self.app.edit_message_text(latest_job['chat_id'], latest_job['tracker_id'], final_text, reply_markup=kb)
        except Exception as e:
            self.db.log_trace(jid, f"Failed to push final completion card: {e}")

        # 6. Nuke the database entry and wipe the hard drive allocation
        global _last_completed
        _last_completed = job_data['title']
        await self.db.delete_job(jid)
        shutil.rmtree(job_dir, ignore_errors=True)

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
            
            if stage in [Stage.QUEUED.value, Stage.DOWNLOADING.value] or "download" in stage:
                await db.update_job(jid, stage=Stage.QUEUED.value, recovered_at_stage=stage)
                dl_q.put_nowait(jid)
                resumed.append(f"  ├ `[DL]` `{title}`")
            elif stage in [Stage.DOWNLOADED.value, Stage.ENCODING.value] or "enc" in stage:
                await db.update_job(jid, stage=Stage.DOWNLOADED.value, recovered_at_stage=stage)
                enc_q.put_nowait(jid)
                resumed.append(f"  ├ `[ENC]` `{title}`")
            elif stage in [Stage.ENCODED.value, Stage.UPLOADING.value] or "upload" in stage:
                await db.update_job(jid, stage=Stage.ENCODED.value, recovered_at_stage=stage)
                up_q.put_nowait(jid)
                resumed.append(f"  ├ `[UP]` `{title}`")

        if resumed and OWNER_ID:
            try: await app.send_message(OWNER_ID, "🔄 **RESUME AUDITOR**\n" + "\n".join(resumed))
            except Exception: pass

# ──────────────────────────── PIPELINE MANAGER (Orchestrator) ───────────

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
                
                # SYS_OP: Wipe the recovery tag upon successful completion of the phase
                await self.db.update_job(jid, stage=success_stage.value, retries=0, recovered_at_stage=None)
                
                if next_q: await next_q.put(jid)
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

# ──────────────────────────── UI & COMMAND ROUTER ───────────────────────

from pyrogram.types import ForceReply

_dashboard_msg_id, _dashboard_chat_id, _dashboard_tab = 0, 0, "root"
_last_completed = "—"
_live_ui_text = {}

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

async def _get_dashboard_components(tab: str, db: JobScheduler, pipeline: PipelineManager) -> tuple[str, InlineKeyboardMarkup]:
    global _last_completed
    
    stage_tab = tab.split(":")[0] if ":" in tab else tab
    expanded_jid = tab.split(":")[1] if ":" in tab else None

    total_storage = sum(f.stat().st_size for f in JOBS_DIR.rglob("*") if f.is_file()) / (1024 ** 3)
    jobs = await db.get_active_jobs()
    
    # Isolate Recovery Jobs from Standard Jobs
    recovery_pool = [j for j in jobs if j.get('recovered_at_stage') is not None]
    standard_jobs = [j for j in jobs if j.get('recovered_at_stage') is None]

    def _base(stage_str):
        if not stage_str: return ""
        return stage_str.split("|")[0].strip().lower() if "|" in stage_str else stage_str.strip().lower()

    # Buckets for standard pipeline
    buckets = {
        "dl": [j for j in standard_jobs if _base(j['stage']) in ["queued", "downloading"]],
        "dl_done": [j for j in standard_jobs if _base(j['stage']) == "downloaded"],
        "enc": [j for j in standard_jobs if _base(j['stage']) in ["encoding", "process"]],
        "enc_done": [j for j in standard_jobs if _base(j['stage']) == "encoded"],
        "up": [j for j in standard_jobs if _base(j['stage']) == "uploading"]
    }

    # ─── NEW: DYNAMIC ACT LIST BUILDER ───
    act_text_blocks = []
    if not buckets['dl'] and not buckets['enc'] and not buckets['up']:
        act_text_blocks.append("`[🔄] ACT  :` `0 DL | 0 PR | 0 UP`")
    else:
        act_text_blocks.append("`[🔄] ACT  :`")
        counter = 1
        
        if buckets['dl']:
            act_text_blocks.append(f"`  {counter}. DL ({len(buckets['dl'])})`")
            for i, j in enumerate(buckets['dl'][:5]): # Capped at 5 for UI safety
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
    # ─────────────────────────────────────

    sync_stat = "`RECOVERY AUDIT ACTIVE`" if recovery_pool else "`SYSTEM NORMAL`"
    
    text = (
        f"💻 **STEALTH MAINFRAME v14**\n"
        f"`━━━━━━━━━━━━━━━━━━━━━━━━━━`\n"
        f"`[⚡] STAT :` `ONLINE & SECURE`\n"
        f"`[⚠️] SYNC :` {sync_stat}\n"
        f"`[💾] DISK :` `{total_storage:.2f} GB`\n"
        f"{act_string}\n"
        f"`[🏁] LAST :` `{_last_completed[:12]}`\n"
        f"`━━━━━━━━━━━━━━━━━━━━━━━━━━`\n"
        f"**Select a subsystem:**"
    )

    kb_lines = []

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
        is_rec_open = stage_tab in ["recovery", "rec_dl", "rec_enc", "rec_up"]
        kb_lines.append([InlineKeyboardButton(f"{'[-]' if is_rec_open else '[+]'} 🚨 RECOVERY POOL ({len(recovery_pool)})", callback_data=f"dash|{'root' if is_rec_open else 'recovery'}")])
        
        if is_rec_open:
            rec_dl = [j for j in recovery_pool if _base(j['recovered_at_stage']) in ["queued", "downloading", "downloaded"]]
            rec_enc = [j for j in recovery_pool if _base(j['recovered_at_stage']) in ["encoding", "encoded"]]
            rec_up = [j for j in recovery_pool if _base(j['recovered_at_stage']) == "uploading"]
            
            build_dropdown("rec_dl", "STALLED DOWNLOADS", "📥", rec_dl, parent_tab="recovery")
            build_dropdown("rec_enc", "STALLED PROCESSING", "⚙️", rec_enc, parent_tab="recovery")
            build_dropdown("rec_up", "STALLED UPLOADS", "📤", rec_up, parent_tab="recovery")
            
            kb_lines.append([InlineKeyboardButton("🗑️ PURGE ALL RECOVERED", callback_data="purge_recovery")])

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
            jid = str(uuid.uuid4())[:8]
            title = msg.text.replace(url, "").strip() or url[:40]
            tracker = await msg.reply(f"`[ ⚡ ] ＴＡＳＫ :` `{title[:30]}`\n`[ ⚙️ ] ＳＴＡＴ :` `QUEUED`", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ CANCEL", callback_data=f"kill|{jid}")]]))
            
            await db.create_job({"id": jid, "url": url, "title": title, "source": "Direct", "quality": "auto", "strategy": LinkClassifier.classify(url), "chat_id": msg.chat.id, "tracker_id": tracker.id})
            await pipeline.dl_q.put(jid)

    @app.on_callback_query()
    async def _router(client: Client, cb: CallbackQuery):
        global _dashboard_tab, _dashboard_msg_id, _dashboard_chat_id
        
        if cb.data == "noop":
            await cb.answer()
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
                f"✏️ **RENAME TASK:** `{jid}`\nReply to this exact message with the new file name.", 
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
                try: await cb.message.edit_text(f"💀 **TASK TERMINATED:** `JOB_{jid}`", reply_markup=None)
                except Exception: pass
            else:
                try:
                    text, kb = await _get_dashboard_components(_dashboard_tab, pipeline_ref.db, pipeline_ref)
                    await cb.message.edit_text(text, reply_markup=kb)
                except Exception: pass
            await cb.answer("Process terminated and payload destroyed.", show_alert=True)
            return

# ──────────────────────────── EVENT LOOPS ─────────────────────────────

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

_last_ui_stage = {}
_job_stats_history = {} 

async def ui_throttle_loop(app: Client, db: JobScheduler):
    global _dashboard_msg_id, _dashboard_chat_id, _dashboard_tab
    while True:
        await asyncio.sleep(3) 
        
        try:
            for job in await db.get_active_jobs():
                jid = job['id']
                if not job['tracker_id']: continue
                
                raw_stage = job['stage']
                base_phase = raw_stage.split("|")[0].strip().lower() if "|" in raw_stage else raw_stage.strip().lower()
                last_phase = _last_ui_stage.get(jid, "")
                
                last_pct = float(job['last_ui_pct']) if job['last_ui_pct'] is not None else -10.0
                current_pct = float(job['pct']) if job['pct'] is not None else 0.0

                if jid not in _job_stats_history:
                    _job_stats_history[jid] = {'speeds': [], 'etas': []}
                
                if "|" in raw_stage:
                    parts = [p.strip() for p in raw_stage.split("|")]
                    if len(parts) >= 3:
                        _job_stats_history[jid]['speeds'].append(_parse_speed(parts[1]))
                        _job_stats_history[jid]['etas'].append(_parse_eta(parts[2]))
                
                if (base_phase != last_phase) or (current_pct - last_pct) >= 10.0: 
                    
                    hist = _job_stats_history[jid]
                    avg_s = _format_speed(sum(hist['speeds']) / len(hist['speeds'])) if hist['speeds'] else None
                    avg_e = _format_eta(sum(hist['etas']) / len(hist['etas'])) if hist['etas'] else None

                    kb = InlineKeyboardMarkup([
                        [InlineKeyboardButton("📄 LOGS", callback_data=f"joblog|{jid}"), 
                         InlineKeyboardButton("❌ KILL", callback_data=f"kill|{jid}")]
                    ])
                    
                    await safe_edit(app, job['chat_id'], job['tracker_id'], _job_tracker_text(job, avg_s, avg_e), kb)
                    
                    await db.update_job(jid, last_ui_pct=current_pct)
                    _last_ui_stage[jid] = base_phase
                    _job_stats_history[jid] = {'speeds': [], 'etas': []} 
                    
            if _dashboard_msg_id and _dashboard_chat_id:
                text, kb = await _get_dashboard_components(_dashboard_tab, db, pipeline_ref)
                await safe_edit(app, _dashboard_chat_id, _dashboard_msg_id, text, kb)
                
        except FloodWait as e:
            await asyncio.sleep(e.value)
        except Exception: 
            pass

async def terminal_loop(db: JobScheduler, pipeline: PipelineManager):
    sys.stdout.write("\033[2J") 
    while True:
        await asyncio.sleep(1) 
        sys.stdout.write("\033[H") 
        sys.stdout.write(f"{C_CYAN}{C_BOLD}=== STEALTH MAINFRAME [LIVE] ==={C_RESET}\n")
        sys.stdout.write(f"QUEUES | DL: {pipeline.dl_q.qsize()} | ENC: {pipeline.enc_q.qsize()} | UP: {pipeline.up_q.qsize()}\n{'─' * 40}\n")
        
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
        
        sys.stdout.write("\033[J") 
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
    try: loop.run_until_complete(main())
    except KeyboardInterrupt: sys.exit(0)