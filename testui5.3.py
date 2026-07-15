"""
stealth_bot.py - v5.3
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
import aiohttp
import random
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

# ──────────────────────────── BATCH CONFIGURATION ─────────────────────
_batch_mode = False
_batch_collection = []
_pending_batches = asyncio.Queue()

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

async def get_random_free_proxy() -> str:
    """Fetches a random HTTP proxy from the Proxifly GitHub repository."""
    url = "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    text = await response.text()
                    proxies = [line.strip() for line in text.split('\n') if line.strip()]
                    if proxies:
                        selected = random.choice(proxies)
                        return f"http://{selected}"
    except Exception as e:
        print(f"Proxy fetch failed: {e}")
    return ""

# ──────────────────────────── SUBSYSTEM 3: ENGINES ──────────────────────

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
            
    async def _pre_download_validation(self, url: str, jid: str, headers: dict, cookie_str: str, proxy_url: str = None) -> bool:
        from curl_cffi.requests import AsyncSession
        self.db.log_trace(jid, "Performing pre-download hardened TLS validation via curl_cffi...")
        
        req_headers = headers.copy() if headers else {}
        if cookie_str:
            req_headers['Cookie'] = cookie_str
            
        proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
        
        try:
            async with AsyncSession(impersonate="chrome", proxies=proxies) as session:
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
                    
                if content_length > 0 and content_length < 100000:
                    self.db.log_trace(jid, "Pre-check failed: Content-Length suspiciously small (<100KB).")
                    return False
                    
                return True
        except Exception as e:
            self.db.log_trace(jid, f"Pre-check warning: Hardened ping encounter ({e}). Passing downstream to engine.")
            return True

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
            
        # --- PHASE 1: EXTRACTION WITH PROXY CASCADING ---
        playwright_data = self._load_cached_payload(dl_dir)
        used_proxy = None
        
        if not playwright_data:
            variant_success = await self._attempt_ytdlp_variants(url, jid, dl_dir)
            if variant_success:
                return

            self.db.log_trace(jid, "yt-dlp variants failed. Escalating to Playwright extraction...")
            
            # Try up to 3 different proxies before attempting a local fallback
            for attempt in range(3):
                proxy_url = await get_random_free_proxy()
                self.db.log_trace(jid, f"Extraction attempt {attempt + 1}/3 using proxy: {proxy_url}")
                
                try:
                    playwright_data = await self._run_playwright_extraction(url, jid, dl_dir, proxy_url)
                    if playwright_data and playwright_data.get('url'):
                        self.db.log_trace(jid, "Extraction successful via proxy.")
                        used_proxy = proxy_url
                        break
                except Exception as e:
                    self.db.log_trace(jid, f"Proxy attempt failed: {e}")
            else:
                # Absolute last resort fallback to local Wi-Fi
                self.db.log_trace(jid, "All proxies exhausted. Falling back to direct local Wi-Fi for extraction...")
                try:
                    playwright_data = await self._run_playwright_extraction(url, jid, dl_dir, proxy_url=None)
                except Exception as e:
                    raise RuntimeError(f"PASS 11 FAILED: Target is completely unreachable. Error: {e}")

            if not playwright_data or not playwright_data.get('url'):
                raise RuntimeError("PASS 11 FAILED: All extraction methods exhausted. Target is highly protected.")
            
            self._save_cached_payload(dl_dir, playwright_data)
            self.db.log_trace(jid, "Playwright extraction successful and payload state cached.")
        else:
            self.db.log_trace(jid, "Loaded cached Playwright payload. Bypassing browser extraction phases.")

        extracted_url = playwright_data['url']
        headers = playwright_data['headers']
        raw_cookies = playwright_data['raw_cookies']
        cookie_str = playwright_data['cookie_str']

        self.db.log_trace(jid, "Delegating authorized payload downstream...")

        # --- PHASE 2: INJECTED PRE-DOWNLOAD VALIDATION GATES ---
        is_valid_url = await self._pre_download_validation(extracted_url, jid, headers, cookie_str, proxy_url=None)
        
        # Cross-validate through the extraction proxy (IP Binding issue check)
        if not is_valid_url and used_proxy:
            self.db.log_trace(jid, "Local validation failed. Retrying validation gate through the extraction proxy (Checking IP Binding)...")
            is_valid_url = await self._pre_download_validation(extracted_url, jid, headers, cookie_str, proxy_url=used_proxy)

        if not is_valid_url:
            cache_file = self._get_payload_cache_path(dl_dir)
            if cache_file and cache_file.exists():
                try:
                    import os
                    os.remove(cache_file)
                    self.db.log_trace(jid, "Toxic payload cache purged.")
                except Exception:
                    pass
            raise RuntimeError("Pre-Download Validation Failed: Target URL points to HTML/Text, not a media file.")

        # --- PHASE 3: DOWNSTREAM ENGINES ---
        if ".m3u8" in extracted_url:
            self.db.log_trace(jid, "PASS 8: Attempting FFmpeg direct capture over local Wi-Fi...")
            if await self._run_ffmpeg_capture(extracted_url, jid, dl_dir, headers, cookie_str):
                return
            self.db.log_trace(jid, "PASS 8 FAILED: FFmpeg stream capture dropped.")

        self.db.log_trace(jid, "PASS 9: Attempting yt-dlp cookie bypass over direct local connection...")
        if await self._run_ytdlp_with_cookies(extracted_url, jid, dl_dir, headers, raw_cookies, proxy_url=None):
            return

        if used_proxy:
            self.db.log_trace(jid, f"PASS 9 (Retry): Local download failed. Tunneling yt-dlp through matching session proxy: {used_proxy}")
            if await self._run_ytdlp_with_cookies(extracted_url, jid, dl_dir, headers, raw_cookies, proxy_url=used_proxy):
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

    async def _run_playwright_extraction(self, url: str, jid: str, dl_dir: Path, proxy_url: str = None) -> dict:
        from playwright.async_api import async_playwright
        from playwright_stealth import Stealth 
        from urllib.parse import urlparse
        import shutil
        import os
        import time
        
        path_to_extension = "/root/stealth_bot_v13/uBOL-home-main/chromium"
        
        user_data_dir = f"/tmp/pw_data_{jid}_{int(time.time())}" 
        
        extracted_payload = {"url": None, "headers": {}, "cookie_str": "", "raw_cookies": []}
        found_urls = []
        capture_headers = {}

        proxy_config = {"server": proxy_url} if proxy_url else None
        
        args = [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-site-isolation-trials", 
            "--disable-web-security",          
            f"--disable-extensions-except={path_to_extension}",
            f"--load-extension={path_to_extension}"
        ]
        
        if not proxy_url:
            args.append("--no-proxy-server")
            
        async with async_playwright() as p:
            context = await p.chromium.launch_persistent_context(
                user_data_dir,
                headless=True,
                proxy=proxy_config,
                executable_path="/usr/bin/chromium",
                user_agent=USER_AGENT,
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
                args=args
            )
            
            page = context.pages[0]
            await Stealth().apply_stealth_async(page)
            await page.wait_for_timeout(2000) 

            async def handle_response(response):
                try:
                    req = response.request
                    url_lower = req.url.lower()
                    content_type = response.headers.get("content-type", "").lower()
                    
                    bad_keywords = ["google", "analytics", "/ad/", "/ads/", "?ad=", "&ad=", "beacon", "vast", "blank", "trailer", "promo"]
                    if any(bad in url_lower for bad in bad_keywords): return
                    
                    is_media = False
                    vtype = "mp4"
                    
                    if "mpegurl" in content_type or "application/x-mpegurl" in content_type or "vnd.apple.mpegurl" in content_type or ".m3u8" in url_lower:
                        is_media = True
                        vtype = "m3u8"
                    elif "video/" in content_type or "audio/mp4" in content_type or ".mp4" in url_lower or ".ts" in url_lower or ".m4s" in url_lower:
                        is_media = True
                        vtype = "mp4"
                    elif "application/octet-stream" in content_type and req.resource_type in ["media", "xhr", "fetch"]:
                        cl = int(response.headers.get("content-length", 0))
                        if cl > 100000: 
                            is_media = True
                            vtype = "mp4"
                        
                    if is_media:
                        found_urls.append({"type": vtype, "url": req.url})
                        headers = await req.all_headers()
                        capture_headers.update(headers)
                except Exception:
                    pass
                    
            page.on("response", handle_response)

            try:
                self.db.log_trace(jid, "Navigating to main target URL...")
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                
                try:
                    await page.evaluate("""() => {
                        const keywords = ['yes', 'enter', 'agree', '18', 'warning'];
                        document.querySelectorAll('a, button, div').forEach(el => {
                            if(el.innerText && keywords.some(k => el.innerText.toLowerCase().includes(k))) {
                                if(el.offsetHeight > 10 && el.offsetHeight < 100) {
                                    try { el.click(); } catch(e){}
                                }
                            }
                        });
                    }""")
                    await page.wait_for_timeout(1500)
                except Exception:
                    pass

                try:
                    fake_player = await page.wait_for_selector("div.vi-on, div.play, .play-button", state="visible", timeout=3000)
                    if fake_player:
                        self.db.log_trace(jid, "Clicking fake player overlay...")
                        await fake_player.click()
                        await page.wait_for_timeout(3000)
                except Exception:
                    pass

                try:
                    self.db.log_trace(jid, f"Scanning main page and {len(page.frames)} child frames for video players...")
                    jw_url = None
                    
                    iframes = await page.query_selector_all("iframe")
                    for iframe in iframes:
                        try:
                            await iframe.scroll_into_view_if_needed()
                            box = await iframe.bounding_box()
                            if box and box["width"] > 100 and box["height"] > 100:
                                await page.mouse.click(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
                                await page.wait_for_timeout(1000)
                        except Exception:
                            pass
                    
                    viewport = page.viewport_size
                    cx = viewport['width'] / 2
                    cy = viewport['height'] / 2
                    click_targets = [(cx, cy), (cx, cy + 100), (50, viewport['height'] - 50)]
                    
                    for x, y in click_targets:
                        await page.mouse.click(x, y)
                        await page.wait_for_timeout(800)
                    
                    await page.wait_for_timeout(6000) 
                    
                    frames_to_search = [page.main_frame] + page.frames
                    
                    for frame in frames_to_search:
                        if "google" in frame.url or "blank" in frame.url or "magsrv" in frame.url: continue
                        
                        try:
                            await frame.evaluate("document.querySelectorAll('.play-button, .vjs-big-play-button, video').forEach(b => b.click());")
                            await frame.evaluate("document.querySelectorAll('video').forEach(v => { v.muted = true; v.playbackRate = 16.0; });")
                            
                            res = await frame.evaluate('''() => {
                                try {
                                    const isBad = (url) => url.match(/trailer|promo|\\/ad\\/|\\/ads\\/|teaser/i);
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
                                    
                                    let dls = Array.from(document.querySelectorAll('a'));
                                    let dl = dls.find(a => (a.href && a.href.includes('.mp4')) || (a.innerText && a.innerText.toLowerCase().includes('download')));
                                    if (dl && dl.href && !isBad(dl.href)) return dl.href;
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
                            self.db.log_trace(jid, "Sniffer logs are empty. Extraction completely failed.")
                            
            except Exception as e:
                self.db.log_trace(jid, f"Playwright critical failure: {e}")

            media_url = extracted_payload.get("url")
            if not media_url:
                await context.close()
                return extracted_payload

            # CRITICAL FIX: Cross-Origin CDN Protection
            main_domain = urlparse(url).netloc.replace("www.", "")
            media_domain = urlparse(media_url).netloc.replace("www.", "")

            extracted_payload["headers"] = {
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

            if main_domain not in media_domain and media_domain not in main_domain:
                self.db.log_trace(jid, f"Cross-Origin CDN detected ({media_domain}). Purging toxic cookies and overriding Referer.")
                extracted_payload["cookie_str"] = ""
                extracted_payload["raw_cookies"] = []
                
                if "twimg.com" in media_domain:
                    extracted_payload["headers"]["Referer"] = "https://twitter.com/"
                    extracted_payload["headers"]["Origin"] = "https://twitter.com"
                else:
                    extracted_payload["headers"]["Referer"] = f"https://{media_domain}/"
                    extracted_payload["headers"]["Origin"] = f"https://{media_domain}"
            else:
                extracted_payload["headers"]["Referer"] = url
                extracted_payload["headers"]["Origin"] = "/".join(url.split("/")[:3])
                extracted_payload["raw_cookies"] = cookies
                extracted_payload["cookie_str"] = "; ".join([f"{c['name']}={c['value']}" for c in cookies])

            await context.close()
            try:
                if os.path.exists(user_data_dir):
                    shutil.rmtree(user_data_dir, ignore_errors=True)
            except Exception as e:
                self.db.log_trace(jid, f"Disk cleanup warning: {e}")

            return extracted_payload

    async def _run_ffmpeg_capture(self, url: str, jid: str, dl_dir: Path, headers: dict, cookie_str: str) -> bool:
        import re
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

    async def _run_ytdlp_with_cookies(self, url: str, jid: str, dl_dir: Path, headers: dict, raw_cookies: list, proxy_url: str = None) -> bool:
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
        
        if proxy_url:
            opts["proxy"] = proxy_url
            
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

    async def validate_media_file(self, file_path: Path, jid: str) -> bool:
        """
        Validates the downloaded file before handing it to FFmpeg.
        Implements checks for size, HTML magic bytes, and ffprobe stream verification.
        """
        import json
        
        if not file_path.exists():
            self.db.log_trace(jid, f"Validation failed: File does not exist at {file_path}")
            return False

        # ─── 1. FILE SIZE CHECK ───
        size_bytes = file_path.stat().st_size
        size_mb = size_bytes / (1024 * 1024)
        self.db.log_trace(jid, f"Validation: File size is {size_mb:.2f} MB")
        
        # If it is less than 100KB, it is likely an error page or tiny broken payload
        if size_bytes < 100000:  
            self.db.log_trace(jid, "Validation failed: File is suspiciously small. Likely an HTML error page.")
            return False

        # ─── 2. MAGIC BYTES / HTML CHECK ───
        try:
            with open(file_path, 'rb') as f:
                # Read the first 256 bytes to sniff the headers
                header = f.read(256).lower()
                if b'<!doctype html' in header or b'<html' in header:
                    self.db.log_trace(jid, "Validation failed: File contains HTML magic bytes. Download aborted.")
                    return False
        except Exception as e:
            self.db.log_trace(jid, f"Validation warning: Could not read magic bytes: {e}")

        # ─── 3. FFPROBE STREAM VERIFICATION ───
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
            
            # Save the ffprobe JSON alongside the download as recommended for diagnostics
            probe_dump_path = file_path.with_suffix('.probe.json')
            with open(probe_dump_path, 'w') as f:
                json.dump(probe_data, f, indent=4)
                
            # Verify the file actually contains a video stream
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

        # ─── INJECTED VALIDATION GATE ───
        self.db.log_trace(jid, "Running Pre-FFmpeg Validation Gate...")
        is_valid = await self.validate_media_file(dl_file, jid)
        if not is_valid:
            raise RuntimeError("Validation Gate Failed: The downloaded payload is not a valid media file. Discarding payload.")
        # ────────────────────────────────

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

        # ─── INJECTED DIAGNOSTIC DUMP (LIGHTWEIGHT) ───
        try:
            self.db.log_trace(jid, "Zipping lightweight diagnostic data before cleanup...")
            zip_target = str(JOBS_DIR / f"JOB_{jid}_diagnostic_success.zip")
            
            # 1. Nuke any lingering zip file from previous runs to prevent appending
            if os.path.exists(zip_target):
                os.remove(zip_target)
            
            # 2. Build a fresh zip using strict file extension whitelisting
            import zipfile
            with zipfile.ZipFile(zip_target, 'w', zipfile.ZIP_DEFLATED) as zf:
                for root, dirs, files in os.walk(job_dir):
                    for file in files:
                        # STRICT FILTER: Ignore absolutely everything except these 5 extensions
                        if file.lower().endswith(('.log', '.html', '.json', '.png', '.txt')):
                            file_path = os.path.join(root, file)
                            arcname = os.path.relpath(file_path, job_dir)
                            zf.write(file_path, arcname)
            
            cap = f"🕵️ **SUCCESS DEBUG**\nPayload captured for `{jid}`.\n(Video files excluded to save space)."
            await self.app.send_document(job_data['chat_id'], document=zip_target, caption=cap)
            
            # 3. Clean up the zip after sending
            if os.path.exists(zip_target):
                os.remove(zip_target)
        except Exception as e:
            self.db.log_trace(jid, f"Failed to send success debug zip: {e}")
        # ────────────────────────────────

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
        job_dir = JOBS_DIR / f"JOB_{jid}"
        
        if job_dir.exists():
            zip_target = str(JOBS_DIR / f"JOB_{jid}_diagnostic.zip")
            
            # 1. Nuke any lingering zip file from previous crashes
            if os.path.exists(zip_target):
                try: os.remove(zip_target)
                except Exception: pass
            
            try:
                # 2. Build a fresh zip using strict file extension whitelisting
                import zipfile
                with zipfile.ZipFile(zip_target, 'w', zipfile.ZIP_DEFLATED) as zf:
                    for root, dirs, files in os.walk(job_dir):
                        for file in files:
                            # STRICT FILTER: Ignore absolutely everything except these 5 extensions
                            if file.lower().endswith(('.log', '.html', '.json', '.png', '.txt')):
                                file_path = os.path.join(root, file)
                                arcname = os.path.relpath(file_path, job_dir)
                                zf.write(file_path, arcname)
                
                await app.send_document(chat_id, document=zip_target, caption=cap)
                
            except Exception as e:
                # Fallback: Just send the raw trace.log if zipping fails entirely
                log_path = job_dir / "trace.log"
                if log_path.exists():
                    try: await app.send_document(chat_id, document=str(log_path), caption=f"{cap}\n*(Failed to zip dir: {e})*")
                    except Exception: pass
            finally:
                # 3. Clean up the zip after sending
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

# ──────────────────────────── PIPELINE MANAGER (Orchestrator) ───────────

class TelegramDispatcher:
    """
    Centralizes all Telegram API requests through a single sender queue.
    Implements token-bucket rate limiting and merges UI updates to prevent FloodWaits.
    """
    def __init__(self, app: Client):
        self.app = app
        self.edit_queue = asyncio.Queue()
        self.pending_edits = {}  
        self.lock = asyncio.Lock()
        
        # Token-bucket rate limiting setup
        self.tokens = 25.0
        self.last_refill = time.time()
        self.rate = 25.0  # Safe global limit

    async def _consume_token(self):
        """Ensures we never burst past Telegram's hard global limits."""
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
        """
        Puts an edit into the queue. If an edit is already pending for this message, 
        it overwrites the old state, merging multiple UI updates into a single refresh.
        """
        async with self.lock:
            key = (chat_id, msg_id)
            is_new = key not in self.pending_edits
            self.pending_edits[key] = (text, kb)
            
        if is_new:
            await self.edit_queue.put(key)

    async def sender_loop(self):
        """The dedicated background worker that safely communicates with Telegram."""
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
                    # Add a micro-sleep to prevent hammering the exact same chat ID too fast
                    await asyncio.sleep(1.0)
                    break
                except MessageNotModified:
                    break
                except FloodWait as e:
                    # Catch FloodWait exceptions, sleep for requested duration, apply exponential backoff
                    sleep_time = e.value + backoff
                    await asyncio.sleep(sleep_time)
                    backoff *= 2
                    retries += 1
                except Exception:
                    break
                    
            self.edit_queue.task_done()
            
class UIAccumulator:
    """
    Monitors database state and accumulates UI updates. 
    Strictly fires Telegram edits ONLY when a job's stage changes, 
    progress jumps by >= 10%, or the active job pool changes.
    """
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
            await asyncio.sleep(4)  # Polling interval (does not cost API calls)
            
            try:
                jobs = await self.db.get_active_jobs()
                current_jids = {j['id'] for j in jobs}
                
                dashboard_needs_update = False
                
                # ─── POOL CHANGE DETECTION ───
                # If a job was added, completed, or failed, we MUST refresh the dashboard
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
                    
                    # Accumulate speed/eta history for smooth UI averages
                    if jid not in self.job_stats_history:
                        self.job_stats_history[jid] = {'speeds': [], 'etas': []}
                    if "|" in raw_stage:
                        parts = [p.strip() for p in raw_stage.split("|")]
                        if len(parts) >= 3:
                            self.job_stats_history[jid]['speeds'].append(_parse_speed(parts[1]))
                            self.job_stats_history[jid]['etas'].append(_parse_eta(parts[2]))

                    # ─── STRICT THRESHOLD EVALUATION ───
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
                        
                        # Queue the individual Job Card update
                        await self.dispatcher.safe_edit_queued(job['chat_id'], job['tracker_id'], _job_tracker_text(job, avg_s, avg_e), kb)
                        
                        # Lock in the new state thresholds
                        self.last_stages[jid] = base_phase
                        self.last_pcts[jid] = current_pct
                        self.job_stats_history[jid] = {'speeds': [], 'etas': []}
                        
                        # Sync to DB so restarts remember the UI state
                        await self.db.update_job(jid, last_ui_pct=current_pct)
                        
                # ─── ACCUMULATED DASHBOARD UPDATE ───
                if dashboard_needs_update and _dashboard_msg_id and _dashboard_chat_id:
                    text, kb = await _get_dashboard_components(_dashboard_tab, self.db, self.pipeline)
                    await self.dispatcher.safe_edit_queued(_dashboard_chat_id, _dashboard_msg_id, text, kb)
                    
            except Exception:
                pass

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
                await self.db.update_job(jid, stage=success_stage.value, retries=0, recovered_at_stage=None)
                
                if next_q: 
                    # ─── BATCH GATEKEEPER ───
                    # If this is the Encoder passing to the Uploader, check if it's a batch task.
                    # Normal tasks pass through instantly. Batch tasks are held back.
                    updated_job = await self.db.get_job(jid)
                    if next_q == self.up_q and updated_job and str(updated_job.get('source', '')).startswith('Batch_'):
                        pass # The Batch Orchestrator will manually release this later
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
    
    global _batch_mode, _batch_collection
    
    # Dynamically check if any active jobs belong to a batch
    batch_active = any(str(j.get('source', '')).startswith('Batch_') for j in standard_jobs)
    
    if _batch_mode:
        stat_str = f"🟡 BATCH COLLECTION ({len(_batch_collection)} ITEMS QUEUED)"
    elif batch_active:
        stat_str = "🔵 BATCH PROCESSING ACTIVE"
    else:
        stat_str = "ONLINE & SECURE"
    
    text = (
        f"💻 **MAINFRAME v5.3**\n"
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
    
    # (Note: The old "FORCE UPLOAD RELEASE" button was removed here because 
    # the new Orchestrator handles the uploader dumps automatically and safely.)

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

async def _monitor_batch_completion(db: JobScheduler, chat_id: int, app: Client):
    global _hold_uploads, _mass_upload_active
    while _hold_uploads:
        await asyncio.sleep(5)
        active_jobs = await db.get_active_jobs()
        
        # Check if any job is still in a pre-upload phase
        pending_stages = ["queued", "downloading", "downloaded", "encoding", "process"]
        is_processing = any(
            any(stage in j.get('stage', '').lower() for stage in pending_stages) 
            for j in active_jobs
        )
        
        if not is_processing and active_jobs:
            _hold_uploads = False
            _mass_upload_active = True
            try:
                await app.send_message(chat_id, "✅ **BATCH PROCESSED**\nAll downloads and encodings complete. Initiating mass upload sequence...")
            except Exception:
                pass
            
            # Wait for mass upload to finish to reset UI state
            while _mass_upload_active:
                await asyncio.sleep(5)
                jobs = await db.get_active_jobs()
                if not jobs:
                    _mass_upload_active = False
            break
            
async def _batch_runner(db: JobScheduler, pipeline: PipelineManager, app: Client):
    batch_counter = 0
    while True:
        # 1. Wait for a batch to be submitted via /end
        batch = await _pending_batches.get()
        batch_counter += 1
        batch_source = f"Batch_{batch_counter}"
        batch_jids = []
        
        # 2. Sequential DL Feed (Wait for DL 1 to finish before submitting DL 2)
        for url, title, chat_id in batch:
            jid = str(uuid.uuid4())[:8]
            batch_jids.append(jid)
            
            tracker = await app.send_message(
                chat_id, 
                f"`[ ⚡ ] ＴＡＳＫ :` `{title[:30]}`\n`[ ⚙️ ] ＳＴＡＴ :` `QUEUED (BATCH {batch_counter})`", 
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ CANCEL", callback_data=f"kill|{jid}")]])
            )
            
            await db.create_job({
                "id": jid, "url": url, "title": title, "source": batch_source, 
                "quality": "auto", "strategy": LinkClassifier.classify(url), 
                "chat_id": chat_id, "tracker_id": tracker.id
            })
            
            # Submit ONE item to download
            await pipeline.dl_q.put(jid)
            
            # Wait for this specific item to clear the download stage
            while True:
                await asyncio.sleep(2)
                job = await db.get_job(jid)
                if not job: 
                    break # Job was killed, move on
                base_stage = job.get('stage', '').split('|')[0].strip().lower()
                # Once it hits "downloaded" (or encoding), we break and submit the next link
                if base_stage not in ["queued", "downloading"]: 
                    break
        
        # 3. Wait for ALL items in this batch to finish processing
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
                
        # 4. Mass Upload Sequence (Dump all finished encodings into the Uploader)
        for jid in batch_jids:
            job = await db.get_job(jid)
            if job:
                base_stage = job.get('stage', '').split('|')[0].strip().lower()
                if base_stage == "encoded":
                    await pipeline.up_q.put(jid)
        
        # 5. Mark batch as complete, move to Batch 2 (if queued)
        _pending_batches.task_done()
        
async def _resume_interrupted_batches(db: JobScheduler, pipeline: PipelineManager, batch_jids: list):
    # 1. Isolate the jobs that still need to be downloaded
    dl_jids = []
    for jid in batch_jids:
        job = await db.get_job(jid)
        if job and job.get('stage') == "queued":
            dl_jids.append(jid)
            
    # 2. Feed the queued downloads one by one
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
                
    # 3. Wait for ALL items in this recovered batch to finish encoding
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
            
    # 4. Dump to uploader
    for jid in batch_jids:
        job = await db.get_job(jid)
        if job:
            base_stage = job.get('stage', '').split('|')[0].strip().lower()
            if base_stage == "encoded":
                await pipeline.up_q.put(jid)

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

    @app.on_message(filters.command(["go"]) & filters.user(OWNER_ID))
    async def batch_go(_, msg: Message):
        global _batch_mode, _batch_collection
        _batch_mode = True
        _batch_collection = []
        await msg.reply("🟢 **BATCH MODE INITIATED**\nPaste your URLs one by one. Send `/end` when finished.")

    @app.on_message(filters.command(["end"]) & filters.user(OWNER_ID))
    async def batch_end(_, msg: Message):
        global _batch_mode, _batch_collection, _pending_batches
        if not _batch_mode:
            return await msg.reply("⚠️ Not in batch mode. Use `/go` first.")
            
        _batch_mode = False
        if not _batch_collection:
            return await msg.reply("⚠️ No links were collected. Batch cancelled.")

        await msg.reply(f"🚀 **BATCH SUBMITTED**\nSent {len(_batch_collection)} tasks to the Orchestrator.")
        
        # Put the entire list of URLs into the processing queue
        await _pending_batches.put(list(_batch_collection))
        _batch_collection.clear()

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
          
    @app.on_message(filters.command(["update"]) & filters.user(OWNER_ID))
    async def update_command(_, msg: Message):
        await msg.reply(
            "🔄 **UPDATE SEQUENCE**\nEnter the script name (e.g., `testui5.1`):",
            reply_markup=ForceReply(selective=True)
        )

    @app.on_message(filters.text & filters.user(OWNER_ID) & filters.reply)
    async def update_catcher(_, msg: Message):
        # Intercept the ForceReply for the update command
        if msg.reply_to_message and "UPDATE SEQUENCE" in msg.reply_to_message.text:
            input_name = msg.text.strip()
            
            # Automatically append .py if you didn't type it
            script_name = f"{input_name}.py" if not input_name.endswith(".py") else input_name

            if not os.path.exists(script_name):
                return await msg.reply(f"❌ File `{script_name}` not found in the Debian directory.")

            # The initial "updating" ping
            progress = await msg.reply(f"🔄 Updating to `{script_name}`... Suspending current processes.")

            # 1. Stop the current client to release the Pyrogram session.session lock
            await app.stop()

            import subprocess
            try:
                # 2. Launch the new script using Python 3.13
                proc = subprocess.Popen(
                    ["python3.13", script_name],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )

                # 3. Wait 6 seconds to monitor for initialization crashes
                try:
                    outs, errs = proc.communicate(timeout=6)
                    crashed = True
                    exit_code = proc.returncode
                except subprocess.TimeoutExpired:
                    # If it didn't exit within 6 seconds, the bot loop is running safely.
                    crashed = False

                if crashed:
                    # 4a. ROLLBACK: Reclaim the session and report the failure
                    await app.start()
                    error_log = errs.strip()[-3500:] if errs else "No traceback available (Immediate Exit)."
                    
                    await progress.edit_text(
                        f"❌ **Update Failed! Rolled back to previous version.**\n"
                        f"**Target:** `{script_name}` | **Exit Code:** `{exit_code}`\n\n"
                        f"**Error Logs:**\n```{error_log}```"
                    )
                else:
                    # 4b. SUCCESS: Edit the message via REST to bypass session lock, then die.
                    import aiohttp
                    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
                    payload = {
                        "chat_id": msg.chat.id,
                        "message_id": progress.id,
                        "text": f"✅ **Updated to `{script_name}`.** The new mainframe instance is now online."
                    }
                    async with aiohttp.ClientSession() as session:
                        await session.post(url, json=payload)

                    # Terminate the old process completely, leaving the new one running in Termux
                    os._exit(0)

            except Exception as e:
                # Failsafe if the subprocess fails to spawn
                await app.start()
                await progress.edit_text(f"🚨 **Critical Execution Error during Update:**\n`{str(e)}`")
            
            return # Prevent this reply from falling through to the normal url_catcher

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
            title = msg.text.replace(url, "").strip() or url[:40]
            
            global _batch_mode, _batch_collection
            if _batch_mode:
                # Add to holding list without triggering processing
                _batch_collection.append((url, title, msg.chat.id))
                await msg.reply(f"✅ Added to batch. Total: {len(_batch_collection)}. Send `/end` to process.", quote=True)
            else:
                # NORMAL LINK: Processes instantly, completely bypassing the batch holds
                jid = str(uuid.uuid4())[:8]
                tracker = await msg.reply(f"`[ ⚡ ] ＴＡＳＫ :` `{title[:30]}`\n`[ ⚙️ ] ＳＴＡＴ :` `QUEUED`", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ CANCEL", callback_data=f"kill|{jid}")]]))
                
                await db.create_job({"id": jid, "url": url, "title": title, "source": "Direct", "quality": "auto", "strategy": LinkClassifier.classify(url), "chat_id": msg.chat.id, "tracker_id": tracker.id})
                await pipeline.dl_q.put(jid)

    # Note: Keep your existing @app.on_callback_query() exactly as it was originally.

    @app.on_callback_query()
    async def _router(client: Client, cb: CallbackQuery):
        global _dashboard_tab, _dashboard_msg_id, _dashboard_chat_id
        
        if cb.data == "noop":
            await cb.answer()
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
    dispatcher = TelegramDispatcher(app) 
    
    # Initialize the new Accumulator
    ui_accumulator = UIAccumulator(db, dispatcher, pipeline)
    
    setup_router(app, db, pipeline)

    async with app:
        recovering_batch_jids = await RecoveryManager.scan_and_requeue(db, pipeline.dl_q, pipeline.enc_q, pipeline.up_q, app)
        pipeline.start_workers()
        
        asyncio.create_task(dispatcher.sender_loop()) 
        asyncio.create_task(ui_accumulator.run_loop()) 
        asyncio.create_task(terminal_loop(db, pipeline))
        
        # ─── BATCH ORCHESTRATORS ───
        asyncio.create_task(_batch_runner(db, pipeline, app))
        if recovering_batch_jids:
            asyncio.create_task(_resume_interrupted_batches(db, pipeline, recovering_batch_jids))
        
        if OWNER_ID:
            m = await app.send_message(OWNER_ID, "🟢 Mainframe Systems Online.")
            global _dashboard_msg_id, _dashboard_chat_id, _dashboard_tab
            _dashboard_msg_id, _dashboard_chat_id = m.id, m.chat.id
            text, kb = await _get_dashboard_components(_dashboard_tab, db, pipeline)
            await dispatcher.safe_edit_queued(_dashboard_chat_id, _dashboard_msg_id, text, kb)

        while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    try: loop.run_until_complete(main())
    except KeyboardInterrupt: sys.exit(0)
