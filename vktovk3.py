"""
vk_bot.py - Dedicated VK-to-VK Playlist Deduplicator & Community Scanner
───────────────────────────────────────────────────────────────
FEATURES (v4.1):
  • VK-to-VK Pipeline: Skips FFmpeg completely. Passes raw files back to VK via aiohttp.
  • /scan Command: Scans a community (Videos + Wall) upfront, provides a paginated UI.
  • /transfer Command: Scans a wall, parses captions via Regex, fuzzy-matches albums, and LINKS native VK videos without downloading.
  • Dynamic Album Targeting: Select videos from the /scan UI, then type #AlbumName to dedupe and queue.
  • Graceful Cancel: Stops new jobs but lets downloaded files finish uploading.
  • Reboot Crash Report: Detailed breakdown of the queue state if the server restarts.
  • Fixed Router: /vk_workers and other commands now correctly bypass the auto-downloader.
───────────────────────────────────────────────────────────────
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
import yt_dlp
from logging.handlers import RotatingFileHandler
from rapidfuzz import process, fuzz

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

MAX_RETRIES = 3                 
MAX_GLOBAL_CONCURRENT = 7       
MIN_FREE_GB = 3.0               
EST_JOB_FOOTPRINT_GB = 1.5      

# --- UI State Constants ---
C_CYAN, C_YELLOW, C_RED, C_GREEN, C_RESET, C_BOLD = "\033[36m", "\033[33m", "\033[31m", "\033[32m", "\033[0m", "\033[1m"
_live_ui_text = {}
_last_completed = "—"
_dash_msg_id, _dash_chat_id = 0, 0
_stack_msg_id, _stack_chat_id = 0, 0
_dash_tab = "playlists"
_expanded_pl = None
_expanded_bucket = None
_expanded_jid = None

# --- In-Memory State for /scan Command ---
_scan_sessions = {}


def make_bar(percent: float, width: int = 10) -> str:
    filled = int(max(0.0, min(percent, 100.0)) / (100.0 / width))
    return "█" * filled + "░" * (width - filled)


def get_free_space_gb() -> float:
    total, used, free = shutil.disk_usage(BASE_DIR)
    return free / (1024 ** 3)


def clean_title(title_str: str) -> str:
    return title_str.split("|||")[0] if "|||" in title_str else title_str


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

    lines = [f"📦 **ACTIVE JOBS** ({len(jobs)})", "`━━━━━━━━━━━━━━━━━━━━━━━━━━`"]
    if not jobs:
        lines.append("`  System idle.`")
    else:
        labels = (('dl', '📥 DOWNLOADING'), ('enc', '⚙️ PREPARING'), ('up', '📤 UPLOADING'))
        shown = 0
        for key, label in labels:
            bucket_jobs = groups[key]
            if not bucket_jobs: continue
            lines.append(f"`{label} ({len(bucket_jobs)})`")
            lines.extend(fmt_job(j) for j in bucket_jobs[:max_per_bucket])
            shown += min(len(bucket_jobs), max_per_bucket)
        extra = len(jobs) - shown
        if extra > 0: lines.append(f"`  …and {extra} more`")
    lines.append(f"`🏁 LAST : {clean_title(_last_completed)[:20]}`")
    return "\n".join(lines)


# ──────────────────────────── VK API HELPERS ─────────────────────────

def get_vk_api():
    VK_TOKEN = getattr(config, "VK_TOKEN", None)
    if not VK_TOKEN: raise ValueError("VK_TOKEN missing in config.")
    import vk_api
    return vk_api.VkApi(token=VK_TOKEN).get_api()


def resolve_vk_community_id(vk, url: str) -> int:
    """Helper to cleanly extract a community/owner ID from a VK URL."""
    clean_url = re.sub(r'(m\.vk\.com|vk\.ru)', 'vk.com', url, flags=re.IGNORECASE)
    
    # A. Match direct club/public/event IDs
    comm_match = re.search(r'vk\.com/(?:club|public|event|groups/|video/owner/-)(\d+)', clean_url)
    if comm_match: return -int(comm_match.group(1))
    
    # B. Match video links (video-123_456)
    vid_match = re.search(r'video(-?\d+)_', clean_url)
    if vid_match: return int(vid_match.group(1))
    
    # C. Match screen names (@my_group)
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
    """Parses a VK caption for: [Production] Name x Name - description"""
    if not caption: return None
    pattern = r'\[\s*(.*?)\s*\]\s*(.*?)\s*-\s*(.*)'
    match = re.search(pattern, caption, flags=re.DOTALL)
    
    if not match: return None 
    
    production = match.group(1).strip()
    names_raw = match.group(2).strip()
    description = match.group(3).strip()
    
    names_list = [
        name.strip() for name in 
        re.split(r'\s*(?:x|&|,|\band\b)\s*', names_raw, flags=re.IGNORECASE) 
        if name.strip()
    ]
    return production, names_list, description


class FuzzyAlbumManager:
    """Loads all existing albums and fuzzy matches targets to avoid duplicates."""
    def __init__(self, vk):
        self.vk = vk
        self.albums = {} # 'Exact Title': ID
        self._load_albums()

    def _load_albums(self):
        offset = 0
        while True:
            res = self.vk.video.getAlbums(count=100, offset=offset)
            items = res.get('items', [])
            if not items: break
            for a in items:
                self.albums[a['title'].strip()] = a['id']
            offset += 100
            if offset >= res.get('count', 0): break

    def get_or_create(self, target_name: str) -> int:
        target_name = target_name.strip()
        if not target_name: return None

        existing_names = list(self.albums.keys())
        if existing_names:
            match = process.extractOne(target_name, existing_names, scorer=fuzz.WRatio)
            if match and match[1] > 85: 
                return self.albums[match[0]] # Return ID of closest match

        # Needs a new album created on VK
        try:
            new_album = self.vk.video.addAlbum(title=target_name)
            new_id = new_album['album_id']
            self.albums[target_name] = new_id
            return new_id
        except Exception as e:
            print(f"Error creating album '{target_name}': {e}")
            return None


# ──────────────────────────── SUBSYSTEM 1: DATABASE ─────────────────────

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
        # New table for the /transfer deduplication
        self.conn.execute('''CREATE TABLE IF NOT EXISTS processed_transfers (
            video_uid TEXT PRIMARY KEY
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

    # --- Methods for /transfer deduplication ---
    async def is_transferred(self, uid: str) -> bool:
        async with self.lock:
            row = self.conn.execute('SELECT 1 FROM processed_transfers WHERE video_uid = ?', (uid,)).fetchone()
            return bool(row)

    async def mark_transferred(self, uid: str):
        async with self.lock:
            self.conn.execute('INSERT OR IGNORE INTO processed_transfers (video_uid) VALUES (?)', (uid,))
            self.conn.commit()

    # --- Standard Playlist Methods ---
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
        async with self.lock:
            return [dict(r) for r in self.conn.execute('SELECT * FROM playlist_items WHERE playlist_id = ? AND status = "pending" LIMIT ?', (pl_id, limit)).fetchall()]

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


# ──────────────────────────── DRIP-FEED ORCHESTRATOR ───────────────────

async def playlist_drip_feed_loop(db: JobScheduler, dl_q: asyncio.Queue, enc_q: asyncio.Queue, up_q: asyncio.Queue,
                                   dl_pool: "WorkerPool", enc_pool: "WorkerPool", up_pool: "WorkerPool"):
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

            eligible_playlists = [pl for pl in active_playlists if pl['status'] not in ('paused', 'cancelled', 'completed', 'cancelling')]
            made_progress = True
            while global_slots_free > 0 and eligible_playlists and made_progress:
                made_progress = False
                for pl in eligible_playlists:
                    if global_slots_free <= 0: break
                    pending_items = await db.get_pending_items(pl['id'], limit=1)
                    if not pending_items: continue
                    item = pending_items[0]
                    await db.claim_item_as_job(item, pl['chat_id'])
                    await dl_q.put(item['id'])
                    global_slots_free -= 1
                    made_progress = True
        except Exception as e:
            log.exception(f"Drip Feed Loop Error: {e}")


# ──────────────────────────── PIPELINE ENGINES ─────────────────────────

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


import aiohttp.payload


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
            target_album_id = int(pl['caption']) if pl and pl.get('caption') and pl['caption'].lstrip('-').isdigit() else None

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


# ──────────────────────────── DASHBOARD & ROUTER ───────────────────────

async def safe_edit(app: Client, chat_id: int, msg_id: int, text: str, kb: InlineKeyboardMarkup | None = None):
    try: await app.edit_message_text(chat_id, msg_id, text, reply_markup=kb)
    except MessageNotModified: pass
    except FloodWait as e: await asyncio.sleep(e.value)
    except Exception: pass


def render_worker_panel(dl_pool: "WorkerPool", enc_pool: "WorkerPool", up_pool: "WorkerPool") -> tuple[str, InlineKeyboardMarkup]:
    text = (
        "🛠 **WORKER POOLS**\n`━━━━━━━━━━━━━━━━━━━━━━━━━━`\n"
        f"`📥 DOWNLOAD : {dl_pool.current_count()}/{dl_pool.target} active`\n"
        f"`⚙️ PREPARE  : {enc_pool.current_count()}/{enc_pool.target} active`\n"
        f"`📤 UPLOAD   : {up_pool.current_count()}/{up_pool.target} active`\n"
        "`━━━━━━━━━━━━━━━━━━━━━━━━━━`\n"
    )
    kb = [
        [InlineKeyboardButton("−", callback_data="wk|dl|-1"), InlineKeyboardButton(f"📥 DL: {dl_pool.target}", callback_data="noop"), InlineKeyboardButton("+", callback_data="wk|dl|1")],
        [InlineKeyboardButton("−", callback_data="wk|enc|-1"), InlineKeyboardButton(f"⚙️ PREP: {enc_pool.target}", callback_data="noop"), InlineKeyboardButton("+", callback_data="wk|enc|1")],
        [InlineKeyboardButton("−", callback_data="wk|up|-1"), InlineKeyboardButton(f"📤 UP: {up_pool.target}", callback_data="noop"), InlineKeyboardButton("+", callback_data="wk|up|1")],
        [InlineKeyboardButton("✅ Done", callback_data="wk|close|0")],
    ]
    return text, InlineKeyboardMarkup(kb)


async def render_dashboard(db: JobScheduler, tab: str = "playlists", exp_pl: str = None, exp_bucket: str = None, exp_jid: str = None) -> tuple[str, InlineKeyboardMarkup]:
    playlists, active_jobs = await db.get_active_playlists(), await db.get_active_jobs()
    total_storage, free_gb = sum(f.stat().st_size for f in JOBS_DIR.rglob("*") if f.is_file()) / (1024 ** 3), get_free_space_gb()

    act_text_blocks = ["`[🔄] ACT  :`"]
    if not active_jobs: act_text_blocks = ["`[🔄] ACT  :` `SYSTEM IDLE`"]
    else:
        for i, j in enumerate(active_jobs[:7]):
            pct, stage_short = float(j.get('pct', 0.0) or 0.0), (j.get('stage') or '').split('|')[0].strip()[:4].upper()
            act_text_blocks.append(f"`  {chr(97+i)}. [{stage_short}] {clean_title(j['title'])[:12]}.. [{make_bar(pct, 8)}] {pct:.0f}%`")

    text = (f"💻 **VK PLAYLIST MAINFRAME**\n`━━━━━━━━━━━━━━━━━━━━━━━━━━`\n"
            f"`[⚡] STAT :` `DRIP-FEED ACTIVE`\n`[💾] USED :` `{total_storage:.2f} GB`  `[🆓] FREE :` `{free_gb:.2f} GB`\n"
            f"{chr(10).join(act_text_blocks)}\n`[🏁] LAST :` `{clean_title(_last_completed)[:12]}`\n`━━━━━━━━━━━━━━━━━━━━━━━━━━`")

    kb = []
    is_root_open = (tab == "playlists")
    kb.append([InlineKeyboardButton(f"{'[-]' if is_root_open else '[+]'} 🎵 ACTIVE PLAYLISTS ({len(playlists)})", callback_data=f"dash|{'root' if is_root_open else 'playlists'}")])

    if playlists:
        if any(p['status'] == 'active' for p in playlists): kb.append([InlineKeyboardButton("⏸ PAUSE ALL PLAYLISTS", callback_data="pause_all")])
        else: kb.append([InlineKeyboardButton("▶️ RESUME ALL PLAYLISTS", callback_data="resume_all")])

    def _base(stage_str): return stage_str.split("|")[0].strip().lower() if stage_str else "queued"

    if is_root_open:
        if not playlists: kb.append([InlineKeyboardButton("└ System Idle", callback_data="noop")])
        else:
            for pl in playlists:
                pl_id, is_this_pl_exp = pl['id'], (exp_pl == pl['id'])
                pl_status_icon = "⏸" if pl['status'] in ("paused", "cancelling") else "▶️"
                status_txt = " [CANCELLING]" if pl['status'] == "cancelling" else ""

                kb.append([InlineKeyboardButton(f" {'[-]' if is_this_pl_exp else '[+]'} {pl_status_icon} Alb: {pl['caption'][:10] or 'Default'}{status_txt} [{pl['downloaded']}/{pl['total']}]", callback_data=f"dash|playlists:{pl_id}" if not is_this_pl_exp else "dash|playlists")])

                if is_this_pl_exp:
                    pl_jobs = [j for j in active_jobs if j.get('playlist_id') == pl_id]
                    buckets = {
                        "dl": [j for j in pl_jobs if _base(j['stage']) in ["queued", "downloading"]],
                        "dl_done": [j for j in pl_jobs if _base(j['stage']) == "downloaded"],
                        "enc": [j for j in pl_jobs if _base(j['stage']) in ["encoding", "process"]],
                        "enc_done": [j for j in pl_jobs if _base(j['stage']) == "encoded"],
                        "up": [j for j in pl_jobs if _base(j['stage']) in ["uploading", "uploaded"]]
                    }

                    def build_bucket(bucket_id, label, icon, job_list):
                        is_b_open = (exp_bucket == bucket_id)
                        kb.append([InlineKeyboardButton(f"    ├ {'[-]' if is_b_open else '[+]'} {icon} {label} ({len(job_list)})", callback_data=f"dash|playlists:{pl_id}:{bucket_id}" if not is_b_open else f"dash|playlists:{pl_id}")])
                        if is_b_open:
                            if not job_list: kb.append([InlineKeyboardButton("      └ Empty", callback_data="noop")])
                            for j in job_list:
                                jid, is_j_open, clean_t = j['id'], (exp_jid == j['id']), clean_title(j['title'])
                                if is_j_open:
                                    speed, eta, p = "—", "—", [x.strip() for x in (j.get('stage') or "").split("|")]
                                    if len(p) >= 3: speed, eta = p[1], p[2]
                                    elif len(p) == 2: speed = p[1]
                                    pct = float(j.get('pct', 0.0) or 0.0)
                                    kb.extend([[InlineKeyboardButton(f"🪪 JOB: {jid}", callback_data="noop")], [InlineKeyboardButton(f"📁 {clean_t[:15]}...", callback_data="noop")], [InlineKeyboardButton(f"⚡ {speed}  |  ⏳ {eta}", callback_data="noop")], [InlineKeyboardButton(f"📊 [{make_bar(pct, 8)}] {pct:.1f}%", callback_data="noop")], [InlineKeyboardButton("📄 LOGS", callback_data=f"joblog|{jid}"), InlineKeyboardButton("❌ KILL", callback_data=f"kill_job|{jid}")], [InlineKeyboardButton("🔙 CLOSE CARD", callback_data=f"dash|playlists:{pl_id}:{bucket_id}")]])
                                else: kb.append([InlineKeyboardButton(f"      ├ ⚡ {clean_t[:10]}.. | {float(j.get('pct', 0.0) or 0.0):.0f}%", callback_data=f"dash|playlists:{pl_id}:{bucket_id}:{jid}")])

                    build_bucket("dl", "DOWNLOADING", "📥", buckets["dl"])
                    build_bucket("dl_done", "WAITING PREP", "⏳", buckets["dl_done"])
                    build_bucket("enc", "PREPARING", "⚙️", buckets["enc"])
                    build_bucket("enc_done", "WAITING UP", "⏳", buckets["enc_done"])
                    build_bucket("up", "UPLOADING", "📤", buckets["up"])

                    if pl['status'] == "cancelling": kb.append([InlineKeyboardButton("⚠️ CANCELLING (Waiting on Uploads)", callback_data="noop")])
                    elif pl['status'] == "paused": kb.append([InlineKeyboardButton("▶️ RESUME", callback_data=f"res|{pl['id']}"), InlineKeyboardButton("🧹 GRACEFUL CANCEL", callback_data=f"graceful_cancel|{pl['id']}")])
                    else: kb.append([InlineKeyboardButton("⏸ PAUSE", callback_data=f"pause|{pl['id']}"), InlineKeyboardButton("🧹 GRACEFUL CANCEL", callback_data=f"graceful_cancel|{pl['id']}")])
                    kb.append([InlineKeyboardButton("❌ PURGE INSTANTLY", callback_data=f"kill|{pl['id']}")])
                    kb.append([InlineKeyboardButton("───────────────────", callback_data="noop")])

    kb.append([InlineKeyboardButton("🔄 REFRESH SYSTEM", callback_data="refresh")])
    return text, InlineKeyboardMarkup(kb)


# --- UI Renderer for /scan Command ---
def render_scan_ui(chat_id: int):
    session = _scan_sessions.get(chat_id)
    if not session: return "❌ Scan session expired.", None

    text = f"🔍 **COMMUNITY SCAN RESULTS**\n🔗 {session['url']}\n`━━━━━━━━━━━━━━━━━━━━━━`\n"
    kb = []
    
    # Videos Section
    vids, vp = session['videos'], session['vid_page']
    text += f"📁 **Community Videos** (Total: {len(vids)})\n"
    if vids:
        start_v = vp * 5
        for i, v in enumerate(vids[start_v:start_v+5]):
            kb.append([InlineKeyboardButton(f"📥 {v['title'][:30]}...", callback_data=f"scan|up|vid|{start_v + i}")])
        nav_v = []
        if vp > 0: nav_v.append(InlineKeyboardButton("◀ Prev Vids", callback_data="scan|page|vid|-1"))
        if start_v + 5 < len(vids): nav_v.append(InlineKeyboardButton("Next Vids ▶", callback_data="scan|page|vid|1"))
        if nav_v: kb.append(nav_v)
        kb.append([InlineKeyboardButton("📥 UPLOAD ALL VIDEOS", callback_data="scan|up|vid|all")])
    else: text += "└ None found.\n"
        
    text += "`━━━━━━━━━━━━━━━━━━━━━━`\n"
    
    # Wall Section
    wall, wp = session['wall'], session['wall_page']
    text += f"📰 **Wall Post Videos** (Total: {len(wall)})\n"
    if wall:
        start_w = wp * 5
        for i, v in enumerate(wall[start_w:start_w+5]):
            kb.append([InlineKeyboardButton(f"📥 {v['title'][:30]}...", callback_data=f"scan|up|wall|{start_w + i}")])
        nav_w = []
        if wp > 0: nav_w.append(InlineKeyboardButton("◀ Prev Wall", callback_data="scan|page|wall|-1"))
        if start_w + 5 < len(wall): nav_w.append(InlineKeyboardButton("Next Wall ▶", callback_data="scan|page|wall|1"))
        if nav_w: kb.append(nav_w)
        kb.append([InlineKeyboardButton("📥 UPLOAD ALL WALL POSTS", callback_data="scan|up|wall|all")])
    else: text += "└ None found.\n"
        
    kb.append([InlineKeyboardButton("❌ CANCEL SCAN", callback_data="scan|close")])
    return text, InlineKeyboardMarkup(kb)


def setup_router(app: Client, db: JobScheduler, dl_q: asyncio.Queue, enc_q: asyncio.Queue, up_q: asyncio.Queue, dl_pool: "WorkerPool", enc_pool: "WorkerPool", up_pool: "WorkerPool"):

    # ==========================================
    # NEW FEATURE: /transfer
    # ==========================================
    @app.on_message(filters.command(["transfer"]) & filters.user(OWNER_ID))
    async def cmd_transfer(_, msg: Message):
        args = msg.text.split(maxsplit=1)
        if len(args) < 2: 
            return await msg.reply("❌ Usage: `/transfer https://vk.com/community_link`")
        
        url = args[1].strip()
        m = await msg.reply(f"⏳ `Resolving target community and initializing transfer engine...`")
        
        # Async worker function to keep Pyrogram responsive
        async def run_transfer_job(chat_id, msg_id, link, db_instance):
            try:
                vk = await asyncio.to_thread(get_vk_api)
                comm_id = await asyncio.to_thread(resolve_vk_community_id, vk, link)
                album_manager = await asyncio.to_thread(FuzzyAlbumManager, vk)
                
                offset = 0
                processed = 0
                added = 0
                skipped = 0
                
                while True:
                    # Fetch 100 posts at a time from VK
                    res = await asyncio.to_thread(vk.wall.get, owner_id=comm_id, count=100, offset=offset)
                    posts = res.get('items', [])
                    if not posts: break
                    
                    for post in posts:
                        processed += 1
                        caption = post.get('text', '')
                        
                        # Find all video attachments on this post
                        video_atts = [att['video'] for att in post.get('attachments', []) if att.get('type') == 'video']
                        
                        if not video_atts:
                            skipped += 1
                            continue
                            
                        # Try to parse the caption with Regex
                        parsed_data = parse_caption(caption)
                        if not parsed_data:
                            skipped += 1
                            continue
                            
                        production, names_list, _ = parsed_data
                        target_names = [production] + names_list
                        
                        # Link each video attachment in the post
                        for v in video_atts:
                            uid = f"{v['owner_id']}_{v['id']}"
                            
                            if await db_instance.is_transferred(uid):
                                skipped += 1
                                continue
                                
                            def do_link():
                                # 1. Find or create the albums
                                resolved_album_ids = [album_manager.get_or_create(n) for n in target_names if n]
                                # 2. Filter out failures and convert to string for the API
                                valid_ids = [str(a_id) for a_id in resolved_album_ids if a_id]
                                
                                if valid_ids:
                                    # 3. Use VK's Native Linker (No downloading required)
                                    vk.video.addToAlbum(
                                        owner_id=v['owner_id'],
                                        video_id=v['id'],
                                        album_ids=",".join(valid_ids)
                                    )
                                    
                            try:
                                await asyncio.to_thread(do_link)
                                await db_instance.mark_transferred(uid)
                                added += 1
                            except Exception as e:
                                log.error(f"Failed to link {uid}: {e}")
                                skipped += 1

                        # Safe UI updates to avoid FloodWaits
                        if processed % 50 == 0:
                            report = (f"🔄 **SCANNING WALL**\n`━━━━━━━━━━━━━━━━━`\n"
                                      f"🔍 **Scanned:** `{processed}` posts\n"
                                      f"🔗 **Linked & Added:** `{added}` videos\n"
                                      f"⏭ **Skipped (No Match/Dupes):** `{skipped}`")
                            await safe_edit(app, chat_id, msg_id, report)

                    offset += 100
                    await asyncio.sleep(0.5) # Rate limit protection
                
                # Final Success Report
                final_report = (f"✅ **TRANSFER COMPLETE**\n`━━━━━━━━━━━━━━━━━`\n"
                                f"🎯 **Target:** `{link}`\n"
                                f"🔍 **Total Posts Checked:** `{processed}`\n"
                                f"🔗 **Successfully Linked:** `{added}`\n"
                                f"⏭ **Skipped:** `{skipped}`")
                await safe_edit(app, chat_id, msg_id, final_report)
                
            except Exception as e:
                await safe_edit(app, chat_id, msg_id, f"❌ Transfer Critical Error: `{e}`")

        # Hand it off to the background
        asyncio.create_task(run_transfer_job(msg.chat.id, m.id, url, db))


    # --- /scan command (Community scanning without upfront deduplication) ---
    @app.on_message(filters.command(["scan"]) & filters.user(OWNER_ID))
    async def cmd_scan(_, msg: Message):
        args = msg.text.split(maxsplit=1)
        if len(args) < 2: return await msg.reply("❌ Usage: `/scan https://vk.com/community_link`")
        url = args[1].strip()
        m = await msg.reply("🔍 `Scanning community (this may take a moment for large groups)...`")

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
                        for att in post.get('attachments', []):
                            if att.get('type') == 'video':
                                v = att['video']
                                uid = f"{v['owner_id']}_{v['id']}"
                                wall.append({'url': f"https://vk.com/video{uid}", 'title': v.get('title', 'VK Video'), 'unique_id': uid})
                    offset += count
                    if offset >= res.get('count', 0) or offset >= 1000: break
            except Exception as e: print(f"Wall fetch err: {e}")

            return videos, wall

        try:
            videos, wall = await asyncio.to_thread(perform_scan)
            if not videos and not wall: return await m.edit("❌ No videos found in that community.")
            
            _scan_sessions[msg.chat.id] = {
                'url': url, 'videos': videos, 'wall': wall, 
                'vid_page': 0, 'wall_page': 0, 'pending_upload': None
            }
            
            text, kb = render_scan_ui(msg.chat.id)
            await m.edit(text, reply_markup=kb)
        except Exception as e:
            await m.edit(f"❌ Scan failed: `{e}`")


    # --- Target Album Listener (For /scan uploads) ---
    @app.on_message(filters.regex(r"^#") & filters.user(OWNER_ID))
    async def catch_album_target(_, msg: Message):
        session = _scan_sessions.get(msg.chat.id)
        if not session or not session.get('pending_upload'):
            return msg.continue_propagation()
            
        album_name = msg.text.strip().lstrip('#').strip()
        
        pending = session['pending_upload']
        m = await msg.reply(f"🔍 Resolving album '{album_name}' & checking for duplicates...")
        
        items_to_process = []
        if pending['type'] == 'vid': source_list = session['videos']
        else: source_list = session['wall']

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
                return await m.edit(f"✅ All selected videos are already in album '{album_name}'. Skipped.")

            pl_id = str(uuid.uuid4())[:8]
            await db.create_playlist(pl_id, session['url'], str(target_album_id), len(missing_videos), msg.chat.id)
            
            db_items = [(str(uuid.uuid4())[:8], pl_id, item['url'], f"{item['title']}|||{item['unique_id']}") for item in missing_videos]
            await db.add_playlist_items(db_items)
            
            skipped = len(items_to_process) - len(missing_videos)
            session['pending_upload'] = None
            
            await m.edit(f"✅ **LOCKED TO ALBUM**\nSelected: `{len(items_to_process)}`\nDuplicates Skipped: `{skipped}`\nQueued: `{len(missing_videos)}`")
            
            global _dash_msg_id, _dash_chat_id, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid
            if _dash_msg_id and _dash_chat_id:
                dash_text, kb = await render_dashboard(db, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid)
                await safe_edit(app, _dash_chat_id, _dash_msg_id, dash_text, kb)
                
            text, kb = render_scan_ui(msg.chat.id)
            if 'msg_id' in session: await safe_edit(app, msg.chat.id, session['msg_id'], text, kb)
                
        except Exception as e:
            await m.edit(f"❌ Error queueing videos: `{e}`")


    # --- Standard Playlist Catcher (Bypasses /commands) ---
    @app.on_message(filters.regex(r"https?://") & filters.user(OWNER_ID) & ~filters.command(["scan", "transfer", "vk_dash", "vk_workers", "vk_pause_all", "vk_resume_all"]))
    async def auto_catch_playlist(_, msg: Message):
        text = msg.text.strip()
        parts = text.split("#", 1)
        url = parts[0].strip()
        
        if len(parts) < 2 or not parts[1].strip():
            return await msg.reply("❌ Please provide a target playlist name using `#Name`\nExample: `https://vk.com/video... #My Archive`")
            
        target_album_name = parts[1].strip()
        url = re.sub(r'vk\.ru', 'vk.com', url, flags=re.IGNORECASE)
        m = await msg.reply(f"🔍 `Querying VK API & resolving album '{target_album_name}'...`")

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

            missing_videos = [v for v in all_videos if v['unique_id'] not in existing_ids]
            return target_album_id, len(all_videos), missing_videos

        try:
            target_album_id, total_found, items_to_upload = await asyncio.to_thread(extract_and_dedupe, url, target_album_name)

            if not items_to_upload:
                if total_found > 0: return await m.edit(f"✅ **All {total_found} videos are already uploaded** to album '{target_album_name}'.")
                else: return await m.edit("❌ No videos found in the provided link.")

            pl_id = str(uuid.uuid4())[:8]
            await db.create_playlist(pl_id, url, str(target_album_id), len(items_to_upload), msg.chat.id)
            db_items = [(str(uuid.uuid4())[:8], pl_id, item['url'], f"{item['title']}|||{item['unique_id']}") for item in items_to_upload]
            await db.add_playlist_items(db_items)
            
            await m.edit(f"✅ **PLAYLIST LOCKED**\nFound `{total_found}` videos.\nSkipped `{total_found - len(items_to_upload)}` duplicates.\nQueued `{len(items_to_upload)}` for drip-feed.")

            global _dash_msg_id, _dash_chat_id, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid
            if _dash_msg_id and _dash_chat_id:
                dash_text, kb = await render_dashboard(db, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid)
                await safe_edit(app, _dash_chat_id, _dash_msg_id, dash_text, kb)

        except Exception as e: await m.edit(f"❌ Extraction error: `{e}`")

    @app.on_message(filters.command(["vk_dash"]) & filters.user(OWNER_ID))
    async def cmd_dash(_, msg: Message):
        global _dash_msg_id, _dash_chat_id, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid
        text, kb = await render_dashboard(db, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid)
        m = await msg.reply(text, reply_markup=kb)
        _dash_msg_id, _dash_chat_id = m.id, m.chat.id

    @app.on_message(filters.command(["vk_pause_all"]) & filters.user(OWNER_ID))
    async def cmd_pause_all(_, msg: Message):
        count = await db.pause_all_playlists()
        await msg.reply(f"⏸ Paused {count} playlist(s).")
        global _dash_msg_id, _dash_chat_id, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid
        if _dash_msg_id and _dash_chat_id:
            text, kb = await render_dashboard(db, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid)
            await safe_edit(app, _dash_chat_id, _dash_msg_id, text, kb)

    @app.on_message(filters.command(["vk_resume_all"]) & filters.user(OWNER_ID))
    async def cmd_resume_all(_, msg: Message):
        count = await db.resume_all_playlists()
        await msg.reply(f"▶️ Resumed {count} playlist(s).")
        global _dash_msg_id, _dash_chat_id, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid
        if _dash_msg_id and _dash_chat_id:
            text, kb = await render_dashboard(db, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid)
            await safe_edit(app, _dash_chat_id, _dash_msg_id, text, kb)

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

        if not changes: return await msg.reply("Usage: `/vk_workers dl=5 enc=3 up=2`")

        lines = []
        pools = {"dl": dl_pool, "enc": enc_pool, "up": up_pool}
        for key, new_target in changes.items():
            pool = pools[key]
            before = pool.current_count()
            await pool.adjust(new_target)
            lines.append(f"{key.upper()} {before} → {new_target}")

        await msg.reply("✅ " + " | ".join(lines))

    @app.on_callback_query()
    async def handle_callbacks(_, cb: CallbackQuery):
        global _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid
        if cb.data == "noop": return await cb.answer()

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
                return await cb.answer("Scan closed.")
                
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
                prompt = "\n\n⚠️ **WAITING FOR TARGET ALBUM**\nSend the album name (e.g. `#MyArchive`) to begin upload."
                await safe_edit(app, cb.message.chat.id, cb.message.id, text + prompt, kb)
                return await cb.answer("Send the Album Name tag in chat.")

        # --- Worker Callbacks ---
        if cb.data.startswith("wk|"):
            _, pool_key, delta_str = cb.data.split("|")
            pools = {"dl": dl_pool, "enc": enc_pool, "up": up_pool}
            if pool_key == "close":
                await cb.answer("Closed."); 
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
            await cb.answer("⏸ ALL PLAYLISTS PAUSED", show_alert=True)
        elif cb.data == "resume_all":
            await db.resume_all_playlists()
            await cb.answer("▶️ ALL PLAYLISTS RESUMED", show_alert=True)

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
            await db.force_fail_job(jid)
            _expanded_jid = None
            await cb.answer("Task terminated.", show_alert=True)

        elif cb.data.startswith("graceful_cancel|"):
            pl_id = cb.data.split("|")[1]
            await db.graceful_cancel_playlist(pl_id)
            await cb.answer("🧹 GRACEFUL CANCEL INITIATED\n\n• Pending downloads wiped.\n• In-progress downloads stopped.\n• Downloaded videos will finish uploading to VK.", show_alert=True)

        elif cb.data.startswith("pause|"):
            pl_id = cb.data.split("|")[1]
            await db.update_playlist(pl_id, status="paused")
            _expanded_pl = pl_id
            await cb.answer("⏸ PLAYLIST PAUSED", show_alert=True)

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

        if cb.data.startswith("dash") or cb.data in ["refresh", "pause_all", "resume_all", "kill_job", "graceful_cancel", "pause", "res", "kill"]:
            text, kb = await render_dashboard(db, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid)
            await safe_edit(app, cb.message.chat.id, cb.message.id, text, kb)


# ──────────────────────────── WORKER LOOPS ────────────────────

async def terminal_loop(db: JobScheduler, dl_q: asyncio.Queue, enc_q: asyncio.Queue, up_q: asyncio.Queue):
    sys.stdout.write("\033[2J")
    while True:
        await asyncio.sleep(2)
        sys.stdout.write("\033[H")
        sys.stdout.write(f"{C_CYAN}{C_BOLD}=== VK MAINFRAME [LIVE] ==={C_RESET}\n")
        sys.stdout.write(f"QUEUES | DL: {dl_q.qsize()} | PREP: {enc_q.qsize()} | UP: {up_q.qsize()}\n{'─' * 40}\n")

        jobs = await db.get_active_jobs()
        if not jobs: sys.stdout.write(f"{C_GREEN}System Idle. Awaiting playlist vectors.{C_RESET}\033[K\n")
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


# ──────────────────────────── BOOTSTRAP & REFRESHER ────────────────────

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

        album_info = f"VK Album ID: {pl['caption']}" if pl.get('caption') and pl['caption'].isdigit() else 'Default'

        report_text = (
            f"⚠️ **CRASH RECOVERY / REBOOT REPORT**\n`━━━━━━━━━━━━━━━━━━━━━━━━━━`\n"
            f"🔗 **URL:** {pl['url']}\n📁 **Destination:** `{album_info}`\n"
            f"⚙️ **Current State:** `HELD ON BOOT`\n\n"
            f"📊 **DETAILED BREAKDOWN:**\n"
            f"  ├ 🌐 Total Videos: `{pl['total']}`\n"
            f"  ├ ✅ Uploaded to VK: `{uploaded_count}`\n"
            f"  ├ 💾 Ready for Upload: `{dl_wait_up}`\n"
            f"  ├ 📥 Currently Downloading: `{dl_now}`\n"
            f"  ├ ⏳ Remaining: `{pending_items}`\n"
            f"  └ ❌ Perm Failures: `{failed_count}`\n`━━━━━━━━━━━━━━━━━━━━━━━━━━`\n"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("▶️ RESUME PLAYLIST", callback_data=f"res|{pl_id}")],
            [InlineKeyboardButton("🧹 FLUSH DOWNLOADED & CANCEL", callback_data=f"graceful_cancel|{pl_id}"), InlineKeyboardButton("❌ PURGE ALL NOW", callback_data=f"kill|{pl_id}")]
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