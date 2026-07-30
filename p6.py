import os
import re
import io
import time
import shutil
import sqlite3
import asyncio
import subprocess
import requests
import vk_api
from collections import deque
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from rich.console import Console
from rich.live import Live
from rich.progress import (
    Progress, TextColumn, BarColumn, DownloadColumn,
    TransferSpeedColumn, TimeRemainingColumn
)

import config

# ============================================================
# CONFIG & INITIALIZATION
# ============================================================
DL_WORKERS = 3
UP_WORKERS = 3
MAX_STAGED_FILES = 6
MIN_FREE_GB = 2.0
DB_PATH = "SysCache/vk_queue.db"
DOWNLOAD_DIR = "SysCache/vk_downloads"

# HIGH-SPEED ENGINE CONFIG
CHUNK_SIZE = 1024 * 1024           # 1 MB: Telegram's max read block size
MEM_BUFFER_SIZE = 8 * 1024 * 1024  # 8 MB: RAM buffer before flushing to disk
ALIGNMENT = 1024 * 1024            # 1 MB: MTProto strict offset alignment

SCHEDULER_INFLIGHT_TARGET = DL_WORKERS * 2
SCHEDULER_TICK = 0.5

# Caps TOTAL concurrent stream_media segments across ALL download workers combined.
# Without this, a burst of requeued jobs (e.g. many corrupt files found at once) can
# each spin up to 4 parallel parts x DL_WORKERS simultaneously, overwhelming the
# single Pyrogram session (FloodWaits, broken pipe).
GLOBAL_MAX_CONCURRENT_SEGMENTS = 6
global_segment_semaphore = asyncio.Semaphore(GLOBAL_MAX_CONCURRENT_SEGMENTS)

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

console = Console()
progress_ui = Progress(
    TextColumn("[bold blue]{task.fields[name]}", justify="right"),
    BarColumn(bar_width=None),
    "[progress.percentage]{task.percentage:>3.1f}%",
    "•", DownloadColumn(),
    "•", TransferSpeedColumn(),
    "•", TimeRemainingColumn(),
    console=console,
    expand=True
)

console.print("[bold yellow]Connecting to VK...[/bold yellow]")
try:
    vk_session = vk_api.VkApi(token=config.VK_TOKEN)
    vk = vk_session.get_api()
    my_vk_id = vk_session.method('users.get')[0]['id']
    console.print(f"[bold green]✅ VK Connected: {my_vk_id}[/bold green]")
except Exception as e:
    console.print(f"[bold red]❌ Failed to connect to VK: {e}[/bold red]")
    exit(1)

bot_app = Client("bot_session", api_id=config.API_ID, api_hash=config.API_HASH, bot_token=config.VK_BOT)
user_app = Client("user_session", api_id=config.API_ID, api_hash=config.API_HASH, max_concurrent_transmissions=30, workers=10)

# ============================================================
# DATABASE & PERSISTENCE
# ============================================================
def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def _init_db():
    conn = _connect()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            chat_id INTEGER,
            msg_chat_id INTEGER,
            msg_id INTEGER,
            album_id INTEGER,
            album_name TEXT,
            query TEXT,
            idx INTEGER,
            status TEXT,
            file_path TEXT,
            caption TEXT,
            updated_at REAL
        )
    """)
    for stmt in (
        "ALTER TABLE jobs ADD COLUMN caption TEXT",
        "ALTER TABLE jobs ADD COLUMN playlist_id TEXT",
        "ALTER TABLE jobs ADD COLUMN is_pilot INTEGER DEFAULT 0",
    ):
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError:
            pass

    conn.execute("""
        CREATE TABLE IF NOT EXISTS playlists (
            playlist_id TEXT PRIMARY KEY,
            chat_id INTEGER,
            query TEXT,
            album_name TEXT,
            album_id INTEGER,
            status TEXT,
            total INTEGER,
            completed INTEGER,
            failed INTEGER,
            skipped_dupes INTEGER,
            created_at REAL,
            updated_at REAL
        )
    """)

    # NEW MONITOR TABLES
    conn.execute("""
        CREATE TABLE IF NOT EXISTS monitored_chats (
            chat_identifier TEXT PRIMARY KEY,
            resolved_id INTEGER,
            added_at REAL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS monitored_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            msg_id INTEGER,
            tag TEXT,
            file_unique_id TEXT,
            caption TEXT,
            is_queued INTEGER DEFAULT 0,
            discovered_at REAL,
            UNIQUE(chat_id, msg_id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_mon_tag ON monitored_messages(tag, is_queued)")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS monitored_tags_meta (
            tag TEXT PRIMARY KEY,
            last_seen_count INTEGER DEFAULT 0,
            last_checked_at REAL
        )
    """)

    conn.execute("CREATE TABLE IF NOT EXISTS control (key TEXT PRIMARY KEY, value TEXT)")
    conn.commit()
    conn.close()

_init_db()

async def db_execute(query, params=(), fetch=None):
    def run():
        conn = _connect()
        try:
            cur = conn.execute(query, params)
            res = cur.fetchone() if fetch == "one" else cur.fetchall() if fetch == "all" else None
            conn.commit()
            return res
        finally:
            conn.close()
    return await asyncio.to_thread(run)

async def save_job(job):
    await db_execute(
        """INSERT INTO jobs (job_id, playlist_id, chat_id, msg_chat_id, msg_id, album_id, album_name, query, idx, is_pilot, status, file_path, caption, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(job_id) DO UPDATE SET status=excluded.status, file_path=excluded.file_path,
               is_pilot=excluded.is_pilot, updated_at=excluded.updated_at""",
        (job['job_id'], job.get('playlist_id'), job['chat_id'], job['msg_chat_id'], job['msg_id'],
         job['album_id'], job['album_name'], job['query'], job['idx'], int(job.get('is_pilot', False)),
         job['status'], job.get('file_path'), job.get('caption', ''), time.time())
    )

async def update_job_status(job_id, status, file_path=None):
    if file_path is not None:
        await db_execute("UPDATE jobs SET status=?, file_path=?, updated_at=? WHERE job_id=?", (status, file_path, time.time(), job_id))
    else:
        await db_execute("UPDATE jobs SET status=?, updated_at=? WHERE job_id=?", (status, time.time(), job_id))

async def delete_job_row(job_id):
    await db_execute("DELETE FROM jobs WHERE job_id=?", (job_id,))

async def set_control(key, value):
    await db_execute("INSERT INTO control (key, value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))

async def get_control(key, default=None):
    row = await db_execute("SELECT value FROM control WHERE key=?", (key,), fetch="one")
    return row[0] if row else default

async def is_msg_in_db(msg_chat_id, msg_id):
    row = await db_execute("SELECT status FROM jobs WHERE msg_chat_id=? AND msg_id=?", (msg_chat_id, msg_id), fetch="one")
    return bool(row)

async def create_playlist(chat_id, query, album_name, album_id, total):
    playlist_id = f"pl_{int(time.time() * 1000)}_{chat_id}"
    await db_execute(
        """INSERT INTO playlists (playlist_id, chat_id, query, album_name, album_id, status, total, completed, failed, skipped_dupes, created_at, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (playlist_id, chat_id, query, album_name, album_id, "WAITING", total, 0, 0, 0, time.time(), time.time())
    )
    return playlist_id

async def set_playlist_status(playlist_id, status):
    await db_execute("UPDATE playlists SET status=?, updated_at=? WHERE playlist_id=?", (status, time.time(), playlist_id))

async def bump_playlist(playlist_id, completed_delta=0, failed_delta=0):
    await db_execute(
        "UPDATE playlists SET completed = completed + ?, failed = failed + ?, updated_at=? WHERE playlist_id=?",
        (completed_delta, failed_delta, time.time(), playlist_id)
    )

async def get_playlist(playlist_id):
    return await db_execute(
        "SELECT playlist_id, chat_id, query, album_name, album_id, status, total, completed, failed, skipped_dupes FROM playlists WHERE playlist_id=?",
        (playlist_id,), fetch="one"
    )

async def list_playlists(limit=15):
    return await db_execute(
        "SELECT playlist_id, album_name, status, total, completed, failed, skipped_dupes FROM playlists WHERE status != 'KILLED' ORDER BY updated_at DESC LIMIT ?",
        (limit,), fetch="all"
    )

# ============================================================
# GLOBAL STATE & UI CONTROL
# ============================================================
download_queue = asyncio.Queue()
upload_queue = asyncio.Queue(maxsize=MAX_STAGED_FILES)
active_jobs = {}
cancelled_jobs = set()
user_states = {}
ui_state = "MAIN"
monitor_page = 0

playlist_queues: dict[str, deque] = {}
playlist_order: deque = deque()
vk_video_title_cache: dict[int, set] = {}

ENGINE_RUNNING = "RUNNING"
ENGINE_PAUSE_REQUESTED = "PAUSE_REQUESTED"
ENGINE_PAUSED = "PAUSED"
engine_state = ENGINE_RUNNING

pause_event = asyncio.Event()
pause_event.set()

def free_space_gb(path=DOWNLOAD_DIR):
    _, _, free = shutil.disk_usage(path)
    return free / (1024 ** 3)

def vk_title_for(album_name, idx, msg_id=None):
    if msg_id:
        return f"{album_name} - P{idx} [TG_{msg_id}]"
    return f"{album_name} - P{idx}"

def display_title(album_name, idx, caption, msg_id=None):
    caption = (caption or "").strip()
    if caption:
        base = caption.split('\n')[0].strip()
        if msg_id and f"[TG_{msg_id}]" not in base:
            return f"{base} [TG_{msg_id}]"
        return base
    return vk_title_for(album_name, idx, msg_id)

async def refresh_vk_cache(album_id):
    try:
        items = await asyncio.to_thread(vk.video.get, owner_id=my_vk_id, album_id=album_id, count=200)
        titles = {v.get('title', '') for v in items.get('items', [])}
    except Exception:
        titles = set()
    vk_video_title_cache[album_id] = titles
    return titles

async def vk_title_exists(album_id, title):
    if album_id not in vk_video_title_cache:
        await refresh_vk_cache(album_id)
    return title in vk_video_title_cache.get(album_id, set())

def enqueue_playlist_job(playlist_id, job):
    playlist_queues.setdefault(playlist_id, deque()).append(job)
    if playlist_id not in playlist_order:
        playlist_order.append(playlist_id)

def update_metrics(task_key, rich_task_id, action, current, total):
    if total == 0: return
    job = active_jobs.get(task_key)
    if not job: return
    percent = (current / total) * 100
    elapsed = time.time() - job['start_time']
    speed_bps = current / elapsed if elapsed > 0 else 0
    eta_sec = (total - current) / speed_bps if speed_bps > 0 else 0
    job.update({
        'progress': percent,
        'speed': f"{speed_bps / (1024*1024):.1f} MB/s" if speed_bps > 0 else "0 MB/s",
        'eta': f"{int(eta_sec // 60)}m {int(eta_sec % 60)}s",
        'action': action
    })
    progress_ui.update(rich_task_id, completed=percent)

# ============================================================
# MONITORING WORKFLOW ENGINE
# ============================================================
async def extract_and_store_message(chat_id, msg_id, text, file_unique_id):
    if not text:
        return
    tags = re.findall(r"#([A-Za-z0-9_]+)", text)
    if not tags:
        return
    
    for raw_tag in tags:
        tag = f"#{raw_tag.lower()}"
        await db_execute(
            """INSERT INTO monitored_messages (chat_id, msg_id, tag, file_unique_id, caption, discovered_at)
               VALUES (?,?,?,?,?,?)
               ON CONFLICT(chat_id, msg_id) DO UPDATE SET caption=excluded.caption""",
            (chat_id, msg_id, tag, file_unique_id, text, time.time())
        )

# ------------------------------------------------------------
# FIX #1: "Peer id invalid" on restart
#
# Pyrogram can only fetch history by a raw numeric chat ID if that peer is
# already present in this session's local peer cache. A stored numeric ID
# from a previous run is NOT guaranteed to still be resolvable after a
# restart. Re-resolving via get_chat() (username or ID) before touching
# history refreshes the peer cache and fixes "Peer id invalid".
# ------------------------------------------------------------
async def scan_chat_history(chat_identifier, resolved_chat_id):
    console.print(f"[bold cyan]🔍 Scanning historical messages for {chat_identifier} ({resolved_chat_id})...[/bold cyan]")
    try:
        cid_str = str(chat_identifier).strip()
        if cid_str.lstrip('-').isdigit():
            chat_obj = await user_app.get_chat(int(cid_str))
        else:
            chat_obj = await user_app.get_chat(cid_str)

        resolved_chat_id = chat_obj.id
        if resolved_chat_id != chat_obj.id:
            pass  # kept for clarity; see update below

        # Keep the DB in sync in case the resolved numeric ID drifted
        await db_execute(
            "UPDATE monitored_chats SET resolved_id=? WHERE chat_identifier=?",
            (resolved_chat_id, chat_identifier)
        )

        async for message in user_app.get_chat_history(resolved_chat_id):
            if message.video or message.document:
                txt = message.caption or message.text or ""
                f_id = message.video.file_unique_id if message.video else (message.document.file_unique_id if message.document else "")
                await extract_and_store_message(resolved_chat_id, message.id, txt, f_id)
        console.print(f"[bold green]✅ Finished historical scan for {chat_identifier}[/bold green]")
    except Exception as e:
        console.print(f"[bold red]❌ Failed scan history for {chat_identifier}: {e}[/bold red]")

@user_app.on_message(filters.video | filters.document)
async def live_monitor_handler(client, message):
    chat_id = message.chat.id
    # Check if this chat is monitored
    row = await db_execute("SELECT chat_identifier FROM monitored_chats WHERE resolved_id=?", (chat_id,), fetch="one")
    if not row:
        return
    txt = message.caption or message.text or ""
    f_id = message.video.file_unique_id if message.video else (message.document.file_unique_id if message.document else "")
    await extract_and_store_message(chat_id, message.id, txt, f_id)

async def add_monitored_target(target_raw):
    target_raw = target_raw.strip()
    resolved_id = None
    try:
        if target_raw.startswith("-100") or target_raw.startswith("-") or target_raw.isdigit():
            resolved_id = int(target_raw)
        else:
            chat_obj = await user_app.get_chat(target_raw)
            resolved_id = chat_obj.id
        
        await db_execute(
            "INSERT INTO monitored_chats (chat_identifier, resolved_id, added_at) VALUES (?,?,?) ON CONFLICT(chat_identifier) DO UPDATE SET resolved_id=excluded.resolved_id",
            (target_raw, resolved_id, time.time())
        )
        asyncio.create_task(scan_chat_history(target_raw, resolved_id))
        return True, resolved_id
    except Exception as e:
        return False, str(e)

# ============================================================
# HIGH-SPEED PARALLEL DOWNLOAD ENGINE (v2 - DC Safe)
# ============================================================
import math

CHUNK_SIZE = 1024 * 1024           
MEM_BUFFER_SIZE = 8 * 1024 * 1024  

def get_part_count(file_size):
    """Conservative Part Allocation to prevent Telegram FloodWaits"""
    mb = file_size / (1024 * 1024)
    if mb < 200: return 1      # Small files don't need parallel connections
    elif mb < 1024: return 2   # Medium files: 2 parallel parts
    elif mb < 3072: return 3   # Large files: 3 parallel parts
    else: return 4             # Massive files: max 4 parallel parts

class ProgressTracker:
    def __init__(self, total, callback):
        self.total = total
        self.downloaded = 0
        self.callback = callback
        self.lock = asyncio.Lock()

    async def update(self, bytes_added):
        async with self.lock:
            self.downloaded += bytes_added
            self.callback(self.downloaded, self.total)

async def _download_segment(client, message, chunk_offset, chunk_limit, part_file, job_id, tracker):
    async with global_segment_semaphore:
        retries = 3
        while retries > 0:
            downloaded_this_attempt = 0
            try:
                buffer = bytearray()
                with open(part_file, "wb") as f:
                    async for chunk in client.stream_media(message, limit=chunk_limit, offset=chunk_offset):
                        if job_id in cancelled_jobs:
                            raise Exception("ForceAbort")
                        await pause_event.wait()

                        buffer.extend(chunk)
                        downloaded_this_attempt += len(chunk)
                        await tracker.update(len(chunk))

                        if len(buffer) >= MEM_BUFFER_SIZE:
                            f.write(buffer)
                            buffer.clear()

                    if buffer:
                        f.write(buffer)
                        buffer.clear()
                break
            except Exception as e:
                if str(e) == "ForceAbort":
                    raise
                retries -= 1
                await tracker.update(-downloaded_this_attempt)
                if retries == 0:
                    raise e
                await asyncio.sleep(2)

async def async_fast_download(client, message, file_path, progress_callback, job_id):
    file_size = message.video.file_size
    total_chunks = math.ceil(file_size / CHUNK_SIZE)
    parts_count = get_part_count(file_size)
    
    if total_chunks <= 1:
        parts_count = 1

    base_chunks = total_chunks // parts_count
    remainder = total_chunks % parts_count

    ranges = []
    current_offset = 0
    for i in range(parts_count):
        limit = base_chunks + (1 if i < remainder else 0)
        ranges.append((current_offset, limit))
        current_offset += limit

    tracker = ProgressTracker(file_size, progress_callback)
    part_files = [f"{file_path}.part{i}" for i in range(parts_count)]

    # Spawn Parallel Tasks
    tasks = []
    for i, (chunk_offset, chunk_limit) in enumerate(ranges):
        if chunk_limit == 0: 
            continue
        tasks.append(
            asyncio.create_task(
                _download_segment(
                    client=client,
                    message=message,
                    chunk_offset=chunk_offset,
                    chunk_limit=chunk_limit,
                    part_file=part_files[i],
                    job_id=job_id,
                    tracker=tracker
                )
            )
        )
        # Stagger requests by 0.5s to prevent Telegram FloodWait flags
        await asyncio.sleep(0.5)

    # Wait for all parallel downloads to finish, but handle failures safely!
    try:
        await asyncio.gather(*tasks)
    except Exception as e:
        # If one chunk fails, kill all the other background chunks
        for task in tasks:
            if not task.done():
                task.cancel()
        raise e  # Pass the error up so the worker can retry properly

    # Now stitch the completed parts together
    with open(file_path, 'wb') as outfile:
        for part in part_files:
            if os.path.exists(part):
                with open(part, 'rb') as infile:
                    while True:
                        chunk = infile.read(MEM_BUFFER_SIZE)
                        if not chunk:
                            break
                        outfile.write(chunk)
                os.remove(part)

    return file_path

# ============================================================
# BUFFERED UPLOAD READER
# ============================================================
class ProgressFileReader(io.IOBase):
    def __init__(self, filename, callback):
        self._f = open(filename, 'rb')
        self._callback = callback
        self._total = os.path.getsize(filename)
        self._read_bytes = 0

    def read(self, size=-1):
        chunk = self._f.read(size)
        self._read_bytes += len(chunk)
        if self._callback:
            self._callback(self._read_bytes, self._total)
        return chunk

    def fileno(self):
        return self._f.fileno()

    def tell(self):
        return self._f.tell()

    def seek(self, offset, whence=io.SEEK_SET):
        res = self._f.seek(offset, whence)
        self._read_bytes = self._f.tell()
        return res

    def close(self):
        self._f.close()
        
    def __len__(self):
        return self._total

# ------------------------------------------------------------
# FIX #2: VK "406 Not Acceptable" on upload
#
# VK's upload.do endpoint rejects requests sent with
# Transfer-Encoding: chunked and needs an explicit Content-Length.
# aiohttp's FormData couldn't reliably determine the size of our custom
# ProgressFileReader and fell back to chunked encoding -> 406.
#
# `requests` computes Content-Length via super_len(), which checks
# fileno() + os.fstat() on file-like objects. Since ProgressFileReader
# implements fileno(), requests gets a real Content-Length and never
# sends chunked. This is run in a worker thread via asyncio.to_thread
# so it doesn't block the event loop.
# ------------------------------------------------------------
def _sync_vk_upload(upload_url, file_path, progress_callback, job_id):
    reader = ProgressFileReader(file_path, progress_callback)
    try:
        files = {'video_file': (os.path.basename(file_path), reader, 'video/mp4')}
        resp = requests.post(upload_url, files=files, timeout=None)
        if resp.status_code != 200:
            # Surface the actual VK response body instead of just the status code,
            # so future failures are diagnosable without re-adding print statements.
            raise Exception(f"VK upload {resp.status_code}: {resp.text[:300]}")
        return resp.json()
    finally:
        reader.close()

# ------------------------------------------------------------
# FIX #3: Corrupt "recovered" files getting pushed straight to upload
#
# On restart, jobs with status "downloaded"/"uploading" whose file still
# exists on disk were trusted blindly and shoved into upload_queue with no
# integrity check. If a previous run crashed or was killed mid-download,
# the file can be truncated (classic symptom: "moov atom not found") --
# VK's upload server correctly 406s on it, and the job silently loops.
# This validates with ffprobe before trusting a recovered file; anything
# invalid gets deleted and requeued for a fresh download instead.
# ------------------------------------------------------------
async def _validate_video_file(file_path):
    def _run():
        try:
            result = subprocess.run(
                ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                 "-of", "default=noprint_wrappers=1:nokey=1", file_path],
                capture_output=True, text=True, timeout=30
            )
            if result.returncode != 0:
                return False
            duration = result.stdout.strip()
            return bool(duration) and float(duration) > 0
        except Exception:
            return False
    return await asyncio.to_thread(_run)

# ============================================================
# JOB COMPLETION / PLAYLIST BOOKKEEPING
# ============================================================
async def on_job_finished(job):
    playlist_id = job.get('playlist_id')
    await delete_job_row(job['job_id'])
    if not playlist_id:
        return

    if job.get('is_pilot'):
        await bump_playlist(playlist_id, completed_delta=1)
        await set_playlist_status(playlist_id, "WAITING_CONFIRMATION")
        return

    await bump_playlist(playlist_id, completed_delta=1)
    row = await get_playlist(playlist_id)
    if row:
        _, _, _, _, _, status, total, completed, failed, skipped = row
        if status not in ("KILLED", "COMPLETED") and (completed + failed + skipped) >= total:
            await set_playlist_status(playlist_id, "COMPLETED")

async def continue_playlist(playlist_id):
    rows = await db_execute(
        """SELECT job_id, chat_id, msg_chat_id, msg_id, album_id, album_name, query, idx, caption
           FROM jobs WHERE playlist_id=? AND status='waiting'""",
        (playlist_id,), fetch="all"
    )
    for r in rows:
        job = {
            'job_id': r[0], 'playlist_id': playlist_id, 'chat_id': r[1], 'msg_chat_id': r[2],
            'msg_id': r[3], 'album_id': r[4], 'album_name': r[5], 'query': r[6], 'idx': r[7],
            'caption': r[8], 'file_path': None, 'is_pilot': False
        }
        await update_job_status(job['job_id'], "queued")
        enqueue_playlist_job(playlist_id, job)
    await set_playlist_status(playlist_id, "RUNNING")

async def kill_playlist(playlist_id):
    await set_playlist_status(playlist_id, "KILLED")
    rows = await db_execute(
        "SELECT job_id, file_path FROM jobs WHERE playlist_id=? AND status NOT IN ('done')",
        (playlist_id,), fetch="all"
    )
    for job_id, file_path in rows:
        cancelled_jobs.add(job_id)
        if file_path and os.path.exists(file_path):
            try: os.remove(file_path)
            except: pass
    await db_execute("DELETE FROM jobs WHERE playlist_id=? AND status='waiting'", (playlist_id,))
    playlist_queues.pop(playlist_id, None)
    try:
        playlist_order.remove(playlist_id)
    except ValueError:
        pass

# ============================================================
# SCHEDULER
# ============================================================
async def scheduler_loop():
    while True:
        await asyncio.sleep(SCHEDULER_TICK)
        if engine_state != ENGINE_RUNNING:
            continue
        if download_queue.qsize() >= SCHEDULER_INFLIGHT_TARGET:
            continue

        attempts = len(playlist_order)
        for _ in range(attempts):
            if not playlist_order:
                break
            pid = playlist_order[0]
            playlist_order.rotate(-1)
            q = playlist_queues.get(pid)
            if not q:
                try: playlist_order.remove(pid)
                except ValueError: pass
                continue

            job = q.popleft()
            if not q:
                try: playlist_order.remove(pid)
                except ValueError: pass

            if job['job_id'] in cancelled_jobs:
                continue

            await update_job_status(job['job_id'], "queued")
            await download_queue.put(job)
            break

# ============================================================
# WORKERS
# ============================================================
async def download_worker(worker_id):
    while True:
        await pause_event.wait()
        job = await download_queue.get()
        rich_task = None
        job_id = job['job_id']
        dl_key = f"{job_id}_DL"
        delete_file_on_exit = False
        file_path = None
        display_name = f"{job['query']} (Pt.{job['idx']})"

        try:
            if job_id in cancelled_jobs:
                continue

            await pause_event.wait()
            while free_space_gb() < MIN_FREE_GB: await asyncio.sleep(5)

            active_jobs[dl_key] = {"name": display_name, "action": "📥 DL", "progress": 0, "speed": "0 MB/s", "eta": "Calc...", "start_time": time.time(), "job_id": job_id}
            rich_task = progress_ui.add_task(f"[cyan]📥 DL {display_name}", total=100, name=display_name)
            await update_job_status(job_id, "downloading")

            def dl_progress(current, total):
                update_metrics(dl_key, rich_task, "📥 DL", current, total)

            msg = await user_app.get_messages(job['msg_chat_id'], job['msg_id'])
            target_file_path = os.path.join(DOWNLOAD_DIR, f"{job_id}.mp4")

            file_path = await async_fast_download(
                client=user_app,
                message=msg,
                file_path=target_file_path,
                progress_callback=dl_progress,
                job_id=job_id
            )

            if file_path and os.path.exists(file_path):
                job['file_path'] = file_path
                await update_job_status(job_id, "downloaded", file_path=file_path)
                await upload_queue.put(job)
            else:
                await update_job_status(job_id, "waiting")

        except Exception as e:
            if str(e) == "ForceAbort":
                console.print(f"[bold yellow]💀 Aborted DL: {display_name}[/bold yellow]")
                delete_file_on_exit = True
            else:
                console.print(f"[bold red]DL failed {display_name}: {e}[/bold red]")
                await update_job_status(job_id, "waiting")
        finally:
            if delete_file_on_exit and file_path and os.path.exists(file_path):
                try: os.remove(file_path)
                except: pass
            if rich_task is not None: progress_ui.remove_task(rich_task)
            active_jobs.pop(dl_key, None)
            download_queue.task_done()

async def upload_worker(worker_id):
    while True:
        job = await upload_queue.get()
        job_id = job['job_id']
        up_key = f"{job_id}_UP"
        file_path = job.get('file_path')
        display_name = f"{job['query']} (Pt.{job['idx']})"
        rich_task = None
        delete_file_on_exit = False

        try:
            if job_id in cancelled_jobs:
                delete_file_on_exit = True
                continue

            if not file_path or not os.path.exists(file_path):
                await update_job_status(job_id, "waiting", file_path=None)
                if job.get('playlist_id'):
                    enqueue_playlist_job(job['playlist_id'], job)
                else:
                    await download_queue.put(job)
                continue

            # Validate BEFORE every upload attempt (not just at startup). Without this,
            # a corrupt/truncated file that fails upload gets pushed straight back into
            # upload_queue and retries forever with the same bad file.
            if not await _validate_video_file(file_path):
                console.print(f"[bold red]⚠️ Corrupt file detected before upload, requeueing for redownload: {file_path}[/bold red]")
                try: os.remove(file_path)
                except: pass
                job['file_path'] = None
                await update_job_status(job_id, "waiting", file_path="")
                if job.get('playlist_id'):
                    enqueue_playlist_job(job['playlist_id'], job)
                else:
                    await download_queue.put(job)
                continue

            active_jobs[up_key] = {"name": display_name, "action": "📤 UP", "progress": 0, "speed": "0 MB/s", "eta": "Calc...", "start_time": time.time(), "job_id": job_id}
            rich_task = progress_ui.add_task(f"[magenta]📤 UP {display_name}", total=100, name=display_name)
            await update_job_status(job_id, "uploading")

            title = display_title(job['album_name'], job['idx'], job.get('caption', ''), job['msg_id'])
            upload_info = await asyncio.to_thread(vk.video.save, name=title, description=job.get('caption', ''), album_id=job['album_id'])

            def up_progress(current, total):
                if job_id in cancelled_jobs: raise Exception("ForceAbort")
                update_metrics(up_key, rich_task, "📤 UP", current, total)

            await asyncio.to_thread(_sync_vk_upload, upload_info['upload_url'], file_path, up_progress, job_id)

            vk_video_title_cache.setdefault(job['album_id'], set()).add(title)
            await update_job_status(job_id, "done") 
            delete_file_on_exit = True
            # Mark monitored message as queued if it came from monitor
            await db_execute("UPDATE monitored_messages SET is_queued=1 WHERE chat_id=? AND msg_id=?", (job['msg_chat_id'], job['msg_id']))
            await on_job_finished(job)

        except Exception as e:
            if str(e) == "ForceAbort":
                console.print(f"[bold yellow]💀 Aborted UP: {display_name}[/bold yellow]")
                delete_file_on_exit = True
            else:
                console.print(f"[bold red]UP failed {display_name}: {e}[/bold red]")
                await update_job_status(job_id, "downloaded")
                await upload_queue.put(job)
                await asyncio.sleep(3)
        finally:
            if delete_file_on_exit and file_path and os.path.exists(file_path):
                try: os.remove(file_path)
                except: pass
            if rich_task is not None: progress_ui.remove_task(rich_task)
            active_jobs.pop(up_key, None)
            upload_queue.task_done()

# ============================================================
# DASHBOARD RENDERING ENGINE
# ============================================================
def _engine_banner():
    if engine_state == ENGINE_RUNNING:
        return "⚡ RUNNING"
    if engine_state == ENGINE_PAUSE_REQUESTED:
        return "🟡 Pause Requested — draining active transfers..."
    return "⏸️ PAUSED"

async def render_dashboard():
    chat_id = await get_control("dashboard_chat_id")
    msg_id = await get_control("dashboard_msg_id")
    if not chat_id or not msg_id: return

    global monitor_page
    scheduled_pending = sum(len(v) for v in playlist_queues.values())
    queued_dl = download_queue.qsize() + scheduled_pending
    staged_up = upload_queue.qsize()
    active_dls = [j for j in active_jobs.values() if j['action'] == "📥 DL"]
    active_ups = [j for j in active_jobs.values() if j['action'] == "📤 UP"]

    text = ""
    buttons = []

    if ui_state == "MAIN":
        monitored_chat_count = await db_execute("SELECT COUNT(*) FROM monitored_chats", fetch="one")
        m_count = monitored_chat_count[0] if monitored_chat_count else 0

        text = (f"📊 **GLOBAL TRANSFER ENGINE**\n{_engine_banner()} | 💾 Free Disk: {free_space_gb():.1f} GB\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📥 **Downloading:** {len(active_dls)} Active | {queued_dl} Queued\n"
                f"📦 **Staged on Disk:** {staged_up} Files\n"
                f"📤 **Uploading:** {len(active_ups)} Active\n"
                f"👁️ **Monitored Chats:** {m_count} Active\n"
                f"━━━━━━━━━━━━━━━━━━\n")

        waiting_playlists = await db_execute(
            "SELECT playlist_id, album_name, total FROM playlists WHERE status='WAITING_CONFIRMATION' ORDER BY updated_at DESC LIMIT 3",
            fetch="all"
        )
        if waiting_playlists:
            text += "🧪 **Pilots awaiting confirmation:**\n"
            for pid, album_name, total in waiting_playlists:
                text += f"• {album_name} ({total - 1} more videos)\n"
                buttons.append([
                    InlineKeyboardButton(f"▶️ Continue {album_name}", callback_data=f"plcontinue_{pid}"),
                    InlineKeyboardButton("💀 Kill", callback_data=f"plkill_{pid}")
                ])
            text += "━━━━━━━━━━━━━━━━━━\n"

        buttons.append([
            InlineKeyboardButton(f"📥 View DLs ({len(active_dls)})", callback_data="ui_DL_VIEW"),
            InlineKeyboardButton(f"📤 View UPs ({len(active_ups)})", callback_data="ui_UP_VIEW")
        ])
        buttons.append([
            InlineKeyboardButton("👁️ Monitor Findings", callback_data="ui_MONITOR_VIEW"),
            InlineKeyboardButton("📋 All Playlists", callback_data="ui_PLAYLISTS")
        ])
        buttons.append([
            InlineKeyboardButton("▶️ Resume" if engine_state != ENGINE_RUNNING else "⏸️ Pause", callback_data="toggle_pause"),
            InlineKeyboardButton("🛑 Clear All Pendings", callback_data="clear_queue")
        ])

    elif ui_state == "MONITOR_VIEW":
        # Pagination & Tag Retrieval via Zero-RAM SQL Aggregation
        total_tags_row = await db_execute("SELECT COUNT(DISTINCT tag) FROM monitored_messages WHERE is_queued=0", fetch="one")
        total_tags = total_tags_row[0] if total_tags_row else 0
        total_vids_row = await db_execute("SELECT COUNT(*) FROM monitored_messages WHERE is_queued=0", fetch="one")
        total_vids = total_vids_row[0] if total_vids_row else 0

        PAGE_SIZE = 5
        max_pages = max(1, math.ceil(total_tags / PAGE_SIZE))
        if monitor_page >= max_pages: monitor_page = max_pages - 1
        if monitor_page < 0: monitor_page = 0

        tags_data = await db_execute(
            "SELECT tag, COUNT(*) as cnt FROM monitored_messages WHERE is_queued=0 GROUP BY tag ORDER BY cnt DESC LIMIT ? OFFSET ?",
            (PAGE_SIZE, monitor_page * PAGE_SIZE), fetch="all"
        )

        text = (f"👁️ **MONITOR FINDINGS** (Page {monitor_page + 1} of {max_pages})\n"
                f"Total Tags Discovered: {total_tags} | Unqueued Videos: {total_vids}\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"Select a tag to inspect breakdown or queue:\n")

        for tag, cnt in (tags_data or []):
            clean_tag_name = tag.replace("#", "")
            buttons.append([InlineKeyboardButton(f"🚀 Queue {tag} ({cnt} vids)", callback_data=f"mon_inspect_{tag}")])

        nav_row = []
        if monitor_page > 0:
            nav_row.append(InlineKeyboardButton("◀️ Prev", callback_data="mon_prev_page"))
        nav_row.append(InlineKeyboardButton(f"Page {monitor_page + 1}/{max_pages}", callback_data="noop"))
        if monitor_page < max_pages - 1:
            nav_row.append(InlineKeyboardButton("Next ▶️", callback_data="mon_next_page"))
        if nav_row:
            buttons.append(nav_row)

        buttons.append([InlineKeyboardButton("🔍 Search Tag", callback_data="mon_search_prompt")])
        buttons.append([InlineKeyboardButton("🔙 Back to Main", callback_data="ui_MAIN")])

    elif ui_state.startswith("MON_INSPECT_"):
        tag = ui_state.replace("MON_INSPECT_", "")
        tg_total_row = await db_execute("SELECT COUNT(*) FROM monitored_messages WHERE tag=?", (tag,), fetch="one")
        tg_total = tg_total_row[0] if tg_total_row else 0

        # Check metadata delta
        meta_row = await db_execute("SELECT last_seen_count FROM monitored_tags_meta WHERE tag=?", (tag,), fetch="one")
        last_seen = meta_row[0] if meta_row else tg_total
        delta_new = max(0, tg_total - last_seen)

        album_name = tag.replace("#", "")
        existing = await asyncio.to_thread(vk.video.getAlbums, owner_id=my_vk_id, count=100)
        album_id = next((alb['id'] for alb in existing['items'] if alb['title'].lower() == album_name.lower()), None)
        
        vk_count = 0
        if album_id:
            await refresh_vk_cache(album_id)
            titles = vk_video_title_cache.get(album_id, set())
            vk_count = len(titles)

        will_add = max(0, tg_total - vk_count)

        text = (f"🔍 **HASHTAG DETAILS: {tag}**\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📱 Telegram Total Found: {tg_total} videos\n"
                f"🆕 Added in Telegram since last check: +{delta_new} videos\n\n"
                f"🌐 **VK Status Breakdown:**\n"
                f"✅ Already in VK: {vk_count} videos\n"
                f"📥 Will be Added to VK: {will_add} videos\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"Ready to queue missing videos for VK?")

        # Update last seen meta
        await db_execute(
            "INSERT INTO monitored_tags_meta (tag, last_seen_count, last_checked_at) VALUES (?,?,?) ON CONFLICT(tag) DO UPDATE SET last_seen_count=excluded.last_seen_count, last_checked_at=excluded.last_checked_at",
            (tag, tg_total, time.time())
        )

        buttons.append([InlineKeyboardButton(f"🚀 Queue {will_add} Videos for VK", callback_data=f"mon_queue_tag_{tag}")])
        buttons.append([InlineKeyboardButton("🔙 Back to Findings", callback_data="ui_MONITOR_VIEW")])

    elif ui_state in ["DL_VIEW", "UP_VIEW"]:
        target_list = active_dls if ui_state == "DL_VIEW" else active_ups
        icon = "📥 DOWNLOADING" if ui_state == "DL_VIEW" else "📤 UPLOADING"
        text = f"{icon} **({len(target_list)} Active Workers)**\n━━━━━━━━━━━━━━━━━━\n"

        for job in target_list:
            filled = int(job['progress'] / 10)
            bar = "🟩" * filled + "⬜" * (10 - filled)
            text += f"▶️ **{job['name']}**\n↳ {bar} {job['progress']:.1f}% ({job['speed']})\n\n"
            buttons.append([InlineKeyboardButton(f"💀 Kill {job['name']}", callback_data=f"kill_{job['job_id']}")])

        if not target_list: text += "No active jobs in this category.\n"
        buttons.append([InlineKeyboardButton("🔙 Back to Main", callback_data="ui_MAIN")])

    elif ui_state == "PLAYLISTS":
        rows = await list_playlists()
        text = "📋 **PLAYLISTS**\n━━━━━━━━━━━━━━━━━━\n"
        if not rows:
            text += "No playlists yet.\n"
        for pid, album_name, status, total, completed, failed, skipped in rows:
            text += f"• **{album_name}** — {status}\n  {completed}/{total} done"
            if failed: text += f", {failed} failed"
            if skipped: text += f", {skipped} skipped (dupes)"
            text += "\n\n"
            if status == "WAITING_CONFIRMATION":
                buttons.append([
                    InlineKeyboardButton(f"▶️ Continue {album_name}", callback_data=f"plcontinue_{pid}"),
                    InlineKeyboardButton("💀 Kill", callback_data=f"plkill_{pid}")
                ])
            elif status in ("PILOT_RUNNING", "RUNNING", "WAITING"):
                buttons.append([InlineKeyboardButton(f"💀 Kill {album_name}", callback_data=f"plkill_{pid}")])
        buttons.append([InlineKeyboardButton("🔙 Back to Main", callback_data="ui_MAIN")])

    try:
        await bot_app.edit_message_text(
            chat_id=int(chat_id),
            message_id=int(msg_id),
            text=text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
    except Exception:
        pass

async def dashboard_updater():
    global engine_state
    while True:
        await asyncio.sleep(4)

        if engine_state == ENGINE_PAUSE_REQUESTED:
            no_active = not any(j['action'] == "📥 DL" for j in active_jobs.values()) and \
                        not any(j['action'] == "📤 UP" for j in active_jobs.values())
            if no_active and upload_queue.empty():
                engine_state = ENGINE_PAUSED
                await set_control("engine_state", ENGINE_PAUSED)

        await render_dashboard()

# ============================================================
# BOT HANDLERS & CALLBACKS
# ============================================================
@bot_app.on_message(filters.command("start"))
async def start_cmd(client, message):
    msg = await message.reply_text("⚙️ Booting Global Dashboard...\n(Pinning message...)")
    try: await msg.pin(both_sides=True)
    except: pass
    await set_control("dashboard_chat_id", message.chat.id)
    await set_control("dashboard_msg_id", msg.id)
    await message.reply_text("👋 Dashboard Ready! Use /monitor to setup group scanning.")

@bot_app.on_message(filters.command("monitor"))
async def monitor_cmd(client, message):
    user_states[message.chat.id] = {'awaiting_monitor_input': True}
    await message.reply_text(
        "👁️ **MONITORING CONFIGURATION**\n"
        "Please send the Group IDs or public @usernames you want to monitor.\n"
        "*(You can enter multiple separated by commas, e.g., `-10012345678, @my_public_group`)*",
        parse_mode=ParseMode.MARKDOWN
    )

@bot_app.on_message(filters.private & filters.text & ~filters.command(["start", "help", "monitor"]))
async def handle_user_input(client, message):
    chat_id = message.chat.id
    state = user_states.get(chat_id, {})

    if state.get('awaiting_monitor_input'):
        user_states.pop(chat_id, None)
        raw_inputs = message.text.split(",")
        status_msg = await message.reply_text("⚙️ Resolving groups and launching background history scanners...")
        
        success_count = 0
        for target in raw_inputs:
            target = target.strip()
            if not target: continue
            success, _ = await add_monitored_target(target)
            if success: success_count += 1

        await status_msg.edit_text(f"✅ Successfully registered and scanning **{success_count}** groups/channels!", parse_mode=ParseMode.MARKDOWN)
        return

    if state.get('awaiting_tag_search'):
        user_states.pop(chat_id, None)
        query_tag = message.text.strip().lower()
        if not query_tag.startswith("#"):
            query_tag = f"#{query_tag}"
        
        exists = await db_execute("SELECT COUNT(*) FROM monitored_messages WHERE tag=?", (query_tag,), fetch="one")
        if not exists or exists[0] == 0:
            return await message.reply_text(f"❌ Tag `{query_tag}` not found in monitored database.")
        
        global ui_state
        ui_state = f"MON_INSPECT_{query_tag}"
        await render_dashboard()
        return

    if not state.get('awaiting_group'):
        search_query = message.text.strip()
        search_query = search_query if search_query.startswith("#") else f"#{search_query}"
        user_states[chat_id] = {'query': search_query, 'awaiting_group': True}
        return await message.reply_text(f"🔍 Search: *{search_query}*\n📌 Send the **Group Chat ID**.", parse_mode=ParseMode.MARKDOWN)

    try:
        target_group_id = int(message.text.strip())
    except ValueError:
        return await message.reply_text("⚠️ Invalid numeric ID.")

    status_msg = await message.reply_text("🔎 Scanning media group and applying Master Index logic...")
    raw_found, processed_groups = [], set()

    try:
        async for msg in user_app.search_messages(chat_id=target_group_id, query=state['query']):
            if msg.media_group_id:
                if msg.media_group_id not in processed_groups:
                    processed_groups.add(msg.media_group_id)
                    album_msgs = await user_app.get_media_group(target_group_id, msg.id)
                    album_msgs = sorted(album_msgs, key=lambda x: x.id)

                    master_caption = ""
                    for am in album_msgs:
                        if am.caption and am.caption.strip().startswith("#"):
                            master_caption = am.caption
                            break

                    track_data = {}
                    if master_caption:
                        lines = [line.strip() for line in master_caption.strip().split('\n') if line.strip()]
                        is_top_match = False
                        if lines and state['query'].lower() in lines[0].lower():
                            is_top_match = True

                        for line in lines[1:]:
                            match = re.match(r'^(\d+)\s*-\s*(.*)', line)
                            if match:
                                idx_str, rest_of_line = match.groups()
                                is_inline_match = state['query'].lower() in line.lower()

                                if is_top_match or is_inline_match:
                                    bracket_match = re.search(r"\(([^)]*)\)", rest_of_line)
                                    track_caption = bracket_match.group(1).strip() if bracket_match else rest_of_line.strip()
                                    track_data[int(idx_str)] = track_caption

                    for i, am in enumerate(album_msgs, start=1):
                        if not am.video and not am.document:
                            continue

                        if master_caption:
                            if i in track_data:
                                am._custom_album = state['query'].replace("#", "")
                                am._custom_caption = track_data[i]
                                am._relative_idx = i
                                raw_found.append(am)
                        else:
                            am._custom_album = state['query'].replace("#", "")
                            am._custom_caption = am.caption or f"Imported ({state['query']})"
                            am._relative_idx = i
                            raw_found.append(am)

            elif msg.video or msg.document:
                msg._custom_album = state['query'].replace("#", "")
                msg._custom_caption = msg.caption or f"Imported ({state['query']})"
                msg._relative_idx = 1
                raw_found.append(msg)

    except Exception as e:
        user_states.pop(chat_id, None)
        return await status_msg.edit_text(f"❌ Error accessing group: `{e}`")

    new_msgs, skipped_count = [], 0
    for msg in raw_found:
        if await is_msg_in_db(msg.chat.id, msg.id):
            skipped_count += 1
        else:
            new_msgs.append(msg)

    if not new_msgs:
        user_states.pop(chat_id, None)
        return await status_msg.edit_text(f"❌ No *new* matching videos found.\n(Skipped {skipped_count} already processed).", parse_mode=ParseMode.MARKDOWN)

    state['found_msgs'] = sorted(new_msgs, key=lambda m: getattr(m, '_relative_idx', 1))
    kbd = InlineKeyboardMarkup([[InlineKeyboardButton(f"🚀 Queue {len(new_msgs)} videos (pilot first)", callback_data="queue_transfer")]])
    await status_msg.edit_text(f"📊 **Found** `{state['query']}`\n🆕 New: *{len(new_msgs)}* | ⏭️ Skipped: *{skipped_count}*", parse_mode=ParseMode.MARKDOWN, reply_markup=kbd)

@bot_app.on_callback_query()
async def handle_buttons(client, callback):
    global engine_state, ui_state, monitor_page
    chat_id = callback.message.chat.id
    data = callback.data

    if data.startswith("ui_"):
        ui_state = data.replace("ui_", "")
        await render_dashboard()
        return await callback.answer()

    elif data == "noop":
        return await callback.answer()

    elif data == "mon_next_page":
        monitor_page += 1
        await render_dashboard()
        return await callback.answer()

    elif data == "mon_prev_page":
        monitor_page -= 1
        await render_dashboard()
        return await callback.answer()

    elif data == "mon_search_prompt":
        user_states[chat_id] = {'awaiting_tag_search': True}
        return await callback.message.reply_text("🔍 Please type the hashtag you want to search for (e.g. `#moviepack1`):")

    elif data.startswith("mon_inspect_"):
        tag = data.replace("mon_inspect_", "")
        ui_state = f"MON_INSPECT_{tag}"
        await render_dashboard()
        return await callback.answer()

    elif data.startswith("mon_queue_tag_"):
        tag = data.replace("mon_queue_tag_", "")
        await callback.answer(f"Queuing missing videos for {tag}...", show_alert=True)

        # Pull unqueued messages for this tag from DB
        rows = await db_execute("SELECT chat_id, msg_id, caption FROM monitored_messages WHERE tag=? AND is_queued=0", (tag,), fetch="all")
        if not rows:
            ui_state = "MONITOR_VIEW"
            await render_dashboard()
            return

        album_name = tag.replace("#", "")
        try:
            existing = await asyncio.to_thread(vk.video.getAlbums, owner_id=my_vk_id, count=100)
            album_id = next((alb['id'] for alb in existing['items'] if alb['title'].lower() == album_name.lower()), None)
            if not album_id:
                new_album = await asyncio.to_thread(vk.video.addAlbum, title=album_name)
                album_id = new_album if isinstance(new_album, int) else new_album['album_id']
        except Exception as e:
            await callback.message.reply_text(f"❌ Failed to resolve VK album: `{e}`")
            return

        await refresh_vk_cache(album_id)

        valid_jobs_data = []
        for idx, (c_id, m_id, caption) in enumerate(rows, start=1):
            title = display_title(album_name, idx, caption, m_id)
            if await vk_title_exists(album_id, title):
                # Mark as queued since it's already in VK
                await db_execute("UPDATE monitored_messages SET is_queued=1 WHERE chat_id=? AND msg_id=?", (c_id, m_id))
            else:
                valid_jobs_data.append((c_id, m_id, caption, idx))

        if not valid_jobs_data:
            ui_state = "MONITOR_VIEW"
            await render_dashboard()
            return

        playlist_id = await create_playlist(chat_id, tag, album_name, album_id, len(valid_jobs_data))
        pilot_added = False

        for c_id, m_id, caption, idx in valid_jobs_data:
            job_id = f"{c_id}_{m_id}"
            is_pilot = not pilot_added
            if is_pilot: pilot_added = True

            job = {
                'job_id': job_id, 'playlist_id': playlist_id, 'chat_id': chat_id,
                'msg_chat_id': c_id, 'msg_id': m_id, 'album_id': album_id,
                'album_name': album_name, 'query': tag, 'idx': idx,
                'is_pilot': is_pilot, 'status': 'waiting', 'file_path': None, 'caption': caption
            }
            await save_job(job)
            cancelled_jobs.discard(job_id)

            if is_pilot:
                await update_job_status(job_id, "queued")
                await download_queue.put(job)

        await set_playlist_status(playlist_id, "PILOT_RUNNING" if pilot_added else "RUNNING")
        ui_state = "MONITOR_VIEW"
        await render_dashboard()

    elif data.startswith("kill_"):
        job_id = data.replace("kill_", "")
        cancelled_jobs.add(job_id)
        await update_job_status(job_id, "cancelled")
        await callback.answer("💀 Poison pill dropped. Job aborting...", show_alert=True)
        await render_dashboard()

    elif data.startswith("plcontinue_"):
        playlist_id = data[len("plcontinue_"):]
        await continue_playlist(playlist_id)
        await callback.answer("▶️ Continuing playlist...")
        await render_dashboard()

    elif data.startswith("plkill_"):
        playlist_id = data[len("plkill_"):]
        await kill_playlist(playlist_id)
        await callback.answer("💀 Playlist killed.", show_alert=True)
        await render_dashboard()

    elif data == "toggle_pause":
        if engine_state == ENGINE_RUNNING:
            engine_state = ENGINE_PAUSE_REQUESTED
            pause_event.clear()
            await set_control("engine_state", ENGINE_PAUSE_REQUESTED)
            await callback.answer("🟡 Pause requested — draining active transfers...")
        else:
            # NOTE: fixed a pre-existing bug here — this was `==` (a no-op comparison)
            # instead of `=`, so Resume never actually flipped engine_state back to RUNNING.
            engine_state = ENGINE_RUNNING
            pause_event.set()
            await set_control("engine_state", ENGINE_RUNNING)
            await callback.answer("▶️ Resumed")
        await render_dashboard()

    elif data == "clear_queue":
        cleared = 0
        while not download_queue.empty():
            job = download_queue.get_nowait()
            download_queue.task_done()
            cancelled_jobs.add(job['job_id'])
            await delete_job_row(job['job_id'])
            cleared += 1
        for pid in list(playlist_queues.keys()):
            q = playlist_queues.pop(pid)
            for job in q:
                cancelled_jobs.add(job['job_id'])
                await delete_job_row(job['job_id'])
                cleared += 1
        playlist_order.clear()
        await render_dashboard()
        await callback.answer(f"Cleared {cleared} pending downloads.", show_alert=True)

    elif data == "queue_transfer":
        state = user_states.get(chat_id)
        if not state: return await callback.answer("Expired session.")
        await callback.answer("Building playlist & running pilot video...")

        album_name = state['query'].replace("#", "")

        try:
            existing = await asyncio.to_thread(vk.video.getAlbums, owner_id=my_vk_id, count=100)
            album_id = next((alb['id'] for alb in existing['items'] if alb['title'].lower() == album_name.lower()), None)
            if not album_id:
                new_album = await asyncio.to_thread(vk.video.addAlbum, title=album_name)
                album_id = new_album if isinstance(new_album, int) else new_album['album_id']
        except Exception as e:
            user_states.pop(chat_id, None)
            return await callback.message.edit_text(f"❌ Failed to resolve VK album: `{e}`")

        await refresh_vk_cache(album_id)

        non_dupe_msgs, skipped_dupes = [], 0
        for msg in state['found_msgs']:
            idx = getattr(msg, '_relative_idx', 1)
            caption = getattr(msg, '_custom_caption', "")
            title = display_title(album_name, idx, caption, msg.id)
            if await vk_title_exists(album_id, title):
                skipped_dupes += 1
            else:
                non_dupe_msgs.append(msg)

        if not non_dupe_msgs:
            user_states.pop(chat_id, None)
            await callback.message.delete()
            if not await get_control("dashboard_msg_id"):
                await start_cmd(client, callback.message)
            else:
                await render_dashboard()
            return

        playlist_id = await create_playlist(chat_id, state['query'], album_name, album_id, len(non_dupe_msgs))
        await db_execute("UPDATE playlists SET skipped_dupes=? WHERE playlist_id=?", (skipped_dupes, playlist_id))

        pilot_msg = non_dupe_msgs[0]

        for msg in non_dupe_msgs:
            idx = getattr(msg, '_relative_idx', 1)
            caption = getattr(msg, '_custom_caption', "")
            is_pilot = (msg is pilot_msg)
            job_id = f"{msg.chat.id}_{msg.id}"
            job = {
                'job_id': job_id, 'playlist_id': playlist_id, 'chat_id': chat_id,
                'msg_chat_id': msg.chat.id, 'msg_id': msg.id, 'album_id': album_id,
                'album_name': album_name, 'query': state['query'], 'idx': idx,
                'is_pilot': is_pilot, 'status': 'waiting', 'file_path': None, 'caption': caption
            }
            await save_job(job)
            cancelled_jobs.discard(job_id)

            if is_pilot:
                await update_job_status(job_id, "queued")
                await download_queue.put(job)

        await set_playlist_status(playlist_id, "PILOT_RUNNING")

        user_states.pop(chat_id, None)
        await callback.message.delete()

        if not await get_control("dashboard_msg_id"):
            await start_cmd(client, callback.message)
        else:
            await render_dashboard()

# ============================================================
# STARTUP
# ============================================================
async def main():
    global engine_state
    await user_app.start()
    await bot_app.start()

    saved_state = await get_control("engine_state", ENGINE_RUNNING)
    engine_state = saved_state if saved_state in (ENGINE_RUNNING, ENGINE_PAUSED) else ENGINE_RUNNING
    if engine_state != ENGINE_RUNNING:
        pause_event.clear()

    # Resume monitoring targets in background
    monitored_targets = await db_execute("SELECT chat_identifier, resolved_id FROM monitored_chats", fetch="all")
    if monitored_targets:
        for c_id, r_id in monitored_targets:
            asyncio.create_task(scan_chat_history(c_id, r_id))
        console.print(f"[bold yellow]👁️ Resumed background scanning for {len(monitored_targets)} monitored groups.[/bold yellow]")

    rows = await db_execute(
        "SELECT job_id, playlist_id, chat_id, msg_chat_id, msg_id, album_id, album_name, query, idx, is_pilot, status, file_path, caption FROM jobs",
        fetch="all"
    )
    recovered = 0
    if rows:
        for row in rows:
            job = {
                'job_id': row[0], 'playlist_id': row[1], 'chat_id': row[2], 'msg_chat_id': row[3],
                'msg_id': row[4], 'album_id': row[5], 'album_name': row[6], 'query': row[7],
                'idx': row[8], 'is_pilot': bool(row[9]), 'file_path': row[11], 'caption': row[12]
            }
            status, file_path = row[10], row[11]

            if status in ("downloaded", "uploading") and file_path and os.path.exists(file_path):
                if await _validate_video_file(file_path):
                    await update_job_status(job['job_id'], "downloaded")
                    await upload_queue.put(job)
                else:
                    console.print(f"[bold red]⚠️ Corrupt recovered file, requeueing for redownload: {file_path}[/bold red]")
                    try: os.remove(file_path)
                    except: pass
                    job['file_path'] = None
                    await update_job_status(job['job_id'], "waiting", file_path="")
                    if job['is_pilot']:
                        await update_job_status(job['job_id'], "queued")
                        await download_queue.put(job)
                    elif job['playlist_id']:
                        enqueue_playlist_job(job['playlist_id'], job)
                    else:
                        await download_queue.put(job)
            elif status == "cancelled":
                continue
            else:
                if file_path and os.path.exists(file_path):
                    try: os.remove(file_path)
                    except: pass
                job['file_path'] = None
                await update_job_status(job['job_id'], "waiting", file_path="")
                if job['is_pilot']:
                    await update_job_status(job['job_id'], "queued")
                    await download_queue.put(job)
                elif job['playlist_id']:
                    enqueue_playlist_job(job['playlist_id'], job)
                else:
                    await download_queue.put(job)
            recovered += 1
        console.print(f"[bold yellow]♻️ Recovered {recovered} jobs.[/bold yellow]")

    active_playlists = await db_execute(
        "SELECT DISTINCT album_id FROM playlists WHERE status NOT IN ('KILLED','COMPLETED')", fetch="all"
    )
    for (album_id,) in (active_playlists or []):
        if album_id:
            await refresh_vk_cache(album_id)

    asyncio.create_task(dashboard_updater())
    asyncio.create_task(scheduler_loop())
    for i in range(DL_WORKERS): asyncio.create_task(download_worker(i))
    for i in range(UP_WORKERS): asyncio.create_task(upload_worker(i))

    with Live(progress_ui, console=console, refresh_per_second=4):
        console.print("[bold green]🚀 Master UI Online. Send /start in bot![/bold green]")
        await asyncio.Event().wait()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
