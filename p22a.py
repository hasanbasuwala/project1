import os
import re
import io
import time
import math
import sqlite3
import asyncio
import logging
import requests
import concurrent.futures
import vk_api
from collections import deque
from requests_toolbelt.multipart.encoder import MultipartEncoder
from pyrogram import Client, filters, enums
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BotCommand

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
STREAM_WORKERS = 5
DB_PATH = "SysCache/vk_queue.db"

SCHEDULER_INFLIGHT_TARGET = STREAM_WORKERS * 2
SCHEDULER_TICK = 0.5

NETWORK_ERROR_WINDOW = 90        
NETWORK_ERROR_THRESHOLD = 6      
NETWORK_COOLDOWN_SECONDS = 45    
RECONNECT_MIN_INTERVAL = 60      

MAX_VK_REUPLOAD_RETRIES = 2      
VK_VERIFY_MAX_WAIT_SECONDS = 120 
VK_VERIFY_POLL_INTERVAL = 15     

FLOOD_WAIT_WINDOW = 120          
FLOOD_WAIT_THRESHOLD = 4         
FLOOD_THROTTLE_SECONDS = 120     

TRANSFER_STAGGER_SECONDS = 1.5
TRANSFER_MAX_RETRIES = 5

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

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
user_app = Client("p_session", api_id=config.API_ID, api_hash=config.API_HASH)

# ============================================================
# TELEGRAM FLOOD-WAIT WATCHER 
# ============================================================
flood_wait_events = deque()
reduced_parallelism_until = 0.0
_flood_wait_re = re.compile(r"Waiting for (\d+) seconds.*upload\.GetFile", re.IGNORECASE)

def _handle_flood_wait_hit(wait_seconds):
    global reduced_parallelism_until
    now = time.time()
    flood_wait_events.append(now)
    while flood_wait_events and now - flood_wait_events[0] > FLOOD_WAIT_WINDOW:
        flood_wait_events.popleft()
    if len(flood_wait_events) >= FLOOD_WAIT_THRESHOLD and now >= reduced_parallelism_until:
        reduced_parallelism_until = now + FLOOD_THROTTLE_SECONDS
        console.print(
            f"[bold red]🚦 Telegram flood-limiting detected. Forcing stream pacing for {FLOOD_THROTTLE_SECONDS}s.[/bold red]"
        )
        flood_wait_events.clear()

class _PyrogramFloodWatcher(logging.Handler):
    def emit(self, record):
        try: msg = record.getMessage()
        except: return
        match = _flood_wait_re.search(msg)
        if match:
            try: _handle_flood_wait_hit(int(match.group(1)))
            except: pass

logging.getLogger("pyrogram").addHandler(_PyrogramFloodWatcher())

_generic_socket_error_events = deque()
GENERIC_SOCKET_ERROR_WINDOW = 30
GENERIC_SOCKET_ERROR_THRESHOLD = 8
_generic_socket_error_re = re.compile(r"socket\.send\(\) raised exception|broken pipe|connection reset", re.IGNORECASE)

def _handle_generic_socket_error():
    now = time.time()
    _generic_socket_error_events.append(now)
    while _generic_socket_error_events and now - _generic_socket_error_events[0] > GENERIC_SOCKET_ERROR_WINDOW:
        _generic_socket_error_events.popleft()
    if len(_generic_socket_error_events) >= GENERIC_SOCKET_ERROR_THRESHOLD:
        _generic_socket_error_events.clear()
        console.print("[bold red]🚨 Low-level socket failures detected — reconnecting sessions.[/bold red]")
        asyncio.create_task(reconnect_client(user_app, "user"))
        asyncio.create_task(reconnect_client(bot_app, "bot"))

class _PyrogramGenericSocketErrorWatcher(logging.Handler):
    def emit(self, record):
        try: msg = record.getMessage()
        except: return
        if _generic_socket_error_re.search(msg):
            try: _handle_generic_socket_error()
            except: pass

logging.getLogger("pyrogram").addHandler(_PyrogramGenericSocketErrorWatcher())

# ============================================================
# NETWORK HEALTH / CIRCUIT BREAKER STATE
# ============================================================
network_error_times = deque()
network_cooldown_until = 0.0

last_reconnect_time = {"user": 0.0, "bot": 0.0}
reconnect_locks = {"user": asyncio.Lock(), "bot": asyncio.Lock()}

CONNECTION_HEALTHCHECK_INTERVAL = 60      
CONNECTION_HEALTHCHECK_TIMEOUT = 15       
CONNECTION_HEALTHCHECK_FAIL_THRESHOLD = 2 
_health_fail_counts = {"user": 0, "bot": 0}

def _is_network_error(exc):
    if isinstance(exc, (ConnectionError, OSError, TimeoutError)): return True
    msg = str(exc)
    return any(s in msg for s in ("Broken pipe", "socket.send", "Connection reset"))

async def record_network_error(exc):
    global network_cooldown_until
    now = time.time()
    network_error_times.append(now)
    while network_error_times and now - network_error_times[0] > NETWORK_ERROR_WINDOW:
        network_error_times.popleft()

    if len(network_error_times) >= NETWORK_ERROR_THRESHOLD and now >= network_cooldown_until:
        network_cooldown_until = now + NETWORK_COOLDOWN_SECONDS
        console.print(f"[bold red]🚨 Network instability detected. Pausing streams for {NETWORK_COOLDOWN_SECONDS}s...[/bold red]")
        network_error_times.clear()
        asyncio.create_task(reconnect_client(user_app, "user"))

async def wait_out_network_cooldown():
    while time.time() < network_cooldown_until:
        await asyncio.sleep(1)

async def reconnect_client(client, label):
    async with reconnect_locks[label]:
        now = time.time()
        if now - last_reconnect_time[label] < RECONNECT_MIN_INTERVAL: return
        last_reconnect_time[label] = now
        try:
            console.print(f"[bold yellow]🔄 Restarting {label} session...[/bold yellow]")
            try: await client.stop()
            except: pass
            await asyncio.sleep(3)
            await client.start()
            _health_fail_counts[label] = 0
            console.print(f"[bold green]✅ {label} session reconnected.[/bold green]")
        except Exception as e:
            console.print(f"[bold red]❌ Failed to reconnect {label}: {e}[/bold red]")

async def connection_watchdog():
    while True:
        await asyncio.sleep(CONNECTION_HEALTHCHECK_INTERVAL)
        for label, client in (("user", user_app), ("bot", bot_app)):
            try:
                await asyncio.wait_for(client.get_me(), timeout=CONNECTION_HEALTHCHECK_TIMEOUT)
                _health_fail_counts[label] = 0
            except Exception as e:
                _health_fail_counts[label] += 1
                if _health_fail_counts[label] >= CONNECTION_HEALTHCHECK_FAIL_THRESHOLD:
                    _health_fail_counts[label] = 0
                    asyncio.create_task(reconnect_client(client, label))

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
            updated_at REAL,
            tier INTEGER DEFAULT 1
        )
    """)
    for stmt in (
        "ALTER TABLE jobs ADD COLUMN caption TEXT",
        "ALTER TABLE jobs ADD COLUMN playlist_id TEXT",
        "ALTER TABLE jobs ADD COLUMN is_pilot INTEGER DEFAULT 0",
        "ALTER TABLE jobs ADD COLUMN tier INTEGER DEFAULT 1",
    ):
        try: conn.execute(stmt)
        except sqlite3.OperationalError: pass

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
    conn.execute("CREATE TABLE IF NOT EXISTS monitored_chats (chat_identifier TEXT PRIMARY KEY, resolved_id INTEGER, added_at REAL)")
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
    conn.execute("CREATE TABLE IF NOT EXISTS monitored_tags_meta (tag TEXT PRIMARY KEY, last_seen_count INTEGER DEFAULT 0, last_checked_at REAL)")
    conn.execute("CREATE TABLE IF NOT EXISTS always_monitors (chat_id INTEGER PRIMARY KEY, chat_title TEXT, status TEXT, last_msg_id INTEGER DEFAULT 0, added_at REAL)")
    conn.execute("CREATE TABLE IF NOT EXISTS selected_monitors (chat_id INTEGER PRIMARY KEY, chat_title TEXT, status TEXT, last_msg_id INTEGER DEFAULT 0, added_at REAL)")
    conn.execute("CREATE TABLE IF NOT EXISTS selected_tags (tag TEXT PRIMARY KEY, added_at REAL)")
    conn.execute("CREATE TABLE IF NOT EXISTS control (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute("CREATE TABLE IF NOT EXISTS tg_routing_destinations (tag TEXT PRIMARY KEY, topic_id INTEGER, topic_title TEXT, created_at REAL)")
    conn.execute("CREATE TABLE IF NOT EXISTS tg_copied_messages (chat_id INTEGER, msg_id INTEGER, tag TEXT, dest_topic_id INTEGER, copied_at REAL, PRIMARY KEY (chat_id, msg_id, dest_topic_id))")
    conn.execute("CREATE TABLE IF NOT EXISTS autotransfer_monitors (chat_id INTEGER PRIMARY KEY, chat_title TEXT, status TEXT, delete_originals INTEGER DEFAULT 0, owner_user_id INTEGER, last_msg_id INTEGER DEFAULT 0, added_at REAL)")
    
    for stmt in (
        "ALTER TABLE autotransfer_monitors ADD COLUMN mode TEXT DEFAULT 'ALL'",
        "ALTER TABLE autotransfer_monitors ADD COLUMN tags TEXT DEFAULT ''",
    ):
        try: conn.execute(stmt)
        except sqlite3.OperationalError: pass

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
        finally: conn.close()
    return await asyncio.to_thread(run)

async def save_job(job):
    await db_execute(
        """INSERT INTO jobs (job_id, playlist_id, chat_id, msg_chat_id, msg_id, album_id, album_name, query, idx, is_pilot, status, file_path, caption, updated_at, tier)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(job_id) DO UPDATE SET status=excluded.status, file_path=excluded.file_path,
               is_pilot=excluded.is_pilot, updated_at=excluded.updated_at, tier=excluded.tier""",
        (job['job_id'], job.get('playlist_id'), job['chat_id'], job['msg_chat_id'], job['msg_id'],
         job['album_id'], job['album_name'], job['query'], job['idx'], int(job.get('is_pilot', False)),
         job['status'], job.get('file_path'), job.get('caption', ''), time.time(), job.get('tier', 1))
    )

async def update_job_status(job_id, status):
    await db_execute("UPDATE jobs SET status=?, updated_at=? WHERE job_id=?", (status, time.time(), job_id))

async def delete_job_row(job_id):
    await db_execute("DELETE FROM jobs WHERE job_id=?", (job_id,))

async def set_control(key, value):
    await db_execute("INSERT INTO control (key, value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))

async def get_control(key, default=None):
    row = await db_execute("SELECT value FROM control WHERE key=?", (key,), fetch="one")
    return row[0] if row else default

async def is_msg_in_db(msg_chat_id, msg_id):
    row = await db_execute(
        "SELECT status FROM jobs WHERE (msg_id=? OR job_id=?) AND status IN ('done', 'streaming', 'queued')", 
        (msg_id, f"{msg_chat_id}_{msg_id}"), fetch="one"
    )
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
    return await db_execute("SELECT playlist_id, chat_id, query, album_name, album_id, status, total, completed, failed, skipped_dupes FROM playlists WHERE playlist_id=?", (playlist_id,), fetch="one")

async def list_playlists(limit=15):
    return await db_execute("SELECT playlist_id, album_name, status, total, completed, failed, skipped_dupes FROM playlists WHERE status != 'KILLED' ORDER BY updated_at DESC LIMIT ?", (limit,), fetch="all")

async def sync_vk_to_local_db():
    console.print("[bold cyan]🔄 Syncing state from VK and cleaning duplicate/broken uploads...[/bold cyan]")
    synced_count, deleted_duplicates_count, deleted_broken_count = 0, 0, 0
    seen_msg_ids = {} 

    try:
        albums_resp = await asyncio.to_thread(vk.video.getAlbums, owner_id=my_vk_id, count=100)
        album_ids = [alb['id'] for alb in albums_resp.get('items', []) if alb.get('id')]
        if None not in album_ids and 0 not in album_ids: album_ids.append(None)

        for album_id in album_ids:
            offset, count = 0, 200
            while True:
                try:
                    kwargs = {'owner_id': my_vk_id, 'count': count, 'offset': offset}
                    if album_id is not None and album_id > 0: kwargs['album_id'] = album_id
                    items = await asyncio.to_thread(vk.video.get, **kwargs)
                except Exception: break

                video_list = items.get('items', [])
                if not video_list: break

                for v in video_list:
                    title, video_id, duration = v.get('title', ''), v.get('id'), v.get('duration', 0)
                    is_processing = v.get('processing', 0)

                    if duration == 0 and not is_processing:
                        try:
                            await asyncio.to_thread(vk.video.delete, owner_id=my_vk_id, video_id=video_id)
                            deleted_broken_count += 1
                        except: pass
                        continue

                    match = re.search(r'\[TG_(\d+)\]', title)
                    if match:
                        msg_id = int(match.group(1))
                        if msg_id in seen_msg_ids:
                            if seen_msg_ids[msg_id] != video_id:
                                try:
                                    await asyncio.to_thread(vk.video.delete, owner_id=my_vk_id, video_id=video_id)
                                    deleted_duplicates_count += 1
                                except: pass
                            continue
                        
                        seen_msg_ids[msg_id] = video_id
                        job_id = f"vk_recovered_{msg_id}"
                        await db_execute(
                            """INSERT INTO jobs (job_id, chat_id, msg_chat_id, msg_id, album_id, album_name, query, idx, status, updated_at, tier)
                               VALUES (?,0,0,?,?,?,'vk_sync',1,'done',?,1) ON CONFLICT(job_id) DO UPDATE SET status='done'""",
                            (job_id, msg_id, album_id if album_id else 0, title.split(' - ')[0], time.time())
                        )
                        synced_count += 1

                if len(video_list) < count: break
                offset += count

        if seen_msg_ids:
            placeholders = ','.join('?' for _ in seen_msg_ids.keys())
            await db_execute(f"UPDATE monitored_messages SET is_queued=1 WHERE msg_id IN ({placeholders})", tuple(seen_msg_ids.keys()))

        console.print(f"[bold green]✅ VK Sync Complete: {synced_count} indexed, {deleted_duplicates_count} dupes purged, {deleted_broken_count} broken videos purged.[/bold green]")
        return synced_count, deleted_duplicates_count, deleted_broken_count
    except Exception as e:
        return 0, 0, 0

# ============================================================
# GLOBAL STATE & UI CONTROL
# ============================================================
stream_queue_t1 = asyncio.Queue()
stream_queue_t2 = asyncio.Queue()

active_jobs = {}
active_bulk_tasks = {}
cancelled_jobs = set()
user_states = {}
ui_state = "MAIN"
monitor_page = 0
vk_reupload_attempts = {}

playlist_queues = {}
playlist_order = deque()
vk_video_title_cache = {}
vk_album_name_cache = {}

ENGINE_RUNNING = "RUNNING"
ENGINE_PAUSE_REQUESTED = "PAUSE_REQUESTED"
ENGINE_PAUSED = "PAUSED"
engine_state = ENGINE_RUNNING

pause_event = asyncio.Event()
pause_event.set()

def vk_title_for(album_name, idx, msg_id=None):
    if msg_id: return f"{album_name} - P{idx} [TG_{msg_id}]"
    return f"{album_name} - P{idx}"

def display_title(album_name, idx, caption, msg_id=None):
    caption = (caption or "").strip()
    if caption:
        base = caption.split('\n')[0].strip()
        if msg_id and f"[TG_{msg_id}]" not in base: return f"{base} [TG_{msg_id}]"
        return base
    return vk_title_for(album_name, idx, msg_id)

def extract_all_tags(text):
    if not text: return []
    return [t.lower() for t in re.findall(r"#([A-Za-z0-9_]+)", text)]

def extract_first_tag(text):
    tags = extract_all_tags(text)
    return tags[0] if tags else None

async def refresh_vk_cache(album_id):
    if not album_id or album_id <= 0: return set()
    for attempt in range(3):
        try:
            items = await asyncio.to_thread(vk.video.get, owner_id=my_vk_id, album_id=album_id, count=200)
            titles = {v.get('title', '') for v in items.get('items', [])}
            vk_video_title_cache[album_id] = titles
            return titles
        except Exception:
            await asyncio.sleep(2 * (attempt + 1))
    return set()

async def vk_title_exists(album_id, title):
    if not album_id or album_id <= 0: return False
    if album_id not in vk_video_title_cache: await refresh_vk_cache(album_id)
    return title in vk_video_title_cache.get(album_id, set())

async def get_or_create_vk_album(album_name):
    if not album_name: return None
    norm_name = album_name.strip().lower()
    if norm_name in vk_album_name_cache: return vk_album_name_cache[norm_name]

    for attempt in range(3):
        try:
            existing = await asyncio.to_thread(vk.video.getAlbums, owner_id=my_vk_id, count=100)
            for alb in existing.get('items', []):
                title = alb.get('title', '')
                alb_id = alb.get('id')
                if alb_id:
                    vk_album_name_cache[title.strip().lower()] = alb_id
                    if title.strip().lower() == norm_name: return alb_id

            new_alb = await asyncio.to_thread(vk.video.addAlbum, title=album_name)
            alb_id = new_alb if isinstance(new_alb, int) else new_alb.get('album_id')
            if alb_id:
                vk_album_name_cache[norm_name] = alb_id
                return alb_id
        except Exception:
            await asyncio.sleep(2 * (attempt + 1))
    return None

async def verify_vk_video_ready(owner_id, video_id):
    waited = 0
    while waited <= VK_VERIFY_MAX_WAIT_SECONDS:
        await asyncio.sleep(VK_VERIFY_POLL_INTERVAL)
        waited += VK_VERIFY_POLL_INTERVAL
        try:
            resp = await asyncio.to_thread(vk.video.get, owner_id=owner_id, videos=f"{owner_id}_{video_id}")
            items = resp.get('items', [])
            if not items: continue
            v = items[0]
            duration, processing = v.get('duration', 0), v.get('processing', 0)
            if duration and duration > 0: return True
            if not processing and duration == 0 and waited >= VK_VERIFY_POLL_INTERVAL * 2: return False
        except Exception: pass
    return False

def enqueue_playlist_job(playlist_id, job):
    playlist_queues.setdefault(playlist_id, deque()).append(job)
    if playlist_id not in playlist_order: playlist_order.append(playlist_id)

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
    try: progress_ui.update(rich_task_id, completed=percent)
    except KeyError: pass

# ============================================================
# MASTER CAPTION BUNDLE PARSER LOGIC
# ============================================================
def parse_master_caption_bundle(album_msgs, target_tags):
    target_tags_lower = [t.lower().replace("#", "") for t in target_tags]
    matched_results = []
    
    master_caption = ""
    for am in album_msgs:
        if am.caption:
            master_caption = am.caption
            break

    top_matched_tags = []
    if master_caption:
        top_matched_tags = [t for t in extract_all_tags(master_caption) if t in target_tags_lower]

    track_data = {}
    if master_caption:
        lines = [line.strip() for line in master_caption.strip().split('\n') if line.strip()]
        for line in lines[1:]: 
            match = re.match(r'^(\d+)[\s\-\.\)]+(.*)', line)
            if match:
                idx_str, rest_of_line = match.groups()
                line_matched_tags = [t for t in extract_all_tags(line) if t in target_tags_lower]
                effective_tags = line_matched_tags if line_matched_tags else top_matched_tags
                if effective_tags:
                    bracket_match = re.search(r"\(([^)]*)\)", rest_of_line)
                    track_caption = bracket_match.group(1).strip() if bracket_match else rest_of_line.strip()
                    track_data[int(idx_str)] = (effective_tags, track_caption)

    for i, am in enumerate(album_msgs, start=1):
        if not am.video and not am.document: continue
        
        if i in track_data:
            tags, cap = track_data[i]
            am._custom_album = tags[0]
            am._custom_albums = tags
            am._custom_caption = cap
            am._relative_idx = i
            am._decouple = True 
            matched_results.append(am)
        elif top_matched_tags:
            am._custom_album = top_matched_tags[0]
            am._custom_albums = top_matched_tags
            am._custom_caption = master_caption if am.caption else ""
            am._relative_idx = i
            am._decouple = False 
            matched_results.append(am)
        else:
            txt = am.caption or am.text or ""
            matched_tags = [t for t in extract_all_tags(txt) if t in target_tags_lower]
            if matched_tags:
                am._custom_album = matched_tags[0] 
                am._custom_albums = matched_tags   
                am._custom_caption = txt
                am._relative_idx = i
                am._decouple = False
                matched_results.append(am)

    return matched_results

# ============================================================
# TELEGRAM TRANSFER ENGINE & SCANNERS
# ============================================================
async def tg_flood_safe(coro_fn, *args, **kwargs):
    attempts = 0
    while True:
        try: return await coro_fn(*args, **kwargs)
        except FloodWait as e:
            attempts += 1
            wait_s = int(getattr(e, "value", 5)) + 2
            await asyncio.sleep(wait_s)
            if attempts >= TRANSFER_MAX_RETRIES: raise

async def tg_get_routing_config(tag):
    return await db_execute("SELECT topic_id, topic_title FROM tg_routing_destinations WHERE tag=?", (tag,), fetch="one")

async def tg_set_routing_config(tag, topic_id, topic_title):
    await db_execute(
        "INSERT INTO tg_routing_destinations (tag, topic_id, topic_title, created_at) VALUES (?,?,?,?) ON CONFLICT(tag) DO UPDATE SET topic_id=excluded.topic_id, topic_title=excluded.topic_title",
        (tag, topic_id, topic_title, time.time())
    )

async def tg_is_message_copied(chat_id, msg_id, dest_topic_id):
    row = await db_execute("SELECT 1 FROM tg_copied_messages WHERE chat_id=? AND msg_id=? AND dest_topic_id=?", (chat_id, msg_id, dest_topic_id), fetch="one")
    return bool(row)

async def tg_mark_message_copied(chat_id, msg_id, tag, dest_topic_id):
    await db_execute("INSERT OR IGNORE INTO tg_copied_messages (chat_id, msg_id, tag, dest_topic_id, copied_at) VALUES (?,?,?,?,?)", (chat_id, msg_id, tag, dest_topic_id, time.time()))

from pyrogram.raw.functions.channels import CreateForumTopic

async def tg_create_destination_topic(tag):
    master_forum_id = await get_control("master_forum_id")
    if not master_forum_id: raise Exception("Master Forum ID is not set.")
    try: chat_identifier = int(master_forum_id)
    except ValueError: chat_identifier = master_forum_id
    title = tag.replace("#", "").strip() or "transfer"
    
    peer = await user_app.resolve_peer(chat_identifier)
    real_chat = await user_app.get_chat(chat_identifier)
    real_chat_id = real_chat.id

    raw_response = await tg_flood_safe(user_app.invoke, CreateForumTopic(channel=peer, title=title, random_id=int(time.time() * 1000)))
    
    topic_id = None
    for update in getattr(raw_response, "updates", []):
        if hasattr(update, "message") and hasattr(update.message, "id"):
            topic_id = update.message.id
            break
            
    if not topic_id: raise Exception("Topic created but ID not parsed.")

    await asyncio.sleep(TRANSFER_STAGGER_SECONDS)
    await tg_set_routing_config(tag, topic_id, title)
    return real_chat_id, topic_id, title

async def tg_resolve_destination_topic(tag):
    master_forum_id = await get_control("master_forum_id")
    if not master_forum_id: raise Exception("Master Forum ID is not set.")
    dest = await tg_get_routing_config(tag)
    if dest and dest[0]:  
        try: real_chat_id = int(master_forum_id)
        except ValueError:
            chat = await user_app.get_chat(master_forum_id)
            real_chat_id = chat.id
            await set_control("master_forum_id", str(real_chat_id)) 
        return real_chat_id, dest[0], dest[1]
    return await tg_create_destination_topic(tag)

async def build_topic_dedupe_cache(master_forum_id, topic_id):
    cache = set()
    try:
        async for msg in user_app.search_messages(master_forum_id, query=""):
            if getattr(msg, "message_thread_id", None) == topic_id or getattr(msg, "reply_to_message_id", None) == topic_id:
                f_id = msg.video.file_unique_id if msg.video else (msg.document.file_unique_id if msg.document else None)
                if f_id: cache.add(f_id)
    except Exception: pass
    return cache

async def tg_execute_message_copy(src_chat_id, m, master_forum_id, topic_id, tag, delete_original, topic_cache=None):
    if await tg_is_message_copied(src_chat_id, m.id, topic_id): return False
    f_id = m.video.file_unique_id if m.video else (m.document.file_unique_id if m.document else None)
    if topic_cache is not None and f_id in topic_cache:
        await tg_mark_message_copied(src_chat_id, m.id, tag, topic_id)
        return False

    decouple = getattr(m, "_decouple", False)
    custom_caption = getattr(m, "_custom_caption", None)

    if m.media_group_id and not decouple:
        try: group_msgs = await user_app.get_media_group(src_chat_id, m.id)
        except Exception: group_msgs = [m]
        
        unseen_msgs = []
        for gm in group_msgs:
            gf_id = gm.video.file_unique_id if gm.video else (gm.document.file_unique_id if gm.document else None)
            if topic_cache is not None and gf_id in topic_cache:
                await tg_mark_message_copied(src_chat_id, gm.id, tag, topic_id)
            else: unseen_msgs.append(gm)

        if not unseen_msgs: return False
        ids = [gm.id for gm in group_msgs]
        
        await tg_flood_safe(user_app.copy_media_group, master_forum_id, src_chat_id, m.id, reply_to_message_id=topic_id)
        for gm in group_msgs:
            await tg_mark_message_copied(src_chat_id, gm.id, tag, topic_id)
            gf_id = gm.video.file_unique_id if gm.video else (gm.document.file_unique_id if gm.document else None)
            if topic_cache is not None and gf_id: topic_cache.add(gf_id)
            
        if delete_original:
            try: await user_app.delete_messages(src_chat_id, ids)
            except Exception: pass
    else:
        kwargs = {}
        if custom_caption: kwargs['caption'] = custom_caption
        await tg_flood_safe(user_app.copy_message, master_forum_id, src_chat_id, m.id, reply_to_message_id=topic_id, **kwargs)
        await tg_mark_message_copied(src_chat_id, m.id, tag, topic_id)
        if topic_cache is not None and f_id: topic_cache.add(f_id)
        if delete_original:
            try: await user_app.delete_messages(src_chat_id, m.id)
            except Exception: pass

    await asyncio.sleep(TRANSFER_STAGGER_SECONDS)
    return True

async def run_custom_transfer(user_chat_id, state, mode, custom_tags):
    target_chat_id = state['target_chat_id']
    target_title = state['target_title']
    command_name = state['cmd_type']
    delete_originals = state.get('delete_originals', False) if command_name == "autotransfer" else (command_name == "transfer")
    target_tags_lower = [t.lower().replace("#", "") for t in custom_tags] if custom_tags else []

    status_msg = await bot_app.send_message(user_chat_id, f"🚀 Initializing Data Stream for **{target_title}**...", parse_mode=ParseMode.MARKDOWN)

    task_id = f"bulk_{int(time.time())}_{target_chat_id}"
    task_state = {
        "msg_obj": status_msg, "target_title": target_title, "cmd_name": command_name,
        "phase": "STREAMING", "scanned_count": 0, "transferred_count": 0,
        "tags_progress": {}, "start_time": time.time(), "scan_complete": False
    }
    active_bulk_tasks[task_id] = task_state
    processed_groups = set()
    topic_memory = {} 

    async def _process_and_transfer_message(m, tag):
        if tag not in topic_memory:
            try:
                mf_id, t_id, _ = await tg_resolve_destination_topic(tag)
                t_cache = await build_topic_dedupe_cache(mf_id, t_id)
                topic_memory[tag] = (mf_id, t_id, t_cache)
                task_state['tags_progress'][tag] = 0 
            except Exception: return
        mf_id, t_id, t_cache = topic_memory[tag]
        f_id = m.video.file_unique_id if m.video else (m.document.file_unique_id if m.document else None)
        if f_id and f_id in t_cache:
            task_state['tags_progress'][tag] += 1
            task_state['transferred_count'] += 1
            return
        try:
            if await tg_execute_message_copy(target_chat_id, m, mf_id, t_id, tag, delete_originals, t_cache):
                task_state['tags_progress'][tag] += 1
                task_state['transferred_count'] += 1
        except Exception: pass

    if mode == "TAGS" and target_tags_lower:
        for tag_query in target_tags_lower:
            query = f"#{tag_query}"
            async for msg in user_app.search_messages(chat_id=target_chat_id, query=query):
                task_state['scanned_count'] += 1
                if not (msg.video or msg.document): continue
                album_msgs = [msg]
                if msg.media_group_id:
                    if msg.media_group_id in processed_groups: continue
                    processed_groups.add(msg.media_group_id)
                    try: album_msgs = sorted(await user_app.get_media_group(target_chat_id, msg.id), key=lambda x: x.id)
                    except Exception: pass
                matched = parse_master_caption_bundle(album_msgs, [query])
                for m in matched:
                    for tag_clean in getattr(m, '_custom_albums', []):
                        await _process_and_transfer_message(m, f"#{tag_clean.lower()}")

    elif mode == "ALL":
        async for msg in user_app.get_chat_history(target_chat_id):
            task_state['scanned_count'] += 1
            if not (msg.video or msg.document): continue
            album_msgs = [msg]
            if msg.media_group_id:
                if msg.media_group_id in processed_groups: continue
                processed_groups.add(msg.media_group_id)
                try: album_msgs = sorted(await user_app.get_media_group(target_chat_id, msg.id), key=lambda x: x.id)
                except Exception: pass
            found_tags_set = set()
            for am in album_msgs:
                extracted = extract_all_tags(am.caption or am.text or "")
                if extracted: found_tags_set.update(extracted)
            found_tags = list(found_tags_set) or ["".join(e for e in target_title if e.isalnum()).lower() or "general"]
            for am in album_msgs:
                for tag in found_tags: await _process_and_transfer_message(am, tag)

    task_state['scan_complete'] = True
    task_state['phase'] = "DONE"

async def bulk_progress_updater():
    while True:
        await asyncio.sleep(4.0)
        for task_id, state in list(active_bulk_tasks.items()):
            msg_obj, phase, cmd, title = state['msg_obj'], state['phase'], state['cmd_name'].title(), state['target_title']
            try:
                if phase == "STREAMING":
                    done, scanned = state['transferred_count'], state['scanned_count']
                    elapsed = time.time() - state['start_time']
                    speed = done / elapsed if elapsed > 0 else 0
                    spinner = ["🌑", "🌒", "🌓", "🌔", "🌕", "🌖", "🌗", "🌘"][int(time.time() * 2) % 8]
                    scan_status = "✅ Finished Scanning!" if state['scan_complete'] else f"{spinner} Scanning active..."
                    text = (f"🚀 **{cmd} Streaming: {title}**\n━━━━━━━━━━━━━━━━━━\n"
                            f"📊 **Live Metrics:**\n🔎 Messages Scanned: **{scanned}**\n📥 Videos Transferred: **{done}**\n"
                            f"📡 Status: {scan_status}\n\n🏷️ **Tags Discovered & Copied:**\n")
                    tag_items = list(state['tags_progress'].items())
                    for tag, count in tag_items[:5]: text += f"• `{tag}`: {count} transferred\n"
                    if len(tag_items) > 5: text += f"_...and {len(tag_items) - 5} more tags._\n"
                    text += f"━━━━━━━━━━━━━━━━━━\n⏱️ Elapsed: {int(elapsed // 60)}m {int(elapsed % 60)}s | ⚡ {speed:.1f} vids/sec\n"
                    await msg_obj.edit_text(text, parse_mode=ParseMode.MARKDOWN)
                elif phase == "DONE":
                    text = (f"✅ **{cmd} Complete: {title}**\n━━━━━━━━━━━━━━━━━━\n"
                            f"🎉 Scan finished! Checked {state['scanned_count']} messages.\n"
                            f"📥 Successfully processed **{state['transferred_count']}** videos.\n\n🏷️ **Final Tag Breakdown:**\n")
                    for tag, count in state['tags_progress'].items(): text += f"• `{tag}`: {count} processed\n"
                    await msg_obj.edit_text(text, parse_mode=ParseMode.MARKDOWN)
                    active_bulk_tasks.pop(task_id, None)
            except Exception: pass

async def run_selective_history_scan(chat_id):
    tags_rows = await db_execute("SELECT tag FROM selected_tags", fetch="all")
    if not tags_rows: return 0, 0, []
    target_tags = [r[0] for r in tags_rows]
    valid_jobs, already_in_vk_cnt, total_found_cnt = [], 0, 0
    processed_media_groups = set()

    async for msg in user_app.get_chat_history(chat_id, limit=500):
        if not (msg.video or msg.document): continue
        album_msgs = [msg]
        if msg.media_group_id:
            if msg.media_group_id in processed_media_groups: continue
            processed_media_groups.add(msg.media_group_id)
            try: album_msgs = sorted(await user_app.get_media_group(chat_id, msg.id), key=lambda x: x.id)
            except Exception: pass

        matched_msgs = parse_master_caption_bundle(album_msgs, target_tags)
        total_found_cnt += len(matched_msgs)

        for m in matched_msgs:
            tag_clean = getattr(m, '_custom_album', None)
            if not tag_clean: continue
            album_id = await get_or_create_vk_album(tag_clean)
            if not album_id: continue
            idx = getattr(m, '_relative_idx', 1)
            cap = getattr(m, '_custom_caption', "")
            title = display_title(tag_clean, idx, cap, m.id)

            if await vk_title_exists(album_id, title) or await is_msg_in_db(chat_id, m.id): already_in_vk_cnt += 1
            else:
                valid_jobs.append({
                    'job_id': f"{chat_id}_{m.id}", 'playlist_id': None, 'chat_id': chat_id, 'msg_chat_id': chat_id, 
                    'msg_id': m.id, 'album_id': album_id, 'album_name': tag_clean, 'query': f"#{tag_clean}", 
                    'idx': idx, 'is_pilot': False, 'status': 'queued', 'caption': cap, 'tier': 2
                })
    return total_found_cnt, already_in_vk_cnt, valid_jobs

async def extract_and_store_message(chat_id, msg_id, text, file_unique_id):
    if not text: return
    tags = re.findall(r"#([A-Za-z0-9_]+)", text)
    if not tags: return
    for raw_tag in tags:
        await db_execute(
            """INSERT INTO monitored_messages (chat_id, msg_id, tag, file_unique_id, caption, discovered_at)
               VALUES (?,?,?,?,?,?) ON CONFLICT(chat_id, msg_id) DO UPDATE SET caption=excluded.caption""",
            (chat_id, msg_id, f"#{raw_tag.lower()}", file_unique_id, text, time.time())
        )

async def scan_chat_history(chat_identifier, resolved_chat_id):
    console.print(f"[bold cyan]🔍 Scanning historical messages for {chat_identifier} ({resolved_chat_id})...[/bold cyan]")
    try:
        cid_str = str(chat_identifier).strip()
        chat_obj = await user_app.get_chat(int(cid_str) if cid_str.lstrip('-').isdigit() else cid_str)
        resolved_chat_id = chat_obj.id
        await db_execute("UPDATE monitored_chats SET resolved_id=? WHERE chat_identifier=?", (resolved_chat_id, chat_identifier))
        async for message in user_app.get_chat_history(resolved_chat_id):
            if message.video or message.document:
                f_id = message.video.file_unique_id if message.video else (message.document.file_unique_id if message.document else "")
                await extract_and_store_message(resolved_chat_id, message.id, message.caption or message.text or "", f_id)
    except Exception as e:
        console.print(f"[bold red]❌ Failed scan history for {chat_identifier}: {e}[/bold red]")

@user_app.on_message(filters.video | filters.document)
async def live_monitor_handler(client, message):
    chat_id = message.chat.id
    txt = message.caption or message.text or ""
    f_id = message.video.file_unique_id if message.video else (message.document.file_unique_id if message.document else "")

    if await db_execute("SELECT chat_identifier FROM monitored_chats WHERE resolved_id=?", (chat_id,), fetch="one"):
        await extract_and_store_message(chat_id, message.id, txt, f_id)

    always_row = await db_execute("SELECT status, chat_title FROM always_monitors WHERE chat_id=?", (chat_id,), fetch="one")
    if always_row and always_row[0] == "ACTIVE":
        tag_clean = extract_first_tag(txt)
        if tag_clean:
            album_id = await get_or_create_vk_album(tag_clean)
            if album_id and not await vk_title_exists(album_id, display_title(tag_clean, 1, txt, message.id)) and not await is_msg_in_db(chat_id, message.id):
                job = {'job_id': f"{chat_id}_{message.id}", 'playlist_id': None, 'chat_id': chat_id, 'msg_chat_id': chat_id, 'msg_id': message.id, 'album_id': album_id, 'album_name': tag_clean, 'query': f"#{tag_clean}", 'idx': 1, 'is_pilot': False, 'status': 'queued', 'caption': txt, 'tier': 2}
                await save_job(job)
                await db_execute("UPDATE always_monitors SET last_msg_id=? WHERE chat_id=?", (message.id, chat_id))
                await stream_queue_t2.put(job)

    selected_row = await db_execute("SELECT status, chat_title FROM selected_monitors WHERE chat_id=?", (chat_id,), fetch="one")
    if selected_row and selected_row[0] == "ACTIVE":
        registered_tags_rows = await db_execute("SELECT tag FROM selected_tags", fetch="all")
        if target_tags := [r[0] for r in registered_tags_rows] if registered_tags_rows else []:
            album_msgs = [message]
            if message.media_group_id:
                try: album_msgs = sorted(await user_app.get_media_group(chat_id, message.id), key=lambda x: x.id)
                except Exception: pass
            for m in parse_master_caption_bundle(album_msgs, target_tags):
                tag_clean = getattr(m, '_custom_album', None)
                if not tag_clean: continue
                album_id = await get_or_create_vk_album(tag_clean)
                if not album_id: continue
                idx, cap = getattr(m, '_relative_idx', 1), getattr(m, '_custom_caption', "")
                if not await vk_title_exists(album_id, display_title(tag_clean, idx, cap, m.id)) and not await is_msg_in_db(chat_id, m.id):
                    job = {'job_id': f"{chat_id}_{m.id}", 'playlist_id': None, 'chat_id': chat_id, 'msg_chat_id': chat_id, 'msg_id': m.id, 'album_id': album_id, 'album_name': tag_clean, 'query': f"#{tag_clean}", 'idx': idx, 'is_pilot': False, 'status': 'queued', 'caption': cap, 'tier': 2}
                    await save_job(job)
                    await db_execute("UPDATE selected_monitors SET last_msg_id=? WHERE chat_id=?", (m.id, chat_id))
                    await stream_queue_t2.put(job)

    autotransfer_row = await db_execute("SELECT status, delete_originals, mode, tags FROM autotransfer_monitors WHERE chat_id=?", (chat_id,), fetch="one")
    if autotransfer_row and autotransfer_row[0] == "ACTIVE":
        del_orig, mode, tags_str = bool(autotransfer_row[1]), autotransfer_row[2] or "ALL", autotransfer_row[3] or ""
        at_target_tags = [t.strip() for t in tags_str.split(",") if t.strip()] if mode == "TAGS" else []
        album_msgs = [message]
        if message.media_group_id:
            try: album_msgs = sorted(await user_app.get_media_group(chat_id, message.id), key=lambda x: x.id)
            except Exception: pass

        if mode == "ALL":
            found_tags = extract_all_tags(message.caption or message.text or "")
            if not found_tags:
                chat_obj = await user_app.get_chat(chat_id)
                found_tags = ["".join(e for e in (chat_obj.title or str(chat_id)) if e.isalnum()).lower() or "general"]
            for tag in found_tags:
                for m in album_msgs:
                    try:
                        mf_id, t_id, _ = await tg_resolve_destination_topic(f"#{tag}")
                        await tg_execute_message_copy(chat_id, m, mf_id, t_id, f"#{tag}", del_orig)
                    except Exception: pass
        else:
            for m in parse_master_caption_bundle(album_msgs, at_target_tags):
                for tag_clean in getattr(m, '_custom_albums', []):
                    try:
                        mf_id, t_id, _ = await tg_resolve_destination_topic(f"#{tag_clean.lower()}")
                        await tg_execute_message_copy(chat_id, m, mf_id, t_id, f"#{tag_clean.lower()}", del_orig)
                    except Exception: pass
        await db_execute("UPDATE autotransfer_monitors SET last_msg_id=? WHERE chat_id=?", (message.id, chat_id))

async def add_monitored_target(target_raw):
    target_raw = target_raw.strip()
    try:
        if target_raw.startswith("-100") or target_raw.startswith("-") or target_raw.isdigit(): resolved_id = int(target_raw)
        else: resolved_id = (await user_app.get_chat(target_raw)).id
        await db_execute(
            "INSERT INTO monitored_chats (chat_identifier, resolved_id, added_at) VALUES (?,?,?) ON CONFLICT(chat_identifier) DO UPDATE SET resolved_id=excluded.resolved_id",
            (target_raw, resolved_id, time.time())
        )
        asyncio.create_task(scan_chat_history(target_raw, resolved_id))
        return True, resolved_id
    except Exception as e: return False, str(e)

# ============================================================
# ZERO-HOP IN-MEMORY STREAMING ENGINE (TOOLBELT ENCODING)
# ============================================================
class PyrogramStreamReader(io.RawIOBase):
    """
    A synchronous file-like bridge that pulls data from an async Pyrogram stream.
    Used by requests_toolbelt.MultipartEncoder to generate a strict Content-Length.
    """
    def __init__(self, client, message, loop, progress_callback, job_id):
        self.client = client
        self.message = message
        self.loop = loop
        self.progress_callback = progress_callback
        self.job_id = job_id
        
        media = message.video or message.document
        self.total_size = media.file_size
        self.len = self.total_size # Required by requests_toolbelt
        self.read_bytes = 0
        
        self.async_gen = self._get_stream_generator()
        self.buffer = bytearray()

    async def _get_stream_generator(self):
        """Forces the Pyrogram stream into a guaranteed async generator."""
        async for chunk in self.client.stream_media(self.message):
            yield chunk

    def read(self, size=-1):
        if self.job_id in cancelled_jobs:
            raise Exception("ForceAbort")

        if self.read_bytes >= self.total_size:
            return b""

        if size == -1: 
            size = 1024 * 1024 * 2 # 2MB buffer standard

        while len(self.buffer) < size and (self.read_bytes + len(self.buffer)) < self.total_size:
            try:
                future = asyncio.run_coroutine_threadsafe(self.async_gen.__anext__(), self.loop)
                chunk = future.result(timeout=60)
                self.buffer.extend(chunk)
            except StopAsyncIteration:
                break
            except Exception as e:
                raise e

        chunk_to_return = self.buffer[:size]
        self.buffer = self.buffer[size:]
        
        self.read_bytes += len(chunk_to_return)
        
        if self.progress_callback:
            self.progress_callback(self.read_bytes, self.total_size)
            
        return bytes(chunk_to_return)

    def __len__(self):
        return self.total_size


async def stream_telegram_to_vk(client, message, upload_url, progress_callback, job_id):
    """Pipes bytes directly from Telegram to VK forcing a strict Content-Length via MultipartEncoder."""
    loop = asyncio.get_running_loop()
    
    def _sync_upload():
        reader = PyrogramStreamReader(client, message, loop, progress_callback, job_id)
        
        # requests-toolbelt safely builds a streamed multipart/form-data payload with a fixed Content-Length
        m = MultipartEncoder(
            fields={'video_file': ('video.mp4', reader, 'video/mp4')}
        )
        
        resp = requests.post(
            upload_url, 
            data=m, 
            headers={'Content-Type': m.content_type}, 
            timeout=None
        )
        
        if resp.status_code != 200:
            raise Exception(f"VK upload failed with status {resp.status_code}: {resp.text[:300]}")
            
        return resp.json()

    return await asyncio.to_thread(_sync_upload)

# ============================================================
# JOB COMPLETION / PLAYLIST BOOKKEEPING
# ============================================================
async def on_job_finished(job):
    playlist_id = job.get('playlist_id')
    await delete_job_row(job['job_id'])
    if not playlist_id: return

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

async def on_job_permanently_failed(job, reason):
    playlist_id = job.get('playlist_id')
    display_name = f"{job.get('query')} (Pt.{job.get('idx')})"
    vk_reupload_attempts.pop(job['job_id'], None)
    await delete_job_row(job['job_id'])

    if playlist_id:
        await bump_playlist(playlist_id, failed_delta=1)
        row = await get_playlist(playlist_id)
        if row:
            _, _, _, _, _, status, total, completed, failed, skipped = row
            if status not in ("KILLED", "COMPLETED") and (completed + failed + skipped) >= total:
                await set_playlist_status(playlist_id, "COMPLETED")

    dashboard_chat_id = await get_control("dashboard_chat_id")
    if dashboard_chat_id:
        try:
            await bot_app.send_message(
                chat_id=int(dashboard_chat_id),
                text=f"⚠️ **Upload Failed Permanently**\n\nVideo: **{display_name}**\nChat: `{job.get('msg_chat_id')}` | Msg ID: `{job.get('msg_id')}`\nReason: {reason}",
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception: pass

async def continue_playlist(playlist_id):
    rows = await db_execute(
        "SELECT job_id, chat_id, msg_chat_id, msg_id, album_id, album_name, query, idx, caption, tier FROM jobs WHERE playlist_id=? AND status='waiting'",
        (playlist_id,), fetch="all"
    )
    for r in rows:
        job = {
            'job_id': r[0], 'playlist_id': playlist_id, 'chat_id': r[1], 'msg_chat_id': r[2],
            'msg_id': r[3], 'album_id': r[4], 'album_name': r[5], 'query': r[6], 'idx': r[7],
            'caption': r[8], 'file_path': None, 'is_pilot': False, 'tier': r[9] if len(r) > 9 else 1
        }
        await update_job_status(job['job_id'], "queued")
        enqueue_playlist_job(playlist_id, job)
    await set_playlist_status(playlist_id, "RUNNING")

async def kill_playlist(playlist_id):
    await set_playlist_status(playlist_id, "KILLED")
    rows = await db_execute("SELECT job_id FROM jobs WHERE playlist_id=? AND status NOT IN ('done')", (playlist_id,), fetch="all")
    for (job_id,) in rows: cancelled_jobs.add(job_id)
    await db_execute("DELETE FROM jobs WHERE playlist_id=? AND status='waiting'", (playlist_id,))
    playlist_queues.pop(playlist_id, None)
    try: playlist_order.remove(playlist_id)
    except ValueError: pass

# ============================================================
# 2-TIER SCHEDULER
# ============================================================
async def scheduler_loop():
    while True:
        await asyncio.sleep(SCHEDULER_TICK)
        if engine_state != ENGINE_RUNNING: continue
        
        pending_t1 = stream_queue_t1.qsize()
        pending_t2 = stream_queue_t2.qsize()
        if (pending_t1 + pending_t2) >= SCHEDULER_INFLIGHT_TARGET: continue

        attempts = len(playlist_order)
        pushed_t1 = False
        for _ in range(attempts):
            if not playlist_order: break
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

            if job['job_id'] in cancelled_jobs: continue
            await update_job_status(job['job_id'], "queued")
            await stream_queue_t1.put(job)
            pushed_t1 = True
            break

        if pushed_t1: continue

# ============================================================
# ZERO-HOP STREAM WORKER
# ============================================================
async def stream_worker(worker_id):
    while True:
        await pause_event.wait()
        
        try:
            job = stream_queue_t1.get_nowait()
            active_q = stream_queue_t1
        except asyncio.QueueEmpty:
            try:
                job = stream_queue_t2.get_nowait()
                active_q = stream_queue_t2
            except asyncio.QueueEmpty:
                await asyncio.sleep(1)
                continue

        rich_task = None
        job_id = job['job_id']
        stream_key = f"{job_id}_STREAM"
        display_name = f"{job['query']} (Pt.{job['idx']})"

        try:
            if job_id in cancelled_jobs: continue

            await pause_event.wait()
            await wait_out_network_cooldown()

            active_jobs[stream_key] = {"name": display_name, "action": "🔄 Stream", "progress": 0, "speed": "0 MB/s", "eta": "Calc...", "start_time": time.time(), "job_id": job_id}
            rich_task = progress_ui.add_task(f"[cyan]🔄 Stream {display_name}", total=100, name=display_name)
            await update_job_status(job_id, "streaming")

            def stream_progress(current, total):
                update_metrics(stream_key, rich_task, "🔄 Stream", current, total)

            msg = None
            for attempt in range(5):
                try:
                    msg = await user_app.get_messages(job['msg_chat_id'], job['msg_id'])
                    break
                except Exception as e:
                    if _is_network_error(e): await record_network_error(e)
                    await asyncio.sleep(3)
                    await wait_out_network_cooldown()

            if not msg: raise Exception("FailedToFetchMessage")

            album_id = job.get('album_id')
            if not album_id or album_id <= 0:
                if job.get('album_name'):
                    album_id = await get_or_create_vk_album(job['album_name'])
                    job['album_id'] = album_id
            if not album_id or album_id <= 0:
                raise Exception(f"Invalid VK Album ID for job {job_id}.")

            title = display_title(job['album_name'], job['idx'], job.get('caption', ''), job['msg_id'])
            upload_succeeded = False

            for save_attempt in range(MAX_VK_REUPLOAD_RETRIES + 1):
                upload_info = None
                for attempt in range(5):
                    try:
                        upload_info = await asyncio.to_thread(vk.video.save, name=title, description=job.get('caption', ''), album_id=album_id)
                        break
                    except Exception as e:
                        await asyncio.sleep(4)

                if not upload_info: raise Exception("VKVideoSaveFailed")

                # ZERO HOP STREAM! (Thread-safe synchronous toolbelt bridge)
                await stream_telegram_to_vk(user_app, msg, upload_info['upload_url'], stream_progress, job_id)

                video_id = upload_info.get('video_id')
                owner_id = upload_info.get('owner_id', my_vk_id)

                if video_id is None:
                    upload_succeeded = True
                    break

                if await verify_vk_video_ready(owner_id, video_id):
                    upload_succeeded = True
                    break

                try: await asyncio.to_thread(vk.video.delete, owner_id=owner_id, video_id=video_id)
                except: pass

            if not upload_succeeded:
                vk_reupload_attempts[job_id] = vk_reupload_attempts.get(job_id, 0) + 1
                await update_job_status(job_id, "failed_vk_verify")
                await on_job_permanently_failed(job, "VK repeatedly failed to process this video.")
                continue

            vk_video_title_cache.setdefault(album_id, set()).add(title)
            await update_job_status(job_id, "done") 
            await db_execute("UPDATE monitored_messages SET is_queued=1 WHERE chat_id=? AND msg_id=?", (job['msg_chat_id'], job['msg_id']))
            await on_job_finished(job)

        except Exception as e:
            if str(e) == "ForceAbort":
                console.print(f"[bold yellow]💀 Aborted Stream: {display_name}[/bold yellow]")
            else:
                console.print(f"[bold red]Stream failed {display_name}: {e}[/bold red]")
                await update_job_status(job_id, "waiting")
                if job.get('tier', 1) == 1:
                    if job.get('playlist_id'): enqueue_playlist_job(job['playlist_id'], job)
                    else: await stream_queue_t1.put(job)
                else: await stream_queue_t2.put(job)
                await asyncio.sleep(3)
        finally:
            if rich_task is not None: progress_ui.remove_task(rich_task)
            active_jobs.pop(stream_key, None)
            active_q.task_done()

# ============================================================
# DASHBOARD RENDERING ENGINE
# ============================================================
def _engine_banner():
    if engine_state == ENGINE_RUNNING: return "⚡ RUNNING"
    if engine_state == ENGINE_PAUSE_REQUESTED: return "🟡 Pause Requested — draining active streams..."
    return "⏸️ PAUSED"

async def render_dashboard():
    chat_id = await get_control("dashboard_chat_id")
    msg_id = await get_control("dashboard_msg_id")
    if not chat_id or not msg_id: return

    global monitor_page
    scheduled_pending = sum(len(v) for v in playlist_queues.values())
    queued_stream = stream_queue_t1.qsize() + stream_queue_t2.qsize() + scheduled_pending
    active_streams = list(active_jobs.values())

    text = ""
    buttons = []

    if ui_state == "MAIN":
        m_count = (await db_execute("SELECT COUNT(*) FROM monitored_chats", fetch="one") or [0])[0]
        a_count = (await db_execute("SELECT COUNT(*) FROM always_monitors WHERE status='ACTIVE'", fetch="one") or [0])[0]
        s_count = (await db_execute("SELECT COUNT(*) FROM selected_monitors WHERE status='ACTIVE'", fetch="one") or [0])[0]
        t_count = (await db_execute("SELECT COUNT(*) FROM selected_tags", fetch="one") or [0])[0]
        at_count = (await db_execute("SELECT COUNT(*) FROM autotransfer_monitors WHERE status='ACTIVE'", fetch="one") or [0])[0]

        breaker_line = ""
        if time.time() < network_cooldown_until: breaker_line = f"🚨 **Network cooldown active** — resuming in ~{int(network_cooldown_until - time.time())}s\n"
        if time.time() < reduced_parallelism_until: breaker_line += f"🚦 **Telegram flood-limit throttle active** for ~{int(reduced_parallelism_until - time.time())}s\n"

        text = (f"📊 **GLOBAL STREAM ENGINE (Zero-Hop)**\n{_engine_banner()}\n{breaker_line}━━━━━━━━━━━━━━━━━━\n"
                f"🔄 **Active Streams:** {len(active_streams)} | **Queued:** {queued_stream}\n"
                f"👁️ **Monitored Chats:** {m_count} Active\n"
                f"📡 **Auto-Sync Groups:** {a_count} Active\n"
                f"🎯 **Selective Monitors:** {s_count} Active | {t_count} Tags\n"
                f"🚀 **Auto-Transfer Groups:** {at_count} Active\n━━━━━━━━━━━━━━━━━━\n")
        
        if active_bulk_tasks:
            text += f"🔎 **Active Autoscans ({len(active_bulk_tasks)}):**\n"
            for task_id, state in active_bulk_tasks.items():
                spinner = ["🌑", "🌒", "🌓", "🌔", "🌕", "🌖", "🌗", "🌘"][int(time.time() * 2) % 8]
                status = "✅ Done" if state['scan_complete'] else f"{spinner} Streaming..."
                text += f"• **{state['target_title']}** - {status}\n  _Scanned: {state['scanned_count']} | Copied: {state['transferred_count']}_\n"
            text += "━━━━━━━━━━━━━━━━━━\n"

        waiting_playlists = await db_execute("SELECT playlist_id, album_name, total FROM playlists WHERE status='WAITING_CONFIRMATION' ORDER BY updated_at DESC LIMIT 3", fetch="all")
        if waiting_playlists:
            text += "🧪 **Pilots awaiting confirmation:**\n"
            for pid, album_name, total in waiting_playlists:
                text += f"• {album_name} ({total - 1} more videos)\n"
                buttons.append([InlineKeyboardButton(f"▶️ Continue {album_name}", callback_data=f"plcontinue_{pid}"), InlineKeyboardButton("💀 Kill", callback_data=f"plkill_{pid}")])
            text += "━━━━━━━━━━━━━━━━━━\n"

        buttons.append([InlineKeyboardButton(f"🔄 View Active Streams ({len(active_streams)})", callback_data="ui_STREAM_VIEW")])
        buttons.append([InlineKeyboardButton("🎯 Selective Sync", callback_data="ui_SELECTED_VIEW"), InlineKeyboardButton("📡 Auto-Sync Groups", callback_data="ui_ALWAYS_VIEW")])
        buttons.append([InlineKeyboardButton("👁️ Monitor Findings", callback_data="ui_MONITOR_VIEW"), InlineKeyboardButton("📋 Playlists", callback_data="ui_PLAYLISTS")])
        buttons.append([InlineKeyboardButton("🚀 Auto-Transfer Groups", callback_data="ui_AUTOTRANSFER_VIEW")])
        buttons.append([InlineKeyboardButton("▶️ Global Resume" if engine_state != ENGINE_RUNNING else "⏸️ Global Pause", callback_data="toggle_pause"), InlineKeyboardButton("🛑 Clear Queues", callback_data="clear_queue")])

    elif ui_state == "STREAM_VIEW":
        text = f"🔄 **ACTIVE STREAMS ({len(active_streams)} Workers)**\n━━━━━━━━━━━━━━━━━━\n"
        for job in active_streams:
            filled = int(job['progress'] / 10)
            bar = "🟩" * filled + "⬜" * (10 - filled)
            text += f"▶️ **{job['name']}**\n↳ {bar} {job['progress']:.1f}% ({job['speed']})\n\n"
            buttons.append([InlineKeyboardButton(f"💀 Kill {job['name']}", callback_data=f"kill_{job['job_id']}")])

        if not active_streams: text += "No active streams in progress.\n"
        buttons.append([InlineKeyboardButton("🔙 Back to Main", callback_data="ui_MAIN")])

    elif ui_state == "SELECTED_VIEW":
        groups = await db_execute("SELECT chat_id, chat_title, status FROM selected_monitors", fetch="all")
        tags_rows = await db_execute("SELECT tag FROM selected_tags", fetch="all")
        tags_list = [r[0] for r in tags_rows] if tags_rows else []

        text = (f"🎯 **SELECTIVE MONITORS (/MonitorSelected)**\n━━━━━━━━━━━━━━━━━━\n"
                f"🏷️ **Global Target Tags ({len(tags_list)}):**\n{', '.join(tags_list) if tags_list else '_No tags set yet_'}\n\n"
                f"📱 **Monitored Groups ({len(groups or [])}):**\n")
        if not groups: text += "_No selective groups configured yet._\n"
        for c_id, c_title, st in (groups or []):
            p_cnt = (await db_execute("SELECT COUNT(*) FROM jobs WHERE chat_id=? AND status IN ('queued','waiting')", (c_id,), fetch="one") or [0])[0]
            text += f"• **{c_title}** (`{c_id}`)\n  Status: **{st}** | Pending Queue: `{p_cnt}` vids\n"
            buttons.append([
                InlineKeyboardButton(f"🔍 Scan History", callback_data=f"sel_scan_{c_id}"),
                InlineKeyboardButton(f"⏸️ Pause" if st == "ACTIVE" else f"▶️ Proceed", callback_data=f"sel_pause_{c_id}" if st == "ACTIVE" else f"sel_proceed_{c_id}"),
                InlineKeyboardButton(f"🛑 Stop", callback_data=f"sel_stop_{c_id}")
            ])
        buttons.append([InlineKeyboardButton("➕ Add Group(s)", callback_data="sel_add_group"), InlineKeyboardButton("🏷️ Add/Remove Tags", callback_data="sel_manage_tags")])
        buttons.append([InlineKeyboardButton("🔙 Back to Main", callback_data="ui_MAIN")])

    elif ui_state == "ALWAYS_VIEW":
        rows = await db_execute("SELECT chat_id, chat_title, status FROM always_monitors", fetch="all")
        text = "📡 **AUTO-SYNC MONITORS (/MonitorAlways)**\n━━━━━━━━━━━━━━━━━━\n"
        if not rows: text += "No continuous auto-sync monitors configured yet.\n"
        for c_id, c_title, st in (rows or []):
            p_cnt = (await db_execute("SELECT COUNT(*) FROM jobs WHERE chat_id=? AND status IN ('queued','waiting')", (c_id,), fetch="one") or [0])[0]
            text += f"• **{c_title}** (`{c_id}`)\n  Status: **{st}** | Pending Queue: `{p_cnt}` vids\n\n"
            if st == "ACTIVE": buttons.append([InlineKeyboardButton(f"⏸️ Pause {c_title[:15]}", callback_data=f"always_pause_{c_id}"), InlineKeyboardButton(f"🛑 Stop Sync", callback_data=f"always_stop_{c_id}")])
            else: buttons.append([InlineKeyboardButton(f"▶️ Proceed {c_title[:15]}", callback_data=f"always_proceed_{c_id}"), InlineKeyboardButton(f"🛑 Stop Sync", callback_data=f"always_stop_{c_id}")])
        buttons.append([InlineKeyboardButton("🔙 Back to Main", callback_data="ui_MAIN")])

    elif ui_state == "AUTOTRANSFER_VIEW":
        rows = await db_execute("SELECT chat_id, chat_title, status, delete_originals FROM autotransfer_monitors", fetch="all")
        text = "🚀 **AUTO-TRANSFER GROUPS (/AutoTransfer)**\n━━━━━━━━━━━━━━━━━━\n"
        if not rows: text += "No auto-transfer monitors configured yet.\n"
        for c_id, c_title, st, del_orig in (rows or []):
            mode = "🗑️ Delete originals" if del_orig else "📋 Keep originals"
            text += f"• **{c_title}** (`{c_id}`)\n  Status: **{st}** | {mode}\n\n"
            if st == "ACTIVE": buttons.append([InlineKeyboardButton(f"⏸️ Pause {c_title[:15]}", callback_data=f"atr_pause_{c_id}"), InlineKeyboardButton(f"🛑 Stop", callback_data=f"atr_stop_{c_id}")])
            else: buttons.append([InlineKeyboardButton(f"▶️ Proceed {c_title[:15]}", callback_data=f"atr_proceed_{c_id}"), InlineKeyboardButton(f"🛑 Stop", callback_data=f"atr_stop_{c_id}")])
        buttons.append([InlineKeyboardButton("🔙 Back to Main", callback_data="ui_MAIN")])

    elif ui_state == "MONITOR_VIEW":
        total_tags = (await db_execute("SELECT COUNT(DISTINCT tag) FROM monitored_messages WHERE is_queued=0", fetch="one") or [0])[0]
        total_vids = (await db_execute("SELECT COUNT(*) FROM monitored_messages WHERE is_queued=0", fetch="one") or [0])[0]
        PAGE_SIZE = 5
        max_pages = max(1, math.ceil(total_tags / PAGE_SIZE))
        if monitor_page >= max_pages: monitor_page = max_pages - 1
        if monitor_page < 0: monitor_page = 0
        tags_data = await db_execute("SELECT tag, COUNT(*) as cnt FROM monitored_messages WHERE is_queued=0 GROUP BY tag ORDER BY cnt DESC LIMIT ? OFFSET ?", (PAGE_SIZE, monitor_page * PAGE_SIZE), fetch="all")
        text = f"👁️ **MONITOR FINDINGS** (Page {monitor_page + 1} of {max_pages})\nTotal Tags Discovered: {total_tags} | Unqueued Videos: {total_vids}\n━━━━━━━━━━━━━━━━━━\n"
        for tag, cnt in (tags_data or []): buttons.append([InlineKeyboardButton(f"🚀 Queue {tag} ({cnt} vids)", callback_data=f"mon_inspect_{tag}")])
        nav_row = []
        if monitor_page > 0: nav_row.append(InlineKeyboardButton("◀️ Prev", callback_data="mon_prev_page"))
        nav_row.append(InlineKeyboardButton(f"Page {monitor_page + 1}/{max_pages}", callback_data="noop"))
        if monitor_page < max_pages - 1: nav_row.append(InlineKeyboardButton("Next ▶️", callback_data="mon_next_page"))
        if nav_row: buttons.append(nav_row)
        buttons.append([InlineKeyboardButton("🔍 Search Tag", callback_data="mon_search_prompt")])
        buttons.append([InlineKeyboardButton("🔙 Back to Main", callback_data="ui_MAIN")])

    elif ui_state.startswith("MON_INSPECT_"):
        tag = ui_state.replace("MON_INSPECT_", "")
        tg_total = (await db_execute("SELECT COUNT(*) FROM monitored_messages WHERE tag=?", (tag,), fetch="one") or [0])[0]
        last_seen = (await db_execute("SELECT last_seen_count FROM monitored_tags_meta WHERE tag=?", (tag,), fetch="one") or [tg_total])[0]
        album_name = tag.replace("#", "")
        album_id = await get_or_create_vk_album(album_name)
        vk_count = len(vk_video_title_cache.get(album_id, set())) if album_id and await refresh_vk_cache(album_id) or True else 0
        will_add = max(0, tg_total - vk_count)
        text = (f"🔍 **HASHTAG DETAILS: {tag}**\n━━━━━━━━━━━━━━━━━━\n"
                f"📱 Telegram Total Found: {tg_total} videos\n🆕 Added in Telegram since last check: +{max(0, tg_total - last_seen)} videos\n\n"
                f"🌐 **VK Status Breakdown:**\n✅ Already in VK: {vk_count} videos\n📥 Will be Added to VK: {will_add} videos\n━━━━━━━━━━━━━━━━━━\n")
        await db_execute("INSERT INTO monitored_tags_meta (tag, last_seen_count, last_checked_at) VALUES (?,?,?) ON CONFLICT(tag) DO UPDATE SET last_seen_count=excluded.last_seen_count, last_checked_at=excluded.last_checked_at", (tag, tg_total, time.time()))
        buttons.append([InlineKeyboardButton(f"🚀 Queue {will_add} Videos for VK", callback_data=f"mon_queue_tag_{tag}")])
        buttons.append([InlineKeyboardButton("🔙 Back to Findings", callback_data="ui_MONITOR_VIEW")])

    elif ui_state == "PLAYLISTS":
        rows = await list_playlists()
        text = "📋 **PLAYLISTS**\n━━━━━━━━━━━━━━━━━━\n"
        if not rows: text += "No playlists yet.\n"
        for pid, album_name, status, total, completed, failed, skipped in rows:
            text += f"• **{album_name}** — {status}\n  {completed}/{total} done"
            if failed: text += f", {failed} failed"
            if skipped: text += f", {skipped} skipped (dupes)"
            text += "\n\n"
            if status == "WAITING_CONFIRMATION": buttons.append([InlineKeyboardButton(f"▶️ Continue {album_name}", callback_data=f"plcontinue_{pid}"), InlineKeyboardButton("💀 Kill", callback_data=f"plkill_{pid}")])
            elif status in ("PILOT_RUNNING", "RUNNING", "WAITING"): buttons.append([InlineKeyboardButton(f"💀 Kill {album_name}", callback_data=f"plkill_{pid}")])
        buttons.append([InlineKeyboardButton("🔙 Back to Main", callback_data="ui_MAIN")])

    try:
        await bot_app.edit_message_text(chat_id=int(chat_id), message_id=int(msg_id), text=text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(buttons))
    except Exception: pass

async def dashboard_updater():
    global engine_state
    while True:
        await asyncio.sleep(4)
        if engine_state == ENGINE_PAUSE_REQUESTED and not active_jobs:
            engine_state = ENGINE_PAUSED
            await set_control("engine_state", ENGINE_PAUSED)
        await render_dashboard()

# ============================================================
# BOT COMMANDS & HANDLERS
# ============================================================
@bot_app.on_message(filters.command("start"))
async def start_cmd(client, message):
    msg = await message.reply_text("⚙️ Booting Master Dashboard...\n(Pinning message...)")
    try: await msg.pin(both_sides=True)
    except: pass
    await set_control("dashboard_chat_id", message.chat.id)
    await set_control("dashboard_msg_id", msg.id)
    global ui_state
    ui_state = "MAIN"
    await render_dashboard()

@bot_app.on_message(filters.command("refresh"))
async def refresh_cmd(client, message):
    global engine_state
    status_msg = await message.reply_text("🔄 **Initiating System & VK Database Refresh...**\n`[1/5]` Pausing transfer engine...", parse_mode=ParseMode.MARKDOWN)
    prev_state = engine_state
    engine_state = ENGINE_PAUSE_REQUESTED
    pause_event.clear()
    await asyncio.sleep(1)

    await status_msg.edit_text("🔄 **Initiating System & VK Database Refresh...**\n`[2/5]` Clearing local job cache & stale message status...", parse_mode=ParseMode.MARKDOWN)
    while not stream_queue_t1.empty(): stream_queue_t1.get_nowait(); stream_queue_t1.task_done()
    while not stream_queue_t2.empty(): stream_queue_t2.get_nowait(); stream_queue_t2.task_done()
    
    playlist_queues.clear()
    playlist_order.clear()
    cancelled_jobs.clear()
    vk_video_title_cache.clear()
    vk_album_name_cache.clear()
    await db_execute("DELETE FROM jobs WHERE status != 'done'")
    await db_execute("UPDATE monitored_messages SET is_queued=0")

    await status_msg.edit_text("🔄 **Initiating System & VK Database Refresh...**\n`[3/5]` Querying live VK catalog & purging broken/duplicate videos...", parse_mode=ParseMode.MARKDOWN)
    synced_cnt, dupes_cnt, broken_cnt = await sync_vk_to_local_db()
    await status_msg.edit_text("🔄 **Initiating System & VK Database Refresh...**\n`[4/5]` Resynchronizing monitored tags with database...", parse_mode=ParseMode.MARKDOWN)
    await asyncio.sleep(1)

    engine_state = ENGINE_RUNNING if prev_state == ENGINE_RUNNING else prev_state
    if engine_state == ENGINE_RUNNING: pause_event.set()

    await status_msg.edit_text(f"✅ **System & VK Sync Refresh Complete!**\n\n• **Live VK Videos Indexed:** `{synced_cnt}`\n• **Duplicates Purged from VK:** `{dupes_cnt}`\n• **Broken Videos Cleaned:** `{broken_cnt}`\n• **Pending Queues:** Reset & Ready\n\n_You can now safely re-run history scans or commands._", parse_mode=ParseMode.MARKDOWN)
    await render_dashboard()

@bot_app.on_message(filters.command("monitor"))
async def monitor_cmd(client, message):
    user_states[message.chat.id] = {'awaiting_monitor_input': True}
    await message.reply_text("👁️ **MONITORING CONFIGURATION**\nSend the Group IDs or usernames you want to monitor for VK.\n*(Multiple allowed separated by commas, e.g., `-10012345678, @my_channel`)*", parse_mode=ParseMode.MARKDOWN)

@bot_app.on_message(filters.command("monitoralways"))
async def monitor_always_cmd(client, message):
    user_states[message.chat.id] = {'awaiting_always_input': True}
    await message.reply_text("📡 **CONTINUOUS AUTO-SYNC CONFIGURATION**\nSend the Group ID or username to continuously mirror ALL tagged videos to VK.", parse_mode=ParseMode.MARKDOWN)

@bot_app.on_message(filters.command("monitorselected"))
async def monitor_selected_cmd(client, message):
    global ui_state
    ui_state = "SELECTED_VIEW"
    await render_dashboard()

@bot_app.on_message(filters.command("setmasterforum"))
async def set_master_forum_cmd(client, message):
    if message.chat.type in (enums.ChatType.GROUP, enums.ChatType.SUPERGROUP):
        full_chat = await client.get_chat(message.chat.id)
        forum_id = full_chat.id
        if not getattr(full_chat, "is_forum", False): return await message.reply_text("⚠️ **Topics are not enabled here.**\nPlease go to Group Settings -> Enable 'Topics', then run this command again.", parse_mode=ParseMode.MARKDOWN)
    else:
        if len(message.command) < 2: return await message.reply_text("Usage in DM: `/setmasterforum <chat_id>`", parse_mode=ParseMode.MARKDOWN)
        forum_id = message.command[1].strip()
    await set_control("master_forum_id", str(forum_id))
    await message.reply_text(f"✅ **Master Forum ID successfully set to:** `{forum_id}`", parse_mode=ParseMode.MARKDOWN)

async def _resolve_target_chat(message, command_name):
    if message.chat.type in (enums.ChatType.GROUP, enums.ChatType.SUPERGROUP): return message.chat.id, (message.chat.title or str(message.chat.id))
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply_text(f"Usage in DM: `/{command_name} <group_id_or_@username>`", parse_mode=ParseMode.MARKDOWN)
        return None, None
    raw_target = parts[1].strip()
    try:
        chat_obj = await user_app.get_chat(int(raw_target) if raw_target.lstrip("-").isdigit() else raw_target)
        return chat_obj.id, (chat_obj.title or raw_target)
    except Exception as e:
        await message.reply_text(f"❌ Couldn't resolve `{raw_target}`: {e}", parse_mode=ParseMode.MARKDOWN)
        return None, None

@bot_app.on_message(filters.command(["transfer", "copy", "autotransfer"]))
async def transfer_copy_auto_cmd(client, message):
    command_name = message.command[0].lower()
    target_chat_id, target_title = await _resolve_target_chat(message, command_name)
    if target_chat_id is None: return
    user_states[message.chat.id] = {'cmd_type': command_name, 'target_chat_id': target_chat_id, 'target_title': target_title}
    action_word = "Auto-Transfer" if command_name == "autotransfer" else command_name.title()
    kbd = InlineKeyboardMarkup([[InlineKeyboardButton(f"🗂️ Process ALL Messages", callback_data="tc_mode_all")], [InlineKeyboardButton(f"🏷️ Process Specific Tags", callback_data="tc_mode_tags")]])
    await message.reply_text(f"⚙️ **Setup {action_word} for: {target_title}**\n\nDo you want to process every media message in this chat, or only messages with specific tags?", reply_markup=kbd, parse_mode=ParseMode.MARKDOWN)

@bot_app.on_message(filters.command("reset"))
async def reset_cmd(client, message):
    confirm_kbd = InlineKeyboardMarkup([[InlineKeyboardButton("⚠️ Yes, wipe everything", callback_data="reset_confirm"), InlineKeyboardButton("Cancel", callback_data="reset_cancel")]])
    await message.reply_text("⚠️ **This will permanently delete ALL jobs, playlists, monitors, topic mappings, transfer history, and downloaded files, and reset the engine to a clean slate.**\n\nAre you sure?", reply_markup=confirm_kbd, parse_mode=ParseMode.MARKDOWN)

@bot_app.on_message(filters.private & filters.text & ~filters.command(["start", "help", "refresh", "monitor", "monitoralways", "monitorselected", "transfer", "copy", "autotransfer", "reset", "setmasterforum"]))
async def handle_user_input(client, message):
    chat_id = message.chat.id
    state = user_states.get(chat_id, {})

    if state.get('awaiting_custom_tags'):
        state['awaiting_custom_tags'] = False
        custom_tags = [x.strip().lower().replace("#", "") for x in message.text.split(",") if x.strip()]
        state['custom_tags'] = custom_tags
        if state['cmd_type'] == "autotransfer":
            kbd = InlineKeyboardMarkup([[InlineKeyboardButton("🗑️ Delete originals", callback_data="atr_setup_del")], [InlineKeyboardButton("📋 Keep originals", callback_data="atr_setup_keep")]])
            await message.reply_text(f"📡 **Auto-Transfer Setup: {state['target_title']}**\nShould originals be deleted after copying to the Master Forum, or kept?", reply_markup=kbd, parse_mode=ParseMode.MARKDOWN)
        else:
            asyncio.create_task(run_custom_transfer(chat_id, state, mode="TAGS", custom_tags=custom_tags))
            user_states.pop(chat_id, None)
        return

    if state.get('awaiting_sel_groups'):
        user_states.pop(chat_id, None)
        raw_inputs = [x.strip() for x in message.text.split(",") if x.strip()]
        status_msg = await message.reply_text("⚙️ Resolving groups and running past message scan against VK...")
        for target in raw_inputs:
            try:
                chat_obj = await user_app.get_chat(int(target) if target.startswith("-100") or target.startswith("-") or target.isdigit() else target)
                resolved_id, title = chat_obj.id, chat_obj.title or target
                await db_execute("INSERT INTO selected_monitors (chat_id, chat_title, status, last_msg_id, added_at) VALUES (?,?,'ACTIVE',0,?) ON CONFLICT(chat_id) DO UPDATE SET chat_title=excluded.chat_title", (resolved_id, title, time.time()))
                tot, in_vk, valid_jobs = await run_selective_history_scan(resolved_id)
                if valid_jobs:
                    user_states[chat_id] = {f"pending_sel_jobs_{resolved_id}": valid_jobs}
                    kbd = InlineKeyboardMarkup([[InlineKeyboardButton(f"🚀 Queue {len(valid_jobs)} New Videos", callback_data=f"sel_confirm_queue_{resolved_id}")], [InlineKeyboardButton("❌ Skip / Back", callback_data="ui_SELECTED_VIEW")]])
                    await status_msg.reply_text(f"🎯 **Selective Scan Preview: {title}**\n━━━━━━━━━━━━━━━━━━\n📱 **Total Matched in TG:** `{tot}` videos\n✅ **Already in VK / Skipped:** `{in_vk}` videos\n📥 **New Ready to Queue:** `{len(valid_jobs)}` videos", reply_markup=kbd, parse_mode=ParseMode.MARKDOWN)
                else:
                    await status_msg.reply_text(f"🎯 **Selective Scan Complete: {title}**\n━━━━━━━━━━━━━━━━━━\nFound `{tot}` matched videos, but all `{in_vk}` are already uploaded to VK!")
            except Exception as e: await message.reply_text(f"❌ Failed to process group `{target}`: {e}")
        global ui_state
        ui_state = "SELECTED_VIEW"
        await render_dashboard()
        return

    if state.get('awaiting_sel_tags'):
        user_states.pop(chat_id, None)
        raw_tags = [x.strip().lower().replace("#", "") for x in message.text.split(",") if x.strip()]
        for t in raw_tags: await db_execute("INSERT INTO selected_tags (tag, added_at) VALUES (?,?) ON CONFLICT(tag) DO NOTHING", (f"#{t}", time.time()))
        await message.reply_text(f"✅ Registered **{len(raw_tags)}** hashtag(s) globally!")
        ui_state = "SELECTED_VIEW"
        await render_dashboard()
        return

    if state.get('awaiting_always_input'):
        user_states.pop(chat_id, None)
        target = message.text.strip()
        status_msg = await message.reply_text("⚙️ Connecting and scanning group history...")
        try:
            chat_obj = await user_app.get_chat(int(target) if target.startswith("-100") or target.startswith("-") or target.isdigit() else target)
            resolved_id, title = chat_obj.id, chat_obj.title or target
            await db_execute("INSERT INTO always_monitors (chat_id, chat_title, status, last_msg_id, added_at) VALUES (?,?,'ACTIVE',0,?) ON CONFLICT(chat_id) DO UPDATE SET chat_title=excluded.chat_title", (resolved_id, title, time.time()))
            found_cnt = 0
            async for msg in user_app.get_chat_history(resolved_id, limit=300):
                if msg.video or msg.document:
                    txt = msg.caption or msg.text or ""
                    tag_clean = extract_first_tag(txt)
                    if tag_clean:
                        album_id = await get_or_create_vk_album(tag_clean)
                        if album_id and not await is_msg_in_db(resolved_id, msg.id):
                            job = {'job_id': f"{resolved_id}_{msg.id}", 'playlist_id': None, 'chat_id': chat_id, 'msg_chat_id': resolved_id, 'msg_id': msg.id, 'album_id': album_id, 'album_name': tag_clean, 'query': f"#{tag_clean}", 'idx': 1, 'is_pilot': False, 'status': 'queued', 'file_path': None, 'caption': txt, 'tier': 2}
                            await save_job(job)
                            await stream_queue_t2.put(job)
                            found_cnt += 1
            await status_msg.edit_text(f"✅ **Auto-Sync Active!**\nGroup: **{title}**\nFound `{found_cnt}` videos.")
        except Exception as e: await status_msg.edit_text(f"❌ Failed to register group: `{e}`")
        return

    if state.get('awaiting_monitor_input'):
        user_states.pop(chat_id, None)
        raw_inputs = message.text.split(",")
        status_msg = await message.reply_text("⚙️ Launching background history scanners...")
        success_count = 0
        for target in raw_inputs:
            if not target.strip(): continue
            success, _ = await add_monitored_target(target.strip())
            if success: success_count += 1
        await status_msg.edit_text(f"✅ Successfully registered **{success_count}** groups!", parse_mode=ParseMode.MARKDOWN)
        return

    if state.get('awaiting_tag_search'):
        user_states.pop(chat_id, None)
        query_tag = message.text.strip().lower()
        if not query_tag.startswith("#"): query_tag = f"#{query_tag}"
        exists = await db_execute("SELECT COUNT(*) FROM monitored_messages WHERE tag=?", (query_tag,), fetch="one")
        if not exists or exists[0] == 0: return await message.reply_text(f"❌ Tag `{query_tag}` not found in monitored database.")
        ui_state = f"MON_INSPECT_{query_tag}"
        await render_dashboard()
        return

    if not state.get('awaiting_group'):
        search_query = message.text.strip().lower()
        search_query = search_query if search_query.startswith("#") else f"#{search_query}"
        user_states[chat_id] = {'query': search_query, 'awaiting_group': True}
        return await message.reply_text(f"🔍 VK Search: *{search_query}*\n📌 Send the **Group Chat ID** to fetch from.", parse_mode=ParseMode.MARKDOWN)

    try: target_group_id = int(message.text.strip())
    except ValueError: return await message.reply_text("⚠️ Invalid numeric ID.")

    status_msg = await message.reply_text("🔎 Scanning media group...")
    raw_found, processed_groups = [], set()
    try:
        async for msg in user_app.search_messages(chat_id=target_group_id, query=state['query']):
            if msg.media_group_id:
                if msg.media_group_id not in processed_groups:
                    processed_groups.add(msg.media_group_id)
                    album_msgs = sorted(await user_app.get_media_group(target_group_id, msg.id), key=lambda x: x.id)
                    raw_found.extend(parse_master_caption_bundle(album_msgs, [state['query']]))
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
        if await is_msg_in_db(msg.chat.id, msg.id): skipped_count += 1
        else: new_msgs.append(msg)

    if not new_msgs:
        user_states.pop(chat_id, None)
        return await status_msg.edit_text(f"❌ No *new* matching videos found.\n(Skipped {skipped_count} already processed).", parse_mode=ParseMode.MARKDOWN)

    state['found_msgs'] = sorted(new_msgs, key=lambda m: getattr(m, '_relative_idx', 1))
    kbd = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🚀 Queue {len(new_msgs)} videos for VK", callback_data="queue_transfer")],
        [InlineKeyboardButton(f"📋 Copy to Master Forum", callback_data="direct_copy_forum")],
        [InlineKeyboardButton(f"🚀 Transfer to Master Forum (Delete Orig)", callback_data="direct_transfer_forum")]
    ])
    await status_msg.edit_text(f"📊 **Found** `{state['query']}`\n🆕 Total Found: *{len(new_msgs)}* | ⏭️ Skipped from VK: *{skipped_count}*", parse_mode=ParseMode.MARKDOWN, reply_markup=kbd)

@bot_app.on_callback_query()
async def handle_buttons(client, callback):
    global engine_state, ui_state, monitor_page
    chat_id, data = callback.message.chat.id, callback.data

    if data.startswith("ui_"):
        ui_state = data.replace("ui_", "")
        await render_dashboard()
        return await callback.answer()
    elif data == "noop": return await callback.answer()
    elif data in ("direct_copy_forum", "direct_transfer_forum"):
        state = user_states.get(chat_id)
        if not state or 'found_msgs' not in state: return await callback.answer("Session expired or no messages found.", show_alert=True)
        delete_originals = (data == "direct_transfer_forum")
        action_name = "Transfer" if delete_originals else "Copy"
        tag, msgs = state['query'], state['found_msgs']
        target_chat_id = msgs[0].chat.id if msgs else None
        if not target_chat_id: return await callback.answer("No valid chat ID found.", show_alert=True)
        await callback.answer(f"Starting {action_name} to Master Forum...")
        await callback.message.edit_text(f"⚙️ **Executing {action_name} for {tag}**\nPlease wait...")
        try:
            master_forum_id, topic_id, topic_title = await tg_resolve_destination_topic(tag)
            topic_cache = await build_topic_dedupe_cache(master_forum_id, topic_id)
            copied = 0
            for m in msgs:
                f_id = m.video.file_unique_id if m.video else (m.document.file_unique_id if m.document else None)
                if f_id and f_id in topic_cache: continue
                try:
                    if await tg_execute_message_copy(target_chat_id, m, master_forum_id, topic_id, tag, delete_originals, topic_cache): copied += 1
                except Exception: pass
            await callback.message.edit_text(f"✅ **{action_name} Complete!**\nMoved {copied}/{len(msgs)} messages to **{topic_title}**.")
        except Exception as e: await callback.message.edit_text(f"❌ **Error during {action_name}:** {e}")
        user_states.pop(chat_id, None)
        await render_dashboard()
        return
    elif data in ("tc_mode_all", "tc_mode_tags"):
        state = user_states.get(chat_id)
        if not state: return await callback.answer("Session expired.", show_alert=True)
        mode = "ALL" if data == "tc_mode_all" else "TAGS"
        state['mode'] = mode
        if mode == "TAGS":
            state['awaiting_custom_tags'] = True
            await callback.message.edit_text("🏷️ **Tag Filter Mode**\nPlease type the tags you want to process, separated by commas (e.g., `#movies, #music`).", parse_mode=ParseMode.MARKDOWN)
            return
        if state['cmd_type'] == "autotransfer":
            kbd = InlineKeyboardMarkup([[InlineKeyboardButton("🗑️ Delete originals", callback_data="atr_setup_del")], [InlineKeyboardButton("📋 Keep originals", callback_data="atr_setup_keep")]])
            await callback.message.edit_text(f"📡 **Auto-Transfer Setup: {state['target_title']}**\nShould originals be deleted after copying to the Master Forum, or kept?", reply_markup=kbd, parse_mode=ParseMode.MARKDOWN)
            return
        else:
            asyncio.create_task(run_custom_transfer(chat_id, state, mode="ALL", custom_tags=[]))
            user_states.pop(chat_id, None)
            return
    elif data in ("atr_setup_del", "atr_setup_keep"):
        state = user_states.get(chat_id)
        if not state: return await callback.answer("Session expired.", show_alert=True)
        delete_originals = (data == "atr_setup_del")
        state['delete_originals'] = delete_originals
        c_id, title, mode, tags_str = state['target_chat_id'], state['target_title'], state['mode'], ",".join(state.get('custom_tags', []))
        await db_execute(
            """INSERT INTO autotransfer_monitors (chat_id, chat_title, status, delete_originals, mode, tags, last_msg_id, added_at)
               VALUES (?,?,'ACTIVE',?,?,?,0,?) ON CONFLICT(chat_id) DO UPDATE SET status='ACTIVE', delete_originals=excluded.delete_originals, mode=excluded.mode, tags=excluded.tags""",
            (c_id, title, int(delete_originals), mode, tags_str, time.time())
        )
        await callback.answer("✅ Auto-Transfer active. Running historical backfill now...", show_alert=True)
        asyncio.create_task(run_custom_transfer(chat_id, state, mode=mode, custom_tags=state.get('custom_tags', [])))
        user_states.pop(chat_id, None)
        return
    elif data.startswith("atr_proceed_"):
        c_id = int(data.replace("atr_proceed_", ""))
        await db_execute("UPDATE autotransfer_monitors SET status='ACTIVE' WHERE chat_id=?", (c_id,))
        await callback.answer("▶️ Resumed Auto-Transfer.")
        await render_dashboard()
    elif data.startswith("atr_pause_"):
        c_id = int(data.replace("atr_pause_", ""))
        await db_execute("UPDATE autotransfer_monitors SET status='PAUSED' WHERE chat_id=?", (c_id,))
        await callback.answer("⏸️ Paused Auto-Transfer.")
        await render_dashboard()
    elif data.startswith("atr_stop_"):
        c_id = int(data.replace("atr_stop_", ""))
        await db_execute("DELETE FROM autotransfer_monitors WHERE chat_id=?", (c_id,))
        await callback.answer("🛑 Auto-Transfer Stopped.")
        await render_dashboard()
    elif data == "reset_cancel":
        await callback.message.edit_text("Reset cancelled.")
        return await callback.answer()
    elif data == "reset_confirm":
        prev_state = engine_state
        engine_state = ENGINE_PAUSE_REQUESTED
        pause_event.clear()
        await callback.message.edit_text("🔄 Resetting everything...")
        await asyncio.sleep(1)

        while not stream_queue_t1.empty(): stream_queue_t1.get_nowait(); stream_queue_t1.task_done()
        while not stream_queue_t2.empty(): stream_queue_t2.get_nowait(); stream_queue_t2.task_done()

        playlist_queues.clear()
        playlist_order.clear()
        cancelled_jobs.clear()
        vk_video_title_cache.clear()
        vk_album_name_cache.clear()
        active_jobs.clear()
        active_bulk_tasks.clear()
        user_states.clear()
        vk_reupload_attempts.clear()

        for table in ("jobs", "playlists", "monitored_chats", "monitored_messages", "monitored_tags_meta", "always_monitors", "selected_monitors", "selected_tags", "tg_routing_destinations", "tg_copied_messages", "autotransfer_monitors"):
            await db_execute(f"DELETE FROM {table}")

        engine_state = ENGINE_RUNNING
        pause_event.set()
        await set_control("engine_state", ENGINE_RUNNING)
        await callback.message.edit_text("✅ **Full reset complete.** All state wiped, engine restarted clean.", parse_mode=ParseMode.MARKDOWN)
        ui_state = "MAIN"
        await render_dashboard()
        return
    elif data == "sel_add_group":
        user_states[chat_id] = {'awaiting_sel_groups': True}
        await callback.message.reply_text("📌 Send the Group ID(s) or @username(s) to monitor selectively (comma-separated):")
        return await callback.answer()
    elif data == "sel_manage_tags":
        user_states[chat_id] = {'awaiting_sel_tags': True}
        await callback.message.reply_text("🏷️ Send the hashtag(s) you want to monitor globally (e.g. `#spiderman, #batman`):")
        return await callback.answer()
    elif data.startswith("sel_scan_"):
        c_id = int(data.replace("sel_scan_", ""))
        await callback.answer("Scanning past history against VK...", show_alert=True)
        tot, in_vk, valid_jobs = await run_selective_history_scan(c_id)
        if valid_jobs:
            user_states[chat_id] = {f"pending_sel_jobs_{c_id}": valid_jobs}
            kbd = InlineKeyboardMarkup([[InlineKeyboardButton(f"🚀 Queue {len(valid_jobs)} New Videos", callback_data=f"sel_confirm_queue_{c_id}")], [InlineKeyboardButton("❌ Skip / Back", callback_data="ui_SELECTED_VIEW")]])
            await callback.message.reply_text(f"🎯 **History Scan Preview**\n━━━━━━━━━━━━━━━━━━\n📱 **Total Matched in TG:** `{tot}` videos\n✅ **Already in VK:** `{in_vk}` videos\n📥 **New Ready to Queue:** `{len(valid_jobs)}` videos", reply_markup=kbd, parse_mode=ParseMode.MARKDOWN)
        else: await callback.message.reply_text(f"ℹ️ Found `{tot}` matching videos, but all `{in_vk}` are already present in VK!")
        return
    elif data.startswith("sel_confirm_queue_"):
        c_id = int(data.replace("sel_confirm_queue_", ""))
        state_key = f"pending_sel_jobs_{c_id}"
        jobs_to_queue = user_states.get(chat_id, {}).get(state_key, [])
        if not jobs_to_queue: return await callback.answer("Session expired or no jobs pending.", show_alert=True)
        for job in jobs_to_queue:
            await save_job(job)
            await stream_queue_t2.put(job)
        user_states.get(chat_id, {}).pop(state_key, None)
        await callback.answer(f"🚀 Queued {len(jobs_to_queue)} videos for streaming!", show_alert=True)
        await callback.message.delete()
        await render_dashboard()
        return
    elif data.startswith("sel_proceed_"):
        c_id = int(data.replace("sel_proceed_", ""))
        await db_execute("UPDATE selected_monitors SET status='ACTIVE' WHERE chat_id=?", (c_id,))
        await callback.answer("▶️ Resumed Selective Auto-Sync.")
        await render_dashboard()
    elif data.startswith("sel_pause_"):
        c_id = int(data.replace("sel_pause_", ""))
        await db_execute("UPDATE selected_monitors SET status='PAUSED' WHERE chat_id=?", (c_id,))
        await callback.answer("⏸️ Paused Selective Auto-Sync.")
        await render_dashboard()
    elif data.startswith("sel_stop_"):
        c_id = int(data.replace("sel_stop_", ""))
        await db_execute("DELETE FROM selected_monitors WHERE chat_id=?", (c_id,))
        await callback.answer("🛑 Selective Sync Stopped.")
        await render_dashboard()
    elif data.startswith("always_proceed_"):
        c_id = int(data.replace("always_proceed_", ""))
        await db_execute("UPDATE always_monitors SET status='ACTIVE' WHERE chat_id=?", (c_id,))
        await callback.answer("▶️ Resumed Auto-Sync.")
        await render_dashboard()
    elif data.startswith("always_pause_"):
        c_id = int(data.replace("always_pause_", ""))
        await db_execute("UPDATE always_monitors SET status='PAUSED' WHERE chat_id=?", (c_id,))
        await callback.answer("⏸️ Paused Auto-Sync.")
        await render_dashboard()
    elif data.startswith("always_stop_"):
        c_id = int(data.replace("always_stop_", ""))
        await db_execute("DELETE FROM always_monitors WHERE chat_id=?", (c_id,))
        await callback.answer("🛑 Auto-Sync Stopped.")
        await render_dashboard()
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
        rows = await db_execute("SELECT chat_id, msg_id, caption FROM monitored_messages WHERE tag=? AND is_queued=0", (tag,), fetch="all")
        if not rows:
            ui_state = "MONITOR_VIEW"
            return await render_dashboard()
        album_name = tag.replace("#", "")
        album_id = await get_or_create_vk_album(album_name)
        if not album_id: return await callback.message.reply_text("❌ Failed to resolve VK album.")
        await refresh_vk_cache(album_id)

        valid_jobs_data = []
        for idx, (c_id, m_id, caption) in enumerate(rows, start=1):
            title = display_title(album_name, idx, caption, m_id)
            if await vk_title_exists(album_id, title): await db_execute("UPDATE monitored_messages SET is_queued=1 WHERE chat_id=? AND msg_id=?", (c_id, m_id))
            else: valid_jobs_data.append((c_id, m_id, caption, idx))

        if not valid_jobs_data:
            ui_state = "MONITOR_VIEW"
            return await render_dashboard()

        playlist_id = await create_playlist(chat_id, tag, album_name, album_id, len(valid_jobs_data))
        pilot_added = False
        for c_id, m_id, caption, idx in valid_jobs_data:
            job_id = f"{c_id}_{m_id}"
            is_pilot = not pilot_added
            if is_pilot: pilot_added = True
            job = {'job_id': job_id, 'playlist_id': playlist_id, 'chat_id': chat_id, 'msg_chat_id': c_id, 'msg_id': m_id, 'album_id': album_id, 'album_name': album_name, 'query': tag, 'idx': idx, 'is_pilot': is_pilot, 'status': 'waiting', 'caption': caption, 'tier': 1}
            await save_job(job)
            cancelled_jobs.discard(job_id)
            if is_pilot:
                await update_job_status(job_id, "queued")
                await stream_queue_t1.put(job)

        await set_playlist_status(playlist_id, "PILOT_RUNNING" if pilot_added else "RUNNING")
        ui_state = "MONITOR_VIEW"
        await render_dashboard()
    elif data.startswith("kill_"):
        job_id = data.replace("kill_", "")
        cancelled_jobs.add(job_id)
        await update_job_status(job_id, "cancelled")
        await callback.answer("💀 Poison pill dropped. Stream aborting...", show_alert=True)
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
            await callback.answer("🟡 Pause requested — draining active streams...")
        else:
            engine_state = ENGINE_RUNNING
            pause_event.set()
            await set_control("engine_state", ENGINE_RUNNING)
            await callback.answer("▶️ Resumed")
        await render_dashboard()
    elif data == "clear_queue":
        cleared = 0
        while not stream_queue_t1.empty():
            job = stream_queue_t1.get_nowait()
            stream_queue_t1.task_done()
            cancelled_jobs.add(job['job_id'])
            await delete_job_row(job['job_id'])
            cleared += 1
        while not stream_queue_t2.empty():
            job = stream_queue_t2.get_nowait()
            stream_queue_t2.task_done()
            cancelled_jobs.add(job['job_id'])
            await delete_job_row(job['job_id'])
            cleared += 1
        for pid in list(playlist_queues.keys()):
            for job in playlist_queues.pop(pid):
                cancelled_jobs.add(job['job_id'])
                await delete_job_row(job['job_id'])
                cleared += 1
        playlist_order.clear()
        await render_dashboard()
        await callback.answer(f"Cleared {cleared} pending streams.", show_alert=True)
    elif data == "queue_transfer":
        state = user_states.get(chat_id)
        if not state: return await callback.answer("Expired session.")
        await callback.answer("Building playlist & running pilot video...")
        album_name = state['query'].replace("#", "")
        album_id = await get_or_create_vk_album(album_name)
        if not album_id:
            user_states.pop(chat_id, None)
            return await callback.message.edit_text("❌ Failed to resolve VK album.")
        await refresh_vk_cache(album_id)

        non_dupe_msgs, skipped_dupes = [], 0
        for msg in state['found_msgs']:
            idx, caption = getattr(msg, '_relative_idx', 1), getattr(msg, '_custom_caption', "")
            if await vk_title_exists(album_id, display_title(album_name, idx, caption, msg.id)): skipped_dupes += 1
            else: non_dupe_msgs.append(msg)

        if not non_dupe_msgs:
            user_states.pop(chat_id, None)
            await callback.message.delete()
            return await render_dashboard()

        playlist_id = await create_playlist(chat_id, state['query'], album_name, album_id, len(non_dupe_msgs))
        await db_execute("UPDATE playlists SET skipped_dupes=? WHERE playlist_id=?", (skipped_dupes, playlist_id))

        pilot_msg = non_dupe_msgs[0]
        for msg in non_dupe_msgs:
            idx, caption = getattr(msg, '_relative_idx', 1), getattr(msg, '_custom_caption', "")
            is_pilot = (msg is pilot_msg)
            job_id = f"{msg.chat.id}_{msg.id}"
            job = {'job_id': job_id, 'playlist_id': playlist_id, 'chat_id': chat_id, 'msg_chat_id': msg.chat.id, 'msg_id': msg.id, 'album_id': album_id, 'album_name': album_name, 'query': state['query'], 'idx': idx, 'is_pilot': is_pilot, 'status': 'waiting', 'caption': caption, 'tier': 1}
            await save_job(job)
            cancelled_jobs.discard(job_id)
            if is_pilot:
                await update_job_status(job_id, "queued")
                await stream_queue_t1.put(job)

        await set_playlist_status(playlist_id, "PILOT_RUNNING")
        user_states.pop(chat_id, None)
        await callback.message.delete()
        await render_dashboard()

# ============================================================
# STARTUP & REBOOT RECOVERY
# ============================================================
async def main():
    global engine_state
    await user_app.start()
    await bot_app.start()

    await bot_app.set_bot_commands([
        BotCommand("start", "⚙️ Master Dashboard"),
        BotCommand("refresh", "🔄 Refresh DB & Resync VK"),
        BotCommand("setmasterforum", "🏷️ Set Master Forum Topic Hub"),
        BotCommand("monitorselected", "🎯 Selective Sync (/MonitorSelected)"),
        BotCommand("monitoralways", "📡 Continuous Auto-Sync (/MonitorAlways)"),
        BotCommand("monitor", "👁️ Monitor Chat History (VK)"),
        BotCommand("transfer", "🚀 Transfer tagged videos (TG Copy + Delete)"),
        BotCommand("copy", "📋 Copy tagged videos (TG Copy + Keep)"),
        BotCommand("autotransfer", "📡 Continuous Telegram Auto-Transfer"),
        BotCommand("reset", "⚠️ Full factory reset")
    ])

    await sync_vk_to_local_db()
    console.print("[bold green]✅ BotFather command menu set.[/bold green]")

    saved_state = await get_control("engine_state", ENGINE_RUNNING)
    engine_state = saved_state if saved_state in (ENGINE_RUNNING, ENGINE_PAUSED) else ENGINE_RUNNING
    if engine_state != ENGINE_RUNNING: pause_event.clear()

    monitored_targets = await db_execute("SELECT chat_identifier, resolved_id FROM monitored_chats", fetch="all")
    if monitored_targets:
        for c_id, r_id in monitored_targets: asyncio.create_task(scan_chat_history(c_id, r_id))

    dashboard_chat_id = await get_control("dashboard_chat_id")
    if dashboard_chat_id:
        try:
            dash_msg = await bot_app.send_message(chat_id=int(dashboard_chat_id), text="⚙️ **System Online / Reboot Detected**\nBooting Master Dashboard...", parse_mode=ParseMode.MARKDOWN)
            try: await dash_msg.pin(both_sides=True)
            except: pass
            await set_control("dashboard_msg_id", dash_msg.id)
        except Exception as e: console.print(f"[bold red]Failed to auto-pin fresh dashboard on startup: {e}[/bold red]")

    always_groups = await db_execute("SELECT chat_id, chat_title, status, last_msg_id FROM always_monitors", fetch="all")
    if always_groups:
        for c_id, title, st, last_mid in always_groups:
            new_vids_cnt = 0
            async for msg in user_app.get_chat_history(c_id, limit=200):
                if msg.id <= last_mid: break
                if msg.video or msg.document:
                    txt = msg.caption or msg.text or ""
                    tag_clean = extract_first_tag(txt)
                    if tag_clean:
                        album_id = await get_or_create_vk_album(tag_clean)
                        if album_id and not await is_msg_in_db(c_id, msg.id):
                            job = {'job_id': f"{c_id}_{msg.id}", 'playlist_id': None, 'chat_id': dashboard_chat_id or c_id, 'msg_chat_id': c_id, 'msg_id': msg.id, 'album_id': album_id, 'album_name': tag_clean, 'query': f"#{tag_clean}", 'idx': 1, 'is_pilot': False, 'status': 'queued', 'caption': txt, 'tier': 2}
                            await save_job(job)
                            if st == "ACTIVE": await stream_queue_t2.put(job)
                            new_vids_cnt += 1

            if dashboard_chat_id:
                kbd = InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Proceed", callback_data=f"always_proceed_{c_id}"), InlineKeyboardButton("⏸️ Pause", callback_data=f"always_pause_{c_id}"), InlineKeyboardButton("🛑 Stop", callback_data=f"always_stop_{c_id}")]])
                await bot_app.send_message(chat_id=int(dashboard_chat_id), text=f"📡 **Continuous Auto-Sync Recovery**\n\nGroup: **{title}** (`{c_id}`)\nPending Files Discovered: `{new_vids_cnt}`\nCurrent Mode: **{st}**", reply_markup=kbd, parse_mode=ParseMode.MARKDOWN)

    selected_groups = await db_execute("SELECT chat_id, chat_title, status, last_msg_id FROM selected_monitors", fetch="all")
    registered_tags = [r[0] for r in (await db_execute("SELECT tag FROM selected_tags", fetch="all") or [])]

    if selected_groups and registered_tags:
        for c_id, title, st, last_mid in selected_groups:
            new_vids_cnt = 0
            async for msg in user_app.get_chat_history(c_id, limit=200):
                if msg.id <= last_mid: break
                if msg.video or msg.document:
                    album_msgs = [msg]
                    if msg.media_group_id:
                        try: album_msgs = sorted(await user_app.get_media_group(c_id, msg.id), key=lambda x: x.id)
                        except Exception: pass
                    for m in parse_master_caption_bundle(album_msgs, registered_tags):
                        tag_clean = getattr(m, '_custom_album', None)
                        if not tag_clean: continue
                        album_id = await get_or_create_vk_album(tag_clean)
                        if not album_id: continue
                        
                        idx, cap = getattr(m, '_relative_idx', 1), getattr(m, '_custom_caption', "")
                        if not await is_msg_in_db(c_id, m.id):
                            job = {'job_id': f"{c_id}_{m.id}", 'playlist_id': None, 'chat_id': dashboard_chat_id or c_id, 'msg_chat_id': c_id, 'msg_id': m.id, 'album_id': album_id, 'album_name': tag_clean, 'query': f"#{tag_clean}", 'idx': idx, 'is_pilot': False, 'status': 'queued', 'caption': cap, 'tier': 2}
                            await save_job(job)
                            if st == "ACTIVE": await stream_queue_t2.put(job)
                            new_vids_cnt += 1

            if dashboard_chat_id:
                kbd = InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Proceed", callback_data=f"sel_proceed_{c_id}"), InlineKeyboardButton("⏸️ Pause", callback_data=f"sel_pause_{c_id}"), InlineKeyboardButton("🛑 Stop", callback_data=f"sel_stop_{c_id}")]])
                await bot_app.send_message(chat_id=int(dashboard_chat_id), text=f"🎯 **Selective Auto-Sync Recovery**\n\nGroup: **{title}** (`{c_id}`)\nPending Files Discovered: `{new_vids_cnt}`\nCurrent Mode: **{st}**", reply_markup=kbd, parse_mode=ParseMode.MARKDOWN)

    autotransfer_groups = await db_execute("SELECT chat_id, chat_title, status, delete_originals FROM autotransfer_monitors", fetch="all")
    if autotransfer_groups and dashboard_chat_id:
        for c_id, title, st, del_orig in autotransfer_groups:
            mode = "🗑️ Delete originals" if del_orig else "📋 Keep originals"
            kbd = InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Proceed", callback_data=f"atr_proceed_{c_id}"), InlineKeyboardButton("⏸️ Pause", callback_data=f"atr_pause_{c_id}"), InlineKeyboardButton("🛑 Stop", callback_data=f"atr_stop_{c_id}")]])
            await bot_app.send_message(chat_id=int(dashboard_chat_id), text=f"🚀 **Auto-Transfer Recovery**\n\nGroup: **{title}** (`{c_id}`)\nMode: {mode}\nCurrent Status: **{st}**", reply_markup=kbd, parse_mode=ParseMode.MARKDOWN)

    rows = await db_execute("SELECT job_id, playlist_id, chat_id, msg_chat_id, msg_id, album_id, album_name, query, idx, is_pilot, status, file_path, caption, tier FROM jobs WHERE status NOT IN ('done', 'cancelled')", fetch="all")
    recovered = 0
    if rows:
        for row in rows:
            job = {'job_id': row[0], 'playlist_id': row[1], 'chat_id': row[2], 'msg_chat_id': row[3], 'msg_id': row[4], 'album_id': row[5], 'album_name': row[6], 'query': row[7], 'idx': row[8], 'is_pilot': bool(row[9]), 'file_path': row[11], 'caption': row[12], 'tier': row[13] if len(row) > 13 else 1}
            target_q = stream_queue_t1 if job['tier'] == 1 else stream_queue_t2

            await update_job_status(job['job_id'], "waiting")
            if job['is_pilot']:
                await update_job_status(job['job_id'], "queued")
                await target_q.put(job)
            elif job['playlist_id']:
                pl_row = await get_playlist(job['playlist_id'])
                if pl_row and pl_row[5] in ("RUNNING", "PILOT_RUNNING"): enqueue_playlist_job(job['playlist_id'], job)
            else:
                await update_job_status(job['job_id'], "queued")
                await target_q.put(job)
            recovered += 1
        console.print(f"[bold yellow]♻️ Recovered {recovered} jobs into streaming queue.[/bold yellow]")

    active_playlists = await db_execute("SELECT DISTINCT album_id FROM playlists WHERE status NOT IN ('KILLED','COMPLETED')", fetch="all")
    for (album_id,) in (active_playlists or []):
        if album_id: await refresh_vk_cache(album_id)

    asyncio.create_task(bulk_progress_updater())
    asyncio.create_task(connection_watchdog())
    asyncio.create_task(dashboard_updater())
    asyncio.create_task(scheduler_loop())
    
    for i in range(STREAM_WORKERS): asyncio.create_task(stream_worker(i))

    with Live(progress_ui, console=console, refresh_per_second=4):
        console.print("[bold green]🚀 Master In-Memory Stream Engine Online. Bot menu ready![/bold green]")
        await asyncio.Event().wait()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())