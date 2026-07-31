import os
import re
import io
import time
import math
import shutil
import sqlite3
import asyncio
import subprocess
import requests
import vk_api
from collections import deque
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
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
DL_WORKERS = 2
UP_WORKERS = 2
MAX_STAGED_FILES = 4
MIN_FREE_GB = 2.0
DB_PATH = "SysCache/vk_queue.db"
DOWNLOAD_DIR = "SysCache/vk_downloads"

CHUNK_SIZE = 1024 * 1024           # 1 MB: Telegram's max read block size
MEM_BUFFER_SIZE = 8 * 1024 * 1024  # 8 MB: RAM buffer before flushing to disk
ALIGNMENT = 1024 * 1024            # 1 MB: MTProto strict offset alignment

SCHEDULER_INFLIGHT_TARGET = DL_WORKERS * 2
SCHEDULER_TICK = 0.5

GLOBAL_MAX_CONCURRENT_SEGMENTS = 3
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
user_app = Client("user_session", api_id=config.API_ID, api_hash=config.API_HASH, max_concurrent_transmissions=12, workers=10)
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

    # ALWAYS MONITORS (/MonitorAlways)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS always_monitors (
            chat_id INTEGER PRIMARY KEY,
            chat_title TEXT,
            status TEXT,
            last_msg_id INTEGER DEFAULT 0,
            added_at REAL
        )
    """)

    # SELECTIVE MONITORS (/MonitorSelected)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS selected_monitors (
            chat_id INTEGER PRIMARY KEY,
            chat_title TEXT,
            status TEXT,
            last_msg_id INTEGER DEFAULT 0,
            added_at REAL
        )
    """)

    # GLOBAL SELECTIVE TAGS
    conn.execute("""
        CREATE TABLE IF NOT EXISTS selected_tags (
            tag TEXT PRIMARY KEY,
            added_at REAL
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
        """INSERT INTO jobs (job_id, playlist_id, chat_id, msg_chat_id, msg_id, album_id, album_name, query, idx, is_pilot, status, file_path, caption, updated_at, tier)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(job_id) DO UPDATE SET status=excluded.status, file_path=excluded.file_path,
               is_pilot=excluded.is_pilot, updated_at=excluded.updated_at, tier=excluded.tier""",
        (job['job_id'], job.get('playlist_id'), job['chat_id'], job['msg_chat_id'], job['msg_id'],
         job['album_id'], job['album_name'], job['query'], job['idx'], int(job.get('is_pilot', False)),
         job['status'], job.get('file_path'), job.get('caption', ''), time.time(), job.get('tier', 1))
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
    row = await db_execute(
        """SELECT status FROM jobs 
           WHERE (msg_id=? OR job_id=?) AND status IN ('done', 'downloading', 'uploading', 'queued')""", 
        (msg_id, f"{msg_chat_id}_{msg_id}"), 
        fetch="one"
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
    return await db_execute(
        "SELECT playlist_id, chat_id, query, album_name, album_id, status, total, completed, failed, skipped_dupes FROM playlists WHERE playlist_id=?",
        (playlist_id,), fetch="one"
    )

async def list_playlists(limit=15):
    return await db_execute(
        "SELECT playlist_id, album_name, status, total, completed, failed, skipped_dupes FROM playlists WHERE status != 'KILLED' ORDER BY updated_at DESC LIMIT ?",
        (limit,), fetch="all"
    )

async def sync_vk_to_local_db():
    """Fetches all albums/videos, purges unavailable/failed VK videos, 
    seeds SQLite with 'done' records, and purges duplicate [TG_ID] videos."""
    console.print("[bold cyan]🔄 Syncing state from VK and cleaning duplicate/broken uploads...[/bold cyan]")
    synced_count = 0
    deleted_duplicates_count = 0
    deleted_broken_count = 0
    
    # CHANGED: Now a dictionary mapping msg_id -> video_id
    seen_msg_ids = {} 

    try:
        albums_resp = await asyncio.to_thread(vk.video.getAlbums, owner_id=my_vk_id, count=100)
        albums = albums_resp.get('items', [])
        album_ids = [alb['id'] for alb in albums if alb.get('id')]
        
        if None not in album_ids and 0 not in album_ids:
            album_ids.append(None)

        for album_id in album_ids:
            offset = 0
            count = 200
            while True:
                try:
                    kwargs = {'owner_id': my_vk_id, 'count': count, 'offset': offset}
                    if album_id is not None and album_id > 0:
                        kwargs['album_id'] = album_id
                    items = await asyncio.to_thread(vk.video.get, **kwargs)
                except Exception:
                    break

                video_list = items.get('items', [])
                if not video_list:
                    break

                for v in video_list:
                    title = v.get('title', '')
                    video_id = v.get('id')
                    duration = v.get('duration', 0)
                    is_processing = v.get('processing', 0)

                    # 1. PURGE UNAVAILABLE/FAILED VIDEOS
                    if duration == 0 and not is_processing:
                        try:
                            await asyncio.to_thread(vk.video.delete, owner_id=my_vk_id, video_id=video_id)
                            deleted_broken_count += 1
                            console.print(f"[bold red]🗑️ Purged broken/unavailable VK video: {title} (ID: {video_id})[/bold red]")
                        except Exception:
                            pass
                        continue

                    # 2. PURGE TG_ID DUPLICATES
                    match = re.search(r'\[TG_(\d+)\]', title)
                    if match:
                        msg_id = int(match.group(1))
                        
                        if msg_id in seen_msg_ids:
                            # NEW LOGIC: Only delete if it's an actual different video file on VK
                            if seen_msg_ids[msg_id] != video_id:
                                try:
                                    await asyncio.to_thread(vk.video.delete, owner_id=my_vk_id, video_id=video_id)
                                    deleted_duplicates_count += 1
                                    console.print(f"[bold yellow]🗑️ Purged duplicate VK video ID {video_id} (TG_{msg_id})[/bold yellow]")
                                except Exception:
                                    pass
                            continue
                        
                        seen_msg_ids[msg_id] = video_id
                        job_id = f"vk_recovered_{msg_id}"
                        
                        await db_execute(
                            """INSERT INTO jobs (job_id, chat_id, msg_chat_id, msg_id, album_id, album_name, query, idx, status, updated_at, tier)
                               VALUES (?,0,0,?,?,?,'vk_sync',1,'done',?,1)
                               ON CONFLICT(job_id) DO UPDATE SET status='done'""",
                            (job_id, msg_id, album_id if album_id else 0, title.split(' - ')[0], time.time())
                        )
                        synced_count += 1

                if len(video_list) < count:
                    break
                offset += count

        # Sync monitored_messages flag with actual live VK videos
        if seen_msg_ids:
            placeholders = ','.join('?' for _ in seen_msg_ids.keys())
            await db_execute(f"UPDATE monitored_messages SET is_queued=1 WHERE msg_id IN ({placeholders})", tuple(seen_msg_ids.keys()))

        console.print(f"[bold green]✅ VK Sync Complete: {synced_count} indexed, {deleted_duplicates_count} dupes purged, {deleted_broken_count} broken videos purged.[/bold green]")
        return synced_count, deleted_duplicates_count, deleted_broken_count
    except Exception as e:
        console.print(f"[bold red]⚠️ VK Sync failed on boot: {e}[/bold red]")
        return 0, 0, 0

# ============================================================
# GLOBAL STATE & UI CONTROL
# ============================================================
download_queue_t1 = asyncio.Queue()
download_queue_t2 = asyncio.Queue()
upload_queue = asyncio.Queue(maxsize=MAX_STAGED_FILES)

active_jobs = {}
cancelled_jobs = set()
user_states = {}
ui_state = "MAIN"
monitor_page = 0

playlist_queues: dict[str, deque] = {}
playlist_order: deque = deque()
vk_video_title_cache: dict[int, set] = {}
vk_album_name_cache: dict[str, int] = {}

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

def extract_all_tags(text):
    if not text:
        return []
    return [t.lower() for t in re.findall(r"#([A-Za-z0-9_]+)", text)]

def extract_first_tag(text):
    tags = extract_all_tags(text)
    return tags[0] if tags else None

async def refresh_vk_cache(album_id):
    if not album_id or album_id <= 0:
        return set()
    for attempt in range(3):
        try:
            items = await asyncio.to_thread(vk.video.get, owner_id=my_vk_id, album_id=album_id, count=200)
            titles = {v.get('title', '') for v in items.get('items', [])}
            vk_video_title_cache[album_id] = titles
            return titles
        except Exception as e:
            await asyncio.sleep(2 * (attempt + 1))
    return set()

async def vk_title_exists(album_id, title):
    if not album_id or album_id <= 0:
        return False
    if album_id not in vk_video_title_cache:
        await refresh_vk_cache(album_id)
    return title in vk_video_title_cache.get(album_id, set())

async def get_or_create_vk_album(album_name):
    if not album_name:
        return None
    norm_name = album_name.strip().lower()
    if norm_name in vk_album_name_cache:
        return vk_album_name_cache[norm_name]

    for attempt in range(3):
        try:
            existing = await asyncio.to_thread(vk.video.getAlbums, owner_id=my_vk_id, count=100)
            for alb in existing.get('items', []):
                title = alb.get('title', '')
                alb_id = alb.get('id')
                if alb_id:
                    vk_album_name_cache[title.strip().lower()] = alb_id
                    if title.strip().lower() == norm_name:
                        return alb_id

            new_alb = await asyncio.to_thread(vk.video.addAlbum, title=album_name)
            alb_id = new_alb if isinstance(new_alb, int) else new_alb.get('album_id')
            if alb_id:
                vk_album_name_cache[norm_name] = alb_id
                return alb_id
        except Exception as e:
            console.print(f"[bold red]❌ Failed to resolve/create VK album '{album_name}': {e} (Attempt {attempt+1}/3)[/bold red]")
            await asyncio.sleep(2 * (attempt + 1))
    return None

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
    
    try:
        progress_ui.update(rich_task_id, completed=percent)
    except KeyError:
        pass

# ============================================================
# MASTER CAPTION BUNDLE PARSER LOGIC
# ============================================================
def parse_master_caption_bundle(album_msgs, target_tags):
    master_caption = ""
    for am in album_msgs:
        if am.caption and am.caption.strip().startswith("#"):
            master_caption = am.caption
            break

    target_tags_lower = [t.lower().replace("#", "") for t in target_tags]
    matched_results = []

    if master_caption:
        lines = [line.strip() for line in master_caption.strip().split('\n') if line.strip()]
        
        top_matched_tag = None
        if lines:
            first_line_tags = extract_all_tags(lines[0])
            for flt in first_line_tags:
                if flt in target_tags_lower:
                    top_matched_tag = flt
                    break

        track_data = {}
        for line in lines[1:]:
            match = re.match(r'^(\d+)\s*-\s*(.*)', line)
            if match:
                idx_str, rest_of_line = match.groups()
                line_tags = extract_all_tags(line)
                
                line_matched_tag = None
                for lt in line_tags:
                    if lt in target_tags_lower:
                        line_matched_tag = lt
                        break

                effective_tag = line_matched_tag or top_matched_tag
                if effective_tag:
                    bracket_match = re.search(r"\(([^)]*)\)", rest_of_line)
                    track_caption = bracket_match.group(1).strip() if bracket_match else rest_of_line.strip()
                    track_data[int(idx_str)] = (effective_tag, track_caption)

        for i, am in enumerate(album_msgs, start=1):
            if not am.video and not am.document:
                continue
            if i in track_data:
                tag, cap = track_data[i]
                am._custom_album = tag
                am._custom_caption = cap
                am._relative_idx = i
                matched_results.append(am)

    else:
        for i, am in enumerate(album_msgs, start=1):
            if not am.video and not am.document:
                continue
            txt = am.caption or am.text or ""
            msg_tags = extract_all_tags(txt)
            matched_tag = next((t for t in msg_tags if t in target_tags_lower), None)
            if matched_tag:
                am._custom_album = matched_tag
                am._custom_caption = txt or f"Imported (#{matched_tag})"
                am._relative_idx = i
                matched_results.append(am)

    return matched_results

# ============================================================
# SELECTIVE HISTORY SCANNER & VK DUPE FILTER
# ============================================================
async def run_selective_history_scan(chat_id):
    """Scans history of chat_id for all registered selected_tags and compares against VK."""
    tags_rows = await db_execute("SELECT tag FROM selected_tags", fetch="all")
    if not tags_rows:
        return 0, 0, []

    target_tags = [r[0] for r in tags_rows]
    valid_jobs = []
    already_in_vk_cnt = 0
    total_found_cnt = 0

    processed_media_groups = set()

    async for msg in user_app.get_chat_history(chat_id, limit=500):
        if not (msg.video or msg.document):
            continue

        album_msgs = [msg]
        if msg.media_group_id:
            if msg.media_group_id in processed_media_groups:
                continue
            processed_media_groups.add(msg.media_group_id)
            try:
                album_msgs = await user_app.get_media_group(chat_id, msg.id)
                album_msgs = sorted(album_msgs, key=lambda x: x.id)
            except Exception:
                album_msgs = [msg]

        matched_msgs = parse_master_caption_bundle(album_msgs, target_tags)
        total_found_cnt += len(matched_msgs)

        for m in matched_msgs:
            tag_clean = getattr(m, '_custom_album', None)
            if not tag_clean:
                continue

            album_id = await get_or_create_vk_album(tag_clean)
            if not album_id:
                continue

            idx = getattr(m, '_relative_idx', 1)
            cap = getattr(m, '_custom_caption', "")
            title = display_title(tag_clean, idx, cap, m.id)

            if await vk_title_exists(album_id, title) or await is_msg_in_db(chat_id, m.id):
                already_in_vk_cnt += 1
            else:
                job_id = f"{chat_id}_{m.id}"
                job = {
                    'job_id': job_id, 'playlist_id': None, 'chat_id': chat_id,
                    'msg_chat_id': chat_id, 'msg_id': m.id, 'album_id': album_id,
                    'album_name': tag_clean, 'query': f"#{tag_clean}", 'idx': idx,
                    'is_pilot': False, 'status': 'queued', 'file_path': None, 'caption': cap, 'tier': 2
                }
                valid_jobs.append(job)

    return total_found_cnt, already_in_vk_cnt, valid_jobs

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

async def scan_chat_history(chat_identifier, resolved_chat_id):
    console.print(f"[bold cyan]🔍 Scanning historical messages for {chat_identifier} ({resolved_chat_id})...[/bold cyan]")
    try:
        cid_str = str(chat_identifier).strip()
        if cid_str.lstrip('-').isdigit():
            chat_obj = await user_app.get_chat(int(cid_str))
        else:
            chat_obj = await user_app.get_chat(cid_str)

        resolved_chat_id = chat_obj.id

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
    txt = message.caption or message.text or ""
    f_id = message.video.file_unique_id if message.video else (message.document.file_unique_id if message.document else "")

    row = await db_execute("SELECT chat_identifier FROM monitored_chats WHERE resolved_id=?", (chat_id,), fetch="one")
    if row:
        await extract_and_store_message(chat_id, message.id, txt, f_id)

    always_row = await db_execute("SELECT status, chat_title FROM always_monitors WHERE chat_id=?", (chat_id,), fetch="one")
    if always_row and always_row[0] == "ACTIVE":
        tag_clean = extract_first_tag(txt)
        if tag_clean:
            album_id = await get_or_create_vk_album(tag_clean)
            if album_id:
                title = display_title(tag_clean, 1, txt, message.id)
                if not await vk_title_exists(album_id, title) and not await is_msg_in_db(chat_id, message.id):
                    job_id = f"{chat_id}_{message.id}"
                    job = {
                        'job_id': job_id, 'playlist_id': None, 'chat_id': chat_id,
                        'msg_chat_id': chat_id, 'msg_id': message.id, 'album_id': album_id,
                        'album_name': tag_clean, 'query': f"#{tag_clean}", 'idx': 1,
                        'is_pilot': False, 'status': 'queued', 'file_path': None, 'caption': txt, 'tier': 2
                    }
                    await save_job(job)
                    await db_execute("UPDATE always_monitors SET last_msg_id=? WHERE chat_id=?", (message.id, chat_id))
                    await download_queue_t2.put(job)

    selected_row = await db_execute("SELECT status, chat_title FROM selected_monitors WHERE chat_id=?", (chat_id,), fetch="one")
    if selected_row and selected_row[0] == "ACTIVE":
        registered_tags_rows = await db_execute("SELECT tag FROM selected_tags", fetch="all")
        target_tags = [r[0] for r in registered_tags_rows] if registered_tags_rows else []
        
        if target_tags:
            album_msgs = [message]
            if message.media_group_id:
                try:
                    album_msgs = await user_app.get_media_group(chat_id, message.id)
                    album_msgs = sorted(album_msgs, key=lambda x: x.id)
                except Exception:
                    album_msgs = [message]

            matched_msgs = parse_master_caption_bundle(album_msgs, target_tags)
            for m in matched_msgs:
                tag_clean = getattr(m, '_custom_album', None)
                if not tag_clean: continue
                
                album_id = await get_or_create_vk_album(tag_clean)
                if not album_id: continue
                
                idx = getattr(m, '_relative_idx', 1)
                cap = getattr(m, '_custom_caption', "")
                title = display_title(tag_clean, idx, cap, m.id)
                
                if not await vk_title_exists(album_id, title) and not await is_msg_in_db(chat_id, m.id):
                    job_id = f"{chat_id}_{m.id}"
                    job = {
                        'job_id': job_id, 'playlist_id': None, 'chat_id': chat_id,
                        'msg_chat_id': chat_id, 'msg_id': m.id, 'album_id': album_id,
                        'album_name': tag_clean, 'query': f"#{tag_clean}", 'idx': idx,
                        'is_pilot': False, 'status': 'queued', 'file_path': None, 'caption': cap, 'tier': 2
                    }
                    await save_job(job)
                    await db_execute("UPDATE selected_monitors SET last_msg_id=? WHERE chat_id=?", (m.id, chat_id))
                    await download_queue_t2.put(job)

async def add_monitored_target(target_raw):
    target_raw = target_raw.strip()
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
# HIGH-SPEED PARALLEL DOWNLOAD ENGINE
# ============================================================
def get_part_count(file_size):
    mb = file_size / (1024 * 1024)
    if mb < 200: return 1
    elif mb < 1024: return 2
    elif mb < 3072: return 3
    else: return 4

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

async def _download_segment(client, file_id, chunk_offset, chunk_limit, part_file, job_id, tracker):
    async with global_segment_semaphore:
        retries = 5
        while retries > 0:
            downloaded_this_attempt = 0
            try:
                buffer = bytearray()
                with open(part_file, "wb") as f:
                    # Pass file_id instead of message object to prevent Pyrogram re-fetches
                    async for chunk in client.stream_media(file_id, limit=chunk_limit, offset=chunk_offset):
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
                console.print(f"[yellow]⚠️ Network dip ({e.__class__.__name__}), retrying in 5s... ({retries} left)[/yellow]")
                await asyncio.sleep(5) # Wait longer for Telegram DC to forgive the IP

async def async_fast_download(client, message, file_path, progress_callback, job_id):
    media = message.video or message.document
    if not media:
        raise Exception("No valid media found in message.")
        
    file_size = media.file_size
    file_id = media.file_id  # Extract raw file_id
    
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

    tasks = []
    for i, (chunk_offset, chunk_limit) in enumerate(ranges):
        if chunk_limit == 0: 
            continue
        tasks.append(
            asyncio.create_task(
                _download_segment(
                    client=client,
                    file_id=file_id,
                    chunk_offset=chunk_offset,
                    chunk_limit=chunk_limit,
                    part_file=part_files[i],
                    job_id=job_id,
                    tracker=tracker
                )
            )
        )
        # Increase stagger to 1.5s so Telegram doesn't flag multiple instant parallel requests
        await asyncio.sleep(1.5)  

    try:
        await asyncio.gather(*tasks)
    except Exception as e:
        for task in tasks:
            if not task.done():
                task.cancel()
        raise e

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
# BUFFERED UPLOAD READER & VALIDATION
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

def _sync_vk_upload(upload_url, file_path, progress_callback, job_id):
    reader = ProgressFileReader(file_path, progress_callback)
    try:
        files = {'video_file': (os.path.basename(file_path), reader, 'video/mp4')}
        resp = requests.post(upload_url, files=files, timeout=None)
        if resp.status_code != 200:
            raise Exception(f"VK upload {resp.status_code}: {resp.text[:300]}")
        return resp.json()
    finally:
        reader.close()

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

async def _ensure_h264_aac(file_path):
    """Probes the video and audio codecs. Converts to H.264/AAC if necessary."""
    def _probe_stream(stream_type):
        try:
            res = subprocess.run(
                ["ffprobe", "-v", "error", "-select_streams", f"{stream_type}:0", 
                 "-show_entries", "stream=codec_name", "-of", "default=noprint_wrappers=1:nokey=1", file_path],
                capture_output=True, text=True, timeout=10
            )
            return res.stdout.strip().lower()
        except Exception:
            return ""

    v_codec = await asyncio.to_thread(_probe_stream, "v")
    a_codec = await asyncio.to_thread(_probe_stream, "a")

    if v_codec == "h264" and (a_codec == "aac" or not a_codec):
        return file_path

    console.print(f"[bold yellow]⚙️ Converting {os.path.basename(file_path)} (Video: {v_codec or 'none'}, Audio: {a_codec or 'none'}) to H.264/AAC...[/bold yellow]")
    
    out_path = f"{file_path}.converted.mp4"
    
    conv_cmd = [
        "ffmpeg", "-y", "-i", file_path,
        "-c:v", "libx264", "-preset", "fast", "-crf", "23",
        "-c:a", "aac", "-b:a", "128k",
        "-movflags", "+faststart",
        out_path
    ]

    process = await asyncio.create_subprocess_exec(
        *conv_cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    await process.wait()

    if process.returncode == 0 and os.path.exists(out_path):
        os.remove(file_path)
        os.rename(out_path, file_path)
        return file_path
    else:
        if os.path.exists(out_path):
            os.remove(out_path)
        raise Exception(f"FFmpeg conversion failed with code {process.returncode}")

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
        """SELECT job_id, chat_id, msg_chat_id, msg_id, album_id, album_name, query, idx, caption, tier
           FROM jobs WHERE playlist_id=? AND status='waiting'""",
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
# 2-TIER SCHEDULER
# ============================================================
async def scheduler_loop():
    while True:
        await asyncio.sleep(SCHEDULER_TICK)
        if engine_state != ENGINE_RUNNING:
            continue
        
        pending_t1 = download_queue_t1.qsize()
        pending_t2 = download_queue_t2.qsize()
        if (pending_t1 + pending_t2) >= SCHEDULER_INFLIGHT_TARGET:
            continue

        attempts = len(playlist_order)
        pushed_t1 = False
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
            await download_queue_t1.put(job)
            pushed_t1 = True
            break

        if pushed_t1:
            continue

# ============================================================
# WORKERS
# ============================================================
async def download_worker(worker_id):
    while True:
        await pause_event.wait()
        
        try:
            job = download_queue_t1.get_nowait()
            active_q = download_queue_t1
        except asyncio.QueueEmpty:
            try:
                job = download_queue_t2.get_nowait()
                active_q = download_queue_t2
            except asyncio.QueueEmpty:
                await asyncio.sleep(1)
                continue

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

            msg = None
            for attempt in range(5):
                try:
                    msg = await user_app.get_messages(job['msg_chat_id'], job['msg_id'])
                    break
                except Exception as e:
                    console.print(f"[yellow]⚠️ Failed to fetch Telegram msg ({e}), retrying {attempt+1}/5...[/yellow]")
                    await asyncio.sleep(3)

            if not msg:
                raise Exception("FailedToFetchMessage")

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
            active_q.task_done()

async def upload_worker(worker_id):
    while True:
        await pause_event.wait()
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
                if job.get('tier', 1) == 1:
                    if job.get('playlist_id'):
                        enqueue_playlist_job(job['playlist_id'], job)
                    else:
                        await download_queue_t1.put(job)
                else:
                    await download_queue_t2.put(job)
                continue

            if not await _validate_video_file(file_path):
                console.print(f"[bold red]⚠️ Corrupt file detected before upload, requeueing for redownload: {file_path}[/bold red]")
                try: os.remove(file_path)
                except: pass
                job['file_path'] = None
                await update_job_status(job_id, "waiting", file_path="")
                if job.get('tier', 1) == 1:
                    if job.get('playlist_id'):
                        enqueue_playlist_job(job['playlist_id'], job)
                    else:
                        await download_queue_t1.put(job)
                else:
                    await download_queue_t2.put(job)
                continue

            try:
                file_path = await _ensure_h264_aac(file_path)
                job['file_path'] = file_path 
            except Exception as e:
                console.print(f"[bold red]❌ Transcoding failed for {display_name}: {e}[/bold red]")
                try: os.remove(file_path)
                except: pass
                job['file_path'] = None
                await update_job_status(job_id, "waiting", file_path="")
                await download_queue_t2.put(job) 
                continue

            # --- STRICT VK ALBUM VALIDATION ---
            album_id = job.get('album_id')
            album_name = job.get('album_name')
            if not album_id or album_id <= 0:
                if album_name:
                    console.print(f"[bold yellow]⚠️ Missing album ID for '{album_name}'. Attempting to resolve...[/bold yellow]")
                    album_id = await get_or_create_vk_album(album_name)
                    job['album_id'] = album_id

            if not album_id or album_id <= 0:
                raise Exception(f"Invalid VK Album ID ({album_id}) for job {job_id}. Halting upload to avoid general uploads.")

            active_jobs[up_key] = {"name": display_name, "action": "📤 UP", "progress": 0, "speed": "0 MB/s", "eta": "Calc...", "start_time": time.time(), "job_id": job_id}
            rich_task = progress_ui.add_task(f"[magenta]📤 UP {display_name}", total=100, name=display_name)
            await update_job_status(job_id, "uploading")

            title = display_title(job['album_name'], job['idx'], job.get('caption', ''), job['msg_id'])
            
            upload_info = None
            for attempt in range(5):
                try:
                    upload_info = await asyncio.to_thread(
                        vk.video.save, 
                        name=title, 
                        description=job.get('caption', ''), 
                        album_id=album_id
                    )
                    break
                except Exception as e:
                    console.print(f"[yellow]⚠️ VK save API call failed ({e}), retrying {attempt+1}/5...[/yellow]")
                    await asyncio.sleep(4)

            if not upload_info:
                raise Exception("VKVideoSaveFailed")

            def up_progress(current, total):
                if job_id in cancelled_jobs: raise Exception("ForceAbort")
                update_metrics(up_key, rich_task, "📤 UP", current, total)

            await asyncio.to_thread(_sync_vk_upload, upload_info['upload_url'], file_path, up_progress, job_id)

            vk_video_title_cache.setdefault(album_id, set()).add(title)
            await update_job_status(job_id, "done") 
            delete_file_on_exit = True
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
    queued_dl = download_queue_t1.qsize() + download_queue_t2.qsize() + scheduled_pending
    staged_up = upload_queue.qsize()
    active_dls = [j for j in active_jobs.values() if j['action'] == "📥 DL"]
    active_ups = [j for j in active_jobs.values() if j['action'] == "📤 UP"]

    text = ""
    buttons = []

    if ui_state == "MAIN":
        monitored_chat_count = await db_execute("SELECT COUNT(*) FROM monitored_chats", fetch="one")
        m_count = monitored_chat_count[0] if monitored_chat_count else 0

        always_count = await db_execute("SELECT COUNT(*) FROM always_monitors WHERE status='ACTIVE'", fetch="one")
        a_count = always_count[0] if always_count else 0

        selected_count = await db_execute("SELECT COUNT(*) FROM selected_monitors WHERE status='ACTIVE'", fetch="one")
        s_count = selected_count[0] if selected_count else 0

        tags_count = await db_execute("SELECT COUNT(*) FROM selected_tags", fetch="one")
        t_count = tags_count[0] if tags_count else 0

        text = (f"📊 **GLOBAL TRANSFER ENGINE**\n{_engine_banner()} | 💾 Free Disk: {free_space_gb():.1f} GB\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📥 **Downloading:** {len(active_dls)} Active | {queued_dl} Queued\n"
                f"📦 **Staged on Disk:** {staged_up} Files\n"
                f"📤 **Uploading:** {len(active_ups)} Active\n"
                f"👁️ **Monitored Chats:** {m_count} Active\n"
                f"📡 **Auto-Sync Groups:** {a_count} Active\n"
                f"🎯 **Selective Monitors:** {s_count} Active | {t_count} Tags\n"
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
            InlineKeyboardButton("🎯 Selective Sync", callback_data="ui_SELECTED_VIEW"),
            InlineKeyboardButton("📡 Auto-Sync Groups", callback_data="ui_ALWAYS_VIEW")
        ])
        buttons.append([
            InlineKeyboardButton("👁️ Monitor Findings", callback_data="ui_MONITOR_VIEW"),
            InlineKeyboardButton("📋 Playlists", callback_data="ui_PLAYLISTS")
        ])
        buttons.append([
            InlineKeyboardButton("▶️ Global Resume" if engine_state != ENGINE_RUNNING else "⏸️ Global Pause", callback_data="toggle_pause"),
            InlineKeyboardButton("🛑 Clear Queues", callback_data="clear_queue")
        ])

    elif ui_state == "SELECTED_VIEW":
        groups = await db_execute("SELECT chat_id, chat_title, status FROM selected_monitors", fetch="all")
        tags_rows = await db_execute("SELECT tag FROM selected_tags", fetch="all")
        tags_list = [r[0] for r in tags_rows] if tags_rows else []

        text = (f"🎯 **SELECTIVE MONITORS (/MonitorSelected)**\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🏷️ **Global Target Tags ({len(tags_list)}):**\n"
                f"{', '.join(tags_list) if tags_list else '_No tags set yet_'}\n\n"
                f"📱 **Monitored Groups ({len(groups or [])}):**\n")

        if not groups:
            text += "_No selective groups configured yet._\n"

        for c_id, c_title, st in (groups or []):
            pending_row = await db_execute("SELECT COUNT(*) FROM jobs WHERE chat_id=? AND status IN ('queued','waiting')", (c_id,), fetch="one")
            p_cnt = pending_row[0] if pending_row else 0
            text += f"• **{c_title}** (`{c_id}`)\n  Status: **{st}** | Pending Queue: `{p_cnt}` vids\n"
            
            buttons.append([
                InlineKeyboardButton(f"🔍 Scan History", callback_data=f"sel_scan_{c_id}"),
                InlineKeyboardButton(f"⏸️ Pause" if st == "ACTIVE" else f"▶️ Proceed", callback_data=f"sel_pause_{c_id}" if st == "ACTIVE" else f"sel_proceed_{c_id}"),
                InlineKeyboardButton(f"🛑 Stop", callback_data=f"sel_stop_{c_id}")
            ])

        buttons.append([
            InlineKeyboardButton("➕ Add Group(s)", callback_data="sel_add_group"),
            InlineKeyboardButton("🏷️ Add/Remove Tags", callback_data="sel_manage_tags")
        ])
        buttons.append([InlineKeyboardButton("🔙 Back to Main", callback_data="ui_MAIN")])

    elif ui_state == "ALWAYS_VIEW":
        rows = await db_execute("SELECT chat_id, chat_title, status FROM always_monitors", fetch="all")
        text = "📡 **AUTO-SYNC MONITORS (/MonitorAlways)**\n━━━━━━━━━━━━━━━━━━\n"
        if not rows:
            text += "No continuous auto-sync monitors configured yet.\n"
        
        for c_id, c_title, st in (rows or []):
            pending_row = await db_execute("SELECT COUNT(*) FROM jobs WHERE chat_id=? AND status IN ('queued','waiting')", (c_id,), fetch="one")
            p_cnt = pending_row[0] if pending_row else 0
            
            text += f"• **{c_title}** (`{c_id}`)\n  Status: **{st}** | Pending Queue: `{p_cnt}` vids\n\n"
            
            if st == "ACTIVE":
                buttons.append([
                    InlineKeyboardButton(f"⏸️ Pause {c_title[:15]}", callback_data=f"always_pause_{c_id}"),
                    InlineKeyboardButton(f"🛑 Stop Sync", callback_data=f"always_stop_{c_id}")
                ])
            else:
                buttons.append([
                    InlineKeyboardButton(f"▶️ Proceed {c_title[:15]}", callback_data=f"always_proceed_{c_id}"),
                    InlineKeyboardButton(f"🛑 Stop Sync", callback_data=f"always_stop_{c_id}")
                ])

        buttons.append([InlineKeyboardButton("🔙 Back to Main", callback_data="ui_MAIN")])

    elif ui_state == "MONITOR_VIEW":
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
                f"━━━━━━━━━━━━━━━━━━\n")

        for tag, cnt in (tags_data or []):
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

        meta_row = await db_execute("SELECT last_seen_count FROM monitored_tags_meta WHERE tag=?", (tag,), fetch="one")
        last_seen = meta_row[0] if meta_row else tg_total
        delta_new = max(0, tg_total - last_seen)

        album_name = tag.replace("#", "")
        album_id = await get_or_create_vk_album(album_name)
        
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
                f"━━━━━━━━━━━━━━━━━━\n")

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
    """Command to wipe stale local state and force a complete resync with VK."""
    global engine_state
    status_msg = await message.reply_text(
        "🔄 **Initiating System & VK Database Refresh...**\n`[1/5]` Pausing transfer engine...",
        parse_mode=ParseMode.MARKDOWN
    )

    prev_state = engine_state
    engine_state = ENGINE_PAUSE_REQUESTED
    pause_event.clear()

    await asyncio.sleep(1)

    await status_msg.edit_text(
        "🔄 **Initiating System & VK Database Refresh...**\n`[2/5]` Clearing local job cache & stale message status...",
        parse_mode=ParseMode.MARKDOWN
    )

    # 1. Clear memory queues
    while not download_queue_t1.empty():
        download_queue_t1.get_nowait()
        download_queue_t1.task_done()
    while not download_queue_t2.empty():
        download_queue_t2.get_nowait()
        download_queue_t2.task_done()
    while not upload_queue.empty():
        upload_queue.get_nowait()
        upload_queue.task_done()
    
    playlist_queues.clear()
    playlist_order.clear()
    cancelled_jobs.clear()
    vk_video_title_cache.clear()
    vk_album_name_cache.clear()

    # 2. Reset local tables
    await db_execute("DELETE FROM jobs WHERE status != 'done'")
    await db_execute("UPDATE monitored_messages SET is_queued=0")

    await status_msg.edit_text(
        "🔄 **Initiating System & VK Database Refresh...**\n`[3/5]` Querying live VK catalog & purging broken/duplicate videos...",
        parse_mode=ParseMode.MARKDOWN
    )

    # 3. Resync live from VK
    synced_cnt, dupes_cnt, broken_cnt = await sync_vk_to_local_db()

    await status_msg.edit_text(
        "🔄 **Initiating System & VK Database Refresh...**\n`[4/5]` Resynchronizing monitored tags with database...",
        parse_mode=ParseMode.MARKDOWN
    )

    await asyncio.sleep(1)

    # 4. Resume engine
    engine_state = ENGINE_RUNNING if prev_state == ENGINE_RUNNING else prev_state
    if engine_state == ENGINE_RUNNING:
        pause_event.set()

    await status_msg.edit_text(
        f"✅ **System & VK Sync Refresh Complete!**\n\n"
        f"• **Live VK Videos Indexed:** `{synced_cnt}`\n"
        f"• **Duplicates Purged from VK:** `{dupes_cnt}`\n"
        f"• **Broken Videos Cleaned:** `{broken_cnt}`\n"
        f"• **Pending Queues:** Reset & Ready\n\n"
        f"_You can now safely re-run history scans or commands._",
        parse_mode=ParseMode.MARKDOWN
    )
    await render_dashboard()

@bot_app.on_message(filters.command("monitor"))
async def monitor_cmd(client, message):
    user_states[message.chat.id] = {'awaiting_monitor_input': True}
    await message.reply_text(
        "👁️ **MONITORING CONFIGURATION**\n"
        "Send the Group IDs or usernames you want to monitor.\n"
        "*(Multiple allowed separated by commas, e.g., `-10012345678, @my_channel`)*",
        parse_mode=ParseMode.MARKDOWN
    )

@bot_app.on_message(filters.command("monitoralways"))
async def monitor_always_cmd(client, message):
    user_states[message.chat.id] = {'awaiting_always_input': True}
    await message.reply_text(
        "📡 **CONTINUOUS AUTO-SYNC CONFIGURATION**\n"
        "Send the Group ID or username to continuously mirror ALL tagged videos to VK.",
        parse_mode=ParseMode.MARKDOWN
    )

@bot_app.on_message(filters.command("monitorselected"))
async def monitor_selected_cmd(client, message):
    global ui_state
    ui_state = "SELECTED_VIEW"
    await render_dashboard()

@bot_app.on_message(filters.private & filters.text & ~filters.command(["start", "help", "refresh", "monitor", "monitoralways", "monitorselected"]))
async def handle_user_input(client, message):
    chat_id = message.chat.id
    state = user_states.get(chat_id, {})

    if state.get('awaiting_sel_groups'):
        user_states.pop(chat_id, None)
        raw_inputs = [x.strip() for x in message.text.split(",") if x.strip()]
        status_msg = await message.reply_text("⚙️ Resolving groups and running past message scan against VK...")

        for target in raw_inputs:
            try:
                if target.startswith("-100") or target.startswith("-") or target.isdigit():
                    chat_obj = await user_app.get_chat(int(target))
                else:
                    chat_obj = await user_app.get_chat(target)

                resolved_id = chat_obj.id
                title = chat_obj.title or target

                await db_execute(
                    "INSERT INTO selected_monitors (chat_id, chat_title, status, last_msg_id, added_at) VALUES (?,?,'ACTIVE',0,?) ON CONFLICT(chat_id) DO UPDATE SET chat_title=excluded.chat_title",
                    (resolved_id, title, time.time())
                )

                tot, in_vk, valid_jobs = await run_selective_history_scan(resolved_id)

                if valid_jobs:
                    state_key = f"pending_sel_jobs_{resolved_id}"
                    user_states[chat_id] = {state_key: valid_jobs}

                    kbd = InlineKeyboardMarkup([
                        [InlineKeyboardButton(f"🚀 Queue {len(valid_jobs)} New Videos", callback_data=f"sel_confirm_queue_{resolved_id}")],
                        [InlineKeyboardButton("❌ Skip / Back", callback_data="ui_SELECTED_VIEW")]
                    ])
                    await status_msg.reply_text(
                        f"🎯 **Selective Scan Preview: {title}**\n━━━━━━━━━━━━━━━━━━\n"
                        f"📱 **Total Matched in TG:** `{tot}` videos\n"
                        f"✅ **Already in VK / Skipped:** `{in_vk}` videos\n"
                        f"📥 **New Ready to Queue:** `{len(valid_jobs)}` videos",
                        reply_markup=kbd,
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    await status_msg.reply_text(
                        f"🎯 **Selective Scan Complete: {title}**\n━━━━━━━━━━━━━━━━━━\n"
                        f"Found `{tot}` matched videos, but all `{in_vk}` are already uploaded to VK!"
                    )

            except Exception as e:
                await message.reply_text(f"❌ Failed to process group `{target}`: {e}")

        global ui_state
        ui_state = "SELECTED_VIEW"
        await render_dashboard()
        return

    if state.get('awaiting_sel_tags'):
        user_states.pop(chat_id, None)
        raw_tags = [x.strip().lower().replace("#", "") for x in message.text.split(",") if x.strip()]
        
        for t in raw_tags:
            full_tag = f"#{t}"
            await db_execute("INSERT INTO selected_tags (tag, added_at) VALUES (?,?) ON CONFLICT(tag) DO NOTHING", (full_tag, time.time()))

        await message.reply_text(f"✅ Registered **{len(raw_tags)}** hashtag(s) globally! Use the '🔍 Scan History' button on any group to find past posts for these tags.")
        ui_state = "SELECTED_VIEW"
        await render_dashboard()
        return

    if state.get('awaiting_always_input'):
        user_states.pop(chat_id, None)
        target = message.text.strip()
        status_msg = await message.reply_text("⚙️ Connecting and scanning group history...")
        
        try:
            if target.startswith("-100") or target.startswith("-") or target.isdigit():
                chat_obj = await user_app.get_chat(int(target))
            else:
                chat_obj = await user_app.get_chat(target)

            resolved_id = chat_obj.id
            title = chat_obj.title or target

            await db_execute(
                "INSERT INTO always_monitors (chat_id, chat_title, status, last_msg_id, added_at) VALUES (?,?,'ACTIVE',0,?) ON CONFLICT(chat_id) DO UPDATE SET chat_title=excluded.chat_title",
                (resolved_id, title, time.time())
            )

            found_cnt = 0
            async for msg in user_app.get_chat_history(resolved_id, limit=300):
                if msg.video or msg.document:
                    txt = msg.caption or msg.text or ""
                    tag_clean = extract_first_tag(txt)
                    if tag_clean:
                        album_id = await get_or_create_vk_album(tag_clean)
                        if album_id:
                            job_id = f"{resolved_id}_{msg.id}"
                            if not await is_msg_in_db(resolved_id, msg.id):
                                job = {
                                    'job_id': job_id, 'playlist_id': None, 'chat_id': chat_id,
                                    'msg_chat_id': resolved_id, 'msg_id': msg.id, 'album_id': album_id,
                                    'album_name': tag_clean, 'query': f"#{tag_clean}", 'idx': 1,
                                    'is_pilot': False, 'status': 'queued', 'file_path': None, 'caption': txt, 'tier': 2
                                }
                                await save_job(job)
                                await download_queue_t2.put(job)
                                found_cnt += 1

            await status_msg.edit_text(f"✅ **Auto-Sync Active!**\nGroup: **{title}**\nFound `{found_cnt}` videos.")
        except Exception as e:
            await status_msg.edit_text(f"❌ Failed to register group: `{e}`")
        return

    if state.get('awaiting_monitor_input'):
        user_states.pop(chat_id, None)
        raw_inputs = message.text.split(",")
        status_msg = await message.reply_text("⚙️ Launching background history scanners...")
        
        success_count = 0
        for target in raw_inputs:
            target = target.strip()
            if not target: continue
            success, _ = await add_monitored_target(target)
            if success: success_count += 1

        await status_msg.edit_text(f"✅ Successfully registered **{success_count}** groups!", parse_mode=ParseMode.MARKDOWN)
        return

    if state.get('awaiting_tag_search'):
        user_states.pop(chat_id, None)
        query_tag = message.text.strip().lower()
        if not query_tag.startswith("#"):
            query_tag = f"#{query_tag}"
        
        exists = await db_execute("SELECT COUNT(*) FROM monitored_messages WHERE tag=?", (query_tag,), fetch="one")
        if not exists or exists[0] == 0:
            return await message.reply_text(f"❌ Tag `{query_tag}` not found in monitored database.")
        
        ui_state = f"MON_INSPECT_{query_tag}"
        await render_dashboard()
        return

    if not state.get('awaiting_group'):
        search_query = message.text.strip()
        search_query = search_query.lower() if search_query.startswith("#") else f"#{search_query.lower()}"
        
        user_states[chat_id] = {'query': search_query, 'awaiting_group': True}
        return await message.reply_text(f"🔍 Search: *{search_query}*\n📌 Send the **Group Chat ID**.", parse_mode=ParseMode.MARKDOWN)

    try:
        target_group_id = int(message.text.strip())
    except ValueError:
        return await message.reply_text("⚠️ Invalid numeric ID.")

    status_msg = await message.reply_text("🔎 Scanning media group...")
    raw_found, processed_groups = [], set()

    try:
        async for msg in user_app.search_messages(chat_id=target_group_id, query=state['query']):
            if msg.media_group_id:
                if msg.media_group_id not in processed_groups:
                    processed_groups.add(msg.media_group_id)
                    album_msgs = await user_app.get_media_group(target_group_id, msg.id)
                    album_msgs = sorted(album_msgs, key=lambda x: x.id)

                    matched = parse_master_caption_bundle(album_msgs, [state['query']])
                    raw_found.extend(matched)

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
    kbd = InlineKeyboardMarkup([[InlineKeyboardButton(f"🚀 Queue {len(new_msgs)} videos", callback_data="queue_transfer")]])
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
            state_key = f"pending_sel_jobs_{c_id}"
            user_states[chat_id] = {state_key: valid_jobs}

            kbd = InlineKeyboardMarkup([
                [InlineKeyboardButton(f"🚀 Queue {len(valid_jobs)} New Videos", callback_data=f"sel_confirm_queue_{c_id}")],
                [InlineKeyboardButton("❌ Skip / Back", callback_data="ui_SELECTED_VIEW")]
            ])
            await callback.message.reply_text(
                f"🎯 **History Scan Preview**\n━━━━━━━━━━━━━━━━━━\n"
                f"📱 **Total Matched in TG:** `{tot}` videos\n"
                f"✅ **Already in VK:** `{in_vk}` videos\n"
                f"📥 **New Ready to Queue:** `{len(valid_jobs)}` videos",
                reply_markup=kbd,
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await callback.message.reply_text(f"ℹ️ Found `{tot}` matching videos, but all `{in_vk}` are already present in VK!")
        return

    elif data.startswith("sel_confirm_queue_"):
        c_id = int(data.replace("sel_confirm_queue_", ""))
        state_key = f"pending_sel_jobs_{c_id}"
        st = user_states.get(chat_id, {})
        jobs_to_queue = st.get(state_key, [])

        if not jobs_to_queue:
            return await callback.answer("Session expired or no jobs pending.", show_alert=True)

        queued_cnt = 0
        for job in jobs_to_queue:
            await save_job(job)
            await download_queue_t2.put(job)
            queued_cnt += 1

        user_states.get(chat_id, {}).pop(state_key, None)
        await callback.answer(f"🚀 Queued {queued_cnt} videos for download!", show_alert=True)
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
            await render_dashboard()
            return

        album_name = tag.replace("#", "")
        album_id = await get_or_create_vk_album(album_name)
        if not album_id:
            await callback.message.reply_text("❌ Failed to resolve VK album.")
            return

        await refresh_vk_cache(album_id)

        valid_jobs_data = []
        for idx, (c_id, m_id, caption) in enumerate(rows, start=1):
            title = display_title(album_name, idx, caption, m_id)
            if await vk_title_exists(album_id, title):
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
                'is_pilot': is_pilot, 'status': 'waiting', 'file_path': None, 'caption': caption, 'tier': 1
            }
            await save_job(job)
            cancelled_jobs.discard(job_id)

            if is_pilot:
                await update_job_status(job_id, "queued")
                await download_queue_t1.put(job)

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
            engine_state = ENGINE_RUNNING
            pause_event.set()
            await set_control("engine_state", ENGINE_RUNNING)
            await callback.answer("▶️ Resumed")
        await render_dashboard()

    elif data == "clear_queue":
        cleared = 0
        while not download_queue_t1.empty():
            job = download_queue_t1.get_nowait()
            download_queue_t1.task_done()
            cancelled_jobs.add(job['job_id'])
            await delete_job_row(job['job_id'])
            cleared += 1
        while not download_queue_t2.empty():
            job = download_queue_t2.get_nowait()
            download_queue_t2.task_done()
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
        album_id = await get_or_create_vk_album(album_name)
        if not album_id:
            user_states.pop(chat_id, None)
            return await callback.message.edit_text("❌ Failed to resolve VK album.")

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
                'is_pilot': is_pilot, 'status': 'waiting', 'file_path': None, 'caption': caption, 'tier': 1
            }
            await save_job(job)
            cancelled_jobs.discard(job_id)

            if is_pilot:
                await update_job_status(job_id, "queued")
                await download_queue_t1.put(job)

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
        BotCommand("monitorselected", "🎯 Selective Sync (/MonitorSelected)"),
        BotCommand("monitoralways", "📡 Continuous Auto-Sync (/MonitorAlways)"),
        BotCommand("monitor", "👁️ Monitor Chat History")
    ])

    await sync_vk_to_local_db()

    console.print("[bold green]✅ BotFather command menu set.[/bold green]")

    saved_state = await get_control("engine_state", ENGINE_RUNNING)
    engine_state = saved_state if saved_state in (ENGINE_RUNNING, ENGINE_PAUSED) else ENGINE_RUNNING
    if engine_state != ENGINE_RUNNING:
        pause_event.clear()

    monitored_targets = await db_execute("SELECT chat_identifier, resolved_id FROM monitored_chats", fetch="all")
    if monitored_targets:
        for c_id, r_id in monitored_targets:
            asyncio.create_task(scan_chat_history(c_id, r_id))

    dashboard_chat_id = await get_control("dashboard_chat_id")
    if dashboard_chat_id:
        try:
            dash_msg = await bot_app.send_message(
                chat_id=int(dashboard_chat_id),
                text="⚙️ **System Online / Reboot Detected**\nBooting Master Dashboard...",
                parse_mode=ParseMode.MARKDOWN
            )
            try: await dash_msg.pin(both_sides=True)
            except: pass
            await set_control("dashboard_msg_id", dash_msg.id)
        except Exception as e:
            console.print(f"[bold red]Failed to auto-pin fresh dashboard on startup: {e}[/bold red]")

    always_groups = await db_execute("SELECT chat_id, chat_title, status, last_msg_id FROM always_monitors", fetch="all")
    if always_groups:
        for c_id, title, st, last_mid in always_groups:
            new_vids_cnt = 0
            async for msg in user_app.get_chat_history(c_id, limit=200):
                if msg.id <= last_mid:
                    break
                if msg.video or msg.document:
                    txt = msg.caption or msg.text or ""
                    tag_clean = extract_first_tag(txt)
                    if tag_clean:
                        album_id = await get_or_create_vk_album(tag_clean)
                        if album_id:
                            job_id = f"{c_id}_{msg.id}"
                            if not await is_msg_in_db(c_id, msg.id):
                                job = {
                                    'job_id': job_id, 'playlist_id': None, 'chat_id': dashboard_chat_id or c_id,
                                    'msg_chat_id': c_id, 'msg_id': msg.id, 'album_id': album_id,
                                    'album_name': tag_clean, 'query': f"#{tag_clean}", 'idx': 1,
                                    'is_pilot': False, 'status': 'queued', 'file_path': None, 'caption': txt, 'tier': 2
                                }
                                await save_job(job)
                                if st == "ACTIVE":
                                    await download_queue_t2.put(job)
                                new_vids_cnt += 1

            if dashboard_chat_id:
                kbd = InlineKeyboardMarkup([
                    [InlineKeyboardButton("▶️ Proceed", callback_data=f"always_proceed_{c_id}"),
                     InlineKeyboardButton("⏸️ Pause", callback_data=f"always_pause_{c_id}"),
                     InlineKeyboardButton("🛑 Stop", callback_data=f"always_stop_{c_id}")]
                ])
                await bot_app.send_message(
                    chat_id=int(dashboard_chat_id),
                    text=f"📡 **Continuous Auto-Sync Recovery**\n\nGroup: **{title}** (`{c_id}`)\nPending Files Discovered: `{new_vids_cnt}`\nCurrent Mode: **{st}**",
                    reply_markup=kbd,
                    parse_mode=ParseMode.MARKDOWN
                )

    selected_groups = await db_execute("SELECT chat_id, chat_title, status, last_msg_id FROM selected_monitors", fetch="all")
    tags_rows = await db_execute("SELECT tag FROM selected_tags", fetch="all")
    registered_tags = [r[0] for r in tags_rows] if tags_rows else []

    if selected_groups and registered_tags:
        for c_id, title, st, last_mid in selected_groups:
            new_vids_cnt = 0
            async for msg in user_app.get_chat_history(c_id, limit=200):
                if msg.id <= last_mid:
                    break
                if msg.video or msg.document:
                    txt = msg.caption or msg.text or ""
                    album_msgs = [msg]
                    if msg.media_group_id:
                        try:
                            album_msgs = await user_app.get_media_group(c_id, msg.id)
                            album_msgs = sorted(album_msgs, key=lambda x: x.id)
                        except Exception:
                            album_msgs = [msg]

                    matched_msgs = parse_master_caption_bundle(album_msgs, registered_tags)
                    for m in matched_msgs:
                        tag_clean = getattr(m, '_custom_album', None)
                        if not tag_clean: continue
                        album_id = await get_or_create_vk_album(tag_clean)
                        if not album_id: continue
                        
                        idx = getattr(m, '_relative_idx', 1)
                        cap = getattr(m, '_custom_caption', "")
                        job_id = f"{c_id}_{m.id}"

                        if not await is_msg_in_db(c_id, m.id):
                            job = {
                                'job_id': job_id, 'playlist_id': None, 'chat_id': dashboard_chat_id or c_id,
                                'msg_chat_id': c_id, 'msg_id': m.id, 'album_id': album_id,
                                'album_name': tag_clean, 'query': f"#{tag_clean}", 'idx': idx,
                                'is_pilot': False, 'status': 'queued', 'file_path': None, 'caption': cap, 'tier': 2
                            }
                            await save_job(job)
                            if st == "ACTIVE":
                                await download_queue_t2.put(job)
                            new_vids_cnt += 1

            if dashboard_chat_id:
                kbd = InlineKeyboardMarkup([
                    [InlineKeyboardButton("▶️ Proceed", callback_data=f"sel_proceed_{c_id}"),
                     InlineKeyboardButton("⏸️ Pause", callback_data=f"sel_pause_{c_id}"),
                     InlineKeyboardButton("🛑 Stop", callback_data=f"sel_stop_{c_id}")]
                ])
                await bot_app.send_message(
                    chat_id=int(dashboard_chat_id),
                    text=f"🎯 **Selective Auto-Sync Recovery**\n\nGroup: **{title}** (`{c_id}`)\nPending Files Discovered: `{new_vids_cnt}`\nCurrent Mode: **{st}**",
                    reply_markup=kbd,
                    parse_mode=ParseMode.MARKDOWN
                )

    rows = await db_execute(
        "SELECT job_id, playlist_id, chat_id, msg_chat_id, msg_id, album_id, album_name, query, idx, is_pilot, status, file_path, caption, tier FROM jobs",
        fetch="all"
    )
    recovered = 0
    if rows:
        for row in rows:
            job = {
                'job_id': row[0], 'playlist_id': row[1], 'chat_id': row[2], 'msg_chat_id': row[3],
                'msg_id': row[4], 'album_id': row[5], 'album_name': row[6], 'query': row[7],
                'idx': row[8], 'is_pilot': bool(row[9]), 'file_path': row[11], 'caption': row[12],
                'tier': row[13] if len(row) > 13 else 1
            }
            status, file_path = row[10], row[11]

            target_q = download_queue_t1 if job['tier'] == 1 else download_queue_t2

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
                        await target_q.put(job)
                    elif job['playlist_id']:
                        enqueue_playlist_job(job['playlist_id'], job)
                    else:
                        await target_q.put(job)
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
                    await target_q.put(job)
                elif job['playlist_id']:
                    enqueue_playlist_job(job['playlist_id'], job)
                else:
                    await target_q.put(job)
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
        console.print("[bold green]🚀 Master Engine Online. Bot menu ready![/bold green]")
        await asyncio.Event().wait()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())