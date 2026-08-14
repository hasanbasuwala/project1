import os
import re
import io
import time
import math
import shutil
import sqlite3
import asyncio
import logging
import subprocess
import requests
import vk_api
from collections import deque
from pyrogram import Client, filters, enums
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BotCommand, ChatPrivileges

from rich.console import Console
from rich.live import Live
from rich.progress import (
    Progress, TextColumn, BarColumn, DownloadColumn,
    TransferSpeedColumn, TimeRemainingColumn
)

import config  # Ensure your config.py has API_ID, API_HASH, VK_BOT (token), and VK_TOKEN

# ============================================================
# CHAPTER 1: EDITABLE CONFIGURATIONS & UI STRINGS
# ============================================================

# --- Core Engine Limits ---
MAX_RELAY_WORKERS = 3           # Concurrent Zero-Disk stream jobs
DL_WORKERS = 2                  # Concurrent Disk Fallback download jobs
UP_WORKERS = 2                  # Concurrent Disk Fallback upload jobs
MAX_STAGED_FILES = 4            # Max files waiting on disk before pausing DLs
MIN_FREE_GB = 2.0               # Minimum disk space required for Fallback jobs

# --- Zero-Disk / Network Limits ---
CHUNK_SIZE = 1024 * 1024        # 1 MB chunk size for Telegram MTProto
QUEUE_MAX_CHUNKS = 8            # Max chunks held in RAM per active Zero-Disk job (~8MB)
SEGMENT_STAGGER_SECONDS = 1.2   # Delay between starting concurrent chunks

# --- Storage Paths ---
DB_PATH = "SysCache/vk_queue.db"
DOWNLOAD_DIR = "SysCache/vk_downloads"

# --- UI Text Strings (Edit these to change bot messages) ---
UI_STRINGS = {
    "dashboard_header": "📊 **GLOBAL TRANSFER ENGINE**",
    "vk_search": "🔍 VK Search: *{query}*\n📌 Send the **Group Chat ID** to fetch from.",
    "found_preview": "📊 **Found** `{query}`\n🆕 Total Found: *{new}* | ⏭️ Skipped from VK: *{skipped}*",
    "booting": "⚙️ Booting Master Dashboard...\n(Pinning message...)",
    "monitor_setup": "👁️ **MONITORING CONFIGURATION**\nSend the Group IDs or usernames to monitor.",
    "auto_sync_setup": "📡 **CONTINUOUS AUTO-SYNC CONFIGURATION**\nSend the Group ID/Username to mirror.",
    "refresh_step_1": "🔄 **Initiating System & VK Database Refresh...**\n`[1/5]` Pausing transfer engine...",
    "topic_set": "✅ **Master Forum ID successfully set to:** `{forum_id}`",
    "scanning_group": "🔎 Scanning media group...",
    "error_resolution": "❌ Couldn't resolve `{target}`: {error}",
    "zero_disk_active": "🚀 Zero-Disk Stream Active",
    "disk_fallback": "💾 Disk Fallback Active"
}

# ============================================================
# SYSTEM INITIALIZATION (Do not edit below this line)
# ============================================================
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Mute noisy Pyrogram logs to keep the console clean during Backpressure pauses
logging.getLogger("pyrogram.session.session").setLevel(logging.CRITICAL)
logging.getLogger("pyrogram.client").setLevel(logging.WARNING)

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
user_app = Client("p_session", api_id=config.API_ID, api_hash=config.API_HASH, max_concurrent_transmissions=5, workers=10)

# --- Global State Tracking ---
download_queue_t1 = asyncio.Queue()
download_queue_t2 = asyncio.Queue()
upload_queue = asyncio.Queue(maxsize=MAX_STAGED_FILES)
relay_queue = asyncio.Queue() # NEW: Dedicated queue for Zero-Disk jobs

active_jobs = {}
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

# ============================================================
# CHAPTER 2: DATABASE INITIALIZATION & STATE PERSISTENCE
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
            tier INTEGER DEFAULT 1,
            is_zero_disk INTEGER DEFAULT 0
        )
    """)
    for stmt in (
        "ALTER TABLE jobs ADD COLUMN caption TEXT",
        "ALTER TABLE jobs ADD COLUMN playlist_id TEXT",
        "ALTER TABLE jobs ADD COLUMN is_pilot INTEGER DEFAULT 0",
        "ALTER TABLE jobs ADD COLUMN tier INTEGER DEFAULT 1",
        "ALTER TABLE jobs ADD COLUMN is_zero_disk INTEGER DEFAULT 0",
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

    conn.execute("""
        CREATE TABLE IF NOT EXISTS always_monitors (
            chat_id INTEGER PRIMARY KEY,
            chat_title TEXT,
            status TEXT,
            last_msg_id INTEGER DEFAULT 0,
            added_at REAL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS selected_monitors (
            chat_id INTEGER PRIMARY KEY,
            chat_title TEXT,
            status TEXT,
            last_msg_id INTEGER DEFAULT 0,
            added_at REAL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS selected_tags (
            tag TEXT PRIMARY KEY,
            added_at REAL
        )
    """)

    conn.execute("CREATE TABLE IF NOT EXISTS control (key TEXT PRIMARY KEY, value TEXT)")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS tg_routing_destinations (
            tag TEXT PRIMARY KEY,
            topic_id INTEGER,
            topic_title TEXT,
            created_at REAL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS tg_copied_messages (
            chat_id INTEGER,
            msg_id INTEGER,
            tag TEXT,
            dest_topic_id INTEGER,
            copied_at REAL,
            PRIMARY KEY (chat_id, msg_id, dest_topic_id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS autotransfer_monitors (
            chat_id INTEGER PRIMARY KEY,
            chat_title TEXT,
            status TEXT,
            delete_originals INTEGER DEFAULT 0,
            owner_user_id INTEGER,
            last_msg_id INTEGER DEFAULT 0,
            added_at REAL,
            mode TEXT DEFAULT 'ALL',
            tags TEXT DEFAULT ''
        )
    """)

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
        """INSERT INTO jobs (job_id, playlist_id, chat_id, msg_chat_id, msg_id, album_id, album_name, query, idx, is_pilot, status, file_path, caption, updated_at, tier, is_zero_disk)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(job_id) DO UPDATE SET status=excluded.status, file_path=excluded.file_path,
               is_pilot=excluded.is_pilot, updated_at=excluded.updated_at, tier=excluded.tier, is_zero_disk=excluded.is_zero_disk""",
        (job['job_id'], job.get('playlist_id'), job['chat_id'], job['msg_chat_id'], job['msg_id'],
         job['album_id'], job['album_name'], job['query'], job['idx'], int(job.get('is_pilot', False)),
         job['status'], job.get('file_path'), job.get('caption', ''), time.time(), job.get('tier', 1), int(job.get('is_zero_disk', False)))
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
           WHERE (msg_id=? OR job_id=?) AND status IN ('done', 'downloading', 'uploading', 'relaying', 'queued')""", 
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
    console.print("[bold cyan]🔄 Syncing state from VK and cleaning duplicate/broken uploads...[/bold cyan]")
    synced_count = 0
    deleted_duplicates_count = 0
    deleted_broken_count = 0
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

                    if duration == 0 and not is_processing:
                        try:
                            await asyncio.to_thread(vk.video.delete, owner_id=my_vk_id, video_id=video_id)
                            deleted_broken_count += 1
                            console.print(f"[bold red]🗑️ Purged broken/unavailable VK video: {title} (ID: {video_id})[/bold red]")
                        except Exception:
                            pass
                        continue

                    match = re.search(r'\[TG_(\d+)\]', title)
                    if match:
                        msg_id = int(match.group(1))
                        
                        if msg_id in seen_msg_ids:
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

        if seen_msg_ids:
            placeholders = ','.join('?' for _ in seen_msg_ids.keys())
            await db_execute(f"UPDATE monitored_messages SET is_queued=1 WHERE msg_id IN ({placeholders})", tuple(seen_msg_ids.keys()))

        console.print(f"[bold green]✅ VK Sync Complete: {synced_count} indexed, {deleted_duplicates_count} dupes purged, {deleted_broken_count} broken videos purged.[/bold green]")
        return synced_count, deleted_duplicates_count, deleted_broken_count
    except Exception as e:
        console.print(f"[bold red]⚠️ VK Sync failed on boot: {e}[/bold red]")
        return 0, 0, 0
        
# ============================================================
# CHAPTER 3: TELEGRAM FORUM ROUTING & DEDUPLICATION ENGINE
# ============================================================

from pyrogram.raw.functions.channels import CreateForumTopic

TRANSFER_STAGGER_SECONDS = 1.5
TRANSFER_MAX_RETRIES = 5

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

async def verify_vk_video_ready(owner_id, video_id):
    waited = 0
    VK_VERIFY_MAX_WAIT_SECONDS = 120
    VK_VERIFY_POLL_INTERVAL = 15
    while waited <= VK_VERIFY_MAX_WAIT_SECONDS:
        await asyncio.sleep(VK_VERIFY_POLL_INTERVAL)
        waited += VK_VERIFY_POLL_INTERVAL
        try:
            resp = await asyncio.to_thread(vk.video.get, owner_id=owner_id, videos=f"{owner_id}_{video_id}")
            items = resp.get('items', [])
            if not items:
                continue
            v = items[0]
            duration = v.get('duration', 0)
            processing = v.get('processing', 0)
            if duration and duration > 0:
                return True
            if not processing and duration == 0 and waited >= VK_VERIFY_POLL_INTERVAL * 2:
                return False
        except Exception as e:
            console.print(f"[yellow]⚠️ VK verify poll failed: {e}[/yellow]")
    return False

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
        all_master_tags = extract_all_tags(master_caption)
        top_matched_tags = [t for t in all_master_tags if t in target_tags_lower]

    track_data = {}
    if master_caption:
        lines = [line.strip() for line in master_caption.strip().split('\n') if line.strip()]
        for line in lines[1:]: 
            match = re.match(r'^(\d+)[\s\-\.\)]+(.*)', line)
            if match:
                idx_str, rest_of_line = match.groups()
                line_tags = extract_all_tags(line)
                line_matched_tags = [t for t in line_tags if t in target_tags_lower]
                
                effective_tags = line_matched_tags if line_matched_tags else top_matched_tags
                if effective_tags:
                    bracket_match = re.search(r"\(([^)]*)\)", rest_of_line)
                    track_caption = bracket_match.group(1).strip() if bracket_match else rest_of_line.strip()
                    track_data[int(idx_str)] = (effective_tags, track_caption)

    for i, am in enumerate(album_msgs, start=1):
        if not am.video and not am.document:
            continue
        
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
            msg_tags = extract_all_tags(txt)
            matched_tags = [t for t in msg_tags if t in target_tags_lower]
            if matched_tags:
                am._custom_album = matched_tags[0] 
                am._custom_albums = matched_tags   
                am._custom_caption = txt
                am._relative_idx = i
                am._decouple = False
                matched_results.append(am)

    return matched_results

async def tg_flood_safe(coro_fn, *args, **kwargs):
    attempts = 0
    while True:
        try:
            return await coro_fn(*args, **kwargs)
        except FloodWait as e:
            attempts += 1
            wait_s = int(getattr(e, "value", 5)) + 2
            console.print(f"[bold red]🚦 FloodWait ({wait_s}s) on TG Copy, pausing...[/bold red]")
            await asyncio.sleep(wait_s)
            if attempts >= TRANSFER_MAX_RETRIES:
                raise

async def tg_get_routing_config(tag):
    return await db_execute(
        "SELECT topic_id, topic_title FROM tg_routing_destinations WHERE tag=?",
        (tag,), fetch="one"
    )

async def tg_set_routing_config(tag, topic_id, topic_title):
    await db_execute(
        """INSERT INTO tg_routing_destinations (tag, topic_id, topic_title, created_at)
           VALUES (?,?,?,?)
           ON CONFLICT(tag) DO UPDATE SET topic_id=excluded.topic_id, topic_title=excluded.topic_title""",
        (tag, topic_id, topic_title, time.time())
    )

async def tg_is_message_copied(chat_id, msg_id, dest_topic_id):
    row = await db_execute(
        "SELECT 1 FROM tg_copied_messages WHERE chat_id=? AND msg_id=? AND dest_topic_id=?",
        (chat_id, msg_id, dest_topic_id), fetch="one"
    )
    return bool(row)

async def tg_mark_message_copied(chat_id, msg_id, tag, dest_topic_id):
    await db_execute(
        "INSERT OR IGNORE INTO tg_copied_messages (chat_id, msg_id, tag, dest_topic_id, copied_at) VALUES (?,?,?,?,?)",
        (chat_id, msg_id, tag, dest_topic_id, time.time())
    )

async def tg_create_destination_topic(tag):
    master_forum_id = await get_control("master_forum_id")
    if not master_forum_id:
        raise Exception("Master Forum ID is not set. Send /setmasterforum <chat_id> to the bot first.")
    
    try:
        chat_identifier = int(master_forum_id)
    except ValueError:
        chat_identifier = master_forum_id
    
    title = tag.replace("#", "").strip() or "transfer"
    peer = await user_app.resolve_peer(chat_identifier)
    real_chat = await user_app.get_chat(chat_identifier)
    real_chat_id = real_chat.id

    raw_response = await tg_flood_safe(
        user_app.invoke, 
        CreateForumTopic(
            channel=peer, 
            title=title,
            random_id=int(time.time() * 1000)
        )
    )
    
    topic_id = None
    for update in getattr(raw_response, "updates", []):
        if hasattr(update, "message") and hasattr(update.message, "id"):
            topic_id = update.message.id
            break
            
    if not topic_id:
        raise Exception("Topic was created in MTProto, but ID could not be parsed.")

    await asyncio.sleep(TRANSFER_STAGGER_SECONDS)
    await tg_set_routing_config(tag, topic_id, title)
    
    return real_chat_id, topic_id, title

async def tg_resolve_destination_topic(tag):
    master_forum_id = await get_control("master_forum_id")
    if not master_forum_id:
        raise Exception("Master Forum ID is not set. Send /setmasterforum <chat_id> to the bot first.")

    dest = await tg_get_routing_config(tag)
    if dest and dest[0]:  
        try:
            real_chat_id = int(master_forum_id)
        except ValueError:
            chat = await user_app.get_chat(master_forum_id)
            real_chat_id = chat.id
            await set_control("master_forum_id", str(real_chat_id)) 
        return real_chat_id, dest[0], dest[1]
    
    return await tg_create_destination_topic(tag)

async def build_topic_dedupe_cache(master_forum_id, topic_id):
    cache = set()
    console.print(f"[cyan]🔄 Building stateless dedupe cache for topic ID {topic_id}...[/cyan]")
    try:
        async for msg in user_app.search_messages(master_forum_id, query=""):
            if getattr(msg, "message_thread_id", None) == topic_id or getattr(msg, "reply_to_message_id", None) == topic_id:
                f_id = msg.video.file_unique_id if msg.video else (msg.document.file_unique_id if msg.document else None)
                if f_id:
                    cache.add(f_id)
    except Exception as e:
        console.print(f"[yellow]⚠️ Dedupe cache build skipped (will fallback to local DB): {e}[/yellow]")
    return cache

async def tg_execute_message_copy(src_chat_id, m, master_forum_id, topic_id, tag, delete_original, topic_cache=None):
    if await tg_is_message_copied(src_chat_id, m.id, topic_id):
        return False

    f_id = m.video.file_unique_id if m.video else (m.document.file_unique_id if m.document else None)
    if topic_cache is not None and f_id in topic_cache:
        await tg_mark_message_copied(src_chat_id, m.id, tag, topic_id)
        return False

    decouple = getattr(m, "_decouple", False)
    custom_caption = getattr(m, "_custom_caption", None)

    if m.media_group_id and not decouple:
        try:
            group_msgs = await user_app.get_media_group(src_chat_id, m.id)
        except Exception:
            group_msgs = [m]
        
        unseen_msgs = []
        for gm in group_msgs:
            gf_id = gm.video.file_unique_id if gm.video else (gm.document.file_unique_id if gm.document else None)
            if topic_cache is not None and gf_id in topic_cache:
                await tg_mark_message_copied(src_chat_id, gm.id, tag, topic_id)
            else:
                unseen_msgs.append(gm)

        if not unseen_msgs: 
            return False

        ids = [gm.id for gm in group_msgs]
        await tg_flood_safe(user_app.copy_media_group, master_forum_id, src_chat_id, m.id, reply_to_message_id=topic_id)
        
        for gm in group_msgs:
            await tg_mark_message_copied(src_chat_id, gm.id, tag, topic_id)
            gf_id = gm.video.file_unique_id if gm.video else (gm.document.file_unique_id if gm.document else None)
            if topic_cache is not None and gf_id:
                topic_cache.add(gf_id)
            
        if delete_original:
            try: await user_app.delete_messages(src_chat_id, ids)
            except Exception: pass
            
    else:
        kwargs = {}
        if custom_caption:
            kwargs['caption'] = custom_caption

        await tg_flood_safe(user_app.copy_message, master_forum_id, src_chat_id, m.id, reply_to_message_id=topic_id, **kwargs)
        await tg_mark_message_copied(src_chat_id, m.id, tag, topic_id)
        
        if topic_cache is not None and f_id:
            topic_cache.add(f_id)

        if delete_original:
            try: await user_app.delete_messages(src_chat_id, m.id)
            except Exception: pass

    await asyncio.sleep(TRANSFER_STAGGER_SECONDS)
    return True

async def run_custom_transfer(user_chat_id, state, mode, custom_tags):
    target_chat_id = state['target_chat_id']
    target_title = state['target_title']
    command_name = state['cmd_type']
    
    if command_name == "autotransfer":
        delete_originals = state.get('delete_originals', False)
    else:
        delete_originals = (command_name == "transfer")

    target_tags_lower = [t.lower().replace("#", "") for t in custom_tags] if custom_tags else []

    status_msg = await bot_app.send_message(
        user_chat_id, 
        f"🔎 Scanning **{target_title}** ({mode} mode)...", 
        parse_mode=ParseMode.MARKDOWN
    )

    results = {}
    processed_groups = set()
    
    if mode == "TAGS" and target_tags_lower:
        for tag in target_tags_lower:
            query = f"#{tag}"
            try:
                async for msg in user_app.search_messages(chat_id=target_chat_id, query=query):
                    if not (msg.video or msg.document): continue
                    
                    album_msgs = [msg]
                    if msg.media_group_id:
                        if msg.media_group_id in processed_groups: continue
                        processed_groups.add(msg.media_group_id)
                        try:
                            album_msgs = await user_app.get_media_group(target_chat_id, msg.id)
                            album_msgs = sorted(album_msgs, key=lambda x: x.id)
                        except Exception: pass
                    
                    matched = parse_master_caption_bundle(album_msgs, [tag])
                    for m in matched:
                        tags_clean = getattr(m, '_custom_albums', [])
                        for tag_clean in tags_clean:
                            results.setdefault(f"#{tag_clean.lower()}", []).append(m)
            except Exception as e:
                console.print(f"[red]⚠️ Fast search failed for {query}: {e}[/red]")

    elif mode == "ALL":
        scanned_count = 0
        async for msg in user_app.get_chat_history(target_chat_id):
            scanned_count += 1
            if scanned_count % 3000 == 0:  
                try:
                    await status_msg.edit_text(f"🔎 Scanning **{target_title}** ({mode} mode)...\n_Scanned {scanned_count} messages so far..._")
                except Exception: pass

            if not (msg.video or msg.document):
                continue
                
            album_msgs = [msg]
            if msg.media_group_id:
                if msg.media_group_id in processed_groups: continue
                processed_groups.add(msg.media_group_id)
                try:
                    album_msgs = await user_app.get_media_group(target_chat_id, msg.id)
                    album_msgs = sorted(album_msgs, key=lambda x: x.id)
                except Exception: pass

            found_tags_set = set()
            for am in album_msgs:
                extracted = extract_all_tags(am.caption or am.text or "")
                if extracted:
                    found_tags_set.update(extracted)
            
            found_tags = list(found_tags_set)
            if not found_tags:
                fallback_tag = "".join(e for e in target_title if e.isalnum()).lower() or "general"
                found_tags = [fallback_tag]
                
            for am in album_msgs:
                for tag in found_tags:
                    results.setdefault(f"#{tag}", []).append(am)

    if not results:
        await status_msg.edit_text("ℹ️ No matching media found based on your criteria.")
        return

    summary = {}
    for tag, msgs in results.items():
        try:
            await status_msg.edit_text(f"📦 Processing {tag} ({len(msgs)} matched)...", parse_mode=ParseMode.MARKDOWN)
            master_forum_id, topic_id, topic_title = await tg_resolve_destination_topic(tag)
        except Exception as e:
            console.print(f"[red]⚠️ Setup failed for {tag}: {e}[/red]")
            continue

        topic_cache = await build_topic_dedupe_cache(master_forum_id, topic_id)

        copied = 0
        for m in sorted(msgs, key=lambda x: x.id):
            f_id = m.video.file_unique_id if m.video else (m.document.file_unique_id if m.document else None)
            if f_id and f_id in topic_cache: continue

            try:
                if await tg_execute_message_copy(target_chat_id, m, master_forum_id, topic_id, tag, delete_originals, topic_cache):
                    copied += 1
            except Exception as e:
                console.print(f"[red]⚠️ Transfer failed for msg {m.id} ({tag}): {e}[/red]")

        summary[tag] = {"dest_title": topic_title, "copied": copied, "found": len(msgs)}

    lines = [f"✅ **{command_name.title()} Backfill Complete: {target_title}**", "━━━━━━━━━━━━━━━━━━"]
    for tag, info in summary.items():
        lines.append(f"• {tag} → **{info['dest_title']}**: {info['copied']}/{info['found']} processed")
    await status_msg.edit_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

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