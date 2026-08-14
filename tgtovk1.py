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
    console.print("[bold cyan]🔄 Syncing state from VK and analyzing for broken/duplicate uploads...[/bold cyan]")
    synced_count = 0
    seen_msg_ids = {} 
    pending_purges = [] # NEW: Collect deletions here instead of executing them

    try:
        albums_resp = await asyncio.to_thread(vk.video.getAlbums, owner_id=my_vk_id, count=100)
        albums = albums_resp.get('items', [])
        album_ids = [alb['id'] for alb in albums if alb.get('id')]
        
        # Map IDs to names for the UI
        album_map = {alb['id']: alb.get('title', 'Unknown Playlist') for alb in albums if alb.get('id')}
        album_map[None] = "Uncategorized/Main"
        
        if None not in album_ids and 0 not in album_ids:
            album_ids.append(None)

        for album_id in album_ids:
            offset = 0
            count = 200
            album_name = album_map.get(album_id, "Unknown Playlist")
            
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

                    # Mark broken videos for purging
                    if duration == 0 and not is_processing:
                        pending_purges.append({
                            'video_id': video_id, 
                            'owner_id': my_vk_id, 
                            'title': title, 
                            'album_name': album_name, 
                            'reason': 'Broken/Processing Failed'
                        })
                        continue

                    match = re.search(r'\[TG_(\d+)\]', title)
                    if match:
                        msg_id = int(match.group(1))
                        
                        if msg_id in seen_msg_ids:
                            if seen_msg_ids[msg_id] != video_id:
                                # Mark duplicate videos for purging
                                pending_purges.append({
                                    'video_id': video_id, 
                                    'owner_id': my_vk_id, 
                                    'title': title, 
                                    'album_name': album_name, 
                                    'reason': f'Duplicate of TG_{msg_id}'
                                })
                            continue
                        
                        seen_msg_ids[msg_id] = video_id
                        job_id = f"vk_recovered_{msg_id}"
                        
                        await db_execute(
                            """INSERT INTO jobs (job_id, chat_id, msg_chat_id, msg_id, album_id, album_name, query, idx, status, updated_at, tier, is_zero_disk)
                               VALUES (?,0,0,?,?,?,'vk_sync',1,'done',?,1,0)
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

        console.print(f"[bold green]✅ VK Sync Complete: {synced_count} indexed, {len(pending_purges)} flagged for purge.[/bold green]")
        return synced_count, pending_purges
    except Exception as e:
        console.print(f"[bold red]⚠️ VK Sync failed on boot: {e}[/bold red]")
        return 0, []
        
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
        
# ============================================================
# CHAPTER 4: CORE PRE-FLIGHT, DEEP PROBE & FFMPEG TRANSCODING
# ============================================================

async def deep_probe_codec(client: Client, message, job_id: str):
    """
    Downloads the first 2MB of a video to inspect its actual codec.
    Returns True if it is H.264/AAC (Web-Optimized) and safe for Zero-Disk.
    Returns False if it requires FFmpeg transcoding via Disk Fallback.
    """
    chunk_file = os.path.join(DOWNLOAD_DIR, f"probe_{job_id}.mp4")
    
    try:
        bytes_downloaded = 0
        with open(chunk_file, "wb") as f:
            async for chunk in client.stream_media(message, limit=2): # 2 MiB
                f.write(chunk)
                bytes_downloaded += len(chunk)
                
        if bytes_downloaded == 0:
            return False

        def run_ffprobe(stream_type):
            try:
                res = subprocess.run(
                    ["ffprobe", "-v", "error", "-select_streams", f"{stream_type}:0", 
                     "-show_entries", "stream=codec_name", "-of", "default=noprint_wrappers=1:nokey=1", chunk_file],
                    capture_output=True, text=True, timeout=5
                )
                return res.stdout.strip().lower()
            except Exception:
                return "unknown"

        v_codec = await asyncio.to_thread(run_ffprobe, "v")
        a_codec = await asyncio.to_thread(run_ffprobe, "a")
        
        console.print(f"[cyan]🔬 Probe {job_id}: Video={v_codec or 'none'}, Audio={a_codec or 'none'}[/cyan]")
        
        if v_codec == "h264" and (a_codec == "aac" or not a_codec):
            return True
        return False
        
    except Exception as e:
        console.print(f"[yellow]⚠️ Deep probe failed for {job_id}: {e}[/yellow]")
        return False
    finally:
        if os.path.exists(chunk_file):
            try: os.remove(chunk_file)
            except: pass


async def inspect_media_for_routing(client: Client, message, job_id: str):
    """
    The Master Gatekeeper. Decides if a job goes to Zero-Disk or Disk Fallback.
    """
    media = message.video or message.document
    if not media:
        return False
        
    mime_type = getattr(media, "mime_type", "Unknown")
    file_name = str(getattr(media, "file_name", "Unknown")).lower()
    
    # 1. Fast Metadata Check
    is_mp4_mime = mime_type == "video/mp4"
    is_mp4_ext = file_name.endswith(".mp4")
    
    if not is_mp4_mime and not is_mp4_ext:
        console.print(f"[yellow]⚠️ {job_id} is {mime_type}. Routing to Disk Fallback.[/yellow]")
        return False
        
    # 2. Deep Probe for internal codecs and 'moov' atom placement
    is_zero_disk_safe = await deep_probe_codec(client, message, job_id)
    return is_zero_disk_safe


# --- DISK FALLBACK VALIDATION & TRANSCODING ---

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

async def _deep_validate_video_file(file_path):
    def _run():
        try:
            result = subprocess.run(
                ["ffmpeg", "-v", "error", "-i", file_path, "-map", "0", "-f", "null", "-"],
                capture_output=True, text=True, timeout=300
            )
            return result.returncode == 0 and not result.stderr.strip()
        except subprocess.TimeoutExpired:
            console.print(f"[yellow]⚠️ Deep validation timed out for {os.path.basename(file_path)}, skipping deep check.[/yellow]")
            return True
        except Exception:
            return False
    return await asyncio.to_thread(_run)

async def _ensure_h264_aac(file_path):
    """
    Forces non-compliant videos into H.264/AAC for VK processing.
    Used exclusively by the Disk Fallback workers.
    """
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
# CHAPTER 5: TRANSFER ENGINES (ZERO-DISK & DISK FALLBACK)
# ============================================================

# --- 5A. ZERO-DISK ENGINE ---

class QueueStreamReader:
    """
    Synchronous wrapper around our asyncio Queue.
    Tricks `requests` into thinking it's reading a local file, forcing the Content-Length header.
    """
    def __init__(self, queue: asyncio.Queue, loop: asyncio.AbstractEventLoop, total_size: int, callback=None):
        self.queue = queue
        self.loop = loop
        self.total_size = total_size
        self.buffer = bytearray()
        self.read_bytes = 0
        self.callback = callback

    def read(self, size=-1):
        while (size == -1 or len(self.buffer) < size) and self.read_bytes < self.total_size:
            if job_id_context.get() in cancelled_jobs:
                break # Abort signal
            try:
                chunk = asyncio.run_coroutine_threadsafe(self.queue.get(), self.loop).result()
                if chunk is None: 
                    break
                self.buffer.extend(chunk)
                self.queue.task_done()
            except Exception:
                break
        
        if size == -1:
            data = bytes(self.buffer)
            self.buffer.clear()
        else:
            data = bytes(self.buffer[:size])
            del self.buffer[:size]
            
        self.read_bytes += len(data)
        if self.callback:
            self.callback(self.read_bytes, self.total_size)
            
        return data

    def __len__(self):
        return self.total_size


import contextvars
job_id_context = contextvars.ContextVar('job_id', default='')


async def telegram_producer(client: Client, message, queue: asyncio.Queue, file_size: int, job_id: str):
    """
    Smart zero-disk streaming. Safely disconnects from Telegram if RAM fills up 
    to prevent idle timeouts, and resumes automatically when RAM frees up.
    """
    chunks_yielded = 0
    total_chunks = math.ceil(file_size / CHUNK_SIZE)
    
    while chunks_yielded < total_chunks:
        if job_id in cancelled_jobs:
            break
            
        if queue.qsize() >= QUEUE_MAX_CHUNKS:
            await asyncio.sleep(0.5)
            continue

        try:
            async for chunk in client.stream_media(message, offset=chunks_yielded):
                if job_id in cancelled_jobs:
                    break
                    
                await queue.put(chunk)
                chunks_yielded += 1
                
                if queue.qsize() >= QUEUE_MAX_CHUNKS:
                    break 
                    
        except Exception as e:
            await asyncio.sleep(2)

    await queue.put(None)


def sync_vk_upload_consumer(upload_url: str, queue: asyncio.Queue, loop: asyncio.AbstractEventLoop, file_size: int, callback):
    """
    Runs in a background thread, consuming the queue via requests.post
    """
    reader = QueueStreamReader(queue, loop, file_size, callback)
    files = {'video_file': ('relay_video.mp4', reader, 'video/mp4')}
    
    resp = requests.post(upload_url, files=files, timeout=None)
    if resp.status_code != 200:
        raise Exception(f"VK returned {resp.status_code}: {resp.text[:300]}")
    
    return resp.json()


# --- 5B. DISK FALLBACK ENGINE ---

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

    def fileno(self): return self._f.fileno()
    def tell(self): return self._f.tell()
    def seek(self, offset, whence=io.SEEK_SET):
        res = self._f.seek(offset, whence)
        self._read_bytes = self._f.tell()
        return res
    def close(self): self._f.close()
    def __len__(self): return self._total

def _sync_vk_fallback_upload(upload_url, file_path, progress_callback, job_id):
    reader = ProgressFileReader(file_path, progress_callback)
    try:
        files = {'video_file': (os.path.basename(file_path), reader, 'video/mp4')}
        resp = requests.post(upload_url, files=files, timeout=None)
        if resp.status_code != 200:
            raise Exception(f"VK upload {resp.status_code}: {resp.text[:300]}")
        return resp.json()
    finally:
        reader.close()


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

async def _download_segment(client, file_id, chunk_offset, chunk_limit, part_file, job_id, tracker, expected_bytes):
    retries = 5
    MEM_BUFFER_SIZE = 8 * 1024 * 1024
    
    while retries > 0:
        downloaded_this_attempt = 0
        try:
            buffer = bytearray()
            with open(part_file, "wb") as f:
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
                        await asyncio.sleep(0.01) 

                if buffer:
                    f.write(buffer)
                    buffer.clear()

            actual_bytes = os.path.getsize(part_file) if os.path.exists(part_file) else 0
            if actual_bytes != expected_bytes:
                raise Exception(f"SegmentSizeMismatch: got {actual_bytes}, expected {expected_bytes}")

            break
        except Exception as e:
            if str(e) == "ForceAbort": raise
            retries -= 1
            await tracker.update(-downloaded_this_attempt)
            if retries == 0: raise e
            await asyncio.sleep(5 * (5 - retries))

async def async_fast_download(client, message, file_path, progress_callback, job_id):
    media = message.video or message.document
    if not media:
        raise Exception("No valid media found in message.")
        
    file_size = media.file_size
    file_id = media.file_id  
    
    total_chunks = math.ceil(file_size / CHUNK_SIZE)
    parts_count = get_part_count(file_size)
    if total_chunks <= 1: parts_count = 1

    base_chunks = total_chunks // parts_count
    remainder = total_chunks % parts_count
    last_chunk_size = file_size - (total_chunks - 1) * CHUNK_SIZE if total_chunks > 0 else 0

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
        if chunk_limit == 0: continue
        includes_final = (chunk_offset + chunk_limit) >= total_chunks
        expected_bytes = (chunk_limit - 1) * CHUNK_SIZE + last_chunk_size if includes_final else chunk_limit * CHUNK_SIZE

        tasks.append(asyncio.create_task(
            _download_segment(client, file_id, chunk_offset, chunk_limit, part_files[i], job_id, tracker, expected_bytes)
        ))
        await asyncio.sleep(SEGMENT_STAGGER_SECONDS)

    try:
        await asyncio.gather(*tasks)
    except Exception as e:
        for task in tasks:
            if not task.done(): task.cancel()
        for part in part_files:
            if os.path.exists(part):
                try: os.remove(part)
                except: pass
        raise e

    with open(file_path, 'wb') as outfile:
        for part in part_files:
            if os.path.exists(part):
                with open(part, 'rb') as infile:
                    while True:
                        chunk = infile.read(8 * 1024 * 1024)
                        if not chunk: break
                        outfile.write(chunk)
                os.remove(part)

    final_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
    if final_size != file_size:
        try: os.remove(file_path)
        except Exception: pass
        raise Exception(f"IncompleteDownload: assembled {final_size} bytes, expected {file_size}")

    return file_path
    
# ============================================================
# CHAPTER 6: SCHEDULER, JOB BOOKKEEPING & WORKER LOOPS
# ============================================================

# --- 6A. JOB BOOKKEEPING ---

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
                text=f"⚠️ **Upload Failed Permanently**\n\nVideo: **{display_name}**\nReason: {reason}",
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception:
            pass

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
    rows = await db_execute("SELECT job_id, file_path FROM jobs WHERE playlist_id=? AND status NOT IN ('done')", (playlist_id,), fetch="all")
    for job_id, file_path in rows:
        cancelled_jobs.add(job_id)
        if file_path and os.path.exists(file_path):
            try: os.remove(file_path)
            except: pass
    await db_execute("DELETE FROM jobs WHERE playlist_id=? AND status='waiting'", (playlist_id,))
    playlist_queues.pop(playlist_id, None)
    try: playlist_order.remove(playlist_id)
    except ValueError: pass


# --- 6B. SCHEDULER LOOP ---

async def scheduler_loop():
    """
    Feeds jobs from playlists into the unified Relay Queue (Zero-Disk First).
    """
    while True:
        await asyncio.sleep(0.5)
        if engine_state != ENGINE_RUNNING: continue
        
        # We target a specific inflight amount across all active queues
        inflight = relay_queue.qsize() + download_queue_t1.qsize() + download_queue_t2.qsize()
        if inflight >= (MAX_RELAY_WORKERS + DL_WORKERS) * 2:
            continue

        attempts = len(playlist_order)
        pushed = False
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

            if job['job_id'] in cancelled_jobs:
                continue

            await update_job_status(job['job_id'], "queued")
            await relay_queue.put(job) # ALL jobs start in the Relay Queue!
            pushed = True
            break

        if pushed: continue


# --- 6C. THE SMART RELAY WORKER (ZERO-DISK GATEKEEPER) ---

async def relay_worker(worker_id):
    while True:
        await pause_event.wait()
        job = await relay_queue.get()
        job_id = job['job_id']
        job_id_context.set(job_id) # For the sync queue reader
        
        up_key = f"{job_id}_RELAY"
        display_name = f"{job['query']} (Pt.{job['idx']})"
        rich_task = None
        
        try:
            if job_id in cancelled_jobs: continue
            await pause_event.wait()
            
            # 1. Fetch Message
            msg = None
            for attempt in range(5):
                try:
                    msg = await user_app.get_messages(job['msg_chat_id'], job['msg_id'])
                    break
                except Exception as e:
                    await asyncio.sleep(3)

            if not msg or not (msg.video or msg.document):
                await on_job_permanently_failed(job, "Could not fetch media from Telegram.")
                continue

            # 2. PRE-FLIGHT GATEKEEPER
            console.print(f"[cyan]🔎 Inspecting {display_name} for Zero-Disk compatibility...[/cyan]")
            is_safe = await inspect_media_for_routing(user_app, msg, job_id)
            
            if not is_safe:
                # ⬇️ ROUTE TO DISK FALLBACK ⬇️
                job['is_zero_disk'] = 0
                target_q = download_queue_t2 if job.get('tier', 1) == 2 else download_queue_t1
                await target_q.put(job)
                continue
                
            # 🚀 PROCEED WITH ZERO-DISK RELAY 🚀
            job['is_zero_disk'] = 1
            file_size = msg.video.file_size if msg.video else msg.document.file_size
            title = display_title(job['album_name'], job['idx'], job.get('caption', ''), job['msg_id'])
            
            album_id = job.get('album_id')
            if not album_id or album_id <= 0:
                album_id = await get_or_create_vk_album(job.get('album_name'))
                job['album_id'] = album_id

            active_jobs[up_key] = {"name": display_name, "action": "🚀 RELAY", "progress": 0, "speed": "0 MB/s", "eta": "Calc...", "start_time": time.time(), "job_id": job_id}
            rich_task = progress_ui.add_task(f"[cyan]🚀 RELAY {display_name}", total=100, name=display_name)
            await update_job_status(job_id, "relaying")

            def relay_progress(current, total):
                if job_id in cancelled_jobs: raise Exception("ForceAbort")
                update_metrics(up_key, rich_task, "🚀 RELAY", current, total)

            # 3. Create VK Session
            upload_info = None
            for attempt in range(5):
                try:
                    upload_info = await asyncio.to_thread(vk.video.save, name=title, description=job.get('caption', ''), album_id=album_id)
                    break
                except Exception as e:
                    await asyncio.sleep(4)

            if not upload_info:
                raise Exception("VKVideoSaveFailed")

            mem_queue = asyncio.Queue(maxsize=QUEUE_MAX_CHUNKS)
            loop = asyncio.get_running_loop()
            
            # Start streaming
            producer_task = asyncio.create_task(telegram_producer(user_app, msg, mem_queue, file_size, job_id))
            
            upload_succeeded = False
            try:
                await asyncio.to_thread(sync_vk_upload_consumer, upload_info['upload_url'], mem_queue, loop, file_size, relay_progress)
                
                video_id = upload_info.get('video_id')
                owner_id = upload_info.get('owner_id', my_vk_id)
                
                if video_id is None:
                    upload_succeeded = True
                else:
                    if await verify_vk_video_ready(owner_id, video_id):
                        upload_succeeded = True
                    else:
                        console.print(f"[bold red]⚠️ VK reports '{display_name}' unavailable after Zero-Disk Relay.[/bold red]")
                        try: await asyncio.to_thread(vk.video.delete, owner_id=owner_id, video_id=video_id)
                        except: pass
            except Exception as e:
                if str(e) != "ForceAbort":
                    console.print(f"[bold red]❌ Relay failed for {display_name}: {e}[/bold red]")
            finally:
                if not producer_task.done():
                    producer_task.cancel()

            if upload_succeeded:
                vk_video_title_cache.setdefault(album_id, set()).add(title)
                await update_job_status(job_id, "done") 
                await db_execute("UPDATE monitored_messages SET is_queued=1 WHERE chat_id=? AND msg_id=?", (job['msg_chat_id'], job['msg_id']))
                await on_job_finished(job)
            else:
                # If Zero-Disk failed mid-stream, route to Disk Fallback to try recovering
                console.print(f"[yellow]⚠️ Zero-Disk stream failed for {display_name}. Falling back to disk...[/yellow]")
                job['is_zero_disk'] = 0
                await update_job_status(job_id, "queued")
                target_q = download_queue_t2 if job.get('tier', 1) == 2 else download_queue_t1
                await target_q.put(job)
                
        except Exception as e:
            if str(e) == "ForceAbort":
                console.print(f"[bold yellow]💀 Aborted RELAY: {display_name}[/bold yellow]")
            else:
                console.print(f"[bold red]Fatal Relay Worker error: {e}[/bold red]")
        finally:
            if rich_task is not None: progress_ui.remove_task(rich_task)
            active_jobs.pop(up_key, None)
            relay_queue.task_done()


# --- 6D. DISK FALLBACK WORKERS ---

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
            if job_id in cancelled_jobs: continue
            await pause_event.wait()
            while free_space_gb() < MIN_FREE_GB: await asyncio.sleep(5)

            active_jobs[dl_key] = {"name": display_name, "action": "📥 DL", "progress": 0, "speed": "0 MB/s", "eta": "Calc...", "start_time": time.time(), "job_id": job_id}
            rich_task = progress_ui.add_task(f"[magenta]📥 DL (Fallback) {display_name}", total=100, name=display_name)
            await update_job_status(job_id, "downloading")

            def dl_progress(current, total):
                update_metrics(dl_key, rich_task, "📥 DL", current, total)

            msg = await user_app.get_messages(job['msg_chat_id'], job['msg_id'])
            target_file_path = os.path.join(DOWNLOAD_DIR, f"{job_id}.mp4")

            file_path = await async_fast_download(user_app, msg, target_file_path, dl_progress, job_id)

            if file_path and os.path.exists(file_path):
                job['file_path'] = file_path
                await update_job_status(job_id, "downloaded", file_path=file_path)
                await upload_queue.put(job)
            else:
                await update_job_status(job_id, "waiting")

        except Exception as e:
            if str(e) == "ForceAbort":
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
                target_q = download_queue_t2 if job.get('tier', 1) == 2 else download_queue_t1
                await target_q.put(job)
                continue

            if not await _validate_video_file(file_path) or not await _deep_validate_video_file(file_path):
                console.print(f"[bold red]⚠️ Corrupt file detected before upload, requeueing for redownload: {file_path}[/bold red]")
                try: os.remove(file_path)
                except: pass
                job['file_path'] = None
                await update_job_status(job_id, "waiting", file_path="")
                target_q = download_queue_t2 if job.get('tier', 1) == 2 else download_queue_t1
                await target_q.put(job)
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

            album_id = job.get('album_id')
            if not album_id or album_id <= 0:
                album_id = await get_or_create_vk_album(job.get('album_name'))
                job['album_id'] = album_id

            active_jobs[up_key] = {"name": display_name, "action": "📤 UP", "progress": 0, "speed": "0 MB/s", "eta": "Calc...", "start_time": time.time(), "job_id": job_id}
            rich_task = progress_ui.add_task(f"[magenta]📤 UP (Fallback) {display_name}", total=100, name=display_name)
            await update_job_status(job_id, "uploading")

            title = display_title(job['album_name'], job['idx'], job.get('caption', ''), job['msg_id'])

            def up_progress(current, total):
                if job_id in cancelled_jobs: raise Exception("ForceAbort")
                update_metrics(up_key, rich_task, "📤 UP", current, total)

            upload_succeeded = False
            for save_attempt in range(3):
                upload_info = None
                for attempt in range(5):
                    try:
                        upload_info = await asyncio.to_thread(vk.video.save, name=title, description=job.get('caption', ''), album_id=album_id)
                        break
                    except Exception:
                        await asyncio.sleep(4)

                if not upload_info:
                    raise Exception("VKVideoSaveFailed")

                await asyncio.to_thread(_sync_vk_fallback_upload, upload_info['upload_url'], file_path, up_progress, job_id)

                video_id = upload_info.get('video_id')
                owner_id = upload_info.get('owner_id', my_vk_id)

                if video_id is None:
                    upload_succeeded = True
                    break

                if await verify_vk_video_ready(owner_id, video_id):
                    upload_succeeded = True
                    break

                try: await asyncio.to_thread(vk.video.delete, owner_id=owner_id, video_id=video_id)
                except Exception: pass

            if not upload_succeeded:
                vk_reupload_attempts[job_id] = vk_reupload_attempts.get(job_id, 0) + 1
                await update_job_status(job_id, "failed_vk_verify")
                await on_job_permanently_failed(job, "VK repeatedly failed to process this video after upload.")
                delete_file_on_exit = True
                continue

            vk_video_title_cache.setdefault(album_id, set()).add(title)
            await update_job_status(job_id, "done") 
            delete_file_on_exit = True
            await db_execute("UPDATE monitored_messages SET is_queued=1 WHERE chat_id=? AND msg_id=?", (job['msg_chat_id'], job['msg_id']))
            await on_job_finished(job)

        except Exception as e:
            if str(e) == "ForceAbort":
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
# CHAPTER 7: UI & DASHBOARD RENDERER
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
    
    # Unified Queue Metrics
    queued_relay = relay_queue.qsize() + scheduled_pending
    queued_dl = download_queue_t1.qsize() + download_queue_t2.qsize()
    staged_up = upload_queue.qsize()
    
    active_relays = [j for j in active_jobs.values() if j['action'] == "🚀 RELAY"]
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

        autotransfer_count = await db_execute("SELECT COUNT(*) FROM autotransfer_monitors WHERE status='ACTIVE'", fetch="one")
        at_count = autotransfer_count[0] if autotransfer_count else 0

        text = (f"{UI_STRINGS['dashboard_header']}\n"
                f"{_engine_banner()} | 💾 Free Disk: {free_space_gb():.1f} GB\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🚀 **Zero-Disk Relay:** {len(active_relays)} Active | {queued_relay} Queued\n"
                f"📥 **Disk Download:** {len(active_dls)} Active | {queued_dl} Queued\n"
                f"📦 **Staged on Disk:** {staged_up} Files\n"
                f"📤 **Disk Upload:** {len(active_ups)} Active\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"👁️ **Monitors:** {m_count} Active | 📡 **Auto-Sync:** {a_count} Active\n"
                f"🎯 **Selective:** {s_count} Active | 🚀 **Auto-Transfer:** {at_count} Active\n"
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
            InlineKeyboardButton(f"🚀 View Relays ({len(active_relays)})", callback_data="ui_RELAY_VIEW")
        ])
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
            InlineKeyboardButton("🚀 Auto-Transfer Groups", callback_data="ui_AUTOTRANSFER_VIEW")
        ])
        buttons.append([
            InlineKeyboardButton("▶️ Global Resume" if engine_state != ENGINE_RUNNING else "⏸️ Global Pause", callback_data="toggle_pause"),
            InlineKeyboardButton("🛑 Clear Queues", callback_data="clear_queue")
        ])

    elif ui_state in ["RELAY_VIEW", "DL_VIEW", "UP_VIEW"]:
        if ui_state == "RELAY_VIEW":
            target_list = active_relays
            icon = "🚀 ZERO-DISK RELAYING"
        elif ui_state == "DL_VIEW":
            target_list = active_dls
            icon = "📥 DOWNLOADING"
        else:
            target_list = active_ups
            icon = "📤 UPLOADING"
            
        text = f"{icon} **({len(target_list)} Active Workers)**\n━━━━━━━━━━━━━━━━━━\n"

        for job in target_list:
            filled = int(job['progress'] / 10)
            bar = "🟩" * filled + "⬜" * (10 - filled)
            text += f"▶️ **{job['name']}**\n↳ {bar} {job['progress']:.1f}% ({job['speed']})\n\n"
            buttons.append([InlineKeyboardButton(f"💀 Kill {job['name']}", callback_data=f"kill_{job['job_id']}")])

        if not target_list: text += "No active jobs in this category.\n"
        buttons.append([InlineKeyboardButton("🔙 Back to Main", callback_data="ui_MAIN")])

    elif ui_state == "SELECTED_VIEW":
        groups = await db_execute("SELECT chat_id, chat_title, status FROM selected_monitors", fetch="all")
        tags_rows = await db_execute("SELECT tag FROM selected_tags", fetch="all")
        tags_list = [r[0] for r in tags_rows] if tags_rows else []

        text = (f"🎯 **SELECTIVE MONITORS (/MonitorSelected)**\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🏷️ **Global Target Tags ({len(tags_list)}):**\n"
                f"{', '.join(tags_list) if tags_list else '_No tags set yet_'}\n\n"
                f"📱 **Monitored Groups ({len(groups or [])}):**\n")

        if not groups: text += "_No selective groups configured yet._\n"

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
        if not rows: text += "No continuous auto-sync monitors configured yet.\n"
        
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

    elif ui_state == "AUTOTRANSFER_VIEW":
        rows = await db_execute("SELECT chat_id, chat_title, status, delete_originals FROM autotransfer_monitors", fetch="all")
        text = "🚀 **AUTO-TRANSFER GROUPS (/AutoTransfer)**\n━━━━━━━━━━━━━━━━━━\n"
        if not rows: text += "No auto-transfer monitors configured yet.\n"

        for c_id, c_title, st, del_orig in (rows or []):
            mode = "🗑️ Delete originals" if del_orig else "📋 Keep originals"
            text += f"• **{c_title}** (`{c_id}`)\n  Status: **{st}** | {mode}\n\n"
            if st == "ACTIVE":
                buttons.append([
                    InlineKeyboardButton(f"⏸️ Pause {c_title[:15]}", callback_data=f"atr_pause_{c_id}"),
                    InlineKeyboardButton(f"🛑 Stop", callback_data=f"atr_stop_{c_id}")
                ])
            else:
                buttons.append([
                    InlineKeyboardButton(f"▶️ Proceed {c_title[:15]}", callback_data=f"atr_proceed_{c_id}"),
                    InlineKeyboardButton(f"🛑 Stop", callback_data=f"atr_stop_{c_id}")
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
        if monitor_page > 0: nav_row.append(InlineKeyboardButton("◀️ Prev", callback_data="mon_prev_page"))
        nav_row.append(InlineKeyboardButton(f"Page {monitor_page + 1}/{max_pages}", callback_data="noop"))
        if monitor_page < max_pages - 1: nav_row.append(InlineKeyboardButton("Next ▶️", callback_data="mon_next_page"))
        if nav_row: buttons.append(nav_row)

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

    elif ui_state == "PLAYLISTS":
        rows = await list_playlists()
        text = "📋 **PLAYLISTS**\n━━━━━━━━━━━━━━━━━━\n"
        if not rows: text += "No playlists yet.\n"
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
            no_active = not any(j['action'] in ("📥 DL", "📤 UP", "🚀 RELAY") for j in active_jobs.values())
            if no_active and upload_queue.empty():
                engine_state = ENGINE_PAUSED
                await set_control("engine_state", ENGINE_PAUSED)
        await render_dashboard()
        
# ============================================================
# CHAPTER 8: BOT COMMANDS, HANDLERS & MAIN LOOP
# ============================================================

@bot_app.on_message(filters.command("start"))
async def start_cmd(client, message):
    msg = await message.reply_text(UI_STRINGS["booting"])
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
    status_msg = await message.reply_text(UI_STRINGS["refresh_step_1"], parse_mode=ParseMode.MARKDOWN)

    prev_state = engine_state
    engine_state = ENGINE_PAUSE_REQUESTED
    pause_event.clear()
    await asyncio.sleep(1)

    await status_msg.edit_text("🔄 **Initiating System & VK Database Refresh...**\n`[2/5]` Clearing local job cache & stale message status...", parse_mode=ParseMode.MARKDOWN)

    while not relay_queue.empty(): relay_queue.get_nowait(); relay_queue.task_done()
    while not download_queue_t1.empty(): download_queue_t1.get_nowait(); download_queue_t1.task_done()
    while not download_queue_t2.empty(): download_queue_t2.get_nowait(); download_queue_t2.task_done()
    while not upload_queue.empty(): upload_queue.get_nowait(); upload_queue.task_done()
    
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
    if engine_state == ENGINE_RUNNING:
        pause_event.set()

    await status_msg.edit_text(
        f"✅ **System & VK Sync Refresh Complete!**\n\n"
        f"• **Live VK Videos Indexed:** `{synced_cnt}`\n"
        f"• **Duplicates Purged from VK:** `{dupes_cnt}`\n"
        f"• **Broken Videos Cleaned:** `{broken_cnt}`\n"
        f"• **Pending Queues:** Reset & Ready",
        parse_mode=ParseMode.MARKDOWN
    )
    await render_dashboard()

@bot_app.on_message(filters.command("monitor"))
async def monitor_cmd(client, message):
    user_states[message.chat.id] = {'awaiting_monitor_input': True}
    await message.reply_text(UI_STRINGS["monitor_setup"], parse_mode=ParseMode.MARKDOWN)

@bot_app.on_message(filters.command("monitoralways"))
async def monitor_always_cmd(client, message):
    user_states[message.chat.id] = {'awaiting_always_input': True}
    await message.reply_text(UI_STRINGS["auto_sync_setup"], parse_mode=ParseMode.MARKDOWN)

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
        if not getattr(full_chat, "is_forum", False):
            return await message.reply_text("⚠️ **Topics are not enabled here.**\nGo to Group Settings -> Enable 'Topics', then run again.", parse_mode=ParseMode.MARKDOWN)
    else:
        if len(message.command) < 2:
            return await message.reply_text("Usage in DM: `/setmasterforum <chat_id>`", parse_mode=ParseMode.MARKDOWN)
        forum_id = message.command[1].strip()

    await set_control("master_forum_id", str(forum_id))
    await message.reply_text(UI_STRINGS["topic_set"].format(forum_id=forum_id), parse_mode=ParseMode.MARKDOWN)

async def _resolve_target_chat(message, command_name):
    if message.chat.type in (enums.ChatType.GROUP, enums.ChatType.SUPERGROUP):
        return message.chat.id, (message.chat.title or str(message.chat.id))

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply_text(f"Usage in DM: `/{command_name} <group_id_or_@username>`", parse_mode=ParseMode.MARKDOWN)
        return None, None

    raw_target = parts[1].strip()
    try:
        if raw_target.lstrip("-").isdigit():
            chat_obj = await user_app.get_chat(int(raw_target))
        else:
            chat_obj = await user_app.get_chat(raw_target)
        return chat_obj.id, (chat_obj.title or raw_target)
    except Exception as e:
        await message.reply_text(UI_STRINGS["error_resolution"].format(target=raw_target, error=e), parse_mode=ParseMode.MARKDOWN)
        return None, None

@bot_app.on_message(filters.command(["transfer", "copy", "autotransfer"]))
async def transfer_copy_auto_cmd(client, message):
    command_name = message.command[0].lower()
    target_chat_id, target_title = await _resolve_target_chat(message, command_name)
    if target_chat_id is None: return

    user_states[message.chat.id] = {
        'cmd_type': command_name,
        'target_chat_id': target_chat_id,
        'target_title': target_title
    }

    action_word = "Auto-Transfer" if command_name == "autotransfer" else command_name.title()
    kbd = InlineKeyboardMarkup([
        [InlineKeyboardButton("🗂️ Process ALL Messages", callback_data="tc_mode_all")],
        [InlineKeyboardButton("🏷️ Process Specific Tags", callback_data="tc_mode_tags")]
    ])
    await message.reply_text(f"⚙️ **Setup {action_word} for: {target_title}**", reply_markup=kbd, parse_mode=ParseMode.MARKDOWN)

@bot_app.on_message(filters.command("reset"))
async def reset_cmd(client, message):
    confirm_kbd = InlineKeyboardMarkup([[
        InlineKeyboardButton("⚠️ Yes, wipe everything", callback_data="reset_confirm"),
        InlineKeyboardButton("Cancel", callback_data="reset_cancel")
    ]])
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
            kbd = InlineKeyboardMarkup([
                [InlineKeyboardButton("🗑️ Delete originals", callback_data="atr_setup_del")],
                [InlineKeyboardButton("📋 Keep originals", callback_data="atr_setup_keep")]
            ])
            await message.reply_text(f"📡 **Auto-Transfer Setup: {state['target_title']}**\nShould originals be deleted after copying to the Master Forum, or kept?", reply_markup=kbd, parse_mode=ParseMode.MARKDOWN)
        else:
            await message.reply_text(f"🚀 Processing {len(custom_tags)} custom tags for {state['target_title']}...")
            asyncio.create_task(run_custom_transfer(chat_id, state, mode="TAGS", custom_tags=custom_tags))
            user_states.pop(chat_id, None)
        return

    if state.get('awaiting_monitor_input'):
        user_states.pop(chat_id, None)
        raw_inputs = message.text.split(",")
        status_msg = await message.reply_text("⚙️ Launching background history scanners...")
        success_count = 0
        for target in raw_inputs:
            if not target.strip(): continue
            success, _ = await add_monitored_target(target)
            if success: success_count += 1
        await status_msg.edit_text(f"✅ Successfully registered **{success_count}** groups!", parse_mode=ParseMode.MARKDOWN)
        return

    if state.get('awaiting_tag_search'):
        user_states.pop(chat_id, None)
        query_tag = message.text.strip().lower()
        if not query_tag.startswith("#"): query_tag = f"#{query_tag}"
        exists = await db_execute("SELECT COUNT(*) FROM monitored_messages WHERE tag=?", (query_tag,), fetch="one")
        if not exists or exists[0] == 0:
            return await message.reply_text(f"❌ Tag `{query_tag}` not found in monitored database.")
        global ui_state
        ui_state = f"MON_INSPECT_{query_tag}"
        await render_dashboard()
        return

    if not state.get('awaiting_group'):
        search_query = message.text.strip()
        search_query = search_query.lower() if search_query.startswith("#") else f"#{search_query.lower()}"
        user_states[chat_id] = {'query': search_query, 'awaiting_group': True}
        return await message.reply_text(UI_STRINGS["vk_search"].format(query=search_query), parse_mode=ParseMode.MARKDOWN)

    try: target_group_id = int(message.text.strip())
    except ValueError: return await message.reply_text("⚠️ Invalid numeric ID.")

    status_msg = await message.reply_text(UI_STRINGS["scanning_group"])
    raw_found, processed_groups = [], set()

    try:
        async for msg in user_app.search_messages(chat_id=target_group_id, query=state['query']):
            if msg.media_group_id:
                if msg.media_group_id not in processed_groups:
                    processed_groups.add(msg.media_group_id)
                    album_msgs = await user_app.get_media_group(target_group_id, msg.id)
                    matched = parse_master_caption_bundle(sorted(album_msgs, key=lambda x: x.id), [state['query']])
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
    
    await status_msg.edit_text(UI_STRINGS["found_preview"].format(query=state['query'], new=len(new_msgs), skipped=skipped_count), parse_mode=ParseMode.MARKDOWN, reply_markup=kbd)

@user_app.on_message(filters.video | filters.document)
async def live_monitor_handler(client, message):
    chat_id = message.chat.id
    txt = message.caption or message.text or ""
    f_id = message.video.file_unique_id if message.video else (message.document.file_unique_id if message.document else "")

    row = await db_execute("SELECT chat_identifier FROM monitored_chats WHERE resolved_id=?", (chat_id,), fetch="one")
    if row: await extract_and_store_message(chat_id, message.id, txt, f_id)

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
                    await relay_queue.put(job) # Pushed directly to relay gatekeeper

@bot_app.on_callback_query()
async def handle_buttons(client, callback):
    global engine_state, ui_state, monitor_page
    chat_id = callback.message.chat.id
    data = callback.data

    if data.startswith("ui_"):
        ui_state = data.replace("ui_", "")
        await render_dashboard()
        return await callback.answer()

    elif data == "noop": return await callback.answer()

    elif data in ("direct_copy_forum", "direct_transfer_forum"):
        state = user_states.get(chat_id)
        if not state or 'found_msgs' not in state: return await callback.answer("Session expired.", show_alert=True)
        delete_originals = (data == "direct_transfer_forum")
        action_name = "Transfer" if delete_originals else "Copy"
        tag = state['query']
        msgs = state['found_msgs']
        target_chat_id = msgs[0].chat.id if msgs else None

        if not target_chat_id: return await callback.answer("No valid chat ID.", show_alert=True)
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
                    if await tg_execute_message_copy(target_chat_id, m, master_forum_id, topic_id, tag, delete_originals, topic_cache):
                        copied += 1
                except Exception: pass
            await callback.message.edit_text(f"✅ **{action_name} Complete!**\nMoved {copied}/{len(msgs)} messages to **{topic_title}**.")
        except Exception as e:
            await callback.message.edit_text(f"❌ **Error during {action_name}:** {e}")
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
            await callback.message.edit_text("🏷️ **Tag Filter Mode**\nType tags separated by commas.", parse_mode=ParseMode.MARKDOWN)
            return
        if state['cmd_type'] == "autotransfer":
            kbd = InlineKeyboardMarkup([[InlineKeyboardButton("🗑️ Delete originals", callback_data="atr_setup_del")], [InlineKeyboardButton("📋 Keep originals", callback_data="atr_setup_keep")]])
            await callback.message.edit_text(f"📡 **Auto-Transfer Setup: {state['target_title']}**\nShould originals be deleted?", reply_markup=kbd, parse_mode=ParseMode.MARKDOWN)
            return
        else:
            await callback.message.edit_text(f"🚀 Processing ALL messages for {state['target_title']}...")
            asyncio.create_task(run_custom_transfer(chat_id, state, mode="ALL", custom_tags=[]))
            user_states.pop(chat_id, None)
            return

    elif data == "reset_confirm":
        prev_state = engine_state
        engine_state = ENGINE_PAUSE_REQUESTED
        pause_event.clear()
        await callback.message.edit_text("🔄 Resetting everything...")
        await asyncio.sleep(1)

        while not relay_queue.empty(): relay_queue.get_nowait(); relay_queue.task_done()
        while not download_queue_t1.empty(): download_queue_t1.get_nowait(); download_queue_t1.task_done()
        while not download_queue_t2.empty(): download_queue_t2.get_nowait(); download_queue_t2.task_done()
        while not upload_queue.empty(): upload_queue.get_nowait(); upload_queue.task_done()

        playlist_queues.clear()
        playlist_order.clear()
        cancelled_jobs.clear()
        vk_video_title_cache.clear()
        vk_album_name_cache.clear()
        active_jobs.clear()
        user_states.clear()
        vk_reupload_attempts.clear()

        for table in ("jobs", "playlists", "monitored_chats", "monitored_messages", "monitored_tags_meta",
                      "always_monitors", "selected_monitors", "selected_tags",
                      "tg_routing_destinations", "tg_copied_messages", "autotransfer_monitors"):
            await db_execute(f"DELETE FROM {table}")

        if os.path.exists(DOWNLOAD_DIR):
            shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)
            os.makedirs(DOWNLOAD_DIR, exist_ok=True)

        engine_state = ENGINE_RUNNING
        pause_event.set()
        await set_control("engine_state", ENGINE_RUNNING)
        await callback.message.edit_text("✅ **Full reset complete.** All state wiped, engine restarted clean.", parse_mode=ParseMode.MARKDOWN)
        ui_state = "MAIN"
        await render_dashboard()
        return

    elif data == "reset_cancel":
        await callback.message.edit_text("Reset cancelled.")
        return await callback.answer()

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
            if await vk_title_exists(album_id, display_title(album_name, idx, caption, msg.id)):
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
                await relay_queue.put(job) # Pilot starts in the Relay Queue

        await set_playlist_status(playlist_id, "PILOT_RUNNING")
        user_states.pop(chat_id, None)
        await callback.message.delete()
        await render_dashboard()

    elif data == "clear_queue":
        cleared = 0
        while not relay_queue.empty():
            job = relay_queue.get_nowait(); relay_queue.task_done()
            cancelled_jobs.add(job['job_id']); await delete_job_row(job['job_id']); cleared += 1
        while not download_queue_t1.empty():
            job = download_queue_t1.get_nowait(); download_queue_t1.task_done()
            cancelled_jobs.add(job['job_id']); await delete_job_row(job['job_id']); cleared += 1
        while not download_queue_t2.empty():
            job = download_queue_t2.get_nowait(); download_queue_t2.task_done()
            cancelled_jobs.add(job['job_id']); await delete_job_row(job['job_id']); cleared += 1
        for pid in list(playlist_queues.keys()):
            q = playlist_queues.pop(pid)
            for job in q:
                cancelled_jobs.add(job['job_id']); await delete_job_row(job['job_id']); cleared += 1
        playlist_order.clear()
        await render_dashboard()
        await callback.answer(f"Cleared {cleared} pending downloads.", show_alert=True)

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

    elif data.startswith("kill_"):
        job_id = data.replace("kill_", "")
        cancelled_jobs.add(job_id)
        await update_job_status(job_id, "cancelled")
        await callback.answer("💀 Poison pill dropped. Job aborting...", show_alert=True)
        await render_dashboard()
        
    elif data == "mon_next_page": monitor_page += 1; await render_dashboard(); return await callback.answer()
    elif data == "mon_prev_page": monitor_page -= 1; await render_dashboard(); return await callback.answer()

async def connection_watchdog():
    # Placeholder for the network watchdog implementation (from original script)
    pass

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

    # Recover waiting jobs and feed them into the Relay Gatekeeper
    rows = await db_execute("SELECT job_id, playlist_id, chat_id, msg_chat_id, msg_id, album_id, album_name, query, idx, is_pilot, status, file_path, caption, tier, is_zero_disk FROM jobs WHERE status NOT IN ('done', 'cancelled')", fetch="all")
    recovered = 0
    if rows:
        for row in rows:
            job = {
                'job_id': row[0], 'playlist_id': row[1], 'chat_id': row[2], 'msg_chat_id': row[3],
                'msg_id': row[4], 'album_id': row[5], 'album_name': row[6], 'query': row[7],
                'idx': row[8], 'is_pilot': bool(row[9]), 'file_path': row[11], 'caption': row[12],
                'tier': row[13] if len(row) > 13 else 1, 'is_zero_disk': bool(row[14])
            }
            status, file_path = row[10], row[11]

            if status in ("downloaded", "uploading") and file_path and os.path.exists(file_path):
                if await _validate_video_file(file_path):
                    await update_job_status(job['job_id'], "downloaded")
                    await upload_queue.put(job)
                else:
                    try: os.remove(file_path)
                    except: pass
                    job['file_path'] = None
                    await update_job_status(job['job_id'], "queued", file_path="")
                    await relay_queue.put(job)
            else:
                if file_path and os.path.exists(file_path):
                    try: os.remove(file_path)
                    except: pass
                job['file_path'] = None
                
                if job['is_pilot'] or not job['playlist_id']:
                    await update_job_status(job['job_id'], "queued", file_path="")
                    await relay_queue.put(job)
                else:
                    pl_row = await get_playlist(job['playlist_id'])
                    if pl_row and pl_row[5] in ("RUNNING", "PILOT_RUNNING"):
                        enqueue_playlist_job(job['playlist_id'], job)
            recovered += 1
        console.print(f"[bold yellow]♻️ Recovered {recovered} jobs into Relay Gatekeeper.[/bold yellow]")

    active_playlists = await db_execute("SELECT DISTINCT album_id FROM playlists WHERE status NOT IN ('KILLED','COMPLETED')", fetch="all")
    for (album_id,) in (active_playlists or []):
        if album_id: await refresh_vk_cache(album_id)

    asyncio.create_task(connection_watchdog())
    asyncio.create_task(dashboard_updater())
    asyncio.create_task(scheduler_loop())
    
    # 🚀 Spin up the new Hybrid Engine Fleet
    for i in range(MAX_RELAY_WORKERS): asyncio.create_task(relay_worker(i))
    for i in range(DL_WORKERS): asyncio.create_task(download_worker(i))
    for i in range(UP_WORKERS): asyncio.create_task(upload_worker(i))

    dashboard_chat_id = await get_control("dashboard_chat_id")
    if dashboard_chat_id:
        try:
            dash_msg = await bot_app.send_message(
                chat_id=int(dashboard_chat_id),
                text=UI_STRINGS["booting"],
                parse_mode=ParseMode.MARKDOWN
            )
            try: await dash_msg.pin(both_sides=True)
            except: pass
            await set_control("dashboard_msg_id", dash_msg.id)
        except Exception as e: pass

    with Live(progress_ui, console=console, refresh_per_second=4):
        console.print("[bold green]🚀 Master Engine Online. Bot menu ready![/bold green]")
        await asyncio.Event().wait()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())