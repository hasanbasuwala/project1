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
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BotCommand
from pyrogram.errors import FloodWait, UserRestricted, SessionRevoked, AuthKeyUnregistered, AuthKeyDuplicated, UserDeactivated

from rich.console import Console
from rich.live import Live
from rich.progress import (
    Progress, TextColumn, BarColumn, DownloadColumn,
    TransferSpeedColumn, TimeRemainingColumn
)

import config

# ============================================================
# CONFIG & INITIALIZATION (Merged)
# ============================================================
DL_WORKERS = 3
UP_WORKERS = 3
MAX_STAGED_FILES = 4
MIN_FREE_GB = 2.0
DB_PATH = "SysCache/vk_queue.db"
DOWNLOAD_DIR = "SysCache/vk_downloads"

CHUNK_SIZE = 1024 * 1024           
MEM_BUFFER_SIZE = 8 * 1024 * 1024  
ALIGNMENT = 1024 * 1024            

SCHEDULER_INFLIGHT_TARGET = DL_WORKERS * 2
SCHEDULER_TICK = 0.5

GLOBAL_MAX_CONCURRENT_SEGMENTS = 3
global_segment_semaphore = asyncio.Semaphore(GLOBAL_MAX_CONCURRENT_SEGMENTS)
SEGMENT_STAGGER_SECONDS = 1.2

# Network / Flood Control
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

# Single Initialization of Clients
bot_app = Client("bot_session", api_id=config.API_ID, api_hash=config.API_HASH, bot_token=config.VK_BOT)
user_app = Client("p_session", api_id=config.API_ID, api_hash=config.API_HASH, max_concurrent_transmissions=5, workers=10)

# System / Autoscan State
sys_status = {"status_icon": "🟢", "status_text": "Optimal", "current_action": "💤 Idle"}
user_states = {}

# ============================================================
# NETWORK WATCHERS (From Source 2)
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
            f"[bold red]🚦 Telegram is flood-limiting file downloads. Forcing sequential downloads.[/bold red]"
        )
        flood_wait_events.clear()

class _PyrogramFloodWatcher(logging.Handler):
    def emit(self, record):
        try:
            msg = record.getMessage()
            match = _flood_wait_re.search(msg)
            if match:
                _handle_flood_wait_hit(int(match.group(1)))
        except Exception:
            pass

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
        asyncio.create_task(reconnect_client(user_app, "user"))
        asyncio.create_task(reconnect_client(bot_app, "bot"))

class _PyrogramGenericSocketErrorWatcher(logging.Handler):
    def emit(self, record):
        try:
            if _generic_socket_error_re.search(record.getMessage()):
                _handle_generic_socket_error()
        except Exception:
            pass

logging.getLogger("pyrogram").addHandler(_PyrogramGenericSocketErrorWatcher())

network_error_times = deque()
network_cooldown_until = 0.0
last_reconnect_time = {"user": 0.0, "bot": 0.0}
reconnect_locks = {"user": asyncio.Lock(), "bot": asyncio.Lock()}

CONNECTION_HEALTHCHECK_INTERVAL = 60      
CONNECTION_HEALTHCHECK_TIMEOUT = 15       
CONNECTION_HEALTHCHECK_FAIL_THRESHOLD = 2 
_health_fail_counts = {"user": 0, "bot": 0}

def _is_network_error(exc):
    if isinstance(exc, (ConnectionError, OSError, TimeoutError)):
        return True
    return any(s in str(exc) for s in ("Broken pipe", "socket.send", "Connection reset"))

async def record_network_error(exc):
    global network_cooldown_until
    now = time.time()
    network_error_times.append(now)
    while network_error_times and now - network_error_times[0] > NETWORK_ERROR_WINDOW:
        network_error_times.popleft()

    if len(network_error_times) >= NETWORK_ERROR_THRESHOLD and now >= network_cooldown_until:
        network_cooldown_until = now + NETWORK_COOLDOWN_SECONDS
        network_error_times.clear()
        asyncio.create_task(reconnect_client(user_app, "user"))

async def wait_out_network_cooldown():
    while time.time() < network_cooldown_until:
        await asyncio.sleep(1)

async def reconnect_client(client, label):
    async with reconnect_locks[label]:
        now = time.time()
        if now - last_reconnect_time[label] < RECONNECT_MIN_INTERVAL:
            return
        last_reconnect_time[label] = now
        try:
            try: await client.stop()
            except Exception: pass
            await asyncio.sleep(3)
            await client.start()
            _health_fail_counts[label] = 0
        except Exception:
            pass

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

async def set_system_state(icon, text, action=None):
    """Helper for AutoScan to update state."""
    sys_status["status_icon"] = icon
    sys_status["status_text"] = text
    if action:
        sys_status["current_action"] = action
    await render_dashboard()

async def flash_message(text: str, delay: int = 10):
    try:
        msg = await bot_app.send_message(config.OWNER_ID, text)
        await asyncio.sleep(delay)
        await msg.delete()
    except Exception:
        pass

# ============================================================
# DATABASE (Merged Source 1 JSON into Source 2 SQLite)
# ============================================================
def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def _init_db():
    conn = _connect()
    # VK Jobs Tables
    conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY, chat_id INTEGER, msg_chat_id INTEGER, msg_id INTEGER, 
            album_id INTEGER, album_name TEXT, query TEXT, idx INTEGER, status TEXT, 
            file_path TEXT, caption TEXT, updated_at REAL, tier INTEGER DEFAULT 1, 
            playlist_id TEXT, is_pilot INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS playlists (
            playlist_id TEXT PRIMARY KEY, chat_id INTEGER, query TEXT, album_name TEXT, 
            album_id INTEGER, status TEXT, total INTEGER, completed INTEGER, failed INTEGER, 
            skipped_dupes INTEGER, created_at REAL, updated_at REAL
        )
    """)
    conn.execute("CREATE TABLE IF NOT EXISTS control (key TEXT PRIMARY KEY, value TEXT)")
    
    # AutoScan Tables
    conn.execute("""
        CREATE TABLE IF NOT EXISTS autoscan_groups (
            chat_id TEXT PRIMARY KEY, chat_title TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS autoscan_vaults (
            tag TEXT PRIMARY KEY, vault_id INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS autoscan_stats (
            stat_name TEXT PRIMARY KEY, stat_value INTEGER
        )
    """)
    # Seed stats if empty
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM autoscan_stats")
    if cur.fetchone()[0] == 0:
        for stat in ["vaults_created", "messages_vaulted", "waits_avoided", "reconnects"]:
            conn.execute("INSERT INTO autoscan_stats (stat_name, stat_value) VALUES (?, ?)", (stat, 0))
            
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

async def increment_stat(stat_name):
    await db_execute("UPDATE autoscan_stats SET stat_value = stat_value + 1 WHERE stat_name = ?", (stat_name,))

async def get_stat(stat_name):
    row = await db_execute("SELECT stat_value FROM autoscan_stats WHERE stat_name = ?", (stat_name,), fetch="one")
    return row[0] if row else 0

async def get_all_autoscan_groups():
    return await db_execute("SELECT chat_id, chat_title FROM autoscan_groups", fetch="all")

async def get_control(key, default=None):
    row = await db_execute("SELECT value FROM control WHERE key=?", (key,), fetch="one")
    return row[0] if row else default

async def set_control(key, value):
    await db_execute("INSERT INTO control (key, value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))

# ============================================================
# AUTOSCAN LOGIC (From Source 1)
# ============================================================
def normalize_tag(raw: str) -> str:
    if not raw: return raw
    tag = raw.strip().lower()
    return tag if tag.startswith("#") else "#" + tag

def parse_master_index(master_caption: str, query: str) -> dict:
    query = normalize_tag(query)
    lines = [line.strip() for line in master_caption.strip().split('\n') if line.strip()]
    if not lines: return {}
    is_top_match = query.lower() in lines[0].lower()
    track_data = {}
    for line in lines[1:]:
        m = re.match(r'^(\d+)\s*-\s*(.*)', line)
        if not m: continue
        idx_str, rest_of_line = m.groups()
        if is_top_match or query.lower() in line.lower():
            bracket_match = re.search(r"\(([^)]*)\)", rest_of_line)
            track_caption = bracket_match.group(1).strip() if bracket_match else rest_of_line.strip()
            track_data[int(idx_str)] = track_caption
    return track_data

async def get_or_create_vault(tag: str, original_chat_title: str):
    tag = normalize_tag(tag)
    row = await db_execute("SELECT vault_id FROM autoscan_vaults WHERE tag=?", (tag,), fetch="one")
    if row: return row[0]

    vault_title = f"{original_chat_title[:30]} Vault - {tag}"
    for _ in range(3):
        try:
            await set_system_state("⏳", "Creating Group...", f"Vault for {tag}")
            new_group = await user_app.create_supergroup(vault_title, f"Auto-archived messages for {tag}")
            await db_execute("INSERT INTO autoscan_vaults (tag, vault_id) VALUES (?,?)", (tag, new_group.id))
            await increment_stat("vaults_created")
            asyncio.create_task(flash_message(f"🆕 **Vault Created:** {tag}"))
            await asyncio.sleep(15)
            await set_system_state("🟢", "Optimal")
            return new_group.id
        except FloodWait as e:
            await increment_stat("waits_avoided")
            await asyncio.sleep(e.value + 5)
        except UserRestricted:
            return None
        except Exception:
            await asyncio.sleep(10)
    return None

async def safe_copy(vault_id: int, chat_id: int, msg_id: int, caption: str = None):
    for _ in range(3):
        try:
            kwargs = {"caption": caption} if caption is not None else {}
            await user_app.copy_message(chat_id=vault_id, from_chat_id=chat_id, message_id=msg_id, **kwargs)
            await increment_stat("messages_vaulted")
            await asyncio.sleep(0.5)
            return True
        except FloodWait as e:
            await increment_stat("waits_avoided")
            await asyncio.sleep(e.value + 2)
        except Exception:
            await asyncio.sleep(10)
    return False

async def expand_tag_matches(chat_id: int, query: str):
    resolved = []
    processed_groups = set()
    query = normalize_tag(query)

    async for msg in user_app.search_messages(chat_id, query=query):
        if msg.media_group_id:
            if msg.media_group_id in processed_groups: continue
            processed_groups.add(msg.media_group_id)
            album_msgs = sorted(await user_app.get_media_group(chat_id, msg.id), key=lambda m: m.id)
            master_caption = next((am.caption for am in album_msgs if am.caption and am.caption.strip().startswith("#")), "")
            track_data = parse_master_index(master_caption, query) if master_caption else {}

            for i, am in enumerate(album_msgs, start=1):
                if not (am.video or am.photo or am.document or am.animation): continue
                if master_caption:
                    if i in track_data:
                        am._resolved_caption = track_data[i]
                        resolved.append(am)
                else:
                    am._resolved_caption = am.caption or f"Imported ({query})"
                    resolved.append(am)
        else:
            if msg.video or msg.photo or msg.document or msg.animation or msg.text:
                msg._resolved_caption = msg.caption or msg.text or f"Imported ({query})"
                resolved.append(msg)
    return resolved

async def process_history_sweep(chat_id: int, chat_title: str, target_tag: str = None, wipe_only: bool = False, delete_after: bool = True):
    await set_system_state("🟢", "Optimal", f"Gathering history from {chat_title}")
    messages_to_process = []
    try:
        if target_tag:
            messages_to_process = await expand_tag_matches(chat_id, target_tag)
        else:
            async for msg in user_app.get_chat_history(chat_id):
                text = msg.text or msg.caption or ""
                if "#" in text:
                    msg._resolved_caption = msg.caption or msg.text or ""
                    messages_to_process.append(msg)
    except Exception as e:
        await set_system_state("🟢", "Optimal", "💤 Idle")
        return

    if not messages_to_process:
        await set_system_state("🟢", "Optimal", "💤 Idle")
        return

    messages_to_process.reverse()
    deleted_count, copied_count = 0, 0

    for idx, msg in enumerate(messages_to_process, 1):
        tags = [target_tag.lower()] if target_tag else list({normalize_tag(t) for t in re.findall(r'(#\w+)', (msg.text or msg.caption or "").lower())})
        if not tags: continue

        success = True if wipe_only else False
        if not wipe_only:
            caption_override = getattr(msg, '_resolved_caption', None)
            for tag in tags:
                vault_id = await get_or_create_vault(tag, chat_title)
                if vault_id: success = await safe_copy(vault_id, chat_id, msg.id, caption=caption_override)

        if success:
            copied_count += 1
            if delete_after:
                try:
                    await user_app.delete_messages(chat_id, msg.id)
                    deleted_count += 1
                except: pass

        if idx % 5 == 0: await set_system_state("🟢", "Optimal", f"Sweeping {chat_title} ({idx}/{len(messages_to_process)})")

    await set_system_state("🟢", "Optimal", "💤 Idle")

# ============================================================
# GLOBAL STATE & VK LOGIC (Source 2)
# ============================================================
download_queue_t1 = asyncio.Queue()
download_queue_t2 = asyncio.Queue()
upload_queue = asyncio.Queue(maxsize=MAX_STAGED_FILES)

active_jobs = {}
cancelled_jobs = set()
ui_state = "MAIN"

ENGINE_RUNNING, ENGINE_PAUSE_REQUESTED, ENGINE_PAUSED = "RUNNING", "PAUSE_REQUESTED", "PAUSED"
engine_state = ENGINE_RUNNING
pause_event = asyncio.Event()
pause_event.set()

def free_space_gb():
    _, _, free = shutil.disk_usage(DOWNLOAD_DIR)
    return free / (1024 ** 3)

async def _download_segment(client, file_id, chunk_offset, chunk_limit, part_file, job_id, tracker, expected_bytes):
    async with global_segment_semaphore:
        retries = 5
        while retries > 0:
            await wait_out_network_cooldown()
            downloaded_this_attempt = 0
            try:
                buffer = bytearray()
                with open(part_file, "wb") as f:
                    async for chunk in client.stream_media(file_id, limit=chunk_limit, offset=chunk_offset):
                        if job_id in cancelled_jobs: raise Exception("ForceAbort")
                        await pause_event.wait()
                        buffer.extend(chunk)
                        downloaded_this_attempt += len(chunk)
                        await tracker.update(len(chunk))
                        if len(buffer) >= MEM_BUFFER_SIZE:
                            f.write(buffer)
                            buffer.clear()
                        if time.time() < reduced_parallelism_until:
                            await asyncio.sleep(0.3)
                    if buffer:
                        f.write(buffer)
                        buffer.clear()

                actual_bytes = os.path.getsize(part_file) if os.path.exists(part_file) else 0
                if actual_bytes != expected_bytes:
                    raise Exception(f"SegmentSizeMismatch: got {actual_bytes} bytes, expected {expected_bytes}")
                break
            except Exception as e:
                if str(e) == "ForceAbort": raise
                retries -= 1
                await tracker.update(-downloaded_this_attempt)
                if _is_network_error(e): await record_network_error(e)
                if retries == 0: raise e
                await asyncio.sleep(5 * (5 - retries))

class ProgressTracker:
    def __init__(self, total, callback):
        self.total, self.downloaded, self.callback, self.lock = total, 0, callback, asyncio.Lock()
    async def update(self, bytes_added):
        async with self.lock:
            self.downloaded += bytes_added
            self.callback(self.downloaded, self.total)

async def async_fast_download(client, message, file_path, progress_callback, job_id):
    media = message.video or message.document
    if not media: raise Exception("No media")
    file_size, file_id = media.file_size, media.file_id
    total_chunks = math.ceil(file_size / CHUNK_SIZE)
    parts_count = 1 if time.time() < reduced_parallelism_until else (1 if file_size < 200*1024*1024 else 3)
    if total_chunks <= 1: parts_count = 1

    base_chunks = total_chunks // parts_count
    remainder = total_chunks % parts_count
    last_chunk_size = file_size - (total_chunks - 1) * CHUNK_SIZE if total_chunks > 0 else 0

    ranges, current_offset = [], 0
    for i in range(parts_count):
        limit = base_chunks + (1 if i < remainder else 0)
        ranges.append((current_offset, limit))
        current_offset += limit

    tracker = ProgressTracker(file_size, progress_callback)
    part_files = [f"{file_path}.part{i}" for i in range(parts_count)]

    tasks = []
    for i, (chunk_offset, chunk_limit) in enumerate(ranges):
        if chunk_limit == 0: continue
        expected_bytes = (chunk_limit - 1) * CHUNK_SIZE + last_chunk_size if (chunk_offset + chunk_limit) >= total_chunks else chunk_limit * CHUNK_SIZE
        tasks.append(asyncio.create_task(_download_segment(client, file_id, chunk_offset, chunk_limit, part_files[i], job_id, tracker, expected_bytes)))
        await asyncio.sleep(SEGMENT_STAGGER_SECONDS)

    try: await asyncio.gather(*tasks)
    except Exception as e:
        for t in tasks:
            if not t.done(): t.cancel()
        for p in part_files:
            if os.path.exists(p): os.remove(p)
        raise e

    with open(file_path, 'wb') as outfile:
        for p in part_files:
            if os.path.exists(p):
                with open(p, 'rb') as infile:
                    while True:
                        chunk = infile.read(MEM_BUFFER_SIZE)
                        if not chunk: break
                        outfile.write(chunk)
                os.remove(p)

    final_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
    if final_size != file_size:
        try: os.remove(file_path)
        except: pass
        raise Exception(f"IncompleteDownload: assembled {final_size}, expected {file_size}")
    return file_path

# ============================================================
# DASHBOARD RENDERING (Merged UI)
# ============================================================
async def render_dashboard():
    chat_id = await get_control("dashboard_chat_id")
    msg_id = await get_control("dashboard_msg_id")
    if not chat_id or not msg_id: return

    text, buttons = "", []

    if ui_state == "MAIN":
        active_dls = [j for j in active_jobs.values() if j['action'] == "📥 DL"]
        active_ups = [j for j in active_jobs.values() if j['action'] == "📤 UP"]

        text = (f"📊 **GLOBAL TRANSFER ENGINE**\n⚡ {engine_state} | 💾 Free Disk: {free_space_gb():.1f} GB\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📥 **Downloading:** {len(active_dls)} Active\n"
                f"📤 **Uploading:** {len(active_ups)} Active\n"
                f"━━━━━━━━━━━━━━━━━━\n")

        buttons.append([
            InlineKeyboardButton("🎯 Selective VK Sync", callback_data="ui_SELECTED_VIEW"),
            InlineKeyboardButton("📡 Auto-Sync VK", callback_data="ui_ALWAYS_VIEW")
        ])
        buttons.append([
            InlineKeyboardButton("🛠️ AutoScan Controls", callback_data="ui_AUTOSCAN_VIEW"),
            InlineKeyboardButton("▶️ Global Resume" if engine_state != ENGINE_RUNNING else "⏸️ Global Pause", callback_data="toggle_pause")
        ])

    elif ui_state == "AUTOSCAN_VIEW":
        groups = await get_all_autoscan_groups()
        vc, mv, wa, rc = await get_stat("vaults_created"), await get_stat("messages_vaulted"), await get_stat("waits_avoided"), await get_stat("reconnects")
        
        text = (f"🛠 **AUTOSCAN JOB CARD** 🛠\n\n"
                f"📡 **System Status:** {sys_status['status_icon']} {sys_status['status_text']}\n"
                f"🔄 **Current Action:** {sys_status['current_action']}\n\n"
                f"📊 **Session Stats:**\n"
                f"🔹 Vaults Created: `{vc}` | Messages Vaulted: `{mv}`\n"
                f"🔹 FloodWaits: `{wa}` | Reconnects: `{rc}`\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📂 **Active Monitored Groups ({len(groups)}):**\n")
        
        if not groups: text += "_None active._\n"
        for idx, (c_id, c_title) in enumerate(groups, 1):
            text += f"{idx}. {c_title} (`{c_id}`)\n"
            
        buttons.append([InlineKeyboardButton("🔙 Back to Main", callback_data="ui_MAIN")])

    try:
        await bot_app.edit_message_text(
            chat_id=int(chat_id), message_id=int(msg_id), text=text, 
            parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(buttons)
        )
    except Exception: pass

async def dashboard_updater():
    global engine_state
    while True:
        await asyncio.sleep(4)
        await render_dashboard()

# ============================================================
# BOT COMMANDS & LIVE HANDLER
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

@bot_app.on_callback_query()
async def handle_buttons(client, callback):
    global ui_state, engine_state
    data = callback.data
    if data.startswith("ui_"):
        ui_state = data.replace("ui_", "")
        await render_dashboard()
        return await callback.answer()
    elif data == "toggle_pause":
        engine_state = ENGINE_PAUSE_REQUESTED if engine_state == ENGINE_RUNNING else ENGINE_RUNNING
        if engine_state == ENGINE_RUNNING: pause_event.set()
        else: pause_event.clear()
        await set_control("engine_state", engine_state)
        await render_dashboard()
        return await callback.answer("Toggled State.")

@user_app.on_message(filters.command(["autoscan", "stopscan", "vault", "copyonly", "wipe"], prefixes=["/", "."]) & filters.me & filters.group)
async def direct_autoscan_commands(client, message):
    cmd = message.command[0].lower()
    chat_id = str(message.chat.id)
    chat_title = message.chat.title or "Archive"
    try: await message.delete()
    except: pass

    if cmd == "autoscan":
        await db_execute("INSERT INTO autoscan_groups (chat_id, chat_title) VALUES (?,?) ON CONFLICT DO UPDATE SET chat_title=excluded.chat_title", (chat_id, chat_title))
        asyncio.create_task(process_history_sweep(int(chat_id), chat_title))
    elif cmd == "stopscan":
        await db_execute("DELETE FROM autoscan_groups WHERE chat_id=?", (chat_id,))
    elif cmd in ["vault", "copyonly", "wipe"] and len(message.command) > 1:
        tag = normalize_tag(message.command[1])
        asyncio.create_task(process_history_sweep(int(chat_id), chat_title, target_tag=tag, wipe_only=(cmd=="wipe"), delete_after=(cmd!="copyonly")))

@user_app.on_message((filters.video | filters.document | filters.text) & ~filters.me)
async def unified_live_listener(client, message):
    chat_id = str(message.chat.id)
    
    # AutoScan Check
    row = await db_execute("SELECT chat_title FROM autoscan_groups WHERE chat_id=?", (chat_id,), fetch="one")
    if row:
        text = message.text or message.caption or ""
        tags = list({normalize_tag(t) for t in re.findall(r'(#\w+)', text.lower())})
        success = False
        if tags:
            await set_system_state("⚡", "Live Event", f"Routing new msg")
            for tag in tags:
                vault_id = await get_or_create_vault(tag, row[0])
                if vault_id and await safe_copy(vault_id, message.chat.id, message.id):
                    success = True
            if success:
                try: await user_app.delete_messages(message.chat.id, message.id)
                except: pass
            await set_system_state("🟢", "Optimal", "💤 Idle")

# ============================================================
# STARTUP ROUTINE
# ============================================================
async def main():
    await user_app.start()
    await bot_app.start()
    
    asyncio.create_task(connection_watchdog())
    asyncio.create_task(dashboard_updater())
    
    with Live(progress_ui, console=console, refresh_per_second=4):
        console.print("[bold green]🚀 Unified Engine Online. Bot menu ready![/bold green]")
        await asyncio.Event().wait()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())