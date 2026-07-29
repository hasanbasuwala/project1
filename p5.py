import os
import re
import io
import time
import math
import shutil
import sqlite3
import asyncio
import aiohttp
import vk_api
import psutil
from collections import deque
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from pyrogram.file_id import FileId
from pyrogram.raw.functions.upload import GetFile
from pyrogram.raw.types import InputDocumentFileLocation

from rich.console import Console
from rich.live import Live
from rich.progress import Progress, TextColumn, BarColumn, DownloadColumn, TransferSpeedColumn, TimeRemainingColumn

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

CHUNK_SIZE = 1024 * 1024
MEM_BUFFER_SIZE = 8 * 1024 * 1024

SCHEDULER_INFLIGHT_TARGET = DL_WORKERS * 2
SCHEDULER_TICK = 0.5

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
    console=console, expand=True
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
# DATABASE (EPHEMERAL QUEUE) & PERSISTENCE
# ============================================================
def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def _init_db():
    conn = _connect()
    # Ephemeral DB: Only holds unfinished active jobs[span_1](start_span)[span_1](end_span)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY, playlist_id TEXT, chat_id INTEGER, msg_chat_id INTEGER, msg_id INTEGER,
            album_id INTEGER, album_name TEXT, query TEXT, idx INTEGER, is_pilot INTEGER DEFAULT 0,
            status TEXT, file_path TEXT, caption TEXT, updated_at REAL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS playlists (
            playlist_id TEXT PRIMARY KEY, chat_id INTEGER, query TEXT, album_name TEXT, album_id INTEGER,
            status TEXT, total INTEGER, completed INTEGER, failed INTEGER, skipped_dupes INTEGER, created_at REAL, updated_at REAL
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
           ON CONFLICT(job_id) DO UPDATE SET status=excluded.status, file_path=excluded.file_path, updated_at=excluded.updated_at""",
        (job['job_id'], job.get('playlist_id'), job['chat_id'], job['msg_chat_id'], job['msg_id'], job['album_id'], job['album_name'], 
         job['query'], job['idx'], int(job.get('is_pilot', False)), job['status'], job.get('file_path'), job.get('caption', ''), time.time())
    )

async def update_job_status(job_id, status, file_path=None):
    if file_path is not None:
        await db_execute("UPDATE jobs SET status=?, file_path=?, updated_at=? WHERE job_id=?", (status, file_path, time.time(), job_id))
    else:
        await db_execute("UPDATE jobs SET status=?, updated_at=? WHERE job_id=?", (status, time.time(), job_id))

async def delete_job_row(job_id):
    # Deletes files after successful upload (Source of Truth Architecture)[span_2](start_span)[span_2](end_span)
    await db_execute("DELETE FROM jobs WHERE job_id=?", (job_id,))

async def set_control(key, value):
    await db_execute("INSERT INTO control (key, value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))

async def get_control(key, default=None):
    row = await db_execute("SELECT value FROM control WHERE key=?", (key,), fetch="one")
    return row[0] if row else default

# ============================================================
# PLAYLIST BOOKKEEPING
# ============================================================
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

async def list_playlists(limit=15):
    return await db_execute("SELECT playlist_id, album_name, status, total, completed, failed, skipped_dupes FROM playlists WHERE status != 'KILLED' ORDER BY updated_at DESC LIMIT ?", (limit,), fetch="all")

# ============================================================
# GLOBAL STATE & VK CACHE
# ============================================================
download_queue = asyncio.Queue()
upload_queue = asyncio.Queue(maxsize=MAX_STAGED_FILES)
active_jobs = {}
cancelled_jobs = set()
user_states = {}
ui_state = "MAIN"
recovery_report = ""

playlist_queues: dict[str, deque] = {}
playlist_order: deque = deque()
vk_video_title_cache: dict[int, set] = {}

ENGINE_RUNNING, ENGINE_PAUSE_REQUESTED, ENGINE_PAUSED = "RUNNING", "PAUSE_REQUESTED", "PAUSED"
engine_state = ENGINE_RUNNING
pause_event = asyncio.Event()
pause_event.set()

def free_space_gb():
    return psutil.disk_usage(DOWNLOAD_DIR).free / (1024 ** 3)

def vk_title_for(album_name, idx):
    return f"{album_name} - P{idx}"

def display_title(album_name, idx, caption):
    caption = (caption or "").strip()
    return caption if caption else vk_title_for(album_name, idx)

async def refresh_vk_cache(album_id):
    # Fetch all existing VK video titles to build an in-memory hash set[span_3](start_span)[span_3](end_span)
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
    if total == 0 or not active_jobs.get(task_key): return
    job = active_jobs[task_key]
    percent = (current / total) * 100
    elapsed = time.time() - job['start_time']
    speed_bps = current / elapsed if elapsed > 0 else 0
    job.update({
        'progress': percent,
        'speed': f"{speed_bps / (1024*1024):.1f} MB/s",
        'eta': f"{int(((total - current) / speed_bps) // 60)}m" if speed_bps > 0 else "Calc...",
        'action': action
    })
    progress_ui.update(rich_task_id, completed=percent)

# ============================================================
# HIGH-SPEED PARALLEL DOWNLOAD ENGINE
# ============================================================
def get_part_count(file_size):
    mb = file_size / (1024 * 1024)
    if mb < 200: return 2
    elif mb < 1024: return 4
    elif mb < 3072: return 6
    else: return 8

class ProgressTracker:
    def __init__(self, total, callback):
        self.total, self.downloaded, self.callback = total, 0, callback
        self.lock = asyncio.Lock()
    async def update(self, bytes_added):
        async with self.lock:
            self.downloaded += bytes_added
            self.callback(self.downloaded, self.total)

async def _download_segment(client, message, chunk_offset, chunk_limit, part_file, job_id, tracker):
    retries = 3
    while retries > 0:
        downloaded_this_attempt = 0
        try:
            buffer = bytearray()
            with open(part_file, "wb") as f:
                async for chunk in client.stream_media(message, limit=chunk_limit, offset=chunk_offset):
                    if job_id in cancelled_jobs: raise Exception("ForceAbort")
                    await pause_event.wait()
                    buffer.extend(chunk)
                    downloaded_this_attempt += len(chunk)
                    await tracker.update(len(chunk))
                    if len(buffer) >= MEM_BUFFER_SIZE:
                        f.write(buffer)
                        buffer.clear()
                if buffer: f.write(buffer)
            break
        except Exception as e:
            if str(e) == "ForceAbort": raise
            retries -= 1
            await tracker.update(-downloaded_this_attempt)
            if retries == 0: raise e
            await asyncio.sleep(2)

async def async_fast_download(client, message, file_path, progress_callback, job_id):
    file_size = message.video.file_size
    total_chunks = math.ceil(file_size / CHUNK_SIZE)
    parts_count = get_part_count(file_size) if total_chunks > 1 else 1
    base_chunks, remainder = total_chunks // parts_count, total_chunks % parts_count

    ranges, current_offset = [], 0
    for i in range(parts_count):
        limit = base_chunks + (1 if i < remainder else 0)
        ranges.append((current_offset, limit))
        current_offset += limit

    tracker = ProgressTracker(file_size, progress_callback)
    part_files = [f"{file_path}.part{i}" for i in range(parts_count)]

    tasks = []
    for i, (chunk_offset, chunk_limit) in enumerate(ranges):
        if chunk_limit > 0:
            tasks.append(asyncio.create_task(_download_segment(client, message, chunk_offset, chunk_limit, part_files[i], job_id, tracker)))

    await asyncio.gather(*tasks)

    with open(file_path, 'wb') as outfile:
        for part in part_files:
            if os.path.exists(part):
                with open(part, 'rb') as infile:
                    while chunk := infile.read(MEM_BUFFER_SIZE): outfile.write(chunk)
                os.remove(part)
    return file_path

class ProgressFileReader(io.IOBase):
    def __init__(self, filename, callback):
        self.f = io.BufferedReader(open(filename, 'rb'), buffer_size=MEM_BUFFER_SIZE)
        self.callback = callback
        self.total = os.path.getsize(filename)
        self.read_bytes = 0
    def read(self, size=-1):
        chunk = self.f.read(MEM_BUFFER_SIZE if size == -1 or size < MEM_BUFFER_SIZE else size)
        self.read_bytes += len(chunk)
        self.callback(self.read_bytes, self.total)
        return chunk
    def close(self): self.f.close()

# ============================================================
# JOB COMPLETION / PIVOT LOGIC
# ============================================================
async def on_job_finished(job):
    playlist_id = job.get('playlist_id')
    await delete_job_row(job['job_id'])
    if not playlist_id: return

    # Pivot Logic: If pilot succeeds, pause playlist awaiting confirmation
    if job.get('is_pilot'):
        await bump_playlist(playlist_id, completed_delta=1)
        await set_playlist_status(playlist_id, "WAITING_CONFIRMATION")
        return

    await bump_playlist(playlist_id, completed_delta=1)
    row = await db_execute("SELECT total, completed, failed, skipped_dupes FROM playlists WHERE playlist_id=?", (playlist_id,), fetch="one")
    if row and (row[1] + row[2] + row[3]) >= row[0]:
        await set_playlist_status(playlist_id, "COMPLETED")

async def continue_playlist(playlist_id):
    rows = await db_execute("SELECT job_id, chat_id, msg_chat_id, msg_id, album_id, album_name, query, idx, caption FROM jobs WHERE playlist_id=? AND status='Waiting'", (playlist_id,), fetch="all")
    for r in rows:
        job = {'job_id': r[0], 'playlist_id': playlist_id, 'chat_id': r[1], 'msg_chat_id': r[2], 'msg_id': r[3], 'album_id': r[4], 'album_name': r[5], 'query': r[6], 'idx': r[7], 'caption': r[8], 'is_pilot': False}
        await update_job_status(job['job_id'], "Waiting Worker")
        enqueue_playlist_job(playlist_id, job)
    await set_playlist_status(playlist_id, "RUNNING")

# ============================================================
# SCHEDULER & WORKERS
# ============================================================
async def scheduler_loop():
    while True:
        await asyncio.sleep(SCHEDULER_TICK)
        if engine_state != ENGINE_RUNNING or download_queue.qsize() >= SCHEDULER_INFLIGHT_TARGET: continue
        for _ in range(len(playlist_order)):
            if not playlist_order: break
            pid = playlist_order[0]
            playlist_order.rotate(-1)
            if q := playlist_queues.get(pid):
                job = q.popleft()
                if not q: playlist_order.remove(pid)
                if job['job_id'] not in cancelled_jobs:
                    await update_job_status(job['job_id'], "Waiting Worker")
                    await download_queue.put(job)
                    break
            else:
                playlist_order.remove(pid)

async def download_worker(worker_id):
    while True:
        await pause_event.wait()
        job = await download_queue.get()
        job_id, dl_key = job['job_id'], f"{job['job_id']}_DL"
        display_name = f"{job['query']} (Pt.{job['idx']})"
        rich_task = delete_file = file_path = None

        try:
            if job_id in cancelled_jobs: continue
            await pause_event.wait()
            while free_space_gb() < MIN_FREE_GB: await asyncio.sleep(5)

            active_jobs[dl_key] = {"name": display_name, "action": "📥 DL", "progress": 0, "speed": "0 MB/s", "eta": "Calc...", "start_time": time.time(), "worker": worker_id}
            rich_task = progress_ui.add_task(f"[cyan]📥 DL {display_name}", total=100, name=display_name)
            await update_job_status(job_id, "Downloading")

            file_path = await async_fast_download(
                user_app, await user_app.get_messages(job['msg_chat_id'], job['msg_id']),
                os.path.join(DOWNLOAD_DIR, f"{job_id}.mp4"),
                lambda c, t: update_metrics(dl_key, rich_task, "📥 DL", c, t), job_id
            )

            if file_path and os.path.exists(file_path):
                job['file_path'] = file_path
                await update_job_status(job_id, "Staged", file_path=file_path)
                await upload_queue.put(job)
            else:
                await update_job_status(job_id, "Failed")
        except Exception as e:
            if str(e) == "ForceAbort": delete_file = True
            await update_job_status(job_id, "Failed")
        finally:
            if delete_file and file_path and os.path.exists(file_path): 
                try: os.remove(file_path) 
                except: pass
            if rich_task is not None: progress_ui.remove_task(rich_task)
            active_jobs.pop(dl_key, None)
            download_queue.task_done()

async def upload_worker(worker_id):
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None)) as session:
        while True:
            job = await upload_queue.get()
            job_id, up_key = job['job_id'], f"{job['job_id']}_UP"
            file_path, display_name = job.get('file_path'), f"{job['query']} (Pt.{job['idx']})"
            rich_task = delete_file = None

            try:
                if job_id in cancelled_jobs:
                    delete_file = True
                    continue
                if not file_path or not os.path.exists(file_path):
                    await update_job_status(job_id, "Failed", file_path=None)
                    continue

                active_jobs[up_key] = {"name": display_name, "action": "📤 UP", "progress": 0, "speed": "0 MB/s", "eta": "Calc...", "start_time": time.time(), "worker": worker_id}
                rich_task = progress_ui.add_task(f"[magenta]📤 UP {display_name}", total=100, name=display_name)
                await update_job_status(job_id, "Uploading")

                title = display_title(job['album_name'], job['idx'], job.get('caption', ''))
                upload_info = await asyncio.to_thread(vk.video.save, name=title, description=job.get('caption', ''), album_id=job['album_id'])

                reader = ProgressFileReader(file_path, lambda c, t: update_metrics(up_key, rich_task, "📤 UP", c, t))
                data = aiohttp.FormData()
                data.add_field('video_file', reader, filename=os.path.basename(file_path))

                async with session.post(upload_info['upload_url'], data=data) as resp:
                    await resp.json()
                reader.close()

                # Add to in-memory hash set[span_4](start_span)[span_4](end_span)
                vk_video_title_cache.setdefault(job['album_id'], set()).add(title)
                await update_job_status(job_id, "Uploaded") 
                delete_file = True # Immediately delete local file[span_5](start_span)[span_5](end_span)
                await on_job_finished(job)

            except Exception as e:
                if str(e) == "ForceAbort": delete_file = True
                await update_job_status(job_id, "Failed")
            finally:
                if delete_file and file_path and os.path.exists(file_path):
                    try: os.remove(file_path) 
                    except: pass
                if rich_task is not None: progress_ui.remove_task(rich_task)
                active_jobs.pop(up_key, None)
                upload_queue.task_done()

# ============================================================
# DASHBOARD ROUTING & UI ENGINE (Displays detailed metrics[span_6](start_span)[span_6](end_span))
# ============================================================
async def render_dashboard():
    chat_id, msg_id = await get_control("dashboard_chat_id"), await get_control("dashboard_msg_id")
    if not chat_id or not msg_id: return

    cpu = psutil.cpu_percent()
    mem = psutil.virtual_memory().percent
    
    text, buttons = "", []

    if ui_state == "MAIN":
        text = (f"📊 **GLOBAL ENGINE** | {'⚡ RUNNING' if engine_state == ENGINE_RUNNING else '⏸️ PAUSED'}\n"
                f"💻 CPU: `{cpu}%` | 🧠 RAM: `{mem}%` | 💾 Free Disk: `{free_space_gb():.1f} GB`\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📥 **DL Queue:** {download_queue.qsize()} | 📤 **UP Queue:** {upload_queue.qsize()}\n"
                f"━━━━━━━━━━━━━━━━━━\n")

        waiting = await db_execute("SELECT playlist_id, album_name, total FROM playlists WHERE status='WAITING_CONFIRMATION' LIMIT 3", fetch="all")
        if waiting:
            text += "🧪 **Pilots awaiting confirmation:**\n"
            for pid, album_name, total in waiting:
                text += f"• {album_name} ({total - 1} more)\n"
                buttons.append([InlineKeyboardButton(f"▶️ Continue {album_name}", callback_data=f"plcontinue_{pid}")])

        buttons.extend([
            [InlineKeyboardButton("📥 Active Downloads", callback_data="ui_DL_VIEW"), InlineKeyboardButton("📤 Active Uploads", callback_data="ui_UP_VIEW")],
            [InlineKeyboardButton("👷 Worker Monitor", callback_data="ui_WORKERS"), InlineKeyboardButton("📋 Playlists", callback_data="ui_PLAYLISTS")],
            [InlineKeyboardButton("▶️ Resume" if engine_state != ENGINE_RUNNING else "⏸️ Pause", callback_data="toggle_pause")]
        ])
        if recovery_report: buttons.append([InlineKeyboardButton("📜 View Recovery Report", callback_data="ui_RECOVERY")])

    elif ui_state in ["DL_VIEW", "UP_VIEW"]:
        target = [j for j in active_jobs.values() if j['action'] == ("📥 DL" if ui_state == "DL_VIEW" else "📤 UP")]
        text = f"{'📥 DLs' if ui_state == 'DL_VIEW' else '📤 UPs'} **({len(target)} Active)**\n━━━━━━━━━━━━━━━━━━\n"
        for job in target:
            text += f"▶️ **{job['name']}**\n↳ {job['progress']:.1f}% | {job['speed']} | ETA: {job['eta']}\n\n"
        if not target: text += "No active jobs.\n"
        buttons.append([InlineKeyboardButton("🔙 Back", callback_data="ui_MAIN")])

    elif ui_state == "WORKERS":
        text = "👷 **WORKER MONITOR**\n━━━━━━━━━━━━━━━━━━\n"
        for k, v in active_jobs.items():
            text += f"[{v['action']}] Worker {v['worker']}: `{v['name']}` ({v['speed']})\n"
        if not active_jobs: text += "All workers idle.\n"
        buttons.append([InlineKeyboardButton("🔙 Back", callback_data="ui_MAIN")])
        
    elif ui_state == "RECOVERY":
        text = f"📜 **BOOT RECOVERY REPORT**\n━━━━━━━━━━━━━━━━━━\n{recovery_report}"
        buttons.append([InlineKeyboardButton("🔙 Back", callback_data="ui_MAIN")])

    try: await bot_app.edit_message_text(chat_id=int(chat_id), message_id=int(msg_id), text=text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(buttons))
    except Exception: pass

async def dashboard_updater():
    global engine_state
    while True:
        await asyncio.sleep(4)
        if engine_state == ENGINE_PAUSE_REQUESTED and not active_jobs and upload_queue.empty():
            engine_state = ENGINE_PAUSED
            await set_control("engine_state", ENGINE_PAUSED)
        await render_dashboard()

# ============================================================
# BOT HANDLERS & SEARCH LOGIC
# ============================================================
@bot_app.on_message(filters.command("start"))
async def start_cmd(client, message):
    msg = await message.reply_text("⚙️ Booting Global Dashboard...")
    await set_control("dashboard_chat_id", message.chat.id)
    await set_control("dashboard_msg_id", msg.id)
    await message.reply_text("👋 Dashboard Ready! Send a hashtag to start.")

@bot_app.on_message(filters.private & filters.text & ~filters.command(["start", "help"]))
async def handle_user_input(client, message):
    chat_id = message.chat.id
    state = user_states.get(chat_id, {})

    if not state.get('awaiting_group'):
        search_query = message.text.strip()
        search_query = search_query if search_query.startswith("#") else f"#{search_query}"
        user_states[chat_id] = {'query': search_query, 'awaiting_group': True}
        return await message.reply_text(f"🔍 Search: *{search_query}*\n📌 Send the **Group Chat ID**.", parse_mode=ParseMode.MARKDOWN)

    try: target_group_id = int(message.text.strip())
    except ValueError: return await message.reply_text("⚠️ Invalid numeric ID.")

    status_msg = await message.reply_text("🔎 Scanning Telegram...")
    raw_found, est_size = [], 0

    try:
        async for msg in user_app.search_messages(chat_id=target_group_id, query=state['query']):
            if msg.video:
                msg._custom_album = state['query'].replace("#", "")
                msg._custom_caption = msg.caption or f"Imported ({state['query']})"
                raw_found.append(msg)
                est_size += msg.video.file_size
    except Exception as e:
        user_states.pop(chat_id, None)
        return await status_msg.edit_text(f"❌ Error: `{e}`")

    state['found_msgs'] = raw_found[::-1] # Reverse to get oldest first
    
    # Pre-flight Search Summary before queueing[span_7](start_span)[span_7](end_span)
    kbd = InlineKeyboardMarkup([[InlineKeyboardButton(f"🚀 Execute Search & Pilot", callback_data="queue_transfer")]])
    await status_msg.edit_text(
        f"📊 **Search Summary**\n"
        f"🔍 Query: `{state['query']}`\n"
        f"📁 Found on Telegram: *{len(raw_found)}*\n"
        f"💾 Est. Total Size: *{est_size / (1024**3):.1f} GB*\n\n"
        f"Hit Execute to check VK for duplicates and run the Pilot video.", reply_markup=kbd)

@bot_app.on_callback_query()
async def handle_buttons(client, callback):
    global engine_state, ui_state
    data = callback.data

    if data.startswith("ui_"):
        ui_state = data.replace("ui_", "")
        return await render_dashboard()
    
    elif data.startswith("plcontinue_"):
        await continue_playlist(data[len("plcontinue_"):])
        await callback.answer("▶️ Queueing remainder of playlist...")
        return await render_dashboard()

    elif data == "queue_transfer":
        state = user_states.get(callback.message.chat.id)
        if not state: return await callback.answer("Expired session.")
        await callback.message.edit_text("⏳ Resolving Album & Checking VK Hash Set...")

        album_name = state['query'].replace("#", "")
        existing = await asyncio.to_thread(vk.video.getAlbums, owner_id=my_vk_id, count=100)
        album_id = next((alb['id'] for alb in existing['items'] if alb['title'].lower() == album_name.lower()), None)
        if not album_id:
            new_album = await asyncio.to_thread(vk.video.addAlbum, title=album_name)
            album_id = new_album if isinstance(new_album, int) else new_album['album_id']

        await refresh_vk_cache(album_id)

        non_dupe_msgs, skipped_dupes = [], 0
        for i, msg in enumerate(state['found_msgs'], start=1):
            if await vk_title_exists(album_id, display_title(album_name, i, getattr(msg, '_custom_caption', ""))):
                skipped_dupes += 1
            else:
                msg._relative_idx = i
                non_dupe_msgs.append(msg)

        if not non_dupe_msgs:
            user_states.pop(callback.message.chat.id, None)
            return await callback.message.edit_text(f"🛑 All {len(state['found_msgs'])} videos already exist on VK.")

        playlist_id = await create_playlist(callback.message.chat.id, state['query'], album_name, album_id, len(non_dupe_msgs))
        await db_execute("UPDATE playlists SET skipped_dupes=? WHERE playlist_id=?", (skipped_dupes, playlist_id))

        # Pivot Logic applied: Queue only the first video as pilot[span_8](start_span)[span_8](end_span)
        pilot_msg = non_dupe_msgs[0]
        for msg in non_dupe_msgs:
            is_pilot = (msg is pilot_msg)
            job = {'job_id': f"{msg.chat.id}_{msg.id}", 'playlist_id': playlist_id, 'chat_id': callback.message.chat.id,
                   'msg_chat_id': msg.chat.id, 'msg_id': msg.id, 'album_id': album_id, 'album_name': album_name, 
                   'query': state['query'], 'idx': msg._relative_idx, 'is_pilot': is_pilot, 'status': 'Waiting', 
                   'caption': getattr(msg, '_custom_caption', "")}
            await save_job(job)
            if is_pilot:
                await update_job_status(job['job_id'], "Waiting Worker")
                await download_queue.put(job)

        await set_playlist_status(playlist_id, "PILOT_RUNNING")
        await callback.message.delete()
        await render_dashboard()

# ============================================================
# STARTUP & RECOVERY LOGIC
# ============================================================
async def main():
    global engine_state, recovery_report
    await user_app.start()
    await bot_app.start()

    engine_state = await get_control("engine_state", ENGINE_RUNNING)
    
    # Reboot Recovery Sequence: Reconciles local DB with actual files[span_9](start_span)[span_9](end_span)
    rows = await db_execute("SELECT * FROM jobs", fetch="all")
    resumed_up, restarted_dl, purged = 0, 0, 0
    if rows:
        for r in rows:
            job = {'job_id': r[0], 'playlist_id': r[1], 'chat_id': r[2], 'msg_chat_id': r[3], 'msg_id': r[4], 'album_id': r[5], 'album_name': r[6], 'query': r[7], 'idx': r[8], 'is_pilot': bool(r[9]), 'file_path': r[11], 'caption': r[12]}
            status = r[10]
            
            # Check if file exists locally to resume upload, else fallback to download[span_10](start_span)[span_10](end_span)
            if status in ("Staged", "Uploading", "Downloading") and job['file_path'] and os.path.exists(job['file_path']):
                await update_job_status(job['job_id'], "Staged")
                await upload_queue.put(job)
                resumed_up += 1
            else:
                if job['file_path'] and os.path.exists(job['file_path']):
                    try: os.remove(job['file_path'])
                    except: pass
                job['file_path'] = None
                await update_job_status(job['job_id'], "Waiting", file_path="")
                
                if job['is_pilot']:
                    await update_job_status(job['job_id'], "Waiting Worker")
                    await download_queue.put(job)
                    restarted_dl += 1
                elif job['playlist_id']: enqueue_playlist_job(job['playlist_id'], job)
                else:
                    await download_queue.put(job)
                    restarted_dl += 1

        recovery_report = f"✅ Resumed Uploads: {resumed_up}\n🔄 Restarted Downloads: {restarted_dl}"
        console.print(f"[bold yellow]♻️ {recovery_report}[/bold yellow]")

    # Rebuild Hash Cache on boot[span_11](start_span)[span_11](end_span)
    for (album_id,) in (await db_execute("SELECT DISTINCT album_id FROM playlists WHERE status NOT IN ('KILLED','COMPLETED')", fetch="all") or []):
        if album_id: await refresh_vk_cache(album_id)

    asyncio.create_task(dashboard_updater())
    asyncio.create_task(scheduler_loop())
    for i in range(DL_WORKERS): asyncio.create_task(download_worker(i))
    for i in range(UP_WORKERS): asyncio.create_task(upload_worker(i))

    with Live(progress_ui, console=console, refresh_per_second=4):
        console.print("[bold green]🚀 Master UI Online. Send /start in bot![/bold green]")
        await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())