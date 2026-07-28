import os
import time
import shutil
import sqlite3
import asyncio
import aiohttp
import vk_api
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

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ---- Persistent logging ----
import logging
LOG_DIR = "SysCache/logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "vk_bot.log")

_file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
_file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"))
file_logger = logging.getLogger("vk_bot")
file_logger.setLevel(logging.INFO)
file_logger.addHandler(_file_handler)
file_logger.propagate = False

console = Console()

def log(plain_msg, rich_msg=None, level="info"):
    console.print(rich_msg if rich_msg is not None else plain_msg)
    getattr(file_logger, level, file_logger.info)(plain_msg)

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
    log(f"Failed to connect to VK: {e}", f"[bold red]❌ Failed to connect to VK: {e}[/bold red]", level="error")
    exit(1)

bot_app = Client("bot_session", api_id=config.API_ID, api_hash=config.API_HASH, bot_token=config.VK_BOT)
user_app = Client("user_session", api_id=config.API_ID, api_hash=config.API_HASH, max_concurrent_transmissions=DL_WORKERS, workers=10)

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
    try:
        conn.execute("ALTER TABLE jobs ADD COLUMN caption TEXT")
    except sqlite3.OperationalError:
        pass
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
        """INSERT INTO jobs (job_id, chat_id, msg_chat_id, msg_id, album_id, album_name, query, idx, status, file_path, caption, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(job_id) DO UPDATE SET status=excluded.status, file_path=excluded.file_path, updated_at=excluded.updated_at""",
        (job['job_id'], job['chat_id'], job['msg_chat_id'], job['msg_id'], job['album_id'], job['album_name'], job['query'], job['idx'], job['status'], job.get('file_path'), job.get('caption', ''), time.time())
    )

async def update_job_status(job_id, status, file_path=None):
    if file_path is not None:
        await db_execute("UPDATE jobs SET status=?, file_path=?, updated_at=? WHERE job_id=?", (status, file_path, time.time(), job_id))
    else:
        await db_execute("UPDATE jobs SET status=?, updated_at=? WHERE job_id=?", (status, time.time(), job_id))

async def set_control(key, value):
    await db_execute("INSERT INTO control (key, value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))

async def get_control(key, default=None):
    row = await db_execute("SELECT value FROM control WHERE key=?", (key,), fetch="one")
    return row[0] if row else default

async def is_msg_in_db(msg_chat_id, msg_id):
    row = await db_execute("SELECT status FROM jobs WHERE msg_chat_id=? AND msg_id=?", (msg_chat_id, msg_id), fetch="one")
    return bool(row)

# ============================================================
# GLOBAL STATE & UI CONTROL
# ============================================================
download_queue = asyncio.Queue()
upload_queue = asyncio.Queue(maxsize=MAX_STAGED_FILES)
active_jobs = {}
cancelled_jobs = set() 
upload_attempts = {}
MAX_UPLOAD_ATTEMPTS = 3
user_states = {}
is_queue_paused = False
ui_state = "MAIN" 

pause_event = asyncio.Event()
pause_event.set()

def free_space_gb(path=DOWNLOAD_DIR):
    _, _, free = shutil.disk_usage(path)
    return free / (1024 ** 3)

import io

class ProgressFileReader(io.RawIOBase):
    def __init__(self, filename, callback):
        self.f = open(filename, 'rb')
        self.callback = callback
        self.total = os.path.getsize(filename)
        self.read_bytes = 0
    def readable(self):
        return True
    def read(self, size=-1):
        chunk = self.f.read(size)
        self.read_bytes += len(chunk)
        self.callback(self.read_bytes, self.total)
        return chunk
    def close(self):
        self.f.close()
        super().close()

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

def build_video_title(job):
    caption = (job.get('caption') or '').strip()
    if caption and not caption.startswith("Imported from Telegram"):
        title = caption.splitlines()[0].strip()
        if len(title) > 100:
            title = title[:97] + "..."
        return title or f"{job['album_name']} - P{job['idx']}"
    return f"{job['album_name']} - P{job['idx']}"

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
        
        try:
            if job_id in cancelled_jobs:
                continue 

            await pause_event.wait()
            while free_space_gb() < MIN_FREE_GB: await asyncio.sleep(5)

            display_name = f"{job['query']} (Pt.{job['idx']})"
            active_jobs[dl_key] = {"name": display_name, "action": "📥 DL", "progress": 0, "speed": "0 MB/s", "eta": "Calc...", "start_time": time.time(), "job_id": job_id}
            rich_task = progress_ui.add_task(f"[cyan]📥 DL {display_name}", total=100, name=display_name)
            await update_job_status(job_id, "downloading")

            def dl_progress(current, total):
                if job_id in cancelled_jobs: raise Exception("ForceAbort")
                update_metrics(dl_key, rich_task, "📥 DL", current, total)

            msg = await user_app.get_messages(job['msg_chat_id'], job['msg_id'])
            expected_size = getattr(msg.video, "file_size", None)
            file_path = await user_app.download_media(msg.video, file_name=os.path.join(DOWNLOAD_DIR, ""), progress=dl_progress)

            if file_path and expected_size and os.path.getsize(file_path) != expected_size:
                actual_size = os.path.getsize(file_path)
                log(f"DL size mismatch {display_name}: got {actual_size} bytes, expected {expected_size}. Discarding and retrying.",
                    f"[bold red]⚠️ DL incomplete for {display_name} ({actual_size}/{expected_size} bytes). Retrying.[/bold red]", level="warning")
                try: os.remove(file_path)
                except OSError: pass
                file_path = None
                await update_job_status(job_id, "pending")
            elif file_path:
                job['file_path'] = file_path
                await update_job_status(job_id, "downloaded", file_path=file_path)
                await upload_queue.put(job)
            else:
                await update_job_status(job_id, "pending")

        except Exception as e:
            if str(e) == "ForceAbort":
                log(f"Aborted DL: {display_name}", f"[bold yellow]💀 Aborted DL: {display_name}[/bold yellow]", level="warning")
                delete_file_on_exit = True
            else:
                log(f"DL failed {display_name}: {e}", f"[bold red]DL failed {display_name}: {e}[/bold red]", level="error")
                await update_job_status(job_id, "pending")
        finally:
            if delete_file_on_exit and file_path and os.path.exists(file_path):
                try: os.remove(file_path)
                except OSError: pass
            if rich_task is not None: progress_ui.remove_task(rich_task)
            active_jobs.pop(dl_key, None)
            download_queue.task_done()

async def upload_worker(worker_id):
    # Added 1-hour timeout to prevent 500 Server Errors on large files
    timeout = aiohttp.ClientTimeout(total=3600)
    async with aiohttp.ClientSession(timeout=timeout) as session:
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
                    await update_job_status(job_id, "pending", file_path=None)
                    await download_queue.put(job)
                    continue

                active_jobs[up_key] = {"name": display_name, "action": "📤 UP", "progress": 0, "speed": "0 MB/s", "eta": "Calc...", "start_time": time.time(), "job_id": job_id}
                rich_task = progress_ui.add_task(f"[magenta]📤 UP {display_name}", total=100, name=display_name)
                await update_job_status(job_id, "uploading")

                upload_info = await asyncio.to_thread(
                    vk.video.save,
                    name=build_video_title(job),
                    description=job.get('caption', ''),
                    album_id=job['album_id']
                )

                def up_progress(current, total):
                    if job_id in cancelled_jobs: raise Exception("ForceAbort")
                    update_metrics(up_key, rich_task, "📤 UP", current, total)

                reader = ProgressFileReader(file_path, up_progress)
                file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
                data = aiohttp.FormData()
                data.add_field('video_file', reader, filename=os.path.basename(file_path))
                
                async with session.post(upload_info['upload_url'], data=data) as resp:
                    body_text = await resp.text()
                    try:
                        upload_result = await resp.json(content_type=None)
                    except Exception:
                        raise Exception(f"Upload endpoint returned non-JSON (status {resp.status}, {file_size_mb:.1f}MB file): {body_text[:300]}")
                    if resp.status != 200 or ('error' in upload_result if isinstance(upload_result, dict) else False):
                        raise Exception(f"Upload endpoint error (status {resp.status}): {upload_result}")
                reader.close()

                await update_job_status(job_id, "done")
                upload_attempts.pop(job_id, None)
                delete_file_on_exit = True

            except Exception as e:
                if str(e) == "ForceAbort":
                    log(f"Aborted UP: {display_name}", f"[bold yellow]💀 Aborted UP: {display_name}[/bold yellow]", level="warning")
                    upload_attempts.pop(job_id, None)
                    delete_file_on_exit = True
                else:
                    log(f"UP failed {display_name}: {e}", f"[bold red]UP failed {display_name}: {e}[/bold red]", level="error")
                    upload_attempts[job_id] = upload_attempts.get(job_id, 0) + 1

                    if upload_attempts[job_id] >= MAX_UPLOAD_ATTEMPTS:
                        log(f"{display_name} failed upload {MAX_UPLOAD_ATTEMPTS}x in a row — discarding file and re-downloading from scratch.", f"[bold red]🔁 {display_name}: {MAX_UPLOAD_ATTEMPTS} upload failures. Forcing fresh re-download.[/bold red]", level="error")
                        upload_attempts.pop(job_id, None)
                        delete_file_on_exit = True
                        await update_job_status(job_id, "pending", file_path=None)
                        await download_queue.put(job)
                    else:
                        await update_job_status(job_id, "downloaded")
                        await upload_queue.put(job)
                    await asyncio.sleep(3)
            finally:
                if delete_file_on_exit and file_path and os.path.exists(file_path):
                    try: os.remove(file_path)
                    except OSError: pass
                if rich_task is not None: progress_ui.remove_task(rich_task)
                active_jobs.pop(up_key, None)
                upload_queue.task_done()

# ============================================================
# DASHBOARD RENDERING ENGINE 
# ============================================================
async def render_dashboard():
    chat_id = await get_control("dashboard_chat_id")
    msg_id = await get_control("dashboard_msg_id")
    if not chat_id or not msg_id: return
    
    queued_dl = download_queue.qsize()
    staged_up = upload_queue.qsize()
    active_dls = [j for j in active_jobs.values() if j['action'] == "📥 DL"]
    active_ups = [j for j in active_jobs.values() if j['action'] == "📤 UP"]

    text = ""
    buttons = []
    global_row = [
        InlineKeyboardButton("▶️ Resume" if is_queue_paused else "⏸️ Pause", callback_data="toggle_pause"),
        InlineKeyboardButton("💀 Kill Active", callback_data="kill_all"),
        InlineKeyboardButton("🔄 Refresh", callback_data="refresh"),
    ]

    if ui_state == "MAIN":
        state_emoji = "⏸️ PAUSED" if is_queue_paused else "⚡ RUNNING"
        text = (f"📊 **GLOBAL TRANSFER ENGINE**\n{state_emoji} | 💾 Free Disk: {free_space_gb():.1f} GB\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📥 **Downloading:** {len(active_dls)} Active | {queued_dl} Queued\n"
                f"📦 **Staged on Disk:** {staged_up} Files\n"
                f"📤 **Uploading:** {len(active_ups)} Active\n"
                f"━━━━━━━━━━━━━━━━━━\n")
        
        buttons.append([
            InlineKeyboardButton(f"📥 View DLs ({len(active_dls)})", callback_data="ui_DL_VIEW"),
            InlineKeyboardButton(f"📤 View UPs ({len(active_ups)})", callback_data="ui_UP_VIEW")
        ])
        buttons.append(global_row)
        buttons.append([InlineKeyboardButton("🛑 Clear All Pendings", callback_data="clear_queue")])

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
        buttons.append(global_row)
        buttons.append([InlineKeyboardButton("🔙 Back to Main", callback_data="ui_MAIN")])

    try:
        await bot_app.edit_message_text(
            chat_id=int(chat_id), message_id=int(msg_id), text=text, 
            parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(buttons)
        )
    except Exception:
        pass 

async def dashboard_updater():
    while True:
        await asyncio.sleep(4)
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
    await message.reply_text("👋 Dashboard Ready! Send a hashtag to start, or use /kill #SearchQuery.")

@bot_app.on_message(filters.command("kill"))
async def kill_query_cmd(client, message):
    if len(message.command) < 2:
        return await message.reply_text("⚠️ Usage: `/kill #SearchQuery`")
    
    target_query = " ".join(message.command[1:]).strip()
    target_query = target_query if target_query.startswith("#") else f"#{target_query}"
    
    rows = await db_execute("SELECT job_id FROM jobs WHERE query=? AND status != 'done' AND status != 'cancelled'", (target_query,), fetch="all")
    
    if not rows:
        return await message.reply_text(f"No active or pending jobs found for `{target_query}`.")
    
    count = 0
    for row in rows:
        job_id = row[0]
        cancelled_jobs.add(job_id)
        await update_job_status(job_id, "cancelled")
        count += 1
        
    await message.reply_text(f"💀 Killed {count} active/pending jobs for `{target_query}`.")
    await render_dashboard()

@bot_app.on_message(filters.private & filters.text & ~filters.command(["start", "help", "kill"]))
async def handle_user_input(client, message):
    chat_id = message.chat.id
    state = user_states.get(chat_id, {})

    if not state.get('awaiting_group'):
        search_query = message.text.strip()
        search_query = search_query if search_query.startswith("#") else f"#{search_query}"
        user_states[chat_id] = {'query': search_query, 'album_name': search_query.replace("#", ""), 'awaiting_group': True}
        return await message.reply_text(f"🔍 Search: *{search_query}*\n📌 Send the **Group Chat ID**.", parse_mode=ParseMode.MARKDOWN)

    try: target_group_id = int(message.text.strip())
    except ValueError: return await message.reply_text("⚠️ Invalid numeric ID.")

    status_msg = await message.reply_text("🔎 Searching and filtering duplicates...")
    raw_found, processed_groups = [], set()

    try:
        async for msg in user_app.search_messages(chat_id=target_group_id, query=state['query']):
            if msg.media_group_id:
                if msg.media_group_id not in processed_groups:
                    processed_groups.add(msg.media_group_id)
                    album_msgs = await user_app.get_media_group(target_group_id, msg.id)
                    for am in album_msgs:
                        if am.video: raw_found.append(am)
            elif msg.video: raw_found.append(msg)
    except Exception as e:
        user_states.pop(chat_id, None)
        return await status_msg.edit_text(f"❌ Error accessing group: `{e}`")

    new_msgs, skipped_count = [], 0
    for msg in raw_found:
        if await is_msg_in_db(msg.chat.id, msg.id): skipped_count += 1
        else: new_msgs.append(msg)

    if not new_msgs:
        user_states.pop(chat_id, None)
        return await status_msg.edit_text(f"❌ No *new* videos found.\n(Skipped {skipped_count} already processed).", parse_mode=ParseMode.MARKDOWN)

    state['found_msgs'] = new_msgs
    state['next_idx'] = 1 
    
    kbd = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🧪 Test 1 Video First", callback_data="queue_test")],
        [InlineKeyboardButton(f"🚀 Queue All {len(new_msgs)}", callback_data="queue_transfer")]
    ])
    await status_msg.edit_text(f"📊 **Found** `{state['query']}`\n🆕 New: *{len(new_msgs)}* | ⏭️ Skipped: *{skipped_count}*", parse_mode=ParseMode.MARKDOWN, reply_markup=kbd)

@bot_app.on_callback_query()
async def handle_buttons(client, callback):
    global is_queue_paused, ui_state
    chat_id = callback.message.chat.id
    data = callback.data

    if data.startswith("ui_"):
        ui_state = data.replace("ui_", "")
        await render_dashboard() 
        return await callback.answer()

    elif data.startswith("kill_") and data != "kill_all":
        job_id = data.replace("kill_", "")
        cancelled_jobs.add(job_id)
        await update_job_status(job_id, "cancelled")
        await callback.answer("💀 Poison pill dropped. Job aborting...", show_alert=True)
        await render_dashboard() 

    elif data == "kill_all":
        count = 0
        for job in list(active_jobs.values()):
            cancelled_jobs.add(job['job_id'])
            await update_job_status(job['job_id'], "cancelled")
            count += 1
        await render_dashboard()
        await callback.answer(f"💀 Killing {count} active job(s)...", show_alert=True)

    elif data == "refresh":
        await render_dashboard()
        await callback.answer("🔄 Refreshed")

    elif data == "toggle_pause":
        is_queue_paused = not is_queue_paused
        if is_queue_paused: pause_event.clear()   
        else: pause_event.set()
        await set_control("paused", "1" if is_queue_paused else "0")
        await render_dashboard()
        await callback.answer(f"Queue {'Paused' if is_queue_paused else 'Resumed'}")

    elif data == "clear_queue":
        cleared = 0
        while not download_queue.empty():
            job = download_queue.get_nowait()
            download_queue.task_done()
            await update_job_status(job['job_id'], "cancelled")
            cleared += 1
        await render_dashboard()
        await callback.answer(f"Cleared {cleared} pending downloads.", show_alert=True)

    elif data == "queue_test":
        state = user_states.get(chat_id)
        if not state or not state.get('found_msgs'): 
            return await callback.answer("Expired session. Search again.")
        await callback.answer("Queuing Test Video...")

        album_name = state['album_name']
        target_album_id = None
        try:
            existing = await asyncio.to_thread(vk.video.getAlbums, owner_id=my_vk_id, count=100)
            target_album_id = next((alb['id'] for alb in existing['items'] if alb['title'].lower() == album_name.lower()), None)
            if not target_album_id:
                new_album = await asyncio.to_thread(vk.video.addAlbum, title=album_name)
                target_album_id = new_album if isinstance(new_album, int) else new_album['album_id']
            state['album_id'] = target_album_id 
        except Exception:
            return await callback.message.edit_text("❌ Failed to resolve VK Album.")

        msg = state['found_msgs'].pop(0)
        idx = state['next_idx']
        state['next_idx'] += 1
        
        job_id = f"{msg.chat.id}_{msg.id}" 
        job = {'job_id': job_id, 'chat_id': chat_id, 'msg_chat_id': msg.chat.id, 'msg_id': msg.id, 'album_id': target_album_id, 'album_name': album_name, 'query': state['query'], 'idx': idx, 'status': 'pending', 'file_path': None, 'caption': msg.caption if msg.caption else f"Imported from Telegram ({state['query']})"}
        await save_job(job)
        await download_queue.put(job)
        cancelled_jobs.discard(job_id)

        remaining = len(state['found_msgs'])
        if remaining > 0:
            await callback.message.edit_text(
                f"🧪 **Test Mode Active:** Queued Part {idx}.\n"
                f"📦 Remaining: {remaining} videos on standby.\n\n"
                f"Check the global dashboard to watch it upload. If it succeeds, queue the rest below.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton(f"🚀 Queue Remaining ({remaining})", callback_data="queue_transfer")
                ]])
            )
        else:
            user_states.pop(chat_id, None)
            await callback.message.delete()
            await render_dashboard()

    elif data == "queue_transfer":
        state = user_states.get(chat_id)
        if not state or not state.get('found_msgs'): 
            return await callback.answer("Expired session.")
        await callback.answer("Adding to queue...")

        album_name = state['album_name']
        target_album_id = state.get('album_id') 

        if not target_album_id:
            try:
                existing = await asyncio.to_thread(vk.video.getAlbums, owner_id=my_vk_id, count=100)
                target_album_id = next((alb['id'] for alb in existing['items'] if alb['title'].lower() == album_name.lower()), None)
                if not target_album_id:
                    new_album = await asyncio.to_thread(vk.video.addAlbum, title=album_name)
                    target_album_id = new_album if isinstance(new_album, int) else new_album['album_id']
            except Exception:
                return await callback.message.edit_text("❌ Failed to resolve VK Album.")

        for msg in state['found_msgs']:
            idx = state['next_idx']
            state['next_idx'] += 1
            job_id = f"{msg.chat.id}_{msg.id}" 
            job = {'job_id': job_id, 'chat_id': chat_id, 'msg_chat_id': msg.chat.id, 'msg_id': msg.id, 'album_id': target_album_id, 'album_name': album_name, 'query': state['query'], 'idx': idx, 'status': 'pending', 'file_path': None, 'caption': msg.caption if msg.caption else f"Imported from Telegram ({state['query']})"}
            await save_job(job)
            await download_queue.put(job)
            cancelled_jobs.discard(job_id)

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
    global is_queue_paused
    await user_app.start()
    await bot_app.start()

    rows = await db_execute("SELECT job_id, chat_id, msg_chat_id, msg_id, album_id, album_name, query, idx, status, file_path, caption FROM jobs WHERE status != 'done' AND status != 'cancelled'", fetch="all")
    if rows:
        for row in rows:
            job = {'job_id': row[0], 'chat_id': row[1], 'msg_chat_id': row[2], 'msg_id': row[3], 'album_id': row[4], 'album_name': row[5], 'query': row[6], 'idx': row[7], 'file_path': row[9], 'caption': row[10]}
            if row[8] in ("downloaded", "uploading") and row[9] and os.path.exists(row[9]):
                await update_job_status(job['job_id'], "downloaded")
                await upload_queue.put(job)
            else:
                if row[9] and os.path.exists(row[9]):
                    try: os.remove(row[9])
                    except OSError: pass
                await update_job_status(job['job_id'], "pending", file_path="")
                job['file_path'] = None
                await download_queue.put(job)
        console.print(f"[bold yellow]♻️ Recovered {len(rows)} jobs.[/bold yellow]")

    is_queue_paused = (await get_control("paused", "0") == "1")
    if is_queue_paused: pause_event.clear()

    asyncio.create_task(dashboard_updater())
    for i in range(DL_WORKERS): asyncio.create_task(download_worker(i))
    for i in range(UP_WORKERS): asyncio.create_task(upload_worker(i))

    with Live(progress_ui, console=console, refresh_per_second=4):
        console.print("[bold green]🚀 Master UI Online. Send /start in bot![/bold green]")
        await asyncio.Event().wait()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())