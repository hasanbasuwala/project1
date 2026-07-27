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
# CONFIG
# ============================================================
DL_WORKERS = 3
UP_WORKERS = 3
MAX_STAGED_FILES = 6          # max downloaded-but-not-uploaded files allowed on disk at once
MIN_FREE_GB = 2.0             # hard floor: DL workers wait if free space drops below this
DB_PATH = "SysCache/vk_queue.db"
DOWNLOAD_DIR = "SysCache/vk_downloads"

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

# ============================================================
# VK SESSION
# ============================================================
console.print("[bold yellow]Connecting to VK...[/bold yellow]")
try:
    vk_session = vk_api.VkApi(token=config.VK_TOKEN)
    vk = vk_session.get_api()
    my_vk_id = vk_session.method('users.get')[0]['id']
    console.print(f"[bold green]✅ VK Connected as User ID: {my_vk_id}[/bold green]")
except Exception as e:
    console.print(f"[bold red]❌ Failed to connect to VK: {e}[/bold red]")
    exit(1)

# ============================================================
# TELEGRAM CLIENTS
# ============================================================
bot_app = Client("bot_session", api_id=config.API_ID, api_hash=config.API_HASH, bot_token=config.VK_BOT)

# SPEED UPGRADE: Increase max_concurrent_transmissions and internal workers
user_app = Client(
    "user_session",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    max_concurrent_transmissions=DL_WORKERS,
    workers=10
)

# ============================================================
# PERSISTENCE LAYER (SQLite, WAL, crash-safe)
# ============================================================
def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS control (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.commit()
    conn.close()

_init_db()

def _db_execute(query, params=(), fetch=None):
    conn = _connect()
    try:
        cur = conn.execute(query, params)
        result = None
        if fetch == "one":
            result = cur.fetchone()
        elif fetch == "all":
            result = cur.fetchall()
        conn.commit()
        return result
    finally:
        conn.close()

async def db_execute(query, params=(), fetch=None):
    return await asyncio.to_thread(_db_execute, query, params, fetch)

async def save_job(job):
    await db_execute(
        """INSERT INTO jobs (job_id, chat_id, msg_chat_id, msg_id, album_id, album_name,
                              query, idx, status, file_path, caption, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(job_id) DO UPDATE SET status=excluded.status,
                                              file_path=excluded.file_path,
                                              updated_at=excluded.updated_at""",
        (job['job_id'], job['chat_id'], job['msg_chat_id'], job['msg_id'], job['album_id'],
         job['album_name'], job['query'], job['idx'], job['status'], job.get('file_path'),
         job.get('caption', ''), time.time())
    )

async def update_job_status(job_id, status, file_path=None):
    if file_path is not None:
        await db_execute("UPDATE jobs SET status=?, file_path=?, updated_at=? WHERE job_id=?",
                          (status, file_path, time.time(), job_id))
    else:
        await db_execute("UPDATE jobs SET status=?, updated_at=? WHERE job_id=?",
                          (status, time.time(), job_id))

async def delete_job(job_id):
    await db_execute("DELETE FROM jobs WHERE job_id=?", (job_id,))

async def get_incomplete_jobs():
    return await db_execute(
        """SELECT job_id, chat_id, msg_chat_id, msg_id, album_id, album_name, query, idx, status, file_path, caption
           FROM jobs WHERE status != 'done'""",
        fetch="all"
    )

async def set_control(key, value):
    await db_execute(
        "INSERT INTO control (key, value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, str(value))
    )

async def get_control(key, default=None):
    row = await db_execute("SELECT value FROM control WHERE key=?", (key,), fetch="one")
    return row[0] if row else default

async def is_msg_in_db(msg_chat_id, msg_id):
    """DEDUPLICATION: Checks if a Telegram message ID is already in the database."""
    row = await db_execute(
        "SELECT status FROM jobs WHERE msg_chat_id=? AND msg_id=?", 
        (msg_chat_id, msg_id), fetch="one"
    )
    return bool(row)

# ============================================================
# GLOBAL STATE
# ============================================================
download_queue = asyncio.Queue()                       
upload_queue = asyncio.Queue(maxsize=MAX_STAGED_FILES) 
active_jobs = {}
user_states = {}
dashboard_msg = None
is_queue_paused = False

pause_event = asyncio.Event()
pause_event.set()

class ProgressFileReader:
    """Custom file reader that tracks upload progress for aiohttp."""
    def __init__(self, filename, callback):
        self.f = open(filename, 'rb')
        self.callback = callback
        self.total = os.path.getsize(filename)
        self.read_bytes = 0

    def read(self, size=-1):
        chunk = self.f.read(size)
        self.read_bytes += len(chunk)
        self.callback(self.read_bytes, self.total)
        return chunk

    def close(self):
        self.f.close()

def update_metrics(job_id, rich_task_id, action, current, total):
    if total == 0: return
    percent = (current / total) * 100
    job = active_jobs.get(job_id)
    if not job: return

    now = time.time()
    elapsed = now - job['start_time']

    if elapsed > 0:
        speed_bps = current / elapsed
        speed_str = f"{speed_bps / (1024*1024):.1f} MB/s"
        eta_sec = (total - current) / speed_bps if speed_bps > 0 else 0
        eta_str = f"{int(eta_sec // 60)}m {int(eta_sec % 60)}s"
    else:
        speed_str = "0 MB/s"
        eta_str = "Calc..."

    job.update({'progress': percent, 'speed': speed_str, 'eta': eta_str, 'action': action})
    progress_ui.update(rich_task_id, completed=percent)

def free_space_gb(path=DOWNLOAD_DIR):
    _, _, free = shutil.disk_usage(path)
    return free / (1024 ** 3)

async def wait_for_disk_space():
    warned = False
    while free_space_gb() < MIN_FREE_GB:
        if not warned:
            console.print(f"[bold red]⚠️ Low disk space (<{MIN_FREE_GB}GB free). Holding new downloads...[/bold red]")
            warned = True
        await asyncio.sleep(5)

# ============================================================
# DOWNLOAD WORKERS
# ============================================================
async def download_worker(worker_id):
    while True:
        await pause_event.wait()
        job = await download_queue.get()
        rich_task = None
        try:
            await pause_event.wait()       
            await wait_for_disk_space()

            job_id = job['job_id']
            display_name = f"{job['query']} (Pt.{job['idx']})"
            active_jobs[job_id] = {
                "name": display_name, "action": "📥 DL", "progress": 0,
                "speed": "0 MB/s", "eta": "Calc...", "start_time": time.time()
            }
            rich_task = progress_ui.add_task(f"[cyan]📥 DL {display_name}", total=100, name=display_name)
            await update_job_status(job_id, "downloading")

            def dl_progress(current, total):
                update_metrics(job_id, rich_task, "📥 DL", current, total)

            file_path = None
            try:
                msg = await user_app.get_messages(job['msg_chat_id'], job['msg_id'])
                file_path = await user_app.download_media(
                    msg.video,
                    file_name=os.path.join(DOWNLOAD_DIR, ""),
                    progress=dl_progress
                )
            except Exception as e:
                console.print(f"[bold red]DL failed {display_name}: {e}[/bold red]")
                await update_job_status(job_id, "pending")
                continue

            if file_path:
                job['file_path'] = file_path
                await update_job_status(job_id, "downloaded", file_path=file_path)
                await upload_queue.put(job)
            else:
                await update_job_status(job_id, "pending")

        finally:
            if rich_task is not None:
                progress_ui.remove_task(rich_task)
            active_jobs.pop(job['job_id'], None)
            download_queue.task_done()

# ============================================================
# UPLOAD WORKERS
# ============================================================
async def upload_worker(worker_id):
    async with aiohttp.ClientSession() as session:
        while True:
            job = await upload_queue.get()
            job_id = job['job_id']
            file_path = job.get('file_path')
            display_name = f"{job['query']} (Pt.{job['idx']})"
            rich_task = None
            delete_file_on_exit = False
            try:
                if not file_path or not os.path.exists(file_path):
                    console.print(f"[bold red]Missing file for {display_name}, re-queuing download[/bold red]")
                    await update_job_status(job_id, "pending", file_path=None)
                    await download_queue.put(job)
                    continue

                active_jobs[job_id] = {
                    "name": display_name, "action": "📤 UP", "progress": 0,
                    "speed": "0 MB/s", "eta": "Calc...", "start_time": time.time()
                }
                rich_task = progress_ui.add_task(f"[magenta]📤 UP {display_name}", total=100, name=display_name)
                await update_job_status(job_id, "uploading")

                # CAPTION UPGRADE: Pass the saved Telegram caption to VK
                upload_info = await asyncio.to_thread(
                    vk.video.save,
                    name=f"{job['album_name']} - Part {job['idx']}",
                    description=job.get('caption', f"Imported from Telegram ({job['query']})"),
                    album_id=job['album_id']
                )

                def up_progress(current, total):
                    update_metrics(job_id, rich_task, "📤 UP", current, total)

                reader = ProgressFileReader(file_path, up_progress)
                data = aiohttp.FormData()
                data.add_field('video_file', reader, filename=os.path.basename(file_path))
                async with session.post(upload_info['upload_url'], data=data) as resp:
                    await resp.json()
                reader.close()

                await update_job_status(job_id, "done")
                # DEDUPLICATION UPGRADE: We no longer delete the job from the DB. 
                # This ensures `is_msg_in_db` remembers it forever.
                delete_file_on_exit = True

            except Exception as e:
                console.print(f"[bold red]UP failed {display_name}: {e}[/bold red]")
                await update_job_status(job_id, "downloaded") 
                await upload_queue.put(job)
                await asyncio.sleep(3)

            finally:
                if delete_file_on_exit and file_path and os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                    except Exception:
                        pass
                if rich_task is not None:
                    progress_ui.remove_task(rich_task)
                active_jobs.pop(job_id, None)
                upload_queue.task_done()

# ============================================================
# CRASH / REBOOT RECOVERY
# ============================================================
async def recover_jobs():
    global is_queue_paused
    rows = await get_incomplete_jobs()
    for row in rows:
        job_id, chat_id, msg_chat_id, msg_id, album_id, album_name, query, idx, status, file_path, caption = row
        job = {
            'job_id': job_id, 'chat_id': chat_id, 'msg_chat_id': msg_chat_id, 'msg_id': msg_id,
            'album_id': album_id, 'album_name': album_name, 'query': query, 'idx': idx,
            'file_path': file_path, 'caption': caption
        }
        if status in ("downloaded", "uploading") and file_path and os.path.exists(file_path):
            await update_job_status(job_id, "downloaded")
            job['file_path'] = file_path
            await upload_queue.put(job)
        else:
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception:
                    pass
            await update_job_status(job_id, "pending", file_path="")
            job['file_path'] = None
            await download_queue.put(job)

    if rows:
        console.print(f"[bold yellow]♻️ Recovered {len(rows)} job(s) from previous session.[/bold yellow]")

    paused = await get_control("paused", "0")
    if paused == "1":
        is_queue_paused = True
        pause_event.clear()
        console.print("[bold yellow]⏸️ Resuming in PAUSED state (as left before shutdown).[/bold yellow]")
    else:
        pause_event.set()

# ============================================================
# TELEGRAM DASHBOARD LOOP
# ============================================================
async def dashboard_updater():
    global dashboard_msg
    while True:
        await asyncio.sleep(4)
        if not dashboard_msg:
            continue

        queued_dl = download_queue.qsize()
        staged_up = upload_queue.qsize()
        if not active_jobs and queued_dl == 0 and staged_up == 0:
            text = "✅ **All transfers completed.**\nQueue is empty."
        else:
            state_emoji = "⏸️ PAUSED" if is_queue_paused else "⚡ RUNNING"
            text = (
                f"📊 **VK Transfer Dashboard**\n"
                f"{state_emoji} | DL Workers: {DL_WORKERS} | UP Workers: {UP_WORKERS}\n"
                f"⏳ Queued DL: {queued_dl} | 📦 Staged for UP: {staged_up}\n"
                f"💾 Free space: {free_space_gb():.1f} GB\n\n"
            )
            for jid, job in active_jobs.items():
                filled = int(job['progress'] / 10)
                bar = "🟩" * filled + "⬜" * (10 - filled)
                text += (
                    f"**{job['name']}**\n"
                    f"↳ {job['action']}: {bar} {job['progress']:.1f}% ({job['speed']})\n"
                    f"↳ ETA: {job['eta']}\n\n"
                )

        kbd = InlineKeyboardMarkup([
            [InlineKeyboardButton("⏸️ Pause Queue" if not is_queue_paused else "▶️ Resume Queue", callback_data="toggle_pause"),
             InlineKeyboardButton("🛑 Clear Pending", callback_data="clear_queue")]
        ])

        try:
            await dashboard_msg.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kbd)
        except Exception:
            pass

# ============================================================
# BOT HANDLERS
# ============================================================
@bot_app.on_message(filters.private & filters.text & ~filters.command(["start", "help"]))
async def handle_user_input(client, message):
    chat_id = message.chat.id
    state = user_states.get(chat_id, {})

    if not state.get('awaiting_group'):
        query = message.text.strip()
        search_query = query if query.startswith("#") else f"#{query}"
        user_states[chat_id] = {
            'query': search_query,
            'album_name': search_query.replace("#", ""),
            'awaiting_group': True
        }
        await message.reply_text(f"🔍 Search: *{search_query}*\n📌 Send the **Group Chat ID**.", parse_mode=ParseMode.MARKDOWN)
        return

    if state.get('awaiting_group'):
        try:
            target_group_id = int(message.text.strip())
        except ValueError:
            await message.reply_text("⚠️ Invalid numeric ID.")
            return

        status_msg = await message.reply_text("🔎 Searching and filtering duplicates...")
        raw_found_msgs = []
        processed_groups = set()

        try:
            async for msg in user_app.search_messages(chat_id=target_group_id, query=state['query']):
                if msg.media_group_id:
                    if msg.media_group_id not in processed_groups:
                        processed_groups.add(msg.media_group_id)
                        album_msgs = await user_app.get_media_group(target_group_id, msg.id)
                        for album_msg in album_msgs:
                            if album_msg.video:
                                raw_found_msgs.append(album_msg)
                else:
                    if msg.video:
                        raw_found_msgs.append(msg)
        except Exception as e:
            await status_msg.edit_text(f"❌ Error accessing group: `{e}`")
            user_states.pop(chat_id, None)
            return

        # DEDUPLICATION UPGRADE: Filter out previously uploaded videos
        new_msgs = []
        skipped_count = 0
        
        for msg in raw_found_msgs:
            if await is_msg_in_db(msg.chat.id, msg.id):
                skipped_count += 1
            else:
                new_msgs.append(msg)

        if not new_msgs:
            await status_msg.edit_text(
                f"❌ No *new* videos found.\n(Skipped {skipped_count} already processed).", 
                parse_mode=ParseMode.MARKDOWN
            )
            user_states.pop(chat_id, None)
            return

        state['found_msgs'] = new_msgs
        start_kbd = InlineKeyboardMarkup([[InlineKeyboardButton(f"🚀 Queue {len(new_msgs)} new videos", callback_data="queue_transfer")]])
        await status_msg.edit_text(
            f"📊 **Search Results for** `{state['query']}`\n"
            f"🆕 New Videos: *{len(new_msgs)}*\n"
            f"⏭️ Skipped (Already Processed): *{skipped_count}*\n\n"
            f"Press below to add to Worker Queue.",
            parse_mode=ParseMode.MARKDOWN, reply_markup=start_kbd
        )

@bot_app.on_callback_query()
async def handle_buttons(client, callback):
    global dashboard_msg, is_queue_paused
    chat_id = callback.message.chat.id
    data = callback.data

    if data == "toggle_pause":
        is_queue_paused = not is_queue_paused
        if is_queue_paused:
            pause_event.clear()   
        else:
            pause_event.set()
        await set_control("paused", "1" if is_queue_paused else "0")
        await callback.answer(f"Queue {'Paused' if is_queue_paused else 'Resumed'}")

    elif data == "clear_queue":
        cleared = 0
        while not download_queue.empty():
            job = download_queue.get_nowait()
            download_queue.task_done()
            await delete_job(job['job_id'])
            cleared += 1
        await callback.answer(f"Cleared {cleared} not-yet-started downloads. Active/staged jobs will finish.", show_alert=True)

    elif data == "queue_transfer":
        state = user_states.get(chat_id)
        if not state:
            return await callback.answer("Expired session.")
        await callback.answer("Adding to queue...")

        album_name = state['album_name']
        try:
            new_album = await asyncio.to_thread(vk.video.addAlbum, title=album_name)
            target_album_id = new_album if isinstance(new_album, int) else new_album['album_id']
        except Exception:
            existing = await asyncio.to_thread(vk.video.getAlbums, owner_id=my_vk_id, count=100)
            target_album_id = next((alb['id'] for alb in existing['items'] if alb['title'].lower() == album_name.lower()), None)

        if not target_album_id:
            return await callback.message.edit_text("❌ Failed to create VK Album.")

        for idx, msg in enumerate(state['found_msgs'], start=1):
            # CAPTION UPGRADE: Capture the Telegram caption
            caption_text = msg.caption if msg.caption else f"Imported from Telegram ({state['query']})"

            job_id = f"{chat_id}_{album_name}_{msg.id}_{idx}"
            job = {
                'job_id': job_id, 'chat_id': chat_id, 'msg_chat_id': msg.chat.id, 'msg_id': msg.id,
                'album_id': target_album_id, 'album_name': album_name, 'query': state['query'],
                'idx': idx, 'status': 'pending', 'file_path': None, 'caption': caption_text
            }
            await save_job(job)
            await download_queue.put(job)

        user_states.pop(chat_id, None)
        await callback.message.delete()

        if not dashboard_msg:
            dashboard_msg = await client.send_message(chat_id, "⚙️ Initializing Dashboard...")

@bot_app.on_message(filters.command("start"))
async def start_cmd(client, message):
    await message.reply_text("👋 **Advanced Queue Bot**\nSend a hashtag to start.", parse_mode=ParseMode.MARKDOWN)

# ============================================================
# MAIN
# ============================================================
async def main():
    await user_app.start()
    await bot_app.start()

    await recover_jobs()

    asyncio.create_task(dashboard_updater())
    for i in range(DL_WORKERS):
        asyncio.create_task(download_worker(i))
    for i in range(UP_WORKERS):
        asyncio.create_task(upload_worker(i))

    with Live(progress_ui, console=console, refresh_per_second=4):
        console.print("[bold green]🚀 Workers Online. Bot is ready in Telegram![/bold green]")
        await asyncio.Event().wait()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())