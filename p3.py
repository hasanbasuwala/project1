import os
import re
import io
import time
import shutil
import sqlite3
import asyncio
import aiohttp
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

# How many jobs the round-robin scheduler is allowed to keep "in flight"
# inside download_queue at once. Keeping this small is what makes large
# playlists interleave with small ones instead of hogging the workers.
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
    # Migrations (safe no-ops if columns already exist)
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
    # v3 design: SQLite only tracks ACTIVE work. Finished/cancelled jobs are removed.
    await db_execute("DELETE FROM jobs WHERE job_id=?", (job_id,))

async def set_control(key, value):
    await db_execute("INSERT INTO control (key, value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))

async def get_control(key, default=None):
    row = await db_execute("SELECT value FROM control WHERE key=?", (key,), fetch="one")
    return row[0] if row else default

async def is_msg_in_db(msg_chat_id, msg_id):
    row = await db_execute("SELECT status FROM jobs WHERE msg_chat_id=? AND msg_id=?", (msg_chat_id, msg_id), fetch="one")
    return bool(row)

# ---- playlist helpers -------------------------------------------------
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

# Round-robin scheduler state: jobs that have been "approved" (pilot passed,
# user hit Continue) but not yet handed to a download worker. Kept per
# playlist so one huge playlist can never starve another.
playlist_queues: dict[str, deque] = {}
playlist_order: deque = deque()

# VK-side dedupe cache: album_id -> set of video titles already on VK.
vk_video_title_cache: dict[int, set] = {}

# Engine lifecycle: RUNNING -> PAUSE_REQUESTED -> (draining) -> PAUSED -> RUNNING
ENGINE_RUNNING = "RUNNING"
ENGINE_PAUSE_REQUESTED = "PAUSE_REQUESTED"
ENGINE_PAUSED = "PAUSED"
engine_state = ENGINE_RUNNING

pause_event = asyncio.Event()
pause_event.set()

def free_space_gb(path=DOWNLOAD_DIR):
    _, _, free = shutil.disk_usage(path)
    return free / (1024 ** 3)

def vk_title_for(album_name, idx):
    return f"{album_name} - P{idx}"

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

class ProgressFileReader(io.IOBase):
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

    def tell(self):
        return self.f.tell()

    def seek(self, offset, whence=io.SEEK_SET):
        return self.f.seek(offset, whence)

    def fileno(self):
        return self.f.fileno()

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
# ASYNC NATIVE FAST DOWNLOADER
# ============================================================
async def async_fast_download(client, message, file_path, progress_callback, job_id):
    file_size = message.video.file_size
    downloaded = 0

    def write_chunk(chunk, f):
        f.write(chunk)

    with open(file_path, "wb") as f:
        async for chunk in client.stream_media(message):
            if job_id in cancelled_jobs:
                raise Exception("ForceAbort")

            await asyncio.to_thread(write_chunk, chunk, f)
            downloaded += len(chunk)
            progress_callback(downloaded, file_size)

    return file_path

# ============================================================
# JOB COMPLETION / PLAYLIST BOOKKEEPING
# ============================================================
async def on_job_finished(job):
    """Called once a job reaches a terminal DONE state."""
    playlist_id = job.get('playlist_id')
    await delete_job_row(job['job_id'])
    if not playlist_id:
        return

    if job.get('is_pilot'):
        # Pilot succeeded -> pause the playlist and wait for user confirmation.
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
# SCHEDULER (round-robin feed into download_queue)
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
    # Uploads are never interrupted by pause: an active upload always finishes.
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None)) as session:
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

                active_jobs[up_key] = {"name": display_name, "action": "📤 UP", "progress": 0, "speed": "0 MB/s", "eta": "Calc...", "start_time": time.time(), "job_id": job_id}
                rich_task = progress_ui.add_task(f"[magenta]📤 UP {display_name}", total=100, name=display_name)
                await update_job_status(job_id, "uploading")

                title = vk_title_for(job['album_name'], job['idx'])
                upload_info = await asyncio.to_thread(vk.video.save, name=title, description=job.get('caption', ''), album_id=job['album_id'])

                def up_progress(current, total):
                    if job_id in cancelled_jobs: raise Exception("ForceAbort")
                    update_metrics(up_key, rich_task, "📤 UP", current, total)

                reader = ProgressFileReader(file_path, up_progress)
                data = aiohttp.FormData()
                data.add_field('video_file', reader, filename=os.path.basename(file_path))

                async with session.post(upload_info['upload_url'], data=data) as resp:
                    await resp.json()
                reader.close()

                vk_video_title_cache.setdefault(job['album_id'], set()).add(title)
                await update_job_status(job_id, "done")
                delete_file_on_exit = True
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

    scheduled_pending = sum(len(v) for v in playlist_queues.values())
    queued_dl = download_queue.qsize() + scheduled_pending
    staged_up = upload_queue.qsize()
    active_dls = [j for j in active_jobs.values() if j['action'] == "📥 DL"]
    active_ups = [j for j in active_jobs.values() if j['action'] == "📤 UP"]

    text = ""
    buttons = []

    if ui_state == "MAIN":
        text = (f"📊 **GLOBAL TRANSFER ENGINE**\n{_engine_banner()} | 💾 Free Disk: {free_space_gb():.1f} GB\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📥 **Downloading:** {len(active_dls)} Active | {queued_dl} Queued\n"
                f"📦 **Staged on Disk:** {staged_up} Files\n"
                f"📤 **Uploading:** {len(active_ups)} Active\n"
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
        buttons.append([InlineKeyboardButton("📋 All Playlists", callback_data="ui_PLAYLISTS")])
        buttons.append([
            InlineKeyboardButton("▶️ Resume" if engine_state != ENGINE_RUNNING else "⏸️ Pause", callback_data="toggle_pause"),
            InlineKeyboardButton("🛑 Clear All Pendings", callback_data="clear_queue")
        ])

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

                    # Sort to guarantee Index 1, 2, 3... order
                    album_msgs = sorted(album_msgs, key=lambda x: x.id)

                    # 1. Locate the Master Index (first caption starting with #)
                    master_caption = ""
                    for am in album_msgs:
                        if am.caption and am.caption.strip().startswith("#"):
                            master_caption = am.caption
                            break

                    # 2. Apply the Top-Down vs Inline Logic
                    track_data = {}
                    if master_caption:
                        lines = [line.strip() for line in master_caption.strip().split('\n') if line.strip()]

                        # Scenario 2: Top-Down Match (Check the very first line)
                        is_top_match = False
                        if lines and state['query'].lower() in lines[0].lower():
                            is_top_match = True

                        # Parse all subsequent lines
                        for line in lines[1:]:
                            match = re.match(r'^(\d+)\s*-\s*(.*)', line)
                            if match:
                                idx_str, rest_of_line = match.groups()

                                # Scenario 1: Inline Match (if Top-Down failed)
                                is_inline_match = state['query'].lower() in line.lower()

                                # If either rule passes, extract the data
                                if is_top_match or is_inline_match:

                                    # Parse Caption: Text inside () or fallback to the whole line
                                    bracket_match = re.search(r"\(([^)]*)\)", rest_of_line)
                                    track_caption = bracket_match.group(1).strip() if bracket_match else rest_of_line.strip()

                                    # Save it into a dictionary mapped by the video number (idx)
                                    track_data[int(idx_str)] = track_caption

                    # 3. Filter the actual videos using our parsed track_data
                    for i, am in enumerate(album_msgs, start=1):
                        if not am.video:
                            continue

                        # If a master index existed, only include videos that passed the logic
                        if master_caption:
                            if i in track_data:
                                am._custom_album = state['query'].replace("#", "")
                                am._custom_caption = track_data[i]
                                am._relative_idx = i
                                raw_found.append(am)
                        else:
                            # Fallback if no Master Index was found at all
                            am._custom_album = state['query'].replace("#", "")
                            am._custom_caption = am.caption or f"Imported ({state['query']})"
                            am._relative_idx = i
                            raw_found.append(am)

            elif msg.video:
                # Single video fallback (not an album)
                msg._custom_album = state['query'].replace("#", "")
                msg._custom_caption = msg.caption or f"Imported ({state['query']})"
                msg._relative_idx = 1
                raw_found.append(msg)

    except Exception as e:
        user_states.pop(chat_id, None)
        return await status_msg.edit_text(f"❌ Error accessing group: `{e}`")

    # Deduplication logic (Telegram-side: same message already processed before)
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
    global engine_state, ui_state
    chat_id = callback.message.chat.id
    data = callback.data

    if data.startswith("ui_"):
        ui_state = data.replace("ui_", "")
        await render_dashboard()
        return await callback.answer()

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

        # Resolve/create the VK album once for the whole playlist.
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

        # Skip anything that already exists on VK (dedupe against VK, not just Telegram).
        non_dupe_msgs, skipped_dupes = [], 0
        for msg in state['found_msgs']:
            idx = getattr(msg, '_relative_idx', 1)
            title = vk_title_for(album_name, idx)
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
        rest_msgs = non_dupe_msgs[1:]

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
            else:
                # Stays WAITING in SQLite until the user hits "Continue".
                pass

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
                await update_job_status(job['job_id'], "downloaded")
                await upload_queue.put(job)
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

    # Rebuild VK title caches for any albums with active playlists.
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
