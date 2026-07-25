"""
vk_bot.py - Dedicated VK Playlist Downloader Microservice
───────────────────────────────────────────────────────────────
ARCHITECTURE:
  • Standalone Bot Token (VK_BOT_TOKEN) & SQLite DB (vk_scheduler.db)
  • Drip-Feed Orchestrator (Prevents RAM/Loop starvation on 600+ playlist items)
  • Dynamic Caption Injector (#caption support)
  • Accordion Dashboard with Pause/Resume/Cancel for Playlists
───────────────────────────────────────────────────────────────
"""

from __future__ import annotations
import asyncio
import json
import logging
import os
import re
import shutil
import subprocess
import time
import uuid
import sys
import sqlite3
from enum import Enum
from pathlib import Path
import yt_dlp
import aiohttp
from logging.handlers import RotatingFileHandler

from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait, MessageNotModified
import config

# ──────────────────────────── CONFIGURATION ─────────────────────────────

BASE_DIR = Path("SysCache_VK")
LOG_DIR = BASE_DIR / "logs"
DB_PATH = BASE_DIR / "vk_scheduler.db"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(name)s – %(message)s",
    handlers=[
        RotatingFileHandler(LOG_DIR / "vk_engine.log", maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8"), 
        logging.StreamHandler()
    ]
)
log = logging.getLogger("vk_stealth_bot")
logging.getLogger("pyrogram").setLevel(logging.ERROR)

API_ID, API_HASH = config.API_ID, config.API_HASH
BOT_TOKEN = getattr(config, "VK_BOT_TOKEN", config.BOT_TOKEN) # Falls back to BOT_TOKEN if VK_BOT_TOKEN isn't set
CHANNEL_ID = config.CHANNEL_ID
OWNER_ID = int(config.OWNER_ID) if hasattr(config, "OWNER_ID") else 0

VK_COOKIES = None
if os.path.exists("extracted_cookies.txt"):
    with open("extracted_cookies.txt", "r", encoding="utf-8") as f:
        VK_COOKIES = f.read().strip()

JOBS_DIR = BASE_DIR / "jobs"
JOBS_DIR.mkdir(parents=True, exist_ok=True)

MAX_DL_WORKERS = 3  # Keep low to prevent IP bans/throttling from VK

# ──────────────────────────── DATABASE ─────────────────────────────────

class JobScheduler:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.lock = asyncio.Lock()
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''CREATE TABLE IF NOT EXISTS playlists (
                id TEXT PRIMARY KEY, url TEXT, caption TEXT, total INTEGER,
                downloaded INTEGER, status TEXT, chat_id INTEGER
            )''')
            conn.execute('''CREATE TABLE IF NOT EXISTS playlist_items (
                id TEXT PRIMARY KEY, playlist_id TEXT, url TEXT, title TEXT, status TEXT
            )''')
            conn.execute('''CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY, url TEXT, title TEXT, playlist_id TEXT,
                stage TEXT, pct REAL, retries INTEGER, chat_id INTEGER
            )''')

    async def create_playlist(self, pl_id: str, url: str, caption: str, total: int, chat_id: int):
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('INSERT INTO playlists VALUES (?, ?, ?, ?, 0, "active", ?)', (pl_id, url, caption, total, chat_id))

    async def add_playlist_items(self, items: list[tuple]):
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.executemany('INSERT INTO playlist_items VALUES (?, ?, ?, ?, "pending")', items)

    async def get_active_playlists((self) -> list[dict]:
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                return [dict(r) for r in conn.execute('SELECT * FROM playlists WHERE status != "completed"').fetchall()]

    async def get_playlist(self, pl_id: str) -> dict:
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute('SELECT * FROM playlists WHERE id = ?', (pl_id,)).fetchone()
                return dict(row) if row else {}

    async def update_playlist(self, pl_id: str, **kwargs):
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                for k, v in kwargs.items():
                    conn.execute(f'UPDATE playlists SET {k} = ? WHERE id = ?', (v, pl_id))

    async def get_pending_items(self, pl_id: str, limit: int = 3) -> list[dict]:
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                return [dict(r) for r in conn.execute('SELECT * FROM playlist_items WHERE playlist_id = ? AND status = "pending" LIMIT ?', (pl_id, limit)).fetchall()]

    async def update_item_status(self, item_id: str, status: str):
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('UPDATE playlist_items SET status = ? WHERE id = ?', (status, item_id))

    async def create_job(self, data: dict):
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('INSERT INTO jobs VALUES (?, ?, ?, ?, "queued", 0.0, 0, ?)',
                             (data['id'], data['url'], data['title'], data['playlist_id'], data['chat_id']))
        root = JOBS_DIR / f"JOB_{data['id']}"
        for d in (root, root / "dl", root / "enc"): d.mkdir(parents=True, exist_ok=True)

    async def update_job(self, jid: str, **kwargs):
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                for k, v in kwargs.items():
                    conn.execute(f'UPDATE jobs SET {k} = ? WHERE id = ?', (v, jid))

    async def delete_job(self, jid: str):
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('DELETE FROM jobs WHERE id = ?', (jid,))

    async def get_active_jobs(self) -> list[dict]:
        async with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                return [dict(r) for r in conn.execute('SELECT * FROM jobs').fetchall()]

# ──────────────────────────── DRIP-FEED ORCHESTRATOR ───────────────────

async def playlist_drip_feed_loop(db: JobScheduler, dl_q: asyncio.Queue):
    """Monitors active playlists and feeds 2-3 items at a time into the queue."""
    while True:
        await asyncio.sleep(4)
        try:
            active_playlists = await db.get_active_playlists()
            active_jobs = await db.get_active_jobs()

            for pl in active_playlists:
                if pl['status'] == 'paused':
                    continue

                pl_id = pl['id']
                # Count how many jobs are currently downloading/encoding for this playlist
                current_active = len([j for j in active_jobs if j.get('playlist_id') == pl_id])

                # Max 2 concurrent worker tasks per playlist to prevent bandwidth collapse
                if current_active < 2:
                    slots_free = 2 - current_active
                    pending_items = await db.get_pending_items(pl_id, limit=slots_free)

                    for item in pending_items:
                        jid = item['id']
                        await db.create_job({
                            'id': jid, 'url': item['url'], 'title': item['title'],
                            'playlist_id': pl_id, 'chat_id': pl['chat_id']
                        })
                        await db.update_item_status(jid, "processing")
                        await dl_q.put(jid)
        except Exception as e:
            log.error(f"Drip Feed Loop Error: {e}")

# ──────────────────────────── PIPELINE ENGINES ─────────────────────────

class DownloaderEngine:
    def __init__(self, db: JobScheduler):
        self.db = db

    async def execute(self, job: dict):
        jid, url = job['id'], job['url']
        dl_dir = JOBS_DIR / f"JOB_{jid}" / "dl"

        cookie_path = dl_dir / f"{jid}_cookies.txt"
        if VK_COOKIES:
            with open(cookie_path, "w", encoding="utf-8") as f:
                f.write("# Netscape HTTP Cookie File\n")
                for item in VK_COOKIES.strip().split(';'):
                    if '=' in item:
                        k, v = item.strip().split('=', 1)
                        f.write(f".vk.com\tTRUE\t/\tTRUE\t2147483647\t{k}\t{v}\n")
                        f.write(f".vkvideo.ru\tTRUE\t/\tTRUE\t2147483647\t{k}\t{v}\n")

        def prog_hook(d):
            if d.get("status") == "downloading":
                try:
                    pct_str = re.sub(r"\x1b[^m]*m", "", d.get("_percent_str", "0.0%")).strip()
                    val = float(re.search(r"[\d.]+", pct_str).group()) if re.search(r"[\d.]+", pct_str) else 0.0
                    asyncio.run_coroutine_threadsafe(self.db.update_job(jid, pct=val, stage="downloading"), loop)
                except Exception: pass

        opts = {
            "outtmpl": str(dl_dir / f"{jid}.%(ext)s"),
            "format": "bestvideo[height<=1080]+bestaudio/best",
            "merge_output_format": "mp4",
            "progress_hooks": [prog_hook],
            "quiet": True, "noprogress": True,
            "cookiefile": str(cookie_path) if VK_COOKIES else None
        }

        await asyncio.to_thread(self._run_ytdlp, url, opts)

    def _run_ytdlp(self, url: str, opts: dict):
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.extract_info(url, download=True)

class EncoderEngine:
    async def execute(self, job: dict):
        jid = job['id']
        dl_dir, enc_dir = JOBS_DIR / f"JOB_{jid}" / "dl", JOBS_DIR / f"JOB_{jid}" / "enc"
        files = [f for f in dl_dir.rglob("*") if f.is_file() and f.suffix.lower() in [".mp4", ".mkv", ".ts", ".webm"]]
        if not files: raise RuntimeError("No downloaded media found.")

        src = max(files, key=lambda p: p.stat().st_size)
        dst = enc_dir / f"{jid}.mp4"

        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-i", str(src), "-c", "copy", "-movflags", "+faststart", str(dst),
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        await proc.wait()

class UploaderEngine:
    def __init__(self, db: JobScheduler, app: Client):
        self.db = db
        self.app = app

    async def execute(self, job: dict):
        jid = job['id']
        enc_file = JOBS_DIR / f"JOB_{jid}" / "enc" / f"{jid}.mp4"
        if not enc_file.exists(): raise RuntimeError("Encoded payload missing.")

        pl = await self.db.get_playlist(job['playlist_id'])
        caption = f"{pl['caption']}\n\n**{job['title']}**" if pl and pl.get('caption') else f"**{job['title']}**"

        await self.app.send_video(
            chat_id=CHANNEL_ID,
            video=str(enc_file),
            caption=caption,
            supports_streaming=True
        )

        # Increment downloaded counter on Playlist master record
        if pl:
            new_count = pl['downloaded'] + 1
            status = "completed" if new_count >= pl['total'] else pl['status']
            await self.db.update_playlist(pl['id'], downloaded=new_count, status=status)
            await self.db.update_item_status(jid, "done")

        await self.db.delete_job(jid)
        shutil.rmtree(JOBS_DIR / f"JOB_{jid}", ignore_errors=True)

# ──────────────────────────── DASHBOARD & ROUTER ───────────────────────

_dash_msg_id, _dash_chat_id = 0, 0

def make_bar(percent: float, width: int = 10) -> str:
    filled = int(max(0.0, min(percent, 100.0)) / (100.0 / width))
    return "█" * filled + "░" * (width - filled)

async def render_dashboard(db: JobScheduler) -> tuple[str, InlineKeyboardMarkup]:
    playlists = await db.get_active_playlists()
    active_jobs = await db.get_active_jobs()

    text = (
        f"🤖 **VK PLAYLIST MAINFRAME**\n"
        f"`━━━━━━━━━━━━━━━━━━━━━━━━━━`\n"
        f"`[🎵] ACTIVE PLAYLISTS :` `{len(playlists)}`\n"
        f"`[⚡] RUNNING WORKERS  :` `{len(active_jobs)}`\n"
        f"`━━━━━━━━━━━━━━━━━━━━━━━━━━`"
    )

    kb = []
    if not playlists:
        kb.append([InlineKeyboardButton("└ System Idle (Send /vk)", callback_data="noop")])
    else:
        for pl in playlists:
            status_icon = "⏸" if pl['status'] == "paused" else "▶️"
            pct = (pl['downloaded'] / pl['total'] * 100) if pl['total'] > 0 else 0
            
            kb.append([InlineKeyboardButton(f"{status_icon} {pl['caption'][:15] or 'Playlist'} [{pl['downloaded']}/{pl['total']}]", callback_data="noop")])
            kb.append([InlineKeyboardButton(f" Progress: [{make_bar(pct, 8)}] {pct:.1f}%", callback_data="noop")])
            
            if pl['status'] == "paused":
                kb.append([
                    InlineKeyboardButton("▶️ RESUME", callback_data=f"res|{pl['id']}"),
                    InlineKeyboardButton("❌ CANCEL", callback_data=f"kill|{pl['id']}")
                ])
            else:
                kb.append([
                    InlineKeyboardButton("⏸ PAUSE", callback_data=f"pause|{pl['id']}"),
                    InlineKeyboardButton("❌ CANCEL", callback_data=f"kill|{pl['id']}")
                ])
            kb.append([InlineKeyboardButton("───────────────────", callback_data="noop")])

    kb.append([InlineKeyboardButton("🔄 REFRESH", callback_data="refresh")])
    return text, InlineKeyboardMarkup(kb)

def setup_router(app: Client, db: JobScheduler, dl_q: asyncio.Queue, enc_q: asyncio.Queue, up_q: asyncio.Queue):

    @app.on_message(filters.command(["vk"]) & filters.user(OWNER_ID))
    async def cmd_vk(_, msg: Message):
        raw = msg.text.replace("/vk", "").strip()
        if not raw:
            return await msg.reply("⚠️ **Usage:** `/vk <url> #caption`")

        parts = raw.split("#", 1)
        url = parts[0].strip()
        caption = f"#{parts[1].strip()}" if len(parts) > 1 else "#VK_Playlist"

        m = await msg.reply("🔍 `Scanning VK Playlist structure...`")

        def extract():
            opts = {'extract_flat': True, 'quiet': True}
            with yt_dlp.YoutubeDL(opts) as ydl:
                return ydl.extract_info(url, download=False)

        try:
            data = await asyncio.to_thread(extract)
            entries = data.get('entries', [])
            if not entries:
                return await m.edit("❌ No videos found in playlist link.")

            pl_id = str(uuid.uuid4())[:8]
            await db.create_playlist(pl_id, url, caption, len(entries), msg.chat.id)

            items = []
            for e in entries:
                v_url = e.get('url') or e.get('webpage_url')
                if v_url:
                    items.append((str(uuid.uuid4())[:8], pl_id, v_url, e.get('title', 'VK Video')))

            await db.add_playlist_items(items)
            await m.edit(f"✅ **PLAYLIST LOCKED**\nFound `{len(items)}` videos.\nDrip-feeding queued.")
        except Exception as e:
            await m.edit(f"❌ Extraction error: `{e}`")

    @app.on_message(filters.command(["vk_dash"]) & filters.user(OWNER_ID))
    async def cmd_dash(_, msg: Message):
        global _dash_msg_id, _dash_chat_id
        text, kb = await render_dashboard(db)
        m = await msg.reply(text, reply_markup=kb)
        _dash_msg_id, _dash_chat_id = m.id, m.chat.id

    @app.on_callback_query()
    async def handle_callbacks(_, cb: CallbackQuery):
        if cb.data == "noop": return await cb.answer()

        if cb.data == "refresh":
            text, kb = await render_dashboard(db)
            try: await cb.message.edit_text(text, reply_markup=kb)
            except Exception: pass
            return await cb.answer("Refreshed.")

        if cb.data.startswith("pause|"):
            pl_id = cb.data.split("|")[1]
            await db.update_playlist(pl_id, status="paused")
            await cb.answer("Playlist Paused.", show_alert=True)

        elif cb.data.startswith("res|"):
            pl_id = cb.data.split("|")[1]
            await db.update_playlist(pl_id, status="active")
            await cb.answer("Playlist Resumed.", show_alert=True)

        elif cb.data.startswith("kill|"):
            pl_id = cb.data.split("|")[1]
            await db.update_playlist(pl_id, status="completed")
            await cb.answer("Playlist Terminated.", show_alert=True)

        text, kb = await render_dashboard(db)
        try: await cb.message.edit_text(text, reply_markup=kb)
        except Exception: pass

# ──────────────────────────── WORKER LOOPS & BOOTSTRAP ─────────────────

async def worker_pipeline(db: JobScheduler, dl_q: asyncio.Queue, enc_q: asyncio.Queue, up_q: asyncio.Queue, app: Client):
    dl_engine = DownloaderEngine(db)
    enc_engine = EncoderEngine()
    up_engine = UploaderEngine(db, app)

    async def dl_worker():
        while True:
            jid = await dl_q.get()
            job = (await db.get_active_jobs())
            j_data = next((j for j in job if j['id'] == jid), None)
            if j_data:
                try:
                    await dl_engine.execute(j_data)
                    await db.update_job(jid, stage="downloaded")
                    await enc_q.put(jid)
                except Exception as e:
                    log.error(f"DL Error {jid}: {e}")
                    await db.delete_job(jid)
            dl_q.task_done()

    async def enc_worker():
        while True:
            jid = await enc_q.get()
            job = (await db.get_active_jobs())
            j_data = next((j for j in job if j['id'] == jid), None)
            if j_data:
                try:
                    await enc_engine.execute(j_data)
                    await db.update_job(jid, stage="encoded")
                    await up_q.put(jid)
                except Exception as e:
                    log.error(f"Enc Error {jid}: {e}")
                    await db.delete_job(jid)
            enc_q.task_done()

    async def up_worker():
        while True:
            jid = await up_q.get()
            job = (await db.get_active_jobs())
            j_data = next((j for j in job if j['id'] == jid), None)
            if j_data:
                try:
                    await up_engine.execute(j_data)
                except Exception as e:
                    log.error(f"UP Error {jid}: {e}")
                    await db.delete_job(jid)
            up_q.task_done()

    for _ in range(2): asyncio.create_task(dl_worker())
    asyncio.create_task(enc_worker())
    asyncio.create_task(up_worker())

async def main():
    app = Client("vk_stealth_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
    db = JobScheduler(DB_PATH)

    dl_q, enc_q, up_q = asyncio.Queue(), asyncio.Queue(), asyncio.Queue()
    setup_router(app, db, dl_q, enc_q, up_q)

    async with app:
        log.info("VK Playlist Bot Online.")
        asyncio.create_task(playlist_drip_feed_loop(db, dl_q))
        asyncio.create_task(worker_pipeline(db, dl_q, enc_q, up_q, app))

        if OWNER_ID:
            await app.send_message(OWNER_ID, "🟢 **VK Playlist Bot Online.** Send `/vk_dash` to launch dashboard.")

        while True:
            await asyncio.sleep(3600)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try: loop.run_until_complete(main())
    except KeyboardInterrupt: sys.exit(0)