import os
import re
import math
import time
import sqlite3
import asyncio
import aiohttp
import vk_api
import psutil
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

import config

# ============================================================
# CONFIGURATION & INITIALIZATION
# ============================================================
# Ensure config.py has: API_ID, API_HASH, BOT_MONITOR, VK_TOKEN, TARGET_GROUP_ID
DL_WORKERS = 3
UP_WORKERS = 3
CHUNK_SIZE = 1024 * 1024 # 1 MB chunks for safe truncation
DB_PATH = "SysCache/monitor_queue.db"
DOWNLOAD_DIR = "SysCache/vk_downloads"

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

try:
    vk_session = vk_api.VkApi(token=config.VK_TOKEN)
    vk = vk_session.get_api()
    my_vk_id = vk_session.method('users.get')[0]['id']
    print(f"✅ VK Connected: {my_vk_id}")
except Exception as e:
    print(f"❌ Failed to connect to VK: {e}")
    exit(1)

bot_app = Client("monitor_bot", api_id=config.API_ID, api_hash=config.API_HASH, bot_token=config.BOT_MONITOR)
user_app = Client("user_session", api_id=config.API_ID, api_hash=config.API_HASH, max_concurrent_transmissions=10)

# ============================================================
# DATABASE SETUP (WAL MODE)
# ============================================================
def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def _init_db():
    conn = _connect()
    # Staging area for discovered media awaiting manual push
    conn.execute("""
        CREATE TABLE IF NOT EXISTS staging (
            msg_id INTEGER PRIMARY KEY, chat_id INTEGER, hashtag TEXT, caption TEXT
        )
    """)
    # Active execution queue
    conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY, hashtag TEXT, msg_id INTEGER, chat_id INTEGER,
            album_id INTEGER, status TEXT, file_path TEXT, file_size INTEGER, caption TEXT
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

async def get_control(key):
    row = await db_execute("SELECT value FROM control WHERE key=?", (key,), fetch="one")
    return row[0] if row else None

async def set_control(key, value):
    await db_execute("INSERT INTO control (key, value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))

# ============================================================
# STATE MANAGEMENT
# ============================================================
download_queue = asyncio.Queue()
upload_queue = asyncio.Queue()
active_metrics = {}

# ============================================================
# DOWNLOAD ENGINE (WITH CRASH RESUME)
# ============================================================
async def safe_resume_download(client, message, file_path, job_id, total_size):
    """Truncates partially downloaded file to the nearest MB and resumes."""
    downloaded_bytes = 0
    start_chunk = 0

    if os.path.exists(file_path):
        current_size = os.path.getsize(file_path)
        if current_size == total_size:
            return file_path
        elif current_size > 0:
            # Truncate to nearest MB boundary to prevent corrupted chunk merging
            clean_chunks = current_size // CHUNK_SIZE
            downloaded_bytes = clean_chunks * CHUNK_SIZE
            start_chunk = clean_chunks
            
            with open(file_path, "r+b") as f:
                f.truncate(downloaded_bytes)

    active_metrics[job_id] = {"action": "📥 DL", "progress": (downloaded_bytes/total_size)*100 if total_size else 0, "speed": "0 MB/s", "start": time.time()}
    
    with open(file_path, "ab" if downloaded_bytes > 0 else "wb") as f:
        async for chunk in client.stream_media(message, offset=start_chunk):
            f.write(chunk)
            downloaded_bytes += len(chunk)
            
            elapsed = time.time() - active_metrics[job_id]["start"]
            speed = downloaded_bytes / elapsed if elapsed > 0 else 0
            active_metrics[job_id]["progress"] = (downloaded_bytes / total_size) * 100
            active_metrics[job_id]["speed"] = f"{speed / (1024*1024):.1f} MB/s"

    return file_path

async def download_worker():
    while True:
        job = await download_queue.get()
        job_id = job['job_id']
        file_path = os.path.join(DOWNLOAD_DIR, f"{job_id}.mp4")
        
        try:
            await db_execute("UPDATE jobs SET status='Downloading' WHERE job_id=?", (job_id,))
            msg = await user_app.get_messages(job['chat_id'], job['msg_id'])
            
            await safe_resume_download(user_app, msg, file_path, job_id, msg.video.file_size)
            
            await db_execute("UPDATE jobs SET status='Staged', file_path=? WHERE job_id=?", (file_path, job_id))
            job['file_path'] = file_path
            await upload_queue.put(job)
        except Exception as e:
            print(f"DL Error on {job_id}: {e}")
            await db_execute("UPDATE jobs SET status='Failed' WHERE job_id=?", (job_id,))
        finally:
            active_metrics.pop(job_id, None)
            download_queue.task_done()

# ============================================================
# UPLOAD ENGINE (WITH VK WATERMARKING)
# ============================================================
class ProgressReader:
    def __init__(self, filename, job_id):
        self.f = open(filename, 'rb')
        self.total = os.path.getsize(filename)
        self.job_id = job_id
        self.read_bytes = 0
        active_metrics[job_id] = {"action": "📤 UP", "progress": 0, "speed": "0 MB/s", "start": time.time()}

    def read(self, size=-1):
        chunk = self.f.read(size)
        self.read_bytes += len(chunk)
        elapsed = time.time() - active_metrics[self.job_id]["start"]
        speed = self.read_bytes / elapsed if elapsed > 0 else 0
        active_metrics[self.job_id]["progress"] = (self.read_bytes / self.total) * 100
        active_metrics[self.job_id]["speed"] = f"{speed / (1024*1024):.1f} MB/s"
        return chunk
    
    def __getattr__(self, attr):
        return getattr(self.f, attr)

async def upload_worker():
    async with aiohttp.ClientSession() as session:
        while True:
            job = await upload_queue.get()
            job_id, file_path = job['job_id'], job['file_path']
            
            try:
                await db_execute("UPDATE jobs SET status='Uploading' WHERE job_id=?", (job_id,))
                
                # WATERMARKING: Inject Telegram msg_id into description for crash-proof deduplication
                vk_description = f"{job['caption']}\n\n[TG_ID: {job['msg_id']}]"
                vk_title = f"{job['hashtag']} - {job['msg_id']}"
                
                upload_info = await asyncio.to_thread(
                    vk.video.save, 
                    name=vk_title[:100], 
                    description=vk_description, 
                    album_id=job['album_id']
                )
                
                reader = ProgressReader(file_path, job_id)
                data = aiohttp.FormData()
                data.add_field('video_file', reader, filename=os.path.basename(file_path))
                
                async with session.post(upload_info['upload_url'], data=data) as resp:
                    await resp.json()
                
                reader.close()
                os.remove(file_path)
                await db_execute("DELETE FROM jobs WHERE job_id=?", (job_id,))
            except Exception as e:
                print(f"UP Error on {job_id}: {e}")
                await db_execute("UPDATE jobs SET status='Failed' WHERE job_id=?", (job_id,))
            finally:
                active_metrics.pop(job_id, None)
                upload_queue.task_done()

# ============================================================
# DASHBOARD UI ENGINE
# ============================================================
async def render_dashboard():
    chat_id = await get_control("dash_chat")
    msg_id = await get_control("dash_msg")
    if not chat_id or not msg_id: return

    # Get Staging Counts
    staging_rows = await db_execute("SELECT hashtag, COUNT(msg_id) FROM staging GROUP BY hashtag", fetch="all")
    
    text = (f"📊 **VK MONITOR DASHBOARD**\n"
            f"💻 CPU: `{psutil.cpu_percent()}%` | 💾 Free Disk: `{psutil.disk_usage(DOWNLOAD_DIR).free / (1024**3):.1f} GB`\n"
            f"━━━━━━━━━━━━━━━━━━\n")
    
    buttons = []
    if staging_rows:
        text += "📁 **READY TO QUEUE:**\n"
        for hashtag, count in staging_rows:
            text += f"• `{hashtag}` - {count} videos\n"
            buttons.append([InlineKeyboardButton(f"▶️ Upload {hashtag} ({count})", callback_data=f"push_{hashtag}")])
        text += "━━━━━━━━━━━━━━━━━━\n"
    else:
        text += "📁 **READY TO QUEUE:** None\n━━━━━━━━━━━━━━━━━━\n"

    # Active Metrics
    dls = [m for m in active_metrics.values() if m['action'] == '📥 DL']
    ups = [m for m in active_metrics.values() if m['action'] == '📤 UP']
    
    text += f"📥 **ACTIVE DOWNLOADS ({len(dls)}):**\n"
    for m in dls: text += f"↳ {m['progress']:.1f}% | {m['speed']}\n"
    
    text += f"\n📤 **ACTIVE UPLOADS ({len(ups)}):**\n"
    for m in ups: text += f"↳ {m['progress']:.1f}% | {m['speed']}\n"
    
    buttons.append([InlineKeyboardButton("🔄 Refresh Dashboard", callback_data="refresh")])

    try:
        await bot_app.edit_message_text(chat_id=int(chat_id), message_id=int(msg_id), text=text, reply_markup=InlineKeyboardMarkup(buttons))
    except Exception:
        pass

async def dashboard_loop():
    while True:
        await asyncio.sleep(3)
        await render_dashboard()

# ============================================================
# BOT HANDLERS & PASSIVE MONITOR
# ============================================================
@bot_app.on_message(filters.command("start") & filters.private)
async def start_cmd(client, message):
    msg = await message.reply_text("⚙️ Booting Dashboard...")
    await set_control("dash_chat", message.chat.id)
    await set_control("dash_msg", msg.id)

@bot_app.on_message(filters.chat(config.TARGET_GROUP_ID) & filters.video)
async def group_scanner(client, message):
    """Passively scans the target group and stages videos based on hashtags."""
    caption = message.caption or ""
    match = re.search(r'(#\w+)', caption)
    if match:
        hashtag = match.group(1)
        await db_execute("INSERT INTO staging (msg_id, chat_id, hashtag, caption) VALUES (?,?,?,?)", 
                         (message.id, message.chat.id, hashtag, caption))
        # Force a UI update to show the new count
        await render_dashboard()

@bot_app.on_callback_query()
async def callbacks(client, callback):
    data = callback.data
    if data == "refresh":
        await callback.answer("Refreshed")
        return await render_dashboard()
        
    elif data.startswith("push_"):
        hashtag = data.replace("push_", "")
        await callback.answer(f"Fetching VK Source of Truth for {hashtag}...")
        
        # 1. Resolve VK Album
        existing_albums = await asyncio.to_thread(vk.video.getAlbums, owner_id=my_vk_id, count=100)
        album_id = next((alb['id'] for alb in existing_albums['items'] if alb['title'].lower() == hashtag.replace("#", "").lower()), None)
        if not album_id:
            new_album = await asyncio.to_thread(vk.video.addAlbum, title=hashtag.replace("#", ""))
            album_id = new_album if isinstance(new_album, int) else new_album['album_id']

        # 2. Fetch VK Source of Truth (Anti-Duplication)
        try:
            vk_items = await asyncio.to_thread(vk.video.get, owner_id=my_vk_id, album_id=album_id, count=200)
            uploaded_msg_ids = set()
            for video in vk_items.get('items', []):
                desc = video.get('description', '')
                match = re.search(r'\[TG_ID:\s*(\d+)\]', desc)
                if match:
                    uploaded_msg_ids.add(int(match.group(1)))
        except Exception:
            uploaded_msg_ids = set()

        # 3. Move from Staging to Jobs (Filtering Duplicates)
        videos = await db_execute("SELECT msg_id, chat_id, caption FROM staging WHERE hashtag=?", (hashtag,), fetch="all")
        queued_count, skipped_count = 0, 0

        for vid in videos:
            msg_id = vid[0]
            if msg_id in uploaded_msg_ids:
                skipped_count += 1
                continue
                
            job_id = f"{vid[1]}_{msg_id}"
            await db_execute(
                "INSERT INTO jobs (job_id, hashtag, msg_id, chat_id, album_id, status, caption) VALUES (?,?,?,?,?,?,?)",
                (job_id, hashtag, msg_id, vid[1], album_id, 'Waiting', vid[2])
            )
            await download_queue.put({'job_id': job_id, 'chat_id': vid[1], 'msg_id': msg_id, 'album_id': album_id, 'caption': vid[2]})
            queued_count += 1
            
        await db_execute("DELETE FROM staging WHERE hashtag=?", (hashtag,))
        await callback.message.reply_text(f"🚀 Pushed {queued_count} videos! (Skipped {skipped_count} already on VK)")
        await render_dashboard()
        
@bot_app.on_message(filters.command("scan") & filters.private)
async def scan_history_cmd(client, message):
    status_msg = await message.reply_text("🔎 Scanning group history for missed videos... This might take a moment.")
    staged_count = 0
    
    try:
        # We use the user_app to search the group's entire video history
        async for msg in user_app.search_messages(chat_id=config.TARGET_GROUP_ID, filter=filters.video):
            caption = msg.caption or ""
            match = re.search(r'(#\w+)', caption)
            if match:
                hashtag = match.group(1)
                # INSERT OR IGNORE safely skips it if it is already sitting in the staging database
                await db_execute(
                    "INSERT OR IGNORE INTO staging (msg_id, chat_id, hashtag, caption) VALUES (?,?,?,?)", 
                    (msg.id, msg.chat.id, hashtag, caption)
                )
                staged_count += 1
                
        await status_msg.edit_text(f"✅ Scan complete! Found {staged_count} historical videos. Check the dashboard.")
        # Force the dashboard to update with the newly discovered files
        await render_dashboard()
        
    except Exception as e:
        await status_msg.edit_text(f"❌ Error during scan: {e}")

# ============================================================
# RECOVERY & MAIN EXECUTION
# ============================================================
async def main():
    await user_app.start()
    await bot_app.start()

    # Crash Recovery Boot Sequence
    rows = await db_execute("SELECT * FROM jobs", fetch="all")
    if rows:
        print(f"♻️ Recovering {len(rows)} incomplete jobs...")
        for r in rows:
            job = {'job_id': r[0], 'hashtag': r[1], 'msg_id': r[2], 'chat_id': r[3], 'album_id': r[4], 'caption': r[8], 'file_path': r[6]}
            status = r[5]
            
            if status in ("Staged", "Uploading") and job['file_path'] and os.path.exists(job['file_path']):
                await upload_queue.put(job)
            else:
                await download_queue.put(job)

    asyncio.create_task(dashboard_loop())
    for _ in range(DL_WORKERS): asyncio.create_task(download_worker())
    for _ in range(UP_WORKERS): asyncio.create_task(upload_worker())
    
    print("🚀 System Online. Listening to target group and waiting for commands...")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())