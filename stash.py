import os
import time
import struct
import sqlite3
import asyncio
import aiohttp
from pyrogram import Client
from pyrogram.errors import FloodWait
from pyrogram.raw.functions.channels import CreateForumTopic

import config  # Imports your existing config.py file

# ============================================================
# CONFIGURATION & CONSTANTS
# ============================================================
STASH_GRAPHQL_URL = "http://localhost:9999/graphql" 
STASH_API_KEY = getattr(config, "STASH_API_KEY", "")                                  
DB_PATH = "SysCache/stash_queue.db"
TRANSFER_STAGGER_SECONDS = 2.0

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# ============================================================
# SQLITE PERSISTENCE LEDGER (CRASH RECOVERY)
# ============================================================
def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_stash_db():
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stash_queue (
            message_id INTEGER PRIMARY KEY,
            chat_id INTEGER,
            file_unique_id TEXT,
            oshash TEXT,
            performers TEXT,
            status TEXT DEFAULT 'PENDING',
            updated_at REAL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS performer_topics (
            performer_name TEXT PRIMARY KEY,
            topic_id INTEGER,
            created_at REAL
        )
    """)
    conn.commit()
    conn.close()

init_stash_db()

# ============================================================
# PROGRESS BAR & DASHBOARD UI
# ============================================================
def generate_progress_bar(current, total, length=15):
    if total == 0:
        return "░" * length
    percentage = current / total
    filled = int(length * percentage)
    return "█" * filled + "░" * (length - filled)

async def live_dashboard_updater(bot_app: Client, status_msg):
    """Background loop that updates the Telegram status message with live DB stats."""
    while True:
        try:
            conn = get_db()
            cur = conn.cursor()
            
            cur.execute("SELECT COUNT(*) FROM stash_queue")
            total = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM stash_queue WHERE status = 'PENDING'")
            pending = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM stash_queue WHERE status = 'COPIED'")
            copied = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM stash_queue WHERE status = 'ERROR'")
            errors = cur.fetchone()[0]
            
            conn.close()

            processed = total - pending
            percentage = (processed / total) * 100 if total > 0 else 0
            bar = generate_progress_bar(processed, total)

            dashboard_text = (
                f"🚀 **Stash Performer Router is Active**\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🗂️ **Total Videos Indexed:** `{total}`\n"
                f"⏳ **Pending in Queue:** `{pending}`\n\n"
                f"📊 **Progress:** {percentage:.1f}%\n"
                f"`[{bar}]`\n\n"
                f"✅ **Successfully Copied:** `{copied}`\n"
                f"⚠️ **Errors / Skipped:** `{errors}`\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"💡 _Safe to reboot at any time. The script remembers where it left off._"
            )

            await status_msg.edit_text(dashboard_text)
            await asyncio.sleep(5.0)  
            
        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(5.0)

# ============================================================
# OSHASH STREAMING CALCULATOR (FIXED BYTE-MATH)
# ============================================================
async def calculate_tg_oshash(client: Client, message):
    media = message.video or message.document
    if not media or getattr(media, "file_size", 0) < 131072:
        return None

    file_id = media.file_id
    file_size = media.file_size
    chunk_size = 65536 

    try:
        # Get head (First 64KB)
        head_buffer = bytearray()
        async for chunk in client.stream_media(file_id, limit=1, offset=0):
            head_buffer.extend(chunk)
            if len(head_buffer) >= chunk_size: 
                break
        head_bytes = bytes(head_buffer[:chunk_size])

        # Get tail (Last 64KB)
        # Pyrogram yields chunks of 1048576 bytes (1MB). 
        # We fetch the last two chunks to guarantee we get the absolute end of the file.
        last_chunk_idx = (file_size - 1) // 1048576
        start_chunk_idx = max(0, last_chunk_idx - 1) 
        
        tail_buffer = bytearray()
        async for chunk in client.stream_media(file_id, limit=3, offset=start_chunk_idx):
            tail_buffer.extend(chunk)

        if len(tail_buffer) < chunk_size: 
            return None
            
        # Accurately slice the exact last 64KB of the true file end
        tail_bytes = bytes(tail_buffer[-chunk_size:])

        # Calculate OSHASH
        hash_val = file_size
        for i in range(0, chunk_size, 8):
            head_val = struct.unpack('<Q', head_bytes[i:i+8])[0]
            tail_val = struct.unpack('<Q', tail_bytes[i:i+8])[0]
            hash_val = (hash_val + head_val + tail_val) & 0xFFFFFFFFFFFFFFFF

        return f"{hash_val:016x}"
    except Exception as e:
        print(f"❌ [OSHASH Error]: {e}")
        return None

# ============================================================
# STASH GRAPHQL HYBRID MATCHER (WITH ERROR ABORT)
# ============================================================
class StashDBError(Exception):
    """Custom exception to halt processing if StashDB fails."""
    pass

async def query_graphql(url: str, query: str, variables: dict = None, api_key: str = ""):
    headers = {"Content-Type": "application/json"}
    if api_key: 
        headers["ApiKey"] = api_key
        
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json={"query": query, "variables": variables or {}}, headers=headers, timeout=10) as resp:
                
                # Check for authentication or server errors BEFORE falling back
                if resp.status != 200:
                    text_response = await resp.text()
                    print(f"\n❌ [StashDB API Error] HTTP {resp.status}: {text_response.strip()}")
                    if resp.status == 401:
                        print("👉 FIX: Your STASHDB_API_KEY in config.py is invalid or missing.")
                    # 🚨 Raising this error stops the script from dumping to 'Unmatched' 🚨
                    raise StashDBError(f"HTTP {resp.status} - API Request Failed")

                data = await resp.json()
                
                # Catch internal GraphQL schema errors
                if "errors" in data:
                    err_msg = data['errors'][0].get('message', 'Unknown Error')
                    print(f"\n❌ [StashDB GraphQL Error]: {err_msg}")
                    raise StashDBError(f"GraphQL Error: {err_msg}")
                    
                return data.get("data")
                
    except aiohttp.ClientError as e:
        print(f"\n❌ [Network Error talking to StashDB]: {e}")
        raise StashDBError(f"Network Connection Failed")

STASHDB_GRAPHQL_URL = "https://stashdb.org/graphql"
STASHDB_API_KEY = getattr(config, "STASHDB_API_KEY", "")

async def find_performers_hybrid(client: Client, message):
    """
    1. Tries OSHASH matching via remote StashDB.org
    2. Falls back to Title/Caption text matching via remote StashDB.org
    """
    oshash = await calculate_tg_oshash(client, message)
    
    # 1. OSHASH Fingerprint Lookup (StashDB direct)
    if oshash:
        gql_hash = """
        query FindByHash($hash: String!) {
          findScenesByFingerprints(fingerprints: [{hash: $hash, algorithm: OSHASH}]) {
            performers { performer { name } }
          }
        }
        """
        res = await query_graphql(STASHDB_GRAPHQL_URL, gql_hash, {"hash": oshash}, STASHDB_API_KEY)
        
        if res and res.get("findScenesByFingerprints"):
            scenes = res["findScenesByFingerprints"]
            if scenes and scenes[0].get("performers"):
                performers = [p["performer"]["name"] for p in scenes[0]["performers"]]
                if performers: 
                    return performers, oshash

    # 2. Text Search Fallback (StashDB direct)
    file_name = message.video.file_name if message.video else (message.document.file_name if message.document else "")
    caption_text = message.caption or message.text or ""
    # Try to extract a clean search term (first line of caption or filename without extension)
    search_term = os.path.splitext(file_name)[0] if file_name else caption_text.split('\n')[0]

    if search_term:
        gql_text = """
        query TextSearch($q: String!) {
          queryScenes(input: {title: $q, page: 1, per_page: 1}) {
            scenes {
              performers { performer { name } }
            }
          }
        }
        """
        res = await query_graphql(STASHDB_GRAPHQL_URL, gql_text, {"q": search_term}, STASHDB_API_KEY)
        
        if res and res.get("queryScenes", {}).get("scenes"):
            scenes = res["queryScenes"]["scenes"]
            if scenes and scenes[0].get("performers"):
                performers = [p["performer"]["name"] for p in scenes[0]["performers"]]
                if performers: 
                    return performers, oshash

    # 3. Default Fallback
    return ["Unmatched Stash Videos"], oshash

# ============================================================
# FORUM TOPIC RESOLVER & PUBLIC SUPERGROUP HANDLER
# ============================================================
async def resolve_performer_topic(user_app: Client, master_forum_id, performer_name: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT topic_id FROM performer_topics WHERE performer_name = ?", (performer_name,))
    row = cur.fetchone()
    if row and row[0]:
        conn.close()
        return row[0]

    try:
        peer = await user_app.resolve_peer(master_forum_id) 
        raw_response = await user_app.invoke(
            CreateForumTopic(
                channel=peer,
                title=performer_name[:128],
                random_id=int(time.time() * 1000)
            )
        )
        topic_id = None
        for update in getattr(raw_response, "updates", []):
            if hasattr(update, "message") and hasattr(update.message, "id"):
                topic_id = update.message.id
                break

        if topic_id:
            cur.execute("INSERT OR REPLACE INTO performer_topics (performer_name, topic_id, created_at) VALUES (?, ?, ?)",
                        (performer_name, topic_id, time.time()))
            conn.commit()
            conn.close()
            await asyncio.sleep(TRANSFER_STAGGER_SECONDS)
            return topic_id
    except Exception as e:
        print(f"[Topic Error] {e}")
    
    conn.close()
    return None

# ============================================================
# MASTER TRANSFER ENGINE
# ============================================================
async def run_stash_archive_routing(user_app: Client, bot_app: Client, src_chat_id, master_forum_id, status_msg):
    conn = get_db()
    
    dashboard_task = asyncio.create_task(live_dashboard_updater(bot_app, status_msg))

    scanned_count = 0
    async for msg in user_app.get_chat_history(src_chat_id):
        media = msg.video or msg.document
        if media:
            scanned_count += 1
            conn.execute(
                "INSERT OR IGNORE INTO stash_queue (message_id, chat_id, file_unique_id, status, updated_at) VALUES (?, ?, ?, 'PENDING', ?)",
                (msg.id, src_chat_id, media.file_unique_id, time.time())
            )
            if scanned_count % 500 == 0: conn.commit()
    conn.commit()

    cur = conn.cursor()
    cur.execute("SELECT message_id FROM stash_queue WHERE status = 'PENDING' ORDER BY message_id ASC")
    pending_msg_ids = [row[0] for row in cur.fetchall()]
    conn.close()

    for msg_id in pending_msg_ids:
        c = get_db()
        try:
            msg = await user_app.get_messages(src_chat_id, msg_id)
            if not msg or not (msg.video or msg.document):
                c.execute("UPDATE stash_queue SET status = 'ERROR' WHERE message_id = ?", (msg_id,))
                c.commit(); c.close(); continue

            performers, oshash = await find_performers_hybrid(user_app, msg)
            copied = False

            for perf_name in performers:
                topic_id = await resolve_performer_topic(user_app, master_forum_id, perf_name)
                if topic_id:
                    await user_app.copy_message(
                        chat_id=master_forum_id,
                        from_chat_id=src_chat_id,
                        message_id=msg_id,
                        reply_to_message_id=topic_id
                    )
                    copied = True
                    await asyncio.sleep(TRANSFER_STAGGER_SECONDS)

            final_status = 'COPIED' if copied else 'ERROR'
            c.execute(
                "UPDATE stash_queue SET status = ?, oshash = ?, performers = ?, updated_at = ? WHERE message_id = ?",
                (final_status, oshash or "", ",".join(performers), time.time(), msg_id)
            )

        except FloodWait as e:
            await asyncio.sleep(e.value + 2)
        except Exception:
            c.execute("UPDATE stash_queue SET status = 'ERROR' WHERE message_id = ?", (msg_id,))
        finally:
            c.commit()
            c.close()

    dashboard_task.cancel()
    await status_msg.edit_text("✅ **Stash Performer Routing Complete!**\nAll videos have been processed and sorted.")