import os
import re
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
DB_PATH = "SysCache/stash_queue.db"
TRANSFER_STAGGER_SECONDS = 2.0
TPDB_GRAPHQL_URL = "https://theporndb.net/graphql"
# Checks for STASHDB_API_KEY first, falls back to STASH_API_KEY if needed
TPDB_API_KEY = getattr(config, "STASHDB_API_KEY", getattr(config, "STASH_API_KEY", ""))

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Global kill-switch to stop active routing loops from p22.py
stop_routing_flag = asyncio.Event() 

# ============================================================
# SQLITE PERSISTENCE LEDGER (CRASH RECOVERY)
# ============================================================
def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_stash_db():
    conn = get_db()
    
    # Option A: StashDB/TPDB Queue
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
    
    # Forum Topics Map
    conn.execute("""
        CREATE TABLE IF NOT EXISTS performer_topics (
            performer_name TEXT PRIMARY KEY,
            topic_id INTEGER,
            created_at REAL
        )
    """)
    
    # Option B: Hashtag Queue
    conn.execute("""
        CREATE TABLE IF NOT EXISTS hashtag_queue (
            message_id INTEGER PRIMARY KEY,
            chat_id INTEGER,
            file_unique_id TEXT,
            hashtags TEXT,
            status TEXT DEFAULT 'PENDING',
            updated_at REAL
        )
    """)
    
    # Immutable Ledger to prevent duplicates if queues are wiped
    conn.execute("""
        CREATE TABLE IF NOT EXISTS master_ledger (
            source_msg_id INTEGER,
            file_unique_id TEXT,
            destination_topic_id INTEGER,
            PRIMARY KEY (source_msg_id, destination_topic_id)
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

async def live_dashboard_updater(bot_app: Client, status_msg, queue_table="stash_queue"):
    """Background loop that updates the Telegram status message with live DB stats."""
    mode_name = "TPDB Matcher" if queue_table == "stash_queue" else "Hashtag Router"
    while True:
        try:
            conn = get_db()
            cur = conn.cursor()
            
            cur.execute(f"SELECT COUNT(*) FROM {queue_table}")
            total = cur.fetchone()[0]
            
            cur.execute(f"SELECT COUNT(*) FROM {queue_table} WHERE status = 'PENDING'")
            pending = cur.fetchone()[0]
            
            cur.execute(f"SELECT COUNT(*) FROM {queue_table} WHERE status = 'COPIED'")
            copied = cur.fetchone()[0]

            cur.execute(f"SELECT COUNT(*) FROM {queue_table} WHERE status = 'ERROR'")
            errors = cur.fetchone()[0]
            
            conn.close()

            processed = total - pending
            percentage = (processed / total) * 100 if total > 0 else 0
            bar = generate_progress_bar(processed, total)

            dashboard_text = (
                f"🚀 **{mode_name} is Active**\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🗂️ **Total Videos Indexed:** `{total}`\n"
                f"⏳ **Pending in Queue:** `{pending}`\n\n"
                f"📊 **Progress:** {percentage:.1f}%\n"
                f"`[{bar}]`\n\n"
                f"✅ **Successfully Copied:** `{copied}`\n"
                f"⚠️ **Errors / Skipped:** `{errors}`\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"💡 _Safe to stop at any time. The ledger prevents duplicates._"
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
        head_buffer = bytearray()
        async for chunk in client.stream_media(file_id, limit=1, offset=0):
            head_buffer.extend(chunk)
            if len(head_buffer) >= chunk_size: 
                break
        head_bytes = bytes(head_buffer[:chunk_size])

        last_chunk_idx = (file_size - 1) // 1048576
        start_chunk_idx = max(0, last_chunk_idx - 1) 
        
        tail_buffer = bytearray()
        async for chunk in client.stream_media(file_id, limit=3, offset=start_chunk_idx):
            tail_buffer.extend(chunk)

        if len(tail_buffer) < chunk_size: 
            return None
            
        tail_bytes = bytes(tail_buffer[-chunk_size:])

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
# TPDB GRAPHQL HYBRID MATCHER (FULLY PATCHED)
# ============================================================
class StashDBError(Exception):
    pass

async def query_graphql(url: str, query: str, variables: dict = None, api_key: str = ""):
    headers = {"Content-Type": "application/json"}
    if api_key: 
        headers["ApiKey"] = api_key
        
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json={"query": query, "variables": variables or {}}, headers=headers, timeout=10) as resp:
                if resp.status != 200:
                    text_response = await resp.text()
                    print(f"\n❌ [API Error] HTTP {resp.status}: {text_response.strip()}")
                    raise StashDBError(f"HTTP {resp.status} - API Request Failed")

                data = await resp.json()
                
                if "errors" in data:
                    err_msg = data['errors'][0].get('message', 'Unknown Error')
                    print(f"\n❌ [GraphQL Error]: {err_msg}")
                    raise StashDBError(f"GraphQL Error: {err_msg}")
                    
                return data.get("data")
    except aiohttp.ClientError as e:
        print(f"\n❌ [Network Error talking to API]: {e}")
        raise StashDBError(f"Network Connection Failed")

async def find_performers_hybrid(client: Client, message):
    oshash = await calculate_tg_oshash(client, message)
    
    if oshash:
        gql_hash = f"""
        query {{
          findScenesBySceneFingerprints(fingerprints: [{{hash: "{oshash}", algorithm: "OSHASH"}}]) {{
            performers {{ performer {{ name }} }}
          }}
        }}
        """
        res = await query_graphql(TPDB_GRAPHQL_URL, gql_hash, {}, TPDB_API_KEY)
        
        if res and res.get("findScenesBySceneFingerprints"):
            results = res["findScenesBySceneFingerprints"]
            if results and isinstance(results, list) and isinstance(results[0], list) and len(results[0]) > 0:
                first_scene = results[0][0]
                if first_scene.get("performers"):
                    performers = [p["performer"]["name"] for p in first_scene["performers"]]
                    if performers: 
                        return performers, oshash

    file_name = message.video.file_name if message.video else (message.document.file_name if message.document else "")
    caption_text = message.caption or message.text or ""
    search_term = os.path.splitext(file_name)[0] if file_name else caption_text.split('\n')[0]

    if search_term:
        # 🚨 FIX: Added sort and direction to the input parameters
        gql_text = """
        query TextSearch($q: String!) {
          queryScenes(input: {title: $q, page: 1, per_page: 1, sort: "date", direction: "desc"}) {
            scenes {
              performers { performer { name } }
            }
          }
        }
        """
        res = await query_graphql(TPDB_GRAPHQL_URL, gql_text, {"q": search_term}, TPDB_API_KEY)
        
        if res and res.get("queryScenes", {}).get("scenes"):
            scenes = res["queryScenes"]["scenes"]
            if scenes and scenes[0].get("performers"):
                performers = [p["performer"]["name"] for p in scenes[0]["performers"]]
                if performers: 
                    return performers, oshash

    return ["Unmatched Stash Videos"], oshash

# ============================================================
# FORUM TOPIC RESOLVER
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
        print(f"[Topic Error] Failed to create/resolve topic for {performer_name}: {e}")
    
    conn.close()
    return None

# ============================================================
# OPTION A: TPDB TRANSFER ENGINE
# ============================================================
async def run_stash_archive_routing(user_app: Client, bot_app: Client, src_chat_id, master_forum_id, status_msg):
    conn = get_db()
    
    dashboard_task = asyncio.create_task(live_dashboard_updater(bot_app, status_msg, "stash_queue"))

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
    cur.execute("SELECT message_id, file_unique_id FROM stash_queue WHERE status = 'PENDING' ORDER BY message_id ASC")
    pending_rows = cur.fetchall()
    conn.close()

    for msg_id, file_unique_id in pending_rows:
        if stop_routing_flag.is_set():
            await status_msg.edit_text("🛑 **Routing Stopped by User!**")
            break

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
                
                c.execute("SELECT 1 FROM master_ledger WHERE source_msg_id = ? AND destination_topic_id = ?", (msg_id, topic_id))
                if c.fetchone():
                    copied = True 
                    continue

                if topic_id:
                    await user_app.copy_message(
                        chat_id=master_forum_id,
                        from_chat_id=src_chat_id,
                        message_id=msg_id,
                        reply_to_message_id=topic_id
                    )
                    
                    c.execute("INSERT INTO master_ledger (source_msg_id, file_unique_id, destination_topic_id) VALUES (?, ?, ?)", 
                              (msg_id, file_unique_id, topic_id))
                    
                    copied = True
                    await asyncio.sleep(TRANSFER_STAGGER_SECONDS)

            final_status = 'COPIED' if copied else 'ERROR'
            c.execute(
                "UPDATE stash_queue SET status = ?, oshash = ?, performers = ?, updated_at = ? WHERE message_id = ?",
                (final_status, oshash or "", ",".join(performers), time.time(), msg_id)
            )

        except FloodWait as e:
            await asyncio.sleep(e.value + 2)
        except Exception as e:
            # 🚨 THIS WILL EXPOSE THE SILENT ERRORS 🚨
            print(f"❌ [TPDB Router Error] Msg {msg_id}: {e}")
            c.execute("UPDATE stash_queue SET status = 'ERROR' WHERE message_id = ?", (msg_id,))
        finally:
            c.commit()
            c.close()

    dashboard_task.cancel()
    if not stop_routing_flag.is_set():
        await status_msg.edit_text("✅ **Performer Routing Complete!**\nAll videos have been processed and sorted.")

# ============================================================
# OPTION B: HASHTAG ROUTING ENGINE
# ============================================================
def extract_hashtags(text):
    if not text:
        return []
    tags = re.findall(r"#(\w+)", text)
    return list(set([t.lower().capitalize() for t in tags]))

async def run_hashtag_routing(user_app: Client, bot_app: Client, src_chat_id, master_forum_id, status_msg):
    conn = get_db()
    
    dashboard_task = asyncio.create_task(live_dashboard_updater(bot_app, status_msg, "hashtag_queue"))

    scanned_count = 0
    async for msg in user_app.get_chat_history(src_chat_id):
        media = msg.video or msg.document
        if media:
            scanned_count += 1
            caption = msg.caption or msg.text or ""
            tags = extract_hashtags(caption)
            
            if not tags:
                tags = ["Unmatched Hashtags"]
                
            conn.execute(
                "INSERT OR IGNORE INTO hashtag_queue (message_id, chat_id, file_unique_id, hashtags, status, updated_at) VALUES (?, ?, ?, ?, 'PENDING', ?)",
                (msg.id, src_chat_id, media.file_unique_id, ",".join(tags), time.time())
            )
            if scanned_count % 500 == 0: conn.commit()
    conn.commit()

    cur = conn.cursor()
    cur.execute("SELECT message_id, file_unique_id, hashtags FROM hashtag_queue WHERE status = 'PENDING' ORDER BY message_id ASC")
    pending_rows = cur.fetchall()
    conn.close()

    for msg_id, file_unique_id, hashtags in pending_rows:
        if stop_routing_flag.is_set():
            await status_msg.edit_text("🛑 **Hashtag Routing Stopped by User!**")
            break

        c = get_db()
        try:
            tags_list = hashtags.split(",")
            copied = False

            for tag_name in tags_list:
                topic_id = await resolve_performer_topic(user_app, master_forum_id, tag_name)
                
                c.execute("SELECT 1 FROM master_ledger WHERE source_msg_id = ? AND destination_topic_id = ?", (msg_id, topic_id))
                if c.fetchone():
                    copied = True
                    continue

                if topic_id:
                    await user_app.copy_message(
                        chat_id=master_forum_id,
                        from_chat_id=src_chat_id,
                        message_id=msg_id,
                        reply_to_message_id=topic_id
                    )
                    
                    c.execute("INSERT INTO master_ledger (source_msg_id, file_unique_id, destination_topic_id) VALUES (?, ?, ?)", 
                              (msg_id, file_unique_id, topic_id))
                    
                    copied = True
                    await asyncio.sleep(TRANSFER_STAGGER_SECONDS)

            final_status = 'COPIED' if copied else 'ERROR'
            c.execute("UPDATE hashtag_queue SET status = ?, updated_at = ? WHERE message_id = ?", (final_status, time.time(), msg_id))

        except FloodWait as e:
            await asyncio.sleep(e.value + 2)
        except Exception as e:
            # 🚨 THIS WILL EXPOSE THE SILENT ERRORS 🚨
            print(f"❌ [Hashtag Router Error] Msg {msg_id}: {e}")
            c.execute("UPDATE hashtag_queue SET status = 'ERROR' WHERE message_id = ?", (msg_id,))
        finally:
            c.commit()
            c.close()

    dashboard_task.cancel()
    if not stop_routing_flag.is_set():
        await status_msg.edit_text("✅ **Hashtag Routing Complete!**\nAll videos sorted by #tags.")