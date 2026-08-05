import re
import time
import asyncio
import aiosqlite
from telethon import TelegramClient, events, functions
from telethon.tl.custom import Button

# --- IMPORT CONFIGURATION ---
from config import API_ID, API_HASH, DROPZONE_GROUP_ID, LIBRARY_GROUP_ID, STASH_CONFIG, STASH_BOT_TOKEN
from stashapi.stashapp import StashInterface

# --- INITIALIZE BOTH CLIENTS ---
user_client = TelegramClient('user_stash_session', API_ID, API_HASH)
bot_client = TelegramClient('bot_stash_session', API_ID, API_HASH)

stash = StashInterface(STASH_CONFIG)
PERFORMER_CACHE = {}

# ================= DATABASE & CACHE SETUP =================

def load_stash_performers():
    """Fetches performers from Stash to build the matching cache."""
    global PERFORMER_CACHE
    print("[STASH] Syncing performer reference database...")
    PERFORMER_CACHE.clear()
    try:
        performers = stash.find_performers()
        count = 0
        for p in performers:
            name = p.get('name', '')
            aliases = p.get('aliases', []) or []
            tag_name = re.sub(r'[^\w]', '', name.title())
            
            if name:
                PERFORMER_CACHE[name.lower().strip()] = f"#{tag_name}"
                count += 1
            for alias in aliases:
                if alias:
                    PERFORMER_CACHE[alias.lower().strip()] = f"#{tag_name}"
        print(f"[STASH] Successfully cached {count} performers.")
    except Exception as e:
        print(f"[WARNING] Stash connection failed: {e}")

async def init_db():
    """Sets up SQLite database."""
    async with aiosqlite.connect('telegram_stash_videos.db') as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS videos (
                message_id INTEGER PRIMARY KEY,
                transferred INTEGER DEFAULT 0
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS tags (
                message_id INTEGER,
                tag TEXT,
                FOREIGN KEY(message_id) REFERENCES videos(message_id)
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS topics (
                tag TEXT PRIMARY KEY,
                topic_id INTEGER
            )
        ''')
        await db.commit()

def extract_actor_tags(text):
    """Extracts hashtag matches."""
    found_tags = set()
    if not text: return list(found_tags)
    text_lower = text.lower()
    for actor_key, hashtag in PERFORMER_CACHE.items():
        if re.search(r'\b' + re.escape(actor_key) + r'\b', text_lower):
            found_tags.add(hashtag.lower())
    for tag in [word.lower() for word in text.split() if word.startswith('#')]:
        found_tags.add(tag)
    return list(found_tags)

# ================= THE WORKER (USER CLIENT) =================

@user_client.on(events.NewMessage(chats=DROPZONE_GROUP_ID))
async def raw_upload_listener(event):
    """Listens for raw uploads, scans tags, and indexes them silently."""
    if not event.message.video: 
        return

    caption = event.message.text or ""
    filename = event.message.video.attributes[0].file_name if event.message.video.attributes else ""
    tags = extract_actor_tags(f"{caption} {filename}")
    
    if not tags: 
        return

    async with aiosqlite.connect('telegram_stash_videos.db') as db:
        await db.execute('INSERT OR IGNORE INTO videos (message_id, transferred) VALUES (?, 0)', (event.message.id,))
        await db.execute('DELETE FROM tags WHERE message_id = ?', (event.message.id,))
        for tag in tags:
            await db.execute('INSERT INTO tags (message_id, tag) VALUES (?, ?)', (event.message.id, tag))
        await db.commit()
    print(f"[USER WORKER] Indexed video {event.message.id} with tags: {tags}")

# ================= THE MANAGER (BOT CLIENT) =================

@bot_client.on(events.NewMessage(chats=DROPZONE_GROUP_ID, pattern=r'^/dashboard'))
async def dashboard_handler(event):
    """Generates the interactive Triage Dashboard."""
    async with aiosqlite.connect('telegram_stash_videos.db') as db:
        query = '''
            SELECT tags.tag, COUNT(videos.message_id) 
            FROM tags 
            JOIN videos ON tags.message_id = videos.message_id 
            WHERE videos.transferred = 0 
            GROUP BY tags.tag 
            ORDER BY COUNT(videos.message_id) DESC
            LIMIT 15
        '''
        cursor = await db.execute(query)
        rows = await cursor.fetchall()

    if not rows:
        await event.reply("✅ **All caught up!** No pending videos to transfer.")
        return

    buttons = []
    for tag, count in rows:
        buttons.append([Button.inline(f"{tag} ({count} pending)", data=f"review_{tag}")])

    await event.reply(
        "🗂 **Triage Dashboard**\nSelect an actor below to review and transfer their pending videos to the Library:",
        buttons=buttons
    )

@bot_client.on(events.CallbackQuery(pattern=b'^review_(.+)'))
async def review_callback(event):
    """Displays action menu for a specific actor."""
    tag = event.pattern_match.group(1).decode('utf-8')
    
    async with aiosqlite.connect('telegram_stash_videos.db') as db:
        cursor = await db.execute('''
            SELECT COUNT(*) FROM videos 
            JOIN tags USING(message_id) 
            WHERE tags.tag = ? AND videos.transferred = 0
        ''', (tag,))
        count = (await cursor.fetchone())[0]

    if count == 0:
        await event.edit(f"✅ All videos for {tag} have already been transferred.")
        return

    buttons = [
        [Button.inline("🚀 Transfer to Library", data=f"transfer_{tag}")],
        [Button.inline("🔙 Back to Dashboard", data="back_dash")]
    ]
    await event.edit(f"🎬 **Actor:** {tag}\n📦 **Pending Videos:** {count}\n\nDo you want to move these to the organized Library group?", buttons=buttons)

@bot_client.on(events.CallbackQuery(pattern=b'^back_dash$'))
async def back_dash_callback(event):
    await dashboard_handler(event)

@bot_client.on(events.CallbackQuery(pattern=b'^transfer_(.+)'))
async def transfer_callback(event):
    """Handles Topic creation (via Bot) and File Transfer (via User)."""
    tag = event.pattern_match.group(1).decode('utf-8')
    await event.edit(f"⏳ **Starting transfer for {tag}...**\nPlease wait.")
    
    async with aiosqlite.connect('telegram_stash_videos.db') as db:
        cursor = await db.execute('SELECT topic_id FROM topics WHERE tag = ?', (tag,))
        row = await cursor.fetchone()
        topic_id = row[0] if row else None

        # 1. Create Topic via Bot Client
        if not topic_id:
            try:
                result = await bot_client(functions.channels.CreateForumTopicRequest(
                    channel=LIBRARY_GROUP_ID,
                    title=tag.replace('#', '')
                ))
                topic_id = result.updates[1].message.id 
                await db.execute('INSERT INTO topics (tag, topic_id) VALUES (?, ?)', (tag, topic_id))
                await db.commit()
            except Exception as e:
                await event.edit(f"❌ **Error creating topic for {tag}:** {e}")
                return

        cursor = await db.execute('''
            SELECT videos.message_id 
            FROM videos 
            JOIN tags USING(message_id) 
            WHERE tags.tag = ? AND videos.transferred = 0
        ''', (tag,))
        pending_messages = [row[0] for row in await cursor.fetchall()]

        # 2. Transfer Media via User Client (Bypasses bot file size limits)
        success_count = 0
        for msg_id in pending_messages:
            try:
                msg = await user_client.get_messages(DROPZONE_GROUP_ID, ids=msg_id)
                if msg and msg.media:
                    await user_client.send_message(
                        LIBRARY_GROUP_ID, 
                        message=msg.text, 
                        file=msg.media, 
                        reply_to=topic_id
                    )
                    await db.execute('UPDATE videos SET transferred = 1 WHERE message_id = ?', (msg_id,))
                    await db.commit()
                    success_count += 1
                    await asyncio.sleep(1.5) # Crucial sleep to prevent FloodWait
            except Exception as e:
                print(f"[ERROR] Failed to transfer msg {msg_id}: {e}")

    await event.edit(f"✅ **Transfer Complete!**\nSuccessfully copied {success_count} videos into the `{tag}` folder in your Library.")

# ================= MAIN EXECUTION =================

async def main():
    await init_db()
    load_stash_performers()
    
    # Start both clients
    await user_client.start()
    print("[SYSTEM] User Client (Worker) started.")
    
    await bot_client.start(bot_token=STASH_BOT_TOKEN)
    print("[SYSTEM] Bot Client (Manager) started.")
    
    print("[READY] Hybrid system online. Send raw files, then type /dashboard to manage.")
    
    # Run concurrently
    await asyncio.gather(
        user_client.run_until_disconnected(),
        bot_client.run_until_disconnected()
    )

if __name__ == '__main__':
    asyncio.run(main())