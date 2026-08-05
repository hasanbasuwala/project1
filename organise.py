import re
import time
import asyncio
import aiosqlite
from telethon import TelegramClient, events, errors
from stashapi.stashapp import StashInterface

# --- IMPORT CONFIGURATION ---
from config import API_ID, API_HASH, SUPERGROUP_ID, STASH_CONFIG, STASH_BOT_TOKEN

client = TelegramClient('telegram_stash_indexer_session', API_ID, API_HASH)
stash = StashInterface(STASH_CONFIG)

PERFORMER_CACHE = {}
START_TIME = time.time()
BOT_STATE = "Initializing..."

def load_stash_performers():
    """Fetches all performers and aliases from Stash to build the matching cache."""
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
                    
        print(f"[STASH] Successfully cached {count} performers (and their aliases).")
        return count
    except Exception as e:
        print(f"[WARNING] Stash connection failed: {e}. Falling back to manual hashtags.")
        return 0

async def init_db():
    """Sets up SQLite database tables for videos, tags, and scanning checkpoints."""
    async with aiosqlite.connect('telegram_stash_videos.db') as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS videos (
                message_id INTEGER PRIMARY KEY,
                thread_id INTEGER,
                link TEXT
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
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value INTEGER
            )
        ''')
        await db.commit()

def extract_actor_tags(text):
    """Extracts hashtag matches based on Stash performers and manual hashtags."""
    found_tags = set()
    if not text:
        return list(found_tags)

    text_lower = text.lower()
    for actor_key, hashtag in PERFORMER_CACHE.items():
        if re.search(r'\b' + re.escape(actor_key) + r'\b', text_lower):
            found_tags.add(hashtag.lower())

    for tag in [word.lower() for word in text.split() if word.startswith('#')]:
        found_tags.add(tag)

    return list(found_tags)

async def process_message(message, db):
    """Indexes a single Telegram video message into SQLite."""
    if not message.video:
        return False

    caption = message.text or ""
    filename = message.video.attributes[0].file_name if message.video.attributes else ""
    combined_text = f"{caption} {filename}"
    
    tags = extract_actor_tags(combined_text)
    if not tags:
        return False

    thread_id = message.message_thread_id or 0
    chat_id_str = str(message.chat_id).replace('-100', '')
    
    link = f"https://t.me/c/{chat_id_str}/{thread_id}/{message.id}" if thread_id else f"https://t.me/c/{chat_id_str}/{message.id}"

    await db.execute('INSERT OR REPLACE INTO videos (message_id, thread_id, link) VALUES (?, ?, ?)', (message.id, thread_id, link))
    await db.execute('DELETE FROM tags WHERE message_id = ?', (message.id,))
    for tag in tags:
        await db.execute('INSERT INTO tags (message_id, tag) VALUES (?, ?)', (message.id, tag))
        
    return True

async def get_checkpoint(db):
    """Fetches the last scanned message ID from DB."""
    async with db.execute("SELECT value FROM settings WHERE key = 'last_scanned_msg_id'") as cursor:
        row = await cursor.fetchone()
        return row[0] if row else 0

async def update_checkpoint(db, message_id):
    """Saves current scanning checkpoint."""
    await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('last_scanned_msg_id', ?)", (message_id,))
    await db.commit()

async def scan_history():
    """Iterates through group history, auto-resuming from the last checkpoint."""
    global BOT_STATE
    BOT_STATE = "Scanning Historical Videos..."
    
    async with aiosqlite.connect('telegram_stash_videos.db') as db:
        last_scanned_id = await get_checkpoint(db)
        print(f"\n[SCAN] Resuming scan from Message ID: {last_scanned_id}...")

        count = 0
        processed_videos = 0
        
        try:
            async for message in client.iter_messages(SUPERGROUP_ID, min_id=last_scanned_id, reverse=True):
                count += 1
                try:
                    if await process_message(message, db):
                        processed_videos += 1

                    if count % 50 == 0:
                        await update_checkpoint(db, message.id)
                        print(f"[SCAN] Checked: {count} msgs | Indexed: {processed_videos} videos | Current ID: {message.id}")

                except errors.FloodWaitError as e:
                    print(f"\n[RATE LIMIT] Sleeping for {e.seconds} seconds... Do not stop the script.")
                    await asyncio.sleep(e.seconds)
                except Exception as e:
                    print(f"[ERROR] Failed on message {message.id}: {e}")
                    
        except errors.FloodWaitError as e:
            print(f"\n[RATE LIMIT] Iterator throttled. Sleeping for {e.seconds} seconds...")
            await asyncio.sleep(e.seconds)

        await update_checkpoint(db, 999999999)
        BOT_STATE = "Live Listening"
        print("\n[SUCCESS] History scan complete! Listening for live updates.")

async def send_split_message(event, header, links_list):
    """Sends results in chunks to bypass Telegram's 4096 character limit."""
    chunk = header + "\n\n"
    for link in links_list:
        link_str = f"🔗 {link}\n"
        # If adding the next link exceeds the limit, send current chunk and reset
        if len(chunk) + len(link_str) > 4000:
            await event.reply(chunk)
            chunk = link_str
        else:
            chunk += link_str
            
    # Send whatever is left in the final chunk
    if chunk.strip():
        await event.reply(chunk)

# ================= TELEGRAM BOT COMMANDS =================

@client.on(events.NewMessage(chats=SUPERGROUP_ID, pattern=r'^/start'))
async def start_handler(event):
    msg = (
        "👋 **Welcome to the Telegram Video Organizer Bot!**\n\n"
        "This bot automatically categorizes group videos using Stash actor references.\n"
        "Type `/help` to see all available commands."
    )
    await event.reply(msg)

@client.on(events.NewMessage(chats=SUPERGROUP_ID, pattern=r'^/help'))
async def help_handler(event):
    msg = (
        "📖 **Bot Commands List**\n\n"
        "• `/status` - Check bot health, uptime, and database stats\n"
        "• `/tags` - List top indexed actor categories & counts\n"
        "• `/search #tag1 #tag2` - Search videos matching ALL tags\n"
        "• `/exclude #tag1 -#tag2` - Search while excluding unwanted tags\n"
        "• `/sync` - Re-sync performer list directly from Stash\n"
        "• `/help` - Show this menu"
    )
    await event.reply(msg)

@client.on(events.NewMessage(chats=SUPERGROUP_ID, pattern=r'^/status'))
async def status_handler(event):
    uptime_seconds = int(time.time() - START_TIME)
    uptime_str = f"{uptime_seconds // 3600}h {(uptime_seconds % 3600) // 60}m {uptime_seconds % 60}s"
    
    async with aiosqlite.connect('telegram_stash_videos.db') as db:
        async with db.execute("SELECT COUNT(*) FROM videos") as cursor:
            total_videos = (await cursor.fetchone())[0]
        async with db.execute("SELECT COUNT(DISTINCT tag) FROM tags") as cursor:
            total_tags = (await cursor.fetchone())[0]
            
    status_msg = (
        f"⚙️ **System Status**\n\n"
        f"• **State:** `{BOT_STATE}`\n"
        f"• **Uptime:** `{uptime_str}`\n"
        f"• **Indexed Videos:** `{total_videos}`\n"
        f"• **Unique Actor Tags:** `{total_tags}`\n"
    )
    await event.reply(status_msg)

@client.on(events.NewMessage(chats=SUPERGROUP_ID, pattern=r'^/tags'))
async def tags_handler(event):
    async with aiosqlite.connect('telegram_stash_videos.db') as db:
        cursor = await db.execute('SELECT tag, COUNT(*) FROM tags GROUP BY tag ORDER BY COUNT(*) DESC LIMIT 30')
        rows = await cursor.fetchall()
        
    if not rows:
        await event.reply("No categorized videos found yet.")
        return
        
    response = "**Top Indexed Actor Categories:**\n\n" + "\n".join([f"• `{tag}` : {count} videos" for tag, count in rows])
    await event.reply(response)

@client.on(events.NewMessage(chats=SUPERGROUP_ID, pattern=r'^/sync'))
async def sync_handler(event):
    await event.reply("🔄 Fetching performer database from Stash...")
    count = load_stash_performers()
    await event.reply(f"✅ **Sync Complete!**\nCurrently tracking `{count}` performers/aliases from Stash.")

@client.on(events.NewMessage(chats=SUPERGROUP_ID, pattern=r'^/search(?:\s+(.+))?$'))
async def search_handler(event):
    raw_query = event.pattern_match.group(1)
    if not raw_query:
        await event.reply("⚠️ Usage: `/search #actor1 #actor2`")
        return
        
    search_tags = [word.lower() for word in raw_query.split() if word.startswith('#')]
    if not search_tags:
        await event.reply("Please provide at least one hashtag (e.g., `/search #actorname`).")
        return

    async with aiosqlite.connect('telegram_stash_videos.db') as db:
        placeholders = ', '.join('?' * len(search_tags))
        query = f'''
            SELECT videos.link 
            FROM videos 
            JOIN tags ON videos.message_id = tags.message_id 
            WHERE tags.tag IN ({placeholders})
            GROUP BY videos.message_id
            HAVING COUNT(DISTINCT tags.tag) = ?
        '''
        cursor = await db.execute(query, search_tags + [len(search_tags)])
        rows = await cursor.fetchall()
        
    if not rows:
        await event.reply(f"No videos found containing ALL requested tags: {', '.join(search_tags)}")
        return
        
    links_list = [row[0] for row in rows]
    header = f"**Found {len(rows)} video(s) for {' & '.join(search_tags)}:**"
    await send_split_message(event, header, links_list)

@client.on(events.NewMessage(chats=SUPERGROUP_ID, pattern=r'^/exclude(?:\s+(.+))?$'))
async def exclude_handler(event):
    raw_query = event.pattern_match.group(1)
    if not raw_query:
        await event.reply("⚠️ Usage: `/exclude #required -#unwanted`")
        return

    required_tags = [word.lower() for word in raw_query.split() if word.startswith('#')]
    excluded_tags = [word[1:].lower() for word in raw_query.split() if word.startswith('-#')]

    if not required_tags:
        await event.reply("You must specify at least one required hashtag (e.g., `/exclude #actor -#unwanted`).")
        return

    async with aiosqlite.connect('telegram_stash_videos.db') as db:
        req_placeholders = ', '.join('?' * len(required_tags))
        req_query = f'''
            SELECT message_id, link 
            FROM videos 
            JOIN tags USING(message_id)
            WHERE tag IN ({req_placeholders})
            GROUP BY message_id
            HAVING COUNT(DISTINCT tag) = ?
        '''
        cursor = await db.execute(req_query, required_tags + [len(required_tags)])
        matched_videos = {row[0]: row[1] for row in await cursor.fetchall()}

        if excluded_tags and matched_videos:
            exc_placeholders = ', '.join('?' * len(excluded_tags))
            msg_ids = tuple(matched_videos.keys())
            msg_placeholders = ', '.join('?' * len(msg_ids))
            
            exc_query = f'''
                SELECT DISTINCT message_id 
                FROM tags 
                WHERE tag IN ({exc_placeholders}) AND message_id IN ({msg_placeholders})
            '''
            cursor = await db.execute(exc_query, excluded_tags + list(msg_ids))
            for row in await cursor.fetchall():
                matched_videos.pop(row[0], None)

    if not matched_videos:
        await event.reply("No videos matched your filter criteria.")
        return

    links_list = list(matched_videos.values())
    header = f"**Found {len(matched_videos)} video(s):**"
    await send_split_message(event, header, links_list)

@client.on(events.NewMessage(chats=SUPERGROUP_ID))
async def live_message_handler(event):
    if event.message.video and BOT_STATE == "Live Listening":
        async with aiosqlite.connect('telegram_stash_videos.db') as db:
            if await process_message(event.message, db):
                await db.commit()

# ================= MAIN EXECUTION =================

async def main():
    await init_db()
    load_stash_performers()
    
    # Starting as a User is mandatory to read historical messages.
    await client.start()
    
    await scan_history()
    await client.run_until_disconnected()

if __name__ == '__main__':
    asyncio.run(main())