import os
import sys
import re
import asyncio
import requests
import vk_api
from pyrogram import Client, filters
from pyrogram.types import Message

import config

# Initialize Telegram Pyrogram Bot
API_ID = getattr(config, "API_ID", 12345)
API_HASH = getattr(config, "API_HASH", "0123456789abcdef0123456789abcdef")

app = Client(
    "stealth_bot",
    bot_token=config.RAINDROP_BOT_TOKEN,
    api_id=API_ID,
    api_hash=API_HASH
)

vk_session = vk_api.VkApi(token=config.VK_TOKEN)
vk = vk_session.get_api()

# ==============================================================================
# RAINDROP API HELPERS
# ==============================================================================

def get_raindrop_headers():
    return {
        "Authorization": f"Bearer {config.RAINDROP_ID}",
        "Content-Type": "application/json"
    }

def get_or_create_collection(collection_name: str) -> int:
    """Finds a Raindrop collection by name, or creates it if it doesn't exist."""
    headers = get_raindrop_headers()
    
    # 1. Search existing collections
    try:
        res = requests.get("https://api.raindrop.io/rest/v1/collections", headers=headers, timeout=10)
        if res.status_code == 200:
            collections = res.json().get('items', [])
            for c in collections:
                if c.get('title', '').lower() == collection_name.lower():
                    return c['_id']
    except Exception as e:
        print(f"Error fetching collections: {e}")

    # 2. If not found, create a new collection
    try:
        payload = {"title": collection_name}
        res = requests.post("https://api.raindrop.io/rest/v1/collection", headers=headers, json=payload, timeout=10)
        if res.status_code == 200:
            return res.json().get('item', {}).get('_id')
    except Exception as e:
        print(f"Error creating collection: {e}")
        
    return -1 # Default to "Unsorted" (ID -1 in Raindrop) if creation fails


def send_to_raindrop(url: str, title: str, tags: list, collection_id: int) -> tuple[bool, str]:
    """Posts a bookmark to a specific Raindrop Collection."""
    headers = get_raindrop_headers()
    payload = {
        "link": url,
        "title": title,
        "tags": tags,
        "collectionId": collection_id 
    }
    
    try:
        response = requests.post(
            "https://api.raindrop.io/rest/v1/raindrop",
            headers=headers,
            json=payload,
            timeout=15
        )
        if response.status_code == 200:
            return True, "Saved to Raindrop"
        else:
            return False, f"API Error {response.status_code}"
    except Exception as e:
        return False, f"Network Error: {str(e)}"

# ==============================================================================
# OTHER HELPERS
# ==============================================================================

def parse_caption(caption: str):
    if not caption:
        return None, []
        
    # 1. Ignore everything after the first '-'
    clean_text = caption.split('-')[0].strip()
    
    # 2. Extract [Production] and the remaining names
    pattern = r'\[([^\]]+)\]\s*(.*)'
    match = re.search(pattern, clean_text, flags=re.DOTALL)
    if not match:
        return None, []
    
    production = match.group(1).strip()
    names_raw = match.group(2).strip()
    
    # 3. Split names by separators: 'x', '&', ',', 'and'
    names_list = [
        name.strip() for name in 
        re.split(r'\s*(?:x|&|,|\band\b)\s*', names_raw, flags=re.IGNORECASE) 
        if name.strip()
    ]
    
    return production, names_list


def make_progress_bar(current: int, total: int, length: int = 10) -> str:
    if total <= 0: return "⬜️" * length
    progress = int((current / total) * length)
    return "🟩" * progress + "⬜️" * (length - progress)

# ==============================================================================
# BOT COMMANDS & HANDLERS
# ==============================================================================

@app.on_message(filters.command("reboot") & filters.private)
async def reboot_bot(client: Client, message: Message):
    await message.reply_text("🔄 **Rebooting bot...**")
    os.execv(sys.executable, [sys.executable] + sys.argv)


@app.on_message(filters.text & filters.private & ~filters.command(["start", "reboot", "help"]))
async def process_vk_url(client: Client, message: Message):
    raw_url = message.text.strip()
    
    dashboard = await message.reply_text("📊 **LIVE DASHBOARD**\n⚙️ *Initializing...*")
    try:
        await dashboard.pin(disable_notification=True)
    except Exception:
        pass
        
    videos_to_process = []
    
    await dashboard.edit_text("📥 Extracting data from VK API...")

    # [VK FETCHING LOGIC REMAINS THE SAME]
    try:
        wall_match = re.search(r'wall(-?\d+)_(\d+)', raw_url)
        playlist_match = re.search(r'(?:playlist|album)/?(-?\d+)_(\d+)', raw_url)
        video_match = re.search(r'video(-?\d+)_(\d+)', raw_url)

        if wall_match:
            post_id = f"{wall_match.group(1)}_{wall_match.group(2)}"
            res = await asyncio.to_thread(vk.wall.getById, posts=post_id)
            if res:
                post = res[0]
                post_text = post.get('text', '')
                video_atts = [att['video'] for att in post.get('attachments', []) if att.get('type') == 'video']
                for v in video_atts:
                    vid_url = f"https://vk.com/video{v['owner_id']}_{v['id']}"
                    title = v.get('title') or post_text[:50] or f"Video {v['id']}"
                    videos_to_process.append({"url": vid_url, "title": title, "caption": post_text})

        elif playlist_match:
            res = await asyncio.to_thread(vk.video.get, owner_id=int(playlist_match.group(1)), album_id=int(playlist_match.group(2)), count=100)
            for v in res.get('items', []):
                vid_url = f"https://vk.com/video{v['owner_id']}_{v['id']}"
                videos_to_process.append({"url": vid_url, "title": v.get('title', ''), "caption": v.get('description', '') or v.get('title', '')})

        elif video_match:
            res = await asyncio.to_thread(vk.video.get, videos=f"{video_match.group(1)}_{video_match.group(2)}")
            if items := res.get('items', []):
                v = items[0]
                vid_url = f"https://vk.com/video{v['owner_id']}_{v['id']}"
                videos_to_process.append({"url": vid_url, "title": v.get('title', ''), "caption": v.get('description', '') or v.get('title', '')})

    except Exception as e:
        await dashboard.edit_text(f"❌ **Error parsing VK URL:**\n`{str(e)}`")
        return

    total = len(videos_to_process)
    if total == 0:
        await dashboard.edit_text("⚠️ **No videos found**.")
        return

    success_count = 0
    fail_count = 0

    for idx, vid_item in enumerate(videos_to_process, start=1):
        # 1. Parse Data
        production, names = parse_caption(vid_item['caption'])
        
        # 2. Determine Collection
        collection_id = -1 # Default to Unsorted
        col_name_str = "Unsorted"
        
        if production:
            col_name_str = production
            # Fetch existing collection ID or create a new one dynamically
            collection_id = await asyncio.to_thread(get_or_create_collection, production)
            tags = names # Keep only actor names as tags
        else:
            tags = []
        
        # 3. Send to Raindrop
        ok, status_msg = await asyncio.to_thread(
            send_to_raindrop, 
            vid_item['url'], 
            vid_item['title'], 
            tags,
            collection_id
        )
        
        if ok: success_count += 1
        else: fail_count += 1

        # 4. Update Dashboard
        bar = make_progress_bar(idx, total)
        tag_str = ", ".join([f"`#{t}`" for t in tags]) if tags else "*None*"
        
        dashboard_text = (
            f"📊 **LIVE DASHBOARD**\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"**Progress:** [{bar}] `{idx}/{total}`\n"
            f"**Success:** {success_count} | **Failed:** {fail_count}\n\n"
            f"🏷 **Latest Item:**\n"
            f"• **Collection:** 📁 `{col_name_str}`\n"
            f"• **Tags:** {tag_str}\n"
            f"• **Status:** {'✅' if ok else '❌'} {status_msg}\n"
            f"━━━━━━━━━━━━━━━━━━━"
        )
        await dashboard.edit_text(dashboard_text)
        await asyncio.sleep(0.5)

    await dashboard.edit_text(
        f"🎉 **TASK COMPLETE**\n"
        f"✅ **Processed:** {total} videos\n"
        f"🔗 **Source Link:** `{raw_url}`"
    )
    try: await dashboard.unpin()
    except Exception: pass

if __name__ == "__main__":
    print("🤖 Stealth Bot with Dynamic Raindrop Collections is active...")
    app.run()