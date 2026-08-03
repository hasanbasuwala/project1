import os
import sys
import re
import asyncio
import requests
import vk_api
from pyrogram import Client, filters
from pyrogram.types import Message

# Import tokens and configs
import config

# Initialize Telegram Pyrogram Bot
# (Assumes API_ID and API_HASH are defined in config.py. If not present, default placeholders are used)
API_ID = getattr(config, "API_ID", 12345)
API_HASH = getattr(config, "API_HASH", "0123456789abcdef0123456789abcdef")

app = Client(
    "stealth_bot",
    bot_token=config.RAINDROP_BOT_TOKEN,
    api_id=API_ID,
    api_hash=API_HASH
)

# Initialize VK API
vk_session = vk_api.VkApi(token=config.VK_TOKEN)
vk = vk_session.get_api()


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def parse_caption(caption: str):
    """Extracts [Production] and actor/performer names from post/video captions using regex."""
    if not caption:
        return None, []
        
    pattern = r'\[([^\]]+)\]\s*([^-]+)(?:-\s*(.*))?'
    match = re.search(pattern, caption, flags=re.DOTALL)
    if not match:
        return None, []
    
    production = match.group(1).strip()
    names_raw = match.group(2).strip()
    
    names_list = [
        name.strip() for name in 
        re.split(r'\s*(?:x|&|,|\band\b)\s*', names_raw, flags=re.IGNORECASE) 
        if name.strip()
    ]
    return production, names_list


def send_to_raindrop(url: str, title: str, tags: list) -> tuple[bool, str]:
    """Posts a bookmark and tags to Raindrop.io using RAINDROP_ID as the API Token."""
    headers = {
        "Authorization": f"Bearer {config.RAINDROP_ID}",
        "Content-Type": "application/json"
    }
    payload = {
        "link": url,
        "title": title,
        "tags": tags
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
            return False, f"Raindrop API Error {response.status_code}"
    except Exception as e:
        return False, f"Network Error: {str(e)}"


def make_progress_bar(current: int, total: int, length: int = 10) -> str:
    """Generates a visual progress bar string."""
    if total <= 0:
        return "⬜️" * length
    progress = int((current / total) * length)
    return "🟩" * progress + "⬜️" * (length - progress)


# ==============================================================================
# BOT COMMANDS & HANDLERS
# ==============================================================================

@app.on_message(filters.command("reboot") & filters.private)
async def reboot_bot(client: Client, message: Message):
    """Restarts the Python script directly from Telegram."""
    await message.reply_text("🔄 **Rebooting bot...**\nRestarting process, please wait a few seconds.")
    os.execv(sys.executable, [sys.executable] + sys.argv)


@app.on_message(filters.text & filters.private & ~filters.command(["start", "reboot", "help"]))
async def process_vk_url(client: Client, message: Message):
    """Main handler: receives VK link, pins dashboard, parses items, pushes to Raindrop."""
    raw_url = message.text.strip()
    
    # 1. Initialize Pinned Dashboard
    dashboard = await message.reply_text("📊 **LIVE DASHBOARD**\n\n⚙️ *Initializing processing pipeline...*")
    try:
        await dashboard.pin(disable_notification=True)
    except Exception:
        pass  # If pinning fails (e.g. missing perms), continue silently
        
    # 2. Identify Link Type & Fetch Video Items from VK
    videos_to_process = []
    
    await dashboard.edit_text(
        "📊 **LIVE DASHBOARD**\n"
        "━━━━━━━━━━━━━━━━━━━\n"
        "**Status:** 📥 Extracting data from VK API...\n"
        f"**Target:** `{raw_url}`"
    )

    try:
        # Check if Wall Post URL
        wall_match = re.search(r'wall(-?\d+)_(\d+)', raw_url)
        # Check if Playlist/Album URL
        playlist_match = re.search(r'(?:playlist|album)/?(-?\d+)_(\d+)', raw_url)
        # Check if Direct Video URL
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
                    videos_to_process.append({
                        "url": vid_url,
                        "title": title,
                        "caption": post_text
                    })

        elif playlist_match:
            owner_id = int(playlist_match.group(1))
            album_id = int(playlist_match.group(2))
            res = await asyncio.to_thread(vk.video.get, owner_id=owner_id, album_id=album_id, count=100)
            items = res.get('items', [])
            
            for v in items:
                vid_url = f"https://vk.com/video{v['owner_id']}_{v['id']}"
                videos_to_process.append({
                    "url": vid_url,
                    "title": v.get('title', f"Video {v['id']}"),
                    "caption": v.get('description', '') or v.get('title', '')
                })

        elif video_match:
            video_id = f"{video_match.group(1)}_{video_match.group(2)}"
            res = await asyncio.to_thread(vk.video.get, videos=video_id)
            items = res.get('items', [])
            if items:
                v = items[0]
                videos_to_process.append({
                    "url": f"https://vk.com/video{v['owner_id']}_{v['id']}",
                    "title": v.get('title', f"Video {v['id']}"),
                    "caption": v.get('description', '') or v.get('title', '')
                })

    except Exception as e:
        await dashboard.edit_text(f"❌ **Error parsing VK URL:**\n`{str(e)}`")
        return

    total = len(videos_to_process)
    if total == 0:
        await dashboard.edit_text("⚠️ **No videos found** for the provided link.")
        try:
            await dashboard.unpin()
        except Exception:
            pass
        return

    # 3. Process items and update Dashboard live
    success_count = 0
    fail_count = 0

    for idx, vid_item in enumerate(videos_to_process, start=1):
        # Parse tags using regex
        production, names = parse_caption(vid_item['caption'])
        tags = ([production] if production else []) + names
        
        # Send to Raindrop API
        ok, status_msg = await asyncio.to_thread(
            send_to_raindrop, 
            vid_item['url'], 
            vid_item['title'], 
            tags
        )
        
        if ok:
            success_count += 1
            icon = "✅"
        else:
            fail_count += 1
            icon = "❌"

        # Update Live Pinned Dashboard
        bar = make_progress_bar(idx, total)
        tag_str = ", ".join([f"`#{t}`" for t in tags]) if tags else "*No tags parsed*"
        
        dashboard_text = (
            f"📊 **LIVE DASHBOARD**\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"**Progress:** [{bar}] `{idx}/{total}`\n"
            f"**Success:** {success_count} | **Failed:** {fail_count}\n\n"
            f"🏷 **Latest Item ({idx}/{total}):**\n"
            f"• **Title:** {vid_item['title'][:40]}...\n"
            f"• **Tags:** {tag_str}\n"
            f"• **Status:** {icon} {status_msg}\n"
            f"━━━━━━━━━━━━━━━━━━━"
        )
        
        await dashboard.edit_text(dashboard_text)
        await asyncio.sleep(0.5)  # Prevents Telegram flood limits

    # 4. Finalize Dashboard
    final_text = (
        f"🎉 **TASK COMPLETE**\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"✅ **Processed:** {total} videos\n"
        f"🟢 **Saved to Raindrop:** {success_count}\n"
        f"🔴 **Failed:** {fail_count}\n\n"
        f"🔗 **Source Link:** `{raw_url}`"
    )
    
    await dashboard.edit_text(final_text)
    try:
        await dashboard.unpin()
    except Exception:
        pass


if __name__ == "__main__":
    print("🤖 Stealth Bot with Raindrop & Live Dashboard is active...")
    app.run()