import asyncio
import json
import os
import re
import sys
import time
from datetime import datetime
from pyrogram import Client, filters, idle
from pyrogram.errors import (
    FloodWait, UserRestricted, SessionRevoked, AuthKeyUnregistered,
    AuthKeyDuplicated, UserDeactivated,
)

import config

DB_FILE = "autoscan_db.json"

# ============================================================
# CANONICAL TAG NORMALIZATION
# ============================================================
def normalize_tag(raw: str) -> str:
    """
    Single source of truth for what a '#Name' tag looks like everywhere in
    this script: lowercase, single leading '#', no surrounding whitespace.
    Every tag lookup/creation goes through this so "#AlanahRae",
    "#alanahrae", " #AlanahRae " etc. all resolve to the same feed topic.
    """
    if not raw:
        return raw
    tag = raw.strip().lower()
    if not tag.startswith("#"):
        tag = "#" + tag
    return tag

def _dedupe_feeds(feeds: dict) -> dict:
    """Folds any pre-existing case-variant duplicate tags into one
    canonical key, keeping whichever topic id was seen first."""
    normalized = {}
    for raw_tag, topic_id in feeds.items():
        canon = normalize_tag(raw_tag)
        if canon not in normalized:
            normalized[canon] = topic_id
    return normalized

# --- DATABASE MANAGEMENT ---
def load_db():
    if not os.path.exists(DB_FILE):
        return {
            "monitored_groups": {},
            "vaults": {},  # Kept as legacy key name in JSON for seamless migration of old vaults dict if needed
            "feeds": {},   # New master key for Forum Topic IDs mapping to tags
            "dashboard_msg_id": None,
            "stats": {"vaults_created": 0, "messages_vaulted": 0, "waits_avoided": 0, "reconnects": 0}
        }
    with open(DB_FILE, "r") as f:
        db_data = json.load(f)
        if isinstance(db_data.get("monitored_groups"), list):
            db_data["monitored_groups"] = {str(k): "Unknown Group" for k in db_data["monitored_groups"]}
        if "stats" not in db_data:
            db_data["stats"] = {"vaults_created": 0, "messages_vaulted": 0, "waits_avoided": 0, "reconnects": 0}
        if "reconnects" not in db_data["stats"]:
            db_data["stats"]["reconnects"] = 0
            
        # Support old vaults dict migration into feeds seamlessly
        raw_feeds = db_data.get("feeds", {})
        if not raw_feeds and db_data.get("vaults"):
            raw_feeds = db_data.get("vaults")
        db_data["feeds"] = _dedupe_feeds(raw_feeds)
        return db_data

def save_db(db_data):
    with open(DB_FILE, "w") as f:
        json.dump(db_data, f, indent=4)

db = load_db()

# --- CLIENTS ---
# `app` = your account (session persists, drives direct in-group commands + live listener)
# `bot` = the control bot (DM wizard, dashboard, status pings) — uses config.COPY_TOKEN
app = Client("d_session", api_id=config.API_ID, api_hash=config.API_HASH, sleep_threshold=60)
bot = Client("my_control_bot", api_id=config.API_ID, api_hash=config.API_HASH, bot_token=config.COPY_TOKEN)

user_states = {}

sys_status = {"status_icon": "🟢", "status_text": "Optimal", "current_action": "💤 Idle"}
net_state = {"connected": True, "disconnected_since": None}
NETWORK_ERRORS = (ConnectionError, OSError, TimeoutError, asyncio.TimeoutError)
FATAL_AUTH_ERRORS = (SessionRevoked, AuthKeyUnregistered, AuthKeyDuplicated, UserDeactivated)

# Flip to True if you need to see every message the account observes while debugging
DEBUG = False

# ============================================================
# MASTER-INDEX CAPTION PARSING (album-aware tag matching)
# ============================================================
def parse_master_index(master_caption: str, query: str) -> dict:
    query = normalize_tag(query)
    lines = [line.strip() for line in master_caption.strip().split('\n') if line.strip()]
    if not lines:
        return {}

    is_top_match = query.lower() in lines[0].lower()
    track_data = {}
    for line in lines[1:]:
        m = re.match(r'^(\d+)\s*-\s*(.*)', line)
        if not m:
            continue
        idx_str, rest_of_line = m.groups()
        is_inline_match = query.lower() in line.lower()
        if is_top_match or is_inline_match:
            bracket_match = re.search(r"\(([^)]*)\)", rest_of_line)
            track_caption = bracket_match.group(1).strip() if bracket_match else rest_of_line.strip()
            track_data[int(idx_str)] = track_caption
    return track_data

async def expand_tag_matches(chat_id: int, query: str):
    resolved = []
    processed_groups = set()
    query = normalize_tag(query)

    async for msg in app.search_messages(chat_id, query=query):
        if msg.media_group_id:
            if msg.media_group_id in processed_groups:
                continue
            processed_groups.add(msg.media_group_id)

            album_msgs = sorted(await app.get_media_group(chat_id, msg.id), key=lambda m: m.id)

            master_caption = ""
            for am in album_msgs:
                if am.caption and am.caption.strip().startswith("#"):
                    master_caption = am.caption
                    break

            track_data = parse_master_index(master_caption, query) if master_caption else {}

            for i, am in enumerate(album_msgs, start=1):
                if not (am.video or am.photo or am.document or am.animation):
                    continue
                if master_caption:
                    if i in track_data:
                        am._resolved_caption = track_data[i]
                        resolved.append(am)
                else:
                    am._resolved_caption = am.caption or f"Imported ({query})"
                    resolved.append(am)
        else:
            if msg.video or msg.photo or msg.document or msg.animation or msg.text:
                msg._resolved_caption = msg.caption or msg.text or f"Imported ({query})"
                resolved.append(msg)

    return resolved

# ============================================================
# SELF-DASHBOARD (Saved Messages)
# ============================================================
async def update_dashboard():
    groups_list = ""
    if not db["monitored_groups"]:
        groups_list = "None"
    else:
        for idx, (cid, title) in enumerate(db["monitored_groups"].items(), 1):
            groups_list += f"{idx}. {title} (`{cid}`)\n"

    stats = db["stats"]
    now = datetime.now().strftime("%H:%M:%S")

    dashboard_text = (
        "🛠 **AUTOSCAN JOB CARD (FORUM FEEDS)** 🛠\n\n"
        f"📡 **System Status:** {sys_status['status_icon']} {sys_status['status_text']}\n"
        f"🔄 **Current Action:** {sys_status['current_action']}\n\n"
        f"📂 **Active Monitored Groups:**\n{groups_list}\n"
        f"📊 **Session Stats:**\n"
        f"🔹 Feeds Created: `{stats.get('vaults_created', 0)}`\n"
        f"🔹 Messages Vaulted: `{stats['messages_vaulted']}`\n"
        f"🔹 FloodWaits Handled: `{stats['waits_avoided']}`\n"
        f"🔹 Reconnects: `{stats.get('reconnects', 0)}`\n\n"
        f"*(Last Updated: {now})*"
    )

    try:
        if db["dashboard_msg_id"]:
            await bot.edit_message_text(config.OWNER_ID, db["dashboard_msg_id"], dashboard_text)
        else:
            msg = await bot.send_message(config.OWNER_ID, dashboard_text)
            db["dashboard_msg_id"] = msg.id
            save_db(db)
            try: await bot.pin_chat_message(config.OWNER_ID, msg.id)
            except: pass
    except Exception as e:
        if "MESSAGE_ID_INVALID" in str(e) or "MESSAGE_NOT_MODIFIED" not in str(e):
            msg = await bot.send_message(config.OWNER_ID, dashboard_text)
            db["dashboard_msg_id"] = msg.id
            save_db(db)
            try: await bot.pin_chat_message(config.OWNER_ID, msg.id)
            except: pass

async def flash_message(text: str, delay: int = 10):
    try:
        msg = await bot.send_message(config.OWNER_ID, text)
        await asyncio.sleep(delay)
        await msg.delete()
    except Exception:
        pass

async def set_system_state(icon, text, action=None):
    sys_status["status_icon"] = icon
    sys_status["status_text"] = text
    if action:
        sys_status["current_action"] = action
    await update_dashboard()

# ============================================================
# NETWORK WATCHDOG
# ============================================================
def fmt_duration(seconds: float) -> str:
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h: return f"{h}h {m}m {s}s"
    if m: return f"{m}m {s}s"
    return f"{s}s"

async def network_watchdog(interval: int = 20):
    await asyncio.sleep(5)
    while True:
        try:
            await bot.get_me()
            if not net_state["connected"]:
                downtime = time.time() - net_state["disconnected_since"]
                net_state["connected"] = True
                net_state["disconnected_since"] = None
                db["stats"]["reconnects"] += 1
                save_db(db)
                await set_system_state("🟢", "Optimal", "💤 Idle")
                asyncio.create_task(flash_message(
                    f"🌐 **Back Online**\nConnection restored after {fmt_duration(downtime)} of downtime.", 20
                ))
        except NETWORK_ERRORS:
            if net_state["connected"]:
                net_state["connected"] = False
                net_state["disconnected_since"] = time.time()
                sys_status["status_icon"] = "🔴"
                sys_status["status_text"] = "Network Down"
                sys_status["current_action"] = "📡 Waiting for connection..."
        except Exception:
            pass
        await asyncio.sleep(interval)

# ============================================================
# CORE LOGIC (Master Supergroup Forum Feeds, Copying, Sweeping)
# ============================================================
async def get_or_create_feed_topic(tag: str):
    """Reuses an existing forum topic feed for this tag inside the master 
    supergroup config.FEED_GROUP_ID; otherwise creates a new topic thread via API."""
    tag = normalize_tag(tag)
    if "feeds" not in db:
        db["feeds"] = {}
    if tag in db["feeds"]:
        return db["feeds"][tag]

    max_retries = 3
    for attempt in range(max_retries):
        try:
            await set_system_state("⏳", "Creating Topic Feed...", f"Feed for {tag}")
            
            # Using Pyrogram create_forum_topic inside the master supergroup
            topic = await app.create_forum_topic(
                chat_id=config.FEED_GROUP_ID,
                title=tag
            )

            db["feeds"][tag] = topic.id
            db["stats"]["vaults_created"] = db["stats"].get("vaults_created", 0) + 1
            save_db(db)

            asyncio.create_task(flash_message(f"🆕 **Feed Topic Created:** {tag}"))
            await asyncio.sleep(2)
            await set_system_state("🟢", "Optimal")
            return topic.id

        except FloodWait as e:
            db["stats"]["waits_avoided"] += 1
            save_db(db)
            wait_time = e.value + 5
            await set_system_state("🔴", "Rate Limited", f"Waiting {wait_time}s before creating topic {tag}")
            asyncio.create_task(flash_message(
                f"⏳ **Rate Limited creating topic '{tag}'** — Waiting {fmt_duration(wait_time)}. "
                f"Will retry automatically.", 20
            ))
            await asyncio.sleep(wait_time)
            await set_system_state("🟢", "Optimal")

        except UserRestricted:
            await set_system_state("🚫", "Account Restricted", "Telegram temporarily blocked action.")
            asyncio.create_task(flash_message("🚨 **ALERT:** Telegram restricted your account temporarily.", 30))
            return None

        except NETWORK_ERRORS:
            await asyncio.sleep(10)

        except Exception as e:
            print(f"❌ Failed to create forum topic feed for {tag}: {e}")
            return None

    asyncio.create_task(flash_message(f"⚠️ Skipped creating feed topic for {tag} after {max_retries} attempts.", 15))
    return None

async def safe_copy(topic_id: int, chat_id: int, msg_id: int, caption: str = None):
    """Copies a message into the master feed supergroup specifying the thread/topic ID."""
    for attempt in range(3):
        try:
            kwargs = {"message_thread_id": topic_id}
            if caption is not None:
                kwargs["caption"] = caption
            await app.copy_message(chat_id=config.FEED_GROUP_ID, from_chat_id=chat_id, message_id=msg_id, **kwargs)
            db["stats"]["messages_vaulted"] += 1
            save_db(db)
            await asyncio.sleep(0.5)
            return True
        except FloodWait as e:
            db["stats"]["waits_avoided"] += 1
            save_db(db)
            await set_system_state("🔴", "Rate Limited", f"Waiting {e.value}s to copy message")
            await asyncio.sleep(e.value + 2)
            await set_system_state("🟢", "Optimal")
        except NETWORK_ERRORS:
            await asyncio.sleep(10)
        except Exception:
            return False
    return False

async def process_history_sweep(chat_id: int, chat_title: str, target_tag: str = None,
                                 wipe_only: bool = False, delete_after: bool = True,
                                 status_message=None):
    async def _status(text):
        if status_message:
            try: await status_message.edit_text(text)
            except: pass
        await set_system_state("🟢", "Optimal", text[:64])

    await _status(f"🔍 Gathering messages from {chat_title}...")
    messages_to_process = []

    try:
        if target_tag:
            messages_to_process = await expand_tag_matches(chat_id, target_tag)
            total_count = len(messages_to_process)
            if total_count == 0:
                await _status(f"❌ No messages found for '{target_tag}' in {chat_title}.")
                return
        else:
            async for msg in app.get_chat_history(chat_id):
                text = msg.text or msg.caption or ""
                if "#" in text:
                    msg._resolved_caption = msg.caption or msg.text or ""
                    messages_to_process.append(msg)
            total_count = len(messages_to_process)
            if total_count == 0:
                await _status(f"✅ No hashtagged messages found in {chat_title}.")
                return
    except Exception as e:
        await _status(f"❌ Error gathering messages: {e}")
        return

    await _status(f"📊 Found {total_count} messages. Processing...")

    processed_count = deleted_count = copied_count = 0
    messages_to_process.reverse()

    for msg in messages_to_process:
        if target_tag:
            tags = [target_tag]
        else:
            text = msg.text or msg.caption or ""
            tags = list({normalize_tag(t) for t in re.findall(r'(#\w+)', text.lower())})

        if not tags:
            continue

        success = False
        if wipe_only:
            success = True
        else:
            caption_override = getattr(msg, '_resolved_caption', None)
            for tag in tags:
                topic_id = await get_or_create_feed_topic(tag)
                if topic_id:
                    success = await safe_copy(topic_id, chat_id, msg.id, caption=caption_override)

        if success:
            copied_count += 1
            if delete_after:
                try:
                    await app.delete_messages(chat_id, msg.id)
                    deleted_count += 1
                except: pass

        processed_count += 1
        if processed_count % 5 == 0:
            await _status(f"⚙️ Sweeping {chat_title} ({processed_count}/{total_count})")

    if wipe_only:
        summary = f"{deleted_count} messages wiped."
    elif delete_after:
        summary = f"{deleted_count} messages routed to feeds and wiped."
    else:
        summary = f"{copied_count} messages copied to feed topics (originals kept)."

    await _status(f"✅ Sweep Complete!\n{chat_title}: {summary}")
    asyncio.create_task(flash_message(f"✅ **Sweep Complete!**\n{chat_title}: {summary}", 20))
    await set_system_state("🟢", "Optimal", "💤 Idle")

# ============================================================
# CONTROL BOT DM WIZARD — BotFather style commands
# ============================================================
def check_owner(_, __, message):
    return bool(message.from_user and message.from_user.id == config.OWNER_ID)
is_owner = filters.create(check_owner)

@bot.on_message(filters.command("start") & is_owner)
async def bot_start(client, message):
    text = (
        "🤖 **AutoScan Forum Feeds Control Panel**\n"
        "🔹 `/status` - Refresh/Pin the Job Card\n"
        "🔹 `/addsource` - Scan history & active live monitor group\n"
        "🔹 `/rmsource` - Stop monitoring a group\n"
        "🔹 `/movetag` - Move a specific #tag (History to Feed Topic)\n"
        "🔹 `/copytag` - Copy a #tag to Feed Topic, keep originals\n"
        "🔹 `/purgetag` - Delete a specific #tag\n"
        "🔹 `/shutdown` - Shut down bot\n\n"
        "💡 You can also type these directly inside the target group itself "
        "(sent from your own account):\n"
        "`/addsource`, `/rmsource`, `/movetag #tag`, `/copytag #tag`, `/purgetag #tag`, `/shutdown`"
    )
    await message.reply_text(text)

@bot.on_message(filters.command("status") & is_owner)
async def cmd_status(client, message):
    db["dashboard_msg_id"] = None
    await update_dashboard()
    await message.delete()

@bot.on_message(filters.command(["addsource", "rmsource", "movetag", "purgetag", "copytag"]) & is_owner & filters.private)
async def initiate_command(client, message):
    cmd = message.command[0].lower()
    user_states[config.OWNER_ID] = {"action": cmd, "step": "need_group"}
    prompt = await message.reply_text(f"🛠️ **Command:** `/{cmd}`\nSend the target **Group ID** or **Username**.")
    user_states[config.OWNER_ID]["prompt_msg"] = prompt.id
    await message.delete()

@bot.on_message(filters.command("shutdown") & is_owner)
async def bot_shutdown(client, message):
    await message.reply_text("🛑 **System Offline.**")
    os._exit(0)

@bot.on_message(filters.text & is_owner & ~filters.command(["start", "status", "addsource", "rmsource", "movetag", "purgetag", "copytag", "shutdown"]))
async def process_wizard_inputs(client, message):
    state = user_states.get(config.OWNER_ID)
    if not state: return

    action = state["action"]
    step = state["step"]

    if step == "need_group":
        raw_id = message.text.strip()
        try:
            chat_id = int(raw_id) if raw_id.replace("-", "").isdigit() else raw_id
            chat = await app.get_chat(chat_id)
            state["chat_id"] = chat.id
            state["chat_title"] = chat.title

            await message.delete()
            try: await bot.delete_messages(config.OWNER_ID, state["prompt_msg"])
            except: pass

            if action in ["movetag", "purgetag", "copytag"]:
                state["step"] = "need_tag"
                prompt = await message.reply_text(f"🎯 Target: **{chat.title}**\nSend the exact **#hashtag**.")
                state["prompt_msg"] = prompt.id
            elif action == "addsource":
                db["monitored_groups"][str(chat.id)] = chat.title
                save_db(db)
                asyncio.create_task(flash_message(f"🔄 **AutoScan Source Added:** {chat.title}"))
                user_states.pop(config.OWNER_ID, None)
                asyncio.create_task(process_history_sweep(chat.id, chat.title))
            elif action == "rmsource":
                if str(chat.id) in db["monitored_groups"]:
                    del db["monitored_groups"][str(chat.id)]
                    save_db(db)
                    asyncio.create_task(flash_message(f"🛑 **Stopped monitoring:** {chat.title}"))
                    await update_dashboard()
                else:
                    asyncio.create_task(flash_message("⚠️ Group not in active list."))
                user_states.pop(config.OWNER_ID, None)

        except Exception as e:
            asyncio.create_task(flash_message(f"❌ Error: {e}\nAre you a member? Try again."))
            await message.delete()

    elif step == "need_tag":
        raw_tag = message.text.strip()
        await message.delete()
        try: await bot.delete_messages(config.OWNER_ID, state["prompt_msg"])
        except: pass

        if not raw_tag.startswith("#"):
            prompt = await message.reply_text("⚠️ Must start with '#'. Try again.")
            state["prompt_msg"] = prompt.id
            return

        tag = normalize_tag(raw_tag)
        chat_id = state["chat_id"]
        chat_title = state["chat_title"]
        user_states.pop(config.OWNER_ID, None)

        asyncio.create_task(flash_message(f"🚀 Initializing {action} for {tag} in {chat_title}..."))
        if action == "movetag":
            asyncio.create_task(process_history_sweep(chat_id, chat_title, target_tag=tag, wipe_only=False, delete_after=True))
        elif action == "copytag":
            asyncio.create_task(process_history_sweep(chat_id, chat_title, target_tag=tag, wipe_only=False, delete_after=False))
        elif action == "purgetag":
            asyncio.create_task(process_history_sweep(chat_id, chat_title, target_tag=tag, wipe_only=True))

# ============================================================
# DIRECT COMMANDS — typed straight into the target chat, self-sent
# ============================================================
@app.on_message(filters.command(["addsource", "rmsource"], prefixes=["/", "."]) & filters.me & filters.group)
async def direct_scan_toggle(client, message):
    cmd = message.command[0].lower()
    chat_id = message.chat.id
    chat_title = message.chat.title or "Archive"
    status = await message.reply_text(f"🔄 Processing `/{cmd}` for {chat_title}...")
    try: await message.delete()
    except: pass

    if cmd == "addsource":
        db["monitored_groups"][str(chat_id)] = chat_title
        save_db(db)
        await status.edit_text(f"🔄 **AutoScan Source Added:** {chat_title}\nSweeping history now...")
        await update_dashboard()
        await process_history_sweep(chat_id, chat_title, status_message=status)
    else:  # rmsource
        if str(chat_id) in db["monitored_groups"]:
            del db["monitored_groups"][str(chat_id)]
            save_db(db)
            await status.edit_text(f"🛑 Stopped monitoring: {chat_title}")
            await update_dashboard()
        else:
            await status.edit_text("⚠️ Group not in active list.")

@app.on_message(filters.command(["movetag", "copytag", "purgetag"], prefixes=["/", "."]) & filters.me & filters.group)
async def direct_tag_command(client, message):
    cmd = message.command[0].lower()
    chat_id = message.chat.id
    chat_title = message.chat.title or "Archive"

    if len(message.command) < 2:
        prompt = await message.reply_text(f"⚠️ Usage: `/{cmd} #tagname`")
        await asyncio.sleep(5)
        try: await prompt.delete()
        except: pass
        try: await message.delete()
        except: pass
        return

    tag = normalize_tag(message.command[1])
    status = await message.reply_text(f"🔍 Starting `/{cmd}` for {tag} in {chat_title}...")
    try: await message.delete()
    except: pass

    if cmd == "movetag":
        await process_history_sweep(chat_id, chat_title, target_tag=tag, wipe_only=False, delete_after=True, status_message=status)
    elif cmd == "copytag":
        await process_history_sweep(chat_id, chat_title, target_tag=tag, wipe_only=False, delete_after=False, status_message=status)
    elif cmd == "purgetag":
        await process_history_sweep(chat_id, chat_title, target_tag=tag, wipe_only=True, status_message=status)

@app.on_message(filters.command("status", prefixes=["/", "."]) & filters.me)
async def direct_status(client, message):
    db["dashboard_msg_id"] = None
    await update_dashboard()
    try: await message.delete()
    except: pass

@app.on_message(filters.command("ping", prefixes=["/", "."]) & filters.me)
async def direct_ping(client, message):
    await message.reply_text("🏓 Userbot is alive!")

@app.on_message(filters.command("shutdown", prefixes=["/", "."]) & filters.me)
async def direct_shutdown(client, message):
    try: await message.reply_text("🛑 **System Offline.**")
    except: pass
    os._exit(0)

# ============================================================
# LIVE LISTENER (auto-routes new hashtagged messages into forum topics)
# ============================================================
@app.on_message(filters.group & ~filters.me, group=1)
async def live_hashtag_listener(client, message):
    chat_id = str(message.chat.id)
    if chat_id not in db["monitored_groups"]:
        return

    text = message.text or message.caption or ""
    tags = list({normalize_tag(t) for t in re.findall(r'(#\w+)', text.lower())})
    if not tags:
        return

    chat_title = message.chat.title or "Archive"
    success = False
    await set_system_state("⚡", "Live Event", f"Routing new msg in {chat_title}")

    for tag in tags:
        topic_id = await get_or_create_feed_topic(tag)
        if topic_id:
            if await safe_copy(topic_id, message.chat.id, message.id):
                success = True

    if success:
        try:
            await app.delete_messages(message.chat.id, message.id)
            asyncio.create_task(flash_message(f"⚡ Live msg routed to topic: {', '.join(tags)}", 5))
        except: pass

    await set_system_state("🟢", "Optimal", "💤 Idle")

if DEBUG:
    @app.on_message(filters.me, group=2)
    async def debug_catch_all(client, message):
        print(f"👤 [SELF] -> {message.chat.type}: {message.text or 'Media'}")

# ============================================================
# STARTUP
# ============================================================
async def _safe_stop():
    try:
        if app.is_connected:
            await app.stop()
    except Exception:
        pass
    try:
        if bot.is_connected:
            await bot.stop()
    except Exception:
        pass

async def main():
    if not hasattr(config, "FEED_GROUP_ID"):
        print("❌ CRITICAL ERROR: FEED_GROUP_ID is missing from your config.py!")
        print("Please add FEED_GROUP_ID = -100xxxxxxxxxx to config.py before running.")
        sys.exit(1)

    print("Starting Userbot and Control Bot safely with Forum Feeds Architecture...")
    backoff = 10
    while True:
        try:
            await app.start()
            await bot.start()
            asyncio.create_task(network_watchdog())
            await set_system_state("🟢", "Optimal", "💤 Idle")
            print("✅ AutoScan Forum Feeds System is Online. Press Ctrl+C to stop.")
            await idle()
            break
        except FATAL_AUTH_ERRORS as e:
            print(f"🚫 FATAL AUTH ERROR: {e}")
            print("One of the sessions has been invalidated. Delete the .session file(s) and rerun.")
            await _safe_stop()
            sys.exit(1)
        except NETWORK_ERRORS as e:
            print(f"⚠️ Lost connection ({e}). Retrying in {backoff}s...")
            await _safe_stop()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 120)
        except Exception as e:
            print(f"❌ Unexpected error: {e}. Retrying in {backoff}s...")
            await _safe_stop()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 120)

    print("\nDisconnecting clients safely...")
    await _safe_stop()
    print("🛑 Shutdown complete.")

if __name__ == "__main__":
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"\n❌ Exited with error: {e}")