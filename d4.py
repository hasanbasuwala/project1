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

DB_FILE = "autoscan_db.json"  # same file d3 used — existing vaults/monitored_groups carry over

# ============================================================
# CANONICAL TAG NORMALIZATION
# ============================================================
def normalize_tag(raw: str) -> str:
    """
    Single source of truth for what a '#Name' tag looks like everywhere in
    this script: lowercase, single leading '#', no surrounding whitespace.
    Every tag lookup/creation goes through this so "#AlanahRae",
    "#alanahrae", " #AlanahRae " etc. all resolve to the same vault.
    """
    if not raw:
        return raw
    tag = raw.strip().lower()
    if not tag.startswith("#"):
        tag = "#" + tag
    return tag

def _dedupe_vaults(vaults: dict) -> dict:
    """Folds any pre-existing case-variant duplicate tags into one
    canonical key, keeping whichever group id was seen first."""
    normalized = {}
    for raw_tag, group_id in vaults.items():
        canon = normalize_tag(raw_tag)
        if canon not in normalized:
            normalized[canon] = group_id
    return normalized

# --- DATABASE MANAGEMENT ---
def load_db():
    if not os.path.exists(DB_FILE):
        return {
            "monitored_groups": {},
            "vaults": {},
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
        db_data["vaults"] = _dedupe_vaults(db_data.get("vaults", {}))
        return db_data

def save_db(db_data):
    with open(DB_FILE, "w") as f:
        json.dump(db_data, f, indent=4)

db = load_db()

# --- CLIENTS ---
# `app` = your account (session persists, drives direct in-group commands + live listener)
# `bot` = the control bot (DM wizard, dashboard, status pings) — needs config.SORT_TOKEN
#         to be your NEW bot's token, and config.OWNER_ID to be your user id.
app = Client("d_session", api_id=config.API_ID, api_hash=config.API_HASH, sleep_threshold=60)
bot = Client("my_control_bot", api_id=config.API_ID, api_hash=config.API_HASH, bot_token=config.SORT_TOKEN)

user_states = {}

sys_status = {"status_icon": "🟢", "status_text": "Optimal", "current_action": "💤 Idle"}
net_state = {"connected": True, "disconnected_since": None}
NETWORK_ERRORS = (ConnectionError, OSError, TimeoutError, asyncio.TimeoutError)
FATAL_AUTH_ERRORS = (SessionRevoked, AuthKeyUnregistered, AuthKeyDuplicated, UserDeactivated)

# Flip to True if you need to see every message the account observes while
# debugging connectivity/handler issues. Off by default.
DEBUG = False

# ============================================================
# MASTER-INDEX CAPTION PARSING (album-aware tag matching)
# ============================================================
def parse_master_index(master_caption: str, query: str) -> dict:
    """
    Parses a caption like:
        #AlanahRae
        1 - Alanah Rae (My Friend 2)
        2 - Alanah Rae (Naught Girls)
    Returns {1: "My Friend 2", 2: "Naught Girls", ...}. If a line has no
    parentheses, the whole remainder after "N - " is used as the caption.
    """
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
    """
    Searches for `query` in chat_id. For every hit that's part of a media
    group (album), pulls the WHOLE album (not just the message that
    matched), resolves the master-index caption, and attaches a
    per-item `_resolved_caption`. Standalone messages keep their own
    caption. Returns the flat list of matched message objects.
    """
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
# SELF-DASHBOARD (Saved Messages) — replaces the separate control bot
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
        "🛠 **AUTOSCAN JOB CARD** 🛠\n\n"
        f"📡 **System Status:** {sys_status['status_icon']} {sys_status['status_text']}\n"
        f"🔄 **Current Action:** {sys_status['current_action']}\n\n"
        f"📂 **Active Monitored Groups:**\n{groups_list}\n"
        f"📊 **Session Stats:**\n"
        f"🔹 Vaults Created: `{stats['vaults_created']}`\n"
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
    """Sends a status message via the control bot (DM to OWNER_ID) and
    deletes it after X seconds."""
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
# CORE LOGIC (vaults, copying, sweeping)
# ============================================================
async def get_or_create_vault(tag: str, original_chat_title: str):
    """Reuses an existing vault for this tag if one exists; otherwise
    creates it (bounded retries, not unbounded recursion)."""
    tag = normalize_tag(tag)
    if tag in db["vaults"]:
        return db["vaults"][tag]

    vault_title = f"{original_chat_title[:30]} Vault - {tag}"
    max_retries = 3

    for attempt in range(max_retries):
        try:
            await set_system_state("⏳", "Creating Group...", f"Vault for {tag}")
            new_group = await app.create_supergroup(vault_title, f"Auto-archived messages for {tag}")

            db["vaults"][tag] = new_group.id
            db["stats"]["vaults_created"] += 1
            save_db(db)

            asyncio.create_task(flash_message(f"🆕 **Vault Created:** {tag}"))

            await set_system_state("🟡", "Anti-Spam Cooldown", f"Resting for 15s after creating {tag}")
            await asyncio.sleep(15)
            await set_system_state("🟢", "Optimal")
            return new_group.id

        except FloodWait as e:
            db["stats"]["waits_avoided"] += 1
            save_db(db)
            wait_time = e.value + 5
            await set_system_state("🔴", "Rate Limited", f"Waiting {wait_time}s before retrying {tag}")
            asyncio.create_task(flash_message(
                f"⏳ **Rate Limited creating '{tag}'** — Telegram wants a {fmt_duration(wait_time)} wait "
                f"before another group can be created. This is an account-level limit, not a bug; "
                f"it will retry automatically once the wait elapses.", 20
            ))
            await asyncio.sleep(wait_time)
            await set_system_state("🟢", "Optimal")

        except UserRestricted:
            await set_system_state("🚫", "Account Restricted", "Telegram temporarily blocked group creation.")
            asyncio.create_task(flash_message("🚨 **ALERT:** Telegram restricted your account from creating new groups for now.", 30))
            return None

        except NETWORK_ERRORS:
            await asyncio.sleep(10)

        except Exception as e:
            print(f"❌ Failed to create vault for {tag}: {e}")
            return None

    asyncio.create_task(flash_message(f"⚠️ Skipped creating vault for {tag} after {max_retries} failed attempts.", 15))
    return None

async def safe_copy(vault_id: int, chat_id: int, msg_id: int, caption: str = None):
    """Copies a message into a vault. `caption`, if given, REPLACES the
    original caption on the copy (the resolved per-video caption from the
    master index)."""
    for attempt in range(3):
        try:
            kwargs = {}
            if caption is not None:
                kwargs["caption"] = caption
            await app.copy_message(chat_id=vault_id, from_chat_id=chat_id, message_id=msg_id, **kwargs)
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
    """
    status_message: an optional Message object (from the triggering command)
    that gets live-edited with progress, giving the same instant inline
    feedback d1 had — in addition to the Saved Messages dashboard.
    """
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
                vault_id = await get_or_create_vault(tag, chat_title)
                if vault_id:
                    success = await safe_copy(vault_id, chat_id, msg.id, caption=caption_override)

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
        summary = f"{deleted_count} messages vaulted and wiped."
    else:
        summary = f"{copied_count} messages copied to vault (originals kept)."

    await _status(f"✅ Sweep Complete!\n{chat_title}: {summary}")
    asyncio.create_task(flash_message(f"✅ **Sweep Complete!**\n{chat_title}: {summary}", 20))
    await set_system_state("🟢", "Optimal", "💤 Idle")

# ============================================================
# CONTROL BOT DM WIZARD — private chat with the bot, no in-group typing
# ============================================================
# Scoped strictly to filters.private so this can never double-fire
# alongside the direct in-group commands below on the same message,
# even if the bot happens to also be a member of that group.

@bot.on_message(filters.command("start") & filters.user(config.OWNER_ID))
async def bot_start(client, message):
    text = (
        "🤖 **AutoScan Control Panel**\n"
        "🔹 `/dashboard` - Refresh/Pin the Job Card\n"
        "🔹 `/autoscan` - Scan history & active live monitor\n"
        "🔹 `/stopscan` - Stop monitoring a group\n"
        "🔹 `/vault` - Move a specific #tag (History)\n"
        "🔹 `/copyonly` - Copy a #tag to vault, keep originals (History)\n"
        "🔹 `/wipe` - Delete a specific #tag (History)\n"
        "🔹 `/stopbot` - Shut down\n\n"
        "💡 You can also type these directly inside the target group itself "
        "(sent from your own account) instead of using this DM wizard:\n"
        "`/autoscan`, `/stopscan`, `/vault #tag`, `/copyonly #tag`, `/wipe #tag`, `/stopbot`"
    )
    await message.reply_text(text)

@bot.on_message(filters.command("dashboard") & filters.user(config.OWNER_ID))
async def cmd_dashboard(client, message):
    db["dashboard_msg_id"] = None
    await update_dashboard()
    await message.delete()

@bot.on_message(filters.command(["autoscan", "stopscan", "vault", "wipe", "copyonly"]) & filters.user(config.OWNER_ID) & filters.private)
async def initiate_command(client, message):
    cmd = message.command[0].lower()
    user_states[config.OWNER_ID] = {"action": cmd, "step": "need_group"}
    prompt = await message.reply_text(f"🛠️ **Command:** `/{cmd}`\nSend the **Group ID** or **Username**.")
    user_states[config.OWNER_ID]["prompt_msg"] = prompt.id
    await message.delete()

@bot.on_message(filters.command("stopbot") & filters.user(config.OWNER_ID))
async def bot_stopbot(client, message):
    await message.reply_text("🛑 **System Offline.**")
    os._exit(0)

@bot.on_message(filters.text & filters.user(config.OWNER_ID) & ~filters.command(["start", "dashboard", "autoscan", "stopscan", "vault", "wipe", "copyonly", "stopbot"]))
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

            if action in ["vault", "wipe", "copyonly"]:
                state["step"] = "need_tag"
                prompt = await message.reply_text(f"🎯 Target: **{chat.title}**\nSend the exact **#hashtag**.")
                state["prompt_msg"] = prompt.id
            elif action == "autoscan":
                db["monitored_groups"][str(chat.id)] = chat.title
                save_db(db)
                asyncio.create_task(flash_message(f"🔄 **AutoScan Activated:** {chat.title}"))
                user_states.pop(config.OWNER_ID, None)
                asyncio.create_task(process_history_sweep(chat.id, chat.title))
            elif action == "stopscan":
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
        if action == "vault":
            asyncio.create_task(process_history_sweep(chat_id, chat_title, target_tag=tag, wipe_only=False, delete_after=True))
        elif action == "copyonly":
            asyncio.create_task(process_history_sweep(chat_id, chat_title, target_tag=tag, wipe_only=False, delete_after=False))
        elif action == "wipe":
            asyncio.create_task(process_history_sweep(chat_id, chat_title, target_tag=tag, wipe_only=True))

# ============================================================
# DIRECT COMMANDS — typed straight into the target chat, self-sent
# ============================================================
@app.on_message(filters.command(["autoscan", "stopscan"], prefixes=["/", "."]) & filters.me & filters.group)
async def direct_scan_toggle(client, message):
    cmd = message.command[0].lower()
    chat_id = message.chat.id
    chat_title = message.chat.title or "Archive"
    status = await message.reply_text(f"🔄 Processing `/{cmd}` for {chat_title}...")
    try: await message.delete()
    except: pass

    if cmd == "autoscan":
        db["monitored_groups"][str(chat_id)] = chat_title
        save_db(db)
        await status.edit_text(f"🔄 **AutoScan Activated:** {chat_title}\nSweeping history now...")
        await update_dashboard()
        await process_history_sweep(chat_id, chat_title, status_message=status)
    else:  # stopscan
        if str(chat_id) in db["monitored_groups"]:
            del db["monitored_groups"][str(chat_id)]
            save_db(db)
            await status.edit_text(f"🛑 Stopped monitoring: {chat_title}")
            await update_dashboard()
        else:
            await status.edit_text("⚠️ Group not in active list.")

@app.on_message(filters.command(["vault", "copyonly", "wipe"], prefixes=["/", "."]) & filters.me & filters.group)
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

    if cmd == "vault":
        await process_history_sweep(chat_id, chat_title, target_tag=tag, wipe_only=False, delete_after=True, status_message=status)
    elif cmd == "copyonly":
        await process_history_sweep(chat_id, chat_title, target_tag=tag, wipe_only=False, delete_after=False, status_message=status)
    elif cmd == "wipe":
        await process_history_sweep(chat_id, chat_title, target_tag=tag, wipe_only=True, status_message=status)

@app.on_message(filters.command("dashboard", prefixes=["/", "."]) & filters.me)
async def direct_dashboard(client, message):
    db["dashboard_msg_id"] = None
    await update_dashboard()
    try: await message.delete()
    except: pass

@app.on_message(filters.command("ping", prefixes=["/", "."]) & filters.me)
async def direct_ping(client, message):
    await message.reply_text("🏓 Userbot is alive!")

@app.on_message(filters.command("stopbot", prefixes=["/", "."]) & filters.me)
async def direct_stopbot(client, message):
    try: await message.reply_text("🛑 **System Offline.**")
    except: pass
    os._exit(0)

# ============================================================
# LIVE LISTENER (auto-vaults new hashtagged messages in monitored groups)
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
        vault_id = await get_or_create_vault(tag, chat_title)
        if vault_id:
            if await safe_copy(vault_id, message.chat.id, message.id):
                success = True

    if success:
        try:
            await app.delete_messages(message.chat.id, message.id)
            asyncio.create_task(flash_message(f"⚡ Live msg vaulted: {', '.join(tags)}", 5))
        except: pass

    await set_system_state("🟢", "Optimal", "💤 Idle")

if DEBUG:
    @app.on_message(filters.me, group=2)
    async def debug_catch_all(client, message):
        print(f"👤 [SELF] -> {message.chat.type}: {message.text or 'Media'}")

# ============================================================
# STARTUP (crash/drop resilient, two clients)
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
    print("Starting Userbot and Control Bot safely...")
    backoff = 10
    while True:
        try:
            await app.start()
            await bot.start()
            asyncio.create_task(network_watchdog())
            await set_system_state("🟢", "Optimal", "💤 Idle")
            print("✅ AutoScan System is Online and Monitoring. Press Ctrl+C to stop.")
            await idle()
            break  # idle() returns on a clean Ctrl+C shutdown
        except FATAL_AUTH_ERRORS as e:
            print(f"🚫 FATAL AUTH ERROR: {e}")
            print("One of the two sessions has been invalidated (e.g. 'Terminate all sessions' "
                  "in Telegram, a revoked bot token, or logged out elsewhere). Retrying cannot fix this.")
            print("Fix: delete the affected .session file(s) in this folder and rerun to log back in.")
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
