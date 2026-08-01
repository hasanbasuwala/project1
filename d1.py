import asyncio
import json
import os
import re
import time
from datetime import datetime
from pyrogram import Client, filters, compose
from pyrogram.errors import FloodWait, UserRestricted

import config

DB_FILE = "autoscan_db.json"

# --- DATABASE MANAGEMENT ---
def load_db():
    if not os.path.exists(DB_FILE):
        return {
            "monitored_groups": {}, # changed to dict to store titles: {id: title}
            "vaults": {},
            "dashboard_msg_id": None,
            "stats": {"vaults_created": 0, "messages_vaulted": 0, "waits_avoided": 0, "reconnects": 0}
        }
    with open(DB_FILE, "r") as f:
        db_data = json.load(f)
        # Handle migration if coming from old list-based monitored_groups
        if isinstance(db_data.get("monitored_groups"), list):
            db_data["monitored_groups"] = {str(k): "Unknown Group" for k in db_data["monitored_groups"]}
        if "stats" not in db_data:
            db_data["stats"] = {"vaults_created": 0, "messages_vaulted": 0, "waits_avoided": 0, "reconnects": 0}
        if "reconnects" not in db_data["stats"]:
            db_data["stats"]["reconnects"] = 0
        return db_data

def save_db(db_data):
    with open(DB_FILE, "w") as f:
        json.dump(db_data, f, indent=4)

db = load_db()

# --- INITIALIZE CLIENTS ---
user = Client("my_userbot", api_id=config.API_ID, api_hash=config.API_HASH, sleep_threshold=60)
bot = Client("my_control_bot", api_id=config.API_ID, api_hash=config.API_HASH, bot_token=config.SORT_TOKEN)

user_states = {}
sys_status = {"status_icon": "🟢", "status_text": "Optimal", "current_action": "💤 Idle"}

# Network watchdog state
net_state = {"connected": True, "disconnected_since": None}

# Network-ish exceptions that mean "we lost the connection", not "something is wrong with the request"
NETWORK_ERRORS = (ConnectionError, OSError, TimeoutError, asyncio.TimeoutError)

# --- DASHBOARD & UI FUNCTIONS ---
async def update_dashboard():
    """Generates and updates the pinned Job Card in the bot chat."""
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
            await bot.pin_chat_message(config.OWNER_ID, msg.id)
    except Exception as e:
        # If message was deleted manually, resend it
        if "MESSAGE_ID_INVALID" in str(e) or "MESSAGE_NOT_MODIFIED" not in str(e):
            msg = await bot.send_message(config.OWNER_ID, dashboard_text)
            db["dashboard_msg_id"] = msg.id
            save_db(db)
            try:
                await bot.pin_chat_message(config.OWNER_ID, msg.id)
            except: pass

async def flash_message(text: str, delay: int = 10):
    """Sends a message and deletes it after X seconds to keep the chat clean."""
    try:
        msg = await bot.send_message(config.OWNER_ID, text)
        await asyncio.sleep(delay)
        await msg.delete()
    except Exception:
        pass

async def set_system_state(icon, text, action=None):
    """Helper to update state and trigger dashboard refresh."""
    sys_status["status_icon"] = icon
    sys_status["status_text"] = text
    if action:
        sys_status["current_action"] = action
    await update_dashboard()

# --- NETWORK WATCHDOG ---
def fmt_duration(seconds: float) -> str:
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"

async def network_watchdog(interval: int = 20):
    """
    Periodically pings Telegram via a cheap call. If the ping fails with a
    network-type error, we mark ourselves disconnected (and stop trying to
    push dashboard/flash updates, since those will just fail too). As soon
    as a ping succeeds again, we notify the owner with the outage duration.
    This survives the whole box losing internet and coming back, because
    Pyrogram's own client will keep trying to reconnect underneath us.
    """
    await asyncio.sleep(5)  # let clients finish connecting first
    while True:
        try:
            await bot.get_me()

            if not net_state["connected"]:
                # We were down, now we're back
                downtime = time.time() - net_state["disconnected_since"]
                net_state["connected"] = True
                net_state["disconnected_since"] = None
                db["stats"]["reconnects"] += 1
                save_db(db)

                await set_system_state("🟢", "Optimal", "💤 Idle")
                asyncio.create_task(flash_message(
                    f"🌐 **Back Online**\nConnection restored after {fmt_duration(downtime)} of downtime. "
                    f"Monitoring resumed normally.",
                    20
                ))

        except NETWORK_ERRORS:
            if net_state["connected"]:
                net_state["connected"] = False
                net_state["disconnected_since"] = time.time()
                sys_status["status_icon"] = "🔴"
                sys_status["status_text"] = "Network Down"
                sys_status["current_action"] = "📡 Waiting for connection..."
                # Don't try to push this to Telegram right now — it's down.
                # It'll show up as soon as update_dashboard succeeds again.

        except Exception:
            # Non-network errors (e.g. auth issues) aren't the watchdog's job.
            pass

        await asyncio.sleep(interval)

# --- CORE LOGIC WITH SAFETY PAUSES ---
async def get_or_create_vault(tag: str, original_chat_title: str):
    if tag in db["vaults"]:
        return db["vaults"][tag]
    
    vault_title = f"{original_chat_title[:30]} Vault - {tag}"
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            await set_system_state("⏳", "Creating Group...", f"Vault for {tag}")
            new_group = await user.create_supergroup(vault_title, f"Auto-archived messages for {tag}")
            
            db["vaults"][tag] = new_group.id
            db["stats"]["vaults_created"] += 1
            save_db(db)
            
            asyncio.create_task(flash_message(f"🆕 **Vault Created:** {tag}"))
            
            # Anti-spam cooldown
            await set_system_state("🟡", "Anti-Spam Cooldown", f"Resting for 15s after creating {tag}")
            await asyncio.sleep(15)
            await set_system_state("🟢", "Optimal")
            return new_group.id
            
        except FloodWait as e:
            db["stats"]["waits_avoided"] += 1
            save_db(db)
            wait_time = e.value + 5
            await set_system_state("🔴", f"Rate Limited", f"Waiting {wait_time}s before retrying {tag}")
            await asyncio.sleep(wait_time)
            await set_system_state("🟢", "Optimal")
            
        except UserRestricted:
            await set_system_state("🚫", "Account Restricted", "Telegram temporarily blocked group creation.")
            asyncio.create_task(flash_message("🚨 **ALERT:** Telegram restricted your account from creating new groups for now.", 30))
            return None

        except NETWORK_ERRORS:
            # Network dropped mid-operation — back off and let the watchdog
            # notice; retry this vault creation once things recover.
            await asyncio.sleep(10)

        except Exception as e:
            print(f"❌ Failed to create vault for {tag}: {e}")
            return None
            
    asyncio.create_task(flash_message(f"⚠️ Skipped creating vault for {tag} after 3 failed attempts.", 15))
    return None

async def safe_copy(vault_id: int, chat_id: int, msg_id: int):
    for attempt in range(3):
        try:
            await user.copy_message(chat_id=vault_id, from_chat_id=chat_id, message_id=msg_id)
            db["stats"]["messages_vaulted"] += 1
            save_db(db)
            await asyncio.sleep(0.5) 
            return True
        except FloodWait as e:
            db["stats"]["waits_avoided"] += 1
            save_db(db)
            await set_system_state("🔴", f"Rate Limited", f"Waiting {e.value}s to copy message")
            await asyncio.sleep(e.value + 2)
            await set_system_state("🟢", "Optimal")
        except NETWORK_ERRORS:
            await asyncio.sleep(10)
        except Exception:
            return False
    return False

async def process_history_sweep(chat_id: int, chat_title: str, target_tag: str = None, wipe_only: bool = False):
    await set_system_state("🟢", "Optimal", f"Gathering history from {chat_title}")
    messages_to_process = []
    
    try:
        if target_tag:
            total_count = await user.search_messages_count(chat_id, query=target_tag)
            if total_count == 0:
                asyncio.create_task(flash_message(f"✅ No messages found for '{target_tag}' in {chat_title}."))
                await set_system_state("🟢", "Optimal", "💤 Idle")
                return
                
            async for msg in user.search_messages(chat_id, query=target_tag):
                messages_to_process.append(msg)
        else:
            async for msg in user.get_chat_history(chat_id):
                text = msg.text or msg.caption or ""
                if "#" in text:
                    messages_to_process.append(msg)
                    
            total_count = len(messages_to_process)
            if total_count == 0:
                asyncio.create_task(flash_message(f"✅ No hashtagged messages found in {chat_title}."))
                await set_system_state("🟢", "Optimal", "💤 Idle")
                return
            
    except Exception as e:
        asyncio.create_task(flash_message(f"❌ Error gathering messages: {e}"))
        await set_system_state("🟢", "Optimal", "💤 Idle")
        return

    processed_count = 0
    deleted_count = 0
    messages_to_process.reverse() 

    for msg in messages_to_process:
        text = msg.text or msg.caption or ""
        tags = list(set(re.findall(r'(#\w+)', text.lower())))
        
        if target_tag:
            if target_tag.lower() not in tags: continue
            tags = [target_tag.lower()]
            
        if not tags: continue
            
        success = False
        if wipe_only:
            success = True
        else:
            for tag in tags:
                vault_id = await get_or_create_vault(tag, chat_title)
                if vault_id:
                    success = await safe_copy(vault_id, chat_id, msg.id)

        if success:
            try:
                await user.delete_messages(chat_id, msg.id)
                deleted_count += 1
            except: pass

        processed_count += 1
        
        # Update dashboard every 5 messages to show progress
        if processed_count % 5 == 0:
            await set_system_state("🟢", "Optimal", f"Sweeping {chat_title} ({processed_count}/{total_count})")

    mode = "wiped" if wipe_only else "vaulted and wiped"
    asyncio.create_task(flash_message(f"✅ **Sweep Complete!**\n{chat_title}: {deleted_count} messages {mode}.", 20))
    await set_system_state("🟢", "Optimal", "💤 Idle")


# --- BOT COMMAND INTERFACE ---
@bot.on_message(filters.command("start") & filters.user(config.OWNER_ID))
async def bot_start(client, message):
    text = (
        "🤖 **AutoScan Control Panel**\n"
        "🔹 `/dashboard` - Refresh/Pin the Job Card\n"
        "🔹 `/autoscan` - Scan history & active live monitor\n"
        "🔹 `/stopscan` - Stop monitoring a group\n"
        "🔹 `/vault` - Move a specific #tag (History)\n"
        "🔹 `/wipe` - Delete a specific #tag (History)\n"
        "🔹 `/stopbot` - Shut down"
    )
    await message.reply_text(text)

@bot.on_message(filters.command("dashboard") & filters.user(config.OWNER_ID))
async def cmd_dashboard(client, message):
    # Force resend of dashboard
    db["dashboard_msg_id"] = None 
    await update_dashboard()
    await message.delete() # keep chat clean

@bot.on_message(filters.command(["autoscan", "stopscan", "vault", "wipe"]) & filters.user(config.OWNER_ID))
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

@bot.on_message(filters.text & filters.user(config.OWNER_ID) & ~filters.command(["start", "dashboard", "autoscan", "stopscan", "vault", "wipe", "stopbot"]))
async def process_wizard_inputs(client, message):
    state = user_states.get(config.OWNER_ID)
    if not state: return 
        
    action = state["action"]
    step = state["step"]
    
    if step == "need_group":
        raw_id = message.text.strip()
        try:
            chat_id = int(raw_id) if raw_id.replace("-", "").isdigit() else raw_id
            chat = await user.get_chat(chat_id)
            state["chat_id"] = chat.id
            state["chat_title"] = chat.title
            
            # Delete user input and prompt for cleanliness
            await message.delete()
            try: await bot.delete_messages(config.OWNER_ID, state["prompt_msg"])
            except: pass
            
            if action in ["vault", "wipe"]:
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
        tag = message.text.strip().lower()
        await message.delete()
        try: await bot.delete_messages(config.OWNER_ID, state["prompt_msg"])
        except: pass

        if not tag.startswith("#"):
            prompt = await message.reply_text("⚠️ Must start with '#'. Try again.")
            state["prompt_msg"] = prompt.id
            return
            
        chat_id = state["chat_id"]
        chat_title = state["chat_title"]
        user_states.pop(config.OWNER_ID, None)
        
        asyncio.create_task(flash_message(f"🚀 Initializing {action} for {tag} in {chat_title}..."))
        if action == "vault":
            asyncio.create_task(process_history_sweep(chat_id, chat_title, target_tag=tag, wipe_only=False))
        elif action == "wipe":
            asyncio.create_task(process_history_sweep(chat_id, chat_title, target_tag=tag, wipe_only=True))

# --- LIVE LISTENER ---
@user.on_message(filters.group & ~filters.me, group=1)
async def live_hashtag_listener(client, message):
    chat_id = str(message.chat.id)
    if chat_id not in db["monitored_groups"]:
        return
        
    text = message.text or message.caption or ""
    tags = list(set(re.findall(r'(#\w+)', text.lower())))
    if not tags: return

    chat_title = message.chat.title or "Archive"
    success = False
    
    # Temporarily update dashboard for live action
    await set_system_state("⚡", "Live Event", f"Routing new msg in {chat_title}")
    
    for tag in tags:
        vault_id = await get_or_create_vault(tag, chat_title)
        if vault_id:
            if await safe_copy(vault_id, message.chat.id, message.id):
                success = True

    if success:
        try:
            await user.delete_messages(message.chat.id, message.id)
            asyncio.create_task(flash_message(f"⚡ Live msg vaulted: {', '.join(tags)}", 5))
        except: pass
        
    await set_system_state("🟢", "Optimal", "💤 Idle")

# --- STARTUP (crash/drop resilient) ---
async def main_startup():
    print("Starting Userbot and Control Bot safely...")
    backoff = 10
    while True:
        try:
            asyncio.create_task(network_watchdog())
            await compose([user, bot])
            # compose() only returns on a clean shutdown (e.g. /stopbot uses os._exit,
            # so normally we won't even get here) — break out if it does.
            break
        except NETWORK_ERRORS as e:
            print(f"⚠️ Lost connection during startup/runtime ({e}). Retrying in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 120)  # exponential backoff, capped at 2 min
        except Exception as e:
            print(f"❌ Unexpected fatal error: {e}. Retrying in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 120)

if __name__ == '__main__':
    # Initialize dashboard on boot
    asyncio.run(main_startup())
