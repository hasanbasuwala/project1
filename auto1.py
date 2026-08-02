import asyncio
import json
import os
import re
import sys
from datetime import datetime
from pyrogram import Client, filters
import pyrogram
from pyrogram.errors import FloodWait, UserRestricted
from pyrogram.types import Message
import config

# ==========================================
# 1. DATABASE & STATE MANAGEMENT
# ==========================================
DB_FILE = "autoscan_db.json"

db = {
    "monitored_groups": {},
    "vaults": {},
    "dashboard_msg_id": None,
    "stats": {
        "vaults_created": 0,
        "messages_vaulted": 0,
        "waits_avoided": 0,
        "reconnects": 0
    }
}

sys_status = {
    "icon": "🟢",
    "text": "System Idle",
    "action": None
}

# State machine for the Bot DM Wizard
user_states = {}

def normalize_tag(raw: str) -> str:
    """The single source of truth for case-insensitive tag matching."""
    if not raw:
        return ""
    cleaned = raw.strip().lower()
    if not cleaned.startswith("#"):
        cleaned = "#" + cleaned
    return cleaned

def load_db():
    global db
    if os.path.exists(DB_FILE):
        with open(DB_FILE, "r", encoding="utf-8") as f:
            loaded = json.load(f)
            db.update(loaded)
            
            if "stats" not in db:
                db["stats"] = {
                    "vaults_created": 0,
                    "messages_vaulted": 0,
                    "waits_avoided": 0,
                    "reconnects": 0
                }

def save_db():
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(db, f, indent=4)

# ==========================================
# 2. CLIENT INITIALIZATION
# ==========================================
user = Client("userbott_session", api_id=config.API_ID, api_hash=config.API_HASH)
bot = Client("bott_session", api_id=config.API_ID, api_hash=config.API_HASH, bot_token=config.SORT_TOKEN)

# ==========================================
# 3. HELPER FUNCTIONS & DASHBOARD
# ==========================================
async def set_system_state(icon, text, action=None):
    sys_status["icon"] = icon
    sys_status["text"] = text
    sys_status["action"] = action
    await update_dashboard()

async def update_dashboard():
    dashboard_text = (
        f"**Autoscan System Dashboard**\n"
        f"**Status:** {sys_status['icon']} {sys_status['text']}\n"
        f"**Action:** {sys_status['action'] or 'None'}\n\n"
        f"📊 **Stats:**\n"
        f"Vaults Created: {db['stats']['vaults_created']}\n"
        f"Messages Vaulted: {db['stats']['messages_vaulted']}\n"
        f"Rate Limits Absorbed: {db['stats']['waits_avoided']}\n"
    )
    
    try:
        if db.get("dashboard_msg_id"):
            await bot.edit_message_text(config.OWNER_ID, db["dashboard_msg_id"], dashboard_text)
        else:
            msg = await bot.send_message(config.OWNER_ID, dashboard_text)
            db["dashboard_msg_id"] = msg.id
            save_db()
            
            # The Fix: Added both_sides=True
            try:
                await bot.pin_chat_message(config.OWNER_ID, msg.id, disable_notification=True, both_sides=True)
            except Exception as pin_err:
                print(f"[Termux Logger] Note: Could not pin dashboard ({pin_err})")
                
    except Exception:
        # If message was deleted manually by you, send a fresh one
        msg = await bot.send_message(config.OWNER_ID, dashboard_text)
        db["dashboard_msg_id"] = msg.id
        save_db()
        try:
            await bot.pin_chat_message(config.OWNER_ID, msg.id, disable_notification=True, both_sides=True)
        except Exception:
            pass


async def get_or_create_vault(original_tag: str) -> int:
    norm_tag = normalize_tag(original_tag)
    
    if norm_tag in db["vaults"]:
        return db["vaults"][norm_tag]

    attempts = 0
    while attempts < 3:
        attempts += 1
        try:
            await set_system_state("⏳", "Creating Group...", f"Creating {original_tag}")
            new_group = await user.create_supergroup(original_tag, f"Archived media for: {original_tag}")
            
            db["vaults"][norm_tag] = new_group.id
            db["stats"]["vaults_created"] += 1
            save_db()
            
            await bot.send_message(config.OWNER_ID, f"✅ Created new vault: {original_tag}")
            await asyncio.sleep(15)
            return new_group.id
            
        except FloodWait as e:
            db["stats"]["waits_avoided"] += 1
            save_db()
            await set_system_state("🔴", "Rate Limited", f"Waiting {e.value}s...")
            await asyncio.sleep(e.value + 5)
        except UserRestricted:
            await bot.send_message(config.OWNER_ID, "❌ Account restricted from creating groups right now.")
            return None
        except Exception:
            await asyncio.sleep(10)

    await bot.send_message(config.OWNER_ID, f"❌ Failed to create vault for {original_tag} after 3 attempts.")
    return None

async def safe_copy(vault_id: int, chat_id: int, msg_id: int) -> bool:
    attempts = 0
    while attempts < 3:
        attempts += 1
        try:
            await user.copy_message(chat_id=vault_id, from_chat_id=chat_id, message_id=msg_id)
            db["stats"]["messages_vaulted"] += 1
            save_db()
            await asyncio.sleep(0.5)
            return True
        except FloodWait as e:
            db["stats"]["waits_avoided"] += 1
            save_db()
            await set_system_state("🔴", "Rate Limited", f"Waiting {e.value}s...")
            await asyncio.sleep(e.value + 5)
        except Exception:
            return False
    return False

def extract_tags(text: str):
    if not text:
        return []
    return list(set(re.findall(r'(#\w+)', text.lower())))

# ==========================================
# 4. CORE ORCHESTRATION (/autoscan logic)
# ==========================================
async def process_history_sweep(chat_id: int, target_tag=None, delete_after=False, wipe_only=False):
    await set_system_state("⏳", "Sweeping History", f"Scanning Chat ID: {chat_id}")
    messages_to_process = []
    
    async for msg in user.get_chat_history(chat_id):
        text = msg.text or msg.caption or ""
        
        if target_tag:
            norm_target = normalize_tag(target_tag)
            if norm_target in text.lower():
                messages_to_process.append(msg)
        else:
            if "#" in text:
                messages_to_process.append(msg)

    if not messages_to_process:
        await bot.send_message(config.OWNER_ID, "❌ No matching messages found to process.")
        await set_system_state("🟢", "System Idle", None)
        return

    messages_to_process.reverse()
    total = len(messages_to_process)
    processed = 0
    deleted_count = 0
    
    await bot.send_message(config.OWNER_ID, f"📊 Sweep Phase 1 Complete. Found {total} messages. Beginning operations...")

    for msg in messages_to_process:
        success_for_msg = False
        
        if wipe_only:
            success_for_msg = True
        else:
            text = msg.text or msg.caption or ""
            tags = [target_tag] if target_tag else extract_tags(text)
            
            for tag in tags:
                original_tag = target_tag if target_tag else tag
                vault_id = await get_or_create_vault(original_tag)
                if vault_id:
                    copy_success = await safe_copy(vault_id, chat_id, msg.id)
                    if copy_success:
                        success_for_msg = True

        if success_for_msg and (delete_after or wipe_only):
            try:
                await user.delete_messages(chat_id, [msg.id])
                deleted_count += 1
                if wipe_only:
                    await asyncio.sleep(0.5) # Throttle wipes
            except FloodWait as e:
                await asyncio.sleep(e.value + 5)
            except Exception:
                pass

        processed += 1
        if processed % 5 == 0:
            await set_system_state("⏳", "Processing...", f"{processed} / {total} processed")

    report = f"✅ **Operation Complete!**\nProcessed: {processed} messages."
    if delete_after or wipe_only:
        report += f"\nDeleted Originals: {deleted_count}"
        
    await bot.send_message(config.OWNER_ID, report)
    await set_system_state("🟢", "System Idle", None)

# ==========================================
# 5. USER COMMANDS (In-Group via Userbot)
# ==========================================
@user.on_message(filters.command("autoscan", prefixes=["/", "."]) & filters.me)
async def direct_autoscan(client, message):
    await message.delete()
    chat_id = message.chat.id
    db["monitored_groups"][str(chat_id)] = message.chat.title or "Unknown Group"
    save_db()
    await bot.send_message(config.OWNER_ID, f"📡 Now monitoring and sweeping: {db['monitored_groups'][str(chat_id)]}")
    asyncio.create_task(process_history_sweep(chat_id, target_tag=None, delete_after=False))

@user.on_message(filters.command("vault", prefixes=["/", "."]) & filters.me)
async def direct_vault(client, message):
    command_parts = message.text.split(" ", 1)
    if len(command_parts) < 2:
        temp = await message.reply_text("⚠️ Usage: `/vault #Tag`")
        await asyncio.sleep(5)
        await temp.delete()
        await message.delete()
        return
    tag = command_parts[1]
    chat_id = message.chat.id
    await message.delete()
    asyncio.create_task(process_history_sweep(chat_id, target_tag=tag, delete_after=True))

@user.on_message(filters.command("wipe", prefixes=["/", "."]) & filters.me)
async def direct_wipe(client, message):
    command_parts = message.text.split(" ", 1)
    if len(command_parts) < 2:
        return
    tag = command_parts[1]
    chat_id = message.chat.id
    await message.delete()
    asyncio.create_task(process_history_sweep(chat_id, target_tag=tag, delete_after=False, wipe_only=True))


# ==========================================
# 7. BOT COMMAND INTERFACE (DM Wizard)
# ==========================================
@bot.on_message(filters.command("start") & filters.user(config.OWNER_ID) & filters.private)
async def bot_start(client, message):
    await message.reply_text(
        "🤖 **Autoscan Control Panel**\n\n"
        "Commands:\n"
        "`/dashboard` - Refresh live status\n"
        "`/autoscan` - Monitor & sweep a group\n"
        "`/vault` - Move a specific tag to a vault\n"
        "`/wipe` - Delete a tag from a group\n"
        "`/stopbot` - Shut down safely"
    )

@bot.on_message(filters.command("dashboard") & filters.user(config.OWNER_ID) & filters.private)
async def cmd_dashboard(client, message):
    await message.delete()
    db["dashboard_msg_id"] = None  # Force recreate
    await update_dashboard()

@bot.on_message(filters.command("stopbot") & filters.user(config.OWNER_ID) & filters.private)
async def bot_stopbot(client, message):
    await message.reply_text("🛑 System Offline. Shutting down...")
    os._exit(0)

@bot.on_message(filters.command(["autoscan", "vault", "wipe"]) & filters.user(config.OWNER_ID) & filters.private)
async def initiate_command(client, message):
    action = message.command[0]
    user_states[config.OWNER_ID] = {"action": action, "step": "need_group"}
    
    await message.reply_text(
        f"⚙️ **{action.title()} Wizard Started**\n\n"
        f"Please send me the target **Group ID** (e.g., `-100123...`) or **Username** (e.g., `@groupname`).\n"
        f"*(You can cancel by sending /start)*"
    )

@bot.on_message(filters.text & filters.user(config.OWNER_ID) & filters.private & ~filters.command(["start", "dashboard", "autoscan", "vault", "wipe", "stopbot"]))
async def process_wizard_inputs(client, message):
    state = user_states.get(config.OWNER_ID)
    if not state:
        return

    if state["step"] == "need_group":
        chat_input = message.text.strip()
        
        # Parse numeric ID vs Username string
        try:
            chat_id_val = int(chat_input)
        except ValueError:
            chat_id_val = chat_input
        
        try:
            # We use the USER client to fetch the chat since the userbot has the memberships
            chat = await user.get_chat(chat_id_val)
            state["chat_id"] = chat.id
            state["chat_title"] = chat.title or str(chat.id)
        except Exception as e:
            await message.reply_text(f"❌ Could not find that chat (are you a member?): `{e}`\n\nPlease try again or send /start to cancel.")
            return

        if state["action"] == "autoscan":
            db["monitored_groups"][str(chat.id)] = state["chat_title"]
            save_db()
            await message.reply_text(f"📡 Now monitoring and sweeping: **{state['chat_title']}**")
            asyncio.create_task(process_history_sweep(chat.id, target_tag=None, delete_after=False))
            del user_states[config.OWNER_ID]
        else:
            state["step"] = "need_tag"
            await message.reply_text(f"✅ Group found: **{state['chat_title']}**\n\nNow, send me the **#Hashtag** you want to {state['action']}:")
            
    elif state["step"] == "need_tag":
        tag = message.text.strip()
        if not tag.startswith("#"):
            await message.reply_text("⚠️ The tag must start with '#'. Try again:")
            return
            
        chat_id = state["chat_id"]
        action = state["action"]
        del user_states[config.OWNER_ID]
        
        await message.reply_text(f"🚀 Initializing {action} for `{tag}`...")
        
        if action == "vault":
            asyncio.create_task(process_history_sweep(chat_id, target_tag=tag, delete_after=True))
        elif action == "wipe":
            asyncio.create_task(process_history_sweep(chat_id, target_tag=tag, delete_after=False, wipe_only=True))

# ==========================================
# 8. MAIN EXECUTION & WATCHDOG
# ==========================================
async def network_watchdog():
    while True:
        try:
            await bot.get_me()
        except (ConnectionError, OSError, TimeoutError, asyncio.TimeoutError):
            db["stats"]["reconnects"] += 1
            save_db()
            sys_status["icon"] = "🔴"
            sys_status["text"] = "Network Down"
        except Exception:
            pass
        await asyncio.sleep(20)

async def main():
    load_db()
    print("[Termux Logger] Initialization started...")

    print("[Termux Logger] Starting Bot Client...")
    await bot.start()
    await bot.send_message(config.OWNER_ID, "🤖 **Bot Client Started Successfully!**")
    print("[Termux Logger] ✅ Bot Client active.")

    print("[Termux Logger] Starting User Client...")
    await user.start()
    await user.send_message("me", "👤 **Userbot Client Started Successfully!**")
    print("[Termux Logger] ✅ User Client active.")

    asyncio.create_task(network_watchdog())
    print("[Termux Logger] Background watchdog running.")

    print("[Termux Logger] Pushing Dashboard to DMs...")
    await set_system_state("🟢", "System Idle", "Online")
    
    print("\n[Termux Logger] 🚀 Autoscan system is fully running! Waiting for commands...\n")
    
    await pyrogram.idle()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Termux Logger] Stopping clients...")
        asyncio.run(user.stop())
        asyncio.run(bot.stop())
        print("[Termux Logger] System gracefully stopped.")