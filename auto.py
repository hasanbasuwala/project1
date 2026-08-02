import asyncio
import json
import os
import re
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
            
            # Ensure stats exist for older DB versions
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
user = Client("userbot_session", api_id=config.API_ID, api_hash=config.API_HASH)
bot = Client("bot_session", api_id=config.API_ID, api_hash=config.API_HASH, bot_token=config.BOT_TOKEN)

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
        if db["dashboard_msg_id"]:
            await bot.edit_message_text(config.OWNER_ID, db["dashboard_msg_id"], dashboard_text)
        else:
            msg = await bot.send_message(config.OWNER_ID, dashboard_text)
            await bot.pin_chat_message(config.OWNER_ID, msg.id, disable_notification=True)
            db["dashboard_msg_id"] = msg.id
            save_db()
    except Exception:
        # If message was deleted or not found, send a new one and re-pin
        msg = await bot.send_message(config.OWNER_ID, dashboard_text)
        await bot.pin_chat_message(config.OWNER_ID, msg.id, disable_notification=True)
        db["dashboard_msg_id"] = msg.id
        save_db()

async def get_or_create_vault(original_tag: str) -> int:
    """Gets existing vault ID, or creates one strictly named after the tag."""
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
            await asyncio.sleep(15)  # Anti-spam cooldown
            return new_group.id
            
        except FloodWait as e:
            db["stats"]["waits_avoided"] += 1
            save_db()
            await set_system_state("🔴", "Rate Limited", f"Waiting {e.value}s...")
            await asyncio.sleep(e.value + 5)
        except UserRestricted:
            await bot.send_message(config.OWNER_ID, "❌ Account restricted from creating groups right now.")
            return None
        except Exception as e:
            await asyncio.sleep(10)

    await bot.send_message(config.OWNER_ID, f"❌ Failed to create vault for {original_tag} after 3 attempts.")
    return None

async def safe_copy(vault_id: int, chat_id: int, msg_id: int) -> bool:
    """Safely copies a message with automatic FloodWait handling."""
    attempts = 0
    while attempts < 3:
        attempts += 1
        try:
            await user.copy_message(chat_id=vault_id, from_chat_id=chat_id, message_id=msg_id)
            db["stats"]["messages_vaulted"] += 1
            save_db()
            await asyncio.sleep(0.5)  # Throttle copying
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
    """Extracts all hashtags from a given text."""
    if not text:
        return []
    return list(set(re.findall(r'(#\w+)', text.lower())))

# ==========================================
# 4. CORE ORCHESTRATION (/autoscan logic)
# ==========================================
async def process_history_sweep(chat_id: int, target_tag=None, delete_after=False):
    """Phase 1: Build list. Phase 2: Process chronological transfers."""
    await set_system_state("⏳", "Sweeping History", f"Scanning Chat ID: {chat_id}")
    
    messages_to_process = []
    
    # PHASE 1: Build the complete list
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

    # PHASE 2: Chronological Processing
    messages_to_process.reverse()  # Oldest to newest
    total = len(messages_to_process)
    processed = 0
    deleted_count = 0
    
    await bot.send_message(config.OWNER_ID, f"📊 Sweep Phase 1 Complete. Found {total} messages. Beginning transfer...")

    for msg in messages_to_process:
        text = msg.text or msg.caption or ""
        tags = [target_tag] if target_tag else extract_tags(text)
        
        success_for_msg = False
        
        for tag in tags:
            original_tag = target_tag if target_tag else tag
            
            vault_id = await get_or_create_vault(original_tag)
            if vault_id:
                copy_success = await safe_copy(vault_id, chat_id, msg.id)
                if copy_success:
                    success_for_msg = True

        if success_for_msg and delete_after:
            try:
                await user.delete_messages(chat_id, [msg.id])
                deleted_count += 1
            except Exception:
                pass

        processed += 1
        if processed % 5 == 0:
            await set_system_state("⏳", "Transferring...", f"{processed} / {total} processed")

    report = f"✅ **Sweep Complete!**\nProcessed: {processed} messages."
    if delete_after:
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
    """Deletes matching items without copying."""
    command_parts = message.text.split(" ", 1)
    if len(command_parts) < 2:
        return
    tag = command_parts[1]
    chat_id = message.chat.id
    norm_tag = normalize_tag(tag)
    await message.delete()
    
    status = await message.reply_text(f"🗑️ Wiping '{tag}'...")
    deleted_count = 0
    
    async for msg in user.search_messages(chat_id, query=tag):
        text = msg.text or msg.caption or ""
        if norm_tag in text.lower():
            try:
                await user.delete_messages(chat_id, [msg.id])
                deleted_count += 1
                await asyncio.sleep(0.5)
            except FloodWait as e:
                await asyncio.sleep(e.value + 5)
                
    await status.edit_text(f"✅ Wiped {deleted_count} messages matching {tag}.")
    await asyncio.sleep(10)
    await status.delete()

# ==========================================
# 6. MAIN EXECUTION & WATCHDOG
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

    # 1. Start Bot Client and Ping
    print("[Termux Logger] Starting Bot Client...")
    await bot.start()
    await bot.send_message(config.OWNER_ID, "🤖 **Bot Client Started Successfully!**")
    print("[Termux Logger] ✅ Bot Client active.")

    # 2. Start User Client and Ping
    print("[Termux Logger] Starting User Client...")
    await user.start()
    await user.send_message("me", "👤 **Userbot Client Started Successfully!**")
    print("[Termux Logger] ✅ User Client active.")

    # 3. Kick off background tasks
    asyncio.create_task(network_watchdog())
    print("[Termux Logger] Background watchdog running.")

    # 4. Generate and Pin Dashboard
    print("[Termux Logger] Pushing Dashboard to DMs...")
    await set_system_state("🟢", "System Idle", "Online")
    
    print("\n[Termux Logger] 🚀 Autoscan system is fully running! Waiting for commands...\n")
    
    # Keep the script alive
    await pyrogram.idle()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Termux Logger] Stopping clients...")
        asyncio.run(user.stop())
        asyncio.run(bot.stop())
        print("[Termux Logger] System gracefully stopped.")