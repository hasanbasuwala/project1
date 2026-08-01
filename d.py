import asyncio
import json
import os
import re
from pyrogram import Client, filters, compose
from pyrogram.errors import FloodWait

import config

DB_FILE = "autoscan_db.json"

# --- DATABASE MANAGEMENT ---
def load_db():
    if not os.path.exists(DB_FILE):
        return {"monitored_groups": [], "vaults": {}}
    with open(DB_FILE, "r") as f:
        return json.load(f)

def save_db(db_data):
    with open(DB_FILE, "w") as f:
        json.dump(db_data, f, indent=4)

db = load_db()

# --- INITIALIZE CLIENTS ---
user = Client("my_userbot", api_id=config.API_ID, api_hash=config.API_HASH)
# Using SORT_TOKEN as requested
bot = Client("my_control_bot", api_id=config.API_ID, api_hash=config.API_HASH, bot_token=config.SORT_TOKEN)

# State tracker for the bot's conversation wizard
user_states = {}

# --- CORE FUNCTIONS (Run by Userbot, Feedback via Bot) ---
async def get_or_create_vault(tag: str, original_chat_title: str):
    """Checks if a vault exists; if not, creates it using the Userbot."""
    if tag in db["vaults"]:
        return db["vaults"][tag]
    
    vault_title = f"{original_chat_title[:30]} Vault - {tag}"
    try:
        new_group = await user.create_supergroup(vault_title, f"Auto-archived messages for {tag}")
        db["vaults"][tag] = new_group.id
        save_db(db)
        await asyncio.sleep(2) # FloodWait prevention
        return new_group.id
    except FloodWait as e:
        await asyncio.sleep(e.value + 1)
        return await get_or_create_vault(tag, original_chat_title)
    except Exception as e:
        print(f"Failed to create vault for {tag}: {e}")
        return None

async def process_history_sweep(chat_id: int, chat_title: str, target_tag: str = None, wipe_only: bool = False):
    """Sweeps history. If target_tag is provided, only processes that tag. If wipe_only, just deletes."""
    await bot.send_message(config.OWNER_ID, f"🔄 **Starting sweep in {chat_title}**...\nGathering messages. This may take a moment.")
    
    query = target_tag if target_tag else "#"
    total_count = await user.search_messages_count(chat_id, query=query)
    
    if total_count == 0:
        await bot.send_message(config.OWNER_ID, f"✅ No messages found for '{query}' in {chat_title}.")
        return

    status_msg = await bot.send_message(config.OWNER_ID, f"📊 Found {total_count} messages. Processing...")

    processed_count = 0
    deleted_count = 0
    
    messages_to_process = []
    async for msg in user.search_messages(chat_id, query=query):
        messages_to_process.append(msg)
            
    messages_to_process.reverse() # Oldest to newest

    for msg in messages_to_process:
        text = msg.text or msg.caption or ""
        tags = list(set(re.findall(r'(#\w+)', text.lower())))
        
        # If a specific tag is requested, filter the rest out
        if target_tag:
            if target_tag.lower() not in tags:
                continue
            tags = [target_tag.lower()]
            
        if not tags:
            continue
            
        success = False
        
        if wipe_only:
            success = True # Skip vaulting, just mark for deletion
        else:
            for tag in tags:
                vault_id = await get_or_create_vault(tag, chat_title)
                if vault_id:
                    try:
                        await user.copy_message(chat_id=vault_id, from_chat_id=chat_id, message_id=msg.id)
                        success = True
                        await asyncio.sleep(0.5)
                    except FloodWait as e:
                        await asyncio.sleep(e.value + 1)
                        await user.copy_message(chat_id=vault_id, from_chat_id=chat_id, message_id=msg.id)
                        success = True
                    except Exception:
                        pass

        if success:
            try:
                await user.delete_messages(chat_id, msg.id)
                deleted_count += 1
            except Exception:
                pass

        processed_count += 1
        if processed_count % 10 == 0:
            try:
                await status_msg.edit_text(f"🔄 **Sweeping {chat_title}...** {processed_count} / {total_count} processed.")
            except Exception:
                pass

    mode_text = "wiped" if wipe_only else "vaulted and wiped"
    await status_msg.edit_text(f"✅ **Sweep Complete for {chat_title}!**\nSuccessfully {mode_text} {deleted_count} messages.")


# --- BOT COMMAND INTERFACE (Control Panel) ---
@bot.on_message(filters.command("start") & filters.user(config.OWNER_ID))
async def bot_start(client, message):
    text = (
        "🤖 **Welcome to the AutoScan Control Panel!**\n\n"
        "Available Commands:\n"
        "🔹 `/dashboard` - View all active monitored groups\n"
        "🔹 `/autoscan` - Scan history & activate live monitoring\n"
        "🔹 `/stopscan` - Stop live monitoring for a group\n"
        "🔹 `/vault` - Move a specific #tag to a vault (History only)\n"
        "🔹 `/wipe` - Delete a specific #tag (History only)\n"
        "🔹 `/stopbot` - Shut down the entire script"
    )
    await message.reply_text(text)

@bot.on_message(filters.command("dashboard") & filters.user(config.OWNER_ID))
async def bot_dashboard(client, message):
    if not db["monitored_groups"]:
        await message.reply_text("📭 **Dashboard:**\nNo groups are currently being monitored.")
        return
        
    text = "📊 **Active Monitored Groups:**\n\n"
    for chat_id in db["monitored_groups"]:
        try:
            chat = await user.get_chat(chat_id)
            title = chat.title or "Unknown Group"
            text += f"✅ {title} (`{chat_id}`)\n"
        except Exception:
            text += f"⚠️ Unknown/Inaccessible Group (`{chat_id}`)\n"
            
    await message.reply_text(text)

@bot.on_message(filters.command(["autoscan", "stopscan", "vault", "wipe"]) & filters.user(config.OWNER_ID))
async def initiate_command(client, message):
    cmd = message.command[0].lower()
    user_states[config.OWNER_ID] = {"action": cmd, "step": "need_group"}
    await message.reply_text(f"🛠️ **Command:** `/{cmd}`\n\nPlease send me the **Group ID** (e.g. `-10012345678`) or **Username** (e.g. `@mygroup`) you want to apply this to.")

@bot.on_message(filters.command("stopbot") & filters.user(config.OWNER_ID))
async def bot_stopbot(client, message):
    await message.reply_text("🛑 **System Offline.** Shutting down all scripts. Restart terminal to wake me up.")
    os._exit(0)

@bot.on_message(filters.text & filters.user(config.OWNER_ID) & ~filters.command(["start", "dashboard", "autoscan", "stopscan", "vault", "wipe", "stopbot"]))
async def process_wizard_inputs(client, message):
    state = user_states.get(config.OWNER_ID)
    if not state:
        return # Ignore random text
        
    action = state["action"]
    step = state["step"]
    
    if step == "need_group":
        raw_id = message.text.strip()
        try:
            # Convert string ID to int if it's purely numerical or starts with a minus
            if raw_id.replace("-", "").isdigit():
                chat_id = int(raw_id)
            else:
                chat_id = raw_id
                
            chat = await user.get_chat(chat_id)
            state["chat_id"] = chat.id
            state["chat_title"] = chat.title
            
            if action in ["vault", "wipe"]:
                state["step"] = "need_tag"
                await message.reply_text(f"🎯 Target Group: **{chat.title}**\n\nNow, send me the exact **#hashtag** you want to {action}.")
            elif action == "autoscan":
                if chat.id not in db["monitored_groups"]:
                    db["monitored_groups"].append(chat.id)
                    save_db(db)
                await message.reply_text(f"🔄 **AutoScan Activated for {chat.title}!**\nI'll start sweeping history and monitoring live.")
                user_states.pop(config.OWNER_ID, None) # Clear state
                # Run sweep in background
                asyncio.create_task(process_history_sweep(chat.id, chat.title))
            elif action == "stopscan":
                if chat.id in db["monitored_groups"]:
                    db["monitored_groups"].remove(chat.id)
                    save_db(db)
                    await message.reply_text(f"🛑 **AutoScan Deactivated.** No longer monitoring **{chat.title}**.")
                else:
                    await message.reply_text(f"⚠️ **{chat.title}** is not currently in the active list.")
                user_states.pop(config.OWNER_ID, None)
                
        except Exception as e:
            await message.reply_text(f"❌ Could not access that group. Are you sure I (the Userbot) am a member? Error: {e}\nTry sending the ID again.")

    elif step == "need_tag":
        tag = message.text.strip().lower()
        if not tag.startswith("#"):
            await message.reply_text("⚠️ A hashtag must start with '#'. Try again.")
            return
            
        chat_id = state["chat_id"]
        chat_title = state["chat_title"]
        user_states.pop(config.OWNER_ID, None) # Clear state
        
        if action == "vault":
            await message.reply_text(f"📦 Starting Vault process for {tag} in **{chat_title}**...")
            asyncio.create_task(process_history_sweep(chat_id, chat_title, target_tag=tag, wipe_only=False))
        elif action == "wipe":
            await message.reply_text(f"🗑️ Starting Wipe process for {tag} in **{chat_title}**...")
            asyncio.create_task(process_history_sweep(chat_id, chat_title, target_tag=tag, wipe_only=True))


# --- LIVE LISTENER (Runs purely on Userbot side) ---
@user.on_message(filters.group & ~filters.me, group=1)
async def live_hashtag_listener(client, message):
    """Silently watches incoming messages in monitored groups and routes them."""
    chat_id = message.chat.id
    
    if chat_id not in db["monitored_groups"]:
        return
        
    text = message.text or message.caption or ""
    tags = list(set(re.findall(r'(#\w+)', text.lower())))
    
    if not tags:
        return

    chat_title = message.chat.title or "Archive"
    success = False
    
    for tag in tags:
        vault_id = await get_or_create_vault(tag, chat_title)
        if vault_id:
            try:
                await user.copy_message(chat_id=vault_id, from_chat_id=chat_id, message_id=message.id)
                success = True
                await asyncio.sleep(0.5) 
            except FloodWait as e:
                await asyncio.sleep(e.value + 1)
                await user.copy_message(chat_id=vault_id, from_chat_id=chat_id, message_id=message.id)
                success = True
            except Exception as e:
                print(f"Live copy failed for {tag}: {e}")

    if success:
        try:
            await user.delete_messages(chat_id, message.id)
        except Exception:
            pass

# --- RUN BOTH CLIENTS ---
if __name__ == '__main__':
    print("Starting Userbot and Control Bot simultaneously...")
    compose([user, bot])