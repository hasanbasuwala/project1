import asyncio
from datetime import datetime
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
import config

# Initialize the Pyrogram client using credentials from config.py
app = Client("my_userbot", api_id=config.API_ID, api_hash=config.API_HASH)

@app.on_message(filters.command("wipe", prefixes=["/", "."]) & filters.me)
async def wipe_messages_advanced(client: Client, message):
    """Searches for a caption/word and deletes all matching messages with live feedback."""
    command_parts = message.text.split(" ", 1)
    if len(command_parts) < 2:
        await message.reply_text("⚠️ Usage: `/wipe #caption`")
        return
    
    query = command_parts[1]
    chat_id = message.chat.id
    status = await message.reply_text(f"🔍 Checking Telegram servers for '{query}'...")

    # 1. INSTANT FEEDBACK: Ask Telegram for the exact count first
    total_count = await client.search_messages_count(chat_id, query=query)
    
    if total_count == 0:
        await status.edit_text(f"❌ No messages found containing '{query}'.")
        return

    await status.edit_text(f"📊 **Found {total_count} messages.**\n⚙️ Gathering message data...")

    # 2. GATHER DATA & USEFUL INFO
    message_ids = []
    oldest_date = None
    newest_date = None

    # Fetch the actual messages
    async for msg in client.search_messages(chat_id, query=query):
        message_ids.append(msg.id)
        
        # Track the time range of the messages
        if msg.date:
            if not newest_date:
                newest_date = msg.date # First message is the newest
            oldest_date = msg.date     # Last message in the loop is the oldest

    # 3. LIVE PROGRESS DELETION
    # Pyrogram can delete chunks of 100 messages at a time
    chunk_size = 100
    deleted_count = 0

    for i in range(0, len(message_ids), chunk_size):
        chunk = message_ids[i:i + chunk_size]
        await client.delete_messages(chat_id, chunk)
        deleted_count += len(chunk)
        
        # Update the status message every chunk
        await status.edit_text(f"🗑️ **Deleting...** {deleted_count} / {total_count} messages processed.")
        await asyncio.sleep(1) # Prevent rate limits on status updates

    # 4. FINAL REPORT WITH USEFUL INFO
    report = (
        f"✅ **Cleanup Complete!**\n\n"
        f"**Search Query:** `{query}`\n"
        f"**Total Wiped:** {deleted_count} messages\n"
    )
    
    if oldest_date and newest_date:
        oldest_str = oldest_date.strftime("%b %d, %Y")
        newest_str = newest_date.strftime("%b %d, %Y")
        report += f"**Time Range:** {oldest_str} ➡️ {newest_str}\n"

    await status.edit_text(report)


@app.on_message(filters.command("vault", prefixes=["/", "."]) & filters.me)
async def vault_messages_advanced(client: Client, message):
    """Creates a private group, transfers matching messages, then deletes them with live feedback."""
    command_parts = message.text.split(" ", 1)
    if len(command_parts) < 2:
        await message.reply_text("⚠️ Usage: `/vault #caption`")
        return
    
    query = command_parts[1]
    original_chat = message.chat
    status = await message.reply_text(f"🔍 Checking Telegram servers for '{query}'...")

    total_count = await client.search_messages_count(original_chat.id, query=query)
    
    if total_count == 0:
        await status.edit_text(f"❌ No messages found containing '{query}'.")
        return

    await status.edit_text(f"📊 **Found {total_count} messages.**\n⚙️ Gathering message data...")

    message_ids = []
    oldest_date = None
    newest_date = None

    async for msg in client.search_messages(original_chat.id, query=query):
        # Exclude the command message itself so we don't vault the command
        if msg.id != message.id: 
            message_ids.append(msg.id)
            
            if msg.date:
                if not newest_date:
                    newest_date = msg.date
                oldest_date = msg.date

    if not message_ids:
        await status.edit_text(f"❌ No transferrable messages found containing '{query}'.")
        return

    # Reverse the list so we transfer them to the vault in chronological order
    message_ids.reverse()

    # Create the new Private Supergroup
    vault_title = f"{original_chat.title or 'Archive'} - {query}"
    await status.edit_text(f"🏗️ Creating new private group: '{vault_title}'...")
    
    try:
        new_group = await client.create_supergroup(vault_title, f"Archived messages containing: {query}")
        vault_id = new_group.id
    except Exception as e:
        await status.edit_text(f"❌ Failed to create group: {e}")
        return

    await status.edit_text(f"✅ Vault created!\n📤 Transferring {len(message_ids)} messages...")

    # Copy messages one by one to avoid FloodWait limits on large transfers
    successful_copies = 0
    for i, msg_id in enumerate(message_ids, 1):
        try:
            await client.copy_message(chat_id=vault_id, from_chat_id=original_chat.id, message_id=msg_id)
            successful_copies += 1
            await asyncio.sleep(0.5) # Respect Telegram's rate limits
        except FloodWait as e:
            await asyncio.sleep(e.value)
            await client.copy_message(chat_id=vault_id, from_chat_id=original_chat.id, message_id=msg_id)
            successful_copies += 1
        except Exception as e:
            print(f"Skipped message {msg_id}: {e}")

        # Update status every 10 messages so we don't flood the API with edits
        if successful_copies % 10 == 0:
            await status.edit_text(f"📤 **Transferring...** {successful_copies} / {len(message_ids)} copied to vault.")

    await status.edit_text(f"🗑️ Transfer complete. Deleting original messages...")
    
    # Delete the original messages from the source group in chunks
    chunk_size = 100
    deleted_count = 0
    for i in range(0, len(message_ids), chunk_size):
        chunk = message_ids[i:i + chunk_size]
        await client.delete_messages(original_chat.id, chunk)
        deleted_count += len(chunk)

    # Final Report
    report = (
        f"✅ **Vault & Cleanup Complete!**\n\n"
        f"**Search Query:** `{query}`\n"
        f"**Vault Created:** `{vault_title}`\n"
        f"**Successfully Transferred & Wiped:** {successful_copies} messages\n"
    )
    
    if oldest_date and newest_date:
        oldest_str = oldest_date.strftime("%b %d, %Y")
        newest_str = newest_date.strftime("%b %d, %Y")
        report += f"**Time Range:** {oldest_str} ➡️ {newest_str}\n"

    await status.edit_text(report)

if __name__ == '__main__':
    print("Userbot is running...")
    app.run()