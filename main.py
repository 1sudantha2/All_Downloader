import os
import logging
import asyncio
import time
import re
import uuid
import psutil
import subprocess
import json
from dotenv import load_dotenv
from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
import yt_dlp
from functools import partial

# --- Configuration & Basic Setup ---
load_dotenv()
# (Previous configuration setup remains the same)
# ... [same as before] ...

# --- NEW: Function to create quality selection keyboard ---
async def create_quality_keyboard(info_dict):
    formats = info_dict.get('formats', [])
    video_id = info_dict.get('id')
    buttons = []
    
    # Filter for relevant video formats (with both video and audio)
    video_formats = {}
    for f in formats:
        if f.get('vcodec') != 'none' and f.get('acodec') != 'none' and f.get('ext') == 'mp4':
            quality = f.get('height')
            if quality and quality not in video_formats: # Add only unique resolutions
                video_formats[quality] = f

    # Sort formats by quality
    sorted_qualities = sorted(video_formats.keys(), reverse=True)
    
    for quality in sorted_qualities:
        f = video_formats[quality]
        filesize = f.get('filesize') or f.get('filesize_approx')
        filesize_str = humanbytes(filesize) if filesize else "N/A"
        
        button_text = f"🎬 {quality}p - ({filesize_str})"
        callback_data = f"download:video:{f['format_id']}:{video_id}"
        buttons.append([InlineKeyboardButton(text=button_text, callback_data=callback_data)])

    # Add MP3 (best audio) option
    best_audio = next((f for f in formats if f.get('acodec') != 'none' and f.get('vcodec') == 'none'), None)
    if best_audio:
        filesize = best_audio.get('filesize') or best_audio.get('filesize_approx')
        filesize_str = humanbytes(filesize) if filesize else "N/A"
        
        button_text = f"🎵 MP3 - ({filesize_str})"
        callback_data = f"download:audio:{best_audio['format_id']}:{video_id}"
        buttons.append([InlineKeyboardButton(text=button_text, callback_data=callback_data)])
        
    return InlineKeyboardMarkup(buttons) if buttons else None

# --- Link Handler (Modified for Quality Selection) ---
@app.on_message(filters.regex(URL_REGEX) & filters.private)
async def link_handler(client, message):
    url_match = re.search(URL_REGEX, message.text)
    if not url_match: return
    
    url = url_match.group(0)
    
    # This new feature is complex, let's enable it for YouTube links first
    if "youtube.com" not in url and "youtu.be" not in url:
        # For non-YouTube links, use the old queue system
        task_id = str(uuid.uuid4())[:8]
        task = { 'id': task_id, 'url': url, 'message': message, 'status': 'Pending', 'status_detail': '', 'added_time': time.time() }
        await TASK_QUEUE.put(task)
        ACTIVE_TASKS[task_id] = task
        await message.reply_text(f"✅ YouTube නොවන link එකක්. එය සාමාන්‍ය පෝලිමට ඇතුළත් කරන ලදී.\nTask ID: `{task_id}`", quote=True)
        return

    # --- New workflow for YouTube ---
    status_message = await message.reply_text("🔎 YouTube link එකක් හඳුනාගත්තා. Format විස්තර ලබාගනිමින්...", quote=True)
    
    ydl_opts = {'quiet': True}
    if os.path.exists(COOKIES_FILE_PATH):
        ydl_opts['cookiefile'] = COOKIES_FILE_PATH

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info_dict = await asyncio.to_thread(ydl.extract_info, url, download=False)
        
        keyboard = await create_quality_keyboard(info_dict)
        
        if keyboard:
            video_title = info_dict.get('title', 'Video Title')
            await status_message.edit_text(
                f"**🎥 Video:** `{video_title}`\n\n කරුණාකර බාගත කිරීමට අවශ්‍ය format එක තෝරන්න:",
                reply_markup=keyboard
            )
        else:
            await status_message.edit_text("❌ සමාවන්න, බාගත කළ හැකි formats සොයාගත නොහැකි විය.")

    except Exception as e:
        logger.error(f"Error fetching formats for {url}: {e}")
        await status_message.edit_text(f"❌ Format විස්තර ලබාගැනීමේදී දෝෂයක් ඇතිවිය: `{e}`")


# --- NEW: Callback Query Handler for Buttons ---
@app.on_callback_query(filters.regex(r"^download:"))
async def button_handler(client, callback_query: CallbackQuery):
    # Acknowledge the button press
    await callback_query.answer("ඔබගේ තේරීම භාරගත්තා!")
    
    # --- Parse callback_data ---
    # Format: "download:type:format_id:video_id"
    parts = callback_query.data.split(':')
    media_type = parts[1]
    format_id = parts[2]
    video_id = parts[3]
    
    original_message = callback_query.message
    await original_message.edit_text(f"⏳ ඔබ තේරූ format එක සකසමින් පවතී (`{format_id}`)...")

    url = f"https://www.youtube.com/watch?v={video_id}"
    task_id = str(uuid.uuid4())[:8]

    # Create a task dictionary and add it to the queue
    task = {
        'id': task_id,
        'url': url,
        'message': original_message.reply_to_message, # The original user message
        'status': 'Pending',
        'status_detail': '',
        'added_time': time.time(),
        'is_button_click': True, # Special flag
        'media_type': media_type,
        'format_id': format_id,
        'status_message_for_edit': original_message, # Message to edit progress on
    }
    
    await TASK_QUEUE.put(task)
    ACTIVE_TASKS[task_id] = task

# --- Queue Worker (Modified to handle button clicks) ---
async def queue_worker():
    logger.info("Queue worker started.")
    while True:
        try:
            task = await TASK_QUEUE.get()
            task_id = task['id']
            message = task['message']
            url = task['url']
            
            # Use the message object passed from the button handler for status updates
            status_message = task.get('status_message_for_edit') or await message.reply_text("⏳ ඔබගේ ඉල්ලීම සකසමින් පවතී...", quote=True)
            
            ACTIVE_TASKS[task_id]['status'] = "Downloading"

            # --- Configure ydl_opts based on task type ---
            ydl_opts = {
                'outtmpl': f'downloads/{task_id} - %(title)s.%(ext)s',
                'quiet': True,
                'progress_hooks': [partial(download_progress_hook, status_message=status_message, task_id=task_id)],
            }
            
            if task.get('is_button_click'):
                if task['media_type'] == 'audio':
                    ydl_opts['format'] = task['format_id']
                    ydl_opts['postprocessors'] = [{
                        'key': 'FFmpegExtractAudio',
                        'preferredcodec': 'mp3',
                        'preferredquality': '192',
                    }]
                else: # video
                    ydl_opts['format'] = task['format_id']
                    ydl_opts['merge_output_format'] = 'mp4'
            else: # Generic download
                ydl_opts['format'] = 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best'
                ydl_opts['merge_output_format'] = 'mp4'


            if "youtube.com" in url or "youtu.be" in url:
                if os.path.exists(COOKIES_FILE_PATH):
                    ydl_opts['cookiefile'] = COOKIES_FILE_PATH
            
            # The rest of the download/upload/cleanup logic is mostly the same
            # ... [Paste the try/except/finally block from the PREVIOUS main.py here] ...
            # ... from "downloaded_files = []" down to the end of the "finally" block ...
            
            downloaded_files = []
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info_dict = await asyncio.to_thread(ydl.extract_info, url, download=True)
                    filepath = ydl.prepare_filename(info_dict)
                    downloaded_files.append(filepath)

                    ACTIVE_TASKS[task_id]['status'] = "Uploading"
                    await status_message.edit_text("📤 Upload කරමින් පවතී...")

                    caption = info_dict.get('description') or info_dict.get('title', '')
                    duration = int(info_dict.get('duration', 0))

                    if task.get('media_type') == 'audio':
                         await message.reply_audio(
                            audio=filepath,
                            caption=caption[:1024],
                            duration=duration,
                            progress=partial(progress_callback, "📤 Upload කරමින්...", status_message)
                         )
                    elif duration > 0:
                        await message.reply_video(
                            video=filepath,
                            caption=caption[:1024],
                            duration=duration,
                            progress=partial(progress_callback, "📤 Upload කරමින්...", status_message)
                        )
                    else:
                        await message.reply_photo(
                            photo=filepath,
                            caption=caption[:1024],
                            progress=partial(progress_callback, "📤 Upload කරමින්...", status_message)
                        )
                    
                    await status_message.delete()
                    if task_id in ACTIVE_TASKS:
                        del ACTIVE_TASKS[task_id]

            except Exception as e:
                logger.error(f"Task {task_id} failed. Error: {e}")
                if task_id in ACTIVE_TASKS:
                    ACTIVE_TASKS[task_id]['status'] = "Error"
                await status_message.edit_text(f"❌ **බාගත කිරීමේ දෝෂයකි!**\n\nURL: `{url}`\nError: `{e}`")

            finally:
                for f in downloaded_files:
                    if os.path.exists(f):
                        os.remove(f)

        except Exception as e:
            logger.error(f"Error in queue worker: {e}")

# --- All other code remains the same ---
# ... (start_command, list_command, status_command, ping_command, speedtest_command) ...
# ... (main execution block) ...

# NOTE: The provided code snippet is a significant modification. 
# You need to integrate these new functions (`create_quality_keyboard`, `button_handler`)
# and modifications (`link_handler`, `queue_worker`) into your existing `main.py` script.
# The command handlers (/status, etc.) and the main execution part do not need to change.
# The placeholder `... [Paste the ... block from the PREVIOUS main.py here] ...` should
# be replaced with the corresponding code block.

# --- Command Handlers (Unchanged) ---
@app.on_message(filters.command("start"))
async def start_command(client, message):
    await message.reply_text("👋 Hi! I'm an advanced media downloader bot.\n- Send me a YouTube link for quality options.\n- Use /list to see the queue.\n- Use /status for server stats.")

@app.on_message(filters.command("list"))
# ... (code is the same as before)
async def list_command(client, message):
    if not ACTIVE_TASKS:
        await message.reply_text("🙂 පෝලිම හිස් ය.")
        return

    response = "**📑 වත්මන් බාගත කිරීමේ පෝලිම:**\n\n"
    for task_id, task in ACTIVE_TASKS.items():
        status_icon = "⏳"
        if task['status'] == 'Downloading': status_icon = "📥"
        elif task['status'] == 'Uploading': status_icon = "📤"
        elif task['status'] == 'Error': status_icon = "❌"
        
        url_short = task['url'][:40] + '...' if len(task['url']) > 40 else task['url']
        response += f"{status_icon} **{task['status']}** - `{task['status_detail']}`\n"
        response += f"   - ID: `{task_id}`\n"
        response += f"   - URL: `{url_short}`\n\n"
        
    await message.reply_text(response)

@app.on_message(filters.command("status"))
# ... (code is the same as before)
async def status_command(client, message):
    status_msg = await message.reply_text("📊 Server තත්ත්වය ලබාගනිමින් පවතී...")
    
    cpu_usage = await asyncio.to_thread(psutil.cpu_percent, interval=1)
    ram = await asyncio.to_thread(psutil.virtual_memory)
    disk = await asyncio.to_thread(psutil.disk_usage, '/')
    uptime = get_readable_time(time.time() - BOT_START_TIME)
    
    response = (
        f"**🤖 BOT STATUS**\n"
        f"  - **Uptime:** `{uptime}`\n\n"
        f"**🖥️ SERVER STATUS**\n"
        f"  - **CPU Usage:** `{cpu_usage}%`\n"
        f"  - **RAM Usage:** `{ram.percent}%` ({humanbytes(ram.used)} / {humanbytes(ram.total)})\n"
        f"  - **Disk Usage:** `{disk.percent}%` ({humanbytes(disk.used)} / {humanbytes(disk.total)})"
    )
    
    await status_msg.edit_text(response)

@app.on_message(filters.command("ping"))
# ... (code is the same as before)
async def ping_command(client, message):
    start_time = time.time()
    ping_msg = await message.reply_text("🏓 Pinging...")
    end_time = time.time()
    latency = round((end_time - start_time) * 1000, 2)
    await ping_msg.edit_text(f"**🏓 Pong!**\n`{latency} ms`")

@app.on_message(filters.command("speedtest"))
# ... (code is the same as before)
async def speedtest_command(client, message):
    speed_msg = await message.reply_text("🌐 වේග පරීක්ෂණයක් ආරම්භ කරමින්... මෙය මිනිත්තුවක් පමණ ගතවිය හැක.")
    try:
        result_bytes = await asyncio.to_thread(subprocess.check_output, ['speedtest-cli', '--json'])
        result_json = json.loads(result_bytes)
        
        download_speed = humanbytes(result_json['download']) + '/s'
        upload_speed = humanbytes(result_json['upload']) + '/s'
        ping = f"{result_json['ping']:.2f} ms"
        client_ip = result_json['client']['ip']
        server_name = result_json['server']['name']

        response = (
            f"**🌐 වේග පරීක්ෂණයේ ප්‍රතිඵල:**\n\n"
            f"  - **Server:** `{server_name}`\n"
            f"  - **IP:** `{client_ip}`\n"
            f"  - **Ping:** `{ping}`\n"
            f"  - **Download:** `{download_speed}`\n"
            f"  - **Upload:** `{upload_speed}`"
        )
        await speed_msg.edit_text(response)
    except Exception as e:
        logger.error(f"Speedtest failed: {e}")
        await speed_msg.edit_text(f"❌ වේග පරීක්ෂණය අසාර්ථක විය. `speedtest-cli` ඔබගේ server එකේ ස්ථාපනය කර ඇත්දැයි පරීක්ෂා කරන්න.\n`{e}`")

# --- Main Execution (Unchanged) ---
async def main():
    async with app:
        # Start the queue worker as a background task
        asyncio.create_task(queue_worker())
        logger.info("Bot and Queue Worker have started.")
        await asyncio.Event().wait() # Keep the bot running indefinitely

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopping...")
