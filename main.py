# =================================================================
# === COMPLETE AND CORRECTED main.py FOR ADVANCED DOWNLOAD BOT ===
# =================================================================

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
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
import yt_dlp
from functools import partial

# --- 1. Configuration & Basic Setup ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
COOKIES_FILE_PATH = os.getenv("COOKIES_FILE_PATH")

if not all([API_ID, API_HASH, BOT_TOKEN]):
    logger.critical("FATAL ERROR: API_ID, API_HASH, or BOT_TOKEN is missing in .env file. Exiting.")
    exit(1)

if not os.path.isdir('downloads'):
    os.makedirs('downloads')

# --- 2. Pyrogram Client Initialization (DEFINED EARLY) ---
app = Client("advanced_downloader_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# --- 3. Global Variables & Helper Functions ---
TASK_QUEUE = asyncio.Queue()
ACTIVE_TASKS = {}
URL_REGEX = r'(https?://\S+)'
BOT_START_TIME = time.time()

def humanbytes(size):
    if not size: return ""
    power = 1024
    t_n = 0
    power_dict = {0: " B", 1: " KB", 2: " MB", 3: " GB", 4: " TB"}
    while size >= power:
        size /= power
        t_n += 1
    return "{:.2f}".format(size) + power_dict[t_n]

def get_readable_time(seconds):
    result = ''
    periods = [('d', 86400), ('h', 3600), ('m', 60), ('s', 1)]
    for period_name, period_seconds in periods:
        if seconds >= period_seconds:
            period_value, seconds = divmod(seconds, period_seconds)
            result += f'{int(period_value)}{period_name}'
    return result if result else '0s'

last_edit_time = {}
async def progress_callback(description, message_to_edit, current, total):
    message_id = message_to_edit.id
    now = time.time()
    if (now - last_edit_time.get(message_id, 0)) < 2:
        return
    last_edit_time[message_id] = now
    
    percentage = current * 100 / total
    progress_bar = "[{0}{1}]".format('█' * int(percentage / 10), '░' * (10 - int(percentage / 10)))
    progress_text = f"**{description}**\n{progress_bar} {percentage:.2f}%\n`{humanbytes(current)} / {humanbytes(total)}`"
    try:
        await message_to_edit.edit_text(progress_text)
    except Exception:
        pass

def download_progress_hook(d, status_message, task_id):
    if d['status'] == 'downloading':
        message_id = status_message.id
        total_bytes = d.get('total_bytes') or d.get('total_bytes_estimate', 0)
        downloaded_bytes = d.get('downloaded_bytes', 0)
        speed = d.get('speed', 0)

        if total_bytes > 0:
            percentage = downloaded_bytes * 100 / total_bytes
            progress_bar = "[{0}{1}]".format('█' * int(percentage / 10), '░' * (10 - int(percentage / 10)))
            if task_id in ACTIVE_TASKS:
                ACTIVE_TASKS[task_id]['status_detail'] = f"{percentage:.1f}% ({humanbytes(speed)}/s)"
            
            global last_edit_time
            now = time.time()
            if (now - last_edit_time.get(message_id, 0)) < 2:
                return
            last_edit_time[message_id] = now
            
            progress_text = f"**📥 බාගත කරමින්...**\n`{d.get('filename', '')}`\n{progress_bar} {percentage:.2f}%\n`{humanbytes(downloaded_bytes)} / {humanbytes(total_bytes)}`\n**Speed:** `{humanbytes(speed)}/s`"
            asyncio.run_coroutine_threadsafe(status_message.edit_text(progress_text), app.loop)

async def create_quality_keyboard(info_dict):
    formats = info_dict.get('formats', [])
    video_id = info_dict.get('id')
    buttons = []
    video_formats = {}
    
    for f in formats:
        if f.get('vcodec') != 'none' and f.get('acodec') != 'none' and f.get('ext') == 'mp4':
            quality = f.get('height')
            if quality and quality <= 1080 and quality not in video_formats:
                video_formats[quality] = f

    sorted_qualities = sorted(video_formats.keys(), reverse=True)
    
    for quality in sorted_qualities:
        f = video_formats[quality]
        filesize = f.get('filesize') or f.get('filesize_approx')
        filesize_str = humanbytes(filesize) if filesize else "N/A"
        button_text = f"🎬 {quality}p - ({filesize_str})"
        callback_data = f"download:video:{f['format_id']}:{video_id}"
        buttons.append([InlineKeyboardButton(text=button_text, callback_data=callback_data)])

    best_audio = next((f for f in sorted(formats, key=lambda x: x.get('filesize', 0) or x.get('filesize_approx', 0), reverse=True) if f.get('acodec') != 'none' and f.get('vcodec') == 'none'), None)
    if best_audio:
        filesize = best_audio.get('filesize') or best_audio.get('filesize_approx')
        filesize_str = humanbytes(filesize) if filesize else "N/A"
        button_text = f"🎵 MP3 (Best Audio) - ({filesize_str})"
        callback_data = f"download:audio:{best_audio['format_id']}:{video_id}"
        buttons.append([InlineKeyboardButton(text=button_text, callback_data=callback_data)])
        
    return InlineKeyboardMarkup(buttons) if buttons else None

# --- 4. Core Worker ---
async def queue_worker():
    logger.info("Queue worker started.")
    while True:
        try:
            task = await TASK_QUEUE.get()
            task_id = task['id']
            message = task['message']
            url = task['url']
            
            status_message = task.get('status_message_for_edit') or await message.reply_text("⏳ ඔබගේ ඉල්ලීම සකසමින් පවතී...", quote=True)
            ACTIVE_TASKS[task_id]['status'] = "Downloading"

            ydl_opts = {'outtmpl': f'downloads/{task_id} - %(title)s.%(ext)s', 'quiet': True, 'progress_hooks': [partial(download_progress_hook, status_message=status_message, task_id=task_id)]}
            
            if task.get('is_button_click'):
                if task['media_type'] == 'audio':
                    ydl_opts.update({'format': task['format_id'], 'postprocessors': [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 'preferredquality': '192'}]})
                else:
                    ydl_opts.update({'format': task['format_id'], 'merge_output_format': 'mp4'})
            else:
                ydl_opts.update({'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best', 'merge_output_format': 'mp4'})

            if "youtube.com" in url or "youtu.be" in url and os.path.exists(COOKIES_FILE_PATH):
                ydl_opts['cookiefile'] = COOKIES_FILE_PATH
            
            downloaded_files = []
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info_dict = await asyncio.to_thread(ydl.extract_info, url, download=True)
                    filepath = ydl.prepare_filename(info_dict)
                    downloaded_files.append(filepath)

                    ACTIVE_TASKS[task_id]['status'] = "Uploading"
                    
                    caption = info_dict.get('description') or info_dict.get('title', '')
                    duration = int(info_dict.get('duration', 0))

                    if task.get('media_type') == 'audio':
                        await message.reply_audio(audio=filepath, caption=caption[:1024], duration=duration, progress=partial(progress_callback, "📤 Upload කරමින්...", status_message))
                    elif duration > 0:
                        await message.reply_video(video=filepath, caption=caption[:1024], duration=duration, progress=partial(progress_callback, "📤 Upload කරමින්...", status_message))
                    else:
                        await message.reply_photo(photo=filepath, caption=caption[:1024], progress=partial(progress_callback, "📤 Upload කරමින්...", status_message))
                    
                    await status_message.delete()
                    if task_id in ACTIVE_TASKS: del ACTIVE_TASKS[task_id]
            except Exception as e:
                logger.error(f"Task {task_id} failed. Error: {e}")
                if task_id in ACTIVE_TASKS: ACTIVE_TASKS[task_id]['status'] = "Error"
                await status_message.edit_text(f"❌ **බාගත කිරීමේ දෝෂයකි!**\n\nURL: `{url}`\nError: `{e}`")
            finally:
                for f in downloaded_files:
                    if os.path.exists(f): os.remove(f)
        except Exception as e:
            logger.error(f"Major error in queue worker: {e}")

# --- 5. Pyrogram Event Handlers ---
@app.on_message(filters.command("start"))
async def start_command(client, message):
    await message.reply_text("👋 Hi! I'm an advanced media downloader bot.\n- Send me a link to download.\n- Use /list to see the queue.\n- Use /status for server stats.")

@app.on_message(filters.regex(URL_REGEX) & filters.private)
async def link_handler(client, message):
    url_match = re.search(URL_REGEX, message.text)
    if not url_match: return
    url = url_match.group(0)

    if "youtube.com" in url or "youtu.be" in url:
        status_message = await message.reply_text("🔎 YouTube link එකක් හඳුනාගත්තා. Format විස්තර ලබාගනිමින්...", quote=True)
        ydl_opts = {'quiet': True}
        if os.path.exists(COOKIES_FILE_PATH): ydl_opts['cookiefile'] = COOKIES_FILE_PATH
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info_dict = await asyncio.to_thread(ydl.extract_info, url, download=False)
            keyboard = await create_quality_keyboard(info_dict)
            if keyboard:
                await status_message.edit_text(f"**🎥 Video:** `{info_dict.get('title', 'N/A')}`\n\nකරුණාකර බාගත කිරීමට අවශ්‍ය format එක තෝරන්න:", reply_markup=keyboard)
            else:
                await status_message.edit_text("❌ සමාවන්න, බාගත කළ හැකි formats සොයාගත නොහැකි විය.")
        except Exception as e:
            await status_message.edit_text(f"❌ Format විස්තර ලබාගැනීමේදී දෝෂයක් ඇතිවිය: `{e}`")
    else:
        task_id = str(uuid.uuid4())[:8]
        task = {'id': task_id, 'url': url, 'message': message, 'status': 'Pending', 'status_detail': '', 'added_time': time.time()}
        await TASK_QUEUE.put(task)
        ACTIVE_TASKS[task_id] = task
        await message.reply_text(f"✅ ඉල්ලීම පෝලිමට ඇතුළත් කරන ලදී.\nTask ID: `{task_id}`", quote=True)

@app.on_callback_query(filters.regex(r"^download:"))
async def button_handler(client, callback_query: CallbackQuery):
    await callback_query.answer("ඔබගේ තේරීම භාරගත්තා!")
    parts = callback_query.data.split(':')
    media_type, format_id, video_id = parts[1], parts[2], parts[3]
    
    await callback_query.message.edit_text(f"⏳ ඔබ තේරූ format එක (`{format_id}`) පෝලිමට ඇතුළත් කරමින්...")
    
    url = f"https://www.youtube.com/watch?v={video_id}"
    task_id = str(uuid.uuid4())[:8]
    task = {
        'id': task_id, 'url': url, 'message': callback_query.message.reply_to_message,
        'status': 'Pending', 'status_detail': '', 'added_time': time.time(),
        'is_button_click': True, 'media_type': media_type, 'format_id': format_id,
        'status_message_for_edit': callback_query.message
    }
    await TASK_QUEUE.put(task)
    ACTIVE_TASKS[task_id] = task

@app.on_message(filters.command("list"))
async def list_command(client, message):
    if not ACTIVE_TASKS:
        return await message.reply_text("🙂 පෝලිම හිස් ය.")
    response = "**📑 වත්මන් බාගත කිරීමේ පෝලිම:**\n\n"
    for task_id, task in list(ACTIVE_TASKS.items()):
        status_icon = {"Pending": "⏳", "Downloading": "📥", "Uploading": "📤", "Error": "❌"}.get(task['status'], "❓")
        url_short = task['url'][:40] + '...' if len(task['url']) > 40 else task['url']
        response += f"{status_icon} **{task['status']}** - `{task['status_detail']}`\n   - ID: `{task_id}` | URL: `{url_short}`\n\n"
    await message.reply_text(response)

@app.on_message(filters.command("status"))
async def status_command(client, message):
    status_msg = await message.reply_text("📊 Server තත්ත්වය ලබාගනිමින් පවතී...")
    cpu = await asyncio.to_thread(psutil.cpu_percent, interval=1)
    ram = await asyncio.to_thread(psutil.virtual_memory)
    disk = await asyncio.to_thread(psutil.disk_usage, '/')
    uptime = get_readable_time(time.time() - BOT_START_TIME)
    response = (
        f"**🤖 BOT STATUS**\n  - **Uptime:** `{uptime}`\n\n"
        f"**🖥️ SERVER STATUS**\n"
        f"  - **CPU:** `{cpu}%`\n"
        f"  - **RAM:** `{ram.percent}%` ({humanbytes(ram.used)}/{humanbytes(ram.total)})\n"
        f"  - **Disk:** `{disk.percent}%` ({humanbytes(disk.used)}/{humanbytes(disk.total)})"
    )
    await status_msg.edit_text(response)
    
@app.on_message(filters.command("ping"))
async def ping_command(client, message):
    start_time = time.time()
    ping_msg = await message.reply_text("🏓 Pinging...")
    latency = round((time.time() - start_time) * 1000, 2)
    await ping_msg.edit_text(f"**🏓 Pong!**\n`{latency} ms`")

@app.on_message(filters.command("speedtest"))
async def speedtest_command(client, message):
    speed_msg = await message.reply_text("🌐 වේග පරීක්ෂණයක් ආරම්භ කරමින්... මෙය මිනිත්තුවක් පමණ ගතවිය හැක.")
    try:
        result_bytes = await asyncio.to_thread(subprocess.check_output, ['speedtest-cli', '--json'])
        result = json.loads(result_bytes)
        response = (
            f"**🌐 වේග පරීක්ෂණයේ ප්‍රතිඵල:**\n\n"
            f"  - **Server:** `{result['server']['name']}`\n"
            f"  - **Ping:** `{result['ping']:.2f} ms`\n"
            f"  - **Download:** `{humanbytes(result['download'])}/s`\n"
            f"  - **Upload:** `{humanbytes(result['upload'])}/s`"
        )
        await speed_msg.edit_text(response)
    except Exception as e:
        await speed_msg.edit_text(f"❌ වේග පරීක්ෂණය අසාර්ථක විය. `speedtest-cli` ස්ථාපනය කර ඇත්දැයි බලන්න.\n`{e}`")

# --- 6. Main Execution Block ---
async def main():
    await app.start()
    logger.info("Bot started.")
    asyncio.create_task(queue_worker())
    logger.info("Queue worker started.")
    await asyncio.Event().wait()
    await app.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopping...")us_command, ping_command, speedtest_command) ...
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
