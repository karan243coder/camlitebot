import os
import time
import asyncio
import math
import shutil
import glob
import json
import re
import gc
from typing import Dict, Union, Set

import httpx
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, Form, UploadFile, File
from contextlib import asynccontextmanager
from starlette.requests import ClientDisconnect

try:
    from pyrogram import Client
    pyrogram_available = True
except ImportError:
    Client = None
    pyrogram_available = False

BOT_TOKEN = os.environ["BOT_TOKEN"]
OWNER_CHAT_ID = os.environ["OWNER_CHAT_ID"]
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "changeme")
DEVICE_TOKEN = os.environ.get("DEVICE_TOKEN", "changeme")
API_ID = os.environ.get("API_ID")
API_HASH = os.environ.get("API_HASH")

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

tg_client: Client = None
processing_queue = asyncio.Queue()
queued_tasks_count = 0

# [MEMORY + QUEUE] Limit to 1 video at a time
MAX_CONCURRENT_TASKS = 1
semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

# [ADMIN SECURITY] Admin whitelist
ADMIN_IDS: Set[str] = {OWNER_CHAT_ID}  # Only owner is admin


def is_admin(chat_id: str) -> bool:
    """Check if user is admin (owner or whitelisted)"""
    return str(chat_id) in ADMIN_IDS


@asynccontextmanager
async def lifespan(app: FastAPI):
    global tg_client
    
    if pyrogram_available and API_ID and API_HASH and BOT_TOKEN:
        try:
            tg_client = Client(
                "cambot_session",
                api_id=int(API_ID),
                api_hash=API_HASH,
                bot_token=BOT_TOKEN,
                in_memory=True
            )
            await tg_client.start()
            print("✓ Pyrogram started (handles 50MB - 2GB files)")
        except Exception as e:
            print(f"✗ Pyrogram failed: {e}")
            tg_client = None
    else:
        print("⚠ Pyrogram disabled (only <50MB files will work)")

    worker_task = asyncio.create_task(queue_worker())
    print("✓ Queue worker started (1 video at a time)")
    print(f"✓ Admin-only mode: Only {OWNER_CHAT_ID} can control bot")
    
    yield
    
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass
    
    if tg_client:
        try:
            await tg_client.stop()
        except:
            pass


app = FastAPI(lifespan=lifespan)

connected_devices: Dict[str, WebSocket] = {}
progress_message_ids: Dict[str, int] = {}
last_edit_time: Dict[str, float] = {}
last_edit_timestamps: Dict[str, float] = {}


def clean_chat_id(chat_id: str) -> Union[int, str]:
    c = str(chat_id).strip()
    if c.startswith("-") and c[1:].isdigit():
        return int(c)
    if c.isdigit():
        return int(c)
    if not c.startswith("@") and not c.startswith("-") and not c.isdigit():
        if any(char.isalpha() for char in c):
            return f"@{c}"
    return c


async def tg_send_message(chat_id: str, text: str, auto_delete: bool = False, delay: int = 5):
    global tg_client
    msg_id = None
    target_clean = clean_chat_id(chat_id)

    if pyrogram_available and tg_client and tg_client.is_connected:
        try:
            msg = await tg_client.send_message(chat_id=target_clean, text=text)
            msg_id = msg.id
            if msg_id and auto_delete:
                asyncio.create_task(schedule_message_deletion(chat_id, msg_id, delay))
            return msg_id
        except Exception as e:
            print(f"[TG] Pyrogram failed: {e}")

    async with httpx.AsyncClient() as client:
        try:
            r = await client.post(f"{TELEGRAM_API}/sendMessage", data={"chat_id": chat_id, "text": text})
            data = r.json()
            if data.get("ok"):
                msg_id = data["result"]["message_id"]
        except:
            pass

    if msg_id and auto_delete:
        asyncio.create_task(schedule_message_deletion(chat_id, msg_id, delay))
    return msg_id


async def tg_delete_message(chat_id: str, message_id: int):
    global tg_client
    target_clean = clean_chat_id(chat_id)
    
    if pyrogram_available and tg_client and tg_client.is_connected:
        try:
            await tg_client.delete_messages(chat_id=target_clean, message_ids=message_id)
            return True
        except:
            pass
    
    async with httpx.AsyncClient() as client:
        try:
            await client.post(f"{TELEGRAM_API}/deleteMessage", data={"chat_id": chat_id, "message_id": message_id})
            return True
        except:
            return False


async def schedule_message_deletion(chat_id: str, message_id: int, delay: int = 5):
    await asyncio.sleep(delay)
    await tg_delete_message(chat_id, message_id)


async def tg_edit_message(chat_id: str, message_id: int, text: str):
    global tg_client
    target_clean = clean_chat_id(chat_id)
    
    if pyrogram_available and tg_client and tg_client.is_connected:
        try:
            await tg_client.edit_message_text(chat_id=target_clean, message_id=message_id, text=text)
            return True
        except Exception as e:
            if "MESSAGE_NOT_MODIFIED" in str(e):
                return True
    
    async with httpx.AsyncClient() as client:
        try:
            await client.post(f"{TELEGRAM_API}/editMessageText", data={"chat_id": chat_id, "message_id": message_id, "text": text})
        except:
            pass


def make_progress_bar(percent: float, length: int = 10) -> str:
    filled = int(round((max(0.0, min(100.0, percent)) / 100) * length))
    return "■" * filled + "□" * (length - max(0, filled))


async def edit_status_throttled(chat_id: str, message_id: int, text: str, force: bool = False):
    key = f"{chat_id}_{message_id}"
    now = time.time()
    if force or now - last_edit_timestamps.get(key, 0) >= 2.5:
        last_edit_timestamps[key] = now
        try:
            await tg_edit_message(chat_id, message_id, text)
        except:
            pass


async def has_audio_track(file_path: str) -> bool:
    cmd = ["ffprobe", "-v", "error", "-select_streams", "a", "-show_entries", "stream=codec_type", "-of", "default=noprint_wrappers=1:nokey=1", file_path]
    try:
        process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, _ = await process.communicate()
        return len(stdout.strip()) > 0
    except:
        return False


async def get_video_duration(file_path: str) -> float:
    cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", file_path]
    try:
        process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, _ = await process.communicate()
        return float(stdout.strip())
    except:
        return 0.0


async def convert_video_to_mp4(input_path: str, output_path: str, chat_id: str, message_id: int) -> bool:
    total_duration = await get_video_duration(input_path)
    if total_duration <= 0:
        total_duration = 1.0
    has_audio = await has_audio_track(input_path)

    cmd = [
        "ffmpeg", "-y",
        "-threads", "1",
        "-i", input_path,
        "-c:v", "libx264",
        "-preset", "ultrafast",
        "-crf", "28",
        "-pix_fmt", "yuv420p",
    ]
    
    if has_audio:
        cmd.extend(["-c:a", "aac", "-b:a", "96k"])
    else:
        cmd.append("-an")
    
    cmd.extend(["-movflags", "+faststart", output_path])

    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE
        )
        
        while True:
            line_bytes = await process.stderr.readline()
            if not line_bytes:
                break
            line = line_bytes.decode(errors='ignore').strip()
            match = re.search(r"time=(\d+):(\d+):(\d+\.\d+)", line)
            if match:
                current_time = int(match.group(1)) * 3600 + int(match.group(2)) * 60 + float(match.group(3))
                percent = min(100.0, (current_time / total_duration) * 100)
                bar = make_progress_bar(percent)
                status_text = f"📊 **[ HD CONVERT ]** 📊\n━━━━━━━━━━━━━━━━━━━━━━\n📹 Direct: Delivered ✅\n⚙️ Converting: [{bar}] {percent:.1f}%\n━━━━━━━━━━━━━━━━━━━━━━"
                await edit_status_throttled(chat_id, message_id, status_text)
        
        await process.wait()
        gc.collect()
        
        return process.returncode == 0
    except Exception as e:
        print(f"[FFMPEG] Error: {e}")
        return False


async def split_video(file_path: str, segment_duration: float) -> list:
    output_pattern = "/tmp/split_part_%03d.mp4"
    for f in glob.glob("/tmp/split_part_*.mp4"):
        try:
            os.remove(f)
        except:
            pass
    
    cmd = ["ffmpeg", "-y", "-threads", "1", "-i", file_path, "-c", "copy", "-map", "0", "-segment_time", str(segment_duration), "-f", "segment", "-reset_timestamps", "1", output_pattern]
    
    try:
        process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        await process.communicate()
        gc.collect()
        
        if process.returncode == 0:
            parts = sorted(glob.glob("/tmp/split_part_*.mp4"))
            if parts:
                return parts
        return [file_path]
    except:
        return [file_path]


async def generate_thumbnail(video_path: str, thumb_path: str) -> bool:
    cmd = ["ffmpeg", "-y", "-threads", "1", "-ss", "00:00:02", "-i", video_path, "-vframes", "1", "-q:v", "5", thumb_path]
    try:
        process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        await process.wait()
        return process.returncode == 0
    except:
        return False


async def progress_callback(current, total, chat_id, message_id, filename, current_part, total_parts, label="Uploading"):
    percent = (current / total) * 100
    bar = make_progress_bar(percent)
    current_mb = current / (1024 * 1024)
    total_mb = total / (1024 * 1024)
    
    if total_parts > 1:
        upload_line = f"📤 {label} Part {current_part}/{total_parts}: [{bar}] {percent:.1f}%\nℹ️ {current_mb:.1f}MB / {total_mb:.1f}MB"
    else:
        upload_line = f"📤 {label}: [{bar}] {percent:.1f}%\nℹ️ {current_mb:.1f}MB / {total_mb:.1f}MB"
    
    status_text = f"📊 **[ STATUS ]** 📊\n━━━━━━━━━━━━━━━━━━━━━━\n{upload_line}\n━━━━━━━━━━━━━━━━━━━━━━"
    await edit_status_throttled(chat_id, message_id, status_text, force=(current == total))


async def safe_send_video(target_chat_clean, video_path, caption, thumb_path, progress_args_tuple, chat_id, status_msg_id, label):
    global tg_client
    if not (pyrogram_available and tg_client and tg_client.is_connected):
        return False
    
    try:
        try:
            await tg_client.get_chat(target_chat_clean)
        except:
            pass

        kwargs = {
            "chat_id": target_chat_clean,
            "video": video_path,
            "caption": caption,
            "supports_streaming": True,
            "progress": progress_callback,
            "progress_args": progress_args_tuple
        }
        if thumb_path and os.path.exists(thumb_path):
            kwargs["thumb"] = thumb_path

        await tg_client.send_video(**kwargs)
        gc.collect()
        
        return True
    except Exception as e:
        print(f"[SEND] Error: {e}")
        return False


async def safe_send_video_http(target_chat, video_path, caption, thumb_path, file_name):
    file_size = os.path.getsize(video_path)
    if file_size > 50 * 1024 * 1024:
        print(f"[SEND] File too large for HTTP ({file_size / (1024*1024):.1f}MB), using Pyrogram")
        return False
    
    try:
        async with httpx.AsyncClient() as client:
            with open(video_path, "rb") as f:
                files = {"video": (file_name, f, "video/mp4")}
                if thumb_path and os.path.exists(thumb_path):
                    with open(thumb_path, "rb") as t:
                        files["thumb"] = (os.path.basename(thumb_path), t, "image/jpeg")
                data = {"chat_id": target_chat, "caption": caption, "supports_streaming": "true"}
                r = await client.post(f"{TELEGRAM_API}/sendVideo", data=data, files=files, timeout=None)
                gc.collect()
                
                return r.status_code == 200 and r.json().get("ok", False)
    except:
        return False


async def send_original_fast(input_path, file_name, chat_id, target_chat, caption, status_msg_id, queue_position):
    target_chat_clean = clean_chat_id(target_chat)
    file_size = os.path.getsize(input_path)
    limit_2gb = 2000 * 1024 * 1024

    base, _ = os.path.splitext(file_name)
    thumb_path = f"/tmp/{base}_orig_thumb.jpg"
    has_thumb = await generate_thumbnail(input_path, thumb_path)
    if not has_thumb or not os.path.exists(thumb_path) or os.path.getsize(thumb_path) == 0:
        thumb_path = None

    if queue_position > 1:
        status_text = f"📊 **[ QUEUE ]** 📊\n━━━━━━━━━━━━━━━━━━━━━━\n⏳ Position: #{queue_position}\n📥 Received: Complete ✅\n📤 Waiting for turn...\n━━━━━━━━━━━━━━━━━━━━━━"
        await edit_status_throttled(chat_id, status_msg_id, status_text, force=True)
        await semaphore.acquire()
        semaphore.release()
    
    status_text = f"📊 **[ FAST DELIVERY ]** 📊\n━━━━━━━━━━━━━━━━━━━━━━\n📥 Received: Complete ✅\n📤 Sending Direct Video... ⏳\n━━━━━━━━━━━━━━━━━━━━━━"
    await edit_status_throttled(chat_id, status_msg_id, status_text, force=True)

    sent = False

    if file_size > limit_2gb:
        status_text = f"📊 **[ FAST DELIVERY ]** 📊\n━━━━━━━━━━━━━━━━━━━━━━\n📥 Received: Complete ✅\n✂️ Splitting {file_size / (1024*1024*1024):.1f}GB File... ⏳\n━━━━━━━━━━━━━━━━━━━━━━"
        await edit_status_throttled(chat_id, status_msg_id, status_text, force=True)

        duration = await get_video_duration(input_path)
        if duration <= 0:
            duration = 600.0
        num_parts = math.ceil(file_size / limit_2gb)
        segment_duration = duration / num_parts
        parts = await split_video(input_path, segment_duration)

        for i, part in enumerate(parts):
            part_name = os.path.basename(part)
            part_caption = f"{caption}\n\n🎬 Direct Part {i+1}/{len(parts)}"
            ok = await safe_send_video(target_chat_clean, part, part_caption, thumb_path, (chat_id, status_msg_id, part_name, i+1, len(parts), "Direct Video"), chat_id, status_msg_id, "Direct Video")
            if not ok:
                ok = await safe_send_video_http(target_chat, part, part_caption, thumb_path, part_name)
            if ok:
                sent = True
            if part != input_path:
                try:
                    os.remove(part)
                except:
                    pass
    else:
        direct_caption = f"{caption}\n\n📹 Direct Video (Original)"
        ok = await safe_send_video(target_chat_clean, input_path, direct_caption, thumb_path, (chat_id, status_msg_id, file_name, 1, 1, "Direct Video"), chat_id, status_msg_id, "Direct Video")
        if not ok:
            ok = await safe_send_video_http(target_chat, input_path, direct_caption, thumb_path, file_name)
        if ok:
            sent = True

    if thumb_path and os.path.exists(thumb_path):
        try:
            os.remove(thumb_path)
        except:
            pass

    return sent


async def background_convert_and_send(input_path, file_name, chat_id, target_chat, caption, status_msg_id):
    target_chat_clean = clean_chat_id(target_chat)
    base, _ = os.path.splitext(file_name)
    output_name = f"{base}_HD.mp4"
    temp_output_path = f"/tmp/{output_name}"

    if os.path.exists(temp_output_path):
        try:
            os.remove(temp_output_path)
        except:
            pass

    status_text = f"📊 **[ HD CONVERT ]** 📊\n━━━━━━━━━━━━━━━━━━━━━━\n📹 Direct: Delivered ✅\n⚙️ Converting: Starting... ⏳\n━━━━━━━━━━━━━━━━━━━━━━"
    await edit_status_throttled(chat_id, status_msg_id, status_text, force=True)

    success = await convert_video_to_mp4(input_path, temp_output_path, chat_id, status_msg_id)

    if not success or not os.path.exists(temp_output_path) or os.path.getsize(temp_output_path) == 0:
        status_text = f"📊 **[ COMPLETE ]** 📊\n━━━━━━━━━━━━━━━━━━━━━━\n📹 Direct: Delivered ✅\n⚙️ HD: Not needed ℹ️\n━━━━━━━━━━━━━━━━━━━━━━\n✅ Done!"
        await edit_status_throttled(chat_id, status_msg_id, status_text, force=True)
        try:
            os.remove(temp_output_path)
        except:
            pass
        return

    converted_size = os.path.getsize(temp_output_path)
    limit_2gb = 2000 * 1024 * 1024

    thumb_path = f"/tmp/{base}_hd_thumb.jpg"
    has_thumb = await generate_thumbnail(temp_output_path, thumb_path)
    if not has_thumb or not os.path.exists(thumb_path) or os.path.getsize(thumb_path) == 0:
        thumb_path = None

    status_text = f"📊 **[ HD CONVERT ]** 📊\n━━━━━━━━━━━━━━━━━━━━━━\n📹 Direct: Delivered ✅\n⚙️ Converting: Complete ✅\n📤 Uploading HD... ⏳\n━━━━━━━━━━━━━━━━━━━━━━"
    await edit_status_throttled(chat_id, status_msg_id, status_text, force=True)

    sent = False

    if converted_size > limit_2gb:
        duration = await get_video_duration(temp_output_path)
        if duration <= 0:
            duration = 600.0
        num_parts = math.ceil(converted_size / limit_2gb)
        segment_duration = duration / num_parts
        parts = await split_video(temp_output_path, segment_duration)

        for i, part in enumerate(parts):
            part_name = os.path.basename(part)
            part_caption = f"{caption}\n\n🎬 HD Part {i+1}/{len(parts)}"
            ok = await safe_send_video(target_chat_clean, part, part_caption, thumb_path, (chat_id, status_msg_id, part_name, i+1, len(parts), "HD Video"), chat_id, status_msg_id, "HD Video")
            if not ok:
                ok = await safe_send_video_http(target_chat, part, part_caption, thumb_path, part_name)
            if ok:
                sent = True
            if part != temp_output_path:
                try:
                    os.remove(part)
                except:
                    pass
    else:
        hd_caption = f"{caption}\n\n🎬 HD Converted Video"
        ok = await safe_send_video(target_chat_clean, temp_output_path, hd_caption, thumb_path, (chat_id, status_msg_id, output_name, 1, 1, "HD Video"), chat_id, status_msg_id, "HD Video")
        if not ok:
            ok = await safe_send_video_http(target_chat, temp_output_path, hd_caption, thumb_path, output_name)
        if ok:
            sent = True

    if sent:
        done_text = f"📊 **[ COMPLETE ]** 📊\n━━━━━━━━━━━━━━━━━━━━━━\n📹 Direct: Delivered ✅\n🎬 HD: Delivered ✅\n━━━━━━━━━━━━━━━━━━━━━━\n🎉 Done!"
    else:
        done_text = f"📊 **[ COMPLETE ]** 📊\n━━━━━━━━━━━━━━━━━━━━━━\n📹 Direct: Delivered ✅\n🎬 HD: Skipped ℹ️\n━━━━━━━━━━━━━━━━━━━━━━\n✅ Done!"
    
    await edit_status_throttled(chat_id, status_msg_id, done_text, force=True)

    for path in [temp_output_path, thumb_path]:
        try:
            if path and os.path.exists(path):
                os.remove(path)
        except:
            pass
    
    for f in glob.glob("/tmp/split_part_*.mp4"):
        try:
            os.remove(f)
        except:
            pass


async def process_and_upload_video(input_path, file_name, chat_id, target_chat, caption="", queue_position=1):
    async with semaphore:
        status_msg_id = progress_message_ids.get(chat_id)
        if not status_msg_id:
            status_msg_id = await tg_send_message(chat_id, "⚙️ Processing...")
            if status_msg_id:
                progress_message_ids[chat_id] = status_msg_id

        await send_original_fast(input_path, file_name, chat_id, target_chat, caption, status_msg_id, queue_position)
        await background_convert_and_send(input_path, file_name, chat_id, target_chat, caption, status_msg_id)

        try:
            if os.path.exists(input_path):
                os.remove(input_path)
        except:
            pass

        for f in glob.glob("/tmp/split_part_*.mp4"):
            try:
                os.remove(f)
            except:
                pass

        progress_message_ids.pop(chat_id, None)
        last_edit_timestamps.pop(f"{chat_id}_{status_msg_id}", None)
        gc.collect()
        
        print(f"[PIPELINE] Video #{queue_position} done + GC")


async def queue_worker():
    global queued_tasks_count
    while True:
        try:
            item = await processing_queue.get()
            queued_tasks_count = max(0, queued_tasks_count - 1)
            queue_position = item.get("queue_position", 1)
            
            try:
                await process_and_upload_video(item["input_path"], item["file_name"], item["chat_id"], item["target_chat"], item["caption"], queue_position)
            except Exception as e:
                print(f"Worker error: {e}")
            
            processing_queue.task_done()
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"Queue error: {e}")
            await asyncio.sleep(1)


@app.post("/webhook/{secret}")
async def telegram_webhook(secret: str, request: Request):
    if secret != WEBHOOK_SECRET:
        return {"ok": False}
    update = await request.json()
    message = update.get("message")
    if not message:
        return {"ok": True}
    
    chat_id = str(message["chat"]["id"])
    text = message.get("text", "").strip()
    msg_id = message.get("message_id")
    
    # [ADMIN SECURITY] Check if user is admin
    if not is_admin(chat_id):
        # Non-admin user - send access denied message
        await tg_send_message(
            chat_id,
            "🚫 **Access Denied**\n\nYou are not authorized to use this bot.\nOnly the owner can control this security camera.",
            auto_delete=True,
            delay=10
        )
        print(f"[SECURITY] Blocked non-admin user: {chat_id}")
        return {"ok": True}
    
    # [ADMIN ONLY] Process admin commands
    if text.startswith("/") and msg_id:
        asyncio.create_task(schedule_message_deletion(chat_id, msg_id, 5))
    
    if text == "/myid":
        await tg_send_message(chat_id, f"✅ Your Chat ID: {chat_id}\n\nYou are ADMIN ✓", auto_delete=True, delay=10)
        return {"ok": True}
    
    if text == "/help":
        help_text = (
            "🔒 **Security Cam Bot - Admin Commands**\n\n"
            "/on - Start recording\n"
            "/off - Stop recording\n"
            "/switch - Switch camera (front/back)\n"
            "/status - Check phone connection\n"
            "/myid - Show your chat ID\n"
            "/help - Show this help\n\n"
            "✅ You are authorized admin"
        )
        await tg_send_message(chat_id, help_text, auto_delete=True, delay=30)
        return {"ok": True"}
    
    cmd = text.lower()
    if cmd in ("/on", "/startcam", "/start_rec"):
        await dispatch_command(chat_id, "start")
    elif cmd in ("/off", "/stopcam", "/stop_rec"):
        await dispatch_command(chat_id, "stop")
    elif cmd in ("/switchcam", "/switch", "/cam"):
        await dispatch_command(chat_id, "switch")
    elif cmd == "/status":
        online = "Phone connected" if connected_devices else "Phone offline"
        await tg_send_message(chat_id, online, auto_delete=True, delay=5)
    
    return {"ok": True}


async def dispatch_command(chat_id: str, cmd: str):
    if not connected_devices:
        await tg_send_message(chat_id, "Phone offline", auto_delete=True, delay=5)
        return
    for ws in list(connected_devices.values()):
        try:
            await ws.send_json({"cmd": cmd, "chat_id": chat_id})
        except:
            pass
    labels = {"start": "Recording ON", "stop": "Recording OFF", "switch": "Camera switch"}
    await tg_send_message(chat_id, f"Command: {labels.get(cmd, cmd)}", auto_delete=True, delay=5)


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    token = websocket.query_params.get("token")
    if token != DEVICE_TOKEN:
        await websocket.close(code=4401)
        return
    await websocket.accept()
    connected_devices[token] = websocket
    try:
        while True:
            message = await websocket.receive()
            if message.get("type") == "websocket.disconnect":
                break
            if "text" in message:
                try:
                    data = json.loads(message["text"])
                    await handle_device_event(data)
                except:
                    pass
    except:
        pass
    finally:
        connected_devices.pop(token, None)


async def handle_device_event(data: dict):
    chat_id = OWNER_CHAT_ID
    event = data.get("event")
    if event == "status":
        text = data.get("text", "")
        mid = progress_message_ids.get(chat_id)
        if mid:
            await tg_edit_message(chat_id, mid, text)
        else:
            mid = await tg_send_message(chat_id, text)
            if mid:
                progress_message_ids[chat_id] = mid
    elif event == "progress":
        percent = int(data.get("percent", 0))
        bar = make_progress_bar(percent)
        text = f"📊 **[ UPLOAD ]** 📊\n━━━━━━━━━━━━━━━━━━━━━━\n📥 Phone → Server: [{bar}] {percent}%\n━━━━━━━━━━━━━━━━━━━━━━"
        now = time.time()
        if now - last_edit_time.get(chat_id, 0) < 2 and percent < 100:
            return
        last_edit_time[chat_id] = now
        mid = progress_message_ids.get(chat_id)
        if mid:
            await tg_edit_message(chat_id, mid, text)
        else:
            mid = await tg_send_message(chat_id, text)
            if mid:
                progress_message_ids[chat_id] = mid


@app.post("/upload")
async def custom_upload(request: Request, chat_id: str = Form(None), caption: str = Form("")):
    global queued_tasks_count
    form_data = await request.form()
    file_field = None
    for key, value in form_data.multi_items():
        if hasattr(value, "filename") and value.filename:
            file_field = value
            break
    if not file_field:
        return {"ok": False}
    target_chat = chat_id or OWNER_CHAT_ID
    file_name = file_field.filename or "video.mp4"
    temp_input_path = f"/tmp/{file_name}"
    with open(temp_input_path, "wb") as buffer:
        shutil.copyfileobj(file_field.file, buffer)
    
    queued_tasks_count += 1
    queue_position = queued_tasks_count
    
    status_msg_id = progress_message_ids.get(OWNER_CHAT_ID)
    if not status_msg_id:
        status_msg_id = await tg_send_message(OWNER_CHAT_ID, f"⚙️ Queued #{queue_position}...")
        if status_msg_id:
            progress_message_ids[OWNER_CHAT_ID] = status_msg_id
    
    await processing_queue.put({
        "input_path": temp_input_path,
        "file_name": file_name,
        "chat_id": OWNER_CHAT_ID,
        "target_chat": target_chat,
        "caption": caption,
        "queue_position": queue_position
    })
    
    return {"ok": True, "queue_position": queue_position}


@app.post("/bot{token}/{method}")
async def telegram_api_proxy(token: str, method: str, request: Request):
    global queued_tasks_count
    if token != BOT_TOKEN:
        return {"ok": False}
    if method not in ("sendVideo", "sendDocument", "sendAudio"):
        async with httpx.AsyncClient() as client:
            headers = {k: v for k, v in request.headers.items() if k.lower() not in ("host", "content-length")}
            body = await request.body()
            r = await client.post(f"https://api.telegram.org/bot{token}/{method}", headers=headers, content=body, params=dict(request.query_params))
            try:
                return r.json()
            except:
                return r.text
    try:
        form_data = await request.form()
    except ClientDisconnect:
        return {"ok": False}
    chat_id = form_data.get("chat_id") or OWNER_CHAT_ID
    caption = form_data.get("caption") or ""
    file_field = None
    for key, value in form_data.multi_items():
        if hasattr(value, "filename") and value.filename:
            file_field = value
            break
    if not file_field:
        async with httpx.AsyncClient() as client:
            headers = {k: v for k, v in request.headers.items() if k.lower() not in ("host", "content-length", "content-type")}
            r = await client.post(f"https://api.telegram.org/bot{token}/{method}", headers=headers, data=dict(form_data), params=dict(request.query_params))
            try:
                return r.json()
            except:
                return r.text
    
    file_name = file_field.filename or "video.mp4"
    temp_input_path = f"/tmp/{file_name}"
    with open(temp_input_path, "wb") as buffer:
        shutil.copyfileobj(file_field.file, buffer)
    
    queued_tasks_count += 1
    queue_position = queued_tasks_count
    
    status_msg_id = progress_message_ids.get(OWNER_CHAT_ID)
    if not status_msg_id:
        status_msg_id = await tg_send_message(OWNER_CHAT_ID, f"⚙️ Queued #{queue_position}...")
        if status_msg_id:
            progress_message_ids[OWNER_CHAT_ID] = status_msg_id
    
    await processing_queue.put({
        "input_path": temp_input_path,
        "file_name": file_name,
        "chat_id": OWNER_CHAT_ID,
        "target_chat": chat_id,
        "caption": caption,
        "queue_position": queue_position
    })
    
    return {"ok": True, "result": {"message_id": 99999, "chat": {"id": int(chat_id) if str(chat_id).replace("-", "").isdigit() else 0, "type": "private"}, "date": int(time.time()), "text": f"Queued #{queue_position}"}}


@app.get("/")
async def root():
    return {"status": "ok", "devices": len(connected_devices), "queue": queued_tasks_count, "admin_only": True}
