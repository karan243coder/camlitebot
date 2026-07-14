import os
import time
import asyncio
import math
import shutil
import glob
import json
import re
from typing import Dict, Union

import httpx
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, Form, UploadFile, File
from contextlib import asynccontextmanager
from starlette.requests import ClientDisconnect

# [SAFE IMPORT] Pyrogram import is wrapped to prevent startup crashes if requirements.txt is missing on remote
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

# Global Pyrogram Client
tg_client: Client = None

# Global asynchronous task queue for FFMPEG to prevent Koyeb server overloads
processing_queue = asyncio.Queue()
queued_tasks_count = 0

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
            print("Pyrogram Client successfully started!")
        except Exception as e:
            print(f"Error starting Pyrogram Client: {e}")
            tg_client = None
    else:
        if not pyrogram_available:
            print("WARNING: Pyrogram module not found! Unlimited uploads disabled. Please ensure requirements.txt is fully updated.")
        else:
            print("API_ID or API_HASH not found. Pyrogram Client disabled.")
            
    # Start continuous background queue worker
    worker_task = asyncio.create_task(queue_worker())
    print("Background FFMPEG Queue Worker started!")
        
    yield
    
    # Cancel worker task on shutdown
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass
    
    if tg_client:
        try:
            await tg_client.stop()
            print("Pyrogram Client stopped.")
        except Exception as e:
            print(f"Error stopping Pyrogram Client: {e}")

app = FastAPI(lifespan=lifespan)

connected_devices: Dict[str, WebSocket] = {}
progress_message_ids: Dict[str, int] = {}
last_edit_time: Dict[str, float] = {}
last_progress_edit: Dict[str, float] = {}
last_edit_timestamps: Dict[str, float] = {}


def clean_chat_id(chat_id: str) -> Union[int, str]:
    c = str(chat_id).strip()
    if c.startswith("-") and c[1:].isdigit():
        return int(c)
    if c.isdigit():
        return int(c)
    return c


async def tg_send_message(chat_id: str, text: str):
    global tg_client
    if pyrogram_available and tg_client and tg_client.is_connected:
        try:
            msg = await tg_client.send_message(chat_id=clean_chat_id(chat_id), text=text)
            return msg.id
        except Exception as e:
            print(f"Pyrogram send_message failed: {e}. Falling back to httpx.")
            
    async with httpx.AsyncClient() as client:
        r = await client.post(f"{TELEGRAM_API}/sendMessage", data={"chat_id": chat_id, "text": text})
        data = r.json()
        if data.get("ok"):
            return data["result"]["message_id"]
    return None


async def tg_edit_message(chat_id: str, message_id: int, text: str):
    global tg_client
    if pyrogram_available and tg_client and tg_client.is_connected:
        try:
            await tg_client.edit_message_text(chat_id=clean_chat_id(chat_id), message_id=message_id, text=text)
            return True
        except Exception as e:
            if "MESSAGE_NOT_MODIFIED" in str(e):
                return True
            print(f"Pyrogram edit_message failed: {e}. Falling back to httpx.")
            
    async with httpx.AsyncClient() as client:
        try:
            await client.post(
                f"{TELEGRAM_API}/editMessageText",
                data={"chat_id": chat_id, "message_id": message_id, "text": text},
            )
        except Exception:
            pass


def make_progress_bar(percent: float, length: int = 10) -> str:
    filled = int(round((max(0.0, min(100.0, percent)) / 100) * length))
    return "■" * filled + "□" * (length - filled)


async def edit_status_throttled(chat_id: str, message_id: int, text: str, force: bool = False):
    key = f"{chat_id}_{message_id}"
    now = time.time()
    if force or now - last_edit_timestamps.get(key, 0) >= 2.5:
        last_edit_timestamps[key] = now
        try:
            await tg_edit_message(chat_id, message_id, text)
        except Exception:
            pass


async def has_audio_track(file_path: str) -> bool:
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "a",
        "-show_entries", "stream=codec_type",
        "-of", "default=noprint_wrappers=1:nokey=1",
        file_path
    ]
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        return len(stdout.strip()) > 0
    except Exception:
        return False


async def convert_video_to_mp4_with_progress(input_path: str, output_path: str, chat_id: str, message_id: int) -> bool:
    total_duration = await get_video_duration(input_path)
    if total_duration <= 0:
        total_duration = 1.0 # Avoid division by zero
        
    has_audio = await has_audio_track(input_path)
    
    cmd = [
        "ffmpeg", "-y",
        "-i", input_path,
        "-c:v", "libx264",
        "-preset", "superfast",
        "-crf", "23",
        "-pix_fmt", "yuv420p",
    ]
    if has_audio:
        cmd.extend(["-c:a", "aac", "-strict", "-2", "-b:a", "128k"])
    else:
        cmd.append("-an")
        
    cmd.extend(["-movflags", "+faststart", output_path])
    
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        while True:
            line_bytes = await process.stderr.readline()
            if not line_bytes:
                break
            line = line_bytes.decode(errors='ignore').strip()
            
            match = re.search(r"time=(\d+):(\d+):(\d+\.\d+)", line)
            if match:
                hours = int(match.group(1))
                minutes = int(match.group(2))
                seconds = float(match.group(3))
                current_time = hours * 3600 + minutes * 60 + seconds
                
                percent = min(100.0, (current_time / total_duration) * 100)
                bar = make_progress_bar(percent)
                
                status_text = (
                    f"📊 **[ CAMLITE PROCESS STATUS ]** 📊\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📥 Receiving: Complete ✅\n"
                    f"⚙️ Converting Video: [{bar}] {percent:.1f}%\n"
                    f"📤 Uploading: Pending ⏳\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━"
                )
                await edit_status_throttled(chat_id, message_id, status_text)
                
        await process.wait()
        return process.returncode == 0
    except Exception as e:
        print(f"Error converting video with progress: {e}")
        return False


async def get_video_duration(file_path: str) -> float:
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        file_path
    ]
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        return float(stdout.strip())
    except Exception as e:
        print(f"ffprobe exception: {e}")
        return 0.0


async def split_video(file_path: str, segment_duration: float) -> list[str]:
    output_pattern = "/tmp/part_%03d.mp4"
    for f in glob.glob("/tmp/part_*.mp4"):
        try:
            os.remove(f)
        except Exception:
            pass
            
    cmd = [
        "ffmpeg", "-y",
        "-i", file_path,
        "-c", "copy",
        "-map", "0",
        "-segment_time", str(segment_duration),
        "-f", "segment",
        "-reset_timestamps", "1",
        output_pattern
    ]
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        await process.communicate()
        if process.returncode == 0:
            return sorted(glob.glob("/tmp/part_*.mp4"))
        return [file_path]
    except Exception as e:
        print(f"split exception: {e}")
        return [file_path]


async def pyrogram_progress_callback(current, total, chat_id, message_id, filename, current_part, total_parts):
    percent = (current / total) * 100
    bar = make_progress_bar(percent)
    current_mb = current / (1024 * 1024)
    total_mb = total / (1024 * 1024)
    
    if total_parts > 1:
        upload_line = f"📤 Uploading Part {current_part}/{total_parts}: [{bar}] {percent:.1f}%\nℹ️ Speed: {current_mb:.1f}MB / {total_mb:.1f}MB"
    else:
        upload_line = f"📤 Uploading Video: [{bar}] {percent:.1f}%\nℹ️ Speed: {current_mb:.1f}MB / {total_mb:.1f}MB"
        
    status_text = (
        f"📊 **[ CAMLITE PROCESS STATUS ]** 📊\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📥 Receiving: Complete ✅\n"
        f"⚙️ Converting: Complete ✅\n"
    )
    
    if total_parts > 1:
        status_text += f"✂️ Splitting: {total_parts} Parts Created ✅\n"
        
    status_text += (
        f"{upload_line}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )
    
    await edit_status_throttled(chat_id, message_id, status_text, force=(current == total))


async def process_and_upload_video(input_path: str, file_name: str, chat_id: str, caption: str = ""):
    global tg_client
    
    # Try to reuse the existing progress message if it exists
    status_msg_id = progress_message_ids.get(chat_id)
    if not status_msg_id:
        status_msg_id = await tg_send_message(chat_id, "⚙️ Processing: Video process ho raha hai...")
        if status_msg_id:
            progress_message_ids[chat_id] = status_msg_id
            
    # Initial status update for converting
    status_text = (
        f"📊 **[ CAMLITE PROCESS STATUS ]** 📊\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📥 Receiving: Complete ✅\n"
        f"⚙️ Converting Video: Starting... ⏳\n"
        f"📤 Uploading: Pending ⏳\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )
    await edit_status_throttled(chat_id, status_msg_id, status_text, force=True)
    
    base, _ = os.path.splitext(file_name)
    output_name = f"{base}_converted.mp4"
    temp_output_path = f"/tmp/{output_name}"
    
    if os.path.exists(temp_output_path):
        try:
            os.remove(temp_output_path)
        except Exception:
            pass
            
    # Convert video with active real-time progress parsing
    success = await convert_video_to_mp4_with_progress(input_path, temp_output_path, chat_id, status_msg_id)
    
    # [ROBUST FALLBACK] If conversion fails (e.g. FFMPEG not installed on Koyeb yet), upload the ORIGINAL raw video!
    upload_path = temp_output_path
    upload_name = output_name
    conversion_failed = False
    
    if not success or not os.path.exists(temp_output_path) or os.path.getsize(temp_output_path) == 0:
        conversion_failed = True
        upload_path = input_path
        upload_name = file_name
        
        status_text = (
            f"📊 **[ CAMLITE PROCESS STATUS ]** 📊\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📥 Receiving: Complete ✅\n"
            f"⚠️ Converting: Failed (Uploading raw video instead) ⚠️\n"
            f"📤 Uploading: Starting... ⏳\n"
            f"━━━━━━━━━━━━━━━━━━━━━━"
        )
        await edit_status_throttled(chat_id, status_msg_id, status_text, force=True)
    else:
        try:
            os.remove(input_path)
        except Exception:
            pass
        
    converted_size = os.path.getsize(upload_path)
    limit_2gb = 2000 * 1024 * 1024 # 2000 MB
    
    # Converting is complete / skipped
    if not conversion_failed:
        status_text = (
            f"📊 **[ CAMLITE PROCESS STATUS ]** 📊\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📥 Receiving: Complete ✅\n"
            f"⚙️ Converting: Complete ✅\n"
            f"📤 Uploading: Starting... ⏳\n"
            f"━━━━━━━━━━━━━━━━━━━━━━"
        )
        await edit_status_throttled(chat_id, status_msg_id, status_text, force=True)
    
    if pyrogram_available and tg_client and tg_client.is_connected:
        try:
            target_chat = clean_chat_id(chat_id)
            if converted_size > limit_2gb:
                status_text = (
                    f"📊 **[ CAMLITE PROCESS STATUS ]** 📊\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📥 Receiving: Complete ✅\n"
                    f"⚙️ Converting: {'Complete ✅' if not conversion_failed else 'Failed ⚠️'}\n"
                    f"✂️ Splitting: 2GB+ File detected. Splitting video... ⏳\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━"
                )
                await edit_status_throttled(chat_id, status_msg_id, status_text, force=True)
                
                duration = await get_video_duration(upload_path)
                if duration <= 0:
                    duration = 600.0
                    
                num_parts = math.ceil(converted_size / limit_2gb)
                segment_duration = duration / num_parts
                
                parts = await split_video(upload_path, segment_duration)
                if len(parts) > 1:
                    status_text = (
                        f"📊 **[ CAMLITE PROCESS STATUS ]** 📊\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📥 Receiving: Complete ✅\n"
                        f"⚙️ Converting: {'Complete ✅' if not conversion_failed else 'Failed ⚠️'}\n"
                        f"✂️ Splitting: {len(parts)} Parts Created ✅\n"
                        f"📤 Uploading: Starting upload... ⏳\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━"
                    )
                    await edit_status_throttled(chat_id, status_msg_id, status_text, force=True)
                    
                    for i, part in enumerate(parts):
                        part_name = os.path.basename(part)
                        part_caption = f"{caption}\n\n🎬 Part {i+1} of {len(parts)}"
                        
                        await tg_client.send_video(
                            chat_id=target_chat,
                            video=part,
                            caption=part_caption,
                            supports_streaming=True, # [STREAMING FIX] Tells Telegram to enable in-app streaming
                            progress=pyrogram_progress_callback,
                            progress_args=(chat_id, status_msg_id, part_name, i+1, len(parts))
                        )
                else:
                    await tg_client.send_video(
                        chat_id=target_chat,
                        video=upload_path,
                        caption=caption,
                        supports_streaming=True, # [STREAMING FIX] Tells Telegram to enable in-app streaming
                        progress=pyrogram_progress_callback,
                        progress_args=(chat_id, status_msg_id, upload_name, 1, 1)
                    )
            else:
                await tg_client.send_video(
                    chat_id=target_chat,
                    video=upload_path,
                    caption=caption,
                    supports_streaming=True, # [STREAMING FIX] Tells Telegram to enable in-app streaming
                    progress=pyrogram_progress_callback,
                    progress_args=(chat_id, status_msg_id, upload_name, 1, 1)
                )
                
            done_text = (
                f"📊 **[ CAMLITE PROCESS STATUS ]** 📊\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📥 Receiving: Complete ✅\n"
                f"⚙️ Converting: {'Complete ✅' if not conversion_failed else 'Failed (Uploaded Raw) ⚠️'}\n"
                f"📤 Uploading: Complete ✅\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🎉 **Done! Video convert aur upload ho gaya!**"
            )
            await edit_status_throttled(chat_id, status_msg_id, done_text, force=True)
            
        except Exception as e:
            await tg_send_message(chat_id, f"❌ Pyrogram Upload Error: {str(e)}")
    else:
        if converted_size > 50 * 1024 * 1024:
            warn_text = (
                f"📊 **[ CAMLITE PROCESS STATUS ]** 📊\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📥 Receiving: Complete ✅\n"
                f"⚙️ Converting: {'Complete ✅' if not conversion_failed else 'Failed ⚠️'}\n"
                f"⚠️ Uploading: Skipped (File is {converted_size / (1024*1024):.1f}MB)\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Telegram Bot API standard upload limit 50MB hai. Unlimited uploads aur splitting ke liye Koyeb me API_ID aur API_HASH environment variables set karein, aur requirements.txt me pyrogram add karein!"
            )
            await edit_status_throttled(chat_id, status_msg_id, warn_text, force=True)
        else:
            status_text = (
                f"📊 **[ CAMLITE PROCESS STATUS ]** 📊\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📥 Receiving: Complete ✅\n"
                f"⚙️ Converting: {'Complete ✅' if not conversion_failed else 'Failed ⚠️'}\n"
                f"📤 Uploading: Via standard Bot API... ⏳\n"
                f"━━━━━━━━━━━━━━━━━━━━━━"
            )
            await edit_status_throttled(chat_id, status_msg_id, status_text, force=True)
            try:
                async with httpx.AsyncClient() as client:
                    with open(upload_path, "rb") as f:
                        files = {"video": (upload_name, f, "video/mp4")}
                        data = {
                            "chat_id": chat_id, 
                            "caption": caption,
                            "supports_streaming": "true" # [STREAMING FIX] Tells Telegram Bot API to support streaming
                        }
                        r = await client.post(f"{TELEGRAM_API}/sendVideo", data=data, files=files, timeout=None)
                        if r.status_code == 200 and r.json().get("ok"):
                            done_text = (
                                f"📊 **[ CAMLITE PROCESS STATUS ]** 📊\n"
                                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                                f"📥 Receiving: Complete ✅\n"
                                f"⚙️ Converting: {'Complete ✅' if not conversion_failed else 'Failed (Uploaded Raw) ⚠️'}\n"
                                f"📤 Uploading: Complete ✅\n"
                                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                                f"🎉 **Done! Video standard Bot API se upload ho gaya!**"
                            )
                            await edit_status_throttled(chat_id, status_msg_id, done_text, force=True)
                        else:
                            await tg_send_message(chat_id, f"❌ Bot API upload failed: {r.text}")
            except Exception as e:
                await tg_send_message(chat_id, f"❌ Standard Bot API Upload Error: {str(e)}")
                
    # Clean up all temp files
    for path in [input_path, temp_output_path]:
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception:
            pass
            
    for f in glob.glob("/tmp/part_*.mp4"):
        try:
            os.remove(f)
        except Exception:
            pass
            
    # Comprehensive clean up of progress states for this run
    progress_message_ids.pop(chat_id, None)
    last_progress_edit.pop(f"{chat_id}_{status_msg_id}", None)
    last_edit_timestamps.pop(f"{chat_id}_{status_msg_id}", None)
    
    return True

# --- Async Background Task Queue Worker ---

async def queue_worker():
    global queued_tasks_count
    while True:
        try:
            item = await processing_queue.get()
            queued_tasks_count = max(0, queued_tasks_count - 1)
            
            input_path = item["input_path"]
            file_name = item["file_name"]
            chat_id = item["chat_id"]
            caption = item["caption"]
            
            try:
                await process_and_upload_video(input_path, file_name, chat_id, caption)
            except Exception as e:
                print(f"Error in queue worker processing: {e}")
                
            processing_queue.task_done()
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"Error in queue worker loop: {e}")
            await asyncio.sleep(1)


# --- Custom Webhook ---

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

    if text == "/myid":
        await tg_send_message(chat_id, f"Tumhara chat ID: {chat_id}")
        return {"ok": True}

    if chat_id != OWNER_CHAT_ID:
        return {"ok": True}

    cmd = text.lower()
    if cmd in ("/on", "/startcam", "/start_rec"):
        await dispatch_command(chat_id, "start")
    elif cmd in ("/off", "/stopcam", "/stop_rec"):
        await dispatch_command(chat_id, "stop")
    elif cmd in ("/switchcam", "/switch", "/cam"):
        await dispatch_command(chat_id, "switch")
    elif cmd == "/status":
        online = "Phone connected, ready" if connected_devices else "Phone offline (app band hai ya net nahi hai)"
        await tg_send_message(chat_id, online)

    return {"ok": True}


async def dispatch_command(chat_id: str, cmd: str):
    if not connected_devices:
        await tg_send_message(chat_id, "Phone abhi offline hai, app khula hai ya nahi check karo")
        return
    for ws in list(connected_devices.values()):
        try:
            await ws.send_json({"cmd": cmd, "chat_id": chat_id})
        except Exception:
            pass
    if cmd == "start":
        label = "Command bheja: recording ON"
    elif cmd == "stop":
        label = "Command bheja: recording OFF"
    else:
        label = "Command bheja: camera switch"
    await tg_send_message(chat_id, label)


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
            msg_type = message.get("type")
            
            if msg_type == "websocket.disconnect":
                break
                
            if "text" in message:
                try:
                    data = json.loads(message["text"])
                    await handle_device_event(data)
                except Exception as e:
                    print(f"Error parsing websocket JSON: {e}")
            elif "bytes" in message:
                print("Received binary data over WebSocket (ignored)")
    except Exception as e:
        print(f"WebSocket exception: {e}")
    finally:
        connected_devices.pop(token, None)


async def handle_device_event(data: dict):
    chat_id = data.get("chat_id") or OWNER_CHAT_ID
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
        
        text = (
            f"📊 **[ CAMLITE PROCESS STATUS ]** 📊\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📥 Receiving from Phone: [{bar}] {percent}%\n"
            f"⚙️ Converting: Pending ⏳\n"
            f"📤 Uploading: Pending ⏳\n"
            f"━━━━━━━━━━━━━━━━━━━━━━"
        )

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

    elif event == "done":
        # Keep the progress_message_ids saved so process_and_upload_video can reuse it!
        # It will be popped when the upload/process pipeline completely finishes.
        pass


# --- Custom Upload & Telegram Bot API Proxy Endpoints ---

@app.post("/upload")
async def custom_upload(
    request: Request,
    chat_id: str = Form(None),
    caption: str = Form(""),
):
    global queued_tasks_count
    form_data = await request.form()
    file_field = None
    for key, value in form_data.items():
        if isinstance(value, UploadFile):
            file_field = value
            break
            
    if not file_field:
        return {"ok": False, "description": "No file uploaded"}
        
    target_chat = chat_id or OWNER_CHAT_ID
    file_name = file_field.filename or "video.mp4"
    temp_input_path = f"/tmp/{file_name}"
    
    with open(temp_input_path, "wb") as buffer:
        shutil.copyfileobj(file_field.file, buffer)
        
    # Put task in Queue to prevent server overload
    status_msg_id = progress_message_ids.get(target_chat)
    if not status_msg_id:
        status_msg_id = await tg_send_message(target_chat, "⚙️ Processing: Video server par aa gaya hai...")
        if status_msg_id:
            progress_message_ids[target_chat] = status_msg_id
            
    if queued_tasks_count > 0:
        status_text = (
            f"📊 **[ CAMLITE PROCESS STATUS ]** 📊\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📥 Receiving: Complete ✅\n"
            f"⏳ Queue Position: #{queued_tasks_count} (Server busy hai, line me laga diya gaya hai...)\n"
            f"━━━━━━━━━━━━━━━━━━━━━━"
        )
        await edit_status_throttled(target_chat, status_msg_id, status_text, force=True)
        
    queued_tasks_count += 1
    
    item = {
        "input_path": temp_input_path,
        "file_name": file_name,
        "chat_id": target_chat,
        "caption": caption
    }
    await processing_queue.put(item)
    return {"ok": True, "description": "Processing queued successfully"}


@app.post("/bot{token}/{method}")
async def telegram_api_proxy(
    token: str,
    method: str,
    request: Request
):
    global queued_tasks_count
    if token != BOT_TOKEN:
        return {"ok": False, "description": "Unauthorized token"}
        
    if method not in ("sendVideo", "sendDocument", "sendAudio"):
        # Proxy straight to Telegram Bot API
        async with httpx.AsyncClient() as client:
            headers = {k: v for k, v in request.headers.items() if k.lower() not in ("host", "content-length")}
            body = await request.body()
            r = await client.post(
                f"https://api.telegram.org/bot{token}/{method}",
                headers=headers,
                content=body,
                params=dict(request.query_params)
            )
            try:
                return r.json()
            except Exception:
                return r.text
                
    # Parse form data for media upload methods
    try:
        form_data = await request.form()
    except ClientDisconnect:
        return {"ok": False, "description": "Client disconnected before upload finished"}
        
    chat_id = form_data.get("chat_id") or OWNER_CHAT_ID
    caption = form_data.get("caption") or ""
    
    file_field = None
    for key, value in form_data.items():
        if isinstance(value, UploadFile):
            file_field = value
            break
            
    if not file_field:
        # Fallback to direct proxy if no actual file is present
        # Since we already called request.form(), we cannot read request.body() anymore.
        # Instead, we pass the parsed form_data dictionary directly to HTTPX!
        async with httpx.AsyncClient() as client:
            headers = {k: v for k, v in request.headers.items() if k.lower() not in ("host", "content-length", "content-type")}
            r = await client.post(
                f"https://api.telegram.org/bot{token}/{method}",
                headers=headers,
                data=dict(form_data),
                params=dict(request.query_params)
            )
            try:
                return r.json()
            except Exception:
                return r.text
                
    # File is present, intercept and put in task Queue
    file_name = file_field.filename or "video.mp4"
    temp_input_path = f"/tmp/{file_name}"
    
    with open(temp_input_path, "wb") as buffer:
        shutil.copyfileobj(file_field.file, buffer)
        
    # Queue status configuration
    status_msg_id = progress_message_ids.get(chat_id)
    if not status_msg_id:
        status_msg_id = await tg_send_message(chat_id, "⚙️ Processing: Video server par aa gaya hai...")
        if status_msg_id:
            progress_message_ids[chat_id] = status_msg_id
            
    if queued_tasks_count > 0:
        status_text = (
            f"📊 **[ CAMLITE PROCESS STATUS ]** 📊\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📥 Receiving: Complete ✅\n"
            f"⏳ Queue Position: #{queued_tasks_count} (Server busy hai, line me laga diya gaya hai...)\n"
            f"━━━━━━━━━━━━━━━━━━━━━━"
        )
        await edit_status_throttled(chat_id, status_msg_id, status_text, force=True)
        
    queued_tasks_count += 1
    
    item = {
        "input_path": temp_input_path,
        "file_name": file_name,
        "chat_id": chat_id,
        "caption": caption
    }
    await processing_queue.put(item)
    
    return {
        "ok": True,
        "result": {
            "message_id": 99999,
            "chat": {
                "id": int(chat_id) if chat_id.replace("-", "").isdigit() else 0,
                "type": "private"
            },
            "date": int(time.time()),
            "text": "File intercepted and processing queued"
        }
    }


@app.get("/")
async def root():
    return {"status": "ok", "devices_connected": len(connected_devices), "queued_tasks": queued_tasks_count}
