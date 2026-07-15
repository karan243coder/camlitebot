import os
import time
import asyncio
import math
import shutil
import glob
import json
import re
import gc
from typing import Dict, Union

import httpx
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, Form, UploadFile, File
from contextlib import asynccontextmanager
from starlette.requests import ClientDisconnect

# [SAFE IMPORT] Pyrogram wrapped to prevent startup crashes
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

# Global async task queue
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
            print("WARNING: Pyrogram not found! Unlimited uploads disabled.")
        else:
            print("API_ID or API_HASH not found. Pyrogram disabled.")

    worker_task = asyncio.create_task(queue_worker())
    print("Background FFMPEG Queue Worker started!")

    yield

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
    if not c.startswith("@") and not c.startswith("-") and not c.isdigit():
        if any(char.isalpha() for char in c):
            return f"@{c}"
    return c


async def tg_send_message(chat_id: str, text: str, auto_delete: bool = False, delay: int = 5):
    global tg_client
    msg_id = None
    target_clean = clean_chat_id(chat_id)
    print(f"[TG_SEND] Sending to {target_clean}: {text[:60]}...")

    if pyrogram_available and tg_client and tg_client.is_connected:
        try:
            msg = await tg_client.send_message(chat_id=target_clean, text=text)
            msg_id = msg.id
            print(f"[TG_SEND] Pyrogram msg sent! ID: {msg_id}")
            if msg_id and auto_delete:
                asyncio.create_task(schedule_message_deletion(chat_id, msg_id, delay))
            return msg_id
        except Exception as e:
            print(f"[TG_SEND] Pyrogram send failed: {e}. Falling back to httpx.")

    async with httpx.AsyncClient() as client:
        try:
            r = await client.post(f"{TELEGRAM_API}/sendMessage", data={"chat_id": chat_id, "text": text})
            data = r.json()
            if data.get("ok"):
                msg_id = data["result"]["message_id"]
                print(f"[TG_SEND] HTTP msg sent! ID: {msg_id}")
        except Exception as err:
            print(f"[TG_SEND] HTTP send failed: {err}")

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
        except Exception as e:
            print(f"[TG_DELETE] Pyrogram delete failed: {e}")

    async with httpx.AsyncClient() as client:
        try:
            await client.post(
                f"{TELEGRAM_API}/deleteMessage",
                data={"chat_id": chat_id, "message_id": message_id},
            )
            return True
        except Exception as err:
            print(f"[TG_DELETE] HTTP delete failed: {err}")
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
            print(f"[TG_EDIT] Pyrogram edit failed: {e}")

    async with httpx.AsyncClient() as client:
        try:
            await client.post(
                f"{TELEGRAM_API}/editMessageText",
                data={"chat_id": chat_id, "message_id": message_id, "text": text},
            )
        except Exception as err:
            print(f"[TG_EDIT] HTTP edit failed: {err}")


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
    print(f"[FFMPEG] Starting conversion for {input_path}")
    total_duration = await get_video_duration(input_path)
    print(f"[FFMPEG] Duration: {total_duration}s")
    if total_duration <= 0:
        total_duration = 1.0

    has_audio = await has_audio_track(input_path)

    cmd = [
        "ffmpeg", "-y",
        "-threads", "1",
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
                hours = int(match.group(1))
                minutes = int(match.group(2))
                seconds = float(match.group(3))
                current_time = hours * 3600 + minutes * 60 + seconds

                percent = min(100.0, (current_time / total_duration) * 100)
                bar = make_progress_bar(percent)

                status_text = (
                    f"📊 **[ CAMLITE CONVERT STATUS ]** 📊\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📥 Original Video: Delivered ✅\n"
                    f"⚙️ Converting HD: [{bar}] {percent:.1f}%\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━"
                )
                await edit_status_throttled(chat_id, message_id, status_text)

        await process.wait()
        success = (process.returncode == 0)
        print(f"[FFMPEG] Conversion done! Return: {process.returncode}")
        return success
    except Exception as e:
        print(f"[FFMPEG] Error: {e}")
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
        print(f"[FFPROBE] Error: {e}")
        return 0.0


async def split_video(file_path: str, segment_duration: float) -> list:
    output_pattern = "/tmp/part_%03d.mp4"
    for f in glob.glob("/tmp/part_*.mp4"):
        try:
            os.remove(f)
        except Exception:
            pass

    cmd = [
        "ffmpeg", "-y",
        "-threads", "1",
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
        print(f"[FFMPEG] Split error: {e}")
        return [file_path]


async def generate_video_thumbnail(video_path: str, thumbnail_path: str) -> bool:
    cmd = [
        "ffmpeg", "-y",
        "-threads", "1",
        "-ss", "00:00:02",
        "-i", video_path,
        "-vframes", "1",
        "-q:v", "2",
        thumbnail_path
    ]
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL
        )
        await process.wait()
        return (process.returncode == 0)
    except Exception:
        return False


async def pyrogram_progress_callback(current, total, chat_id, message_id, filename, current_part, total_parts, label="Uploading"):
    percent = (current / total) * 100
    bar = make_progress_bar(percent)
    current_mb = current / (1024 * 1024)
    total_mb = total / (1024 * 1024)

    if total_parts > 1:
        upload_line = f"📤 {label} Part {current_part}/{total_parts}: [{bar}] {percent:.1f}%\nℹ️ {current_mb:.1f}MB / {total_mb:.1f}MB"
    else:
        upload_line = f"📤 {label}: [{bar}] {percent:.1f}%\nℹ️ {current_mb:.1f}MB / {total_mb:.1f}MB"

    status_text = (
        f"📊 **[ CAMLITE PROCESS STATUS ]** 📊\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{upload_line}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )
    await edit_status_throttled(chat_id, message_id, status_text, force=(current == total))


# ============================================================
# [NEW] FAST DELIVERY: Send original video IMMEDIATELY
# ============================================================
async def send_original_fast(input_path: str, file_name: str, chat_id: str, target_chat: str, caption: str, status_msg_id: int):
    """Sends the original video from phone to Telegram immediately without any FFMPEG conversion."""
    global tg_client
    target_chat_clean = clean_chat_id(target_chat)
    file_size = os.path.getsize(input_path)
    limit_2gb = 2000 * 1024 * 1024

    # Generate thumbnail from original
    base, _ = os.path.splitext(file_name)
    thumb_path = f"/tmp/{base}_orig_thumb.jpg"
    has_thumb = await generate_video_thumbnail(input_path, thumb_path)
    if not has_thumb or not os.path.exists(thumb_path) or os.path.getsize(thumb_path) == 0:
        thumb_path = None

    # Update status
    status_text = (
        f"📊 **[ CAMLITE FAST DELIVERY ]** 📊\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📥 Received: Complete ✅\n"
        f"📤 Sending Direct Video: Starting... ⏳\n"
        f"⚙️ HD Convert: Will start after delivery\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )
    await edit_status_throttled(chat_id, status_msg_id, status_text, force=True)

    sent_success = False

    if pyrogram_available and tg_client and tg_client.is_connected:
        try:
            # Resolve peer
            try:
                await tg_client.get_chat(target_chat_clean)
            except Exception:
                pass

            if file_size > limit_2gb:
                # Split and send
                status_text = (
                    f"📊 **[ CAMLITE FAST DELIVERY ]** 📊\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📥 Received: Complete ✅\n"
                    f"✂️ Splitting 2GB+ File... ⏳\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━"
                )
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
                    await tg_client.send_video(
                        chat_id=target_chat_clean,
                        video=part,
                        caption=part_caption,
                        supports_streaming=True,
                        thumb=thumb_path,
                        progress=pyrogram_progress_callback,
                        progress_args=(chat_id, status_msg_id, part_name, i+1, len(parts), "Direct Video")
                    )
                    # Clean split parts
                    try:
                        os.remove(part)
                    except Exception:
                        pass
            else:
                # Single file send
                direct_caption = f"{caption}\n\n📹 Direct Video (Original)"
                await tg_client.send_video(
                    chat_id=target_chat_clean,
                    video=input_path,
                    caption=direct_caption,
                    supports_streaming=True,
                    thumb=thumb_path,
                    progress=pyrogram_progress_callback,
                    progress_args=(chat_id, status_msg_id, file_name, 1, 1, "Direct Video")
                )

            sent_success = True
            print("[FAST] Original video sent successfully via Pyrogram!")

        except Exception as e:
            print(f"[FAST] Pyrogram direct send failed: {e}")

    # Fallback to HTTP Bot API if Pyrogram failed and file < 50MB
    if not sent_success and file_size <= 50 * 1024 * 1024:
        try:
            async with httpx.AsyncClient() as client:
                with open(input_path, "rb") as f:
                    files = {"video": (file_name, f, "video/mp4")}
                    if thumb_path:
                        with open(thumb_path, "rb") as t:
                            files["thumb"] = (os.path.basename(thumb_path), t, "image/jpeg")
                    data = {
                        "chat_id": target_chat,
                        "caption": f"{caption}\n\n📹 Direct Video (Original)",
                        "supports_streaming": "true"
                    }
                    r = await client.post(f"{TELEGRAM_API}/sendVideo", data=data, files=files, timeout=None)
                    if r.status_code == 200 and r.json().get("ok"):
                        sent_success = True
                        print("[FAST] Original video sent via HTTP Bot API!")
        except Exception as e:
            print(f"[FAST] HTTP fallback failed: {e}")

    # Clean thumb
    if thumb_path and os.path.exists(thumb_path):
        try:
            os.remove(thumb_path)
        except Exception:
            pass

    return sent_success


# ============================================================
# [NEW] BACKGROUND CONVERT: After fast delivery, convert in background
# ============================================================
async def background_convert_and_send(input_path: str, file_name: str, chat_id: str, target_chat: str, caption: str, status_msg_id: int):
    """Converts video with FFMPEG in background and sends the HD converted version."""
    global tg_client
    target_chat_clean = clean_chat_id(target_chat)

    base, _ = os.path.splitext(file_name)
    output_name = f"{base}_HD.mp4"
    temp_output_path = f"/tmp/{output_name}"

    if os.path.exists(temp_output_path):
        try:
            os.remove(temp_output_path)
        except Exception:
            pass

    # Start conversion
    status_text = (
        f"📊 **[ CAMLITE HD CONVERT ]** 📊\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📥 Original Video: Delivered ✅\n"
        f"⚙️ Converting HD: Starting... ⏳\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )
    await edit_status_throttled(chat_id, status_msg_id, status_text, force=True)

    success = await convert_video_to_mp4_with_progress(input_path, temp_output_path, chat_id, status_msg_id)

    if not success or not os.path.exists(temp_output_path) or os.path.getsize(temp_output_path) == 0:
        status_text = (
            f"📊 **[ CAMLITE HD CONVERT ]** 📊\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📥 Original Video: Delivered ✅\n"
            f"⚙️ HD Convert: Skipped (already compatible) ℹ️\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ Direct video already sent!"
        )
        await edit_status_throttled(chat_id, status_msg_id, status_text, force=True)
        # Clean up
        try:
            os.remove(temp_output_path)
        except Exception:
            pass
        return

    converted_size = os.path.getsize(temp_output_path)
    limit_2gb = 2000 * 1024 * 1024

    # Thumbnail for converted
    thumb_path = f"/tmp/{base}_hd_thumb.jpg"
    has_thumb = await generate_video_thumbnail(temp_output_path, thumb_path)
    if not has_thumb or not os.path.exists(thumb_path) or os.path.getsize(thumb_path) == 0:
        thumb_path = None

    status_text = (
        f"📊 **[ CAMLITE HD CONVERT ]** 📊\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📥 Original Video: Delivered ✅\n"
        f"⚙️ Converting HD: Complete ✅\n"
        f"📤 Uploading HD: Starting... ⏳\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )
    await edit_status_throttled(chat_id, status_msg_id, status_text, force=True)

    sent = False

    if pyrogram_available and tg_client and tg_client.is_connected:
        try:
            try:
                await tg_client.get_chat(target_chat_clean)
            except Exception:
                pass

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
                    await tg_client.send_video(
                        chat_id=target_chat_clean,
                        video=part,
                        caption=part_caption,
                        supports_streaming=True,
                        thumb=thumb_path,
                        progress=pyrogram_progress_callback,
                        progress_args=(chat_id, status_msg_id, part_name, i+1, len(parts), "HD Video")
                    )
                    try:
                        os.remove(part)
                    except Exception:
                        pass
            else:
                hd_caption = f"{caption}\n\n🎬 HD Converted Video"
                await tg_client.send_video(
                    chat_id=target_chat_clean,
                    video=temp_output_path,
                    caption=hd_caption,
                    supports_streaming=True,
                    thumb=thumb_path,
                    progress=pyrogram_progress_callback,
                    progress_args=(chat_id, status_msg_id, output_name, 1, 1, "HD Video")
                )
            sent = True
            print("[CONVERT] HD video sent successfully!")
        except Exception as e:
            print(f"[CONVERT] Pyrogram HD send failed: {e}")

    if not sent and converted_size <= 50 * 1024 * 1024:
        try:
            async with httpx.AsyncClient() as client:
                with open(temp_output_path, "rb") as f:
                    files = {"video": (output_name, f, "video/mp4")}
                    if thumb_path:
                        with open(thumb_path, "rb") as t:
                            files["thumb"] = (os.path.basename(thumb_path), t, "image/jpeg")
                    data = {
                        "chat_id": target_chat,
                        "caption": f"{caption}\n\n🎬 HD Converted Video",
                        "supports_streaming": "true"
                    }
                    r = await client.post(f"{TELEGRAM_API}/sendVideo", data=data, files=files, timeout=None)
                    if r.status_code == 200 and r.json().get("ok"):
                        sent = True
        except Exception as e:
            print(f"[CONVERT] HTTP HD send failed: {e}")

    # Final status
    if sent:
        done_text = (
            f"📊 **[ CAMLITE COMPLETE ]** 📊\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📹 Direct Video: Delivered ✅\n"
            f"🎬 HD Converted: Delivered ✅\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎉 **Dono video bhej diye gaye!**"
        )
    else:
        done_text = (
            f"📊 **[ CAMLITE COMPLETE ]** 📊\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📹 Direct Video: Delivered ✅\n"
            f"🎬 HD Converted: Failed ⚠️ (Direct video already sent)\n"
            f"━━━━━━━━━━━━━━━━━━━━━━"
        )
    await edit_status_throttled(chat_id, status_msg_id, done_text, force=True)

    # Cleanup
    for path in [temp_output_path, thumb_path]:
        try:
            if path and os.path.exists(path):
                os.remove(path)
        except Exception:
            pass
    for f in glob.glob("/tmp/part_*.mp4"):
        try:
            os.remove(f)
        except Exception:
            pass


# ============================================================
# MAIN PIPELINE: Fast Delivery + Background Convert
# ============================================================
async def process_and_upload_video(input_path: str, file_name: str, chat_id: str, target_chat: str, caption: str = ""):
    """
    NEW FLOW:
    1. Send original video to Telegram IMMEDIATELY (no waiting!)
    2. Then convert with FFMPEG in background
    3. Send HD converted version too
    """
    status_chat_clean = clean_chat_id(chat_id)

    # Get or create status message
    status_msg_id = progress_message_ids.get(chat_id)
    if not status_msg_id:
        status_msg_id = await tg_send_message(chat_id, "⚙️ Video server par aa gaya, bhej rahe hain...")
        if status_msg_id:
            progress_message_ids[chat_id] = status_msg_id

    # STEP 1: FAST DELIVERY - Send original video immediately
    fast_ok = await send_original_fast(input_path, file_name, chat_id, target_chat, caption, status_msg_id)

    if not fast_ok:
        await tg_send_message(chat_id, "❌ Direct video bhejne mein error aaya!")

    # STEP 2: BACKGROUND CONVERT - Convert and send HD version
    await background_convert_and_send(input_path, file_name, chat_id, target_chat, caption, status_msg_id)

    # Cleanup original input file
    try:
        if os.path.exists(input_path):
            os.remove(input_path)
    except Exception:
        pass

    # Clean progress state
    progress_message_ids.pop(chat_id, None)
    last_progress_edit.pop(f"{chat_id}_{status_msg_id}", None)
    last_edit_timestamps.pop(f"{chat_id}_{status_msg_id}", None)

    gc.collect()
    print("[PIPELINE] All done! Cleanup complete.")
    return True


# --- Queue Worker ---
async def queue_worker():
    global queued_tasks_count
    while True:
        try:
            item = await processing_queue.get()
            queued_tasks_count = max(0, queued_tasks_count - 1)

            input_path = item["input_path"]
            file_name = item["file_name"]
            chat_id = item["chat_id"]
            target_chat = item["target_chat"]
            caption = item["caption"]

            try:
                await process_and_upload_video(input_path, file_name, chat_id, target_chat, caption)
            except Exception as e:
                print(f"Error in queue worker: {e}")

            processing_queue.task_done()
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"Queue worker error: {e}")
            await asyncio.sleep(1)


# --- Webhook ---
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

    if text.startswith("/") and msg_id:
        asyncio.create_task(schedule_message_deletion(chat_id, msg_id, 5))

    if text == "/myid":
        await tg_send_message(chat_id, f"Tumhara chat ID: {chat_id}", auto_delete=True, delay=5)
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
        online = "Phone connected, ready" if connected_devices else "Phone offline"
        await tg_send_message(chat_id, online, auto_delete=True, delay=5)

    return {"ok": True}


async def dispatch_command(chat_id: str, cmd: str):
    if not connected_devices:
        await tg_send_message(chat_id, "Phone offline hai", auto_delete=True, delay=5)
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

    await tg_send_message(chat_id, label, auto_delete=True, delay=5)


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
                    print(f"WS JSON parse error: {e}")
            elif "bytes" in message:
                print("Received binary over WS (ignored)")

    except Exception as e:
        print(f"WebSocket exception: {e}")
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

        text = (
            f"📊 **[ CAMLITE UPLOAD STATUS ]** 📊\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📥 Phone → Server: [{bar}] {percent}%\n"
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
        pass


# --- Upload Endpoint ---
@app.post("/upload")
async def custom_upload(
    request: Request,
    chat_id: str = Form(None),
    caption: str = Form(""),
):
    global queued_tasks_count
    form_data = await request.form()

    file_field = None
    for key, value in form_data.multi_items():
        if hasattr(value, "filename") and value.filename:
            file_field = value
            break

    if not file_field:
        return {"ok": False, "description": "No file uploaded"}

    target_chat = chat_id or OWNER_CHAT_ID
    file_name = file_field.filename or "video.mp4"
    temp_input_path = f"/tmp/{file_name}"

    with open(temp_input_path, "wb") as buffer:
        shutil.copyfileobj(file_field.file, buffer)

    status_msg_id = progress_message_ids.get(OWNER_CHAT_ID)
    if not status_msg_id:
        status_msg_id = await tg_send_message(OWNER_CHAT_ID, "⚙️ Video server par aa gaya...")
        if status_msg_id:
            progress_message_ids[OWNER_CHAT_ID] = status_msg_id

    if queued_tasks_count > 0:
        status_text = (
            f"📊 **[ CAMLITE QUEUE ]** 📊\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏳ Position: #{queued_tasks_count}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━"
        )
        await edit_status_throttled(OWNER_CHAT_ID, status_msg_id, status_text, force=True)

    queued_tasks_count += 1

    item = {
        "input_path": temp_input_path,
        "file_name": file_name,
        "chat_id": OWNER_CHAT_ID,
        "target_chat": target_chat,
        "caption": caption
    }
    await processing_queue.put(item)
    return {"ok": True, "description": "Queued for fast delivery"}


# --- Telegram API Proxy ---
@app.post("/bot{token}/{method}")
async def telegram_api_proxy(token: str, method: str, request: Request):
    global queued_tasks_count

    if token != BOT_TOKEN:
        return {"ok": False, "description": "Unauthorized"}

    if method not in ("sendVideo", "sendDocument", "sendAudio"):
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

    try:
        form_data = await request.form()
    except ClientDisconnect:
        return {"ok": False, "description": "Client disconnected"}

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

    file_name = file_field.filename or "video.mp4"
    temp_input_path = f"/tmp/{file_name}"

    with open(temp_input_path, "wb") as buffer:
        shutil.copyfileobj(file_field.file, buffer)

    status_msg_id = progress_message_ids.get(OWNER_CHAT_ID)
    if not status_msg_id:
        status_msg_id = await tg_send_message(OWNER_CHAT_ID, "⚙️ Video server par aa gaya...")
        if status_msg_id:
            progress_message_ids[OWNER_CHAT_ID] = status_msg_id

    if queued_tasks_count > 0:
        status_text = (
            f"📊 **[ CAMLITE QUEUE ]** 📊\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏳ Position: #{queued_tasks_count}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━"
        )
        await edit_status_throttled(OWNER_CHAT_ID, status_msg_id, status_text, force=True)

    queued_tasks_count += 1

    item = {
        "input_path": temp_input_path,
        "file_name": file_name,
        "chat_id": OWNER_CHAT_ID,
        "target_chat": chat_id,
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
            "text": "File intercepted and queued for fast delivery"
        }
    }


@app.get("/")
async def root():
    return {"status": "ok", "devices_connected": len(connected_devices), "queued_tasks": queued_tasks_count}
