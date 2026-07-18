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

MAX_CONCURRENT_TASKS = 1
semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

ADMIN_IDS: Set[str] = {OWNER_CHAT_ID}


def is_admin(chat_id: str) -> bool:
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
            print(f"[TG] Pyrogram send_message failed: {e}")
    async with httpx.AsyncClient() as client:
        try:
            r = await client.post(f"{TELEGRAM_API}/sendMessage", data={"chat_id": chat_id, "text": text})
            data = r.json()
            if data.get("ok"):
                msg_id = data["result"]["message_id"]
        except Exception as e:
            print(f"[TG] HTTP sendMessage failed: {e}")
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
            print(f"[TG] Pyrogram delete failed: {e}")

    async with httpx.AsyncClient() as client:
        try:
            await client.post(f"{TELEGRAM_API}/deleteMessage", data={"chat_id": chat_id, "message_id": message_id})
            return True
        except Exception as e:
            print(f"[TG] HTTP delete failed: {e}")
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
            print(f"[TG] Pyrogram edit failed: {e}")

    async with httpx.AsyncClient() as client:
        try:
            await client.post(f"{TELEGRAM_API}/editMessageText", data={"chat_id": chat_id, "message_id": message_id, "text": text})
        except Exception as e:
            print(f"[TG] HTTP edit failed: {e}")


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
        except Exception as e:
            print(f"[STATUS] edit_status_throttled failed: {e}")


# ============================================================
# PHOTO UPLOAD
# ============================================================
async def upload_photo_to_telegram(file_path: str, file_name: str, chat_id: str, target_chat: str, caption: str = ""):
    global tg_client
    target_chat_clean = clean_chat_id(target_chat)

    status_msg_id = progress_message_ids.get(chat_id)
    if not status_msg_id:
        status_msg_id = await tg_send_message(chat_id, "📸 Photo uploading...")
        if status_msg_id:
            progress_message_ids[chat_id] = status_msg_id

    status_text = f"📸 **[ PHOTO DELIVERY ]** 📸\n━━━━━━━━━━━━━━━━━━━━━━\n📤 Uploading photo... ⏳\n━━━━━━━━━━━━━━━━━━━━━━"
    await edit_status_throttled(chat_id, status_msg_id, status_text, force=True)

    sent = False

    if pyrogram_available and tg_client and tg_client.is_connected:
        try:
            try:
                await tg_client.get_chat(target_chat_clean)
            except:
                pass
            await tg_client.send_photo(
                chat_id=target_chat_clean,
                photo=file_path,
                caption=caption
            )
            sent = True
            print(f"[PHOTO] Sent via Pyrogram: {file_name}")
        except Exception as e:
            print(f"[PHOTO] Pyrogram failed: {e}")

    if not sent:
        try:
            async with httpx.AsyncClient() as client:
                with open(file_path, "rb") as f:
                    files = {"photo": (file_name, f, "image/jpeg")}
                    data = {"chat_id": target_chat, "caption": caption}
                    r = await client.post(f"{TELEGRAM_API}/sendPhoto", data=data, files=files, timeout=60)
                    if r.status_code == 200 and r.json().get("ok", False):
                        sent = True
                        print(f"[PHOTO] Sent via HTTP: {file_name}")
                    else:
                        print(f"[PHOTO] HTTP bad response: {r.status_code} {r.text[:300]}")
        except Exception as e:
            print(f"[PHOTO] HTTP failed: {e}")

    if sent:
        done_text = f"📸 **[ PHOTO DELIVERED ]** 📸\n━━━━━━━━━━━━━━━━━━━━━━\n✅ Photo: {file_name}\n━━━━━━━━━━━━━━━━━━━━━━"
    else:
        done_text = f"📸 **[ PHOTO FAILED ]** 📸\n━━━━━━━━━━━━━━━━━━━━━━\n❌ Upload failed\n━━━━━━━━━━━━━━━━━━━━━━"

    await edit_status_throttled(chat_id, status_msg_id, done_text, force=True)

    try:
        if os.path.exists(file_path):
            os.remove(file_path)
    except:
        pass

    progress_message_ids.pop(chat_id, None)
    last_edit_timestamps.pop(f"{chat_id}_{status_msg_id}", None)
    gc.collect()

    return sent


# ============================================================
# VIDEO - DIRECT ONLY (NO HD CONVERT)
# ============================================================

async def get_video_duration(file_path: str) -> float:
    cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", file_path]
    try:
        process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, _ = await process.communicate()
        return float(stdout.strip())
    except:
        return 0.0


async def split_video(file_path: str, segment_duration: float) -> list:
    output_pattern = "/tmp/split_part_%03d.mp4"
    for f in glob.glob("/tmp/split_part_*.mp4"):
        try:
            os.remove(f)
        except:
            pass

    cmd = ["ffmpeg", "-y", "-threads", "1", "-i", file_path, "-c", "copy", "-map", "0",
           "-segment_time", str(segment_duration), "-f", "segment", "-reset_timestamps", "1", output_pattern]
    try:
        process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        await process.communicate()
        gc.collect()
        if process.returncode == 0:
            parts = sorted(glob.glob("/tmp/split_part_*.mp4"))
            if parts:
                return parts
        return [file_path]
    except Exception as e:
        print(f"[SPLIT] failed: {e}")
        return [file_path]


async def generate_thumbnail(video_path: str, thumb_path: str) -> bool:
    cmd = ["ffmpeg", "-y", "-threads", "1", "-ss", "00:00:02", "-i", video_path, "-vframes", "1", "-q:v", "5", thumb_path]
    try:
        process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        await process.wait()
        return process.returncode == 0
    except Exception as e:
        print(f"[THUMB] failed: {e}")
        return False


async def faststart_video(input_path: str, output_path: str) -> bool:
    """Sirf moov atom ko start mein relocate karta hai (metadata only, NO re-encode, NO quality loss).
    CameraX ke recorded video mein moov atom END mein hota hai jisse Telegram send_video fail karta hai."""
    cmd = [
        "ffmpeg", "-y", "-threads", "1",
        "-i", input_path,
        "-c", "copy",
        "-movflags", "+faststart",
        output_path
    ]
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await process.communicate()
        gc.collect()
        if process.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 1024:
            return True
        else:
            print(f"[FASTSTART] ffmpeg returned {process.returncode}: {stderr.decode(errors='ignore')[-300:]}")
            return False
    except Exception as e:
        print(f"[FASTSTART] failed: {e}")
        return False


async def progress_callback(current, total, chat_id, message_id, filename, current_part, total_parts, label="Uploading"):
    percent = (current / total) * 100 if total > 0 else 0
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
        print(f"[SEND] Pyrogram not available for {label}")
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
        print(f"[SEND] Pyrogram OK: {label} -> {video_path}")
        return True
    except Exception as e:
        print(f"[SEND] Pyrogram {label} failed: {e}")
        return False


async def safe_send_video_http(target_chat, video_path, caption, thumb_path, file_name):
    file_size = os.path.getsize(video_path)
    if file_size > 50 * 1024 * 1024:
        print(f"[SEND] File too large for HTTP ({file_size / (1024*1024):.1f}MB), need Pyrogram")
        return False

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(None, connect=30)) as client:
            with open(video_path, "rb") as f:
                files = {"video": (file_name, f, "video/mp4")}
                if thumb_path and os.path.exists(thumb_path):
                    with open(thumb_path, "rb") as t:
                        files["thumb"] = (os.path.basename(thumb_path), t, "image/jpeg")
                data = {"chat_id": target_chat, "caption": caption, "supports_streaming": "true"}
                r = await client.post(f"{TELEGRAM_API}/sendVideo", data=data, files=files)
                gc.collect()
                resp = r.json() if r.status_code == 200 else None
                ok = (r.status_code == 200 and resp and resp.get("ok", False))
                if ok:
                    print(f"[SEND] HTTP OK: {file_name}")
                else:
                    print(f"[SEND] HTTP failed {r.status_code}: {r.text[:300] if not r.status_code==200 else resp}")
                return ok
    except Exception as e:
        print(f"[SEND] HTTP exception: {e}")
        return False


async def send_video_direct(input_path, file_name, chat_id, target_chat, caption, status_msg_id, queue_position):
    """Sirf original direct video bhejo - koi HD convert nahi. Sirf faststart metadata fix (quality same)."""
    target_chat_clean = clean_chat_id(target_chat)
    file_size = os.path.getsize(input_path)
    limit_2gb = 2000 * 1024 * 1024
    base, _ = os.path.splitext(file_name)

    # Queue wait indicator
    if queue_position > 1:
        status_text = f"📊 **[ QUEUE ]** 📊\n━━━━━━━━━━━━━━━━━━━━━━\n⏳ Position: #{queue_position}\n📥 Received: Complete ✅\n📤 Waiting for turn...\n━━━━━━━━━━━━━━━━━━━━━━"
        await edit_status_throttled(chat_id, status_msg_id, status_text, force=True)
        await semaphore.acquire()
        semaphore.release()

    # Faststart metadata fix (no re-encode) taaki Telegram moov atom accept kare
    status_text = f"📊 **[ VIDEO DELIVERY ]** 📊\n━━━━━━━━━━━━━━━━━━━━━━\n📥 Received: Complete ✅\n⚙️ Preparing... ⏳\n━━━━━━━━━━━━━━━━━━━━━━"
    await edit_status_throttled(chat_id, status_msg_id, status_text, force=True)

    prepared_path = input_path
    temp_faststart_path = f"/tmp/{base}_fs.mp4"
    if os.path.exists(temp_faststart_path):
        try:
            os.remove(temp_faststart_path)
        except:
            pass
    faststart_ok = await faststart_video(input_path, temp_faststart_path)
    if faststart_ok and os.path.exists(temp_faststart_path) and os.path.getsize(temp_faststart_path) > 1024:
        prepared_path = temp_faststart_path
        print(f"[DELIVERY] Faststart ready: {os.path.getsize(prepared_path)} bytes")
    else:
        print("[DELIVERY] Faststart unavailable, sending raw file")
        # Cleanup failed temp
        if os.path.exists(temp_faststart_path):
            try:
                os.remove(temp_faststart_path)
            except:
                pass

    # Thumbnail prepared file se banao (faststart wali se) warna original se
    thumb_path = f"/tmp/{base}_thumb.jpg"
    has_thumb = await generate_thumbnail(prepared_path, thumb_path)
    if not has_thumb or not os.path.exists(thumb_path) or os.path.getsize(thumb_path) == 0:
        thumb_path = None

    prepared_size = os.path.getsize(prepared_path)
    status_text = f"📊 **[ VIDEO DELIVERY ]** 📊\n━━━━━━━━━━━━━━━━━━━━━━\n📤 Sending Video ({prepared_size/(1024*1024):.1f}MB)... ⏳\n━━━━━━━━━━━━━━━━━━━━━━"
    await edit_status_throttled(chat_id, status_msg_id, status_text, force=True)

    video_caption = caption if caption and caption.strip() else f"📹 {file_name}"
    sent = False

    if prepared_size > limit_2gb:
        status_text = f"📊 **[ VIDEO DELIVERY ]** 📊\n━━━━━━━━━━━━━━━━━━━━━━\n✂️ Splitting {prepared_size/(1024*1024*1024):.1f}GB... ⏳\n━━━━━━━━━━━━━━━━━━━━━━"
        await edit_status_throttled(chat_id, status_msg_id, status_text, force=True)
        duration = await get_video_duration(prepared_path)
        if duration <= 0:
            duration = 600.0
        num_parts = math.ceil(prepared_size / limit_2gb)
        segment_duration = duration / num_parts
        parts = await split_video(prepared_path, segment_duration)
        for i, part in enumerate(parts):
            part_name = os.path.basename(part)
            part_caption = f"{video_caption}\n\n🎬 Part {i+1}/{len(parts)}"
            ok = await safe_send_video(target_chat_clean, part, part_caption, thumb_path,
                                       (chat_id, status_msg_id, part_name, i+1, len(parts), "Video"),
                                       chat_id, status_msg_id, "Video")
            if not ok:
                ok = await safe_send_video_http(target_chat, part, part_caption, thumb_path, part_name)
            if ok:
                sent = True
            else:
                print(f"[DELIVERY] Part {i+1}/{len(parts)} FAILED")
            if part != input_path and part != prepared_path:
                try:
                    os.remove(part)
                except:
                    pass
    else:
        ok = await safe_send_video(target_chat_clean, prepared_path, video_caption, thumb_path,
                                   (chat_id, status_msg_id, file_name, 1, 1, "Video"),
                                   chat_id, status_msg_id, "Video")
        if not ok:
            ok = await safe_send_video_http(target_chat, prepared_path, video_caption, thumb_path, file_name)
        sent = ok

    # Cleanup
    for path in [thumb_path, temp_faststart_path]:
        if path and os.path.exists(path):
            try:
                os.remove(path)
            except:
                pass

    if sent:
        done_text = f"📊 **[ COMPLETE ]** 📊\n━━━━━━━━━━━━━━━━━━━━━━\n✅ Video Delivered\n━━━━━━━━━━━━━━━━━━━━━━\n🎉 Done!"
    else:
        done_text = f"📊 **[ COMPLETE ]** 📊\n━━━━━━━━━━━━━━━━━━━━━━\n❌ Upload Failed (see server logs)\n━━━━━━━━━━━━━━━━━━━━━━"
    await edit_status_throttled(chat_id, status_msg_id, done_text, force=True)

    return sent


async def process_and_upload_video(input_path, file_name, chat_id, target_chat, caption="", queue_position=1):
    async with semaphore:
        status_msg_id = progress_message_ids.get(chat_id)
        if not status_msg_id:
            status_msg_id = await tg_send_message(chat_id, "⚙️ Processing...")
            if status_msg_id:
                progress_message_ids[chat_id] = status_msg_id

        print(f"[PIPELINE] Processing video #{queue_position}: {file_name} size={os.path.getsize(input_path) if os.path.exists(input_path) else 'MISSING'}")

        try:
            await send_video_direct(input_path, file_name, chat_id, target_chat, caption, status_msg_id, queue_position)
        except Exception as e:
            print(f"[PIPELINE] Video pipeline crashed: {e}")
            try:
                fail_text = f"📊 **[ COMPLETE ]** 📊\n━━━━━━━━━━━━━━━━━━━━━━\n❌ Upload Failed: {str(e)[:80]}\n━━━━━━━━━━━━━━━━━━━━━━"
                await edit_status_throttled(chat_id, status_msg_id, fail_text, force=True)
            except:
                pass

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
        for f in glob.glob("/tmp/*_fs.mp4"):
            try:
                os.remove(f)
            except:
                pass
        for f in glob.glob("/tmp/*_thumb.jpg"):
            try:
                os.remove(f)
            except:
                pass
        progress_message_ids.pop(chat_id, None)
        last_edit_timestamps.pop(f"{chat_id}_{status_msg_id}", None)
        gc.collect()

        print(f"[PIPELINE] Video #{queue_position} done")


async def queue_worker():
    global queued_tasks_count
    while True:
        try:
            item = await processing_queue.get()
            queued_tasks_count = max(0, queued_tasks_count - 1)
            queue_position = item.get("queue_position", 1)

            if item.get("type") == "photo":
                await upload_photo_to_telegram(item["input_path"], item["file_name"], item["chat_id"], item["target_chat"], item["caption"])
            else:
                await process_and_upload_video(item["input_path"], item["file_name"], item["chat_id"], item["target_chat"], item["caption"], queue_position)

            processing_queue.task_done()
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[QUEUE] Worker error: {e}")
            await asyncio.sleep(1)


# ============================================================
# WEBHOOK
# ============================================================
@app.post("/webhook/{token}")
async def telegram_webhook(token: str, request: Request):
    if token != BOT_TOKEN and token != WEBHOOK_SECRET:
        return {"ok": False}
    update = await request.json()
    message = update.get("message")
    if not message:
        return {"ok": True}

    chat_id = str(message["chat"]["id"])
    text = message.get("text", "").strip()
    msg_id = message.get("message_id")

    if not is_admin(chat_id):
        await tg_send_message(
            chat_id,
            "🚫 **Access Denied**\n\nYou are not authorized to use this bot.\nOnly the owner can control this security camera.",
            auto_delete=True,
            delay=10
        )
        print(f"[SECURITY] Blocked non-admin: {chat_id}")
        return {"ok": True}

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
            "/photo - Take a photo 📸\n"
            "/status - Check phone connection\n"
            "/myid - Show your chat ID\n"
            "/help - Show this help\n\n"
            "✅ You are authorized admin"
        )
        await tg_send_message(chat_id, help_text, auto_delete=True, delay=30)
        return {"ok": True}

    cmd = text.lower()
    if cmd in ("/on", "/startcam", "/start_rec"):
        await dispatch_command(chat_id, "start")
    elif cmd in ("/off", "/stopcam", "/stop_rec"):
        await dispatch_command(chat_id, "stop")
    elif cmd in ("/switchcam", "/switch", "/cam"):
        await dispatch_command(chat_id, "switch")
    elif cmd == "/photo":
        await dispatch_command(chat_id, "photo")
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
        except Exception as e:
            print(f"[WS] dispatch failed: {e}")
    labels = {
        "start": "Recording ON",
        "stop": "Recording OFF",
        "switch": "Camera switch",
        "photo": "📸 Taking photo..."
    }
    await tg_send_message(chat_id, f"Command: {labels.get(cmd, cmd)}", auto_delete=True, delay=5)


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    token = websocket.query_params.get("token")
    if token != DEVICE_TOKEN:
        await websocket.close(code=4401)
        return
    await websocket.accept()
    connected_devices[token] = websocket
    print(f"[WS] Device connected: {token}")
    try:
        while True:
            message = await websocket.receive()
            if message.get("type") == "websocket.disconnect":
                break
            if "text" in message:
                try:
                    data = json.loads(message["text"])
                    await handle_device_event(data)
                except Exception as e:
                    print(f"[WS] event error: {e}")
    except Exception as e:
        print(f"[WS] connection error: {e}")
    finally:
        connected_devices.pop(token, None)
        print(f"[WS] Device disconnected: {token}")


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
async def custom_upload(
    request: Request,
    chat_id: str = Form(None),
    caption: str = Form(""),
    file_type: str = Form("video")
):
    global queued_tasks_count
    try:
        form_data = await request.form()
    except Exception as e:
        print(f"[UPLOAD] form parse failed: {e}")
        return {"ok": False, "error": str(e)}

    file_field = None
    for key, value in form_data.multi_items():
        if hasattr(value, "filename") and value.filename:
            file_field = value
            break
    if not file_field:
        return {"ok": False, "error": "no file"}

    target_chat = chat_id or OWNER_CHAT_ID
    file_name = file_field.filename or ("photo.jpg" if file_type == "photo" else "video.mp4")
    # Sanitize filename taaki path issues na aaye
    file_name = re.sub(r'[^A-Za-z0-9._-]+', '_', file_name)
    temp_input_path = f"/tmp/{int(time.time()*1000)}_{file_name}"

    try:
        with open(temp_input_path, "wb") as buffer:
            shutil.copyfileobj(file_field.file, buffer)
        size = os.path.getsize(temp_input_path)
        print(f"[UPLOAD] Received: {file_name} ({size} bytes) type={file_type} target={target_chat}")
    except Exception as e:
        print(f"[UPLOAD] save failed: {e}")
        return {"ok": False, "error": str(e)}

    queued_tasks_count += 1
    queue_position = queued_tasks_count

    status_msg_id = progress_message_ids.get(OWNER_CHAT_ID)
    if not status_msg_id:
        emoji = "📸" if file_type == "photo" else "⚙️"
        status_msg_id = await tg_send_message(OWNER_CHAT_ID, f"{emoji} Queued #{queue_position}...")
        if status_msg_id:
            progress_message_ids[OWNER_CHAT_ID] = status_msg_id

    await processing_queue.put({
        "input_path": temp_input_path,
        "file_name": file_name,
        "chat_id": OWNER_CHAT_ID,
        "target_chat": target_chat,
        "caption": caption,
        "queue_position": queue_position,
        "type": file_type
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
    except Exception as e:
        print(f"[PROXY] form parse failed: {e}")
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
    file_name = re.sub(r'[^A-Za-z0-9._-]+', '_', file_name)
    temp_input_path = f"/tmp/{int(time.time()*1000)}_{file_name}"
    try:
        with open(temp_input_path, "wb") as buffer:
            shutil.copyfileobj(file_field.file, buffer)
        print(f"[PROXY] Received: {file_name} ({os.path.getsize(temp_input_path)} bytes) method={method}")
    except Exception as e:
        print(f"[PROXY] save failed: {e}")
        return {"ok": False}

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
        "queue_position": queue_position,
        "type": "video"
    })

    return {"ok": True, "result": {"message_id": 99999, "chat": {"id": int(chat_id) if str(chat_id).replace("-", "").isdigit() else 0, "type": "private"}, "date": int(time.time()), "text": f"Queued #{queue_position}"}}


@app.get("/")
async def root():
    return {"status": "ok", "devices": len(connected_devices), "queue": queued_tasks_count, "admin_only": True}
