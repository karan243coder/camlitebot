import os
import time
import asyncio
import math
import shutil
import glob
import json
import gc
from typing import Dict, Union, Set

import httpx
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, Form
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
STARTUP_PRINTED = False

def is_admin(chat_id: str) -> bool:
    return str(chat_id) in ADMIN_IDS

@asynccontextmanager
async def lifespan(app: FastAPI):
    global tg_client, STARTUP_PRINTED
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
            print("✓ Pyrogram started (50MB - 2GB)")
        except Exception as e:
            print(f"✗ Pyrogram failed: {e}")
            tg_client = None
    else:
        print("⚠ Pyrogram disabled")

    worker_task = asyncio.create_task(queue_worker())
    print("✓ Queue worker started")
    print("✓ Admin: " + OWNER_CHAT_ID)
    print("=" * 60)
    print("🚀 SERVER STARTED IN DIRECT-ONLY MODE")
    print("🚀 NO HD CONVERTER - ONLY ONE VIDEO PER RECORDING")
    print("🚀 NO 'HD: Skipped' MESSAGE")
    if pyrogram_available and tg_client and tg_client.is_connected:
        print("✅ PYROGRAM CONNECTED — Large files up to 2GB supported")
    else:
        print("⚠️  ⚠️  ⚠️  PYROGRAM NOT WORKING — FILES OVER 50MB WILL FAIL!  ⚠️  ⚠️  ⚠️")
        print("    Fix: Set API_ID + API_HASH env vars in Koyeb correctly")
    print("=" * 60)
    STARTUP_PRINTED = True

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
        if any(ch.isalpha() for ch in c):
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
            print(f"[TG] pyro send_message fail: {e}")
    async with httpx.AsyncClient() as client:
        try:
            r = await client.post(f"{TELEGRAM_API}/sendMessage", data={"chat_id": chat_id, "text": text})
            d = r.json()
            if d.get("ok"):
                msg_id = d["result"]["message_id"]
        except Exception as e:
            print(f"[TG] http sendMessage fail: {e}")
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
            print(f"[TG] pyro edit fail: {e}")
    async with httpx.AsyncClient() as client:
        try:
            await client.post(f"{TELEGRAM_API}/editMessageText", data={"chat_id": chat_id, "message_id": message_id, "text": text})
        except Exception as e:
            print(f"[TG] http edit fail: {e}")

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
            print(f"[STATUS] edit fail: {e}")

# ============ PHOTO ============
async def upload_photo_to_telegram(file_path: str, file_name: str, chat_id: str, target_chat: str, caption: str = ""):
    global tg_client
    target_clean = clean_chat_id(target_chat)
    mid = progress_message_ids.get(chat_id)
    if not mid:
        mid = await tg_send_message(chat_id, "📸 Photo uploading...")
        if mid:
            progress_message_ids[chat_id] = mid

    await edit_status_throttled(chat_id, mid,
        f"📸 **[ PHOTO DELIVERY ]** 📸\n━━━━━━━━━━━━━━━━━━━━━━\n📤 Uploading photo... ⏳\n━━━━━━━━━━━━━━━━━━━━━━", force=True)
    sent = False

    if pyrogram_available and tg_client and tg_client.is_connected:
        try:
            try: await tg_client.get_chat(target_clean)
            except: pass
            await tg_client.send_photo(chat_id=target_clean, photo=file_path, caption=caption)
            sent = True
            print(f"[PHOTO] pyro ok: {file_name}")
        except Exception as e:
            print(f"[PHOTO] pyro fail: {e}")

    if not sent:
        try:
            async with httpx.AsyncClient() as client:
                with open(file_path, "rb") as f:
                    files = {"photo": (file_name, f, "image/jpeg")}
                    data = {"chat_id": target_chat, "caption": caption}
                    r = await client.post(f"{TELEGRAM_API}/sendPhoto", data=data, files=files, timeout=60)
                    if r.status_code == 200 and r.json().get("ok", False):
                        sent = True
                        print(f"[PHOTO] http ok: {file_name}")
                    else:
                        print(f"[PHOTO] http fail {r.status_code}: {r.text[:200]}")
        except Exception as e:
            print(f"[PHOTO] http ex: {e}")

    done = (f"📸 **[ PHOTO DELIVERED ]** 📸\n━━━━━━━━━━━━━━━━━━━━━━\n✅ Photo: {file_name}\n━━━━━━━━━━━━━━━━━━━━━━"
            if sent else
            f"📸 **[ PHOTO FAILED ]** 📸\n━━━━━━━━━━━━━━━━━━━━━━\n❌ Upload failed\n━━━━━━━━━━━━━━━━━━━━━━")
    await edit_status_throttled(chat_id, mid, done, force=True)
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
    except: pass
    progress_message_ids.pop(chat_id, None)
    last_edit_timestamps.pop(f"{chat_id}_{mid}", None)
    gc.collect()
    return sent

# ============ VIDEO (DIRECT ONLY, NO HD) ============
async def get_video_duration(file_path: str) -> float:
    cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", file_path]
    try:
        p = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        out, _ = await p.communicate()
        return float(out.strip())
    except:
        return 0.0

async def split_video(file_path: str, segment_duration: float) -> list:
    pat = "/tmp/split_part_%03d.mp4"
    for f in glob.glob("/tmp/split_part_*.mp4"):
        try: os.remove(f)
        except: pass
    cmd = ["ffmpeg", "-y", "-threads", "1", "-i", file_path, "-c", "copy", "-map", "0",
           "-segment_time", str(segment_duration), "-f", "segment", "-reset_timestamps", "1", pat]
    try:
        p = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        await p.communicate()
        gc.collect()
        if p.returncode == 0:
            parts = sorted(glob.glob("/tmp/split_part_*.mp4"))
            if parts: return parts
        return [file_path]
    except Exception as e:
        print(f"[SPLIT] fail: {e}")
        return [file_path]

async def generate_thumbnail(video_path: str, thumb_path: str) -> bool:
    cmd = ["ffmpeg", "-y", "-threads", "1", "-ss", "00:00:02", "-i", video_path, "-vframes", "1", "-q:v", "5", thumb_path]
    try:
        p = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        await p.wait()
        return p.returncode == 0
    except Exception as e:
        print(f"[THUMB] fail: {e}")
        return False

async def faststart_video(input_path: str, output_path: str) -> bool:
    """Metadata-only moov relocate (no re-encode, no quality loss)."""
    cmd = ["ffmpeg", "-y", "-threads", "1", "-i", input_path, "-c", "copy", "-movflags", "+faststart", output_path]
    try:
        p = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
        _, err = await p.communicate()
        if p.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 1024:
            return True
        print(f"[FASTSTART] ffmpeg code={p.returncode}: {err.decode(errors='ignore')[-200:]}")
        return False
    except Exception as e:
        print(f"[FASTSTART] fail: {e}")
        return False

async def progress_callback(current, total, chat_id, message_id, filename, current_part, total_parts, label="Uploading"):
    percent = (current / total) * 100 if total > 0 else 0
    bar = make_progress_bar(percent)
    cmb = current / (1024*1024)
    tmb = total / (1024*1024)
    line = (f"📤 {label} Part {current_part}/{total_parts}: [{bar}] {percent:.1f}%\nℹ️ {cmb:.1f}MB / {tmb:.1f}MB"
            if total_parts > 1 else
            f"📤 {label}: [{bar}] {percent:.1f}%\nℹ️ {cmb:.1f}MB / {tmb:.1f}MB")
    txt = f"📊 **[ STATUS ]** 📊\n━━━━━━━━━━━━━━━━━━━━━━\n{line}\n━━━━━━━━━━━━━━━━━━━━━━"
    await edit_status_throttled(chat_id, message_id, txt, force=(current == total))

async def try_pyro_send(target_clean, path, caption, thumb, pargs, label) -> bool:
    global tg_client
    if not (pyrogram_available and tg_client and tg_client.is_connected):
        return False
    try:
        try: await tg_client.get_chat(target_clean)
        except: pass
        kw = {"chat_id": target_clean, "video": path, "caption": caption,
              "supports_streaming": True, "progress": progress_callback, "progress_args": pargs}
        if thumb and os.path.exists(thumb):
            kw["thumb"] = thumb
        await tg_client.send_video(**kw)
        gc.collect()
        print(f"[SEND] pyro ok: {label}")
        return True
    except Exception as e:
        print(f"[SEND] pyro {label} fail: {e}")
        return False

async def try_http_send(target, path, caption, thumb, fname) -> bool:
    sz = os.path.getsize(path)
    if sz > 50*1024*1024:
        print(f"[SEND] too big for http ({sz/(1024*1024):.1f}MB)")
        return False
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(None, connect=30)) as client:
            # Build files dict properly — video ALWAYS attached, thumb optional
            fvideo = open(path, "rb")
            try:
                files = {"video": (fname, fvideo, "video/mp4")}
                fthumb = None
                if thumb and os.path.exists(thumb):
                    try:
                        fthumb = open(thumb, "rb")
                        files["thumb"] = (os.path.basename(thumb), fthumb, "image/jpeg")
                    except:
                        fthumb = None
                try:
                    data = {"chat_id": target, "caption": caption, "supports_streaming": "true"}
                    r = await client.post(f"{TELEGRAM_API}/sendVideo", data=data, files=files)
                finally:
                    if fthumb:
                        try: fthumb.close()
                        except: pass
                gc.collect()
                if r.status_code == 200 and r.json().get("ok", False):
                    print(f"[SEND] http ok: {fname}")
                    return True
                print(f"[SEND] http fail {r.status_code}: {r.text[:300]}")
                return False
            finally:
                try: fvideo.close()
                except: pass
    except Exception as e:
        print(f"[SEND] http ex: {e}")
        return False

async def do_send_one_video(video_path: str, file_name: str, chat_id: str, target_chat: str, caption: str,
                            status_msg_id: int, label_name: str) -> bool:
    """Attempt to send a single video file. Returns True on success."""
    target_clean = clean_chat_id(target_chat)
    sz = os.path.getsize(video_path)
    base, _ = os.path.splitext(file_name)
    thumb = f"/tmp/{base}_th.jpg"
    await generate_thumbnail(video_path, thumb)
    if not os.path.exists(thumb) or os.path.getsize(thumb) == 0:
        thumb = None

    # 50MB+ files require pyrogram (HTTP API hard-limit)
    use_pyro_first = bool(pyrogram_available and tg_client and tg_client.is_connected)
    sent = False

    if sz > 50*1024*1024 and not use_pyro_first:
        await edit_status_throttled(chat_id, status_msg_id,
            f"📊 **[ ERROR ]** 📊\n━━━━━━━━━━━━━━━━━━━━━━\n❌ File {sz/(1024*1024):.1f}MB hai (>50MB)\n⚠️ Pyrogram chalu nahi hai (API_ID/API_HASH missing)\nKoyeb env vars check karo!\n━━━━━━━━━━━━━━━━━━━━━━", force=True)
        if thumb and os.path.exists(thumb):
            try: os.remove(thumb)
            except: pass
        return False

    if sz > 2000*1024*1024:
        dur = await get_video_duration(video_path) or 600.0
        n = math.ceil(sz / (2000*1024*1024))
        parts = await split_video(video_path, dur / n)
        for i, part in enumerate(parts):
            pn = os.path.basename(part)
            pc = f"{caption}\n\n🎬 Part {i+1}/{len(parts)}" if caption else f"🎬 Part {i+1}/{len(parts)}"
            ok = await try_pyro_send(target_clean, part, pc, thumb,
                                     (chat_id, status_msg_id, pn, i+1, len(parts), label_name), f"{label_name} p{i+1}")
            if not ok:
                ok = await try_http_send(target_chat, part, pc, thumb, pn)
            if ok:
                sent = True
            if part != video_path:
                try: os.remove(part)
                except: pass
    else:
        # Pyrogram se pehle try karo (50MB+ ke liye zaroori, chhote pe bhi reliable)
        if use_pyro_first:
            ok = await try_pyro_send(target_clean, video_path, caption, thumb,
                                     (chat_id, status_msg_id, file_name, 1, 1, label_name), label_name)
            if not ok and sz <= 50*1024*1024:
                ok = await try_http_send(target_chat, video_path, caption, thumb, file_name)
        else:
            ok = await try_http_send(target_chat, video_path, caption, thumb, file_name)
        sent = ok

    if thumb and os.path.exists(thumb):
        try: os.remove(thumb)
        except: pass
    return sent

async def deliver_direct_video(input_path: str, file_name: str, chat_id: str, target_chat: str, caption: str,
                                status_msg_id: int, queue_position: int) -> bool:
    """DELIVER EXACTLY ONE VIDEO. No HD. Tries faststart version first, falls back to raw."""
    base, _ = os.path.splitext(file_name)

    if queue_position > 1:
        await edit_status_throttled(chat_id, status_msg_id,
            f"📊 **[ QUEUE ]** 📊\n━━━━━━━━━━━━━━━━━━━━━━\n⏳ Position: #{queue_position}\n📥 Received: Complete ✅\n📤 Waiting for turn...\n━━━━━━━━━━━━━━━━━━━━━━", force=True)
        await semaphore.acquire()
        semaphore.release()

    await edit_status_throttled(chat_id, status_msg_id,
        f"📊 **[ VIDEO DELIVERY ]** 📊\n━━━━━━━━━━━━━━━━━━━━━━\n📥 Received: Complete ✅\n📤 Preparing... ⏳\n━━━━━━━━━━━━━━━━━━━━━━", force=True)

    # Step 1: Try faststart (moov at front — Telegram prefers this for streaming)
    fs_path = f"/tmp/{base}_fs.mp4"
    for old in (fs_path,):
        try:
            if os.path.exists(old): os.remove(old)
        except: pass
    fs_ok = await faststart_video(input_path, fs_path)

    prepared = input_path
    used_fs = False
    if fs_ok and os.path.exists(fs_path) and os.path.getsize(fs_path) > 1024:
        prepared = fs_path
        used_fs = True
        print(f"[DELIVERY] using faststart copy: {os.path.getsize(prepared)} bytes")
    else:
        if os.path.exists(fs_path):
            try: os.remove(fs_path)
            except: pass
        print("[DELIVERY] faststart unavailable, will try raw")

    await edit_status_throttled(chat_id, status_msg_id,
        f"📊 **[ VIDEO DELIVERY ]** 📊\n━━━━━━━━━━━━━━━━━━━━━━\n📤 Sending Video ({os.path.getsize(prepared)/(1024*1024):.1f}MB)... ⏳\n━━━━━━━━━━━━━━━━━━━━━━", force=True)

    cap = caption if caption and caption.strip() else f"📹 {file_name}"
    sent = await do_send_one_video(prepared, file_name, chat_id, target_chat, cap, status_msg_id, "Video")

    # Fallback: if faststart version failed, try raw original
    if not sent and used_fs:
        print("[DELIVERY] faststart version failed, retrying with raw file")
        await edit_status_throttled(chat_id, status_msg_id,
            f"📊 **[ VIDEO DELIVERY ]** 📊\n━━━━━━━━━━━━━━━━━━━━━━\n🔄 Retrying with raw file... ⏳\n━━━━━━━━━━━━━━━━━━━━━━", force=True)
        sent = await do_send_one_video(input_path, file_name, chat_id, target_chat, cap, status_msg_id, "Video-retry")

    # Cleanup temp fs file
    if os.path.exists(fs_path):
        try: os.remove(fs_path)
        except: pass

    if sent:
        done = "📊 **[ COMPLETE ]** 📊\n━━━━━━━━━━━━━━━━━━━━━━\n📹 Direct: Delivered ✅\n━━━━━━━━━━━━━━━━━━━━━━\n✅ Done!"
    else:
        done = "📊 **[ COMPLETE ]** 📊\n━━━━━━━━━━━━━━━━━━━━━━\n❌ Upload Failed (check Koyeb logs)\n━━━━━━━━━━━━━━━━━━━━━━"
    await edit_status_throttled(chat_id, status_msg_id, done, force=True)
    return sent

async def process_and_upload_video(input_path, file_name, chat_id, target_chat, caption="", queue_position=1):
    async with semaphore:
        mid = progress_message_ids.get(chat_id)
        if not mid:
            mid = await tg_send_message(chat_id, "⚙️ Processing...")
            if mid: progress_message_ids[chat_id] = mid
        print(f"[PIPELINE] video #{queue_position}: {file_name} size={os.path.getsize(input_path) if os.path.exists(input_path) else 'MISSING'}")
        try:
            await deliver_direct_video(input_path, file_name, chat_id, target_chat, caption, mid, queue_position)
        except Exception as e:
            print(f"[PIPELINE] crash: {e}")
            try:
                await edit_status_throttled(chat_id, mid,
                    f"📊 **[ COMPLETE ]** 📊\n━━━━━━━━━━━━━━━━━━━━━━\n❌ Error: {str(e)[:80]}\n━━━━━━━━━━━━━━━━━━━━━━", force=True)
            except: pass
        try:
            if os.path.exists(input_path): os.remove(input_path)
        except: pass
        for pat in ("/tmp/split_part_*.mp4", "/tmp/*_fs.mp4", "/tmp/*_th.jpg"):
            for f in glob.glob(pat):
                try: os.remove(f)
                except: pass
        progress_message_ids.pop(chat_id, None)
        last_edit_timestamps.pop(f"{chat_id}_{mid}", None)
        gc.collect()
        print(f"[PIPELINE] video #{queue_position} done")

async def queue_worker():
    global queued_tasks_count
    while True:
        try:
            item = await processing_queue.get()
            queued_tasks_count = max(0, queued_tasks_count - 1)
            qp = item.get("queue_position", 1)
            if item.get("type") == "photo":
                await upload_photo_to_telegram(item["input_path"], item["file_name"], item["chat_id"], item["target_chat"], item["caption"])
            else:
                await process_and_upload_video(item["input_path"], item["file_name"], item["chat_id"], item["target_chat"], item["caption"], qp)
            processing_queue.task_done()
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[QUEUE] error: {e}")
            await asyncio.sleep(1)

# ============ WEBHOOK ============
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
        await tg_send_message(chat_id,
            "🚫 **Access Denied**\n\nOnly the owner can control this camera.",
            auto_delete=True, delay=10)
        return {"ok": True}

    if text.startswith("/") and msg_id:
        asyncio.create_task(schedule_message_deletion(chat_id, msg_id, 5))

    if text == "/myid":
        await tg_send_message(chat_id, f"✅ Your Chat ID: {chat_id}\n\nYou are ADMIN ✓", auto_delete=True, delay=10)
        return {"ok": True}
    if text == "/help":
        ht = ("🔒 **Security Cam Bot**\n\n"
              "/on - Start recording\n"
              "/off - Stop recording\n"
              "/switch - Switch camera\n"
              "/photo - Take photo 📸\n"
              "/status - Phone status\n"
              "/myid - Chat ID\n"
              "/help - This help\n\n"
              "✅ Authorized")
        await tg_send_message(chat_id, ht, auto_delete=True, delay=30)
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
        await tg_send_message(chat_id, "Phone connected" if connected_devices else "Phone offline", auto_delete=True, delay=5)

    return {"ok": True}

async def dispatch_command(chat_id: str, cmd: str):
    if not connected_devices:
        await tg_send_message(chat_id, "Phone offline", auto_delete=True, delay=5)
        return
    for ws in list(connected_devices.values()):
        try: await ws.send_json({"cmd": cmd, "chat_id": chat_id})
        except: pass
    labels = {"start":"Recording ON","stop":"Recording OFF","switch":"Camera switch","photo":"📸 Taking photo..."}
    await tg_send_message(chat_id, f"Command: {labels.get(cmd, cmd)}", auto_delete=True, delay=5)

@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    token = websocket.query_params.get("token")
    if token != DEVICE_TOKEN:
        await websocket.close(code=4401)
        return
    await websocket.accept()
    connected_devices[token] = websocket
    print(f"[WS] + {token}")
    try:
        while True:
            m = await websocket.receive()
            if m.get("type") == "websocket.disconnect": break
            if "text" in m:
                try:
                    data = json.loads(m["text"])
                    await handle_device_event(data)
                except Exception as e:
                    print(f"[WS] event err: {e}")
    except Exception as e:
        print(f"[WS] conn err: {e}")
    finally:
        connected_devices.pop(token, None)
        print(f"[WS] - {token}")

async def handle_device_event(data: dict):
    cid = OWNER_CHAT_ID
    ev = data.get("event")
    if ev == "status":
        t = data.get("text", "")
        m = progress_message_ids.get(cid)
        if m: await tg_edit_message(cid, m, t)
        else:
            nm = await tg_send_message(cid, t)
            if nm: progress_message_ids[cid] = nm
    elif ev == "progress":
        pct = int(data.get("percent", 0))
        bar = make_progress_bar(pct)
        t = f"📊 **[ UPLOAD ]** 📊\n━━━━━━━━━━━━━━━━━━━━━━\n📥 Phone → Server: [{bar}] {pct}%\n━━━━━━━━━━━━━━━━━━━━━━"
        now = time.time()
        if now - last_edit_time.get(cid, 0) < 2 and pct < 100:
            return
        last_edit_time[cid] = now
        m = progress_message_ids.get(cid)
        if m: await tg_edit_message(cid, m, t)
        else:
            nm = await tg_send_message(cid, t)
            if nm: progress_message_ids[cid] = nm
    elif ev == "done":
        progress_message_ids.pop(cid, None)

@app.post("/upload")
async def custom_upload(request: Request, chat_id: str = Form(None), caption: str = Form(""), file_type: str = Form("video")):
    global queued_tasks_count
    try: form = await request.form()
    except Exception as e:
        print(f"[UPLOAD] form err: {e}")
        return {"ok": False}
    ff = None
    for k, v in form.multi_items():
        if hasattr(v, "filename") and v.filename:
            ff = v; break
    if not ff: return {"ok": False}
    target = chat_id or OWNER_CHAT_ID
    fn = ff.filename or ("photo.jpg" if file_type == "photo" else "video.mp4")
    safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in fn)
    tmp = f"/tmp/{int(time.time()*1000)}_{safe}"
    with open(tmp, "wb") as b: shutil.copyfileobj(ff.file, b)
    sz = os.path.getsize(tmp) if os.path.exists(tmp) else 0
    print(f"[UPLOAD] {safe} ({sz} bytes = {sz/(1024*1024):.1f}MB) type={file_type} target={target}")

    # ⚠️  Pyrogram nahi hai aur file 50MB se upar hai → pehle hi bata do ki fail karega
    if not (pyrogram_available and tg_client and tg_client.is_connected) and sz > 45*1024*1024:
        try: os.remove(tmp)
        except: pass
        msg = (f"❌ File {sz/(1024*1024):.1f}MB hai (>50MB). Pyrogram chalu nahi hai isliye upload nahi ho payega.\n"
               f"Koyeb me API_ID + API_HASH env vars sahi se daalo.")
        await tg_send_message(OWNER_CHAT_ID, msg)
        return {"ok": False, "error": "pyrogram_required", "size": sz}

    queued_tasks_count += 1; qp = queued_tasks_count
    mid = progress_message_ids.get(OWNER_CHAT_ID)
    if not mid:
        em = "📸" if file_type == "photo" else "⚙️"
        mid = await tg_send_message(OWNER_CHAT_ID, f"{em} Queued #{qp} ({sz/(1024*1024):.1f}MB)...")
        if mid: progress_message_ids[OWNER_CHAT_ID] = mid
    await processing_queue.put({"input_path":tmp,"file_name":safe,"chat_id":OWNER_CHAT_ID,"target_chat":target,
                               "caption":caption,"queue_position":qp,"type":file_type})
    return {"ok": True, "queue_position": qp, "size": sz}

@app.post("/bot{token}/{method}")
async def telegram_api_proxy(token: str, method: str, request: Request):
    global queued_tasks_count
    if token != BOT_TOKEN: return {"ok": False}
    if method not in ("sendVideo","sendDocument","sendAudio"):
        async with httpx.AsyncClient() as client:
            headers = {k:v for k,v in request.headers.items() if k.lower() not in ("host","content-length")}
            body = await request.body()
            r = await client.post(f"https://api.telegram.org/bot{token}/{method}", headers=headers, content=body, params=dict(request.query_params))
            try: return r.json()
            except: return r.text
    try: form = await request.form()
    except ClientDisconnect: return {"ok": False}
    except Exception as e:
        print(f"[PROXY] form err: {e}"); return {"ok": False}
    cid = form.get("chat_id") or OWNER_CHAT_ID
    cap = form.get("caption") or ""
    ff = None
    for k, v in form.multi_items():
        if hasattr(v, "filename") and v.filename:
            ff = v; break
    if not ff:
        async with httpx.AsyncClient() as client:
            headers = {k:v for k,v in request.headers.items() if k.lower() not in ("host","content-length","content-type")}
            r = await client.post(f"https://api.telegram.org/bot{token}/{method}", headers=headers, data=dict(form), params=dict(request.query_params))
            try: return r.json()
            except: return r.text
    fn = ff.filename or "video.mp4"
    safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in fn)
    tmp = f"/tmp/{int(time.time()*1000)}_{safe}"
    with open(tmp, "wb") as b: shutil.copyfileobj(ff.file, b)
    print(f"[PROXY] {safe} ({os.path.getsize(tmp)} bytes) method={method}")
    queued_tasks_count += 1; qp = queued_tasks_count
    mid = progress_message_ids.get(OWNER_CHAT_ID)
    if not mid:
        mid = await tg_send_message(OWNER_CHAT_ID, f"⚙️ Queued #{qp}...")
        if mid: progress_message_ids[OWNER_CHAT_ID] = mid
    await processing_queue.put({"input_path":tmp,"file_name":safe,"chat_id":OWNER_CHAT_ID,"target_chat":cid,
                               "caption":cap,"queue_position":qp,"type":"video"})
    return {"ok":True,"result":{"message_id":99999,"chat":{"id":int(cid) if str(cid).replace("-","").isdigit() else 0,"type":"private"},"date":int(time.time()),"text":f"Queued #{qp}"}}

@app.get("/")
async def root():
    return {"status":"ok","mode":"direct-only-no-HD","devices":len(connected_devices),"queue":queued_tasks_count,"admin_only":True}
