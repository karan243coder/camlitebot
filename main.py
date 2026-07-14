import os
import time
from typing import Dict

import httpx
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect

BOT_TOKEN = os.environ["BOT_TOKEN"]
OWNER_CHAT_ID = os.environ["OWNER_CHAT_ID"]
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "changeme")
DEVICE_TOKEN = os.environ.get("DEVICE_TOKEN", "changeme")

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

app = FastAPI()

connected_devices: Dict[str, WebSocket] = {}
progress_message_ids: Dict[str, int] = {}
last_edit_time: Dict[str, float] = {}


async def tg_send_message(chat_id: str, text: str):
    async with httpx.AsyncClient() as client:
        r = await client.post(f"{TELEGRAM_API}/sendMessage", data={"chat_id": chat_id, "text": text})
        data = r.json()
        if data.get("ok"):
            return data["result"]["message_id"]
    return None


async def tg_edit_message(chat_id: str, message_id: int, text: str):
    async with httpx.AsyncClient() as client:
        await client.post(
            f"{TELEGRAM_API}/editMessageText",
            data={"chat_id": chat_id, "message_id": message_id, "text": text},
        )


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

    # anyone can ask their own chat id once, to configure the owner id
    if text == "/myid":
        await tg_send_message(chat_id, f"Tumhara chat ID: {chat_id}")
        return {"ok": True}

    # everything else: owner only
    if chat_id != OWNER_CHAT_ID:
        return {"ok": True}

    cmd = text.lower()
    if cmd in ("/on", "/startcam", "/start_rec"):
        await dispatch_command(chat_id, "start")
    elif cmd in ("/off", "/stopcam", "/stop_rec"):
        await dispatch_command(chat_id, "stop")
    elif cmd in ("/switchcam", "/switch"):
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
            data = await websocket.receive_json()
            await handle_device_event(data)
    except WebSocketDisconnect:
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
        stage = data.get("stage", "Uploading")
        text = f"{stage}: {percent}%"

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

        if percent >= 100:
            progress_message_ids.pop(chat_id, None)

    elif event == "done":
        progress_message_ids.pop(chat_id, None)


@app.get("/")
async def root():
    return {"status": "ok", "devices_connected": len(connected_devices)}
