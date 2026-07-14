# CamBotServer — Koyeb Deployment Guide

यह छोटा server Telegram से **webhook** (instant push, polling नहीं) receive करता है और फ़ोन से खुले **WebSocket** पर तुरंत command भेज देता है। यही आपकी "बहुत fast connection" है।

## 1) GitHub पर डालो
इस folder को अपने GitHub repo में push कर दो (जैसा आपने कहा, पहले GitHub फिर Koyeb)।

## 2) Koyeb पर Deploy करो
1. https://app.koyeb.com पर account बनाओ / login करो।
2. **Create Service → GitHub** → अपना repo select करो (Dockerfile अपने-आप detect हो जाएगा)।
3. **Environment Variables** में ये चारों डालो:
   - `BOT_TOKEN` = आपका Telegram bot token
   - `OWNER_CHAT_ID` = आपकी personal chat ID (नहीं पता तो नीचे step 4 देखो)
   - `WEBHOOK_SECRET` = कोई भी random लंबा string, जैसे `xk29d81jf`
   - `DEVICE_TOKEN` = कोई दूसरा random string, जैसे `p73mq02za` (यही फ़ोन app में डालना है)
4. Deploy दबाओ। कुछ मिनट में एक public URL मिलेगा जैसे:
   `https://your-app-name.koyeb.app`

## 3) Telegram Webhook Set करो
Deploy होने के बाद, browser में यह URL खोलो (अपनी values भरकर):

```
https://api.telegram.org/bot<BOT_TOKEN>/setWebhook?url=https://your-app-name.koyeb.app/webhook/<WEBHOOK_SECRET>
```

Response में `"ok":true` दिखना चाहिए। बस, अब Telegram हर message सीधे आपके server को instantly push करेगा।

## 4) अपनी Chat ID पता करो (अगर नहीं पता)
Telegram पर अपने bot को `/myid` भेजो, तुरंत जवाब आएगा। वह ID `OWNER_CHAT_ID` env variable में डालो और service को restart करो (Koyeb dashboard से env var update करने पर auto-redeploy होता है)।

## 5) App से जोड़ो
फ़ोन app में:
- **Bot Server WebSocket URL**: `wss://your-app-name.koyeb.app/ws`
- **Device Token**: वही जो Step 2 में `DEVICE_TOKEN` रखा था

## Test कैसे करें
1. Koyeb dashboard में service "Running" दिखना चाहिए।
2. Browser में `https://your-app-name.koyeb.app/` खोलो → `{"status":"ok","devices_connected":0}` दिखेगा। App खोलकर service start करने के बाद यह `1` हो जाना चाहिए।
3. Telegram पर `/status` भेजो — "Phone connected, ready" आना चाहिए।
4. `/on` भेजो — फ़ोन turant record करना शुरू कर देगा, notification बदल जाएगी।
5. `/off` भेजो — recording रुकेगी, फिर Telegram पर वही message लगातार update होगा: "Uploading: 10%... 40%... 100%", फिर mp4 channel पर आ जाएगी।
