import aiohttp
import os

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID",   "YOUR_CHAT_ID_HERE")

TELEGRAM_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

async def notify(text: str):
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(
                TELEGRAM_URL,
                json={"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"},
                timeout=aiohttp.ClientTimeout(total=10),
            )
    except Exception as e:
        print(f"[Telegram] Failed to send: {e}")
