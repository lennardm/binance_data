import asyncio
from datetime import datetime, timezone

import liquidation_logger
import orderbook_premium_logger
import rest_poller
import ws_marketwide_logger
from notifier import notify

COLLECTORS = {
    "liquidations": liquidation_logger.run,
    "orderbook":    orderbook_premium_logger.run,
    "rest_poller":  rest_poller.run,
    "marketwide":   ws_marketwide_logger.run,
}

HEARTBEAT_INTERVAL = 3600  # seconds — one Telegram message per hour

def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

async def guarded(name: str, coro_factory, min_delay=2, max_delay=300):
    delay = min_delay
    while True:
        try:
            await coro_factory()
            delay = min_delay  # reset on clean exit
        except Exception as e:
            print(f"[{name}] crashed: {e} — restarting in {delay}s")
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_delay)

async def main():
    await asyncio.gather(
        guarded("liquidations", liquidation_logger.run(),
        guarded("orderbook",     orderbook_premium_logger.run()),
        guarded("rest_poller",   rest_poller.run()),
        guarded("marketwide",    ws_marketwide_logger.run())
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped.")
