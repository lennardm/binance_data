import asyncio
import websockets
import json
import csv
import os
from datetime import datetime, timezone
from pathlib import Path

STREAM_URL = "wss://fstream.binance.com/ws/!forceOrder@arr"
OUTPUT_DIR = Path("liquidations")
OUTPUT_DIR.mkdir(exist_ok=True)

FIELDNAMES = [
    "event_time", "symbol", "side", "order_type",
    "time_in_force", "orig_qty", "price", "avg_price",
    "order_status", "last_filled_qty", "filled_accum_qty", "trade_time",
    "usd_value"
]

def get_daily_csv_path() -> Path:
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return OUTPUT_DIR / f"liquidations_{date_str}.csv"

def write_row(row: dict):
    path = get_daily_csv_path()
    file_exists = path.exists()
    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

def parse_message(msg: dict) -> dict:
    o = msg["o"]
    qty = float(o.get("q", 0))
    avg_price = float(o.get("ap", 0))
    return {
        "event_time":      datetime.fromtimestamp(msg["E"] / 1000, tz=timezone.utc).isoformat(),
        "symbol":          o["s"],
        "side":            o["S"],
        "order_type":      o["o"],
        "time_in_force":   o["f"],
        "orig_qty":        o["q"],
        "price":           o["p"],
        "avg_price":       o["ap"],
        "order_status":    o["X"],
        "last_filled_qty": o["l"],
        "filled_accum_qty":o["z"],
        "trade_time":      datetime.fromtimestamp(o["T"] / 1000, tz=timezone.utc).isoformat(),
        "usd_value":       round(qty * avg_price, 2),
    }

async def stream_liquidations():
    reconnect_delay = 5
    while True:
        try:
            print(f"[{datetime.now(timezone.utc).isoformat()}] Connecting to {STREAM_URL}")
            async with websockets.connect(
                STREAM_URL,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                reconnect_delay = 5  # reset on successful connect
                print("Connected. Streaming liquidations...")
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        # all-market stream wraps in a list sometimes
                        events = msg if isinstance(msg, list) else [msg]
                        for event in events:
                            if event.get("e") == "forceOrder":
                                row = parse_message(event)
                                write_row(row)
                                print(f"{row['event_time']}  {row['symbol']:12s} "
                                      f"{row['side']:4s}  ${row['usd_value']:>12,.2f}")
                    except (KeyError, ValueError) as e:
                        print(f"Parse error: {e} | raw: {raw[:200]}")
        except (websockets.exceptions.ConnectionClosed,
                OSError, asyncio.TimeoutError) as e:
            print(f"Connection error: {e}. Reconnecting in {reconnect_delay}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)  # exponential backoff

if __name__ == "__main__":
    asyncio.run(stream_liquidations())
