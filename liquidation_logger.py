import asyncio
import aiohttp
import websockets
import json
import csv
from datetime import datetime, timezone
from pathlib import Path

# ── Directories ───────────────────────────────────────────────────────────────
LIQ_DIR     = Path("liquidations")
FUNDING_DIR = Path("funding_rates")
LIQ_DIR.mkdir(exist_ok=True)
FUNDING_DIR.mkdir(exist_ok=True)

# ── Constants ─────────────────────────────────────────────────────────────────
LIQ_STREAM_URL     = "wss://fstream.binance.com/ws/!forceOrder@arr"
FUNDING_SYMBOLS    = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
FUNDING_STREAM_URL = (
    "wss://fstream.binance.com/stream?streams="
    + "/".join(f"{s.lower()}@markPrice@1s" for s in FUNDING_SYMBOLS)
)

LIQ_FIELDNAMES = [
    "event_time", "symbol", "side", "order_type",
    "time_in_force", "orig_qty", "price", "avg_price",
    "order_status", "last_filled_qty", "filled_accum_qty",
    "trade_time", "usd_value",
]
FUNDING_FIELDNAMES = [
    "timestamp", "symbol", "mark_price",
    "index_price", "funding_rate", "next_funding_time",
]

# ── File helpers ──────────────────────────────────────────────────────────────
def utc_date_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def get_liq_path() -> Path:
    return LIQ_DIR / f"liquidations_{utc_date_str()}.csv"

def get_funding_path(symbol: str) -> Path:
    return FUNDING_DIR / f"funding_{symbol}_{utc_date_str()}.csv"

def append_row(path: Path, fieldnames: list, row: dict):
    file_exists = path.exists()
    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

# ── Liquidation parsing ───────────────────────────────────────────────────────
def parse_liquidation(msg: dict) -> dict:
    o         = msg["o"]
    qty       = float(o.get("q", 0))
    avg_price = float(o.get("ap", 0))
    return {
        "event_time":       datetime.fromtimestamp(msg["E"] / 1000, tz=timezone.utc).isoformat(),
        "symbol":           o["s"],
        "side":             o["S"],
        "order_type":       o["o"],
        "time_in_force":    o["f"],
        "orig_qty":         o["q"],
        "price":            o["p"],
        "avg_price":        o["ap"],
        "order_status":     o["X"],
        "last_filled_qty":  o["l"],
        "filled_accum_qty": o["z"],
        "trade_time":       datetime.fromtimestamp(o["T"] / 1000, tz=timezone.utc).isoformat(),
        "usd_value":        round(qty * avg_price, 2),
    }

# ── Funding rate parsing ──────────────────────────────────────────────────────
def parse_funding(msg: dict) -> dict:
    data = msg.get("data", msg)
    return {
        "timestamp":        datetime.now(timezone.utc).isoformat(),
        "symbol":           data["s"],
        "mark_price":       data["p"],
        "index_price":      data["i"],
        "funding_rate":     data["r"],
        "next_funding_time": datetime.fromtimestamp(
                                data["T"] / 1000, tz=timezone.utc
                            ).isoformat(),
    }

# ── Liquidation stream ────────────────────────────────────────────────────────
async def stream_liquidations():
    reconnect_delay = 5
    while True:
        try:
            print(f"[{datetime.now(timezone.utc).isoformat()}] Connecting liquidation stream...")
            async with websockets.connect(
                LIQ_STREAM_URL,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                reconnect_delay = 5
                print("Liquidation stream connected.")
                async for raw in ws:
                    try:
                        msg    = json.loads(raw)
                        events = msg if isinstance(msg, list) else [msg]
                        for event in events:
                            if event.get("e") == "forceOrder":
                                row = parse_liquidation(event)
                                append_row(get_liq_path(), LIQ_FIELDNAMES, row)
                                print(
                                    f"{row['event_time']}  "
                                    f"{row['symbol']:12s}  "
                                    f"{row['side']:4s}  "
                                    f"${row['usd_value']:>14,.2f}"
                                )
                    except (KeyError, ValueError) as e:
                        print(f"Liquidation parse error: {e}")

        except (websockets.exceptions.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            print(f"Liquidation stream error: {e}. Reconnecting in {reconnect_delay}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)

# ── Funding rate stream ───────────────────────────────────────────────────────
async def stream_funding_rates():
    reconnect_delay = 5
    last_written: dict[str, str] = {}   # symbol → last funding_rate written
    while True:
        try:
            print(f"[{datetime.now(timezone.utc).isoformat()}] Connecting funding rate stream...")
            async with websockets.connect(
                FUNDING_STREAM_URL,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                reconnect_delay = 5
                print(f"Funding stream connected: {FUNDING_SYMBOLS}")
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        if msg.get("data", {}).get("e") == "markPriceUpdate":
                            row    = parse_funding(msg)
                            symbol = row["symbol"]
                            # Only write when funding rate changes to avoid redundant rows
                            if last_written.get(symbol) != row["funding_rate"]:
                                append_row(
                                    get_funding_path(symbol),
                                    FUNDING_FIELDNAMES,
                                    row,
                                )
                                last_written[symbol] = row["funding_rate"]
                                print(
                                    f"{row['timestamp']}  "
                                    f"{symbol:12s}  "
                                    f"mark={float(row['mark_price']):>10,.2f}  "
                                    f"funding={float(row['funding_rate'])*100:+.4f}%"
                                )
                    except (KeyError, ValueError) as e:
                        print(f"Funding parse error: {e}")

        except (websockets.exceptions.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            print(f"Funding stream error: {e}. Reconnecting in {reconnect_delay}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)

# ── Entry point ───────────────────────────────────────────────────────────────
async def main():
    await asyncio.gather(
        stream_liquidations(),
        stream_funding_rates(),
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped.")
