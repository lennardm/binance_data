import asyncio
import aiohttp
import json
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

# ── Config ────────────────────────────────────────────────────────────────────
SYMBOLS        = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
DEPTH_LEVELS   = 20
POLL_INTERVAL  = 0.5   # seconds between snapshots
FLUSH_INTERVAL = 300   # seconds between Parquet flushes (5 minutes)
OUT_DIR        = Path("orderbook_heatmap")
OUT_DIR.mkdir(exist_ok=True)

# ── Parquet helpers ───────────────────────────────────────────────────────────
def get_parquet_path(symbol: str) -> Path:
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return OUT_DIR / f"orderbook_{symbol}_{date_str}.parquet"

def flush_to_parquet(buffer: list, symbol: str):
    if not buffer:
        return
    path    = get_parquet_path(symbol)
    df_new  = pd.DataFrame(buffer)
    if path.exists():
        df_existing = pd.read_parquet(path)
        df_out      = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_out = df_new
    df_out.to_parquet(path, index=False, compression="snappy")
    print(
        f"Flushed {len(buffer):>6} rows → {path.name}  "
        f"(total {len(df_out):,} rows, "
        f"{path.stat().st_size / 1024 / 1024:.2f} MB)"
    )

# ── Order book poller (one coroutine per symbol) ──────────────────────────────
async def poll_orderbook(symbol: str, buffers: dict, session: aiohttp.ClientSession):
    url = (
        f"https://fapi.binance.com/fapi/v1/depth"
        f"?symbol={symbol}&limit={DEPTH_LEVELS}"
    )
    print(f"Starting order book poll: {symbol}")
    poll_count = 0
    while True:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                data = await resp.json()
                ts   = datetime.now(timezone.utc).isoformat()
                rows = []
                for price, qty in data.get("bids", []):
                    rows.append({
                        "timestamp": ts,
                        "symbol":    symbol,
                        "side":      "bid",
                        "price":     float(price),
                        "quantity":  float(qty),
                    })
                for price, qty in data.get("asks", []):
                    rows.append({
                        "timestamp": ts,
                        "symbol":    symbol,
                        "side":      "ask",
                        "price":     float(price),
                        "quantity":  float(qty),
                    })
                buffers[symbol].extend(rows)
                poll_count += 1
                if poll_count % 20 == 0:  # print every 10 seconds (20 × 0.5s)
                    best_bid = rows[0]["price"] if rows else "?"
                    print(f"{ts}  {symbol:12s}  best_bid={best_bid:>10}  buffer={len(buffers[symbol]):,} rows")
        except Exception as e:
            print(f"Depth poll error ({symbol}): {e}")
        await asyncio.sleep(POLL_INTERVAL)

# ── Periodic flusher ──────────────────────────────────────────────────────────
async def periodic_flush(buffers: dict):
    while True:
        await asyncio.sleep(FLUSH_INTERVAL)
        for symbol, buf in buffers.items():
            if buf:
                flush_to_parquet(buf.copy(), symbol)
                buf.clear()
                print(f"Buffer cleared: {symbol}")

# ── Entry point ───────────────────────────────────────────────────────────────
async def main():
    buffers: dict[str, list] = defaultdict(list)
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            periodic_flush(buffers),
            *[poll_orderbook(sym, buffers, session) for sym in SYMBOLS],
        )

if __name__ == "__main__":
    buffers: dict[str, list] = defaultdict(list)

    async def _main():
        async with aiohttp.ClientSession() as session:
            try:
                await asyncio.gather(
                    periodic_flush(buffers),
                    *[poll_orderbook(sym, buffers, session) for sym in SYMBOLS],
                )
            except asyncio.CancelledError:
                pass

    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        print("Shutting down — flushing remaining buffers...")
        for symbol, buf in buffers.items():
            flush_to_parquet(buf, symbol)
        print("Done.")

