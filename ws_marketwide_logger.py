import asyncio
import websockets
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────
OUT_DIR = Path("marketwide")
TICKER_DIR = OUT_DIR / "tickers_24h"
BOOK_DIR   = OUT_DIR / "book_ticker"
for d in [TICKER_DIR, BOOK_DIR]:
    d.mkdir(parents=True, exist_ok=True)

FLUSH_INTERVAL_S = 300  # 5 minutes
MAX_TICKER_ROWS = 2_000_000
MAX_BOOK_ROWS   = 2_000_000

# Streams:
# - !ticker@arr: 24h rolling ticker stats for symbols that changed; update speed 1000ms
# - !bookTicker: best bid/ask for all symbols; update speed 5s
STREAMS = ["!ticker@arr", "!bookTicker"]

WS_URL = "wss://fstream.binance.com/stream?streams=" + "/".join(s.replace("!", "%21") for s in STREAMS)

# ── Helpers ───────────────────────────────────────────────────────────────────
def utc_date_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def parquet_path(directory: Path, prefix: str):
    return directory / f"{prefix}_{utc_date_str()}.parquet"

def write_parquet_append(path: Path, rows: list[dict]):
    if not rows:
        return
    df_new = pd.DataFrame(rows)
    if path.exists():
        df_old = pd.read_parquet(path)
        df_out = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_out = df_new
    df_out.to_parquet(path, index=False, compression="snappy")

# ── Main collector ────────────────────────────────────────────────────────────
async def run():
    ticker_buf = []
    book_buf   = []

    last_flush_t = time.time()
    reconnect_delay = 5

    while True:
        try:
            print(f"{now_iso()}  Connecting → {WS_URL}")
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=10, close_timeout=5) as ws:
                reconnect_delay = 5
                print(f"{now_iso()}  Connected. Streams={STREAMS}")

                async for raw in ws:
                    msg = json.loads(raw)
                    payload = msg.get("data", {})
                    stream  = msg.get("stream", "")

                    ts = now_iso()

                    if stream.endswith("!ticker@arr") or stream.endswith("%21ticker@arr"):
                        # payload is a list of tickers that changed
                        if isinstance(payload, list):
                            for t in payload:
                                ticker_buf.append({
                                    "timestamp": ts,
                                    "e": t.get("e"),
                                    "E": t.get("E"),
                                    "s": t.get("s"),
                                    "c": t.get("c"),
                                    "P": t.get("P"),
                                    "v": t.get("v"),
                                    "q": t.get("q"),
                                    "h": t.get("h"),
                                    "l": t.get("l"),
                                    "w": t.get("w"),
                                    "o": t.get("o"),
                                    "Q": t.get("Q"),
                                    "p": t.get("p"),
                                })

                    elif stream.endswith("!bookTicker") or stream.endswith("%21bookTicker"):
                        # payload is a single bookTicker event
                        if isinstance(payload, dict):
                            book_buf.append({
                                "timestamp": ts,
                                "e": payload.get("e"),
                                "E": payload.get("E"),
                                "T": payload.get("T"),
                                "u": payload.get("u"),
                                "s": payload.get("s"),
                                "b": payload.get("b"),  # best bid price
                                "B": payload.get("B"),  # best bid qty
                                "a": payload.get("a"),  # best ask price
                                "A": payload.get("A"),  # best ask qty
                            })

                    # periodic flush
                    now_t = time.time()
                    if now_t - last_flush_t >= FLUSH_INTERVAL_S:
                        write_parquet_append(parquet_path(TICKER_DIR, "ticker24h"), ticker_buf)
                        write_parquet_append(parquet_path(BOOK_DIR, "bookTicker"), book_buf)
                        print(
                            f"{now_iso()}  FLUSH  "
                            f"ticker_rows={len(ticker_buf):,}  book_rows={len(book_buf):,}"
                        )
                        ticker_buf.clear()
                        book_buf.clear()
                        last_flush_t = now_t

                    if len(ticker_buf) >= MAX_TICKER_ROWS or len(book_buf) >= MAX_BOOK_ROWS:
                        write_parquet_append(parquet_path(TICKER_DIR, "ticker24h"), ticker_buf) 
                        write_parquet_append(parquet_path(BOOK_DIR, "bookTicker"), book_buf)
                        print(
                            f"{now_iso()}  FLUSH(size)  forced"
                            f"ticker_rows={len(ticker_buf):,}  book_rows={len(book_buf):,}"
                        )
                        ticker_buf.clear()
                        book_buf.clear()
                        last_flush_t = time.time()

        except (websockets.exceptions.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            # flush what we have before reconnect
            write_parquet_append(parquet_path(TICKER_DIR, "ticker24h"), ticker_buf); ticker_buf.clear()
            write_parquet_append(parquet_path(BOOK_DIR, "bookTicker"), book_buf); book_buf.clear()

            print(f"{now_iso()}  WS error: {e}. Reconnect in {reconnect_delay}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("Stopped.")
