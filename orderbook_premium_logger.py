import asyncio
import aiohttp
import websockets
import json
import time
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

WS_UPDATE_SPEED = "100ms"     # "100ms" or "500ms"
TILE_INTERVAL_S = 0.5         # derived heatmap tiles frequency (seconds)
SNAPSHOT_INTERVAL_S = 60      # REST 1000-level snapshot frequency (seconds)
FLUSH_INTERVAL_S = 300        # Parquet flush cadence (seconds)

SNAPSHOT_LIMIT = 1000         # max depth on futures REST order book
BOOK_PRUNE_FAR_PCT = 0.05     # prune levels >5% away from mid to cap RAM

# Tile settings (log2 price buckets are robust across price scales)
TILE_BUCKETS_PER_1PCT = 40    # higher = finer heatmap
TILE_RANGE_PCT = 0.02         # store tiles +/-2% around mid

BASE_URL = "https://fapi.binance.com"
OUT_DIR = Path("orderbook_premium")
RAW_DIR = OUT_DIR / "raw_diffs"
SNP_DIR = OUT_DIR / "snapshots"
TIL_DIR = OUT_DIR / "tiles"
for d in [RAW_DIR, SNP_DIR, TIL_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── Helpers ───────────────────────────────────────────────────────────────────
def utc_date_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def parquet_path(directory: Path, prefix: str, symbol: str):
    return directory / f"{prefix}_{symbol}_{utc_date_str()}.parquet"

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def to_float(x):
    try:
        return float(x)
    except Exception:
        return math.nan

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

@dataclass
class LocalBook:
    bids: dict[float, float]
    asks: dict[float, float]
    last_update_id: int | None = None

    def best_bid(self):
        return max(self.bids.keys()) if self.bids else None

    def best_ask(self):
        return min(self.asks.keys()) if self.asks else None

    def mid(self):
        bb = self.best_bid()
        ba = self.best_ask()
        if bb is None or ba is None:
            return None
        return (bb + ba) / 2.0

    def apply_side_updates(self, side: dict[float, float], updates: list):
        for price_s, qty_s in updates:
            p = float(price_s)
            q = float(qty_s)
            if q == 0.0:
                side.pop(p, None)
            else:
                side[p] = q

    def apply_diff_event(self, ev: dict):
        self.apply_side_updates(self.bids, ev.get("b", []))
        self.apply_side_updates(self.asks, ev.get("a", []))
        self.last_update_id = ev.get("u", self.last_update_id)

    def prune_far(self):
        m = self.mid()
        if m is None:
            return
        lo = m * (1.0 - BOOK_PRUNE_FAR_PCT)
        hi = m * (1.0 + BOOK_PRUNE_FAR_PCT)
        self.bids = {p: q for p, q in self.bids.items() if p >= lo}
        self.asks = {p: q for p, q in self.asks.items() if p <= hi}

# ── Snapshot fetch ────────────────────────────────────────────────────────────
async def fetch_snapshot(symbol: str, session: aiohttp.ClientSession) -> dict:
    url = f"{BASE_URL}/fapi/v1/depth?symbol={symbol}&limit={SNAPSHOT_LIMIT}"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
        data = await resp.json()
        return data

def load_snapshot_into_book(symbol: str, snap: dict, book: LocalBook):
    book.bids = {float(p): float(q) for p, q in snap.get("bids", [])}
    book.asks = {float(p): float(q) for p, q in snap.get("asks", [])}
    book.last_update_id = int(snap.get("lastUpdateId")) if snap.get("lastUpdateId") is not None else None

# ── Tile generation ───────────────────────────────────────────────────────────
def bucket_key(price: float, mid: float) -> int:
    # Use log2(price/mid) scaled so buckets are ~uniform in % terms
    x = math.log(price / mid, 2)
    # approx: 1% move ~ log2(1.01) ≈ 0.014355; use TILE_BUCKETS_PER_1PCT for resolution
    scale = TILE_BUCKETS_PER_1PCT / math.log(1.01, 2)
    return int(math.floor(x * scale))

def make_tiles(symbol: str, book: LocalBook, ts: str) -> list[dict]:
    m = book.mid()
    if m is None:
        return []
    lo = m * (1.0 - TILE_RANGE_PCT)
    hi = m * (1.0 + TILE_RANGE_PCT)

    buckets = defaultdict(float)

    for p, q in book.bids.items():
        if lo <= p <= hi:
            buckets[("bid", bucket_key(p, m))] += q

    for p, q in book.asks.items():
        if lo <= p <= hi:
            buckets[("ask", bucket_key(p, m))] += q

    rows = []
    for (side, b), qty in buckets.items():
        rows.append({
            "timestamp": ts,
            "symbol": symbol,
            "mid": m,
            "side": side,
            "bucket": b,
            "quantity": qty,
        })
    return rows

# ── Stream + recording per symbol ─────────────────────────────────────────────
async def run_symbol(symbol: str):
    raw_buf = []
    snap_buf = []
    tile_buf = []

    book = LocalBook(bids={}, asks={}, last_update_id=None)

    ws_url = f"wss://fstream.binance.com/ws/{symbol.lower()}@depth@{WS_UPDATE_SPEED}"

    async with aiohttp.ClientSession() as session:
        last_snapshot_t = 0.0
        last_tile_t = 0.0
        last_flush_t = time.time()

        async def do_snapshot(reason: str):
            nonlocal last_snapshot_t
            snap = await fetch_snapshot(symbol, session)
            ts = now_iso()
            snap_buf.append({
                "timestamp": ts,
                "symbol": symbol,
                "reason": reason,
                "lastUpdateId": int(snap.get("lastUpdateId", -1)),
                "bids": json.dumps(snap.get("bids", [])),
                "asks": json.dumps(snap.get("asks", [])),
                "limit": SNAPSHOT_LIMIT,
            })
            load_snapshot_into_book(symbol, snap, book)
            last_snapshot_t = time.time()
            print(f"{ts}  SNAP {symbol} lastUpdateId={book.last_update_id} ({reason})")

        await do_snapshot("startup")

        reconnect_delay = 5
        while True:
            try:
                print(f"{now_iso()}  WS connect {symbol} → {ws_url}")
                async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10, close_timeout=5) as ws:
                    reconnect_delay = 5
                    async for raw in ws:
                        ev = json.loads(raw)
                        ts = now_iso()

                        # Record raw diff event (compact)
                        raw_buf.append({
                            "timestamp": ts,
                            "symbol": ev.get("s", symbol),
                            "E": ev.get("E"),
                            "U": ev.get("U"),
                            "u": ev.get("u"),
                            "b": json.dumps(ev.get("b", [])),
                            "a": json.dumps(ev.get("a", [])),
                        })

                        # Apply to local book
                        book.apply_diff_event(ev)

                        # Periodic snapshot (checkpoint) and pruning
                        now_t = time.time()
                        if now_t - last_snapshot_t >= SNAPSHOT_INTERVAL_S:
                            await do_snapshot("interval")

                        if int(now_t) % 10 == 0:  # light prune trigger
                            book.prune_far()

                        # Make tiles
                        if now_t - last_tile_t >= TILE_INTERVAL_S:
                            tiles = make_tiles(symbol, book, ts)
                            tile_buf.extend(tiles)
                            last_tile_t = now_t

                        # Flush to Parquet
                        if now_t - last_flush_t >= FLUSH_INTERVAL_S:
                            write_parquet_append(parquet_path(RAW_DIR, "rawdiff", symbol), raw_buf); raw_buf.clear()
                            write_parquet_append(parquet_path(SNP_DIR, "snapshot", symbol), snap_buf); snap_buf.clear()
                            write_parquet_append(parquet_path(TIL_DIR, "tiles", symbol), tile_buf); tile_buf.clear()
                            last_flush_t = now_t
                            print(f"{ts}  FLUSH {symbol}")

            except (websockets.exceptions.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
                # Flush buffers before reconnecting
                write_parquet_append(parquet_path(RAW_DIR, "rawdiff", symbol), raw_buf); raw_buf.clear()
                write_parquet_append(parquet_path(SNP_DIR, "snapshot", symbol), snap_buf); snap_buf.clear()
                write_parquet_append(parquet_path(TIL_DIR, "tiles", symbol), tile_buf); tile_buf.clear()

                print(f"{now_iso()}  WS error {symbol}: {e}. Reconnect in {reconnect_delay}s...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)

                # Resnapshot on reconnect to re-anchor local book
                try:
                    await do_snapshot("reconnect")
                except Exception as se:
                    print(f"{now_iso()}  SNAP error {symbol}: {se}")

# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    await asyncio.gather(*[run_symbol(sym) for sym in SYMBOLS])

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped.")
