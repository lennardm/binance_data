import asyncio
import aiohttp
import csv
from datetime import datetime, timezone
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
SYMBOLS         = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
OI_INTERVAL     = 60    # seconds
LS_INTERVAL     = 300   # seconds
KLINE_INTERVAL  = 60    # seconds
FNG_INTERVAL    = 3600  # seconds (once per hour is plenty)

BASE_URL        = "https://fapi.binance.com"
FNG_URL         = "https://api.alternative.me/fng/?limit=1"

# ── Directories ───────────────────────────────────────────────────────────────
OI_DIR     = Path("open_interest")
LS_DIR     = Path("longshort_ratio")
KLINE_DIR  = Path("klines")
FNG_DIR    = Path("fear_greed")

for d in [OI_DIR, LS_DIR, KLINE_DIR, FNG_DIR]:
    d.mkdir(exist_ok=True)

# ── Fieldnames ────────────────────────────────────────────────────────────────
OI_FIELDNAMES    = ["timestamp", "symbol", "open_interest"]
LS_FIELDNAMES    = ["timestamp", "symbol", "type", "long_ratio", "short_ratio", "long_short_ratio"]
KLINE_FIELDNAMES = ["timestamp", "symbol", "open", "high", "low", "close",
                    "volume", "taker_buy_volume", "taker_buy_quote_volume", "trades"]
FNG_FIELDNAMES   = ["timestamp", "value", "classification"]

# ── File helpers ──────────────────────────────────────────────────────────────
def utc_date_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def get_path(directory: Path, prefix: str, symbol: str = None) -> Path:
    suffix = f"_{symbol}" if symbol else ""
    return directory / f"{prefix}{suffix}_{utc_date_str()}.csv"

def append_row(path: Path, fieldnames: list, row: dict):
    file_exists = path.exists()
    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

# ── Open Interest ─────────────────────────────────────────────────────────────
async def poll_open_interest(symbol: str, session: aiohttp.ClientSession):
    url = f"{BASE_URL}/fapi/v1/openInterest?symbol={symbol}"
    await asyncio.sleep(SYMBOLS.index(symbol) * 2)  # stagger startup
    while True:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()
                ts   = datetime.now(timezone.utc).isoformat()
                row  = {
                    "timestamp":     ts,
                    "symbol":        symbol,
                    "open_interest": float(data["openInterest"]),
                }
                append_row(get_path(OI_DIR, "oi", symbol), OI_FIELDNAMES, row)
                print(f"{ts}  OI      {symbol:12s}  {float(data['openInterest']):>16,.3f}")
        except Exception as e:
            print(f"OI error ({symbol}): {e}")
        await asyncio.sleep(OI_INTERVAL)

# ── Long/Short Ratio ──────────────────────────────────────────────────────────
async def poll_longshort(symbol: str, ratio_type: str, session: aiohttp.ClientSession):
    """
    ratio_type: 'global'   → globalLongShortAccountRatio
                'top'      → topLongShortAccountRatio
    """
    endpoint = (
        "globalLongShortAccountRatio" if ratio_type == "global"
        else "topLongShortAccountRatio"
    )
    url = f"{BASE_URL}/futures/data/{endpoint}?symbol={symbol}&period=5m&limit=1"
    await asyncio.sleep(SYMBOLS.index(symbol) * 3)
    while True:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()
                if data:
                    entry = data[0]
                    ts    = datetime.now(timezone.utc).isoformat()
                    row   = {
                        "timestamp":        ts,
                        "symbol":           symbol,
                        "type":             ratio_type,
                        "long_ratio":       entry["longAccount"],
                        "short_ratio":      entry["shortAccount"],
                        "long_short_ratio": entry["longShortRatio"],
                    }
                    append_row(get_path(LS_DIR, "ls", symbol), LS_FIELDNAMES, row)
                    print(
                        f"{ts}  L/S-{ratio_type:<6s}  {symbol:12s}  "
                        f"L={float(entry['longAccount']):.3f}  "
                        f"S={float(entry['shortAccount']):.3f}"
                    )
        except Exception as e:
            print(f"L/S error ({symbol}/{ratio_type}): {e}")
        await asyncio.sleep(LS_INTERVAL)

# ── Klines (1m candles) ───────────────────────────────────────────────────────
async def poll_klines(symbol: str, session: aiohttp.ClientSession):
    url = f"{BASE_URL}/fapi/v1/klines?symbol={symbol}&interval=1m&limit=1"
    await asyncio.sleep(SYMBOLS.index(symbol) * 4)
    while True:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()
                if data:
                    k  = data[0]
                    ts = datetime.now(timezone.utc).isoformat()
                    row = {
                        "timestamp":              ts,
                        "symbol":                 symbol,
                        "open":                   k[1],
                        "high":                   k[2],
                        "low":                    k[3],
                        "close":                  k[4],
                        "volume":                 k[5],
                        "taker_buy_volume":       k[9],
                        "taker_buy_quote_volume": k[10],
                        "trades":                 k[8],
                    }
                    append_row(get_path(KLINE_DIR, "klines", symbol), KLINE_FIELDNAMES, row)
                    print(
                        f"{ts}  KLINE   {symbol:12s}  "
                        f"C={float(k[4]):>10,.2f}  "
                        f"V={float(k[5]):>12,.2f}"
                    )
        except Exception as e:
            print(f"Kline error ({symbol}): {e}")
        await asyncio.sleep(KLINE_INTERVAL)

# ── Fear & Greed Index ────────────────────────────────────────────────────────
async def poll_fear_greed(session: aiohttp.ClientSession):
    while True:
        try:
            async with session.get(FNG_URL, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()
                entry = data["data"][0]
                ts    = datetime.now(timezone.utc).isoformat()
                row   = {
                    "timestamp":      ts,
                    "value":          entry["value"],
                    "classification": entry["value_classification"],
                }
                append_row(get_path(FNG_DIR, "fng"), FNG_FIELDNAMES, row)
                print(f"{ts}  F&G  value={entry['value']}  ({entry['value_classification']})")
        except Exception as e:
            print(f"Fear & Greed error: {e}")
        await asyncio.sleep(FNG_INTERVAL)

# ── Entry point ───────────────────────────────────────────────────────────────
async def main():
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            # Open Interest — all symbols
            *[poll_open_interest(sym, session) for sym in SYMBOLS],
            # Global L/S ratio — all symbols
            *[poll_longshort(sym, "global", session) for sym in SYMBOLS],
            # Top trader L/S ratio — all symbols
            *[poll_longshort(sym, "top", session) for sym in SYMBOLS],
            # 1m klines — all symbols
            *[poll_klines(sym, session) for sym in SYMBOLS],
            # Fear & Greed — once per hour
            poll_fear_greed(session),
        )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped.")
