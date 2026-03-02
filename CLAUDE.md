# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project does

Continuously collects Binance Futures market data for **BTCUSDT, ETHUSDT, SOLUSDT** and writes it to daily CSV/Parquet files. Three scripts run simultaneously, each gathering a different data type.

## Running the scripts

```bash
python3 liquidation_logger.py   # WebSocket: liquidations + funding rates
python3 rest_poller.py          # REST polling: OI, L/S ratios, klines, Fear & Greed
python3 heatmap_logger.py       # REST polling: order book depth → Parquet
```

Dependencies: `websockets`, `aiohttp`, `pandas`, `pyarrow` (for Parquet).

`liqdata.py` is the original prototype for liquidation streaming — superseded by `liquidation_logger.py`.

## Script architecture

All scripts use `asyncio.gather` to run coroutines concurrently in a single process. Each coroutine has its own reconnect/retry loop. Files rotate at UTC midnight.

### `liquidation_logger.py` — two WebSocket streams
| Stream | Source | Output | Notes |
|--------|--------|--------|-------|
| All-market liquidations | `wss://fstream.binance.com/ws/!forceOrder@arr` | `liquidations/liquidations_YYYY-MM-DD.csv` | Every `forceOrder` event |
| Mark price / funding rate | `wss://fstream.binance.com/stream?streams=...@markPrice@1s` | `funding_rates/funding_{SYMBOL}_YYYY-MM-DD.csv` | Only writes on funding rate change |

### `rest_poller.py` — four REST pollers (Binance Futures API + alternative.me)
| Poller | Endpoint | Interval | Output |
|--------|----------|----------|--------|
| Open interest | `/fapi/v1/openInterest` | 60 s | `open_interest/oi_{SYMBOL}_YYYY-MM-DD.csv` |
| Global L/S ratio | `/futures/data/globalLongShortAccountRatio` | 300 s | `longshort_ratio/ls_{SYMBOL}_YYYY-MM-DD.csv` |
| Top trader L/S ratio | `/futures/data/topLongShortAccountRatio` | 300 s | `longshort_ratio/ls_{SYMBOL}_YYYY-MM-DD.csv` |
| 1m klines | `/fapi/v1/klines?interval=1m&limit=1` | 60 s | `klines/klines_{SYMBOL}_YYYY-MM-DD.csv` |
| Fear & Greed index | `api.alternative.me/fng` | 3600 s | `fear_greed/fng_YYYY-MM-DD.csv` |

Startup is staggered per symbol (2–4 s) to avoid burst requests.

### `heatmap_logger.py` — order book depth → Parquet
Polls `/fapi/v1/depth?limit=20` every **0.5 s** per symbol. Rows (timestamp, symbol, side, price, quantity) accumulate in memory and are flushed to **Snappy-compressed Parquet** every **5 minutes** (appended to the daily file). Output: `orderbook_heatmap/orderbook_{SYMBOL}_YYYY-MM-DD.parquet`.

## Output directory map

```
liquidations/          CSV  — one file per day
funding_rates/         CSV  — one file per symbol per day
open_interest/         CSV  — one file per symbol per day
longshort_ratio/       CSV  — one file per symbol per day (global + top rows mixed)
klines/                CSV  — one file per symbol per day
fear_greed/            CSV  — one file per day
orderbook_heatmap/     Parquet — one file per symbol per day
```
