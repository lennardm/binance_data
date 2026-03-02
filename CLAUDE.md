# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project does

`liqdata.py` connects to the Binance Futures all-market liquidation WebSocket stream (`wss://fstream.binance.com/ws/!forceOrder@arr`) and records every forced liquidation order to daily CSV files in the `liquidations/` directory.

## Running

```bash
python3 liqdata.py
```

Requires the `websockets` package (`pip install websockets`). No other dependencies beyond the standard library.

## Architecture

Single-file script with three layers:

1. **WebSocket layer** (`stream_liquidations`): Maintains a persistent connection with auto-reconnect and exponential backoff (5s → 60s cap). Handles both single-event and list-wrapped message formats.

2. **Parse layer** (`parse_message`): Extracts fields from the nested `o` (order) object in each `forceOrder` event. Computes `usd_value = orig_qty × avg_price`.

3. **Storage layer** (`write_row` / `get_daily_csv_path`): Appends rows to `liquidations/liquidations_YYYY-MM-DD.csv`, creating a new file with headers each UTC day.

## Output format

CSV columns: `event_time`, `symbol`, `side` (BUY/SELL), `order_type`, `time_in_force`, `orig_qty`, `price`, `avg_price`, `order_status`, `last_filled_qty`, `filled_accum_qty`, `trade_time`, `usd_value`.

Timestamps are ISO 8601 in UTC. Files rotate at UTC midnight.
