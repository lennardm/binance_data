```markdown
# Binance Futures Data Collector

A suite of Python scripts that continuously collect Binance USD-M Futures market data to local Parquet/CSV files, managed as a single systemd service with Telegram notifications.

---

## Project Structure

```
/opt/collector/app/
├── main.py                      # Entry point — runs all collectors
├── notifier.py                  # Telegram notification module
├── liquidation_logger.py        # Liquidations + funding rates (WebSocket)
├── orderbook_premium_logger.py  # Order book diffs + snapshots + tiles (WebSocket + REST)
├── rest_poller.py               # OI, L/S ratios, klines, Fear & Greed (REST polling)
└── ws_marketwide_logger.py      # All-market 24h tickers + bookTicker (WebSocket)

/opt/collector/data/
├── liquidations/                # liquidations_YYYY-MM-DD.csv
├── funding_rates/               # funding_BTCUSDT_YYYY-MM-DD.csv
├── orderbook_premium/
│   ├── raw_diffs/               # rawdiff_BTCUSDT_YYYY-MM-DD.parquet
│   ├── snapshots/               # snapshot_BTCUSDT_YYYY-MM-DD.parquet
│   └── tiles/                  # tiles_BTCUSDT_YYYY-MM-DD.parquet
├── open_interest/               # oi_BTCUSDT_YYYY-MM-DD.csv
├── longshort_ratio/             # ls_BTCUSDT_YYYY-MM-DD.csv
├── klines/                      # klines_BTCUSDT_YYYY-MM-DD.csv
├── fear_greed/                  # fng_YYYY-MM-DD.csv
├── marketwide/
│   ├── tickers_24h/             # ticker24h_YYYY-MM-DD.parquet
│   └── book_ticker/             # bookTicker_YYYY-MM-DD.parquet
```

---

## What Is Collected

| Script | Data | Mechanism | Interval | Format |
|---|---|---|---|---|
| `liquidation_logger.py` | All-symbol force liquidations | WebSocket `!forceOrder@arr` | Real-time | CSV |
| `liquidation_logger.py` | Funding rate + mark/index price | WebSocket `@markPrice@1s` | On change | CSV |
| `orderbook_premium_logger.py` | Diff-depth order book events | WebSocket `@depth@100ms` | Real-time | Parquet |
| `orderbook_premium_logger.py` | 1000-level order book snapshots | REST `/fapi/v1/depth` | Every 60s + reconnect | Parquet |
| `orderbook_premium_logger.py` | Heatmap tiles (±2% around mid) | Derived from local book | Every 500ms | Parquet |
| `rest_poller.py` | Open interest | REST `/fapi/v1/openInterest` | Every 60s | CSV |
| `rest_poller.py` | Global long/short ratio | REST `/futures/data/globalLongShortAccountRatio` | Every 5min | CSV |
| `rest_poller.py` | Top trader long/short ratio | REST `/futures/data/topLongShortAccountRatio` | Every 5min | CSV |
| `rest_poller.py` | 1m klines (OHLCV + taker buy vol) | REST `/fapi/v1/klines` | Every 60s | CSV |
| `rest_poller.py` | Fear & Greed Index | REST `api.alternative.me/fng/` | Every hour | CSV |
| `ws_marketwide_logger.py` | 24h rolling ticker stats (all symbols) | WebSocket `!ticker@arr` | ~1s (on change) | Parquet |
| `ws_marketwide_logger.py` | Best bid/ask all symbols | WebSocket `!bookTicker` | Every 5s | Parquet |

**Symbols tracked for depth OI/L/S/klines:** BTCUSDT, ETHUSDT, SOLUSDT (configurable in each script's `SYMBOLS` list).

---

## Requirements

```bash
pip install websockets aiohttp pandas pyarrow
```

Python 3.12+ recommended.

---

## Configuration

### Telegram notifications
Create a bot via `@BotFather` on Telegram and get your chat ID:
```bash
curl "https://api.telegram.org/bot<BOT_TOKEN>/getUpdates"
```

Store credentials in `/opt/collector/secrets.env` (never commit this file):
```
TELEGRAM_BOT_TOKEN=your_token_here
TELEGRAM_CHAT_ID=your_chat_id_here
```
```bash
chmod 600 /opt/collector/secrets.env
```

### Symbols and intervals
Edit the `SYMBOLS` list and interval constants at the top of each script.

---

## Running Manually (development)

```bash
cd /opt/collector/app
python main.py
```

Stop with `Ctrl+C` — buffers are flushed to Parquet before exit.

---

## Running as a systemd Service (production)

### Create a dedicated user
```bash
sudo adduser --system --group --home /opt/collector collector
sudo -u collector python3 -m venv /opt/collector/venv
sudo -u collector /opt/collector/venv/bin/pip install websockets aiohttp pandas pyarrow
```

### Create the service unit
`/etc/systemd/system/binance-collector.service`:
```ini
[Unit]
Description=Binance Futures data collector
Wants=network-online.target
After=network-online.target

[Service]
User=collector
Group=collector
WorkingDirectory=/opt/collector/app
Environment=PYTHONUNBUFFERED=1
EnvironmentFile=/opt/collector/secrets.env
ExecStart=/opt/collector/venv/bin/python /opt/collector/app/main.py
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

### Enable and start
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now binance-collector.service
```

### Useful commands
```bash
# Live logs
journalctl -u binance-collector.service -f

# Status
sudo systemctl status binance-collector.service

# Restart manually
sudo systemctl restart binance-collector.service

# Stop
sudo systemctl stop binance-collector.service
```

---

## Telegram Notifications

| Event | Message |
|---|---|
| Service starts | ✅ Collector started at YYYY-MM-DD HH:MM:SS UTC |
| Module crashes | ⚠️ [name] crashed — restarting in Xs |
| Module restarts | 🔄 [name] restarting now... |
| Hourly heartbeat | 💓 Heartbeat YYYY-MM-DD HH:MM:SS UTC |

---

## Crash Recovery & Backoff

Each collector module runs inside a `guarded()` wrapper with exponential backoff:

| Crash # | Wait before restart |
|---|---|
| 1 | 5s |
| 2 | 10s |
| 3 | 20s |
| 4 | 40s |
| 5+ | 300s (cap) |

If one module crashes, the others keep running unaffected.

---

## Data Continuity & Gaps

- **WebSocket feeds** (liquidations, order book diffs) are real-time push only — data missed during downtime cannot be recovered.
- **REST-polled feeds** (klines, OI, L/S ratios) can often be backfilled using Binance historical endpoints for the missed window.
- All files use **UTC timestamps** and **daily rotation** — filenames encode the UTC date so files roll over cleanly at midnight regardless of reboots.
- Order book raw diffs include Binance sequence IDs (`U`, `u`) and snapshots include `lastUpdateId` so data integrity can be verified and gaps can be detected programmatically.

---

## Backups

Recommended: hourly rsync to a second machine or NAS:
```bash
rsync -av --partial --append-verify \
  /opt/collector/data/ \
  backupuser@backuphost:/data/binance/collector/
```

Schedule via cron (`crontab -e`):
```
0 * * * * /usr/local/bin/collector-backup.sh
```

---

## Reading the Data

```python
import pandas as pd

# Liquidations
df = pd.read_csv("liquidations/liquidations_2026-03-03.csv", parse_dates=["event_time"])

# Order book tiles (heatmap)
df = pd.read_parquet("orderbook_premium/tiles/tiles_BTCUSDT_2026-03-03.parquet")

# 24h tickers
df = pd.read_parquet("marketwide/tickers_24h/ticker24h_2026-03-03.parquet")
```

---

## License

Private — data collection for personal use and potential resale. Do not redistribute scripts or collected data without permission.
```

Save this as `README.md` in `/opt/collector/app/`. Let me know if you want a section added (e.g. data schema per file, gap detection, or a backfill guide for the REST feeds).