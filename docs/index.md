# marketgoblin

> Download, store, and load financial OHLCV data — fast and without fuss.

**marketgoblin** is a lightweight market data platform built on [Polars](https://pola.rs/) and [yfinance](https://github.com/ranaroussi/yfinance). It fetches OHLCV data, slices it into monthly Parquet files, writes JSON sidecars with metadata, and lets you load it back with a single call.

## Installation

```bash
pip install marketgoblin
```

## Quick Start

```python
from marketgoblin import MarketGoblin

goblin = MarketGoblin(provider="yahoo", save_path="./data")

# Fetch and persist
lf = goblin.fetch("AAPL", "2024-01-01", "2024-03-31", parse_dates=True)
print(lf.collect())

# Load back from disk
lf = goblin.load("AAPL", "2024-01-01", "2024-03-31", parse_dates=True)
print(lf.collect())

# Batch fetch
results = goblin.fetch_many(["AAPL", "MSFT", "GOOGL"], "2024-01-01", "2024-03-31")
for symbol, lf in results.items():
    print(f"{symbol}: {lf.collect().height} rows")
```

## Data conventions

| Property | Detail |
|---|---|
| Date on disk | `int32` YYYYMMDD (e.g. `20240101`); `parse_dates=True` returns `pl.Date` |
| OHLC columns | `float32` |
| Volume column | `int64` |
| Parquet path | `{save_path}/{provider}/ohlcv/{adjusted\|raw}/{SYMBOL}/{SYMBOL}_{YYYY-MM}.pq` |
| JSON sidecar | Same path, `.json` extension — row count, date range, OHLCV stats, missing trading days |

## Features

- **Single-symbol and batch fetch** — `fetch()` and `fetch_many()` with thread-pool concurrency
- **Disk persistence** — monthly `.pq` slices with atomic writes; JSON sidecar per slice
- **Lazy evaluation** — all data paths return `pl.LazyFrame`
- **Retry logic** — `YahooSource` retries transient failures with exponential backoff
- **Rate limiting** — `fetch_many()` respects a configurable requests-per-second cap
- **Input validation** — dates are validated for format and ordering before any I/O
- **Pluggable providers** — subclass `BaseSource` and register in one line
