# marketgoblin

> Download, store, and load financial market data — fast and without fuss.

[![PyPI](https://img.shields.io/pypi/v/marketgoblin?color=green)](https://pypi.org/project/marketgoblin/)
![Python](https://img.shields.io/badge/python-3.13-blue)
![License](https://img.shields.io/badge/license-MIT-green)
[![CI](https://github.com/aexsalomao/marketgoblin/actions/workflows/ci.yml/badge.svg)](https://github.com/aexsalomao/marketgoblin/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/aexsalomao/marketgoblin/branch/master/graph/badge.svg)](https://codecov.io/gh/aexsalomao/marketgoblin)
[![Docs](https://img.shields.io/badge/docs-aexsalomao.github.io%2Fmarketgoblin-blue)](https://aexsalomao.github.io/marketgoblin)

**marketgoblin** is a lightweight market data platform built on [Polars](https://pola.rs/) and [yfinance](https://github.com/ranaroussi/yfinance). It fetches multiple datasets (OHLCV, shares-outstanding, …), slices them into monthly Parquet files, writes JSON sidecars with metadata, and lets you load them back with a single call.

---

## Features

- **Multi-dataset** — OHLCV and shares-outstanding selected via a `Dataset` enum; per-source dispatch makes it easy to add more
- **Single-symbol and batch fetch** — `fetch()` and `fetch_many()` with thread-pool concurrency
- **Disk persistence** — monthly `.pq` slices with atomic writes; JSON sidecar per slice
- **Lazy evaluation** — all data paths return `pl.LazyFrame` (Polars)
- **Date flexibility** — dates stored as `int32` YYYYMMDD on disk; use `parse_dates=True` to get `pl.Date`
- **Retry logic** — `YahooSource` retries transient failures with exponential backoff (3 attempts)
- **Rate limiting** — `fetch_many()` respects a configurable requests-per-second cap (default: 2 req/s)
- **Input validation** — dates and dataset/adjusted combinations are validated before any I/O
- **Pluggable providers** — subclass `BaseSource`, implement `_build_dispatch()`, register in one line; `CSVSource` included

---

## Installation

```bash
pip install marketgoblin
```

Or with [uv](https://github.com/astral-sh/uv):

```bash
uv add marketgoblin
```

For development:

```bash
git clone https://github.com/aexsalomao/marketgoblin
cd marketgoblin
uv sync --extra dev
```

---

## Quick Start

```python
from marketgoblin import Dataset, MarketGoblin

goblin = MarketGoblin(provider="yahoo", save_path="./data")

# Fetch and persist OHLCV
lf = goblin.fetch("AAPL", "2024-01-01", "2024-03-31", parse_dates=True)
print(lf.collect())

# Load back from disk
lf = goblin.load("AAPL", "2024-01-01", "2024-03-31", parse_dates=True)
print(lf.collect())

# Fetch shares-outstanding (corporate-action driven, sparse series)
shares = goblin.fetch("AAPL", "2024-01-01", "2024-03-31", dataset=Dataset.SHARES, parse_dates=True)
print(shares.collect())

# Batch fetch — failed symbols are logged, never crash the batch
results = goblin.fetch_many(["AAPL", "MSFT", "GOOGL"], "2024-01-01", "2024-03-31")
for symbol, lf in results.items():
    print(f"{symbol}: {lf.collect().height} rows")
```

Run the full walkthrough:

```bash
python example.py
```

---

## API

### `MarketGoblin`

```python
MarketGoblin(provider: str, api_key: str | None = None, save_path: str | Path | None = None, **source_kwargs)
```

| Method | Description |
|---|---|
| `fetch(symbol, start, end, dataset=Dataset.OHLCV, adjusted=True, parse_dates=False)` | Download, save to disk (if `save_path` set), return `LazyFrame` |
| `load(symbol, start, end, dataset=Dataset.OHLCV, adjusted=True, parse_dates=False)` | Load from disk; raises `RuntimeError` if no `save_path` |
| `fetch_many(symbols, start, end, dataset=Dataset.OHLCV, adjusted=True, parse_dates=False, max_workers=8, requests_per_second=2.0)` | Batch fetch via `ThreadPoolExecutor`, rate-limited |
| `supported_datasets` (property) | `frozenset[Dataset]` of datasets the configured provider supports |

### Datasets

| Dataset | Provider support | Columns |
|---|---|---|
| `Dataset.OHLCV` | `yahoo`, `csv` | `date` (int32), `open` / `high` / `low` / `close` (float32), `volume` (int64), `symbol` |
| `Dataset.SHARES` | `yahoo` | `date` (int32), `shares` (int64), `symbol` |

`adjusted` only applies to OHLCV. Passing `adjusted=False` with another dataset raises `ValueError`.

### Data on disk

| Property | Detail |
|---|---|
| Date column | `int32` YYYYMMDD (e.g. `20240101`); `parse_dates=True` → `pl.Date` |
| OHLC columns | `float32` |
| Volume column | `int64` |
| Shares column | `int64` |
| OHLCV parquet path | `{save_path}/{provider}/ohlcv/{adjusted\|raw}/{SYMBOL}/{SYMBOL}_{YYYY-MM}.pq` |
| SHARES parquet path | `{save_path}/{provider}/shares/{SYMBOL}/{SYMBOL}_{YYYY-MM}.pq` |
| JSON sidecar | Same path, `.json` extension — row count, date range, per-dataset stats (OHLCV also records missing trading days) |

---

## Adding a Provider

```python
import polars as pl

from marketgoblin import Dataset
from marketgoblin.sources.base import BaseSource, Fetcher

class MySource(BaseSource):
    name = "mysource"

    def _build_dispatch(self) -> dict[Dataset, Fetcher]:
        return {Dataset.OHLCV: self._fetch_ohlcv}

    def _fetch_ohlcv(self, symbol: str, start: str, end: str, adjusted: bool = True) -> pl.LazyFrame:
        ...  # return a normalized LazyFrame
```

Then register it in `goblin.py`:

```python
_SOURCES = {"yahoo": YahooSource, "csv": CSVSource, "mysource": MySource}
```

A `CSVSource` is included out of the box for loading local CSV files:

```python
goblin = MarketGoblin(provider="csv", data_dir="./csv_files")
lf = goblin.fetch("AAPL", "2024-01-01", "2024-03-31")
```

---

## Running Tests

```bash
pytest
pytest --cov=marketgoblin   # with coverage
```

---

## License

MIT © [Antônio Salomão](https://github.com/aexsalomao)
