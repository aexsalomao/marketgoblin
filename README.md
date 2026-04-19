# marketgoblin

> Download, store, and load financial market data — fast and without fuss.

[![PyPI](https://img.shields.io/pypi/v/marketgoblin?color=green)](https://pypi.org/project/marketgoblin/)
![Python](https://img.shields.io/badge/python-3.13-blue)
![License](https://img.shields.io/badge/license-MIT-green)
[![CI](https://github.com/aexsalomao/marketgoblin/actions/workflows/ci.yml/badge.svg)](https://github.com/aexsalomao/marketgoblin/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/aexsalomao/marketgoblin/branch/master/graph/badge.svg)](https://codecov.io/gh/aexsalomao/marketgoblin)
[![Docs](https://img.shields.io/badge/docs-aexsalomao.github.io%2Fmarketgoblin-blue)](https://aexsalomao.github.io/marketgoblin)

**marketgoblin** is a lightweight market data platform built on [Polars](https://pola.rs/) and [yfinance](https://github.com/ranaroussi/yfinance). It fetches multiple datasets (OHLCV, shares-outstanding, dividends), slices them into monthly Parquet files, writes JSON sidecars with metadata, and lets you load them back with a single call.

---

## Features

- **Multi-dataset** — OHLCV, shares-outstanding, and dividends selected via a `Dataset` enum; per-source dispatch makes it easy to add more
- **Tidy stacked OHLCV** — adjusted and raw prices live in one frame, distinguished by an `is_adjusted` bool column; one network call per symbol covers both
- **Single-symbol and batch fetch** — `fetch()` and `fetch_many()` with thread-pool concurrency
- **Disk persistence** — monthly `.pq` slices with atomic writes; JSON sidecar per slice
- **Lazy evaluation** — all data paths return `pl.LazyFrame` (Polars)
- **Date flexibility** — dates stored as `int32` YYYYMMDD on disk; use `parse_dates=True` to get `pl.Date`
- **Retry logic** — `YahooSource` retries transient failures with exponential backoff (3 attempts)
- **Rate limiting** — `fetch_many()` respects a configurable requests-per-second cap (default: 2 req/s)
- **Input validation** — dates are validated before any I/O; unsupported `(provider, dataset)` pairs raise at the dispatch boundary
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
import polars as pl
from marketgoblin import Dataset, MarketGoblin

goblin = MarketGoblin(provider="yahoo", save_path="./data")

# Fetch and persist OHLCV — tidy stacked frame: each trading day appears
# twice (is_adjusted=True / False). Filter to pick a variant.
lf = goblin.fetch("AAPL", "2024-01-01", "2024-03-31", parse_dates=True)
adjusted = lf.filter(pl.col("is_adjusted")).collect()
print(adjusted)

# Load back from disk (no network call)
lf = goblin.load("AAPL", "2024-01-01", "2024-03-31", parse_dates=True)
print(lf.collect())

# Shares outstanding — sparse, corporate-action-driven series
shares = goblin.fetch("AAPL", "2024-01-01", "2024-03-31", dataset=Dataset.SHARES, parse_dates=True)
print(shares.collect())

# Dividends — event-driven (typically quarterly)
dividends = goblin.fetch("AAPL", "2024-01-01", "2024-03-31", dataset=Dataset.DIVIDENDS, parse_dates=True)
print(dividends.collect())

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
| `fetch(symbol, start, end, dataset=Dataset.OHLCV, parse_dates=False)` | Download, save to disk (if `save_path` set), return `LazyFrame` |
| `load(symbol, start, end, dataset=Dataset.OHLCV, parse_dates=False)` | Load from disk; raises `RuntimeError` if no `save_path` |
| `fetch_many(symbols, start, end, dataset=Dataset.OHLCV, parse_dates=False, max_workers=8, requests_per_second=2.0)` | Batch fetch via `ThreadPoolExecutor`, rate-limited |
| `supported_datasets` (property) | `frozenset[Dataset]` of datasets the configured provider supports |

### Datasets

| Dataset | Provider support | Columns |
|---|---|---|
| `Dataset.OHLCV` | `yahoo`, `csv` | `date` (int32), `open` / `high` / `low` / `close` (float32), `volume` (int64), `is_adjusted` (bool), `symbol` |
| `Dataset.SHARES` | `yahoo` | `date` (int32), `shares` (int64), `symbol` |
| `Dataset.DIVIDENDS` | `yahoo` | `date` (int32), `dividend` (float32), `symbol` |

OHLCV is returned as a tidy stacked frame: each trading day appears twice (`is_adjusted=True` and `is_adjusted=False`). Filter downstream (`.filter(pl.col("is_adjusted"))`) to pick a variant. Adjusted Open/High/Low are derived locally from the `Adj Close / Close` ratio — verified to match yfinance's `auto_adjust=True` output exactly while halving network calls.

### Data on disk

| Property | Detail |
|---|---|
| Date column | `int32` YYYYMMDD (e.g. `20240101`); `parse_dates=True` → `pl.Date` |
| OHLC columns | `float32` |
| Volume column | `int64` |
| Shares column | `int64` |
| Dividend column | `float32` |
| Parquet path | `{save_path}/{provider}/{dataset}/{SYMBOL}/{SYMBOL}_{YYYY-MM}.pq` |
| JSON sidecar | Same path, `.json` extension — row count, date range, per-dataset stats (OHLCV also records `has_adjusted`/`has_raw` and missing trading days) |

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

    def _fetch_ohlcv(self, symbol: str, start: str, end: str) -> pl.LazyFrame:
        ...  # return a normalized LazyFrame with an is_adjusted column
```

Per-dataset fetchers all share the `(symbol, start, end)` signature — there is no `adjusted` toggle, since OHLCV variants are stacked into a single frame distinguished by the `is_adjusted` column.

Then register it in `goblin.py`:

```python
_SOURCES = {"yahoo": YahooSource, "csv": CSVSource, "mysource": MySource}
```

A `CSVSource` is included out of the box for loading local CSV files (CSVs hold a single variant — pass `is_adjusted=...` to stamp the flag on every row):

```python
goblin = MarketGoblin(provider="csv", data_dir="./csv_files", is_adjusted=True)
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
