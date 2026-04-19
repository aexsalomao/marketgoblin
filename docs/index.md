---
hide:
  - navigation
  - toc
---

<div align="center" markdown>

# marketgoblin

**Download, store, and load financial market data — fast, without fuss.**

[![PyPI](https://img.shields.io/pypi/v/marketgoblin?color=green)](https://pypi.org/project/marketgoblin/)
[![Python](https://img.shields.io/pypi/pyversions/marketgoblin)](https://pypi.org/project/marketgoblin/)
[![License](https://img.shields.io/github/license/aexsalomao/marketgoblin)](https://github.com/aexsalomao/marketgoblin/blob/master/LICENSE)
[![CI](https://github.com/aexsalomao/marketgoblin/actions/workflows/ci.yml/badge.svg)](https://github.com/aexsalomao/marketgoblin/actions)

```bash
pip install marketgoblin
```

</div>

---

## Features

<div class="grid cards" markdown>

-   :material-download-circle:{ .lg .middle } **Multi-dataset fetch & persist**

    ---

    Download OHLCV, shares-outstanding, or dividends by symbol and date range via a `Dataset` enum. When `save_path` is set, data is automatically sliced into monthly Parquet files on disk.

    [:octicons-arrow-right-24: API Reference](api.md)

-   :material-lightning-bolt:{ .lg .middle } **Lazy by default**

    ---

    Built on [Polars](https://pola.rs/). Every data path returns a `pl.LazyFrame` — nothing is computed until you call `.collect()`.

-   :material-sync:{ .lg .middle } **Batch fetching**

    ---

    `fetch_many()` uses a `ThreadPoolExecutor` with a token-bucket rate limiter. Failed symbols are logged and skipped — they never crash the batch.

-   :material-puzzle-outline:{ .lg .middle } **Pluggable sources**

    ---

    Subclass `BaseSource`, implement one method, register in one line. Ships with `YahooSource` and `CSVSource` out of the box.

-   :material-shield-check:{ .lg .middle } **Reliable by default**

    ---

    `YahooSource` retries transient failures with exponential backoff. Writes are atomic (`.tmp` rename). JSON sidecars record metadata per slice.

-   :material-database:{ .lg .middle } **Predictable storage layout**

    ---

    Uniform across datasets: `{save_path}/{provider}/{dataset}/{SYMBOL}/{SYMBOL}_{YYYY-MM}.pq`. OHLCV adjusted + raw share one file, distinguished by the `is_adjusted` column. Every slice is inspectable and portable with any Parquet reader.

</div>

---

## Quick start

=== "Fetch & save"

    ```python
    import polars as pl
    from marketgoblin import MarketGoblin

    goblin = MarketGoblin(provider="yahoo", save_path="./data")

    # OHLCV is a tidy stacked frame — each day appears twice
    # (is_adjusted=True / False). Filter to pick a variant.
    lf = goblin.fetch("AAPL", "2024-01-01", "2024-03-31", parse_dates=True)
    print(lf.filter(pl.col("is_adjusted")).collect())
    ```

=== "Shares outstanding"

    ```python
    from marketgoblin import Dataset, MarketGoblin

    goblin = MarketGoblin(provider="yahoo", save_path="./data")

    shares = goblin.fetch(
        "AAPL",
        "2024-01-01",
        "2024-03-31",
        dataset=Dataset.SHARES,
        parse_dates=True,
    )
    print(shares.collect())
    ```

=== "Dividends"

    ```python
    from marketgoblin import Dataset, MarketGoblin

    goblin = MarketGoblin(provider="yahoo", save_path="./data")

    dividends = goblin.fetch(
        "AAPL",
        "2024-01-01",
        "2024-03-31",
        dataset=Dataset.DIVIDENDS,
        parse_dates=True,
    )
    print(dividends.collect())
    ```

=== "Load from disk"

    ```python
    from marketgoblin import MarketGoblin

    goblin = MarketGoblin(provider="yahoo", save_path="./data")

    # No network call — reads straight from Parquet
    lf = goblin.load("AAPL", "2024-01-01", "2024-03-31", parse_dates=True)
    print(lf.collect())
    ```

=== "Batch fetch"

    ```python
    from marketgoblin import MarketGoblin

    goblin = MarketGoblin(provider="yahoo", save_path="./data")

    results = goblin.fetch_many(
        ["AAPL", "MSFT", "GOOGL", "NVDA"],
        start="2024-01-01",
        end="2024-03-31",
        max_workers=4,
        requests_per_second=2.0,
    )

    for symbol, lf in results.items():
        print(f"{symbol}: {lf.collect().height} rows")
    ```

=== "Custom CSV source"

    ```python
    from marketgoblin import MarketGoblin

    # Reads {data_dir}/AAPL.csv
    # Expected columns: date, open, high, low, close, volume, symbol
    goblin = MarketGoblin(provider="csv", data_dir="./csv_files")

    lf = goblin.fetch("AAPL", "2024-01-01", "2024-03-31")
    print(lf.collect())
    ```

<div align="center" markdown>

[:octicons-arrow-right-24: Full API Reference](api.md){ .md-button .md-button--primary }
[:octicons-arrow-right-24: Contributing](contributing.md){ .md-button }

</div>

---

## Data conventions

| Property | Value |
|---|---|
| Date on disk | `int32` YYYYMMDD (e.g. `20240101`) — use `parse_dates=True` to get `pl.Date` |
| OHLC columns | `float32` |
| Volume column | `int64` |
| `is_adjusted` column | `bool` (OHLCV only — distinguishes adjusted vs raw rows) |
| Shares column | `int64` |
| Dividend column | `float32` |
| Parquet path | `{save_path}/{provider}/{dataset}/{SYMBOL}/{SYMBOL}_{YYYY-MM}.pq` (uniform across datasets) |
| JSON sidecar | Same path, `.json` — row count, date range, per-dataset stats (OHLCV also records `has_adjusted`/`has_raw` and missing trading days) |

## Datasets

| Dataset | Providers | Notes |
|---|---|---|
| `Dataset.OHLCV` | `yahoo`, `csv` | Daily open/high/low/close/volume. Tidy stacked: each day appears twice with `is_adjusted=True`/`False`. Adjusted Open/High/Low are derived locally from `Adj Close / Close` (zero numerical drift vs `auto_adjust=True`, half the network calls). |
| `Dataset.SHARES` | `yahoo` | Shares outstanding — sparse, corporate-action-driven cadence. Deduplicated to one row per day (last value wins). |
| `Dataset.DIVIDENDS` | `yahoo` | Cash dividend events — typically quarterly. Filtered to the requested `[start, end]` range. |
