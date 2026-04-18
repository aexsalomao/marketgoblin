# API Reference

## `Dataset`

Enum of datasets the platform can fetch and store. Exported from the package root.

```python
from marketgoblin import Dataset

Dataset.OHLCV   # "ohlcv"
Dataset.SHARES  # "shares"
```

`Dataset` is a `StrEnum`, so members serialize directly to path segments and JSON.

| Member | Columns | Providers |
|---|---|---|
| `Dataset.OHLCV` | `date` (int32 YYYYMMDD), `open` / `high` / `low` / `close` (float32), `volume` (int64), `symbol` | `yahoo`, `csv` |
| `Dataset.SHARES` | `date` (int32 YYYYMMDD), `shares` (int64), `symbol` | `yahoo` |

`adjusted` only applies to OHLCV (split/dividend price adjustment). Passing
`adjusted=False` together with any other dataset raises `ValueError` at the
public API boundary — there are no silent fallbacks.

---

## `MarketGoblin`

The main entry point for all data operations.

```python
from marketgoblin import Dataset, MarketGoblin

goblin = MarketGoblin(provider="yahoo", save_path="./data")
```

### Constructor

```python
MarketGoblin(
    provider: str,
    api_key: str | None = None,
    save_path: str | Path | None = None,
    **source_kwargs,
)
```

| Parameter | Description |
|---|---|
| `provider` | Data source name: `"yahoo"` or `"csv"` |
| `api_key` | API key for providers that require one (not needed for Yahoo) |
| `save_path` | Root directory for disk persistence. Required for `load()`. |
| `**source_kwargs` | Extra keyword arguments forwarded to the source constructor (e.g. `data_dir` for `CSVSource`) |

### Properties

#### `supported_datasets`

```python
supported_datasets: frozenset[Dataset]
```

Datasets that the configured provider can fetch. For `"yahoo"`:
`{Dataset.OHLCV, Dataset.SHARES}`. For `"csv"`: `{Dataset.OHLCV}`.

### Methods

#### `fetch()`

```python
fetch(
    symbol: str,
    start: str,
    end: str,
    dataset: Dataset = Dataset.OHLCV,
    adjusted: bool = True,
    parse_dates: bool = False,
) -> pl.LazyFrame
```

Downloads data for the requested `dataset`. If `save_path` is set, persists monthly Parquet slices to disk and returns data loaded back from disk.

#### `load()`

```python
load(
    symbol: str,
    start: str,
    end: str,
    dataset: Dataset = Dataset.OHLCV,
    adjusted: bool = True,
    parse_dates: bool = False,
) -> pl.LazyFrame
```

Loads previously saved data from disk. Raises `RuntimeError` if `save_path` was not set.

#### `fetch_many()`

```python
fetch_many(
    symbols: list[str],
    start: str,
    end: str,
    dataset: Dataset = Dataset.OHLCV,
    adjusted: bool = True,
    parse_dates: bool = False,
    max_workers: int = 8,
    requests_per_second: float = 2.0,
) -> dict[str, pl.LazyFrame]
```

Batch fetch using a `ThreadPoolExecutor`. Failed symbols are logged and excluded — they never crash the batch. Rate-limited to `requests_per_second`.

---

## `BaseSource`

Abstract base class for data sources. Concrete sources declare which datasets
they support by returning a dispatch table from `_build_dispatch()`; the base
class handles lookup and error reporting.

```python
import polars as pl

from marketgoblin import Dataset
from marketgoblin.sources.base import BaseSource, Fetcher

class MySource(BaseSource):
    name = "mysource"

    def _build_dispatch(self) -> dict[Dataset, Fetcher]:
        return {Dataset.OHLCV: self._fetch_ohlcv}

    def _fetch_ohlcv(
        self, symbol: str, start: str, end: str, adjusted: bool = True
    ) -> pl.LazyFrame:
        ...
```

The `Fetcher` signature is `(symbol, start, end, adjusted) -> pl.LazyFrame`. `adjusted` is OHLCV-specific and should be accepted-and-ignored by non-OHLCV fetchers.

Register in `goblin.py`:

```python
_SOURCES = {"yahoo": YahooSource, "csv": CSVSource, "mysource": MySource}
```

---

## `YahooSource`

Backed by [yfinance](https://github.com/ranaroussi/yfinance). Supports both `Dataset.OHLCV` and `Dataset.SHARES`.

- **OHLCV:** `yf.Ticker(symbol).history(auto_adjust=adjusted)`.
- **SHARES:** `yf.Ticker(symbol).get_shares_full(start, end)` — sparse, corporate-action-driven; deduplicated to one row per day (last value wins).

Transient failures are retried with exponential backoff (3 attempts, 1 s / 2 s delays). Empty-data `ValueError`s propagate immediately.

---

## `CSVSource`

Reads OHLCV data from local CSV files. Useful for backtesting or offline use. `Dataset.SHARES` is not supported and raises `ValueError` at the dispatch layer.

Expected CSV columns: `date` (YYYY-MM-DD), `open`, `high`, `low`, `close`, `volume`, `symbol`.

```python
goblin = MarketGoblin(provider="csv", save_path="./data", data_dir="./csv_files")
lf = goblin.fetch("AAPL", "2024-01-01", "2024-03-31")
```

Looks for `{data_dir}/AAPL.csv`.

---

## `DiskStorage`

Handles Parquet persistence. Used internally by `MarketGoblin` when `save_path` is set. Path scheme is dataset-aware:

- **OHLCV:** `{base_path}/{provider}/ohlcv/{adjusted|raw}/{SYMBOL}/{SYMBOL}_{YYYY-MM}.pq`
- **SHARES:** `{base_path}/{provider}/shares/{SYMBOL}/{SYMBOL}_{YYYY-MM}.pq` (no `adjusted|raw` segment — price adjustment is meaningless for share counts)

Each Parquet file has a JSON sidecar at the same path with `.json` extension.

### OHLCV sidecar

| Key | Description |
|---|---|
| `row_count` | Number of rows in this slice |
| `start_date` / `end_date` | First and last date as YYYYMMDD |
| `expected_trading_days` | Weekday count for the month |
| `missing_days` | Weekdays not present in the data (likely holidays) |
| `close_min` / `close_max` | Close price range |
| `volume_min` / `volume_max` | Volume range |
| `price_adjusted` | Whether prices are split/dividend-adjusted |
| `currency` | Price currency (default `"USD"`) |
| `downloaded_at` | ISO 8601 timestamp |
| `file_size_bytes` | Size of the `.pq` file |

### SHARES sidecar

| Key | Description |
|---|---|
| `row_count` | Number of rows in this slice |
| `start_date` / `end_date` | First and last date as YYYYMMDD |
| `shares_min` / `shares_max` | Share-count range |
| `downloaded_at` | ISO 8601 timestamp |
| `file_size_bytes` | Size of the `.pq` file |

No missing-days analysis for SHARES — cadence is corporate-action-driven and irregular, so absence of a date is not a signal.
