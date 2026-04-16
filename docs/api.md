# API Reference

## `MarketGoblin`

The main entry point for all data operations.

```python
from marketgoblin import MarketGoblin

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

### Methods

#### `fetch()`

```python
fetch(
    symbol: str,
    start: str,
    end: str,
    adjusted: bool = True,
    parse_dates: bool = False,
) -> pl.LazyFrame
```

Downloads OHLCV data. If `save_path` is set, persists monthly Parquet slices to disk and returns data loaded back from disk.

#### `load()`

```python
load(
    symbol: str,
    start: str,
    end: str,
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
    adjusted: bool = True,
    parse_dates: bool = False,
    max_workers: int = 8,
    requests_per_second: float = 2.0,
) -> dict[str, pl.LazyFrame]
```

Batch fetch using a `ThreadPoolExecutor`. Failed symbols are logged and excluded — they never crash the batch. Rate-limited to `requests_per_second`.

---

## `BaseSource`

Abstract base class for data sources.

```python
from marketgoblin.sources.base import BaseSource
import polars as pl

class MySource(BaseSource):
    name = "mysource"

    def fetch(self, symbol: str, start: str, end: str, adjusted: bool = True) -> pl.LazyFrame:
        ...
```

Register in `vault.py`:

```python
_SOURCES = {"yahoo": YahooSource, "csv": CSVSource, "mysource": MySource}
```

---

## `CSVSource`

Reads OHLCV data from local CSV files. Useful for backtesting or offline use.

Expected CSV columns: `date` (YYYY-MM-DD), `open`, `high`, `low`, `close`, `volume`, `symbol`.

```python
goblin = MarketGoblin(provider="csv", save_path="./data", data_dir="./csv_files")
lf = goblin.fetch("AAPL", "2024-01-01", "2024-03-31")
```

Looks for `{data_dir}/AAPL.csv`.

---

## `DiskStorage`

Handles Parquet persistence. Used internally by `MarketGoblin` when `save_path` is set.

**Layout:** `{base_path}/{provider}/ohlcv/{adjusted|raw}/{SYMBOL}/{SYMBOL}_{YYYY-MM}.pq`

Each Parquet file has a JSON sidecar at the same path with `.json` extension containing:

| Key | Description |
|---|---|
| `row_count` | Number of rows in this slice |
| `start_date` / `end_date` | First and last date as YYYYMMDD |
| `expected_trading_days` | Weekday count for the month |
| `missing_days` | Weekdays not present in the data (likely holidays) |
| `close_min` / `close_max` | Close price range |
| `volume_min` / `volume_max` | Volume range |
| `downloaded_at` | ISO 8601 timestamp |
| `file_size_bytes` | Size of the `.pq` file |
