# marketgoblin

Market data platform for downloading, storing, and snapshotting financial OHLCV data.

- **Python:** 3.13 | **Build:** uv_build | **License:** MIT
- **Core deps:** `polars>=1.0`, `yfinance>=0.2`, `pyarrow>=15.0`
- **Dev deps:** `pytest>=8.0`, `pytest-cov>=5.0`

## Setup & Tests

```bash
uv sync --extra dev
pytest
pytest --cov=marketgoblin  # with coverage
```

## Example Runner

`example.py` is the canonical manual test. Keep it up to date as the API evolves. The variable referencing `MarketGoblin(...)` is named `goblin`.

## Project Layout

```
src/marketgoblin/
    __init__.py           # exports MarketGoblin; __version__ = "0.1.0"
    vault.py              # MarketGoblin — public API facade
    _normalize.py         # normalize() + parse_dates() — pure, no local imports
    _metadata.py          # build() + write() — pure, no local imports
    sources/
        base.py           # BaseSource ABC
        yahoo.py          # YahooSource (yfinance)
    storage/
        disk.py           # DiskStorage — monthly .pq slices + JSON sidecars
tests/
    test_metadata.py
    test_normalize.py
    test_storage.py
    test_vault.py
example.py
```

## Module APIs

### `vault.py` — `MarketGoblin`

```python
_SOURCES: dict[str, type[BaseSource]] = {"yahoo": YahooSource}

class MarketGoblin:
    def __init__(self, provider: str, api_key: str | None = None, save_path: str | Path | None = None)
    def fetch(self, symbol, start, end, adjusted=True, parse_dates=False) -> pl.LazyFrame
    def load(self, symbol, start, end, adjusted=True, parse_dates=False) -> pl.LazyFrame
    def fetch_many(self, symbols, start, end, adjusted=True, parse_dates=False, max_workers=8) -> dict[str, pl.LazyFrame]
```

- `fetch()` downloads via the source, saves to disk (if `save_path` set), returns `LazyFrame`
- `load()` requires `save_path`; raises `RuntimeError` otherwise
- `fetch_many()` uses `ThreadPoolExecutor`; failed symbols are logged and excluded — never crash the batch
- To add a provider: subclass `BaseSource`, implement `fetch()`, add to `_SOURCES`

### `_normalize.py`

```python
_NUMERIC_COLS = ["open", "high", "low", "close", "volume"]

def normalize(lf: pl.LazyFrame) -> pl.LazyFrame   # → float32 OHLC, int64 volume, int32 YYYYMMDD date
def parse_dates(lf: pl.LazyFrame) -> pl.LazyFrame  # → int32 YYYYMMDD → pl.Date
```

Called by `YahooSource.fetch()` (normalize) and `DiskStorage.load()` / `vault.py` (parse_dates).

### `_metadata.py`

```python
def build(chunk, provider, symbol, ym, path, price_adjusted=True, currency="USD") -> dict
def write(metadata: dict, path: Path) -> None  # atomic via .tmp rename
```

`build()` computes: row_count, date range, close/volume min/max, expected vs. missing trading days (weekday-based). `write()` writes `.json` sidecar next to the `.pq` file.

### `sources/base.py` — `BaseSource`

```python
class BaseSource(ABC):
    name: str
    def __init__(self, api_key: str | None = None)
    @abstractmethod
    def fetch(self, symbol, start, end, adjusted=True) -> pl.LazyFrame
```

### `sources/yahoo.py` — `YahooSource`

```python
class YahooSource(BaseSource):
    name = "yahoo"
    def fetch(self, symbol, start, end, adjusted=True) -> pl.LazyFrame
```

Uses `yf.Ticker(symbol).history(auto_adjust=adjusted)`. Raises `ValueError` if yfinance returns empty. Calls `normalize()` before returning.

### `storage/disk.py` — `DiskStorage`

```python
class DiskStorage:
    def __init__(self, base_path: str | Path)
    def save(self, provider, symbol, lf, adjusted=True) -> None
    def load(self, provider, symbol, start, end, parse_dates=False, adjusted=True) -> pl.LazyFrame

    # private
    def _symbol_dir(self, provider, symbol, adjusted) -> Path
    def _slice_path(self, provider, symbol, ym, adjusted) -> Path
    def _atomic_write(self, df, path) -> None
```

`save()` splits by month (`_ym` temp column), calls `_atomic_write` + `_build_metadata` + `_write_metadata` per slice. `load()` uses `pl.scan_parquet(pattern)` with `date.is_between(start_int, end_int)`.

## Data Conventions

- **Date on disk:** `int32` YYYYMMDD (e.g. `20240101`); use `parse_dates=True` to get `pl.Date`
- **OHLC:** `float32` | **Volume:** `int64`
- **Parquet path:** `{base_path}/{provider}/ohlcv/{adjusted|raw}/{SYMBOL}/{SYMBOL}_{YYYY-MM}.pq`
- **JSON sidecar:** same path, `.json` extension — written atomically after each `.pq`
- **Atomic writes:** `.tmp` rename for both `.pq` and `.json`

## Import Graph

```
vault.py
  ├── _normalize.parse_dates
  ├── sources.yahoo.YahooSource  ──→  _normalize.normalize
  │                              ──→  sources.base.BaseSource
  └── storage.disk.DiskStorage   ──→  _metadata.build, _metadata.write
                                 ──→  _normalize.parse_dates

_normalize.py   (no local imports)
_metadata.py    (no local imports)
```

## Code Style

- **Simple first:** simplest correct implementation before optimizing
- **Polars over pandas** everywhere; **parquet over CSV/JSON** for persistence
- **Pure functions** preferred; inject dependencies for testability
- **Docstrings** on public APIs where behavior isn't obvious from the signature
- Apply design patterns only where they genuinely reduce complexity

## Git Branches

- `{name}_fix_{description}` — bug fixes
- `{name}_dev_{description}` — new features
