# marketgoblin

Market data platform for downloading, storing, and snapshotting financial OHLCV data.

- **Python:** 3.13 | **Build:** uv_build | **License:** MIT
- **Core deps:** `polars>=1.0`, `yfinance>=0.2`, `pyarrow>=15.0`
- **Dev deps:** `pytest>=8.0`, `pytest-cov>=5.0`, `ruff>=0.8.0`, `mypy>=1.10.0`, `pre-commit>=3.7.0`, `mkdocs>=1.6.0`, `mkdocs-material>=9.5.0`

## Setup & Tests

```bash
uv sync --extra dev
pre-commit install       # install ruff + mypy hooks
pytest
pytest --cov=marketgoblin  # with coverage
```

## Tooling

- **Lint + format:** `ruff check . && ruff format .` — configured in `[tool.ruff]` in `pyproject.toml`
- **Type checking:** `mypy src/` — configured in `[tool.mypy]` (strict mode, excludes tests/)
- **Pre-commit:** `.pre-commit-config.yaml` runs ruff and mypy on every commit
- **CI:** `.github/workflows/ci.yml` — lint → format → typecheck → pytest + Codecov on push/PR
- **Docs:** `.github/workflows/docs.yml` — deploys MkDocs to GitHub Pages on push to master

## Example Runner

`example.py` is the canonical manual test. Keep it up to date as the API evolves. The variable referencing `MarketGoblin(...)` is named `goblin`.

## Project Layout

```
src/marketgoblin/
    __init__.py           # exports MarketGoblin; __version__ = "0.1.0"
    goblin.py              # MarketGoblin — public API facade
    _normalize.py         # normalize() + parse_dates() — pure, no local imports
    _metadata.py          # build() + write() — pure, no local imports
    sources/
        base.py           # BaseSource ABC
        yahoo.py          # YahooSource (yfinance) — retries with backoff
        csv_source.py     # CSVSource — reads local CSV files
    storage/
        disk.py           # DiskStorage — monthly .pq slices + JSON sidecars
tests/
    test_metadata.py
    test_normalize.py
    test_storage.py
    test_goblin.py
    test_csv_source.py
docs/                     # MkDocs source (index.md, api.md, contributing.md, changelog.md)
example.py
mkdocs.yml
```

## Module APIs

### `goblin.py` — `MarketGoblin`

```python
_SOURCES: dict[str, type[BaseSource]] = {"yahoo": YahooSource, "csv": CSVSource}

_validate_dates(start, end)  # raises ValueError for bad format or start >= end

class _RateLimiter:          # token-bucket, thread-safe; used in fetch_many()
    def __init__(self, requests_per_second: float)
    def acquire(self) -> None

class MarketGoblin:
    def __init__(self, provider: str, api_key: str | None = None, save_path: str | Path | None = None, **source_kwargs)
    def fetch(self, symbol, start, end, adjusted=True, parse_dates=False) -> pl.LazyFrame
    def load(self, symbol, start, end, adjusted=True, parse_dates=False) -> pl.LazyFrame
    def fetch_many(self, symbols, start, end, adjusted=True, parse_dates=False, max_workers=8, requests_per_second=2.0) -> dict[str, pl.LazyFrame]
```

- `fetch()` validates dates, downloads via source, saves to disk (if `save_path` set), returns `LazyFrame`
- `load()` requires `save_path`; raises `RuntimeError` otherwise
- `fetch_many()` uses `ThreadPoolExecutor` + `_RateLimiter`; failed symbols logged and excluded — never crashes the batch
- `**source_kwargs` are forwarded to the source constructor (e.g. `data_dir` for `CSVSource`)
- To add a provider: subclass `BaseSource`, implement `fetch()`, add to `_SOURCES`

### `_normalize.py`

```python
_OHLC_COLS = ["open", "high", "low", "close"]

def normalize(lf: pl.LazyFrame) -> pl.LazyFrame   # → float32 OHLC, int64 volume, int32 YYYYMMDD date
def parse_dates(lf: pl.LazyFrame) -> pl.LazyFrame  # → int32 YYYYMMDD → pl.Date
```

Called by `YahooSource.fetch()` and `CSVSource.fetch()` (normalize), and `DiskStorage.load()` / `goblin.py` (parse_dates).

### `_metadata.py`

```python
def build(chunk, provider, symbol, ym, file_size_bytes, price_adjusted=True, currency="USD") -> dict[str, Any]
def write(metadata: dict[str, Any], path: Path) -> None  # atomic via .tmp rename
```

`build()` takes `file_size_bytes: int` (not `path`) — the caller reads `path.stat().st_size` after the atomic write. Computes: row_count, date range, close/volume min/max, expected vs. missing trading days (weekday-based). `write()` writes `.json` sidecar next to the `.pq` file.

### `sources/base.py` — `BaseSource`

```python
class BaseSource(ABC):
    name: str
    def __init__(self, api_key: str | None = None, **kwargs: Any)
    @abstractmethod
    def fetch(self, symbol, start, end, adjusted=True) -> pl.LazyFrame
```

`**kwargs` is accepted and ignored by the base so subclasses can add their own constructor params without breaking the registry instantiation pattern.

### `sources/yahoo.py` — `YahooSource`

```python
class YahooSource(BaseSource):
    name = "yahoo"
    def fetch(self, symbol, start, end, adjusted=True) -> pl.LazyFrame
```

Uses `yf.Ticker(symbol).history(auto_adjust=adjusted)`. Raises `ValueError` if yfinance returns empty. Retries up to 3 times with exponential backoff (1 s, 2 s) on network errors. Calls `normalize()` before returning.

### `sources/csv_source.py` — `CSVSource`

```python
class CSVSource(BaseSource):
    name = "csv"
    def __init__(self, api_key: str | None = None, data_dir: str | Path = ".", **kwargs: Any)
    def fetch(self, symbol, start, end, adjusted=True) -> pl.LazyFrame
```

Reads `{data_dir}/{SYMBOL}.csv`. Expected columns: `date` (YYYY-MM-DD), `open`, `high`, `low`, `close`, `volume`, `symbol`. Raises `ValueError` if file not found. Calls `normalize()` before returning.

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

`save()` splits by month (`_ym` temp column), calls `_atomic_write` + `_build_metadata(file_size_bytes=path.stat().st_size)` + `_write_metadata` per slice. `load()` uses `pl.scan_parquet(pattern)` with `date.is_between(start_int, end_int)`.

## Data Conventions

- **Date on disk:** `int32` YYYYMMDD (e.g. `20240101`); use `parse_dates=True` to get `pl.Date`
- **OHLC:** `float32` | **Volume:** `int64`
- **Parquet path:** `{base_path}/{provider}/ohlcv/{adjusted|raw}/{SYMBOL}/{SYMBOL}_{YYYY-MM}.pq`
- **JSON sidecar:** same path, `.json` extension — written atomically after each `.pq`
- **Atomic writes:** `.tmp` rename for both `.pq` and `.json`

## Import Graph

```
goblin.py
  ├── _normalize.parse_dates
  ├── sources.yahoo.YahooSource  ──→  _normalize.normalize
  │                              ──→  sources.base.BaseSource
  ├── sources.csv_source.CSVSource ──→ _normalize.normalize
  │                                ──→ sources.base.BaseSource
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
- **Ruff** enforces PEP 8 + isort + pyupgrade; **mypy** strict mode on `src/`
- Apply design patterns only where they genuinely reduce complexity

## Git Branches

- `{name}_fix_{description}` — bug fixes
- `{name}_dev_{description}` — new features
