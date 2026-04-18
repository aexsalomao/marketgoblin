# marketgoblin

Market data platform for downloading, storing, and snapshotting financial data.
Supports multiple datasets (OHLCV, shares-outstanding, ...) via a per-source
dispatch layer.

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
    __init__.py           # exports MarketGoblin, Dataset; __version__
    datasets.py           # Dataset StrEnum (OHLCV, SHARES, ...)
    goblin.py             # MarketGoblin — public API facade
    _normalize.py         # normalize_ohlcv, normalize_shares, parse_dates — pure
    _metadata.py          # build_ohlcv, build_shares, write — pure
    sources/
        base.py           # BaseSource ABC + Fetcher type alias
        yahoo.py          # YahooSource — OHLCV + SHARES via yfinance
        csv_source.py     # CSVSource — OHLCV-only
    storage/
        disk.py           # DiskStorage — dataset-aware monthly .pq slices
tests/
    test_metadata.py
    test_normalize.py
    test_storage.py
    test_goblin.py
    test_csv_source.py
    test_yahoo.py         # YahooSource fetch tests (yfinance mocked)
docs/                     # MkDocs source (index.md, api.md, contributing.md, changelog.md)
example.py
mkdocs.yml
```

## Module APIs

### `datasets.py` — `Dataset`

```python
class Dataset(StrEnum):
    OHLCV = "ohlcv"
    SHARES = "shares"
```

`StrEnum` so members serialize directly to path segments and JSON. Public API
— exported from package root.

### `goblin.py` — `MarketGoblin`

```python
_SOURCES: dict[str, type[BaseSource]] = {"yahoo": YahooSource, "csv": CSVSource}

_validate_dates(start, end)              # bad format or start >= end → ValueError
_validate_dataset_args(dataset, adjusted) # adjusted=False with non-OHLCV → ValueError

class _RateLimiter:          # token-bucket, thread-safe; used in fetch_many()

class MarketGoblin:
    def __init__(self, provider, api_key=None, save_path=None, **source_kwargs)
    @property
    def supported_datasets(self) -> frozenset[Dataset]
    def fetch(self, symbol, start, end, dataset=Dataset.OHLCV, adjusted=True, parse_dates=False) -> pl.LazyFrame
    def load(self, symbol, start, end, dataset=Dataset.OHLCV, adjusted=True, parse_dates=False) -> pl.LazyFrame
    def fetch_many(self, symbols, start, end, dataset=Dataset.OHLCV, adjusted=True, parse_dates=False, max_workers=8, requests_per_second=2.0) -> dict[str, pl.LazyFrame]
```

- `dataset` defaults to `Dataset.OHLCV` so existing callers don't break
- `adjusted` only valid for OHLCV; passing `adjusted=False` with another dataset raises `ValueError` at the boundary (no silent fallback)
- `fetch()` validates dates + dataset args, downloads via source, saves to disk (if `save_path` set), returns `LazyFrame`
- `load()` requires `save_path`; raises `RuntimeError` otherwise
- `fetch_many()` uses `ThreadPoolExecutor` + `_RateLimiter`; failed symbols logged and excluded — never crashes the batch
- `**source_kwargs` are forwarded to the source constructor (e.g. `data_dir` for `CSVSource`)
- To add a provider: subclass `BaseSource`, implement `_build_dispatch()`, add to `_SOURCES`
- To add a dataset: extend `Dataset` enum, add `_fetch_<dataset>` method on relevant sources and register in their `_build_dispatch()`, add a `normalize_<dataset>` and `build_<dataset>` for storage

### `_normalize.py`

```python
_OHLC_COLS = ["open", "high", "low", "close"]

def normalize_ohlcv(lf)  -> pl.LazyFrame   # → float32 OHLC, int64 volume, int32 YYYYMMDD date
def normalize_shares(lf) -> pl.LazyFrame   # → int64 shares, int32 YYYYMMDD date
def parse_dates(lf)      -> pl.LazyFrame   # → int32 YYYYMMDD → pl.Date
```

Each `normalize_*` is dataset-specific. `parse_dates` works on any frame with an int32 `date` column.

### `_metadata.py`

```python
def build_ohlcv(chunk, provider, symbol, ym, file_size_bytes, price_adjusted=True, currency="USD") -> dict
def build_shares(chunk, provider, symbol, ym, file_size_bytes) -> dict
def write(metadata, path) -> None  # atomic via .tmp rename
```

`build_ohlcv()` computes row_count, date range, close/volume min/max, expected vs. missing trading days (weekday-based).
`build_shares()` computes row_count, date range, shares min/max — no missing-days analysis (shares cadence is irregular).
Both take `file_size_bytes: int` (caller reads `path.stat().st_size` after the atomic write).

### `sources/base.py` — `BaseSource`

```python
Fetcher = Callable[[str, str, str, bool], pl.LazyFrame]  # (symbol, start, end, adjusted)

class BaseSource(ABC):
    name: str
    def __init__(self, api_key=None, **kwargs)
    @abstractmethod
    def _build_dispatch(self) -> dict[Dataset, Fetcher]
    @property
    def supported_datasets(self) -> frozenset[Dataset]
    def fetch(self, dataset, symbol, start, end, adjusted=True) -> pl.LazyFrame
```

`fetch()` looks up the handler in `self._dispatch`; raises `ValueError` if the source doesn't support the requested dataset.
Per-dataset fetchers all share the `(symbol, start, end, adjusted)` signature; `adjusted` is OHLCV-specific and must be accepted-and-ignored by non-OHLCV fetchers.

### `sources/yahoo.py` — `YahooSource`

```python
class YahooSource(BaseSource):
    name = "yahoo"
    def _fetch_ohlcv(self, symbol, start, end, adjusted=True) -> pl.LazyFrame
    def _fetch_shares(self, symbol, start, end, adjusted=True) -> pl.LazyFrame
    def _retry_fetch(self, fetch_fn, symbol) -> pl.LazyFrame
```

- OHLCV: `yf.Ticker(symbol).history(auto_adjust=adjusted)`
- Shares: `yf.Ticker(symbol).get_shares_full(start, end)` — sparse, irregular series; deduplicated to one row per day (last value wins)
- `_retry_fetch` retries on transient errors with backoff (1 s, 2 s); `ValueError` (empty data) propagates immediately
- Both fetchers call the appropriate `normalize_*` before returning

### `sources/csv_source.py` — `CSVSource`

```python
class CSVSource(BaseSource):
    name = "csv"
    def __init__(self, api_key=None, data_dir=".", **kwargs)
    def _fetch_ohlcv(self, symbol, start, end, adjusted=True) -> pl.LazyFrame  # adjusted ignored
```

Reads `{data_dir}/{SYMBOL}.csv`. Expected columns: `date` (YYYY-MM-DD), `open`, `high`, `low`, `close`, `volume`, `symbol`. OHLCV-only — `Dataset.SHARES` raises `ValueError` at the dispatch layer.

### `storage/disk.py` — `DiskStorage`

```python
class DiskStorage:
    def __init__(self, base_path)
    def save(self, provider, symbol, dataset, lf, adjusted=True) -> None
    def load(self, provider, symbol, dataset, start, end, parse_dates=False, adjusted=True) -> pl.LazyFrame

    # private
    def _symbol_dir(self, provider, symbol, dataset, adjusted) -> Path
    def _slice_path(self, provider, symbol, dataset, ym, adjusted) -> Path
    def _build_metadata(self, chunk, provider, symbol, dataset, ym, file_size_bytes, adjusted) -> dict
    def _atomic_write(self, df, path) -> None
```

Path scheme is dataset-aware: OHLCV adds an `adjusted|raw` variant segment, SHARES does not.
`_build_metadata` dispatches to `build_ohlcv` / `build_shares`. Missing-days warnings only emitted for OHLCV.

## Data Conventions

- **Date on disk:** `int32` YYYYMMDD (e.g. `20240101`); use `parse_dates=True` to get `pl.Date`
- **OHLC:** `float32` | **Volume:** `int64` | **Shares:** `int64` (large-cap counts overflow int32)
- **Parquet paths:**
    - OHLCV:  `{base_path}/{provider}/ohlcv/{adjusted|raw}/{SYMBOL}/{SYMBOL}_{YYYY-MM}.pq`
    - SHARES: `{base_path}/{provider}/shares/{SYMBOL}/{SYMBOL}_{YYYY-MM}.pq`
- **JSON sidecar:** same path, `.json` extension — written atomically after each `.pq`
- **Atomic writes:** `.tmp` rename for both `.pq` and `.json`

## Import Graph

```
goblin.py
  ├── datasets.Dataset
  ├── _normalize.parse_dates
  ├── sources.yahoo.YahooSource     ──→ _normalize.normalize_ohlcv, normalize_shares
  │                                 ──→ sources.base.BaseSource, Fetcher
  │                                 ──→ datasets.Dataset
  ├── sources.csv_source.CSVSource  ──→ _normalize.normalize_ohlcv
  │                                 ──→ sources.base.BaseSource, Fetcher
  │                                 ──→ datasets.Dataset
  └── storage.disk.DiskStorage      ──→ _metadata.build_ohlcv, build_shares, write
                                    ──→ _normalize.parse_dates
                                    ──→ datasets.Dataset

datasets.py     (no local imports)
_normalize.py   (no local imports)
_metadata.py    (no local imports)
```

## Code Style

See `code-style.md` and `testing.md` in this directory for general Python conventions.

## Git Branches

- `{name}_fix_{description}` — bug fixes
- `{name}_dev_{description}` — new features
