# marketgoblin

Market data platform for downloading, storing, and snapshotting financial data.
Supports multiple datasets (OHLCV, shares-outstanding, dividends, ...) via a
per-source dispatch layer.

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
    datasets.py           # Dataset StrEnum (OHLCV, SHARES, DIVIDENDS, ...)
    goblin.py             # MarketGoblin — public API facade
    _normalize.py         # normalize_ohlcv, normalize_shares, normalize_dividends, parse_dates — pure
    _metadata.py          # build_ohlcv, build_shares, build_dividends, write — pure
    sources/
        base.py           # BaseSource ABC + Fetcher type alias
        yahoo.py          # YahooSource — OHLCV + SHARES + DIVIDENDS via yfinance
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
    DIVIDENDS = "dividends"
```

`StrEnum` so members serialize directly to path segments and JSON. Public API
— exported from package root.

### `goblin.py` — `MarketGoblin`

```python
_SOURCES: dict[str, type[BaseSource]] = {"yahoo": YahooSource, "csv": CSVSource}

_validate_dates(start, end)              # bad format or start >= end → ValueError

class _RateLimiter:          # token-bucket, thread-safe; used in fetch_many()

class MarketGoblin:
    def __init__(self, provider, api_key=None, save_path=None, **source_kwargs)
    @property
    def supported_datasets(self) -> frozenset[Dataset]
    def fetch(self, symbol, start, end, dataset=Dataset.OHLCV, parse_dates=False) -> pl.LazyFrame
    def load(self, symbol, start, end, dataset=Dataset.OHLCV, parse_dates=False) -> pl.LazyFrame
    def fetch_many(self, symbols, start, end, dataset=Dataset.OHLCV, parse_dates=False, max_workers=8, requests_per_second=2.0) -> dict[str, pl.LazyFrame]
```

- `dataset` defaults to `Dataset.OHLCV` so existing callers don't break
- OHLCV is returned as a tidy stacked frame with an `is_adjusted: bool` column — each trading day appears twice (adjusted + raw). Filter downstream (`.filter(pl.col("is_adjusted"))`) to pick a variant. No `adjusted` parameter on the public API.
- `fetch()` validates dates, downloads via source, saves to disk (if `save_path` set), returns `LazyFrame`
- `load()` requires `save_path`; raises `RuntimeError` otherwise
- `fetch_many()` uses `ThreadPoolExecutor` + `_RateLimiter`; failed symbols logged and excluded — never crashes the batch
- `**source_kwargs` are forwarded to the source constructor (e.g. `data_dir`, `is_adjusted` for `CSVSource`)
- To add a provider: subclass `BaseSource`, implement `_build_dispatch()`, add to `_SOURCES`
- To add a dataset: extend `Dataset` enum, add `_fetch_<dataset>` method on relevant sources and register in their `_build_dispatch()`, add a `normalize_<dataset>` and `build_<dataset>` for storage, and extend `DiskStorage._build_metadata` dispatch

### `_normalize.py`

```python
_OHLC_COLS = ["open", "high", "low", "close"]

def normalize_ohlcv(lf)     -> pl.LazyFrame   # → float32 OHLC, int64 volume, bool is_adjusted, int32 YYYYMMDD date
def normalize_shares(lf)    -> pl.LazyFrame   # → int64 shares, int32 YYYYMMDD date
def normalize_dividends(lf) -> pl.LazyFrame   # → float32 dividend, int32 YYYYMMDD date
def parse_dates(lf)         -> pl.LazyFrame   # → int32 YYYYMMDD → pl.Date
```

Each `normalize_*` is dataset-specific. `parse_dates` works on any frame with an int32 `date` column.

### `_metadata.py`

```python
def build_ohlcv(chunk, provider, symbol, ym, file_size_bytes, currency="USD") -> dict
def build_shares(chunk, provider, symbol, ym, file_size_bytes) -> dict
def build_dividends(chunk, provider, symbol, ym, file_size_bytes, currency="USD") -> dict
def write(metadata, path) -> None  # atomic via .tmp rename
```

`build_ohlcv()` computes row_count + unique_days (stacked OHLCV has 2 rows per date), date range, close/volume min/max, expected vs. missing trading days (weekday-based, computed on unique dates), and `has_adjusted`/`has_raw` flags describing which variants are present in the slice.
`build_shares()` computes row_count, date range, shares min/max — no missing-days analysis (shares cadence is irregular).
`build_dividends()` computes row_count, date range, dividend min/max/total — no missing-days analysis (dividends are event-driven).
All take `file_size_bytes: int` (caller reads `path.stat().st_size` after the atomic write).

### `sources/base.py` — `BaseSource`

```python
Fetcher = Callable[[str, str, str], pl.LazyFrame]  # (symbol, start, end)

class BaseSource(ABC):
    name: str
    def __init__(self, api_key=None, **kwargs)
    @abstractmethod
    def _build_dispatch(self) -> dict[Dataset, Fetcher]
    @property
    def supported_datasets(self) -> frozenset[Dataset]
    def fetch(self, dataset, symbol, start, end) -> pl.LazyFrame
```

`fetch()` looks up the handler in `self._dispatch`; raises `ValueError` if the source doesn't support the requested dataset.
Per-dataset fetchers all share the `(symbol, start, end)` signature — no `adjusted` toggle, since OHLCV is returned as a tidy stacked frame containing both variants.

### `sources/yahoo.py` — `YahooSource`

```python
class YahooSource(BaseSource):
    name = "yahoo"
    def _fetch_ohlcv(self, symbol, start, end) -> pl.LazyFrame
    def _fetch_shares(self, symbol, start, end) -> pl.LazyFrame
    def _fetch_dividends(self, symbol, start, end) -> pl.LazyFrame
    def _retry_fetch(self, fetch_fn, symbol) -> pl.LazyFrame
```

- OHLCV: one `yf.Ticker(symbol).history(auto_adjust=False, actions=False)` call returns raw OHLC + `Adj Close` + Volume. Adjusted Open/High/Low are derived locally via `ratio = Adj Close / Close`; adjusted Close == Adj Close; Volume is identical across variants. This matches yfinance's own `auto_adjust=True` output exactly (verified zero numerical drift) while halving the network load. Output is concatenated with `is_adjusted=True`/`False` rows stacked, sorted by `(date, is_adjusted)`.
- Shares: `yf.Ticker(symbol).get_shares_full(start, end)` — sparse, irregular series; deduplicated to one row per day (last value wins)
- Dividends: `yf.Ticker(symbol).dividends` returns the full series; filtered to `[start, end]` after normalization
- `_retry_fetch` retries on transient errors with backoff (1 s, 2 s); `ValueError` (empty data) propagates immediately
- Each fetcher calls the appropriate `normalize_*` before returning

### `sources/csv_source.py` — `CSVSource`

```python
class CSVSource(BaseSource):
    name = "csv"
    def __init__(self, api_key=None, data_dir=".", is_adjusted=True, **kwargs)
    def _fetch_ohlcv(self, symbol, start, end) -> pl.LazyFrame
```

Reads `{data_dir}/{SYMBOL}.csv`. Expected columns: `date` (YYYY-MM-DD), `open`, `high`, `low`, `close`, `volume`, `symbol`. CSVs are assumed to hold a single variant (adjusted or raw) — `CSVSource` stamps the configured `is_adjusted` flag on every row. OHLCV-only — other datasets raise `ValueError` at the dispatch layer.

### `storage/disk.py` — `DiskStorage`

```python
class DiskStorage:
    def __init__(self, base_path)
    def save(self, provider, symbol, dataset, lf) -> None
    def load(self, provider, symbol, dataset, start, end, parse_dates=False) -> pl.LazyFrame

    # private
    def _symbol_dir(self, provider, symbol, dataset) -> Path
    def _slice_path(self, provider, symbol, dataset, ym) -> Path
    def _build_metadata(self, chunk, provider, symbol, dataset, ym, file_size_bytes) -> dict
    def _atomic_write(self, df, path) -> None
```

Path scheme is uniform across datasets: `{base_path}/{provider}/{dataset}/{SYMBOL}/{SYMBOL}_{YYYY-MM}.pq`. OHLCV no longer has an adjusted/raw variant segment — both variants live in the same parquet files, distinguished by the `is_adjusted` column.
`_build_metadata` dispatches to `build_ohlcv` / `build_shares` / `build_dividends`. Missing-days warnings only emitted for OHLCV.

## Data Conventions

- **Date on disk:** `int32` YYYYMMDD (e.g. `20240101`); use `parse_dates=True` to get `pl.Date`
- **OHLC:** `float32` | **Volume:** `int64` | **is_adjusted:** `bool` | **Shares:** `int64` (large-cap counts overflow int32) | **Dividend:** `float32`
- **Parquet paths:** `{base_path}/{provider}/{dataset}/{SYMBOL}/{SYMBOL}_{YYYY-MM}.pq` — one layout for every dataset
- **JSON sidecar:** same path, `.json` extension — written atomically after each `.pq`
- **Atomic writes:** `.tmp` rename for both `.pq` and `.json`

## Import Graph

```
goblin.py
  ├── datasets.Dataset
  ├── _normalize.parse_dates
  ├── sources.yahoo.YahooSource     ──→ _normalize.normalize_ohlcv, normalize_shares, normalize_dividends
  │                                 ──→ sources.base.BaseSource, Fetcher
  │                                 ──→ datasets.Dataset
  ├── sources.csv_source.CSVSource  ──→ _normalize.normalize_ohlcv
  │                                 ──→ sources.base.BaseSource, Fetcher
  │                                 ──→ datasets.Dataset
  └── storage.disk.DiskStorage      ──→ _metadata.build_ohlcv, build_shares, build_dividends, write
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
