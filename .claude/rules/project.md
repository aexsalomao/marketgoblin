# marketgoblin

Market data platform for downloading, storing, and snapshotting financial data.
Supports multiple datasets (OHLCV, shares-outstanding, dividends, splits, daily
fundamentals, quarterly statements, ...) via a per-source dispatch layer.

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
    datasets.py           # Dataset StrEnum (OHLCV, SHARES, DIVIDENDS, SPLITS, FUNDAMENTALS_DAILY, FUNDAMENTALS_STATEMENTS)
    goblin.py             # MarketGoblin — public API facade
    _normalize.py         # normalize_ohlcv, normalize_shares, normalize_dividends, normalize_splits, normalize_fundamentals_daily, normalize_statements, parse_dates — pure
    _metadata.py          # build_ohlcv, build_shares, build_dividends, build_splits, build_fundamentals_daily, build_fundamentals_statements, write — pure
    sources/
        base.py           # BaseSource ABC + Fetcher type alias
        yahoo.py          # YahooSource — OHLCV + SHARES + DIVIDENDS via yfinance
        _yahoo_parsing.py # Pure helpers behind YahooSource (info → TickerMetadata, etc.)
        tiingo.py         # TiingoSource — OHLCV + SHARES + DIVIDENDS via tiingo.TiingoClient
        _tiingo_parsing.py # Pure helpers behind TiingoSource (JSON → frames + dataclasses)
    storage/
        disk.py           # DiskStorage — dataset-aware monthly .pq slices
tests/
    test_metadata.py
    test_normalize.py
    test_storage.py
    test_goblin.py
    test_yahoo.py         # YahooSource fetch tests (yfinance mocked)
    test_tiingo.py        # TiingoSource fetch tests (TiingoClient + requests mocked)
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
    SPLITS = "splits"
    FUNDAMENTALS_DAILY = "fundamentals_daily"
    FUNDAMENTALS_STATEMENTS = "fundamentals_statements"
```

`StrEnum` so members serialize directly to path segments and JSON. Public API
— exported from package root.

### `goblin.py` — `MarketGoblin`

```python
_SOURCES: dict[str, type[BaseSource]] = {"yahoo": YahooSource, "tiingo": TiingoSource}

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
- `**source_kwargs` are forwarded to the source constructor
- To add a provider: subclass `BaseSource`, implement `_build_dispatch()`, add to `_SOURCES`
- To add a dataset: extend `Dataset` enum, add `_fetch_<dataset>` method on relevant sources and register in their `_build_dispatch()`, add a `normalize_<dataset>` and `build_<dataset>` for storage, and extend `DiskStorage._build_metadata` dispatch

### `_normalize.py`

```python
_OHLC_COLS = ["open", "high", "low", "close"]

def normalize_ohlcv(lf)     -> pl.LazyFrame   # → float32 OHLC, int64 volume, bool is_adjusted, int32 YYYYMMDD date
def normalize_shares(lf)    -> pl.LazyFrame   # → int64 shares, int32 YYYYMMDD date
def normalize_dividends(lf) -> pl.LazyFrame   # → float32 dividend, int32 YYYYMMDD date
def normalize_splits(lf)    -> pl.LazyFrame   # → float32 split_factor, int32 YYYYMMDD date
def normalize_fundamentals_daily(lf) -> pl.LazyFrame  # → int64 market_cap/enterprise_val, float32 ratios, int32 YYYYMMDD date
def normalize_statements(lf) -> pl.LazyFrame  # → int16 fiscal_year, int8 fiscal_quarter, float32 EPS, float64 revenue, int32 YYYYMMDD date
def parse_dates(lf)         -> pl.LazyFrame   # → int32 YYYYMMDD → pl.Date
```

Each `normalize_*` is dataset-specific. `parse_dates` works on any frame with an int32 `date` column.

### `_metadata.py`

```python
def build_ohlcv(chunk, provider, symbol, ym, file_size_bytes, currency="USD") -> dict
def build_shares(chunk, provider, symbol, ym, file_size_bytes) -> dict
def build_dividends(chunk, provider, symbol, ym, file_size_bytes, currency="USD") -> dict
def build_splits(chunk, provider, symbol, ym, file_size_bytes) -> dict
def build_fundamentals_daily(chunk, provider, symbol, ym, file_size_bytes) -> dict
def build_fundamentals_statements(chunk, provider, symbol, ym, file_size_bytes) -> dict
def write(metadata, path) -> None  # atomic via .tmp rename
```

`build_ohlcv()` computes row_count + unique_days (stacked OHLCV has 2 rows per date), date range, close/volume min/max, expected vs. missing trading days (weekday-based, computed on unique dates), and `has_adjusted`/`has_raw` flags describing which variants are present in the slice.
`build_shares()` computes row_count, date range, shares min/max — no missing-days analysis (shares cadence is irregular).
`build_dividends()` computes row_count, date range, dividend min/max/total — no missing-days analysis (dividends are event-driven).
`build_splits()` computes row_count, date range, split_factor min/max — no missing-days analysis (splits are event-driven and rare).
`build_fundamentals_daily()` computes row_count, date range, market_cap min/max, pe_ratio min/max — no missing-days analysis (Tiingo's daily fundamentals occasionally drop bars around corporate actions, not worth alarming on).
`build_fundamentals_statements()` computes row_count, date range, fiscal_year and eps_diluted min/max — quarterly cadence so the slice typically holds one row.
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

### `sources/tiingo.py` — `TiingoSource`

```python
class TiingoSource(BaseSource):
    name = "tiingo"
    def __init__(self, api_key=None, **kwargs)            # wraps tiingo.TiingoClient
    def _fetch_ohlcv(self, symbol, start, end) -> pl.LazyFrame
    def _fetch_shares(self, symbol, start, end) -> pl.LazyFrame
    def _fetch_dividends(self, symbol, start, end) -> pl.LazyFrame
    def _fetch_splits(self, symbol, start, end) -> pl.LazyFrame
    def _fetch_fundamentals_daily(self, symbol, start, end) -> pl.LazyFrame
    def _fetch_fundamentals_statements(self, symbol, start, end) -> pl.LazyFrame
    def fetch_metadata(self, symbol, *, fast=False) -> TickerMetadata
    def fetch_classification(self, symbol) -> Classification
    def _retry_fetch(self, fetch_fn, symbol) -> pl.LazyFrame
```

- OHLCV: one `client.get_ticker_price(symbol, startDate, endDate, fmt="json", frequency="daily")` call returns each trading day's raw OHLCV plus adjusted variants (`adjOpen`, `adjHigh`, `adjLow`, `adjClose`, `adjVolume`) and `divCash` / `splitFactor`. The two variants are split into the project's stacked tidy frame, sorted by `(date, is_adjusted)`.
- Shares: Tiingo's daily Fundamentals endpoint has `marketCap` but no shares field. We join `client.get_ticker_price(...)` + `client.get_fundamentals_daily(...)` on date and derive `shares = round(marketCap / close)` per day.
- Dividends: derived from the same prices endpoint as OHLCV — rows with `divCash > 0`.
- Splits: derived from the same prices endpoint as OHLCV — rows with `splitFactor != 1.0`.
- Fundamentals daily: one `client.get_fundamentals_daily(...)` call returns per-trading-day `marketCap`, `enterpriseVal`, `peRatio`, `pbRatio`, `trailingPEG1Y`. Renamed to snake_case, missing fields surface as null. Paid endpoint.
- Fundamentals statements: one `client.get_fundamentals_statements(..., asReported=…)` call returns nested per-quarter payloads; the parser flattens `incomeStatement` codes (`epsDil`, `epsBasic`, `revenue`) into named columns. Filing date is the canonical `date`. The `as_reported` kw-only constructor flag (default True) toggles point-in-time vs latest-restated. Paid endpoint.
- `fetch_metadata`: merges `client.get_ticker_metadata` + latest row from `client.get_fundamentals_daily` (`marketCap`, `peRatio`) + latest close via `client.get_ticker_price` (used to derive `shares_outstanding`). `fast=True` skips both paid lookups.
- `fetch_classification`: direct `requests.get` against `/tiingo/fundamentals/meta` (paid; not wrapped by the Python client). Sector / industry strings → slugified `SectorProfile` / `IndustryProfile` keys; constituent fields stay at dataclass defaults.
- All Tiingo REST calls send the symbol lowercase; on-disk `symbol` columns are uppercase.
- Pure parsing helpers (frame-builders, dataclass-builders, `requests.get` wrapper) live in `_tiingo_parsing.py` so the `TiingoSource` class stays a thin orchestrator.
- `_retry_fetch` retries on transient errors with backoff (1 s, 2 s); `ValueError` (empty data) propagates immediately.

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
  │                                 ──→ sources._yahoo_parsing (build_ticker_metadata, fetch_sector_profile, ...)
  │                                 ──→ datasets.Dataset
  ├── sources.tiingo.TiingoSource   ──→ _normalize.normalize_ohlcv, normalize_shares, normalize_dividends
  │                                 ──→ sources.base.BaseSource, Fetcher
  │                                 ──→ sources._tiingo_parsing (prices_rows_to_stacked_ohlcv, build_tiingo_metadata, ...)
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
