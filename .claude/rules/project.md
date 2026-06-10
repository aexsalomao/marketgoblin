# marketgoblin

Market data platform for downloading, storing, and snapshotting financial data.
Supports multiple datasets (OHLCV, shares-outstanding, dividends, splits, daily
fundamentals, quarterly statements, ...) via a per-source dispatch layer.

- **Python:** 3.13 | **Build:** uv_build | **License:** MIT
- **Core deps:** `polars>=1.0`, `yfinance>=0.2`, `pyarrow>=15.0`, `tiingo>=0.16`, `python-dotenv>=1.2.2`
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
    __init__.py           # exports MarketGoblin, Dataset, TickerMetadata, Classification + profiles, sector-index types/loaders; __version__
    _bootstrap.py         # side-effect import: load_dotenv() so TIINGO_API_KEY etc. resolve from .env (imported first by __init__)
    datasets.py           # Dataset StrEnum (OHLCV, SHARES, DIVIDENDS, SPLITS, FUNDAMENTALS_DAILY, FUNDAMENTALS_STATEMENTS)
    goblin.py             # MarketGoblin — public API facade
    _normalize.py         # normalize_ohlcv, normalize_shares, normalize_dividends, normalize_splits, normalize_fundamentals_daily, normalize_statements, normalize_trades, parse_dates — pure
    _metadata.py          # build_ohlcv, build_shares, build_dividends, build_splits, build_fundamentals_daily, build_fundamentals_statements, build_trades, write — pure
    _serialization.py     # JSONSerializable — to_dict / from_dict mixin for the persisted dataclasses
    ticker_metadata.py    # TickerMetadata dataclass — unified, source-agnostic ticker profile
    classification.py     # SectorProfile / IndustryProfile / Classification dataclasses
    sector_indices.py     # Public sector→index API: load_sector_indices / refresh_sector_indices + re-exported types
    _sector_indices_parser.py   # Private GICS taxonomy + S&P 500 scrape parser → SectorIndexMapping (and its dataclasses)
    _sector_indices_data/       # Shipped JSON: us.json (snapshot) + gics_taxonomy_us.json (curated 4-level taxonomy)
    sources/
        base.py           # BaseSource ABC + Fetcher type alias
        yahoo.py          # YahooSource — OHLCV + SHARES + DIVIDENDS via yfinance
        _yahoo_parsing.py # Pure helpers behind YahooSource (info → TickerMetadata, etc.)
        tiingo.py         # TiingoSource — OHLCV/SHARES/DIVIDENDS/SPLITS/FUNDAMENTALS_* via tiingo.TiingoClient
        _tiingo_parsing/  # Pure helpers behind TiingoSource (package, see below)
            __init__.py   # re-exports the orchestrator-facing surface
            common.py     # coercions, slugify, ISO-date parse
            prices.py     # OHLCV / dividends / splits from the prices payload
            fundamentals.py  # daily valuation, quarterly statements, derived shares
            metadata.py   # TickerMetadata + Classification adapters; /fundamentals/meta REST call
        alpaca.py         # AlpacaSource — intraday TRADES (tick) via Alpaca Data API v2 REST
        _alpaca_parsing.py # Pure helper behind AlpacaSource (trades payload → tidy frame)
    storage/
        disk.py           # DiskStorage — dataset-aware monthly .pq slices + metadata/classification JSON
scripts/
    build_sector_map.py   # CLI: ticker list → Yahoo classifications → (ticker, sector) parquet
tests/
    conftest.py           # make_statements_frame fixture (full STATEMENT_FIELDS scaffold)
    _tiingo_data.py       # shared Tiingo JSON-shape builders (imported by test_tiingo*)
    test_metadata.py
    test_normalize.py
    test_storage.py
    test_goblin.py
    test_ticker_metadata.py
    test_classification.py
    test_sector_indices.py
    test_yahoo.py             # YahooSource fetch tests (yfinance mocked)
    test_tiingo.py            # TiingoSource orchestration tests (TiingoClient + requests mocked)
    test_tiingo_prices.py     # prices/dividends/splits parser unit tests
    test_tiingo_fundamentals.py  # daily/statements/derived-shares parser unit tests
    test_tiingo_metadata.py   # metadata/classification/slugify parser unit tests
    _alpaca_data.py           # shared Alpaca JSON-shape builders (imported by test_alpaca*)
    test_alpaca.py            # AlpacaSource orchestration tests (requests/session mocked)
    test_alpaca_parsing.py    # trades-payload parser unit tests
docs/                     # MkDocs source (index.md, api.md, providers.md, contributing.md, changelog.md)
notebooks/                # marketgoblin_walkthrough.ipynb
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
    TRADES = "trades"
```

`StrEnum` so members serialize directly to path segments and JSON. Public API
— exported from package root.

### `goblin.py` — `MarketGoblin`

```python
_SOURCES: dict[str, type[BaseSource]] = {"yahoo": YahooSource, "tiingo": TiingoSource, "alpaca": AlpacaSource}
_DATE_FMT = "%Y-%m-%d"

_validate_dates(start, end)              # bad format or start >= end → ValueError

class _RateLimiter:          # token-bucket, thread-safe; used in fetch_many()

class MarketGoblin:
    def __init__(self, provider, api_key=None, save_path=None, **source_kwargs)
    @property
    def supported_datasets(self) -> frozenset[Dataset]
    def fetch(self, symbol, start, end, dataset=Dataset.OHLCV, parse_dates=False) -> pl.LazyFrame
    def load(self, symbol, start, end, dataset=Dataset.OHLCV, parse_dates=False) -> pl.LazyFrame
    def fetch_metadata(self, symbol, *, fast=False) -> TickerMetadata
    def load_metadata(self, symbol) -> TickerMetadata
    def fetch_classification(self, symbol) -> Classification
    def load_classification(self, symbol) -> Classification
    def fetch_many(self, symbols, start, end, dataset=Dataset.OHLCV, parse_dates=False, max_workers=8, requests_per_second=2.0) -> dict[str, pl.LazyFrame]
```

- `dataset` defaults to `Dataset.OHLCV` so existing callers don't break
- OHLCV is returned as a tidy stacked frame with an `is_adjusted: bool` column — each trading day appears twice (adjusted + raw). Filter downstream (`.filter(pl.col("is_adjusted"))`) to pick a variant. No `adjusted` parameter on the public API.
- `fetch()` validates dates, downloads via source, saves to disk (if `save_path` set), returns `LazyFrame`
- `load()` requires `save_path`; raises `RuntimeError` otherwise
- `fetch_metadata` / `fetch_classification` delegate to the source's like-named methods (Tiingo only today), persisting to disk when `save_path` is set; `load_metadata` / `load_classification` read the JSON sidecars back. All four require the active source to implement the method, else `RuntimeError` / `AttributeError`-style failure surfaces.
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
def normalize_statements(lf) -> pl.LazyFrame  # → int16 fiscal_year, int8 fiscal_quarter, int32 YYYYMMDD date, + every STATEMENT_FIELDS line item × {as_reported, adjusted} (float64 for $/share-counts, float32 for per-share/ratios). STATEMENT_FIELDS is the single source of truth for the statements on-disk schema.
def normalize_trades(lf)    -> pl.LazyFrame   # → float32 price, int64 size/trade_id, ns-UTC timestamp, derived int32 YYYYMMDD date; canonical column order, sorted by timestamp
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
def build_trades(chunk, provider, symbol, ym, file_size_bytes, currency="USD") -> dict
def write(metadata, path) -> None  # atomic via .tmp rename
```

`build_ohlcv()` computes row_count + unique_days (stacked OHLCV has 2 rows per date), date range, close/volume min/max, expected vs. missing trading days (weekday-based, computed on unique dates), and `has_adjusted`/`has_raw` flags describing which variants are present in the slice.
`build_shares()` computes row_count, date range, shares min/max — no missing-days analysis (shares cadence is irregular).
`build_dividends()` computes row_count, date range, dividend min/max/total — no missing-days analysis (dividends are event-driven).
`build_splits()` computes row_count, date range, split_factor min/max — no missing-days analysis (splits are event-driven and rare).
`build_fundamentals_daily()` computes row_count, date range, market_cap min/max, pe_ratio min/max — no missing-days analysis (Tiingo's daily fundamentals occasionally drop bars around corporate actions, not worth alarming on).
`build_fundamentals_statements()` computes row_count, date range, fiscal_year min/max, and as-reported eps_diluted / revenue / net_income min/max — quarterly cadence so the slice typically holds one row.
`build_trades()` computes row_count, unique_days, date range, nanosecond timestamp span (first/last trade), price min/max, and `volume_total` (sum of per-trade size) — intraday/event-dense, so no missing-days analysis; on a partial feed (IEX) `volume_total` is a fraction of consolidated volume by design.
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
- Fundamentals statements: two `client.get_fundamentals_statements(..., asReported=True/False)` calls return nested per-quarter payloads; the parser flattens all four sections (`incomeStatement`, `balanceSheet`, `cashFlow`, `overview`) into named columns and outer-joins the two variants on `(fiscal_year, fiscal_quarter)`. The Tiingo dataCode→column map (`_TIINGO_STATEMENT_CODES`) is guarded at import against `_normalize.STATEMENT_FIELDS` (single source of truth). Note Tiingo's basic-EPS code is `eps`, not `epsBasic`. Filing date is the canonical `date`. Paid endpoint.
- `fetch_metadata`: merges `client.get_ticker_metadata` + latest row from `client.get_fundamentals_daily` (`marketCap`, `peRatio`) + latest close via `client.get_ticker_price` (used to derive `shares_outstanding`). `fast=True` skips both paid lookups.
- `fetch_classification`: direct `requests.get` against `/tiingo/fundamentals/meta` (paid; not wrapped by the Python client). Sector / industry strings → slugified `SectorProfile` / `IndustryProfile` keys; constituent fields stay at dataclass defaults.
- All Tiingo REST calls send the symbol lowercase; on-disk `symbol` columns are uppercase.
- Pure parsing helpers (frame-builders, dataclass-builders, `requests.get` wrapper) live in the `_tiingo_parsing/` package so the `TiingoSource` class stays a thin orchestrator — see its section below. `tiingo.py` imports the orchestrator-facing names from the package root (`_tiingo_parsing/__init__.py` re-exports them).
- `_retry_fetch` retries on transient errors with backoff (1 s, 2 s); `ValueError` (empty data) propagates immediately.

### `sources/_tiingo_parsing/` — Tiingo parser package

Pure, side-effect-free adapters between Tiingo's JSON shapes and the project's
frame/dataclass schema. Split by concern; `__init__.py` re-exports the
orchestrator-facing names so `tiingo.py` (and any legacy importer) keeps using
`from ..._tiingo_parsing import <name>`. Concern-specific tests import from the
concrete submodule.

- `common.py` — `coerce_int`, `coerce_float`, `first_present`, `slugify`, `parse_tiingo_date_col`. No Tiingo-shape knowledge; the shared primitives the other submodules build on.
- `prices.py` — projects the prices payload: `prices_rows_to_base_lf`, `build_raw_ohlcv_lf`, `build_adjusted_ohlcv_lf`, `stack_ohlcv`, `prices_rows_to_stacked_ohlcv`, `prices_rows_to_dividends`, `prices_rows_to_splits`.
- `fundamentals.py` — `fundamentals_daily_rows_to_lf`, `statements_rows_to_lf`, `derive_shares_from_marketcap`, plus the `_TIINGO_STATEMENT_CODES` dataCode→column map and the two import-time guards (drift vs. `_normalize.STATEMENT_FIELDS`, and duplicate-code detection) that fail loud at import if the map and on-disk schema diverge.
- `metadata.py` — `fetch_latest_close`, `fetch_latest_fundamentals`, `build_tiingo_metadata`, `fetch_fundamentals_meta`, `build_tiingo_classification`. The only submodule importing `requests` (the `/fundamentals/meta` REST call); patch `_tiingo_parsing.metadata.requests` in tests.

### `sources/alpaca.py` — `AlpacaSource`

```python
class AlpacaSource(BaseSource):
    name = "alpaca"
    def __init__(self, api_key=None, api_secret=None, feed="iex", **kwargs)
    def _fetch_trades(self, symbol, start, end) -> pl.LazyFrame
    def _download_trades(self, symbol, start, end) -> list[dict]   # paginates next_page_token
    def _retry_fetch(self, fetch_fn, symbol) -> _T
```

- Supports a single dataset: `Dataset.TRADES` — intraday tick-by-tick executions.
- TRADES: paginated `GET data.alpaca.markets/v2/stocks/{SYMBOL}/trades` (follows `next_page_token`); rows projected by `trades_rows_to_lf` then `normalize_trades`.
- Credentials resolve from `api_key`/`api_secret` args or `ALPACA_API_KEY`/`ALPACA_API_SECRET`. The cred check runs in `_fetch_trades` *before* the retry loop (a missing key is not transient). The free Basic plan serves `feed="iex"` (real ticks, IEX-only ≈ a few % of consolidated volume); `feed="sip"` needs a paid plan.
- Symbol is upper-cased in the request URL and the on-disk `symbol` column.
- `_retry_fetch` mirrors Tiingo's: retries transient errors with 1 s / 2 s backoff; `ValueError` (empty data) propagates immediately.

### `storage/disk.py` — `DiskStorage`

```python
class DiskStorage:
    def __init__(self, base_path)
    def save(self, provider, symbol, dataset, lf) -> None
    def load(self, provider, symbol, dataset, start, end, parse_dates=False) -> pl.LazyFrame
    def save_metadata(self, provider, metadata: TickerMetadata) -> None
    def load_metadata(self, provider, symbol) -> TickerMetadata
    def save_classification(self, provider, classification: Classification) -> None
    def load_classification(self, provider, symbol) -> Classification

    # private
    def _symbol_dir(self, provider, symbol, dataset) -> Path
    def _slice_path(self, provider, symbol, dataset, ym) -> Path
    def _metadata_path(self, provider, symbol) -> Path          # {base}/{provider}/metadata/{SYMBOL}.json
    def _classification_path(self, provider, symbol) -> Path    # {base}/{provider}/classification/{SYMBOL}.json
    def _build_metadata(self, chunk, provider, symbol, dataset, ym, file_size_bytes) -> dict
    def _atomic_write(self, df, path) -> None
```

Path scheme is uniform across datasets: `{base_path}/{provider}/{dataset}/{SYMBOL}/{SYMBOL}_{YYYY-MM}.pq`. OHLCV no longer has an adjusted/raw variant segment — both variants live in the same parquet files, distinguished by the `is_adjusted` column.
`_build_metadata` dispatches per `Dataset` to the matching `build_*` in `_metadata.py` (ohlcv / shares / dividends / splits / fundamentals_daily / fundamentals_statements / trades). Missing-days warnings only emitted for OHLCV. TRADES slices fit the same monthly-`.pq` machinery because `normalize_trades` derives an int32 `date` from each trade's `timestamp` — slicing and the load-range filter key on `date` exactly as the daily datasets do, while the full nanosecond `timestamp` is preserved as the real time axis. Merging is the exception: `_merge_existing` dedups TRADES on full per-trade identity (every column except the list-typed `conditions`), not the `date` key the daily datasets use, so a partial-day re-fetch unions rather than evicting the day.
`TickerMetadata` and `Classification` are persisted as standalone JSON (not monthly slices) under `{provider}/metadata/` and `{provider}/classification/`, atomically via `.tmp` rename; the load methods raise `FileNotFoundError` when absent.

### `ticker_metadata.py` — `TickerMetadata`

Frozen `JSONSerializable` dataclass: a unified, source-agnostic ticker profile
(`symbol` + optional quote identity, profile, valuation, history-metadata, and
provenance fields, plus an `extras: dict` catch-all and `is_fast: bool`).
Source adapters build it (`build_tiingo_metadata`, the Yahoo equivalent);
`DiskStorage` round-trips it as JSON.

### `classification.py` — `SectorProfile` / `IndustryProfile` / `Classification`

Three frozen `JSONSerializable` dataclasses. `Classification` bundles a
`symbol` with optional `SectorProfile` and `IndustryProfile` sub-profiles plus
provenance; it overrides `from_dict` to rebuild the nested profiles. Tiingo
populates only the `key`/`name` (and industry→sector linkage); constituent
fields (`top_companies`, `etf_symbol`, `market_cap`, ...) stay at defaults for
sources that don't expose them.

### `sector_indices.py` + `_sector_indices_parser.py` — GICS sector→index mapping

```python
def load_sector_indices(market="US") -> SectorIndexMapping       # read shipped JSON
def refresh_sector_indices(market="US", output_path=None) -> SectorIndexMapping  # re-parse + rewrite
```

`sector_indices.py` is the public facade (exported from the package root). It
reads the shipped snapshot in `_sector_indices_data/us.json`, or re-runs the
private parser. `_SUPPORTED_MARKETS = {"US"}`; unsupported market → `ValueError`.

`_sector_indices_parser.py` holds the 4-level GICS dataclasses (`SectorIndex` >
`IndustryGroup` > `Industry` > `SubIndustry`, all `JSONSerializable`) and
`SectorIndexMapping`. `parse_us_sector_indices(fetcher=None, taxonomy_path=None)`
loads the curated `gics_taxonomy_us.json`, scrapes S&P 500 constituents from
Wikipedia (`_WikitableParser`) for sub-industry counts, and rolls them up; both
the fetcher and taxonomy path are injectable for tests. `write_mapping` /
`load_taxonomy` are the I/O helpers.

### `_serialization.py` — `JSONSerializable`

`to_dict` / `from_dict` mixin for the persisted dataclasses. Flat dataclasses
use it directly; classes with nested dataclass fields inherit `to_dict` and
override `from_dict`. `from_dict` silently drops unknown keys so old JSON
sidecars stay loadable after the schema grows a field.

### `_bootstrap.py`

Side-effect-only module: runs `dotenv.load_dotenv()` at import so credentials
(e.g. `TIINGO_API_KEY`) resolve from a local `.env` without per-shell exports.
Imported first by `__init__.py`; existing shell exports take precedence.

## Data Conventions

- **Date on disk:** `int32` YYYYMMDD (e.g. `20240101`); use `parse_dates=True` to get `pl.Date`
- **OHLC:** `float32` | **Volume:** `int64` | **is_adjusted:** `bool` | **Shares:** `int64` (large-cap counts overflow int32) | **Dividend:** `float32`
- **Trades (tick):** intraday dataset — `timestamp` is `Datetime("ns", "UTC")` (the real axis), `price` `float32`, `size`/`trade_id` `int64`, `conditions` `list[str]`; the int32 `date` is *derived* from the **US/Eastern session date** of `timestamp` (so extended-hours prints that cross UTC midnight land on the right session) and lets the daily monthly-slice machinery apply unchanged
- **Parquet paths:** `{base_path}/{provider}/{dataset}/{SYMBOL}/{SYMBOL}_{YYYY-MM}.pq` — one layout for every dataset
- **JSON sidecar:** same path, `.json` extension — written atomically after each `.pq`
- **Atomic writes:** `.tmp` rename for both `.pq` and `.json`

## Import Graph

```
__init__.py  ──→ _bootstrap (side effect), goblin, datasets, ticker_metadata,
                 classification, sector_indices

goblin.py
  ├── datasets.Dataset
  ├── _normalize.parse_dates
  ├── ticker_metadata.TickerMetadata, classification.Classification
  ├── sources.yahoo.YahooSource     ──→ _normalize.normalize_* (ohlcv/shares/dividends)
  │                                 ──→ sources.base.BaseSource, Fetcher
  │                                 ──→ sources._yahoo_parsing (build_ticker_metadata, fetch_sector_profile, ...)
  │                                 ──→ datasets.Dataset
  ├── sources.tiingo.TiingoSource   ──→ _normalize.normalize_* (ohlcv/shares/dividends/splits/fundamentals_daily/statements)
  │                                 ──→ sources.base.BaseSource, Fetcher
  │                                 ──→ sources._tiingo_parsing (prices_rows_to_stacked_ohlcv, build_tiingo_metadata, ...)
  │                                 ──→ classification, ticker_metadata, datasets.Dataset
  ├── sources.alpaca.AlpacaSource   ──→ _normalize.normalize_trades
  │                                 ──→ sources.base.BaseSource, Fetcher
  │                                 ──→ sources._alpaca_parsing (trades_rows_to_lf)
  │                                 ──→ datasets.Dataset
  └── storage.disk.DiskStorage      ──→ _metadata.build_* + write
                                    ──→ _normalize.parse_dates
                                    ──→ classification, ticker_metadata, datasets.Dataset

sources/_tiingo_parsing/
  __init__.py    ──→ prices, fundamentals, metadata           (re-export only)
  common.py      (no local imports)
  prices.py      ──→ _tiingo_parsing.common
  fundamentals.py──→ _tiingo_parsing.common, _normalize (STATEMENT_FIELDS, STATEMENT_VARIANTS)
  metadata.py    ──→ _tiingo_parsing.common, classification, ticker_metadata

sources/_alpaca_parsing.py ──→ (polars only — no local imports)

sector_indices.py        ──→ _sector_indices_parser
_sector_indices_parser.py──→ _serialization
ticker_metadata.py       ──→ _serialization
classification.py        ──→ _serialization

datasets.py     (no local imports)
_normalize.py   (no local imports)
_metadata.py    (no local imports)
_serialization.py (no local imports)
_bootstrap.py   (no local imports — runs dotenv.load_dotenv())
```

## Code Style

See `code-style.md` and `testing.md` in this directory for general Python conventions.

## Git Branches

- `{name}_fix_{description}` — bug fixes
- `{name}_dev_{description}` — new features
