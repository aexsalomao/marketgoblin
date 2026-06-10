# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.7.0] - 2026-06-10

### Added
- **Alpaca provider** (`MarketGoblin(provider="alpaca")`) — fourth data source, backed by Alpaca's Data API v2 REST endpoint, exposing the new `Dataset.TRADES`
- `Dataset.TRADES` — intraday tick-by-tick trades (nanosecond UTC `timestamp`, `price`, `size`, `exchange`, `conditions`, `trade_id`, `tape`). The on-disk int32 `date` is derived from the **US/Eastern** trading-session date so extended-hours prints that cross UTC midnight land on the correct day and monthly slice. Credentials resolve from `ALPACA_API_KEY` / `ALPACA_API_SECRET`; the free IEX feed is the default (`feed="iex"`), `feed="sip"` for the paid consolidated tape
- `normalize_trades` / `build_trades` and the pure `trades_rows_to_lf` parser for the new dataset

### Changed
- `DiskStorage._merge_existing` is now dataset-aware: `TRADES` slices dedup on full per-trade identity instead of the calendar `date` key, so a partial-day re-fetch unions ticks rather than evicting the rest of the day
- Hoisted the shared `_retry_fetch` transient-error retry helper into `BaseSource`, removing the duplicated copies from the Yahoo, Tiingo, and Alpaca sources

### Fixed
- `trades_rows_to_lf` now backfills optional trade fields (`conditions`, `trade_id`, ...) absent from an entire page as typed nulls instead of raising `ColumnNotFoundError` on sparse (e.g. IEX) feeds

## [0.6.0] - 2026-06-10

### Changed
- `DiskStorage.save()` now **merges** each month slice with the rows already on disk instead of replacing the whole slice: existing rows the incoming frame doesn't cover are kept, and incoming rows win where identities overlap (vendor restatements replace stale bars). A partial-range fetch no longer erases the rest of a month — only deleting the slice file discards history
- `marketgoblin.__version__` is now derived from the installed package metadata (single source of truth: the `pyproject.toml` version) so it can't drift from the released artifact; falls back to `0.0.0+unknown` when run from an uninstalled source tree

### Fixed
- `DiskStorage` slice merge keyed on `date` alone, so a chunk carrying only one OHLCV variant (`is_adjusted` True/False) evicted both existing rows for that date and silently dropped the untouched variant. The merge now keys on `(date, is_adjusted)` for stacked OHLCV
- `MarketGoblin.fetch_many(requests_per_second=0)` raised a bare `ZeroDivisionError`, and a negative rate silently disabled limiting; `_RateLimiter` now rejects non-positive rates with a clear `ValueError`
- `Classification.from_dict` coerced a malformed empty sub-profile dict (`{}`) to `None` via a truthiness check instead of failing loud; it now uses explicit `is not None` checks

## [0.5.1] - 2026-06-01

### Fixed
- `marketgoblin.__version__` reported `0.4.0` on the 0.5.0 release — the package `__version__` string was never bumped. It now tracks the released version

### Changed
- Internal: split `sources/_tiingo_parsing.py` into a `_tiingo_parsing/` package (`common` / `prices` / `fundamentals` / `metadata`) with a re-exporting `__init__`, and reorganized the Tiingo test suite to match. No public API or behavior change

## [0.5.0] - 2026-06-01

### Added
- **Tiingo provider** (`MarketGoblin(provider="tiingo")`) — third data source backed by `tiingo.TiingoClient`, supporting OHLCV, SHARES, and DIVIDENDS. OHLCV comes from a single prices call carrying both raw and adjusted variants; SHARES is derived as `round(marketCap / close)`; DIVIDENDS and SPLITS are extracted from the same prices payload
- `Dataset.SPLITS` — event-driven `split_factor` multiplier (e.g. `2.0` = 2-for-1, `0.5` = reverse), Tiingo-backed
- `Dataset.FUNDAMENTALS_DAILY` — per-trading-day `market_cap`, `enterprise_val`, `pe_ratio`, `pb_ratio`, `trailing_peg_1y` (Tiingo, paid endpoint)
- `Dataset.FUNDAMENTALS_STATEMENTS` — full quarterly income statement, balance sheet, cash flow, and overview: ~76 line items, each in both as-reported (point-in-time) and restated (adjusted) variants (Tiingo, paid endpoint). Dollar amounts and share counts are `float64`; per-share figures and ratios are `float32`
- `TiingoSource.fetch_metadata()` / `fetch_classification()` — Tiingo-backed `TickerMetadata` and sector/industry `Classification`
- `_normalize.STATEMENT_FIELDS` — single source of truth for the statements on-disk schema (names, dtypes, order); the Tiingo dataCode→column map is guarded against drift and duplicate codes at import
- `normalize_splits` / `normalize_fundamentals_daily` / `normalize_statements` and `build_splits` / `build_fundamentals_daily` / `build_fundamentals_statements` for the new datasets
- Automatic `.env` loading at package import (`_bootstrap.py`) for credential management (e.g. `TIINGO_API_KEY`); `.env.example` added
- Provider comparison docs (`docs/providers.md`), a walkthrough notebook (`notebooks/marketgoblin_walkthrough.ipynb`), and a sector-map build script (`scripts/build_sector_map.py`)

### Changed
- Tiingo's two statements calls (`asReported=True`/`False`) are issued behind separate retries and merged on `(fiscal_year, fiscal_quarter)` so a transient failure on one variant doesn't replay the other

### Fixed
- Basic EPS was read from a non-existent Tiingo code (`epsBasic`); it is `eps`, so `eps_basic_*` had been silently null
- Environment-dependent Tiingo dataset test made deterministic

### Removed
- `CSVSource` and the `"csv"` provider — the local-CSV OHLCV source was unused. `MarketGoblin(provider="csv", ...)` now raises `ValueError: Unknown provider` (**breaking**)
- `TODO.md` roadmap from the repo root

## [0.4.0] - 2026-04-20

### Added
- `marketgoblin.sector_indices` — public module exposing refreshable US GICS sector → index/ETF mappings
- `SectorIndexMapping`, `SectorIndex`, `IndustryGroup`, `Industry`, `SubIndustry` dataclasses — full 4-level GICS tree (sector → industry group → industry → sub-industry) with per-level GICS codes and `constituent_count`
- `load_sector_indices(market="US")` — read the shipped JSON snapshot (`src/marketgoblin/_sector_indices_data/us.json`)
- `refresh_sector_indices(market="US", output_path=None)` — re-run the parser against the S&P 500 Wikipedia constituents page and rewrite the snapshot
- Curated GICS 2023 taxonomy shipped as `gics_taxonomy_us.json` (11 sectors, 25 industry groups, 73 industries, 163 sub-industries) — parser joins scraped constituents against it and rolls counts up the hierarchy; unknown upstream sub-industries fail loud
- `TODO.md` roadmap at the repo root tracking coverage phases (US index families, international markets) and parser hardening
- `hypothesis>=6.100` added to the `dev` extra — powers property tests for rollup invariants (sum-of-children == parent at every level) and JSON roundtrip

## [0.3.0] - 2026-04-20

### Added
- `TickerMetadata` dataclass — unified, source-agnostic ticker profile collapsing yfinance's `info` / `fast_info` / `history_metadata` / `isin` into one shape
- `Classification`, `SectorProfile`, `IndustryProfile` dataclasses — sector + industry classification for a ticker via `yf.Sector` / `yf.Industry`
- `MarketGoblin.fetch_metadata(symbol, *, fast=False)` / `load_metadata(symbol)` — live-fetch or disk-load ticker metadata
- `MarketGoblin.fetch_classification(symbol)` / `load_classification(symbol)` — live-fetch or disk-load sector + industry classification
- `YahooSource.fetch_metadata()` and `fetch_classification()` — yfinance-backed implementations with retry/backoff; classification parallelizes sector + industry lookups
- `DiskStorage.save_metadata` / `load_metadata` / `save_classification` / `load_classification` — JSON persistence at `{provider}/metadata/{SYMBOL}.json` and `{provider}/classification/{SYMBOL}.json`
- `JSONSerializable` mixin (`_serialization.py`) — shared `to_dict` / `from_dict` for JSON-backed dataclasses; tolerates unknown keys on load

### Changed
- `YahooSource` split into orchestration (`yahoo.py`) and pure adapter/parser helpers (`_yahoo_parsing.py`)
- `_metadata.write()` generalized to accept any target path and create parent dirs (was sidecar-only)

## [0.2.0] - 2026-04-20

### Added
- `Dataset` enum (`OHLCV`, `SHARES`, `DIVIDENDS`) exported from the package root for dataset selection
- Shares-outstanding dataset via Yahoo (`yfinance.Ticker.get_shares_full`) — sparse, corporate-action-driven series deduplicated to one row per day
- Dividends dataset via Yahoo (`yfinance.Ticker.dividends`) — event-driven series filtered to the requested date range
- `is_adjusted: bool` column on OHLCV frames — adjusted and raw variants now live in a single tidy stacked series
- `MarketGoblin.supported_datasets` property exposing the datasets a provider supports
- `dataset=` parameter on `fetch()`, `load()`, and `fetch_many()` (defaults to `Dataset.OHLCV` — existing callers unchanged)
- `CSVSource(is_adjusted=...)` init kwarg stamps the variant flag on every row (CSVs hold a single variant by assumption)
- `normalize_shares()`, `normalize_dividends()` in `_normalize.py` and `build_shares()`, `build_dividends()` in `_metadata.py`
- Uniform dataset-aware path scheme in `DiskStorage`: `{provider}/{dataset}/{SYMBOL}/{SYMBOL}_{YYYY-MM}.pq` — no `adjusted|raw` segment for any dataset

### Changed
- Per-source dataset dispatch: sources declare supported datasets via `_build_dispatch()`; `BaseSource.fetch()` takes a `Dataset` as its first argument
- OHLCV is fetched in a single `yf.Ticker.history(auto_adjust=False)` call — adjusted Open/High/Low are derived locally via the `Adj Close / Close` ratio (zero numerical drift vs yfinance's `auto_adjust=True`, half the network calls)
- OHLCV metadata sidecar: `price_adjusted` replaced by `has_adjusted` / `has_raw`; missing-days analysis now runs on unique dates; new `unique_days` field

### Removed
- `adjusted` parameter from `MarketGoblin.fetch()` / `load()` / `fetch_many()`, `BaseSource.fetch()`, per-dataset `Fetcher` signature, and `DiskStorage.save()` / `load()` — OHLCV variants are distinguished by the `is_adjusted` column instead (**breaking**)

## [0.1.2] - 2026-04-17

### Removed
- Undocumented `report=True` option on `MarketGoblin` and the `download_report.csv` sidecar — not part of the public API surface defined in `.claude/rules/project.md`

### Changed
- File header comments on every module per `code-style.md` rule 10
- Flattened `for` loops in tests to comply with `testing.md` rule 32 (no logic in tests)
- Fixed volume dtype in test fixtures (`Float32` → `Int64`) and `file_size_bytes` arg type in `test_metadata.py`

## [0.1.1] - 2026-04-16

### Added
- Retry logic with exponential backoff in `YahooSource.fetch()` (3 attempts, 1 s / 2 s delays)
- Rate limiting in `fetch_many()` via a token-bucket `_RateLimiter` (default: 2 req/s)
- Input validation for date format and ordering in `fetch()`, `load()`, and `fetch_many()`
- `CSVSource` — a file-backed OHLCV source for local CSV data
- `**source_kwargs` forwarding in `MarketGoblin.__init__()` for provider-specific options
- Documentation site at [aexsalomao.github.io/marketgoblin](https://aexsalomao.github.io/marketgoblin)
- Automated PyPI publish workflow via GitHub Actions Trusted Publishing (OIDC)
- Ruff linting + formatting, mypy strict type checking, pre-commit hooks
- GitHub Actions CI workflow (lint → format → typecheck → test → Codecov)

### Changed
- Volume column dtype changed from `float32` to `int64` for accuracy

## [0.1.0] - 2026-04-16

### Added
- Initial release
- `MarketGoblin` public API facade (`fetch`, `load`, `fetch_many`)
- `YahooSource` backed by yfinance
- `DiskStorage` — monthly Parquet slices with atomic writes and JSON sidecars
- `normalize()` and `parse_dates()` in `_normalize.py`
- `build()` and `write()` metadata helpers in `_metadata.py`
- 33 unit tests across all modules
