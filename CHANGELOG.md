# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
