# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Retry logic with exponential backoff in `YahooSource.fetch()` (3 attempts, 1 s / 2 s delays)
- Rate limiting in `fetch_many()` via a token-bucket `_RateLimiter` (default: 2 req/s)
- Input validation for date format and ordering in `fetch()`, `load()`, and `fetch_many()`
- `CSVSource` — a file-backed OHLCV source for local CSV data (proves the plugin pattern)
- `**source_kwargs` forwarding in `MarketGoblin.__init__()` for provider-specific options
- Ruff linting + formatting (`pyproject.toml [tool.ruff]`)
- Mypy type checking (`pyproject.toml [tool.mypy]`)
- Pre-commit hooks (`.pre-commit-config.yaml`)
- GitHub Actions CI workflow (lint → format → typecheck → test → Codecov upload)
- GitHub Actions docs workflow (MkDocs → GitHub Pages on push to master)
- MkDocs documentation site (`mkdocs.yml`, `docs/`)
- MIT `LICENSE` file
- `CONTRIBUTING.md`

## [0.1.0] - 2026-04-16

### Added
- Initial release
- `MarketGoblin` public API facade (`fetch`, `load`, `fetch_many`)
- `YahooSource` backed by yfinance
- `DiskStorage` — monthly Parquet slices with atomic writes and JSON sidecars
- `normalize()` and `parse_dates()` in `_normalize.py`
- `build()` and `write()` metadata helpers in `_metadata.py`
- 33 unit tests across all modules
