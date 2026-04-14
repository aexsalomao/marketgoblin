# marketgoblin

Market data platform for downloading, storing, and snapshotting financial OHLCV data.

> Tech stack, dependencies, and Python version are defined in `pyproject.toml` — refer there, do not duplicate here.

## Example Runner

`example.py` at the project root is the canonical manual test. It must run end-to-end with hardcoded example parameters (symbols, date range, etc.) and demonstrate core functionality. Keep it up to date as the API evolves.

## Code Style

- **Simple first, production ready**: Write the simplest correct implementation before optimizing. Avoid premature abstraction.
- **Testable by design**: Prefer pure functions, minimal side effects, and injected dependencies so units can be tested in isolation.
- **Docstrings**: Add docstrings to public functions, classes, and modules where behavior is not self-evident from the signature alone.
- **Modular**: One responsibility per module/class. Compose small pieces rather than building monoliths.
- **Design patterns**: Apply standard patterns (factory, repository, strategy, etc.) where they genuinely reduce complexity — not for their own sake.

## Preferred Libraries

- **DataFrames**: Polars over pandas everywhere.
- **Storage**: Parquet over CSV/JSON for all persistent tabular data.

## Git Branches

- `{name}_fix_{fix-description}` — bug fixes
- `{name}_dev_{feature-description}` — new development

## Setup

```bash
uv sync --extra dev
```

## Running tests

```bash
pytest
pytest --cov=marketgoblin  # with coverage
```

## Project layout

```
src/marketgoblin/
    vault.py          # MarketGoblin — public API (fetch, load, fetch_many)
    _normalize.py     # cast numerics to float32, date to int32 YYYYMMDD
    _metadata.py      # build + atomically write JSON sidecar per parquet slice
    sources/
        base.py       # BaseSource ABC
        yahoo.py      # YahooSource (yfinance)
    storage/
        disk.py       # DiskStorage — per-month .pq files with JSON sidecars
```

## Data conventions

- **Date** stored as `int32` YYYYMMDD (e.g. `20260101`) on disk; convert with `parse_dates=True` for `pl.Date`
- **OHLCV numerics** stored as `float32`
- **Parquet layout**: `{base_path}/{provider}/ohlcv/{adjusted|raw}/{SYMBOL}/{SYMBOL}_{YYYY-MM}.pq`
- Each `.pq` slice has a JSON sidecar at the same path with `.json` extension
- Writes are atomic via a `.tmp` rename

## Adding a new source

1. Subclass `BaseSource` in `src/marketgoblin/sources/`
2. Implement `fetch(symbol, start, end, adjusted) -> pl.LazyFrame` — must return a normalized frame (call `normalize()` from `_normalize.py`)
3. Register the new source in the `_SOURCES` dict in `vault.py`
