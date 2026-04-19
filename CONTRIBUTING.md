# Contributing

Thanks for your interest in contributing to marketgoblin!

## Setup

```bash
git clone https://github.com/aexsalomao/marketgoblin
cd marketgoblin
uv sync --extra dev
pre-commit install
```

## Workflow

1. Fork the repo and create a branch: `{name}_fix_{description}` or `{name}_dev_{description}`
2. Make your changes
3. Run the full check suite locally before pushing:

```bash
uv run ruff check .
uv run ruff format .
uv run mypy src/
uv run pytest
```

4. Open a pull request against `master`

CI runs the same checks automatically on every PR.

## Adding a Data Source

Subclass `BaseSource`, declare supported datasets via `_build_dispatch()`, and register in `goblin.py`:

```python
# src/marketgoblin/sources/mysource.py
import polars as pl

from marketgoblin import Dataset
from marketgoblin.sources.base import BaseSource, Fetcher

class MySource(BaseSource):
    name = "mysource"

    def _build_dispatch(self) -> dict[Dataset, Fetcher]:
        return {Dataset.OHLCV: self._fetch_ohlcv}

    def _fetch_ohlcv(self, symbol: str, start: str, end: str) -> pl.LazyFrame:
        ...  # return a normalized LazyFrame with an is_adjusted column
```

```python
# src/marketgoblin/goblin.py
_SOURCES = {"yahoo": YahooSource, "csv": CSVSource, "mysource": MySource}
```

Per-dataset fetchers all share the `(symbol, start, end)` signature. OHLCV fetchers return a tidy stacked frame containing both adjusted and raw rows distinguished by `is_adjusted`.

## Adding a Dataset

Adding a new `Dataset` member is a four-step change:

1. Extend `Dataset` in `src/marketgoblin/datasets.py`
2. Add `_fetch_<dataset>` to relevant sources and register it in their `_build_dispatch()`
3. Add `normalize_<dataset>` in `_normalize.py` and `build_<dataset>` in `_metadata.py`
4. Extend `DiskStorage._build_metadata` dispatch in `storage/disk.py`

Add tests in `tests/test_<thing>.py` covering the happy path and error cases.

## Code Style

- Ruff for linting and formatting (PEP 8, enforced via pre-commit)
- mypy for static type checking — all public functions must be fully annotated
- Polars over pandas everywhere
- Pure functions preferred; inject dependencies for testability
- No speculative abstractions — solve the problem at hand

## Future contributions

Areas where help is especially welcome:

- Additional data sources (Polygon.io, Alpha Vantage, IBKR)
- Async `fetch()` / `fetch_many()` support
- CLI entrypoint
- More sophisticated missing-day logic (holiday calendars per exchange)
