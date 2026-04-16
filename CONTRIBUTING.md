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

Subclass `BaseSource`, implement `fetch()`, and register in `goblin.py`:

```python
# src/marketgoblin/sources/mysource.py
from marketgoblin.sources.base import BaseSource
import polars as pl

class MySource(BaseSource):
    name = "mysource"

    def fetch(self, symbol: str, start: str, end: str, adjusted: bool = True) -> pl.LazyFrame:
        ...  # return a normalized LazyFrame
```

```python
# src/marketgoblin/goblin.py
_SOURCES = {"yahoo": YahooSource, "csv": CSVSource, "mysource": MySource}
```

Add tests in `tests/test_mysource.py` covering the happy path and error cases.

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
