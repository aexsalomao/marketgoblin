# Pure adapter/parser helpers for TiingoSource, split by concern:
#   common        — coercions, slugify, ISO-date parsing
#   prices        — OHLCV / dividends / splits from the prices endpoint
#   fundamentals  — daily valuation, quarterly statements, derived shares
#   metadata      — ticker metadata + sector/industry classification
# This package re-exports the orchestrator-facing surface so TiingoSource (and
# legacy importers) can keep doing `from ..._tiingo_parsing import <name>`.
# Tests targeting a single concern import from the concrete submodule.

from marketgoblin.sources._tiingo_parsing.fundamentals import (
    derive_shares_from_marketcap,
    fundamentals_daily_rows_to_lf,
    statements_rows_to_lf,
)
from marketgoblin.sources._tiingo_parsing.metadata import (
    build_tiingo_classification,
    build_tiingo_metadata,
    fetch_fundamentals_meta,
    fetch_latest_close,
    fetch_latest_fundamentals,
)
from marketgoblin.sources._tiingo_parsing.prices import (
    prices_rows_to_dividends,
    prices_rows_to_splits,
    prices_rows_to_stacked_ohlcv,
)

__all__ = [
    "build_tiingo_classification",
    "build_tiingo_metadata",
    "derive_shares_from_marketcap",
    "fetch_fundamentals_meta",
    "fetch_latest_close",
    "fetch_latest_fundamentals",
    "fundamentals_daily_rows_to_lf",
    "prices_rows_to_dividends",
    "prices_rows_to_splits",
    "prices_rows_to_stacked_ohlcv",
    "statements_rows_to_lf",
]
