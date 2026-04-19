# Dataset enum — closed set of supported data types (OHLCV, shares, dividends, ...).
# StrEnum so members serialize directly to path segments and JSON.

from enum import StrEnum


class Dataset(StrEnum):
    """Datasets that marketgoblin can fetch and store."""

    OHLCV = "ohlcv"
    SHARES = "shares"
    DIVIDENDS = "dividends"
