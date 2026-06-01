# Prices-endpoint parsers. Tiingo's daily prices payload carries raw OHLCV,
# adjusted OHLCV (adj* columns), divCash and splitFactor in one shape, so
# OHLCV / DIVIDENDS / SPLITS are all projected from the same base frame here.

from typing import Any

import polars as pl

from marketgoblin.sources._tiingo_parsing.common import parse_tiingo_date_col


def prices_rows_to_base_lf(rows: list[dict[str, Any]], symbol: str) -> pl.LazyFrame:
    """Wrap Tiingo's prices payload in a LazyFrame with a parsed date and uppercase symbol.

    Raises ``ValueError`` on empty input — the caller's ``_retry_fetch`` treats
    this as a non-transient error and propagates immediately.
    """
    if not rows:
        raise ValueError(f"No OHLCV data returned for {symbol}")
    return (
        pl.from_dicts(rows)
        .lazy()
        .pipe(parse_tiingo_date_col)
        .with_columns(pl.lit(symbol.upper()).alias("symbol"))
    )


def build_raw_ohlcv_lf(base: pl.LazyFrame) -> pl.LazyFrame:
    """Project the raw OHLCV variant from a parsed prices frame."""
    return base.select(
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "symbol",
        pl.lit(False).alias("is_adjusted"),
    )


def build_adjusted_ohlcv_lf(base: pl.LazyFrame) -> pl.LazyFrame:
    """Project the adjusted OHLCV variant, renaming ``adj*`` columns to canonical names."""
    return base.select(
        "date",
        pl.col("adjOpen").alias("open"),
        pl.col("adjHigh").alias("high"),
        pl.col("adjLow").alias("low"),
        pl.col("adjClose").alias("close"),
        pl.col("adjVolume").alias("volume"),
        "symbol",
        pl.lit(True).alias("is_adjusted"),
    )


def stack_ohlcv(adjusted: pl.LazyFrame, raw: pl.LazyFrame) -> pl.LazyFrame:
    """Concatenate adjusted + raw and sort to the on-disk row order."""
    return pl.concat([adjusted, raw]).sort(["date", "is_adjusted"])


def prices_rows_to_stacked_ohlcv(rows: list[dict[str, Any]], symbol: str) -> pl.LazyFrame:
    """Compose the full prices→stacked OHLCV pipeline."""
    base = prices_rows_to_base_lf(rows, symbol)
    return stack_ohlcv(build_adjusted_ohlcv_lf(base), build_raw_ohlcv_lf(base))


def prices_rows_to_dividends(rows: list[dict[str, Any]], symbol: str) -> pl.LazyFrame:
    """Extract dividend events (``divCash > 0``) from the prices payload."""
    base = prices_rows_to_base_lf(rows, symbol)
    return base.filter(pl.col("divCash") > 0).select(
        "date",
        pl.col("divCash").alias("dividend"),
        "symbol",
    )


def prices_rows_to_splits(rows: list[dict[str, Any]], symbol: str) -> pl.LazyFrame:
    """Extract split events (``splitFactor != 1.0``) from the prices payload.

    Tiingo emits ``splitFactor`` on every prices row; non-event days carry
    ``1.0``. The output frame is event-only — one row per actual split.
    """
    base = prices_rows_to_base_lf(rows, symbol)
    return base.filter(pl.col("splitFactor") != 1.0).select(
        "date",
        pl.col("splitFactor").alias("split_factor"),
        "symbol",
    )
