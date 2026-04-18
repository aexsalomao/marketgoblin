# Pure helpers for per-dataset dtype normalization.
# normalize_ohlcv / normalize_shares cast incoming frames to the on-disk schema;
# parse_dates() converts the stored int32 YYYYMMDD date back to pl.Date.

import polars as pl

_OHLC_COLS = ["open", "high", "low", "close"]


def normalize_ohlcv(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Cast OHLC to float32, volume to int64, and date to int32 YYYYMMDD (e.g. 20260101)."""
    return lf.with_columns(
        [pl.col(c).cast(pl.Float32) for c in _OHLC_COLS]
        + [pl.col("volume").cast(pl.Int64)]
        + [pl.col("date").dt.strftime("%Y%m%d").cast(pl.Int32)]
    )


def normalize_shares(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Cast shares to int64 and date to int32 YYYYMMDD.

    Shares outstanding are integer counts; int64 is required because large-cap
    counts (e.g. AAPL ~1.5e10) overflow int32.
    """
    return lf.with_columns(
        pl.col("shares").cast(pl.Int64),
        pl.col("date").dt.strftime("%Y%m%d").cast(pl.Int32),
    )


def parse_dates(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Convert int32 YYYYMMDD date back to pl.Date for in-memory use."""
    return lf.with_columns(pl.col("date").cast(pl.String).str.to_date("%Y%m%d"))
