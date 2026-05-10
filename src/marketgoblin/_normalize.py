# Pure helpers for per-dataset dtype normalization.
# normalize_ohlcv / normalize_shares / normalize_dividends cast incoming frames
# to the on-disk schema; parse_dates() converts the stored int32 YYYYMMDD date
# back to pl.Date.

import polars as pl

_OHLC_COLS = ["open", "high", "low", "close"]


def normalize_ohlcv(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Cast OHLC to float32, volume to int64, is_adjusted to bool, date to int32 YYYYMMDD.

    The input frame must include an ``is_adjusted`` column — OHLCV is stored as a
    tidy stacked series where adjusted and raw variants coexist and are
    distinguished by this boolean flag.
    """
    return lf.with_columns(
        [pl.col(c).cast(pl.Float32) for c in _OHLC_COLS]
        + [
            pl.col("volume").cast(pl.Int64),
            pl.col("is_adjusted").cast(pl.Boolean),
            pl.col("date").dt.strftime("%Y%m%d").cast(pl.Int32),
        ]
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


def normalize_dividends(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Cast dividend to float32 and date to int32 YYYYMMDD."""
    return lf.with_columns(
        pl.col("dividend").cast(pl.Float32),
        pl.col("date").dt.strftime("%Y%m%d").cast(pl.Int32),
    )


def normalize_splits(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Cast split_factor to float32 and date to int32 YYYYMMDD.

    ``split_factor`` is the per-event multiplier (e.g. ``2.0`` for a 2-for-1
    forward split, ``0.5`` for a 1-for-2 reverse). Float32 is sufficient —
    the factor is always a small, exactly-representable rational.
    """
    return lf.with_columns(
        pl.col("split_factor").cast(pl.Float32),
        pl.col("date").dt.strftime("%Y%m%d").cast(pl.Int32),
    )


def normalize_fundamentals_daily(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Cast the daily-fundamentals frame to the on-disk schema.

    Market-cap and enterprise-value can hit 1e13 for the largest US names —
    int64 is required (int32 caps at ~2.1e9). Ratio columns are float32:
    sufficient precision for valuation multiples and half the disk footprint
    of float64.
    """
    return lf.with_columns(
        pl.col("market_cap").cast(pl.Int64),
        pl.col("enterprise_val").cast(pl.Int64),
        pl.col("pe_ratio").cast(pl.Float32),
        pl.col("pb_ratio").cast(pl.Float32),
        pl.col("trailing_peg_1y").cast(pl.Float32),
        pl.col("date").dt.strftime("%Y%m%d").cast(pl.Int32),
    )


def normalize_statements(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Cast quarterly-statements frame to the on-disk schema.

    ``date`` carries the filing date (cast to int32 YYYYMMDD per project
    convention; ``parse_dates`` recovers it as ``pl.Date`` on load).
    Revenue stays float64 because large-cap quarterly revenue (1e11+)
    overflows float32's safe-integer range. EPS is float32 — values rarely
    exceed ±100 and the precision loss is below reporting granularity.
    """
    return lf.with_columns(
        pl.col("fiscal_year").cast(pl.Int16),
        pl.col("fiscal_quarter").cast(pl.Int8),
        pl.col("eps_diluted").cast(pl.Float32),
        pl.col("eps_basic").cast(pl.Float32),
        pl.col("revenue").cast(pl.Float64),
        pl.col("date").dt.strftime("%Y%m%d").cast(pl.Int32),
    )


def parse_dates(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Convert int32 YYYYMMDD date back to pl.Date for in-memory use."""
    return lf.with_columns(pl.col("date").cast(pl.String).str.to_date("%Y%m%d"))
