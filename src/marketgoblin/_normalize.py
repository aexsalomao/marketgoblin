import polars as pl

_NUMERIC_COLS = ["open", "high", "low", "close", "volume"]


def normalize(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Cast numerics to float32 and date to int32 YYYYMMDD (e.g. 20260101)."""
    return lf.with_columns(
        [pl.col(c).cast(pl.Float32) for c in _NUMERIC_COLS]
        + [pl.col("date").dt.strftime("%Y%m%d").cast(pl.Int32)]
    )


def parse_dates(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Convert int32 YYYYMMDD date back to pl.Date for in-memory use."""
    return lf.with_columns(
        pl.col("date").cast(pl.Utf8).str.to_date("%Y%m%d")
    )
