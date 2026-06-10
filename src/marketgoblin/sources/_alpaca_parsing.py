# Pure parsing helpers behind AlpacaSource. Projects Alpaca's Data API v2
# /trades JSON rows into the project's tidy frame shape. No network, no state —
# the HTTP pagination lives in the AlpacaSource orchestrator.

from typing import Any

import polars as pl

# Alpaca trade payload keys -> tidy column names (Data API v2 /stocks/{sym}/trades).
_TRADE_FIELD_MAP: dict[str, str] = {
    "t": "timestamp",
    "x": "exchange",
    "p": "price",
    "s": "size",
    "c": "conditions",
    "i": "trade_id",
    "z": "tape",
}

# Optional fields Alpaca may omit from EVERY row of a page (e.g. `c`/`i` on the
# IEX feed). When a key is absent from all rows polars drops the column entirely,
# so we backfill it as a typed null — otherwise normalize_trades' cast/select
# would raise ColumnNotFoundError. `timestamp` is the one required field.
_OPTIONAL_COLUMN_DTYPES: dict[str, pl.DataType] = {
    "exchange": pl.String(),
    "price": pl.Float64(),
    "size": pl.Int64(),
    "conditions": pl.List(pl.String()),
    "trade_id": pl.Int64(),
    "tape": pl.String(),
}


def trades_rows_to_lf(rows: list[dict[str, Any]], symbol: str) -> pl.LazyFrame:
    """Project raw Alpaca trade dicts into a tidy frame keyed on a UTC timestamp.

    The on-disk ``symbol`` is upper-cased. Timestamps are parsed to nanosecond
    UTC datetimes; dtype casting, the derived int32 ``date``, and the canonical
    column order are applied by :func:`marketgoblin._normalize.normalize_trades`.

    Raises:
        ValueError: If ``rows`` is empty (no trades in the window) — propagated
            without retry, matching the other sources' empty-data contract.
    """
    if not rows:
        raise ValueError(f"No trades data returned for {symbol}")

    projected = [
        {_TRADE_FIELD_MAP[k]: v for k, v in row.items() if k in _TRADE_FIELD_MAP} for row in rows
    ]
    df = pl.DataFrame(projected).with_columns(
        pl.col("timestamp").str.to_datetime(time_unit="ns", time_zone="UTC"),
        pl.lit(symbol.upper()).alias("symbol"),
    )
    # Backfill any optional column absent from the whole page as a typed null.
    missing = [
        pl.lit(None).cast(dtype).alias(name)
        for name, dtype in _OPTIONAL_COLUMN_DTYPES.items()
        if name not in df.columns
    ]
    return df.with_columns(missing).lazy() if missing else df.lazy()
