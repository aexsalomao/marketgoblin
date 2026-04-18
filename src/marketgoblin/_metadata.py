# Metadata sidecar helpers.
# build_ohlcv / build_shares compute per-slice summary stats; write() atomically
# persists the dict as a JSON sidecar next to the .pq file.

import calendar
import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl


def build_ohlcv(
    chunk: pl.DataFrame,
    provider: str,
    symbol: str,
    ym: str,
    file_size_bytes: int,
    price_adjusted: bool = True,
    currency: str = "USD",
) -> dict[str, Any]:
    """Build a metadata dict for a saved OHLCV parquet slice.

    Computes summary stats (row count, date range, OHLCV min/max) and derives
    missing trading days by comparing chunk dates against all weekdays in the month.
    """
    stats = chunk.select(
        [
            pl.col("date").min().alias("start_date"),
            pl.col("date").max().alias("end_date"),
            pl.len().alias("row_count"),
            pl.col("close").min().alias("close_min"),
            pl.col("close").max().alias("close_max"),
            pl.col("volume").min().alias("volume_min"),
            pl.col("volume").max().alias("volume_max"),
        ]
    ).row(0, named=True)

    year, month = map(int, ym.split("-"))
    last_day = calendar.monthrange(year, month)[1]
    all_weekdays = pl.date_range(
        date(year, month, 1), date(year, month, last_day), "1d", eager=True
    )
    all_weekdays = all_weekdays.filter(all_weekdays.dt.weekday() <= 5)

    weekday_ints = all_weekdays.dt.strftime("%Y%m%d").cast(pl.Int32)
    actual_dates = chunk["date"].to_list()
    missing = (
        all_weekdays.filter(~weekday_ints.is_in(actual_dates)).dt.strftime("%Y-%m-%d").to_list()
    )

    return {
        "symbol": symbol,
        "provider": provider,
        "year_month": ym,
        "row_count": stats["row_count"],
        "start_date": stats["start_date"],
        "end_date": stats["end_date"],
        "expected_trading_days": len(all_weekdays),
        "missing_days": missing,
        "columns": chunk.columns,
        "downloaded_at": datetime.now().isoformat(timespec="seconds"),
        "file_size_bytes": file_size_bytes,
        "price_adjusted": price_adjusted,
        "currency": currency,
        "close_min": float(stats["close_min"]),
        "close_max": float(stats["close_max"]),
        "volume_min": float(stats["volume_min"]),
        "volume_max": float(stats["volume_max"]),
    }


def build_shares(
    chunk: pl.DataFrame,
    provider: str,
    symbol: str,
    ym: str,
    file_size_bytes: int,
) -> dict[str, Any]:
    """Build a metadata dict for a saved shares-outstanding parquet slice.

    Shares are reported at irregular cadence (corporate-action driven), so no
    'expected days' / 'missing days' analysis applies — that's OHLCV-specific.
    """
    stats = chunk.select(
        [
            pl.col("date").min().alias("start_date"),
            pl.col("date").max().alias("end_date"),
            pl.len().alias("row_count"),
            pl.col("shares").min().alias("shares_min"),
            pl.col("shares").max().alias("shares_max"),
        ]
    ).row(0, named=True)

    return {
        "symbol": symbol,
        "provider": provider,
        "year_month": ym,
        "row_count": stats["row_count"],
        "start_date": stats["start_date"],
        "end_date": stats["end_date"],
        "columns": chunk.columns,
        "downloaded_at": datetime.now().isoformat(timespec="seconds"),
        "file_size_bytes": file_size_bytes,
        "shares_min": int(stats["shares_min"]),
        "shares_max": int(stats["shares_max"]),
    }


def write(metadata: dict[str, Any], path: Path) -> None:
    """Atomically write metadata as JSON sidecar next to the .pq file."""
    json_path = path.with_suffix(".json")
    tmp = json_path.with_name(json_path.name + ".tmp")
    tmp.write_text(json.dumps(metadata, indent=2))
    os.replace(tmp, json_path)
