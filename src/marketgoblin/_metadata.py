# Metadata sidecar helpers.
# build_ohlcv / build_shares / build_dividends compute per-slice summary stats;
# write() atomically persists the dict as a JSON sidecar next to the .pq file.

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
    currency: str = "USD",
) -> dict[str, Any]:
    """Build a metadata dict for a saved OHLCV parquet slice.

    OHLCV slices are tidy stacked frames: each trading day may appear with
    ``is_adjusted=True`` and/or ``is_adjusted=False``. Missing-day analysis is
    performed on the set of *unique* dates in the chunk, so holidays surface
    once regardless of how many variants are stored.
    """
    stats = chunk.select(
        [
            pl.col("date").min().alias("start_date"),
            pl.col("date").max().alias("end_date"),
            pl.len().alias("row_count"),
            pl.col("date").n_unique().alias("unique_days"),
            pl.col("close").min().alias("close_min"),
            pl.col("close").max().alias("close_max"),
            pl.col("volume").min().alias("volume_min"),
            pl.col("volume").max().alias("volume_max"),
            pl.col("is_adjusted").any().alias("has_adjusted"),
            (~pl.col("is_adjusted")).any().alias("has_raw"),
        ]
    ).row(0, named=True)

    year, month = map(int, ym.split("-"))
    last_day = calendar.monthrange(year, month)[1]
    all_weekdays = pl.date_range(
        date(year, month, 1), date(year, month, last_day), "1d", eager=True
    )
    all_weekdays = all_weekdays.filter(all_weekdays.dt.weekday() <= 5)

    weekday_ints = all_weekdays.dt.strftime("%Y%m%d").cast(pl.Int32)
    actual_dates = chunk["date"].unique().to_list()
    missing = (
        all_weekdays.filter(~weekday_ints.is_in(actual_dates)).dt.strftime("%Y-%m-%d").to_list()
    )

    return {
        "symbol": symbol,
        "provider": provider,
        "year_month": ym,
        "row_count": stats["row_count"],
        "unique_days": stats["unique_days"],
        "start_date": stats["start_date"],
        "end_date": stats["end_date"],
        "expected_trading_days": len(all_weekdays),
        "missing_days": missing,
        "columns": chunk.columns,
        "downloaded_at": datetime.now().isoformat(timespec="seconds"),
        "file_size_bytes": file_size_bytes,
        "has_adjusted": bool(stats["has_adjusted"]),
        "has_raw": bool(stats["has_raw"]),
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


def build_dividends(
    chunk: pl.DataFrame,
    provider: str,
    symbol: str,
    ym: str,
    file_size_bytes: int,
    currency: str = "USD",
) -> dict[str, Any]:
    """Build a metadata dict for a saved dividends parquet slice.

    Dividends are event-driven (typically quarterly), so no missing-days
    analysis applies.
    """
    stats = chunk.select(
        [
            pl.col("date").min().alias("start_date"),
            pl.col("date").max().alias("end_date"),
            pl.len().alias("row_count"),
            pl.col("dividend").min().alias("dividend_min"),
            pl.col("dividend").max().alias("dividend_max"),
            pl.col("dividend").sum().alias("dividend_total"),
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
        "currency": currency,
        "dividend_min": float(stats["dividend_min"]),
        "dividend_max": float(stats["dividend_max"]),
        "dividend_total": float(stats["dividend_total"]),
    }


def write(data: dict[str, Any], path: Path) -> None:
    """Atomically write a dict as JSON at ``path``. Creates parent dirs if needed.

    Used for parquet sidecars and for standalone metadata/classification records.
    ``default=str`` keeps non-JSON-native values (Path, Enum) from exploding.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, default=str))
    os.replace(tmp, path)
