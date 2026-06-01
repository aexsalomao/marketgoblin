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


def build_splits(
    chunk: pl.DataFrame,
    provider: str,
    symbol: str,
    ym: str,
    file_size_bytes: int,
) -> dict[str, Any]:
    """Build a metadata dict for a saved splits parquet slice.

    Splits are event-driven (rare — typically zero or one per ticker per
    decade), so no missing-days analysis applies. ``split_factor`` carries
    the per-event multiplier; min/max bound the slice for sanity-checking
    against known corporate actions.
    """
    stats = chunk.select(
        [
            pl.col("date").min().alias("start_date"),
            pl.col("date").max().alias("end_date"),
            pl.len().alias("row_count"),
            pl.col("split_factor").min().alias("split_factor_min"),
            pl.col("split_factor").max().alias("split_factor_max"),
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
        "split_factor_min": float(stats["split_factor_min"]),
        "split_factor_max": float(stats["split_factor_max"]),
    }


def build_fundamentals_daily(
    chunk: pl.DataFrame,
    provider: str,
    symbol: str,
    ym: str,
    file_size_bytes: int,
) -> dict[str, Any]:
    """Build a metadata dict for a saved daily-fundamentals parquet slice.

    Daily-cadence valuation metrics (one row per trading day). No
    missing-days analysis: the upstream endpoint occasionally drops bars
    around corporate actions, and a slice missing a few days is normal —
    not worth the noise of an OHLCV-style alarm.
    """
    stats = chunk.select(
        [
            pl.col("date").min().alias("start_date"),
            pl.col("date").max().alias("end_date"),
            pl.len().alias("row_count"),
            pl.col("market_cap").min().alias("market_cap_min"),
            pl.col("market_cap").max().alias("market_cap_max"),
            pl.col("pe_ratio").min().alias("pe_ratio_min"),
            pl.col("pe_ratio").max().alias("pe_ratio_max"),
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
        "market_cap_min": _safe_int(stats["market_cap_min"]),
        "market_cap_max": _safe_int(stats["market_cap_max"]),
        "pe_ratio_min": _safe_float(stats["pe_ratio_min"]),
        "pe_ratio_max": _safe_float(stats["pe_ratio_max"]),
    }


def build_fundamentals_statements(
    chunk: pl.DataFrame,
    provider: str,
    symbol: str,
    ym: str,
    file_size_bytes: int,
) -> dict[str, Any]:
    """Build a metadata dict for a saved quarterly-statements parquet slice.

    Quarterly cadence — typically 1 row per slice (one filing per fiscal
    quarter, landing in the filing-date month). Captures fiscal-period
    coverage plus as-reported EPS, revenue and net-income bounds (the
    headline figures driving PEAD/SUE) for sanity-checking against known
    outliers. The slice now carries the full statement surface; bounds stay
    focused on these few anchors rather than every line item.
    """
    stats = chunk.select(
        [
            pl.col("date").min().alias("start_date"),
            pl.col("date").max().alias("end_date"),
            pl.len().alias("row_count"),
            pl.col("fiscal_year").min().alias("fiscal_year_min"),
            pl.col("fiscal_year").max().alias("fiscal_year_max"),
            pl.col("eps_diluted_as_reported").min().alias("eps_diluted_as_reported_min"),
            pl.col("eps_diluted_as_reported").max().alias("eps_diluted_as_reported_max"),
            pl.col("revenue_as_reported").min().alias("revenue_as_reported_min"),
            pl.col("revenue_as_reported").max().alias("revenue_as_reported_max"),
            pl.col("net_income_as_reported").min().alias("net_income_as_reported_min"),
            pl.col("net_income_as_reported").max().alias("net_income_as_reported_max"),
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
        "fiscal_year_min": _safe_int(stats["fiscal_year_min"]),
        "fiscal_year_max": _safe_int(stats["fiscal_year_max"]),
        "eps_diluted_as_reported_min": _safe_float(stats["eps_diluted_as_reported_min"]),
        "eps_diluted_as_reported_max": _safe_float(stats["eps_diluted_as_reported_max"]),
        "revenue_as_reported_min": _safe_float(stats["revenue_as_reported_min"]),
        "revenue_as_reported_max": _safe_float(stats["revenue_as_reported_max"]),
        "net_income_as_reported_min": _safe_float(stats["net_income_as_reported_min"]),
        "net_income_as_reported_max": _safe_float(stats["net_income_as_reported_max"]),
    }


def _safe_int(value: Any) -> int | None:
    return int(value) if value is not None else None


def _safe_float(value: Any) -> float | None:
    return float(value) if value is not None else None


def write(data: dict[str, Any], path: Path) -> None:
    """Atomically write a dict as JSON at ``path``. Creates parent dirs if needed.

    Used for parquet sidecars and for standalone metadata/classification records.
    ``default=str`` keeps non-JSON-native values (Path, Enum) from exploding.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, default=str))
    os.replace(tmp, path)
