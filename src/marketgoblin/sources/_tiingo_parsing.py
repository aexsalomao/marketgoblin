# Pure adapter/parser helpers for TiingoSource.
# Bridges Tiingo's JSON shapes (list[dict] from TiingoClient + raw /fundamentals/meta
# REST responses) into marketgoblin's typed dataclasses and on-disk frame schema.
# Tests live in tests/test_tiingo.py.

import logging
import re
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import polars as pl
import requests  # type: ignore[import-untyped]

from marketgoblin.classification import Classification, IndustryProfile, SectorProfile
from marketgoblin.ticker_metadata import TickerMetadata

logger = logging.getLogger(__name__)

_TIINGO_BASE_URL = "https://api.tiingo.com"
_FUNDAMENTALS_META_PATH = "/tiingo/fundamentals/meta"
_REQUEST_TIMEOUT_SECONDS = 10

# Window for the "latest row" lookups feeding fetch_metadata. 7 calendar days
# covers any normal long-weekend gap (≥ 5 trading days).
_LATEST_LOOKBACK_DAYS = 7

# Tiingo's daily endpoint serves USD prices on standard plans; metadata
# doesn't carry a currency field, so this is the documented default.
_DEFAULT_CURRENCY = "USD"

# Tiingo's get_fundamentals_daily endpoint exposes valuation metrics
# (marketCap, peRatio, pbRatio, ...) but NOT a shares-outstanding field —
# absolute shares live on the quarterly statements endpoint. We derive a
# daily shares series as round(marketCap / close) by joining the prices and
# fundamentals payloads.


def coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def first_present(d: dict[str, Any], *keys: str) -> Any:
    """Return the first value whose key exists in d with a non-None value."""
    for key in keys:
        if key in d and d[key] is not None:
            return d[key]
    return None


_SLUG_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def slugify(value: str) -> str:
    """Lower-cased, hyphenated slug. ``"Information Technology"`` → ``"information-technology"``."""
    return _SLUG_NON_ALNUM.sub("-", value.lower()).strip("-")


def parse_tiingo_date_col(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Parse Tiingo's ISO date strings to ``pl.Date``.

    Tiingo returns ``"YYYY-MM-DDT00:00:00.000Z"``; sliced to the first 10 chars
    we get a clean ``YYYY-MM-DD`` parsable by polars.
    """
    return lf.with_columns(pl.col("date").str.slice(0, 10).str.to_date("%Y-%m-%d"))


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


def derive_shares_from_marketcap(
    prices_rows: list[dict[str, Any]],
    fundamentals_rows: list[dict[str, Any]],
    symbol: str,
) -> pl.LazyFrame:
    """Join daily prices and fundamentals on date, derive ``shares = marketCap / close``.

    Tiingo's daily Fundamentals endpoint carries ``marketCap`` but no absolute
    shares field, so we recover shares from market cap and the same-day raw
    close. The resulting frame is daily-cadence, matching the shape Yahoo's
    ``get_shares_full`` produces (one row per trading day).

    Raises ``ValueError`` when either upstream payload is empty or no rows
    survive the join (no overlapping trading days).
    """
    if not prices_rows:
        raise ValueError(f"No price data returned for {symbol}")
    if not fundamentals_rows:
        raise ValueError(f"No fundamentals data returned for {symbol}")

    closes = pl.from_dicts(prices_rows).lazy().pipe(parse_tiingo_date_col).select("date", "close")
    market_caps = (
        pl.from_dicts(fundamentals_rows)
        .lazy()
        .pipe(parse_tiingo_date_col)
        .select("date", "marketCap")
        .filter(pl.col("marketCap") > 0)
    )

    # Materialize once: we need both an emptiness check (the join is the only
    # place the "no overlap" failure can be detected) and a LazyFrame for the
    # caller. Re-wrapping the eager result in .lazy() keeps the public contract
    # while paying for plan execution exactly once.
    derived = (
        closes.join(market_caps, on="date", how="inner")
        .filter(pl.col("close") > 0)
        .with_columns(
            (pl.col("marketCap") / pl.col("close")).round().cast(pl.Int64).alias("shares"),
            pl.lit(symbol.upper()).alias("symbol"),
        )
        .select("date", "shares", "symbol")
        .unique(subset=["date"], keep="last")
        .sort("date")
        .collect()
    )

    if derived.is_empty():
        raise ValueError(f"No overlapping trading days in prices+fundamentals for {symbol}")
    return derived.lazy()


def fetch_latest_close(client: Any, symbol: str) -> float | None:
    """Pull the most recent raw close price for a ticker.

    Used to derive ``shares_outstanding`` for ``TickerMetadata`` from the
    paired ``marketCap`` value. Restricts the call to a small lookback window
    so the response stays cheap.
    """
    today = datetime.now(tz=UTC).date()
    start = (today - timedelta(days=_LATEST_LOOKBACK_DAYS)).isoformat()
    rows = client.get_ticker_price(
        symbol.lower(),
        startDate=start,
        endDate=today.isoformat(),
        fmt="json",
        frequency="daily",
    )
    if not rows:
        return None
    return coerce_float(rows[-1].get("close"))


def fetch_latest_fundamentals(client: Any, symbol: str) -> dict[str, Any] | None:
    """Pull the most recent daily-fundamentals row for a ticker.

    Restricts the call to a small lookback window so the response stays cheap
    even on actively-traded tickers. Returns ``None`` when Tiingo has no recent
    rows (uncommon for liquid US equities, but possible for newly listed names).
    """
    today = datetime.now(tz=UTC).date()
    start = (today - timedelta(days=_LATEST_LOOKBACK_DAYS)).isoformat()
    rows = client.get_fundamentals_daily(
        symbol.lower(),
        startDate=start,
        endDate=today.isoformat(),
        fmt="json",
    )
    if not rows:
        return None
    return cast(dict[str, Any], rows[-1])


def build_tiingo_metadata(
    symbol: str,
    provider: str,
    meta: dict[str, Any],
    fundamentals_row: dict[str, Any] | None,
    latest_close: float | None,
    *,
    is_fast: bool,
) -> TickerMetadata:
    """Merge Tiingo's metadata + (optional) latest-fundamentals row into a TickerMetadata.

    ``shares_outstanding`` is derived from ``marketCap / latest_close`` because
    Tiingo's daily Fundamentals endpoint doesn't expose shares directly.

    Fields Tiingo doesn't expose (``isin``, ``beta``, ``forward_pe``, ``country``,
    ``timezone``, ``quote_type``) are left at their dataclass ``None`` default.
    """
    fundamentals = fundamentals_row or {}
    market_cap = coerce_int(first_present(fundamentals, "marketCap"))
    shares = (
        round(market_cap / latest_close)
        if market_cap is not None and latest_close is not None and latest_close > 0
        else None
    )

    return TickerMetadata(
        symbol=symbol,
        currency=first_present(meta, "currency") or _DEFAULT_CURRENCY,
        exchange=first_present(meta, "exchangeCode", "exchange"),
        name=first_present(meta, "name"),
        business_summary=first_present(meta, "description"),
        first_trade_date=first_present(meta, "startDate"),
        market_cap=market_cap,
        shares_outstanding=shares,
        trailing_pe=coerce_float(first_present(fundamentals, "peRatio", "trailingPE")),
        provider=provider,
        fetched_at=datetime.now(tz=UTC).isoformat(timespec="seconds"),
        is_fast=is_fast,
    )


def fetch_fundamentals_meta(symbol: str, api_key: str | None) -> dict[str, Any]:
    """GET ``/tiingo/fundamentals/meta`` for a single ticker.

    Returns the first list element, or ``{}`` when Tiingo returns an empty list
    (so :func:`build_tiingo_classification` degrades to a Classification with
    null sub-profiles instead of raising).
    """
    response = requests.get(
        f"{_TIINGO_BASE_URL}{_FUNDAMENTALS_META_PATH}",
        params={"tickers": symbol.lower()},
        headers={"Authorization": f"Token {api_key}"},
        timeout=_REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    if not payload:
        return {}
    return cast(dict[str, Any], payload[0])


def build_tiingo_classification(
    symbol: str,
    provider: str,
    meta_row: dict[str, Any],
) -> Classification:
    """Build a Classification from a /fundamentals/meta row.

    Tiingo doesn't expose constituent data (top companies, ETFs, market cap),
    so the SectorProfile / IndustryProfile sub-fields stay at their defaults.
    """
    sector_name = first_present(meta_row, "sector")
    industry_name = first_present(meta_row, "industry")

    sector = SectorProfile(key=slugify(sector_name), name=sector_name) if sector_name else None
    industry = (
        IndustryProfile(
            key=slugify(industry_name),
            name=industry_name,
            sector_key=slugify(sector_name) if sector_name else None,
            sector_name=sector_name,
        )
        if industry_name
        else None
    )

    return Classification(
        symbol=symbol,
        sector=sector,
        industry=industry,
        provider=provider,
        fetched_at=datetime.now(tz=UTC).isoformat(timespec="seconds"),
    )
