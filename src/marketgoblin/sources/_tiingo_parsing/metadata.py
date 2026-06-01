# Ticker metadata + sector/industry classification adapters. Bridges Tiingo's
# get_ticker_metadata / get_fundamentals_daily and the raw /fundamentals/meta
# REST endpoint (not wrapped by the Python client) into TickerMetadata and
# Classification dataclasses.

from datetime import UTC, datetime, timedelta
from typing import Any, cast

import requests  # type: ignore[import-untyped]

from marketgoblin.classification import Classification, IndustryProfile, SectorProfile
from marketgoblin.sources._tiingo_parsing.common import (
    coerce_float,
    coerce_int,
    first_present,
    slugify,
)
from marketgoblin.ticker_metadata import TickerMetadata

_TIINGO_BASE_URL = "https://api.tiingo.com"
_FUNDAMENTALS_META_PATH = "/tiingo/fundamentals/meta"
_REQUEST_TIMEOUT_SECONDS = 10

# Window for the "latest row" lookups feeding fetch_metadata. 7 calendar days
# covers any normal long-weekend gap (≥ 5 trading days).
_LATEST_LOOKBACK_DAYS = 7

# Tiingo's daily endpoint serves USD prices on standard plans; metadata
# doesn't carry a currency field, so this is the documented default.
_DEFAULT_CURRENCY = "USD"


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
