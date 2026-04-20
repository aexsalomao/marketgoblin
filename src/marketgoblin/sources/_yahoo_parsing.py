# Pure adapter/parser helpers for YahooSource.
# Bridges yfinance's heterogeneous return types (FastInfo, DataFrames,
# dict-ish objects, unix timestamps) into marketgoblin's typed dataclasses.
# Tests live in tests/test_ticker_metadata.py and tests/test_classification.py.

import logging
from datetime import UTC, datetime
from typing import Any

import yfinance as yf

from marketgoblin.classification import IndustryProfile, SectorProfile
from marketgoblin.ticker_metadata import TickerMetadata

logger = logging.getLogger(__name__)


def safe_dict(obj: Any) -> dict[str, Any]:
    """Coerce yfinance objects (fast_info, history_metadata, info) into a plain dict.

    fast_info is a ``FastInfo`` object (dict-like but not a dict); history_metadata
    and info are dicts already. Returns {} on None.

    Intentionally tolerant at the yfinance boundary: the ``FastInfo`` shape has
    shifted across yfinance versions, so we swallow iteration failures and
    return an empty dict rather than propagate an implementation detail of
    upstream into our callers.
    """
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    try:
        return {k: obj[k] for k in obj.keys()}  # noqa: SIM118 — keys() is explicit on FastInfo
    except Exception:
        return {}


def safe_isin(ticker: yf.Ticker) -> str | None:
    """ISIN lookup can 404 or raise on non-equity symbols. Treat failure as missing."""
    try:
        value = ticker.isin
    except Exception:
        return None
    if not value or value in {"-", "N/A"}:
        return None
    return str(value)


def first_present(d: dict[str, Any], *keys: str) -> Any:
    """Return the first value whose key exists in d with a non-None value."""
    for key in keys:
        if key in d and d[key] is not None:
            return d[key]
    return None


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


def first_trade_date_iso(history_meta: dict[str, Any]) -> str | None:
    """yfinance returns firstTradeDate as a unix timestamp (seconds). Convert to ISO date."""
    raw = first_present(history_meta, "firstTradeDate")
    if raw is None:
        return None
    try:
        ts = int(raw)
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(ts, tz=UTC).date().isoformat()


def top_symbols(obj: Any) -> list[str]:
    """Extract ticker symbols from yfinance's heterogeneous 'top X' containers.

    ``yf.Sector.top_companies`` returns a pandas DataFrame indexed by symbol.
    ``yf.Sector.top_etfs`` returns a plain ``dict[symbol, name]``.
    """
    if obj is None:
        return []
    if hasattr(obj, "index"):  # DataFrame: index holds the tickers
        return [str(x) for x in obj.index.tolist()]
    if isinstance(obj, dict):
        return [str(k) for k in obj]
    return []


def fetch_sector_profile(key: str) -> SectorProfile | None:
    """Wrap ``yf.Sector(key)`` access. Returns None if the lookup fails entirely."""
    try:
        sector = yf.Sector(key)
    except Exception as exc:
        logger.warning("yf.Sector lookup failed | key=%s error=%s", key, exc)
        return None

    overview = safe_dict(getattr(sector, "overview", None))
    return SectorProfile(
        key=key,
        name=getattr(sector, "name", None),
        etf_symbol=getattr(sector, "symbol", None),
        market_cap=coerce_int(first_present(overview, "market_cap", "marketCap")),
        employee_count=coerce_int(
            first_present(overview, "employee_count", "employeeCount", "employees")
        ),
        top_companies=top_symbols(getattr(sector, "top_companies", None)),
        top_etfs=top_symbols(getattr(sector, "top_etfs", None)),
        industries=top_symbols(getattr(sector, "industries", None)),
    )


def fetch_industry_profile(key: str) -> IndustryProfile | None:
    """Wrap ``yf.Industry(key)`` access. Returns None if the lookup fails entirely."""
    try:
        industry = yf.Industry(key)
    except Exception as exc:
        logger.warning("yf.Industry lookup failed | key=%s error=%s", key, exc)
        return None

    return IndustryProfile(
        key=key,
        name=getattr(industry, "name", None),
        sector_key=getattr(industry, "sector_key", None),
        sector_name=getattr(industry, "sector_name", None),
        etf_symbol=getattr(industry, "symbol", None),
        top_companies=top_symbols(getattr(industry, "top_companies", None)),
        top_performing_companies=top_symbols(getattr(industry, "top_performing_companies", None)),
        top_growth_companies=top_symbols(getattr(industry, "top_growth_companies", None)),
    )


def build_ticker_metadata(
    symbol: str,
    provider: str,
    fast_info: dict[str, Any],
    history_meta: dict[str, Any],
    info: dict[str, Any],
    isin: str | None,
    is_fast: bool,
) -> TickerMetadata:
    """Merge yfinance's overlapping metadata surfaces into a TickerMetadata.

    fast_info wins for fields it provides (cheaper and more current); info fills
    in profile fields that fast_info doesn't carry.
    """
    return TickerMetadata(
        symbol=symbol,
        currency=first_present(fast_info, "currency") or first_present(info, "currency"),
        exchange=(
            first_present(fast_info, "exchange")
            or first_present(info, "exchange", "fullExchangeName")
        ),
        quote_type=(
            first_present(fast_info, "quote_type", "quoteType") or first_present(info, "quoteType")
        ),
        name=first_present(info, "longName", "shortName"),
        sector=first_present(info, "sector"),
        sector_key=first_present(info, "sectorKey"),
        sector_display=first_present(info, "sectorDisp"),
        industry=first_present(info, "industry"),
        industry_key=first_present(info, "industryKey"),
        industry_display=first_present(info, "industryDisp"),
        country=first_present(info, "country"),
        business_summary=first_present(info, "longBusinessSummary"),
        isin=isin,
        market_cap=coerce_int(
            first_present(fast_info, "market_cap", "marketCap") or first_present(info, "marketCap")
        ),
        shares_outstanding=coerce_int(
            first_present(fast_info, "shares") or first_present(info, "sharesOutstanding")
        ),
        beta=coerce_float(first_present(info, "beta")),
        trailing_pe=coerce_float(first_present(info, "trailingPE")),
        forward_pe=coerce_float(first_present(info, "forwardPE")),
        timezone=(
            first_present(history_meta, "timezone", "exchangeTimezoneName")
            or first_present(fast_info, "timezone")
            or first_present(info, "exchangeTimezoneName")
        ),
        first_trade_date=first_trade_date_iso(history_meta),
        provider=provider,
        fetched_at=datetime.now(tz=UTC).isoformat(timespec="seconds"),
        is_fast=is_fast,
    )
