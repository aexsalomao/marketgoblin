# TickerMetadata — unified, source-agnostic ticker profile.
# Collapses yfinance's info / fast_info / history_metadata / isin into one shape.

from dataclasses import dataclass, field
from typing import Any

from marketgoblin._serialization import JSONSerializable


@dataclass(frozen=True, slots=True)
class TickerMetadata(JSONSerializable):
    """Point-in-time profile for a ticker.

    A single shape for every metadata source. Fields default to ``None`` because
    availability varies by provider and instrument type (equity vs ETF vs crypto).

    ``is_fast`` records whether the record was built from lightweight endpoints
    only (no scraped ``.info`` call). Useful for telling cheap fetches apart
    from full ones when reloading from disk.
    """

    symbol: str

    # Quote identity (available from lightweight endpoints).
    currency: str | None = None
    exchange: str | None = None
    quote_type: str | None = None

    # Profile (heavy endpoints).
    name: str | None = None
    sector: str | None = None
    sector_key: str | None = None  # slug, e.g. "technology" — feeds yf.Sector
    sector_display: str | None = None
    industry: str | None = None
    industry_key: str | None = None  # slug, e.g. "consumer-electronics" — feeds yf.Industry
    industry_display: str | None = None
    country: str | None = None
    business_summary: str | None = None
    isin: str | None = None

    # Valuation.
    market_cap: int | None = None
    shares_outstanding: int | None = None
    beta: float | None = None
    trailing_pe: float | None = None
    forward_pe: float | None = None

    # History metadata.
    timezone: str | None = None
    first_trade_date: str | None = None  # ISO 8601 date

    # Provenance.
    provider: str | None = None
    fetched_at: str | None = None  # ISO 8601 datetime
    is_fast: bool = False

    # Anything the caller wants to keep but isn't modeled above.
    extras: dict[str, Any] = field(default_factory=dict)
