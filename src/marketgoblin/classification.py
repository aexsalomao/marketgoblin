# Classification — sector and industry profiles for a ticker.
# Backed by yfinance's Sector / Industry APIs; source-agnostic in shape.

from dataclasses import dataclass, field
from typing import Any, Self

from marketgoblin._serialization import JSONSerializable


@dataclass(frozen=True, slots=True)
class SectorProfile(JSONSerializable):
    """Sector-level profile (slug, representative ETF, constituents)."""

    key: str
    name: str | None = None
    etf_symbol: str | None = None  # Yahoo's representative sector ETF (e.g. XLK for tech).
    market_cap: int | None = None
    employee_count: int | None = None
    top_companies: list[str] = field(default_factory=list)  # ticker symbols
    top_etfs: list[str] = field(default_factory=list)  # ticker symbols
    industries: list[str] = field(default_factory=list)  # industry keys within this sector


@dataclass(frozen=True, slots=True)
class IndustryProfile(JSONSerializable):
    """Industry-level profile (slug, parent sector, constituents)."""

    key: str
    name: str | None = None
    sector_key: str | None = None
    sector_name: str | None = None
    etf_symbol: str | None = None
    top_companies: list[str] = field(default_factory=list)
    top_performing_companies: list[str] = field(default_factory=list)
    top_growth_companies: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class Classification(JSONSerializable):
    """Bundled sector + industry lookup for a ticker.

    Either profile may be ``None`` if the upstream data is missing (e.g. ETFs,
    crypto, or tickers whose sector/industry keys aren't populated).

    Inherits ``to_dict`` from :class:`JSONSerializable` — ``asdict`` recurses
    through the nested profiles — but overrides ``from_dict`` to rebuild them
    as :class:`SectorProfile` / :class:`IndustryProfile` instances rather than
    raw dicts.
    """

    symbol: str
    sector: SectorProfile | None = None
    industry: IndustryProfile | None = None
    provider: str | None = None
    fetched_at: str | None = None  # ISO 8601 datetime

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        sector = SectorProfile.from_dict(data["sector"]) if data.get("sector") else None
        industry = IndustryProfile.from_dict(data["industry"]) if data.get("industry") else None
        return cls(
            symbol=data["symbol"],
            sector=sector,
            industry=industry,
            provider=data.get("provider"),
            fetched_at=data.get("fetched_at"),
        )
