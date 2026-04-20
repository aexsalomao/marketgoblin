# Public API for sector → index mappings. Users read the shipped JSON via
# ``load_sector_indices`` and optionally re-run the private parser via
# ``refresh_sector_indices`` to rewrite it.

import json
import logging
from pathlib import Path

from marketgoblin._sector_indices_parser import (
    Industry,
    IndustryGroup,
    SectorIndex,
    SectorIndexMapping,
    SubIndustry,
    parse_us_sector_indices,
    write_mapping,
)

logger = logging.getLogger(__name__)

_DATA_DIR = Path(__file__).parent / "_sector_indices_data"
_SUPPORTED_MARKETS: frozenset[str] = frozenset({"US"})

# Market → parser. One entry per supported market; extend alongside the parser.
_PARSERS = {
    "US": parse_us_sector_indices,
}


def _market_path(market: str) -> Path:
    return _DATA_DIR / f"{market.lower()}.json"


def _normalize_market(market: str) -> str:
    upper = market.upper()
    if upper not in _SUPPORTED_MARKETS:
        raise ValueError(
            f"Unknown market '{market}'. Supported: {sorted(_SUPPORTED_MARKETS)}"
        )
    return upper


def load_sector_indices(market: str = "US") -> SectorIndexMapping:
    """Read the shipped sector → index mapping for ``market``.

    Raises:
        ValueError: If ``market`` is not supported.
        FileNotFoundError: If the shipped JSON is missing — run
            :func:`refresh_sector_indices` first.
    """
    normalized = _normalize_market(market)
    path = _market_path(normalized)
    if not path.exists():
        raise FileNotFoundError(
            f"No sector-index mapping for {normalized} at {path}. "
            "Run refresh_sector_indices() to generate it."
        )
    data = json.loads(path.read_text(encoding="utf-8"))
    return SectorIndexMapping.from_dict(data)


def refresh_sector_indices(
    market: str = "US",
    output_path: Path | None = None,
) -> SectorIndexMapping:
    """Re-run the parser for ``market`` and rewrite the JSON snapshot.

    Args:
        market: Market code (currently only ``"US"``).
        output_path: Where to write the JSON. Defaults to the shipped
            in-package location — which is read-only in a wheel install,
            so pass an explicit path when running outside a source checkout.
    """
    normalized = _normalize_market(market)
    parser = _PARSERS[normalized]
    mapping = parser()
    target = output_path or _market_path(normalized)
    write_mapping(mapping, target)
    return mapping


__all__ = [
    "Industry",
    "IndustryGroup",
    "SectorIndex",
    "SectorIndexMapping",
    "SubIndustry",
    "load_sector_indices",
    "refresh_sector_indices",
]
