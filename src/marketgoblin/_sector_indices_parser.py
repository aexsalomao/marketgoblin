# Private parser for sector → index mappings. Loads the curated 4-level GICS
# taxonomy (sector > industry group > industry > sub-industry), scrapes the
# S&P 500 constituents page for sub-industry counts, and rolls them up to
# produce the shipped JSON snapshot.

import json
import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Self
from urllib.request import Request, urlopen

from marketgoblin._serialization import JSONSerializable

logger = logging.getLogger(__name__)

SP500_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
_USER_AGENT = "marketgoblin/sector-indices-parser (+https://github.com/aexsalomao/marketgoblin)"
_HTTP_TIMEOUT_S = 30
_GICS_SECTOR_HEADER = "gics sector"
_GICS_SUB_INDUSTRY_HEADER = "gics sub-industry"

_DATA_DIR = Path(__file__).parent / "_sector_indices_data"
_TAXONOMY_PATH = _DATA_DIR / "gics_taxonomy_us.json"

# GICS sector name → (SPDR Select Sector ETF, S&P 500 sector index symbol).
# Keys must match the canonical sector names in the GICS taxonomy JSON.
_US_SECTOR_TICKERS: dict[str, tuple[str, str]] = {
    "Energy": ("XLE", "S5ENRS"),
    "Materials": ("XLB", "S5MATR"),
    "Industrials": ("XLI", "S5INDU"),
    "Consumer Discretionary": ("XLY", "S5COND"),
    "Consumer Staples": ("XLP", "S5CONS"),
    "Health Care": ("XLV", "S5HLTH"),
    "Financials": ("XLF", "S5FINL"),
    "Information Technology": ("XLK", "S5INFT"),
    "Communication Services": ("XLC", "S5TELS"),
    "Utilities": ("XLU", "S5UTIL"),
    "Real Estate": ("XLRE", "S5RLST"),
}


@dataclass(frozen=True, slots=True)
class SubIndustry(JSONSerializable):
    """GICS level 4: the most granular classification."""

    code: str  # 8-digit GICS code
    name: str
    constituent_count: int = 0


@dataclass(frozen=True, slots=True)
class Industry(JSONSerializable):
    """GICS level 3: groups related sub-industries."""

    code: str  # 6-digit GICS code
    name: str
    constituent_count: int = 0
    sub_industries: list[SubIndustry] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        return cls(
            code=data["code"],
            name=data["name"],
            constituent_count=data.get("constituent_count", 0),
            sub_industries=[SubIndustry.from_dict(s) for s in data.get("sub_industries", [])],
        )


@dataclass(frozen=True, slots=True)
class IndustryGroup(JSONSerializable):
    """GICS level 2: groups related industries within a sector."""

    code: str  # 4-digit GICS code
    name: str
    constituent_count: int = 0
    industries: list[Industry] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        return cls(
            code=data["code"],
            name=data["name"],
            constituent_count=data.get("constituent_count", 0),
            industries=[Industry.from_dict(i) for i in data.get("industries", [])],
        )


@dataclass(frozen=True, slots=True)
class SectorIndex(JSONSerializable):
    """GICS level 1 sector mapped to its US index/ETF tickers plus the
    full 4-level breakdown (industry groups → industries → sub-industries).
    """

    sector_name: str
    spdr_etf: str
    sp500_index_symbol: str
    code: str | None = None  # 2-digit GICS sector code
    constituent_count: int | None = None  # total S&P 500 members in this sector
    industry_groups: list[IndustryGroup] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        return cls(
            sector_name=data["sector_name"],
            spdr_etf=data["spdr_etf"],
            sp500_index_symbol=data["sp500_index_symbol"],
            code=data.get("code"),
            constituent_count=data.get("constituent_count"),
            industry_groups=[IndustryGroup.from_dict(g) for g in data.get("industry_groups", [])],
        )


@dataclass(frozen=True, slots=True)
class SectorIndexMapping(JSONSerializable):
    """All GICS sector → index mappings for a single market snapshot."""

    market: str
    source_url: str
    fetched_at: str
    sectors: list[SectorIndex] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        return cls(
            market=data["market"],
            source_url=data["source_url"],
            fetched_at=data["fetched_at"],
            sectors=[SectorIndex.from_dict(s) for s in data.get("sectors", [])],
        )


class _WikitableParser(HTMLParser):
    """Extract rows from the first ``<table class="wikitable">`` on a page."""

    def __init__(self) -> None:
        super().__init__()
        self._in_target_table = False
        self._table_depth = 0
        self._in_cell = False
        self._cell_buf: list[str] = []
        self._current_row: list[str] = []
        self._header_index: dict[str, int] | None = None
        self.rows: list[dict[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "table":
            if self._in_target_table:
                self._table_depth += 1
                return
            classes = dict(attrs).get("class", "") or ""
            if "wikitable" in classes.split():
                self._in_target_table = True
                self._table_depth = 1
            return

        if not self._in_target_table:
            return

        if tag == "tr":
            self._current_row = []
        elif tag in ("th", "td"):
            self._in_cell = True
            self._cell_buf = []

    def handle_endtag(self, tag: str) -> None:
        if not self._in_target_table:
            return

        if tag == "table":
            self._table_depth -= 1
            if self._table_depth == 0:
                self._in_target_table = False
            return

        if tag in ("th", "td") and self._in_cell:
            cell = "".join(self._cell_buf).strip()
            self._current_row.append(cell)
            self._in_cell = False
            self._cell_buf = []
        elif tag == "tr":
            self._finish_row()

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._cell_buf.append(data)

    def _finish_row(self) -> None:
        if not self._current_row:
            return
        if self._header_index is None:
            self._header_index = {cell.lower(): idx for idx, cell in enumerate(self._current_row)}
            return
        row = {
            name: self._current_row[idx]
            for name, idx in self._header_index.items()
            if idx < len(self._current_row)
        }
        self.rows.append(row)


def _default_fetcher(url: str) -> str:
    """Fetch a URL and return the response body as text. Injectable for tests."""
    request = Request(url, headers={"User-Agent": _USER_AGENT})
    with urlopen(request, timeout=_HTTP_TIMEOUT_S) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        body: bytes = response.read()
        return body.decode(charset)


def _extract_constituents(html: str) -> list[tuple[str, str]]:
    """Parse the S&P 500 constituents page; return (sector, sub_industry) per row."""
    parser = _WikitableParser()
    parser.feed(html)
    parser.close()

    if not parser.rows:
        raise ValueError("No wikitable rows found — has the source page changed?")

    first = parser.rows[0]
    for header in (_GICS_SECTOR_HEADER, _GICS_SUB_INDUSTRY_HEADER):
        if header not in first:
            raise ValueError(f"No '{header}' column in parsed rows — has the source page changed?")

    return [
        (row[_GICS_SECTOR_HEADER], row[_GICS_SUB_INDUSTRY_HEADER])
        for row in parser.rows
        if row.get(_GICS_SECTOR_HEADER) and row.get(_GICS_SUB_INDUSTRY_HEADER)
    ]


def load_taxonomy(path: Path | None = None) -> dict[str, Any]:
    """Load the curated GICS 4-level taxonomy JSON. Injectable for tests."""
    source = path or _TAXONOMY_PATH
    return json.loads(source.read_text(encoding="utf-8"))  # type: ignore[no-any-return]


def _build_sub_industry_index(taxonomy: dict[str, Any]) -> dict[str, tuple[str, str, str]]:
    """Return ``sub_industry_name → (sector_name, industry_group_code, industry_code)``.

    This flat index lets the join step (O(n) over scraped rows) look up the
    full parent chain for each sub-industry without walking the nested tree.
    """
    index: dict[str, tuple[str, str, str]] = {}
    for sector in taxonomy["sectors"]:
        for group in sector["industry_groups"]:
            for industry in group["industries"]:
                for sub in industry["sub_industries"]:
                    name = sub["name"]
                    if name in index and index[name] != (
                        sector["name"],
                        group["code"],
                        industry["code"],
                    ):
                        raise ValueError(
                            f"Sub-industry '{name}' appears twice in taxonomy "
                            "with different parents."
                        )
                    index[name] = (sector["name"], group["code"], industry["code"])
    return index


def _build_mapping(
    constituents: list[tuple[str, str]],
    taxonomy: dict[str, Any],
    source_url: str,
    market: str,
) -> SectorIndexMapping:
    """Join scraped constituents against the curated taxonomy and roll up counts.

    Each level's ``constituent_count`` is the sum of its children. Empty
    sub-industries/industries/groups are preserved in the output tree — they
    just have a count of 0.

    Raises:
        ValueError: If upstream reports a sub-industry not in the taxonomy,
            or a sector without a configured ticker.
    """
    sub_lookup = _build_sub_industry_index(taxonomy)

    unknown_subs = {sub for _, sub in constituents if sub not in sub_lookup}
    if unknown_subs:
        raise ValueError(
            f"Unknown GICS sub-industries from upstream: {sorted(unknown_subs)}. "
            "Update gics_taxonomy_us.json before refreshing."
        )

    unknown_sectors = {sector for sector, _ in constituents if sector not in _US_SECTOR_TICKERS}
    if unknown_sectors:
        raise ValueError(
            f"Unexpected GICS sectors from upstream: {sorted(unknown_sectors)}. "
            "Update _US_SECTOR_TICKERS before refreshing."
        )

    sub_counts: Counter[str] = Counter(sub for _, sub in constituents)

    sectors: list[SectorIndex] = []
    for sector_def in taxonomy["sectors"]:
        sector_name = sector_def["name"]
        if sector_name not in _US_SECTOR_TICKERS:
            raise ValueError(
                f"Taxonomy sector '{sector_name}' has no configured ticker in _US_SECTOR_TICKERS."
            )
        etf, index_symbol = _US_SECTOR_TICKERS[sector_name]

        groups: list[IndustryGroup] = []
        sector_total = 0
        for group_def in sector_def["industry_groups"]:
            industries: list[Industry] = []
            group_total = 0
            for industry_def in group_def["industries"]:
                subs = [
                    SubIndustry(
                        code=sub["code"],
                        name=sub["name"],
                        constituent_count=sub_counts.get(sub["name"], 0),
                    )
                    for sub in industry_def["sub_industries"]
                ]
                industry_total = sum(s.constituent_count for s in subs)
                group_total += industry_total
                industries.append(
                    Industry(
                        code=industry_def["code"],
                        name=industry_def["name"],
                        constituent_count=industry_total,
                        sub_industries=subs,
                    )
                )
            sector_total += group_total
            groups.append(
                IndustryGroup(
                    code=group_def["code"],
                    name=group_def["name"],
                    constituent_count=group_total,
                    industries=industries,
                )
            )

        sectors.append(
            SectorIndex(
                sector_name=sector_name,
                spdr_etf=etf,
                sp500_index_symbol=index_symbol,
                code=sector_def["code"],
                constituent_count=sector_total,
                industry_groups=groups,
            )
        )

    return SectorIndexMapping(
        market=market,
        source_url=source_url,
        fetched_at=datetime.now(UTC).isoformat(timespec="seconds"),
        sectors=sectors,
    )


def parse_us_sector_indices(
    fetcher: Any = None,
    taxonomy_path: Path | None = None,
) -> SectorIndexMapping:
    """Fetch + parse the US GICS sector → index mapping.

    Args:
        fetcher: Optional ``(url) -> str`` callable. Defaults to an
            ``urllib`` fetch of the S&P 500 constituents Wikipedia page.
        taxonomy_path: Optional override for the curated GICS JSON path.
    """
    fetch = fetcher or _default_fetcher
    taxonomy = load_taxonomy(taxonomy_path)
    logger.info("fetching sector source | url=%s", SP500_WIKI_URL)
    html = fetch(SP500_WIKI_URL)
    constituents = _extract_constituents(html)
    return _build_mapping(constituents, taxonomy, source_url=SP500_WIKI_URL, market="US")


def write_mapping(mapping: SectorIndexMapping, path: Path) -> None:
    """Write the mapping to ``path`` as pretty-printed JSON (atomic rename)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(mapping.to_dict(), indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)
    logger.info("sector indices written | path=%s sectors=%d", path, len(mapping.sectors))
