import json
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from marketgoblin._sector_indices_parser import (
    _US_SECTOR_TICKERS,
    Industry,
    IndustryGroup,
    SectorIndex,
    SectorIndexMapping,
    SubIndustry,
    load_taxonomy,
    parse_us_sector_indices,
    write_mapping,
)
from marketgoblin.sector_indices import load_sector_indices, refresh_sector_indices

_SAMPLE_HTML = """
<html><body>
<table class="wikitable sortable">
<tr><th>Symbol</th><th>Security</th><th>GICS Sector</th><th>GICS Sub-Industry</th></tr>
<tr><td>AAPL</td><td>Apple</td>
    <td>Information Technology</td>
    <td>Technology Hardware, Storage & Peripherals</td></tr>
<tr><td>MSFT</td><td>Microsoft</td><td>Information Technology</td><td>Systems Software</td></tr>
<tr><td>ORCL</td><td>Oracle</td><td>Information Technology</td><td>Application Software</td></tr>
<tr><td>JPM</td><td>JPMorgan</td><td>Financials</td><td>Diversified Banks</td></tr>
<tr><td>XOM</td><td>Exxon</td><td>Energy</td><td>Integrated Oil & Gas</td></tr>
</table>
</body></html>
"""


@pytest.fixture
def sample_fetcher():
    return lambda _url: _SAMPLE_HTML


# ---------- Parser — happy path ----------


def test_parse_us_sector_indices_returns_all_eleven_sectors(sample_fetcher):
    mapping = parse_us_sector_indices(fetcher=sample_fetcher)

    assert len(mapping.sectors) == 11
    assert {s.sector_name for s in mapping.sectors} == set(_US_SECTOR_TICKERS)


def test_parse_us_sector_indices_populates_sector_level_tickers(sample_fetcher):
    mapping = parse_us_sector_indices(fetcher=sample_fetcher)

    by_name = {s.sector_name: s for s in mapping.sectors}
    assert by_name["Information Technology"].spdr_etf == "XLK"
    assert by_name["Information Technology"].sp500_index_symbol == "S5INFT"
    assert by_name["Information Technology"].code == "45"


def test_parse_us_sector_indices_builds_full_four_level_tree(sample_fetcher):
    mapping = parse_us_sector_indices(fetcher=sample_fetcher)

    sector = next(s for s in mapping.sectors if s.sector_name == "Information Technology")
    assert len(sector.industry_groups) > 0
    group = sector.industry_groups[0]
    assert group.code.startswith("45")
    assert len(group.industries) > 0
    industry = group.industries[0]
    assert industry.code.startswith("45")
    assert len(industry.sub_industries) > 0
    assert all(isinstance(s, SubIndustry) for s in industry.sub_industries)


def test_parse_us_sector_indices_rolls_counts_up_sub_to_industry(sample_fetcher):
    mapping = parse_us_sector_indices(fetcher=sample_fetcher)
    it = next(s for s in mapping.sectors if s.sector_name == "Information Technology")

    for group in it.industry_groups:
        for industry in group.industries:
            assert industry.constituent_count == sum(
                s.constituent_count for s in industry.sub_industries
            )


def test_parse_us_sector_indices_rolls_counts_up_industry_to_group(sample_fetcher):
    mapping = parse_us_sector_indices(fetcher=sample_fetcher)

    for sector in mapping.sectors:
        for group in sector.industry_groups:
            assert group.constituent_count == sum(
                i.constituent_count for i in group.industries
            )


def test_parse_us_sector_indices_rolls_counts_up_group_to_sector(sample_fetcher):
    mapping = parse_us_sector_indices(fetcher=sample_fetcher)

    for sector in mapping.sectors:
        assert sector.constituent_count == sum(
            g.constituent_count for g in sector.industry_groups
        )


def test_parse_us_sector_indices_attributes_scraped_count_to_correct_sub_industry(
    sample_fetcher,
):
    mapping = parse_us_sector_indices(fetcher=sample_fetcher)
    all_subs = [
        sub
        for sector in mapping.sectors
        for group in sector.industry_groups
        for industry in group.industries
        for sub in industry.sub_industries
    ]
    by_name = {s.name: s for s in all_subs}

    assert by_name["Application Software"].constituent_count == 1
    assert by_name["Systems Software"].constituent_count == 1
    assert by_name["Diversified Banks"].constituent_count == 1
    assert by_name["Integrated Oil & Gas"].constituent_count == 1
    # Everything else should be zero.
    non_zero = {s.name for s in all_subs if s.constituent_count > 0}
    assert len(non_zero) == 5  # matches the 5 HTML rows


# ---------- Parser — validation ----------


def test_parse_us_sector_indices_rejects_unknown_sub_industry():
    html = """
    <table class="wikitable">
    <tr><th>Symbol</th><th>GICS Sector</th><th>GICS Sub-Industry</th></tr>
    <tr><td>XYZ</td><td>Information Technology</td><td>Quantum Supremacy Stuff</td></tr>
    </table>
    """
    with pytest.raises(ValueError, match="Unknown GICS sub-industries"):
        parse_us_sector_indices(fetcher=lambda _url: html)


def test_parse_us_sector_indices_rejects_missing_sub_industry_column():
    html = """
    <table class="wikitable">
    <tr><th>Symbol</th><th>GICS Sector</th></tr>
    <tr><td>AAPL</td><td>Information Technology</td></tr>
    </table>
    """
    with pytest.raises(ValueError, match="No 'gics sub-industry' column"):
        parse_us_sector_indices(fetcher=lambda _url: html)


def test_parse_us_sector_indices_rejects_empty_html():
    with pytest.raises(ValueError, match="No wikitable rows"):
        parse_us_sector_indices(fetcher=lambda _url: "<html></html>")


def test_parse_us_sector_indices_skips_rows_with_blank_cells():
    html = """
    <table class="wikitable">
    <tr><th>Symbol</th><th>GICS Sector</th><th>GICS Sub-Industry</th></tr>
    <tr><td>AAA</td><td>Information Technology</td><td>Systems Software</td></tr>
    <tr><td>BBB</td><td></td><td>Application Software</td></tr>
    <tr><td>CCC</td><td>Information Technology</td><td></td></tr>
    </table>
    """
    mapping = parse_us_sector_indices(fetcher=lambda _url: html)
    it = next(s for s in mapping.sectors if s.sector_name == "Information Technology")

    assert it.constituent_count == 1


# ---------- Taxonomy ----------


def test_taxonomy_has_eleven_sectors():
    taxonomy = load_taxonomy()

    assert {s["name"] for s in taxonomy["sectors"]} == set(_US_SECTOR_TICKERS)


def test_taxonomy_sub_industry_names_are_unique():
    taxonomy = load_taxonomy()
    names = [
        sub["name"]
        for sector in taxonomy["sectors"]
        for group in sector["industry_groups"]
        for industry in group["industries"]
        for sub in industry["sub_industries"]
    ]

    assert len(names) == len(set(names))


def test_taxonomy_codes_reflect_hierarchy():
    taxonomy = load_taxonomy()

    for sector in taxonomy["sectors"]:
        for group in sector["industry_groups"]:
            assert group["code"].startswith(sector["code"])
            for industry in group["industries"]:
                assert industry["code"].startswith(group["code"])
                for sub in industry["sub_industries"]:
                    assert sub["code"].startswith(industry["code"])


# ---------- Serialization ----------


def test_write_mapping_roundtrips_via_json(tmp_path: Path, sample_fetcher):
    mapping = parse_us_sector_indices(fetcher=sample_fetcher)
    path = tmp_path / "us.json"

    write_mapping(mapping, path)
    loaded = SectorIndexMapping.from_dict(json.loads(path.read_text(encoding="utf-8")))

    assert loaded == mapping


def test_sector_index_from_dict_rebuilds_nested_tree():
    data = {
        "sector_name": "Information Technology",
        "spdr_etf": "XLK",
        "sp500_index_symbol": "S5INFT",
        "code": "45",
        "constituent_count": 3,
        "industry_groups": [
            {
                "code": "4510",
                "name": "Software & Services",
                "constituent_count": 3,
                "industries": [
                    {
                        "code": "451030",
                        "name": "Software",
                        "constituent_count": 3,
                        "sub_industries": [
                            {
                                "code": "45103010",
                                "name": "Application Software",
                                "constituent_count": 2,
                            },
                            {
                                "code": "45103020",
                                "name": "Systems Software",
                                "constituent_count": 1,
                            },
                        ],
                    }
                ],
            }
        ],
    }

    sector = SectorIndex.from_dict(data)

    assert isinstance(sector.industry_groups[0], IndustryGroup)
    assert isinstance(sector.industry_groups[0].industries[0], Industry)
    assert isinstance(
        sector.industry_groups[0].industries[0].sub_industries[0], SubIndustry
    )


# ---------- Public API ----------


def test_load_sector_indices_reads_shipped_snapshot():
    mapping = load_sector_indices("US")

    assert mapping.market == "US"
    assert len(mapping.sectors) == 11
    # Shipped snapshot has full taxonomy but zero counts — user refreshes to populate.
    all_subs = [
        sub
        for sector in mapping.sectors
        for group in sector.industry_groups
        for industry in group.industries
        for sub in industry.sub_industries
    ]
    assert len(all_subs) > 150
    assert all(s.constituent_count == 0 for s in all_subs)


def test_load_sector_indices_rejects_unknown_market():
    with pytest.raises(ValueError, match="Unknown market"):
        load_sector_indices("BR")


def test_refresh_sector_indices_writes_to_custom_path(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        "marketgoblin._sector_indices_parser._default_fetcher",
        lambda _url: _SAMPLE_HTML,
    )
    out = tmp_path / "us.json"

    mapping = refresh_sector_indices("US", output_path=out)

    assert out.exists()
    reloaded = SectorIndexMapping.from_dict(json.loads(out.read_text(encoding="utf-8")))
    assert reloaded == mapping


# ---------- Property-based tests ----------


def _leaf_sub_industry_names() -> list[str]:
    """All sub-industry names in the shipped taxonomy — for PBT strategies."""
    taxonomy = load_taxonomy()
    return [
        sub["name"]
        for sector in taxonomy["sectors"]
        for group in sector["industry_groups"]
        for industry in group["industries"]
        for sub in industry["sub_industries"]
    ]


_LEAF_NAMES = _leaf_sub_industry_names()


def _html_from_constituents(rows: list[str]) -> str:
    """Render a wikitable with a row per sub-industry name (sector derived from taxonomy)."""
    taxonomy = load_taxonomy()
    sub_to_sector: dict[str, str] = {}
    for sector in taxonomy["sectors"]:
        for group in sector["industry_groups"]:
            for industry in group["industries"]:
                for sub in industry["sub_industries"]:
                    sub_to_sector[sub["name"]] = sector["name"]

    body = "\n".join(
        f"<tr><td>SYM{i}</td><td>{sub_to_sector[name]}</td><td>{name}</td></tr>"
        for i, name in enumerate(rows)
    )
    return f"""
    <table class="wikitable">
    <tr><th>Symbol</th><th>GICS Sector</th><th>GICS Sub-Industry</th></tr>
    {body}
    </table>
    """


@given(st.lists(st.sampled_from(_LEAF_NAMES), min_size=1, max_size=40))
@settings(max_examples=50, deadline=None)
def test_property_counts_roll_up_correctly_at_every_level(sub_industry_names):
    mapping = parse_us_sector_indices(
        fetcher=lambda _url: _html_from_constituents(sub_industry_names)
    )

    total = 0
    for sector in mapping.sectors:
        total += sector.constituent_count
        assert sector.constituent_count == sum(
            g.constituent_count for g in sector.industry_groups
        )
        for group in sector.industry_groups:
            assert group.constituent_count == sum(
                i.constituent_count for i in group.industries
            )
            for industry in group.industries:
                assert industry.constituent_count == sum(
                    s.constituent_count for s in industry.sub_industries
                )

    assert total == len(sub_industry_names)


@given(st.lists(st.sampled_from(_LEAF_NAMES), min_size=0, max_size=20))
@settings(max_examples=30, deadline=None)
def test_property_mapping_roundtrips_through_json(sub_industry_names):
    mapping = parse_us_sector_indices(
        fetcher=lambda _url: _html_from_constituents(sub_industry_names)
        if sub_industry_names
        else _SAMPLE_HTML
    )

    reloaded = SectorIndexMapping.from_dict(json.loads(json.dumps(mapping.to_dict())))

    assert reloaded == mapping


@given(st.sampled_from(_LEAF_NAMES))
@settings(max_examples=30, deadline=None)
def test_property_each_sub_industry_rolls_up_to_exactly_one_sector(sub_name):
    mapping = parse_us_sector_indices(
        fetcher=lambda _url: _html_from_constituents([sub_name])
    )

    sectors_with_count = [s for s in mapping.sectors if s.constituent_count > 0]
    assert len(sectors_with_count) == 1
    assert sectors_with_count[0].constituent_count == 1
