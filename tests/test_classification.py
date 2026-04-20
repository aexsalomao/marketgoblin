from unittest.mock import MagicMock, PropertyMock, patch

import pandas as pd
import pytest

from marketgoblin import Classification, IndustryProfile, MarketGoblin, SectorProfile
from marketgoblin.sources.yahoo import YahooSource


class FakeFastInfo:
    def __init__(self, data: dict) -> None:
        self._data = data

    def keys(self):
        return self._data.keys()

    def __getitem__(self, key):
        return self._data[key]


AAPL_INFO_WITH_KEYS = {
    "sectorKey": "technology",
    "industryKey": "consumer-electronics",
}


def _ticker_with_info(info: dict):
    mock = MagicMock()
    type(mock).info = PropertyMock(return_value=info)
    # fast_info/history_metadata/isin aren't used by fetch_classification, but
    # keep them benign in case the implementation touches them.
    type(mock).fast_info = PropertyMock(return_value=FakeFastInfo({}))
    type(mock).history_metadata = PropertyMock(return_value={})
    type(mock).isin = PropertyMock(return_value=None)
    return mock


def _fake_sector():
    sector = MagicMock()
    sector.name = "Technology"
    sector.symbol = "XLK"
    sector.overview = {"market_cap": 20_000_000_000_000, "employee_count": 10_000_000}
    # yfinance returns top_companies as a DataFrame indexed by symbol.
    sector.top_companies = pd.DataFrame(
        {"name": ["Apple", "Microsoft", "Nvidia"]},
        index=["AAPL", "MSFT", "NVDA"],
    )
    sector.top_etfs = {"XLK": "Tech Select Sector SPDR", "VGT": "Vanguard Tech ETF"}
    sector.industries = pd.DataFrame(
        {"name": ["Consumer Electronics", "Software"]},
        index=["consumer-electronics", "software-infrastructure"],
    )
    return sector


def _fake_industry():
    industry = MagicMock()
    industry.name = "Consumer Electronics"
    industry.sector_key = "technology"
    industry.sector_name = "Technology"
    industry.symbol = None
    industry.top_companies = pd.DataFrame({"name": ["Apple", "Sony"]}, index=["AAPL", "SONY"])
    industry.top_performing_companies = pd.DataFrame({"name": ["Apple"]}, index=["AAPL"])
    industry.top_growth_companies = pd.DataFrame({"name": ["Sony"]}, index=["SONY"])
    return industry


@pytest.fixture
def source() -> YahooSource:
    return YahooSource()


# --- dataclass round-trips ---


def test_classification_round_trip_through_dict():
    original = Classification(
        symbol="AAPL",
        sector=SectorProfile(key="technology", name="Technology", etf_symbol="XLK"),
        industry=IndustryProfile(key="consumer-electronics", sector_key="technology"),
        provider="yahoo",
    )
    restored = Classification.from_dict(original.to_dict())
    assert restored == original


def test_classification_handles_missing_subprofiles():
    original = Classification(symbol="BTC-USD", sector=None, industry=None, provider="yahoo")
    restored = Classification.from_dict(original.to_dict())
    assert restored == original
    assert restored.sector is None
    assert restored.industry is None


def test_sector_profile_from_dict_ignores_unknown_keys():
    data = {"key": "technology", "name": "Tech", "stray": "ignore"}
    restored = SectorProfile.from_dict(data)
    assert restored.key == "technology"
    assert restored.name == "Tech"


# --- YahooSource.fetch_classification ---


def test_fetch_classification_merges_sector_and_industry(source):
    with (
        patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker,
        patch("marketgoblin.sources._yahoo_parsing.yf.Sector", return_value=_fake_sector()),
        patch("marketgoblin.sources._yahoo_parsing.yf.Industry", return_value=_fake_industry()),
    ):
        ticker.return_value = _ticker_with_info(AAPL_INFO_WITH_KEYS)
        classification = source.fetch_classification("aapl")

    assert classification.symbol == "AAPL"
    assert classification.provider == "yahoo"
    assert classification.sector is not None
    assert classification.sector.key == "technology"
    assert classification.sector.etf_symbol == "XLK"
    assert classification.sector.market_cap == 20_000_000_000_000
    assert classification.sector.top_companies == ["AAPL", "MSFT", "NVDA"]
    assert classification.sector.top_etfs == ["XLK", "VGT"]
    assert classification.sector.industries == [
        "consumer-electronics",
        "software-infrastructure",
    ]
    assert classification.industry is not None
    assert classification.industry.key == "consumer-electronics"
    assert classification.industry.sector_key == "technology"
    assert classification.industry.top_companies == ["AAPL", "SONY"]


def test_fetch_classification_returns_none_subprofiles_when_keys_missing(source):
    with (
        patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker,
        patch("marketgoblin.sources._yahoo_parsing.yf.Sector") as sector_cls,
        patch("marketgoblin.sources._yahoo_parsing.yf.Industry") as industry_cls,
    ):
        ticker.return_value = _ticker_with_info({})  # no sectorKey / industryKey
        classification = source.fetch_classification("BTC-USD")

    assert classification.sector is None
    assert classification.industry is None
    # Don't issue sector/industry lookups when keys are absent.
    sector_cls.assert_not_called()
    industry_cls.assert_not_called()


def test_fetch_classification_tolerates_sector_lookup_failure(source):
    with (
        patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker,
        patch(
            "marketgoblin.sources._yahoo_parsing.yf.Sector",
            side_effect=RuntimeError("upstream 500"),
        ),
        patch("marketgoblin.sources._yahoo_parsing.yf.Industry", return_value=_fake_industry()),
    ):
        ticker.return_value = _ticker_with_info(AAPL_INFO_WITH_KEYS)
        classification = source.fetch_classification("AAPL")

    # Sector lookup failed — but industry still resolves.
    assert classification.sector is None
    assert classification.industry is not None
    assert classification.industry.key == "consumer-electronics"


def test_fetch_classification_uppercases_symbol(source):
    with (
        patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker,
        patch("marketgoblin.sources._yahoo_parsing.yf.Sector", return_value=_fake_sector()),
        patch("marketgoblin.sources._yahoo_parsing.yf.Industry", return_value=_fake_industry()),
    ):
        ticker.return_value = _ticker_with_info(AAPL_INFO_WITH_KEYS)
        classification = source.fetch_classification("aapl")
    assert classification.symbol == "AAPL"


# --- CSVSource does not support classification ---


def test_csv_source_fetch_classification_raises():
    goblin = MarketGoblin(provider="csv")
    with pytest.raises(NotImplementedError):
        goblin.fetch_classification("AAPL")


# --- MarketGoblin.fetch_classification + load_classification ---


def test_goblin_fetch_classification_returns_dataclass():
    goblin = MarketGoblin(provider="yahoo")
    with (
        patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker,
        patch("marketgoblin.sources._yahoo_parsing.yf.Sector", return_value=_fake_sector()),
        patch("marketgoblin.sources._yahoo_parsing.yf.Industry", return_value=_fake_industry()),
    ):
        ticker.return_value = _ticker_with_info(AAPL_INFO_WITH_KEYS)
        result = goblin.fetch_classification("AAPL")
    assert isinstance(result, Classification)
    assert result.sector is not None
    assert result.sector.etf_symbol == "XLK"


def test_goblin_fetch_classification_saves_to_disk(tmp_path):
    goblin = MarketGoblin(provider="yahoo", save_path=tmp_path)
    with (
        patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker,
        patch("marketgoblin.sources._yahoo_parsing.yf.Sector", return_value=_fake_sector()),
        patch("marketgoblin.sources._yahoo_parsing.yf.Industry", return_value=_fake_industry()),
    ):
        ticker.return_value = _ticker_with_info(AAPL_INFO_WITH_KEYS)
        goblin.fetch_classification("AAPL")

    assert (tmp_path / "yahoo" / "classification" / "AAPL.json").exists()


def test_goblin_load_classification_round_trips(tmp_path):
    goblin = MarketGoblin(provider="yahoo", save_path=tmp_path)
    with (
        patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker,
        patch("marketgoblin.sources._yahoo_parsing.yf.Sector", return_value=_fake_sector()),
        patch("marketgoblin.sources._yahoo_parsing.yf.Industry", return_value=_fake_industry()),
    ):
        ticker.return_value = _ticker_with_info(AAPL_INFO_WITH_KEYS)
        original = goblin.fetch_classification("AAPL")

    restored = goblin.load_classification("AAPL")
    assert restored == original


def test_goblin_load_classification_requires_save_path():
    goblin = MarketGoblin(provider="yahoo")
    with pytest.raises(RuntimeError, match="save_path"):
        goblin.load_classification("AAPL")


def test_goblin_load_classification_missing_symbol_raises(tmp_path):
    goblin = MarketGoblin(provider="yahoo", save_path=tmp_path)
    with pytest.raises(FileNotFoundError):
        goblin.load_classification("NOPE")
