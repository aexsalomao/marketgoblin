from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from marketgoblin import MarketGoblin, TickerMetadata
from marketgoblin.sources.yahoo import YahooSource


class FakeFastInfo:
    """Mimics yfinance's FastInfo: dict-like but not a dict."""

    def __init__(self, data: dict) -> None:
        self._data = data

    def keys(self):
        return self._data.keys()

    def __getitem__(self, key):
        return self._data[key]


AAPL_FAST_INFO = {
    "currency": "USD",
    "exchange": "NMS",
    "quote_type": "EQUITY",
    "market_cap": 3_000_000_000_000,
    "shares": 15_000_000_000,
    "timezone": "America/New_York",
}

AAPL_HISTORY_META = {
    "timezone": "America/New_York",
    "firstTradeDate": 345479400,  # 1980-12-12 14:30:00 UTC
    "exchangeTimezoneName": "America/New_York",
}

AAPL_INFO = {
    "longName": "Apple Inc.",
    "sector": "Technology",
    "sectorKey": "technology",
    "sectorDisp": "Technology",
    "industry": "Consumer Electronics",
    "industryKey": "consumer-electronics",
    "industryDisp": "Consumer Electronics",
    "country": "United States",
    "longBusinessSummary": "Apple designs, manufactures...",
    "beta": 1.25,
    "trailingPE": 32.5,
    "forwardPE": 28.1,
    "marketCap": 3_000_000_000_000,
    "sharesOutstanding": 15_000_000_000,
    "exchangeTimezoneName": "America/New_York",
}


def _patched_ticker(
    fast_info=None,
    history_meta=None,
    info=None,
    isin="US0378331005",
):
    mock = MagicMock()
    type(mock).fast_info = PropertyMock(
        return_value=FakeFastInfo(fast_info) if fast_info is not None else FakeFastInfo({})
    )
    type(mock).history_metadata = PropertyMock(return_value=history_meta or {})
    type(mock).info = PropertyMock(return_value=info or {})
    type(mock).isin = PropertyMock(return_value=isin)
    return mock


@pytest.fixture
def source() -> YahooSource:
    return YahooSource()


# --- dataclass round-trip ---


def test_ticker_metadata_round_trip_through_dict():
    original = TickerMetadata(symbol="AAPL", currency="USD", market_cap=1_000, is_fast=True)
    restored = TickerMetadata.from_dict(original.to_dict())
    assert restored == original


def test_from_dict_ignores_unknown_keys():
    data = {"symbol": "AAPL", "currency": "USD", "not_a_field": "ignore_me"}
    restored = TickerMetadata.from_dict(data)
    assert restored.symbol == "AAPL"
    assert restored.currency == "USD"


# --- YahooSource.fetch_metadata ---


def test_fetch_metadata_merges_all_sources(source):
    with patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker:
        ticker.return_value = _patched_ticker(AAPL_FAST_INFO, AAPL_HISTORY_META, AAPL_INFO)
        meta = source.fetch_metadata("aapl")

    assert meta.symbol == "AAPL"
    assert meta.name == "Apple Inc."
    assert meta.sector == "Technology"
    assert meta.sector_key == "technology"
    assert meta.sector_display == "Technology"
    assert meta.industry == "Consumer Electronics"
    assert meta.industry_key == "consumer-electronics"
    assert meta.industry_display == "Consumer Electronics"
    assert meta.currency == "USD"
    assert meta.exchange == "NMS"
    assert meta.market_cap == 3_000_000_000_000
    assert meta.beta == pytest.approx(1.25)
    assert meta.trailing_pe == pytest.approx(32.5)
    assert meta.isin == "US0378331005"
    assert meta.timezone == "America/New_York"
    assert meta.first_trade_date == "1980-12-12"
    assert meta.provider == "yahoo"
    assert meta.is_fast is False


def test_fetch_metadata_fast_skips_info_and_isin(source):
    info_mock = PropertyMock(return_value=AAPL_INFO)
    isin_mock = PropertyMock(return_value="US0378331005")
    mock_ticker = MagicMock()
    type(mock_ticker).fast_info = PropertyMock(return_value=FakeFastInfo(AAPL_FAST_INFO))
    type(mock_ticker).history_metadata = PropertyMock(return_value=AAPL_HISTORY_META)
    type(mock_ticker).info = info_mock
    type(mock_ticker).isin = isin_mock

    with patch("marketgoblin.sources.yahoo.yf.Ticker", return_value=mock_ticker):
        meta = source.fetch_metadata("AAPL", fast=True)

    assert meta.is_fast is True
    assert meta.currency == "USD"
    assert meta.market_cap == 3_000_000_000_000
    # Heavy-endpoint fields should be absent.
    assert meta.name is None
    assert meta.sector is None
    assert meta.isin is None
    info_mock.assert_not_called()
    isin_mock.assert_not_called()


def test_fetch_metadata_handles_missing_fields(source):
    with patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker:
        ticker.return_value = _patched_ticker({}, {}, {}, isin=None)
        meta = source.fetch_metadata("XYZ")

    assert meta.symbol == "XYZ"
    assert meta.currency is None
    assert meta.market_cap is None
    assert meta.first_trade_date is None


def test_fetch_metadata_tolerates_isin_failure(source):
    mock_ticker = MagicMock()
    type(mock_ticker).fast_info = PropertyMock(return_value=FakeFastInfo(AAPL_FAST_INFO))
    type(mock_ticker).history_metadata = PropertyMock(return_value=AAPL_HISTORY_META)
    type(mock_ticker).info = PropertyMock(return_value=AAPL_INFO)
    type(mock_ticker).isin = PropertyMock(side_effect=RuntimeError("upstream 404"))

    with patch("marketgoblin.sources.yahoo.yf.Ticker", return_value=mock_ticker):
        meta = source.fetch_metadata("AAPL")

    assert meta.isin is None
    assert meta.name == "Apple Inc."  # rest of the fetch still succeeds


def test_fetch_metadata_normalizes_symbol_upper(source):
    with patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker:
        ticker.return_value = _patched_ticker(AAPL_FAST_INFO, AAPL_HISTORY_META, AAPL_INFO)
        meta = source.fetch_metadata("aapl")
    assert meta.symbol == "AAPL"


def test_fetch_metadata_retries_on_transient_error(source):
    call_count = {"n": 0}

    def flaky(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] < 2:
            raise ConnectionError("transient")
        return _patched_ticker(AAPL_FAST_INFO, AAPL_HISTORY_META, AAPL_INFO)

    with (
        patch("marketgoblin.sources.yahoo.yf.Ticker", side_effect=flaky),
        patch("marketgoblin.sources.yahoo.time.sleep"),
    ):
        meta = source.fetch_metadata("AAPL")
    assert meta.symbol == "AAPL"
    assert call_count["n"] == 2


# --- CSVSource does not support metadata ---


def test_csv_source_fetch_metadata_raises():
    goblin = MarketGoblin(provider="csv")
    with pytest.raises(NotImplementedError):
        goblin.fetch_metadata("AAPL")


# --- MarketGoblin.fetch_metadata + load_metadata (disk round-trip) ---


def test_goblin_fetch_metadata_returns_dataclass():
    goblin = MarketGoblin(provider="yahoo")
    with patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker:
        ticker.return_value = _patched_ticker(AAPL_FAST_INFO, AAPL_HISTORY_META, AAPL_INFO)
        meta = goblin.fetch_metadata("AAPL")
    assert isinstance(meta, TickerMetadata)
    assert meta.symbol == "AAPL"


def test_goblin_fetch_metadata_saves_to_disk_when_save_path_set(tmp_path):
    goblin = MarketGoblin(provider="yahoo", save_path=tmp_path)
    with patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker:
        ticker.return_value = _patched_ticker(AAPL_FAST_INFO, AAPL_HISTORY_META, AAPL_INFO)
        goblin.fetch_metadata("AAPL")

    saved = tmp_path / "yahoo" / "metadata" / "AAPL.json"
    assert saved.exists()


def test_goblin_load_metadata_round_trips(tmp_path):
    goblin = MarketGoblin(provider="yahoo", save_path=tmp_path)
    with patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker:
        ticker.return_value = _patched_ticker(AAPL_FAST_INFO, AAPL_HISTORY_META, AAPL_INFO)
        original = goblin.fetch_metadata("AAPL")

    restored = goblin.load_metadata("AAPL")
    assert restored == original


def test_goblin_load_metadata_requires_save_path():
    goblin = MarketGoblin(provider="yahoo")
    with pytest.raises(RuntimeError, match="save_path"):
        goblin.load_metadata("AAPL")


def test_goblin_load_metadata_missing_symbol_raises(tmp_path):
    goblin = MarketGoblin(provider="yahoo", save_path=tmp_path)
    with pytest.raises(FileNotFoundError):
        goblin.load_metadata("NOPE")


def test_goblin_fetch_metadata_does_not_save_without_save_path():
    goblin = MarketGoblin(provider="yahoo")
    with patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker:
        ticker.return_value = _patched_ticker(AAPL_FAST_INFO, AAPL_HISTORY_META, AAPL_INFO)
        # Should not raise — storage is None, save path is simply not touched.
        meta = goblin.fetch_metadata("AAPL")
    assert meta.symbol == "AAPL"
