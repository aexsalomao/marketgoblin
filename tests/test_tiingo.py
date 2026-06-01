from datetime import date
from typing import Any
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import requests
from tiingo.restclient import RestClientError

from marketgoblin.classification import Classification
from marketgoblin.datasets import Dataset
from marketgoblin.sources import _tiingo_parsing
from marketgoblin.sources._tiingo_parsing import (
    build_adjusted_ohlcv_lf,
    build_raw_ohlcv_lf,
    build_tiingo_classification,
    build_tiingo_metadata,
    derive_shares_from_marketcap,
    fetch_fundamentals_meta,
    fetch_latest_close,
    fetch_latest_fundamentals,
    fundamentals_daily_rows_to_lf,
    prices_rows_to_base_lf,
    prices_rows_to_dividends,
    prices_rows_to_splits,
    prices_rows_to_stacked_ohlcv,
    slugify,
    stack_ohlcv,
    statements_rows_to_lf,
)
from marketgoblin.sources.tiingo import TiingoSource
from marketgoblin.ticker_metadata import TickerMetadata

# ---------- fixtures: Tiingo JSON shapes ----------


def _make_prices_rows() -> list[dict[str, Any]]:
    return [
        {
            "date": "2024-01-02T00:00:00.000Z",
            "open": 100.0,
            "high": 105.0,
            "low": 95.0,
            "close": 100.0,
            "volume": 80_000_000,
            "adjOpen": 97.0,
            "adjHigh": 101.85,
            "adjLow": 92.15,
            "adjClose": 97.0,
            "adjVolume": 80_000_000,
            "divCash": 0.0,
            "splitFactor": 1.0,
        },
        {
            "date": "2024-01-03T00:00:00.000Z",
            "open": 110.0,
            "high": 115.0,
            "low": 108.0,
            "close": 110.0,
            "volume": 75_000_000,
            "adjOpen": 106.7,
            "adjHigh": 111.55,
            "adjLow": 104.76,
            "adjClose": 106.7,
            "adjVolume": 75_000_000,
            "divCash": 0.24,
            "splitFactor": 1.0,
        },
    ]


def _make_prices_rows_with_split() -> list[dict[str, Any]]:
    """Two trading days, the second carrying a 4-for-1 split."""
    return [
        {
            "date": "2020-08-28T00:00:00.000Z",
            "open": 500.0,
            "high": 510.0,
            "low": 495.0,
            "close": 506.0,
            "volume": 50_000_000,
            "adjOpen": 125.0,
            "adjHigh": 127.5,
            "adjLow": 123.75,
            "adjClose": 126.5,
            "adjVolume": 200_000_000,
            "divCash": 0.0,
            "splitFactor": 1.0,
        },
        {
            "date": "2020-08-31T00:00:00.000Z",
            "open": 127.0,
            "high": 130.0,
            "low": 126.0,
            "close": 129.0,
            "volume": 200_000_000,
            "adjOpen": 127.0,
            "adjHigh": 130.0,
            "adjLow": 126.0,
            "adjClose": 129.0,
            "adjVolume": 200_000_000,
            "divCash": 0.0,
            "splitFactor": 4.0,
        },
    ]


def _make_statements_rows_as_reported() -> list[dict[str, Any]]:
    """Tiingo's asReported=True payload — point-in-time announced values."""
    return [
        {
            "date": "2024-08-01T00:00:00.000Z",
            "year": 2024,
            "quarter": 3,
            "statementData": {
                "incomeStatement": [
                    {"dataCode": "epsDil", "value": 1.40},
                    {"dataCode": "epsBasic", "value": 1.41},
                    {"dataCode": "revenue", "value": 85_777_000_000},
                ],
            },
        },
        {
            "date": "2024-05-02T00:00:00.000Z",
            "year": 2024,
            "quarter": 2,
            "statementData": {
                "incomeStatement": [
                    {"dataCode": "epsDil", "value": 1.53},
                    {"dataCode": "epsBasic", "value": 1.54},
                    {"dataCode": "revenue", "value": 90_753_000_000},
                ],
            },
        },
    ]


def _make_statements_rows_adjusted() -> list[dict[str, Any]]:
    """Tiingo's asReported=False payload — latest restated / adjusted values.
    Slightly different EPS to exercise the variant-merging join."""
    return [
        {
            "date": "2024-08-01T00:00:00.000Z",
            "year": 2024,
            "quarter": 3,
            "statementData": {
                "incomeStatement": [
                    {"dataCode": "epsDil", "value": 1.42},
                    {"dataCode": "epsBasic", "value": 1.43},
                    {"dataCode": "revenue", "value": 85_777_000_000},
                ],
            },
        },
        {
            "date": "2024-05-02T00:00:00.000Z",
            "year": 2024,
            "quarter": 2,
            "statementData": {
                "incomeStatement": [
                    {"dataCode": "epsDil", "value": 1.55},
                    {"dataCode": "epsBasic", "value": 1.56},
                    {"dataCode": "revenue", "value": 90_753_000_000},
                ],
            },
        },
    ]


def _make_fundamentals_rows() -> list[dict[str, Any]]:
    # Tiingo's get_fundamentals_daily returns valuation metrics only — no
    # shares field. marketCap is the anchor we use to derive shares.
    return [
        {
            "date": "2024-01-02T00:00:00.000Z",
            "marketCap": 1_500_000_000_000,
            "enterpriseVal": 1_550_000_000_000,
            "peRatio": 32.5,
            "pbRatio": 50.0,
            "trailingPEG1Y": 2.0,
        },
        {
            "date": "2024-01-03T00:00:00.000Z",
            "marketCap": 1_650_000_000_000,
            "enterpriseVal": 1_700_000_000_000,
            "peRatio": 32.6,
            "pbRatio": 50.1,
            "trailingPEG1Y": 2.0,
        },
    ]


def _make_metadata_dict() -> dict[str, Any]:
    return {
        "ticker": "AAPL",
        "name": "Apple Inc.",
        "exchangeCode": "NASDAQ",
        "startDate": "1980-12-12",
        "endDate": "2024-03-31",
        "description": "Apple makes consumer electronics.",
    }


def _make_meta_payload() -> list[dict[str, Any]]:
    return [
        {
            "ticker": "AAPL",
            "name": "Apple Inc.",
            "sector": "Information Technology",
            "industry": "Technology Hardware Storage & Peripherals",
            "sicCode": "3571",
            "sicSector": "Manufacturing",
            "sicIndustry": "Electronic Computers",
        }
    ]


# ---------- Tier 1: pure helpers ----------


def test_prices_rows_to_base_lf_parses_iso_date():
    lf = prices_rows_to_base_lf(_make_prices_rows(), "aapl")
    df = lf.collect()
    assert df["date"].dtype == pl.Date
    assert df["symbol"].to_list() == ["AAPL", "AAPL"]


def test_prices_rows_to_base_lf_raises_on_empty():
    with pytest.raises(ValueError, match="No OHLCV data"):
        prices_rows_to_base_lf([], "AAPL")


def test_build_raw_ohlcv_lf_uses_raw_columns():
    base = prices_rows_to_base_lf(_make_prices_rows(), "AAPL")
    df = build_raw_ohlcv_lf(base).collect()

    assert df["close"].to_list() == [100.0, 110.0]
    assert df["is_adjusted"].to_list() == [False, False]


def test_build_adjusted_ohlcv_lf_uses_adj_columns():
    base = prices_rows_to_base_lf(_make_prices_rows(), "AAPL")
    df = build_adjusted_ohlcv_lf(base).collect()

    assert df["close"].to_list() == [97.0, 106.7]
    assert df["open"].to_list() == [97.0, 106.7]
    assert df["is_adjusted"].to_list() == [True, True]


def test_stack_ohlcv_concats_and_sorts():
    base = prices_rows_to_base_lf(_make_prices_rows(), "AAPL")
    stacked = stack_ohlcv(build_adjusted_ohlcv_lf(base), build_raw_ohlcv_lf(base)).collect()

    assert stacked.height == 4
    # sorted by (date, is_adjusted) — within a date, raw (False) precedes adjusted (True)
    assert stacked["date"].to_list() == sorted(stacked["date"].to_list())
    jan_2 = stacked.filter(pl.col("date") == pl.date(2024, 1, 2))
    assert jan_2["is_adjusted"].to_list() == [False, True]


def test_prices_rows_to_stacked_ohlcv_full_pipeline():
    df = prices_rows_to_stacked_ohlcv(_make_prices_rows(), "AAPL").collect()
    assert df.height == 4
    expected = {"date", "open", "high", "low", "close", "volume", "symbol", "is_adjusted"}
    assert set(df.columns) == expected


def test_prices_rows_to_dividends_filters_zero_divcash():
    df = prices_rows_to_dividends(_make_prices_rows(), "AAPL").collect()
    # Only the second row has divCash > 0
    assert df.height == 1
    assert df["dividend"].to_list() == [0.24]


def test_prices_rows_to_splits_filters_unit_split_factor():
    # No split events in the standard fixture (both rows have splitFactor == 1.0)
    df = prices_rows_to_splits(_make_prices_rows(), "AAPL").collect()
    assert df.height == 0


def test_prices_rows_to_splits_extracts_split_event():
    df = prices_rows_to_splits(_make_prices_rows_with_split(), "AAPL").collect()
    assert df.height == 1
    assert df["date"].to_list() == [date(2020, 8, 31)]
    assert df["split_factor"].to_list() == [4.0]
    assert df["symbol"].to_list() == ["AAPL"]


def test_prices_rows_to_splits_raises_on_empty():
    with pytest.raises(ValueError, match="No OHLCV data"):
        prices_rows_to_splits([], "AAPL")


def test_fundamentals_daily_rows_to_lf_dtypes_and_renames():
    lf = fundamentals_daily_rows_to_lf(_make_fundamentals_rows(), "AAPL")
    df = lf.collect()
    assert df.schema["date"] == pl.Date
    assert set(df.columns) == {
        "date",
        "market_cap",
        "enterprise_val",
        "pe_ratio",
        "pb_ratio",
        "trailing_peg_1y",
        "symbol",
    }
    assert df["market_cap"].to_list() == [1_500_000_000_000, 1_650_000_000_000]
    assert df["symbol"].to_list() == ["AAPL", "AAPL"]


def test_fundamentals_daily_rows_to_lf_handles_missing_metric_fields():
    rows = [
        # Tiingo occasionally omits ratios for tickers without earnings.
        {"date": "2024-01-02T00:00:00.000Z", "marketCap": 1_500_000_000_000},
        {"date": "2024-01-03T00:00:00.000Z", "marketCap": 1_650_000_000_000, "peRatio": 32.6},
    ]
    df = fundamentals_daily_rows_to_lf(rows, "AAPL").collect()
    assert df.height == 2
    assert df["pe_ratio"].to_list() == [None, pytest.approx(32.6)]
    assert df["pb_ratio"].to_list() == [None, None]


def test_fundamentals_daily_rows_to_lf_raises_on_empty():
    with pytest.raises(ValueError, match="No fundamentals data"):
        fundamentals_daily_rows_to_lf([], "AAPL")


def test_statements_rows_to_lf_extracts_both_variants():
    df = statements_rows_to_lf(
        _make_statements_rows_as_reported(),
        _make_statements_rows_adjusted(),
        "AAPL",
    ).collect()
    assert df.height == 2
    assert df.schema["date"] == pl.Date
    assert sorted(df["fiscal_year"].to_list()) == [2024, 2024]
    assert sorted(df["fiscal_quarter"].to_list()) == [2, 3]
    by_quarter = {row["fiscal_quarter"]: row for row in df.iter_rows(named=True)}
    # As-reported and adjusted EPS coexist in the same row
    assert by_quarter[3]["eps_diluted_as_reported"] == pytest.approx(1.40)
    assert by_quarter[3]["eps_basic_as_reported"] == pytest.approx(1.41)
    assert by_quarter[3]["eps_diluted_adjusted"] == pytest.approx(1.42)
    assert by_quarter[3]["eps_basic_adjusted"] == pytest.approx(1.43)
    assert by_quarter[3]["revenue"] == 85_777_000_000.0
    assert by_quarter[3]["symbol"] == "AAPL"


def test_statements_rows_to_lf_handles_one_side_missing_quarter():
    # Adjusted side carries an extra quarter the as-reported call didn't have
    # (common: very old history is sometimes only in restated form).
    extra_adjusted = _make_statements_rows_adjusted() + [
        {
            "date": "2024-02-01T00:00:00.000Z",
            "year": 2024,
            "quarter": 1,
            "statementData": {
                "incomeStatement": [
                    {"dataCode": "epsDil", "value": 1.99},
                    {"dataCode": "epsBasic", "value": 2.00},
                ],
            },
        },
    ]
    df = statements_rows_to_lf(
        _make_statements_rows_as_reported(),
        extra_adjusted,
        "AAPL",
    ).collect()
    assert df.height == 3
    by_quarter = {row["fiscal_quarter"]: row for row in df.iter_rows(named=True)}
    # Q1 row has nulls on the as-reported side
    assert by_quarter[1]["eps_diluted_as_reported"] is None
    assert by_quarter[1]["eps_diluted_adjusted"] == pytest.approx(1.99)
    assert by_quarter[1]["date"] == date(2024, 2, 1)


def test_statements_rows_to_lf_handles_empty_adjusted_side():
    df = statements_rows_to_lf(
        _make_statements_rows_as_reported(),
        [],
        "AAPL",
    ).collect()
    assert df.height == 2
    assert df["eps_diluted_adjusted"].to_list() == [None, None]
    assert df["eps_diluted_as_reported"].to_list() == [pytest.approx(1.40), pytest.approx(1.53)]


def test_statements_rows_to_lf_handles_missing_income_codes():
    as_reported = [
        {
            "date": "2024-08-01T00:00:00.000Z",
            "year": 2024,
            "quarter": 3,
            "statementData": {
                "incomeStatement": [{"dataCode": "epsDil", "value": 1.40}],
            },
        },
    ]
    df = statements_rows_to_lf(as_reported, [], "AAPL").collect()
    assert df.height == 1
    assert df["eps_diluted_as_reported"].to_list() == [pytest.approx(1.40)]
    assert df["eps_basic_as_reported"].to_list() == [None]
    assert df["revenue"].to_list() == [None]


def test_statements_rows_to_lf_handles_missing_statement_data():
    rows = [{"date": "2024-08-01T00:00:00.000Z", "year": 2024, "quarter": 3}]
    df = statements_rows_to_lf(rows, [], "AAPL").collect()
    assert df.height == 1
    assert df["eps_diluted_as_reported"].to_list() == [None]


def test_statements_rows_to_lf_raises_on_both_empty():
    with pytest.raises(ValueError, match="No statements data"):
        statements_rows_to_lf([], [], "AAPL")


def test_statements_rows_to_lf_uppercases_symbol():
    df = statements_rows_to_lf(
        _make_statements_rows_as_reported(),
        _make_statements_rows_adjusted(),
        "aapl",
    ).collect()
    assert df["symbol"].unique().to_list() == ["AAPL"]


def test_derive_shares_from_marketcap_divides_marketcap_by_close():
    df = derive_shares_from_marketcap(
        _make_prices_rows(), _make_fundamentals_rows(), "AAPL"
    ).collect()
    # marketCap / close: 1.5e12 / 100 = 1.5e10 ; 1.65e12 / 110 = 1.5e10
    assert df["shares"].to_list() == [15_000_000_000, 15_000_000_000]
    assert df["symbol"].to_list() == ["AAPL", "AAPL"]


def test_derive_shares_from_marketcap_drops_dates_missing_from_either_side():
    prices = _make_prices_rows()
    fundamentals = [_make_fundamentals_rows()[0]]  # only 2024-01-02
    df = derive_shares_from_marketcap(prices, fundamentals, "AAPL").collect()
    assert df.height == 1


def test_derive_shares_from_marketcap_raises_on_empty_prices():
    with pytest.raises(ValueError, match="No price data"):
        derive_shares_from_marketcap([], _make_fundamentals_rows(), "AAPL")


def test_derive_shares_from_marketcap_raises_on_empty_fundamentals():
    with pytest.raises(ValueError, match="No fundamentals"):
        derive_shares_from_marketcap(_make_prices_rows(), [], "AAPL")


def test_derive_shares_from_marketcap_raises_on_no_overlap():
    fundamentals = [
        {"date": "2099-01-02T00:00:00.000Z", "marketCap": 1_000_000_000_000},
    ]
    with pytest.raises(ValueError, match="No overlapping"):
        derive_shares_from_marketcap(_make_prices_rows(), fundamentals, "AAPL")


def test_derive_shares_from_marketcap_skips_zero_marketcap_rows():
    fundamentals = [
        {"date": "2024-01-02T00:00:00.000Z", "marketCap": 0},  # halted/delisted
        {"date": "2024-01-03T00:00:00.000Z", "marketCap": 1_650_000_000_000},
    ]
    df = derive_shares_from_marketcap(_make_prices_rows(), fundamentals, "AAPL").collect()
    assert df.height == 1
    assert df["date"].to_list() == [date(2024, 1, 3)]


def test_derive_shares_from_marketcap_skips_null_marketcap_rows():
    fundamentals = [
        {"date": "2024-01-02T00:00:00.000Z", "marketCap": None},
        {"date": "2024-01-03T00:00:00.000Z", "marketCap": 1_650_000_000_000},
    ]
    df = derive_shares_from_marketcap(_make_prices_rows(), fundamentals, "AAPL").collect()
    assert df.height == 1
    assert df["date"].to_list() == [date(2024, 1, 3)]


def test_build_tiingo_metadata_full_merges_meta_and_fundamentals():
    meta = build_tiingo_metadata(
        symbol="AAPL",
        provider="tiingo",
        meta=_make_metadata_dict(),
        fundamentals_row=_make_fundamentals_rows()[-1],
        latest_close=110.0,
        is_fast=False,
    )
    assert meta.symbol == "AAPL"
    assert meta.name == "Apple Inc."
    assert meta.exchange == "NASDAQ"
    assert meta.business_summary == "Apple makes consumer electronics."
    assert meta.first_trade_date == "1980-12-12"
    assert meta.market_cap == 1_650_000_000_000
    # 1.65e12 / 110 = 1.5e10
    assert meta.shares_outstanding == 15_000_000_000
    assert meta.trailing_pe == pytest.approx(32.6)
    assert meta.currency == "USD"
    assert meta.provider == "tiingo"
    assert meta.is_fast is False


def test_build_tiingo_metadata_fast_skips_fundamentals_fields():
    meta = build_tiingo_metadata(
        symbol="AAPL",
        provider="tiingo",
        meta=_make_metadata_dict(),
        fundamentals_row=None,
        latest_close=None,
        is_fast=True,
    )
    assert meta.is_fast is True
    assert meta.name == "Apple Inc."
    assert meta.market_cap is None
    assert meta.shares_outstanding is None
    assert meta.trailing_pe is None


def test_build_tiingo_metadata_shares_outstanding_none_when_close_missing():
    meta = build_tiingo_metadata(
        symbol="AAPL",
        provider="tiingo",
        meta=_make_metadata_dict(),
        fundamentals_row=_make_fundamentals_rows()[-1],
        latest_close=None,
        is_fast=False,
    )
    assert meta.market_cap == 1_650_000_000_000
    assert meta.shares_outstanding is None


def test_build_tiingo_classification_populates_sector_and_industry():
    classification = build_tiingo_classification("AAPL", "tiingo", _make_meta_payload()[0])
    assert classification.symbol == "AAPL"
    assert classification.provider == "tiingo"
    assert classification.sector is not None
    assert classification.sector.key == "information-technology"
    assert classification.sector.name == "Information Technology"
    assert classification.industry is not None
    assert classification.industry.key == "technology-hardware-storage-peripherals"
    assert classification.industry.sector_key == "information-technology"
    assert classification.industry.sector_name == "Information Technology"


def test_build_tiingo_classification_handles_missing_keys():
    classification = build_tiingo_classification("BTC-USD", "tiingo", {})
    assert classification.symbol == "BTC-USD"
    assert classification.sector is None
    assert classification.industry is None


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("Information Technology", "information-technology"),
        ("Health Care", "health-care"),
        ("Energy", "energy"),
        ("Consumer Discretionary & Staples", "consumer-discretionary-staples"),
        ("  Trim Me  ", "trim-me"),
    ],
)
def test_slugify_examples(raw, expected):
    assert slugify(raw) == expected


def test_fetch_fundamentals_meta_returns_first_row():
    fake_response = type(
        "R",
        (),
        {"json": lambda self: _make_meta_payload(), "raise_for_status": lambda self: None},
    )()
    with patch.object(_tiingo_parsing.requests, "get", return_value=fake_response):
        row = fetch_fundamentals_meta("AAPL", api_key="k")
    assert row["sector"] == "Information Technology"


def test_fetch_fundamentals_meta_returns_empty_on_empty_payload():
    fake_response = type(
        "R",
        (),
        {"json": lambda self: [], "raise_for_status": lambda self: None},
    )()
    with patch.object(_tiingo_parsing.requests, "get", return_value=fake_response):
        row = fetch_fundamentals_meta("UNKNOWN", api_key="k")
    assert row == {}


# ---------- Tier 2: TiingoSource orchestration ----------


@pytest.fixture
def source() -> TiingoSource:
    # api_key keeps the TiingoClient constructor happy; we patch all I/O below.
    return TiingoSource(api_key="test-key")


def test_init_resolves_api_key_from_env_var(monkeypatch):
    # Without this fallback, fetch_classification would send "Token None" as
    # the bearer (the tiingo client reads TIINGO_API_KEY itself, but
    # self.api_key — used by the meta endpoint — would stay None).
    monkeypatch.setenv("TIINGO_API_KEY", "env-key")
    source = TiingoSource()
    assert source.api_key == "env-key"


def test_init_explicit_api_key_takes_precedence_over_env(monkeypatch):
    monkeypatch.setenv("TIINGO_API_KEY", "env-key")
    source = TiingoSource(api_key="explicit-key")
    assert source.api_key == "explicit-key"


def test_supported_datasets(source):
    assert source.supported_datasets == frozenset(
        {
            Dataset.OHLCV,
            Dataset.SHARES,
            Dataset.DIVIDENDS,
            Dataset.SPLITS,
            Dataset.FUNDAMENTALS_DAILY,
            Dataset.FUNDAMENTALS_STATEMENTS,
        }
    )


def test_fetch_ohlcv_returns_normalized_lazy_frame(source):
    with patch.object(source._client, "get_ticker_price", return_value=_make_prices_rows()):
        df = source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert df.schema["date"] == pl.Int32
    assert df.schema["close"] == pl.Float32
    assert df.schema["volume"] == pl.Int64
    assert df.schema["is_adjusted"] == pl.Boolean


def test_fetch_ohlcv_stacks_adjusted_and_raw(source):
    with patch.object(source._client, "get_ticker_price", return_value=_make_prices_rows()):
        df = source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert df.height == 4
    assert df.filter(pl.col("is_adjusted")).height == 2
    assert df.filter(~pl.col("is_adjusted")).height == 2


def test_fetch_ohlcv_lowercases_symbol_for_api(source):
    with patch.object(source._client, "get_ticker_price", return_value=_make_prices_rows()) as mock:
        source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert mock.call_args.args[0] == "aapl"


def test_fetch_dividends_lowercases_symbol_for_api(source):
    with patch.object(source._client, "get_ticker_price", return_value=_make_prices_rows()) as mock:
        source.fetch(Dataset.DIVIDENDS, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert mock.call_args.args[0] == "aapl"


def test_fetch_shares_lowercases_symbol_for_both_api_calls(source):
    with (
        patch.object(
            source._client, "get_ticker_price", return_value=_make_prices_rows()
        ) as price_mock,
        patch.object(
            source._client, "get_fundamentals_daily", return_value=_make_fundamentals_rows()
        ) as fundamentals_mock,
    ):
        source.fetch(Dataset.SHARES, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert price_mock.call_args.args[0] == "aapl"
    assert fundamentals_mock.call_args.args[0] == "aapl"


def test_fetch_metadata_lowercases_symbol_for_api(source):
    with (
        patch.object(
            source._client, "get_ticker_metadata", return_value=_make_metadata_dict()
        ) as meta_mock,
        patch(
            "marketgoblin.sources.tiingo.fetch_latest_fundamentals",
            return_value=_make_fundamentals_rows()[-1],
        ) as fundamentals_mock,
        patch("marketgoblin.sources.tiingo.fetch_latest_close", return_value=110.0) as close_mock,
    ):
        source.fetch_metadata("AAPL")
    # get_ticker_metadata is called inline by the orchestrator; the latest-*
    # helpers receive the raw symbol and lowercase it themselves at the API
    # boundary (covered by their own unit tests below).
    assert meta_mock.call_args.args[0] == "aapl"
    assert fundamentals_mock.call_args.args[1] == "AAPL"
    assert close_mock.call_args.args[1] == "AAPL"


def test_fetch_classification_lowercases_symbol_for_api(source):
    with patch.object(_tiingo_parsing.requests, "get") as request_mock:
        request_mock.return_value.json.return_value = _make_meta_payload()
        request_mock.return_value.raise_for_status.return_value = None
        source.fetch_classification("AAPL")
    assert request_mock.call_args.kwargs["params"]["tickers"] == "aapl"


def test_fetch_classification_sends_token_in_authorization_header(source):
    with patch.object(_tiingo_parsing.requests, "get") as request_mock:
        request_mock.return_value.json.return_value = _make_meta_payload()
        request_mock.return_value.raise_for_status.return_value = None
        source.fetch_classification("AAPL")
    headers = request_mock.call_args.kwargs["headers"]
    assert headers == {"Authorization": "Token test-key"}
    assert "token" not in request_mock.call_args.kwargs["params"]


def test_fetch_latest_close_lowercases_symbol_for_api():
    fake_client = MagicMock()
    fake_client.get_ticker_price.return_value = [{"close": 100.0}]
    fetch_latest_close(fake_client, "AAPL")
    assert fake_client.get_ticker_price.call_args.args[0] == "aapl"


def test_fetch_latest_fundamentals_lowercases_symbol_for_api():
    fake_client = MagicMock()
    fake_client.get_fundamentals_daily.return_value = [{"marketCap": 1}]
    fetch_latest_fundamentals(fake_client, "AAPL")
    assert fake_client.get_fundamentals_daily.call_args.args[0] == "aapl"


def test_fetch_ohlcv_uppercases_symbol_in_output(source):
    with patch.object(source._client, "get_ticker_price", return_value=_make_prices_rows()):
        df = source.fetch(Dataset.OHLCV, "aapl", "2024-01-01", "2024-01-31").collect()
    assert df["symbol"].unique().to_list() == ["AAPL"]


def test_fetch_ohlcv_empty_raises(source):
    with (
        patch.object(source._client, "get_ticker_price", return_value=[]),
        pytest.raises(ValueError, match="No OHLCV data"),
    ):
        source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-01-31")


def test_fetch_dividends_returns_only_div_events(source):
    with patch.object(source._client, "get_ticker_price", return_value=_make_prices_rows()):
        df = source.fetch(Dataset.DIVIDENDS, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert df.schema["dividend"] == pl.Float32
    assert df["date"].to_list() == [20240103]


def test_fetch_dividends_filters_date_range(source):
    with patch.object(source._client, "get_ticker_price", return_value=_make_prices_rows()):
        df = source.fetch(Dataset.DIVIDENDS, "AAPL", "2024-01-04", "2024-01-31").collect()
    # The single divCash row falls on 2024-01-03, outside [2024-01-04, 2024-01-31]
    assert df.height == 0


def test_fetch_splits_lowercases_symbol_for_api(source):
    with patch.object(
        source._client, "get_ticker_price", return_value=_make_prices_rows_with_split()
    ) as mock:
        source.fetch(Dataset.SPLITS, "AAPL", "2020-08-01", "2020-09-30").collect()
    assert mock.call_args.args[0] == "aapl"


def test_fetch_splits_returns_normalized_frame(source):
    with patch.object(
        source._client, "get_ticker_price", return_value=_make_prices_rows_with_split()
    ):
        df = source.fetch(Dataset.SPLITS, "AAPL", "2020-08-01", "2020-09-30").collect()
    assert df.schema["date"] == pl.Int32
    assert df.schema["split_factor"] == pl.Float32
    assert df["date"].to_list() == [20200831]
    assert df["split_factor"].to_list() == [pytest.approx(4.0)]


def test_fetch_splits_filters_date_range(source):
    with patch.object(
        source._client, "get_ticker_price", return_value=_make_prices_rows_with_split()
    ):
        df = source.fetch(Dataset.SPLITS, "AAPL", "2020-09-01", "2020-09-30").collect()
    # The split event falls on 2020-08-31, outside [2020-09-01, 2020-09-30]
    assert df.height == 0


def test_fetch_splits_returns_empty_when_no_splits_in_window(source):
    with patch.object(source._client, "get_ticker_price", return_value=_make_prices_rows()):
        df = source.fetch(Dataset.SPLITS, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert df.height == 0


def test_fetch_fundamentals_daily_lowercases_symbol_for_api(source):
    with patch.object(
        source._client, "get_fundamentals_daily", return_value=_make_fundamentals_rows()
    ) as mock:
        source.fetch(Dataset.FUNDAMENTALS_DAILY, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert mock.call_args.args[0] == "aapl"


def test_fetch_fundamentals_daily_returns_normalized_frame(source):
    with patch.object(
        source._client, "get_fundamentals_daily", return_value=_make_fundamentals_rows()
    ):
        df = source.fetch(Dataset.FUNDAMENTALS_DAILY, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert df.schema["date"] == pl.Int32
    assert df.schema["market_cap"] == pl.Int64
    assert df.schema["enterprise_val"] == pl.Int64
    assert df.schema["pe_ratio"] == pl.Float32
    assert df.schema["pb_ratio"] == pl.Float32
    assert df.schema["trailing_peg_1y"] == pl.Float32
    assert df["market_cap"].to_list() == [1_500_000_000_000, 1_650_000_000_000]


def test_fetch_fundamentals_daily_uppercases_symbol_in_output(source):
    with patch.object(
        source._client, "get_fundamentals_daily", return_value=_make_fundamentals_rows()
    ):
        df = source.fetch(Dataset.FUNDAMENTALS_DAILY, "aapl", "2024-01-01", "2024-01-31").collect()
    assert df["symbol"].unique().to_list() == ["AAPL"]


def test_fetch_fundamentals_daily_empty_raises(source):
    with (
        patch.object(source._client, "get_fundamentals_daily", return_value=[]),
        pytest.raises(ValueError, match="No fundamentals"),
    ):
        source.fetch(Dataset.FUNDAMENTALS_DAILY, "AAPL", "2024-01-01", "2024-01-31")


def _statements_side_effect(
    *args: Any,
    **kwargs: Any,
) -> list[dict[str, Any]]:
    """Mock side_effect: return as-reported or adjusted payload depending on
    the asReported kwarg the source sends."""
    if kwargs.get("asReported") is True:
        return _make_statements_rows_as_reported()
    return _make_statements_rows_adjusted()


def test_fetch_statements_lowercases_symbol_for_api(source):
    with patch.object(
        source._client,
        "get_fundamentals_statements",
        side_effect=_statements_side_effect,
    ) as mock:
        source.fetch(Dataset.FUNDAMENTALS_STATEMENTS, "AAPL", "2022-01-01", "2024-12-31").collect()
    # Both calls (asReported=True and asReported=False) lowercase the symbol
    assert all(call.args[0] == "aapl" for call in mock.call_args_list)


def test_fetch_statements_calls_both_as_reported_variants(source):
    with patch.object(
        source._client,
        "get_fundamentals_statements",
        side_effect=_statements_side_effect,
    ) as mock:
        source.fetch(Dataset.FUNDAMENTALS_STATEMENTS, "AAPL", "2022-01-01", "2024-12-31").collect()
    # The dataset always fetches both variants — one call with asReported=True
    # (point-in-time) and one with asReported=False (latest restated).
    as_reported_flags = [call.kwargs["asReported"] for call in mock.call_args_list]
    assert sorted(as_reported_flags) == [False, True]


def test_fetch_statements_returns_normalized_frame(source):
    with patch.object(
        source._client,
        "get_fundamentals_statements",
        side_effect=_statements_side_effect,
    ):
        df = source.fetch(
            Dataset.FUNDAMENTALS_STATEMENTS, "AAPL", "2022-01-01", "2024-12-31"
        ).collect()
    assert df.schema["date"] == pl.Int32
    assert df.schema["fiscal_year"] == pl.Int16
    assert df.schema["fiscal_quarter"] == pl.Int8
    assert df.schema["eps_diluted_as_reported"] == pl.Float32
    assert df.schema["eps_basic_as_reported"] == pl.Float32
    assert df.schema["eps_diluted_adjusted"] == pl.Float32
    assert df.schema["eps_basic_adjusted"] == pl.Float32
    assert df.schema["revenue"] == pl.Float64
    assert df.height == 2


def test_fetch_statements_empty_raises(source):
    with (
        patch.object(source._client, "get_fundamentals_statements", return_value=[]),
        pytest.raises(ValueError, match="No statements data"),
    ):
        source.fetch(Dataset.FUNDAMENTALS_STATEMENTS, "AAPL", "2022-01-01", "2024-12-31")


def test_fetch_shares_returns_normalized_frame(source):
    with (
        patch.object(source._client, "get_ticker_price", return_value=_make_prices_rows()),
        patch.object(
            source._client, "get_fundamentals_daily", return_value=_make_fundamentals_rows()
        ),
    ):
        df = source.fetch(Dataset.SHARES, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert df.schema["date"] == pl.Int32
    assert df.schema["shares"] == pl.Int64
    assert df["shares"].to_list() == [15_000_000_000, 15_000_000_000]


def test_fetch_shares_empty_fundamentals_raises(source):
    with (
        patch.object(source._client, "get_ticker_price", return_value=_make_prices_rows()),
        patch.object(source._client, "get_fundamentals_daily", return_value=[]),
        pytest.raises(ValueError, match="No fundamentals"),
    ):
        source.fetch(Dataset.SHARES, "AAPL", "2024-01-01", "2024-01-31")


def test_fetch_metadata_calls_metadata_fundamentals_and_close(source):
    with (
        patch.object(source._client, "get_ticker_metadata", return_value=_make_metadata_dict()),
        patch(
            "marketgoblin.sources.tiingo.fetch_latest_fundamentals",
            return_value=_make_fundamentals_rows()[-1],
        ),
        patch("marketgoblin.sources.tiingo.fetch_latest_close", return_value=110.0),
    ):
        meta = source.fetch_metadata("AAPL")
    assert isinstance(meta, TickerMetadata)
    assert meta.symbol == "AAPL"
    assert meta.name == "Apple Inc."
    assert meta.market_cap == 1_650_000_000_000
    assert meta.shares_outstanding == 15_000_000_000
    assert meta.is_fast is False


def test_fetch_metadata_fast_skips_fundamentals_and_close_calls(source):
    with (
        patch.object(source._client, "get_ticker_metadata", return_value=_make_metadata_dict()),
        patch("marketgoblin.sources.tiingo.fetch_latest_fundamentals") as fundamentals_mock,
        patch("marketgoblin.sources.tiingo.fetch_latest_close") as close_mock,
    ):
        meta = source.fetch_metadata("AAPL", fast=True)
    fundamentals_mock.assert_not_called()
    close_mock.assert_not_called()
    assert meta.is_fast is True
    assert meta.market_cap is None
    assert meta.shares_outstanding is None


def test_fetch_classification_uses_meta_endpoint(source):
    with patch(
        "marketgoblin.sources.tiingo.fetch_fundamentals_meta",
        return_value=_make_meta_payload()[0],
    ):
        classification = source.fetch_classification("AAPL")
    assert isinstance(classification, Classification)
    assert classification.sector is not None
    assert classification.sector.key == "information-technology"
    assert classification.industry is not None
    assert classification.industry.sector_key == "information-technology"


def test_fetch_classification_returns_empty_profiles_when_meta_absent(source):
    with patch("marketgoblin.sources.tiingo.fetch_fundamentals_meta", return_value={}):
        classification = source.fetch_classification("UNKNOWN")
    assert classification.sector is None
    assert classification.industry is None


def test_retry_on_transient_error(source):
    call_count = {"n": 0}

    def flaky(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] < 2:
            raise RestClientError("transient")
        return _make_prices_rows()

    with (
        patch.object(source._client, "get_ticker_price", side_effect=flaky),
        patch("marketgoblin.sources.tiingo.time.sleep"),
    ):
        df = source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert df.height == 4
    assert call_count["n"] == 2


def test_value_error_does_not_retry(source):
    call_count = {"n": 0}

    def empty(*args, **kwargs):
        call_count["n"] += 1
        return []

    with (
        patch.object(source._client, "get_ticker_price", side_effect=empty),
        pytest.raises(ValueError),
    ):
        source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-01-31")
    assert call_count["n"] == 1


def test_retry_on_transient_error_in_shares_path(source):
    # SHARES makes two API calls; transient failure on the second must still
    # be retried by the orchestrator-level _retry_fetch wrapper.
    fundamentals_calls = {"n": 0}

    def flaky_fundamentals(*args, **kwargs):
        fundamentals_calls["n"] += 1
        if fundamentals_calls["n"] < 2:
            raise RestClientError("transient")
        return _make_fundamentals_rows()

    with (
        patch.object(source._client, "get_ticker_price", return_value=_make_prices_rows()),
        patch.object(source._client, "get_fundamentals_daily", side_effect=flaky_fundamentals),
        patch("marketgoblin.sources.tiingo.time.sleep"),
    ):
        df = source.fetch(Dataset.SHARES, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert df.height == 2
    assert fundamentals_calls["n"] == 2


def test_retry_on_transient_error_in_classification_path(source):
    # fetch_classification uses requests.get directly, not the TiingoClient —
    # the retry wrapper has to handle that path too.
    call_count = {"n": 0}

    def flaky_meta(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] < 2:
            raise requests.HTTPError("transient")
        return _make_meta_payload()[0]

    with (
        patch("marketgoblin.sources.tiingo.fetch_fundamentals_meta", side_effect=flaky_meta),
        patch("marketgoblin.sources.tiingo.time.sleep"),
    ):
        classification = source.fetch_classification("AAPL")
    assert classification.sector is not None
    assert call_count["n"] == 2


def test_fetch_unsupported_dataset_raises(source):
    # Force an unsupported dataset by clearing the dispatch table — same shape
    # as the YahooSource test, ensures the BaseSource error path stays wired.
    source._dispatch = {Dataset.OHLCV: source._fetch_ohlcv}
    with pytest.raises(ValueError, match="does not support dataset"):
        source.fetch(Dataset.SHARES, "AAPL", "2024-01-01", "2024-01-31")


def test_http_error_propagates_after_retries(source):
    with (
        patch.object(
            source._client,
            "get_ticker_price",
            side_effect=requests.HTTPError("upstream down"),
        ),
        patch("marketgoblin.sources.tiingo.time.sleep"),
        pytest.raises(requests.HTTPError),
    ):
        source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-01-31")
