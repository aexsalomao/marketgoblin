# Unit tests for the Tiingo fundamentals parsers — daily valuation frame,
# quarterly statements variant-merge, and the derived daily shares series.
# Pure functions over Tiingo payloads, no network or TiingoSource.

from datetime import date

import polars as pl
import pytest

from marketgoblin.sources._tiingo_parsing.fundamentals import (
    derive_shares_from_marketcap,
    fundamentals_daily_rows_to_lf,
    statements_rows_to_lf,
)
from tests._tiingo_data import (
    make_fundamentals_rows,
    make_prices_rows,
    make_statements_rows_adjusted,
    make_statements_rows_as_reported,
)


def test_fundamentals_daily_rows_to_lf_dtypes_and_renames():
    lf = fundamentals_daily_rows_to_lf(make_fundamentals_rows(), "AAPL")
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
        make_statements_rows_as_reported(),
        make_statements_rows_adjusted(),
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
    assert by_quarter[3]["revenue_as_reported"] == 85_777_000_000.0
    # Balance-sheet, cash-flow and overview codes flatten alongside income
    assert by_quarter[3]["total_assets_as_reported"] == 331_612_000_000.0
    assert by_quarter[3]["free_cash_flow_as_reported"] == 26_700_000_000.0
    assert by_quarter[3]["roe_as_reported"] == pytest.approx(0.32)
    assert by_quarter[3]["symbol"] == "AAPL"


def test_statements_rows_to_lf_handles_one_side_missing_quarter():
    # Adjusted side carries an extra quarter the as-reported call didn't have
    # (common: very old history is sometimes only in restated form).
    extra_adjusted = make_statements_rows_adjusted() + [
        {
            "date": "2024-02-01T00:00:00.000Z",
            "year": 2024,
            "quarter": 1,
            "statementData": {
                "incomeStatement": [
                    {"dataCode": "epsDil", "value": 1.99},
                    {"dataCode": "eps", "value": 2.00},
                ],
            },
        },
    ]
    df = statements_rows_to_lf(
        make_statements_rows_as_reported(),
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
        make_statements_rows_as_reported(),
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
    assert df["revenue_as_reported"].to_list() == [None]


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
        make_statements_rows_as_reported(),
        make_statements_rows_adjusted(),
        "aapl",
    ).collect()
    assert df["symbol"].unique().to_list() == ["AAPL"]


def test_derive_shares_from_marketcap_divides_marketcap_by_close():
    df = derive_shares_from_marketcap(
        make_prices_rows(), make_fundamentals_rows(), "AAPL"
    ).collect()
    # marketCap / close: 1.5e12 / 100 = 1.5e10 ; 1.65e12 / 110 = 1.5e10
    assert df["shares"].to_list() == [15_000_000_000, 15_000_000_000]
    assert df["symbol"].to_list() == ["AAPL", "AAPL"]


def test_derive_shares_from_marketcap_drops_dates_missing_from_either_side():
    prices = make_prices_rows()
    fundamentals = [make_fundamentals_rows()[0]]  # only 2024-01-02
    df = derive_shares_from_marketcap(prices, fundamentals, "AAPL").collect()
    assert df.height == 1


def test_derive_shares_from_marketcap_raises_on_empty_prices():
    with pytest.raises(ValueError, match="No price data"):
        derive_shares_from_marketcap([], make_fundamentals_rows(), "AAPL")


def test_derive_shares_from_marketcap_raises_on_empty_fundamentals():
    with pytest.raises(ValueError, match="No fundamentals"):
        derive_shares_from_marketcap(make_prices_rows(), [], "AAPL")


def test_derive_shares_from_marketcap_raises_on_no_overlap():
    fundamentals = [
        {"date": "2099-01-02T00:00:00.000Z", "marketCap": 1_000_000_000_000},
    ]
    with pytest.raises(ValueError, match="No overlapping"):
        derive_shares_from_marketcap(make_prices_rows(), fundamentals, "AAPL")


def test_derive_shares_from_marketcap_skips_zero_marketcap_rows():
    fundamentals = [
        {"date": "2024-01-02T00:00:00.000Z", "marketCap": 0},  # halted/delisted
        {"date": "2024-01-03T00:00:00.000Z", "marketCap": 1_650_000_000_000},
    ]
    df = derive_shares_from_marketcap(make_prices_rows(), fundamentals, "AAPL").collect()
    assert df.height == 1
    assert df["date"].to_list() == [date(2024, 1, 3)]


def test_derive_shares_from_marketcap_skips_null_marketcap_rows():
    fundamentals = [
        {"date": "2024-01-02T00:00:00.000Z", "marketCap": None},
        {"date": "2024-01-03T00:00:00.000Z", "marketCap": 1_650_000_000_000},
    ]
    df = derive_shares_from_marketcap(make_prices_rows(), fundamentals, "AAPL").collect()
    assert df.height == 1
    assert df["date"].to_list() == [date(2024, 1, 3)]
