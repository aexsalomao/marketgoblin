from datetime import date

import polars as pl
import pytest

from marketgoblin._normalize import (
    normalize_dividends,
    normalize_fundamentals_daily,
    normalize_ohlcv,
    normalize_shares,
    normalize_splits,
    normalize_statements,
    normalize_trades,
    parse_dates,
)


def make_raw_trades() -> pl.LazyFrame:
    # Timestamps deliberately out of order to exercise the sort.
    return pl.DataFrame(
        {
            "timestamp": pl.Series(
                [
                    "2026-05-01T13:30:01Z",
                    "2026-05-01T13:30:00Z",
                    "2026-05-04T14:00:00Z",
                ]
            ).str.to_datetime(time_unit="ns", time_zone="UTC"),
            "symbol": ["SPY", "SPY", "SPY"],
            "exchange": ["V", "V", "D"],
            "price": pl.Series([500.2, 500.1, 501.0], dtype=pl.Float64),
            "size": pl.Series([50, 100, 200], dtype=pl.Int64),
            "conditions": [["@"], ["@", "I"], ["@"]],
            "trade_id": pl.Series([2, 1, 3], dtype=pl.Int64),
            "tape": ["B", "B", "B"],
        }
    ).lazy()


def test_normalize_trades_casts_price_to_float32_and_date_to_int32():
    df = normalize_trades(make_raw_trades()).collect()

    assert df.schema["price"] == pl.Float32
    assert df.schema["date"] == pl.Int32


def test_normalize_trades_derives_date_from_timestamp():
    df = normalize_trades(make_raw_trades()).collect()

    assert df["date"].to_list() == [20260501, 20260501, 20260504]


def test_normalize_trades_sorts_by_timestamp():
    df = normalize_trades(make_raw_trades()).collect()

    timestamps = df["timestamp"].to_list()
    assert timestamps == sorted(timestamps)


def test_normalize_trades_emits_canonical_column_order():
    df = normalize_trades(make_raw_trades()).collect()

    assert df.columns == [
        "date",
        "timestamp",
        "symbol",
        "exchange",
        "price",
        "size",
        "conditions",
        "trade_id",
        "tape",
    ]


@pytest.mark.parametrize(
    ("utc_timestamp", "expected_date"),
    [
        ("2026-01-15T14:30:00Z", 20260115),  # 09:30 ET — regular session
        ("2026-01-16T00:30:00Z", 20260115),  # 19:30 ET prior day (EST after-hours)
        ("2026-02-01T00:30:00Z", 20260131),  # last-of-month EST after-hours -> Jan 31
    ],
    ids=["regular_session", "after_hours_crosses_utc_midnight", "after_hours_month_boundary"],
)
def test_normalize_trades_date_uses_eastern_session(utc_timestamp, expected_date):
    lf = pl.DataFrame(
        {
            "timestamp": pl.Series([utc_timestamp]).str.to_datetime(
                time_unit="ns", time_zone="UTC"
            ),
            "symbol": ["SPY"],
            "exchange": ["V"],
            "price": pl.Series([500.0], dtype=pl.Float64),
            "size": pl.Series([100], dtype=pl.Int64),
            "conditions": [["@"]],
            "trade_id": pl.Series([1], dtype=pl.Int64),
            "tape": ["B"],
        }
    ).lazy()

    df = normalize_trades(lf).collect()

    assert df["date"].to_list() == [expected_date]


def make_raw_ohlcv() -> pl.LazyFrame:
    return pl.DataFrame(
        {
            "date": [date(2024, 1, 2), date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 3)],
            "open": pl.Series([185.0, 184.5, 186.0, 185.5], dtype=pl.Float64),
            "high": pl.Series([187.0, 186.5, 188.0, 187.5], dtype=pl.Float64),
            "low": pl.Series([183.0, 182.5, 184.0, 183.5], dtype=pl.Float64),
            "close": pl.Series([186.0, 185.5, 187.0, 186.5], dtype=pl.Float64),
            "volume": pl.Series(
                [80_000_000.0, 80_000_000.0, 75_000_000.0, 75_000_000.0], dtype=pl.Float64
            ),
            "symbol": ["AAPL"] * 4,
            "is_adjusted": [True, False, True, False],
        }
    ).lazy()


def make_raw_shares() -> pl.LazyFrame:
    return pl.DataFrame(
        {
            "date": [date(2024, 1, 2), date(2024, 1, 15)],
            "shares": pl.Series([15_000_000_000, 14_900_000_000], dtype=pl.Int64),
            "symbol": ["AAPL", "AAPL"],
        }
    ).lazy()


def make_raw_dividends() -> pl.LazyFrame:
    return pl.DataFrame(
        {
            "date": [date(2024, 2, 9), date(2024, 5, 10)],
            "dividend": pl.Series([0.24, 0.25], dtype=pl.Float64),
            "symbol": ["AAPL", "AAPL"],
        }
    ).lazy()


def test_normalize_ohlcv_numeric_dtypes():
    df = normalize_ohlcv(make_raw_ohlcv()).collect()
    assert df.schema["open"] == pl.Float32
    assert df.schema["high"] == pl.Float32
    assert df.schema["low"] == pl.Float32
    assert df.schema["close"] == pl.Float32
    assert df.schema["volume"] == pl.Int64


def test_normalize_ohlcv_is_adjusted_is_bool():
    df = normalize_ohlcv(make_raw_ohlcv()).collect()
    assert df.schema["is_adjusted"] == pl.Boolean
    assert df["is_adjusted"].to_list() == [True, False, True, False]


def test_normalize_ohlcv_date_is_int32():
    df = normalize_ohlcv(make_raw_ohlcv()).collect()
    assert df.schema["date"] == pl.Int32


def test_normalize_ohlcv_date_format():
    df = normalize_ohlcv(make_raw_ohlcv()).collect()
    assert df["date"][0] == 20240102
    assert df["date"][2] == 20240103


def test_normalize_shares_dtypes():
    df = normalize_shares(make_raw_shares()).collect()
    assert df.schema["shares"] == pl.Int64
    assert df.schema["date"] == pl.Int32


def test_normalize_shares_date_format():
    df = normalize_shares(make_raw_shares()).collect()
    assert df["date"].to_list() == [20240102, 20240115]


def test_normalize_shares_preserves_large_counts():
    df = normalize_shares(make_raw_shares()).collect()
    assert df["shares"][0] == 15_000_000_000


def make_raw_splits() -> pl.LazyFrame:
    return pl.DataFrame(
        {
            "date": [date(2020, 8, 31), date(2014, 6, 9)],
            "split_factor": pl.Series([4.0, 7.0], dtype=pl.Float64),
            "symbol": ["AAPL", "AAPL"],
        }
    ).lazy()


def test_normalize_splits_dtypes():
    df = normalize_splits(make_raw_splits()).collect()
    assert df.schema["split_factor"] == pl.Float32
    assert df.schema["date"] == pl.Int32


def test_normalize_splits_date_format():
    df = normalize_splits(make_raw_splits()).collect()
    assert df["date"].to_list() == [20200831, 20140609]


def test_parse_dates_works_for_splits():
    df = parse_dates(normalize_splits(make_raw_splits())).collect()
    assert df.schema["date"] == pl.Date
    assert df["date"][0] == date(2020, 8, 31)


def test_normalize_dividends_dtypes():
    df = normalize_dividends(make_raw_dividends()).collect()
    assert df.schema["dividend"] == pl.Float32
    assert df.schema["date"] == pl.Int32


def test_normalize_dividends_date_format():
    df = normalize_dividends(make_raw_dividends()).collect()
    assert df["date"].to_list() == [20240209, 20240510]


def make_raw_fundamentals_daily() -> pl.LazyFrame:
    return pl.DataFrame(
        {
            "date": [date(2024, 1, 2), date(2024, 1, 3)],
            "market_cap": pl.Series([1_500_000_000_000, 1_650_000_000_000], dtype=pl.Int64),
            "enterprise_val": pl.Series([1_550_000_000_000, 1_700_000_000_000], dtype=pl.Int64),
            "pe_ratio": pl.Series([32.5, 32.6], dtype=pl.Float64),
            "pb_ratio": pl.Series([50.0, 50.1], dtype=pl.Float64),
            "trailing_peg_1y": pl.Series([2.0, 2.0], dtype=pl.Float64),
            "symbol": ["AAPL", "AAPL"],
        }
    ).lazy()


def test_normalize_fundamentals_daily_dtypes():
    df = normalize_fundamentals_daily(make_raw_fundamentals_daily()).collect()
    assert df.schema["market_cap"] == pl.Int64
    assert df.schema["enterprise_val"] == pl.Int64
    assert df.schema["pe_ratio"] == pl.Float32
    assert df.schema["pb_ratio"] == pl.Float32
    assert df.schema["trailing_peg_1y"] == pl.Float32
    assert df.schema["date"] == pl.Int32


def test_normalize_fundamentals_daily_date_format():
    df = normalize_fundamentals_daily(make_raw_fundamentals_daily()).collect()
    assert df["date"].to_list() == [20240102, 20240103]


def test_normalize_fundamentals_daily_preserves_large_market_cap():
    df = normalize_fundamentals_daily(make_raw_fundamentals_daily()).collect()
    assert df["market_cap"][1] == 1_650_000_000_000


def test_parse_dates_works_for_fundamentals_daily():
    df = parse_dates(normalize_fundamentals_daily(make_raw_fundamentals_daily())).collect()
    assert df.schema["date"] == pl.Date
    assert df["date"][0] == date(2024, 1, 2)


@pytest.fixture
def raw_statements(make_statements_frame) -> pl.LazyFrame:
    # Pre-normalize wire shape: pl.Date / Int64 periods / all-Float64 fields,
    # with the two headline anchors normalize tests assert on.
    return make_statements_frame(
        dates=[date(2024, 8, 1), date(2024, 5, 2)],
        fiscal_years=[2024, 2024],
        fiscal_quarters=[3, 2],
        anchors={
            "eps_diluted_as_reported": [1.40, 1.53],
            "revenue_as_reported": [85_777_000_000.0, 90_753_000_000.0],
        },
        on_disk=False,
    )


def test_normalize_statements_dtypes(raw_statements):
    df = normalize_statements(raw_statements).collect()
    assert df.schema["date"] == pl.Int32
    assert df.schema["fiscal_year"] == pl.Int16
    assert df.schema["fiscal_quarter"] == pl.Int8
    assert df.schema["eps_diluted_as_reported"] == pl.Float32
    assert df.schema["eps_basic_adjusted"] == pl.Float32
    assert df.schema["revenue_as_reported"] == pl.Float64
    assert df.schema["total_assets_adjusted"] == pl.Float64
    assert df.schema["roe_as_reported"] == pl.Float32


def test_normalize_statements_date_format(raw_statements):
    df = normalize_statements(raw_statements).collect()
    assert df["date"].to_list() == [20240801, 20240502]


def test_normalize_statements_preserves_large_revenue(raw_statements):
    df = normalize_statements(raw_statements).collect()
    assert df["revenue_as_reported"][0] == 85_777_000_000.0


def test_parse_dates_works_for_statements(raw_statements):
    df = parse_dates(normalize_statements(raw_statements)).collect()
    assert df.schema["date"] == pl.Date
    assert df["date"][0] == date(2024, 8, 1)


def test_parse_dates_returns_date_type():
    df = parse_dates(normalize_ohlcv(make_raw_ohlcv())).collect()
    assert df.schema["date"] == pl.Date


def test_parse_dates_values():
    df = parse_dates(normalize_ohlcv(make_raw_ohlcv())).collect()
    assert df["date"][0] == date(2024, 1, 2)


def test_parse_dates_works_for_shares():
    df = parse_dates(normalize_shares(make_raw_shares())).collect()
    assert df.schema["date"] == pl.Date
    assert df["date"][0] == date(2024, 1, 2)


def test_parse_dates_works_for_dividends():
    df = parse_dates(normalize_dividends(make_raw_dividends())).collect()
    assert df.schema["date"] == pl.Date
    assert df["date"][0] == date(2024, 2, 9)
