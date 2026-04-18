from datetime import date

import polars as pl

from marketgoblin._normalize import normalize_ohlcv, normalize_shares, parse_dates


def make_raw_ohlcv() -> pl.LazyFrame:
    return pl.DataFrame(
        {
            "date": [date(2024, 1, 2), date(2024, 1, 3)],
            "open": pl.Series([185.0, 186.0], dtype=pl.Float64),
            "high": pl.Series([187.0, 188.0], dtype=pl.Float64),
            "low": pl.Series([183.0, 184.0], dtype=pl.Float64),
            "close": pl.Series([186.0, 187.0], dtype=pl.Float64),
            "volume": pl.Series([80_000_000.0, 75_000_000.0], dtype=pl.Float64),
            "symbol": ["AAPL", "AAPL"],
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


def test_normalize_ohlcv_numeric_dtypes():
    df = normalize_ohlcv(make_raw_ohlcv()).collect()
    assert df.schema["open"] == pl.Float32
    assert df.schema["high"] == pl.Float32
    assert df.schema["low"] == pl.Float32
    assert df.schema["close"] == pl.Float32
    assert df.schema["volume"] == pl.Int64


def test_normalize_ohlcv_date_is_int32():
    df = normalize_ohlcv(make_raw_ohlcv()).collect()
    assert df.schema["date"] == pl.Int32


def test_normalize_ohlcv_date_format():
    df = normalize_ohlcv(make_raw_ohlcv()).collect()
    assert df["date"][0] == 20240102
    assert df["date"][1] == 20240103


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


def test_parse_dates_returns_date_type():
    df = parse_dates(normalize_ohlcv(make_raw_ohlcv())).collect()
    assert df.schema["date"] == pl.Date


def test_parse_dates_values():
    df = parse_dates(normalize_ohlcv(make_raw_ohlcv())).collect()
    assert df["date"][0] == date(2024, 1, 2)
    assert df["date"][1] == date(2024, 1, 3)


def test_parse_dates_works_for_shares():
    df = parse_dates(normalize_shares(make_raw_shares())).collect()
    assert df.schema["date"] == pl.Date
    assert df["date"][0] == date(2024, 1, 2)
