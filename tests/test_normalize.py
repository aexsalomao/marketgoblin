from datetime import date

import polars as pl

from marketgoblin._normalize import normalize, parse_dates


def make_raw() -> pl.LazyFrame:
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


def test_normalize_numeric_dtypes():
    df = normalize(make_raw()).collect()
    assert df.schema["open"] == pl.Float32
    assert df.schema["high"] == pl.Float32
    assert df.schema["low"] == pl.Float32
    assert df.schema["close"] == pl.Float32
    assert df.schema["volume"] == pl.Int64


def test_normalize_date_is_int32():
    df = normalize(make_raw()).collect()
    assert df.schema["date"] == pl.Int32


def test_normalize_date_format():
    df = normalize(make_raw()).collect()
    assert df["date"][0] == 20240102
    assert df["date"][1] == 20240103


def test_parse_dates_returns_date_type():
    df = parse_dates(normalize(make_raw())).collect()
    assert df.schema["date"] == pl.Date


def test_parse_dates_values():
    df = parse_dates(normalize(make_raw())).collect()
    assert df["date"][0] == date(2024, 1, 2)
    assert df["date"][1] == date(2024, 1, 3)
