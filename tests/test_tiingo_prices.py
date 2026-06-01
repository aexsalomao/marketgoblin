# Unit tests for the Tiingo prices parsers (OHLCV / dividends / splits) —
# pure functions over Tiingo's prices payload, no network or TiingoSource.

from datetime import date

import polars as pl
import pytest

from marketgoblin.sources._tiingo_parsing.prices import (
    build_adjusted_ohlcv_lf,
    build_raw_ohlcv_lf,
    prices_rows_to_base_lf,
    prices_rows_to_dividends,
    prices_rows_to_splits,
    prices_rows_to_stacked_ohlcv,
    stack_ohlcv,
)
from tests._tiingo_data import make_prices_rows, make_prices_rows_with_split


def test_prices_rows_to_base_lf_parses_iso_date():
    lf = prices_rows_to_base_lf(make_prices_rows(), "aapl")
    df = lf.collect()
    assert df["date"].dtype == pl.Date
    assert df["symbol"].to_list() == ["AAPL", "AAPL"]


def test_prices_rows_to_base_lf_raises_on_empty():
    with pytest.raises(ValueError, match="No OHLCV data"):
        prices_rows_to_base_lf([], "AAPL")


def test_build_raw_ohlcv_lf_uses_raw_columns():
    base = prices_rows_to_base_lf(make_prices_rows(), "AAPL")
    df = build_raw_ohlcv_lf(base).collect()

    assert df["close"].to_list() == [100.0, 110.0]
    assert df["is_adjusted"].to_list() == [False, False]


def test_build_adjusted_ohlcv_lf_uses_adj_columns():
    base = prices_rows_to_base_lf(make_prices_rows(), "AAPL")
    df = build_adjusted_ohlcv_lf(base).collect()

    assert df["close"].to_list() == [97.0, 106.7]
    assert df["open"].to_list() == [97.0, 106.7]
    assert df["is_adjusted"].to_list() == [True, True]


def test_stack_ohlcv_concats_and_sorts():
    base = prices_rows_to_base_lf(make_prices_rows(), "AAPL")
    stacked = stack_ohlcv(build_adjusted_ohlcv_lf(base), build_raw_ohlcv_lf(base)).collect()

    assert stacked.height == 4
    # sorted by (date, is_adjusted) — within a date, raw (False) precedes adjusted (True)
    assert stacked["date"].to_list() == sorted(stacked["date"].to_list())
    jan_2 = stacked.filter(pl.col("date") == pl.date(2024, 1, 2))
    assert jan_2["is_adjusted"].to_list() == [False, True]


def test_prices_rows_to_stacked_ohlcv_full_pipeline():
    df = prices_rows_to_stacked_ohlcv(make_prices_rows(), "AAPL").collect()
    assert df.height == 4
    expected = {"date", "open", "high", "low", "close", "volume", "symbol", "is_adjusted"}
    assert set(df.columns) == expected


def test_prices_rows_to_dividends_filters_zero_divcash():
    df = prices_rows_to_dividends(make_prices_rows(), "AAPL").collect()
    # Only the second row has divCash > 0
    assert df.height == 1
    assert df["dividend"].to_list() == [0.24]


def test_prices_rows_to_splits_filters_unit_split_factor():
    # No split events in the standard fixture (both rows have splitFactor == 1.0)
    df = prices_rows_to_splits(make_prices_rows(), "AAPL").collect()
    assert df.height == 0


def test_prices_rows_to_splits_extracts_split_event():
    df = prices_rows_to_splits(make_prices_rows_with_split(), "AAPL").collect()
    assert df.height == 1
    assert df["date"].to_list() == [date(2020, 8, 31)]
    assert df["split_factor"].to_list() == [4.0]
    assert df["symbol"].to_list() == ["AAPL"]


def test_prices_rows_to_splits_raises_on_empty():
    with pytest.raises(ValueError, match="No OHLCV data"):
        prices_rows_to_splits([], "AAPL")
