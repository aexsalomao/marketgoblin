from unittest.mock import MagicMock, patch

import pandas as pd
import polars as pl
import pytest

from marketgoblin.datasets import Dataset
from marketgoblin.sources.yahoo import YahooSource


def make_pandas_shares() -> pd.Series:
    # Simulate yfinance get_shares_full output: TZ-aware DatetimeIndex with
    # multiple entries on the same day to test deduplication.
    idx = pd.DatetimeIndex(
        [
            "2024-01-02 09:30",
            "2024-01-02 16:00",  # same day, later — should win
            "2024-01-15 09:30",
        ],
        tz="America/New_York",
    )
    return pd.Series([15_000_000_000, 14_950_000_000, 14_900_000_000], index=idx)


def make_pandas_ohlcv() -> pd.DataFrame:
    idx = pd.DatetimeIndex(["2024-01-02", "2024-01-03"])
    idx.name = "Date"
    return pd.DataFrame(
        {
            "Open": [185.0, 186.0],
            "High": [187.0, 188.0],
            "Low": [183.0, 184.0],
            "Close": [186.0, 187.0],
            "Volume": [80_000_000, 75_000_000],
        },
        index=idx,
    )


@pytest.fixture
def source() -> YahooSource:
    return YahooSource()


def test_supported_datasets(source):
    assert source.supported_datasets == frozenset({Dataset.OHLCV, Dataset.SHARES})


def test_fetch_shares_returns_lazy_frame(source):
    with patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker:
        ticker.return_value.get_shares_full.return_value = make_pandas_shares()
        lf = source.fetch(Dataset.SHARES, "AAPL", "2024-01-01", "2024-01-31")
    assert isinstance(lf, pl.LazyFrame)


def test_fetch_shares_schema(source):
    with patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker:
        ticker.return_value.get_shares_full.return_value = make_pandas_shares()
        df = source.fetch(Dataset.SHARES, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert df.schema["date"] == pl.Int32
    assert df.schema["shares"] == pl.Int64
    assert "symbol" in df.columns


def test_fetch_shares_dedupes_intraday_to_last_value(source):
    with patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker:
        ticker.return_value.get_shares_full.return_value = make_pandas_shares()
        df = source.fetch(Dataset.SHARES, "AAPL", "2024-01-01", "2024-01-31").collect()

    jan_2 = df.filter(pl.col("date") == 20240102)
    assert len(jan_2) == 1
    assert jan_2["shares"][0] == 14_950_000_000


def test_fetch_shares_uppercases_symbol(source):
    with patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker:
        ticker.return_value.get_shares_full.return_value = make_pandas_shares()
        df = source.fetch(Dataset.SHARES, "aapl", "2024-01-01", "2024-01-31").collect()
    assert df["symbol"][0] == "AAPL"


def test_fetch_shares_empty_raises(source):
    with patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker:
        ticker.return_value.get_shares_full.return_value = pd.Series(dtype=float)
        with pytest.raises(ValueError, match="No shares data"):
            source.fetch(Dataset.SHARES, "AAPL", "2024-01-01", "2024-01-31")


def test_fetch_shares_none_raises(source):
    with patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker:
        ticker.return_value.get_shares_full.return_value = None
        with pytest.raises(ValueError, match="No shares data"):
            source.fetch(Dataset.SHARES, "AAPL", "2024-01-01", "2024-01-31")


def test_fetch_ohlcv_returns_normalized_lazy_frame(source):
    with patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker:
        ticker.return_value.history.return_value = make_pandas_ohlcv()
        df = source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert df.schema["date"] == pl.Int32
    assert df.schema["close"] == pl.Float32
    assert df["date"].to_list() == [20240102, 20240103]


def test_fetch_unsupported_dataset_raises(source):
    # Force an unsupported dataset by clearing the dispatch table.
    source._dispatch = {Dataset.OHLCV: source._fetch_ohlcv}
    with pytest.raises(ValueError, match="does not support dataset"):
        source.fetch(Dataset.SHARES, "AAPL", "2024-01-01", "2024-01-31")


def test_retry_on_transient_error(source):
    call_count = {"n": 0}

    def flaky(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] < 2:
            raise ConnectionError("transient")
        mock = MagicMock()
        mock.history.return_value = make_pandas_ohlcv()
        return mock

    with (
        patch("marketgoblin.sources.yahoo.yf.Ticker", side_effect=flaky),
        patch("marketgoblin.sources.yahoo.time.sleep"),  # don't actually sleep
    ):
        df = source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert len(df) == 2
    assert call_count["n"] == 2


def test_value_error_does_not_retry(source):
    call_count = {"n": 0}

    def empty(*args, **kwargs):
        call_count["n"] += 1
        mock = MagicMock()
        mock.history.return_value = pd.DataFrame()
        return mock

    with (
        patch("marketgoblin.sources.yahoo.yf.Ticker", side_effect=empty),
        pytest.raises(ValueError),
    ):
        source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-01-31")
    assert call_count["n"] == 1
