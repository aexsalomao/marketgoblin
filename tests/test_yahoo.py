from unittest.mock import MagicMock, PropertyMock, patch

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
    # Matches yfinance's auto_adjust=False, actions=False shape.
    # Adj Close is ~97% of Close → adjusted OHLC should differ from raw.
    idx = pd.DatetimeIndex(["2024-01-02", "2024-01-03"])
    idx.name = "Date"
    return pd.DataFrame(
        {
            "Open": [100.0, 110.0],
            "High": [105.0, 115.0],
            "Low": [95.0, 108.0],
            "Close": [100.0, 110.0],
            "Adj Close": [97.0, 106.7],
            "Volume": [80_000_000, 75_000_000],
        },
        index=idx,
    )


def make_pandas_dividends() -> pd.Series:
    idx = pd.DatetimeIndex(["2023-11-10", "2024-02-09", "2024-05-10"], tz="America/New_York")
    idx.name = "Date"
    return pd.Series([0.24, 0.24, 0.25], index=idx, name="Dividends")


@pytest.fixture
def source() -> YahooSource:
    return YahooSource()


def test_supported_datasets(source):
    assert source.supported_datasets == frozenset(
        {Dataset.OHLCV, Dataset.SHARES, Dataset.DIVIDENDS}
    )


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
    assert df.schema["is_adjusted"] == pl.Boolean


def test_fetch_ohlcv_stacks_adjusted_and_raw(source):
    with patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker:
        ticker.return_value.history.return_value = make_pandas_ohlcv()
        df = source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-01-31").collect()
    # Two trading days × two variants
    assert len(df) == 4
    assert df.filter(pl.col("is_adjusted")).height == 2
    assert df.filter(~pl.col("is_adjusted")).height == 2


def test_fetch_ohlcv_uses_single_history_call(source):
    # The refactor derives adjusted OHLC from raw + Adj Close, so only one
    # history() call should fire per fetch.
    with patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker:
        ticker.return_value.history.return_value = make_pandas_ohlcv()
        source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert ticker.return_value.history.call_count == 1
    call_kwargs = ticker.return_value.history.call_args.kwargs
    assert call_kwargs.get("auto_adjust") is False


def test_fetch_ohlcv_adjusted_rows_match_adj_close(source):
    with patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker:
        ticker.return_value.history.return_value = make_pandas_ohlcv()
        df = source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-01-31").collect()

    adjusted_close = df.filter(pl.col("is_adjusted")).sort("date")["close"].to_list()
    raw_close = df.filter(~pl.col("is_adjusted")).sort("date")["close"].to_list()
    # Adjusted close should equal the Adj Close values from the mocked frame.
    assert adjusted_close == pytest.approx([97.0, 106.7], rel=1e-3)
    assert raw_close == pytest.approx([100.0, 110.0], rel=1e-3)


def test_fetch_ohlcv_adjusted_ohl_scaled_by_ratio(source):
    with patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker:
        ticker.return_value.history.return_value = make_pandas_ohlcv()
        df = source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-01-31").collect()

    jan_2_adjusted = df.filter((pl.col("date") == 20240102) & pl.col("is_adjusted")).row(
        0, named=True
    )
    # ratio = 97 / 100 = 0.97 → Open 100*0.97, High 105*0.97, Low 95*0.97
    assert jan_2_adjusted["open"] == pytest.approx(97.0, rel=1e-3)
    assert jan_2_adjusted["high"] == pytest.approx(101.85, rel=1e-3)
    assert jan_2_adjusted["low"] == pytest.approx(92.15, rel=1e-3)


def test_fetch_ohlcv_volume_identical_across_variants(source):
    with patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker:
        ticker.return_value.history.return_value = make_pandas_ohlcv()
        df = source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-01-31").collect()
    adjusted_vol = df.filter(pl.col("is_adjusted")).sort("date")["volume"].to_list()
    raw_vol = df.filter(~pl.col("is_adjusted")).sort("date")["volume"].to_list()
    assert adjusted_vol == raw_vol


def test_fetch_ohlcv_empty_raises(source):
    with patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker:
        ticker.return_value.history.return_value = pd.DataFrame()
        with pytest.raises(ValueError, match="No OHLCV data"):
            source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-01-31")


def test_fetch_dividends_returns_normalized_frame(source):
    with patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker:
        type(ticker.return_value).dividends = PropertyMock(return_value=make_pandas_dividends())
        df = source.fetch(Dataset.DIVIDENDS, "AAPL", "2024-01-01", "2024-12-31").collect()
    assert df.schema["date"] == pl.Int32
    assert df.schema["dividend"] == pl.Float32
    # Only the 2024 dividends fall in range
    assert df["date"].to_list() == [20240209, 20240510]


def test_fetch_dividends_filters_date_range(source):
    with patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker:
        type(ticker.return_value).dividends = PropertyMock(return_value=make_pandas_dividends())
        df = source.fetch(Dataset.DIVIDENDS, "AAPL", "2024-03-01", "2024-12-31").collect()
    assert df["date"].to_list() == [20240510]


def test_fetch_dividends_empty_raises(source):
    with patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker:
        type(ticker.return_value).dividends = PropertyMock(return_value=pd.Series(dtype=float))
        with pytest.raises(ValueError, match="No dividend data"):
            source.fetch(Dataset.DIVIDENDS, "AAPL", "2024-01-01", "2024-12-31")


def test_fetch_dividends_uppercases_symbol(source):
    with patch("marketgoblin.sources.yahoo.yf.Ticker") as ticker:
        type(ticker.return_value).dividends = PropertyMock(return_value=make_pandas_dividends())
        df = source.fetch(Dataset.DIVIDENDS, "aapl", "2024-01-01", "2024-12-31").collect()
    assert df["symbol"][0] == "AAPL"


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
    assert len(df) == 4
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
