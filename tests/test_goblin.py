from unittest.mock import patch

import polars as pl
import pytest

from marketgoblin import Dataset, MarketGoblin


def make_lf(symbol: str) -> pl.LazyFrame:
    return pl.DataFrame(
        {
            "date": pl.Series([20240102, 20240103], dtype=pl.Int32),
            "open": pl.Series([100.0, 101.0], dtype=pl.Float32),
            "high": pl.Series([102.0, 103.0], dtype=pl.Float32),
            "low": pl.Series([98.0, 99.0], dtype=pl.Float32),
            "close": pl.Series([101.0, 102.0], dtype=pl.Float32),
            "volume": pl.Series([1_000_000, 2_000_000], dtype=pl.Int64),
            "symbol": [symbol, symbol],
        }
    ).lazy()


@pytest.fixture
def goblin():
    return MarketGoblin(provider="yahoo")


def test_fetch_many_returns_all_symbols(goblin):
    with patch.object(goblin, "fetch", side_effect=lambda s, *a, **kw: make_lf(s)):
        results = goblin.fetch_many(["AAPL", "MSFT", "GOOGL"], "2024-01-01", "2024-01-31")

    assert set(results.keys()) == {"AAPL", "MSFT", "GOOGL"}


def test_fetch_many_isolates_failures(goblin):
    def side_effect(symbol, *args, **kwargs):
        if symbol == "BAD":
            raise ValueError("no data")
        return make_lf(symbol)

    with patch.object(goblin, "fetch", side_effect=side_effect):
        results = goblin.fetch_many(["AAPL", "BAD", "MSFT"], "2024-01-01", "2024-01-31")

    assert "AAPL" in results
    assert "MSFT" in results
    assert "BAD" not in results


def test_fetch_many_all_fail(goblin):
    with patch.object(goblin, "fetch", side_effect=ValueError("no data")):
        results = goblin.fetch_many(["BAD1", "BAD2"], "2024-01-01", "2024-01-31")

    assert results == {}


def test_fetch_many_returns_lazy_frames(goblin):
    with patch.object(goblin, "fetch", side_effect=lambda s, *a, **kw: make_lf(s)):
        results = goblin.fetch_many(["AAPL"], "2024-01-01", "2024-01-31")

    assert isinstance(results["AAPL"], pl.LazyFrame)


def test_fetch_many_empty_symbols(goblin):
    results = goblin.fetch_many([], "2024-01-01", "2024-01-31")
    assert results == {}


def test_unknown_provider_raises():
    with pytest.raises(ValueError, match="Unknown provider"):
        MarketGoblin(provider="bloomberg")


def test_supported_datasets_yahoo(goblin):
    assert Dataset.OHLCV in goblin.supported_datasets
    assert Dataset.SHARES in goblin.supported_datasets


def test_supported_datasets_csv(tmp_path):
    g = MarketGoblin(provider="csv", data_dir=tmp_path)
    assert Dataset.OHLCV in g.supported_datasets
    assert Dataset.SHARES not in g.supported_datasets


def test_fetch_rejects_adjusted_with_shares(goblin):
    with pytest.raises(ValueError, match="adjusted"):
        goblin.fetch("AAPL", "2024-01-01", "2024-01-31", dataset=Dataset.SHARES, adjusted=False)


def test_load_rejects_adjusted_with_shares(tmp_path):
    g = MarketGoblin(provider="yahoo", save_path=tmp_path)
    with pytest.raises(ValueError, match="adjusted"):
        g.load("AAPL", "2024-01-01", "2024-01-31", dataset=Dataset.SHARES, adjusted=False)


def test_fetch_many_rejects_adjusted_with_shares(goblin):
    with pytest.raises(ValueError, match="adjusted"):
        goblin.fetch_many(
            ["AAPL"], "2024-01-01", "2024-01-31", dataset=Dataset.SHARES, adjusted=False
        )


def test_fetch_rejects_unsupported_dataset_via_csv(tmp_path):
    g = MarketGoblin(provider="csv", data_dir=tmp_path)
    with pytest.raises(ValueError, match="does not support dataset"):
        g.fetch("AAPL", "2024-01-01", "2024-01-31", dataset=Dataset.SHARES)


def test_load_without_save_path_raises(goblin):
    with pytest.raises(RuntimeError, match="save_path"):
        goblin.load("AAPL", "2024-01-01", "2024-01-31")
