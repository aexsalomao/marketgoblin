from pathlib import Path

import polars as pl
import pytest

from marketgoblin.datasets import Dataset
from marketgoblin.sources.csv_source import CSVSource


def write_csv(tmp_path: Path, symbol: str) -> Path:
    path = tmp_path / f"{symbol}.csv"
    path.write_text(
        "date,open,high,low,close,volume,symbol\n"
        "2024-01-02,185.0,187.0,183.0,186.0,80000000.0,AAPL\n"
        "2024-01-03,186.0,188.0,184.0,187.0,75000000.0,AAPL\n"
        "2024-02-01,187.0,189.0,185.0,188.0,70000000.0,AAPL\n"
    )
    return path


@pytest.fixture
def source(tmp_path: Path) -> CSVSource:
    write_csv(tmp_path, "AAPL")
    return CSVSource(data_dir=tmp_path)


def test_fetch_returns_lazy_frame(source: CSVSource) -> None:
    lf = source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-12-31")
    assert isinstance(lf, pl.LazyFrame)


def test_fetch_row_count(source: CSVSource) -> None:
    df = source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-12-31").collect()
    assert len(df) == 3


def test_fetch_date_filter(source: CSVSource) -> None:
    df = source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert len(df) == 2
    assert df["date"].to_list() == [20240102, 20240103]


def test_fetch_schema(source: CSVSource) -> None:
    df = source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-12-31").collect()
    assert df.schema["date"] == pl.Int32
    assert df.schema["open"] == pl.Float32
    assert df.schema["high"] == pl.Float32
    assert df.schema["low"] == pl.Float32
    assert df.schema["close"] == pl.Float32
    assert df.schema["volume"] == pl.Int64
    assert df.schema["is_adjusted"] == pl.Boolean


def test_fetch_stamps_is_adjusted_default_true(source: CSVSource) -> None:
    df = source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-12-31").collect()
    assert df["is_adjusted"].to_list() == [True, True, True]


def test_fetch_stamps_is_adjusted_false_when_configured(tmp_path: Path) -> None:
    write_csv(tmp_path, "AAPL")
    source = CSVSource(data_dir=tmp_path, is_adjusted=False)
    df = source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-12-31").collect()
    assert df["is_adjusted"].to_list() == [False, False, False]


def test_fetch_missing_file_raises(tmp_path: Path) -> None:
    source = CSVSource(data_dir=tmp_path)
    with pytest.raises(ValueError, match="No CSV file found"):
        source.fetch(Dataset.OHLCV, "MISSING", "2024-01-01", "2024-12-31")


def test_fetch_symbol_uppercased(source: CSVSource) -> None:
    df = source.fetch(Dataset.OHLCV, "aapl", "2024-01-01", "2024-12-31").collect()
    assert df["symbol"].to_list() == ["AAPL"] * 3


def test_fetch_shares_unsupported(source: CSVSource) -> None:
    with pytest.raises(ValueError, match="does not support dataset"):
        source.fetch(Dataset.SHARES, "AAPL", "2024-01-01", "2024-12-31")


def test_fetch_via_market_goblin(tmp_path: Path) -> None:
    from marketgoblin import MarketGoblin

    csv_dir = tmp_path / "csvs"
    csv_dir.mkdir()
    write_csv(csv_dir, "AAPL")

    goblin = MarketGoblin(provider="csv", data_dir=csv_dir)
    lf = goblin.fetch("AAPL", "2024-01-01", "2024-12-31")
    assert lf.collect().height == 3
