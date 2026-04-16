from datetime import date
from pathlib import Path

import polars as pl
import pytest

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
    lf = source.fetch("AAPL", "2024-01-01", "2024-12-31")
    assert isinstance(lf, pl.LazyFrame)


def test_fetch_row_count(source: CSVSource) -> None:
    df = source.fetch("AAPL", "2024-01-01", "2024-12-31").collect()
    assert len(df) == 3


def test_fetch_date_filter(source: CSVSource) -> None:
    df = source.fetch("AAPL", "2024-01-01", "2024-01-31").collect()
    assert len(df) == 2
    assert df["date"].to_list() == [20240102, 20240103]


def test_fetch_schema(source: CSVSource) -> None:
    df = source.fetch("AAPL", "2024-01-01", "2024-12-31").collect()
    assert df.schema["date"] == pl.Int32
    for col in ["open", "high", "low", "close"]:
        assert df.schema[col] == pl.Float32
    assert df.schema["volume"] == pl.Int64


def test_fetch_missing_file_raises(tmp_path: Path) -> None:
    source = CSVSource(data_dir=tmp_path)
    with pytest.raises(ValueError, match="No CSV file found"):
        source.fetch("MISSING", "2024-01-01", "2024-12-31")


def test_fetch_symbol_uppercased(source: CSVSource) -> None:
    df = source.fetch("aapl", "2024-01-01", "2024-12-31").collect()
    assert df["symbol"].to_list() == ["AAPL"] * 3


def test_fetch_via_market_goblin(tmp_path: Path) -> None:
    """Integration: CSVSource wired through MarketGoblin."""
    from marketgoblin import MarketGoblin

    csv_dir = tmp_path / "csvs"
    csv_dir.mkdir()
    write_csv(csv_dir, "AAPL")

    goblin = MarketGoblin(provider="csv", data_dir=csv_dir)
    lf = goblin.fetch("AAPL", "2024-01-01", "2024-12-31")
    assert lf.collect().height == 3
