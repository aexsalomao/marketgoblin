from datetime import date

import polars as pl
import pytest

from marketgoblin.storage.disk import DiskStorage


def make_lf() -> pl.LazyFrame:
    # Two rows in Jan, two in Feb
    return pl.DataFrame({
        "date":   pl.Series([20240102, 20240103, 20240201, 20240202], dtype=pl.Int32),
        "open":   pl.Series([185.0, 186.0, 187.0, 188.0], dtype=pl.Float32),
        "high":   pl.Series([187.0, 188.0, 189.0, 190.0], dtype=pl.Float32),
        "low":    pl.Series([183.0, 184.0, 185.0, 186.0], dtype=pl.Float32),
        "close":  pl.Series([186.0, 187.0, 188.0, 189.0], dtype=pl.Float32),
        "volume": pl.Series([80e6, 75e6, 70e6, 65e6], dtype=pl.Float32),
        "symbol": ["AAPL"] * 4,
    }).lazy()


@pytest.fixture
def storage(tmp_path) -> DiskStorage:
    return DiskStorage(tmp_path)


def test_save_creates_pq_files(storage, tmp_path):
    storage.save("yahoo", "AAPL", make_lf())
    assert (tmp_path / "yahoo" / "ohlcv" / "adjusted" / "AAPL" / "AAPL_2024-01.pq").exists()
    assert (tmp_path / "yahoo" / "ohlcv" / "adjusted" / "AAPL" / "AAPL_2024-02.pq").exists()


def test_save_creates_json_sidecars(storage, tmp_path):
    storage.save("yahoo", "AAPL", make_lf())
    assert (tmp_path / "yahoo" / "ohlcv" / "adjusted" / "AAPL" / "AAPL_2024-01.json").exists()
    assert (tmp_path / "yahoo" / "ohlcv" / "adjusted" / "AAPL" / "AAPL_2024-02.json").exists()


def test_save_raw_goes_to_raw_dir(storage, tmp_path):
    storage.save("yahoo", "AAPL", make_lf(), adjusted=False)
    assert (tmp_path / "yahoo" / "ohlcv" / "raw" / "AAPL" / "AAPL_2024-01.pq").exists()


def test_save_no_tmp_files_left(storage, tmp_path):
    storage.save("yahoo", "AAPL", make_lf())
    assert list(tmp_path.rglob("*.tmp")) == []


def test_load_row_count(storage):
    storage.save("yahoo", "AAPL", make_lf())
    df = storage.load("yahoo", "AAPL", "2024-01-01", "2024-01-31").collect()
    assert len(df) == 2


def test_load_date_filter(storage):
    storage.save("yahoo", "AAPL", make_lf())
    df = storage.load("yahoo", "AAPL", "2024-01-01", "2024-01-31").collect()
    assert df["date"].to_list() == [20240102, 20240103]


def test_load_schema(storage):
    storage.save("yahoo", "AAPL", make_lf())
    df = storage.load("yahoo", "AAPL", "2024-01-01", "2024-12-31").collect()
    assert df.schema["date"] == pl.Int32
    for col in ["open", "high", "low", "close", "volume"]:
        assert df.schema[col] == pl.Float32


def test_load_parse_dates(storage):
    storage.save("yahoo", "AAPL", make_lf())
    df = storage.load("yahoo", "AAPL", "2024-01-01", "2024-12-31", parse_dates=True).collect()
    assert df.schema["date"] == pl.Date
    assert df["date"][0] == date(2024, 1, 2)


def test_load_raises_for_unknown_symbol(storage):
    with pytest.raises(FileNotFoundError):
        storage.load("yahoo", "UNKNOWN", "2024-01-01", "2024-12-31")


def test_load_raw_raises_when_only_adjusted_exists(storage):
    storage.save("yahoo", "AAPL", make_lf(), adjusted=True)
    with pytest.raises(FileNotFoundError):
        storage.load("yahoo", "AAPL", "2024-01-01", "2024-12-31", adjusted=False)


def test_load_adjusted_raises_when_only_raw_exists(storage):
    storage.save("yahoo", "AAPL", make_lf(), adjusted=False)
    with pytest.raises(FileNotFoundError):
        storage.load("yahoo", "AAPL", "2024-01-01", "2024-12-31", adjusted=True)


def test_adjusted_and_raw_are_isolated(storage, tmp_path):
    storage.save("yahoo", "AAPL", make_lf(), adjusted=True)
    storage.save("yahoo", "AAPL", make_lf(), adjusted=False)
    assert (tmp_path / "yahoo" / "ohlcv" / "adjusted" / "AAPL" / "AAPL_2024-01.pq").exists()
    assert (tmp_path / "yahoo" / "ohlcv" / "raw" / "AAPL" / "AAPL_2024-01.pq").exists()


def test_load_raw(storage):
    storage.save("yahoo", "AAPL", make_lf(), adjusted=False)
    df = storage.load("yahoo", "AAPL", "2024-01-01", "2024-12-31", adjusted=False).collect()
    assert len(df) == 4
