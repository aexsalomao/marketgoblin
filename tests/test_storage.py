from datetime import date

import polars as pl
import pytest

from marketgoblin.datasets import Dataset
from marketgoblin.storage.disk import DiskStorage


def make_ohlcv_lf() -> pl.LazyFrame:
    # Two rows in Jan, two in Feb
    return pl.DataFrame(
        {
            "date": pl.Series([20240102, 20240103, 20240201, 20240202], dtype=pl.Int32),
            "open": pl.Series([185.0, 186.0, 187.0, 188.0], dtype=pl.Float32),
            "high": pl.Series([187.0, 188.0, 189.0, 190.0], dtype=pl.Float32),
            "low": pl.Series([183.0, 184.0, 185.0, 186.0], dtype=pl.Float32),
            "close": pl.Series([186.0, 187.0, 188.0, 189.0], dtype=pl.Float32),
            "volume": pl.Series([80_000_000, 75_000_000, 70_000_000, 65_000_000], dtype=pl.Int64),
            "symbol": ["AAPL"] * 4,
        }
    ).lazy()


def make_shares_lf() -> pl.LazyFrame:
    # Sparse — one entry in Jan, one in Feb
    return pl.DataFrame(
        {
            "date": pl.Series([20240115, 20240228], dtype=pl.Int32),
            "shares": pl.Series([15_000_000_000, 14_900_000_000], dtype=pl.Int64),
            "symbol": ["AAPL", "AAPL"],
        }
    ).lazy()


@pytest.fixture
def storage(tmp_path) -> DiskStorage:
    return DiskStorage(tmp_path)


def test_save_ohlcv_creates_pq_files(storage, tmp_path):
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_lf())
    assert (tmp_path / "yahoo" / "ohlcv" / "adjusted" / "AAPL" / "AAPL_2024-01.pq").exists()
    assert (tmp_path / "yahoo" / "ohlcv" / "adjusted" / "AAPL" / "AAPL_2024-02.pq").exists()


def test_save_ohlcv_creates_json_sidecars(storage, tmp_path):
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_lf())
    assert (tmp_path / "yahoo" / "ohlcv" / "adjusted" / "AAPL" / "AAPL_2024-01.json").exists()
    assert (tmp_path / "yahoo" / "ohlcv" / "adjusted" / "AAPL" / "AAPL_2024-02.json").exists()


def test_save_ohlcv_raw_goes_to_raw_dir(storage, tmp_path):
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_lf(), adjusted=False)
    assert (tmp_path / "yahoo" / "ohlcv" / "raw" / "AAPL" / "AAPL_2024-01.pq").exists()


def test_save_no_tmp_files_left(storage, tmp_path):
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_lf())
    assert list(tmp_path.rglob("*.tmp")) == []


def test_save_shares_uses_no_variant_segment(storage, tmp_path):
    storage.save("yahoo", "AAPL", Dataset.SHARES, make_shares_lf())
    assert (tmp_path / "yahoo" / "shares" / "AAPL" / "AAPL_2024-01.pq").exists()
    assert (tmp_path / "yahoo" / "shares" / "AAPL" / "AAPL_2024-02.pq").exists()
    # Confirm no adjusted/raw segment is created for shares
    assert not (tmp_path / "yahoo" / "shares" / "adjusted").exists()
    assert not (tmp_path / "yahoo" / "shares" / "raw").exists()


def test_save_shares_creates_sidecars(storage, tmp_path):
    storage.save("yahoo", "AAPL", Dataset.SHARES, make_shares_lf())
    sidecar = tmp_path / "yahoo" / "shares" / "AAPL" / "AAPL_2024-01.json"
    assert sidecar.exists()


def test_load_ohlcv_row_count(storage):
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_lf())
    df = storage.load("yahoo", "AAPL", Dataset.OHLCV, "2024-01-01", "2024-01-31").collect()
    assert len(df) == 2


def test_load_ohlcv_date_filter(storage):
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_lf())
    df = storage.load("yahoo", "AAPL", Dataset.OHLCV, "2024-01-01", "2024-01-31").collect()
    assert df["date"].to_list() == [20240102, 20240103]


def test_load_ohlcv_schema(storage):
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_lf())
    df = storage.load("yahoo", "AAPL", Dataset.OHLCV, "2024-01-01", "2024-12-31").collect()
    assert df.schema["date"] == pl.Int32
    assert df.schema["open"] == pl.Float32
    assert df.schema["high"] == pl.Float32
    assert df.schema["low"] == pl.Float32
    assert df.schema["close"] == pl.Float32
    assert df.schema["volume"] == pl.Int64


def test_load_ohlcv_parse_dates(storage):
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_lf())
    df = storage.load(
        "yahoo", "AAPL", Dataset.OHLCV, "2024-01-01", "2024-12-31", parse_dates=True
    ).collect()
    assert df.schema["date"] == pl.Date
    assert df["date"][0] == date(2024, 1, 2)


def test_load_raises_for_unknown_symbol(storage):
    with pytest.raises(FileNotFoundError):
        storage.load("yahoo", "UNKNOWN", Dataset.OHLCV, "2024-01-01", "2024-12-31")


def test_load_raw_raises_when_only_adjusted_exists(storage):
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_lf(), adjusted=True)
    with pytest.raises(FileNotFoundError):
        storage.load("yahoo", "AAPL", Dataset.OHLCV, "2024-01-01", "2024-12-31", adjusted=False)


def test_load_adjusted_raises_when_only_raw_exists(storage):
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_lf(), adjusted=False)
    with pytest.raises(FileNotFoundError):
        storage.load("yahoo", "AAPL", Dataset.OHLCV, "2024-01-01", "2024-12-31", adjusted=True)


def test_adjusted_and_raw_are_isolated(storage, tmp_path):
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_lf(), adjusted=True)
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_lf(), adjusted=False)
    assert (tmp_path / "yahoo" / "ohlcv" / "adjusted" / "AAPL" / "AAPL_2024-01.pq").exists()
    assert (tmp_path / "yahoo" / "ohlcv" / "raw" / "AAPL" / "AAPL_2024-01.pq").exists()


def test_ohlcv_and_shares_are_isolated(storage, tmp_path):
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_lf())
    storage.save("yahoo", "AAPL", Dataset.SHARES, make_shares_lf())
    assert (tmp_path / "yahoo" / "ohlcv" / "adjusted" / "AAPL" / "AAPL_2024-01.pq").exists()
    assert (tmp_path / "yahoo" / "shares" / "AAPL" / "AAPL_2024-01.pq").exists()


def test_load_shares_row_count(storage):
    storage.save("yahoo", "AAPL", Dataset.SHARES, make_shares_lf())
    df = storage.load("yahoo", "AAPL", Dataset.SHARES, "2024-01-01", "2024-12-31").collect()
    assert len(df) == 2


def test_load_shares_schema(storage):
    storage.save("yahoo", "AAPL", Dataset.SHARES, make_shares_lf())
    df = storage.load("yahoo", "AAPL", Dataset.SHARES, "2024-01-01", "2024-12-31").collect()
    assert df.schema["date"] == pl.Int32
    assert df.schema["shares"] == pl.Int64


def test_load_shares_parse_dates(storage):
    storage.save("yahoo", "AAPL", Dataset.SHARES, make_shares_lf())
    df = storage.load(
        "yahoo", "AAPL", Dataset.SHARES, "2024-01-01", "2024-12-31", parse_dates=True
    ).collect()
    assert df.schema["date"] == pl.Date


def test_save_lowercase_symbol_loadable_as_uppercase(storage, tmp_path):
    storage.save("yahoo", "aapl", Dataset.OHLCV, make_ohlcv_lf())
    df = storage.load("yahoo", "AAPL", Dataset.OHLCV, "2024-01-01", "2024-12-31").collect()
    assert len(df) == 4
    # Files should be written under the uppercase dir regardless of input case.
    assert (tmp_path / "yahoo" / "ohlcv" / "adjusted" / "AAPL").exists()
