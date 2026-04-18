import json

import polars as pl
import pytest

from marketgoblin._metadata import build_ohlcv, build_shares, write


def make_ohlcv_chunk() -> pl.DataFrame:
    # Four trading days in Jan 2024: 2nd (Tue) through 5th (Fri)
    return pl.DataFrame(
        {
            "date": pl.Series([20240102, 20240103, 20240104, 20240105], dtype=pl.Int32),
            "open": pl.Series([185.0, 186.0, 187.0, 188.0], dtype=pl.Float32),
            "high": pl.Series([187.0, 188.0, 189.0, 190.0], dtype=pl.Float32),
            "low": pl.Series([183.0, 184.0, 185.0, 186.0], dtype=pl.Float32),
            "close": pl.Series([186.0, 187.0, 188.0, 189.0], dtype=pl.Float32),
            "volume": pl.Series([80_000_000, 75_000_000, 70_000_000, 65_000_000], dtype=pl.Int64),
            "symbol": ["AAPL"] * 4,
        }
    )


def make_shares_chunk() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "date": pl.Series([20240102, 20240115, 20240130], dtype=pl.Int32),
            "shares": pl.Series([15_000_000_000, 14_900_000_000, 14_800_000_000], dtype=pl.Int64),
            "symbol": ["AAPL"] * 3,
        }
    )


@pytest.fixture
def fake_pq(tmp_path) -> object:
    path = tmp_path / "AAPL_2024-01.pq"
    path.write_bytes(b"")
    return path


def test_build_ohlcv_has_all_keys(fake_pq):
    meta = build_ohlcv(make_ohlcv_chunk(), "yahoo", "AAPL", "2024-01", 0)
    expected = {
        "symbol",
        "provider",
        "year_month",
        "row_count",
        "start_date",
        "end_date",
        "expected_trading_days",
        "missing_days",
        "columns",
        "downloaded_at",
        "file_size_bytes",
        "price_adjusted",
        "currency",
        "close_min",
        "close_max",
        "volume_min",
        "volume_max",
    }
    assert set(meta.keys()) == expected


def test_build_ohlcv_stats(fake_pq):
    meta = build_ohlcv(make_ohlcv_chunk(), "yahoo", "AAPL", "2024-01", 0)
    assert meta["symbol"] == "AAPL"
    assert meta["provider"] == "yahoo"
    assert meta["row_count"] == 4
    assert meta["start_date"] == 20240102
    assert meta["end_date"] == 20240105
    assert meta["price_adjusted"] is True
    assert meta["currency"] == "USD"


def test_build_ohlcv_close_min_max(fake_pq):
    meta = build_ohlcv(make_ohlcv_chunk(), "yahoo", "AAPL", "2024-01", 0)
    assert meta["close_min"] == pytest.approx(186.0, rel=1e-3)
    assert meta["close_max"] == pytest.approx(189.0, rel=1e-3)


def test_build_ohlcv_missing_days_includes_holidays(fake_pq):
    meta = build_ohlcv(make_ohlcv_chunk(), "yahoo", "AAPL", "2024-01", 0)
    # Jan 1 (New Year's) and Jan 15 (MLK Day) are weekdays not in chunk
    assert "2024-01-01" in meta["missing_days"]
    assert "2024-01-15" in meta["missing_days"]


def test_build_ohlcv_missing_days_readable_format(fake_pq):
    meta = build_ohlcv(make_ohlcv_chunk(), "yahoo", "AAPL", "2024-01", 0)
    assert all(len(d) == 10 and d[4] == "-" and d[7] == "-" for d in meta["missing_days"])


def test_write_creates_json(tmp_path, fake_pq):
    meta = build_ohlcv(make_ohlcv_chunk(), "yahoo", "AAPL", "2024-01", 0)
    write(meta, fake_pq)

    json_path = fake_pq.with_suffix(".json")
    assert json_path.exists()
    assert json.loads(json_path.read_text())["symbol"] == "AAPL"


def test_write_no_tmp_files_left(tmp_path, fake_pq):
    meta = build_ohlcv(make_ohlcv_chunk(), "yahoo", "AAPL", "2024-01", 0)
    write(meta, fake_pq)
    assert list(tmp_path.glob("*.tmp")) == []


def test_build_ohlcv_price_adjusted_true(fake_pq):
    meta = build_ohlcv(make_ohlcv_chunk(), "yahoo", "AAPL", "2024-01", 0, price_adjusted=True)
    assert meta["price_adjusted"] is True


def test_build_ohlcv_price_adjusted_false(fake_pq):
    meta = build_ohlcv(make_ohlcv_chunk(), "yahoo", "AAPL", "2024-01", 0, price_adjusted=False)
    assert meta["price_adjusted"] is False


def test_build_shares_has_all_keys(fake_pq):
    meta = build_shares(make_shares_chunk(), "yahoo", "AAPL", "2024-01", 0)
    expected = {
        "symbol",
        "provider",
        "year_month",
        "row_count",
        "start_date",
        "end_date",
        "columns",
        "downloaded_at",
        "file_size_bytes",
        "shares_min",
        "shares_max",
    }
    assert set(meta.keys()) == expected


def test_build_shares_stats(fake_pq):
    meta = build_shares(make_shares_chunk(), "yahoo", "AAPL", "2024-01", 0)
    assert meta["symbol"] == "AAPL"
    assert meta["row_count"] == 3
    assert meta["start_date"] == 20240102
    assert meta["end_date"] == 20240130
    assert meta["shares_min"] == 14_800_000_000
    assert meta["shares_max"] == 15_000_000_000


def test_build_shares_omits_ohlcv_only_keys(fake_pq):
    meta = build_shares(make_shares_chunk(), "yahoo", "AAPL", "2024-01", 0)
    assert "missing_days" not in meta
    assert "expected_trading_days" not in meta
    assert "price_adjusted" not in meta
