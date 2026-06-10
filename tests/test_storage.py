from datetime import date

import polars as pl
import pytest

from marketgoblin.datasets import Dataset
from marketgoblin.storage.disk import DiskStorage


def make_ohlcv_lf() -> pl.LazyFrame:
    # Two trading days in Jan and two in Feb, stacked adjusted + raw.
    dates = [20240102, 20240103, 20240201, 20240202]
    return pl.DataFrame(
        {
            "date": pl.Series(dates * 2, dtype=pl.Int32),
            "open": pl.Series([185.0, 186.0, 187.0, 188.0] * 2, dtype=pl.Float32),
            "high": pl.Series([187.0, 188.0, 189.0, 190.0] * 2, dtype=pl.Float32),
            "low": pl.Series([183.0, 184.0, 185.0, 186.0] * 2, dtype=pl.Float32),
            "close": pl.Series([186.0, 187.0, 188.0, 189.0] * 2, dtype=pl.Float32),
            "volume": pl.Series(
                [80_000_000, 75_000_000, 70_000_000, 65_000_000] * 2, dtype=pl.Int64
            ),
            "symbol": ["AAPL"] * 8,
            "is_adjusted": [True] * 4 + [False] * 4,
        }
    ).lazy()


def make_shares_lf() -> pl.LazyFrame:
    return pl.DataFrame(
        {
            "date": pl.Series([20240115, 20240228], dtype=pl.Int32),
            "shares": pl.Series([15_000_000_000, 14_900_000_000], dtype=pl.Int64),
            "symbol": ["AAPL", "AAPL"],
        }
    ).lazy()


def make_dividends_lf() -> pl.LazyFrame:
    return pl.DataFrame(
        {
            "date": pl.Series([20240209], dtype=pl.Int32),
            "dividend": pl.Series([0.24], dtype=pl.Float32),
            "symbol": ["AAPL"],
        }
    ).lazy()


def make_splits_lf() -> pl.LazyFrame:
    return pl.DataFrame(
        {
            "date": pl.Series([20200831], dtype=pl.Int32),
            "split_factor": pl.Series([4.0], dtype=pl.Float32),
            "symbol": ["AAPL"],
        }
    ).lazy()


def make_fundamentals_daily_lf() -> pl.LazyFrame:
    return pl.DataFrame(
        {
            "date": pl.Series([20240102, 20240201], dtype=pl.Int32),
            "market_cap": pl.Series([1_500_000_000_000, 1_700_000_000_000], dtype=pl.Int64),
            "enterprise_val": pl.Series([1_550_000_000_000, 1_750_000_000_000], dtype=pl.Int64),
            "pe_ratio": pl.Series([32.5, 33.0], dtype=pl.Float32),
            "pb_ratio": pl.Series([50.0, 50.5], dtype=pl.Float32),
            "trailing_peg_1y": pl.Series([2.0, 2.1], dtype=pl.Float32),
            "symbol": ["AAPL"] * 2,
        }
    ).lazy()


@pytest.fixture
def statements_lf(make_statements_frame) -> pl.LazyFrame:
    # Two filings landing in different months — exercises the monthly slice
    # split for a quarterly-cadence dataset. Carries every statement field in
    # both variants so the full merged-variant shape round-trips correctly.
    return make_statements_frame(
        dates=[date(2024, 8, 1), date(2024, 5, 2)],
        fiscal_years=[2024, 2024],
        fiscal_quarters=[3, 2],
        anchors={
            "eps_diluted_as_reported": [1.40, 1.53],
            "revenue_as_reported": [85_777e6, 90_753e6],
            "net_income_as_reported": [21_448e6, 23_636e6],
        },
        on_disk=True,
    )


@pytest.fixture
def storage(tmp_path) -> DiskStorage:
    return DiskStorage(tmp_path)


def test_save_ohlcv_creates_pq_files(storage, tmp_path):
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_lf())
    assert (tmp_path / "yahoo" / "ohlcv" / "AAPL" / "AAPL_2024-01.pq").exists()
    assert (tmp_path / "yahoo" / "ohlcv" / "AAPL" / "AAPL_2024-02.pq").exists()


def test_save_ohlcv_creates_json_sidecars(storage, tmp_path):
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_lf())
    assert (tmp_path / "yahoo" / "ohlcv" / "AAPL" / "AAPL_2024-01.json").exists()
    assert (tmp_path / "yahoo" / "ohlcv" / "AAPL" / "AAPL_2024-02.json").exists()


def test_save_ohlcv_has_no_variant_segment(storage, tmp_path):
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_lf())
    # The tidy stacked layout drops the adjusted/raw directory split.
    assert not (tmp_path / "yahoo" / "ohlcv" / "adjusted").exists()
    assert not (tmp_path / "yahoo" / "ohlcv" / "raw").exists()


def test_save_no_tmp_files_left(storage, tmp_path):
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_lf())
    assert list(tmp_path.rglob("*.tmp")) == []


def test_save_shares_uses_flat_path(storage, tmp_path):
    storage.save("yahoo", "AAPL", Dataset.SHARES, make_shares_lf())
    assert (tmp_path / "yahoo" / "shares" / "AAPL" / "AAPL_2024-01.pq").exists()
    assert (tmp_path / "yahoo" / "shares" / "AAPL" / "AAPL_2024-02.pq").exists()


def test_save_shares_creates_sidecars(storage, tmp_path):
    storage.save("yahoo", "AAPL", Dataset.SHARES, make_shares_lf())
    sidecar = tmp_path / "yahoo" / "shares" / "AAPL" / "AAPL_2024-01.json"
    assert sidecar.exists()


def test_save_dividends_creates_pq_files(storage, tmp_path):
    storage.save("yahoo", "AAPL", Dataset.DIVIDENDS, make_dividends_lf())
    assert (tmp_path / "yahoo" / "dividends" / "AAPL" / "AAPL_2024-02.pq").exists()


def test_save_dividends_creates_sidecars(storage, tmp_path):
    storage.save("yahoo", "AAPL", Dataset.DIVIDENDS, make_dividends_lf())
    sidecar = tmp_path / "yahoo" / "dividends" / "AAPL" / "AAPL_2024-02.json"
    assert sidecar.exists()


def test_load_ohlcv_row_count(storage):
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_lf())
    df = storage.load("yahoo", "AAPL", Dataset.OHLCV, "2024-01-01", "2024-01-31").collect()
    # Two unique days × 2 variants
    assert len(df) == 4


def test_load_ohlcv_date_filter(storage):
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_lf())
    df = storage.load("yahoo", "AAPL", Dataset.OHLCV, "2024-01-01", "2024-01-31").collect()
    assert set(df["date"].to_list()) == {20240102, 20240103}


def test_load_ohlcv_schema(storage):
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_lf())
    df = storage.load("yahoo", "AAPL", Dataset.OHLCV, "2024-01-01", "2024-12-31").collect()
    assert df.schema["date"] == pl.Int32
    assert df.schema["open"] == pl.Float32
    assert df.schema["high"] == pl.Float32
    assert df.schema["low"] == pl.Float32
    assert df.schema["close"] == pl.Float32
    assert df.schema["volume"] == pl.Int64
    assert df.schema["is_adjusted"] == pl.Boolean


def test_load_ohlcv_filter_by_is_adjusted(storage):
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_lf())
    lf = storage.load("yahoo", "AAPL", Dataset.OHLCV, "2024-01-01", "2024-12-31")
    adjusted = lf.filter(pl.col("is_adjusted")).collect()
    raw = lf.filter(~pl.col("is_adjusted")).collect()
    assert len(adjusted) == 4
    assert len(raw) == 4


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


def test_ohlcv_and_shares_are_isolated(storage, tmp_path):
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_lf())
    storage.save("yahoo", "AAPL", Dataset.SHARES, make_shares_lf())
    assert (tmp_path / "yahoo" / "ohlcv" / "AAPL" / "AAPL_2024-01.pq").exists()
    assert (tmp_path / "yahoo" / "shares" / "AAPL" / "AAPL_2024-01.pq").exists()


def test_ohlcv_and_dividends_are_isolated(storage, tmp_path):
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_lf())
    storage.save("yahoo", "AAPL", Dataset.DIVIDENDS, make_dividends_lf())
    assert (tmp_path / "yahoo" / "ohlcv" / "AAPL" / "AAPL_2024-01.pq").exists()
    assert (tmp_path / "yahoo" / "dividends" / "AAPL" / "AAPL_2024-02.pq").exists()


def test_load_shares_row_count(storage):
    storage.save("yahoo", "AAPL", Dataset.SHARES, make_shares_lf())
    df = storage.load("yahoo", "AAPL", Dataset.SHARES, "2024-01-01", "2024-12-31").collect()
    assert len(df) == 2


def test_load_shares_schema(storage):
    storage.save("yahoo", "AAPL", Dataset.SHARES, make_shares_lf())
    df = storage.load("yahoo", "AAPL", Dataset.SHARES, "2024-01-01", "2024-12-31").collect()
    assert df.schema["date"] == pl.Int32
    assert df.schema["shares"] == pl.Int64


def test_load_dividends_row_count(storage):
    storage.save("yahoo", "AAPL", Dataset.DIVIDENDS, make_dividends_lf())
    df = storage.load("yahoo", "AAPL", Dataset.DIVIDENDS, "2024-01-01", "2024-12-31").collect()
    assert len(df) == 1


def test_load_dividends_schema(storage):
    storage.save("yahoo", "AAPL", Dataset.DIVIDENDS, make_dividends_lf())
    df = storage.load("yahoo", "AAPL", Dataset.DIVIDENDS, "2024-01-01", "2024-12-31").collect()
    assert df.schema["date"] == pl.Int32
    assert df.schema["dividend"] == pl.Float32


def test_load_shares_parse_dates(storage):
    storage.save("yahoo", "AAPL", Dataset.SHARES, make_shares_lf())
    df = storage.load(
        "yahoo", "AAPL", Dataset.SHARES, "2024-01-01", "2024-12-31", parse_dates=True
    ).collect()
    assert df.schema["date"] == pl.Date


def test_save_lowercase_symbol_loadable_as_uppercase(storage, tmp_path):
    storage.save("yahoo", "aapl", Dataset.OHLCV, make_ohlcv_lf())
    df = storage.load("yahoo", "AAPL", Dataset.OHLCV, "2024-01-01", "2024-12-31").collect()
    assert len(df) == 8
    assert (tmp_path / "yahoo" / "ohlcv" / "AAPL").exists()


def make_ohlcv_partial_lf() -> pl.LazyFrame:
    # A later partial-month fetch: one PRE-EXISTING Jan date (restated close) + one NEW date.
    dates = [20240103, 20240104]
    return pl.DataFrame(
        {
            "date": pl.Series(dates * 2, dtype=pl.Int32),
            "open": pl.Series([186.5, 189.0] * 2, dtype=pl.Float32),
            "high": pl.Series([188.5, 191.0] * 2, dtype=pl.Float32),
            "low": pl.Series([184.5, 187.0] * 2, dtype=pl.Float32),
            "close": pl.Series([999.0, 190.0] * 2, dtype=pl.Float32),
            "volume": pl.Series([60_000_000, 55_000_000] * 2, dtype=pl.Int64),
            "symbol": ["AAPL"] * 4,
            "is_adjusted": [True] * 2 + [False] * 2,
        }
    ).lazy()


def test_save_partial_month_merges_with_existing_slice(storage):
    # Regression: a fetch covering only part of a month must NOT erase the rest of the
    # slice (the paper-trading daily loop fetches a ~7-day tail every evening).
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_lf())
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_partial_lf())
    df = storage.load("yahoo", "AAPL", Dataset.OHLCV, "2024-01-01", "2024-01-31").collect()
    # 0102 kept from the first save, 0103 restated, 0104 added — each in 2 variants
    assert sorted(set(df["date"].to_list())) == [20240102, 20240103, 20240104]
    assert len(df) == 6


def test_save_overlapping_dates_take_new_rows(storage):
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_lf())
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_partial_lf())
    df = storage.load("yahoo", "AAPL", Dataset.OHLCV, "2024-01-01", "2024-01-31").collect()
    restated = df.filter((pl.col("date") == 20240103) & pl.col("is_adjusted"))
    assert len(restated) == 1  # no duplicate variant rows for the overlapping date
    assert restated["close"][0] == 999.0  # the restatement won
    untouched = df.filter((pl.col("date") == 20240102) & pl.col("is_adjusted"))
    assert untouched["close"][0] == 186.0  # the uncovered date survived verbatim


def test_save_partial_month_leaves_other_months_alone(storage):
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_lf())
    storage.save("yahoo", "AAPL", Dataset.OHLCV, make_ohlcv_partial_lf())
    feb = storage.load("yahoo", "AAPL", Dataset.OHLCV, "2024-02-01", "2024-02-29").collect()
    assert set(feb["date"].to_list()) == {20240201, 20240202}


def test_save_splits_creates_pq_files(storage, tmp_path):
    storage.save("tiingo", "AAPL", Dataset.SPLITS, make_splits_lf())
    assert (tmp_path / "tiingo" / "splits" / "AAPL" / "AAPL_2020-08.pq").exists()


def test_save_splits_creates_sidecars(storage, tmp_path):
    storage.save("tiingo", "AAPL", Dataset.SPLITS, make_splits_lf())
    sidecar = tmp_path / "tiingo" / "splits" / "AAPL" / "AAPL_2020-08.json"
    assert sidecar.exists()


def test_load_splits_row_count(storage):
    storage.save("tiingo", "AAPL", Dataset.SPLITS, make_splits_lf())
    df = storage.load("tiingo", "AAPL", Dataset.SPLITS, "2020-01-01", "2020-12-31").collect()
    assert len(df) == 1


def test_load_splits_schema(storage):
    storage.save("tiingo", "AAPL", Dataset.SPLITS, make_splits_lf())
    df = storage.load("tiingo", "AAPL", Dataset.SPLITS, "2020-01-01", "2020-12-31").collect()
    assert df.schema["date"] == pl.Int32
    assert df.schema["split_factor"] == pl.Float32


def test_load_splits_parse_dates(storage):
    storage.save("tiingo", "AAPL", Dataset.SPLITS, make_splits_lf())
    df = storage.load(
        "tiingo", "AAPL", Dataset.SPLITS, "2020-01-01", "2020-12-31", parse_dates=True
    ).collect()
    assert df.schema["date"] == pl.Date
    assert df["date"][0] == date(2020, 8, 31)


def test_save_fundamentals_daily_creates_pq_files(storage, tmp_path):
    storage.save("tiingo", "AAPL", Dataset.FUNDAMENTALS_DAILY, make_fundamentals_daily_lf())
    assert (tmp_path / "tiingo" / "fundamentals_daily" / "AAPL" / "AAPL_2024-01.pq").exists()
    assert (tmp_path / "tiingo" / "fundamentals_daily" / "AAPL" / "AAPL_2024-02.pq").exists()


def test_save_fundamentals_daily_creates_sidecars(storage, tmp_path):
    storage.save("tiingo", "AAPL", Dataset.FUNDAMENTALS_DAILY, make_fundamentals_daily_lf())
    sidecar = tmp_path / "tiingo" / "fundamentals_daily" / "AAPL" / "AAPL_2024-01.json"
    assert sidecar.exists()


def test_load_fundamentals_daily_row_count(storage):
    storage.save("tiingo", "AAPL", Dataset.FUNDAMENTALS_DAILY, make_fundamentals_daily_lf())
    df = storage.load(
        "tiingo", "AAPL", Dataset.FUNDAMENTALS_DAILY, "2024-01-01", "2024-12-31"
    ).collect()
    assert len(df) == 2


def test_load_fundamentals_daily_schema(storage):
    storage.save("tiingo", "AAPL", Dataset.FUNDAMENTALS_DAILY, make_fundamentals_daily_lf())
    df = storage.load(
        "tiingo", "AAPL", Dataset.FUNDAMENTALS_DAILY, "2024-01-01", "2024-12-31"
    ).collect()
    assert df.schema["date"] == pl.Int32
    assert df.schema["market_cap"] == pl.Int64
    assert df.schema["enterprise_val"] == pl.Int64
    assert df.schema["pe_ratio"] == pl.Float32
    assert df.schema["pb_ratio"] == pl.Float32
    assert df.schema["trailing_peg_1y"] == pl.Float32


def test_load_fundamentals_daily_parse_dates(storage):
    storage.save("tiingo", "AAPL", Dataset.FUNDAMENTALS_DAILY, make_fundamentals_daily_lf())
    df = storage.load(
        "tiingo",
        "AAPL",
        Dataset.FUNDAMENTALS_DAILY,
        "2024-01-01",
        "2024-12-31",
        parse_dates=True,
    ).collect()
    assert df.schema["date"] == pl.Date
    assert df["date"][0] == date(2024, 1, 2)


def test_save_statements_creates_pq_files(storage, tmp_path, statements_lf):
    storage.save("tiingo", "AAPL", Dataset.FUNDAMENTALS_STATEMENTS, statements_lf)
    base = tmp_path / "tiingo" / "fundamentals_statements" / "AAPL"
    assert (base / "AAPL_2024-05.pq").exists()
    assert (base / "AAPL_2024-08.pq").exists()


def test_save_statements_creates_sidecars(storage, tmp_path, statements_lf):
    storage.save("tiingo", "AAPL", Dataset.FUNDAMENTALS_STATEMENTS, statements_lf)
    sidecar = tmp_path / "tiingo" / "fundamentals_statements" / "AAPL" / "AAPL_2024-05.json"
    assert sidecar.exists()


def test_load_statements_row_count(storage, statements_lf):
    storage.save("tiingo", "AAPL", Dataset.FUNDAMENTALS_STATEMENTS, statements_lf)
    df = storage.load(
        "tiingo", "AAPL", Dataset.FUNDAMENTALS_STATEMENTS, "2024-01-01", "2024-12-31"
    ).collect()
    assert len(df) == 2


def test_load_statements_schema(storage, statements_lf):
    storage.save("tiingo", "AAPL", Dataset.FUNDAMENTALS_STATEMENTS, statements_lf)
    df = storage.load(
        "tiingo", "AAPL", Dataset.FUNDAMENTALS_STATEMENTS, "2024-01-01", "2024-12-31"
    ).collect()
    assert df.schema["date"] == pl.Int32
    assert df.schema["fiscal_year"] == pl.Int16
    assert df.schema["fiscal_quarter"] == pl.Int8
    assert df.schema["eps_diluted_as_reported"] == pl.Float32
    assert df.schema["eps_basic_as_reported"] == pl.Float32
    assert df.schema["eps_diluted_adjusted"] == pl.Float32
    assert df.schema["eps_basic_adjusted"] == pl.Float32
    assert df.schema["revenue_as_reported"] == pl.Float64
    assert df.schema["total_assets_adjusted"] == pl.Float64


def test_load_statements_parse_dates(storage, statements_lf):
    storage.save("tiingo", "AAPL", Dataset.FUNDAMENTALS_STATEMENTS, statements_lf)
    df = storage.load(
        "tiingo",
        "AAPL",
        Dataset.FUNDAMENTALS_STATEMENTS,
        "2024-01-01",
        "2024-12-31",
        parse_dates=True,
    ).collect()
    assert df.schema["date"] == pl.Date
    assert sorted(df["date"].to_list()) == [date(2024, 5, 2), date(2024, 8, 1)]
