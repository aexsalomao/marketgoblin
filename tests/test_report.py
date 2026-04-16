import csv
from unittest.mock import patch

import polars as pl
import pytest

from marketgoblin import MarketGoblin


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


def read_report(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_report_requires_save_path():
    with pytest.raises(ValueError, match="report=True requires save_path"):
        MarketGoblin(provider="yahoo", report=True)


def test_report_not_created_when_disabled(tmp_path):
    goblin = MarketGoblin(provider="yahoo", save_path=tmp_path)
    with (
        patch.object(goblin._source, "fetch", return_value=make_lf("AAPL")),
        patch.object(goblin._storage, "save"),
        patch.object(goblin._storage, "load", return_value=make_lf("AAPL")),
    ):
        goblin.fetch("AAPL", "2024-01-01", "2024-01-31")
    assert not (tmp_path / "download_report.csv").exists()


def test_report_created_on_success(tmp_path):
    goblin = MarketGoblin(provider="yahoo", save_path=tmp_path, report=True)
    with (
        patch.object(goblin._source, "fetch", return_value=make_lf("AAPL")),
        patch.object(goblin._storage, "save"),
        patch.object(goblin._storage, "load", return_value=make_lf("AAPL")),
    ):
        goblin.fetch("AAPL", "2024-01-01", "2024-01-31")

    rows = read_report(tmp_path / "download_report.csv")
    assert len(rows) == 1
    row = rows[0]
    assert row["symbol"] == "AAPL"
    assert row["provider"] == "yahoo"
    assert row["adjusted"] == "True"
    assert row["requested_start"] == "2024-01-01"
    assert row["requested_end"] == "2024-01-31"
    assert row["actual_start"] == "2024-01-02"
    assert row["actual_end"] == "2024-01-03"
    assert row["rows_fetched"] == "2"
    assert row["status"] == "success"
    assert row["error_type"] == ""
    assert row["error_message"] == ""
    assert int(row["duration_ms"]) >= 0


def test_report_appends_without_duplicate_header(tmp_path):
    goblin = MarketGoblin(provider="yahoo", save_path=tmp_path, report=True)
    with (
        patch.object(goblin._source, "fetch", return_value=make_lf("AAPL")),
        patch.object(goblin._storage, "save"),
        patch.object(goblin._storage, "load", return_value=make_lf("AAPL")),
    ):
        goblin.fetch("AAPL", "2024-01-01", "2024-01-31")
        goblin.fetch("AAPL", "2024-02-01", "2024-02-28")

    report_path = tmp_path / "download_report.csv"
    rows = read_report(report_path)
    assert len(rows) == 2

    with open(report_path, encoding="utf-8") as f:
        lines = f.readlines()
    # header + 2 data rows only
    assert lines[0].startswith("timestamp")
    assert len(lines) == 3


def test_report_records_error(tmp_path):
    goblin = MarketGoblin(provider="yahoo", save_path=tmp_path, report=True)
    err = ValueError("no data for AAPL")
    with patch.object(goblin._source, "fetch", side_effect=err), pytest.raises(ValueError):
        goblin.fetch("AAPL", "2024-01-01", "2024-01-31")

    rows = read_report(tmp_path / "download_report.csv")
    assert len(rows) == 1
    row = rows[0]
    assert row["symbol"] == "AAPL"
    assert row["status"] == "error"
    assert row["error_type"] == "ValueError"
    assert "no data for AAPL" in row["error_message"]
    assert row["actual_start"] == ""
    assert row["actual_end"] == ""
    assert row["rows_fetched"] == ""
    assert int(row["duration_ms"]) >= 0


def test_report_fetch_many_records_all_symbols(tmp_path):
    goblin = MarketGoblin(provider="yahoo", save_path=tmp_path, report=True)

    def side_effect(symbol, *args, **kwargs):
        if symbol == "BAD":
            raise ValueError("no data")
        with (
            patch.object(goblin._storage, "save"),
            patch.object(goblin._storage, "load", return_value=make_lf(symbol)),
        ):
            return make_lf(symbol)

    with patch.object(goblin._source, "fetch", side_effect=side_effect):
        goblin.fetch_many(["AAPL", "BAD", "MSFT"], "2024-01-01", "2024-01-31")

    rows = read_report(tmp_path / "download_report.csv")
    assert len(rows) == 3
    statuses = {r["symbol"]: r["status"] for r in rows}
    assert statuses["AAPL"] == "success"
    assert statuses["MSFT"] == "success"
    assert statuses["BAD"] == "error"
