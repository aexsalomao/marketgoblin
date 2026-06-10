# Unit tests for the pure Alpaca trades parser (trades_rows_to_lf). No network.

import polars as pl
import pytest

from marketgoblin._normalize import normalize_trades
from marketgoblin.sources._alpaca_parsing import trades_rows_to_lf
from tests._alpaca_data import make_trade_rows


def test_trades_rows_to_lf_projects_payload_keys_to_tidy_columns():
    lf = trades_rows_to_lf(make_trade_rows(), "SPY")

    df = lf.collect()
    assert set(df.columns) == {
        "timestamp",
        "exchange",
        "price",
        "size",
        "conditions",
        "trade_id",
        "tape",
        "symbol",
    }


def test_trades_rows_to_lf_parses_timestamp_to_ns_utc():
    lf = trades_rows_to_lf(make_trade_rows(), "SPY")

    assert lf.collect_schema()["timestamp"] == pl.Datetime(time_unit="ns", time_zone="UTC")


def test_trades_rows_to_lf_uppercases_symbol():
    lf = trades_rows_to_lf(make_trade_rows(), "spy")

    assert lf.collect()["symbol"].unique().to_list() == ["SPY"]


def test_trades_rows_to_lf_drops_unknown_payload_keys():
    rows = make_trade_rows()
    rows[0]["unexpected"] = "ignored"

    df = trades_rows_to_lf(rows, "SPY").collect()

    assert "unexpected" not in df.columns


def test_trades_rows_to_lf_empty_raises():
    with pytest.raises(ValueError, match="No trades data"):
        trades_rows_to_lf([], "SPY")


def test_trades_rows_to_lf_backfills_optional_keys_missing_from_whole_page():
    # A thin page where every row omits conditions (c) and trade_id (i).
    rows = [{"t": "2026-05-01T13:30:00Z", "x": "V", "p": 500.0, "s": 100, "z": "B"}]

    df = trades_rows_to_lf(rows, "SPY").collect()

    assert df.schema["conditions"] == pl.List(pl.String)
    assert df.schema["trade_id"] == pl.Int64
    assert df["trade_id"].to_list() == [None]


def test_trades_rows_to_lf_normalizes_when_optional_keys_absent():
    # Regression: an all-missing optional column used to crash normalize_trades
    # with ColumnNotFoundError instead of yielding nulls.
    rows = [{"t": "2026-05-01T13:30:00Z", "x": "V", "p": 500.0, "s": 100, "z": "B"}]

    df = trades_rows_to_lf(rows, "SPY").pipe(normalize_trades).collect()

    assert df.height == 1
    assert df["trade_id"].to_list() == [None]
