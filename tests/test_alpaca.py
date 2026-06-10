# Orchestration tests for AlpacaSource: dataset dispatch, credential handling,
# symbol casing at the API boundary, pagination, and retry/error semantics.
# The pure row->frame projection is covered in test_alpaca_parsing.

from unittest.mock import Mock, patch

import polars as pl
import pytest
import requests

from marketgoblin.datasets import Dataset
from marketgoblin.sources.alpaca import AlpacaSource
from marketgoblin.sources.base import _MAX_RETRIES
from tests._alpaca_data import make_trade_rows, make_trades_response


@pytest.fixture
def source() -> AlpacaSource:
    return AlpacaSource(api_key="test-key", api_secret="test-secret")


def _response(payload: dict) -> Mock:
    resp = Mock()
    resp.json.return_value = payload
    resp.raise_for_status.return_value = None
    return resp


def test_supported_datasets(source):
    assert source.supported_datasets == frozenset({Dataset.TRADES})


def test_init_resolves_credentials_from_env(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "env-key")
    monkeypatch.setenv("ALPACA_API_SECRET", "env-secret")

    source = AlpacaSource()

    assert source.api_key == "env-key"
    assert source._api_secret == "env-secret"


def test_init_explicit_credentials_take_precedence_over_env(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "env-key")

    source = AlpacaSource(api_key="explicit-key", api_secret="explicit-secret")

    assert source.api_key == "explicit-key"


def test_fetch_trades_returns_normalized_lazy_frame(source):
    with patch.object(source._session, "get", return_value=_response(make_trades_response())):
        df = source.fetch(Dataset.TRADES, "SPY", "2026-05-01", "2026-06-01").collect()
    assert df.schema["date"] == pl.Int32
    assert df.schema["timestamp"] == pl.Datetime(time_unit="ns", time_zone="UTC")
    assert df.schema["price"] == pl.Float32
    assert df.schema["size"] == pl.Int64
    assert df.schema["trade_id"] == pl.Int64


def test_fetch_trades_derives_int32_date_from_timestamp(source):
    with patch.object(source._session, "get", return_value=_response(make_trades_response())):
        df = source.fetch(Dataset.TRADES, "SPY", "2026-05-01", "2026-06-01").collect()
    assert df["date"].to_list() == [20260501, 20260501, 20260504]


def test_fetch_trades_uppercases_symbol_in_output(source):
    with patch.object(source._session, "get", return_value=_response(make_trades_response())):
        df = source.fetch(Dataset.TRADES, "spy", "2026-05-01", "2026-06-01").collect()
    assert df["symbol"].unique().to_list() == ["SPY"]


def test_fetch_trades_uppercases_symbol_in_request_url(source):
    with patch.object(
        source._session, "get", return_value=_response(make_trades_response())
    ) as mock:
        source.fetch(Dataset.TRADES, "spy", "2026-05-01", "2026-06-01").collect()
    assert mock.call_args.args[0] == "https://data.alpaca.markets/v2/stocks/SPY/trades"


def test_fetch_trades_sends_iex_feed_by_default(source):
    with patch.object(
        source._session, "get", return_value=_response(make_trades_response())
    ) as mock:
        source.fetch(Dataset.TRADES, "SPY", "2026-05-01", "2026-06-01").collect()
    assert mock.call_args.kwargs["params"]["feed"] == "iex"


def test_fetch_trades_uses_configured_feed(monkeypatch):
    source = AlpacaSource(api_key="k", api_secret="s", feed="sip")
    with patch.object(
        source._session, "get", return_value=_response(make_trades_response())
    ) as mock:
        source.fetch(Dataset.TRADES, "SPY", "2026-05-01", "2026-06-01").collect()
    assert mock.call_args.kwargs["params"]["feed"] == "sip"


def test_fetch_trades_paginates_until_token_exhausted(source):
    first = make_trade_rows()[:2]
    second = make_trade_rows()[2:]
    pages = [
        _response(make_trades_response(first, next_page_token="tok")),
        _response(make_trades_response(second, next_page_token=None)),
    ]
    with patch.object(source._session, "get", side_effect=pages) as mock:
        df = source.fetch(Dataset.TRADES, "SPY", "2026-05-01", "2026-06-01").collect()
    assert mock.call_count == 2
    assert df.height == 3
    assert mock.call_args_list[1].kwargs["params"]["page_token"] == "tok"


def test_fetch_trades_forwards_date_window_to_params(source):
    with patch.object(
        source._session, "get", return_value=_response(make_trades_response())
    ) as mock:
        source.fetch(Dataset.TRADES, "SPY", "2026-05-01", "2026-06-01").collect()
    params = mock.call_args.kwargs["params"]
    assert params["start"] == "2026-05-01"
    assert params["end"] == "2026-06-01"


def test_fetch_trades_tolerates_null_trades_page(source):
    # Alpaca can return "trades": null alongside a token; the `or []` guard must hold.
    pages = [
        _response({"trades": None, "symbol": "SPY", "next_page_token": "tok"}),
        _response(make_trades_response(next_page_token=None)),
    ]
    with patch.object(source._session, "get", side_effect=pages):
        df = source.fetch(Dataset.TRADES, "SPY", "2026-05-01", "2026-06-01").collect()
    assert df.height == 3


def test_fetch_trades_empty_raises_without_retry(source):
    empty = _response(make_trades_response(rows=[], next_page_token=None))
    with (
        patch.object(source._session, "get", return_value=empty) as mock,
        pytest.raises(ValueError, match="No trades data"),
    ):
        source.fetch(Dataset.TRADES, "SPY", "2026-05-01", "2026-06-01")
    assert mock.call_count == 1


def test_fetch_trades_missing_credentials_raises(monkeypatch):
    monkeypatch.delenv("ALPACA_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_API_SECRET", raising=False)
    source = AlpacaSource()
    with pytest.raises(RuntimeError, match="ALPACA_API_KEY"):
        source.fetch(Dataset.TRADES, "SPY", "2026-05-01", "2026-06-01")


def test_retry_on_transient_error(source):
    with (
        patch.object(
            source._session,
            "get",
            side_effect=[requests.HTTPError("transient"), _response(make_trades_response())],
        ),
        patch("marketgoblin.sources.base.time.sleep"),
    ):
        df = source.fetch(Dataset.TRADES, "SPY", "2026-05-01", "2026-06-01").collect()
    assert df.height == 3


def test_http_error_propagates_after_exhausting_retries(source):
    with (
        patch.object(
            source._session, "get", side_effect=requests.HTTPError("upstream down")
        ) as mock,
        patch("marketgoblin.sources.base.time.sleep"),
        pytest.raises(requests.HTTPError),
    ):
        source.fetch(Dataset.TRADES, "SPY", "2026-05-01", "2026-06-01")
    assert mock.call_count == _MAX_RETRIES


def test_fetch_unsupported_dataset_raises(source):
    with pytest.raises(ValueError, match="does not support dataset"):
        source.fetch(Dataset.OHLCV, "SPY", "2026-05-01", "2026-06-01")
