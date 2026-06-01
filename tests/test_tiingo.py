# Orchestration tests for TiingoSource: dataset dispatch, symbol casing at the
# API boundary, date-range filtering, retry/error semantics, and the
# metadata/classification flows. Pure parser helpers are covered in
# test_tiingo_prices / test_tiingo_fundamentals / test_tiingo_metadata.

from typing import Any
from unittest.mock import patch

import polars as pl
import pytest
import requests
from tiingo.restclient import RestClientError

from marketgoblin.classification import Classification
from marketgoblin.datasets import Dataset
from marketgoblin.sources._tiingo_parsing import metadata as tiingo_metadata
from marketgoblin.sources.tiingo import TiingoSource
from marketgoblin.ticker_metadata import TickerMetadata
from tests._tiingo_data import (
    make_fundamentals_rows,
    make_meta_payload,
    make_metadata_dict,
    make_prices_rows,
    make_prices_rows_with_split,
    make_statements_rows_adjusted,
    make_statements_rows_as_reported,
)


@pytest.fixture
def source() -> TiingoSource:
    # api_key keeps the TiingoClient constructor happy; we patch all I/O below.
    return TiingoSource(api_key="test-key")


def test_init_resolves_api_key_from_env_var(monkeypatch):
    # Without this fallback, fetch_classification would send "Token None" as
    # the bearer (the tiingo client reads TIINGO_API_KEY itself, but
    # self.api_key — used by the meta endpoint — would stay None).
    monkeypatch.setenv("TIINGO_API_KEY", "env-key")
    source = TiingoSource()
    assert source.api_key == "env-key"


def test_init_explicit_api_key_takes_precedence_over_env(monkeypatch):
    monkeypatch.setenv("TIINGO_API_KEY", "env-key")
    source = TiingoSource(api_key="explicit-key")
    assert source.api_key == "explicit-key"


def test_supported_datasets(source):
    assert source.supported_datasets == frozenset(
        {
            Dataset.OHLCV,
            Dataset.SHARES,
            Dataset.DIVIDENDS,
            Dataset.SPLITS,
            Dataset.FUNDAMENTALS_DAILY,
            Dataset.FUNDAMENTALS_STATEMENTS,
        }
    )


def test_fetch_ohlcv_returns_normalized_lazy_frame(source):
    with patch.object(source._client, "get_ticker_price", return_value=make_prices_rows()):
        df = source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert df.schema["date"] == pl.Int32
    assert df.schema["close"] == pl.Float32
    assert df.schema["volume"] == pl.Int64
    assert df.schema["is_adjusted"] == pl.Boolean


def test_fetch_ohlcv_stacks_adjusted_and_raw(source):
    with patch.object(source._client, "get_ticker_price", return_value=make_prices_rows()):
        df = source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert df.height == 4
    assert df.filter(pl.col("is_adjusted")).height == 2
    assert df.filter(~pl.col("is_adjusted")).height == 2


def test_fetch_ohlcv_lowercases_symbol_for_api(source):
    with patch.object(source._client, "get_ticker_price", return_value=make_prices_rows()) as mock:
        source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert mock.call_args.args[0] == "aapl"


def test_fetch_dividends_lowercases_symbol_for_api(source):
    with patch.object(source._client, "get_ticker_price", return_value=make_prices_rows()) as mock:
        source.fetch(Dataset.DIVIDENDS, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert mock.call_args.args[0] == "aapl"


def test_fetch_shares_lowercases_symbol_for_both_api_calls(source):
    with (
        patch.object(
            source._client, "get_ticker_price", return_value=make_prices_rows()
        ) as price_mock,
        patch.object(
            source._client, "get_fundamentals_daily", return_value=make_fundamentals_rows()
        ) as fundamentals_mock,
    ):
        source.fetch(Dataset.SHARES, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert price_mock.call_args.args[0] == "aapl"
    assert fundamentals_mock.call_args.args[0] == "aapl"


def test_fetch_metadata_lowercases_symbol_for_api(source):
    with (
        patch.object(
            source._client, "get_ticker_metadata", return_value=make_metadata_dict()
        ) as meta_mock,
        patch(
            "marketgoblin.sources.tiingo.fetch_latest_fundamentals",
            return_value=make_fundamentals_rows()[-1],
        ) as fundamentals_mock,
        patch("marketgoblin.sources.tiingo.fetch_latest_close", return_value=110.0) as close_mock,
    ):
        source.fetch_metadata("AAPL")
    # get_ticker_metadata is called inline by the orchestrator; the latest-*
    # helpers receive the raw symbol and lowercase it themselves at the API
    # boundary (covered by their own unit tests in test_tiingo_metadata).
    assert meta_mock.call_args.args[0] == "aapl"
    assert fundamentals_mock.call_args.args[1] == "AAPL"
    assert close_mock.call_args.args[1] == "AAPL"


def test_fetch_classification_lowercases_symbol_for_api(source):
    with patch.object(tiingo_metadata.requests, "get") as request_mock:
        request_mock.return_value.json.return_value = make_meta_payload()
        request_mock.return_value.raise_for_status.return_value = None
        source.fetch_classification("AAPL")
    assert request_mock.call_args.kwargs["params"]["tickers"] == "aapl"


def test_fetch_classification_sends_token_in_authorization_header(source):
    with patch.object(tiingo_metadata.requests, "get") as request_mock:
        request_mock.return_value.json.return_value = make_meta_payload()
        request_mock.return_value.raise_for_status.return_value = None
        source.fetch_classification("AAPL")
    headers = request_mock.call_args.kwargs["headers"]
    assert headers == {"Authorization": "Token test-key"}
    assert "token" not in request_mock.call_args.kwargs["params"]


def test_fetch_ohlcv_uppercases_symbol_in_output(source):
    with patch.object(source._client, "get_ticker_price", return_value=make_prices_rows()):
        df = source.fetch(Dataset.OHLCV, "aapl", "2024-01-01", "2024-01-31").collect()
    assert df["symbol"].unique().to_list() == ["AAPL"]


def test_fetch_ohlcv_empty_raises(source):
    with (
        patch.object(source._client, "get_ticker_price", return_value=[]),
        pytest.raises(ValueError, match="No OHLCV data"),
    ):
        source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-01-31")


def test_fetch_dividends_returns_only_div_events(source):
    with patch.object(source._client, "get_ticker_price", return_value=make_prices_rows()):
        df = source.fetch(Dataset.DIVIDENDS, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert df.schema["dividend"] == pl.Float32
    assert df["date"].to_list() == [20240103]


def test_fetch_dividends_filters_date_range(source):
    with patch.object(source._client, "get_ticker_price", return_value=make_prices_rows()):
        df = source.fetch(Dataset.DIVIDENDS, "AAPL", "2024-01-04", "2024-01-31").collect()
    # The single divCash row falls on 2024-01-03, outside [2024-01-04, 2024-01-31]
    assert df.height == 0


def test_fetch_splits_lowercases_symbol_for_api(source):
    with patch.object(
        source._client, "get_ticker_price", return_value=make_prices_rows_with_split()
    ) as mock:
        source.fetch(Dataset.SPLITS, "AAPL", "2020-08-01", "2020-09-30").collect()
    assert mock.call_args.args[0] == "aapl"


def test_fetch_splits_returns_normalized_frame(source):
    with patch.object(
        source._client, "get_ticker_price", return_value=make_prices_rows_with_split()
    ):
        df = source.fetch(Dataset.SPLITS, "AAPL", "2020-08-01", "2020-09-30").collect()
    assert df.schema["date"] == pl.Int32
    assert df.schema["split_factor"] == pl.Float32
    assert df["date"].to_list() == [20200831]
    assert df["split_factor"].to_list() == [pytest.approx(4.0)]


def test_fetch_splits_filters_date_range(source):
    with patch.object(
        source._client, "get_ticker_price", return_value=make_prices_rows_with_split()
    ):
        df = source.fetch(Dataset.SPLITS, "AAPL", "2020-09-01", "2020-09-30").collect()
    # The split event falls on 2020-08-31, outside [2020-09-01, 2020-09-30]
    assert df.height == 0


def test_fetch_splits_returns_empty_when_no_splits_in_window(source):
    with patch.object(source._client, "get_ticker_price", return_value=make_prices_rows()):
        df = source.fetch(Dataset.SPLITS, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert df.height == 0


def test_fetch_fundamentals_daily_lowercases_symbol_for_api(source):
    with patch.object(
        source._client, "get_fundamentals_daily", return_value=make_fundamentals_rows()
    ) as mock:
        source.fetch(Dataset.FUNDAMENTALS_DAILY, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert mock.call_args.args[0] == "aapl"


def test_fetch_fundamentals_daily_returns_normalized_frame(source):
    with patch.object(
        source._client, "get_fundamentals_daily", return_value=make_fundamentals_rows()
    ):
        df = source.fetch(Dataset.FUNDAMENTALS_DAILY, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert df.schema["date"] == pl.Int32
    assert df.schema["market_cap"] == pl.Int64
    assert df.schema["enterprise_val"] == pl.Int64
    assert df.schema["pe_ratio"] == pl.Float32
    assert df.schema["pb_ratio"] == pl.Float32
    assert df.schema["trailing_peg_1y"] == pl.Float32
    assert df["market_cap"].to_list() == [1_500_000_000_000, 1_650_000_000_000]


def test_fetch_fundamentals_daily_uppercases_symbol_in_output(source):
    with patch.object(
        source._client, "get_fundamentals_daily", return_value=make_fundamentals_rows()
    ):
        df = source.fetch(Dataset.FUNDAMENTALS_DAILY, "aapl", "2024-01-01", "2024-01-31").collect()
    assert df["symbol"].unique().to_list() == ["AAPL"]


def test_fetch_fundamentals_daily_empty_raises(source):
    with (
        patch.object(source._client, "get_fundamentals_daily", return_value=[]),
        pytest.raises(ValueError, match="No fundamentals"),
    ):
        source.fetch(Dataset.FUNDAMENTALS_DAILY, "AAPL", "2024-01-01", "2024-01-31")


def _statements_side_effect(
    *args: Any,
    **kwargs: Any,
) -> list[dict[str, Any]]:
    """Mock side_effect: return as-reported or adjusted payload depending on
    the asReported kwarg the source sends."""
    if kwargs.get("asReported") is True:
        return make_statements_rows_as_reported()
    return make_statements_rows_adjusted()


def test_fetch_statements_lowercases_symbol_for_api(source):
    with patch.object(
        source._client,
        "get_fundamentals_statements",
        side_effect=_statements_side_effect,
    ) as mock:
        source.fetch(Dataset.FUNDAMENTALS_STATEMENTS, "AAPL", "2022-01-01", "2024-12-31").collect()
    # Both calls (asReported=True and asReported=False) lowercase the symbol
    assert all(call.args[0] == "aapl" for call in mock.call_args_list)


def test_fetch_statements_calls_both_as_reported_variants(source):
    with patch.object(
        source._client,
        "get_fundamentals_statements",
        side_effect=_statements_side_effect,
    ) as mock:
        source.fetch(Dataset.FUNDAMENTALS_STATEMENTS, "AAPL", "2022-01-01", "2024-12-31").collect()
    # The dataset always fetches both variants — one call with asReported=True
    # (point-in-time) and one with asReported=False (latest restated).
    as_reported_flags = [call.kwargs["asReported"] for call in mock.call_args_list]
    assert sorted(as_reported_flags) == [False, True]


def test_fetch_statements_returns_normalized_frame(source):
    with patch.object(
        source._client,
        "get_fundamentals_statements",
        side_effect=_statements_side_effect,
    ):
        df = source.fetch(
            Dataset.FUNDAMENTALS_STATEMENTS, "AAPL", "2022-01-01", "2024-12-31"
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
    assert df.schema["roe_as_reported"] == pl.Float32
    assert df.height == 2


def test_fetch_statements_empty_raises(source):
    with (
        patch.object(source._client, "get_fundamentals_statements", return_value=[]),
        pytest.raises(ValueError, match="No statements data"),
    ):
        source.fetch(Dataset.FUNDAMENTALS_STATEMENTS, "AAPL", "2022-01-01", "2024-12-31")


def test_fetch_shares_returns_normalized_frame(source):
    with (
        patch.object(source._client, "get_ticker_price", return_value=make_prices_rows()),
        patch.object(
            source._client, "get_fundamentals_daily", return_value=make_fundamentals_rows()
        ),
    ):
        df = source.fetch(Dataset.SHARES, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert df.schema["date"] == pl.Int32
    assert df.schema["shares"] == pl.Int64
    assert df["shares"].to_list() == [15_000_000_000, 15_000_000_000]


def test_fetch_shares_empty_fundamentals_raises(source):
    with (
        patch.object(source._client, "get_ticker_price", return_value=make_prices_rows()),
        patch.object(source._client, "get_fundamentals_daily", return_value=[]),
        pytest.raises(ValueError, match="No fundamentals"),
    ):
        source.fetch(Dataset.SHARES, "AAPL", "2024-01-01", "2024-01-31")


def test_fetch_metadata_calls_metadata_fundamentals_and_close(source):
    with (
        patch.object(source._client, "get_ticker_metadata", return_value=make_metadata_dict()),
        patch(
            "marketgoblin.sources.tiingo.fetch_latest_fundamentals",
            return_value=make_fundamentals_rows()[-1],
        ),
        patch("marketgoblin.sources.tiingo.fetch_latest_close", return_value=110.0),
    ):
        meta = source.fetch_metadata("AAPL")
    assert isinstance(meta, TickerMetadata)
    assert meta.symbol == "AAPL"
    assert meta.name == "Apple Inc."
    assert meta.market_cap == 1_650_000_000_000
    assert meta.shares_outstanding == 15_000_000_000
    assert meta.is_fast is False


def test_fetch_metadata_fast_skips_fundamentals_and_close_calls(source):
    with (
        patch.object(source._client, "get_ticker_metadata", return_value=make_metadata_dict()),
        patch("marketgoblin.sources.tiingo.fetch_latest_fundamentals") as fundamentals_mock,
        patch("marketgoblin.sources.tiingo.fetch_latest_close") as close_mock,
    ):
        meta = source.fetch_metadata("AAPL", fast=True)
    fundamentals_mock.assert_not_called()
    close_mock.assert_not_called()
    assert meta.is_fast is True
    assert meta.market_cap is None
    assert meta.shares_outstanding is None


def test_fetch_classification_uses_meta_endpoint(source):
    with patch(
        "marketgoblin.sources.tiingo.fetch_fundamentals_meta",
        return_value=make_meta_payload()[0],
    ):
        classification = source.fetch_classification("AAPL")
    assert isinstance(classification, Classification)
    assert classification.sector is not None
    assert classification.sector.key == "information-technology"
    assert classification.industry is not None
    assert classification.industry.sector_key == "information-technology"


def test_fetch_classification_returns_empty_profiles_when_meta_absent(source):
    with patch("marketgoblin.sources.tiingo.fetch_fundamentals_meta", return_value={}):
        classification = source.fetch_classification("UNKNOWN")
    assert classification.sector is None
    assert classification.industry is None


def test_retry_on_transient_error(source):
    call_count = {"n": 0}

    def flaky(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] < 2:
            raise RestClientError("transient")
        return make_prices_rows()

    with (
        patch.object(source._client, "get_ticker_price", side_effect=flaky),
        patch("marketgoblin.sources.tiingo.time.sleep"),
    ):
        df = source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert df.height == 4
    assert call_count["n"] == 2


def test_value_error_does_not_retry(source):
    call_count = {"n": 0}

    def empty(*args, **kwargs):
        call_count["n"] += 1
        return []

    with (
        patch.object(source._client, "get_ticker_price", side_effect=empty),
        pytest.raises(ValueError),
    ):
        source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-01-31")
    assert call_count["n"] == 1


def test_retry_on_transient_error_in_shares_path(source):
    # SHARES makes two API calls; transient failure on the second must still
    # be retried by the orchestrator-level _retry_fetch wrapper.
    fundamentals_calls = {"n": 0}

    def flaky_fundamentals(*args, **kwargs):
        fundamentals_calls["n"] += 1
        if fundamentals_calls["n"] < 2:
            raise RestClientError("transient")
        return make_fundamentals_rows()

    with (
        patch.object(source._client, "get_ticker_price", return_value=make_prices_rows()),
        patch.object(source._client, "get_fundamentals_daily", side_effect=flaky_fundamentals),
        patch("marketgoblin.sources.tiingo.time.sleep"),
    ):
        df = source.fetch(Dataset.SHARES, "AAPL", "2024-01-01", "2024-01-31").collect()
    assert df.height == 2
    assert fundamentals_calls["n"] == 2


def test_retry_on_transient_error_in_classification_path(source):
    # fetch_classification uses requests.get directly, not the TiingoClient —
    # the retry wrapper has to handle that path too.
    call_count = {"n": 0}

    def flaky_meta(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] < 2:
            raise requests.HTTPError("transient")
        return make_meta_payload()[0]

    with (
        patch("marketgoblin.sources.tiingo.fetch_fundamentals_meta", side_effect=flaky_meta),
        patch("marketgoblin.sources.tiingo.time.sleep"),
    ):
        classification = source.fetch_classification("AAPL")
    assert classification.sector is not None
    assert call_count["n"] == 2


def test_fetch_unsupported_dataset_raises(source):
    # Force an unsupported dataset by clearing the dispatch table — same shape
    # as the YahooSource test, ensures the BaseSource error path stays wired.
    source._dispatch = {Dataset.OHLCV: source._fetch_ohlcv}
    with pytest.raises(ValueError, match="does not support dataset"):
        source.fetch(Dataset.SHARES, "AAPL", "2024-01-01", "2024-01-31")


def test_http_error_propagates_after_retries(source):
    with (
        patch.object(
            source._client,
            "get_ticker_price",
            side_effect=requests.HTTPError("upstream down"),
        ),
        patch("marketgoblin.sources.tiingo.time.sleep"),
        pytest.raises(requests.HTTPError),
    ):
        source.fetch(Dataset.OHLCV, "AAPL", "2024-01-01", "2024-01-31")
