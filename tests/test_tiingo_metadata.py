# Unit tests for the Tiingo metadata + classification adapters and the slug
# helper. The /fundamentals/meta REST call is exercised by patching the
# `requests` bound in the metadata submodule.

from unittest.mock import MagicMock, patch

import pytest

from marketgoblin.sources._tiingo_parsing import metadata as tiingo_metadata
from marketgoblin.sources._tiingo_parsing.common import slugify
from marketgoblin.sources._tiingo_parsing.metadata import (
    build_tiingo_classification,
    build_tiingo_metadata,
    fetch_fundamentals_meta,
    fetch_latest_close,
    fetch_latest_fundamentals,
)
from tests._tiingo_data import make_fundamentals_rows, make_meta_payload, make_metadata_dict


def test_build_tiingo_metadata_full_merges_meta_and_fundamentals():
    meta = build_tiingo_metadata(
        symbol="AAPL",
        provider="tiingo",
        meta=make_metadata_dict(),
        fundamentals_row=make_fundamentals_rows()[-1],
        latest_close=110.0,
        is_fast=False,
    )
    assert meta.symbol == "AAPL"
    assert meta.name == "Apple Inc."
    assert meta.exchange == "NASDAQ"
    assert meta.business_summary == "Apple makes consumer electronics."
    assert meta.first_trade_date == "1980-12-12"
    assert meta.market_cap == 1_650_000_000_000
    # 1.65e12 / 110 = 1.5e10
    assert meta.shares_outstanding == 15_000_000_000
    assert meta.trailing_pe == pytest.approx(32.6)
    assert meta.currency == "USD"
    assert meta.provider == "tiingo"
    assert meta.is_fast is False


def test_build_tiingo_metadata_fast_skips_fundamentals_fields():
    meta = build_tiingo_metadata(
        symbol="AAPL",
        provider="tiingo",
        meta=make_metadata_dict(),
        fundamentals_row=None,
        latest_close=None,
        is_fast=True,
    )
    assert meta.is_fast is True
    assert meta.name == "Apple Inc."
    assert meta.market_cap is None
    assert meta.shares_outstanding is None
    assert meta.trailing_pe is None


def test_build_tiingo_metadata_shares_outstanding_none_when_close_missing():
    meta = build_tiingo_metadata(
        symbol="AAPL",
        provider="tiingo",
        meta=make_metadata_dict(),
        fundamentals_row=make_fundamentals_rows()[-1],
        latest_close=None,
        is_fast=False,
    )
    assert meta.market_cap == 1_650_000_000_000
    assert meta.shares_outstanding is None


def test_build_tiingo_classification_populates_sector_and_industry():
    classification = build_tiingo_classification("AAPL", "tiingo", make_meta_payload()[0])
    assert classification.symbol == "AAPL"
    assert classification.provider == "tiingo"
    assert classification.sector is not None
    assert classification.sector.key == "information-technology"
    assert classification.sector.name == "Information Technology"
    assert classification.industry is not None
    assert classification.industry.key == "technology-hardware-storage-peripherals"
    assert classification.industry.sector_key == "information-technology"
    assert classification.industry.sector_name == "Information Technology"


def test_build_tiingo_classification_handles_missing_keys():
    classification = build_tiingo_classification("BTC-USD", "tiingo", {})
    assert classification.symbol == "BTC-USD"
    assert classification.sector is None
    assert classification.industry is None


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("Information Technology", "information-technology"),
        ("Health Care", "health-care"),
        ("Energy", "energy"),
        ("Consumer Discretionary & Staples", "consumer-discretionary-staples"),
        ("  Trim Me  ", "trim-me"),
    ],
)
def test_slugify_examples(raw, expected):
    assert slugify(raw) == expected


def test_fetch_fundamentals_meta_returns_first_row():
    fake_response = type(
        "R",
        (),
        {"json": lambda self: make_meta_payload(), "raise_for_status": lambda self: None},
    )()
    with patch.object(tiingo_metadata.requests, "get", return_value=fake_response):
        row = fetch_fundamentals_meta("AAPL", api_key="k")
    assert row["sector"] == "Information Technology"


def test_fetch_fundamentals_meta_returns_empty_on_empty_payload():
    fake_response = type(
        "R",
        (),
        {"json": lambda self: [], "raise_for_status": lambda self: None},
    )()
    with patch.object(tiingo_metadata.requests, "get", return_value=fake_response):
        row = fetch_fundamentals_meta("UNKNOWN", api_key="k")
    assert row == {}


def test_fetch_latest_close_lowercases_symbol_for_api():
    fake_client = MagicMock()
    fake_client.get_ticker_price.return_value = [{"close": 100.0}]
    fetch_latest_close(fake_client, "AAPL")
    assert fake_client.get_ticker_price.call_args.args[0] == "aapl"


def test_fetch_latest_fundamentals_lowercases_symbol_for_api():
    fake_client = MagicMock()
    fake_client.get_fundamentals_daily.return_value = [{"marketCap": 1}]
    fetch_latest_fundamentals(fake_client, "AAPL")
    assert fake_client.get_fundamentals_daily.call_args.args[0] == "aapl"
