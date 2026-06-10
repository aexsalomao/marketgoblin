# TiingoSource — Tiingo-backed provider for OHLCV, shares-outstanding,
# dividends, ticker metadata, and sector/industry classification.
# Tiingo's daily prices endpoint returns raw + adjusted OHLCV plus divCash in
# one call, so OHLCV and DIVIDENDS share the underlying request shape.
# Pure parsing / Tiingo-adapter helpers live in _tiingo_parsing.

import os
from typing import Any

import polars as pl
from tiingo import TiingoClient

from marketgoblin._normalize import (
    normalize_dividends,
    normalize_fundamentals_daily,
    normalize_ohlcv,
    normalize_shares,
    normalize_splits,
    normalize_statements,
)
from marketgoblin.classification import Classification
from marketgoblin.datasets import Dataset
from marketgoblin.sources._tiingo_parsing import (
    build_tiingo_classification,
    build_tiingo_metadata,
    derive_shares_from_marketcap,
    fetch_fundamentals_meta,
    fetch_latest_close,
    fetch_latest_fundamentals,
    fundamentals_daily_rows_to_lf,
    prices_rows_to_dividends,
    prices_rows_to_splits,
    prices_rows_to_stacked_ohlcv,
    statements_rows_to_lf,
)
from marketgoblin.sources.base import BaseSource, Fetcher
from marketgoblin.ticker_metadata import TickerMetadata


class TiingoSource(BaseSource):
    """Tiingo source. Supports OHLCV, shares, and dividends.

    OHLCV and dividends are served from a single endpoint
    (``/tiingo/daily/{ticker}/prices``); each row carries raw OHLCV, adjusted
    OHLCV (``adjOpen`` / … / ``adjVolume``) and a ``divCash`` field. We split
    that into the project's stacked tidy frame (one row per ``(date,
    is_adjusted)``) for OHLCV and into a separate event-only frame for
    dividends — the dispatch layer keeps the two flows independent even though
    they share an upstream call shape.

    Shares-outstanding is derived, not fetched directly: Tiingo's daily
    Fundamentals endpoint exposes ``marketCap`` but no shares field, so
    ``_fetch_shares`` joins ``client.get_ticker_price`` and
    ``client.get_fundamentals_daily`` on date and computes
    ``shares = round(marketCap / close)``. Both endpoints require a paid
    subscription. Sector / industry classification is fetched directly from
    ``/tiingo/fundamentals/meta`` (also paid) since the official Python client
    does not wrap that endpoint.
    """

    name = "tiingo"

    def __init__(self, api_key: str | None = None, **kwargs: Any) -> None:
        # Resolve from TIINGO_API_KEY before super() stores it. Without this,
        # callers who rely on the env var (the pattern the tiingo client itself
        # documents) end up with self.api_key=None, which silently breaks
        # fetch_classification — it passes self.api_key through to the
        # /fundamentals/meta endpoint as a bearer token.
        api_key = api_key or os.environ.get("TIINGO_API_KEY")
        super().__init__(api_key, **kwargs)
        config = {"api_key": api_key} if api_key else None
        self._client = TiingoClient(config)

    def _build_dispatch(self) -> dict[Dataset, Fetcher]:
        return {
            Dataset.OHLCV: self._fetch_ohlcv,
            Dataset.SHARES: self._fetch_shares,
            Dataset.DIVIDENDS: self._fetch_dividends,
            Dataset.SPLITS: self._fetch_splits,
            Dataset.FUNDAMENTALS_DAILY: self._fetch_fundamentals_daily,
            Dataset.FUNDAMENTALS_STATEMENTS: self._fetch_fundamentals_statements,
        }

    def _fetch_ohlcv(self, symbol: str, start: str, end: str) -> pl.LazyFrame:
        def do_fetch() -> pl.LazyFrame:
            rows = self._client.get_ticker_price(
                symbol.lower(),
                startDate=start,
                endDate=end,
                fmt="json",
                frequency="daily",
            )
            return prices_rows_to_stacked_ohlcv(rows, symbol).pipe(normalize_ohlcv)

        return self._retry_fetch(do_fetch, symbol)

    def _fetch_shares(self, symbol: str, start: str, end: str) -> pl.LazyFrame:
        def do_fetch() -> pl.LazyFrame:
            ticker = symbol.lower()
            prices = self._client.get_ticker_price(
                ticker, startDate=start, endDate=end, fmt="json", frequency="daily"
            )
            fundamentals = self._client.get_fundamentals_daily(
                ticker, startDate=start, endDate=end, fmt="json"
            )
            return derive_shares_from_marketcap(prices, fundamentals, symbol).pipe(normalize_shares)

        return self._retry_fetch(do_fetch, symbol)

    def _fetch_dividends(self, symbol: str, start: str, end: str) -> pl.LazyFrame:
        start_int = int(start.replace("-", ""))
        end_int = int(end.replace("-", ""))

        def do_fetch() -> pl.LazyFrame:
            rows = self._client.get_ticker_price(
                symbol.lower(),
                startDate=start,
                endDate=end,
                fmt="json",
                frequency="daily",
            )
            return (
                prices_rows_to_dividends(rows, symbol)
                .pipe(normalize_dividends)
                .filter(pl.col("date").is_between(start_int, end_int))
            )

        return self._retry_fetch(do_fetch, symbol)

    def _fetch_splits(self, symbol: str, start: str, end: str) -> pl.LazyFrame:
        start_int = int(start.replace("-", ""))
        end_int = int(end.replace("-", ""))

        def do_fetch() -> pl.LazyFrame:
            rows = self._client.get_ticker_price(
                symbol.lower(),
                startDate=start,
                endDate=end,
                fmt="json",
                frequency="daily",
            )
            return (
                prices_rows_to_splits(rows, symbol)
                .pipe(normalize_splits)
                .filter(pl.col("date").is_between(start_int, end_int))
            )

        return self._retry_fetch(do_fetch, symbol)

    def _fetch_fundamentals_daily(self, symbol: str, start: str, end: str) -> pl.LazyFrame:
        def do_fetch() -> pl.LazyFrame:
            rows = self._client.get_fundamentals_daily(
                symbol.lower(),
                startDate=start,
                endDate=end,
                fmt="json",
            )
            return fundamentals_daily_rows_to_lf(rows, symbol).pipe(normalize_fundamentals_daily)

        return self._retry_fetch(do_fetch, symbol)

    def _fetch_fundamentals_statements(self, symbol: str, start: str, end: str) -> pl.LazyFrame:
        # Tiingo's statements endpoint exposes asReported as a request flag,
        # not as separate columns in one response — to ship both variants in
        # the same on-disk slice we issue both calls and merge in the parser.
        # Each call is wrapped in its own _retry_fetch so a transient failure
        # on the second call doesn't replay the first (network-wise harmless,
        # but doubles billed quota and obscures rate-limit signals).
        def fetch_as_reported() -> list[dict[str, Any]]:
            return self._client.get_fundamentals_statements(  # type: ignore[no-any-return]
                symbol.lower(),
                startDate=start,
                endDate=end,
                asReported=True,
                fmt="json",
            )

        def fetch_adjusted() -> list[dict[str, Any]]:
            return self._client.get_fundamentals_statements(  # type: ignore[no-any-return]
                symbol.lower(),
                startDate=start,
                endDate=end,
                asReported=False,
                fmt="json",
            )

        as_reported_rows = self._retry_fetch(fetch_as_reported, symbol)
        adjusted_rows = self._retry_fetch(fetch_adjusted, symbol)
        return statements_rows_to_lf(as_reported_rows, adjusted_rows, symbol).pipe(
            normalize_statements
        )

    def fetch_metadata(self, symbol: str, *, fast: bool = False) -> TickerMetadata:
        """Build a unified TickerMetadata from Tiingo's metadata + daily fundamentals.

        Args:
            symbol: Ticker symbol (case-insensitive; normalized upper-case).
            fast: If True, skip the paid Fundamentals call and return only the
                lightweight ``get_ticker_metadata`` fields (name, exchange,
                description, first-trade date). Cheap but no valuation data.
        """

        def do_fetch() -> TickerMetadata:
            meta = self._client.get_ticker_metadata(symbol.lower())
            if fast:
                fundamentals_row = None
                latest_close = None
            else:
                fundamentals_row = fetch_latest_fundamentals(self._client, symbol)
                latest_close = fetch_latest_close(self._client, symbol)
            return build_tiingo_metadata(
                symbol=symbol.upper(),
                provider=self.name,
                meta=meta,
                fundamentals_row=fundamentals_row,
                latest_close=latest_close,
                is_fast=fast,
            )

        return self._retry_fetch(do_fetch, symbol)

    def fetch_classification(self, symbol: str) -> Classification:
        """Look up sector + industry via Tiingo's ``/fundamentals/meta`` endpoint.

        Either profile is ``None`` if Tiingo returns no sector/industry for the
        ticker (e.g. funds, ADRs without sector classification).
        """

        def do_fetch() -> Classification:
            meta_row = fetch_fundamentals_meta(symbol, self.api_key)
            return build_tiingo_classification(
                symbol=symbol.upper(),
                provider=self.name,
                meta_row=meta_row,
            )

        return self._retry_fetch(do_fetch, symbol)
