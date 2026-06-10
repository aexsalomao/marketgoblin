# AlpacaSource — Alpaca-backed provider for intraday TRADES (tick) data.
# Alpaca has no official client wrapper here; trades come from the Data API v2
# REST endpoint, paginated via next_page_token. Pure row->frame projection lives
# in _alpaca_parsing; this class is a thin orchestrator (HTTP + retry).

import os
from typing import Any

import polars as pl
import requests  # type: ignore[import-untyped]

from marketgoblin._normalize import normalize_trades
from marketgoblin.datasets import Dataset
from marketgoblin.sources._alpaca_parsing import trades_rows_to_lf
from marketgoblin.sources.base import BaseSource, Fetcher

_TRADES_URL = "https://data.alpaca.markets/v2/stocks/{symbol}/trades"
_DEFAULT_FEED = "iex"  # the free Basic-plan equities feed
_PAGE_LIMIT = 10_000  # Alpaca max page size for trades
_REQUEST_TIMEOUT_SECONDS = 30


class AlpacaSource(BaseSource):
    """Alpaca source. Supports intraday TRADES (tick-by-tick executions).

    Credentials resolve from ``ALPACA_API_KEY`` / ``ALPACA_API_SECRET`` (or the
    ``api_key`` / ``api_secret`` constructor args). The free Basic plan serves
    the IEX feed (``feed="iex"``) — real trade-by-trade data, but only IEX
    executions (a few percent of consolidated volume). Pass ``feed="sip"`` with
    a paid plan for the full consolidated tape.
    """

    name = "alpaca"

    def __init__(
        self,
        api_key: str | None = None,
        api_secret: str | None = None,
        feed: str = _DEFAULT_FEED,
        **kwargs: Any,
    ) -> None:
        # Resolve both creds from the environment before super() stores the key,
        # so callers relying on ALPACA_* env vars (the documented pattern) get a
        # usable session. The key lives on self.api_key (via super); the secret —
        # which BaseSource has no slot for — lives on self._api_secret.
        api_key = api_key or os.environ.get("ALPACA_API_KEY")
        self._api_secret = api_secret or os.environ.get("ALPACA_API_SECRET")
        self._feed = feed
        super().__init__(api_key, **kwargs)
        self._session = requests.Session()
        if api_key and self._api_secret:
            self._session.headers.update(
                {"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": self._api_secret}
            )

    def _build_dispatch(self) -> dict[Dataset, Fetcher]:
        return {Dataset.TRADES: self._fetch_trades}

    def _fetch_trades(self, symbol: str, start: str, end: str) -> pl.LazyFrame:
        # Validate config at the boundary, before the retry loop — a missing
        # credential is not a transient error and must not be retried.
        if not self.api_key or not self._api_secret:
            raise RuntimeError(
                "AlpacaSource requires ALPACA_API_KEY and ALPACA_API_SECRET "
                "(free Alpaca Basic credentials)."
            )

        def do_fetch() -> pl.LazyFrame:
            rows = self._download_trades(symbol, start, end)
            return trades_rows_to_lf(rows, symbol).pipe(normalize_trades)

        return self._retry_fetch(do_fetch, symbol)

    def _download_trades(self, symbol: str, start: str, end: str) -> list[dict[str, Any]]:
        """Page through the full [start, end] trades window, following next_page_token.

        Note: every page is accumulated into one in-memory list before a frame is
        built, so a wide window on a liquid name (millions of ticks) materializes
        fully. Keep windows bounded — a single trading day at a time for liquid
        ETFs — until a streaming/chunked path exists.
        """
        url = _TRADES_URL.format(symbol=symbol.upper())
        all_rows: list[dict[str, Any]] = []
        page_token: str | None = None
        while True:
            params: dict[str, str | int] = {
                "start": start,
                "end": end,
                "feed": self._feed,
                "limit": _PAGE_LIMIT,
            }
            if page_token:
                params["page_token"] = page_token

            response = self._session.get(url, params=params, timeout=_REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            payload = response.json()

            all_rows.extend(payload.get("trades") or [])
            page_token = payload.get("next_page_token")
            if not page_token:
                return all_rows
