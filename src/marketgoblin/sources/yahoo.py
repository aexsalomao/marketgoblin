# YahooSource — yfinance-backed provider for OHLCV, shares-outstanding,
# dividends, ticker metadata, and sector/industry classification. Each
# dataset has its own _fetch_* method; transient failures retry with
# exponential backoff via a shared _retry_fetch helper. Pure parsing /
# yfinance-adapter helpers live in _yahoo_parsing.

import logging
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any, TypeVar

import polars as pl
import yfinance as yf

from marketgoblin._normalize import (
    normalize_dividends,
    normalize_ohlcv,
    normalize_shares,
)
from marketgoblin.classification import Classification
from marketgoblin.datasets import Dataset
from marketgoblin.sources._yahoo_parsing import (
    build_ticker_metadata,
    fetch_industry_profile,
    fetch_sector_profile,
    first_present,
    safe_dict,
    safe_isin,
)
from marketgoblin.sources.base import BaseSource, Fetcher
from marketgoblin.ticker_metadata import TickerMetadata

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3
_RETRY_DELAYS = [1.0, 2.0]  # seconds between attempts (len == _MAX_RETRIES - 1)

_T = TypeVar("_T")


class YahooSource(BaseSource):
    """Yahoo Finance source. Supports OHLCV, shares, and dividends.

    OHLCV is returned as a tidy stacked frame: every trading day appears twice,
    once with ``is_adjusted=True`` (split/dividend adjusted) and once with
    ``is_adjusted=False`` (raw). Callers filter by ``is_adjusted`` downstream.

    A single ``yf.Ticker.history(auto_adjust=False)`` call returns raw OHLCV plus
    ``Adj Close``; adjusted Open/High/Low are derived via the ``Adj Close / Close``
    ratio (volume is identical across both modes). This matches yfinance's own
    ``auto_adjust=True`` output exactly while halving the network load.

    Shares are sparse (corporate-action driven, irregular cadence) — the raw
    series is deduplicated to one row per day (last value wins) before storage.

    Dividends are event-driven (typically quarterly); the full series from
    ``yf.Ticker.dividends`` is filtered to the requested date range.
    """

    name = "yahoo"

    def _build_dispatch(self) -> dict[Dataset, Fetcher]:
        return {
            Dataset.OHLCV: self._fetch_ohlcv,
            Dataset.SHARES: self._fetch_shares,
            Dataset.DIVIDENDS: self._fetch_dividends,
        }

    def _fetch_ohlcv(self, symbol: str, start: str, end: str) -> pl.LazyFrame:
        def do_fetch() -> pl.LazyFrame:
            raw = yf.Ticker(symbol).history(start=start, end=end, auto_adjust=False, actions=False)
            if raw.empty:
                raise ValueError(f"No OHLCV data returned for {symbol} ({start} to {end})")

            base = (
                pl.from_pandas(raw.reset_index())
                .rename(lambda col: col.lower())
                .rename({"adj close": "adj_close"})
                .with_columns(pl.col("date").dt.date())
            )
            ratio = pl.col("adj_close") / pl.col("close")
            symbol_upper = symbol.upper()

            unadjusted = base.select(
                "date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                pl.lit(symbol_upper).alias("symbol"),
                pl.lit(False).alias("is_adjusted"),
            )
            adjusted = base.select(
                "date",
                (pl.col("open") * ratio).alias("open"),
                (pl.col("high") * ratio).alias("high"),
                (pl.col("low") * ratio).alias("low"),
                pl.col("adj_close").alias("close"),
                "volume",
                pl.lit(symbol_upper).alias("symbol"),
                pl.lit(True).alias("is_adjusted"),
            )

            return (
                pl.concat([adjusted, unadjusted])
                .sort(["date", "is_adjusted"])
                .lazy()
                .pipe(normalize_ohlcv)
            )

        return self._retry_fetch(do_fetch, symbol)

    def _fetch_shares(self, symbol: str, start: str, end: str) -> pl.LazyFrame:
        def do_fetch() -> pl.LazyFrame:
            raw = yf.Ticker(symbol).get_shares_full(start=start, end=end)
            if raw is None or raw.empty:
                raise ValueError(f"No shares data returned for {symbol} ({start} to {end})")

            df = raw.reset_index()
            df.columns = ["date", "shares"]
            return (
                pl.from_pandas(df)
                # yfinance currently returns TZ-aware timestamps from get_shares_full;
                # strip TZ before extracting date. If yfinance ever returns naive
                # datetimes, replace_time_zone(None) on a naive col will error and
                # this branch needs a dtype check.
                .with_columns(
                    pl.col("date").dt.replace_time_zone(None).dt.date(),
                    pl.lit(symbol.upper()).alias("symbol"),
                )
                # yfinance can return multiple intraday entries for the same day;
                # keep the last reported value per day (most recent estimate).
                .group_by("date", "symbol", maintain_order=True)
                .agg(pl.col("shares").last())
                .sort("date")
                .lazy()
                .pipe(normalize_shares)
            )

        return self._retry_fetch(do_fetch, symbol)

    def _fetch_dividends(self, symbol: str, start: str, end: str) -> pl.LazyFrame:
        start_int = int(start.replace("-", ""))
        end_int = int(end.replace("-", ""))

        def do_fetch() -> pl.LazyFrame:
            raw = yf.Ticker(symbol).dividends
            if raw is None or raw.empty:
                raise ValueError(f"No dividend data returned for {symbol}")

            df = raw.reset_index()
            df.columns = ["date", "dividend"]
            return (
                pl.from_pandas(df)
                .with_columns(
                    pl.col("date").dt.replace_time_zone(None).dt.date(),
                    pl.lit(symbol.upper()).alias("symbol"),
                )
                .sort("date")
                .lazy()
                .pipe(normalize_dividends)
                .filter(pl.col("date").is_between(start_int, end_int))
            )

        return self._retry_fetch(do_fetch, symbol)

    def fetch_metadata(self, symbol: str, *, fast: bool = False) -> TickerMetadata:
        """Build a unified TickerMetadata from yfinance's fragmented endpoints.

        yfinance exposes overlapping metadata surfaces: ``fast_info`` (cheap,
        cached quote fields), ``history_metadata`` (timezone, first trade date),
        ``isin()`` (identifier lookup), and ``info`` (scraped profile — slow).
        This method merges all of them into one :class:`TickerMetadata`.

        Args:
            symbol: Ticker symbol (case-insensitive; normalized upper-case).
            fast: If True, skip the scraped ``.info`` call and ``isin()``. Uses
                only ``fast_info`` + ``history_metadata``. Cheap but sparse.
        """

        def do_fetch() -> TickerMetadata:
            ticker = yf.Ticker(symbol)
            fast_info = safe_dict(getattr(ticker, "fast_info", None))
            history_meta = safe_dict(getattr(ticker, "history_metadata", None))

            if fast:
                info: dict[str, Any] = {}
                isin: str | None = None
            else:
                info = safe_dict(getattr(ticker, "info", None))
                isin = safe_isin(ticker)

            return build_ticker_metadata(
                symbol=symbol.upper(),
                provider=self.name,
                fast_info=fast_info,
                history_meta=history_meta,
                info=info,
                isin=isin,
                is_fast=fast,
            )

        return self._retry_fetch(do_fetch, symbol)

    def fetch_classification(self, symbol: str) -> Classification:
        """Look up sector + industry profiles for a ticker via ``yf.Sector`` / ``yf.Industry``.

        Two step lookup: ``ticker.info`` gives the sector/industry *keys*
        (slugs); those keys feed ``yf.Sector(key)`` and ``yf.Industry(key)`` to
        fetch constituent data (top companies, representative ETFs, ...). Either
        profile is ``None`` if the key is missing on the ticker (e.g. ETFs,
        crypto).
        """

        def do_fetch() -> Classification:
            info = safe_dict(getattr(yf.Ticker(symbol), "info", None))
            sector_key = first_present(info, "sectorKey")
            industry_key = first_present(info, "industryKey")

            # Sector and industry lookups are independent upstream calls; run
            # them concurrently to halve wall-clock when both keys are present.
            with ThreadPoolExecutor(max_workers=2) as pool:
                sector_future = (
                    pool.submit(fetch_sector_profile, sector_key) if sector_key else None
                )
                industry_future = (
                    pool.submit(fetch_industry_profile, industry_key) if industry_key else None
                )
                sector = sector_future.result() if sector_future else None
                industry = industry_future.result() if industry_future else None

            return Classification(
                symbol=symbol.upper(),
                sector=sector,
                industry=industry,
                provider=self.name,
                fetched_at=datetime.now(tz=UTC).isoformat(timespec="seconds"),
            )

        return self._retry_fetch(do_fetch, symbol)

    def _retry_fetch(
        self,
        fetch_fn: Callable[[], _T],
        symbol: str,
    ) -> _T:
        """Retry fetch_fn on transient errors with exponential backoff. ValueError propagates."""
        last_exc: Exception = RuntimeError("unreachable")
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                return fetch_fn()
            except ValueError:
                raise  # domain validation — don't retry
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "fetch attempt %d/%d failed | symbol=%s error=%s",
                    attempt,
                    _MAX_RETRIES,
                    symbol,
                    exc,
                )
                if attempt < _MAX_RETRIES:
                    time.sleep(_RETRY_DELAYS[attempt - 1])

        logger.error(
            "all %d fetch attempts failed | symbol=%s error=%s",
            _MAX_RETRIES,
            symbol,
            last_exc,
        )
        raise last_exc
