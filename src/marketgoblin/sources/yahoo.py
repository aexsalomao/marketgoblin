# YahooSource — OHLCV + shares-outstanding + dividends provider backed by
# yfinance. Each dataset has its own _fetch_* method; transient failures retry
# with exponential backoff via a shared _retry_fetch helper.

import logging
import time
from collections.abc import Callable

import polars as pl
import yfinance as yf

from marketgoblin._normalize import (
    normalize_dividends,
    normalize_ohlcv,
    normalize_shares,
)
from marketgoblin.datasets import Dataset
from marketgoblin.sources.base import BaseSource, Fetcher

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3
_RETRY_DELAYS = [1.0, 2.0]  # seconds between attempts (len == _MAX_RETRIES - 1)


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

    def _retry_fetch(
        self,
        fetch_fn: Callable[[], pl.LazyFrame],
        symbol: str,
    ) -> pl.LazyFrame:
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
