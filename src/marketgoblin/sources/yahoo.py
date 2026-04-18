# YahooSource — OHLCV + shares-outstanding provider backed by yfinance.
# Each dataset has its own _fetch_* method; transient failures retry with
# exponential backoff via a shared _retry_fetch helper.

import logging
import time
from collections.abc import Callable

import polars as pl
import yfinance as yf

from marketgoblin._normalize import normalize_ohlcv, normalize_shares
from marketgoblin.datasets import Dataset
from marketgoblin.sources.base import BaseSource, Fetcher

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3
_RETRY_DELAYS = [1.0, 2.0]  # seconds between attempts (len == _MAX_RETRIES - 1)


class YahooSource(BaseSource):
    """Yahoo Finance source. Supports OHLCV (yf.history) and shares (yf.get_shares_full).

    OHLCV uses ``auto_adjust`` to control split/dividend adjustment.
    Shares are sparse (corporate-action driven, irregular cadence) — the raw
    series is deduplicated to one row per day (last value wins) before storage.
    """

    name = "yahoo"

    def _build_dispatch(self) -> dict[Dataset, Fetcher]:
        return {
            Dataset.OHLCV: self._fetch_ohlcv,
            Dataset.SHARES: self._fetch_shares,
        }

    def _fetch_ohlcv(
        self, symbol: str, start: str, end: str, adjusted: bool = True
    ) -> pl.LazyFrame:
        def do_fetch() -> pl.LazyFrame:
            raw = yf.Ticker(symbol).history(start=start, end=end, auto_adjust=adjusted)
            if raw.empty:
                raise ValueError(f"No OHLCV data returned for {symbol} ({start} to {end})")
            return (
                pl.from_pandas(raw.reset_index())
                .rename(lambda col: col.lower())
                .select(["date", "open", "high", "low", "close", "volume"])
                .with_columns(
                    pl.col("date").dt.date(),
                    pl.lit(symbol.upper()).alias("symbol"),
                )
                .lazy()
                .pipe(normalize_ohlcv)
            )

        return self._retry_fetch(do_fetch, symbol)

    def _fetch_shares(
        self, symbol: str, start: str, end: str, adjusted: bool = True
    ) -> pl.LazyFrame:
        # `adjusted` is OHLCV-specific and ignored here.
        del adjusted

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
