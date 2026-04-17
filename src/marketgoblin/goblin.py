# MarketGoblin — public API facade.
# Wraps a data source (Yahoo, CSV, ...) and optional DiskStorage, exposing
# fetch / load / fetch_many with validation, rate limiting, and logging.

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from marketgoblin._normalize import parse_dates as _parse_dates
from marketgoblin.sources.base import BaseSource
from marketgoblin.sources.csv_source import CSVSource
from marketgoblin.sources.yahoo import YahooSource
from marketgoblin.storage.disk import DiskStorage

logger = logging.getLogger(__name__)

_SOURCES: dict[str, type[BaseSource]] = {
    "yahoo": YahooSource,
    "csv": CSVSource,
}

_DATE_FMT = "%Y-%m-%d"


def _validate_dates(start: str, end: str) -> None:
    """Raise ValueError for bad format or start >= end."""
    try:
        s = datetime.strptime(start, _DATE_FMT)
        e = datetime.strptime(end, _DATE_FMT)
    except ValueError:
        raise ValueError(f"Dates must be 'YYYY-MM-DD'. Got start={start!r}, end={end!r}")
    if s >= e:
        raise ValueError(f"start must be before end. Got {start} >= {end}")


class _RateLimiter:
    """Token-bucket rate limiter safe for use across threads."""

    def __init__(self, requests_per_second: float) -> None:
        self._interval = 1.0 / requests_per_second
        self._last: float = 0.0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait = self._interval - (now - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()


class MarketGoblin:
    """Public API for fetching, saving, and loading OHLCV market data.

    Wraps a data source (e.g. Yahoo Finance) and optional disk storage.
    When save_path is provided, fetch() persists data as monthly parquet slices
    and subsequent load() calls read directly from disk without re-downloading.
    """

    def __init__(
        self,
        provider: str,
        api_key: str | None = None,
        save_path: str | Path | None = None,
        **source_kwargs: Any,
    ) -> None:
        if provider not in _SOURCES:
            raise ValueError(f"Unknown provider '{provider}'. Available: {list(_SOURCES)}")

        self._provider = provider
        self._source = _SOURCES[provider](api_key=api_key, **source_kwargs)
        self._storage = DiskStorage(save_path) if save_path else None

    def fetch(
        self,
        symbol: str,
        start: str,
        end: str,
        adjusted: bool = True,
        parse_dates: bool = False,
    ) -> pl.LazyFrame:
        """Download OHLCV data. Saves to disk if save_path was set.

        Args:
            symbol: Ticker symbol e.g. 'AAPL'.
            start: Start date as 'YYYY-MM-DD'.
            end: End date as 'YYYY-MM-DD'.
            adjusted: If True, use split/dividend adjusted prices (default).
            parse_dates: If True, return date as pl.Date instead of int32.

        Raises:
            ValueError: If dates are malformed or start >= end.
        """
        _validate_dates(start, end)
        price_type = "adjusted" if adjusted else "raw"
        logger.info(
            "fetch started | symbol=%s provider=%s range=%s:%s %s",
            symbol,
            self._provider,
            start,
            end,
            price_type,
        )
        t0 = time.perf_counter()

        lf = self._source.fetch(symbol, start, end, adjusted=adjusted)

        if self._storage:
            self._storage.save(self._provider, symbol, lf, adjusted=adjusted)
            lf = self._storage.load(
                self._provider, symbol, start, end, parse_dates, adjusted=adjusted
            )
            elapsed = time.perf_counter() - t0
            logger.info("fetch complete | symbol=%s saved=True elapsed=%.2fs", symbol, elapsed)
            return lf

        df = lf.collect()
        elapsed = time.perf_counter() - t0
        logger.info(
            "fetch complete | symbol=%s rows=%d saved=False elapsed=%.2fs",
            symbol,
            df.height,
            elapsed,
        )
        return _parse_dates(df.lazy()) if parse_dates else df.lazy()

    def load(
        self,
        symbol: str,
        start: str,
        end: str,
        adjusted: bool = True,
        parse_dates: bool = False,
    ) -> pl.LazyFrame:
        """Load previously saved data from disk.

        Args:
            symbol: Ticker symbol e.g. 'AAPL'.
            start: Start date as 'YYYY-MM-DD'.
            end: End date as 'YYYY-MM-DD'.
            adjusted: If True, load from adjusted store (default).
            parse_dates: If True, return date as pl.Date instead of int32.

        Raises:
            ValueError: If dates are malformed or start >= end.
            RuntimeError: If save_path was not set.
        """
        _validate_dates(start, end)
        if not self._storage:
            raise RuntimeError("load() requires save_path to be set.")

        return self._storage.load(
            self._provider, symbol, start, end, parse_dates, adjusted=adjusted
        )

    def fetch_many(
        self,
        symbols: list[str],
        start: str,
        end: str,
        adjusted: bool = True,
        parse_dates: bool = False,
        max_workers: int = 8,
        requests_per_second: float = 2.0,
    ) -> dict[str, pl.LazyFrame]:
        """Download OHLCV data for multiple symbols concurrently.

        Failed symbols are logged and excluded from the result — they never
        crash the batch.

        Args:
            symbols: List of ticker symbols e.g. ['AAPL', 'MSFT'].
            start: Start date as 'YYYY-MM-DD'.
            end: End date as 'YYYY-MM-DD'.
            adjusted: If True, use split/dividend adjusted prices (default).
            parse_dates: If True, return date as pl.Date instead of int32.
            max_workers: Max concurrent threads (default 8).
            requests_per_second: Max requests per second across all threads (default 2.0).

        Raises:
            ValueError: If dates are malformed or start >= end.
        """
        _validate_dates(start, end)
        logger.info("fetch_many started | symbols=%d range=%s:%s", len(symbols), start, end)
        t0 = time.perf_counter()

        limiter = _RateLimiter(requests_per_second)
        results: dict[str, pl.LazyFrame] = {}

        def _rate_limited_fetch(symbol: str) -> pl.LazyFrame:
            limiter.acquire()
            return self.fetch(symbol, start, end, adjusted, parse_dates)

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_rate_limited_fetch, symbol): symbol for symbol in symbols}
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    results[symbol] = future.result()
                except Exception as e:
                    logger.error("fetch failed | symbol=%s error=%s", symbol, e)

        elapsed = time.perf_counter() - t0
        logger.info(
            "fetch_many complete | success=%d failed=%d elapsed=%.2fs",
            len(results),
            len(symbols) - len(results),
            elapsed,
        )
        return results
