# MarketGoblin — public API facade.
# Wraps a data source (Yahoo, CSV, ...) and optional DiskStorage, exposing
# fetch / load / fetch_many with validation, rate limiting, and logging.
# Datasets (OHLCV, shares, dividends, ...) are dispatched at the source layer.

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from marketgoblin._normalize import parse_dates as _parse_dates
from marketgoblin.classification import Classification
from marketgoblin.datasets import Dataset
from marketgoblin.sources.base import BaseSource
from marketgoblin.sources.csv_source import CSVSource
from marketgoblin.sources.yahoo import YahooSource
from marketgoblin.storage.disk import DiskStorage
from marketgoblin.ticker_metadata import TickerMetadata

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
    """Public API for fetching, saving, and loading market data.

    Wraps a data source (e.g. Yahoo Finance) and optional disk storage.
    When save_path is provided, fetch() persists data as monthly parquet slices
    and subsequent load() calls read directly from disk without re-downloading.

    Datasets are selected via the `dataset` parameter (default: ``Dataset.OHLCV``).
    OHLCV is returned as a tidy stacked frame with an ``is_adjusted`` bool
    column — filter it downstream to pick the variant you need.
    Available datasets per source are exposed via ``goblin.supported_datasets``.
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

    @property
    def supported_datasets(self) -> frozenset[Dataset]:
        return self._source.supported_datasets

    def fetch(
        self,
        symbol: str,
        start: str,
        end: str,
        dataset: Dataset = Dataset.OHLCV,
        parse_dates: bool = False,
    ) -> pl.LazyFrame:
        """Download data for a symbol. Saves to disk if save_path was set.

        Args:
            symbol: Ticker symbol e.g. 'AAPL'.
            start: Start date as 'YYYY-MM-DD'.
            end: End date as 'YYYY-MM-DD'.
            dataset: Which dataset to fetch (default OHLCV).
            parse_dates: If True, return date as pl.Date instead of int32.

        Raises:
            ValueError: If dates are malformed or start >= end.
        """
        _validate_dates(start, end)

        logger.info(
            "fetch started | symbol=%s provider=%s dataset=%s range=%s:%s",
            symbol,
            self._provider,
            dataset,
            start,
            end,
        )
        t0 = time.perf_counter()

        lf = self._source.fetch(dataset, symbol, start, end)

        if self._storage:
            self._storage.save(self._provider, symbol, dataset, lf)
            lf = self._storage.load(self._provider, symbol, dataset, start, end, parse_dates)
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
        dataset: Dataset = Dataset.OHLCV,
        parse_dates: bool = False,
    ) -> pl.LazyFrame:
        """Load previously saved data from disk.

        Raises:
            ValueError: If dates are malformed or start >= end.
            RuntimeError: If save_path was not set.
        """
        _validate_dates(start, end)
        if not self._storage:
            raise RuntimeError("load() requires save_path to be set.")

        return self._storage.load(self._provider, symbol, dataset, start, end, parse_dates)

    def fetch_metadata(self, symbol: str, *, fast: bool = False) -> TickerMetadata:
        """Live-fetch a unified ticker metadata profile. Saves to disk if ``save_path`` was set.

        yfinance exposes metadata through several overlapping surfaces
        (``info``, ``fast_info``, ``history_metadata``, ``isin``); this method
        returns them as a single :class:`TickerMetadata`.

        Args:
            symbol: Ticker symbol (case-insensitive).
            fast: If True, skip the scraped ``.info`` call. Cheaper but returns
                only quote-identity + history-metadata fields.
        """
        logger.info(
            "fetch_metadata started | symbol=%s provider=%s fast=%s",
            symbol,
            self._provider,
            fast,
        )
        t0 = time.perf_counter()
        metadata = self._source.fetch_metadata(symbol, fast=fast)

        if self._storage:
            self._storage.save_metadata(self._provider, metadata)

        elapsed = time.perf_counter() - t0
        logger.info(
            "fetch_metadata complete | symbol=%s saved=%s elapsed=%.2fs",
            symbol,
            self._storage is not None,
            elapsed,
        )
        return metadata

    def load_metadata(self, symbol: str) -> TickerMetadata:
        """Load previously saved ticker metadata from disk.

        Raises:
            RuntimeError: If ``save_path`` was not set.
            FileNotFoundError: If no metadata exists for ``symbol``.
        """
        if not self._storage:
            raise RuntimeError("load_metadata() requires save_path to be set.")
        return self._storage.load_metadata(self._provider, symbol)

    def fetch_classification(self, symbol: str) -> Classification:
        """Live-fetch sector + industry classification for a ticker.

        Saves to disk if ``save_path`` was set. Either the sector or industry
        sub-profile may be ``None`` if the upstream ticker has no classification
        key (common for ETFs, crypto, indices).
        """
        logger.info(
            "fetch_classification started | symbol=%s provider=%s",
            symbol,
            self._provider,
        )
        t0 = time.perf_counter()
        classification = self._source.fetch_classification(symbol)

        if self._storage:
            self._storage.save_classification(self._provider, classification)

        elapsed = time.perf_counter() - t0
        logger.info(
            "fetch_classification complete | symbol=%s saved=%s elapsed=%.2fs",
            symbol,
            self._storage is not None,
            elapsed,
        )
        return classification

    def load_classification(self, symbol: str) -> Classification:
        """Load previously saved classification from disk.

        Raises:
            RuntimeError: If ``save_path`` was not set.
            FileNotFoundError: If no classification exists for ``symbol``.
        """
        if not self._storage:
            raise RuntimeError("load_classification() requires save_path to be set.")
        return self._storage.load_classification(self._provider, symbol)

    def fetch_many(
        self,
        symbols: list[str],
        start: str,
        end: str,
        dataset: Dataset = Dataset.OHLCV,
        parse_dates: bool = False,
        max_workers: int = 8,
        requests_per_second: float = 2.0,
    ) -> dict[str, pl.LazyFrame]:
        """Download data for multiple symbols concurrently.

        Failed symbols are logged and excluded from the result — they never
        crash the batch.

        Raises:
            ValueError: If dates are malformed or start >= end.
        """
        _validate_dates(start, end)
        logger.info(
            "fetch_many started | symbols=%d dataset=%s range=%s:%s",
            len(symbols),
            dataset,
            start,
            end,
        )
        t0 = time.perf_counter()

        limiter = _RateLimiter(requests_per_second)
        results: dict[str, pl.LazyFrame] = {}

        def _rate_limited_fetch(symbol: str) -> pl.LazyFrame:
            limiter.acquire()
            return self.fetch(symbol, start, end, dataset, parse_dates)

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
