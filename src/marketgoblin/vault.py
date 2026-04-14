import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import polars as pl

from marketgoblin._normalize import parse_dates as _parse_dates
from marketgoblin.sources.base import BaseSource
from marketgoblin.sources.yahoo import YahooSource
from marketgoblin.storage.disk import DiskStorage

logger = logging.getLogger(__name__)

_SOURCES: dict[str, type[BaseSource]] = {
    "yahoo": YahooSource,
}


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
    ) -> None:
        if provider not in _SOURCES:
            raise ValueError(f"Unknown provider '{provider}'. Available: {list(_SOURCES)}")

        self._provider = provider
        self._source = _SOURCES[provider](api_key=api_key)
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
        """
        price_type = "adjusted" if adjusted else "raw"
        logger.info("fetch started | symbol=%s provider=%s range=%s:%s %s", symbol, self._provider, start, end, price_type)
        t0 = time.perf_counter()

        lf = self._source.fetch(symbol, start, end, adjusted=adjusted)

        if self._storage:
            self._storage.save(self._provider, symbol, lf, adjusted=adjusted)
            lf = self._storage.load(self._provider, symbol, start, end, parse_dates, adjusted=adjusted)
            elapsed = time.perf_counter() - t0
            logger.info("fetch complete | symbol=%s rows=%d saved=True elapsed=%.2fs", symbol, lf.collect().height, elapsed)
            return lf

        df = lf.collect()
        elapsed = time.perf_counter() - t0
        logger.info("fetch complete | symbol=%s rows=%d saved=False elapsed=%.2fs", symbol, df.height, elapsed)
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
        """
        if not self._storage:
            raise RuntimeError("load() requires save_path to be set.")

        return self._storage.load(self._provider, symbol, start, end, parse_dates, adjusted=adjusted)

    def fetch_many(
        self,
        symbols: list[str],
        start: str,
        end: str,
        adjusted: bool = True,
        parse_dates: bool = False,
        max_workers: int = 8,
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
        """
        logger.info("fetch_many started | symbols=%d range=%s:%s", len(symbols), start, end)
        t0 = time.perf_counter()

        results: dict[str, pl.LazyFrame] = {}

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(self.fetch, symbol, start, end, adjusted, parse_dates): symbol
                for symbol in symbols
            }
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    results[symbol] = future.result()
                except Exception as e:
                    logger.error("fetch failed | symbol=%s error=%s", symbol, e)

        elapsed = time.perf_counter() - t0
        logger.info(
            "fetch_many complete | success=%d failed=%d elapsed=%.2fs",
            len(results), len(symbols) - len(results), elapsed,
        )
        return results
