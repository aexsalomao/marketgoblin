import logging
import time
from typing import Any

import polars as pl
import yfinance as yf

from marketgoblin._normalize import normalize
from marketgoblin.sources.base import BaseSource

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3
_RETRY_DELAYS = [1.0, 2.0]  # seconds between attempts (len == _MAX_RETRIES - 1)


class YahooSource(BaseSource):
    """OHLCV source backed by yfinance (Yahoo Finance).

    Uses yfinance's auto_adjust flag to control split/dividend adjustment.
    Returns a normalized LazyFrame (float32 OHLCV, int32 YYYYMMDD date).
    Retries up to 3 times with exponential backoff on transient failures.
    """

    name = "yahoo"

    def __init__(self, api_key: str | None = None, **kwargs: Any) -> None:
        super().__init__(api_key, **kwargs)

    def fetch(self, symbol: str, start: str, end: str, adjusted: bool = True) -> pl.LazyFrame:
        last_exc: Exception = RuntimeError("unreachable")

        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                raw = yf.Ticker(symbol).history(start=start, end=end, auto_adjust=adjusted)
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "fetch attempt %d/%d failed | symbol=%s error=%s",
                    attempt, _MAX_RETRIES, symbol, exc,
                )
                if attempt < _MAX_RETRIES:
                    time.sleep(_RETRY_DELAYS[attempt - 1])
                continue

            if raw.empty:
                raise ValueError(f"No data returned for {symbol} ({start} to {end})")

            lf = (
                pl.from_pandas(raw.reset_index())
                .rename(lambda col: col.lower())
                .select(["date", "open", "high", "low", "close", "volume"])
                .with_columns(
                    pl.col("date").dt.date(),
                    pl.lit(symbol.upper()).alias("symbol"),
                )
                .lazy()
            )
            return normalize(lf)

        logger.error(
            "all %d fetch attempts failed | symbol=%s error=%s",
            _MAX_RETRIES, symbol, last_exc,
        )
        raise last_exc
