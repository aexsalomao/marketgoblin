import logging

import polars as pl
import yfinance as yf

from marketgoblin._normalize import normalize
from marketgoblin.sources.base import BaseSource

logger = logging.getLogger(__name__)


class YahooSource(BaseSource):
    """OHLCV source backed by yfinance (Yahoo Finance).

    Uses yfinance's auto_adjust flag to control split/dividend adjustment.
    Returns a normalized LazyFrame (float32 OHLCV, int32 YYYYMMDD date).
    """

    name = "yahoo"

    def fetch(self, symbol: str, start: str, end: str, adjusted: bool = True) -> pl.LazyFrame:
        raw = yf.Ticker(symbol).history(start=start, end=end, auto_adjust=adjusted)

        if raw.empty:
            logger.error("no data returned | symbol=%s range=%s:%s", symbol, start, end)
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
