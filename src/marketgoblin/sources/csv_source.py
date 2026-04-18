# CSVSource — OHLCV provider backed by local CSV files.
# Reads {data_dir}/{SYMBOL}.csv, filters by date range, and returns a
# normalized LazyFrame. Useful for backtesting and offline use. OHLCV-only.

from pathlib import Path
from typing import Any

import polars as pl

from marketgoblin._normalize import normalize_ohlcv
from marketgoblin.datasets import Dataset
from marketgoblin.sources.base import BaseSource, Fetcher


class CSVSource(BaseSource):
    """OHLCV source backed by local CSV files.

    Looks for a file named ``{data_dir}/{SYMBOL}.csv`` for each requested symbol.
    Useful for backtesting with historical snapshots or offline use.

    Expected CSV columns (case-insensitive):
        date (YYYY-MM-DD), open, high, low, close, volume, symbol

    Example::

        goblin = MarketGoblin(provider="csv", data_dir="./csv_files")
        lf = goblin.fetch("AAPL", "2024-01-01", "2024-03-31")
    """

    name = "csv"

    def __init__(
        self,
        api_key: str | None = None,
        data_dir: str | Path = ".",
        **kwargs: Any,
    ) -> None:
        super().__init__(api_key, **kwargs)
        self.data_dir = Path(data_dir)

    def _build_dispatch(self) -> dict[Dataset, Fetcher]:
        return {Dataset.OHLCV: self._fetch_ohlcv}

    def _fetch_ohlcv(
        self, symbol: str, start: str, end: str, adjusted: bool = True
    ) -> pl.LazyFrame:
        # `adjusted` is meaningless for CSV — files are pre-adjusted by the caller.
        del adjusted

        path = self.data_dir / f"{symbol.upper()}.csv"
        if not path.exists():
            raise ValueError(f"No CSV file found for {symbol} at {path}")

        start_int = int(start.replace("-", ""))
        end_int = int(end.replace("-", ""))

        lf = (
            pl.scan_csv(path)
            .rename(lambda col: col.lower())
            .select(["date", "open", "high", "low", "close", "volume", "symbol"])
            .with_columns(
                pl.col("date").str.to_date("%Y-%m-%d"),
                pl.col("symbol").str.to_uppercase(),
            )
        )

        return normalize_ohlcv(lf).filter(pl.col("date").is_between(start_int, end_int))
