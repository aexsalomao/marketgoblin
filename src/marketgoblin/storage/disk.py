# DiskStorage — persists per-dataset frames as monthly parquet slices with
# JSON sidecars. All writes are atomic (.tmp rename); loads return a LazyFrame
# filtered to the requested date range. Path scheme is dataset-aware:
# OHLCV adds an adjusted/raw variant segment, shares does not.

import logging
import os
from pathlib import Path
from typing import Any

import polars as pl

from marketgoblin._metadata import build_ohlcv as _build_ohlcv_metadata
from marketgoblin._metadata import build_shares as _build_shares_metadata
from marketgoblin._metadata import write as _write_metadata
from marketgoblin._normalize import parse_dates as _parse_dates
from marketgoblin.datasets import Dataset

logger = logging.getLogger(__name__)


class DiskStorage:
    """Persist and load dataset frames as monthly parquet slices on disk.

    Layout:
        OHLCV:  {base_path}/{provider}/ohlcv/{adjusted|raw}/{SYMBOL}/{SYMBOL}_{YYYY-MM}.pq
        SHARES: {base_path}/{provider}/shares/{SYMBOL}/{SYMBOL}_{YYYY-MM}.pq

    Each slice has a JSON sidecar at the same path with a .json extension.
    All writes are atomic via a .tmp rename.
    """

    def __init__(self, base_path: str | Path) -> None:
        self.base_path = Path(base_path)

    def save(
        self,
        provider: str,
        symbol: str,
        dataset: Dataset,
        lf: pl.LazyFrame,
        adjusted: bool = True,
    ) -> None:
        """Split by month and atomically write one .pq file per month."""
        # Normalize symbol case at the boundary so save() and load() always agree
        # regardless of how the caller spelled the ticker.
        symbol = symbol.upper()
        df = lf.collect().with_columns(
            (
                pl.col("date").cast(pl.String).str.slice(0, 4)
                + "-"
                + pl.col("date").cast(pl.String).str.slice(4, 2)
            ).alias("_ym")
        )

        for ym in df["_ym"].unique().sort():
            chunk = df.filter(pl.col("_ym") == ym).drop("_ym").sort("date")
            path = self._slice_path(provider, symbol, dataset, ym, adjusted)
            path.parent.mkdir(parents=True, exist_ok=True)
            self._atomic_write(chunk, path)
            meta = self._build_metadata(
                chunk, provider, symbol, dataset, ym, path.stat().st_size, adjusted
            )
            _write_metadata(meta, path)
            logger.info(
                "slice saved | %s rows=%d size=%db",
                path.name,
                meta["row_count"],
                meta["file_size_bytes"],
            )
            if dataset == Dataset.OHLCV and meta["missing_days"]:
                logger.warning(
                    "missing days | symbol=%s month=%s count=%d days=%s",
                    symbol,
                    ym,
                    len(meta["missing_days"]),
                    meta["missing_days"],
                )

    def load(
        self,
        provider: str,
        symbol: str,
        dataset: Dataset,
        start: str,
        end: str,
        parse_dates: bool = False,
        adjusted: bool = True,
    ) -> pl.LazyFrame:
        """Load dataset slices from disk, filtered to [start, end]."""
        symbol = symbol.upper()
        symbol_dir = self._symbol_dir(provider, symbol, dataset, adjusted)
        if not symbol_dir.exists():
            raise FileNotFoundError(f"No {dataset} data found for {symbol} at {symbol_dir}")
        pattern = (symbol_dir / f"{symbol}_*.pq").as_posix()
        start_int = int(start.replace("-", ""))
        end_int = int(end.replace("-", ""))

        lf = pl.scan_parquet(pattern).filter(pl.col("date").is_between(start_int, end_int))

        return _parse_dates(lf) if parse_dates else lf

    def _symbol_dir(
        self, provider: str, symbol: str, dataset: Dataset, adjusted: bool = True
    ) -> Path:
        base = self.base_path / provider / dataset
        if dataset == Dataset.OHLCV:
            variant = "adjusted" if adjusted else "raw"
            return base / variant / symbol
        return base / symbol

    def _slice_path(
        self,
        provider: str,
        symbol: str,
        dataset: Dataset,
        ym: str,
        adjusted: bool = True,
    ) -> Path:
        return self._symbol_dir(provider, symbol, dataset, adjusted) / f"{symbol}_{ym}.pq"

    def _build_metadata(
        self,
        chunk: pl.DataFrame,
        provider: str,
        symbol: str,
        dataset: Dataset,
        ym: str,
        file_size_bytes: int,
        adjusted: bool,
    ) -> dict[str, Any]:
        if dataset == Dataset.OHLCV:
            return _build_ohlcv_metadata(
                chunk, provider, symbol, ym, file_size_bytes, price_adjusted=adjusted
            )
        return _build_shares_metadata(chunk, provider, symbol, ym, file_size_bytes)

    def _atomic_write(self, df: pl.DataFrame, path: Path) -> None:
        tmp = path.with_suffix(".tmp")
        df.write_parquet(tmp)
        os.replace(tmp, path)
