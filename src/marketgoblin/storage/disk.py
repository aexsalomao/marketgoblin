# DiskStorage — persists per-dataset frames as monthly parquet slices with
# JSON sidecars. All writes are atomic (.tmp rename); loads return a LazyFrame
# filtered to the requested date range. OHLCV is stored as a tidy stacked
# frame (adjusted + raw coexist, distinguished by is_adjusted column).

import json
import logging
import os
from pathlib import Path
from typing import Any

import polars as pl

from marketgoblin._metadata import build_dividends as _build_dividends_metadata
from marketgoblin._metadata import build_fundamentals_daily as _build_fundamentals_daily_metadata
from marketgoblin._metadata import (
    build_fundamentals_statements as _build_fundamentals_statements_metadata,
)
from marketgoblin._metadata import build_ohlcv as _build_ohlcv_metadata
from marketgoblin._metadata import build_shares as _build_shares_metadata
from marketgoblin._metadata import build_splits as _build_splits_metadata
from marketgoblin._metadata import write as _write_metadata
from marketgoblin._normalize import parse_dates as _parse_dates
from marketgoblin.classification import Classification
from marketgoblin.datasets import Dataset
from marketgoblin.ticker_metadata import TickerMetadata

logger = logging.getLogger(__name__)


class DiskStorage:
    """Persist and load dataset frames as monthly parquet slices on disk.

    Layout:
        {base_path}/{provider}/{dataset}/{SYMBOL}/{SYMBOL}_{YYYY-MM}.pq

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
    ) -> None:
        """Split by month and atomically write one .pq file per month.

        Each month slice is MERGED with any rows already on disk: existing rows
        the incoming frame does not cover are kept, and incoming rows win where
        identities overlap (vendor restatements replace stale bars). Row identity
        is ``(date, is_adjusted)`` for stacked OHLCV, plain ``date`` otherwise.
        A partial-range fetch therefore never erases the rest of a month — only
        deleting the slice file discards history.
        """
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
            path = self._slice_path(provider, symbol, dataset, ym)
            path.parent.mkdir(parents=True, exist_ok=True)
            chunk = self._merge_existing(chunk, path)
            self._atomic_write(chunk, path)
            meta = self._build_metadata(chunk, provider, symbol, dataset, ym, path.stat().st_size)
            _write_metadata(meta, path.with_suffix(".json"))
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
    ) -> pl.LazyFrame:
        """Load dataset slices from disk, filtered to [start, end]."""
        symbol = symbol.upper()
        symbol_dir = self._symbol_dir(provider, symbol, dataset)
        if not symbol_dir.exists():
            raise FileNotFoundError(f"No {dataset} data found for {symbol} at {symbol_dir}")
        pattern = (symbol_dir / f"{symbol}_*.pq").as_posix()
        start_int = int(start.replace("-", ""))
        end_int = int(end.replace("-", ""))

        lf = pl.scan_parquet(pattern).filter(pl.col("date").is_between(start_int, end_int))

        return _parse_dates(lf) if parse_dates else lf

    def save_metadata(self, provider: str, metadata: TickerMetadata) -> None:
        """Atomically write TickerMetadata as a JSON file.

        Layout: ``{base_path}/{provider}/metadata/{SYMBOL}.json``. Metadata is
        point-in-time (no date axis), so it sits outside the monthly-slice scheme.
        """
        path = self._metadata_path(provider, metadata.symbol)
        _write_metadata(metadata.to_dict(), path)
        logger.info("metadata saved | %s", path.name)

    def load_metadata(self, provider: str, symbol: str) -> TickerMetadata:
        """Read a saved TickerMetadata from disk. Raises if missing."""
        path = self._metadata_path(provider, symbol)
        if not path.exists():
            raise FileNotFoundError(f"No metadata found for {symbol.upper()} at {path}")
        return TickerMetadata.from_dict(json.loads(path.read_text()))

    def _metadata_path(self, provider: str, symbol: str) -> Path:
        return self.base_path / provider / "metadata" / f"{symbol.upper()}.json"

    def save_classification(self, provider: str, classification: Classification) -> None:
        """Atomically write a Classification as JSON.

        Layout: ``{base_path}/{provider}/classification/{SYMBOL}.json``.
        """
        path = self._classification_path(provider, classification.symbol)
        _write_metadata(classification.to_dict(), path)
        logger.info("classification saved | %s", path.name)

    def load_classification(self, provider: str, symbol: str) -> Classification:
        """Read a saved Classification from disk. Raises if missing."""
        path = self._classification_path(provider, symbol)
        if not path.exists():
            raise FileNotFoundError(f"No classification found for {symbol.upper()} at {path}")
        return Classification.from_dict(json.loads(path.read_text()))

    def _classification_path(self, provider: str, symbol: str) -> Path:
        return self.base_path / provider / "classification" / f"{symbol.upper()}.json"

    def _symbol_dir(self, provider: str, symbol: str, dataset: Dataset) -> Path:
        return self.base_path / provider / dataset / symbol

    def _slice_path(self, provider: str, symbol: str, dataset: Dataset, ym: str) -> Path:
        return self._symbol_dir(provider, symbol, dataset) / f"{symbol}_{ym}.pq"

    def _build_metadata(
        self,
        chunk: pl.DataFrame,
        provider: str,
        symbol: str,
        dataset: Dataset,
        ym: str,
        file_size_bytes: int,
    ) -> dict[str, Any]:
        if dataset == Dataset.OHLCV:
            return _build_ohlcv_metadata(chunk, provider, symbol, ym, file_size_bytes)
        if dataset == Dataset.DIVIDENDS:
            return _build_dividends_metadata(chunk, provider, symbol, ym, file_size_bytes)
        if dataset == Dataset.SPLITS:
            return _build_splits_metadata(chunk, provider, symbol, ym, file_size_bytes)
        if dataset == Dataset.FUNDAMENTALS_DAILY:
            return _build_fundamentals_daily_metadata(chunk, provider, symbol, ym, file_size_bytes)
        if dataset == Dataset.FUNDAMENTALS_STATEMENTS:
            return _build_fundamentals_statements_metadata(
                chunk, provider, symbol, ym, file_size_bytes
            )
        return _build_shares_metadata(chunk, provider, symbol, ym, file_size_bytes)

    def _merge_existing(self, chunk: pl.DataFrame, path: Path) -> pl.DataFrame:
        """Union an incoming month chunk with the slice already at ``path``, new rows winning.

        Keeps existing rows the incoming frame does not cover (a partial-range fetch
        must not erase the rest of the month) and takes the incoming rows wherever
        identities overlap. Row identity is ``(date, is_adjusted)`` for stacked OHLCV
        and plain ``date`` otherwise — keying on date alone would evict the untouched
        variant when a chunk carries only one. An unreadable existing slice falls back
        to plain replacement.
        """
        if not path.exists():
            return chunk
        try:
            existing = pl.read_parquet(path)
        except Exception:  # noqa: BLE001 - corrupt slice -> rewrite it from the fresh fetch
            logger.warning("unreadable slice %s; replacing with fetched rows", path.name)
            return chunk
        keys = (
            ["date", "is_adjusted"]
            if "is_adjusted" in chunk.columns and "is_adjusted" in existing.columns
            else ["date"]
        )
        kept = existing.join(chunk.select(keys), on=keys, how="anti")
        if kept.is_empty():
            return chunk
        return pl.concat([kept, chunk], how="vertical_relaxed").sort(keys)

    def _atomic_write(self, df: pl.DataFrame, path: Path) -> None:
        tmp = path.with_suffix(".tmp")
        df.write_parquet(tmp)
        os.replace(tmp, path)
