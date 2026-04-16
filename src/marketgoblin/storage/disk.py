import logging
import os
from pathlib import Path

import polars as pl

from marketgoblin._metadata import build as _build_metadata
from marketgoblin._metadata import write as _write_metadata
from marketgoblin._normalize import parse_dates as _parse_dates

logger = logging.getLogger(__name__)


class DiskStorage:
    """Persist and load OHLCV data as monthly parquet slices on disk.

    Layout: {base_path}/{provider}/ohlcv/{adjusted|raw}/{SYMBOL}/{SYMBOL}_{YYYY-MM}.pq
    Each slice has a JSON sidecar at the same path with a .json extension.
    All writes are atomic via a .tmp rename.
    """

    def __init__(self, base_path: str | Path) -> None:
        self.base_path = Path(base_path)

    def save(self, provider: str, symbol: str, lf: pl.LazyFrame, adjusted: bool = True) -> None:
        """Split by month and atomically write one .pq file per month."""
        df = lf.collect().with_columns(
            (
                pl.col("date").cast(pl.String).str.slice(0, 4)
                + "-"
                + pl.col("date").cast(pl.String).str.slice(4, 2)
            ).alias("_ym")
        )

        for ym in df["_ym"].unique().sort():
            chunk = df.filter(pl.col("_ym") == ym).drop("_ym").sort("date")
            path = self._slice_path(provider, symbol, ym, adjusted)
            path.parent.mkdir(parents=True, exist_ok=True)
            self._atomic_write(chunk, path)
            meta = _build_metadata(
                chunk, provider, symbol, ym, path.stat().st_size, price_adjusted=adjusted
            )
            _write_metadata(meta, path)
            logger.info(
                "slice saved | %s rows=%d size=%db",
                path.name,
                meta["row_count"],
                meta["file_size_bytes"],
            )
            if meta["missing_days"]:
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
        start: str,
        end: str,
        parse_dates: bool = False,
        adjusted: bool = True,
    ) -> pl.LazyFrame:
        """Load OHLCV from disk. Date is int32 by default; pass parse_dates=True for pl.Date."""
        symbol_dir = self._symbol_dir(provider, symbol, adjusted)
        if not symbol_dir.exists():
            raise FileNotFoundError(f"No data found for {symbol} at {symbol_dir}")
        pattern = (symbol_dir / f"{symbol}_*.pq").as_posix()
        start_int = int(start.replace("-", ""))
        end_int = int(end.replace("-", ""))

        lf = pl.scan_parquet(pattern).filter(pl.col("date").is_between(start_int, end_int))

        return _parse_dates(lf) if parse_dates else lf

    def _symbol_dir(self, provider: str, symbol: str, adjusted: bool = True) -> Path:
        price_type = "adjusted" if adjusted else "raw"
        return self.base_path / provider / "ohlcv" / price_type / symbol

    def _slice_path(self, provider: str, symbol: str, ym: str, adjusted: bool = True) -> Path:
        return self._symbol_dir(provider, symbol, adjusted) / f"{symbol}_{ym}.pq"

    def _atomic_write(self, df: pl.DataFrame, path: Path) -> None:
        tmp = path.with_suffix(".tmp")
        df.write_parquet(tmp)
        os.replace(tmp, path)
