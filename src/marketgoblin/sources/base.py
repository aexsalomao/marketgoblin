from abc import ABC, abstractmethod
from typing import Any

import polars as pl


class BaseSource(ABC):
    name: str

    def __init__(self, api_key: str | None = None, **kwargs: Any) -> None:
        self.api_key = api_key

    @abstractmethod
    def fetch(self, symbol: str, start: str, end: str, adjusted: bool = True) -> pl.LazyFrame:
        """Download OHLCV data. Returns a normalised LazyFrame (float32, int32 date)."""
