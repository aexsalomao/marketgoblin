# BaseSource — contract every data provider implements.
# Subclasses register per-dataset fetchers via _build_dispatch(); fetch()
# routes by Dataset and raises if the source doesn't support it.

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

import polars as pl

from marketgoblin.datasets import Dataset

# Each fetcher receives (symbol, start, end, adjusted) and returns a normalized
# LazyFrame. `adjusted` is OHLCV-specific; non-OHLCV fetchers must accept and
# ignore it so the dispatch table has a uniform signature.
Fetcher = Callable[[str, str, str, bool], pl.LazyFrame]


class BaseSource(ABC):
    name: str

    def __init__(self, api_key: str | None = None, **kwargs: Any) -> None:
        self.api_key = api_key
        # _build_dispatch() runs here, so subclasses must initialize their own
        # instance state AFTER super().__init__(). The dispatch table only stores
        # bound method references (no eager state read), so this works as long as
        # subclass _fetch_* methods don't read instance attrs at registration time.
        self._dispatch: dict[Dataset, Fetcher] = self._build_dispatch()

    @abstractmethod
    def _build_dispatch(self) -> dict[Dataset, Fetcher]:
        """Return a map of supported datasets to their fetcher methods."""

    @property
    def supported_datasets(self) -> frozenset[Dataset]:
        return frozenset(self._dispatch)

    def fetch(
        self,
        dataset: Dataset,
        symbol: str,
        start: str,
        end: str,
        adjusted: bool = True,
    ) -> pl.LazyFrame:
        """Dispatch to the per-dataset fetcher. Raises if dataset unsupported."""
        handler = self._dispatch.get(dataset)
        if handler is None:
            supported = sorted(self._dispatch)
            raise ValueError(
                f"Source '{self.name}' does not support dataset '{dataset}'. Supported: {supported}"
            )
        return handler(symbol, start, end, adjusted)
