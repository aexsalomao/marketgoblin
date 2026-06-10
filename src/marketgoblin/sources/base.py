# BaseSource — contract every data provider implements.
# Subclasses register per-dataset fetchers via _build_dispatch(); fetch()
# routes by Dataset and raises if the source doesn't support it.

import logging
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, TypeVar

import polars as pl

from marketgoblin.classification import Classification
from marketgoblin.datasets import Dataset
from marketgoblin.ticker_metadata import TickerMetadata

logger = logging.getLogger(__name__)

# Each fetcher receives (symbol, start, end) and returns a normalized LazyFrame.
# OHLCV fetchers return a tidy stacked frame with an `is_adjusted` bool column
# carrying both adjusted and raw variants — no separate toggle is needed.
Fetcher = Callable[[str, str, str], pl.LazyFrame]

# Shared transient-error retry policy for every source.
_MAX_RETRIES = 3
_RETRY_DELAYS = [1.0, 2.0]  # seconds between attempts (len == _MAX_RETRIES - 1)

_T = TypeVar("_T")


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
    ) -> pl.LazyFrame:
        """Dispatch to the per-dataset fetcher. Raises if dataset unsupported."""
        handler = self._dispatch.get(dataset)
        if handler is None:
            supported = sorted(self._dispatch)
            raise ValueError(
                f"Source '{self.name}' does not support dataset '{dataset}'. Supported: {supported}"
            )
        return handler(symbol, start, end)

    def fetch_metadata(self, symbol: str, *, fast: bool = False) -> TickerMetadata:
        """Return a TickerMetadata snapshot. Override per-source.

        Metadata is point-in-time and not a time series, so it sits outside the
        Dataset dispatch table.
        """
        raise NotImplementedError(f"Source '{self.name}' does not support metadata fetching.")

    def fetch_classification(self, symbol: str) -> Classification:
        """Return sector + industry classification for a ticker. Override per-source."""
        raise NotImplementedError(f"Source '{self.name}' does not support classification fetching.")

    def _retry_fetch(self, fetch_fn: Callable[[], _T], symbol: str) -> _T:
        """Retry fetch_fn on transient errors with backoff. ValueError propagates.

        Shared by every source: transient (network) failures retry with a short
        backoff; a ``ValueError`` (empty data / domain validation) propagates
        immediately without retry.
        """
        last_exc: Exception = RuntimeError("unreachable")
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                return fetch_fn()
            except ValueError:
                raise  # empty data / domain validation — don't retry
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "fetch attempt %d/%d failed | symbol=%s error=%s",
                    attempt,
                    _MAX_RETRIES,
                    symbol,
                    exc,
                )
                if attempt < _MAX_RETRIES:
                    time.sleep(_RETRY_DELAYS[attempt - 1])

        logger.error(
            "all %d fetch attempts failed | symbol=%s error=%s", _MAX_RETRIES, symbol, last_exc
        )
        raise last_exc
