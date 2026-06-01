# Shared primitives for the Tiingo parsers: nullable coercions, slugification,
# and the ISO-date column parse. Imported by the prices / fundamentals /
# metadata submodules; holds no Tiingo-shape knowledge of its own.

import re
from typing import Any

import polars as pl

_SLUG_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def first_present(d: dict[str, Any], *keys: str) -> Any:
    """Return the first value whose key exists in d with a non-None value."""
    for key in keys:
        if key in d and d[key] is not None:
            return d[key]
    return None


def slugify(value: str) -> str:
    """Lower-cased, hyphenated slug. ``"Information Technology"`` → ``"information-technology"``."""
    return _SLUG_NON_ALNUM.sub("-", value.lower()).strip("-")


def parse_tiingo_date_col(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Parse Tiingo's ISO date strings to ``pl.Date``.

    Tiingo returns ``"YYYY-MM-DDT00:00:00.000Z"``; sliced to the first 10 chars
    we get a clean ``YYYY-MM-DD`` parsable by polars.
    """
    return lf.with_columns(pl.col("date").str.slice(0, 10).str.to_date("%Y-%m-%d"))
