# Shared pytest fixtures.
# `make_statements_frame` builds a full quarterly-statements frame (every
# STATEMENT_FIELDS column in both variants) so the normalize / metadata /
# storage suites stop hand-rolling the same ~150-column scaffold three ways.

from datetime import date

import polars as pl
import pytest

from marketgoblin._normalize import STATEMENT_FIELDS, STATEMENT_VARIANTS


@pytest.fixture
def make_statements_frame():
    """Factory for a statements frame covering every field in both variants.

    Keyword-only args:
        dates: one filing ``date`` per row (its length sets the row count).
        fiscal_years / fiscal_quarters: per-row period values.
        anchors: optional ``{column_name: [values]}`` overriding specific field
            columns with real values for assertions; every other field is 0.0.
        on_disk: ``True`` → normalized on-disk schema (int32 YYYYMMDD date,
            Int16/Int8 fiscal periods, per-field dtypes). ``False`` →
            pre-normalize wire shape (pl.Date, Int64 periods, all-Float64).
        lazy: return a LazyFrame (default) or eager DataFrame.
    """

    def _build(
        *,
        dates: list[date],
        fiscal_years: list[int],
        fiscal_quarters: list[int],
        anchors: dict[str, list] | None = None,
        on_disk: bool,
        lazy: bool = True,
    ) -> pl.LazyFrame | pl.DataFrame:
        anchors = anchors or {}
        n = len(dates)

        if on_disk:
            date_col = pl.Series([int(d.strftime("%Y%m%d")) for d in dates], dtype=pl.Int32)
            year_col = pl.Series(fiscal_years, dtype=pl.Int16)
            quarter_col = pl.Series(fiscal_quarters, dtype=pl.Int8)
            field_dtypes = dict(STATEMENT_FIELDS)
        else:
            date_col = pl.Series(dates, dtype=pl.Date)
            year_col = pl.Series(fiscal_years, dtype=pl.Int64)
            quarter_col = pl.Series(fiscal_quarters, dtype=pl.Int64)
            field_dtypes = {name: pl.Float64() for name, _ in STATEMENT_FIELDS}

        data: dict[str, pl.Series | list] = {
            "date": date_col,
            "fiscal_year": year_col,
            "fiscal_quarter": quarter_col,
        }
        for name, _dtype in STATEMENT_FIELDS:
            for variant in STATEMENT_VARIANTS:
                col = f"{name}_{variant}"
                values = anchors.get(col, [0.0] * n)
                data[col] = pl.Series(values, dtype=field_dtypes[name])
        data["symbol"] = ["AAPL"] * n

        frame = pl.DataFrame(data)
        return frame.lazy() if lazy else frame

    return _build
