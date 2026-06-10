# Pure helpers for per-dataset dtype normalization.
# normalize_ohlcv / normalize_shares / normalize_dividends cast incoming frames
# to the on-disk schema; parse_dates() converts the stored int32 YYYYMMDD date
# back to pl.Date.

import polars as pl

_OHLC_COLS = ["open", "high", "low", "close"]


def normalize_ohlcv(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Cast OHLC to float32, volume to int64, is_adjusted to bool, date to int32 YYYYMMDD.

    The input frame must include an ``is_adjusted`` column — OHLCV is stored as a
    tidy stacked series where adjusted and raw variants coexist and are
    distinguished by this boolean flag.
    """
    return lf.with_columns(
        [pl.col(c).cast(pl.Float32) for c in _OHLC_COLS]
        + [
            pl.col("volume").cast(pl.Int64),
            pl.col("is_adjusted").cast(pl.Boolean),
            pl.col("date").dt.strftime("%Y%m%d").cast(pl.Int32),
        ]
    )


def normalize_shares(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Cast shares to int64 and date to int32 YYYYMMDD.

    Shares outstanding are integer counts; int64 is required because large-cap
    counts (e.g. AAPL ~1.5e10) overflow int32.
    """
    return lf.with_columns(
        pl.col("shares").cast(pl.Int64),
        pl.col("date").dt.strftime("%Y%m%d").cast(pl.Int32),
    )


def normalize_dividends(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Cast dividend to float32 and date to int32 YYYYMMDD."""
    return lf.with_columns(
        pl.col("dividend").cast(pl.Float32),
        pl.col("date").dt.strftime("%Y%m%d").cast(pl.Int32),
    )


def normalize_splits(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Cast split_factor to float32 and date to int32 YYYYMMDD.

    ``split_factor`` is the per-event multiplier (e.g. ``2.0`` for a 2-for-1
    forward split, ``0.5`` for a 1-for-2 reverse). Float32 is sufficient —
    the factor is always a small, exactly-representable rational.
    """
    return lf.with_columns(
        pl.col("split_factor").cast(pl.Float32),
        pl.col("date").dt.strftime("%Y%m%d").cast(pl.Int32),
    )


def normalize_fundamentals_daily(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Cast the daily-fundamentals frame to the on-disk schema.

    Market-cap and enterprise-value can hit 1e13 for the largest US names —
    int64 is required (int32 caps at ~2.1e9). Ratio columns are float32:
    sufficient precision for valuation multiples and half the disk footprint
    of float64.
    """
    return lf.with_columns(
        pl.col("market_cap").cast(pl.Int64),
        pl.col("enterprise_val").cast(pl.Int64),
        pl.col("pe_ratio").cast(pl.Float32),
        pl.col("pb_ratio").cast(pl.Float32),
        pl.col("trailing_peg_1y").cast(pl.Float32),
        pl.col("date").dt.strftime("%Y%m%d").cast(pl.Int32),
    )


# --- Quarterly statements on-disk schema ---------------------------------
# Every line item is stored in two variants — as-reported (point-in-time, what
# the market saw at filing) and restated (adjusted) — written as
# ``<name>_as_reported`` / ``<name>_adjusted``. This registry is the single
# source of truth for the statements column names, dtypes and order; the Tiingo
# parser (the only producer of this dataset) maps its dataCodes onto these
# names and is guarded against drift at import time.
STATEMENT_VARIANTS: tuple[str, str] = ("as_reported", "adjusted")

# (base name, dtype). Dollar line items and absolute share counts are float64 —
# large-cap quarterly figures (revenue/assets 1e11+, shares ~1e10) exceed
# float32's exact-integer range (±1.6e7); per-share figures and ratios are
# float32, where values rarely exceed ±100 and precision loss sits below
# reporting granularity.
_F64 = pl.Float64()
_F32 = pl.Float32()
STATEMENT_FIELDS: tuple[tuple[str, pl.DataType], ...] = (
    # income statement
    ("revenue", _F64),
    ("cost_of_revenue", _F64),
    ("gross_profit", _F64),
    ("operating_expenses", _F64),
    ("sga", _F64),
    ("rnd", _F64),
    ("operating_income", _F64),
    ("ebit", _F64),
    ("ebitda", _F64),
    ("ebt", _F64),
    ("interest_expense", _F64),
    ("tax_expense", _F64),
    ("net_income", _F64),
    ("net_income_common_stock", _F64),
    ("net_income_disc_ops", _F64),
    ("consolidated_income", _F64),
    ("non_controlling_interests", _F64),
    ("preferred_dividends", _F64),
    ("eps_basic", _F32),
    ("eps_diluted", _F32),
    ("weighted_avg_shares", _F64),
    ("weighted_avg_shares_diluted", _F64),
    # balance sheet
    ("cash_and_eq", _F64),
    ("accounts_receivable", _F64),
    ("inventory", _F64),
    ("investments_current", _F64),
    ("assets_current", _F64),
    ("ppe", _F64),
    ("investments", _F64),
    ("investments_non_current", _F64),
    ("intangibles", _F64),
    ("tax_assets", _F64),
    ("assets_non_current", _F64),
    ("total_assets", _F64),
    ("accounts_payable", _F64),
    ("debt_current", _F64),
    ("deferred_revenue", _F64),
    ("liabilities_current", _F64),
    ("debt_non_current", _F64),
    ("liabilities_non_current", _F64),
    ("tax_liabilities", _F64),
    ("deposits", _F64),
    ("total_liabilities", _F64),
    ("total_debt", _F64),
    ("equity", _F64),
    ("retained_earnings", _F64),
    ("accumulated_oci", _F64),
    ("shares_basic", _F64),
    # cash flow
    ("net_cash_ops", _F64),
    ("net_cash_investing", _F64),
    ("net_cash_financing", _F64),
    ("net_cash_flow", _F64),
    ("fx_effect_on_cash", _F64),
    ("capex", _F64),
    ("free_cash_flow", _F64),
    ("depreciation_amortization", _F64),
    ("stock_based_comp", _F64),
    ("dividends_paid", _F64),
    ("issuance_repayment_debt", _F64),
    ("issuance_repayment_equity", _F64),
    ("business_acq_disposals", _F64),
    ("investments_acq_disposals", _F64),
    # overview (ratios are float32; book_value is a dollar total → float64)
    ("book_value", _F64),
    ("book_value_per_share", _F32),
    ("revenue_per_share", _F32),
    ("roe", _F32),
    ("roa", _F32),
    ("gross_margin", _F32),
    ("profit_margin", _F32),
    ("current_ratio", _F32),
    ("debt_equity", _F32),
    ("long_term_debt_equity", _F32),
    ("piotroski_f_score", _F32),
    ("revenue_qoq", _F32),
    ("eps_qoq", _F32),
    ("share_factor", _F32),
)


def normalize_statements(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Cast a quarterly-statements frame to the on-disk schema.

    Every field in :data:`STATEMENT_FIELDS` is carried in both an as-reported
    and an adjusted variant so downstream PEAD/SUE consumers can swap
    point-in-time vs restated figures without re-fetching. ``date`` is the
    filing date, cast to int32 YYYYMMDD per project convention
    (``parse_dates`` recovers ``pl.Date`` on load).
    """
    casts = [
        pl.col("fiscal_year").cast(pl.Int16),
        pl.col("fiscal_quarter").cast(pl.Int8),
        pl.col("date").dt.strftime("%Y%m%d").cast(pl.Int32),
    ]
    casts += [
        pl.col(f"{name}_{variant}").cast(dtype)
        for name, dtype in STATEMENT_FIELDS
        for variant in STATEMENT_VARIANTS
    ]
    return lf.with_columns(casts)


# Trades (tick) on-disk schema. Unlike the daily datasets, the canonical time
# axis is the nanosecond ``timestamp``; the int32 ``date`` is *derived* from it
# so the monthly slice / merge / load machinery (which partitions by YYYY-MM and
# filters on ``date``) works unchanged for intraday data.
_TRADES_COLUMNS: tuple[str, ...] = (
    "date",
    "timestamp",
    "symbol",
    "exchange",
    "price",
    "size",
    "conditions",
    "trade_id",
    "tape",
)

# US equities trading calendar. The derived `date` is the Eastern session date,
# NOT the UTC date: extended hours run to 20:00 ET, which crosses UTC midnight
# (00:00–01:00 UTC), so a UTC date would misstamp after-hours prints and route
# end-of-month sessions into the next month's slice.
_SESSION_TZ = "America/New_York"


def normalize_trades(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Cast a trades frame to the on-disk schema and derive int32 YYYYMMDD date.

    Input must carry a tz-aware ``timestamp`` (Datetime). The derived ``date`` is
    the US/Eastern trading-session date (``_SESSION_TZ``): the timestamp is
    converted to Eastern before truncation so extended-hours prints that cross
    UTC midnight still land on the correct session and monthly slice. The stored
    ``timestamp`` itself stays UTC.
    """
    return (
        lf.with_columns(
            pl.col("price").cast(pl.Float32),
            pl.col("size").cast(pl.Int64),
            pl.col("trade_id").cast(pl.Int64),
            pl.col("timestamp")
            .dt.convert_time_zone(_SESSION_TZ)
            .dt.strftime("%Y%m%d")
            .cast(pl.Int32)
            .alias("date"),
        )
        .select(_TRADES_COLUMNS)
        .sort("timestamp")
    )


def parse_dates(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Convert int32 YYYYMMDD date back to pl.Date for in-memory use."""
    return lf.with_columns(pl.col("date").cast(pl.String).str.to_date("%Y%m%d"))
