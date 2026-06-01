# Pure adapter/parser helpers for TiingoSource.
# Bridges Tiingo's JSON shapes (list[dict] from TiingoClient + raw /fundamentals/meta
# REST responses) into marketgoblin's typed dataclasses and on-disk frame schema.
# Tests live in tests/test_tiingo.py.

import logging
import re
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import polars as pl
import requests  # type: ignore[import-untyped]

from marketgoblin._normalize import STATEMENT_FIELDS, STATEMENT_VARIANTS
from marketgoblin.classification import Classification, IndustryProfile, SectorProfile
from marketgoblin.ticker_metadata import TickerMetadata

logger = logging.getLogger(__name__)

_TIINGO_BASE_URL = "https://api.tiingo.com"
_FUNDAMENTALS_META_PATH = "/tiingo/fundamentals/meta"
_REQUEST_TIMEOUT_SECONDS = 10

# Window for the "latest row" lookups feeding fetch_metadata. 7 calendar days
# covers any normal long-weekend gap (≥ 5 trading days).
_LATEST_LOOKBACK_DAYS = 7

# Tiingo's daily endpoint serves USD prices on standard plans; metadata
# doesn't carry a currency field, so this is the documented default.
_DEFAULT_CURRENCY = "USD"

# Tiingo's get_fundamentals_daily endpoint exposes valuation metrics
# (marketCap, peRatio, pbRatio, ...) but NOT a shares-outstanding field —
# absolute shares live on the quarterly statements endpoint. We derive a
# daily shares series as round(marketCap / close) by joining the prices and
# fundamentals payloads.


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


_SLUG_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def slugify(value: str) -> str:
    """Lower-cased, hyphenated slug. ``"Information Technology"`` → ``"information-technology"``."""
    return _SLUG_NON_ALNUM.sub("-", value.lower()).strip("-")


def parse_tiingo_date_col(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Parse Tiingo's ISO date strings to ``pl.Date``.

    Tiingo returns ``"YYYY-MM-DDT00:00:00.000Z"``; sliced to the first 10 chars
    we get a clean ``YYYY-MM-DD`` parsable by polars.
    """
    return lf.with_columns(pl.col("date").str.slice(0, 10).str.to_date("%Y-%m-%d"))


def prices_rows_to_base_lf(rows: list[dict[str, Any]], symbol: str) -> pl.LazyFrame:
    """Wrap Tiingo's prices payload in a LazyFrame with a parsed date and uppercase symbol.

    Raises ``ValueError`` on empty input — the caller's ``_retry_fetch`` treats
    this as a non-transient error and propagates immediately.
    """
    if not rows:
        raise ValueError(f"No OHLCV data returned for {symbol}")
    return (
        pl.from_dicts(rows)
        .lazy()
        .pipe(parse_tiingo_date_col)
        .with_columns(pl.lit(symbol.upper()).alias("symbol"))
    )


def build_raw_ohlcv_lf(base: pl.LazyFrame) -> pl.LazyFrame:
    """Project the raw OHLCV variant from a parsed prices frame."""
    return base.select(
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "symbol",
        pl.lit(False).alias("is_adjusted"),
    )


def build_adjusted_ohlcv_lf(base: pl.LazyFrame) -> pl.LazyFrame:
    """Project the adjusted OHLCV variant, renaming ``adj*`` columns to canonical names."""
    return base.select(
        "date",
        pl.col("adjOpen").alias("open"),
        pl.col("adjHigh").alias("high"),
        pl.col("adjLow").alias("low"),
        pl.col("adjClose").alias("close"),
        pl.col("adjVolume").alias("volume"),
        "symbol",
        pl.lit(True).alias("is_adjusted"),
    )


def stack_ohlcv(adjusted: pl.LazyFrame, raw: pl.LazyFrame) -> pl.LazyFrame:
    """Concatenate adjusted + raw and sort to the on-disk row order."""
    return pl.concat([adjusted, raw]).sort(["date", "is_adjusted"])


def prices_rows_to_stacked_ohlcv(rows: list[dict[str, Any]], symbol: str) -> pl.LazyFrame:
    """Compose the full prices→stacked OHLCV pipeline."""
    base = prices_rows_to_base_lf(rows, symbol)
    return stack_ohlcv(build_adjusted_ohlcv_lf(base), build_raw_ohlcv_lf(base))


def prices_rows_to_dividends(rows: list[dict[str, Any]], symbol: str) -> pl.LazyFrame:
    """Extract dividend events (``divCash > 0``) from the prices payload."""
    base = prices_rows_to_base_lf(rows, symbol)
    return base.filter(pl.col("divCash") > 0).select(
        "date",
        pl.col("divCash").alias("dividend"),
        "symbol",
    )


def prices_rows_to_splits(rows: list[dict[str, Any]], symbol: str) -> pl.LazyFrame:
    """Extract split events (``splitFactor != 1.0``) from the prices payload.

    Tiingo emits ``splitFactor`` on every prices row; non-event days carry
    ``1.0``. The output frame is event-only — one row per actual split.
    """
    base = prices_rows_to_base_lf(rows, symbol)
    return base.filter(pl.col("splitFactor") != 1.0).select(
        "date",
        pl.col("splitFactor").alias("split_factor"),
        "symbol",
    )


# Wire-level schema for daily Fundamentals from_dicts. Declared explicitly so a
# missing field on a row materializes as a null column instead of crashing the
# projection; Tiingo skips ratios for tickers without earnings (pre-IPO
# history, ADRs).
_FUNDAMENTALS_DAILY_WIRE_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.String(),
    "marketCap": pl.Float64(),
    "enterpriseVal": pl.Float64(),
    "peRatio": pl.Float64(),
    "pbRatio": pl.Float64(),
    "trailingPEG1Y": pl.Float64(),
}


def fundamentals_daily_rows_to_lf(
    rows: list[dict[str, Any]],
    symbol: str,
) -> pl.LazyFrame:
    """Wrap Tiingo's daily-fundamentals payload in a typed LazyFrame.

    Output columns: ``date`` (pl.Date), ``market_cap`` (Float64 — cast to
    Int64 by ``normalize_fundamentals_daily``), ``enterprise_val`` (Float64),
    ``pe_ratio``, ``pb_ratio``, ``trailing_peg_1y`` (all Float64), and
    ``symbol`` (str). Missing fields on a row surface as null. Raises
    ``ValueError`` on empty input.
    """
    if not rows:
        raise ValueError(f"No fundamentals data returned for {symbol}")
    return (
        pl.from_dicts(rows, schema=_FUNDAMENTALS_DAILY_WIRE_SCHEMA)
        .lazy()
        .pipe(parse_tiingo_date_col)
        .with_columns(pl.lit(symbol.upper()).alias("symbol"))
        .select(
            "date",
            pl.col("marketCap").alias("market_cap"),
            pl.col("enterpriseVal").alias("enterprise_val"),
            pl.col("peRatio").alias("pe_ratio"),
            pl.col("pbRatio").alias("pb_ratio"),
            pl.col("trailingPEG1Y").alias("trailing_peg_1y"),
            "symbol",
        )
    )


# Tiingo dataCode for each on-disk statement field (see
# _normalize.STATEMENT_FIELDS). Spans all four statement sections — Tiingo
# ships incomeStatement / balanceSheet / cashFlow / overview in one payload, so
# full coverage costs no extra request. The base names here must exactly cover
# STATEMENT_FIELDS; a mismatch is caught at import below.
_TIINGO_STATEMENT_CODES: dict[str, str] = {
    # income statement
    "revenue": "revenue",
    "cost_of_revenue": "costRev",
    "gross_profit": "grossProfit",
    "operating_expenses": "opex",
    "sga": "sga",
    "rnd": "rnd",
    "operating_income": "opinc",
    "ebit": "ebit",
    "ebitda": "ebitda",
    "ebt": "ebt",
    "interest_expense": "intexp",
    "tax_expense": "taxExp",
    "net_income": "netinc",
    "net_income_common_stock": "netIncComStock",
    "net_income_disc_ops": "netIncDiscOps",
    "consolidated_income": "consolidatedIncome",
    "non_controlling_interests": "nonControllingInterests",
    "preferred_dividends": "prefDVDs",
    "eps_basic": "eps",  # Tiingo's basic-EPS code is "eps", not "epsBasic"
    "eps_diluted": "epsDil",
    "weighted_avg_shares": "shareswa",
    "weighted_avg_shares_diluted": "shareswaDil",
    # balance sheet
    "cash_and_eq": "cashAndEq",
    "accounts_receivable": "acctRec",
    "inventory": "inventory",
    "investments_current": "investmentsCurrent",
    "assets_current": "assetsCurrent",
    "ppe": "ppeq",
    "investments": "investments",
    "investments_non_current": "investmentsNonCurrent",
    "intangibles": "intangibles",
    "tax_assets": "taxAssets",
    "assets_non_current": "assetsNonCurrent",
    "total_assets": "totalAssets",
    "accounts_payable": "acctPay",
    "debt_current": "debtCurrent",
    "deferred_revenue": "deferredRev",
    "liabilities_current": "liabilitiesCurrent",
    "debt_non_current": "debtNonCurrent",
    "liabilities_non_current": "liabilitiesNonCurrent",
    "tax_liabilities": "taxLiabilities",
    "deposits": "deposits",
    "total_liabilities": "totalLiabilities",
    "total_debt": "debt",
    "equity": "equity",
    "retained_earnings": "retainedEarnings",
    "accumulated_oci": "accoci",
    "shares_basic": "sharesBasic",
    # cash flow
    "net_cash_ops": "ncfo",
    "net_cash_investing": "ncfi",
    "net_cash_financing": "ncff",
    "net_cash_flow": "ncf",
    "fx_effect_on_cash": "ncfx",
    "capex": "capex",
    "free_cash_flow": "freeCashFlow",
    "depreciation_amortization": "depamor",
    "stock_based_comp": "sbcomp",
    "dividends_paid": "payDiv",
    "issuance_repayment_debt": "issrepayDebt",
    "issuance_repayment_equity": "issrepayEquity",
    "business_acq_disposals": "businessAcqDisposals",
    "investments_acq_disposals": "investmentsAcqDisposals",
    # overview
    "book_value": "bookVal",
    "book_value_per_share": "bvps",
    "revenue_per_share": "rps",
    "roe": "roe",
    "roa": "roa",
    "gross_margin": "grossMargin",
    "profit_margin": "profitMargin",
    "current_ratio": "currentRatio",
    "debt_equity": "debtEquity",
    "long_term_debt_equity": "longTermDebtEquity",
    "piotroski_f_score": "piotroskiFScore",
    "revenue_qoq": "revenueQoQ",
    "eps_qoq": "epsQoQ",
    "share_factor": "shareFactor",
}

# The four sections Tiingo emits under statementData. Flattened into one row.
_STATEMENT_SECTIONS: tuple[str, ...] = (
    "incomeStatement",
    "balanceSheet",
    "cashFlow",
    "overview",
)

# Fail loud at import if the code map and on-disk schema have drifted apart —
# a typo or a forgotten field would otherwise surface as a silent all-null
# column or a KeyError deep in a fetch.
_SCHEMA_NAMES = {name for name, _ in STATEMENT_FIELDS}
_drift = _SCHEMA_NAMES ^ set(_TIINGO_STATEMENT_CODES)
if _drift:
    raise RuntimeError(
        f"Tiingo statement code map out of sync with _normalize.STATEMENT_FIELDS: {sorted(_drift)}"
    )

# A second guard: two fields pointing at the same Tiingo dataCode would both
# silently receive identical values (the flattener keys by code), with the
# name-only check above none the wiser. Easy to do by hand in a 76-entry dict.
_dupes = sorted(
    code
    for code in set(_TIINGO_STATEMENT_CODES.values())
    if list(_TIINGO_STATEMENT_CODES.values()).count(code) > 1
)
if _dupes:
    raise RuntimeError(f"Duplicate Tiingo dataCodes mapped to multiple statement fields: {_dupes}")


def _index_statement_codes(payload: dict[str, Any]) -> dict[str, Any]:
    """Flatten all of a quarter's statement sections into one ``{dataCode: value}`` dict.

    Each Tiingo statement section (incomeStatement, balanceSheet, cashFlow,
    overview) is a list of ``{dataCode, value}`` pairs. Codes are globally
    unique across sections, so collapsing them into a single dict is lossless.
    """
    data = payload.get("statementData", {}) or {}
    indexed: dict[str, Any] = {}
    for section in _STATEMENT_SECTIONS:
        for item in data.get(section, []) or []:
            code = item.get("dataCode")
            if code is not None:
                indexed[code] = item.get("value")
    return indexed


def _statement_payload_to_row(
    payload: dict[str, Any],
    *,
    suffix: str,
) -> dict[str, Any]:
    """Flatten one quarterly Tiingo statement payload into a single dict.

    Lifts every code in :data:`_TIINGO_STATEMENT_CODES` into a named column.
    ``suffix`` distinguishes the two endpoint variants when the same payload
    shape is fetched twice (asReported=True vs False) — values land in
    ``<name>_<suffix>`` so the eventual outer join can pivot both variants
    into a single row per quarter. Codes absent from a payload surface as
    ``None``.
    """
    indexed = _index_statement_codes(payload)
    row: dict[str, Any] = {
        "date": payload.get("date"),  # Filing date (ISO string)
        "fiscal_year": coerce_int(payload.get("year")),
        "fiscal_quarter": coerce_int(payload.get("quarter")),
    }
    for name, _dtype in STATEMENT_FIELDS:
        row[f"{name}_{suffix}"] = coerce_float(indexed.get(_TIINGO_STATEMENT_CODES[name]))
    return row


def _statements_wire_schema(suffix: str) -> dict[str, pl.DataType]:
    """Wire-level schema for one variant's from_dicts call. Declared
    explicitly so a missing field on a row materializes as a null column
    instead of crashing the projection."""
    schema: dict[str, pl.DataType] = {
        "date": pl.String(),
        "fiscal_year": pl.Int64(),
        "fiscal_quarter": pl.Int64(),
    }
    for name, _dtype in STATEMENT_FIELDS:
        schema[f"{name}_{suffix}"] = pl.Float64()
    return schema


# Variant suffixes are owned by _normalize.STATEMENT_VARIANTS — bind names to
# its members so the wire/flatten/join paths can't drift from the on-disk schema.
_AS_REPORTED_SUFFIX, _ADJUSTED_SUFFIX = STATEMENT_VARIANTS


def _one_variant_to_lf(
    rows: list[dict[str, Any]],
    suffix: str,
) -> pl.LazyFrame:
    """One asReported variant's rows → typed LazyFrame, before merging."""
    flattened = [_statement_payload_to_row(payload, suffix=suffix) for payload in rows]
    return (
        pl.from_dicts(flattened, schema=_statements_wire_schema(suffix))
        .lazy()
        .pipe(parse_tiingo_date_col)
    )


def statements_rows_to_lf(
    as_reported_rows: list[dict[str, Any]],
    adjusted_rows: list[dict[str, Any]],
    symbol: str,
) -> pl.LazyFrame:
    """Merge Tiingo's two statements variants into a single typed LazyFrame.

    Tiingo's ``/fundamentals/<ticker>/statements`` endpoint exposes
    point-in-time announced values (``asReported=True``) and latest restated
    values (``asReported=False``). PEAD/SUE wants both available in one frame
    so the strategy layer can A/B the variant choice without re-fetching;
    we issue one HTTP call per variant and outer-join on
    ``(fiscal_year, fiscal_quarter)``.

    The canonical ``date`` column is the as-reported filing date (what the
    market saw at announcement). When the as-reported call returned no row
    for a quarter that the adjusted call did surface, the adjusted call's
    filing date (still meaningful — last restatement) is used as fallback.

    Output columns: ``date`` (Date, filing date), ``fiscal_year`` /
    ``fiscal_quarter`` (Int), then every field in
    :data:`~marketgoblin._normalize.STATEMENT_FIELDS` as
    ``<name>_as_reported`` and ``<name>_adjusted`` (Float), and ``symbol``
    (str).

    Raises ``ValueError`` when both inputs are empty — a ticker with no
    quarterly history at all is a non-transient upstream condition.
    """
    if not as_reported_rows and not adjusted_rows:
        raise ValueError(f"No statements data returned for {symbol}")

    ar_lf = (
        _one_variant_to_lf(as_reported_rows, _AS_REPORTED_SUFFIX)
        if as_reported_rows
        else _empty_variant_lf(_AS_REPORTED_SUFFIX)
    )
    adj_lf = (
        _one_variant_to_lf(adjusted_rows, _ADJUSTED_SUFFIX)
        if adjusted_rows
        else _empty_variant_lf(_ADJUSTED_SUFFIX)
    )

    # Outer-join so quarters reported in one variant but not the other still
    # surface (common: very old history is sometimes only in restated form).
    # The only overlapping non-key column is ``date``, which collides into
    # ``date_adj_join``; every value field is variant-suffixed so both sides
    # survive untouched.
    merged = ar_lf.join(
        adj_lf,
        on=["fiscal_year", "fiscal_quarter"],
        how="full",
        suffix="_adj_join",
        coalesce=True,
    )
    # Backfill date from whichever side has it. As-reported wins on conflict —
    # that's the announcement-time fact PEAD cares about.
    variant_cols = [
        f"{name}_{variant}" for name, _dtype in STATEMENT_FIELDS for variant in STATEMENT_VARIANTS
    ]
    return (
        merged.with_columns(pl.coalesce("date", "date_adj_join").alias("date"))
        .drop("date_adj_join")
        .with_columns(pl.lit(symbol.upper()).alias("symbol"))
        .select("date", "fiscal_year", "fiscal_quarter", *variant_cols, "symbol")
    )


def _empty_variant_lf(suffix: str) -> pl.LazyFrame:
    """Empty frame with the wire schema — used when one variant returned no
    rows so the outer join still has both sides to operate on."""
    return (
        pl.from_dicts([], schema=_statements_wire_schema(suffix)).lazy().pipe(parse_tiingo_date_col)
    )


def derive_shares_from_marketcap(
    prices_rows: list[dict[str, Any]],
    fundamentals_rows: list[dict[str, Any]],
    symbol: str,
) -> pl.LazyFrame:
    """Join daily prices and fundamentals on date, derive ``shares = marketCap / close``.

    Tiingo's daily Fundamentals endpoint carries ``marketCap`` but no absolute
    shares field, so we recover shares from market cap and the same-day raw
    close. The resulting frame is daily-cadence, matching the shape Yahoo's
    ``get_shares_full`` produces (one row per trading day).

    Raises ``ValueError`` when either upstream payload is empty or no rows
    survive the join (no overlapping trading days).
    """
    if not prices_rows:
        raise ValueError(f"No price data returned for {symbol}")
    if not fundamentals_rows:
        raise ValueError(f"No fundamentals data returned for {symbol}")

    closes = pl.from_dicts(prices_rows).lazy().pipe(parse_tiingo_date_col).select("date", "close")
    market_caps = (
        pl.from_dicts(fundamentals_rows)
        .lazy()
        .pipe(parse_tiingo_date_col)
        .select("date", "marketCap")
        .filter(pl.col("marketCap") > 0)
    )

    # Materialize once: we need both an emptiness check (the join is the only
    # place the "no overlap" failure can be detected) and a LazyFrame for the
    # caller. Re-wrapping the eager result in .lazy() keeps the public contract
    # while paying for plan execution exactly once.
    derived = (
        closes.join(market_caps, on="date", how="inner")
        .filter(pl.col("close") > 0)
        .with_columns(
            (pl.col("marketCap") / pl.col("close")).round().cast(pl.Int64).alias("shares"),
            pl.lit(symbol.upper()).alias("symbol"),
        )
        .select("date", "shares", "symbol")
        .unique(subset=["date"], keep="last")
        .sort("date")
        .collect()
    )

    if derived.is_empty():
        raise ValueError(f"No overlapping trading days in prices+fundamentals for {symbol}")
    return derived.lazy()


def fetch_latest_close(client: Any, symbol: str) -> float | None:
    """Pull the most recent raw close price for a ticker.

    Used to derive ``shares_outstanding`` for ``TickerMetadata`` from the
    paired ``marketCap`` value. Restricts the call to a small lookback window
    so the response stays cheap.
    """
    today = datetime.now(tz=UTC).date()
    start = (today - timedelta(days=_LATEST_LOOKBACK_DAYS)).isoformat()
    rows = client.get_ticker_price(
        symbol.lower(),
        startDate=start,
        endDate=today.isoformat(),
        fmt="json",
        frequency="daily",
    )
    if not rows:
        return None
    return coerce_float(rows[-1].get("close"))


def fetch_latest_fundamentals(client: Any, symbol: str) -> dict[str, Any] | None:
    """Pull the most recent daily-fundamentals row for a ticker.

    Restricts the call to a small lookback window so the response stays cheap
    even on actively-traded tickers. Returns ``None`` when Tiingo has no recent
    rows (uncommon for liquid US equities, but possible for newly listed names).
    """
    today = datetime.now(tz=UTC).date()
    start = (today - timedelta(days=_LATEST_LOOKBACK_DAYS)).isoformat()
    rows = client.get_fundamentals_daily(
        symbol.lower(),
        startDate=start,
        endDate=today.isoformat(),
        fmt="json",
    )
    if not rows:
        return None
    return cast(dict[str, Any], rows[-1])


def build_tiingo_metadata(
    symbol: str,
    provider: str,
    meta: dict[str, Any],
    fundamentals_row: dict[str, Any] | None,
    latest_close: float | None,
    *,
    is_fast: bool,
) -> TickerMetadata:
    """Merge Tiingo's metadata + (optional) latest-fundamentals row into a TickerMetadata.

    ``shares_outstanding`` is derived from ``marketCap / latest_close`` because
    Tiingo's daily Fundamentals endpoint doesn't expose shares directly.

    Fields Tiingo doesn't expose (``isin``, ``beta``, ``forward_pe``, ``country``,
    ``timezone``, ``quote_type``) are left at their dataclass ``None`` default.
    """
    fundamentals = fundamentals_row or {}
    market_cap = coerce_int(first_present(fundamentals, "marketCap"))
    shares = (
        round(market_cap / latest_close)
        if market_cap is not None and latest_close is not None and latest_close > 0
        else None
    )

    return TickerMetadata(
        symbol=symbol,
        currency=first_present(meta, "currency") or _DEFAULT_CURRENCY,
        exchange=first_present(meta, "exchangeCode", "exchange"),
        name=first_present(meta, "name"),
        business_summary=first_present(meta, "description"),
        first_trade_date=first_present(meta, "startDate"),
        market_cap=market_cap,
        shares_outstanding=shares,
        trailing_pe=coerce_float(first_present(fundamentals, "peRatio", "trailingPE")),
        provider=provider,
        fetched_at=datetime.now(tz=UTC).isoformat(timespec="seconds"),
        is_fast=is_fast,
    )


def fetch_fundamentals_meta(symbol: str, api_key: str | None) -> dict[str, Any]:
    """GET ``/tiingo/fundamentals/meta`` for a single ticker.

    Returns the first list element, or ``{}`` when Tiingo returns an empty list
    (so :func:`build_tiingo_classification` degrades to a Classification with
    null sub-profiles instead of raising).
    """
    response = requests.get(
        f"{_TIINGO_BASE_URL}{_FUNDAMENTALS_META_PATH}",
        params={"tickers": symbol.lower()},
        headers={"Authorization": f"Token {api_key}"},
        timeout=_REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    if not payload:
        return {}
    return cast(dict[str, Any], payload[0])


def build_tiingo_classification(
    symbol: str,
    provider: str,
    meta_row: dict[str, Any],
) -> Classification:
    """Build a Classification from a /fundamentals/meta row.

    Tiingo doesn't expose constituent data (top companies, ETFs, market cap),
    so the SectorProfile / IndustryProfile sub-fields stay at their defaults.
    """
    sector_name = first_present(meta_row, "sector")
    industry_name = first_present(meta_row, "industry")

    sector = SectorProfile(key=slugify(sector_name), name=sector_name) if sector_name else None
    industry = (
        IndustryProfile(
            key=slugify(industry_name),
            name=industry_name,
            sector_key=slugify(sector_name) if sector_name else None,
            sector_name=sector_name,
        )
        if industry_name
        else None
    )

    return Classification(
        symbol=symbol,
        sector=sector,
        industry=industry,
        provider=provider,
        fetched_at=datetime.now(tz=UTC).isoformat(timespec="seconds"),
    )
