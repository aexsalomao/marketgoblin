# Shared Tiingo JSON-shape builders for the Tiingo parser/orchestration suites.
# Plain factory functions (not fixtures) so they can be called inline and
# composed; imported by test_tiingo*.py.

from typing import Any


def make_prices_rows() -> list[dict[str, Any]]:
    return [
        {
            "date": "2024-01-02T00:00:00.000Z",
            "open": 100.0,
            "high": 105.0,
            "low": 95.0,
            "close": 100.0,
            "volume": 80_000_000,
            "adjOpen": 97.0,
            "adjHigh": 101.85,
            "adjLow": 92.15,
            "adjClose": 97.0,
            "adjVolume": 80_000_000,
            "divCash": 0.0,
            "splitFactor": 1.0,
        },
        {
            "date": "2024-01-03T00:00:00.000Z",
            "open": 110.0,
            "high": 115.0,
            "low": 108.0,
            "close": 110.0,
            "volume": 75_000_000,
            "adjOpen": 106.7,
            "adjHigh": 111.55,
            "adjLow": 104.76,
            "adjClose": 106.7,
            "adjVolume": 75_000_000,
            "divCash": 0.24,
            "splitFactor": 1.0,
        },
    ]


def make_prices_rows_with_split() -> list[dict[str, Any]]:
    """Two trading days, the second carrying a 4-for-1 split."""
    return [
        {
            "date": "2020-08-28T00:00:00.000Z",
            "open": 500.0,
            "high": 510.0,
            "low": 495.0,
            "close": 506.0,
            "volume": 50_000_000,
            "adjOpen": 125.0,
            "adjHigh": 127.5,
            "adjLow": 123.75,
            "adjClose": 126.5,
            "adjVolume": 200_000_000,
            "divCash": 0.0,
            "splitFactor": 1.0,
        },
        {
            "date": "2020-08-31T00:00:00.000Z",
            "open": 127.0,
            "high": 130.0,
            "low": 126.0,
            "close": 129.0,
            "volume": 200_000_000,
            "adjOpen": 127.0,
            "adjHigh": 130.0,
            "adjLow": 126.0,
            "adjClose": 129.0,
            "adjVolume": 200_000_000,
            "divCash": 0.0,
            "splitFactor": 4.0,
        },
    ]


def make_statements_rows_as_reported() -> list[dict[str, Any]]:
    """Tiingo's asReported=True payload — point-in-time announced values.

    Carries codes from all four statement sections so tests exercise the full
    cross-section flattening, not just the income statement. Tiingo's basic-EPS
    code is ``eps`` (not ``epsBasic``).
    """
    return [
        {
            "date": "2024-08-01T00:00:00.000Z",
            "year": 2024,
            "quarter": 3,
            "statementData": {
                "incomeStatement": [
                    {"dataCode": "epsDil", "value": 1.40},
                    {"dataCode": "eps", "value": 1.41},
                    {"dataCode": "revenue", "value": 85_777_000_000},
                ],
                "balanceSheet": [{"dataCode": "totalAssets", "value": 331_612_000_000}],
                "cashFlow": [{"dataCode": "freeCashFlow", "value": 26_700_000_000}],
                "overview": [{"dataCode": "roe", "value": 0.32}],
            },
        },
        {
            "date": "2024-05-02T00:00:00.000Z",
            "year": 2024,
            "quarter": 2,
            "statementData": {
                "incomeStatement": [
                    {"dataCode": "epsDil", "value": 1.53},
                    {"dataCode": "eps", "value": 1.54},
                    {"dataCode": "revenue", "value": 90_753_000_000},
                ],
            },
        },
    ]


def make_statements_rows_adjusted() -> list[dict[str, Any]]:
    """Tiingo's asReported=False payload — latest restated / adjusted values.
    Slightly different EPS to exercise the variant-merging join."""
    return [
        {
            "date": "2024-08-01T00:00:00.000Z",
            "year": 2024,
            "quarter": 3,
            "statementData": {
                "incomeStatement": [
                    {"dataCode": "epsDil", "value": 1.42},
                    {"dataCode": "eps", "value": 1.43},
                    {"dataCode": "revenue", "value": 85_777_000_000},
                ],
            },
        },
        {
            "date": "2024-05-02T00:00:00.000Z",
            "year": 2024,
            "quarter": 2,
            "statementData": {
                "incomeStatement": [
                    {"dataCode": "epsDil", "value": 1.55},
                    {"dataCode": "eps", "value": 1.56},
                    {"dataCode": "revenue", "value": 90_753_000_000},
                ],
            },
        },
    ]


def make_fundamentals_rows() -> list[dict[str, Any]]:
    # Tiingo's get_fundamentals_daily returns valuation metrics only — no
    # shares field. marketCap is the anchor we use to derive shares.
    return [
        {
            "date": "2024-01-02T00:00:00.000Z",
            "marketCap": 1_500_000_000_000,
            "enterpriseVal": 1_550_000_000_000,
            "peRatio": 32.5,
            "pbRatio": 50.0,
            "trailingPEG1Y": 2.0,
        },
        {
            "date": "2024-01-03T00:00:00.000Z",
            "marketCap": 1_650_000_000_000,
            "enterpriseVal": 1_700_000_000_000,
            "peRatio": 32.6,
            "pbRatio": 50.1,
            "trailingPEG1Y": 2.0,
        },
    ]


def make_metadata_dict() -> dict[str, Any]:
    return {
        "ticker": "AAPL",
        "name": "Apple Inc.",
        "exchangeCode": "NASDAQ",
        "startDate": "1980-12-12",
        "endDate": "2024-03-31",
        "description": "Apple makes consumer electronics.",
    }


def make_meta_payload() -> list[dict[str, Any]]:
    return [
        {
            "ticker": "AAPL",
            "name": "Apple Inc.",
            "sector": "Information Technology",
            "industry": "Technology Hardware Storage & Peripherals",
            "sicCode": "3571",
            "sicSector": "Manufacturing",
            "sicIndustry": "Electronic Computers",
        }
    ]
