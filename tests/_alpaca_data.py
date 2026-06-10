# Shared Alpaca JSON-shape builders for the Alpaca source + parser tests.
# Mirrors the raw Data API v2 /trades payload (keys t/x/p/s/c/i/z, paginated
# via next_page_token).

from typing import Any


def make_trade_rows() -> list[dict[str, Any]]:
    """Three raw trades across two trading days in May 2026 (one month)."""
    return [
        {
            "t": "2026-05-01T13:30:00.111111111Z",
            "x": "V",
            "p": 500.10,
            "s": 100,
            "c": ["@"],
            "i": 1,
            "z": "B",
        },
        {
            "t": "2026-05-01T13:30:01.222222222Z",
            "x": "V",
            "p": 500.20,
            "s": 50,
            "c": ["@", "I"],
            "i": 2,
            "z": "B",
        },
        {
            "t": "2026-05-04T14:00:00.333333333Z",
            "x": "D",
            "p": 501.00,
            "s": 200,
            "c": ["@"],
            "i": 3,
            "z": "B",
        },
    ]


def make_trades_response(
    rows: list[dict[str, Any]] | None = None,
    next_page_token: str | None = None,
) -> dict[str, Any]:
    """Wrap trade rows in the Data API envelope ({trades, symbol, next_page_token})."""
    return {
        "trades": rows if rows is not None else make_trade_rows(),
        "symbol": "SPY",
        "next_page_token": next_page_token,
    }
