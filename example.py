"""Runnable example demonstrating core MarketGoblin functionality."""

import logging
import tempfile

from marketgoblin import MarketGoblin

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

PROVIDER = "yahoo"
SYMBOLS = ["AAPL", "MSFT", "GOOGL"]
START = "2024-01-01"
END = "2024-03-31"

with tempfile.TemporaryDirectory() as save_path:
    vault = MarketGoblin(provider=PROVIDER, save_path=save_path)

    # Single fetch — saves to disk, returns LazyFrame
    print("\n--- Single fetch ---")
    lf = vault.fetch("AAPL", START, END, parse_dates=True)
    df = lf.collect()
    print(df.head())

    # Load back from disk
    print("\n--- Load from disk ---")
    lf2 = vault.load("AAPL", START, END, parse_dates=True)
    print(lf2.collect().head())

    # Batch fetch
    print("\n--- Batch fetch ---")
    results = vault.fetch_many(SYMBOLS, START, END)
    for symbol, lf in results.items():
        rows = lf.collect().height
        print(f"  {symbol}: {rows} rows")
