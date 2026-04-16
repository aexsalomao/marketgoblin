"""
Runnable walkthrough of core MarketGoblin functionality.

Demonstrates:
  - Single-symbol fetch + disk persistence
  - Loading saved data back from disk
  - Inspecting the DataFrame schema and a metadata sidecar
  - Batch fetch with fetch_many()
"""

import json
import logging
import tempfile
from pathlib import Path

from marketgoblin import MarketGoblin

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

PROVIDER = "yahoo"
SYMBOLS = ["AAPL", "MSFT", "GOOGL"]
START = "2024-01-01"
END = "2024-03-31"

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

with tempfile.TemporaryDirectory() as tmp:
    save_path = Path(tmp)
    goblin = MarketGoblin(provider=PROVIDER, save_path=save_path)

    # -----------------------------------------------------------------------
    # 1. Single fetch — downloads from Yahoo, persists monthly .pq slices
    # -----------------------------------------------------------------------
    print("\n=== 1. Single fetch (AAPL) ===")
    lf = goblin.fetch("AAPL", START, END, parse_dates=True)
    df = lf.collect()
    print(df)
    print(f"\nSchema: {df.schema}")

    # -----------------------------------------------------------------------
    # 2. Load back from disk
    # -----------------------------------------------------------------------
    print("\n=== 2. Load from disk (AAPL) ===")
    lf2 = goblin.load("AAPL", START, END, parse_dates=True)
    df2 = lf2.collect()
    print(df2)

    # -----------------------------------------------------------------------
    # 3. Inspect a JSON metadata sidecar
    # -----------------------------------------------------------------------
    print("\n=== 3. Metadata sidecar (first slice) ===")
    sidecars = sorted(save_path.rglob("*.json"))
    if sidecars:
        meta = json.loads(sidecars[0].read_text())
        for key, value in meta.items():
            print(f"  {key}: {value}")

    # -----------------------------------------------------------------------
    # 4. Batch fetch — failed symbols are logged, never crash the batch
    # -----------------------------------------------------------------------
    print("\n=== 4. Batch fetch ===")
    results = goblin.fetch_many(SYMBOLS, START, END)
    for symbol, lf in results.items():
        rows = lf.collect().height
        print(f"  {symbol}: {rows} rows")
