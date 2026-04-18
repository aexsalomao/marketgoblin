"""
Runnable walkthrough of core MarketGoblin functionality.

Demonstrates:
  - Single-symbol OHLCV fetch + disk persistence
  - Loading saved data back from disk
  - Inspecting the DataFrame schema and a metadata sidecar
  - Batch fetch with fetch_many()
  - Shares-outstanding fetch (separate dataset)
"""

import json
import logging
import tempfile
from pathlib import Path

from marketgoblin import Dataset, MarketGoblin

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
    print(f"Supported datasets: {sorted(d.value for d in goblin.supported_datasets)}")

    # -----------------------------------------------------------------------
    # 1. Single OHLCV fetch — downloads from Yahoo, persists monthly .pq slices
    # -----------------------------------------------------------------------
    print("\n=== 1. Single OHLCV fetch (AAPL) ===")
    lf = goblin.fetch("AAPL", START, END, parse_dates=True)
    df = lf.collect()
    print(df)
    print(f"\nSchema: {df.schema}")

    # -----------------------------------------------------------------------
    # 2. Load OHLCV back from disk
    # -----------------------------------------------------------------------
    print("\n=== 2. Load OHLCV from disk (AAPL) ===")
    lf2 = goblin.load("AAPL", START, END, parse_dates=True)
    df2 = lf2.collect()
    print(df2)

    # -----------------------------------------------------------------------
    # 3. Inspect a JSON metadata sidecar
    # -----------------------------------------------------------------------
    print("\n=== 3. Metadata sidecar (first slice) ===")
    sidecars = sorted(save_path.rglob("ohlcv/**/*.json"))
    if sidecars:
        meta = json.loads(sidecars[0].read_text())
        for key, value in meta.items():
            print(f"  {key}: {value}")

    # -----------------------------------------------------------------------
    # 4. Batch OHLCV fetch — failed symbols are logged, never crash the batch
    # -----------------------------------------------------------------------
    print("\n=== 4. Batch OHLCV fetch ===")
    results = goblin.fetch_many(SYMBOLS, START, END)
    for symbol, lf in results.items():
        rows = lf.collect().height
        print(f"  {symbol}: {rows} rows")

    # -----------------------------------------------------------------------
    # 5. Shares-outstanding fetch — sparse, irregular cadence
    # -----------------------------------------------------------------------
    print("\n=== 5. Shares fetch (AAPL) ===")
    shares_lf = goblin.fetch("AAPL", START, END, dataset=Dataset.SHARES, parse_dates=True)
    shares_df = shares_lf.collect()
    print(shares_df)
    print(f"Schema: {shares_df.schema}")
