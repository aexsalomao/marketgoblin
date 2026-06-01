# Read a newline-delimited ticker list, fetch Yahoo classifications via marketgoblin,
# and write a (ticker, sector) parquet. Runs inside marketgoblin's own venv.
#
# Failures are logged but do not halt the run — the sanity check only needs a
# subset of the corpus to have sector labels.

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import polars as pl

from marketgoblin import MarketGoblin

logger = logging.getLogger("build_sector_map")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tickers-in", type=Path, required=True)
    parser.add_argument("--sector-map-out", type=Path, required=True)
    parser.add_argument("--save-path", type=Path, default=Path.home() / ".mg-cache")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    tickers = [t.strip() for t in args.tickers_in.read_text().splitlines() if t.strip()]
    logger.info("fetching classification for %d tickers", len(tickers))

    goblin = MarketGoblin("yahoo", save_path=args.save_path)

    rows: list[dict[str, str]] = []
    ok = 0
    miss = 0
    failed = 0
    for i, ticker in enumerate(tickers, 1):
        try:
            classification = goblin.load_classification(ticker)
        except (FileNotFoundError, RuntimeError):
            try:
                classification = goblin.fetch_classification(ticker)
            except Exception as e:
                failed += 1
                if failed <= 10:
                    logger.warning("fetch failed for %s: %s", ticker, e)
                continue

        sector_key = classification.sector.key if classification.sector else None
        if sector_key is None:
            miss += 1
            continue

        rows.append({"ticker": ticker, "sector": sector_key})
        ok += 1

        if i % 50 == 0:
            logger.info("progress %d / %d (ok=%d miss=%d failed=%d)", i, len(tickers), ok, miss, failed)

    df = pl.DataFrame(rows, schema={"ticker": pl.String, "sector": pl.String})
    args.sector_map_out.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(args.sector_map_out)
    logger.info(
        "wrote %d rows to %s (ok=%d miss=%d failed=%d)",
        df.height, args.sector_map_out, ok, miss, failed,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
