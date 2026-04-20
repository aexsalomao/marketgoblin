---
name: parquet-slice-check
description: Verify the on-disk Parquet slice invariants after changes to `DiskStorage`, `_normalize`, `_metadata`, `goblin.fetch/fetch_many`, or any `BaseSource` subclass. Use when the user touches storage/normalization code, adds a new dataset, or adds a new provider in marketgoblin.
---

# Parquet Slice Sanity Check

marketgoblin writes **monthly Parquet slices** with a **JSON sidecar** per slice. Path scheme (current):

```
{provider}/{dataset}/{SYMBOL}/{SYMBOL}_{YYYY-MM}.pq
{provider}/{dataset}/{SYMBOL}/{SYMBOL}_{YYYY-MM}.json
```

Where `dataset ∈ {ohlcv, shares, dividends}`. OHLCV encodes adjusted/raw in an `is_adjusted: bool` column — there is **no** `adjusted|raw` path segment for any dataset.

## When to invoke

Automatically, when any of these are touched:
- `src/marketgoblin/storage/*.py`
- `src/marketgoblin/_normalize.py`, `_metadata.py`
- `src/marketgoblin/goblin.py` (the `fetch` / `fetch_many` / `load` methods)
- `src/marketgoblin/sources/*.py` (new or modified provider)
- `src/marketgoblin/datasets.py`

## Invariants to verify

Run the public API against a tmp dir and check that the artifacts produced satisfy these:

### Path & filename

- One `.pq` file per `(provider, dataset, symbol, year-month)`.
- Each `.pq` has a matching `.json` sidecar with the same stem.
- Filenames are `{SYMBOL}_{YYYY-MM}.pq`, symbol **uppercase**, `YYYY-MM` zero-padded.
- Writes are **atomic**: no `.pq.tmp` or partial files left after a crash simulation.

### Schema

- `date` column is `Int32` (YYYYMMDD) on disk — not `pl.Date`.
- OHLCV has `is_adjusted: Bool` and the slice contains both variants stacked.
- Shares dataset: one row per day (Yahoo's sparse series is deduplicated).
- Dividends dataset: event rows only, strictly within the requested date window.
- No nulls in `date` or `symbol`.

### Loader round-trip

- `load(symbol, dataset=...)` returns a `pl.LazyFrame`.
- `load(..., parse_dates=True)` yields `pl.Date`; default is `Int32`.
- Loading after `fetch` produces a frame whose row count and `date` range match what was fetched.

### Sidecar metadata

- Sidecar JSON includes `provider`, `dataset`, `symbol`, `start_date`, `end_date`, `row_count`.
- `row_count` matches the slice's actual row count.
- `start_date`/`end_date` match the min/max of the `date` column.

### Dispatch boundary

- Unsupported `(provider, dataset)` pairs raise at the dispatch boundary, **not** after I/O. Add/extend a test if the new code introduces a new pair.

### Rate limiting & retries (when `sources/*` is touched)

- `fetch_many` respects the requests-per-second cap (default 2).
- `YahooSource` retries transient failures 3× with exponential backoff.

## How to run the check

1. Write a short `pytest` that exercises `MarketGoblin.fetch` / `fetch_many` / `load` against `tmp_path` with a stub `BaseSource` that returns a deterministic frame.
2. Assert each invariant above that's relevant to the changed area.
3. Run `pytest -k "slice or storage or parquet"` to confirm existing coverage still passes.
4. If a new provider was added, also add a round-trip test in `tests/sources/test_<provider>.py` using `CSVSource`'s pattern as the reference.

## What not to do

- Don't mock `polars` or the filesystem — use `tmp_path` and a fake `BaseSource`. See `.claude/rules/testing.md` §27 (prefer fakes over mocks).
- Don't assert on internal helper calls — assert on the files produced and what `load` returns.
- Don't skip the atomicity check by assuming "the write succeeded, it must be fine." Atomic writes are easy to break when refactoring.
