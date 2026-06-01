# Providers & Capabilities

marketgoblin talks to data sources through a uniform `MarketGoblin` facade. Each
provider supports a different slice of the catalog. This page is the map: pick a
provider, see exactly what it can fetch.

```python
from marketgoblin import MarketGoblin, Dataset

goblin = MarketGoblin(provider="yahoo")          # or "tiingo"
print(sorted(d.value for d in goblin.supported_datasets))
```

`goblin.supported_datasets` always reflects the live truth for the configured
provider — the tables below are the same information, written down.

---

## Provider summary

| Provider | Backend | Auth | Best for |
|---|---|---|---|
| `yahoo` | `yfinance` | None | Free OHLCV, shares, dividends, metadata, sector/industry |
| `tiingo` | `tiingo` client + REST | **API key** | Full catalog incl. splits & fundamentals (paid endpoints) |

`tiingo` reads its key from the `api_key=` constructor argument or the
`TIINGO_API_KEY` environment variable (auto-loaded from a `.env` on import).

---

## Dataset support matrix

Selected via the `dataset=` argument to `fetch()` / `load()` / `fetch_many()`
(default `Dataset.OHLCV`).

| `Dataset` | `yahoo` | `tiingo` | Notes |
|---|:---:|:---:|---|
| `OHLCV` | ✅ | ✅ | Tidy **stacked** frame — every day appears twice (`is_adjusted=True`/`False`). |
| `SHARES` | ✅ | ✅ | Sparse, corporate-action cadence. Tiingo derives `shares = round(marketCap / close)`. |
| `DIVIDENDS` | ✅ | ✅ | Event-driven cash dividends, filtered to `[start, end]`. |
| `SPLITS` | ❌ | ✅ | Event-driven `split_factor` (e.g. `2.0` = 2-for-1, `0.5` = reverse). |
| `FUNDAMENTALS_DAILY` | ❌ | ✅ | Daily `market_cap`, `enterprise_val`, `pe_ratio`, `pb_ratio`, `trailing_peg_1y`. **Paid.** |
| `FUNDAMENTALS_STATEMENTS` | ❌ | ✅ | Quarterly EPS (diluted/basic × as-reported/adjusted) + revenue. **Paid.** |

Requesting an unsupported dataset raises `ValueError` at the dispatch layer.

## Non-dataset features

These return typed dataclasses (not frames) and sit outside the `Dataset` enum.

| Feature | Method | `yahoo` | `tiingo` |
|---|---|:---:|:---:|
| Ticker metadata | `fetch_metadata()` / `load_metadata()` | ✅ | ✅ |
| Sector / industry classification | `fetch_classification()` / `load_classification()` | ✅ | ✅ |

The `load_*` variants require `save_path` to be set (else `RuntimeError`).

### Provider-agnostic helpers

Not tied to any provider — importable straight from the package root:

| Helper | Returns | Purpose |
|---|---|---|
| `load_sector_indices(market="US")` | `SectorIndexMapping` | Read the shipped sector → index hierarchy (GICS-style). |
| `refresh_sector_indices(market="US")` | `SectorIndexMapping` | Re-parse and rewrite the shipped mapping. |

---

## On-disk schema by dataset

Persisted only when `save_path` is set. Layout is uniform:
`{save_path}/{provider}/{dataset}/{SYMBOL}/{SYMBOL}_{YYYY-MM}.pq` with a `.json`
sidecar next to each slice. `date` is stored as `int32` YYYYMMDD; pass
`parse_dates=True` to recover `pl.Date`.

| Dataset | Columns (besides `date`, `symbol`) |
|---|---|
| `OHLCV` | `open`, `high`, `low`, `close` (`float32`), `volume` (`int64`), `is_adjusted` (`bool`) |
| `SHARES` | `shares` (`int64`) |
| `DIVIDENDS` | `dividend` (`float32`) |
| `SPLITS` | `split_factor` (`float32`) |
| `FUNDAMENTALS_DAILY` | `market_cap`, `enterprise_val` (`int64`); `pe_ratio`, `pb_ratio`, `trailing_peg_1y` (`float32`) |
| `FUNDAMENTALS_STATEMENTS` | `fiscal_year` (`int16`), `fiscal_quarter` (`int8`), `eps_{diluted,basic}_{as_reported,adjusted}` (`float32`), `revenue` (`float64`) |

Metadata and classification are point-in-time (no date axis) and stored as a
single JSON each: `{save_path}/{provider}/{metadata,classification}/{SYMBOL}.json`.

---

## Choosing a provider

- **Just want prices, for free?** → `yahoo`. OHLCV + shares + dividends + metadata,
  no key required.
- **Need splits or fundamentals?** → `tiingo` with a paid key. It's the only
  provider covering the full catalog.

See the [API Reference](api.md) for full signatures, and the companion notebook
(`notebooks/marketgoblin_walkthrough.ipynb`) for runnable examples of every
feature above.
