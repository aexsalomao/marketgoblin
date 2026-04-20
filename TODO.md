# TODO

## Sector / Industry → Index Mappings

Static, refreshable mappings of sector (and eventually industry) to the
representative ETF + index ticker for a given market. Parser is private; the
shipped JSON under `src/marketgoblin/_sector_indices_data/` is what the public
API exposes.

### Coverage roadmap

- [x] **Phase 1 — US sectors.** S&P 500 GICS sectors → SPDR Select Sector
      ETFs + S&P 500 sector index symbols (11 sectors). Output: `us.json`.
- [x] **Phase 2 — US sub-industries within sectors.** Added GICS Sub-Industry
      (level 4) counts per sector, scraped from the S&P 500 constituents page.
      The intermediate GICS Industry Group (~25) and Industry (~74) levels
      were deferred — they aren't exposed on the constituents page and need
      a separate curated taxonomy source (see Phase 2.5).
- [x] **Phase 2.5 — Intermediate GICS levels.** Added full 4-level GICS
      taxonomy (sector → industry group → industry → sub-industry) as a
      curated JSON (`gics_taxonomy_us.json`, post-2023 GICS). Parser joins
      scraped constituents against the taxonomy and rolls up counts at
      every level; unknown upstream sub-industries fail loud. Still
      deferred: per-industry ticker representation — not every GICS
      industry maps to a clean tradable product.
- [ ] **Phase 3 — Multiple US index families.** Extend beyond S&P 500 to
      Nasdaq-100, Russell 1000/2000, Dow 30, each with their own sector
      breakdowns and representative products.

### International markets (not scoped yet)

- [ ] **UK / Europe** — ICB taxonomy (FTSE 100/250, STOXX Europe 600).
- [ ] **Brazil** — B3 sector classification, IBOV sub-indices.
- [ ] **Japan** — TOPIX-17 / TOPIX-33 sector indices.

### Parser hardening

- [ ] Retry + backoff on HTTP fetch (currently one shot).
- [ ] Surface a warning when upstream sector list diverges from the static
      `_US_SECTOR_TICKERS` table (new sector, renamed sector).
- [ ] Cache raw HTML locally during development to avoid hammering Wikipedia
      while iterating on the parser.
- [ ] Verify the curated GICS taxonomy (`gics_taxonomy_us.json`) against
      the MSCI published list — it was seeded from public GICS knowledge
      and may have minor code/name drift vs. the latest MSCI revision.
