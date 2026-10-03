# Package validation, 2026-10-03

The execution components and downloader passed **81 tests** in the Windows research
environment and in a clean Linux Python 3.13 virtual environment installed
from this package. Linux versions are recorded in
`../requirements-tested-linux.txt`; numpy 1.26.4 was used for Windows tests.
These checks establish the specifically declared source comparisons and
guards, not blanket live-account parity.

* Maker: 55 tests, including 42 saved source-generated golden comparisons
  across all 14 modes, independent production queue/reference comparisons,
  footprint and through-trade diagnostics, source and fixture fingerprints.
* Taker: 22 tests, including 483 archived A2 orders at 16 delays, **7,728** exact
  shares/principal comparisons against both original mathematics and saved
  outputs, seven mathematical AST fingerprints, original event-book prefix
  comparisons, budget/fee/latency/unknown guards and synthetic Binance L2
  mechanics. No external restricted quote data is in the fixtures.
* Download safety: four checks accept ordinary nested files and reject
  traversal, absolute paths and symlink archive members.
* Actual data integration: one BTC5 and one BTC15 contract, strict venue-before
  taker book, a $25 BUY budget, and a separate 25-share BUY resting below the
  ask for maker queue replay. Both engines ran with quantity/cost bounds.
  These orders are synthetic integration cases, not BoneOhio trades.
* Baseline: 7,680 prespecified fixed-time paired-outcome observations across
  768 contracts; 6,340 pass declared current-state/end-point availability and
  observed flow-ID-gap checks. The fresh wallet snapshot contains 2,256 BTC
  BUY rows; all-asset counts and chain identity audits are separate context.
  Native aggTrade duplicate IDs removed: zero in this export.

The baseline's rank associations of price with later API-dated BUY activity
are approximately .140 on September 30 and .145 on October 1; directional
Binance move associations are .131 and .077. These are descriptive,
overlapping observations, with censored negatives and a 5-second API label
guard that does not eliminate API timing uncertainty. They do not establish
an entry rule, statistical independence, actual venue reaction or profit.
The script reproduces these values and saves the full denominator table.

All **3,917 public data files** passed byte length, SHA256 and declared Parquet
row-count checks; all 768 contracts are present. Feature-source timestamps are
strictly before their query on the declared cache axis; unavailable prior
states are null. Independently licensed lifecycle metadata verifies all
**773** core/diagnostic contract identities, including both token outcomes.

The Polymarket 100ms feature view has 83.4056% seeded/fresh/uncrossed states on
the normalized archive clock. Unknown prior states have null source values.
These checks do not certify a complete order stream or BoneOhio's clock.
Raw exports were verified after ZSTD rewrite with Arrow equality and source
stat/hash checks; per-file fingerprints and release checksums support
independent revalidation. Fresh public-wallet page/chain-link audits are in
`../context/VALIDATION.json` and its linked provenance files.

Reproduce locally:

```bash
python -m pip install -e '.[test]'
python -m pytest -q
python scripts/fetch_data.py --groups all
python scripts/validate_bundle.py --require-all
python scripts/run_baseline.py
python scripts/run_replay_smoke.py
```
