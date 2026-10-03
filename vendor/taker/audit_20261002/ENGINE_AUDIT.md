# TailTaker source audit and isolated candidate

Original source: `<SOURCE_HOME>/tailtaker_20261001_FINAL/exec_exact.py` and
`parity.py`, copied locally by the root agent. Originals remain unchanged.
This subtask inspected their full source, fingerprinted `p3_v57_model.json`, and read the shared
`SPEC.md`, `PLAN.md`, `CONTRACT.md` and contract implementation. No remote
source read or backtest succeeded in this agent: Remote Desktop Commander calls
hung twice; the parent supplied the local snapshots and owns source cluster compute.

## Proven source defects and limitations

1. **Outcome leakage in forced request size.** `parity.py:103` chooses actual
   `making` when positive, otherwise `principal_usd`, otherwise $1. A partially
   filled $10 order spending $2 is replayed as a $2 request. The candidate uses
   only original `principal_usd`; missing intent principal stays unknown.
   This proves a faulty comparison input, not the economic impact on this cohort
   until the real replay is run. Actual signed-order amount semantics still need
   deployed client/source verification.
2. **Missing depth becomes an ordinary fill/no-fill.** `exec_exact.py:19` limits
   the sweep to four levels; `184–198` breaks on invalid/missing depth and records
   either accumulated size or zero with no unknown-evidence marker.
   `parity.py:89–100` repeats this behavior. Candidate diagnostics preserve a
   known prefix as incomplete and represent entirely missing books as null;
   they use all supplied contiguous levels and do not jump an unknown level.
3. **Matching time is moved silently.** `exec_exact.py:30–32,142–149` snaps the
   requested delay to the nearest grid, including upward to a later book.
   A 201 ms input becomes 200 ms; a 214 ms input becomes 225 ms. The diagnostic
   requires the exact requested grid column, with no implicit snap. Fixed 200 ms
   is a declared scenario and does not establish the observed match clock.
4. **Wrong boundary clock and obsolete alternate regime.** `144–145` applies
   the dated hold at decision time, not ingress. The `regime_tape` branch retains
   the superseded September 10 switch. The shared contract handles ingress and
   exact official boundary timestamps; it never adds a hold to supplied total
   latency a second time. The candidate forced replay uses total latency only.
5. **No shared wallet or own-depth state.** `153–198` has only per-contract
   counters/principal/share caps. There is no cash, confirmed balance timestamp,
   reservation, timeout, allowance, per-wallet chronology or stable liquidity
   identity. Each intent reads its own untouched snapshot. Source alone cannot
   prove which actual episode reused liquidity or unavailable cash. The shared
   contract and `execute_prepared` adapter provide a strict path for an explicit
   share-sized intent/book/account; public aggregate views cannot fabricate those
   inputs and therefore remain unqualified.
6. **Side-ambiguous join risk.** `parity.py:76` joins time and contract without
   side while `exec_exact.py:40` uses `is_yes` too. Candidate refuses multiple
   matching rows and selects the tail side opposite the logged favourite.
   The parent-provided compute schema probe reports 745,539 views, 1,864 orders
   and zero duplicate time/contract view keys, so this is not an observed duplicate
   defect in the saved cohort. `exact_book.py` explicitly sets `tail_y` from
   `is_yes`, `fav_y=not tail_y`, and emits that favourite ladder: candidate
   orientation is source-backed.
7. **Fee policy and type scope are hard-coded.** `exec_exact.py:190` computes
   `.07*p*(1-p)*quantity` separately per level, and `196` subtracts its cash value.
   This source **does not have the VWAP fee formula defect**. It lacks dated
   fee collection/rounding metadata, market minima/tick validation and FOK
   atomicity; it describes a FAK strategy. Historical share-vs-cash collection
   cannot be resolved from that formula alone. Do not assert Tail sent FOK.
8. `book_age_ms` and `btc_source_ms` are loaded at `36–38`, but the signal and
   executor have no mandatory stale-data gate. A caller-supplied mask may impose
   one, so establish actual arm configuration before quantifying this divergence.
9. **Guessed venue clock in the view producer.** In `exact_book.py:views`, missing
   `venue_ts_ms` is replaced with `ts_ns//1_000_000 - 15`. That 15 ms estimate is
   not an observed venue time; there is no exported per-view component flag.
   The number of affected real components has not been measured in this subtask.
   Arrival columns are explicitly snapshots through `decision+L-1 ms`, not the
   matched event itself. This supports the candidate's clock-unknown restriction.
10. **Transaction-wide print attribution.** `exact_book.py:prints_for` selects
   `transaction_hash in order.tx OR taker_order_id == order_id`, then aggregates
   every selected print into shares/VWAP. Multiple allocations or distinct orders
   within a transaction can contaminate order-level labels. The source does not
   require a matching token/wallet or disambiguate maker/complement allocations.
   This is a source risk requiring canonical receipt/order reconciliation, not a
   claimed count of wrongly attributed saved orders. The source introduction
   also acknowledges deeper WS phantom levels: carried top-of-book agreement
   alone cannot certify the full historical ladder.

## Candidate and executable comparison

`forced_replay_candidate.py` extracts and executes the **actual** legacy `fak`
AST without importing the script's data-loading and output side effects. It
compares the legacy outcome-sized request against original submitted principal
using a single declared latency. Original source files and their SHA256 values
are recorded in the output. It writes one row per input order, never expands an
ambiguous view join, and reports missing/partial view coverage alongside arm/day
development-versus-later-validation groups.

Every numerical historical-view result is explicitly `UNQUALIFIED`. These
views cannot prove component clocks, full depth, economic liquidity identity,
fee policy or account ledger. A known-prefix diagnostic is not reported as
corrected economics or a deployable backtest. No fee/cash/PnL is invented.

From the candidate experiment root, on a source cluster compute node:

```bash
python tailtaker/forced_replay_candidate.py \
  --orders <SOURCE_DATA>/exp/tailtaker_20261001_FINAL/analysis/live_orders_latency.parquet \
  --views <SOURCE_DATA>/exp/tailtaker_20261001_FINAL/exact/exact_views.parquet \
  --out <SOURCE_DATA>/exp/taker_20261002_PARITY/tailtaker \
  --latency-ms 200
python -m unittest discover -s tailtaker -p test_forced_replay_candidate.py -v
```

Output: `forced_intents.csv`, `forced_intents.parquet`, `summary.json`. The
diagnostic earlier/later split is fixed at 2026-09-29 UTC, following PLAN.md;
neither cohort is claimed pristine unseen data. No parameter is fitted to fills.

## Local verification and fingerprints

The initial test run failed because the candidate module did not exist. Fourteen
synthetic semantic regressions pass after implementation; an additional CLI
Parquet integration test requires pyarrow and is skipped in local scratch where
that dependency is unavailable. A direct regression executes the original fak
function to reproduce its outcome-sized input effect. Run the Parquet test on
source cluster with the actual replay. Tests
cover leakage, unknown vs zero, all recorded levels, truncation, exact latency,
zero-size levels, missing-level holes, invalid prices, duplicate joins and
orientation. Independent review reproduced a malformed `.90,.50` ask ladder
incorrectly becoming a complete zero at an `.80` limit; a failing regression
preceded the fix that validates all known price ordering before early stop.
They are synthetic and are not represented as real episodes.

Original source SHA256:

| File | SHA256 |
|---|---|
| exec_exact.py | 1896c7bfe2f99484b70a4f412270b36e1ec1a8a54ffc65a08958cfc6e0278fb8 |
| parity.py | da40c5a4ba70bf6f1077c12d9c868e2296a9cc2d1ec3d8f05e432a15977e2daa |
| p3_v57_model.json | aa0eb36a23ff0a6f6567a8e21633f156724ca4bbc04f55b06d6d8a50090ab4ca |

No canonical engine replacement or strategy qualification is justified yet.
