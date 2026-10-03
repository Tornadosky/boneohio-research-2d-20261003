# Portable maker execution reference — 2026-10-03

`bonebundle.maker` replays declared BUY maker orders against the full normalized
Polymarket bid-depth events and trade tape. It contains actual Q99-tested queue
mechanics and the latest decision-replay recommendations. It does not infer a
BoneOhio entry, cancel or repost rule. The order quantity is the resting remainder;
an initial marketable sweep must be evaluated separately with the taker engine.

## Sources and exact version

The canonical tested strategy-engine snapshot is
`q99_20261002_DEPLOYGAP/lat/pkg_v9fp/queue99_backtester.py`, original SHA256
`9a35c079aa5e22dd469db23cf6f607e22e871dbeaafbbdf682f1b2648113c6e3`.
The newer 2026-10-03 decision replay and recommendation are
`q99_20261002_DEPLOYGAP/code/replay/replay.py`, original SHA256
`5275b4425b6f3ee5562e62e39ef623b55ddf1187df80eef42b8b01522c73ad7e`,
and its accompanying `notes/REPLAY.md`.

The previous BoneOhio FINAL canonical replay used
`q99_20261002_DEPLOYGAP/code/fill/fillrep.py`, original SHA256
`f099f56ab7e540de09242cd462e9210cddcb91d400f2c66a1376378bd8c4ad83`.
That implementation remains included for independent parity comparisons.

[SOURCE_MANIFEST.json](../vendor/maker/SOURCE_MANIFEST.json) records original and
distributed hashes for every source/documentation snapshot. Personal storage
prefixes, source hosts and private account aliases were anonymized for public
distribution. The seven extracted functions in `_reference.py` remain verbatim;
AST equality against the distributed decision-replay source is tested. The original
runners have hard-coded paths and import-time output creation, so use the portable
API below. Importing this API does not run a job, create output directories or
connect to a host. The complete strategy-engine file is reference material; its
Q99 decision logic and missing original project dependencies are not part of this
portable execution API.

## Main modes

| Mode | Meaning | Research use |
|---|---|---|
| `eng` | Exact tested production fill path: virtual FIFO join, complement prints, unbounded trade-through, sizematch cancellations, 47 ms effective print shift, optional venue-time own removal | Historical reference and sensitivity baseline |
| `eng_ftfix` | Latest replay recommendation: displayed better-bid subtraction, bounded trade-through, own placement steps snapped to their depth footprints | Default portable virtual-order replay; compare with `eng` |
| `fp3` | Real recorded order inserted into its size footprint; better-bid subtraction, unbounded through, 60 ms print shift | Historical order-conditioned forensics only |
| `fp3_thr` | `fp3` with bounded trade-through | Through-size sensitivity |

The API also retains the exact replay addendum's diagnostics: `eng_book`,
`eng_sh60`, `eng_noown`, `eng_thr`, `eng_own5`, `eng_fix`, `eng_ownft`,
`fp3_df1`, `eng_dm`, and `fp3_dm`. `fp3_df1` converts unexplained depth loss
into fills and greatly overfills the original Q99 cohort. The depth-matched `dm`
proposals underfilled the cohort and were rejected as implemented. They remain
explicit diagnostics, not recommended defaults. Their fallback timing is fixed at
47 ms; their reported first event can be a depth event rather than a print.

## Mechanics preserved together

- Reconstruct absolute bid-level changes and full snapshots in
  `(venue_ts_ms, seq)` order. Snapshots zero absent tracked levels. Replay ties are
  print, depth, then virtual join; depth ties preserve `seq`.
- For BUY outcome `y` at limit `L`, count same-token SELL and opposite-token BUY
  prints. Map opposite price `p` to `1-p`, admitting mapped prices `<=L`.
- Book-aware modes subtract displayed bid quantity strictly above `L` from finite
  sweep quantity, evaluated at `print_venue_ms - shift_ms - 0.5`. This includes
  `.991`–`.999` depth for a `.99` order. A bounded through-print consumes its
  available finite size; the production and original canonical references instead
  empty the level on mapped prices strictly below `L`.
- FIFO consumes the queue front. Unexplained reductions cancel the newest external
  lot with exactly matching size, otherwise newest external lots first. Base
  models never turn an unexplained reduction into our fill.
- A recorded footprint reconstructs external quantity before the exact
  `(time, seq)` row, inserts our remaining size, then places extra size behind it.
  The optional own-removal table belongs to that same historical wallet, market,
  outcome and limit. Removing another wallet's size would be invalid.

The 60 ms print lead is a cohort-calibrated modeling assumption. The production
venue-feed path uses `60-13=47` ms. Neither is a certified conversion to an observed
match/submission clock. Stress `print_shift_ms=0/20/47/60/120` and cancellation
timing; retain the source default in a clearly labeled baseline. Reported first
fill times in the principal modes are **original print venue times**, not observed
decision times or shifted match times. Fills can have a print timestamp later than
the cancel cutoff while their modeled effective time precedes it.

## API and data contract

After installing the package, from the repository root:

```python
from bonebundle.maker import MakerOrder, load_market, replay_order

depth, trades = load_market("data/poly", "btc_5m", CONTRACT_START_MS)
order = MakerOrder(
    outcome=0,                 # 0 = UP/YES, 1 = DOWN/NO
    price_micros=990_000,      # price times 1,000,000
    quantity=100.0,            # shares, resting remainder after any initial sweep
    place_venue_ms=PLACEMENT_MS,
    cutoff_venue_ms=CUTOFF_MS, # declared cancel-effective or declared expiry cap
    order_id="declared_candidate_001",
)
baseline = replay_order(depth, trades, order, mode="eng")
recommended = replay_order(depth, trades, order, mode="eng_ftfix")
```

The cache root contains
`normalized_depth_events/market_key=<key>/contract_start_ms=<ms>/session_id=<id>/*.parquet`
and the corresponding `trade_tape` layout. `load_market` accepts an explicit root,
or `DATA_ROOT`, or `./data/poly`. It supports the BTC5/BTC15 scope of this handoff.
Multiple witness/session directories require an explicit `session_id`; they are
never silently merged. Missing depth raises an error. An absent tape returns an
empty tape, matching the source runner; consult the coverage manifest before
interpreting zero fills as evidence.

Depth columns: `venue_ts_ms`, `seq`, `outcome`, `event_type`, `side_code`,
`price_micros`, `size_micros`, `bid_prices`, `bid_sizes`. `side_code=0` is bid;
snapshots store comma-separated price and quantity micro-units. Trade columns:
`venue_ts_ms`, `seq`, `outcome`, `price_micros`, `size_micros`, `trade_side`;
`trade_side=0` is aggressor BUY and `1` is SELL. Prices/sizes become probability
and shares by dividing by one million. No original storage directory is required.

`replay_order` returns a `MakerResult` with simulated shares, first print time,
external queue ahead, level-row count, chosen print shift, own-removal status and
historical footprint status. It computes no fees, rebates, settlement PnL, cash,
selection score or drawdown. Join independently verified settlement labels and
actual order prices afterward. `replay_orders` provides a multi-order/mode table.

For an own-order replay, `own_steps=` must contain the exact account/token/level's
`venue_ms, delta` signed changes; cumulative own size is removed on venue time.
`eng_ftfix` snaps positive own steps to a level increase carrying their size within
`[-10,+2]` ms. This is a forensic data correction, not future information available
to a new hypothetical order. New virtual orders should normally omit `own_steps`.
For `fp3`, pass an independently reconstructed `footprint=(venue_ms, seq)` when
available. Automatic footprint search spans placement `+/-3000` ms and is
historical-only. Missing footprints raise `MissingFootprintError` by default.
`require_footprint=False` enables the original Q99 synthetic-entry diagnostic;
do not use it to fabricate missing BoneOhio footprints.

The command-line wrapper accepts a CSV containing `market_key`,
`contract_start_ms`, `outcome`, `price_micros`, `quantity`, `place_venue_ms`, and
`cutoff_venue_ms`; optional fields are `cancel_venue_ms`, `order_id`,
`footprint_venue_ms`, and `footprint_seq`:

```bash
python -m bonebundle.maker --data-root data/poly --orders inputs/orders.csv \
    --modes eng,eng_ftfix --out outputs/maker_replay.csv
```

The wrapper also accepts `--own-steps exact_account_steps.parquet`, whose columns
are `contract_start_ms,outcome,limit,venue_ms,delta` and whose `outcome` is UP/DOWN.
Its price is a probability, unlike `MakerOrder.price_micros`. Never infer posting
or cancellation time from API seconds without carrying timestamp uncertainty.

## Verification and practical limits

`python -m pytest tests/maker -q` passed **55 checks** on both Windows (10.11 s)
and Linux (8.40 s) on 2026-10-03. Three anonymized accepted live Q99 orders carry 1,366,012 actual depth
rows and 4,678 actual public print rows. Expected outputs were generated on the
original source using read-only caches, with no original experiment writes. All
14 modes match quantity, first time and queue ahead. Independent AST-only
execution of production `_fp_simulate` and the previous canonical `fillrep`
queue also matches the portable wrapper. Source/fixture hashes are verified.

| Real diagnostic | Live shares | `eng` | `eng_ftfix` |
|---|---:|---:|---:|
| Large complement/sweep order | 421.9375 | 1,169 | 1,005.9375 |
| Short bounded-through race | 25 | 748 | 55 |
| Own-footprint losing fill | 70.92 | 12.22 | 32.22 |

The fixtures deliberately preserve known residual errors. They test implementation
parity with the reference, not a promise that every live fill is reproduced.
They are selected known mechanisms and must not be used as held-out strategy
tests. The private order IDs and source account identifiers were discarded.

The full Q99 decision replay reported 4,111 resolved accepted orders: `eng` had
98.0% fill yes/no agreement, 1.03 times live shares; `eng_ftfix` had 98.3% and 1.02
times shares. Those figures are cohort-specific. In BoneOhio FINAL, the previous
canonical model replayed 846 filled orders at 0.854 times real shares; pure-maker
share ratio was 0.764 and both pure-maker losing orders were entirely missed.
The filled-only sample cannot estimate specificity. Unknown unfilled orders,
inferred timestamps, artificial expiry caps and large-order execution remain
open problems. The new two-day data does not independently close them.

Do not tune queue ahead from settlement outcomes or transfer Q99's entry windows,
adverse-cancel clock, keep-queue/retry-busy behavior, repost rules or sizing into
BoneOhio. Compare recorded-order reconstruction separately from prospective
signal experiments, and retain explicit coverage/timing/censoring limitations.
