# Taker parity audit — 2026-10-02

**Verdict: DATA / NO-GO for claiming backtest-live equivalence.** This experiment reproduced defects, built guarded candidate adapters and a qualification gate, and replayed recoverable captured intents. It does not establish full strategy/account parity for any family. Missing evidence remains unknown; no corrected wallet PnL or profitable deployment is claimed. No trading/recording service, live account or original engine was changed.

Code/report: `<SOURCE_HOME>/taker_20261002_PARITY`. Evidence/results: `<SOURCE_DATA>/exp/taker_20261002_PARITY`. PC: `<SOURCE_NOTES>/experiments/taker/taker_20261002_PARITY`. Open `dashboard.html` for the portable readout and `REPRODUCE.md` for exact commands.

## What materially changes the interpretation

1. **Sizing semantics differ.** A captured early MC FOK row stores 5 shares at a .20 cap, but reports 33.333334 shares filled at .03 with principal 1.00000002. Another stores 5 at .23 and reports 5.476191 at .21. The captured current signer explicitly distinguishes collateral-budget FOK/FAK BUYs from quantity-limited GTC. Its code corroborates the mechanism; it does not prove the exact earlier release. A fixed-five-share backtest is not an equivalent order. The generic shared matcher now rejects Polymarket BUY FAK/FOK share semantics; a proven spend-sized adapter is still required.
2. **Wallets do not identify strategies.** Kraken gate40 START records bind the ETH/TailTaker wallet during Sep17–23; AVG v2 later binds the separate AVG wallet, which also hosts maker activity. A2 changes base/cap from $1.25/$5 to $5/$20 at Sep30 09:15:05.851786 UTC. Shadow-v3 contains explicit `live=true` / `exec_mode=live` records and dry-run intervals; runtime mode alone is not independently reconciled money flow. `DEPLOYMENT_FINDINGS.md` supplies the eight-wallet map and exact source intervals.
3. **Tail's prior forced-size comparison leaks the outcome.** It uses actual `making` as the original budget and fabricates $1 when missing. The candidate uses original `principal_usd` only. On this saved cohort the change is small: +0.04255372898 simulated shares across 1,634 comparable observations. This defect does not explain the broad live shortfall by itself.
4. **No-fill and timing claims were overstated.** The Tail comparison subset contains only positive recorded fills. It cannot estimate false-fill accuracy. Of 1,864 latency observations, 878 have several equal-size depth-decrement candidates; none has a uniquely established millisecond venue match from that heuristic. Ninety have unknown recorded fill quantity, and 133 lack print/depth support. Those gaps are not observed zero fills.
5. **Timeouts and duplicate-looking legs matter.** Captured MC has a POST/cancel-uncertain order later reported filled, and another still UNKNOWN. A q99 control has two distinct 20-share trades at the same price and second; naive deduplication loses 20 shares. Exact trade/allocation IDs are retained. Failed cancellation, local zero audit fields and precomputed order hashes do not prove zero execution or release cash.
6. **Sampler recovery has an executable counterexample.** On a deterministic causal quote fixture, the original scheduler recovery changes a frozen fitted probability by 0.149691. The candidate reproduces punctual features and both fitted probabilities exactly. This is a real-source synthetic regression, not proof that all historical signal disagreements are fixed; the October 1 generated replay uses Binance input while live MC uses Kraken.

## Evidence and deployment population

Six archives were extracted on compute and their current member bytes rehashed: **10,455 files / 18,006,393,412 bytes**, with zero hash/size/missing-member discrepancies. API verification checked **178 requests across 12 address directories**, including four metadata requests. The saved derived views contain 20,555 all-role trade rows, 10,869 taker-filtered rows and 27,962 activity rows. These are overlapping views, not unique executions or order counts. Six identical within-page observations remain explicit multiplicity ambiguity. All 119,892 raw page observations lack an explicit venue order ID.

The final census records **5547 journal order/event observations**, with 3403 distinct explicit order IDs; complete submitted and no-fill denominators remain unknown. The SQLite census includes maker controls and overlapping snapshots: its 411,633 order/history observations and 9,638 trade observations must not be added to journal/API rows as independent fills. Sources, row/line ordinals, identities and source hashes are retained in the heavy ledgers.

The final deployment extraction contains **1030 observed sessions** (including recorder/control services, not distinct taker deployments), 900 runtime config events and 663 captured config/unit snapshots. It reports 12 runtime wallet bindings and 0 explicit runtime code/config hash bindings. First/last observations bound recorded coverage, not continuous uptime. Current snapshots are not backfilled into earlier deployments. Interleaved systemd journals are partitioned by unit/invocation/PID/boot.

`order_wallet_links/` adds collision-guarded exact-transaction candidate joins. It preserves competing orders, missing/conflicting assets and repeated separate API endpoint observations. A candidate wallet association is not receipt-level fill or cash reconciliation. The final join yields 3,831 observation edges covering 1,277 explicit order IDs; all remain unresolved because the order-side token is missing. It does not upgrade the three weak wallet bindings or establish a fill denominator. Its complete summary is included in the dashboard source bundle.

## Forced execution comparisons

### TailTaker: one fixed 200 ms scenario

All **1,864** saved intents are retained, across A, A2, p3_v57 and wf_psize. There are 151 missing views and four incomplete depth prefixes. No per-order delay was selected to fit fills.

| Split | Input observations | Comparable positive-fill observations | Both predict fill | Missed fills | Legacy shares | Revised diagnostic shares | Recorded live shares |
|---|---:|---:|---:|---:|---:|---:|---:|
| Before Sep29 | 1,527 | 1,435 | 1,423 | 12 | 2,557.206800 | 2,557.246770 | 2,576.136921 |
| Sep29 onward | 337 | 199 | 199 | 0 | 418.758293 | 418.760877 | 421.300704 |

Both legacy and candidate have the same fill confusion on this positive-only subset. Later dates were predeclared for validation, but had prior analysis and are not pristine unseen data. Complete fee/cash, signed amount semantics, depth identity and component clocks are absent; **zero rows are execution-qualified**.

Captured authenticated history separately supplies **120 distinct TAKER/CONFIRMED legs and order IDs** (165.332091 shares, 124.14873558 reported gross principal). Exact-ID comparison joins **111** orders with no ID collisions; recorded live quantity matches all 111 exactly. Nine authenticated orders are outside the saved forced cohort. Candidate quantity comparison is available for 97: mean absolute gap **0.053504 shares**, maximum **1.063816**. Candidate VWAP matches 92 comparable captured prices to floating-point precision. This verifies those captured price/quantity comparisons, not complete fill histories or venue latency. Counterpart maker allocations stay separate from own shares. Raw `fee_rate_bps=0` is not receipt proof of zero fee.

### MC: captured orders and independent generated signals

The forced-input bridge preserves **46 captured SQLite observations** and **92 fixed-delay scenarios** at 250/500 ms. At each delay, nine have usable immediate-crossing books, six are NOT_SENT, one has no decision time, and 30 have source gaps. Seven of nine stored-share quantity comparisons fall within the declared tolerance, with a maximum gap **28.333334 shares**, driven by the unresolved early sizing semantics. Stored shares are explicitly unverified as signed fixed-share size. GTC immediate crossing is not a terminal resting/cancel outcome; cash is not released on that calculation. All rows remain unqualified.

The separate unchanged-R4 generated-signal comparison covers the same 239 resolved markets. At 250 ms the candidate retains 162 generated attempts / 486 plan rows (138 diagnostic fills and 348 no-fills). At 500 ms: 185 attempts / 555 rows (108 fills, 447 no-fills). The original wrapper only expanded preliminary successful one-share events and compressed failed attempts; the candidate exposes every generated preliminary attempt and source error. Upstream one-share path truncation and signal-source mismatch still prevent a complete deployed strategy trajectory. These plan rows must not be summed as a wallet equity curve.

## Implemented safeguards and limits

- `tailtaker/forced_replay_candidate.py`: original-intent budget, exact requested delay column, side-checked joins, all available ordered depth, explicit missing/truncated evidence, actual legacy-source comparison. No silently guessed book or latency.
- `misscalc/mc_replay_candidate.py` and `pure_taker_search_candidate.py`: generated-attempt retention, explicit source failures, no missing-book zero, no aggregate wallet PnL certification.
- `misscalc/mc_forced_replay.py`: recorded intents, separate strategy/submission caps, strict venue-clock reader, token checks, raw authenticated leg identities, sizing uncertainty, source manifests and fixed timing scenarios.
- `misscalc/feature_service_candidate.py`: causal recovery of eligible queued observations on missed sampler ticks; no service installation.
- `taker_contract.py`: explicit generic share semantics, FAK/FOK atomicity, limits, per-leg fees, reservations, idempotence, chronological cash and persistent economic liquidity IDs. It rejects unsupported Polymarket BUY FAK/FOK sizing and GTC; it is not a production venue emulator.
- `qualification.py`: exhaustive attempt membership, preserved legs, dated deployment/venue/sizing evidence, forced signed terms, causal clocks, held-out declarations and ordered cash/inventory checkpoints. Unknown evidence gives UNQUALIFIED; known counterexamples give FAIL. Hash-shaped references and true flags do not authenticate their own contents; independent source audits remain required.

The officially documented crypto holds are 250 ms before Aug17 11:00 UTC, 50 ms until Sep4 14:00 UTC, and 150 ms after. Apply the correct boundary at venue ingress; never add a hold twice to total latency. Public second timestamps, POST response RTT and nearest book decrements do not establish that ingress/match time. Source: https://docs.polymarket.com/changelog/predictions . Market/date-specific tick, minimum and fee collection evidence is still required.

## Go/no-go and exact remaining work

| Population | Verdict | Main unresolved requirements |
|---|---|---|
| Tail A/A2/wf/p3 | NO-GO for parity claim | Complete negative attempts, original signed sizing, full causal depth/component clocks, fees and wallet cash; limited authenticated overlap is earlier history |
| Legacy ETH / shadow-v3 / AVG polybot | UNQUALIFIED | Dated exact engine/config binding, role and wallet joins, missing or rotated historical journals, matched full-depth/account replay |
| AVG gate40 / v2 / maker control | UNQUALIFIED | Recovered wallet transition must be applied per episode; full submitted population, source/scan phase, fees/cash and engine comparison missing |
| ReverseTaker / MC | NO-GO for parity claim | Spend-vs-share signed semantics, GTC lifecycle, fixed Kraken signal replay, missing venue-depth coverage and complete cash events |
| Kosta/F13 / PolyHFT | UNQUALIFIED | Event-level live attribution and deployment engine identity; paper evaluation logs are not real execution labels |
| Queue99 taker overlays | UNQUALIFIED | Separate actual maker/taker allocations and taker stop lifecycle; maker control evidence does not certify takers |

No full end-to-end cash reconciliation, historical markout panel or corrected economic curve is claimed. Missing opening balances, allowance/reservation history, receipts, settlement/redemption flows and continuous depth cannot be reconstructed by fitting a constant delay. The additional historical WAL directory `<SOURCE_DATA>/wal_cohort_a_tailmaker` was located (July/August files), but was not decoded or used to qualify a historical strategy in this iteration. Other family engines remain pending exact deployment-to-backtester binding; this report does not imply they were all repaired.

For future qualification, capture immutable run/release/config identity, pre-send attempt and exact signed maker/taker terms, both wall/monotonic clocks, source event/receive times, complete order/trade lifecycle and allocation IDs, book sequence/normalization provenance, pending reservations and the full shared-wallet cash/position event stream. Validate a venue-correct spend/GTC adapter against separately held-out attempts before routing new strategies through it. A future public-book backtest cannot be promised identical outcomes for every counterfactual order; this package prevents unsupported equivalence claims and the demonstrated silent assumptions.

## Verification and handoff

**167 distinct final test cases passed**: 40 generic matcher, 31 qualification gate, 41 census/clock/verifier/join, 15 Tail (including Parquet integration), 13 captured-evidence, 15 captured MC, and 12 original-versus-candidate MC cases. The 10 final mapping checks repeat a subset and are not added again. Fresh verification results are in `results/verification.json` and the source cluster `verification/` logs. Final source verification runs the shared matcher/gate, census, Tail Parquet integration, real-evidence fixtures, captured MC bridge and original-versus-candidate regression suite. Baseline assertion failures in the MC audit are intentional defect reproductions; candidate tests must pass.

Job 12062813 completed all main stages successfully. Final-data job 12063299 produced replay/census outputs, then was canceled during a slow deployment extraction. Mapping job 12063326 completed the optimized deployment extraction and exact transaction joins. Job 12063299 initially failed two test groups because the sanitized evidence fixture existed only under the heavy path; final-check job 12063309 copies that fixture into the test workspace and reruns the groups plus the final matcher/gate. Failed invocations are retained and are not relabeled successful. Source and result hashes accompany the delivered package.
