# BoneOhio: established mechanics and unresolved strategy

Prepared **2026-10-03**, from Viper Atlas and the October 2 FINAL audit. Target
public wallet: `0x48ac40fc545cf327edd5365435c3a9f385614a7e`.

The useful working description is a **lower-price aggressive BUY arm plus a
near-.99 sweep/rest BUY arm**, with external-price and Polymarket-book signals.
We have recognizable mechanics, a failed trade-for-trade replica and a material
maker execution gap. We have not recovered its exact feed, complete entry
selection, capital state, true submission latency or a certified strategy.
Keep that distinction when proposing new correlations.

The package's main raw-market study is the last two complete available UTC
days, **September 30 and October 1**, on BTC 5m and BTC 15m. Historical wallet
history, previous studies and five losing-maker diagnostics are deliberately
separate context. These dates have already been inspected; they are reused
historical research, not a sealed future test. Latest bundle coverage and fresh
wallet delta manifests take precedence over source snapshots' cutoff dates.

The fresh public API walk now covers the full requested two-day window:
**3,314 fills, 828 taker-only fills and 4,333 activity rows**, with the latest
fill at October 1 23:59:09 UTC and all cursors exhausted. Existing plus 307
newly decoded target-only public transactions directly link every fresh fill
to its token/salt with zero role disagreements. The 1,109 observed filled
salts include 844 near-.99 limits and 265 other limits, with 584 pure taker,
289 pure maker and 236 mixed-role salts. These are all-coin two-day counts;
BTC5/BTC15 must be filtered through slugs/token catalogs. They establish a
stronger current identity foundation, not complete order lifecycles or
zero-fill submissions. Fresh provenance is in `context/wallet/FRESH_*.json`.

The fresh BTC5/BTC15 print audit now adds **1,613 direct and 635 complement
price-consistent candidates** for 2,256 Bitcoin fills, reaching October 1
23:59:05.894 UTC. Eight fills remain unresolved; seven API times are beyond
the stored post-expiry tail and one lies inside it. API-minus-print median
is 2,213ms. These are inferred public-print attributions, not authenticated
entry or decision times. See `context/FRESH_VENUE_MATCHING.md` for the portable
matcher, precise boundaries, preserved ambiguity rules and source hashes.

## Which previous claims survive

October 2 `FINAL_RESULT.md` supersedes the stronger October 1 ENTRY and early
October 2 PARITY narratives. The earlier claims of a proven Binance-only
feed, approximately 110ms reaction, Europe hosting, proven signing time and
phantom-depth cause must not be carried forward as facts. ENTRY/PARITY remain
historical leads. Their hashes are preserved in `context/PROVENANCE.json`.

| Finding | Evidence and limit |
|---|---|
| Observed trading is BUY oriented with lower-price and near-.99 behavior | Public fills and decoded order roles/limits; fills omit zero-fill attempts. Mixed `MT` means the same salt appears in both taker and maker positions. |
| Lower-price activity follows fast underlying moves and cheap/stale asks | Association and an interpretable candidate; exact moments remain poorly reproduced. |
| Near-.99 orders can sweep immediately and rest their residual | Decoded fill roles plus depth footprints and residual histories. Fingerprinted posts/exits are inferred. |
| Cancellation often coincides with an adverse underlying move | Useful event association; not a directly observed feed-to-cancel latency. |
| Reposting is uncommon in the observed near-.99 sample | 94% of contract-sides use one order. Only 13% of inferred cancellations have a later order, with a median 19s wait. No step-down ladder was observed. Never import Q99's repost rule automatically. |
| The wallet's limit/size structure is visible | Signed calldata gives limits and raw quantities; `order_ts` need not be signing/submission time. Equal fill price and limit does not prove when an order was prepared. |

Sources: `context/historical/FINAL_RESULT.md`, `MAKER_RUNBOOK.md`, public order
and chain-link Parquet tables in `context/wallet/`.

## Feed and timestamp evidence

The main Polymarket `ts_ns` lineage is now specifically audited: every
September 30–October 1 hour uses the vendor's earliest-received per-row
witness merge, converted from microseconds and clipped at the venue
timestamp. Derived cleanup rows reuse parent timestamps. The feature
field is `source_cache_ns`. This is a vendor collector-receipt proxy,
not BoneOhio receipt or one fixed native recording route; it is separate
from the native Binance observer clock. The source adapter's distinct
venue-plus-median-lag reclocking function was not used in this cache.
Exact source and hourly manifest hashes are in
`context/PFLOW_CLOCK_PROVENANCE.json`.

The historical cache contains batched Binance perpetual mids and Binance
**aggTrade**. In aggTrade, `T` is trade time and `E` is publication/event time.
Observer receipt is a third clock. The route captured 28.7 million trades with
median `recv-T` 132ms and p90 270ms; the short multi-venue study median was
223ms, and October 1 median 122ms. Long recorder-stall tails exist. These are
our route's properties, not the target's network latency. At tested cutoffs,
222 of 304 latest-`T` selections had not arrived at the recorder.

Wallet API seconds lag public prints by a median 2,467ms, p90 3,358ms, over
43,009 matched groups. Use API seconds for coarse activity and public print
links for event forensics. `m=print_ms-40` is an inferred matching proxy.
Subtracting a presumed hold from a completed move path does not identify
decision time or geography. There is no certified constant 40ms conversion
between batched mids and trade clocks.

The direct transaction/token matches all agree on earliest mapped time.
10,815 transaction-only groups map to the opposite token's print; 10,770
are within one cent after complementing the price. None is a low-price
taker. These can be legitimate complement executions rather than bad
timestamps. Preserve exact token, transaction, salt and role identity rather
than dropping every opposite-token match or treating transaction grouping as
an exact order key. Another 861 historical groups lack mapped prints.

Fixed-time case/control comparisons on September 12–13 retained 168 of
201 BTC5 low-price taker groups. The same-side control is 30s earlier in the
same contract, away from recorded wallet prints. On the identical 47-pair
check cohort, adding Kraken perpetual receipt moves to Binance receipt moves
changed AUC 0.7861 to 0.8151 and log loss 0.6361 to 0.6169. Bybit added
associations in some comparisons. Small reused cohorts, correlated venues,
route differences and multiple comparisons prevent a precise vendor/feed
identification. The main two-day cache does not automatically include those
historical multi-venue raw streams.

Earlier `bsa_fit2.py` and `mv_feed.py` picked positive times by the maximum
anchored fair-value edge in a presumed window. Their feature-dependent event
selection inflated edge discrimination. Their roughly 0.03 F1 remains a
failed replication result. Do not choose each case's timestamp to maximize
the signal you are testing.

Sources: `context/historical/feed_clock/` and the primary specifications in
`SOURCES.md`. Current Polymarket provider documentation changed relative to
the archived FINAL description; neither current nor archived wording proves
the target's historical oracle/vendor subscription.

## Maker parity and why depth is necessary

The canonical replay removes the recorded historical footprint, inserts
residual shares at the inferred footprint sequence, processes full snapshots
and absolute level updates in `(venue_ms, seq)` order, and consumes both
same-token SELL and complemented opposite-token BUY prints. It shifts prints
by 60ms in the reference, handles through-price fills, subtracts bids above the
limit from finite sweeps and attributes unexplained reductions to external
cancellations. Exact rules and historical flags are in `MAKER_RUNBOOK.md`.

| Canonical recorded-order sample | Orders | Filled-order recall | Simulated / actual shares |
|---|---:|---:|---:|
| All resting components | 846 | 83.92% | 0.8543 |
| Pure maker `M` | 540 | 75.74% | 0.7639 |
| Sweep/rest `MT` | 306 | 98.37% | 1.0258 |

Canonical gross resting PnL was **$7,839.98 simulated versus $6,580.54
actual**, with drawdown $820 versus $1,507. Both pure-maker losing orders are
entirely missed: **2,841.36 actual shares versus zero simulated**. This is a
resting-.99 component, default zero maker fee and no rebate; it is not the
whole wallet's economic PnL.

All 846 sampled orders actually filled. Consequently zero false fills is
structural and the percentage is recall, not specificity. 475 inferred-alive
orders use contract-end plus ten minutes as an artificial replay cap.
Trade-conditioned footprints and actual lifecycle information make this
forensic execution accounting, not an independent strategy backtest.

Sequence corrections changed no fills; better-bid subtraction changed only
$4.68 in the isolated comparison. Snapshot-only replay reproduced the losing
days and gave $6,089 PnL/$1,524 drawdown, but 18,073 snapshot comparisons show
zero downward resets. Carried .99 depth above reported best bid appeared in
21 orders and none of the five losers. Phantom depth remains unproven.
Head-first cancellation and turning unexplained depth drops into fills also
remain rejected diagnostics. Do not select a Q haircut or snapshot model
because it fixes the known losses.

Independent reference-engine evidence from smaller accepted .99 orders is
better: 631 accepted, 628 replayed, three missing cache; 98.73% yes/no,
seven false fills, one miss and 1.0767 share ratio. The BTC5 subset has
185 accepted orders, 15 actual fills, one false fill and 1.0705 share ratio.
Its median size is 200 shares versus 3,000 for BoneOhio pure maker. That
validates reference mechanics in that bounded regime; it does not validate
BoneOhio's larger queues. Only aggregate parity tables are exported.

An old overlapping Q99 archive has unknown outcome labels on 1,064 .99
variant rows and converted missing payoff to zero. Its fill quantities are
usable; its old PnL and loser classifications must be suppressed.

Sources: `context/historical/maker/`, canonical variant rows, role metrics and
five explicitly known-outcome exemplars in `diagnostic_contracts.csv`.

## Entry selection remains unresolved

For September 21–30 BTC5, first eligibility with favourite ask .985–.99 and
at most 180 seconds remaining has 27.95% precision and 96.03% recall against
observed target orders. A prior-15-minute public-print gate chosen on design
data changes this to 29.62% and 91.59%, removing 249 unmatched selections
while discarding 37 observed matches. No tested variant exceeds 37.37%
precision on the reused period.

Very recent activity, coin allocation, turnover, simple concurrency/gross
exposure caps and coarse calendar controls failed to recover a stable rule.
Target order counts decline over weeks while eligibility remains approximately
stable. Prior fill activity is not a demonstrated available cash balance,
net position, outstanding-order limit or authentic public attribution time.
An eligible token without a recorded fill can represent an unobserved
zero-fill submission. It must remain a censored negative.

Sources: `context/historical/SELECTION_RESULT.md` and every retained rule,
chronological denominator and candidate-feature table under `selection/`.

## The existing taker candidate is a benchmark

BI1/F2 uses an aligned Binance perpetual move of at least 3bp over 1s,
side ask .05–.95, ask increase at most .02 over 1s, 10–300s to expiry,
1s feed-gap guards, one order per contract-side and 20-share FAK capped at
ask+.01, held to resolution. It captures a plausible mechanism without
reproducing the wallet's entries.

Fresh strict depth replay of 3,915 saved E-clock intentions, with default
fee `0.07*p*(1-p)*shares` per consumed level, gave net PnL
$1,909/$1,713/$1,568/$1,500/$1,090 at total latencies
300/325/350/400/500ms. Corresponding drawdowns were
$117/$102/$97/$86/$87. Partial October 1 at 300ms was
**-$13.20 from 28 fills**, versus a stale legacy-book result -$0.90/29.
Default fees and valid source book status matter.

This is conditional execution of saved event-clock intentions. F100 is
already part of total latency; adding it again double-counts delay. It is
not a rebuilt receipt-causal strategy, new sealed validation or proof of
the target's rules. Payout booked at contract end is an equity proxy rather
than live mark-to-market or redemption cashflow. Later own paper engineering
does not remove these historical identification limits.

Sources: `context/historical/TAKER_RESULT.md`, `taker/metadata.json`,
coverage, latency and daily/weekly tables. Use the bundled latest execution
engines for new experiments; keep strategy triggers independent of engine
parity evidence from Q99/avg_bot/vol075/TailTaker/A2.

## What a useful new result should say

Separate the mechanisms you can identify, the inputs that explain observed
events beyond simple controls, the lifecycle/selection that remains missing
and the executable economics supported by a qualified engine. Prefer a small
falsifiable approximate policy with errors and counterexamples to a profitable
curve whose labels or timestamps were optimized after looking at outcomes.
Follow `RESEARCH_PLAN.md`; preserve failed hypotheses and coverage masks.
