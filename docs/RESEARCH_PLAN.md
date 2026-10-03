# Research plan: infer an approximate BoneOhio policy

Prepared 2026-10-03. The objective is to explain observed BoneOhio behavior
with a small falsifiable policy and establish which additional measurements
are needed. Optimize identification of the wallet's arms, entries, sizes and
lifecycles before evaluating economic outcomes. BI1 is a comparison mechanism,
not the answer the researcher is expected to rediscover.

Run bounded stages, save reproducible code and every failed hypothesis, then
continue only where the evidence supports a useful discriminating experiment.
If the inputs cannot identify a behavior, state that limit and specify the
missing measurement. A weak association is not a reason to search endlessly
until a profitable curve appears.

## 1. Qualify inputs and rebuild event identity

Verify manifests and SHA256 values. Census the expected 288 BTC5 and 96 BTC15
contract starts per complete UTC day, paired tokens, oracle labels and
actual event/feed coverage. Read one partition at a time; bounded memory is
more useful than whole-period pandas loads. Reject unseeded or stale execution
states and keep feature missingness rather than filling future values backward.

Preserve the native symbol/instrument/provider, venue/event time, genuine
receipt time where available, `seq`, update IDs, warmup and lifecycle tails.
Keep API-second, venue-print, calldata-timestamp, inferred-decision and
inferred-exit labels in separate columns. Build two feature tracks:
retrospective venue-event association and receipt-causal availability for
each recorded route. Do not silently substitute the former into the latter.
The main Polymarket feature `source_cache_ns` is the vendor's per-row
earliest-witness receipt clipped at venue, as audited in
`context/PFLOW_CLOCK_PROVENANCE.json`; it is not one fixed collector's
availability. Native Binance receipt features are a distinct track.

Create one identity table from public fill rows and the exact
transaction/token/salt links, recording the join type, role and coverage.
Use direct token joins first; explicitly label complement-token joins.
Aggregate repeated fills by salt only after keeping per-fill roles and prices.
Separate pure taker, pure maker and sweep/rest, near-.99 and lower-price
limits, and 5m versus 15m. Price paid alone is insufficient to infer order
type. Preserve unmatched rows and late source cutoffs in every denominator.

Construct a state/coverage table for every eligible contract-side, including
periods with no observed target fill. Public filled orders omit true zero-fill
submissions. Describe absent labels as censored negatives and repeat key
tests with a stricter population in which book/feed and wallet coverage
are all present. Report how execution selection can affect each result.

Required first output: `coverage.csv`, `identity_audit.csv`,
`unmatched.csv`, `arm_counts.csv` and a short clock dictionary with exact
included/excluded ranges. Do not begin model ranking until these reconcile.

## 2. Make a small descriptive atlas

Produce chronological plots and tables of observed entry/limit price,
side, tenor, size, share/notional tiers, inferred time to expiry, signed
timestamp age, number of salts per token, mixed-role sweep amount, resting
residual, inferred cancel age and repeat-order gap. Label every clock
assumption on the plot. Overlay both outcome books, Binance candidate
feeds, prints and replay validity around several representative winners,
losers, unexplained takers and apparent non-entries. Select examples by a
declared stratified rule, not by how well they fit a proposed mechanism.

Use the small all-coin public fill history for UTC-hour/day activity,
coin/tenor allocation and whether BTC activity is part of broader trading
bursts. Normalize BTC/Bitcoin etc. through token/slug metadata while retaining
source strings. Distinguish gross BUY turnover, outstanding unresolved
observed positions, net paired inventory and cashflow. If activity includes
redeem/merge/split/transfer events, reconcile them explicitly; a current
positions snapshot alone cannot reconstruct historical capital.

Give the near-.99 and lower-price arms separate descriptive tables. A mixed
order may intentionally take cheap asks and retain a .99 tail. Its resting
component must not be counted as a second independent strategy decision.

## 3. Test a short hypothesis list with fixed event times

Register the feature windows, decision-anchor uncertainty, controls, models
and evaluation metrics before running the main comparisons. Use modest
interpretable models first, then ask whether a complex model adds robust
incremental explanation on the same complete-case population.

| Hypothesis | Useful inputs and a discriminating test |
|---|---|
| External impulse plus a stale Polymarket ask | Aligned returns over predeclared 100/250/500/1000/3000ms windows; ask change and quote age; spread, taker-print flow and time to expiry. Test incremental value of the interaction after conditioning on ask level, time, volatility and basic market state. |
| Cheap-ask jump independently triggers entries | Prior downward ask change, size entering best ask, spread/complement changes and thin book. Compare cases with little aligned external move against identically eligible no-observed-fill controls. Preserve quote/event sequence and whether the cheap size was actually executable. |
| Market-anchored fair value or digital-option edge | Causal market strike/boundary value, distance from strike, realized volatility using only past data, time remaining and a small model of win probability. Compare price residual to a simple move feature; never use final oracle direction, future volatility or a retrospective maximum edge to choose the label time. |
| Underlying order pressure precedes the move | Binance bid/ask spread, queue imbalance at 1/5/10 levels where available, microprice, signed aggTrade flow, depth changes, refill/cancel pressure. Ask whether they add information beyond recent return, price level and availability. More levels alone are not a guarantee of a causal signal. |
| One particular feed drives the target | Separately identified perpetual/spot trade, bookTicker and other venue/provider receipt features on identical pairs. Use fixed clock sensitivity and route masks. Correlated prices can leave the exact vendor unidentified; do not optimize a unique latency for each case. |
| Near-.99 selection is probability/time based | Favourite ask, time to expiry, underlying distance/volatility, reversal risk, spread/depth and side liquidity. Compare all eligible token opportunities, then add activity/capacity proxies incrementally. |
| Size is a liquidity budget or risk cap | Requested signed quantity, immediate sweep depth, resting residual, prior observed exposure and paired-side inventory. Test discrete size tiers versus available liquidity; include censored/residual orders and avoid deriving size from actual future fills. |
| Reversal cancels a resting tail | Prior adverse underlying returns, near-strike risk, side-price/spread changes and queue/depth changes at inferred cancellation. Use right-censored hazard models or bounded event study, not fallback contract-end caps as real exits. |
| Allocation across coins/tenors explains non-entry | Strictly earlier public activity, observed unexpired gross exposure and broad allocation state. Test whether incremental selection association persists after baseline eligibility and time. These proxies cannot certify capital or missing orders. |

The theory may combine several mechanisms. Test interactions only when they
have an economic meaning and enough observations. For a 15m versus 5m
comparison, normalize underlying moves/strike distance by past volatility
and remaining time; don't simply transfer a threshold selected on BTC5.
Overlapping 5m and 15m contracts are dependent and must remain in the same
chronological split.

Decision time is latent. Define one fixed primary print-to-decision offset
and a short sensitivity grid justified by evidence, or integrate a declared
decision-time interval. Publish the uncertainty. A model that improves only
when its cases choose their best offset is unqualified. Keep maker post
fingerprints separate from lower-price print-anchor proxies.

## 4. Use chronological controls and honest evaluation

For case/control association use same side/contract controls at declared
earlier times, outside a window around other observed target prints. Also
use population controls matched on time to expiry, ask-price bin, spread,
volatility regime and feed/book availability across all eligible contracts.
No-observed-fill controls remain censored. Show denominator and exclusion
changes for each feed. A balanced control sample's AUC is not population
precision; report performance at the actual opportunity rate separately.

A transparent two-day study can use September 30 for design and October 1
for a once-declared check, but both days are already historically examined.
State that it is reused checking. Avoid random row splits and event twins
across overlapping tenor windows. Group uncertainty by contract/time block
and do not present a two-day day-bootstrap as evidence of generalization.
Use historical context tables to check earlier directionality where feature
coverage exists; they do not supply missing raw market feeds.

For classifiers report precision/recall/F1 and PR curves, calibrated likelihood
or log loss, event timing errors and the real base rate. Match orders using
one declared contract/token/timing tolerance with one-to-one assignment;
report tolerance sensitivity rather than tuning it by outcome. Use salt-linked
observed entries as the target where possible; report transaction-token
aggregation separately. Distinguish no order, no observed fill and unsupported
timing labels.

For an approximate policy report arm/side agreement, eligible-token selection,
size error, number of false apparent entries, missed targets and event-time
error. Daily PnL correlation is secondary. Report UP/DOWN and BTC5/BTC15
separately, as well as chronological hour/volatility/queue-size strata.

## 5. Require falsification and robustness

Retain all tested variants, including earlier unsuccessful rules. At minimum
run these checks where data permits:

1. Swap side alignment and shift feature times before/after the event; a
   causal lead should not emerge solely with future features. Preserve
   contract boundaries and local trading intensity in time-shift nulls.
2. Circularly shift wallet events within compatible contract/time bins or
   permute within day/price/tenor strata; compare against the declared real
   controls. Random controls drawn from impossible times are too easy.
3. Perturb receipt/event latency across a declared small grid, enforce
   `receipt <= decision`, and remove stale/gap states. Compare complete-case
   samples so missingness does not fabricate feed superiority.
4. Exclude self-affected book updates, target transaction prints and
   salt-linked footprint fields from predictive features. For maker replay
   remove the historical footprint once; do not leave it in depth while
   inserting another counterfactual copy.
5. Mutate future rows and outcomes; earlier features and entries must remain
   unchanged. Audit tie order, exact identity joins, complement mapping,
   integer units, .001 ticks and boundary/warmup handling.
6. Compare nested simple models and neighborhoods of thresholds; report
   unstable/tiny subgroups, multiple comparisons and outlier dependence.
   Register a limited search budget. Do not expand a grid after every check
   failure until a successful parameter appears.

Before claiming a feed lead, compare genuine receipt and retrospective event
versions and explicitly state that the collector differs from the wallet.
Historical oracle and exact feed vendor can remain unidentified even when
the mechanism is well supported.

## 6. Separate strategy inference from execution parity

Use the bundled latest maker and taker reference logic and their documented
versions. Reference validation from Q99 or avg_bot/vol075/TailTaker/A2 applies
to specified execution semantics and qualified cohorts, not to the truth of
BoneOhio entry rules. New engine assumptions require a named diagnostic,
retained reference output and independently justified comparison.

First replay actual historical observed orders with exact identified fields
and clearly inferred lifecycle times. Require independent accepted-order
parity evidence including nonfills; report misses, false fills, quantities,
price/time error and queue-size/price/role strata. Check the known maker
loss partitions only as execution forensics. They are provided precisely
because the canonical model misses the important adverse fills; no Q
haircut, snapshot selection or synthetic depth fill may be fitted to their
known outcomes.

Then replay independently generated approximate-policy orders with full
finite-depth FAK, per-level fees, limit bounds, partial/nonfills, realistic
latency stress and fixed settlement labels. Record trigger/decision/arrival
and hold components separately. Report execution exclusions, candidate/fill
counts and fee totals. For maker counterfactuals, unknown queue position and
cancellation may require ranges rather than a precise economics claim.
Suppress outcomes missing resolution, not their fill observations.

Present gross/net PnL, per-share edge, drawdown, adverse selection, losing
cases, sizing/capacity stress and both day results. Include initial-zero
equity. State whether drawdown is contract-end payout proxy or actual
cash/mark-to-market. Never use the final win/loss label to select entry
windows, queue haircuts, capital gates or latency.

## 7. Deliver a defensible approximate strategy and next gate

Save a versioned small policy or policy family, its exact feature/clock rules,
frozen parameters and code hashes. Add a table for every hypothesized behavior:
supported, rejected, unresolved; evidence size; incremental effect; coverage;
counterexamples; next discriminating measurement. Provide charts around
representative failures and the complete eligible-event denominator.

The final dossier should identify the simplest supported trigger/price/size
and lifecycle model, how far it reproduces the target, which variants failed
and why confidence is bounded. A clear approximate strategy may be enough
to guide the next experiment even if exact feed or selection cannot be
identified. Do not label a weak replica recovered merely because its PnL
looks similar.

The best next data, when ambiguity persists, is genuine prospective external
feed receipt, independent paired Polymarket market sockets, decision and
submission clocks, accepted-order acknowledgements, user-stream matches,
cancel lifecycle and both filled/unfilled orders from an authorized own
account. This public package cannot obtain BoneOhio's private user stream.
Capture provider-specific historical oracle inputs and settlement/cashflow
if fair value or capital explanations remain unresolved. Freeze candidate
rules before untouched future days and evaluate without retuning.

Required deliverables: reproducible runnable code, manifests and parameters;
input/identity/coverage audit; descriptive arm/lifecycle atlas; all hypothesis
and null-test tables; chronological model/entry metrics; execution-parity
report with known adverse failures; approximate strategy specification;
uncertainty and missing-data note. Save small outputs and checkpoint after
each stage so another cloud run can continue without Viper access.
