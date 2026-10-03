# Evidence limits and label rules

Prepared 2026-10-03. The package supports exploratory mechanism research and
execution forensics. September 30–October 1 are reused historical observations,
not untouched validation. Two days cannot establish a stable recovered policy.

## Observed, derived and unknown fields

| Quantity | Status | Safe use |
|---|---|---|
| Public API token, tx, price, shares, integer-second timestamp | Observed API representation | Coarse wallet activity, price/quantity distributions, exact identity joins; audit duplicate/pagination coverage. Seconds cannot locate subsecond decisions. |
| Public-chain salt, token, maker/taker amounts, order timestamp field, argument position, transaction/block | Decoded observed calldata | Link exact filled order identity and role; keep raw units. The timestamp field is not proof of wall-clock signing/submission. |
| API-derived maker/taker role | Historical membership in a taker-only API pull | Descriptive labels subject to API mapping/completeness. Prefer per-fill decoded role where available; one salt can switch role after a sweep. |
| `first_ms`, `last_ms`, `print_ms`, `mapped_print_venue_ms` | Retrospectively attributed public venue prints | Event-forensics anchor with mapping provenance. Unique transaction/token and price consistency do not certify causal entry time. Never call these authenticated submit or match receipts. |
| `m` | `print_ms-40ms` assumption | Sensitivity anchor only; require alternate offsets and a range of candidate decision windows. |
| `post_ms`, `state`, `t_end`, `remain_end` | Visible book-size footprint inference | Historical conditional replay and lifecycle hypotheses. Genuine placement, cancel ACK and order exit are not observed. |
| `start`, `next_start`, `prev_end` in maker history | Derived lifecycle ordering | `start` falls back to first fill without a fingerprint. Censored states do not become observed exits. |
| Binance aggTrade `T` / `E` | Venue trade / event-publication time | Retrospective event association; receipt-aware inference requires the record to have arrived. |
| External-feed receipt field | Recorder observation clock where genuine | A causal availability bound for that collector. Not the target wallet's clock/location. |
| Main Poly cache `ts_ns` / feature `source_cache_ns` | Vendor earliest-per-row collector receipt, clipped to venue; derived cleanup rows reuse a parent timestamp | The two-day source manifests declare earliest-received witness merging. This is not BoneOhio receipt or one fixed native observer. Preserve the exact lineage below. |
| Outcome/fee/PnL/`won` | Retrospective resolution and research accounting | Evaluation only. Unknown payout stays null and is excluded/flagged, never zero by default. |
| Apparent target non-entry | No filled order observed | Censored negative, potentially an unobserved zero-fill submission. It is not certified non-submission. |
| Gross unexpired observed flow | Public fills plus contractual expiry | Activity/exposure proxy; not available collateral, net inventory or actual redemption cash. |

Token IDs and salts are large identifiers. Preserve strings and never convert
token IDs to floating point. Prices/shares expressed as micros divide by
1,000,000 exactly. In decoded BUY orders `maker_amt` and `fill_amt` are in
the maker asset (USDC raw units), while share size comes from the other asset;
the aggregate `fill_sh` was historically derived using the order limit and
is approximate for taker executions with price improvement. Do not sum it
as a second independent observed fill total.

## The historical wallet sources have different cutoffs

| Export | Historical scope / missingness |
|---|---|
| `context/wallet/fills_history.parquet` | 162,688 fills, August 15–October 1 21:08:38 UTC; the retained API snapshot's right tail is censored. Raw coin strings include aliases and eight normalized groups, not just BTC. |
| `decoded_order_summary.parquet` | 23,751 salts from transactions selected since September 10; order timestamp fields run September 9 23:58:34.154 through October 1 21:03:23.116 UTC. BTC/ETH/SOL catalogs map 13,261 rows; 10,490 have no catalog coin mapping. |
| `chain_order_fill_links.parquet` | 57,880 decoded BoneOhio appearances across two retained public transaction parts. Contains filled transactions only; no zero-fill ledger. |
| `maker_lifecycle_history.parquet` | 8,875 near-.99 records across BTC/ETH/SOL 5m/15m/1h. Only 5,731 have a post fingerprint and 1,907 have a finite inferred exit; 3,824 `alive`, 3,144 null states. This broad table is not the 846-order maker replay cohort. |
| Order summaries and lifecycle `first_ms`/`last_ms` | Their original mapped tape reaches only October 1 12:49:02.174 UTC, even though order timestamp fields extend later. Missing afternoon mappings must not imply afternoon inactivity. |
| `matched_fill_groups_history.parquet` | BTC/ETH/SOL 5m/15m only, 43,870 groups; 43,009 prints through October 1 19:49:06.703 UTC, 861 missing. Some groups validly use complement-token prints. |

Exact source/output hashes, columns, ranges and counts are in
`context/PROVENANCE.json`. Fresh two-day public-wallet exports are separate
bundle inputs. Prefer their declared coverage for the main period; retain
historical rows and conflicts when merging. A later API timestamp alone is
not a completeness certificate. Audit pages, cursor exhaustion, all-vs-taker
pull consistency, deduplication and transaction/token joins. Activity snapshots
may add redemptions/merges/splits, but current positions cannot reconstruct
unobserved historical orders or exact past cash availability.

The fresh bounded public v2 walks completed October 3: **3,314 fills**, 828
taker-only and 4,333 activity rows, all cursors exhausted, no exact duplicates
and no taker keys missing from the all-fill set. Requested bounds cover the
whole two-day window; the latest recorded fill is October 1 23:59:09 UTC.
Public service status reported a 1s serving lag at retrieval; that is source
freshness rather than independent completeness or receipt certification.

All 307 transactions missing from the historical chain source were decoded
with the existing explicit ABI and public RPC, giving 307 target appearances
and 57 salts with zero exceptions. Direct transaction/token joins cover all
fresh fills with zero role disagreement; the resulting 1,109 filled-order
summaries cover only activity in the slice. API reporting seconds and order
timestamp fields remain separate. The separate conservative print matcher
finds 1,613 direct and 635 complement price-consistent candidates among 2,256
Bitcoin fills. Eight remain unresolved and 1,058 all-coin rows are outside the
BTC catalog. All complement candidates have a separately decoded maker role;
this does not establish pure-maker behavior for their whole signed-order salt.
The matcher preserves multiple-candidate ambiguity and null mapped times,
although this particular run has no ambiguous preferred candidates.

Latest candidate print time is October 1 23:59:05.894 UTC. Seven unresolved API
times exceed contract end by 63–107 seconds; the converter only stored venue
trades strictly before contract end plus 60 seconds. One unresolved fill lies
inside the observed partition range. Partition first/last print fields and API
boundary flags preserve this diagnostic without certifying continuous capture
or repairing missing events. See `context/FRESH_VENUE_MATCHING.md` and
`wallet/FRESH_VENUE_PROVENANCE.json` inside context. True placements, decisions,
cancel states, unfilled orders and exact capital paths remain unobserved.

Activity includes 1,014 redemptions, two maker rebates, two taker rebates and
one withdrawal, in addition to 3,314 trades. Include those observed cashflows
in accounting when exact asset units are known, but do not infer an initial
balance or full net inventory from two days. Public role inference was
provisional before the direct chain validation; retain both labels and their
comparison rather than silently rewriting source fields.

## Market and external coverage

The two main UTC days should contain 576 BTC5 and 192 BTC15 contracts.
Contract presence and nonnull resolution labels do not prove continuous full
depth, causal receipt clocks or an available fill at every query. Keep seed,
staleness, gap, crossed-book, invalid timestamp, replay/BBO mismatch and source
flags. Warmup rows and late lifecycle/settlement rows outside the headline
interval are boundary support, not additional complete days.

The pflow cache name `pflow_v1` describes that built lineage. A surrounding
`v4` layout or recent build does not turn every source partition into the
same source-generation version. Inspect `file_name`, manifest and source
quality sidecars. Record Polymarket event and receipt conventions per table.
Best-available does not mean zero-gap or full-depth certified.

The main two-day pflow clock was traced through the actual converter source
and **all 48 hourly manifests**, plus one warmup and one tail hour. Every
examined hour declares earliest-received merging across vendor witnesses,
with the winning witness chosen separately for each row. The converter sets
`ts_ns=max(timestamp_received_us*1000, venue_ts_ms*1000000)`.
Earlier `LAST ACTIVE` hours subtract `arrival_skew_us` before that clipping;
none of the main 48 hours uses that branch. The table adapter preserves
the supplied clock; its separate venue-plus-median-lag `reclock` routine
is not used in this pflow-only path. Synthetic BBO-pruning rows reuse their
parent timestamp and receive new sequence numbers. See
`context/PFLOW_CLOCK_PROVENANCE.json` for source hashes, branches and each
hour's manifest fingerprint.

This establishes a vendor merged collector-receipt proxy for the period,
not a single physically observable route or the target's availability. The
derived 100ms view uses strict prior cache timestamps and carries the field
as `source_cache_ns`; source validity and age are bounded feature checks,
not a certification of source capture or wallet causality. Native Binance
recorder receipt-asof is a separate clock track. Do not apply a constant
invented venue-to-receipt delay to make these routes equivalent.

Full Polymarket event depth matters for residual maker queue, .001 tick
levels above .99, complement trades and finite FAK fills. Top-of-book or
several levels alone cannot validate large pure-maker fills. Full Binance
depth needs snapshots plus contiguous update-ID chains; partial levels are
usable order-pressure features only within their stated scope. If extra
Binance levels are unavailable, do not manufacture them from a mid or BBO.
An aggTrade stream is not a separately recorded individual raw-trade stream.

Earlier short multi-venue comparisons and case/control tables cover
September 12–13. They are useful context but not simultaneous additional
feeds on September 30–October 1. Historical Chainlink was not tested in
FINAL. Any extra feed included now must carry its own symbol, venue,
instrument, provider, clock, route, coverage and update-ID provenance.

The public release excludes CryptoHFTData raw/reconstructed tapes and
per-event vendor price/size sequences because the source terms restrict
redistribution. That exclusion leaves some hoped-for extra vendor levels or
simultaneous venue coverage unavailable through a public download. Main
public-feed research must use included native-recorder inputs; optional
vendor acquisition belongs to the recipient's own authorized access. A
missing vendor tape is not a reason to substitute invented levels or
relabel a retrospective series. See
[CryptoHFTData's terms](https://cryptohftdata.com/terms) and the main package
distribution notes.

## Execution and economic acceptance limits

Recorded-order maker replay conditions on actual historically filled orders,
inferred posts/exits and a historical footprint. It cannot estimate the
specificity or economics of orders that a new signal would have submitted.
The canonical maker still misses both large pure-maker losing orders.
Independent reference parity on smaller accepted orders does not close
that gap. Keep every model's false fills, misses, share ratio, first-fill
error, losing-order behavior and size/queue regimes alongside PnL.

Five optional known-loss raw partitions are explicitly execution diagnostics.
They must not enter a claimed fresh holdout, tuning target or objective to
repair Q by matching PnL. Reject a claimed maker economic replica until
parity can explain important adverse cases under independently justified
mechanics and accepted zero-fill/cancel samples.

For taker experiments keep decision source availability, venue arrival,
hold/transport latency, finite depth, partial fills, limit cap, size semantics
and per-level fees explicit. Historical BI1 total latency already includes
F100; do not count it twice. Use valid real settlements and distinguish
contract-end payout-proxy equity, marked-to-market drawdown and actual
redemption cashflow. Rebate exclusions, missing fees and missing outcome
rows must be stated. Profitability is secondary to identification accuracy.

## Dated provider documentation correction

The immutable October 2 FINAL report says PolyBolt `price.crypto` is Pyth.
On **October 3**, the currently opened official migration page instead says
`price.crypto` defaults to Chainlink where supported, supplies a source
field and permits provider pinning; `price.crypto.twap` is the 60-second
Chainlink TWAP. The package preserves the old report and this discrepancy.
Use explicit payload provider/topic metadata for any new data. Current
documentation cannot prove BoneOhio's historical feed. See
[Polymarket's migration page](https://docs.polymarket.com/migrate/rtds-to-polybolt)
and `SOURCES.md`.

## Conclusions the cloud researcher must keep separate

Correlation around filled events is not a recovered decision rule. A high
balanced case/control AUC is not high precision in the enormous eligible-event
population. A filled-only target is selected by execution. A good same-order
replay is not an independent policy. A positive two-day equity curve is not
stable future edge. An approximate strategy should state which observations
it explains and the unresolved measurements needed to distinguish competing
stories.
