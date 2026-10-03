# BoneOhio 0.99 selection: prior activity explains only a small part of the gap

**Verdict: unresolved selection mechanism; Grade C, reused historical diagnostic.** Causal prior-print activity has some association with observed wallet orders, but it does not recover the missing selection rule. The simple 15-minute activity gate chosen on the design window raises September 21–30 precision from 27.95% to 29.62%, while recall falls from 96.03% to 91.59%. No tested variant exceeds 37.37% precision on that reused window. These are order-presence diagnostics, not a trading backtest, P&L result, or deployment recommendation.

## Question and method

The candidate universe is BTC 5-minute Polymarket contracts with existing ENTRY grids. For each contract-token, select the first 100 ms grid state with favourite ask in `[0.985, 0.99]`, time to expiry in `[1, 180]` seconds, and book age in `[0, 1000)` ms. The target is at least one observed `b_orders` 0.99 order on that contract-token, without requiring an inferred decision timestamp or successful post-time fingerprint.

Activity features use only `gt_orders.print_ms + specified_delay < candidate_time`. They exclude every print in the candidate's own contract, including the opposite token. Windows of 5, 15, 60, 240 and 1,440 minutes, 1- and 5-second availability-delay sensitivities, same/other coin and same-market activity, prior gross flow, concurrently unexpired observed contracts and coarse calendar controls are exported. The restricted design-only activity-window choice compares the baseline and the five delay-zero lookbacks; it chooses 15 minutes. It is **not** the highest-scoring design rule among all other control families.

No outcome, future known activity window, signing time, inferred decision time, realised P&L or resolution is used as a feature. All grid-covered dates stay in the denominators, including periods of little activity. Calendar cells are fitted on September 10–20 only. All 41 distinct rule variants and the duplicated design-chosen result are retained, including unsuccessful controls.

## Main results

| Reused period | Rule | Selected | Matched | False positive | False negative | Precision | Recall | F1 |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Sep 10–20 | First eligibility | 3,148 | 1,418 | 1,730 | 14 | 45.04% | 99.02% | 0.6192 |
| Sep 10–20 | Prior print within 15m | 3,113 | 1,411 | 1,702 | 21 | 45.33% | 98.53% | 0.6209 |
| Sep 21–30 | First eligibility | 2,859 | 799 | 2,060 | 33 | 27.95% | 96.03% | 0.4329 |
| Sep 21–30 | Prior print within 15m | 2,573 | 762 | 1,811 | 70 | 29.62% | 91.59% | 0.4476 |
| Oct 1, through 12:50 UTC contract start | First eligibility | 154 | 34 | 120 | 1 | 22.08% | 97.14% | 0.3598 |
| Oct 1, same partial window | Prior print within 15m | 139 | 34 | 105 | 1 | 24.46% | 97.14% | 0.3908 |

The 15-minute gate removes only 249 of 2,060 false positives on September 21–30 (12.1%) and also discards 37 of the baseline's 799 matched targets. It still selects 1,811 contract-tokens without a corresponding observed order. Across all available dates, the baseline matches 2,251 of 2,299 observed target contract-tokens, but only 36.54% of its 6,161 candidates have a target record. The 15-minute gate changes those figures to 2,207 matches out of 5,825 selections, or 37.89% precision and 96.00% recall.

## Other explanations tested

- **Very recent activity:** the 5-minute gate improves September 21–30 precision to 36.51% and F1 to 0.4831, but recall falls to 71.39%. Its design F1, 0.6177, is slightly below the baseline's 0.6192. Five seconds of assumed attribution delay changes September 21–30 F1 to 0.4778; the broad association survives this small delay but still does not reproduce selection.
- **General activity versus coin allocation:** on September 21–30, a 15-minute prior print in BTC5 gives precision/recall 34.08%/78.61%; any BTC market gives 32.41%/85.70%; another coin gives 32.45%/79.57%. Activity across other coins also carries an association, consistent with broad trading-intensity variation. This does not identify its cause.
- **Prior turnover:** requiring at least $5,000 of gross absolute observed fill notional in the previous hour has the highest September 21–30 F1 among the tested rules: 0.5105, precision 37.37%, recall 80.53%. It was not the chosen simple activity-window rule. On partial October 1, F1 falls to 0.3308, below the baseline's 0.3598; precision is 22.45% and recall 62.86%. This remains an exploratory activity association, not recovered capital logic.
- **Concurrency or gross-exposure caps:** requiring no other observed, unexpired contract gives September 21–30 precision 23.69%, recall 53.85%, F1 0.3290. Allowing at most one gives F1 0.4034. Limiting unexpired gross observed fill notional to $100 or $1,000 gives F1 0.3636 or 0.4165. These simple capacity-suppression hypotheses perform worse than the baseline. Conversely, at least one other unexpired contract has higher precision (36.26%) but only 42.19% recall. Nothing here establishes a wallet cash balance or available capital.
- **Calendar:** design-selected 4-hour UTC buckets have September 21–30 F1 0.4239, and selected weekdays have F1 0.3825, both below baseline. Combining 15-minute activity with the selected hours gives F1 0.4262. These coarse calendar controls do not solve the gap.
- **Venue:** no between-venue control is identifiable in this Polymarket-only prediction dataset.

## Chronological stability

The later deterioration comes mainly from fewer observed target orders while the simple price/time condition continues to trigger in approximately every contract.

| Seven-day interval, end exclusive | Eligible tokens | Observed target tokens | Baseline precision | 15m-gate precision | Baseline recall | 15m-gate recall |
|---|---:|---:|---:|---:|---:|---:|
| Sep 10–17 | 2,031 | 998 | 49.14% | 49.40% | 100.00% | 99.80% |
| Sep 17–24 | 1,972 | 804 | 39.20% | 39.85% | 96.14% | 95.02% |
| Sep 24–Oct 1 | 2,004 | 462 | 22.26% | 23.56% | 96.54% | 89.39% |

The prior-print gate does not arrest that decline. This is a descriptive pattern in already examined history; no period in this study is fresh or sealed out-of-sample.

## Coverage, timing and identification limits

- Successfully read 6,188 existing BTC5 grids, with no read failures or negative book-age rows. There are 6,161 eligible contract-token candidates and 2,299 observed target contract-tokens. All 2,299 targets in the supplied study date range lie in grid-covered contracts, but only 2,251 have a matching eligible candidate token.
- September 11 lacks one scheduled grid and September 19 lacks 14. September 21–30 has all 2,880 scheduled grids. October 1 includes only 155 of 288 scheduled contracts, ending at 12:50 UTC; its target sample is only 35 contract-tokens. Grid existence is not a certificate of complete event capture.
- Activity uses 43,009 matched transaction-token records for BTC, ETH and SOL, 5m/15m, from September 10 00:00:19.079 UTC to October 1 19:49:06.703 UTC. Another 861 source records (1.96%) have no mapped print time and cannot enter causal features. Earlier history and other coins, markets and venues are absent. Missing prints can resemble inactivity.
- Public venue print time is not demonstrated collector receipt time or wallet-attribution availability. `gt_orders` maps aggregated transaction-token data to the transaction's earliest matched print. Therefore exact immediate knowledge of aggregate notional and every attributed token is not proven. Delay-zero results are idealized; 1s/5s count sensitivities are not a complete live-availability certification.
- The target denotes an observed `b_orders` record. This study does not read a complete submission ledger. A false positive therefore does **not** prove the wallet never attempted an order or submitted an order that remained unfilled and was cancelled.
- Among matched targets with known first prints, candidate-minus-print medians are −5.83s, −6.25s and −4.68s across design, September 21–30 and partial October 1. Candidate eligibility precedes the first target print in 95.49%, 97.12% and 94.12%, respectively; only approximately 43–47% are within five seconds. Timing is missing for 32 of the 2,251 matches. Order-presence overlap is not decision-time reproduction, and even preceding a print need not precede the wallet's decision.
- Concurrency and unexpired notional use known contract expiry, not actual redemption/settlement time. They count observed gross flow, not net inventory, cash, margin, outstanding orders or available capital.
- A gate is evaluated at first baseline eligibility. It does not model waiting until later activity re-enables a contract. No execution, fees, taker hold, queue, sizing, exits, equity or MPDD is replayed here.

## Reproduction and evidence

Executed centrally by the root agent in Slurm job **12060945**, using the `general` partition with 128 requested cores and 12 grid-reader workers. No live services, old experiments, caches, ATLAS or ledger were modified by this subtask.

```bash
/mpcdf/soft/RHEL_9/packages/x86_64/python-waterboa/2025.06/bin/python \
  $VIPER_HOME/vania/boneohio_20261002_FINAL/selection/selection_activity.py --workers 12
```

Code SHA256: `b0ac8e4d570b6a0e62332c94c3a981a0c0e492d143579d82cfc1a4ddd2ac9ea0`.

Inputs are read-only: `$VIPER_PTMP/exp/boneohio_20261001_ENTRY/grid/btc/btc_5m/*.parquet` and `$VIPER_PTMP/exp/boneohio_20261002_PARITY/{b_orders,gt_orders}.parquet`. The ENTRY 100 ms grid is the candidate engine; this is a standalone selection diagnostic, not a fill-model version.

Outputs: `$VIPER_PTMP/exp/boneohio_20261002_FINAL/selection/`. The primary audit files are `selection_metrics.csv`, `selection_daily.csv`, `selection_weekly.csv`, `coverage_daily.csv`, `candidate_timing.csv`, `activity_scope.csv`, `selection_strata.csv` and `methodology.json`. Full candidate features, prediction masks, target tokens and per-grid coverage are retained on Viper as parquet/CSV. Local copies of the summary files accompany this result. The root report supplies the combined dashboard; this subtask has no strategy equity curve to plot.

Verification before execution: Python compilation, 288 comparisons against brute-force activity/concurrency calculations, and a future-event mutation test proving that later amounts cannot change earlier feature values. Runtime completed with no grid errors. Post-run review reconciled candidate/target totals and daily/weekly denominators.
