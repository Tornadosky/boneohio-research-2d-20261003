# BI-1 fixed-intent taker latency stress

Status: completed. Slurm job `12060943`, `general`, 128 requested CPUs, exit 0; 34 seconds scheduler elapsed (31.44 seconds measured Python replay), 46.7 GB reported peak RSS. Grade C: reused historical strategy robustness, not an independent forward validation.

At default fees, the frozen strategy remains profitable across these recorded execution-delay scenarios. The evidence does not justify promoting it from shadow: the latest reused partial day loses at the baseline, the recent edge is smaller, and the experiment cannot certify actual receive-time strategy decisions.

## Results

All-period rows cover 3,915 saved intentions in 3,052 BTC5 contracts, August 18 through October 1. October 1 is partial: the final saved contract starts at 19:25 UTC.

| Total E→match | Added matching delay | Net P&L | Payout-proxy DD | Net/DD | Fills | Depth coverage | Sep 21–30 net | Oct 1 partial net |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 300 ms | +0 ms | $1,909.41 | $117.01 | 16.32 | 1,550 | 99.642% | $651.37 | $-13.20 |
| 325 ms | +25 ms | $1,713.35 | $101.81 | 16.83 | 1,456 | 99.617% | $538.14 | $-11.11 |
| 350 ms | +50 ms | $1,567.52 | $96.99 | 16.16 | 1,400 | 99.617% | $547.84 | $5.45 |
| 400 ms | +100 ms | $1,499.58 | $85.85 | 17.47 | 1,297 | 99.642% | $526.23 | $14.58 |
| 500 ms | +200 ms | $1,089.57 | $87.35 | 12.47 | 1,161 | 99.617% | $343.55 | $17.33 |

Baseline gross P&L is $2,220.77 and fees are $311.36; 29,637.10 shares execute at an average price of $0.4655, giving 6.443 cents net per share. The first +25 ms reduces total net by 10.3%; +200 ms reduces it by 42.9%. September 21–30 net falls from $651.37 to $343.55 between 300 and 500 ms. The non-monotone drawdown and small October 1 outcomes are conditional fill-subset effects, not a reason to select a slower latency.

Chronological L300 results: reused training through September 20 $1,271.25 net/$117.01 DD/1,179 fills; September 21–30 reused validation $651.37/$44.36/343 fills; October 1 partial reused validation −$13.20/$38.56/28 fills.

Weekly L300 net cents per share are 1.06, 2.59, 0.95, 15.49, 11.58, 10.39, and 6.13 for weeks starting August 17, August 24, August 31, September 7, September 14, September 21, and September 28 respectively. Boundary weeks are partial. This is materially weaker before September 7 and decays across recent weekly cohorts; it is not stationary evidence.

At L300, 3,901/3,915 intentions have usable depth. Thirteen fail the 1-second book-age guard and one has a missing/invalid BBO. At L400 one additional BBO/ladder inconsistency appears; every rejected intention remains in its variant denominator. No read/replay error or invalid-venue rejection appears in the returned coverage table.

The legacy L300 diagnostic yields $1,945.04 net, $117.01 DD, and 1,563 fills. It reproduces the prior October 1 result (−$0.9003, 29 fills). The strict replay differs in exactly 13 filled intentions, all removed by the stale-book guard: −260 shares and −$35.6283 net in aggregate; all 3,901 covered intentions have identical quantity and P&L. On October 1 this is a single 20-share stale-book winner worth +$12.3042 in the legacy model, explaining the strict −$13.2045 result. There is no baseline covered-intent change attributable to deeper levels or clock fallback in this run.

Net/DD is the terminal historical net divided by maximum contract-end-payout-proxy drawdown. It is not a return on capital, annualized statistic, live cash risk, or guarantee of future results.


This checks how the already frozen BI-1 BTC five-minute decisions execute after small extra matching delays. It does not recover BoneOhio's complete taker strategy, regenerate the strategy on Binance aggTrade time T, or establish a deployable edge. Every period is reused research or validation, including October 1.

## Frozen experiment

Source intentions are `ENTRY/stress/trades_h_F2.parquet` through September 30 and `trades_fwd_F2.parquet` on October 1. The `h_` export applies the original gap mask. The newer forward export is authoritative for its day so the partial older export cannot select a conflicting first entry. Source intentions must contain at most one order per market and token side; conflicting repeated keys fail the run.

The saved BI-1 rule is side-aligned Binance perp movement at least 3 bp over 1 second, ask 0.05–0.95, ask increase at most 2 cents in 1 second, 10–300 seconds remaining, original gap checks, FAK limit decision ask +1 cent, 20 shares, hold until resolution. No thresholds, fee multipliers, instrument sleeves, or entry counts are selected by this experiment.

F100 is the saved venue-book decision snapshot at cached Binance depth-push timestamp E+100 ms. Total E-to-matching latencies are 300, 325, 350, 400, and 500 ms. Thus the post-decision budgets are 200, 225, 250, 300, and 400 ms. The +25/+50/+100/+200 stresses add matching delay after the frozen decision. F100 is included in L; it is not added a second time. There is no assumed historical hold splice. The official September 4 14:00 UTC hold change appears only as a descriptive split.

## Execution and accounting

Code: `taker_stress.py`, engine `frozen-bi1-full-depth-v1`. Cache: `$VIPER_PTMP/polycache_btc_twap/cache_btc_pflow_v1/normalized_depth_events`, BTC5 partitions corresponding to the saved intentions.

A full ask ladder is reconstructed from snapshots and absolute size updates, ordered by positive venue timestamp, source sequence, and stable source order. An event whose venue timestamp equals the match timestamp is excluded. The query identity contains the intention ID and latency; saved intention identity includes market, contract start, token side, and trigger time. A snapshot is required. Levels below the carried venue ask are pruned as in the earlier executor; if that ask is absent from the reconstructed ladder, coverage fails. A side's last event must be at most 1 second old. No arbitrary 12-level cap is used in the strict variants.

Invalid venue timestamps are never replaced with `ts_ns/1e6−15`. An invalid row only invalidates a query once its recorded `ts_ns` precedes the query and its source sequence follows the last valid snapshot; a later valid snapshot recovers the side. A future invalid row does not reject earlier intentions. This does not certify `ts_ns` as a genuine independently observed local receipt clock. Decisions remain conditional on the old F100 cache.

Fees are exactly `sum(0.07*p_level*(1-p_level)*filled_shares_level)` USDC. Net P&L is winning payout minus execution cost and fee. Unavailable/unseeded/stale/inconsistent depth and read errors produce no simulated fill and remain in the intention denominator with explicit coverage reasons. No liquidity is inferred from a later event. There is one frozen order per market-side, avoiding repeated use of the same side ladder by this strategy; competing orders and historical counterfactual liquidity remain unobserved.

Drawdown uses cumulative final P&L ordered and grouped at contract end (`cs+300000`) as a resolution-payout-time proxy. This is not true payout transaction time, live cash drawdown, or marked-to-market drawdown. MPDD is terminal net P&L divided by the maximum drawdown of that series. Daily equity is sampled from that series; `equity.csv` retains each contract-end step. Chronological split membership is by contract start. Weekly splits start Monday and also use contract start.

## Legacy comparison and limits

`legacy_L300_diagnostic` executes the same saved intentions using the earlier ladder policy: timestamp fallback, seeded ladder, carried-BBO pruning, and first 12 levels, without the added coverage guards. It is diagnostic, not another selected strategy. Its merge preserves token side, correcting the earlier comparison's omission of that key. `legacy_comparison.csv` records intention-level quantity/P&L differences and strict coverage reason; `coverage.csv` preserves all rejects.

The original candidate timestamps are Binance depth-push E, not aggTrade execution T. F100 book selection used Polymarket venue time, not verified local receipt time. These tests isolate sensitivity of frozen decisions to execution delay. They cannot determine whether a functional strategy observing an actual delayed feed would make the same decisions. Missing intention dates do not prove complete market/feed coverage, and the October 1 forward export is limited to the source's recorded window.

## Validation and reproduction

All 3,052 selected contracts were read successfully: 955,357,397 normalized source rows, zero duplicate event UIDs removed, zero missing partitions, and zero read/replay errors. Every variant’s daily and weekly net/fills, coverage count, default-fee subtraction, terminal equity, and exact contract-end maximum drawdown reconcile to the summary. Executed script SHA256 matches the reviewed local file. The Slurm log confirms self-check success before replay.

Local static compilation passed. Synthetic checks passed for strict equal-time exclusion, full-depth partial fills, per-level default fees, limit rejection, distinct token-side identity, missing snapshots, stale books, invalid past/future timestamps, and contract-end accounting across midnight. Each scheduled run repeats the execution self-check before loading data.

```bash
mkdir -p $VIPER_HOME/vania/boneohio_20261002_FINAL/taker
mkdir -p $VIPER_PTMP/exp/boneohio_20261002_FINAL/taker
sbatch $VIPER_HOME/vania/boneohio_20261002_FINAL/taker/run.sbatch
```

The runner requests `general`, 128 CPUs, 30 minutes, and 120 single-thread workers. Direct reproduction inside that allocation:

```bash
/mpcdf/soft/RHEL_9/packages/x86_64/python-waterboa/2025.06/bin/python \
  $VIPER_HOME/vania/boneohio_20261002_FINAL/taker/taker_stress.py --np 120 \
  --out $VIPER_PTMP/exp/boneohio_20261002_FINAL/taker \
  --inputs $VIPER_PTMP/exp/boneohio_20261001_ENTRY/stress/trades_h_F2.parquet \
  $VIPER_PTMP/exp/boneohio_20261001_ENTRY/stress/trades_fwd_F2.parquet
```

Outputs: `latency_summary.csv`, `daily.csv`, `weekly.csv`, `equity.csv`, `coverage.csv`, `legacy_comparison.csv`, `contract_audit.csv`, `trades_all_variants.parquet`, `frozen_intents.parquet`, `metadata.json`, and the Slurm log. Metadata records exact source paths, sizes and mtimes, script SHA256, clocks, assumptions, and runtime.
