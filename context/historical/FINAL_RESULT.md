# BoneOhio final research audit — 2026-10-02

**Neither arm is certified for live deployment.** The lower-price arm has a testable Binance impulse candidate with substantial latency sensitivity, but it does not reproduce the wallet. The .99 sweep/rest mechanics are recognizable; its pure resting-maker economics still fail execution parity. New tests narrow the uncertainty and correct several stronger earlier claims. No live services, orders, or recorder settings were changed. Fees stayed at the existing default throughout.

This experiment is `boneohio_20261002_FINAL`. Code: `$VIPER_HOME/vania/boneohio_20261002_FINAL`; outputs: `$VIPER_PTMP/exp/boneohio_20261002_FINAL`; PC: `$LOCAL_TFKI/experiments\boneohio\boneohio_20261002_FINAL`. Open `dashboard.html` for the complete interactive dossier, full equity/drawdown paths, chronology, exclusions, negative variants, source tables, and reproduction notes. All performance periods have been examined before: **Grade C research**, not new sealed validation.

## What the feed investigation establishes

The cached Binance mids are batched depth updates. The historical Binance trade table contains **aggTrade**, not an independently captured stream of individual trades. Binance documents E as event time and T as trade time; neither is local availability. The individual-symbol bookTicker stream is a plausible real-time candidate, but this study has no separately identified historical capture to test it against aggTrade. Current stream specifications are primary-source context, not proof of the historical bot's subscription.

The fresh test uses fixed feature times independent of price/edge: `decision = print_ms - 190 = inferred_match - 150`. It retains 168 of 201 BTC5 low-price taker transaction-token groups from September 12 00:00 to September 13 21:30 UTC, paired with the same side 30 seconds earlier inside the same contract and away from recorded wallet prints. Both return endpoints must be at most one second stale. These balanced controls measure association, not trading precision. Missingness differs across feeds; only identical complete pairs are compared within each incremental model.

Nine recorded venues/market streams plus Binance aggTrade were evaluated on event and receipt clocks, with 500/1000 ms moves, lag curves, and a fixed logistic model fitted on September 12 and checked on September 13. On 47 paired check cases, adding Kraken perpetual receipt-time moves to Binance receipt-time moves raises AUC from **0.7861 to 0.8151** and lowers log loss from **0.6361 to 0.6169**. Bybit also carries incremental associations in several comparisons. These are small, reused, dependent samples with multiple comparisons and different collection routes; they neither identify Kraken/Bybit as the bot's feed nor justify the old categorical claim that no other venue explains its trades.

Among 52 matched cases with Binance aligned one-second move below 0.5 bp, Bybit perp has at least a 0.5 bp aligned event-time move in 19 and at least 1 bp in four; Kraken perp has seven and zero respectively. This threshold denotes insufficient aligned movement, **not absence of any exchange event**. Full counts and coverage are retained in `feed_clock/unexplained.csv`. Historical Chainlink, separate Binance raw trades, and individual-symbol bookTicker remain untested with the loaded sources. Current capture cannot retroactively resolve those cases.

## Clock and label audit

- Wallet API seconds trail mapped venue prints by median **2,467 ms**, p90 3,358 ms, over 43,009 matched groups. API time is unsuitable for subsecond trigger attribution. There are 861 unmatched groups.
- All 32,194 direct transaction/token matches have the same earliest time as the transaction mapping. The other 10,815 transaction-only mappings are opposite-outcome prints; 10,770 are within one cent of the complemented wallet price. None is a low-price taker. This is consistent with Polymarket complement execution, not evidence of 10,815 wrong timestamps. No mixed-role groups, duplicate keys, or multiple-print transactions were found in this extracted sample. Exact signed-order identity is still not equivalent to transaction-token grouping.
- Across 28.7 million cached Binance aggTrades, median receipt minus T is 132 ms, p90 270 ms. In the short multivenue window the median is 223 ms; on October 1 it is 122 ms. Recorder stalls produce long tails. These are properties of our observation routes, not the wallet's network latency. Of 304 valid latest-T selections at the tested cutoffs, **222 had not yet arrived at this recorder**.
- `m = print - 40 ms` is inferred, not observed matching time. Earlier first-crossing 50%/90% statistics use the subsequently completed price path and are not direct submission/reaction measurements. Subtracting the hold does not establish hosting geography. A constant “300 ms becomes 340 ms” conversion between price and trade clocks is not certified.
- `bsa_fit2.py` and `mv_feed.py` choose the positive event with maximum anchored fair-value edge inside a presumed decision window. Their edge AUC is therefore feature-dependent label construction, not an independently measured ceiling. The prior low F1 still demonstrates poor replication, but the AUC should not be promoted into proof of the hidden strategy.
- Official Polymarket history dates the crypto hold changes to **250→50 ms on August 17 at 11:00 UTC**, then **50→150 ms on September 4 at 14:00 UTC**. The old approximate September 10 date is superseded. Event, publication, receipt, decision, submit, matching, print and API clocks must stay separate.

## Maker: canonical Q99 replay still misses the important losses

| Resting .99 replay | Simulated / actual shares | Losing shares | Gross resting P&L | Drawdown |
|---|---:|---:|---:|---:|
| Actual | 1.000× | 4,322.02 | $6,580.54 | $1,507.07 |
| Original simplified | 0.854× | 1,473.95 | $7,840.81 | $820.42 |
| Canonical Q99 FIX | 0.854× | 1,473.95 | $7,839.98 | $820.42 |
| Snapshot-only diagnostic | 0.957× | 4,345.31 | $6,089.06 | $1,524.27 |

Canonical FIX covers all 846 orders with an exact identified footprint and no replay errors. Pure maker: 540 orders, 75.74% filled-order recall, 0.764× shares; **both pure-maker losing orders are entirely missed: 2,841.36 actual shares versus zero simulated**. Mixed sweep/rest: 306 orders, 98.37% recall, 1.026× shares; losing shares 1,473.95 simulated versus 1,480.66 actual. This is the useful separation between the two execution cases.

All 846 sampled orders actually filled. Thus “zero false fills” is structural and the headline agreement is recall, not specificity. Furthermore, 475 inferred-alive orders use contract end plus ten minutes as an artificial replay cap. This is historical fixed-order execution forensics, not independently generated strategy P&L; the quoted maker P&L is only the resting .99 component, excludes rebates, and uses the default zero maker fee.

Sequence-order correction changes no fills. Canonical subtraction of bids above .99 changes only $4.68 in the isolated comparison. Snapshot-only replay recovers the September 19/22 losing days, but **18,073 live-window snapshot comparisons show no downward reset**. Carried .99 above reported best bid appears on 21 orders, none of the five losing orders. Snapshot-generated deltas can limit that comparator. Phantom depth remains a hypothesis; snapshot-only cannot be chosen as the correct model just because it fits known losses. Head-first cancellation and unexplained-depth-as-fill variants remain rejected diagnostics. No Q haircut was fitted to outcomes.

The independent own-live check includes **631 accepted LB .99 orders, 628 replayed, three without cache**: 98.73% yes/no agreement, seven false fills, one miss, 1.077× shares. BTC5 alone: 185 orders, 15 actual fills, one false fill, 1.071× shares. Own BTC5 median size is 200 versus 3,000 for BoneOhio pure maker; this does not validate the larger queues. The archived afternoon Q99 table has unknown outcomes in all 1,064 overlapping .99 variant rows, and its summary converts missing winner to payoff zero. Its old P&L and loser labels must be suppressed until re-resolved; fill quantities reproduce exactly.

The full launcher and Q reconstruction guide is `maker/RUNBOOK.md`, now condensed into ATLAS. It distinguishes `lf` from `lf2`, documents footprint insertion, complement mapping, finite-sweep subtraction, seq ties, cancellation attribution, flags, feed selection and the fact that `Q99_LIVE_KQ_RULE` is a retry lifecycle rule rather than a Q multiplier. BoneOhio must not inherit Q99 reposting without evidence.

## Taker: a frozen candidate, with a material latency penalty

The frozen BI1/F2 rule is Binance perp aligned move at least 3 bp in one second, side ask .05–.95, ask increase at most two cents over a second, 10–300 seconds left, feed-gap guard at one second, one order per contract-side, 20-share FAK capped at ask plus one cent, held to outcome. This study keeps the 3,915 saved intentions and default fee `0.07 × p × (1-p) × shares` per consumed level.

| Total modeled latency | Net P&L | Drawdown | Fills | Sep21–30 net |
|---|---:|---:|---:|---:|
| 300 ms | $1,909.41 | $117.01 | 1,550 | $651.37 |
| 325 ms | $1,713.35 | $101.81 | 1,456 | $538.14 |
| 350 ms | $1,567.52 | $96.99 | 1,400 | $547.84 |
| 400 ms | $1,499.58 | $85.85 | 1,297 | $526.23 |
| 500 ms | $1,089.57 | $87.35 | 1,161 | $343.55 |

The strict full-depth replay rejects stale/unseeded depth and uses venue rows strictly before match, preserving token identity. It does not invent missing venue times or truncate to 12 levels. Coverage is 99.62–99.64%. F100 is already included in total latency: do not add hold twice. This is **conditional execution sensitivity on frozen E-clock intentions**, not a freshly rebuilt trade-time or receipt-time strategy.

Partial October 1 is **−$13.20 at 300 ms, 28 fills**, versus legacy −$0.90/29 fills. One stale-book winner explains that difference. Across all periods, 13 rejected stale fills change net by −$35.63, and every covered intention agrees with the legacy replay. A slower variant's better result on that small day is not a reason to select it. Equity credits known final outcomes at contract-end proxy; it is not live mark-to-market or payout cashflow. Source coverage is not certification of complete market capture. No fee sensitivity tests were run.

## Activity windows do not recover selection

The .985–.99 favourite-ask/≤180s condition retains September 21–30 precision 27.95%, recall 96.03%. The design-selected prior-15-minute-print gate changes these to **29.62% and 91.59%**: it removes 249 unmatched selections but discards 37 matches. No tested rule exceeds 37.37% precision on that reused period. Turnover, concurrency/exposure and calendar controls do not produce a stable recovered rule; observed target orders decline while eligible contracts remain approximately stable.

The target is an observed reconstructed order, not a complete submission ledger. Apparent non-entries can include unobserved zero-fill attempts. Prior print-time activity is also not demonstrated wallet-attribution availability; 1s/5s delays only stress a proxy. No maker P&L is selected on hindsight “active” windows. Full methodology and all 41 unique rules plus the design-picked duplicate are in `selection/RESULT.md` and the dashboard.

## What can be used next

1. Keep the two research implementations and canonical Q99 queue reference. Do not convert the current maker replay into an economic deployment claim; first explain large pure-maker losing fills on independent accepted-order samples, including zero fills, full lifecycle exits and correct outcomes.
2. For a causal taker rebuild, capture individual-symbol Binance bookTicker plus aggTrade with T/E/local monotonic receipt and raw IDs, alongside the actual Kraken/other candidate route and independent Polymarket token sockets/user-WS. Measure feed-to-decision-to-submit-to-fill directly. Existing fixed-intention stress results are a benchmark, not a receipt-clock certificate.
3. Historical Chainlink is untested. For new capture, preserve exact provider/topic and timestamps. Current Polymarket migration docs say PolyBolt `price.crypto` is Pyth even as replacement for the old Chainlink topic; `price.crypto.twap` is the 60-second Chainlink TWAP. Do not call the former a Chainlink spot test or substitute future capture for historical evidence.
4. Freeze any revised signal before new untouched days; retain default fees and measured latency distributions, include nonfills, stale-source exclusions, consecutive losing days, size/queue regimes and capital constraints. The current October 1 result does not pass a live-deployment gate.

## Verification and reproducibility

Completed Slurm jobs: baseline12060897; maker12060942; canonical/own-live12060949; taker12060943; selection12060945; feed/clock12061350 (supersedes12061346 paired-model diagnostic). All successful, on general allocations requesting128cores. The initial maker launcher12060937 failed before computation because CRLF normalization damaged a shell line; corrected launcher was verified before12060942. No failed result was silently retained as successful.

Maker: nine synthetic semantics checks, 6,768+3,384 unique variant rows, quantity bounds/outcome labels, zero replay errors, original baseline reproduction. Taker: all3,052contracts read, no read/missing/duplicate errors, all equity/fees/daily/weekly totals reconciled. Selection: 6,188grids, no read failures, 288 brute-force feature comparisons and future-mutation invariance. Feed/clock: fixed times, stable as-of ordering, symbol uniqueness, direct/complement join checks, matched complete-case incremental cohorts; script SHA and provenance in methodology.json. Source scripts and result tables accompany the report. Dashboard verification records are under `report/`.

Primary documentation checked2026-10-02:
- https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market
- https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public
- https://docs.polymarket.com/changelog/predictions
- https://docs.polymarket.com/migrate/rtds-to-polybolt
