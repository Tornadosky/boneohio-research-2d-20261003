# tailtaker_20261003_A2PARITY: does the A2 backtest reproduce every live A2 order (cohort_A + cohort_B), and what does more size earn?

Date (UTC): 2026-10-03 | Family: tailtaker | Status: complete
Verdict: **Parity achieved layer by layer once three backtest conventions are fixed** (strike = close of the boundary second,
BTC = Binance USD-M **bookTicker** on the box clock, A2 book age = ms since the last top-of-book change). With them the backtest
reproduces every live A2 intent on both live boxes; the remaining differences are millisecond-jitter edge cases. The presend gates
(BTC bookTicker age > 200 ms, Polymarket top-of-book age > 250 ms) are replayable and outcome-neutral. Size: per-share edge holds
~1.9 Â¢ to 30 sh, 1.76 Â¢ at 50, 1.64 Â¢ at 100, then saturates (1.33 Â¢ at 200).
Evidence grade: **A** for parity (live orders of both boxes, wallet-reconciled) Â· **B** for the size sweep (pendulumflow 08-21 â†’ 10-02,
A2 tercile model fitted through 09-18 â‡’ honest from 09-19).

## Question
User 10-03: verify A2 by checking live orders from both live boxes (cohort_A = cohort A, cohort_B = live box B) against the backtest; we need total
parity; if not, find and test what causes the disparity; if parity holds, what is it worth to increase shares?

## Data
- Live: A2 audit logs of both boxes (every 8 s trade tick logged with its block reason, every intent, every order + venue reply),
  pulled read-only 10-03 10:05Z. cohort_A 09-23 â†’ 10-03 (base $1.25, $5 from 09-30 09:15); cohort_B 10-01 20:27 â†’ 10-03 (share sizing).
  Wallet activity from the public data API: every live order matched by transaction hash (cohort_A 503/503, cohort_B 93/93; shares and price exact;
  fee = 0.07Â·pÂ·(1âˆ’p)Â·shares within $0.004 in total).
- Backtest: `cache_btc_pflow_v1` btc_5m, 12,590 contracts 08-18 â†’ **10-02 13:50** (pendulumflow publishes nothing later), venue clock,
  event-level depth replay (`exact_book.py`), pool rebuilt (`pool_build.py --family pflow --clock venue`).
- **New input:** source recorder recorder Binance USD-M bookTicker BTCUSDT (golden stage `source_recorder/*/data/bookticker`), 62.1 M updates
  10-01 12:00 â†’ 10-03 10:19 (`btdecode.py`). Its source recorder receive clock lags (p50 205 ms, p90 854 ms) â‡’ use Binance event time E.

## Results
### 0. Live vs live (same code, same markets, own feeds), 12,900 common trade ticks
Same decision class on 12,895; the 5 differences are size-cap effects (shares vs $ cap). Strike and BTC identical. 145 common intents:
same presend-gate outcome on 139, same fill price on all 90 double fills â‡’ the gates are driven by the venue streams, not the box.

### 1. Strike â€” root cause #1 (convention)
Research/backtest strike = last Binance price **before** the boundary; live strike = last mid **received in the boundary second**
(source arrives p50 985 ms into the second). Backtest boundary strike = live on 63â€“65 % of markets (p90 |Î”| $9.7). Live convention on the
cache feed: 94â€“95 %; on the bookTicker feed (E + 120 ms): **98.4 % (cohort_B) / 98.7 % (cohort_A)**; every remaining miss is a live update that
arrived in the last 1â€“8 ms of the second (network jitter).

### 2. BTC feed â€” root cause #2 (stream + clock)
Cache = Binance depth-diff @100 ms mids; live = bookTicker. Best cache clock (venue + 75 ms) reproduces live 1 Hz closes on 97 %;
bookTicker on venue + 120 ms (= the box's measured event age) 98.4â€“98.7 %. Sigma(60 s) identical on 73â€“79 % of live intents with the cache
feed, 90â€“94 % with bookTicker.

### 3. Decision parity (live rule order replayed; forced state = live intent counter)
| inputs | cohort_B intents both / live-only / BT-only | cohort_A |
|---|---|---|
| backtest as built (boundary strike, cache mids) | 39 / 3 / 2 | 700 / 50 / 64 |
| live strike convention on the cache feed | **42 / 0 / 0** | 715 / 35 / 29 |
| box strike, cache BTC/sigma | 42 / 0 / 0 | 728 / 22 / 23 |
| live z (rule-code check) | 42 / 0 / 0 | 750 / 0 / 6 |
| **bookTicker E+120 ms + live strike convention (10-01 13:00 â†’ 10-03 10:15)** | **147 / 0 / 0** | **157 / 0 / 0** |
With bookTicker the class agrees on 99.97 % of compared ticks (1 tail-bid/pre-filter edge case per box), including 105 (cohort_B) / 107 (cohort_A)
intents **after** the pendulumflow cache end. Polymarket decision book = venue book at decide âˆ’ 10 ms (tail bid 97â€“98 %, favourite ask 100 %).

### 4. Presend gates â€” root cause #3 (â‰ˆ 34 % of live intents never sent)
- BTC gate (binance-md: event age = now âˆ’ E; open â‰¤ 190, max 200, hard-stale 250 ms; depth5 fallback not actionable): replay "newest
  bookTicker E visible at the box (E + 120 ms â‰¤ t) older than 200 ms â‡’ blocked" predicts **282 / 289** live outcomes (97.6 %); misses are
  Â±1 ms edge cases (3 `BTC_OPEN` = < 1 ms budget left). â‡’ a third of intents are skipped because Binance's top of book was quiet â‰¥ 80 ms.
- Polymarket gate (`poly_freshness` book age > 250 ms): the box PBBO republishes on top-of-book price **or size** changes of either token;
  "no such change for > 250 ms at decide âˆ’ 10 ms" predicts **31 / 31** POLY_BOOK_AGE blocks, 99.3 % of non-BTC-blocked intents.
- Economic effect (every live intent replayed at 50 sh): blocked âˆ’0.51 Â¢/sh, sent +1.36 Â¢/sh, permutation p = 0.35 â‡’ **outcome-neutral**;
  the gates cut volume ~34 % without selecting winners or losers.

### 5. A2 multiplier
As built 88 % (cohort_B) / 90 % (cohort_A) tercile agreement. Residual EV gap ~5e-4 traced to **book age**: backtest 0 ms (100 ms venue grid),
live = ms since the box's last PBBO publish (p50 13 ms); LR weight 0.009/sd on log1p(age) â‡’ â‰ˆ 5e-4 EV. With the live strike convention +
derived book age: **cohort_B 42/42** (EV |Î”| p50 2.8e-5 on the bookTicker ring), cohort_A 97.3 % (cache ring). Thickness (first qualification) 98â€“100 %.

### 6. Execution and PnL on identical orders (pendulumflow window)
Decision â†’ match p50 203 (cohort_B) / 205 (cohort_A) ms (150 ms hold + ~54 ms). FAK vs the favourite-ask ladder at decide + 200 ms:
| box | orders | fill/no-fill agree | shares exact | VWAP exact | PnL live vs BT |
|---|---|---|---|---|---|
| cohort_B | 29 | 100 % | 100 % | 100 % | identical to the cent |
| cohort_A | 454 | 99.3 % | 96 % | 99.3 % | $10.43 vs $10.58 |
cohort_A residual: 3 zero-fills where another taker emptied a thin level first; venue spends exactly 1 Â¢ less than the principal on 4 % of fills
(SDK rounds taker shares to 4 dp; â‰ˆ $0.0003/order).

### 7. Free-running end-to-end (bookTicker BTC, live strike, derived book age, both gates, live sizing, FAK @200 ms)
10-01 13:05 (cohort_A) / cohort_B warm-up â†’ 10-02 13:50: **same order list** â€” cohort_B 42/42, cohort_A 49/49 intents, 0 extra either side; principal equal
97.6 % / 98 %, gate outcome 97.6 % / 98 %, fills 100 % / 97 %. Each residual is one ms-level edge case (gate age 186 vs 200 ms; an A2 EV
3e-4 from a tercile cut; one thin-book fill).

### 8. Size sweep on the corrected engine (gate = 34 % random thinning, median of 20 seeds; own fills consumed from the ladder)
Sizing = live share rule: base Ã— limit Ã— premium Ã— thick Ã— A2, $1 floor, cap 4 Ã— base, 4 intents, FAK @200 ms, fee charged.
| base sh | $/day 08-21â†’10-02 | max DD | PnLÃ·DD | Â¢/sh | worst market | worst day | $/day OOS 09-19â†’ | OOS DD |
|---|---|---|---|---|---|---|---|---|
| 5 | 3.8 | 23 | 6.6 | 1.91 | âˆ’18 | âˆ’11 | 3.2 | 23 |
| 10 | 7.1 | 48 | 6.2 | 1.87 | âˆ’38 | âˆ’23 | 5.7 | 48 |
| 20 | 14.2 | 89 | 6.9 | 1.91 | âˆ’72 | âˆ’47 | 11.9 | 89 |
| 30 | 19.8 | 128 | 6.8 | 1.85 | âˆ’109 | âˆ’67 | 17.8 | 128 |
| 50 | 29.7 | 220 | 5.8 | 1.76 | âˆ’181 | âˆ’120 | 25.4 | 193 |
| 75 | 39.0 | 312 | 5.4 | 1.63 | âˆ’242 | âˆ’178 | 34.1 | 296 |
| 100 | 50.4 | 380 | 5.6 | 1.64 | âˆ’253 | âˆ’224 | 41.4 | 380 |
| 150 | 59.1 | 458 | 5.8 | 1.46 | âˆ’362 | âˆ’313 | 56.9 | 456 |
| 200 | 65.2 | 560 | 5.1 | 1.33 | âˆ’482 | âˆ’421 | 73.6 | 556 |
No-gate equivalents are ~1.3Ã— the $/day (50 sh: $36/day, DD $276). Marginal value of one more base share: ~$0.66/day up to 30 sh,
~$0.5/day to 100 sh, $0.17/day 100â†’150, $0.12/day 150â†’200 while DD keeps growing ~$4/base share. Weekly (50 sh, gate median):
all 7 weeks positive (+$35 â€¦ +$327); at 100 sh one negative week (08-24, âˆ’$27). Cap 4Ã— base is right: 50/100 halves PnLÃ·DD (3.6).
Live realised edge agrees: cohort_A 1.55 Â¢/sh (1,082 sh), cohort_B 1.86 Â¢/sh (322 sh) vs backtest 1.2â€“1.9 Â¢/sh at small size.
**Value of more size:** worth it up to ~100 sh base (â‰ˆ linear $/day, PnLÃ·DD flat 5.5â€“7); 150â€“200 sh adds little and doubles the
worst market. The honest window (09-19 â†’, 14 days) holds the whole-period max DD â‡’ PnLÃ·DD 1.5â€“1.9 there; size in steps.

## Addendum 2026-10-03 (same day): which convention earns more? (user follow-up)
Question: the parity gaps came from live and backtest using different inputs; which input setting gives the better equity curve, so live
can be switched if the backtest's convention is better? One setting changed at a time from the live config; 50 sh base, cap 200,
08-21 â†’ 10-02 13:50 (43 days); paired day-block bootstrap vs the live config; A2 model fitted through 09-18 â‡’ 09-19â†’ is out-of-sample.
Official start price = Gamma `eventMetadata.priceToBeat` (Chainlink BTC/USD TWAP-60 stream value at the boundary; 13,646 markets).

| change vs live config | $/day | max DD | Â¢/sh | Î” total [95 %] | Î” from 09-19 [95 %] |
|---|---|---|---|---|---|
| live (strike after, live book age, both gates) | 33.1 | 244 | 1.90 | â€” | â€” |
| **strike = last price before the boundary** | **41.4** | **181** | **2.27** | **+$351 [+81, +732]** | **+$165 [+13, +432]** |
| strike = 60 s average before (TWAP-60) | 31.2 | 299 | 1.67 | âˆ’$83 [âˆ’928, +787] | +$7 |
| strike = official priceToBeat + rolling basis | 28.6 | 555 | 1.35 | âˆ’$193 [âˆ’1,257, +836] | +$1 |
| A2 book age = grid (0 ms) | 33.8 | 282 | 1.89 | +$26 [âˆ’64, +83] | âˆ’$19 |
| no Polymarket gate | 31.1 | 244 | 1.66 | âˆ’$85 [âˆ’403, +145] | âˆ’$144 |
| no BTC gate (depth proxy, 84 % vs live gate) | 38.2 | 242 | 1.72 | +$213 [âˆ’85, +486] | +$31 |
| strike before + no BTC gate | 47.3 | 189 | 2.06 | +$603 [+192, +1,050] | +$194 [âˆ’101, +530] |
BTC feed (only 25 h testable, 10-01 13:05 â†’ 10-02 13:50): bookTicker vs depth stream differ by < $1.50 at 50 sh â‡’ not rankable yet.
Strike accuracy vs the official price (median |error| after a causal basis, from 09-19): before $12.8, after $13.0, TWAP-60 $3.4,
official+basis $3.5 â‡’ the closest strike does NOT earn more: the z â‰¥ 3 envelope and the A2 model were built on the spot-before price.
**Verdict (superseded by Addendum 2):** switch live to the strike before the boundary (one-line change: last mid received before the slot start); keep bookTicker,
the live book age and the Polymarket gate; BTC gate undecided (re-test with exact bookTicker in a few weeks). Dashboard
`config_lab.html` (https://claude.ai/artifact/JXvMyDfBvzq5e3LV2JpEXV); code `factors.py`, `factors_bt.py`, `factors2.py`, `simfactors.py`,
`simstrikes.py`, `dashdata.py`, `proxytest.py`; outputs `analysis/FACTORS_50.json`, `STRIKES_50.json`, `CONFIG_LAB_DATA.json`, `PROXYTEST.json`,
`inputs/price_to_beat_btc5m.parquet`.

## Addendum 2 (2026-10-03, supersedes the strike verdict of Addendum 1): bookTicker-equivalent feed, full history
Data: cryptohftdata Binance USD-M BTCUSDT order book (hourly parquet; full snapshot each hour + every diff, ~26 ms, unbroken update-id
chain, receive â‰ˆ E + 123 ms), 08-17 â†’ 10-03 09:59, 1,138 files / 23 GB (`chd/orderbook`; 08-17 00 â†’ 08-19 09 are zstd-wrapped, unused).
Their "ticker" is the 24 h rolling ticker, not bookTicker. Top of book rebuilt per message (`chd_top.py`, numba): 78.6 M top changes.
Validation (`chdval.py`): 1 Hz closes on E + 110 ms = live strikes 98.0 % (cohort_B) / 98.2 % (cohort_A), = source recorder bookTicker closes 99.3 %; BTC gate
replay (age > 180 ms on E + 100) = live 92 % (exact bookTicker 97.6 %, depth proxy 84 %).
Results (`factors_chd2.py`, `simchd2.py`; 50 sh, Poly gate on, paired vs live config on this feed):
| change vs live (after strike, BTC gate, z â‰¥ 3) | $/day | max DD | Â¢/sh | Î” 43 d [95 %] | Î” from 09-19 |
|---|---|---|---|---|---|
| live config | 28.0 | 226 | 1.70 | â€” | â€” |
| strike = event time (last top change with Binance E â‰¤ boundary) | 30.7 | 226 | 1.80 | +$114 [+44, +200] | +$7 [âˆ’22, +38] |
| strike = box before (last mid received before the boundary) | 30.1 | 226 | 1.76 | +$90 [âˆ’287, +468] | +$126 [âˆ’18, +388] |
| depth-stream feed (cache) | 30.9 | 197 | 1.87 | +$125 [âˆ’24, +346] | +$35 |
| no BTC gate | 32.7 | 230 | 1.52 | +$199 [âˆ’260, +591] | +$36 (post-09-22 âˆ’$24) |
| z â‰¥ 2.75 / 3.25 / 3.5 | 23.3 / 25.6 / 23.1 | 384 / 181 / 180 | 1.21 / 1.94 / 2.14 | âˆ’$198 / âˆ’$99 / âˆ’$206 | â€” |
The cache's research strike equals this feed's price ~150 ms before the boundary on 95.6 % of markets (`strikecmp.py`), i.e. the same rule as
"box before"; the earlier +$351 came from the cache's BTC price series and is not reproduced on live-equivalent prices (box-before minus
after flips sign across z: +21 / +159 / +90 / âˆ’21 / âˆ’68 at z 2.5 â€¦ 3.5). z â‰¥ 3 is not a knife edge (3.0â€“3.5 flat, PnLÃ·DD 5.3â€“6.0) and
works with either strike; below 3 DD doubles. **Verdict:** no live change is supported. Optional low-value change: event-time strike
(+$114/43 d, all before 09-19). The tested `--strike-mode before` patch (unit + replay tests: 528/528 strikes = backtest) is NOT recommended.
Dashboard v2 https://claude.ai/artifact/JXvMyDfBvzq5e3LV2JpEXV.

## Reproduce and locate
- Code: source cluster `<SOURCE_HOME>/tailtaker_20261003_A2PARITY/` â€” `build.sbatch` (pool_build â†’ exact_book â†’ a2_feats; job 12071308), `tickpar.py`,
  `decpar.py`, `clockpar.py`, `btdecode.py` + `bt.sbatch` (job 12071462), `btclock.py`, `btdec.py`, `a2par.py`, `gatepar.py`, `polygate.py`,
  `execpar.py`, `e2e.py`, `gatepnl.py`, `sizing.py` + `sizing.sbatch` (job 12071555), `simsize.py`, `weekly.py`. Python waterboa 2025.06.
- Live code/model verified by sha256 on both boxes: `a2_model.py` 228ecfaaâ€¦, `a2_live_model.json` 94096de2â€¦; cohort_A strategy 7d9d705câ€¦
  (cohort_B = same + share sizing).
- Outputs: `<SOURCE_DATA>/exp/tailtaker_20261003_A2PARITY/{inputs,pools,exact,analysis}` â€” TICKPAR_INPUTS, DECPAR, CLOCKPAR, BTCLOCK,
  BTDEC, A2PAR, GATEPAR, POLYGATE, EXECPAR, E2E, GATEPNL, SIZING .json; per-tick parquet.
- PC: `<SOURCE_NOTES>\experiments\tailtaker\tailtaker_20261003_A2PARITY\`.

## Limitations and next action
- bookTicker exists only from 10-01 12:00 (source recorder) â‡’ the 08-21 â†’ 10-02 size sweep uses the cache feed (decision parity there ~91 % cohort_A
  Jaccard, symmetric) and a statistical gate. Polymarket books after 10-02 13:50 are not in any cache â‡’ cohort_B/cohort_A checks after that are
  decision-level only.
- A2 tercile model fitted through 09-18: only 14 OOS days; live samples small (cohort_A 7 / cohort_B 1 losing contracts in the window).
- Venue 1 Â¢ rounding and competition for thin levels are not modelled (â‰¤ 0.7 % of orders).
- Next: make the research engine use the three live conventions (strike, bookTicker clock, book age) and record bookTicker continuously
  (source recorder already does) so future parity checks need no reconstruction. Re-run the sweep after â‰¥ 200 more live markets.

## Documentation completion
Ledger row appended (source cluster `<SOURCE_HOME>/EXPERIMENTS.md`, PC mirror). Family page `research/families/tailtaker.md` updated. Index regenerated.
