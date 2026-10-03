# REPLAY — decision-replay of every captured live q99 order: is the FILL MODEL right when the decisions are held equal? (10-03)

Part of HANDOFF_PARITY_20261003 P3 "If backtester (1)". Grade **A** (real venue-accepted live orders, exact venue times, the same
pendulumflow book the parity runs use). Codenames: account_a main wallet, account_b test wallet, account_c the third anonymous account.

## Method
Every captured, venue-accepted live order (`inputs_<acct>/live_orders.parquet`: `captured & place_venue_ms & phase != rejected`) is posted
again at its exact venue placement time, price, size and token and taken out at its exact cancel-effective venue time (market end if it
rested), on `cache_<coin>_pflow_v1` (depth `normalized_depth_events`, prints `trade_tape`, venue clock). Only fills are simulated. Live fills =
ledger `matched`; live first fill = user-WS `server_ms` (decoded fills), else ledger `match_time`. PnL = payoff at resolution
(`resfill/poly_oracle_resolutions_*`), 0 fee. Code `code/replay/replay.py` (job `replay_sb.sh`), `replay_sum.py`, `perwin.py`, `drill.py`;
outputs `reference-storage/q99_20261002_DEPLOYGAP/replay/` (`orders_{account_a,account_b,account_c}.parquet`, `orders_all.parquet`, `orders_scored.parquet`,
`summary_v4.md`, `per_window.csv`, `per_profile.csv`).
Modes: **eng** = exact port of the production `pkg_v9fp` `_fp_simulate` (Q99_FILLPAR=1, complement BUY prints, trade-through empties L,
sizematch cancels, prints 47 ms early = 60 − 13 ms feed, own-order removal from `own_steps2`/`own_steps` on the venue clock, CUT_SHIFTED);
virtual order joins the back of the level at the live placement time. Proposed-fix variants: **eng_thr** (book-aware prints + *bounded*
trade-through: a print below L consumes only size − displayed bids better than L), **eng_ownft** (each own placement step snapped to its
depth footprint), **eng_ftfix** (both), **eng_own5** (blanket 5 ms lead; rejected), **eng_dm / fp3_dm** (depth-matched prints, see below;
rejected as implemented). **fp3** = FILLPARITY fillpar3 best variant (the real order enters at its own footprint row, book-aware prints,
unbounded trade-through, 60 ms). Diagnostics: eng_book, eng_sh60, eng_noown (no own removal), fp3_df1 (depth-fill).

Scope: 4,111 resolved orders with book data (account_a 4,087: btc_5m 1,398 / eth_5m 1,166 / sol_5m 663 / btc_15m 365 / eth_15m 283 / sol_15m 212;
account_c 24). Out of scope (placed after the book data end, 10-02 13:50 UTC contracts): account_a 2,516, account_b all 196, account_c 14 — rerun when PFEXT extends the
cache (same job).

## Verdict
**The production fill model is right given the live decisions.** On 4,111 real orders: fill yes/no 98.0 %, exact share agreement (±1 %)
95.7 %, engine ÷ live shares 1.03, PnL −$1,255 vs live −$1,512. Every account_a profile is within ±5 % of live shares (btc_5m 1.04, eth_5m 0.99,
sol_5m 0.97, btc_15m 1.02, eth_15m 0.97, sol_15m 1.00); PnL within $16 per profile except **btc_5m (+$232 optimistic)**. That gap is
almost entirely the 10-01 13:26–18:30 keep-queue KQ_90_97 window (`PAR_account_a_btc_5m_10`: −$744 replay vs −$960 live on the same orders);
every other parity window is within ±$16 (three between $10 and $16: btc_5m_04, eth_5m_06, sol_5m_07).
⇒ The large parity gaps of RESULT §9 (only-engine markets, keep-queue, BTC15) are **decision** differences, not fill-model errors:
- 15m losers: 21 BTC15 + 4 ETH15 captured loser orders, live 0 fills, replay 0 fills; the BTC15 engine losses are orders live never placed.
- Winners vs losers: losers 0.96×, winners 1.03× shares (mild optimism, ≈ 0.4 ¢/sh). 0.99 arm 1.06× (PnL −$276 vs −$298); orders resting to
  market end 1.00×; cancelled orders 1.09× (the over-fills are concentrated on short-lived cancelled orders, see mechanisms).
account_c: only 24 captured orders in scope (2 BTC5 fills: 116 vs 150 sh) — no account_c fill verdict possible beyond "consistent".

## Mechanisms behind the residual (the largest |Δ PnL| orders, `drill.py`)
1. **Own order as a phantom ahead (BT bug, M1 residue).** Own placement steps sit at the user-WS stamp − 1 ms, but the order's size appears in
   the depth 2–4 ms earlier (footprint). For those ms our own order is "another order ahead"; when the depth row that adds it is merged with
   other changes, sizematch cannot remove it and newest-first cancels remove orders *behind* us instead. 11.5 % of orders join with the queue
   ahead inflated by exactly their own size (e.g. BTC5 UP 0.94 601 sh: queue ahead 950 vs footprint 349; live 71, engine 12). The same
   1–4 ms window exists in the production runs whenever a BT twin opens around the live placement (twin open Δ −9…+4 ms).
   **Fix (data, no engine change): snap every own +step to its depth footprint** (eng_ownft): missed fills 50 → 39, fill y/n 98.3 %.
   A blanket 5 ms lead (eng_own5) is worse (it subtracts our size from rows before the footprint and reorders the queue).
2. **Unbounded trade-through (BT bug).** A print below L "empties level L" even when it is a 30–60-share sweep, and with the 47 ms shift it can
   fall after our cancel (BTC5 UP 0.97 748 sh, life 66 ms: live 25, engine 748 — prints 0.97 ×30 ×30 then 0.96 at +107 ms). **Fix:** a print
   below L consumes only size − (bids better than L) at L (eng_thr; engine change: `_fp_simulate` needs the better-level depth at print time).
   Shares 1.03 → 1.01.
3. **Print lag > shift on large complement/sweep prints (open).** BTC5 DOWN 0.92 1,169 sh: level L dropped 984.44 at +380 ms, the 1,288-share
   complement print is stamped +464 ms (84 ms later, > the 47/60 ms shift), so the book-aware subtraction reads the better levels *after* they
   were consumed and the whole print hits L; live got exactly 984.44 − 562.5 ahead = 421.94, engine 1,169 / book-aware 1,006. On KQ deep arms
   (0.90–0.94, 400–1,200 sh) this over-fills winners and under-fills losers ⇒ the +$216 KQ-window optimism. A depth-matched rule (each print
   claims the actual L-level decreases in [print − 200, print + 50] ms and consumes FIFO at the depth time) reproduces that order exactly but,
   as implemented, under-fills overall (0.88×; it misses matches whose depth change is outside the window or merged with cancels) — not
   recommended until the matching is refined.
4. **Fills with no print in the tape (data).** e.g. BTC5 UP 0.95 361 sh live 303 at +1,187 ms, no print at L in the pendulumflow tape (−$15).
   Depth-fill (fp3_df1) catches these but doubles shares overall (1.99×) — keep off.
5. **Race at open** (print within ~70 ms of placement) — a handful of false fills, small $.
With fixes 1+2 (**eng_ftfix**): fill y/n 98.3 %, exact 96.2 %, 1.02× shares, PnL −$1,352 vs −$1,512 (gap $257 → $160; KQ window −$795 vs −$960;
sum over windows |Δ| $316 → $230). The footprint-entry model fp3 gets the KQ window to −$950 (sum |Δ| $130) but one 54-share BTC15 loser
false fill (−$53); its entry rule is not available to a BT order, so eng_ftfix is the portable recommendation.

## Recommendation to the main agent (do not patch the production package from here)
- **Adopt fix 1 now** (own_steps3: footprint-snapped own placement steps, built from the cache by the same search as `replay.py` `own_ft`);
  it is a data change that the parity runs read through `Q99_FILLPAR_OWN_STEPS`.
- **Fix 2** is an engine change (`Q99_FILLPAR_THROUGH=bounded`): small but consistent improvement (−1–2 % shares on cancelled orders).
- Treat the btc_5m KQ-window replay gap (+$216 on $16k-share window, ≈1.3 ¢/sh) as the remaining fill-model uncertainty for deep keep-queue
  arms; it is in the optimistic direction.
- Rerun `replay_sb.sh` + `replay_sum.py` after the next PFEXT round (2.7k later orders, incl. the test wallet's first 196).

## Tables (generated: `replay_sum.py`, modes eng and eng_ftfix; all modes in `replay/summary_v4.md`)

Orders in scope (book data present, resolved): 4111; out of scope (after the book data end): {'account_c': 14, 'account_a': 2516, 'account_b': 196}

### Mode comparison, all accounts

| group | orders | filled live / sim | fill y/n agree | exact (±1 %) | sim÷live sh | shares live / sim | PnL live / sim | missed / false |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| eng | 4111 | 829 / 810 | 98.0% | 95.7% | 1.03 | 68,447 / 70,416 | $-1,512 / $-1,255 | 50 / 31 |
| eng_book | 4111 | 829 / 808 | 98.0% | 95.7% | 1.02 | 68,447 / 70,034 | $-1,512 / $-1,271 | 51 / 30 |
| eng_sh60 | 4111 | 829 / 811 | 98.0% | 95.5% | 1.04 | 68,447 / 71,157 | $-1,512 / $-1,379 | 51 / 33 |
| eng_thr | 4111 | 829 / 807 | 98.1% | 95.8% | 1.01 | 68,447 / 69,159 | $-1,512 / $-1,307 | 51 / 29 |
| eng_own5 | 4111 | 829 / 808 | 98.1% | 95.6% | 1.00 | 68,447 / 68,393 | $-1,512 / $-1,366 | 50 / 29 |
| eng_fix | 4111 | 829 / 805 | 98.1% | 95.6% | 0.98 | 68,447 / 67,182 | $-1,512 / $-1,416 | 51 / 27 |
| eng_ownft | 4111 | 829 / 821 | 98.3% | 96.1% | 1.03 | 68,447 / 70,803 | $-1,512 / $-1,302 | 39 / 31 |
| eng_ftfix | 4111 | 829 / 818 | 98.3% | 96.2% | 1.02 | 68,447 / 69,571 | $-1,512 / $-1,352 | 40 / 29 |
| eng_dm | 4111 | 829 / 779 | 96.0% | 91.2% | 0.88 | 68,447 / 60,308 | $-1,512 / $-1,413 | 107 / 57 |
| fp3_dm | 4111 | 829 / 844 | 96.9% | 93.7% | 0.98 | 68,447 / 67,170 | $-1,512 / $-1,486 | 56 / 71 |
| fp3 | 4111 | 829 / 826 | 98.3% | 96.1% | 1.04 | 68,447 / 71,242 | $-1,512 / $-1,569 | 37 / 34 |
| fp3_thr | 4111 | 829 / 824 | 98.3% | 96.1% | 1.01 | 68,447 / 69,009 | $-1,512 / $-1,495 | 37 / 32 |
| fp3_df1 | 4111 | 829 / 1380 | 85.7% | 82.7% | 1.99 | 68,447 / 136,541 | $-1,512 / $-3,930 | 18 / 569 |
| eng_noown | 4111 | 829 / 539 | 92.3% | 90.9% | 0.67 | 68,447 / 45,694 | $-1,512 / $-1,281 | 303 / 13 |

### Per account × profile — mode `eng`

| group | orders | filled live / sim | fill y/n agree | exact (±1 %) | sim÷live sh | shares live / sim | PnL live / sim | missed / false |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| account_c btc_5m | 15 | 2 / 2 | 100.0% | 93.3% | 0.77 | 150 / 116 | $3 / $2 | 0 / 0 |
| account_c eth_5m | 9 | 1 / 1 | 100.0% | 100.0% | 1.00 | 10 / 10 | $1 / $1 | 0 / 0 |
| account_a btc_15m | 365 | 90 / 90 | 100.0% | 99.5% | 1.02 | 1,856 / 1,895 | $49 / $50 | 0 / 0 |
| account_a btc_5m | 1398 | 309 / 307 | 97.3% | 92.2% | 1.04 | 54,093 / 56,324 | $-1,186 / $-954 | 20 / 18 |
| account_a eth_15m | 283 | 49 / 47 | 97.9% | 97.5% | 0.97 | 2,314 / 2,254 | $48 / $47 | 4 / 2 |
| account_a eth_5m | 1166 | 252 / 243 | 98.2% | 96.9% | 0.99 | 5,615 / 5,533 | $-211 / $-199 | 15 / 6 |
| account_a sol_15m | 212 | 41 / 40 | 98.6% | 98.6% | 1.00 | 756 / 755 | $-51 / $-51 | 2 / 1 |
| account_a sol_5m | 663 | 85 / 80 | 98.0% | 97.1% | 0.97 | 3,653 / 3,529 | $-165 / $-150 | 9 / 4 |

### Splits — mode `eng` (all accounts)

| group | orders | filled live / sim | fill y/n agree | exact (±1 %) | sim÷live sh | shares live / sim | PnL live / sim | missed / false |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| losers | 186 | 47 / 47 | 97.8% | 94.1% | 0.96 | 4,522 / 4,358 | $-4,224 / $-4,072 | 2 / 2 |
| winners | 3925 | 782 / 763 | 98.0% | 95.8% | 1.03 | 63,925 / 66,058 | $2,712 / $2,817 | 48 / 29 |
| 0.99 | 665 | 89 / 94 | 98.6% | 97.6% | 1.06 | 4,739 / 5,025 | $-298 / $-276 | 2 / 7 |
| <0.99 | 3446 | 740 / 716 | 97.9% | 95.3% | 1.03 | 63,709 / 65,391 | $-1,214 / $-979 | 48 / 24 |
| cancelled | 3557 | 290 / 271 | 97.9% | 95.5% | 1.09 | 21,739 / 23,673 | $-220 / $22 | 46 / 27 |
| resting to market end | 554 | 539 / 539 | 98.6% | 97.1% | 1.00 | 46,708 / 46,743 | $-1,292 / $-1,277 | 4 / 4 |
| 15m | 860 | 180 / 177 | 99.0% | 98.6% | 1.00 | 4,926 / 4,904 | $47 / $46 | 6 / 3 |
| 5m | 3251 | 649 / 633 | 97.8% | 94.9% | 1.03 | 63,521 / 65,512 | $-1,559 / $-1,300 | 44 / 28 |

### Profile × winners/losers — mode `eng`

| group | orders | filled live / sim | fill y/n agree | exact (±1 %) | sim÷live sh | shares live / sim | PnL live / sim | missed / false |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| btc_15m losers | 21 | 0 / 0 | 100.0% | 100.0% | nan | 0 / 0 | $0 / $0 | 0 / 0 |
| btc_15m winners | 344 | 90 / 90 | 100.0% | 99.4% | 1.02 | 1,856 / 1,895 | $49 / $50 | 0 / 0 |
| btc_5m losers | 52 | 15 / 15 | 96.2% | 86.5% | 0.96 | 3,596 / 3,466 | $-3,350 / $-3,230 | 1 / 1 |
| btc_5m winners | 1361 | 296 / 294 | 97.4% | 92.4% | 1.05 | 50,647 / 52,974 | $2,167 / $2,278 | 19 / 17 |
| eth_15m losers | 4 | 0 / 0 | 100.0% | 100.0% | nan | 0 / 0 | $0 / $0 | 0 / 0 |
| eth_15m winners | 279 | 49 / 47 | 97.8% | 97.5% | 0.97 | 2,314 / 2,254 | $48 / $47 | 4 / 2 |
| eth_5m losers | 90 | 24 / 24 | 97.8% | 96.7% | 0.98 | 616 / 601 | $-570 / $-557 | 1 / 1 |
| eth_5m winners | 1085 | 229 / 220 | 98.2% | 97.0% | 0.99 | 5,009 / 4,943 | $360 / $359 | 14 / 5 |
| sol_15m losers | 4 | 3 / 3 | 100.0% | 100.0% | 1.00 | 70 / 70 | $-68 / $-68 | 0 / 0 |
| sol_15m winners | 208 | 38 / 37 | 98.6% | 98.6% | 1.00 | 686 / 685 | $17 / $17 | 2 / 1 |
| sol_5m losers | 15 | 5 / 5 | 100.0% | 93.3% | 0.92 | 241 / 222 | $-236 / $-217 | 0 / 0 |
| sol_5m winners | 648 | 80 / 75 | 98.0% | 97.2% | 0.97 | 3,413 / 3,307 | $70 / $67 | 9 / 4 |

### Per account × profile — mode `eng_ftfix`

| group | orders | filled live / sim | fill y/n agree | exact (±1 %) | sim÷live sh | shares live / sim | PnL live / sim | missed / false |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| account_c btc_5m | 15 | 2 / 2 | 100.0% | 93.3% | 0.77 | 150 / 116 | $3 / $2 | 0 / 0 |
| account_c eth_5m | 9 | 1 / 1 | 100.0% | 100.0% | 1.00 | 10 / 10 | $1 / $1 | 0 / 0 |
| account_a btc_15m | 365 | 90 / 90 | 100.0% | 99.5% | 1.02 | 1,856 / 1,895 | $49 / $50 | 0 / 0 |
| account_a btc_5m | 1398 | 309 / 308 | 97.4% | 92.2% | 1.02 | 54,093 / 55,347 | $-1,186 / $-1,005 | 19 / 18 |
| account_a eth_15m | 283 | 49 / 47 | 97.9% | 97.5% | 0.97 | 2,314 / 2,238 | $48 / $46 | 4 / 2 |
| account_a eth_5m | 1166 | 252 / 243 | 98.5% | 97.5% | 0.98 | 5,615 / 5,494 | $-211 / $-230 | 13 / 4 |
| account_a sol_15m | 212 | 41 / 41 | 99.1% | 99.1% | 1.00 | 756 / 756 | $-51 / $-51 | 1 / 1 |
| account_a sol_5m | 663 | 85 / 86 | 98.9% | 98.9% | 1.02 | 3,653 / 3,714 | $-165 / $-166 | 3 / 4 |

### Splits — mode `eng_ftfix` (all accounts)

| group | orders | filled live / sim | fill y/n agree | exact (±1 %) | sim÷live sh | shares live / sim | PnL live / sim | missed / false |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| losers | 186 | 47 / 47 | 97.8% | 95.7% | 0.98 | 4,522 / 4,411 | $-4,224 / $-4,122 | 2 / 2 |
| winners | 3925 | 782 / 771 | 98.3% | 96.2% | 1.02 | 63,925 / 65,160 | $2,712 / $2,770 | 38 / 27 |
| 0.99 | 665 | 89 / 95 | 98.8% | 98.3% | 1.08 | 4,739 / 5,129 | $-298 / $-294 | 1 / 7 |
| <0.99 | 3446 | 740 / 723 | 98.2% | 95.8% | 1.01 | 63,709 / 64,443 | $-1,214 / $-1,059 | 39 / 22 |
| cancelled | 3557 | 290 / 279 | 98.3% | 96.0% | 1.04 | 21,739 / 22,708 | $-220 / $-78 | 36 / 25 |
| resting to market end | 554 | 539 / 539 | 98.6% | 97.7% | 1.00 | 46,708 / 46,863 | $-1,292 / $-1,274 | 4 / 4 |
| 15m | 860 | 180 / 178 | 99.1% | 98.7% | 0.99 | 4,926 / 4,890 | $47 / $45 | 5 / 3 |
| 5m | 3251 | 649 / 640 | 98.1% | 95.5% | 1.02 | 63,521 / 64,681 | $-1,559 / $-1,398 | 35 / 26 |

### Profile × winners/losers — mode `eng_ftfix`

| group | orders | filled live / sim | fill y/n agree | exact (±1 %) | sim÷live sh | shares live / sim | PnL live / sim | missed / false |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| btc_15m losers | 21 | 0 / 0 | 100.0% | 100.0% | nan | 0 / 0 | $0 / $0 | 0 / 0 |
| btc_15m winners | 344 | 90 / 90 | 100.0% | 99.4% | 1.02 | 1,856 / 1,895 | $49 / $50 | 0 / 0 |
| btc_5m losers | 52 | 15 / 15 | 96.2% | 88.5% | 0.97 | 3,596 / 3,475 | $-3,350 / $-3,239 | 1 / 1 |
| btc_5m winners | 1361 | 296 / 295 | 97.4% | 92.4% | 1.03 | 50,647 / 51,988 | $2,167 / $2,236 | 18 / 17 |
| eth_15m losers | 4 | 0 / 0 | 100.0% | 100.0% | nan | 0 / 0 | $0 / $0 | 0 / 0 |
| eth_15m winners | 279 | 49 / 47 | 97.8% | 97.5% | 0.97 | 2,314 / 2,238 | $48 / $46 | 4 / 2 |
| eth_5m losers | 90 | 24 / 24 | 97.8% | 97.8% | 1.02 | 616 / 625 | $-570 / $-580 | 1 / 1 |
| eth_5m winners | 1085 | 229 / 220 | 98.6% | 97.5% | 0.97 | 5,009 / 4,879 | $360 / $350 | 12 / 3 |
| sol_15m losers | 4 | 3 / 3 | 100.0% | 100.0% | 1.00 | 70 / 70 | $-68 / $-68 | 0 / 0 |
| sol_15m winners | 208 | 38 / 38 | 99.0% | 99.0% | 1.00 | 686 / 686 | $17 / $17 | 1 / 1 |
| sol_5m losers | 15 | 5 / 5 | 100.0% | 100.0% | 1.00 | 241 / 241 | $-236 / $-236 | 0 / 0 |
| sol_5m winners | 648 | 80 / 81 | 98.9% | 98.9% | 1.02 | 3,413 / 3,473 | $70 / $70 | 3 / 4 |

### Largest |Δ PnL| orders (mode `eng` − live)

| account | profile | market (UTC) | side @px | size | live / eng / fp3 fs | won | Δ PnL | mechanism |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| account_a | btc_5m | 10-01 14:00 | DOWN @0.92 | 1169 | 422 / 1169 / 956 | W | +59.76 | partial size: sweep print counted in full |
| account_a | btc_5m | 10-01 16:45 | UP @0.94 | 601 | 71 / 12 / 32 | L | +55.18 | partial size: live 71 vs sim 12 (queue ahead 950 / footprint 349) |
| account_a | btc_5m | 10-01 16:35 | UP @0.90 | 964 | 356 / 301 / 301 | L | +49.50 | partial size: live 356 vs sim 301 (queue ahead 1316 / footprint 1311) |
| account_a | btc_5m | 10-01 13:50 | UP @0.94 | 1163 | 678 / 1163 / 1163 | W | +29.11 | partial size: live 678 vs sim 1163 (queue ahead 243 / footprint 243) |
| account_a | eth_5m | 10-01 19:55 | DOWN @0.91 | 38 | 25 / 0 / 25 | L | +22.75 | engine missed, footprint replay fills 25: queue ahead at join 43 (engine) vs 5 (footprint) |
| account_a | btc_5m | 10-01 16:10 | UP @0.92 | 740 | 465 / 740 / 740 | W | +21.97 | partial size: live 465 vs sim 740 (queue ahead 344 / footprint 0) |
| account_a | btc_5m | 10-01 14:55 | UP @0.97 | 748 | 25 / 748 / 748 | W | +21.69 | partial size: live 25 vs sim 748 (queue ahead 35 / footprint 35) |
| account_a | sol_5m | 10-01 13:45 | UP @0.99 | 59 | 55 / 36 / 55 | L | +18.69 | partial size: live 55 vs sim 36 (queue ahead 104 / footprint 45) |
| account_a | btc_5m | 10-01 17:40 | UP @0.95 | 361 | 303 / 0 / 0 | W | -15.13 | filled without a print in the tape (level drop only: depth-fill) |
| account_a | btc_5m | 10-01 17:50 | UP @0.93 | 230 | 15 / 0 / 15 | L | +14.32 | engine missed, footprint replay fills 15: queue ahead at join 289 (engine) vs 65 (footprint) |
| account_a | btc_5m | 10-01 17:50 | UP @0.91 | 344 | 32 / 42 / 31 | L | -9.10 | partial size: sweep print counted in full |
| account_a | btc_5m | 10-01 16:05 | UP @0.90 | 504 | 245 / 235 / 247 | L | +9.00 | partial size: live 245 vs sim 235 (queue ahead 504 / footprint 504) |
| account_a | btc_5m | 09-28 12:20 | DOWN @0.96 | 700 | 332 / 525 / 322 | W | +7.74 | partial size: live 332 vs sim 525 (queue ahead 289 / footprint 289) |
| account_a | btc_5m | 10-01 16:55 | DOWN @0.92 | 725 | 616 / 523 / 478 | W | -7.42 | partial size: live 616 vs sim 523 (queue ahead 224 / footprint 224) |
| account_a | eth_5m | 10-01 12:55 | DOWN @0.92 | 90 | 0 / 90 / 90 | W | +7.20 | race at open: print 68 ms after placement |

### Fill-model $ per parity window (captured orders only; `perwin.py` → `replay/per_window.csv`)

| window tag | config | orders | live sh | live PnL | eng PnL | eng_ftfix PnL | fp3 PnL |
| --- | --- | --- | --- | --- | --- | --- | --- |
| PAR_account_a_btc_15m_01 | retry_busy__plus_095_096__vol5 | 13 | 55 | $1.8 | $1.8 | $1.8 | $1.8 |
| PAR_account_a_btc_15m_02 | retry_busy__plus_095_096__vol5 | 7 | 10 | $0.2 | $0.2 | $0.2 | $0.2 |
| PAR_account_a_btc_15m_03 | retry_busy__plus_095_096__vol5 | 97 | 995 | $26.7 | $27.5 | $27.5 | $27.5 |
| PAR_account_a_btc_15m_04 | retry_busy__plus_095_096__vol5 | 29 | 480 | $12.1 | $12.1 | $12.1 | $-41.4 |
| PAR_account_a_btc_15m_05 | retry_busy__plus_095_096__vol5 | 18 | 16 | $0.2 | $0.2 | $0.2 | $0.2 |
| PAR_account_a_btc_15m_06 | retry_busy__plus_095_096__vol5 | 83 | 133 | $4.0 | $4.0 | $4.0 | $4.0 |
| PAR_account_a_btc_15m_07 | retry_busy__plus_095_096__vol5 | 118 | 167 | $4.3 | $4.3 | $4.3 | $4.3 |
| PAR_account_a_btc_5m_01 | retry_busy__plus_095_096__upma | 79 | 523 | $14.9 | $15.8 | $15.8 | $15.8 |
| PAR_account_a_btc_5m_02 | retry_busy__plus_095_096__upma | 29 | 432 | $-10.2 | $-12.2 | $-10.8 | $-11.4 |
| PAR_account_a_btc_5m_03 | retry_busy__plus_095_096__upma | 106 | 1,073 | $34.2 | $38.1 | $38.1 | $33.7 |
| PAR_account_a_btc_5m_04 | retry_busy__plus_095_096__upma | 47 | 411 | $15.3 | $25.7 | $24.1 | $18.6 |
| PAR_account_a_btc_5m_05 | retry_busy__plus_095_096__upma | 79 | 2,723 | $94.9 | $97.4 | $95.4 | $97.4 |
| PAR_account_a_btc_5m_06 | retry_busy__plus_095_096__upma | 76 | 2,147 | $72.0 | $77.6 | $77.6 | $68.3 |
| PAR_account_a_btc_5m_07 | retry_busy__plus_095_096__upma | 115 | 5,630 | $-500.0 | $-505.6 | $-503.9 | $-502.1 |
| PAR_account_a_btc_5m_08 | retry_busy__plus_095_096__upma | 136 | 4,754 | $163.2 | $167.9 | $167.5 | $160.0 |
| PAR_account_a_btc_5m_09 | retry_busy__plus_095_096__upma | 308 | 14,286 | $-323.4 | $-327.8 | $-327.1 | $-329.8 |
| PAR_account_a_btc_5m_10 | keep_queue__range_090_097 | 271 | 16,102 | $-960.4 | $-744.5 | $-794.6 | $-949.6 |
| PAR_account_a_btc_5m_11 | retry_busy__plus_095_096__upma | 152 | 6,012 | $213.7 | $213.2 | $213.2 | $218.6 |
| PAR_account_a_eth_15m_01 | btc5_portfolio_ser_m001_lb3_d1 | 15 | 77 | $1.4 | $1.4 | $1.4 | $1.4 |
| PAR_account_a_eth_15m_02 | btc5_portfolio_ser_m001_lb3_d1 | 3 | 6 | $0.2 | $0.2 | $0.2 | $0.2 |
| PAR_account_a_eth_15m_03 | btc5_portfolio_ser_m001_lb3_d1 | 45 | 468 | $8.8 | $9.2 | $9.2 | $8.8 |
| PAR_account_a_eth_15m_04 | btc5_portfolio_ser_m001_lb3_d1 | 29 | 505 | $9.6 | $8.1 | $8.1 | $8.1 |
| PAR_account_a_eth_15m_05 | btc5_portfolio_ser_m001_lb3_d1 | 26 | 299 | $6.4 | $6.4 | $6.4 | $6.4 |
| PAR_account_a_eth_15m_06 | btc5_portfolio_ser_m001_lb3_d1 | 52 | 749 | $15.6 | $15.9 | $15.4 | $15.7 |
| PAR_account_a_eth_15m_07 | btc5_portfolio_ser_m001_lb3_d1 | 105 | 89 | $2.4 | $2.1 | $2.1 | $2.4 |
| PAR_account_a_eth_5m_01 | keep_queue__range_090_098__vol | 45 | 42 | $3.5 | $5.5 | $5.5 | $5.5 |
| PAR_account_a_eth_5m_02 | keep_queue__range_090_098__vol | 9 | 17 | $1.2 | $0.6 | $0.6 | $0.6 |
| PAR_account_a_eth_5m_03 | keep_queue__range_090_098__vol | 219 | 1,017 | $4.7 | $-3.2 | $-3.7 | $0.9 |
| PAR_account_a_eth_5m_04 | keep_queue__range_090_098__vol | 88 | 517 | $-49.2 | $-51.6 | $-51.6 | $-51.6 |
| PAR_account_a_eth_5m_05 | keep_queue__range_090_098__vol | 76 | 192 | $14.0 | $13.0 | $13.0 | $12.6 |
| PAR_account_a_eth_5m_06 | keep_queue__range_090_098__vol | 330 | 1,243 | $54.3 | $69.1 | $54.6 | $63.2 |
| PAR_account_a_eth_5m_07 | keep_queue__range_090_098__vol | 399 | 2,588 | $-239.8 | $-232.5 | $-248.2 | $-250.1 |
| PAR_account_a_sol_15m_01 | btc5_portfolio_ser_m0001_lb3_f | 11 | 31 | $0.9 | $1.2 | $1.2 | $1.2 |
| PAR_account_a_sol_15m_03 | btc5_portfolio_ser_m0001_lb3_f | 44 | 509 | $-47.3 | $-48.2 | $-48.2 | $-48.2 |
| PAR_account_a_sol_15m_04 | btc5_portfolio_ser_m0001_lb3_f | 18 | 101 | $3.0 | $3.0 | $3.0 | $3.0 |
| PAR_account_a_sol_15m_05 | btc5_portfolio_ser_m0001_lb3_f | 27 | 36 | $0.9 | $0.9 | $0.9 | $0.9 |
| PAR_account_a_sol_15m_06 | btc5_portfolio_ser_m0001_lb3_f | 39 | 21 | $0.5 | $0.5 | $0.5 | $0.5 |
| PAR_account_a_sol_15m_07 | btc5_portfolio_ser_m0001_lb3_f | 73 | 58 | $-8.5 | $-8.6 | $-8.5 | $-8.5 |
| PAR_account_a_sol_5m_01 | btc5_portfolio_ser_m0001_lb3_f | 22 | 0 | $0.0 | $0.0 | $0.0 | $0.0 |
| PAR_account_a_sol_5m_02 | btc5_portfolio_ser_m0001_lb3_f | 7 | 5 | $0.1 | $0.1 | $0.1 | $0.1 |
| PAR_account_a_sol_5m_03 | btc5_portfolio_ser_m0001_lb3_f | 182 | 842 | $18.6 | $19.2 | $19.2 | $19.1 |
| PAR_account_a_sol_5m_04 | btc5_portfolio_ser_m0001_lb3_f | 54 | 268 | $-13.3 | $-13.3 | $-13.3 | $-13.3 |
| PAR_account_a_sol_5m_05 | btc5_portfolio_ser_m0001_lb3_f | 61 | 261 | $6.7 | $5.6 | $6.3 | $6.7 |
| PAR_account_a_sol_5m_06 | btc5_portfolio_ser_m0001_lb3_f | 118 | 931 | $-39.4 | $-39.7 | $-38.8 | $-38.8 |
| PAR_account_a_sol_5m_07 | keep_queue__range_097_099 | 219 | 1,347 | $-138.0 | $-121.8 | $-139.4 | $-139.4 |
| PAR_account_c_btc_5m_04 | retry_busy__plus_095_096 | 15 | 150 | $3.0 | $2.3 | $2.3 | $2.3 |
| PAR_account_c_eth_5m_05 | keep_queue__range_090_099__vol | 9 | 10 | $0.6 | $0.6 | $0.6 | $1.6 |
