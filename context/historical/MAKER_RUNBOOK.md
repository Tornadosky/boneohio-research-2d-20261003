ATLAS-ready maker / Q guidance (source-inspected 2026-10-02; result figures to append after jobs)

Canonical Q99 strategy launcher:
  $VIPER_HOME/vania/q99_20261002_DEPLOYGAP/code/one.sh
  argument order: TAG COIN MIN FEED SPEC MULT DIV MINSH MAXSH START END OWNPROF MOM TSTART ACCT OPEN_MS:CANCEL_MS MODE
  MODE=lf selects $VIPER_PTMP/exp/q99_20261002_DEPLOYGAP/h4/pkg2.
  MODE=lf2 selects $VIPER_PTMP/exp/q99_20261002_DEPLOYGAP/pkgLF2 and adds event-lifecycle hooks; it is a distinct forensic mode, not an alias for lf.
  FEED=kraken uses account bot Kraken ticks; kraken_act additionally includes activity-fix rows; pflow uses the cache's Binance mids.
  OWNPROF selects inputs_<ACCT>/own_steps_<OWNPROF>.parquet, or none. Record which account/profile was removed; never subtract a different wallet's orders.
  Run every new experiment in a new dated output namespace; preserve the launcher's package, cache, own steps and flags in a manifest. The stock launcher writes into DEPLOYGAP's results/index/log directories, so inspect/copy/redirect it before a no-mutation experiment.

Required fill flags used by that launcher:
  Q99_FILLPAR=1
  Q99_FILLPAR_EFFECTIVE_SHIFT_MS=60
  Q99_FILLPAR_CANCEL_RULE=sizematch
  Q99_FILLPAR_THROUGH=1
  Q99_FILLPAR_COMPLEMENT=1
  Optional historical footprint source: Q99_FILLPAR_OWN_STEPS=<exact account/profile parquet>
  Default Q99_FP_FORCE_ENV is PAPER_SEND_START_DELAY_MS=0.2, PAPER_SHADOW_ORDER_OPEN_DELAY_MS=36.8, BINANCE_ADVERSE_CANCEL_DELAY_MS=24 (milliseconds).
  lf additionally enables Q99_LIVE_KQ_RULE=1, Q99_LIVE_ACK_MS=0, Q99_LIVE_MIN_ORDER_SHARES=5.
  lf2 additionally enables Q99_LIVE_SIZE_EVENTS=1, Q99_LIVE_BLOCK_EVENTS=1, Q99_LIVE_RB_EVENTS=1 and Q99_LIVE_SIZE_STEP=min; LF2_FLAGS passes explicit extra switches.

Q99_LIVE_KQ_RULE is a decision-lifecycle correction, not a queue-ahead multiplier:
  In _vania_live_rules, a serialized block-mode, non-portfolio strategy without retry_blocked_signals is changed to retry_blocked_signals=True and retry_supersede_lower_on_higher_signal=False. Busy-time signals remain eligible after the occupied slot is released. ACK cooldown is separately set to zero. This preserves Q99's live repost behavior; it must not be transferred to BoneOhio merely because both buy near .99. BoneOhio's measured lifecycle is mostly one order per market, with much slower and less frequent reposts.

Canonical queue mechanics (source: code/fill/fillrep.py, verbatim fillpar3 reference functions):
  1. Replay full snapshots and absolute bid-level updates in (venue_ts_ms, seq) order. A full snapshot resets absent tracked levels to zero. Do not interpolate a future depth observation into Q at entry.
  2. For a recorded live order, identify the (venue timestamp, seq) footprint carrying the order's size. Reconstruct queued external lots before this event, cancel unexplained reductions under the rule below, insert our remaining quantity, then put positive extra quantity behind it. Q ahead is the reconstructed external quantity before insertion, not total displayed size including ours.
  3. For a BUY of outcome y at L, qualifying sell-like prints are same-outcome SELL and opposite-outcome BUY, with opposite price mapped as 1-p. FIX admits mapped prices <=L.
  4. Apply each print at venue timestamp minus 60 ms. For a finite sweep, available quantity at L is max(0, print quantity minus displayed bids strictly above L), evaluated just before the effective print (minus 0.5 ms in the reference). At .001 ticks, .991-.999 bids are therefore relevant to a .99 order.
  5. A mapped print strictly through L empties the queue in FIX. For level reductions with no assigned print, cancel the newest external lot whose size exactly matches the reduction; otherwise cancel newest external lots first. Never turn an unexplained level drop into our fill in the base model (DF1 is a stress diagnostic).
  6. Process trade, depth, then synthetic post ties consistently; preserve depth seq ties. The own-live reference inserts at the real footprint seq and excludes events at/after exit. User-WS cancel exit is cancel_venue_ms-1; inferred BoneOhio exit is a different measurement and must be labeled.
  7. Preserve complement mapping, through, better-bid subtraction, sequence ordering, and own-footprint handling together. 'Same token exact-price only', scalar Q haircuts, or a static book are diagnostic alternatives, not the validated reference.
  8. Stress shift timing (e.g. 0/20/60/120 ms), cancellation attribution, and sweep conventions; report false fills, misses, share ratio, first-fill error and losing-order behavior. Print lag and observed own fills do not make an inferred counterfactual order lifecycle exact.

Forensic versus prospective:
  Historical fixed-order parity uses actual posting/cancel times and may use actual fill-derived footprint removal. It audits execution accounting but does not recover an independent entry signal or authorize a prospective PNL claim. b_replay.py removes own size using realized historical fills, unlike the reference queue's explicit footprint insertion. Both must be labeled as order-conditioned replay.
  The simplified BoneOhio b_replay.py also lost cross-type (time,seq) tie order when concatenating deltas and snapshots, used np.interp only in its ahead0 diagnostic, and omitted better-level sweep subtraction. The interpolation flaw affects its displayed ahead0, not directly its fp_fill result. Keep those issues separate.

Independent own-live evidence and caveat:
  Source notes/FILL.md reports 2,676 accepted captured LB orders in the first window: FIX 98.4% yes/no, 0.996x live shares. Its later 1,156-order addendum is 98.2% / 1.112x, and BTC KQ_90_97 is 1.163x. Therefore do not cite one aggregate as universal maker validation.
  fill/replay_labeled_kq.parquet has up_wins typed null in inspected schema; fillsum.py falls back to (winner==side), silently assigning payoff0 when winner is missing. Its afternoon PNL and loser classification are unusable without a new valid resolution join. Fill quantities/agreements remain usable. The new own99 audit deliberately reports fills only.

New bounded audit reproduction (under a single Slurm general job requesting -c128; no overlapping maker job):
  cd $VIPER_HOME/vania/boneohio_20261002_FINAL/maker
  PY=/mpcdf/soft/RHEL_9/packages/x86_64/python-waterboa/2025.06/bin/python
  $PY check_semantics.py
  OUT=$VIPER_PTMP/exp/boneohio_20261002_FINAL/maker_smoke $PY maker_audit.py --markets btc_5m --workers 2 --max-contracts 2
  $PY maker_audit.py --markets btc_5m --workers 96
  WORKERS=96 $PY canonical_bone.py
  WORKERS=8 $PY own99_audit.py
  Source dependencies are immutable existing b_replay.py and Q99 code/fill/fillrep.py; new results stay in FINAL/maker, FINAL/maker/canonical, FINAL/maker/own99.

Output interpretation:
  queue_variants.csv/parquet are per-order/per-model, summary.csv includes all/state/chronological/09-19/09-22 scopes, date_metrics.csv and week_metrics.csv preserve temporal results, equity.csv includes initial-zero-aware drawdown, loser_orders.csv preserves the known losing sample.
  maker PNL is gross settlement of the resting .99 component only: shares*(payoff-.99), no rebate, not full-wallet PNL.
  maker_audit ahead_interp_old/ahead_old_asof/ahead_seq_asof are RAW displayed .99 depth including the historical footprint (intentionally matching the archived diagnostic), not external Q ahead. canonical_bone ahead_seq_footprint is the external pre-insertion depth estimate. Label them distinctly.
  snapshot_checks.parquet and snapshot_down_resets.csv record carried depth before a full snapshot and snapshot depth after it. Positive-to-zero snapshot resets demonstrate disagreement, not by themselves missing-removal causation. carry99_above_best_observations measures .99 size present while reported best bid is below .99; duplicate mirrored observations and recorder ordering must still be investigated.
  canonical/excluded.csv lists orders lacking a footprint at the exact previously inferred timestamp; compare canonical versus simplified on the intersection before interpreting aggregate changes. canonical_nobetter60 holds the reference queue constant while suppressing better-bid subtraction.
  own99/manifest.json records accepted/replayed/no-cache coverage, archive reproducibility and unknown payoff labels; own99/summary.csv, daily.csv and fix_orders.csv are independent captured-live .99 evidence.

Prescribed conclusions before results:
  No chosen Q haircut, no invented depth fills, no resetting Q to fit five known losers. Reject/qualify maker economics until parity is supported in the relevant own-live price/size/regime and independently in BoneOhio's order-conditioned sample. Do not call snapshot-only replay or head-first cancellations a better estimator solely because they improve realized PNL parity.

Fresh 8-variant results (Slurm 12060942, successful; CSVs inspected locally):
  Original60 exactly reproduces the prior 846-order replay: 931,475.946 simulated / 1,090,255.847 real shares (0.854365), gross resting PNL $7,840.81 / $6,580.54 and DD $820.42 / $1,507.07. Ordered60 is identical. Entry interpolation versus as-of and old versus seq-ordered depth differs on zero of 846 entries in this sample; these source hazards are not the observed cause.
  Snapshot60 is a diagnostic, not a calibrated selection: 1,043,436.566 shares (0.957057), PNL $6,089.06, DD $1,524.27; losing shares 4,345.31 / 4,322.02. It reproduces Sept19/22 adverse days (-$1,087.15 / -$653.85 vs actual -$1,072.55 / -$658.02), while Original60 incorrectly shows +$372.25 / +$546.54.
  Front-first cancellation raises shares to 1.074788x but PNL to $10,065.43 and still simulates only 1,652.51 losing shares; more fills are not better execution parity. No-self variant simulates zero losing shares, demonstrating the double-counting problem if the historical footprint is left in depth and a new own order is also inserted.
  Snapshot comparisons: 18,073 order-window snapshot observations, zero downward resets and zero positive-to-zero transitions. 21 orders have any carried .99 above reported BBO (324 of 3,810,710 observed known-BBO rows); none is one of the five losing orders. These observations do not establish phantom depth as the missing-loser cause. Snapshot-generated preceding deltas could limit this comparator; direct raw-message attribution would be needed before a causal claim.
  All 846 sampled orders actually filled. Therefore apparent yes/no agreement is filled-order recall, and false-fill count zero is structural. This sample cannot estimate specificity against canceled/unfilled orders; use independent captured own-live .99 orders for that test.

Canonical replay and independent own-live .99 results (Slurm 12060949; CSVs inspected locally):
  Canonical60 covers all 846 orders, no missing/ambiguous exact-time footprints, zero replay errors. Simulated 931,393.219 shares / 1,090,255.847 (0.854289), same 136 misses, PNL $7,839.98 / $6,580.54 and DD $820.42 / $1,507.07. Switching off better-bid subtraction adds only 468.367 shares / $4.68. Thus the identified source-level simplifications do not explain the material underfill.
  Pure M: 540 orders, recall 75.74%, shares 0.763864x; BOTH losing pure-maker orders missed entirely (real 2,841.36 shares, simulated zero). Mixed taker/rest MT: 306 orders, recall 98.37%, shares 1.025831x, losing shares 1,473.95 / 1,480.66. Evidence file role_metrics.csv separates these cases.
  Recorded lifecycle limitation: 475 Bone orders have state=alive and use contract_end+600000 as an artificial replay cap; 124 inferred cancels have median lifetime 6,845 ms and 247 inferred full fills 16,444 ms. Never report the fallback-cap lifetime as directly observed. Remaining size median is 2,604 shares overall (3,000 pure M), versus 200 shares in the own-live BTC5 .99 cohort.
  Independent fresh own99 canonical replay: 631 accepted captured LB .99 orders; 628 with cache, 3 without. FIX: 98.726% yes/no, 98.567% exact (1% of size / .01-share tolerance), seven false fills and one missed, 4,601.06 simulated / 4,273.45 actual shares (1.076662x). BTC5 subset: 185 orders, 15 actually filled, one false fill, no misses, 1,822 / 1,702 shares (1.070505x). This establishes a much better reference on smaller Q99 orders but does not identify BoneOhio's large-order queue.
  Own99 old ENG exact/same-token rule gives only 0.265530x shares (40 missed), and DF1 unexplained-depth-as-fill gives 1.378118x (31 false fills). Those are rejected diagnostics. LIFO changes little at .99, 1.074790x; SH20 increases to 1.110775x; SWX recovers the one miss but stays 1.081276x. No model or parameter was selected using the five BoneOhio losers.
  Fresh own99 matches the archived overlapping replay quantities exactly. The afternoon archive's 1,064 .99 variant rows (133 distinct orders) all have unknown payoff; suppress their old PNL and loser labels. This is a missing-outcome label bug in the summary, not proof they were losses.
  Final maker verdict: canonical model reproduced and independently checked; BoneOhio resting maker economics fail parity, especially pure-M losing fills. Phantom-depth causation remains unproven. Snapshot-only is a useful diagnostic, not a validated substitute. Proceed with research/causal order reconstruction, not a claimed deployable .99 maker edge.
  Verification: 6,768 level-variant rows (846x8) and 3,384 canonical rows (846x4), unique (salt,variant), no negative quantities, no fills exceeding remaining order quantity, no missing Bone outcome labels, empty error arrays, original baseline reproduced numerically. Nine focused synthetic semantics checks passed; all four Python scripts compiled.
