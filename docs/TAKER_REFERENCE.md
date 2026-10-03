# Taker execution reference — 2026-10-03

The runnable adapter is `bonebundle.taker`. Its budget sweep and Polymarket event replay come from the latest `tailtaker_20261003_A2PARITY` execution study, with its same-day Addendum 2 preserved. The archived numerical routines remain under `vendor/taker/a2_20261003`; private source paths and own execution identities are redacted. `SOURCE_MANIFEST.json` records original and distributed SHA256 hashes. Seven mathematical functions have independently captured, version-neutral AST fingerprints proving that their executable mathematics survived those redactions.

The package does **not** claim one universally validated backtester for avg_bot, vol075, TailTaker and BoneOhio. The October 2 cross-family audit explicitly withdrew that broad claim; the October 3 A2 study subsequently established narrower A2 parity. Keep both findings:

| Evidence | What it establishes | What it does not establish |
|---|---|---|
| avg_bot / vol075 historical momentum work | Relevant latency and FAK research; historical paper and deployment evidence | Complete dated signed-order/engine binding, all attempts and wallet cash parity remain unqualified in the October 2 audit |
| TailTaker October 1 exact executor | Event-level arrival ladders, principal-budget execution, per-level fees and live strategy conventions | Four-level depth cap, latency snapping, unknown-clock fallback and absent shared-wallet/depletion state restrict general use |
| A2 October 3, both recorded cohorts | All live intents reproduce with declared feed/strike/book-age conventions; bounded execution cohort includes zero fills; the 200 ms replay matches cohort B 29/29 fills exactly and cohort A 99.3% of 454 fill/no-fill results | Exact millisecond competition, SDK cents/share rounding, other wallets, other strategies or arbitrary depth/size are not certified |
| This portable adapter | Shares and principal reproduce all 483 archived A2 observations at all 16 saved latencies: **7,728 comparisons**; real 4,096-event depth prefix reproduces the original as-of replay | Extending beyond four levels and strict evidence guards are explicit engineering changes, not newly validated live economics |

The A2 Addendum 2 supersedes the earlier recommendation to change strikes: better live-equivalent BTC reconstruction did not support changing the live config. Model coefficients, feed conventions and selection gates are strategy-specific; they are not transferred to BoneOhio.

## Run and use

```bash
python -m unittest discover -s tests/taker -p 'test_*.py' -v
python vendor/taker/report_validation.py
```

Core matching and in-memory replay use the standard library. Parquet input and archived validation require the package's NumPy, pandas and PyArrow dependencies. The original CHD numerical loop is tested on **synthetic** rows without requiring numba; no provider L2 values are distributed.

```python
from bonebundle.taker import DepthReplay, match_budget_fak, match_time_ms

# Use the manifest's qualified contract coverage, never an outcome filter.
replay = DepthReplay.from_parquet(
    'data/poly', 'btc_5m', contract_start_ms,
    clock='venue', coverage_ok=coverage_is_qualified,
)
match_ms = match_time_ms(decision_ms, total_latency_ms=300)
arrival = replay.book(is_yes=True, match_ms=match_ms, max_book_age_ms=1000)
result = match_budget_fak(arrival, principal_usd=10, limit_price=.91)
```

`total_latency_ms` already includes the hold. Alternatively supply `ingress_latency_ms`; the helper adds the dated crypto hold at **venue ingress**: 250 ms before August 17 11:00 UTC, 50 ms until September 4 14:00 UTC, then 150 ms. Exactly one latency argument is required. There is no nearest-grid snapping. These are declared timing scenarios, not observed BoneOhio submission clocks.

Queries must ascend; use separate replay objects for decision and arrival views, or construct a new object to rewind. Venue arrivals use **strictly earlier** events, ordered by `(venue_ts_ms, seq)`. Decision views may be inclusive. Snapshots reset both ladders of their token; level updates replace absolute quantity. Carried BBO prunes inconsistent top levels while retaining all deeper recorded levels. Missing venue time is never replaced by receipt minus an assumed constant. Missing seeds, malformed events, known coverage gaps, crossed books and stale token state return `UNQUALIFIED`; known empty executable liquidity returns `NO_FILL`.

`clock='receive'` is a legacy API label selecting the preserved `ts_ns` axis. **For pflow, `ts_ns` is a normalized cache timestamp, not a certified actual recorder receipt.** This admits only a declared cache-availability scenario; it does not prove what was actually available to a live observer. It cannot be passed to the venue matcher. Even independently recorded observer receipt would not reconstruct BoneOhio's own feed receipt. `top_age_ms(decision_ms, feed_lag_ms=10)` measures time since either token's top price **or size** changed under a declared lag scenario. This is distinct from age since the last token depth event and from a recorded live observer's gate.

The default `depth_mode='full'` consumes all recorded compliant levels. `depth_mode='a2_four'` preserves the historical four-level cap and four-decimal price rounding for parity. A partial result in that mode is a four-level benchmark; deeper available liquidity can alter it. Deep WebSocket phantom levels remain a source caveat, even when BBO agrees.

BUY FAK is **principal-budget sized**: $1 at a .20 cap can buy 33.333333 shares at .03. `fixed_share_benchmark` is a separately marked comparison and must not be presented as certified Polymarket BUY FAK semantics. Request sizing uses submitted principal, never observed realized spend or outcome. Market minima, signed maker/taker unit rounding and tick-policy admission must be established separately for a new strategy.

Fees are valued **per consumed price leg** as `0.07 * p * (1-p) * shares`. Archived `execpar.py`/`simsize.py` reports instead value fees at VWAP; the frozen source and saved PnL remain unchanged. The supplied 200 ms cohort happens to match on a single price per order, so the measured fee-convention delta is numerical zero. The synthetic two-price regression demonstrates why these conventions differ in general. Historical cash-versus-share collection and per-maker rounding are not recovered by this level model.

Sell the losing tail means BUY its favourite complement at limit `1 - tail_bid`; select the **other token's direct ask ladder once**. Do not add the tail bid representation as extra liquidity. `mapped_complement_price` preserves micro-price mapping. Maker print/complement matching belongs to the separate maker component.

`ConsumptionLedger(contract_id)` implements the A2 size-study nonreplenishment sensitivity: prior hypothetical own fills are subtracted by token and price until contract end. It guards contract identity and chronology. Public aggregate L2 cannot prove actual refill/cancel identities, so this is an explicit scenario rather than a universal impact model. The frozen no-consumption matcher remains available through `depletion=None`.

`settlement_pnl(result, token_won)` needs an official binary outcome; missing labels stay `None`. Its result is a contract-end payout proxy, excluding mark-to-market, funding/capital lockup and actual redemptions. No shared-wallet balance, allowance, reservation or timeout ledger is fabricated. The October 2 generic guarded share/account contract is included for review, and explicitly rejects Polymarket BUY FAK/FOK share semantics; it is not silently relabelled as this budget adapter.

## Optional external L2 — private direct download

Provider data is excluded from this public distribution. CryptoHFTData's standard terms restrict redistribution and downstream repackaging; downloading directly does not grant publication rights. See the [provider terms](https://www.cryptohftdata.com/terms).

```bash
python vendor/taker/download_optional_feeds.py --dry-run
python vendor/taker/download_optional_feeds.py --output ../boneohio-private-feeds
```

The downloader uses the provider's documented [anonymous REST endpoint](https://www.cryptohftdata.com/docs/rest-authentication), without reading credentials, with serial rate limiting, bounded retries, explicit 404/access failures, checksum receipts and an hourly resume cursor. It preserves downloaded bytes and rejects output inside this repository. Default UTC hours run September 29 23:00 through October 2 00:59, providing warm-up and an end tail around the two study days. Change `--start`, `--end` or repeated `--series exchange:symbol:type` as needed. Missing hours are evidence gaps, never zero activity.

Defaults request Binance USD-M BTCUSDT L2/trades and Kraken derivatives PF_XBTUSD L2/trades. The actual `/v1/status` registry checked October 3 advertises `kraken_derivatives`; the static [exchange documentation](https://www.cryptohftdata.com/docs/rest-exchanges) calls it `kraken_futures`. The script pins the observed registry spelling and allows explicit override; it does not silently treat a 404 as a feed alias.

```python
from bonebundle.taker import read_binance_l2_hour
for view in read_binance_l2_hour(
    '../boneohio-private-feeds/binance_futures/2026-09-30/00/BTCUSDT_orderbook.parquet',
    levels=10,
):
    # GAP or UNKNOWN sequence evidence is excluded from strict qualified work.
    if view.qualified:
        use_features(view.event_ms, view.transaction_ms, view.recv_ns,
                     view.bids, view.asks)
```

The [provider L2 schema](https://www.cryptohftdata.com/datasets/binance-orderbook-data) keeps `received_time` in nanoseconds and separate exchange event/transaction clocks. Our Binance reader reproduces A2's hourly checkpoint plus absolute-update reconstruction, preserving integer nanoseconds, retaining all levels internally and emitting top N or full depth. `only_top_changes=True` reproduces top price/size-change emission. Original `chd_top_v2.py` is archived with provenance; the runnable adapter requires a single full hourly checkpoint and refuses missing/multiple seeds instead of guessing. Previous-update IDs are checked when supplied; missing continuity is `UNKNOWN`, gaps remain `GAP`. Original snapshot bridging and exchange-specific sequence differences need a separate strict streaming adapter before certification. The Binance 0.1 USD price quantum is not a general Kraken reader. Kraken bytes can be studied with their own source/sequence rules rather than passed through this Binance implementation.

Use true receipt times for causal signal tests, E/T for clearly labelled association comparisons, and frozen shifts only as sensitivity assumptions. A reconstructed L2 top is not the original bookTicker wire stream; provider `ticker` is rolling statistics, not BBO. Never publish downloaded vendor files or their reconstructed/reaggregated quote data as part of experiment outputs.

## Validation artifacts

`tests/taker/fixtures/a2_execution_inputs.parquet` holds anonymous fixture IDs, original principal/limit/outcome and exact four-level arrival inputs for all 483 archived observations. `EXECPAR.json` and `E2E.json` preserve the dated summaries with anonymous cohort names. `real_depth_prefix.parquet` is a source-backed Polymarket snapshot/update prefix, selected by chronology without outcome selection. It is a validation fixture, not another study day. Their source/output hashes and selection rules are recorded beside them. No CHD market-data fixture, token, API key, own order ID or private infrastructure path is required.

`tests/taker/VALIDATION.json` records source-function equivalence and the frozen-versus-per-level fee comparison. Semantic tests cover clocks, boundaries, same-ms exclusion, snapshot/absolute-update behavior, complement orientation, full-versus-four-level sweeps, budget improvement, depletion, unknown outcomes, malformed/stale data, integer receipt precision, synthetic CHD-loop parity and downloader retry/resume primitives.
