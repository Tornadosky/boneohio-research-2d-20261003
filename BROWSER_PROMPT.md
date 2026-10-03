Use this repository to infer an approximate, interpretable BoneOhio strategy.
Work autonomously through data checks, feature construction, controlled
experiments and a runnable candidate. The aim is to explain observed behavior
and what predicts it; profitability alone is insufficient.

1. Read AGENTS.md, README.md, all findings/limits/research-plan documents and
   the maker/taker reference notes. Install dependencies, run the parity tests,
   fetch metadata/features/native recorder feeds, and run the baseline. Check
   manifests and censoring before defining labels. Polymarket pflow ts_ns is
   a normalized cache axis with uncertified actual receipt provenance; native
   Binance receipt is separately recorded. Declare that observation scenario.
2. Reconcile fresh public fills/activity and decoded chain links with historical
   filled salts and matched prints. Preserve mixed maker/taker orders, direct
   and complement tokens, all asset aliases and timestamp uncertainty. Define
   the aggressive lower-price arm and near-.99 sweep/rest arm separately.
3. Construct fixed-time paired-side controls and candidate order opportunities
   on BTC5/BTC15 across all 768 contracts. Analyze Binance returns at multiple
   horizons, signed trade flow, top imbalance, Polymarket full-depth imbalance,
   stale/cheap asks, spread, queue/size changes, time to expiry and move from
   opening-price proxies. Model bursts, prior activity, possible inventory and
   redemption timing. Missing observed fills are censored negatives.
4. Use September 30 for development. Register a small interpretable hypothesis
   set, freeze it, then test on October 1; these are reused historical dates.
   Compare maker and taker rules, BTC5/BTC15 transfer and fixed-time matched
   controls. Show denominators, uncertainty, ablations and failures. If useful,
   fetch Binance full L2 and Kraken directly using the optional provider tool
   into this private workspace; never publish those vendor bytes/derived tapes.
5. Download exact poly shards as needed. Evaluate proposed trades with the
   supplied maker/taker mechanics under declared clocks, latency, fees,
   budgets, queue/depletion and quality limits. Distinguish source-parity mode
   from prospective simulation. Replay the known maker-loss diagnostics to
   identify model failures; never tune a signal on their known outcomes.
6. Persist code, configurations, experiment tables and a concise evidence
   ledger under results/. Continue until you have the best supported compact
   approximation and a clear account of what it fails to explain, or identify
   precisely which unavailable observations prevent discrimination. Do not
   present correlation, a synthetic fill assumption or a fitted PnL as proof
   of the true strategy.

Deliver a runnable candidate, reproduction commands, an evidence table with
development/holdout results, execution limitations, rejected alternatives and
the next most informative observations needed. No trading or live deployment.
