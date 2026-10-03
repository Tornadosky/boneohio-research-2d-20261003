# BoneOhio two-day research workspace

Self-contained inputs for reconstructing an approximate BoneOhio strategy in
browser Codex. Public wallet: `0x48ac40fc545cf327edd5365435c3a9f385614a7e`.
Prepared October 3, 2026. Main market interval: **September 30 and October 1,
2026, UTC**, the last two complete days in the best available pendulumflow
cache. October 2 is partial and deliberately outside this study.

## Start in browser Codex

Connect this GitHub repository to a Codex cloud environment. Set its setup
command to **`bash scripts/setup_cloud.sh`** for dependencies, tests, all data,
checksums and the baseline. For a smaller first setup use
`BONE_DATA_GROUPS=metadata,features,recorder bash scripts/setup_cloud.sh`.
Enable agent network access to GitHub release downloads if additional shards
will be fetched during the task: `github.com`, `api.github.com`,
`release-assets.githubusercontent.com` and `objects.githubusercontent.com`.
Optional vendor downloads also need `api.cryptohftdata.com` and its download
redirect hosts. Cloud environment configuration is described in the
[official Codex guide](https://learn.chatgpt.com/docs/environments/cloud-environments).
The equivalent manual quick start is:

```bash
python -m pip install -e '.[test]'
python -m pytest -q
python scripts/fetch_data.py --groups metadata,features,recorder
python scripts/validate_bundle.py
python scripts/run_baseline.py
```

Then send [BROWSER_PROMPT.md](BROWSER_PROMPT.md) as the task. The default fetch
is about 195 MiB and runs the initial fixed-time study. Download the exact
Polymarket replay inputs as needed:

```bash
python scripts/fetch_data.py --list --groups all
python scripts/fetch_data.py --groups poly --tenor btc_5m --day 2026-09-30
# Or the entire public data package:
python scripts/fetch_data.py --groups all
python scripts/validate_bundle.py --require-all
```

All release assets together are about **3.53 GiB**, with about **4.22 GiB** of
extracted files. Archives are deleted after verified extraction. The downloader
checks available disk, uses SHA256 and rejects unsafe archive paths. Select
one day/tenor for a small environment; full download needs room for the largest
temporary archive, dependencies and experiment outputs. No Viper connection,
wallet credentials, SSH keys or paid API keys are required.

## What is here

| Input | Location and scope |
|---|---|
| Exact Polymarket books and trades | Release group `poly`: 576 BTC5 and 192 BTC15 contracts; normalized depth events, checkpoint books, BBO and trade tape; original integer micro units and event/receipt clocks preserved. |
| Catalogs, settlements and data quality | `metadata`: token/condition mappings, resolved oracle labels, Binance boundary proxies, per-contract coverage. Settlement labels are retrospective targets. |
| Fast feature view | `features`: strict normalized-cache-clock 100ms paired book states. Fresh/seeded/uncrossed states qualify; **83.41%** pass this particular check. Cache time is merged vendor collector receipt clipped at venue time, not one native observer or BoneOhio receipt; this check is not an execution certificate. |
| Native Binance feed | `recorder`: BTCUSDT perpetual BBO/mids and aggTrades, with one-hour warmup and ten-minute tail. Original receive, publication and trade clocks retained. Top quantities and signed flow are available. |
| Fresh observed activity | `context/wallet/fresh_*`: 3,314 all-asset fills, 828 taker-only fills and 4,333 public activity rows for the two days. Includes redemption/rebate cashflows and the late October 1 tail. |
| Fresh venue-link extension | 2,256 BTC fills: 1,613 direct and 635 complement price-consistent print candidates, eight unmatched; join flags/provenance and a portable matcher are supplied. These are inferred retrospective links, not submission observations. |
| Historical context | `context/wallet/` plus `context/historical/`: broad wallet history, decoded signed orders and transaction links, inferred maker lifecycles, matched venue prints, rejected hypotheses and prior validation. Read provenance for separate coverage cutoffs. |
| Known maker-loss diagnostics | Release group `diagnostics`: five earlier BTC5 contracts selected for failure analysis, outside the two-day sample. They are not a holdout. |
| Maker reference | `bonebundle.maker`: latest Q99 deployment-gap replay, including `eng_ftfix`, historical production `eng` and footprint diagnostics; anonymous actual parity fixtures and archived source hashes. |
| Taker reference | `bonebundle.taker`: latest A2 arrival-depth / principal-budget FAK mechanics, full-depth mode and historical four-level mode, causal guards, depletion sensitivity and actual A2 comparisons. |
| Optional external full-depth/multi-venue feeds | Direct-download utility and reader described in `docs/TAKER_REFERENCE.md`. CryptoHFTData Binance L2/Kraken tapes cannot be redistributed publicly; they are excluded from releases. Fetch directly into a private research workspace if useful. |

Raw `depth_clock` is omitted because it is a large derived table. Exact event
replay and the explicitly sampled feature view are supplied. Release contents,
source/output fingerprints and transformations are recorded in
`data_manifest.json`, `assets.json`, `context/PROVENANCE.json`, fresh-wallet
provenance and the vendor source manifests.

## Read before interpreting results

Start with [docs/BONEOHIO_FINDINGS.md](docs/BONEOHIO_FINDINGS.md), then
[docs/EVIDENCE_LIMITS.md](docs/EVIDENCE_LIMITS.md) and
[docs/RESEARCH_PLAN.md](docs/RESEARCH_PLAN.md). The October 2 FINAL audit
supersedes earlier strong replica/feed/latency claims. The useful current
description is a lower-price aggressive BUY arm plus a near-.99 sweep/rest
arm; exact entries, latent capital state and large maker losses remain
unexplained. The maker and taker packages reproduce declared reference
mechanics; prior validation has material limits.

Public fills omit zero-fill orders and true submit/cancel observations.
Signed timestamps, chain timestamps, venue prints and recorder arrival are
distinct. Binance boundary prices are proxies, not verified historical
settlement feed values. These dates have been inspected in earlier research;
chronological reuse is an internal holdout, not a pristine future evaluation.

`scripts/run_baseline.py` produces fixed-time paired-outcome observations,
native Binance return/flow features, guarded later API-dated fill labels and
separate day summaries. It records stale endpoints, observed aggTrade gaps
and receipt intervals. Its target is observed API-dated BUY activity, not recovered
entry intent or profitability. Results are written under ignored `results/`.

This repository contains research code and recorded public market/wallet
observations. Source snapshots have separate original and sanitized
distribution hashes. Provider attribution and applicable external-data
restrictions are recorded in [docs/SOURCES.md](docs/SOURCES.md).
