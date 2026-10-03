# Work in this public research workspace

Read README.md, docs/BONEOHIO_FINDINGS.md, docs/EVIDENCE_LIMITS.md,
docs/RESEARCH_PLAN.md and both execution reference documents before research.
This workspace is self-contained. It has no Viper connection or private
credentials; historical source-host pointers are provenance references.

Install with `python -m pip install -e '.[test]'`, run `python -m pytest -q`,
fetch the selected release data and validate hashes. Start with
`metadata,features,recorder`; add exact `poly` shards for execution tests.
Keep downloaded data/results ignored. Do not commit CryptoHFTData feeds or
derived quote/feature sequences: the provider forbids redistribution.

Keep maker near-.99 sweep/rest and lower-price taker activity separate.
Preserve seconds-resolution API uncertainty, signed-order timestamp ambiguity,
direct/complement token identity, mixed-role salts, missing/unfilled orders,
right censoring and quality failures. Report unresolved evidence rather than
inventing complete submissions, cancellations or historical feed receipt.

Use strict receipt-asof views where genuine receipt is recorded. Polymarket
pflow ts_ns is earliest-per-row merged vendor collector receipt clipped at
venue time, not one native observer or BoneOhio receipt. Declare that
observation scenario. Use strict venue-before views for declared arrival
replay. Never use final outcomes, future quotes,
the target's later fills, inferred later cancellations or hindsight-derived
footprints as prospective signal inputs. Frozen historical footprint replay
is explicitly forensic.

Develop on September 30, freeze a small hypothesis family, then evaluate on
October 1; label both dates reused historical data. Diagnostics selected on
losses are failure-analysis fixtures. Evaluate event matching, selection,
execution and accounting separately, with denominator/coverage and negative
results. Q99 rules do not become BoneOhio rules by default.

Persist runnable experiments and small reports under results/. Keep a dated
research ledger, exact command/config/source hashes and remaining hypotheses.
Proceed through the agreed investigation until an approximate rule is
supported or the available evidence cannot distinguish remaining rules.
State that limit explicitly; do not force a strategy match.
