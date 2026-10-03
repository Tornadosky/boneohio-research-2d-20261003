# Historical evidence library

Prepared 2026-10-03 for the public BoneOhio browser research bundle. Read
`../docs/BONEOHIO_FINDINGS.md`, `../docs/EVIDENCE_LIMITS.md` and
`../docs/RESEARCH_PLAN.md` before building labels or choosing a fill model.

This directory contains small public-wallet evidence and selected research
results. The main market interval is **2026-09-30 00:00 UTC through 2026-10-02
00:00 UTC**, but this library intentionally retains earlier context and known
execution failures. It does not extend market-data coverage by implication.

| Files | Purpose and interpretation |
|---|---|
| `wallet/fills_history.parquet` | 162,688 API fill rows across observed coins/tenors, August 15 through October 1 21:08:38 UTC; raw public fields plus historical research-derived role/outcome/fee/PnL. No unfilled orders. |
| `wallet/decoded_order_summary.parquet` | 23,751 filled signed-order salts since September 10; exact observed calldata fields mixed with explicitly derived mappings/aggregates. `order_ts` is not an observed submit time. |
| `wallet/chain_order_fill_links.parquet` | 57,880 BoneOhio-only decoded order appearances, with transaction/token/salt, raw amount units and role by calldata argument position. Exact public links let the researcher undo grouping ambiguities. No counterpart wallets, signatures or opaque metadata. |
| `wallet/maker_lifecycle_history.parquet` | 8,875 historical near-.99 order summaries on BTC/ETH/SOL; fingerprint-derived post/cancel/filled states, censoring and repost history. Not a complete lifecycle or submission ledger. |
| `wallet/matched_fill_groups_history.parquet` | 43,870 BTC/ETH/SOL 5m/15m transaction-token groups, including 861 unmatched print times. Price/PnL and inferred `m` must not become causal features. |
| `wallet/matched_venue_prints_history.parquet` | 43,009 mapped public prints through October 1 19:49:06.703 UTC. Preserve direct/complement token distinctions. |
| `wallet/fresh_fills_two_days.parquet`, `fresh_taker_fills_two_days.parquet` | Fresh exhausted public v2 walks: 3,314 fills and 828 taker-only fills on September 30–October 1, latest at October 1 23:59:09 UTC. API role inference is explicitly provisional. |
| `wallet/fresh_activity_two_days.parquet` | 4,333 public activity rows: trades, 1,014 redemptions, two maker rebates, two taker rebates and one withdrawal. Not complete historical capital. |
| `wallet/fresh_tail_chain_links.parquet` | All 307 fresh transactions absent the historical chain source were publicly decoded: 307 target order appearances, 57 salts; no ABI/selector failures. |
| `wallet/fresh_order_fill_identity_two_days.parquet`, `fresh_signed_orders_two_days.parquet` | Direct transaction/token joins cover all 3,314 fills with zero API-vs-decoded-role disagreements; 1,109 observed filled salts, including 844 near-.99 and 265 other-limit salts. These summaries cover fills inside the slice, not entire lifetimes. |
| `wallet/fresh_fill_venue_links.parquet`, `fresh_venue_print_candidates.parquet` | 1,613 direct and 635 complement public-print candidates for 2,256 Bitcoin fills; eight unresolved, 1,058 other-asset rows outside the market catalog. Unique price consistency remains inferred attribution, not a certified causal entry timestamp. |
| `FRESH_VENUE_MATCHING.md`, `match_fresh_venue.py`, `legacy_gt_reference.py` | Portable matching recipe/function with null and ambiguity states; sanitized original matcher as historical reference. Seven unmatched API times are beyond the source's contract-end-plus-60s tail; one remains unexplained. |
| `historical/FINAL_RESULT.md`, `SELECTION_RESULT.md`, `TAKER_RESULT.md`, `MAKER_RUNBOOK.md` | Authoritative October 2 conclusions and exact assumptions. Research-root paths are sanitized into variables; these are evidence references, not cloud launch commands. |
| `historical/maker/canonical/*` | All four canonical diagnostic variants and temporal/role metrics, including the pure-maker loss failures. |
| `historical/maker/*` | Eight earlier queue diagnostic summaries and unsuccessful explanations. Snapshot-only is unvalidated. |
| `historical/maker/own99/{summary,daily}.csv` | Independent reference-engine parity aggregates. Individual private account traces are excluded. |
| `historical/maker/diagnostic_contracts.csv` | Five known-outcome BTC5 losing-order exemplars outside the two-day slice. Their optional raw partitions belong in `data/diagnostics`, never in a strategy holdout. |
| `historical/selection/*` | All examined rules, chronology/denominators and causal prior-activity features. Target absence is a censored negative. No price/outcome optimization claim. |
| `historical/feed_clock/*` | Fixed-time paired case/controls, route coverage, clock audit, complement/unmatched join exceptions and feed comparisons. This short multi-venue study is September 12–13, not new two-day multi-venue tape. |
| `historical/taker/*` | Frozen BI1/F2 conditional execution sensitivity and coverage. Experimental candidate rather than recovered BoneOhio entries. |
| `historical/baseline/*` | Historical wallet descriptive performance aggregates. Outcomes and payouts are retrospective. |

`PROVENANCE.json` records the source SHA256, exported SHA256, source bytes,
columns, transformations and wallet coverage. Source pointers use
`$VIPER_PTMP/exp/<task>/...` and `$VIPER_HOME/vania/<task>/...`; private source
account/host components have been removed. Byte-identical selected Parquet
tables retain source metadata. Sanitized text has a distinct output hash.
Predecessor reports and derivation scripts have source fingerprints without
copies, preserving the supersession chain without exporting a private handoff.
Fresh sources and transformations use separate `wallet/FRESH_PROVENANCE.json`,
`FRESH_CHAIN_PROVENANCE.json`, `FRESH_IDENTITY_PROVENANCE.json` and
`FRESH_VENUE_PROVENANCE.json`; they record
bounded cursor walks, page response hashes, public RPC audits and output hashes.
The new print matcher records every input tape hash and exact unresolved IDs,
plus observed partition ranges computed before transaction filtering.
`PFLOW_CLOCK_PROVENANCE.json` audits all 48 main hourly source manifests and
two boundary hours: earliest-received per-row vendor merging, timestamp
unit conversion, venue clipping, historical alternate branch and converter/
adapter hashes. It distinguishes native feed receipt from the Poly
`source_cache_ns` normalized vendor collector-receipt proxy.

The exporter is reproducible from the original read-only research directories:

```bash
export BONEOHIO_SOURCE_EXP=/path/to/original/exp
export BONEOHIO_SOURCE_NAV=/path/to/original/vania
python context/export_context.py --out /path/to/new/context
```

This is a source-host extraction utility. A cloud researcher uses the exported
files; they do not need Viper access. Main-bundle manifests define fresh wallet
deltas, actual market/feed coverage and downloads separately.

Two provenance records point to the single chain link output because it was
constructed from two source parts. Historical coin strings contain aliases
(`bitcoin`/`btc`, `ethereum`/`eth`, `solana`/`sol`) and include eight normalized
asset groups in this snapshot; normalize from slugs/token metadata and retain
the original value. Cache-based order mappings are BTC/ETH/SOL only. A null
`coin` there means no catalog mapping, not a deleted order or inactive coin.

All 55 uniquely exported historical evidence files were SHA256-verified after
copying; `VALIDATION.json` now verifies 63 unique historical/fresh data outputs,
including the print-candidate export, with zero hash mismatches.
No private own-account wallet order traces, credentials, SSH configuration,
fleet settings or signatures are in this curated library. The old research
reports are historical snapshots; see `../docs/SOURCES.md` for the dated
Polymarket provider-documentation correction.

No CryptoHFTData tick tape, reconstructed vendor book or per-event vendor
price/size sequence is exported. Historical feed-clock comparisons use native
recorder/pflow-cache sources and retain aggregate metrics plus Polymarket
wallet identities; the large underlying series were not copied. See the main
package's distribution/data-license notes for optional vendor access.
