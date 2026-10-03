# Public Polymarket catalog provenance, 2026-10-03

The public catalog was rebuilt from independently witnessed PendulumFlow V3
`new_market` events. It covers all 768 two-day BTC5/BTC15 contracts and all five
older known-loss diagnostic contracts. The three maker parity fixtures fall
within the verified two-day contracts. No target was silently accepted without
an independent lifecycle record.

For each contract, the reconstruction checked the condition identifier, both
token identifiers, their UP/DOWN orientation, the engine market identifier and
the contract duration against the original cache. All 773 contracts matched.
The existing integer market and asset identifiers are generated locally and
were retained to preserve the event-table joins. Slugs independently observed
in the lifecycle stream determine the BTC key, tenor and contract start; contract
ends are start plus five or fifteen minutes. Optional `tick_str`,
`min_order_size_str` and `neg_risk` properties were omitted.

The retained market columns are `market_local_id`, `market_id`, `slug`, `asset`,
`tenor`, `market_key`, `contract_start_ms`, `contract_end_ms`, `condition_id`,
`session_id` and `end_ts_ms`. Asset columns are `asset_local_id`,
`market_local_id`, `market_key`, `contract_start_ms`, `contract_end_ms`,
`market_id`, `token_id`, `is_yes`, `outcome` and `session_id`.

`PUBLIC_CATALOG_PROVENANCE.json` records every independent contract witness,
the 54 source-hour URLs and original source SHA256 values, and each delivered
catalog SHA256. Those original source digests match the archive manifests and
the previously completed checksum-verifying mirror download ledger. This
metadata-only validation did not rehash the multi-gigabyte source event hours.
The scan used four workers with OMP, OPENBLAS, MKL and NUMBA thread counts set to
one, read only the rare lifecycle row groups, and completed in 2.3 seconds.
The source implementation is `rebuild_public_catalog.py`; its SHA256 is
`76615dd4b43859fff08ac959804e8d695f4401785bf2ddadb7beb2c1249542ca`.

The delivered core catalogs have 768 market and 1,536 asset rows. The diagnostic
catalogs have five market and ten asset rows. Data-manifest catalog hashes were
updated after the completed causal clock correction. Event tables and original
cache files were not changed by the catalog reconstruction.

PendulumFlow V3 is explicitly licensed under
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) in the archive's
[license notice](https://archive.pendulumflow.com/LICENSE.txt). The archive
requests collector credit, the era, access date and exact source checksums; see
its [citation guidance](https://archive.pendulumflow.com/cite). This package
uses V3, rather than the separately mirrored third-party eras.

Suggested attribution: pendulumflow. Polymarket Orderbook Archive, V3.
https://archive.pendulumflow.com, accessed 2026-10-03. CC BY 4.0.
This package filters BTC5/BTC15 contracts, converts units and schemas,
constructs replay checkpoints, rebuilds catalog identities and re-encodes
Parquet. Source receipt clocks represent the earliest merged collector copy
per row. The collector does not endorse this package, and the source data is
provided without warranty. See the package data license and clock provenance
for the remaining transformations and timing limits.
