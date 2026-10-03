# Fresh public fills to public print candidates

Prepared 2026-10-03. `match_fresh_venue.py` extends the historical matched-print
coverage using the released public Polymarket trade tape. The resulting links
are retrospective attribution candidates. A unique price-consistent candidate
does not certify BoneOhio's decision, submit, matching-engine or receipt time.

The completed two-day run reads 253 trade-tape files for contracts with observed
Bitcoin fills. It retains all 3,314 fresh all-coin wallet rows, maps 2,256 to the
BTC5/BTC15 catalog, and finds **1,613 direct and 635 complement candidates**.
Eight Bitcoin rows have no transaction print in their stored contract partition;
1,058 other-asset rows are explicitly outside this market-data export. No
preferred candidate is ambiguous in this particular run. The function preserves
ambiguity when applied to another interval or a tape with duplicate sessions.

The latest attributed public print is **October 1 23:59:05.894 UTC**, extending
the old October 1 19:49:06.703 cutoff by 287 candidate links. API seconds minus
candidate venue time have median 2,213ms, p90 2,957ms and range 844–3,937ms.
These are reporting differences, not reaction or submission latencies. All 635
complement candidates have a separately decoded maker role; price complementation
alone would not establish that role.

## Reproduce or extend the run

Install the package dependencies and download metadata plus the raw Polymarket
shards containing the requested contracts, using the main package downloader.
The full two-day Polymarket release permits the following run from the package
root; no external credentials or source-host access are needed:

```bash
python context/match_fresh_venue.py --output results/fresh_wallet_match
```

For a small operational check:

```bash
python context/match_fresh_venue.py --max-contracts 5 --output results/fresh_wallet_match_small
```

The bounded run explicitly leaves remaining Bitcoin contracts
`not_examined_bounded_run`. An absent downloaded partition stays
`missing_downloaded_tape_partition`. Neither becomes a negative or a zero
timestamp. To use another export, supply `--data-root` and `--wallet-root`;
the latter must contain `fresh_fills_two_days.parquet` with the current public
fill schema. Optional `fresh_order_fill_identity_two_days.parquet` attaches
decoded order identity and role. Refresh wallet inputs with the supplied
`refresh_wallet.py`, then decode/derive identity separately if needed; do not
pretend an old identity file covers new transactions.

The reusable `link_one_fill(fill, prints, price_tolerance=.01)` function accepts
one wallet record and a DataFrame of raw public prints already confined to its
catalog contract and transaction. IDs remain strings. Its rules are:

1. Preserve every same-token candidate and every opposite catalog-token
   candidate. Direct means exact public transaction hash plus token ID.
   Complement means the opposite binary outcome within that same contract,
   with price mapped as `1 - raw_print_price`.
2. Prefer direct candidates if present. Otherwise evaluate the complement
   candidates. More than one preferred candidate leaves mapped fields null,
   even when timestamps and prices coincide. Do not choose by API lag,
   favorable price, size, future outcome or edge.
3. A single candidate needs a positive venue timestamp and price consistency
   within the declared one-cent tolerance. Quantity differences remain visible;
   they are not silently reconciled or used to choose a print.
4. Preserve source file, physical row ordinal, event UID, session, sequence,
   raw token, price/size, venue time and normalized cache time. Keep the
   complement distinction in all subsequent analyses. Unknown or zero venue
   time never falls back to cache time.

`wallet/fresh_fill_venue_links.parquet` contains all wallet rows and their
statuses. `wallet/fresh_venue_print_candidates.parquet` preserves the candidate
rows. `wallet/FRESH_VENUE_PROVENANCE.json` records input/output SHA256, script
fingerprint, settings, lag distribution and the exact eight unresolved IDs.
Recomputed outputs go to the requested results directory rather than replacing
the immutable context evidence.

## The eight unresolved Bitcoin rows

The source converter retained trades from 15 minutes before contract start
through **strictly before contract end plus 60 seconds**. This is an explicit
`PRE_MS, POST_MS = 15 * 60_000, 60_000` source rule in
`vendor_20260930_PFCACHE/pfcache.py`, SHA256
`67c89e5b305548264f29bbb3576610456e569d2b8d1935c3bd267f625dbb8ac2`.
The broader clock lineage is in `PFLOW_CLOCK_PROVENANCE.json`.

Seven unresolved wallet API times are 63–107 seconds after contract end and
23.142–96.725 seconds after the last stored positive-time print in the
respective partition. The eighth API time is 40 seconds before expiry and
68.648 seconds before its partition's last print. This suggests a tail-coverage
issue for seven records and leaves one unexplained. API lag and sparse prints
mean these flags do not prove where the missing event occurred or that capture
was continuous. Keep all eight unresolved until broader raw tails or an
independent public event source establish a link.

Per-row `partition_first_venue_ms`, `partition_last_venue_ms` and
`partition_source_print_rows` use all source prints before transaction filtering.
`api_after_contract_end_ms`, `api_minus_partition_last_venue_ms` and boundary
flags are diagnostics. They are not continuous-coverage certificates or
replacement print timestamps. No new zero-fill or cancellation ledger is
created by this procedure.

## Historical matcher reference

`legacy_gt_reference.py` preserves our original
`boneohio_20261002_PARITY/gt.py` logic with private filesystem roots removed.
It is a historical source reference, not a portable launch script. Original
SHA256 is `b982cb48ab5d80312babb773b1cb6e3e49afd3cd752516dc2fbf95490f6c69b7`;
sanitized SHA256 is
`2f55e6e506174029a4b46dcf839dd08d7d0919f977d98d610ab2e2f45ee279b1`.
It grouped wallet transaction/token rows, chose an earliest public print at
transaction level and assigned `m = print_ms - 40ms`. That historical convention
can admit complement links and cannot certify exact token attribution or true
matching time. The new conservative matcher retains direct/complement and
ambiguity states and does not manufacture an `m` field.

Use print candidates to improve event-window forensics while retaining an
explicit range of plausible earlier decision times. Exclude target prints,
their affected book changes and linked order fingerprints from predictive
features. No CryptoHFTData tape, private account data or provider credential is
read or redistributed by either reference.
