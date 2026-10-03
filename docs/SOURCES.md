# Primary specifications and evidence sources

Checked **2026-10-03**. These links establish current field semantics and
protocol distinctions. They do not identify the target wallet's historical
feed, latency, geography or subscription. Archived research files retain
their own source hashes in `context/PROVENANCE.json`.

| Source | Relevant support |
|---|---|
| [Binance USD-M aggregate-trade market stream](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market) | aggTrade combines same-price/taking-side fills; `T` means trade time, `E` means event time, and aggregate trade IDs and first/last underlying IDs are distinct fields. Neither timestamp states when a researcher or wallet received the update. |
| [Binance USD-M book streams](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public) | Individual bookTicker, partial depth and diff depth are distinct streams. Diff depth carries first/final/previous update IDs; receipt-aware reconstruction must preserve the relevant chain and snapshot. Several displayed levels do not establish the complete book. |
| [Polymarket real-time market data](https://docs.polymarket.com/market-data/realtime-data) | Public book, price-change, trade, tick-size and resolution messages have different payloads. The public trade payload identifies an asset and transaction; preserve .001 tick changes, both outcomes and complement links. It does not provide an authenticated third-party order lifecycle. |
| [Polymarket real-time order updates](https://docs.polymarket.com/trading/realtime-order-updates) | Account order changes and trade updates require authentication for that account. A public fill archive cannot be treated as the target's own user stream or complete ledger of unfilled/cancelled orders. Do not request target credentials. |
| [Polymarket RTDS-to-PolyBolt migration](https://docs.polymarket.com/migrate/rtds-to-polybolt) | Currently documents `price.crypto` defaulting to Chainlink where supported, explicit provider/source handling and a separate 60-second Chainlink TWAP. This differs from archived October 2 FINAL's Pyth wording. Record the discrepancy and payload provider instead of silently relabeling historical data. |
| [Polymarket public Data API v2](https://data-api.polymarket.com/v2/docs) | Public user-scoped trades/activity use opaque cursor walks and bounded time windows. The refresh requests inclusive `end=Oct2-1 second`, then client-filters the strict two-day interval. Exhausted pagination and service freshness are documented, while private order lifecycle is still unavailable. |

Distribution additionally excludes CryptoHFTData raw and reconstructed tapes
under its source [terms](https://cryptohftdata.com/terms). The context library
contains aggregate historical native-recorder feed comparisons and public
Polymarket wallet records, without exported CHD price/size sequences. Optional
vendor acquisition must respect the recipient's own access/terms. This does
not certify redistribution rights for an unrelated data source.

Receipt causality is a modeling inference from these separate venue fields
and our measured arrival data: a signal using an update before the observer
could receive it is retrospective. Our arrival clock still does not equal
the target's arrival clock. Geography inferred by subtracting a hypothetical
hold from a completed price move is unsupported.

Research sources and their authority, in order:

1. `context/historical/FINAL_RESULT.md`: October 2 corrections and bounded
   maker/selection/feed/taker conclusions.
2. `SELECTION_RESULT.md`, `TAKER_RESULT.md`, `MAKER_RUNBOOK.md` plus their
   complete exported negative metrics, methodology and source hashes.
3. `context/wallet/` public fields, exact decoded chain links and explicitly
   inferred historical mappings, with cutoff masks from provenance.
4. Earlier ENTRY/PARITY are fingerprinted predecessors. Their stronger
   signing/feed/geography and phantom-depth claims are superseded or
   qualified by FINAL; their low replication F1 remains useful negative
   evidence.

Cloud experiments should cite exact package files, hashes, columns,
coverage, parameters and engine revisions. Live specification pages can
change; this dated review must not overwrite immutable report history.
