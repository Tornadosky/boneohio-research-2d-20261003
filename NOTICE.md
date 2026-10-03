# Data attribution and distribution boundaries

The Polymarket event data and its normalized/sampled derivatives use the
**V3** collection era of the **pendulumflow Polymarket Orderbook Archive**:
[archive.pendulumflow.com](https://archive.pendulumflow.com), accessed
2026-10-03, licensed under
[Creative Commons Attribution 4.0](https://creativecommons.org/licenses/by/4.0/).
See the archive's [licence](https://archive.pendulumflow.com/LICENSE.txt) and
[citation instructions](https://archive.pendulumflow.com/cite).

Changes made here: selected BTC 5m/15m contracts; converted native event
fields into the documented cache schema; retained original clocks and integer
price/size units; lossless ZSTD Parquet rewrites; strictly sampled 100ms feature
states; explicit nulls for unavailable prior observations; reconstructed
public catalog identities from independently licensed lifecycle metadata.
Source and output hashes identify the actual bytes. This is an independent
research bundle, with no endorsement by pendulumflow or Polymarket.

Native Binance market observations were collected by our recorder. Public
BoneOhio wallet fills/activity were queried from Polymarket's public Data API;
signed-order facts come from public Polygon transaction calldata. Reports and
execution implementations are our research outputs, with original and
sanitized distribution fingerprints. The CC BY notice above identifies the
pendulumflow data specifically; it does not relicense unrelated source code,
third-party services or their data.

CryptoHFTData Binance/Kraken tapes, reconstructed quote sequences and derived
feed tapes are **excluded** from public releases. Its
[terms](https://www.cryptohftdata.com/terms) prohibit redistribution and
repackaging without separate rights. The optional downloader fetches directly
into a private research workspace; it does not grant redistribution rights.
Telonex market-metadata files are also excluded; public catalog reconstruction
must independently verify identities against licensed pendulumflow lifecycle
messages, retaining only our generated local IDs and derived routing fields.
