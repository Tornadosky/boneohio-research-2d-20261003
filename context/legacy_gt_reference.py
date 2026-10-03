# HISTORICAL REFERENCE ONLY. Source semantics are superseded by the guarded matcher.
# The legacy implementation assigns transaction-earliest prints without token ambiguity checks
# and infers m=print-40; neither operation observes submission/matching time.
# Private source roots are replaced by SOURCE_* placeholders; do not execute this file.

#!/usr/bin/env python3
"""Ground truth for BoneSim parity: every BoneOhio fill since 09-10 on btc/eth/sol 5m/15m joined to the (extended) pflow tape by tx hash;
one row per (tx, token) = one order event: role, avg price, shares, pnl, is_yes, contract, first print venue ms, book-change time m=print-40.
Also per-tx print prices (for sweeps). -> gt_orders.parquet"""
import glob, numpy as np, pandas as pd, pyarrow.parquet as pq
from multiprocessing import Pool
E0 = 'SOURCE_ENTRY/'
P = 'SOURCE_PARITY/'
CACHE = {c: f'SOURCE_POLYCACHE_{c}_twap/cache_{c}_pflow_v1' for c in ('btc', 'eth', 'sol')}
COLS = ['venue_ts_ms', 'token_id', 'is_yes', 'price_micros', 'size_micros', 'side', 'transaction_hash']


def one(a):
    (c, mk, cs), txs = a
    fs = glob.glob(f'{CACHE[c]}/trade_tape/market_key={mk}/contract_start_ms={cs}/*/*.parquet')
    if not fs:
        return None
    t = pd.concat([pq.ParquetFile(f).read(columns=COLS).to_pandas() for f in fs])
    t['transaction_hash'] = t.transaction_hash.str.lower()
    return t[t.transaction_hash.isin(txs)]


if __name__ == '__main__':
    f = pd.read_parquet(E0 + 'fills_pnl.parquet')
    f = f[(f.time_utc >= '2026-09-10') & f.coin.isin(['btc', 'eth', 'sol']) & f.tf.isin(['5m', '15m'])].copy()
    f['transaction_hash'] = f.transaction_hash.str.lower()
    am = []
    for c, p in CACHE.items():
        a = pd.read_parquet(f'{p}/catalog/assets.parquet')[['token_id', 'market_key', 'contract_start_ms', 'contract_end_ms', 'is_yes']]
        am.append(a)
    am = pd.concat(am).drop_duplicates('token_id')
    f = f.merge(am, on='token_id', how='inner')
    jobs = [((g.coin.iloc[0], mk, int(cs)), set(g.transaction_hash)) for (mk, cs), g in f.groupby(['market_key', 'contract_start_ms'])]
    with Pool(8) as p:
        tp = pd.concat([x for x in p.map(one, jobs, chunksize=8) if x is not None and len(x)])
    tv = tp.groupby('transaction_hash').venue_ts_ms.min()
    f['print_ms'] = f.transaction_hash.map(tv)
    g = f.groupby(['transaction_hash', 'token_id']).agg(role=('role', 'first'), coin=('coin', 'first'), mk=('market_key', 'first'),
                                                        cs=('contract_start_ms', 'first'), ce=('contract_end_ms', 'first'), is_yes=('is_yes', 'first'),
                                                        sh=('size', 'sum'), usd=('usd', 'sum'), pnl=('pnl', 'sum'), won=('won', 'first'),
                                                        print_ms=('print_ms', 'first'), ts=('timestamp', 'first')).reset_index()
    g['px'] = g.usd / g.sh
    g['m'] = g.print_ms - 40
    g['grp'] = np.where(g.px < .95, 'low', np.where(g.px >= .97, 'hi', 'mid'))
    g.to_parquet(P + 'gt_orders.parquet', index=False)
    tp.to_parquet(P + 'gt_prints.parquet', index=False)
    x = g[g.role == 'TAKER']
    print('orders', len(g), 'taker', len(x), 'with print', x.print_ms.notna().mean().round(4), 'last print', pd.to_datetime(x.print_ms.max(), unit='ms'))
    print(x.groupby([pd.to_datetime(x.ts, unit='s').dt.strftime('%m-%d'), 'grp']).size().unstack().tail(4))
