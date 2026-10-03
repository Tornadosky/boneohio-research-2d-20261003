#!/usr/bin/env python3
"""Rebuild a bookTicker-like top-of-book stream from cryptohftdata Binance USD-M BTCUSDT L2 (hourly snapshot + every diff, ~26 ms).
Per diff message (final_update_id) after applying all its levels: emit (E, T, recv_ns, bid, ask, bid_qty, ask_qty) when the top changed.
Output: chd/top/<hour>.parquet, then chd/top_all.parquet."""
import os
for _k in ('OMP_NUM_THREADS', 'NUMBA_NUM_THREADS'):
    os.environ[_k] = '1'
import sys, time
from multiprocessing import get_context
from pathlib import Path
import numpy as np, pandas as pd, pyarrow.parquet as pq
from numba import njit
D = Path('<SOURCE_DATA>/exp/tailtaker_20261003_A2PARITY/chd'); IN = D / 'orderbook'; OUT = D / 'top'; OUT.mkdir(exist_ok=True)
NT = 4_000_000          # price ticks of 0.1 USD -> up to $400k


@njit(cache=True)
def replay(msg_start, side, tick, qty, E, T, R):
    bq = np.zeros(NT); aq = np.zeros(NT)
    nmsg = len(msg_start) - 1
    oE = np.empty(nmsg, np.int64); oT = np.empty(nmsg, np.int64); oR = np.empty(nmsg, np.int64)
    ob = np.empty(nmsg, np.int64); oa = np.empty(nmsg, np.int64); obq = np.empty(nmsg); oaq = np.empty(nmsg)
    best_b = -1; best_a = NT; k = 0
    lb = -2; la = -2; lbq = -1.0; laq = -1.0
    for m in range(nmsg):
        for i in range(msg_start[m], msg_start[m + 1]):
            t = tick[i]
            if t <= 0 or t >= NT: continue
            if side[i] == 0:
                bq[t] = qty[i]
                if qty[i] > 0 and t > best_b: best_b = t
                elif qty[i] <= 0 and t == best_b:
                    while best_b > 0 and bq[best_b] <= 0: best_b -= 1
            else:
                aq[t] = qty[i]
                if qty[i] > 0 and t < best_a: best_a = t
                elif qty[i] <= 0 and t == best_a:
                    while best_a < NT - 1 and aq[best_a] <= 0: best_a += 1
        if best_b <= 0 or best_a >= NT - 1: continue
        if best_b != lb or best_a != la or bq[best_b] != lbq or aq[best_a] != laq:
            lb = best_b; la = best_a; lbq = bq[best_b]; laq = aq[best_a]
            oE[k] = E[msg_start[m]]; oT[k] = T[msg_start[m]]; oR[k] = R[msg_start[m]]; ob[k] = lb; oa[k] = la; obq[k] = lbq; oaq[k] = laq; k += 1
    return oE[:k], oT[:k], oR[:k], ob[:k], oa[:k], obq[:k], oaq[:k]


def work(f):
    dst = OUT / f.name
    if dst.exists(): return f.name, 'skip'
    try:
        src = f
        with open(f, 'rb') as fh:
            if fh.read(4) == bytes([0x28, 0xB5, 0x2F, 0xFD]):          # early files: whole parquet wrapped in zstd
                import pyarrow as pa
                src = pa.BufferReader(pa.input_stream(str(f), compression='zstd').read())
        d = pq.read_table(src, columns=['received_time', 'event_time', 'transaction_time', 'event_type', 'final_update_id', 'side', 'price', 'quantity']).to_pandas()
        d['snap'] = (d.event_type == 'snapshot').astype(np.int8)
        d = d.sort_values(['final_update_id', 'snap'], ascending=[True, False], kind='stable').reset_index(drop=True)
        snap = d.snap.to_numpy() == 1
        # message boundaries: the snapshot block is one message, then one per final_update_id
        key = np.where(snap, -1, d.final_update_id.to_numpy(np.int64))
        o = np.argsort(np.where(snap, 0, 1), kind='stable'); d = d.iloc[o].reset_index(drop=True); key = key[o]
        starts = np.r_[0, np.flatnonzero(key[1:] != key[:-1]) + 1, len(key)].astype(np.int64)
        tick = np.rint(pd.to_numeric(d.price).to_numpy() * 10).astype(np.int64)
        qty = pd.to_numeric(d.quantity).to_numpy(float)
        side = (d.side.to_numpy() != 'bid').astype(np.int8)
        E = d.event_time.to_numpy(np.int64); T = d.transaction_time.fillna(d.event_time).to_numpy().astype(np.int64)
        R = d.received_time.to_numpy(np.int64)
        oE, oT, oR, ob, oa, obq, oaq = replay(starts, side, tick, qty, E, T, R)
        pd.DataFrame({'E': oE, 'T': oT, 'recv_ns': oR, 'bid': ob / 10, 'ask': oa / 10, 'bid_qty': obq, 'ask_qty': oaq}).to_parquet(dst, index=False)
        return f.name, len(oE)
    except Exception as e:
        return f.name, 'ERR ' + repr(e)[:200]


if __name__ == '__main__':
    t0 = time.time()
    fs = sorted(IN.glob('*.parquet'))
    with get_context('fork').Pool(int(sys.argv[1]) if len(sys.argv) > 1 else 100) as p:
        for i, (n, r) in enumerate(p.imap_unordered(work, fs), 1):
            if isinstance(r, str) and r.startswith('ERR'): print(n, r, flush=True)
            if i % 100 == 0: print('PROGRESS', i, len(fs), round(time.time() - t0), flush=True)
    parts = [pd.read_parquet(f) for f in sorted(OUT.glob('*.parquet'))]
    A = pd.concat(parts, ignore_index=True).sort_values(['E', 'recv_ns'], kind='stable')
    A = A[A.ask > A.bid]
    A.to_parquet(D / 'top_all.parquet', index=False)
    print('TOP_DONE rows', len(A), 'hours', len(parts), pd.to_datetime(A.E.min(), unit='ms'), pd.to_datetime(A.E.max(), unit='ms'), round(time.time() - t0), flush=True)
