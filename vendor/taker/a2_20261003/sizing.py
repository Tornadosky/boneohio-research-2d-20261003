#!/usr/bin/env python3
"""tailtaker_20261003_A2PARITY step 12: A2 share-size study on the parity-validated engine (pendulumflow 08-18 -> 10-02 13:50).

Engine corrections found by the parity work (vs tailtaker_20261001_FINAL):
  strike  = live convention (close of wall second `slot`) on the cache feed, z recomputed on every 2 s row
  A2      = deployed model with the box-style book age (ms since the last top-of-book change, 10 ms feed lag)
  gate    = live presend gates are outcome-neutral (perm p 0.35) -> random thinning at the live block rate (34 %), 20 seeds
  fill    = spend-sized FAK BUY favourite at 1 - tail bid vs the favourite-ask ladder at decide + 200 ms (measured p50 203-205 ms)
Sizing = live cohort_B rule: principal = base_shares x limit x premium x thick x A2, $1 floor, cap = cap_shares (default 4 x base).
Own-consumption: 'cons' subtracts our earlier fills in the same contract from the same price levels (no replenishment);
'free' assumes the book refilled within 8 s.
"""
import os
for _k in ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS'):
    os.environ[_k] = '1'
import json, sys, time
from multiprocessing import get_context
from pathlib import Path
from types import SimpleNamespace
import numpy as np, pandas as pd, pyarrow as pa, pyarrow.parquet as pq
sys.path.insert(0, '<SOURCE_HOME>/tailtaker_wf_20260922/wfa_release')
from tailtaker_wfa.a2_model import A2Model, physics_features  # noqa: E402
O = Path('<SOURCE_DATA>/exp/tailtaker_20261003_A2PARITY'); OUT = O / 'analysis'
RAW = Path('<SOURCE_DATA>/polycache_btc_twap/cache_btc_pflow_v1')
MODEL = A2Model('<SOURCE_DATA>/tailtaker_wf_20260922/v1_deploy_20260923/fp/a2_live/a2_live_model.json')
G = {}


def top_events(cs):
    fs = sorted((RAW / 'normalized_depth_events' / 'market_key=btc_5m' / f'contract_start_ms={cs}').rglob('*.parquet'))
    if not fs:
        return None
    cols = ['venue_ts_ms', 'seq', 'is_yes', 'event_type', 'side', 'price_micros', 'best_bid_micros', 'best_ask_micros']
    t = pa.concat_tables([pq.ParquetFile(f).read(columns=cols) for f in fs])
    d = {c: (t[c].cast(t[c].type.value_type) if pa.types.is_dictionary(t[c].type) else t[c]).to_numpy(zero_copy_only=False) for c in cols}
    o = np.lexsort((d['seq'], d['venue_ts_ms'])); d = {k: v[o] for k, v in d.items()}
    v = d['venue_ts_ms']; typ = np.asarray(d['event_type'], dtype=str); side = np.asarray(d['side'], dtype=str)
    px = d['price_micros']; bb = d['best_bid_micros']; ba = d['best_ask_micros']
    top = (typ == 'snapshot') | ((side == 'bid') & (px == bb)) | ((side == 'ask') & (px == ba))
    last = {}
    for i in range(len(v)):
        y = bool(d['is_yes'][i]); k = (int(bb[i]), int(ba[i]))
        if last.get(y) != k:
            top[i] = True; last[y] = k
    return v[top]


def feats(cs):
    """per contract: corrected candidate rows (trade ticks, 60-180 s, 2-5 c, z_conv >= 3) with A2 mult, ladder and outcome."""
    P = G['P']; a, b = G['idx'][cs]; d = P.iloc[a:b]
    slot = cs // 1000
    K = G['mid'].get(slot + 1); s = G['src'].get(slot + 1)
    if K is None or s // 1000 != slot:
        return []
    ring = G['ring']; top = top_events(cs)
    q = {True: {}, False: {}}; fq = {}; out = []
    for r in d.itertuples():
        dist = r.btc - K
        if dist == 0: continue
        y = dist < 0                                  # tail = yes token when BTC is below the strike
        if y != bool(r.is_yes): continue             # side flipped vs the pool row: no event views for that side (z ~ 0 anyway)
        p = round(float(r.dl10_tbid), 4) if np.isfinite(r.dl10_tbid) else float(r.d_bid)
        z = abs(dist) / (r.sigma_1s * np.sqrt(max(r.tte, 1.)))
        q[y][int(r.decide_ms)] = p
        if y not in fq and .02 - 2e-6 <= p <= .08 + 2e-6 and z >= 3: fq[y] = float(r.tte)
        el = (int(r.decide_ms) - cs) // 1000
        if (el - 10) % 8 or not (60 <= r.tte <= 180) or not (.02 - 1e-7 <= p <= .05 + 1e-7) or z < 3: continue
        if not (np.isfinite(r.a200_px0)): continue
        age = 0.
        if top is not None and len(top):
            k = np.searchsorted(top, int(r.decide_ms) - 10, side='right') - 1
            age = float(int(r.decide_ms) - (top[k] + 10)) if k >= 0 else 0.
        ft = dict(slot=slot, scheduled_ms=int(r.decide_ms), tte=float(r.tte), tail_bid=p, tail_ask=float(r.d_ask), tail_bid_size=float(r.dl10_tbsz)
                  if np.isfinite(r.dl10_tbsz) else float(r.d_bid_size), tail_ask_size=float(r.d_ask_size), fq_tte=fq.get(y),
                  quotes={k2: v for k2, v in q[y].items()}, book_age_ms=max(age, 0.))
        phys = physics_features(ring, int(r.decide_ms) // 1000, K, float(r.tte), slot, 60)
        _, ev, _ = MODEL.ev_from(phys, ft)
        am = MODEL.mults[0] if ev < MODEL.cuts[0] else (MODEL.mults[1] if ev < MODEL.cuts[1] else MODEL.mults[2])
        pm = min(2., max(.25, (p - .0255) / .01453)); tm = 1.5 if (fq.get(y) is not None and fq[y] >= 120) else 1.
        lad = [(round(float(getattr(r, f'a200_px{j}')), 4), float(getattr(r, f'a200_sz{j}'))) for j in range(4)]
        out.append(dict(cs=cs, ts=int(r.decide_ms), tte=float(r.tte), p=p, z=z, mult=pm * tm * am, a2=am, ev=ev, thick=tm,
                        won_tail=float(r.won), lad=json.dumps(lad)))
    return out


def main():
    t0 = time.time()
    P = pd.read_parquet(O / 'pools/btc_5m__pflow__venue_causal.parquet',
                        columns=['contract_start_ms', 'decide_ms', 'is_yes', 'tte', 'btc', 'sigma_1s', 'd_bid', 'd_ask', 'd_bid_size', 'd_ask_size', 'won'])
    P = P[P.contract_start_ms >= int(pd.Timestamp('2026-08-20', tz='UTC').value // 10**6)]
    V = pd.read_parquet(O / 'exact/exact_views.parquet', columns=['contract_start_ms', 'decide_ms', 'is_yes', 'dl10_tbid', 'dl10_tbsz'] +
                        [f'a200_{x}{k}' for x in ('px', 'sz') for k in range(4)])
    P = P.merge(V, on=['contract_start_ms', 'decide_ms', 'is_yes'], how='left').sort_values(['contract_start_ms', 'decide_ms']).reset_index(drop=True)
    u, s, n = np.unique(P.contract_start_ms.to_numpy(np.int64), return_index=True, return_counts=True)
    G['P'] = P; G['idx'] = {int(c): (int(x), int(x + k)) for c, x, k in zip(u, s, n)}
    M = pd.read_parquet(O / 'pools/btc_mid_1hz_causal_long.parquet', columns=['ts_s', 'mid', 'source_ts_ms'])
    G['mid'] = dict(zip(M.ts_s.to_numpy(np.int64), M.mid.to_numpy(float)))
    G['src'] = dict(zip(M.ts_s.to_numpy(np.int64), M.source_ts_ms.to_numpy(np.int64)))
    G['ring'] = SimpleNamespace(by_visible={int(t): SimpleNamespace(mid=float(m)) for t, m in G['mid'].items()})
    rows = []
    with get_context('fork').Pool(120) as pool:
        for k, r in enumerate(pool.imap_unordered(feats, list(G['idx']), chunksize=4), 1):
            rows += r
            if k % 2000 == 0: print('PROGRESS', k, len(G['idx']), len(rows), round(time.time() - t0), flush=True)
    C = pd.DataFrame(rows).sort_values(['cs', 'ts']).reset_index(drop=True)
    C.to_parquet(OUT / 'sizing_candidates.parquet', index=False)
    print('CANDIDATES', len(C), C.cs.nunique(), round(time.time() - t0), flush=True)


if __name__ == '__main__':
    main()
