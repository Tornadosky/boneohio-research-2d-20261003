#!/usr/bin/env python3
"""tailtaker_20261003_A2PARITY step 9: free-running end-to-end backtest of A2 with live-faithful inputs vs every live order.

Engine = strategy_wfa.evaluate re-implemented sample by sample (2 s samples from elapsed 10 s, trade ticks 10 + 8k s):
  BTC     : source recorder bookTicker 1 Hz closes on the box clock (Binance E + 120 ms); strike = close of wall second `slot`;
            sigma over 61 contiguous closes; BTC age <= 3 s
  book    : event-level venue book at decide - 10 ms (exact_views dl10, tail side); other side from the 100 ms pool row
  A2      : deployed a2_model.py on the bookTicker ring, market fields from the replayed book, 2 s quote history
  sizing  : per box (cohort_B shares: base 5 x limit x mult, cap 20 sh; cohort_A: $5 x mult, cap $20), $1 floor, 4 intents
  gate    : binance-md replay: blocked when (send time - newest bookTicker E visible at the box) > 200 ms
  fill    : spend-sized FAK BUY favourite at 1 - tail bid vs the favourite-ask ladder at decide + 200 ms; fee 0.07 p(1-p)
Window: bookTicker coverage + 1 h A2 warm-up -> pendulumflow cache end, and after each box's own warm-up.
"""
import json, sys
from pathlib import Path
from types import SimpleNamespace
import numpy as np, pandas as pd
sys.path.insert(0, '<SOURCE_HOME>/tailtaker_wf_20260922/wfa_release')
from tailtaker_wfa.a2_model import A2Model, physics_features  # noqa: E402
O = Path('<SOURCE_DATA>/exp/tailtaker_20261003_A2PARITY'); OUT = O / 'analysis'
MODEL = A2Model('<SOURCE_DATA>/tailtaker_wf_20260922/v1_deploy_20260923/fp/a2_live/a2_live_model.json')
POOL_END = json.loads((O / 'pools/btc_5m__pflow__venue_manifest.json').read_text())['last_contract_ms'] + 300_000
M = pd.read_parquet(O / 'pools/bookticker_1hz_source_recorder.parquet')
BY = dict(zip(M.ts_s.to_numpy(np.int64), M.mid.to_numpy(float)))
SRC = dict(zip(M.ts_s.to_numpy(np.int64), M.source_ts_ms.to_numpy(np.int64)))
RING = SimpleNamespace(by_visible={int(t): SimpleNamespace(mid=float(m)) for t, m in BY.items()})
EV = np.sort(pd.read_parquet(O / 'pools/bookticker_btc_source_recorder.parquet', columns=['venue_ts_ms']).venue_ts_ms.to_numpy(np.int64))
T0 = (int(M.ts_s.min()) + 3700) * 1000
ST = pd.read_parquet(O / 'inputs/live_starts_both.parquet')
starts = {b: np.sort(g.ts_ns.astype(np.int64).to_numpy() // 1_000_000) for b, g in ST.groupby('box')}
LO = {b: max(T0, int(s[-1]) + 3_700_000) if s[-1] < POOL_END else T0 for b, s in starts.items()}
LO['cohort_A'] = max(T0, int(starts['cohort_A'][starts['cohort_A'] < T0][-1]) + 3_700_000)
SIZE = {'cohort_B': dict(base_shares=5., cap_shares=20.), 'cohort_A': dict(base_usd=5., cap_usd=20.)}
lo_all = min(LO.values())
P = pd.read_parquet(O / 'pools/btc_5m__pflow__venue_causal.parquet',
                    columns=['contract_start_ms', 'decide_ms', 'is_yes', 'd_bid', 'd_ask', 'd_bid_size', 'd_ask_size', 'c_bid', 'c_ask',
                             'book_age_ms', 'won'])
P = P[(P.contract_start_ms >= lo_all - 300_000) & (P.contract_start_ms + 300_000 <= POOL_END)]
V = pd.read_parquet(O / 'exact/exact_views.parquet')
V = V[(V.contract_start_ms >= lo_all - 300_000)]
keep = ['contract_start_ms', 'decide_ms', 'is_yes', 'dl10_tbid', 'dl10_task', 'dl10_tbsz', 'dl10_fask'] + [f'a200_{x}{k}' for x in ('px', 'sz') for k in range(4)]
P = P.merge(V[keep], on=['contract_start_ms', 'decide_ms', 'is_yes'], how='left')
WON = {}
for r in P.drop_duplicates('contract_start_ms').itertuples():
    WON[int(r.contract_start_ms)] = float(r.won) if r.is_yes else 1. - float(r.won)     # UP won


import pyarrow as pa, pyarrow.parquet as pq
from multiprocessing import get_context
RAW = Path('<SOURCE_DATA>/polycache_btc_twap/cache_btc_pflow_v1')


def top_events(cs):
    """venue times of top-of-book changes (BBO price, or a level update at the best price, either token) = box PBBO publishes."""
    fs = sorted((RAW / 'normalized_depth_events' / 'market_key=btc_5m' / f'contract_start_ms={cs}').rglob('*.parquet'))
    if not fs:
        return cs, None
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
    return cs, v[top]


def top_age(cs, t, lag=10):
    v = TOP.get(int(cs))
    if v is None or not len(v):
        return np.nan
    k = np.searchsorted(v, t - lag, side='right') - 1
    return float(t - (v[k] + lag)) if k >= 0 else np.nan


def close_visible(t_sec):
    k = t_sec
    while k not in BY and k > t_sec - 5: k -= 1
    return (k, BY[k]) if k in BY else (None, None)


def strike_of(slot):
    v = BY.get(slot + 1); s = SRC.get(slot + 1)
    return v if (v is not None and s // 1000 == slot) else None


def run(box):
    sz = SIZE[box]; orders = []
    for cs, g in P[P.contract_start_ms >= LO[box] - 300_000].groupby('contract_start_ms'):
        slot = cs // 1000; K = strike_of(slot)
        rows = {int(r.decide_ms): r for r in g.itertuples()}
        attempts = 0; used_usd = 0.; used_sh = 0.; fq = {}; quotes = {'UP': {}, 'DOWN': {}}
        for el in range(10, 296, 2):
            sched = cs + el * 1000
            tte = 300. - el
            if K is None or sched < LO[box]: continue
            k, cur = close_visible(sched // 1000)
            if cur is None or any((k - j) not in BY for j in range(61)): continue
            if sched - SRC[k] > 3000 or sched - SRC[k] < 0: continue
            mm = np.array([BY[k - 60 + j] for j in range(61)]); d = np.diff(mm)
            sig = float(np.sqrt(((d - d.mean()) ** 2).mean()))
            if not np.isfinite(sig) or sig <= 0: continue
            dist = cur - K
            if dist == 0: continue
            tail = 'UP' if dist < 0 else 'DOWN'; fav = 'DOWN' if tail == 'UP' else 'UP'
            r = rows.get(sched)
            if r is None: continue
            pool_tail = 'UP' if r.is_yes else 'DOWN'
            if tail == pool_tail:
                p = round(float(r.dl10_tbid), 4) if np.isfinite(r.dl10_tbid) else float(r.d_bid)
                dask = round(float(r.dl10_task), 4) if np.isfinite(r.dl10_task) else float(r.d_ask)
                bsz = float(r.dl10_tbsz) if np.isfinite(r.dl10_tbsz) else float(r.d_bid_size); asz = float(r.d_ask_size)
                favask = round(float(r.dl10_fask), 4) if np.isfinite(r.dl10_fask) else float(r.c_ask)
                ladder = [(round(float(getattr(r, f'a200_px{j}')), 4), float(getattr(r, f'a200_sz{j}'))) for j in range(4)]
            else:      # flipped side vs the pool row: other token from the 100 ms grid, no event ladder
                p = float(r.c_bid); dask = float(r.c_ask); bsz = asz = np.nan; favask = float(r.d_ask); ladder = None
            age = top_age(cs, sched)
            age = float(age) if np.isfinite(age) else float(max(r.book_age_ms, 0))
            if not (0 < p < dask <= 1. and .005 <= p <= .30): continue
            z = abs(dist) / (sig * np.sqrt(max(tte, 1.)))
            quotes[tail][sched] = p
            if tail not in fq and .02 - 2e-6 <= p <= .08 + 2e-6 and z >= 3: fq[tail] = tte
            if (el - 10) % 8: continue
            if attempts >= 4 or not (60 <= tte <= 180) or not (.02 - 1e-7 <= p <= .05 + 1e-7) or z < 3: continue
            limit = round(1. - p, 4)
            room = (sz['cap_shares'] - used_sh) * limit if 'base_shares' in sz else sz['cap_usd'] - used_usd
            if room < 1. - 1e-9: continue
            pm = min(2., max(.25, (p - .0255) / .01453)); fqt = fq.get(tail); tm = 1.5 if (fqt is not None and fqt >= 120) else 1.
            ft = dict(slot=slot, scheduled_ms=sched, tte=tte, tail_bid=p, tail_ask=dask, tail_bid_size=bsz, tail_ask_size=asz,
                      fq_tte=fqt, quotes=quotes[tail], book_age_ms=age)
            phys = physics_features(RING, sched // 1000, K, tte, slot, 60)
            _, ev, _ = MODEL.ev_from(phys, ft)
            am = MODEL.mults[0] if ev < MODEL.cuts[0] else (MODEL.mults[1] if ev < MODEL.cuts[1] else MODEL.mults[2])
            base = sz['base_shares'] * limit if 'base_shares' in sz else sz['base_usd']
            principal = round(min(max(1., base * pm * tm * am), room), 2)
            if principal < 1. - 1e-9 or favask <= 0: continue
            attempts += 1
            j = np.searchsorted(EV, sched + 1 - 120, side='right') - 1
            gate_age = sched + 1 - EV[max(j, 0)]
            o = dict(box=box, cs=cs, sched=sched, tail_side=tail, side=fav, tail_bid=p, z=z, sigma=sig, strike=K, btc=cur, a2_mult=am, a2_ev=ev,
                     thick=tm, premium=pm, principal=principal, limit=limit, gate_age=int(gate_age), poly_age=top_age(cs, sched + 1),
                     gate_block=bool(gate_age > 200))
            o['gate_block'] = bool(o['gate_block'] or (np.isfinite(o['poly_age']) and o['poly_age'] > 250))
            if o['gate_block'] or ladder is None:
                o.update(shares=0., cost=0.); orders.append(o); continue
            got = cost = 0.
            for a, q in ladder:
                if not (np.isfinite(a) and np.isfinite(q)) or a > limit + 1e-9 or q <= 0: break
                take = min(q, (principal - cost) / a)
                if take <= 1e-9: break
                got += take; cost += take * a
                if cost >= principal - 1e-6: break
            o.update(shares=got, cost=cost)
            if got > 1e-9:
                used_usd += principal; used_sh += principal / limit
            orders.append(o)
    return pd.DataFrame(orders)


with get_context('fork').Pool(8) as _p:
    TOP = dict(_p.imap_unordered(top_events, sorted(P.contract_start_ms.unique()), chunksize=8))
B = pd.concat([run(b) for b in ('cohort_B', 'cohort_A')], ignore_index=True)
B['fav_won'] = [WON.get(int(c), np.nan) if s == 'UP' else 1 - WON.get(int(c), np.nan) for c, s in zip(B.cs, B.side)]
px = B.cost / B.shares.where(B.shares > 0)
B['fee'] = (.07 * px * (1 - px) * B.shares).fillna(0.)
B['pnl'] = (B.shares * B.fav_won - B.cost - B.fee).fillna(0.)
L = pd.read_parquet(O / 'inputs/live_ticks_both.parquet')
for c in ('slot', 'scheduled_ms', 'taking', 'making', 'principal_usd', 'a2_mult', 'z', 'tail_bid'):
    L[c] = pd.to_numeric(L[c], errors='coerce')
L = L[L.event.str.startswith('ORDER_') | (L.event == 'AUDIT_BLOCK')].copy()
L['cs'] = L.slot.astype(np.int64) * 1000; L['sched'] = L.scheduled_ms.astype(np.int64)
L = L[[(t >= LO[b]) and (c + 300_000 <= POOL_END) for b, t, c in zip(L.box, L.sched, L.cs)]]
L['px'] = L.making / L.taking.where(L.taking > 0)
L['fav_won'] = [WON.get(int(c), np.nan) if s == 'UP' else 1 - WON.get(int(c), np.nan) for c, s in zip(L.cs, L.side)]
L['pnl_live'] = (L.taking * L.fav_won - L.making - .07 * L.px * (1 - L.px) * L.taking).fillna(0.)
J = B.merge(L[['box', 'cs', 'sched', 'event', 'reason', 'taking', 'making', 'principal_usd', 'a2_mult', 'pnl_live']], on=['box', 'cs', 'sched'],
            how='outer', suffixes=('', '_live'), indicator=True)
R = {'window_ms': LO, 'pool_end_ms': POOL_END}
for b, g in J.groupby('box'):
    both = g[g._merge == 'both']
    sent_live = both.event.str.startswith('ORDER_'); sent_bt = ~both.gate_block.astype(bool)
    R[b] = dict(intents_bt=int((g._merge != 'right_only').sum()), intents_live=int((g._merge != 'left_only').sum()), intents_both=int(len(both)),
                bt_only=int((g._merge == 'left_only').sum()), live_only=int((g._merge == 'right_only').sum()),
                principal_eq=float((np.abs(both.principal - both.principal_usd) < .005).mean()) if len(both) else None,
                a2_mult_eq=float((both.a2_mult == both.a2_mult_live).mean()) if len(both) else None,
                gate_agree=float((sent_live == sent_bt).mean()) if len(both) else None,
                sent_both=int((sent_live & sent_bt).sum()),
                fill_agree_on_sent_both=float(((both.taking.fillna(0) > 0) == (both.shares > 0))[sent_live & sent_bt].mean()) if (sent_live & sent_bt).any() else None,
                shares_live=float(g.taking.fillna(0).sum()), shares_bt=float(g.shares.fillna(0).sum()),
                pnl_live=float(g.pnl_live.fillna(0).sum()), pnl_bt=float(g.pnl.fillna(0).sum()),
                contracts_live=int(g[g.taking.fillna(0) > 0].cs.nunique()), contracts_bt=int(g[g.shares.fillna(0) > 0].cs.nunique()))
    print(b, json.dumps(R[b], indent=1), flush=True)
    x = g[(g._merge != 'both') | ((g._merge == 'both') & ((g.event.str.startswith('ORDER_')) == g.gate_block.astype(bool)))]
    print(x[['cs', 'sched', '_merge', 'event', 'reason', 'gate_age', 'poly_age', 'gate_block', 'principal', 'principal_usd', 'a2_mult', 'a2_mult_live', 'z', 'tail_bid', 'shares', 'taking', 'pnl', 'pnl_live']].to_string(), flush=True)
J.to_parquet(OUT / 'e2e_orders.parquet', index=False)
(OUT / 'E2E.json').write_text(json.dumps(R, indent=1, default=float))
