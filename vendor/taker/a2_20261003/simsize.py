#!/usr/bin/env python3
"""tailtaker_20261003_A2PARITY step 13: A2 share-size sweep on the corrected candidates (sizing.py output)."""
import json
from pathlib import Path
import numpy as np, pandas as pd
O = Path('<SOURCE_DATA>/exp/tailtaker_20261003_A2PARITY'); OUT = O / 'analysis'
C = pd.read_parquet(OUT / 'sizing_candidates.parquet').sort_values(['cs', 'ts']).reset_index(drop=True)
C['lad'] = C.lad.map(json.loads)
T = lambda s: int(pd.Timestamp(s, tz='UTC').value // 10**6)
LO = T('2026-08-21'); END = int(C.cs.max()) + 300_000
PERIODS = {'all_0821': (LO, END), 'hold150_0904': (T('2026-09-04 14:00'), END), 'oos_0919': (T('2026-09-19'), END), 'live_0923': (T('2026-09-23 13:50'), END)}
C = C[C.cs >= LO]
GATE_P = 0.34
groups = [(int(cs), g.to_dict('records')) for cs, g in C.groupby('cs')]


def sim(base, cap, gate_rng=None, cons=True, max_int=4):
    out = []
    for cs, rows in groups:
        att = 0; used = 0.; taken = {}
        for r in rows:
            if att >= max_int: break
            limit = round(1. - r['p'], 4)
            room = (cap - used) * limit
            if room < 1. - 1e-9: continue
            principal = round(min(max(1., base * limit * r['mult']), room), 2)
            att += 1
            if gate_rng is not None and gate_rng.random() < GATE_P:
                continue
            got = cost = 0.
            for px, sz in r['lad']:
                if not (np.isfinite(px) and np.isfinite(sz)) or px > limit + 1e-9 or sz <= 0: break
                avail = sz - (taken.get(px, 0.) if cons else 0.)
                if avail <= 1e-9: continue
                take = min(avail, (principal - cost) / px)
                if take <= 1e-9: break
                got += take; cost += take * px; taken[px] = taken.get(px, 0.) + take
                if cost >= principal - 1e-6: break
            if got > 1e-9:
                used += principal / limit
                pr = cost / got
                out.append((cs, r['ts'], got, cost, got * (1. - r['won_tail']) - cost - .07 * pr * (1 - pr) * got))
    return pd.DataFrame(out, columns=['cs', 'ts', 'shares', 'cost', 'pnl'])


def metrics(f, lo, hi):
    g = f[(f.cs >= lo) & (f.cs < hi)]
    days = (hi - lo) / 86_400_000
    if not len(g): return dict(net=0., per_day=0., dd=0., pdd=None)
    cp = g.groupby('cs').pnl.sum().sort_index(); eq = np.r_[0., np.cumsum(cp.values)]
    dd = float(np.max(np.maximum.accumulate(eq) - eq)); net = float(cp.sum())
    dp = g.groupby(pd.to_datetime(g.cs, unit='ms').dt.date).pnl.sum()
    return dict(net=round(net, 2), per_day=round(net / days, 2), dd=round(dd, 2), pdd=round(net / dd, 2) if dd > 0 else None,
                cps=round(100 * net / g.shares.sum(), 3), shares_per_day=round(float(g.shares.sum()) / days, 1),
                fills=int(len(g)), contracts=int(len(cp)), worst_contract=round(float(cp.min()), 2), worst_day=round(float(dp.min()), 2),
                pos_days=round(float((dp > 0).mean()), 3), avg_order_sh=round(float(g.shares.mean()), 1), max_contract_cost=round(float(g.groupby('cs').cost.sum().max()), 2))


R = {'gate_block_rate': GATE_P, 'periods': {k: [v[0], v[1]] for k, v in PERIODS.items()}, 'candidates': int(len(C)), 'rows': []}
for base in (5, 10, 20, 30, 40, 50, 75, 100, 150, 200):
    for cons in (True, False):
        nog = sim(base, 4 * base, None, cons)
        row = dict(base=base, cap=4 * base, cons=cons, nogate={k: metrics(nog, *v) for k, v in PERIODS.items()})
        seeds = [sim(base, 4 * base, np.random.default_rng(s), cons) for s in range(20)]
        for k, v in PERIODS.items():
            ms = [metrics(f, *v) for f in seeds]
            row.setdefault('gate', {})[k] = {m: [round(float(np.percentile([x[m] for x in ms if x.get(m) is not None], q)), 3) for q in (10, 50, 90)]
                                             for m in ('net', 'per_day', 'dd', 'pdd', 'cps', 'worst_contract', 'worst_day', 'pos_days')}
        R['rows'].append(row)
        a = row['gate']['all_0821']; o = row['gate']['oos_0919']
        print(f"base {base:3d} cap {4*base:3d} cons={cons!s:5s} | all: $/d {a['per_day'][1]:7.2f} DD {a['dd'][1]:7.1f} PDD {a['pdd'][1]:6.2f} c/sh {a['cps'][1]:5.2f} "
              f"worst mkt {a['worst_contract'][1]:7.1f} | OOS: $/d {o['per_day'][1]:7.2f} DD {o['dd'][1]:6.1f} PDD {o['pdd'][1]:6.2f} c/sh {o['cps'][1]:5.2f}", flush=True)
# cap variants at 50 sh base
for base, cap in ((50, 200), (50, 100), (50, 300), (100, 200), (100, 400)):
    f = sim(base, cap, np.random.default_rng(0), True)
    R.setdefault('cap_variants', []).append(dict(base=base, cap=cap, **{k: metrics(f, *v) for k, v in PERIODS.items()}))
    print('cap variant', base, cap, R['cap_variants'][-1]['all_0821'], flush=True)
(OUT / 'SIZING.json').write_text(json.dumps(R, indent=1, default=float))
print('DONE')
