#!/usr/bin/env python3
"""tailtaker_20261003_A2PARITY step 8: execution + PnL parity of every live A2 order (both boxes) inside the pendulumflow window.

1. Venue latency: print time (tape, by transaction hash) and match time (favourite-ask decrease == matched size) - decision.
2. Fill parity: the live order (spend-sized FAK BUY favourite, limit = 1 - tail bid, live principal) replayed against the
   favourite-ask ladder (4 levels) as of decide + L; fill / no-fill, shares, VWAP vs live (wallet-confirmed amounts).
3. PnL parity per order and per contract: live = taking * fav_won - making - fee(0.07 p(1-p)), outcome = cache oracle;
   backtest = same formula on the replayed fill.
"""
import json, sys
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, '<SOURCE_HOME>/tailtaker_20261003_A2PARITY')
import exec_exact as X  # noqa: E402
O = Path('<SOURCE_DATA>/exp/tailtaker_20261003_A2PARITY'); OUT = O / 'analysis'
POOL_END = json.loads((O / 'pools/btc_5m__pflow__venue_manifest.json').read_text())['last_contract_ms'] + 300_000
ev = pd.read_parquet(O / 'inputs/live_ticks_both.parquet')
for c in ('slot', 'scheduled_ms', 'taking', 'making', 'submitted_limit', 'principal_usd', 'post_ms', 'sign_ms', 'total_ms', 'tail_bid'):
    ev[c] = pd.to_numeric(ev[c], errors='coerce')
pr = pd.read_parquet(O / 'exact/live_prints.parquet')
lc = pd.read_parquet(O / 'exact/live_level_changes.parquet')
o = ev[ev.event.isin(['ORDER_POST_RETURNED', 'ORDER_ZERO_FILL'])].copy()
o['order_id'] = o.order_id.str.lower(); o['cs'] = o.slot.astype(np.int64) * 1000; o['sched'] = o.scheduled_ms.astype(np.int64)
o['fav_yes'] = o.side == 'UP'; o['filled'] = o.taking.fillna(0) > 0
o = o[o.cs + 300_000 <= POOL_END]
o = o.merge(pr.drop(columns=['cs']), on='order_id', how='left').merge(lc[['order_id', 'changes']], on='order_id', how='left')


def match_time(r):
    if not r.filled or not isinstance(r.changes, str) or not np.isfinite(r.print_first_ms):
        return np.nan
    best = None
    for t, p, old, new in json.loads(r.changes):
        if t > r.print_last_ms or t < r.sched:
            continue
        dec = old - new
        if dec > 0 and abs(dec - r.taking) <= max(0.02, 0.01 * r.taking):
            best = t if best is None else (max(best, t) if t <= r.print_first_ms else best)
    if best is None:
        c = [t for t, p, old, new in json.loads(r.changes) if r.sched <= t <= r.print_first_ms and old - new > 0]
        best = max(c) if c else np.nan
    return best


o['match_ms'] = o.apply(match_time, axis=1)
o['L_match'] = o.match_ms - o.sched; o['L_print'] = o.print_first_ms - o.sched
R = {'orders': o.groupby('box').size().to_dict()}
R['latency'] = {b: dict(n=int(g.filled.sum()), located=int(g.print_first_ms.notna().sum()), match=int(g.match_ms.notna().sum()),
                        L_match_p10_50_90=np.nanpercentile(g.L_match, [10, 50, 90]).round(1).tolist() if g.L_match.notna().any() else None,
                        L_print_p50=float(np.nanmedian(g.L_print)) if g.L_print.notna().any() else None,
                        post_ms_p50=float(g.post_ms.median()))
                for b, g in o.groupby('box')}
print('LATENCY', json.dumps(R['latency'], indent=1), flush=True)
V = pd.read_parquet(O / 'exact/exact_views.parquet')
V = V[V.contract_start_ms >= int(o.cs.min())]
m = o.merge(V, left_on=['cs', 'sched'], right_on=['contract_start_ms', 'decide_ms'], how='left')
m = m[m.decide_ms.notna()].copy()


def fak(row, L):
    lim = round(float(row.submitted_limit), 4); usd = float(row.principal_usd); got = cost = 0.
    for k in range(X.NLEV):
        a, q = row[f'a{L}_px{k}'], row[f'a{L}_sz{k}']
        if not (np.isfinite(a) and np.isfinite(q)): break
        a = round(float(a), 4)
        if a > lim + 1e-9 or q <= 0: break
        take = min(q, (usd - cost) / a)
        if take <= 1e-9: break
        got += take; cost += take * a
        if cost >= usd - 1e-6: break
    return got, cost


upwon = pd.read_parquet(O / 'pools/btc_5m__pflow__venue_causal.parquet', columns=['contract_start_ms', 'is_yes', 'won']).drop_duplicates('contract_start_ms')
upwon = dict(zip(upwon.contract_start_ms, np.where(upwon.is_yes, upwon.won, 1 - upwon.won)))
m['fav_won'] = [upwon.get(int(c), np.nan) if y else 1 - upwon.get(int(c), np.nan) for c, y in zip(m.cs, m.fav_yes)]
m['px_live'] = np.where(m.filled, m.making / m.taking, np.nan)
m['fee_live'] = np.where(m.filled, .07 * m.px_live * (1 - m.px_live) * m.taking, 0.)
m['pnl_live'] = np.where(m.filled, m.taking * m.fav_won - m.making - m.fee_live, 0.)
R['fill'] = {}
for L in X.ARR:
    res = m.apply(lambda r: fak(r, L), axis=1)
    sh = np.array([x[0] for x in res]); co = np.array([x[1] for x in res])
    m[f'bt_sh{L}'] = sh; m[f'bt_cost{L}'] = co
    for b in m.box.unique():
        g = m.box == b
        yl = m.filled[g].to_numpy(); yb = sh[g] > 1e-9
        both = yl & yb
        R['fill'].setdefault(b, {})[L] = dict(n=int(g.sum()), agree=round(float((yl == yb).mean()), 4), live_fill=int(yl.sum()), bt_fill=int(yb.sum()),
                                              live_only=int((yl & ~yb).sum()), bt_only=int((~yl & yb).sum()),
                                              shares_exact=round(float((np.abs(sh[g][both] - m.taking[g].to_numpy()[both]) < 0.01).mean()), 4) if both.any() else None,
                                              shares_ratio=round(float(sh[g][both].sum() / m.taking[g].to_numpy()[both].sum()), 4) if both.any() else None,
                                              vwap_exact=round(float((np.abs(co[g][both] / sh[g][both] - m.px_live[g].to_numpy()[both]) < 1e-4).mean()), 4) if both.any() else None)
for b in R['fill']:
    print(b, {L: R['fill'][b][L] for L in (100, 150, 175, 200, 225, 250, 300)}, flush=True)
# PnL parity at the measured latency (overhead + 150 ms hold): L = 200 (p50 decision->match)
Lm = 200
m['fee_bt'] = .07 * (m[f'bt_cost{Lm}'] / np.maximum(m[f'bt_sh{Lm}'], 1e-12)) * (1 - m[f'bt_cost{Lm}'] / np.maximum(m[f'bt_sh{Lm}'], 1e-12)) * m[f'bt_sh{Lm}']
m['pnl_bt'] = m[f'bt_sh{Lm}'] * m.fav_won - m[f'bt_cost{Lm}'] - m.fee_bt
R['pnl_same_orders'] = {b: dict(orders=int(len(g)), live=round(float(g.pnl_live.sum()), 4), bt=round(float(g.pnl_bt.sum()), 4),
                                live_shares=round(float(g.taking.fillna(0).sum()), 3), bt_shares=round(float(g[f'bt_sh{Lm}'].sum()), 3),
                                per_order_abs_diff_p90=round(float((g.pnl_live - g.pnl_bt).abs().quantile(.9)), 4),
                                contracts=int(g.cs.nunique()), losers_live=int((g.groupby('cs').pnl_live.sum() < 0).sum()),
                                losers_bt=int((g.groupby('cs').pnl_bt.sum() < 0).sum()))
                        for b, g in m.groupby('box')}
print('PNL same orders', json.dumps(R['pnl_same_orders'], indent=1), flush=True)
bad = m[(m.filled != (m[f'bt_sh{Lm}'] > 1e-9)) | ((m.filled) & (np.abs(m[f'bt_sh{Lm}'] - m.taking) > 0.01))]
print(bad[['box', 'cs', 'sched', 'event', 'submitted_limit', 'principal_usd', 'taking', 'making', f'bt_sh{Lm}', f'bt_cost{Lm}', 'L_match', 'L_print',
           f'a{Lm}_px0', f'a{Lm}_sz0', f'a{Lm}_px1', f'a{Lm}_sz1', 'a0_px0', 'a0_sz0']].to_string(), flush=True)
import re
m.drop(columns=[c for c in m.columns if re.match(r'^(dl\d+_|a\d+_(px|sz)\d)', c)], errors='ignore').to_parquet(OUT / 'exec_parity.parquet', index=False)
(OUT / 'EXECPAR.json').write_text(json.dumps(R, indent=1, default=float))
