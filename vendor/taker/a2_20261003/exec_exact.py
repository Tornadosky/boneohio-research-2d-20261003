#!/usr/bin/env python3
"""tailtaker_20261001_FINAL executor: the LIVE rules of p3_v57 / wf_psize / A / A2 on event-level books.

Differences from the frozen research executor (research.run):
  * decision book = venue book as of decide_ms - DLAG (live feed lag), not the 100 ms grid;
  * the FAK (BUY favourite, limit 1 - tail bid, never chase) is matched against the favourite ask LADDER
    as of decide_ms + L, L = per-row latency (fixed, or overhead + Polymarket taker hold by era);
  * size in principal USD (live) or fixed shares; taker fee 0.07*p*(1-p) per share (ATLAS 7);
  * p3_v57 features/gate copied from tailtaker_live/strategy.py (release_20260930_split), tiers in/out;
  * wf family copied from tailtaker_wfa/strategy_wfa.py (premium, thick, A2 tercile, $1 floor, 4x cap).
Every qualifying trade tick consumes one of the 4 intents whether or not it fills (live semantics).
"""
import json, math
from pathlib import Path
import numpy as np
import pandas as pd

ARR = (0, 25, 50, 75, 100, 125, 150, 175, 200, 225, 250, 275, 300, 350, 400, 500)
NLEV = 4
T_HOLD150 = int(pd.Timestamp('2026-09-04 14:00', tz='UTC').value // 10**6)    # changelog 50 -> 150 ms
T_HOLD150_TAPE = int(pd.Timestamp('2026-09-10 00:00', tz='UTC').value // 10**6)
T_HOLD50 = int(pd.Timestamp('2026-08-17 11:00', tz='UTC').value // 10**6)      # changelog 250 -> 50 ms
P3 = json.loads((Path(__file__).parent / 'p3_v57_model.json').read_text())


def hold_ms(ts, switch=T_HOLD150):
    return np.where(ts < T_HOLD50, 250, np.where(ts < switch, 50, 150))


def snap_lat(L):
    g = np.asarray(ARR)
    return g[np.abs(g[None, :] - np.asarray(L)[:, None]).argmin(1)]


def load(pool, views, a2=None, lo=None, hi=None):
    cols = ['contract_start_ms', 'decide_ms', 'is_yes', 'tte', 'dist', 'sigma_1s', 'z', 'vel10', 'vel30', 'won', 'd_bid', 'd_ask',
            'd_bid_size', 'd_ask_size', 'book_age_ms', 'btc_source_ms', 'mom3', 'btc', 'strike', 'sigma_300s', 'vel60']
    P = pd.read_parquet(pool, columns=cols)
    V = pd.read_parquet(views)
    d = P.merge(V, on=['contract_start_ms', 'decide_ms', 'is_yes'], how='inner')
    if a2 is not None:
        A = pd.read_parquet(a2)[['contract_start_ms', 'decide_ms', 'a2_mult', 'a2_ev']]
        d = d.merge(A, on=['contract_start_ms', 'decide_ms'], how='left')
    if lo is not None: d = d[d.contract_start_ms >= lo]
    if hi is not None: d = d[d.contract_start_ms < hi]
    return d.sort_values(['contract_start_ms', 'decide_ms']).reset_index(drop=True)


def decision_cols(d, D):
    p = np.round(d[f'dl{D}_tbid'].to_numpy(float), 4); dask = np.round(d[f'dl{D}_task'].to_numpy(float), 4)
    bsz = d[f'dl{D}_tbsz'].to_numpy(float); fask = np.round(d[f'dl{D}_fask'].to_numpy(float), 4)
    return p, dask, bsz, fask


def lag_lookup(d, p, secs):
    """value of the decision tail bid / |dist| at decide - secs on the same contract (any side), NaN if no row."""
    key = d.contract_start_ms.to_numpy(np.int64) * 1000 + (d.decide_ms.to_numpy(np.int64) - d.contract_start_ms.to_numpy(np.int64)) // 1000
    tgt = key - secs
    j = np.clip(np.searchsorted(key, tgt), 0, len(key) - 1); ok = key[j] == tgt
    return j, ok


def p3_signal(d, D):
    """tailtaker_live.strategy.TailTakerP3V57.evaluate, vectorised. Returns (ok_mask, model_ph, gate_ev, reason codes)."""
    m = P3['model']; env = P3['envelope']; p3 = P3['p3_v57']
    p, dask, bsz, fask = decision_cols(d, D)
    tte = d.tte.to_numpy(float); dist = d.dist.to_numpy(float); ad = np.abs(dist); sig = d.sigma_1s.to_numpy(float)
    broad = (p > 0) & (p < dask) & (dask <= 1) & (p >= .005) & (p <= .30)
    j, ok = lag_lookup(d, p, 30)
    # live LagRow exists only for broad-valid rows; the pool row at t-30 s is broad-valid by construction
    plag = np.where(ok, p[j], np.nan); adlag = np.where(ok, ad[j], np.nan)
    okl = ok & np.isfinite(plag) & broad[j]
    om = np.where(okl, p - plag, 0.); dw = np.where(okl, (adlag - ad) / sig, 0.)
    el = (d.decide_ms.to_numpy(np.int64) - d.contract_start_ms.to_numpy(np.int64)) // 1000
    tick = ((el - env['phase_s']) % env['trade_step_s']) == 0
    z = ad / (sig * np.sqrt(np.maximum(tte, 1.)))
    sg = np.where(dist < 0, 1., -1.)
    h = (d.decide_ms.to_numpy(np.int64) // 3_600_000) % 24
    X = np.column_stack([100 * p, sg * d.vel10.to_numpy(float) / sig, sg * d.vel30.to_numpy(float) / sig, np.log(np.maximum(ad, 1e-6)),
                         np.log(np.maximum(sig, 1e-6)), tte / 300., np.log(np.maximum(z, .001)), 100 * (dask - p), np.log1p(np.maximum(bsz, 0)),
                         100 * om, np.clip(dw, -20, 20), np.sin(2 * np.pi * h / 24), np.cos(2 * np.pi * h / 24)])
    eta = m['w'][0] + ((X - np.asarray(m['mu'])) / np.asarray(m['sd'])) @ np.asarray(m['w'][1:])
    ph = 1 / (1 + np.exp(-np.clip(eta, -30, 30)))
    gev = .928 * p - ph
    env_ok = broad & tick & (tte >= env['tte_lo']) & (tte <= env['tte_hi']) & (p >= env['tail_bid_lo'] - 1e-7) & (p <= env['tail_bid_hi'] + 1e-7) & (z >= env['z_min'])
    gate = (dw <= P3['thr_dw']) & (gev >= P3['thr_ev']) & (gev > 0)
    is3 = np.abs(p - p3['tail_bid']) < 2e-6
    mom = d.mom3.to_numpy(float)
    v57 = (ad >= p3['abs_start_move_min_usd']) & (ad <= p3['abs_start_move_max_usd']) & np.isfinite(mom) & (mom >= p3['mom3_favorite_min_usd'])
    sig_ok = env_ok & gate & (~is3 | v57) & (fask > 0)
    # rows that pass the envelope (consume an intent only if the whole signal passes: live claims after evaluate() == OPEN)
    return sig_ok, env_ok, ph, gev, z


def wf_signal(d, D, z_min=3., tte_lo=60., tte_hi=180., bid_lo=.02, bid_hi=.05):
    p, dask, bsz, fask = decision_cols(d, D)
    tte = d.tte.to_numpy(float); ad = np.abs(d.dist.to_numpy(float)); sig = d.sigma_1s.to_numpy(float)
    z = ad / (sig * np.sqrt(np.maximum(tte, 1.)))
    broad = (p > 0) & (p < dask) & (dask <= 1) & (p >= .005) & (p <= .30)
    el = (d.decide_ms.to_numpy(np.int64) - d.contract_start_ms.to_numpy(np.int64)) // 1000
    tick = ((el - 10) % 8) == 0
    ok = broad & tick & (tte >= tte_lo) & (tte <= tte_hi) & (p >= bid_lo - 1e-7) & (p <= bid_hi + 1e-7) & (z >= z_min) & (fask > 0)
    # thickness: first 2 s row where this tail side qualified (2-8c, z>=3), its TTE
    cs = d.contract_start_ms.to_numpy(np.int64); isy = d.is_yes.to_numpy(bool)
    q = broad & (p >= .02 - 2e-6) & (p <= .08 + 2e-6) & (z >= 3)
    fq = np.full(len(d), np.nan)
    key = pd.DataFrame({'cs': cs, 'y': isy, 'q': q, 'tte': tte})
    first = key[key.q].groupby(['cs', 'y']).tte.max()      # earliest row = largest TTE
    fq = key.set_index(['cs', 'y']).index.map(first.to_dict()).to_numpy(float)
    # only rows at/after the first qualification may use it (causal): first-qual TTE >= current TTE
    fq = np.where(np.isfinite(fq) & (fq >= tte), fq, np.nan)
    return ok, z, fq


def execute(d, arm):
    """arm: dict(name, family 'p3'|'wf', D, lat ('fixed',L)|('regime',O)|('regime_tape',O), size ('usd',x)|('shares',n),
    tiers {'in','out'} (p3, usd), base, premium, thick, a2, cap_mult, max_intents, plus envelope overrides)."""
    D = arm.get('D', 20)
    cs = d.contract_start_ms.to_numpy(np.int64); ts = d.decide_ms.to_numpy(np.int64)
    p, dask, bsz, fask = decision_cols(d, D)
    tte = d.tte.to_numpy(float)
    if arm['family'] == 'p3':
        sig, envok, ph, gev, z = p3_signal(d, D)
        if arm.get('env_override'):
            o = arm['env_override']
            sig = sig & (z >= o.get('z_min', 0)) & (tte >= o.get('tte_lo', 0)) & (tte <= o.get('tte_hi', 999)) & (p <= o.get('bid_hi', 1) + 1e-7)
        inside = (z >= 3) & (tte >= 60) & (tte <= 180) & (p <= .05 + 1e-7)
        mult = np.ones(len(d)); fq = None
    else:
        e = arm.get('env', {})
        sig, z, fq = wf_signal(d, D, **e)
        inside = np.ones(len(d), bool)
        mult = np.ones(len(d))
        if arm.get('premium', True): mult *= np.clip((p - .0255) / .01453, .25, 2.)
        if arm.get('thick'): mult *= np.where(np.isfinite(fq) & (fq >= 120), 1.5, 1.)
        if arm.get('a2'):
            am = d.a2_mult.to_numpy(float)
            sig = sig & np.isfinite(am)          # A2 fails closed without the model
            mult *= np.where(np.isfinite(am), am, 1.)
    if arm.get('mask') is not None:
        sig = sig & np.asarray(arm['mask'](d), dtype=bool)   # extra signal condition (no intent consumed when False)
    lat = arm['lat']
    if lat[0] == 'fixed': L = np.full(len(d), lat[1])
    elif lat[0] == 'regime': L = lat[1] + hold_ms(ts)
    elif lat[0] == 'regime_tape': L = lat[1] + hold_ms(ts, T_HOLD150_TAPE)
    else: raise ValueError(lat)
    L = snap_lat(L)
    px = np.round(np.stack([np.choose(np.searchsorted(ARR, L), [d[f'a{a}_px{k}'].to_numpy(float) for a in ARR]) for k in range(NLEV)], 1), 4)
    sz = np.stack([np.choose(np.searchsorted(ARR, L), [d[f'a{a}_sz{k}'].to_numpy(float) for a in ARR]) for k in range(NLEV)], 1)
    won = d.won.to_numpy(float)
    max_int = arm.get('max_intents', 4)
    out = []; attempts = 0
    cur = None; n_int = 0; used = 0.; used_sh = 0.
    idx = np.flatnonzero(sig)
    for i in idx:
        c = cs[i]
        if c != cur: cur, n_int, used, used_sh = c, 0, 0., 0.
        if n_int >= max_int: continue
        # sizing
        size = arm['size']
        usd = None
        if arm['family'] == 'p3':
            if size[0] == 'usd':
                usd = arm['tiers']['in' if inside[i] else 'out']
                if usd <= 0: continue          # tier switched off: no intent consumed
        else:
            base = size[1] if size[0] == 'usd' else None
            if base is not None:
                cap_left = arm.get('cap_mult', 4.) * base - used
                usd = max(1.0, base * mult[i]); usd = round(min(usd, cap_left), 2)
                if usd < 1.0 - 1e-9: continue
        want_sh = None
        if size[0] == 'shares':
            if arm['family'] == 'p3':
                want_sh = arm['tiers']['in' if inside[i] else 'out']
                if want_sh <= 0: continue
            else:
                want_sh = size[1] * (mult[i] if arm.get('scale_shares') else 1.)
            want_sh = min(want_sh, arm.get('cap_sh', np.inf) - used_sh)
            if want_sh < 1.0: continue
        n_int += 1; attempts += 1
        limit = round(1. - p[i], 4)
        got = 0.; cost = 0.; fee = 0.
        for k in range(NLEV):
            a = px[i, k]; q = sz[i, k]
            if not (np.isfinite(a) and np.isfinite(q)) or a > limit + 1e-9 or q <= 0: break
            if want_sh is not None: take = min(q, want_sh - got)
            else: take = min(q, (usd - cost) / a)
            if take <= 1e-9: break
            got += take; cost += take * a; fee += .07 * a * (1 - a) * take
            if (want_sh is not None and got >= want_sh - 1e-9) or (want_sh is None and cost >= usd - 1e-6): break
        if got > 1e-9:
            used_sh += got
            if arm['family'] == 'wf': used += cost
            fav_won = 1. - won[i]
            out.append((c, ts[i], int(L[i]), got, cost, fee, got * fav_won - cost - fee, won[i], p[i], tte[i], z[i], bool(inside[i]), n_int))
        else:
            out.append((c, ts[i], int(L[i]), 0., 0., 0., 0., won[i], p[i], tte[i], z[i], bool(inside[i]), n_int))
    f = pd.DataFrame(out, columns=['contract', 'ts', 'lat', 'shares', 'cost', 'fee', 'pnl', 'won', 'p', 'tte', 'z', 'inside', 'intent'])
    f.attrs['attempts'] = attempts
    return f


DAY = 86_400_000


def metrics(f, lo=None, hi=None):
    g = f[f.shares > 0]
    if lo is not None: g = g[g.contract >= lo]
    if hi is not None: g = g[g.contract < hi]
    if not len(g):
        return dict(net=0., maxdd=0., mpdd=None, contracts=0, fills=0, shares=0., cps=None, losers=0, worst=0., per_day=0.)
    cp = g.groupby('contract').pnl.sum().sort_index()
    eq = np.r_[0., np.cumsum(cp.values)]; dd = float(np.max(np.maximum.accumulate(eq) - eq))
    net = float(cp.sum())
    span = ((hi if hi is not None else int(f.contract.max()) + 300_000) - (lo if lo is not None else int(f.contract.min()))) / DAY
    return dict(net=net, maxdd=dd, mpdd=net / dd if dd > 1e-9 else None, contracts=int(len(cp)), fills=int(len(g)),
                shares=float(g.shares.sum()), cost=float(g.cost.sum()), cps=100 * net / float(g.shares.sum()),
                losers=int((cp < 0).sum()), worst=float(-cp.min()), per_day=net / span if span > 0 else None,
                fill_rate=float((f.shares > 0).mean()) if len(f) else None)
