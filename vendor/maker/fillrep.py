"""DEPLOYGAP H5 — FILLPARITY v3 order-level replay of EVERY captured accepted live account_a order (all profiles), pendulumflow book.

Model (validated in q99_20260930_FILLPARITY, 98.9 % fill yes/no on 553 BTC orders): FIX = both|book|sizematch|sh60|df0 —
complement-token BUY prints count as SELL at 1-p, a sweep print reaching L contributes size - (displayed better bids), a print
through L empties L, unexplained level decreases cancel an exact-size order (newest such) else newest-first, prints applied
60 ms early, entry at the level update carrying our size (footprint), exit 1 ms before the user-WS cancel stamp.
Diagnostic variants: ENG (engine-like: same-token SELL @L only, flat, newest-first, no shift), LIFO, SH20, DF1, exit +/-5 ms.
Replay functions are copied verbatim from FILLPARITY fillpar3.py.
    python fillrep.py [START_ISO END_ISO]          -> X/fill/replay.parquet, attr.parquet, diag.parquet
"""
import glob, json, os, sys
import numpy as np, pandas as pd, pyarrow.parquet as pq
from concurrent.futures import ProcessPoolExecutor

X = 'reference-storage/q99_20261002_DEPLOYGAP'
OUT = os.environ.get('OUT', f'{X}/fill')
os.makedirs(OUT, exist_ok=True)
MIC = 1_000_000
VARIANTS = {'FIX': ('both', 'book', 'sizematch', 60, 0, 0), 'ENG': ('same', 'flat', 'lifo', 0, 0, 0),
            'LIFO': ('both', 'book', 'lifo', 60, 0, 0), 'SH20': ('both', 'book', 'sizematch', 20, 0, 0),
            'DF1': ('both', 'book', 'sizematch', 60, 1, 0), 'EXP5': ('both', 'book', 'sizematch', 60, 0, 5),
            'EXM5': ('both', 'book', 'sizematch', 60, 0, -5),
            'SWX': ('bothx', 'book', 'sizematch', 60, 0, 0)}   # also sell-like prints ABOVE L: size - displayed better bids reaches L


def cache(coin):
    return f'reference-storage/polycache_{coin}_twap/cache_{coin}_pflow_v1'


def load_market(mk, slot):
    C = cache(mk.split('_')[0])
    fd = glob.glob(f'{C}/normalized_depth_events/market_key={mk}/contract_start_ms={slot * 1000}/session_id=*/*.parquet')
    ft = glob.glob(f'{C}/trade_tape/market_key={mk}/contract_start_ms={slot * 1000}/session_id=*/*.parquet')
    if not fd:
        return None, None
    dcols = ['ts_ns', 'venue_ts_ms', 'seq', 'flags', 'outcome', 'event_type', 'side_code', 'price_micros', 'size_micros',
             'bid_prices', 'bid_sizes', 'ask_prices', 'ask_sizes']
    d = pd.concat([pq.ParquetFile(f).read(columns=dcols).to_pandas() for f in fd], ignore_index=True)
    tcols = ['ts_ns', 'venue_ts_ms', 'seq', 'outcome', 'price_micros', 'size_micros', 'trade_side', 'transaction_hash']
    t = pd.concat([pq.ParquetFile(f).read(columns=tcols).to_pandas() for f in ft], ignore_index=True) if ft else pd.DataFrame(columns=tcols)
    return d, t


# ---------------- verbatim from fillpar3.py ----------------
def token_bids(d, oc, lo_mic):
    m = (d.outcome == oc) & (d.event_type == 'delta_level') & (d.side_code == 0) & (d.price_micros >= lo_mic)
    a = d.loc[m, ['venue_ts_ms', 'seq', 'price_micros', 'size_micros']].copy()
    snaps = d[(d.outcome == oc) & (d.event_type == 'snapshot')]
    parsed = []
    prices = set(a.price_micros.unique().tolist()) | {lo_mic}
    for r in snaps.itertuples():
        ps = [int(x) for x in str(r.bid_prices).split(',') if x not in ('', 'nan', 'None')]
        qs = [int(x) for x in str(r.bid_sizes).split(',') if x not in ('', 'nan', 'None')]
        have = {p: q for p, q in zip(ps, qs) if p >= lo_mic}
        prices |= set(have)
        parsed.append((r.venue_ts_ms, r.seq, have))
    rows = [(t, sq, p, have.get(p, 0)) for t, sq, have in parsed for p in prices]
    if rows:
        a = pd.concat([a, pd.DataFrame(rows, columns=a.columns)], ignore_index=True)
    return a.sort_values(['venue_ts_ms', 'seq'], kind='stable').reset_index(drop=True)


def above_asof(bids, L, times):
    out = np.zeros(len(times))
    ab = bids[bids.price_micros > L]
    for p, g in ab.groupby('price_micros'):
        t = g.venue_ts_ms.to_numpy(np.float64); s = g.size_micros.to_numpy(np.float64) / MIC
        k = np.searchsorted(t, times, side='right') - 1
        out += np.where(k >= 0, s[np.clip(k, 0, None)], 0.0)
    return out


def replay(lvl, prints, R0, foot_in, t_exit, rule, depth_fill):
    ev = [(t, 1, sq, v) for t, sq, v in zip(lvl.venue_ts_ms.to_numpy(), lvl.seq.to_numpy(), lvl['size'].to_numpy())]
    ev += [(tp, 0, -1, (amt, thr, tv)) for tp, amt, thr, tv in prints]
    ev.sort(key=lambda e: (e[0], e[1], e[2]))
    q = []
    tot = 0.0
    ours_in = False; R = R0; filled = 0.0; fills = []
    for t, typ, seq, val in ev:
        if t >= t_exit:
            break
        if typ == 0:
            amt, thr, tv = val
            if thr:
                amt = 1e18
            while amt > 1e-9 and q:
                c = min(q[0][0], amt)
                if q[0][1]:
                    fills.append((tv, c)); filled += c; R -= c
                q[0][0] -= c; amt -= c; tot -= c
                if q[0][0] <= 1e-9: q.pop(0)
            if tot < 0: tot = 0.0
            if ours_in and R <= 1e-9:
                break
            continue
        new = float(val)
        if not ours_in and (t, seq) == foot_in:
            extra = new - tot - R0
            if extra < 0:
                tot -= (-extra - _cancel(q, -extra, rule))
                extra = 0.0
            q.append([R0, True]); ours_in = True; tot += R0
            if extra > 1e-9:
                q.append([extra, False]); tot += extra
            continue
        delta = new - tot
        if delta > 1e-9:
            q.append([delta, False]); tot += delta
        elif delta < -1e-9:
            left = _cancel(q, -delta, rule)
            tot -= (-delta - left)
            if left > 1e-9 and ours_in and depth_fill:
                for x in q:
                    if x[1]:
                        c = min(x[0], left); x[0] -= c; R -= c; filled += c; fills.append((t, c)); left -= c; tot -= c
                q = [x for x in q if x[0] > 1e-9]
                if R <= 1e-9:
                    break
    return filled, (fills[0][0] if fills else np.nan), fills


def _cancel(q, amt, rule):
    others = [i for i, x in enumerate(q) if not x[1]]
    if rule in ('sizematch', 'sizefifo') and others:
        cand = [i for i in others if abs(q[i][0] - amt) < 1e-6]
        if cand:
            i = cand[-1] if rule == 'sizematch' else cand[0]
            q.pop(i)
            return 0.0
    for i in reversed(others):
        if amt <= 1e-9: break
        c = min(q[i][0], amt); q[i][0] -= c; amt -= c
    q[:] = [x for x in q if x[0] > 1e-9]
    return amt
# ------------------------------------------------------------


def run_market(args):
    mk, slot, g, fills_by = args
    try:
        d, t = load_market(mk, slot)
    except Exception as e:
        print('LOADFAIL', mk, slot, e, flush=True)
        return [], [], []
    if d is None:
        return [], [], [dict(profile=mk, slot=slot, id=r.id, no_cache=True) for r in g.itertuples()]
    out, attr, diag = [], [], []
    for r in g.itertuples():
        oc = r.outcome; L = r.px_mic
        bids = token_bids(d, oc, L)
        lvl = bids[bids.price_micros == L].rename(columns={'size_micros': 'sz'})
        lvl = pd.DataFrame(dict(venue_ts_ms=lvl.venue_ts_ms.to_numpy(np.float64), seq=lvl.seq.to_numpy(),
                                size=lvl.sz.to_numpy(np.float64) / MIC))
        prev = lvl['size'].shift(1).fillna(0.0)
        inc = lvl[(lvl['size'] - prev >= r.size - 1e-6) & lvl.venue_ts_ms.between(r.place_venue_ms - 3000, r.place_venue_ms + 3000)]
        if len(inc):
            k = (inc.venue_ts_ms - r.place_venue_ms).abs().argmin()
            foot_in = (float(inc.venue_ts_ms.iloc[k]), int(inc.seq.iloc[k])); foot_dt = float(inc.venue_ts_ms.iloc[k] - r.place_venue_ms)
            q_ahead = float(prev.loc[inc.index[k]])
        else:
            foot_in = None; foot_dt = np.nan
            before = lvl[lvl.venue_ts_ms <= r.place_venue_ms]
            q_ahead = float(before['size'].iloc[-1]) if len(before) else 0.0
        t_exit0 = (r.cancel_venue_ms - 1) if pd.notna(r.cancel_venue_ms) else r.t_out
        tt = t.copy()
        tt['mp'] = np.where(tt.outcome == oc, tt.price_micros, MIC - tt.price_micros)
        sell_like = ((tt.outcome == oc) & (tt.trade_side == 1)) | ((tt.outcome != oc) & (tt.trade_side == 0))
        allp = tt[sell_like].copy()
        allx = allp.assign(kind=np.where(allp.outcome == oc, 'same', 'comp'), size=allp.size_micros / MIC).sort_values(['venue_ts_ms', 'seq'])
        tt = tt[sell_like & (tt.mp <= L)].copy()
        tt['kind'] = np.where(tt.outcome == oc, 'same', 'comp')
        tt['size'] = tt.size_micros / MIC
        tt = tt.sort_values(['venue_ts_ms', 'seq'])
        # tick-0.001 market? (any bid price of this token in our range that is not a whole cent)
        tick001 = bool(((bids.price_micros % 10000) != 0).any())
        base = dict(profile=mk, slot=slot, id=r.id, run=r.run, ref=r.ref, side=r.side, limit=r.limit_price, size=r.size,
                    place_ms=r.place_venue_ms, cancel_ms=r.cancel_venue_ms, cancel_reason=r.cancel_reason, act_filled=r.act_filled,
                    act_first_ms=r.act_first_ms, foot=foot_in is not None, foot_dt=foot_dt, q_ahead=q_ahead, tick001=tick001,
                    n_lvl_events=int(((lvl.venue_ts_ms >= r.place_venue_ms) & (lvl.venue_ts_ms < t_exit0)).sum()),
                    n_prints_life=int(((tt.venue_ts_ms >= r.place_venue_ms - 60) & (tt.venue_ts_ms < t_exit0 + 60)).sum()))
        if foot_in is None:
            foot_in = (float(r.place_venue_ms), -1)
            lvl = pd.concat([lvl, pd.DataFrame(dict(venue_ts_ms=[float(r.place_venue_ms)], seq=[-1], size=[np.nan]))],
                            ignore_index=True).sort_values(['venue_ts_ms', 'seq'])
            lvl['size'] = lvl['size'].ffill().fillna(0.0)
            m = (lvl.venue_ts_ms == float(r.place_venue_ms)) & (lvl.seq == -1)
            lvl.loc[m, 'size'] = lvl.loc[m, 'size'] + r.size
        prep = {}
        for name, (ts, sw, rule, sh, df, exs) in VARIANTS.items():
            key = (ts, sw, sh)
            if key not in prep:
                p0 = allx if ts == 'bothx' else tt if ts == 'both' else tt[tt.kind == 'same']
                tv = p0.venue_ts_ms.to_numpy(np.float64); sz = p0['size'].to_numpy(); mp = p0.mp.to_numpy()
                abv = above_asof(bids, L, tv - sh - 0.5) if len(p0) else np.zeros(0)
                amt = sz if sw == 'flat' else np.maximum(0.0, sz - abv)
                thr = (mp < L) & (sw == 'book')
                prep[key] = [(tv[i] - sh, amt[i], bool(thr[i]), tv[i]) for i in range(len(p0)) if (sw == 'book' or mp[i] == L)]
            fq, ff, fls = replay(lvl, prep[key], r.size, foot_in, t_exit0 + exs, rule, df)
            out.append(dict(base, variant=name, sim_filled=fq, sim_first=ff, sim_fills=json.dumps([(float(a), float(b)) for a, b in fls])))
        # attribution of each real fill to a cache print (sell-like, mapped price <= L, |dt| <= 5 ms), else 'none'
        for f in fills_by.get((r.run, r.profile, r.ref), []):
            sms, qty = f
            w = allp[(allp.venue_ts_ms - sms).abs() <= 5]
            w = w.assign(mp=np.where(w.outcome == oc, w.price_micros, MIC - w.price_micros))
            hit = w[w.mp <= L]
            kind = 'none'
            if len(hit):
                h = hit.iloc[(hit.venue_ts_ms - sms).abs().argmin()]
                kind = ('same' if h.outcome == oc else 'comp') + ('_at' if h.mp == L else '_through')
            attr.append(dict(profile=mk, slot=slot, id=r.id, server_ms=sms, qty=qty, kind=kind, n_any_5ms=len(w)))
        diag.append(dict(base))
    print(mk, slot, len(g), flush=True)
    return out, attr, diag


def main():
    L = pd.read_parquet(f'{X}/inputs_account_a/live_orders.parquet')
    A = L[L.captured & (L.phase != 'rejected') & L.place_venue_ms.notna()].copy()
    if len(sys.argv) > 2:
        lo, hi = pd.Timestamp(sys.argv[1]).timestamp(), pd.Timestamp(sys.argv[2]).timestamp()
        A = A[(A.slot >= lo) & (A.slot < hi)]
    f = pd.read_parquet(f'{X}/decoded/account_a/fills.parquet')
    f = f[f.status == 2].sort_values('server_ms').drop_duplicates(['run', 'trade_key'])
    agg = f.groupby(['run', 'profile', 'ref']).agg(act_filled=('qty', 'sum'), act_first_ms=('server_ms', 'min')).reset_index()
    A = A.merge(agg, on=['run', 'profile', 'ref'], how='left')   # refs are run-local PER STREAM
    A['act_filled'] = A.act_filled.fillna(0.0)
    A['t_out'] = ((A.slot + A.duration_sec) * 1000).astype(float)
    A['outcome'] = np.where(A.side == 'UP', 0, 1)
    A['px_mic'] = (A.limit_price * 100).round().astype(int) * 10_000
    fb = {}
    for x in f.itertuples():
        fb.setdefault((x.run, x.profile, x.ref), []).append((int(x.server_ms), float(x.qty)))
    tasks = []
    for (mk, slot), g in A.groupby(['profile_id', 'slot']):
        keys = set(zip(g.run, g.profile, g.ref))
        tasks.append((mk, int(slot), g, {k: v for k, v in fb.items() if k in keys}))
    print('orders', len(A), 'markets', len(tasks), flush=True)
    R, AT, DG = [], [], []
    with ProcessPoolExecutor(int(os.environ.get('WORKERS', 4))) as ex:
        for o, a, dg in ex.map(run_market, tasks, chunksize=2):
            R += o; AT += a; DG += dg
    sfx = os.environ.get('SFX', '')
    pd.DataFrame(R).to_parquet(f'{OUT}/replay{sfx}.parquet')
    pd.DataFrame(AT).to_parquet(f'{OUT}/attr{sfx}.parquet')
    pd.DataFrame(DG).to_parquet(f'{OUT}/diag{sfx}.parquet')
    print('DONE', len(R), len(AT), len(DG), flush=True)


if __name__ == '__main__':
    main()
