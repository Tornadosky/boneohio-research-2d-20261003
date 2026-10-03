#!/usr/bin/env python3
"""tailtaker_20261001_FINAL step 2: event-level Polymarket book views for every candidate pool row.

The research pool samples the venue book on a 100 ms grid and prices a FAK at decide+300/500 ms from the
top level only. Here both tokens' full books are replayed from `normalized_depth_events` (venue_ts_ms, seq
order; snapshots reseed a token; levels pruned to the carried venue BBO, which is exact at the top while
deeper WS levels can hold phantoms) and, for each candidate row (contract, decide_ms, tail side), we emit:

  decision views  dl{D}_*  : tail bid / tail ask / tail bid size / favourite ask as of decide_ms - D ms
                             (D = feed lag of the live bot; events with venue_ts <= t are visible)
  arrival ladders a{L}_px{k}, a{L}_sz{k}, k = 0..3 : favourite-token ask ladder strictly before decide_ms + L
                             (what a FAK BUY of the favourite matches when it reaches the book L ms later)

Live orders (from the cohort A audit logs) get their exact venue print time from the trade tape by
transaction hash, and the venue time of the favourite-ask level decrease that equals the matched size
(the match itself; prints are stamped ~38 ms after the book change).
"""
import os
for _k in ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS', 'NUMBA_NUM_THREADS'):
    os.environ[_k] = '1'
import argparse, json, time, traceback
from multiprocessing import get_context
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow as pa, pyarrow.parquet as pq
pa.set_cpu_count(1); pa.set_io_thread_count(1)

SC = 1_000_000
DLAG = (0, 10, 20, 30, 50, 75, 100, 150)
ARR = (0, 25, 50, 75, 100, 125, 150, 175, 200, 225, 250, 275, 300, 350, 400, 500)
NLEV = 4
RAW = None; MK = 'btc_5m'
ROWS = {}; LIVE = {}


def parse_arr(s):
    s = str(s or '').strip()
    if not s or s in ('None', 'nan'):
        return []
    return json.loads(s) if s.startswith('[') else [int(float(x)) for x in s.split(',') if x]


def rd(kind, cs, cols):
    fs = sorted((RAW / kind / f'market_key={MK}' / f'contract_start_ms={cs}').rglob('*.parquet'))
    if not fs:
        return None
    parts = []
    for f in fs:
        t = pq.ParquetFile(f).read(columns=cols)
        parts.append({c: (t[c].cast(t[c].type.value_type) if pa.types.is_dictionary(t[c].type) else t[c]).to_numpy(zero_copy_only=False)
                      for c in cols})
    return {c: np.concatenate([p[c] for p in parts]) for c in cols}


class Book:
    """Both tokens, both sides; level dicts price_micros -> size (shares)."""
    def __init__(self):
        self.lv = {(y, s): {} for y in (True, False) for s in ('bid', 'ask')}
        self.bbo = {(y, s): None for y in (True, False) for s in ('bid', 'ask')}
        self.seeded = {True: False, False: False}

    def best(self, y, s):
        d = self.lv[(y, s)]; b = self.bbo[(y, s)]
        if s == 'bid':
            ks = [p for p, q in d.items() if q > 1e-9 and (b is None or b < 0 or p <= b)]
            return max(ks) if ks else None
        ks = [p for p, q in d.items() if q > 1e-9 and (b is None or b < 0 or p >= b)]
        return min(ks) if ks else None

    def ladder(self, y, n):
        d = self.lv[(y, 'ask')]; b = self.bbo[(y, 'ask')]
        ks = sorted(p for p, q in d.items() if q > 1e-9 and (b is None or b < 0 or p >= b))[:n]
        return [(p, d[p]) for p in ks]


def views(cs, rows, live_orders):
    ev = rd('normalized_depth_events', cs, ['venue_ts_ms', 'ts_ns', 'seq', 'is_yes', 'event_type', 'side', 'price_micros',
                                             'size_micros', 'best_bid_micros', 'best_ask_micros', 'bid_prices', 'bid_sizes',
                                             'ask_prices', 'ask_sizes'])
    if ev is None or not len(ev['seq']):
        return None, 'no_events'
    t = np.where(ev['venue_ts_ms'] > 0, ev['venue_ts_ms'], ev['ts_ns'] // 1_000_000 - 15).astype(np.int64)
    o = np.lexsort((ev['seq'], t))
    t = t[o]; ev = {k: v[o] for k, v in ev.items()}
    typ = np.asarray(ev['event_type'], dtype=str); side = np.asarray(ev['side'], dtype=str)
    isy = ev['is_yes'].astype(bool); px = ev['price_micros'].astype(np.int64); sz = ev['size_micros'].astype(np.int64)
    bb = ev['best_bid_micros'].astype(np.int64); ba = ev['best_ask_micros'].astype(np.int64)
    # queries: (time, kind, idx); kind 0 = decision view (<= t), 1 = arrival (< t), 2 = live-order level watch
    n = len(rows['decide_ms'])
    qs = []
    for i in range(n):
        dm = int(rows['decide_ms'][i])
        for k, D in enumerate(DLAG):
            qs.append((dm - D, 0, i, k))
        for k, L in enumerate(ARR):
            qs.append((dm + L - 1, 0, i, 100 + k))   # strictly before decide+L == as of decide+L-1 ms
    qs.sort()
    out = {f'dl{D}_{c}': np.full(n, np.nan, np.float32) for D in DLAG for c in ('tbid', 'task', 'tbsz', 'fask')}
    for L in ARR:
        for k in range(NLEV):
            out[f'a{L}_px{k}'] = np.full(n, np.nan, np.float32); out[f'a{L}_sz{k}'] = np.full(n, np.nan, np.float32)
    B = Book(); j = 0; ne = len(t); agree = [0, 0]
    # live order watch: per order, record every favourite-ask level change at its fill prices in [sched, print+50]
    watch = []
    for lo in live_orders:
        watch.append(dict(lo, changes=[]))
    def apply(e):
        y = bool(isy[e]); s = side[e]
        if typ[e] == 'snapshot':
            for sd, ps, ss in (('bid', ev['bid_prices'][e], ev['bid_sizes'][e]), ('ask', ev['ask_prices'][e], ev['ask_sizes'][e])):
                pr = parse_arr(ps); qq = parse_arr(ss)
                B.lv[(y, sd)] = {int(p): q / SC for p, q in zip(pr, qq)}
            B.seeded[y] = True
        elif s in ('bid', 'ask'):
            d = B.lv[(y, s)]; p = int(px[e])
            old = d.get(p, 0.)
            if sz[e] > 0: d[p] = sz[e] / SC
            else: d.pop(p, None)
            if s == 'ask':
                for w in watch:
                    if w['fav_yes'] == y and w['t_lo'] <= t[e] <= w['t_hi'] and p in w['prices']:
                        w['changes'].append((int(t[e]), p, old, sz[e] / SC))
        if bb[e] >= 0: B.bbo[(y, 'bid')] = int(bb[e])
        if ba[e] >= 0: B.bbo[(y, 'ask')] = int(ba[e])
    for tq, _, i, k in qs:
        while j < ne and t[j] <= tq:
            apply(j); j += 1
        tail_y = bool(rows['is_yes'][i]); fav_y = not tail_y
        if not (B.seeded[True] and B.seeded[False]):
            continue
        if k < 100:
            D = DLAG[k]
            tb = B.best(tail_y, 'bid'); ta = B.best(tail_y, 'ask'); fa = B.best(fav_y, 'ask')
            if tb is not None:
                out[f'dl{D}_tbid'][i] = tb / SC; out[f'dl{D}_tbsz'][i] = B.lv[(tail_y, 'bid')][tb]
                if D == 0:
                    agree[1] += 1; agree[0] += int(B.bbo[(tail_y, 'bid')] == tb)
            if ta is not None: out[f'dl{D}_task'][i] = ta / SC
            if fa is not None: out[f'dl{D}_fask'][i] = fa / SC
        else:
            L = ARR[k - 100]
            for m, (p, q) in enumerate(B.ladder(fav_y, NLEV)):
                out[f'a{L}_px{m}'][i] = p / SC; out[f'a{L}_sz{m}'][i] = q
    while j < ne:
        apply(j); j += 1
    return (out, agree, watch), None


def prints_for(cs, live_orders):
    if not live_orders:
        return []
    tr = rd('trade_tape', cs, ['venue_ts_ms', 'ts_ns', 'is_yes', 'price_micros', 'size_micros', 'side', 'transaction_hash',
                                'taker_order_id', 'event_uid'])
    res = []
    if tr is None:
        return [dict(order_id=o['order_id'], n_prints=0) for o in live_orders]
    _, u = np.unique(tr['event_uid'], return_index=True)
    tr = {k: v[u] for k, v in tr.items()}
    tx = np.char.lower(np.asarray(tr['transaction_hash'], dtype=str))
    toid = np.char.lower(np.asarray(tr['taker_order_id'], dtype=str))
    for o in live_orders:
        m = np.isin(tx, o['tx']) | (toid == o['order_id'])
        if not m.any():
            res.append(dict(order_id=o['order_id'], n_prints=0)); continue
        res.append(dict(order_id=o['order_id'], n_prints=int(m.sum()), print_first_ms=int(tr['venue_ts_ms'][m].min()),
                        print_last_ms=int(tr['venue_ts_ms'][m].max()), print_recv_first_ns=int(tr['ts_ns'][m].min()),
                        print_shares=float(tr['size_micros'][m].sum() / SC),
                        print_vwap=float((tr['price_micros'][m] * tr['size_micros'][m]).sum() / max(1, tr['size_micros'][m].sum()) / SC),
                        print_is_yes=bool(tr['is_yes'][m][0]), print_side=int(tr['side'][m][0]),
                        print_prices=json.dumps(sorted(set(int(x) for x in tr['price_micros'][m])))))
    return res


def worker(cs):
    try:
        rows = ROWS.get(cs)
        lo = LIVE.get(cs, [])
        if rows is None and not lo:
            return {'cs': cs, 'skip': True}
        if rows is None:
            rows = {'decide_ms': np.array([], np.int64), 'is_yes': np.array([], bool)}
        pr = prints_for(cs, lo)
        pmap = {p['order_id']: p for p in pr}
        lw = []
        for o in lo:
            p = pmap.get(o['order_id'], {})
            prices = set(json.loads(p['print_prices'])) if p.get('print_prices') else {int(round(o['limit'] * SC))}
            # complement prints (other token BUY at 1-x) mean our favourite fill price is SC - x
            if p.get('print_is_yes') is not None and p['print_is_yes'] != o['fav_yes']:
                prices = {SC - x for x in prices}
            lw.append(dict(order_id=o['order_id'], fav_yes=o['fav_yes'], prices=prices, t_lo=o['sched'] - 50,
                           t_hi=(p.get('print_last_ms') or o['sched'] + 1000) + 50))
        v, err = views(cs, rows, lw)
        if v is None:
            return {'cs': cs, 'blocked': err, 'prints': pr}
        out, agree, watch = v
        for w in watch:
            w['prices'] = sorted(w['prices']); w.pop('t_lo'); w.pop('t_hi')
        return {'cs': cs, 'out': out, 'agree': agree, 'prints': pr, 'watch': watch, 'n': len(rows['decide_ms'])}
    except Exception:
        return {'cs': cs, 'error': traceback.format_exc()}


def main():
    global RAW, MK
    ap = argparse.ArgumentParser()
    ap.add_argument('--pool', required=True); ap.add_argument('--raw', required=True); ap.add_argument('--out', required=True)
    ap.add_argument('--live', required=True); ap.add_argument('--workers', type=int, default=120); ap.add_argument('--limit', type=int, default=0)
    ap.add_argument('--market', default='btc_5m')
    a = ap.parse_args(); RAW = Path(a.raw); MK = a.market; out = Path(a.out); out.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    cols = ['contract_start_ms', 'decide_ms', 'is_yes', 'd_bid', 'z', 'tte']
    t = pq.read_table(a.pool, columns=cols, use_threads=False)
    d = {c: np.asarray(t[c]) for c in cols}
    keep = np.isfinite(d['d_bid'])   # all pool rows (0.5-30c): lag features need the t-30 s row of either side
    d = {k: v[keep] for k, v in d.items()}
    o = np.lexsort((d['decide_ms'], d['contract_start_ms'])); d = {k: v[o] for k, v in d.items()}
    u, s, n = np.unique(d['contract_start_ms'], return_index=True, return_counts=True)
    for c, a0, k in zip(u, s, n):
        ROWS[int(c)] = {'decide_ms': d['decide_ms'][a0:a0 + k].astype(np.int64), 'is_yes': d['is_yes'][a0:a0 + k].astype(bool)}
    L = pd.read_parquet(a.live)
    L = L[L.event.isin(['ORDER_POST_RETURNED', 'ORDER_ZERO_FILL'])]
    for r in L.itertuples():
        cs = int(r.slot) * 1000
        LIVE.setdefault(cs, []).append(dict(order_id=str(r.order_id).lower(), tx=[x.lower() for x in str(r.tx or '').split(',') if x and x != '<NA>'],
                                            fav_yes=(str(r.side) == 'UP'), limit=float(r.submitted_limit), sched=int(r.scheduled_ms)))
    tasks = sorted(set(ROWS) | set(LIVE))
    if a.limit: tasks = [t for t in tasks if t in ROWS and t in LIVE][-a.limit:]
    print('ROWS', int(keep.sum()), 'contracts', len(ROWS), 'live contracts', len(LIVE), 'tasks', len(tasks), flush=True)
    keys = []; cols_out = {}; prints = []; watches = []; agree = [0, 0]; blocked = {}; errs = []
    with get_context('fork').Pool(a.workers) as p:
        for nn, r in enumerate(p.imap_unordered(worker, tasks, chunksize=2), 1):
            if 'error' in r: errs.append(r); continue
            prints += [dict(p_, cs=r['cs']) for p_ in r.get('prints', [])]
            if 'blocked' in r: blocked[r['blocked']] = blocked.get(r['blocked'], 0) + 1; continue
            if r.get('skip'): continue
            watches += [dict(w, cs=r['cs']) for w in r['watch']]
            agree[0] += r['agree'][0]; agree[1] += r['agree'][1]
            if r['n']:
                rw = ROWS[r['cs']]
                keys.append(pd.DataFrame({'contract_start_ms': r['cs'], 'decide_ms': rw['decide_ms'], 'is_yes': rw['is_yes']}))
                for k, v in r['out'].items(): cols_out.setdefault(k, []).append(v)
            if nn % 1000 == 0: print('PROGRESS', nn, len(tasks), round(time.time() - t0), flush=True)
    if errs: print('ERRORS', len(errs), errs[0]['error'][-1500:], flush=True)
    df = pd.concat(keys, ignore_index=True)
    for k, v in cols_out.items(): df[k] = np.concatenate(v)
    df.to_parquet(out / 'exact_views.parquet', index=False)
    pd.DataFrame(prints).to_parquet(out / 'live_prints.parquet', index=False)
    pd.DataFrame([dict(w, changes=json.dumps(w['changes']), prices=json.dumps(w['prices'])) for w in watches]).to_parquet(out / 'live_level_changes.parquet', index=False)
    meta = dict(rows=len(df), blocked=blocked, errors=len(errs), top_agree=agree, dlag=DLAG, arr=ARR, secs=round(time.time() - t0))
    (out / 'EXACT_META.json').write_text(json.dumps(meta, indent=1))
    if errs: (out / 'EXACT_ERRORS.json').write_text(json.dumps(errs[:20], indent=1))
    print('COMPLETE', json.dumps(meta), flush=True)


if __name__ == '__main__':
    main()
