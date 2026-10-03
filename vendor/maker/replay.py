"""Decision-replay of live q99 orders (HANDOFF_PARITY_20261003 P3 backtester (1)): every captured, venue-accepted live order of
account_a / account_b / account_c is posted again at its exact venue placement time (place_venue_ms), price, size and token, and taken out at its
exact cancel-effective time (cancel_venue_ms; market end if it rested), on the pendulumflow book cache. Only the FILLS are
simulated, so any difference from the live fills is the fill model's (decisions are held equal).

Modes (all on the venue clock, pendulumflow depth rows + trade tape):
  eng       the production engine's FILLPAR path (lat/pkg_v9fp `_fp_simulate`, Q99_FILLPAR=1 COMPLEMENT=1 THROUGH=1 CANCEL_RULE=sizematch
            EFFECTIVE_SHIFT_MS=60 with polymarket feed 13 ms => prints 47 ms early, OWN_VENUE=1 CUT_SHIFTED=1): a virtual order joins
            the back of the level at place_venue_ms; the level is the displayed size minus the account's own resting steps
            (inputs_<acct>/own_steps2_<prof>.parquet, own_steps for account_c); a print exactly at L consumes its full size from the front;
            a print below L (trade-through) empties the level; unexplained decreases cancel an exact-size other order (newest), else
            newest-first.
  eng_book  eng + sweep-aware prints: a print at L contributes size − (displayed bids better than L just before it)
  eng_sh60  eng with prints 60 ms early (instead of 47)
  eng_noown eng without own-order removal (diagnostic: our own resting size counted as a stranger ahead)
  eng_thr   eng_book + bounded trade-through: a print below L consumes only size - (bids better than L) instead of emptying L
  eng_own5  eng with own placement steps applied 5 ms early (the depth footprint precedes the user-WS stamp by 1-4 ms)
  eng_fix   eng_thr + eng_own5
  eng_ownft eng with each own placement step snapped to its depth footprint row
  eng_ftfix eng_thr + eng_ownft
  eng_dm    depth-matched prints (sim_dm), virtual join, footprint-aligned own steps
  fp3_dm    depth-matched prints, real order at its footprint
  fp3       q99_20260930_FILLPARITY fillpar3 best variant: the real order enters at its own footprint in the level (the update that
            carries our size), sweep-aware prints, trade-through, sizematch cancels, prints 60 ms early
  fp3_df1   fp3 + depth-fill (a level drop below everything but us is our unprinted fill)
  fp3_thr   fp3 with the bounded trade-through
Live side: ledger `matched` (= fs) per order; first fill = user-WS server_ms from decoded fills (captured) else ledger match_time
(floor seconds, ≤ 1.5 s early). PnL = payoff at resolution (ledger winner), 0 fee.

Output X/replay/orders_<acct>.parquet (+ orders_all.parquet). usage: replay.py [WORKERS]
"""
import glob, os, sys
import numpy as np, pandas as pd, pyarrow.parquet as pq
from concurrent.futures import ProcessPoolExecutor

X = 'reference-storage/q99_20261002_DEPLOYGAP'
OUT = os.environ.get('RP_OUT', f'{X}/replay')
os.makedirs(OUT, exist_ok=True)
MIC = 1_000_000
FEED = 13.0
ENG_MODES = ('eng', 'eng_book', 'eng_sh60', 'eng_noown', 'eng_thr', 'eng_own5', 'eng_fix', 'eng_ownft', 'eng_ftfix')
MODES = ENG_MODES + ('fp3', 'fp3_df1', 'fp3_thr', 'eng_dm', 'fp3_dm')
OWN = {}


def cache_dir(coin):
    return f'reference-storage/polycache_{coin}_twap/cache_{coin}_pflow_v1'


def load_market(coin, mk, slot):
    C = cache_dir(coin)
    fd = glob.glob(f'{C}/normalized_depth_events/market_key={mk}/contract_start_ms={slot * 1000}/session_id=*/*.parquet')
    ft = glob.glob(f'{C}/trade_tape/market_key={mk}/contract_start_ms={slot * 1000}/session_id=*/*.parquet')
    if not fd:
        return None, None
    dcols = ['venue_ts_ms', 'seq', 'outcome', 'event_type', 'side_code', 'price_micros', 'size_micros', 'bid_prices', 'bid_sizes']
    d = pd.concat([pq.ParquetFile(f).read(columns=dcols).to_pandas() for f in fd], ignore_index=True)
    tcols = ['venue_ts_ms', 'seq', 'outcome', 'price_micros', 'size_micros', 'trade_side']
    t = pd.concat([pq.ParquetFile(f).read(columns=tcols).to_pandas() for f in ft], ignore_index=True) if ft else pd.DataFrame(columns=tcols)
    return d, t


def _pm(x):
    v = float(x)
    return int(round(v * MIC)) if v < 2 else int(round(v))


def token_bids(d, oc, lo_mic):
    """Bid-side level rows (delta + snapshot-expanded) of token oc at prices >= lo_mic, venue/seq sorted: venue_ts_ms, seq, price_micros, size."""
    m = (d.outcome == oc) & (d.event_type == 'delta_level') & (d.side_code == 0) & (d.price_micros >= lo_mic)
    a = d.loc[m, ['venue_ts_ms', 'seq', 'price_micros', 'size_micros']].copy()
    snaps = d[(d.outcome == oc) & (d.event_type == 'snapshot')]
    parsed = []
    prices = set(a.price_micros.unique().tolist())
    for r in snaps.itertuples():
        ps = [_pm(x) for x in str(r.bid_prices).split(',') if x not in ('', 'nan', 'None')]
        qs = [float(x) for x in str(r.bid_sizes).split(',') if x not in ('', 'nan', 'None')]
        have = {p: q for p, q in zip(ps, qs) if p >= lo_mic}
        prices |= set(have)
        parsed.append((r.venue_ts_ms, r.seq, have))
    rows = [(t, sq, p, have.get(p, 0)) for t, sq, have in parsed for p in prices]
    if rows:
        a = pd.concat([a, pd.DataFrame(rows, columns=a.columns)], ignore_index=True)
    a['size'] = a.size_micros.astype(np.float64) / MIC
    return a.sort_values(['venue_ts_ms', 'seq'], kind='stable').reset_index(drop=True)[['venue_ts_ms', 'seq', 'price_micros', 'size']]


def above_asof(bids, L, times):
    out = np.zeros(len(times))
    ab = bids[bids.price_micros > L]
    for p, g in ab.groupby('price_micros'):
        t = g.venue_ts_ms.to_numpy(np.float64); s = g['size'].to_numpy(np.float64)
        k = np.searchsorted(t, times, side='right') - 1
        out += np.where(k >= 0, s[np.clip(k, 0, None)], 0.0)
    return out


def _cancel(q, amt):
    """sizematch: drop an other order of exactly that size (newest such), else newest-first; returns the unattributed part."""
    others = [i for i, x in enumerate(q) if not x[1]]
    if others:
        cand = [i for i in others if abs(q[i][0] - amt) < 1e-6]
        if cand:
            q.pop(cand[-1])
            return 0.0
    for i in reversed(others):
        if amt <= 1e-9:
            break
        c = min(q[i][0], amt); q[i][0] -= c; amt -= c
    q[:] = [x for x in q if x[0] > 1e-9]
    return amt


def sim_engine(lvl_t, lvl_s, lvl_q, own, prints, t_open, t_cut, R0):
    """Exact port of pkg_v9fp `_fp_simulate` (virtual order). prints: list of (t_event, amt, through, t_venue)."""
    ev = []
    if own is not None:
        own_t, own_c = own
    for t, sq, v in zip(lvl_t, lvl_q, lvl_s):
        if t >= t_cut:
            continue
        if own is not None:
            k = int(np.searchsorted(own_t, t, side='right')) - 1
            if k >= 0:
                v = max(0.0, v - max(0.0, float(own_c[k])))
        ev.append((t, 1, sq, v))
    ev.append((t_open, 2, 0, None))
    for te, amt, thr, tv in prints:
        ev.append((te, 0, 0, (amt, thr, tv)))
    ev.sort(key=lambda e: (e[0], e[1], e[2]))
    q = []; others = 0.0; joined = False; R = float(R0); filled = 0.0; fills = []; qa = np.nan
    for t, typ, sq, val in ev:
        if typ == 2:
            if t_open < t_cut:
                qa = others; q.append([R, True]); joined = True
            continue
        if t >= t_cut and typ == 1:
            break
        if typ == 0:
            amt, thr, tv = val
            if thr:
                amt = 1e18
            while amt > 1e-9 and q:
                c = min(q[0][0], amt)
                if q[0][1]:
                    fills.append((tv, c)); filled += c; R -= c
                else:
                    others -= c
                q[0][0] -= c; amt -= c
                if q[0][0] <= 1e-9:
                    q.pop(0)
            others = max(0.0, others)
            if joined and R <= 1e-9:
                break
            continue
        delta = val - others
        if delta > 1e-9:
            q.append([delta, False]); others += delta
        elif delta < -1e-9:
            left = _cancel(q, -delta)
            others = max(0.0, others - (-delta - left))
    return filled, (fills[0][0] if fills else np.nan), qa


def sim_fp3(lvl, prints, R0, foot_in, t_exit, depth_fill):
    """fillpar3 `replay` (real order enters at its footprint)."""
    ev = [(t, 1, sq, v) for t, sq, v in zip(lvl.venue_ts_ms.to_numpy(), lvl.seq.to_numpy(), lvl['size'].to_numpy())]
    ev += [(tp, 0, -1, (amt, thr, tv)) for tp, amt, thr, tv in prints]
    ev.sort(key=lambda e: (e[0], e[1], e[2]))
    q = []; tot = 0.0; ours_in = False; R = R0; filled = 0.0; fills = []; qa = np.nan
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
                tot -= (-extra - _cancel(q, -extra))
                extra = 0.0
            qa = tot
            q.append([R0, True]); ours_in = True; tot += R0
            if extra > 1e-9:
                q.append([extra, False]); tot += extra
            continue
        delta = new - tot
        if delta > 1e-9:
            q.append([delta, False]); tot += delta
        elif delta < -1e-9:
            left = _cancel(q, -delta)
            tot -= (-delta - left)
            if left > 1e-9 and ours_in and depth_fill:
                for x in q:
                    if x[1]:
                        c = min(x[0], left); x[0] -= c; R -= c; filled += c; fills.append((t, c)); left -= c; tot -= c
                q = [x for x in q if x[0] > 1e-9]
                if R <= 1e-9:
                    break
    return filled, (fills[0][0] if fills else np.nan), qa


def sim_dm(lvl_t, lvl_s, lvl_q, prints, t_join, t_cut, R0, foot_in=None, pre=200.0, post=50.0):
    """Depth-matched fills (proposed rule): a print reaching L (same-token SELL at <= L, complement BUY at >= 1-L, incl. trade-through)
    claims the actual decreases of level L in [print - pre, print + post] ms (earliest first, capped at the print size); a claimed
    decrease consumes the queue FIFO at the DEPTH time (so print lag and sweep size no longer matter); unclaimed decreases are cancels
    (sizematch, else newest first); a print that finds no decrease at all (depth gap) is applied as a print of its size at print - 47 ms.
    lvl_*: level L series (eng: displayed minus own; footprint mode: raw incl. our real order, foot_in = (t, seq) of our footprint row).
    Returns filled, first fill (venue ms of the consuming event), queue ahead at join."""
    n = len(lvl_t)
    dec = np.maximum(0.0, np.r_[0.0, lvl_s[:-1]] - lvl_s) if n else np.zeros(0)
    claim = np.zeros(n)
    extra = []
    for tv, S, atL in prints:
        lo, hi = np.searchsorted(lvl_t, tv - pre, side='left'), np.searchsorted(lvl_t, tv + post, side='right')
        rem = S; got = 0.0
        for k in range(lo, hi):
            c = min(dec[k] - claim[k], rem)
            if c > 1e-9:
                claim[k] += c; rem -= c; got += c
            if rem <= 1e-9: break
        if got <= 1e-9 and atL:
            extra.append((tv - (60.0 - FEED), S))
    ev = [(lvl_t[k], 1, lvl_q[k], k) for k in range(n) if lvl_t[k] < t_cut]
    ev += [(te, 0, 0, S) for te, S in extra if te < t_cut]
    if foot_in is None:
        ev.append((t_join, 2, 0, None))
    ev.sort(key=lambda e: (e[0], e[1], e[2]))
    q = []; others = 0.0; R = float(R0); filled = 0.0; first = np.nan; qa = np.nan; joined = False

    def consume(amt, t):
        nonlocal others, R, filled, first
        while amt > 1e-9 and q:
            c = min(q[0][0], amt)
            if q[0][1]:
                filled += c; R -= c
                if first != first: first = t
            else:
                others -= c
            q[0][0] -= c; amt -= c
            if q[0][0] <= 1e-9: q.pop(0)
        others = max(0.0, others)

    for t, typ, sq, val in ev:
        if typ == 2:
            qa = others; q.append([R, True]); joined = True
            continue
        if typ == 0:
            consume(val, t)
        else:
            k = val
            if claim[k] > 1e-9:
                consume(claim[k], t)
            new = float(lvl_s[k])
            tot = others + (R if (foot_in is not None and joined) else 0.0)
            if foot_in is not None and not joined and (t, sq) == foot_in:
                ex = new - others - R0
                if ex < 0:
                    others = max(0.0, others - (-ex - _cancel(q, -ex))); ex = 0.0
                qa = others; q.append([R, True]); joined = True
                if ex > 1e-9: q.append([ex, False]); others += ex
                continue
            delta = new - tot
            if delta > 1e-9:
                q.append([delta, False]); others += delta
            elif delta < -1e-9:
                left = _cancel(q, -delta)
                others = max(0.0, others - (-delta - left))
        if joined and R <= 1e-9:
            break
    return filled, first, qa


def own_steps(acct, prof):
    k = (acct, prof)
    if k not in OWN:
        f = f'{X}/inputs_{acct}/own_steps2_{prof}.parquet'
        if not os.path.exists(f): f = f'{X}/inputs_{acct}/own_steps_{prof}.parquet'
        if os.path.exists(f):
            df = pd.read_parquet(f).sort_values('venue_ms')
            OWN[k] = {kk: (g['venue_ms'].to_numpy(np.float64), g['delta'].to_numpy(np.float64))
                      for kk, g in df.groupby(['contract_start_ms', 'outcome', 'limit'])}
        else:
            OWN[k] = {}
    return OWN[k]


def run_market(args):
    prof, slot, g = args
    coin, mins = prof.split('_')[0], int(prof.split('_')[1][:-1])
    try:
        d, t = load_market(coin, prof, slot)
    except Exception as e:
        print('LOAD_ERR', prof, slot, repr(e)[:120], flush=True)
        return []
    if d is None:
        return [dict(r._asdict(), no_cache=True) for r in g.itertuples(index=False)]
    lo = int(g.px_mic.min())
    BIDS = {oc: token_bids(d, oc, lo) for oc in sorted(set(g.outcome))}
    if len(t):
        t = t.copy(); t['size'] = t.size_micros.astype(np.float64) / MIC
    out = []
    for r in g.itertuples(index=False):
        oc, L = int(r.outcome), int(r.px_mic)
        bids = BIDS[oc]
        lvl = bids[bids.price_micros == L][['venue_ts_ms', 'seq', 'size']].reset_index(drop=True)
        lvl['venue_ts_ms'] = lvl.venue_ts_ms.astype(np.float64)
        # prints that reach level L of our token: same-token SELL at mp <= L, complement BUY at 1 - q <= L
        if len(t):
            mp = np.where(t.outcome == oc, t.price_micros, MIC - t.price_micros)
            sell_like = ((t.outcome == oc) & (t.trade_side == 1)) | ((t.outcome != oc) & (t.trade_side == 0))
            p0 = t[sell_like.to_numpy() & (mp <= L)].copy(); p0['mp'] = mp[sell_like.to_numpy() & (mp <= L)]
            p0 = p0.sort_values(['venue_ts_ms', 'seq'], kind='stable')
        else:
            p0 = pd.DataFrame(columns=['venue_ts_ms', 'seq', 'size', 'mp'])
        tv = p0.venue_ts_ms.to_numpy(np.float64); sz = p0['size'].to_numpy(np.float64); mpv = p0.mp.to_numpy()
        t_open = float(r.place_venue_ms); t_cut = float(r.t_out)
        res = dict(r._asdict())
        res['lvl_rows'] = len(lvl)
        # --- engine modes
        own_raw = own_steps(r.acct, prof).get((int(slot) * 1000, r.side, round(float(r.px), 2)))
        own_d = (own_raw[0], np.cumsum(own_raw[1])) if own_raw is not None else None
        own_l5 = None
        if own_raw is not None:      # proposed fix: an own placement (+ step) is applied 5 ms early (the depth footprint precedes the user-WS stamp by 1-4 ms)
            t5 = np.where(own_raw[1] > 0, own_raw[0] - 5.0, own_raw[0]); o5 = np.argsort(t5, kind='stable')
            own_l5 = (t5[o5], np.cumsum(own_raw[1][o5]))
        lt, ls, lq = lvl.venue_ts_ms.to_numpy(), lvl['size'].to_numpy(), lvl.seq.to_numpy()
        own_ft = None
        if own_raw is not None:      # proposed fix: every own placement step snapped to its depth footprint (level increase >= size within [-10, +2] ms)
            inc = ls - np.r_[0.0, ls[:-1]] if len(ls) else ls
            tf = own_raw[0].copy()
            for j in range(len(tf)):
                dj = own_raw[1][j]
                if dj <= 0: continue
                m = np.flatnonzero((inc >= dj - 1e-6) & (lt >= tf[j] - 10) & (lt <= tf[j] + 2))
                if len(m): tf[j] = lt[m[np.argmin(np.abs(lt[m] - own_raw[0][j]))]]
            of = np.argsort(tf, kind='stable'); own_ft = (tf[of], np.cumsum(own_raw[1][of]))
        abv_cache = {}
        for mode in ENG_MODES:
            sh = 60.0 if mode == 'eng_sh60' else 60.0 - FEED
            book = mode in ('eng_book', 'eng_thr', 'eng_fix', 'eng_ftfix')
            bounded = mode in ('eng_thr', 'eng_fix', 'eng_ftfix')
            own = None if mode == 'eng_noown' else (own_l5 if mode in ('eng_own5', 'eng_fix') else own_ft if mode in ('eng_ownft', 'eng_ftfix') else own_d)
            if book:
                if sh not in abv_cache:
                    abv_cache[sh] = above_asof(bids, L, tv - sh - 0.5) if len(tv) else np.zeros(0)
                amt = np.maximum(0.0, sz - abv_cache[sh])
            else:
                amt = sz
            prints = []
            for i in range(len(tv)):
                if not (t_open < tv[i] and tv[i] - sh < t_cut):
                    continue
                if mpv[i] == L:
                    prints.append((tv[i] - sh, float(amt[i]), False, tv[i]))
                elif mpv[i] < L:
                    # production: a trade-through empties level L; proposed (bounded): it only consumes size - (bids better than L)
                    prints.append((tv[i] - sh, float(amt[i]), False, tv[i]) if bounded else (tv[i] - sh, 0.0, True, tv[i]))
            f, ff, qa = sim_engine(lt, ls, lq, own, prints, t_open, t_cut, float(r.size))
            res[f'{mode}_fs'] = f; res[f'{mode}_first'] = ff; res[f'{mode}_qa'] = qa
        # --- fp3 modes (real order enters at its footprint)
        prev = lvl['size'].shift(1).fillna(0.0)
        inc = lvl[(lvl['size'] - prev >= float(r.size) - 1e-6) & lvl.venue_ts_ms.between(t_open - 3000, t_open + 3000)]
        lv3 = lvl
        if len(inc):
            k = int((inc.venue_ts_ms - t_open).abs().argmin()); foot_in = (float(inc.venue_ts_ms.iloc[k]), int(inc.seq.iloc[k])); res['foot'] = True
            res['foot_dt'] = float(inc.venue_ts_ms.iloc[k] - t_open)
        else:
            foot_in = (t_open, -1); res['foot'] = False; res['foot_dt'] = np.nan
            lv3 = pd.concat([lvl, pd.DataFrame(dict(venue_ts_ms=[t_open], seq=[-1], size=[np.nan]))], ignore_index=True).sort_values(['venue_ts_ms', 'seq'])
            lv3['size'] = lv3['size'].ffill().fillna(0.0)
            m = (lv3.venue_ts_ms == t_open) & (lv3.seq == -1)
            lv3.loc[m, 'size'] = lv3.loc[m, 'size'] + float(r.size)
        t_exit = (float(r.cancel_venue_ms) - 1) if r.cancel_venue_ms == r.cancel_venue_ms else t_cut
        abv = above_asof(bids, L, tv - 60.0 - 0.5) if len(tv) else np.zeros(0)
        amt = np.maximum(0.0, sz - abv)
        prints = [(tv[i] - 60.0, float(amt[i]), bool(mpv[i] < L), tv[i]) for i in range(len(tv))]
        prints_b = [(tv[i] - 60.0, float(amt[i]), False, tv[i]) for i in range(len(tv))]
        for mode, dfill, pr in (('fp3', 0, prints), ('fp3_df1', 1, prints), ('fp3_thr', 0, prints_b)):
            f, ff, qa = sim_fp3(lv3, pr, float(r.size), foot_in, t_exit, dfill)
            res[f'{mode}_fs'] = f; res[f'{mode}_first'] = ff; res[f'{mode}_qa'] = qa
        # --- depth-matched modes (proposed rule)
        prints_dm = [(tv[i], float(sz[i]), bool(mpv[i] == L)) for i in range(len(tv))]
        if own_ft is not None and len(lt):
            k = np.searchsorted(own_ft[0], lt, side='right') - 1
            adj = np.maximum(0.0, ls - np.where(k >= 0, np.maximum(0.0, own_ft[1][np.clip(k, 0, None)]), 0.0))
        else:
            adj = ls
        f, ff, qa = sim_dm(lt, adj, lq, prints_dm, t_open, t_cut, float(r.size))
        res['eng_dm_fs'] = f; res['eng_dm_first'] = ff; res['eng_dm_qa'] = qa
        f, ff, qa = sim_dm(lv3.venue_ts_ms.to_numpy(np.float64), lv3['size'].to_numpy(np.float64), lv3.seq.to_numpy(), prints_dm, t_open, t_exit, float(r.size), foot_in=foot_in)
        res['fp3_dm_fs'] = f; res['fp3_dm_first'] = ff; res['fp3_dm_qa'] = qa
        out.append(res)
    return out


def live_orders(acct):
    L = pd.read_parquet(f'{X}/inputs_{acct}/live_orders.parquet')
    if 'captured' not in L: return pd.DataFrame()
    L = L[L.captured.fillna(False).astype(bool) & L.place_venue_ms.notna() & (L.phase != 'rejected')].copy()
    L['acct'] = acct
    L['prof'] = L.profile_id
    L['dur_s'] = np.where(L.prof.str.endswith('_5m'), 300, 900)
    L['end_ms'] = (L.slot + L.dur_s) * 1000
    L['t_out'] = np.minimum(L.cancel_venue_ms.fillna(L.end_ms), L.end_ms).astype(float)
    L['outcome'] = np.where(L.side == 'UP', 0, 1)
    L['px'] = L.limit_price.round(2)
    L['px_mic'] = (L.px * 100).round().astype(int) * 10_000
    L['size'] = L['size'].astype(float)
    # live first fill: user-WS server_ms from decoded fills (status 2 = matched), else ledger match_time (floor s)
    try:
        F = pd.read_parquet(f'{X}/decoded/{acct}/fills.parquet')
        F = F[F.status == 2].sort_values('server_ms').drop_duplicates(['run', 'trade_key'])
        a = F.groupby(['run', 'profile', 'ref']).agg(live_first_ws=('server_ms', 'min'), live_ws_qty=('qty', 'sum')).reset_index()
        L = L.merge(a, left_on=['run', 'profile', 'ref'], right_on=['run', 'profile', 'ref'], how='left')
    except Exception as e:
        print('fills', acct, repr(e)[:100]); L['live_first_ws'] = np.nan; L['live_ws_qty'] = np.nan
    T = pd.read_csv(f'{X}/live_{acct}/ledger/trades.csv.gz', low_memory=False)
    T = T.groupby('order_id').match_time.min().rename('live_first_mt') * 1000
    L = L.merge(T, left_on='id', right_index=True, how='left')
    L['live_first'] = L.live_first_ws.fillna(L.live_first_mt)
    res = []
    for prof in L.prof.unique():
        r = pd.read_parquet(f'{X}/resfill/poly_oracle_resolutions_{prof}.parquet')
        r = r[r.resolved.astype(bool)]
        res.append(pd.DataFrame(dict(prof=prof, slot=r.contract_start_ms // 1000, up_wins=r.up_wins_oracle.astype(bool))))
    L = L.merge(pd.concat(res).drop_duplicates(['prof', 'slot']), on=['prof', 'slot'], how='left')
    L['won'] = np.where(L.up_wins.isna(), np.nan, (L.side == 'UP') == L.up_wins.fillna(False))
    keep = ['acct', 'prof', 'slot', 'id', 'run', 'ref', 'side', 'outcome', 'px', 'px_mic', 'size', 'phase', 'fs', 'pnl', 'won', 'place_venue_ms',
            'cancel_venue_ms', 'cancel_reason', 't_out', 'end_ms', 'live_first', 'live_first_ws', 'cancel_attempts', 'arm_id']
    for c in keep:
        if c not in L: L[c] = np.nan
    return L[keep]


def main():
    W = int(sys.argv[1]) if len(sys.argv) > 1 else 32
    parts = [live_orders(a) for a in ('account_a', 'account_b', 'account_c')]
    od = pd.concat([p for p in parts if len(p)], ignore_index=True)
    print('orders', od.groupby('acct').size().to_dict(), flush=True)
    tasks = [(prof, int(slot), g) for (prof, slot), g in od.groupby(['prof', 'slot'])]
    if os.environ.get('RP_LIMIT'):
        step = max(1, len(tasks) // int(os.environ['RP_LIMIT']))
        tasks = tasks[::step][:int(os.environ['RP_LIMIT'])]
    rows = []
    with ProcessPoolExecutor(W) as ex:
        for i, res in enumerate(ex.map(run_market, tasks, chunksize=4)):
            rows.extend(res)
            if i % 200 == 0: print('markets', i, len(tasks), flush=True)
    R = pd.DataFrame(rows)
    R.to_parquet(f'{OUT}/orders_all.parquet')
    for a, g in R.groupby('acct'):
        g.to_parquet(f'{OUT}/orders_{a}.parquet')
    print('done', len(R))


if __name__ == '__main__':
    main()
