"""Exact function extraction from Q99 decision replay (2026-10-03).

See vendor/maker/SOURCE_MANIFEST.json and docs/MAKER_REFERENCE.md.
No source runner is imported: its original hard-coded output mkdir is avoided.
"""
import numpy as np
import pandas as pd
MIC = 1_000_000
FEED = 13.0


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
