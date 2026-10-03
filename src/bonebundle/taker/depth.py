"""Event replay derived from A2PARITY exact_book.py, with exact-clock guards.

No venue time is fabricated from cache timestamp time. Updates are absolute sizes;
snapshots replace both ladders for their token. Carried BBO prunes inconsistent
top levels just as the dated A2 reference does. Full depth is retained.
"""
from dataclasses import dataclass
import json
from pathlib import Path

SCALE = 1_000_000


@dataclass(frozen=True)
class BookView:
    is_yes: bool
    match_ms: int
    source_ms: int | None
    clock_basis: str
    asks: tuple[tuple[float, float], ...]
    bids: tuple[tuple[float, float], ...] = ()
    qualified: bool = True
    reason: str = 'RESEARCH_DEPTH_VIEW'
    strict_before: bool = True
    contract_id: str = ''
    complete: bool = True
    source_venue_ms: int | None = None


def _array(value):
    if value is None:
        return []
    if isinstance(value, str):
        if value in ('', 'nan', 'None'):
            return []
        return json.loads(value) if value.startswith('[') else [int(float(x)) for x in value.split(',') if x]
    return list(value)


class DepthReplay:
    """Ascending causal queries against normalized_depth_events for one contract.

    Venue matching uses venue timestamps. Legacy ``clock='receive'`` selects
    preserved ts_ns; pflow ts_ns is a normalized cache clock, NOT certified actual
    recorder receipt. It supports a declared cache-availability scenario only,
    never an exchange-arrival ladder. Same-ms venue ordering is seq.
    ``coverage_ok`` must come from the bundle coverage manifest, not a PnL check.
    """
    def __init__(self, events, *, clock='venue', coverage_ok=True, contract_id=''):
        if clock not in ('venue', 'receive'):
            raise ValueError('clock must be venue or receive')
        self.clock = clock
        self.contract_id = str(contract_id)
        self.reason = None if coverage_ok else 'COVERAGE_GAP'
        self.events = []
        self.levels = {(y,s): {} for y in (True, False) for s in ('bid','ask')}
        self.bbo = {(y,s): None for y in (True, False) for s in ('bid','ask')}
        self.seeded = {True: False, False: False}
        self.last_ns = {True: None, False: None}
        self.last_venue_ms = {True: None, False: None}
        self.last_top_change_ns = None
        self.index = 0
        self.query_ns = -1
        for ordinal, raw in enumerate(events):
            e = dict(raw)
            venue = e.get('venue_ts_ms')
            receipt = e.get('ts_ns')
            if clock == 'venue':
                if not isinstance(venue, int) or isinstance(venue, bool) or venue <= 0:
                    self.reason = 'UNKNOWN_VENUE_TIMESTAMP'
                    continue
                stamp = venue * SCALE
            else:
                if not isinstance(receipt, int) or isinstance(receipt, bool) or receipt <= 0:
                    self.reason = 'UNKNOWN_RECEIVE_TIMESTAMP'
                    continue
                stamp = receipt
            self.events.append((stamp, int(e.get('seq', ordinal)), ordinal, e))
        self.events.sort(key=lambda item: item[:3])

    @classmethod
    def from_parquet(cls, poly_root, market_key, contract_start_ms, **kwargs):
        import pyarrow.parquet as pq
        root = Path(poly_root) / 'normalized_depth_events' / f'market_key={market_key}' / f'contract_start_ms={int(contract_start_ms)}'
        files = sorted(root.rglob('*.parquet'))
        rows = []
        for path in files:
            rows.extend(pq.ParquetFile(path).read().to_pylist())
        kwargs.setdefault('contract_id', f'{market_key}:{int(contract_start_ms)}')
        kwargs['coverage_ok'] = bool(kwargs.get('coverage_ok', True)) and bool(files)
        return cls(rows, **kwargs)

    def _apply(self, stamp, e):
        try:
            previous_top = self._top_signature()
            y = e['is_yes']
            if not isinstance(y, bool):
                raise ValueError('invalid token side')
            if e['event_type'] == 'snapshot':
                for side, pc, qc in (('bid','bid_prices','bid_sizes'),('ask','ask_prices','ask_sizes')):
                    prices, sizes = _array(e[pc]), _array(e[qc])
                    if len(prices) != len(sizes):
                        raise ValueError('snapshot length mismatch')
                    d = {}
                    for p,q in zip(prices,sizes):
                        p,q = int(p),int(q)
                        if not 0 <= p <= SCALE or q < 0 or p in d:
                            raise ValueError('invalid snapshot level')
                        if q > 0:
                            d[p] = q / SCALE
                    self.levels[(y,side)] = d
                self.seeded[y] = True
            elif e.get('side') in ('bid','ask'):
                side = e['side']
                p,q = int(e['price_micros']),int(e['size_micros'])
                if not 0 <= p <= SCALE or q < 0:
                    raise ValueError('invalid absolute level')
                d = self.levels[(y,side)]
                if q:
                    d[p] = q / SCALE
                else:
                    d.pop(p,None)
            for side,col in (('bid','best_bid_micros'),('ask','best_ask_micros')):
                value = e.get(col)
                if value is not None and int(value) >= 0:
                    if int(value) > SCALE:
                        raise ValueError('invalid BBO')
                    self.bbo[(y,side)] = int(value)
            self.last_ns[y] = stamp
            self.last_venue_ms[y] = int(e['venue_ts_ms']) if int(e.get('venue_ts_ms') or 0)>0 else None
            if self._top_signature()!=previous_top:
                self.last_top_change_ns = stamp
        except (KeyError, TypeError, ValueError, OverflowError):
            self.reason = 'INVALID_DEPTH_EVENT'

    def _top_signature(self):
        signature = []
        for y in (True,False):
            for side in ('bid','ask'):
                bbo = self.bbo[(y,side)]
                levels = self.levels[(y,side)]
                if bbo is not None:
                    signature.append((bbo,levels.get(bbo,0.)))
                else:
                    # Initial seed without carried BBO: only this rare path
                    # scans depth. Ordinary level updates remain constant cost.
                    items = [(p,q) for p,q in levels.items() if q>0]
                    signature.append((max(items) if side=='bid' else min(items)) if items else None)
        return tuple(signature)

    def top_age_ms(self, decision_ms, *, feed_lag_ms=0):
        """A2-style age of either token's last top price OR size change.

        Apply a declared observation-lag scenario to venue E. A fixed lag is a
        sensitivity convention, not a recovered actual observer arrival clock.
        """
        if isinstance(feed_lag_ms,bool) or not isinstance(feed_lag_ms,int) or feed_lag_ms<0:
            raise ValueError('nonnegative exact integer feed_lag_ms required')
        self.asof(decision_ms-feed_lag_ms,strict=False)
        if self.reason or not all(self.seeded.values()) or self.last_top_change_ns is None:
            return None
        return (decision_ms*1_000_000-self.last_top_change_ns)/1_000_000-feed_lag_ms

    def asof(self, target_ms, *, strict=True):
        if isinstance(target_ms, bool) or not isinstance(target_ms, int) or target_ms < 0:
            raise ValueError('exact integer target_ms required')
        target_ns = target_ms * SCALE
        # A strict query at t may be followed by an inclusive query at t; the
        # reverse would have already consumed forbidden same-time components.
        watermark = target_ns - 1 if strict else target_ns
        if watermark < self.query_ns:
            raise ValueError('DepthReplay queries must ascend; construct a new replay to rewind')
        self.query_ns = watermark
        while self.index < len(self.events) and self.events[self.index][0] <= watermark:
            stamp,_,_,e = self.events[self.index]
            self._apply(stamp,e)
            self.index += 1
        return self

    def book(self, is_yes, match_ms, *, strict=True, max_book_age_ms=1000):
        if not isinstance(is_yes,bool):
            raise ValueError('is_yes must be bool')
        if isinstance(max_book_age_ms,bool) or not isinstance(max_book_age_ms,int) or max_book_age_ms<0:
            raise ValueError('explicit nonnegative integer maximum age required')
        self.asof(match_ms,strict=strict)
        reason = self.reason
        if not all(self.seeded.values()):
            reason = reason or 'UNSEEDED_TOKEN'
        latest = self.last_ns[is_yes]
        source_ms = latest // SCALE if latest is not None else None
        if latest is None:
            reason = reason or 'NO_TOKEN_DEPTH'
        elif match_ms * SCALE - latest > max_book_age_ms * SCALE:
            reason = reason or 'STALE_TOKEN_DEPTH'
        ladders = {}
        for side in ('bid','ask'):
            bbo = self.bbo[(is_yes,side)]
            items = [(p / SCALE,q) for p,q in self.levels[(is_yes,side)].items()
                     if q>0 and (bbo is None or (p<=bbo if side=='bid' else p>=bbo))]
            items.sort(reverse=side=='bid')
            ladders[side] = tuple(items)
        if ladders['bid'] and ladders['ask'] and ladders['bid'][0][0] >= ladders['ask'][0][0]:
            reason = reason or 'CROSSED_BOOK'
        return BookView(is_yes,match_ms,source_ms,self.clock,ladders['ask'],ladders['bid'],
                        qualified=reason is None,reason=reason or 'RESEARCH_DEPTH_VIEW',
                        strict_before=strict,contract_id=self.contract_id,
                        source_venue_ms=self.last_venue_ms[is_yes])
