"""Read CHD hourly Binance USD-M L2, retaining E, T and observer receipt clocks.

Derived from the latest A2PARITY chd_top_v2.py reconstruction. Whole-hour
snapshot and absolute updates remain available in the raw bundle; this iterator
adds configurable levels without silently equating publication and receipt.
"""
from dataclasses import dataclass
from pathlib import Path
import math
from numbers import Integral


@dataclass(frozen=True)
class BinanceL2View:
    event_ms: int | None
    transaction_ms: int | None
    recv_ns: int | None
    final_update_id: int | None
    bids: tuple
    asks: tuple
    is_snapshot: bool
    clock_qualified: bool
    sequence_status: str
    source_message_ordinal: int

    @property
    def mid(self):
        return (self.bids[0][0]+self.asks[0][0])/2 if self.bids and self.asks else None

    @property
    def qualified(self):
        return self.clock_qualified and self.sequence_status in ('SNAPSHOT_SEED','PREVIOUS_ID_CHAIN_MATCH')


def _integer(value):
    if isinstance(value,bool):
        return None
    if isinstance(value,Integral):
        return int(value) if value>=0 else None
    if isinstance(value,str) and value.isdigit():
        return int(value)
    try:
        number = float(value)
        if not math.isfinite(number) or number<0 or number>2**53 or number!=int(number):
            return None
        return int(number)
    except (ValueError,TypeError,OverflowError):
        return None


def reconstruct_binance_l2(rows, *, levels=10, only_top_changes=False):
    """Yield message-level L2 after an hourly snapshot then each diff.

    BTCUSDT source price quantum is 0.1 USD, matching the dated A2 array replay.
    Depth is kept in full internally. ``levels=None`` emits all carried levels.
    Sorting by update-id reproduces the source reconstruction, not a synthetic
    receive-clock chronology. Missing previous-update-id fields leave chain
    continuity UNKNOWN; a sequence mismatch, when fields exist, stays GAP until
    a fresh snapshot. No interpolation, automatic gap repair or constant latency.
    """
    if levels is not None and (isinstance(levels,bool) or not isinstance(levels,int) or levels<=0):
        raise ValueError('levels must be positive integer or None for all')
    raw = [dict(row) for row in rows]
    snapshots = [r for r in raw if r.get('event_type')=='snapshot']
    updates = [r for r in raw if r.get('event_type')!='snapshot']
    if not snapshots:
        raise ValueError('hour lacks its full seed snapshot')
    snapshot_messages = {(r.get('received_time'),r.get('final_update_id'),r.get('last_update_id')) for r in snapshots}
    if len(snapshot_messages)!=1:
        raise ValueError('multiple snapshot messages require a reset-aware source adapter; A2 hourly format requires one checkpoint')
    if any(_integer(r.get('final_update_id')) is None for r in updates):
        raise ValueError('unknown diff final_update_id')
    snapshots.sort(key=lambda r:_integer(r.get('final_update_id')) or 0)
    updates.sort(key=lambda r:int(r['final_update_id']))
    groups = [snapshots]
    for row in updates:
        if len(groups)==1 or groups[-1][0]['final_update_id']!=row['final_update_id']:
            groups.append([row])
        else:
            groups[-1].append(row)
    bids,asks = {},{}
    previous = None
    chain_gap = False
    last_top = None
    for ordinal,group in enumerate(groups):
        first = group[0]
        is_snapshot = ordinal==0
        final_id = _integer(first.get('final_update_id'))
        prev_field = next((c for c in ('previous_final_update_id','prev_final_update_id','previous_update_id','pu') if c in first),None)
        if is_snapshot:
            bids.clear(); asks.clear(); chain_gap=False
            sequence = 'SNAPSHOT_SEED'
        elif prev_field is not None:
            prev_id = _integer(first.get(prev_field))
            if previous is not None and prev_id!=previous:
                chain_gap = True
            sequence = 'GAP' if chain_gap else 'PREVIOUS_ID_CHAIN_MATCH'
        else:
            sequence = 'GAP' if chain_gap else 'UNKNOWN_NO_PREVIOUS_ID'
        for row in group:
            side = row.get('side')
            if side not in ('bid','ask'):
                raise ValueError('unknown L2 side')
            p,q = float(row['price']),float(row['quantity'])
            if not math.isfinite(p) or not math.isfinite(q) or p<=0 or q<0:
                raise ValueError('invalid L2 price/quantity')
            tick = int(round(p*10))
            if abs(tick/10-p)>1e-6:
                raise ValueError('BTCUSDT L2 price is off the source 0.1 USD grid')
            d = bids if side=='bid' else asks
            if q:
                d[tick] = q
            else:
                d.pop(tick,None)
        previous = final_id
        if not bids or not asks:
            continue
        bp,ap = max(bids),min(asks)
        top = (bp,ap,bids[bp],asks[ap])
        if bp>=ap:
            # The dated reference drops crossed rows after rebuilding. Keep the
            # same policy; never expose a crossed book as a usable feature.
            continue
        if only_top_changes and top==last_top:
            continue
        last_top = top
        es = {_integer(r.get('event_time')) for r in group}
        ts = {_integer(r.get('transaction_time')) for r in group}
        rs = {_integer(r.get('received_time')) for r in group}
        event = next(iter(es)) if len(es)==1 else None
        transaction = next(iter(ts)) if len(ts)==1 else None
        receipt = next(iter(rs)) if len(rs)==1 else None
        clock_ok = event is not None and event>0 and receipt is not None and receipt>0
        # Preserve a missing T as None: substituting E would invent trade time.
        b = sorted(bids.items(),reverse=True)
        a = sorted(asks.items())
        if levels is not None:
            b,a = b[:levels],a[:levels]
        yield BinanceL2View(event,transaction,receipt,final_id,
            tuple((p/10,q) for p,q in b),tuple((p/10,q) for p,q in a),
            is_snapshot,clock_ok,sequence,ordinal)


def read_binance_l2_hour(path, *, levels=10, only_top_changes=False):
    """Bound memory by reading a single original hourly Parquet checkpoint file.

    Whole-Parquet zstd wrappers in older CHD hours are detected. This returns an
    iterator: process one hour at a time instead of concatenating all raw hours.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    path = Path(path)
    source = path
    with path.open('rb') as stream:
        if stream.read(4)==bytes([0x28,0xB5,0x2F,0xFD]):
            source = pa.BufferReader(pa.input_stream(str(path),compression='zstd').read())
    schema = pq.ParquetFile(source).schema_arrow
    required = ['received_time','event_time','transaction_time','event_type','final_update_id','side','price','quantity']
    missing = [c for c in required if c not in schema.names]
    if missing:
        raise ValueError(f'Unsupported CHD L2 schema: missing {missing}')
    optional = [c for c in ('previous_final_update_id','prev_final_update_id','previous_update_id','pu','first_update_id') if c in schema.names]
    rows = pq.ParquetFile(source).read(columns=required+optional).to_pylist()
    return reconstruct_binance_l2(rows,levels=levels,only_top_changes=only_top_changes)
