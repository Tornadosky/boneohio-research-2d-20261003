"""Read-only extraction of the complete bounded A2 execution-parity cohort.

Run on Viper with --output in the new task directory. Original outputs unchanged.
Only allowlisted public clocks/order economics leave the original audit.
"""
import argparse
import ast
import hashlib
import json
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq


def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def canonical_ast(node):
    if isinstance(node,ast.AST):
        return dict(type=type(node).__name__,fields={key:canonical_ast(value) for key,value in ast.iter_fields(node)
                                                   if value is not None and value!=[]})
    if isinstance(node,list):
        return [canonical_ast(value) for value in node]
    return node


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--source-root', type=Path, required=True)
    p.add_argument('--depth-root', type=Path)
    a = p.parse_args()
    origin = a.source_root
    source = origin / 'analysis/exec_parity.parquet'
    views = origin / 'exact/exact_views.parquet'
    cols = ['box', 'event', 'order_id', 'cs', 'sched', 'fav_yes', 'submitted_limit',
            'principal_usd', 'taking', 'making', 'fav_won', 'fee_bt', 'pnl_bt']
    delays = (0,25,50,75,100,125,150,175,200,225,250,275,300,350,400,500)
    cols += [f'bt_{kind}{delay}' for delay in delays for kind in ('sh', 'cost')]
    orders = pd.read_parquet(source, columns=cols)
    viewcols = ['contract_start_ms', 'decide_ms', 'is_yes'] + [
        f'a{delay}_{kind}{level}' for delay in delays for kind in ('px','sz') for level in range(4)]
    low, high = orders.cs.min(), orders.cs.max()
    batches = []
    for batch in pq.ParquetFile(views).iter_batches(columns=viewcols, batch_size=16384):
        frame = batch.to_pandas()
        frame = frame[(frame.contract_start_ms >= low) & (frame.contract_start_ms <= high)]
        if len(frame):
            batches.append(frame)
    candidates = pd.concat(batches, ignore_index=True)
    if candidates.duplicated(['contract_start_ms','decide_ms']).any():
        raise RuntimeError('Side-ambiguous original view keys')
    result = orders.merge(candidates, left_on=['cs','sched'], right_on=['contract_start_ms','decide_ms'],
                          how='left', validate='many_to_one')
    if result.decide_ms.isna().any() or (result.is_yes == result.fav_yes).any():
        raise RuntimeError('Missing or opposite-orientation A2 execution inputs')
    a.output.mkdir(parents=True, exist_ok=True)
    target = a.output / 'a2_execution_inputs.parquet'
    result.to_parquet(target, index=False, compression='zstd')
    manifest = dict(description='All 483 archived A2 execpar observations, public economic allowlist plus exact arrival inputs',
                    orders=len(result), delays=list(delays), source=[dict(path=str(source),sha256=sha(source)),
                    dict(path=str(views),sha256=sha(views))], output=dict(file=target.name,sha256=sha(target)))
    source_code = Path(__file__).resolve().parents[2]/'tailtaker_20261003_A2PARITY'
    # Remote source files can be supplied independently from data roots.
    if source_code.exists():
        manifest['original_function_ast_sha256'] = {}
        for filename,names in [('execpar.py',('fak',)),('exec_exact.py',('execute','hold_ms','snap_lat')),
                               ('exact_book.py',('Book','parse_arr','views'))]:
            tree = ast.parse((source_code/filename).read_text())
            for node in tree.body:
                if isinstance(node,(ast.FunctionDef,ast.ClassDef)) and node.name in names:
                    payload = json.dumps(canonical_ast(node),sort_keys=True,separators=(',',':')).encode()
                    value = hashlib.sha256(payload).hexdigest()
                    manifest['original_function_ast_sha256'][f'{filename}:{node.name}'] = value
    (a.output/'a2_fixture_provenance.json').write_text(json.dumps(manifest, indent=2)+'\n')
    if a.depth_root:
        cs = int(result.cs.max())
        files = sorted((a.depth_root/'normalized_depth_events'/ 'market_key=btc_5m'/f'contract_start_ms={cs}').rglob('*.parquet'))
        # A chronological source prefix, never outcome-picked. The normalized
        # file stores delta/snapshot blocks separately; physical-row prefixes
        # would omit the earlier seed snapshots and be an invalid book fixture.
        dcols = ['venue_ts_ms','ts_ns','seq','is_yes','event_type','side','price_micros','size_micros',
                 'best_bid_micros','best_ask_micros','bid_prices','bid_sizes','ask_prices','ask_sizes']
        batches = []
        for path in files:
            batches.append(pq.ParquetFile(path).read(columns=dcols).to_pandas())
        ordered = pd.concat(batches,ignore_index=True).sort_values(['venue_ts_ms','seq']).reset_index(drop=True)
        first_seed = int(ordered.index[ordered.event_type=='snapshot'][0])
        depth = ordered.iloc[first_seed:first_seed+4096]
        dt = a.output/'real_depth_prefix.parquet'
        depth.to_parquet(dt,index=False,compression='zstd')
        dm = dict(contract_start_ms=cs,market_key='btc_5m',selection='Latest contract in archived A2 execution cohort; 4096 chronological rows beginning at first observed snapshot; earlier unseeded deltas excluded',
                  columns=dcols,rows=len(depth),source=[dict(file=p.name,sha256=sha(p)) for p in files],output=dict(file=dt.name,sha256=sha(dt)))
        (a.output/'depth_fixture_provenance.json').write_text(json.dumps(dm,indent=2)+'\n')
    print(json.dumps(dict(orders=len(result),bytes=target.stat().st_size)))


if __name__ == '__main__':
    main()
