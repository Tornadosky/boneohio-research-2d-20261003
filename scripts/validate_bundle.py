#!/usr/bin/env python3
"""Verify downloaded file hashes and the declared chronological coverage."""
import argparse,hashlib,json
from pathlib import Path
import pyarrow.parquet as pq
import pyarrow.compute as pc

ROOT=Path(__file__).resolve().parents[1]

def main():
    ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('--require-all',action='store_true');args=ap.parse_args()
    manifest=json.loads((ROOT/'data_manifest.json').read_text())
    n=missing=0
    for r in manifest['files']:
        f=ROOT/r['path']
        if not f.exists():missing+=1;continue
        h=hashlib.sha256()
        with f.open('rb') as stream:
            for b in iter(lambda:stream.read(4*1024*1024),b''):h.update(b)
        assert f.stat().st_size==r['bytes'],r['path']+' size'
        assert h.hexdigest()==r['sha256'],r['path']+' SHA256'
        if f.suffix=='.parquet':
            pf=pq.ParquetFile(f)
            if 'rows' in r:assert pf.metadata.num_rows==r['rows'],r['path']+' rows'
            if r['path'].startswith('data/features/poly_grid_100ms/'):
                t=pf.read(columns=['has_prior_state','query_ms','source_cache_ns'])
                prior=t.filter(t['has_prior_state'])
                assert bool(pc.all(pc.less(prior['source_cache_ns'],pc.multiply(prior['query_ms'],1000000))).as_py()),r['path']+' future cache state'
                unavailable=t.filter(pc.invert(t['has_prior_state']))['source_cache_ns']
                assert unavailable.null_count==len(unavailable),r['path']+' unmasked unavailable state'
        n+=1
    if args.require_all and missing:raise AssertionError(f'{missing} required files missing')
    for source in ['vendor/maker/SOURCE_MANIFEST.json','vendor/taker/SOURCE_MANIFEST.json']:
        # Their differential tests check distributed code and fixture fingerprints.
        if not (ROOT/source).exists():print('Optional source manifest absent:',source)
    cov=ROOT/'data/coverage.parquet'
    if cov.exists():
        d=pq.ParquetFile(cov).read().to_pandas();s=d[~d.diagnostic]
        assert len(s)==768 and not s.duplicated(['market_key','contract_start_ms']).any()
        for mk,expected in [('btc_5m',576),('btc_15m',192)]:assert (s.market_key==mk).sum()==expected
        print('Coverage: 768 contracts; qualified grid fraction:',round(s.qualified_grid_rows.sum()/s.grid_rows.sum(),5))
    print(f'Verified {n} files; {missing} not downloaded (selection supported).')

if __name__=='__main__':main()
