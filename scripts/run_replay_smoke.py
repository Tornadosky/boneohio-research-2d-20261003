#!/usr/bin/env python3
"""Run declared synthetic orders against actual exported books, one per tenor.

This is an integration check, not a fitted BoneOhio rule or live fill parity.
"""
import json
from pathlib import Path
import pyarrow.parquet as pq
from bonebundle.maker import MakerOrder,load_market,replay_order
from bonebundle.taker import DepthReplay,match_budget_fak

ROOT=Path(__file__).resolve().parents[1]

def main():
    cov=pq.ParquetFile(ROOT/'data/coverage.parquet').read().to_pandas()
    rows=[]
    for mk in ['btc_5m','btc_15m']:
        chosen=None
        for c in cov[(~cov.diagnostic)&cov.market_key.eq(mk)&cov.qualified_grid_rows.gt(0)].sort_values('contract_start_ms').itertuples():
            cs=int(c.contract_start_ms);folder=ROOT/'data/poly/normalized_depth_events'/f'market_key={mk}'/f'contract_start_ms={cs}'
            if list(folder.glob('session_id=*/*.parquet')):chosen=cs;break
        if chosen is None:raise FileNotFoundError('Fetch poly shards for '+mk+' before replay smoke')
        cs=chosen;ce=cs+(300000 if mk=='btc_5m' else 900000)
        grid=pq.ParquetFile(ROOT/'data/features/poly_grid_100ms'/f'market_key={mk}'/f'contract_start_ms={cs}'/'grid.parquet').read().to_pandas()
        queries=grid[grid.book_valid & grid.query_ms.ge(cs+30000)&grid.query_ms.lt(ce-10000)].query_ms.astype('int64').tolist()
        replay=DepthReplay.from_parquet(ROOT/'data/poly',mk,cs,clock='venue',coverage_ok=True)
        taker=None;when=None;resting_price=None
        for query in queries[::10]:
            book=replay.book(True,int(query),strict=True)
            result=match_budget_fak(book,25.,.99)
            if result.status!='UNQUALIFIED' and book.bids and book.asks and 0<book.bids[0][0]<book.asks[0][0]:
                taker=result;when=int(query);resting_price=int(round(book.bids[0][0]*1000000));break
        if taker is None:raise AssertionError('No qualified real venue book found for smoke '+mk)
        depth,trades=load_market(ROOT/'data/poly',mk,cs)
        order=MakerOrder(0,resting_price,25.,when,min(when+10000,ce),order_id='synthetic-resting-smoke')
        maker={}
        for mode in ['eng','eng_ftfix']:
            result=replay_order(depth,trades,order,mode=mode)
            assert 0<=result.simulated_shares<=25.+1e-8
            maker[mode]=result.to_dict()
        assert 0<=taker.principal_cost<=25.+1e-6
        rows.append({'market_key':mk,'contract_start_ms':cs,'declared_venue_ms':when,'taker_status':taker.status,'taker_shares':taker.matched_shares,'taker_principal':taker.principal_cost,'maker':maker})
    out=ROOT/'results/replay_smoke';out.mkdir(parents=True,exist_ok=True)
    (out/'SUMMARY.json').write_text(json.dumps({'purpose':'integration only; synthetic BUYs on actual data','cases':rows},indent=2))
    print(json.dumps(rows,indent=2))

if __name__=='__main__':main()
