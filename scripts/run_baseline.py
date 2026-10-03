#!/usr/bin/env python3
"""Build fixed-time paired observations using strict declared source clocks.

Poly uses normalized cache time, Binance genuine native receipt timestamps.
Target is a later API-dated public BUY fill, not entry/submission intent.
Absences are censored controls. These descriptive correlations do not identify
the trader's rule and are not an execution/PnL backtest.
"""
import argparse,json
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from bonebundle.data import strict_asof_indices

ROOT=Path(__file__).resolve().parents[1]

def read_files(files,cols):
    return pd.concat([pq.ParquetFile(f).read(columns=cols).to_pandas() for f in files],ignore_index=True)

def main():
    ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('--data-root',type=Path,default=ROOT/'data');ap.add_argument('--output',type=Path,default=ROOT/'results/baseline');ap.add_argument('--label-guard-ms',type=int,default=5000);args=ap.parse_args()
    if args.label_guard_ms<0:raise ValueError('Label guard must be nonnegative')
    root=args.data_root
    mids=read_files(sorted((root/'feeds/binance_recorder/binance_mids').glob('*.parquet')),['ts_ns','mid','bid_qty_steps','ask_qty_steps']).sort_values('ts_ns',kind='stable')
    mt=mids.ts_ns.to_numpy();mp=mids.mid.to_numpy()
    trades=read_files(sorted((root/'feeds/binance_recorder/binance_trades').glob('*.parquet')),['ts_ns','agg_id','price','qty','is_buyer_maker']).sort_values('ts_ns',kind='stable')
    duplicates=int(trades.duplicated('agg_id').sum())
    repeated=trades[trades.duplicated('agg_id',keep=False)]
    if len(repeated) and bool(repeated.groupby('agg_id')[['price','qty','is_buyer_maker']].nunique().gt(1).any().any()):raise ValueError('Conflicting aggTrade economic fields; investigate source witnesses')
    trades=trades.drop_duplicates('agg_id',keep='first')
    # A gap becomes observable only when its later id has arrived; not a complete capture certificate.
    byid=trades.sort_values('agg_id',kind='stable');gaps=byid[byid.agg_id.diff().gt(1)].ts_ns.sort_values().to_numpy()
    tt=trades.ts_ns.to_numpy();q=trades.qty.to_numpy();signed=q*np.where(trades.is_buyer_maker,-1,1)
    cumq=np.r_[0,np.cumsum(q)];cumsigned=np.r_[0,np.cumsum(signed)]
    fills=pq.ParquetFile(ROOT/'context/wallet/fresh_fills_two_days.parquet').read().to_pandas()
    keys=fills.slug.str.extract(r'^btc-updown-(5m|15m)-(\d+)$')
    fills=fills.assign(market_key=keys[0].map({'5m':'btc_5m','15m':'btc_15m'}),cs=pd.to_numeric(keys[1],errors='coerce')*1000,fill_ms=fills.timestamp*1000)
    fills=fills[fills.market_key.notna() & fills.side.eq('BUY')]
    bymarket={(mk,int(cs)):d for (mk,cs),d in fills.groupby(['market_key','cs'])}
    records=[]
    for f in sorted((root/'features/poly_grid_100ms').glob('market_key=*/contract_start_ms=*/grid.parquet')):
        grid=pq.ParquetFile(f).read().to_pandas();mk=str(grid.market_key.iloc[0]);cs=int(grid.contract_start_ms.iloc[0]);duration=300000 if mk=='btc_5m' else 900000;ce=cs+duration
        observed=bymarket.get((mk,cs),fills.iloc[0:0])
        # Prespecified offsets and an expiry-relative point; neither uses final outcomes.
        for offset in sorted(set([15000,30000,60000,120000,duration-30000])):
            when=cs+offset;g=grid[grid.query_ms.eq(when)].iloc[0];ns=when*1000000
            mi=int(strict_asof_indices(mt,[ns])[0]);mid=mp[mi] if mi>=0 else np.nan
            opening=int(strict_asof_indices(mt,[cs*1000000])[0]);open_mid=mp[opening] if opening>=0 else np.nan
            common={'market_key':mk,'contract_start_ms':cs,'query_ms':when,'day':pd.Timestamp(cs,unit='ms',tz='UTC').strftime('%Y-%m-%d'),'seconds_left':(ce-when)/1000,'poly_book_valid':bool(g.book_valid),'poly_cache_age_ms':float(g.state_age_ms),'binance_age_ms':(ns-mt[mi])/1e6 if mi>=0 else np.nan,'binance_open_age_ms':(cs*1000000-mt[opening])/1e6 if opening>=0 else np.nan,'binance_mid':mid,'binance_move_from_open_bps':(mid/open_mid-1)*10000}
            if mi>=0:
                b,a=mids.iloc[mi][['bid_qty_steps','ask_qty_steps']]
                common['binance_top_imbalance']=(b-a)/(b+a) if b+a else np.nan
            for horizon in [100,500,1000,2000,5000]:
                pi=int(strict_asof_indices(mt,[ns-horizon*1000000])[0]);p=mp[pi] if pi>=0 else np.nan
                common[f'binance_return_{horizon}ms_bps']=(mid/p-1)*10000
                common[f'binance_return_endpoint_age_{horizon}ms']=(ns-horizon*1000000-mt[pi])/1e6 if pi>=0 else np.nan
            hi=np.searchsorted(tt,ns,side='left')
            for horizon in [1000,5000]:
                lo=np.searchsorted(tt,ns-horizon*1000000,side='left');volume=cumq[hi]-cumq[lo]
                common[f'binance_flow_{horizon}ms']=(cumsigned[hi]-cumsigned[lo])/volume if volume else np.nan
                common[f'binance_volume_{horizon}ms']=volume
                common[f'observed_agg_id_gaps_{horizon}ms']=int(np.searchsorted(gaps,ns,side='left')-np.searchsorted(gaps,ns-horizon*1000000,side='left'))
                span=np.r_[ns-horizon*1000000,tt[lo:hi],ns]
                common[f'largest_trade_receipt_interval_{horizon}ms']=float(np.diff(span).max()/1e6)
            for outcome,prefix in [(0,'yes'),(1,'no')]:
                future=observed[observed.outcome_index.eq(outcome)&observed.fill_ms.ge(when+args.label_guard_ms)&observed.fill_ms.lt(ce)]
                records.append(common|{'outcome_index':outcome,'bid':float(g[prefix+'_bid'])/1e6,'ask':float(g[prefix+'_ask'])/1e6,'bid_size':float(g[prefix+'_bid_size'])/1e6,'ask_size':float(g[prefix+'_ask_size'])/1e6,'observed_api_later_buy_fill':int(len(future)>0),'observed_api_later_fill_count':len(future),'observed_api_later_fill_shares':float(future['size'].sum()),'observed_api_later_taker_shares':float(future.loc[future.api_role.eq('TAKER'),'size'].sum())})
    d=pd.DataFrame(records)
    if d.empty:raise RuntimeError('Download groups metadata,features,recorder first')
    d['qualified']=d.poly_book_valid & d.binance_age_ms.between(0,1000)
    d['return_endpoints_fresh']=d[[c for c in d if c.startswith('binance_return_endpoint_age_')]].le(1000).all(axis=1)&d.binance_open_age_ms.between(0,1000)
    d['no_observed_flow_id_gap']=d.observed_agg_id_gaps_5000ms.eq(0)
    d['feature_available']=d.qualified & d.return_endpoints_fresh & d.no_observed_flow_id_gap
    d['bid_bucket']=pd.cut(d.bid,[-np.inf,.5,.9,.97,.985,np.inf],labels=['<=.5','.5-.9','.9-.97','.97-.985','>.985'])
    d['directional_move_bps']=d.binance_move_from_open_bps*np.where(d.outcome_index.eq(0),1,-1)
    qualified=d[d.feature_available]
    aggregates=qualified.groupby(['day','market_key','seconds_left','bid_bucket'],observed=True).agg(observations=('observed_api_later_buy_fill','size'),observed_api_later_fill_rate=('observed_api_later_buy_fill','mean')).reset_index()
    summary={'rows':len(d),'contracts':int(d[['market_key','contract_start_ms']].drop_duplicates().shape[0]),'current_state_qualified_rows':int(d.qualified.sum()),'feature_available_rows':len(qualified),'deduplicated_native_aggtrade_rows':duplicates,'btc_public_buy_fill_rows':len(fills),'label_guard_ms':args.label_guard_ms,'target':'later API-dated BUY fill; API timing lag unresolved even with guard; unknown submission; absences censored','clocks':'strict normalized-cache Poly ts_ns asof (not certified real receipt); native Binance receipt-asof; public fills second-resolution block/API time; Binance opening price proxy','flow_coverage':'observed agg-id gaps and largest receipt intervals retained; neither certifies complete native feed capture','chronology':'September30 development; October1 descriptive internal holdout; dates previously inspected; no parameter fitting','spearman_by_day':{day:{c:None if pd.isna(v) else float(v) for c,v in sub[['bid','seconds_left','directional_move_bps','observed_api_later_buy_fill']].rank().corr()['observed_api_later_buy_fill'].drop('observed_api_later_buy_fill').items()} for day,sub in qualified.groupby('day')}}
    args.output.mkdir(parents=True,exist_ok=True)
    d.to_parquet(args.output/'fixed_time_observations.parquet',index=False)
    aggregates.to_csv(args.output/'descriptive_buckets.csv',index=False)
    (args.output/'SUMMARY.json').write_text(json.dumps(summary,indent=2))
    print(json.dumps(summary,indent=2))

if __name__=='__main__':main()
