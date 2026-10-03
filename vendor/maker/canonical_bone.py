#!/usr/bin/env python3
"""Exact Q99 reference queue mechanics applied to fixed historical BoneOhio orders.
Entry is the .99 footprint at the already inferred post_ms, never an outcome-fitted Q.
No footprint -> reported exclusion, not fabricated fill. Writes only FINAL/maker/canonical.
"""
import os
for k in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS'): os.environ[k]='1'
import importlib.util, json, sys, traceback
from pathlib import Path
from multiprocessing import get_context
import numpy as np
import pandas as pd
import maker_audit as b
OUT=Path(os.environ.get('CANONICAL_OUT','reference-storage/boneohio_20261002_FINAL/maker/canonical'))
# Import has an output-dir creation side effect: redirect it into the new experiment.
os.environ['OUT']=str(OUT)
SOURCE=os.environ.get('Q99_FILL_SOURCE',str(Path.home()/'vania/q99_20261002_DEPLOYGAP/code/fill/fillrep.py'))
spec=importlib.util.spec_from_file_location('reference_fill',SOURCE)
q=importlib.util.module_from_spec(spec); sys.modules[spec.name]=q; spec.loader.exec_module(q)

def work(arg):
    (coin,mk,cs),orders=arg
    try:
        d,t=q.load_market(mk,int(cs)//1000)
        rows=[]; excludes=[]
        for r in orders.itertuples():
            oc=0 if bool(r.is_yes) else 1; L=990000
            bids=q.token_bids(d,oc,L)
            level=bids[bids.price_micros==L].copy()
            level['size']=level.size_micros/1e6
            prev=level['size'].shift(1).fillna(0.)
            inc=level[(level.venue_ts_ms==int(r.post_ms))&(level['size']-prev>=float(r.rem0)-1e-6)]
            if not len(inc):
                excludes.append(dict(salt=r.salt,coin=coin,mk=mk,cs=cs,t0=r.post_ms,rem0=r.rem0,reason='no_exact_time_footprint'))
                continue
            foot=(int(inc.iloc[0].venue_ts_ms),int(inc.iloc[0].seq))
            ahead=float(prev.loc[inc.index[0]])
            tt=t.copy(); tt['mp']=np.where(tt.outcome==oc,tt.price_micros,1000000-tt.price_micros)
            sell=((tt.outcome==oc)&(tt.trade_side==1))|((tt.outcome!=oc)&(tt.trade_side==0))
            tt=tt[sell&(tt.mp<=L)].sort_values(['venue_ts_ms','seq'],kind='stable')
            tv=tt.venue_ts_ms.to_numpy(float); vol=tt.size_micros.to_numpy(float)/1e6; mp=tt.mp.to_numpy()
            t1=int(r.t_end) if np.isfinite(r.t_end) else int(r.ce)+600000
            common=dict(salt=r.salt,coin=coin,mk=mk,cs=cs,ce=r.ce,roles=r.roles,state=r.state,rem0=r.rem0,
                real_sh=r.real_sh,real_first=min([p for p,_ in r.fills]) if r.fills else np.nan,won=r.won,
                t0=int(r.post_ms),t1=t1,footprint_seq=foot[1],footprint_candidates=len(inc),ahead_seq_footprint=ahead,
                tick001=bool(((bids.price_micros%10000)!=0).any()))
            for name,shift,book in [('canonical60',60,True),('canonical20',20,True),('canonical0',0,True),('canonical_nobetter60',60,False)]:
                ab=q.above_asof(bids,L,tv-shift-.5) if len(tv) else np.zeros(0)
                amt=np.maximum(0.,vol-ab) if book else vol
                prints=[(tv[i]-shift,amt[i],bool(mp[i]<L),tv[i]) for i in range(len(tv))]
                qty,first,fills=q.replay(level,prints,float(r.rem0),foot,t1,'sizematch',0)
                live=(tv-shift>=r.post_ms)&(tv-shift<t1)
                rows.append(dict(common,variant=name,sim_sh=qty,sim_first=first,
                    live_qualifying_prints=int(live.sum()),live_prints_with_better_depth=int((live&(ab>.5)).sum()),
                    live_better_depth_subtracted=float(np.minimum(vol,ab)[live].sum())))
        return rows,excludes,[]
    except Exception: return [],[],[dict(coin=coin,mk=mk,cs=cs,error=traceback.format_exc())]

def main():
    OUT.mkdir(parents=True,exist_ok=True)
    orders=b.inputs(os.environ.get('MARKETS','btc_5m').split(',')); jobs=list(orders.groupby(['coin','market_key','cs']))
    rows=[]; excluded=[]; errors=[]
    print('canonical BoneOhio',len(orders),'orders',len(jobs),'contracts',flush=True)
    with get_context('fork').Pool(int(os.environ.get('WORKERS','96'))) as p:
        for i,(rr,ee,er) in enumerate(p.imap_unordered(work,jobs,chunksize=1),1):
            rows+=rr; excluded+=ee; errors+=er
            if i%50==0: print('done',i,'rows',len(rows),'excluded',len(excluded),'errors',len(errors),flush=True)
    pd.DataFrame(excluded).to_csv(OUT/'excluded.csv',index=False)
    (OUT/'errors.json').write_text(json.dumps(errors,indent=2))
    if rows:
        b.OUT=OUT; b.summarize(pd.DataFrame(rows))
        print((OUT/'summary.json').read_text(),flush=True)
    print('excluded',len(excluded),'errors',len(errors),flush=True)
    if errors: raise SystemExit(2)

if __name__=='__main__': main()
