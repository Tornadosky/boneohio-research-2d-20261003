import ast
import importlib.util
import json
from pathlib import Path
import sys
import unittest

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src'))
from bonebundle.taker import (BookView, ConsumptionLedger, DepthReplay, crypto_hold_ms,
    match_time_ms, match_budget_fak, fixed_share_benchmark, mapped_complement_price, settlement_pnl)
from bonebundle.taker.execution import HOLD50,HOLD150


def book(asks, *, match=1000, contract='test'):
    return BookView(True,match,match-1,'venue',tuple(asks),contract_id=contract)


def snap(y,t,seq,asks,bids=(),**extra):
    return dict(venue_ts_ms=t,ts_ns=(t+25)*1_000_000,seq=seq,is_yes=y,event_type='snapshot',side='',
                price_micros=-1,size_micros=0,best_bid_micros=max([p for p,q in bids],default=-1),
                best_ask_micros=min([p for p,q in asks],default=-1),bid_prices=[p for p,q in bids],
                bid_sizes=[q for p,q in bids],ask_prices=[p for p,q in asks],ask_sizes=[q for p,q in asks],**extra)


class Semantics(unittest.TestCase):
    def test_spend_size_price_improvement_differs_from_fixed_shares(self):
        view = book([(0.03,100)])
        result = match_budget_fak(view,1,.20)
        self.assertAlmostEqual(result.matched_shares,100/3)
        self.assertAlmostEqual(result.principal_cost,1)
        self.assertEqual(fixed_share_benchmark(view,5,.2).matched_shares,5)

    def test_fee_is_per_level_and_full_depth_extends_four(self):
        view = book([(.2,5),(.8,5)])
        result = match_budget_fak(view,5,.9)
        self.assertAlmostEqual(result.matched_shares,10)
        self.assertAlmostEqual(result.fee_cash_value,.112)
        self.assertNotAlmostEqual(result.fee_cash_value,.07*.5*.5*10)
        deeper = book([(.9+i*.01,1) for i in range(6)])
        self.assertEqual(match_budget_fak(deeper,10,.96).matched_shares,6)
        self.assertEqual(match_budget_fak(deeper,10,.96,depth_mode='a2_four').matched_shares,4)

    def test_limit_partial_and_known_empty(self):
        self.assertEqual(match_budget_fak(book([]),1,.5).status,'NO_FILL')
        result = match_budget_fak(book([(.4,2),(.6,10)]),2,.5)
        self.assertEqual(result.status,'PARTIAL')
        self.assertEqual(result.matched_shares,2)
        self.assertEqual(result.principal_cost,.8)

    def test_unknown_stale_receipt_and_invalid_are_distinct_from_no_fill(self):
        view = BookView(True,1000,999,'venue',(),qualified=False,reason='NO_CACHE')
        self.assertEqual(match_budget_fak(view,1,.5).status,'UNQUALIFIED')
        recv = BookView(True,1000,999,'receive',((.4,10),))
        self.assertEqual(match_budget_fak(recv,1,.5).reason,'RECEIPT_BOOK_IS_NOT_VENUE_MATCH_BOOK')
        self.assertEqual(match_budget_fak(book([(.6,1),(.4,1)]),1,.5).status,'UNQUALIFIED')
        self.assertEqual(match_budget_fak(book([(.4,1)]),float('nan'),.5).status,'UNQUALIFIED')
        self.assertEqual(match_budget_fak(book([(.4,1)]),1,True).status,'UNQUALIFIED')

    def test_own_consumption_preserves_token_identity_and_chronology(self):
        ledger = ConsumptionLedger('test')
        first = match_budget_fak(book([(.4,10)]),3,.5,depletion=ledger)
        second = match_budget_fak(book([(.4,10)],match=1010),3,.5,depletion=ledger)
        self.assertEqual(first.matched_shares,7.5)
        self.assertEqual(second.matched_shares,2.5)
        self.assertEqual(match_budget_fak(book([(.4,10)],match=1005),1,.5,depletion=ledger).status,'UNQUALIFIED')
        self.assertEqual(match_budget_fak(book([(.4,10)],match=1020,contract='other'),1,.5,depletion=ledger).status,'UNQUALIFIED')

    def test_hold_at_ingress_total_includes_hold_once(self):
        for boundary,before,after in ((HOLD50,250,50),(HOLD150,50,150)):
            self.assertEqual(crypto_hold_ms(boundary-1),before)
            self.assertEqual(crypto_hold_ms(boundary),after)
            self.assertEqual(match_time_ms(boundary-5,ingress_latency_ms=5),boundary+after)
        self.assertEqual(match_time_ms(HOLD150,total_latency_ms=200),HOLD150+200)
        with self.assertRaises(ValueError):
            match_time_ms(HOLD150,total_latency_ms=200,ingress_latency_ms=50)
        with self.assertRaises(ValueError):
            match_time_ms(HOLD150,total_latency_ms=200.5)

    def test_unknown_outcome_stays_unknown(self):
        result = match_budget_fak(book([(.4,10)]),1,.5)
        self.assertIsNone(settlement_pnl(result,None))
        self.assertAlmostEqual(settlement_pnl(result,1),2.5-1-.07*.4*.6*2.5)
        self.assertEqual(mapped_complement_price(.999),.001)


class DepthSemantics(unittest.TestCase):
    def events(self):
        return [snap(True,900,1,[(400000,10000000)],[(390000,3000000)]),
                snap(False,900,2,[(610000,10000000)],[(600000,3000000)])]

    def test_strict_arrival_excludes_future_and_same_ms_events(self):
        events = self.events()+[snap(True,1000,3,[(300000,10000000)])]
        replay = DepthReplay(events)
        view = replay.book(True,1000)
        self.assertEqual(view.asks,((.4,10.),))
        self.assertEqual(view.source_ms,900)
        self.assertEqual(replay.book(True,1001).asks,((.3,10.),))

    def test_absolute_updates_sequence_snapshot_reset_and_complement(self):
        events = self.events()+[
            dict(venue_ts_ms=950,ts_ns=975000000,seq=4,is_yes=True,event_type='price_change',side='ask',
                 price_micros=400000,size_micros=1000000,best_bid_micros=390000,best_ask_micros=400000),
            dict(venue_ts_ms=950,ts_ns=975000000,seq=5,is_yes=True,event_type='price_change',side='ask',
                 price_micros=400000,size_micros=2000000,best_bid_micros=390000,best_ask_micros=400000),
            snap(True,960,6,[(500000,7000000)])]
        replay = DepthReplay(reversed(events))
        self.assertEqual(replay.book(True,951).asks,((.4,2.),))
        self.assertEqual(replay.book(False,951).asks,((.61,10.),))
        self.assertEqual(replay.book(True,961).asks,((.5,7.),))

    def test_missing_venue_never_receipt_minus_assumed_lag(self):
        events = self.events()
        events[0]['venue_ts_ms'] = 0
        result = DepthReplay(events).book(True,1000)
        self.assertFalse(result.qualified)
        self.assertEqual(result.reason,'UNKNOWN_VENUE_TIMESTAMP')

    def test_top_age_counts_size_changes_on_either_token(self):
        events = self.events()+[dict(venue_ts_ms=950,ts_ns=975000000,seq=4,is_yes=False,event_type='price_change',side='ask',
                     price_micros=610000,size_micros=2000000,best_bid_micros=600000,best_ask_micros=610000)]
        replay = DepthReplay(events)
        self.assertEqual(replay.top_age_ms(1000,feed_lag_ms=10),40.)

    def test_unseeded_and_stale_fail_closed(self):
        self.assertFalse(DepthReplay(self.events()[:1]).book(True,1000).qualified)
        self.assertEqual(DepthReplay(self.events()).book(True,3000,max_book_age_ms=1000).reason,'STALE_TOKEN_DEPTH')
        self.assertEqual(DepthReplay(self.events(),coverage_ok=False).book(True,1000).reason,'COVERAGE_GAP')
        replay = DepthReplay(self.events())
        replay.book(True,1000)
        with self.assertRaises(ValueError):
            replay.book(True,999)


class ArchivedA2Parity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.frame = pd.read_parquet(ROOT/'tests/taker/fixtures/a2_execution_inputs.parquet')
        cls.delays = json.loads((ROOT/'tests/taker/fixtures/a2_fixture_provenance.json').read_text())['delays']
        # Compile ONLY the actual archived function; importing execpar would
        # access private source paths and rerun its top-level analysis.
        tree = ast.parse((ROOT/'vendor/taker/a2_20261003/execpar.py').read_text())
        node = next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='fak')
        env = {'np':np,'X':type('OriginalConstants',(),{'NLEV':4})}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'<archived_A2_fak>','exec'),env)
        cls.original_fak = staticmethod(env['fak'])

    def test_complete_483_order_16_latency_match_against_original_and_saved_outputs(self):
        self.assertEqual(len(self.frame),483)
        comparisons = 0
        for _,row in self.frame.iterrows():
            for delay in self.delays:
                levels = []
                for k in range(4):
                    p,q = row[f'a{delay}_px{k}'],row[f'a{delay}_sz{k}']
                    if not (np.isfinite(p) and np.isfinite(q)):
                        break
                    levels.append((round(float(p),4),float(q)))
                view = BookView(bool(row.fav_yes),int(row.sched+delay),int(row.sched+delay-1),
                                'venue',tuple(levels),contract_id='archived-A2-four-level-view')
                result = match_budget_fak(view,float(row.principal_usd),round(float(row.submitted_limit),4),depth_mode='a2_four')
                expected_sh,expected_cost = self.original_fak(row,delay)
                self.assertNotEqual(result.status,'UNQUALIFIED',msg=(row.fixture_id,delay,result.reason))
                self.assertAlmostEqual(result.matched_shares,expected_sh,places=10,msg=(row.fixture_id,delay))
                self.assertAlmostEqual(result.principal_cost,expected_cost,places=10,msg=(row.fixture_id,delay))
                self.assertAlmostEqual(result.matched_shares,float(row[f'bt_sh{delay}']),places=10)
                self.assertAlmostEqual(result.principal_cost,float(row[f'bt_cost{delay}']),places=10)
                comparisons += 1
        self.assertEqual(comparisons,7728)

    def test_200ms_aggregate_pnl_reproduces_frozen_report_with_original_vwap_fee(self):
        # Preserve the dated PnL convention, then show per-level fees separately.
        report = json.loads((ROOT/'tests/taker/fixtures/EXECPAR.json').read_text())
        for cohort,g in self.frame.groupby('box'):
            expected = report['pnl_same_orders'][cohort]
            self.assertEqual(len(g),expected['orders'])
            self.assertAlmostEqual(float(g.pnl_bt.sum()),expected['bt'],places=4)
            self.assertAlmostEqual(float(g.bt_sh200.sum()),expected['bt_shares'],places=2)
            for _,row in g.iterrows():
                sh,cost = float(row.bt_sh200),float(row.bt_cost200)
                px = cost/max(sh,1e-12)
                legacy_fee = .07*px*(1-px)*sh
                self.assertAlmostEqual(legacy_fee,float(row.fee_bt),places=12)
                self.assertAlmostEqual(sh*row.fav_won-cost-legacy_fee,float(row.pnl_bt),places=12)

    def test_port_fee_matches_archived_exec_exact_per_level_on_identical_ladder(self):
        spec = importlib.util.spec_from_file_location('frozen_a2_executor',ROOT/'vendor/taker/a2_20261003/exec_exact.py')
        original = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(original)
        cs = HOLD150+300000
        row = dict(contract_start_ms=cs,decide_ms=cs+202000,is_yes=True,tte=98.,dist=10.,sigma_1s=.2,won=0.,
                   dl10_tbid=.05,dl10_task=.06,dl10_tbsz=20.,dl10_fask=.9)
        for delay in original.ARR:
            for k in range(4):
                row[f'a{delay}_px{k}'] = [.9,.95,np.nan,np.nan][k]
                row[f'a{delay}_sz{k}'] = [2.,3.,np.nan,np.nan][k]
        frame = pd.DataFrame([row])
        reference = original.execute(frame,dict(family='wf',D=10,lat=('fixed',200),size=('usd',10),premium=False))
        self.assertEqual(len(reference),1)
        result = match_budget_fak(book([(.9,2),(.95,3)]),10,.95,depth_mode='a2_four')
        self.assertAlmostEqual(result.principal_cost,float(reference.iloc[0].cost),places=12)
        self.assertAlmostEqual(result.fee_cash_value,float(reference.iloc[0].fee),places=12)
        self.assertAlmostEqual(settlement_pnl(result,1),float(reference.iloc[0].pnl),places=12)

    def test_real_depth_prefix_reproduces_original_asof_replay(self):
        import pyarrow.parquet as pq
        table = pq.ParquetFile(ROOT/'tests/taker/fixtures/real_depth_prefix.parquet').read()
        frame = table.to_pandas()
        self.assertEqual(len(frame),4096)
        self.assertEqual(set(frame[frame.event_type=='snapshot'].is_yes),{True,False})
        tree = ast.parse((ROOT/'vendor/taker/a2_20261003/exact_book.py').read_text())
        nodes = [n for n in tree.body if isinstance(n,(ast.ClassDef,ast.FunctionDef)) and n.name in ('parse_arr','Book','views')]
        data = {c:frame[c].to_numpy() for c in frame.columns}
        env = dict(np=np,json=json,SC=1_000_000,DLAG=(0,10,20,30,50,75,100,150),
                   ARR=tuple(self.delays),NLEV=4,rd=lambda *args:data)
        exec(compile(ast.Module(body=nodes,type_ignores=[]),'<archived_A2_depth>','exec'),env)
        timestamps = np.unique(frame.venue_ts_ms.to_numpy(np.int64))
        first_both_seeded = int(frame[frame.event_type=='snapshot'].groupby('is_yes').venue_ts_ms.min().max())
        times = timestamps[timestamps>first_both_seeded]
        chosen = times[np.linspace(0,len(times)-1,min(80,len(times)),dtype=int)]+1
        comparisons = 0
        for tail_yes in (True,False):
            rows = dict(decide_ms=chosen,is_yes=np.full(len(chosen),tail_yes))
            original,err = env['views'](0,rows,[])
            self.assertIsNone(err)
            expected = original[0]
            replay = DepthReplay(table.to_pylist())
            for i,target in enumerate(chosen):
                view = replay.book(not tail_yes,int(target),max_book_age_ms=1_000_000)
                self.assertTrue(view.qualified,msg=view.reason)
                for k,(price,size) in enumerate(view.asks[:4]):
                    self.assertAlmostEqual(price,float(expected[f'a0_px{k}'][i]),places=6)
                    self.assertAlmostEqual(size,float(expected[f'a0_sz{k}'][i]),delta=max(1e-5,1e-7*size))
                    comparisons += 1
        self.assertGreaterEqual(comparisons,160)


if __name__=='__main__':
    unittest.main()
