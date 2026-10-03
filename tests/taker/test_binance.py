import ast
import importlib.util
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
import tempfile
import io
import urllib.error

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src'))
from bonebundle.taker import reconstruct_binance_l2


def level(u,side,p,q,*,snapshot=False,prev=None,E=None,T=None,R=None):
    row = dict(final_update_id=u,side=side,price=str(p),quantity=str(q),event_type='snapshot' if snapshot else 'update',
               event_time=E if E is not None else 1000+u,transaction_time=T if T is not None else 999+u,
               received_time=R if R is not None else 1_790_000_000_000_000_001+u)
    if prev is not None:
        row['prev_final_update_id'] = prev
    return row


class BinanceTests(unittest.TestCase):
    def rows(self):
        return [level(100,'bid',100.,2,snapshot=True),level(100,'bid',99.9,3,snapshot=True),
                level(100,'ask',100.1,4,snapshot=True),level(100,'ask',100.2,5,snapshot=True),
                level(101,'bid',100.,7,prev=100),level(101,'ask',100.1,0,prev=100),
                level(102,'bid',99.9,8,prev=101),level(103,'ask',100.2,9,prev=102)]

    def test_several_levels_absolute_update_message_grouping_and_nanosecond_clock(self):
        records = list(reconstruct_binance_l2(self.rows(),levels=10))
        self.assertEqual(len(records),4)
        self.assertEqual(records[1].bids,((100.,7.),(99.9,3.)))
        self.assertEqual(records[1].asks,((100.2,5.),))
        self.assertEqual(records[1].recv_ns,1_790_000_000_000_000_102)
        self.assertEqual(records[1].event_ms,1101)
        self.assertEqual(records[1].transaction_ms,1100)
        self.assertEqual(records[1].sequence_status,'PREVIOUS_ID_CHAIN_MATCH')
        self.assertTrue(records[1].clock_qualified)

    def test_missing_T_preserved_and_gap_not_silently_repaired(self):
        rows = self.rows()
        rows[-1]['transaction_time'] = None
        rows[-1]['prev_final_update_id'] = 999
        records = list(reconstruct_binance_l2(rows))
        self.assertIsNone(records[-1].transaction_ms)
        self.assertEqual(records[-1].sequence_status,'GAP')
        self.assertFalse(records[-1].qualified)
        without_prev = [{k:v for k,v in r.items() if k!='prev_final_update_id'} for r in rows]
        self.assertEqual(list(reconstruct_binance_l2(without_prev))[-1].sequence_status,'UNKNOWN_NO_PREVIOUS_ID')

    def test_top_change_mode_exact_against_archived_numba_loop_on_synthetic_rows(self):
        rows = self.rows()
        actual = list(reconstruct_binance_l2(rows,only_top_changes=True))
        tree = ast.parse((ROOT/'vendor/taker/a2_20261003/chd_top_v2.py').read_text())
        node = next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='replay')
        node.decorator_list = []  # Execute original math without numba dependency.
        env = dict(np=np,NT=2000)
        exec(compile(ast.Module(body=[node],type_ignores=[]),'<dated_CHD_replay>','exec'),env)
        starts = np.array([0,4,6,7,8],dtype=np.int64)
        sides = np.array([int(r['side']!='bid') for r in rows])
        ticks = np.array([round(float(r['price'])*10) for r in rows])
        quantities = np.array([float(r['quantity']) for r in rows])
        E = np.array([r['event_time'] for r in rows],dtype=np.int64)
        T = np.array([r['transaction_time'] for r in rows],dtype=np.int64)
        R = np.array([r['received_time'] for r in rows],dtype=np.int64)
        e,t,r,b,a,bq,aq = env['replay'](starts,sides,ticks,quantities,E,T,R)
        self.assertEqual(len(actual),len(e))
        for i,v in enumerate(actual):
            self.assertEqual((v.event_ms,v.transaction_ms,v.recv_ns),(int(e[i]),int(t[i]),int(r[i])))
            self.assertEqual(v.bids[0],(b[i]/10,bq[i]))
            self.assertEqual(v.asks[0],(a[i]/10,aq[i]))

    def test_absent_seed_invalid_level_and_crossed_book(self):
        with self.assertRaises(ValueError):
            list(reconstruct_binance_l2([level(1,'bid',100,2)]))
        with self.assertRaises(ValueError):
            list(reconstruct_binance_l2([level(1,'bid',100.01,2,snapshot=True)]))
        crossed = [level(1,'bid',100.2,2,snapshot=True),level(1,'ask',100.1,2,snapshot=True)]
        self.assertEqual(list(reconstruct_binance_l2(crossed)),[])

    def test_downloader_builds_anonymous_hour_url_without_credentials(self):
        spec = importlib.util.spec_from_file_location('optional_feeds',ROOT/'vendor/taker/download_optional_feeds.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        url,relative = module.source_url('binance_futures','BTCUSDT','orderbook',module.parse_hour('2026-09-29T23:00:00Z'))
        self.assertEqual(relative,'binance_futures/2026-09-29/23/BTCUSDT_orderbook.parquet')
        self.assertNotIn('api_key',url)
        self.assertNotIn('Authorization',url)
        self.assertTrue(url.startswith('https://api.cryptohftdata.com/v1/download?file='))

    def test_downloader_bounded_retries_and_byte_preserving_private_output(self):
        spec = importlib.util.spec_from_file_location('optional_feeds',ROOT/'vendor/taker/download_optional_feeds.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        synthetic = b'PAR1SYNTHETIC_FORMAT_FIXTURE_PAR1'
        with tempfile.TemporaryDirectory(prefix='bonebundle-test-') as directory:
            destination = Path(directory)/'test.parquet'
            error = urllib.error.HTTPError('https://example.test',429,'rate limit',{},None)
            with patch.object(module.time,'sleep'),patch.object(module.urllib.request,'urlopen',side_effect=[error,io.BytesIO(synthetic)]) as request:
                result = module.download('https://example.test',destination,retries=2)
                self.assertEqual(result['status'],'DOWNLOADED')
                self.assertEqual(result['attempts'],2)
                self.assertEqual(destination.read_bytes(),synthetic)
                self.assertEqual(request.call_count,2)
            missing = urllib.error.HTTPError('https://example.test',404,'absent',{},None)
            with patch.object(module.time,'sleep'),patch.object(module.urllib.request,'urlopen',side_effect=missing) as request:
                self.assertEqual(module.download('https://example.test',destination,retries=4)['status'],'MISSING_404')
                self.assertEqual(request.call_count,1)


if __name__=='__main__':
    unittest.main()
