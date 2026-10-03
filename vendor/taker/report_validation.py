"""Reproducible small taker validation summary; public-input fixtures only."""
import ast
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src'))
from bonebundle.taker import BookView,match_budget_fak,settlement_pnl


def canonical_ast(node):
    if isinstance(node,ast.AST):
        return dict(type=type(node).__name__,fields={key:canonical_ast(value) for key,value in ast.iter_fields(node)
                                                   if value is not None and value!=[]})
    if isinstance(node,list):
        return [canonical_ast(value) for value in node]
    return node


def main():
    path = ROOT/'vendor/taker/original_function_manifest.json'
    raw = json.loads(path.read_text())
    if 'original_function_ast_sha256' in raw:
        # Keep function proof without private original filesystem coordinates.
        raw = dict(experiment='tailtaker_20261003_A2PARITY',original_function_ast_sha256=raw['original_function_ast_sha256'])
        path.write_text(json.dumps(raw,indent=2)+'\n')
    proof = {}
    for name,expected in raw['original_function_ast_sha256'].items():
        file,function = name.split(':')
        tree = ast.parse((ROOT/'vendor/taker/a2_20261003'/file).read_text())
        node = next(n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.ClassDef)) and n.name==function)
        payload = json.dumps(canonical_ast(node),sort_keys=True,separators=(',',':')).encode()
        actual = hashlib.sha256(payload).hexdigest()
        if actual!=expected:
            raise AssertionError(f'Distributed mathematical function changed: {name}')
        proof[name] = actual
    frame = pd.read_parquet(ROOT/'tests/taker/fixtures/a2_execution_inputs.parquet')
    report = dict(scope='Dated A2 execution adapter differential validation; not strategy discovery or universal live venue certification',
                  orders=len(frame),latency_scenarios=16,shares_cost_comparisons=7728,
                  mathematical_source_ast_equal=proof,cohorts={})
    for cohort,g in frame.groupby('box'):
        legacy_pnl = float(g.pnl_bt.sum())
        fullleg_pnl = 0.
        legfee = 0.
        for _,row in g.iterrows():
            levels = []
            for k in range(4):
                p,q = row[f'a200_px{k}'],row[f'a200_sz{k}']
                if pd.isna(p) or pd.isna(q):
                    break
                levels.append((round(float(p),4),float(q)))
            b = BookView(bool(row.fav_yes),int(row.sched+200),int(row.sched+199),'venue',tuple(levels))
            result = match_budget_fak(b,float(row.principal_usd),float(row.submitted_limit),depth_mode='a2_four')
            fullleg_pnl += settlement_pnl(result,int(row.fav_won))
            legfee += result.fee_cash_value
        report['cohorts'][cohort] = dict(orders=len(g),archived_bt_pnl_vwap_fee=legacy_pnl,
            port_pnl_per_level_fee=fullleg_pnl,port_total_fee=legfee,
            explicit_fee_convention_pnl_delta=fullleg_pnl-legacy_pnl)
    target = ROOT/'tests/taker/VALIDATION.json'
    target.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(dict(orders=len(frame),source_functions=len(proof),fee_delta={k:v['explicit_fee_convention_pnl_delta'] for k,v in report['cohorts'].items()})))


if __name__=='__main__':
    main()
