"""Build fresh identity/arm summaries from exported public fields only."""
from pathlib import Path
import argparse
from datetime import datetime, timezone
import hashlib
import json

import pandas as pd


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(root):
    sources = [root / name for name in ['fresh_fills_two_days.parquet',
               'chain_order_fill_links.parquet', 'fresh_tail_chain_links.parquet']]
    fills = pd.read_parquet(sources[0])
    old = pd.read_parquet(sources[1])
    new = pd.read_parquet(sources[2])
    cols = ['tx', 'block', 'cond', 'idx', 'token', 'maker_amt', 'taker_amt',
            'side', 'sig_type', 'order_ts_ms', 'salt', 'fill_amt', 'fee_amt', 'decoded_role']
    chain = pd.concat([old[cols], new[cols]], ignore_index=True)
    chain = chain[chain.tx.isin(fills.transaction_hash)].copy()
    chain = chain.drop_duplicates(['tx', 'token', 'salt', 'idx'])
    chain['token'] = chain.token.astype(str)
    chain['salt'] = chain.salt.astype(str)
    chain['order_limit'] = chain.apply(lambda r: r.maker_amt / r.taker_amt
                          if r.side == 0 else r.taker_amt / r.maker_amt, axis=1)
    chain['requested_shares'] = chain.apply(lambda r: (r.taker_amt if r.side == 0 else r.maker_amt) / 1e6, axis=1)
    left = fills.copy()
    left['token_id'] = left.token_id.astype(str)
    left['api_fill_row_id'] = range(len(left))
    joined = left.merge(chain, left_on=['transaction_hash', 'token_id'],
                        right_on=['tx', 'token'], how='left', indicator=True,
                        suffixes=('_api', '_chain'))
    joined['chain_join_type'] = joined['_merge'].map({'both': 'direct_tx_token',
                               'left_only': 'unmatched', 'right_only': 'unexpected'}).astype(str)
    joined = joined.drop(columns=['_merge'])
    joined['api_vs_decoded_role_agreement'] = ((joined.api_role == 'TAKER') & (joined.decoded_role == 'TAKER')) | (
                    (joined.api_role == 'NOT_IN_TAKER_PULL') & (joined.decoded_role == 'MAKER'))
    summary = chain.groupby('salt', sort=True).agg(
        token=('token', 'first'), signed_timestamp_field_ms=('order_ts_ms', 'first'),
        order_limit=('order_limit', 'first'), requested_shares=('requested_shares', 'first'),
        chain_side=('side', 'first'), n_filled_transactions=('tx', 'nunique'),
        observed_roles=('decoded_role', lambda x: ''.join(sorted({v[0] for v in x}))),
        raw_maker_asset_fill_total=('fill_amt', 'sum'), raw_fee_total=('fee_amt', 'sum')).reset_index()
    time_join = joined[joined.salt.notna()].groupby('salt').agg(
        api_first_fill_seconds=('timestamp', 'min'), api_last_fill_seconds=('timestamp', 'max'),
        api_observed_shares=('size', 'sum'), api_observed_usdc=('price', lambda x: float((x * joined.loc[x.index, 'size']).sum())))
    summary = summary.merge(time_join, on='salt', how='left')
    summary['arm_by_observed_limit'] = summary.order_limit.map(lambda x: 'near_099' if abs(x - .99) <= .0006 else 'other_limit')
    outputs = []
    for name, frame in [('fresh_order_fill_identity_two_days.parquet', joined),
                        ('fresh_signed_orders_two_days.parquet', summary)]:
        path = root / name
        frame.to_parquet(path, index=False, compression='zstd')
        outputs.append({'path': name, 'rows': len(frame), 'columns': list(frame.columns),
                        'sha256': digest(path), 'bytes': path.stat().st_size})
    report = {'derived_utc': datetime.now(timezone.utc).isoformat(),
        'script_sha256': digest(Path(__file__)),
        'sources': [{'file': p.name, 'sha256': digest(p)} for p in sources],
        'fresh_api_rows': len(fills), 'fresh_api_transactions': int(fills.transaction_hash.nunique()),
        'matched_api_rows': int(joined[joined.chain_join_type == 'direct_tx_token'].api_fill_row_id.nunique()),
        'unmatched_api_rows': int(joined[joined.chain_join_type == 'unmatched'].api_fill_row_id.nunique()),
        'joined_rows': len(joined), 'role_disagreements': int((~joined.api_vs_decoded_role_agreement & joined.salt.notna()).sum()),
        'signed_orders': len(summary), 'order_arm_counts': summary.arm_by_observed_limit.value_counts().to_dict(),
        'order_role_counts': summary.observed_roles.value_counts().to_dict(),
        'outputs': outputs,
        'limits': ['Exact transaction+token join only. No inferred complement join is fabricated.',
                   'API timestamps remain second-granularity reporting times, not decision/submission time.',
                   'Order identity/roles/amounts are decoded public filled calldata only.',
                   'Signed timestamp field is observed but its signing/submission interpretation is not certified.',
                   'Maker lifecycle, venue prints, outcomes, fees/PnL/capital paths need separate qualified joins.',
                   'No zero-fill orders; aggregate economics on this filled sample are selected by execution.',
                   'Near_099 classification is by observed limit, not proof of GTC order type.',
                   'Summary covers fills inside the two-day slice, not each order full lifetime.']}
    (root / 'FRESH_IDENTITY_PROVENANCE.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps({k: report[k] for k in ['fresh_api_rows', 'matched_api_rows',
          'unmatched_api_rows', 'joined_rows', 'role_disagreements', 'signed_orders',
          'order_arm_counts', 'order_role_counts']}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, required=True)
    run(parser.parse_args().root)
