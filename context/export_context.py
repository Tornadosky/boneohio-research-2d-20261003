"""Reproduce the curated historical context export on its source research host.

Only explicitly named, read-only public-wallet evidence and research summaries
are admitted. No credentials, raw transaction signatures, other wallets' order
traces, fleet configuration or whole private handoff are exported.
"""
from pathlib import Path
import argparse
import hashlib
import json
import os
import re
from datetime import datetime, timezone

import pandas as pd
import pyarrow.parquet as pq

EXP = Path(os.environ.get('BONEOHIO_SOURCE_EXP', '/source/exp'))
NAV = Path(os.environ.get('BONEOHIO_SOURCE_NAV', '/source/vania'))
ENTRY = EXP / 'boneohio_20261001_ENTRY'
PARITY = EXP / 'boneohio_20261002_PARITY'
FINAL = EXP / 'boneohio_20261002_FINAL'
TASK = 'boneohio_20261003_BROWSER_BUNDLE'
BONE = '0x48ac40fc545cf327edd5365435c3a9f385614a7e'


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def pointer(path):
    value = str(path)
    value = value.replace(str(EXP), '$VIPER_PTMP/exp')
    value = value.replace(str(NAV), '$VIPER_HOME/vania')
    return value


def sanitize(text):
    text = text.replace(str(EXP), '$VIPER_PTMP/exp')
    text = re.sub(r'/ptmp/[^/\s]+/', '$VIPER_PTMP/', text)
    text = text.replace(str(NAV), '$VIPER_HOME/vania')
    text = text.replace('~/vania', '$VIPER_HOME/vania')
    text = re.sub(r'/u/[^/\s]+/', '$VIPER_HOME/', text)
    text = text.replace('D:\\tfki\\', '$LOCAL_TFKI/')
    text = text.replace('D:/tfki/', '$LOCAL_TFKI/')
    return text


def inspect():
    paths = [ENTRY / 'fills_pnl.parquet', ENTRY / 'bone_orders.parquet',
             ENTRY / 'orders_from_fills.parquet', ENTRY / 'chain/s0910_part000.parquet',
             PARITY / 'b_orders.parquet', PARITY / 'gt_orders.parquet', PARITY / 'gt_prints.parquet']
    for path in paths:
        p = pq.ParquetFile(path)
        print(json.dumps({'source': pointer(path), 'rows': p.metadata.num_rows,
                          'columns': p.schema_arrow.names}))


def export(out):
    out.mkdir(parents=True, exist_ok=True)
    records = []

    def record(source, target, transform, extra=None):
        item = {'source': pointer(source), 'source_bytes': source.stat().st_size,
                'source_sha256': sha(source), 'output': target.relative_to(out).as_posix(),
                'output_bytes': target.stat().st_size, 'output_sha256': sha(target),
                'transform': transform}
        if extra:
            item.update(extra)
        records.append(item)

    def copy_text(source, dest, status='FINAL authoritative; historical reused evidence'):
        target = out / dest
        target.parent.mkdir(parents=True, exist_ok=True)
        raw = source.read_text(encoding='utf-8')
        target.write_text(sanitize(raw), encoding='utf-8', newline='\n')
        record(source, target, 'UTF-8; research-root paths replaced with documented variables',
               {'interpretation': status})

    copy_text(NAV / 'boneohio_20261002_FINAL/RESULT.md', 'historical/FINAL_RESULT.md')
    copy_text(NAV / 'boneohio_20261002_FINAL/selection/RESULT.md', 'historical/SELECTION_RESULT.md')
    copy_text(NAV / 'boneohio_20261002_FINAL/taker/RESULT.md', 'historical/TAKER_RESULT.md')
    copy_text(NAV / 'boneohio_20261002_FINAL/maker/RUNBOOK.md', 'historical/MAKER_RUNBOOK.md')
    for rel in [
        'baseline/summary.csv', 'baseline/daily.csv',
        'maker/summary.csv', 'maker/date_metrics.csv', 'maker/week_metrics.csv',
        'maker/loser_orders.csv', 'maker/snapshot_down_resets.csv',
        'maker/summary.json', 'maker/errors.json',
        'maker/canonical/summary.csv', 'maker/canonical/date_metrics.csv',
        'maker/canonical/week_metrics.csv', 'maker/canonical/loser_orders.csv',
        'maker/canonical/queue_variants.parquet', 'maker/canonical/errors.json',
        'maker/own99/summary.csv', 'maker/own99/daily.csv',
        'taker/latency_summary.csv', 'taker/daily.csv', 'taker/weekly.csv',
        'taker/coverage.csv', 'taker/contract_audit.csv', 'taker/metadata.json',
        'selection/selection_metrics.csv', 'selection/selection_daily.csv',
        'selection/selection_weekly.csv', 'selection/coverage_daily.csv',
        'selection/candidate_timing.csv', 'selection/activity_scope.csv',
        'selection/selection_strata.csv', 'selection/methodology.json',
        'selection/candidate_features.parquet', 'selection/real_contract_tokens.parquet',
        'feed_clock/availability.csv', 'feed_clock/clock_summary.json',
        'feed_clock/feed_comparison.csv', 'feed_clock/fixed_wallet_pairs.csv',
        'feed_clock/incremental.csv', 'feed_clock/join_scopes.csv',
        'feed_clock/join_exceptions.csv', 'feed_clock/lag_curves.csv',
        'feed_clock/methodology.json', 'feed_clock/unexplained.csv',
    ]:
        source = FINAL / rel
        target = out / 'historical' / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        if source.suffix == '.parquet':
            target.write_bytes(source.read_bytes())
            z = pq.ParquetFile(target)
            record(source, target, 'byte-identical curated historical evidence',
                   {'rows': z.metadata.num_rows, 'columns': z.schema_arrow.names})
        else:
            copy_text(source, f'historical/{rel}')
    copy_text(NAV / 'boneohio_20261002_FINAL/maker/role_metrics.csv',
              'historical/maker/role_metrics.csv')

    def write_wallet(source, dest, columns, interpretation):
        frame = pd.read_parquet(source, columns=columns)
        target = out / 'wallet' / dest
        target.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(target, index=False, compression='zstd')
        ranges = {}
        for col in ['timestamp', 'order_ts', 'print_ms', 'venue_ts_ms',
                    'first_ms', 'last_ms', 'cs', 'ce', 'post_ms', 't_end']:
            if col in frame and frame[col].notna().any():
                ranges[col] = {'min': float(frame[col].min()), 'max': float(frame[col].max()),
                               'nonnull': int(frame[col].notna().sum())}
        counts = {}
        for col in ['coin', 'tf', 'mk', 'market_key', 'roles', 'role', 'state']:
            if col in frame:
                counts[col] = frame[col].value_counts(dropna=False).to_dict()
        record(source, target, 'all original rows; explicit column whitelist; Zstandard Parquet',
               {'rows': len(frame), 'columns': columns, 'ranges': ranges,
                'counts': counts, 'interpretation': interpretation})
        return frame

    write_wallet(ENTRY / 'fills_pnl.parquet', 'fills_history.parquet',
        ['side', 'token_id', 'condition_id', 'size', 'price', 'timestamp', 'slug',
         'event_slug', 'outcome', 'outcome_index', 'transaction_hash', 'role',
         'coin', 'tf', 'start', 'usd', 'won', 'fee', 'pnl', 'band'],
        'Public fill API history and historical derived labels. API seconds are not decision time. '
        'Role is API taker-only membership; settlement/fee/PnL are research-derived, not live cashflow. '
        'Observed fills cannot reveal zero-fill orders or capital. Right-censored at source maximum timestamp.')
    common_order_columns = ['salt', 'order_ts', 'token', 'limit', 'req_sh', 'side', 'n_tx',
        'roles', 'taker_usdc', 'maker_usdc', 'first_ms', 'last_ms', 'block0', 'coin',
        'coin_slug', 'tf', 'slug', 'market_key', 'cs', 'ce', 'is_yes', 'won',
        'sig_type', 'fill_sh', 'age_first_ms']
    write_wallet(ENTRY / 'bone_orders.parquet', 'decoded_order_summary.parquet', common_order_columns,
        'All 23,751 historically decoded filled signed-order salts. No zero-fill submissions. '
        'order_ts is a calldata field, not proven signing/submission time. '
        'first_ms/last_ms are transaction print mappings; fill_sh is a limit-price-based research derivation.')
    write_wallet(PARITY / 'b_orders.parquet', 'maker_lifecycle_history.parquet',
        common_order_columns + ['found', 'post_ms', 'state', 't_end', 'remain_end',
                                'start', 'next_start', 'prev_end', 'prev_state'],
        'Observed 0.99 filled-order history on BTC/ETH/SOL. post_ms/state/t_end are '
        'book-size fingerprint reconstructions, not authenticated user-WS receipts. '
        'state=alive is censored; synthetic replay cap is not observed exit. '
        'start falls back to first fill if no post fingerprint; never use as exact submission label.')
    write_wallet(PARITY / 'gt_orders.parquet', 'matched_fill_groups_history.parquet',
        ['transaction_hash', 'token_id', 'role', 'coin', 'mk', 'cs', 'ce', 'is_yes',
         'sh', 'usd', 'pnl', 'won', 'print_ms', 'ts', 'px', 'm', 'grp'],
        '43,870 transaction-token groups, 861 unmatched print times. m=print_ms-40 is inferred. '
        'Transaction-only opposite-token complement joins are preserved and separately audited. '
        'Groups are not exact signed-order identities or a complete submission ledger.')
    write_wallet(PARITY / 'gt_prints.parquet', 'matched_venue_prints_history.parquet',
        ['venue_ts_ms', 'token_id', 'is_yes', 'price_micros', 'size_micros', 'side', 'transaction_hash'],
        '43,009 original mapped public prints; use exact transaction+token or documented complement links. '
        'Venue time is not wallet receipt/match/submission time.')

    # Exact public chain transaction-token-salt links; admit BoneOhio maker rows
    # only, so counterpart wallets and arbitrary calldata metadata remain out.
    chain_frames = []
    chain_sources = sorted((ENTRY / 'chain').glob('s0910_part*.parquet'))
    chain_cols = ['tx', 'block', 'cond', 'idx', 'token', 'maker_amt', 'taker_amt',
                  'side', 'sig_type', 'order_ts_ms', 'salt', 'fill_amt', 'fee_amt']
    for source in chain_sources:
        df = pd.read_parquet(source, columns=['maker'] + chain_cols)
        selected = df[(df['maker'].str.lower() == BONE) & (df.idx >= -1)].copy()
        selected = selected[chain_cols]
        selected['decoded_role'] = selected['idx'].map(lambda x: 'TAKER' if x == -1 else 'MAKER')
        chain_frames.append(selected)
    chain = pd.concat(chain_frames, ignore_index=True)
    target = out / 'wallet/chain_order_fill_links.parquet'
    chain.to_parquet(target, index=False, compression='zstd')
    for source in chain_sources:
        record(source, target, 'filter maker == public BoneOhio address and valid decoded idx >= -1; '
                'concatenate sources; explicit public chain-field whitelist',
               {'rows': len(chain), 'columns': list(chain.columns),
                'interpretation': 'Per-fill calldata order link, role by matchOrders argument position. '
                                  'fill_amt/fee_amt are exact raw maker-asset units; BUY maker asset is USDC. '
                                  'No observed zero-fill order, signature, signer, builder or counterpart details.'})

    losers = pd.read_csv(FINAL / 'maker/canonical/loser_orders.csv')
    diagnostic = losers[['salt', 'coin', 'mk', 'cs', 'ce', 'roles', 'state',
                         'real_sh', 'won']].drop_duplicates('salt')
    diagnostic['historical_diagnostic'] = True
    target = out / 'historical/maker/diagnostic_contracts.csv'
    diagnostic.to_csv(target, index=False)
    record(FINAL / 'maker/canonical/loser_orders.csv', target,
           'unique historical losing order salts; separate known-outcome execution diagnostics',
           {'rows': len(diagnostic), 'columns': list(diagnostic.columns),
            'interpretation': 'Known losses outside the latest two days. No signal selection or Q fitting to these outcomes.'})
    print('DIAGNOSTIC_CONTRACTS', diagnostic.to_json(orient='records'))

    for source in [NAV / 'boneohio_20261001_ENTRY/pull_trades.py',
                   NAV / 'boneohio_20261001_ENTRY/decode_txs.py',
                   NAV / 'boneohio_20261001_ENTRY/orders.py',
                   NAV / 'boneohio_20261002_PARITY/b_repost.py',
                   NAV / 'boneohio_20261002_PARITY/gt.py']:
        if source.exists():
            records.append({'source': pointer(source), 'source_bytes': source.stat().st_size,
                            'source_sha256': sha(source), 'output': None,
                            'transform': 'historical wallet extraction/derivation code fingerprint only'})

    # Predecessor hashes are recorded to make supersession traceable without
    # copying the private handoff or restating its outdated headline conclusions.
    for path in [NAV / 'boneohio_20261001_ENTRY/RESULT.md',
                 NAV / 'boneohio_20261002_PARITY/RESULT.md',
                 NAV / 'boneohio_20261001_ENTRY/rules/RULES.md']:
        records.append({'source': pointer(path), 'source_bytes': path.stat().st_size,
                        'source_sha256': sha(path), 'output': None,
                        'transform': 'source fingerprint only; conclusions superseded/qualified by FINAL'})

    metadata = {'exported_utc': datetime.now(timezone.utc).isoformat(),
                'task': TASK, 'scope': 'historical research evidence; latest market data is separate',
                'pointer_roots': {'$VIPER_PTMP': 'source HPC scratch root; private account component removed',
                                  '$VIPER_HOME': 'source researcher home root; private account component removed'},
                'files': records,
                'omissions': ['private handoff', 'fleet/trading configurations',
                              'individual own-account Q99 orders', 'credentials',
                              'transaction signatures', 'large market/feed datasets']}
    (out / 'PROVENANCE.json').write_text(json.dumps(metadata, indent=2), encoding='utf-8')
    print(json.dumps({'output': pointer(out), 'files': len(records),
                      'bytes': sum(p.stat().st_size for p in out.rglob('*') if p.is_file())}))


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--inspect', action='store_true')
    ap.add_argument('--out', type=Path, default=EXP / TASK / 'context')
    args = ap.parse_args()
    if args.inspect:
        inspect()
    else:
        export(args.out)
