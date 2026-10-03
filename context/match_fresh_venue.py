"""Conservative public-fill to public-print links, with ambiguity preserved.

Links improve retrospective event labels. They do not observe wallet receipt,
decision, submission, matching-engine time, or canceled/unfilled orders.
"""
from pathlib import Path
from datetime import datetime, timezone
import argparse
import hashlib
import json

import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
TAPE_COLS = ['session_id', 'seq', 'event_uid', 'venue_ts_ms', 'ts_ns', 'flags',
             'token_id', 'is_yes', 'price_micros', 'size_micros', 'side', 'transaction_hash']


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def nullable_int(value):
    return None if pd.isna(value) else int(value)


def link_one_fill(fill, prints, price_tolerance=.01):
    """Return one summary and ALL direct/complement candidates for a mapped fill.

    prints must already be confined to the same catalog contract and public
    transaction. No nearest-time, maximum-edge, future outcome or favorable
    fill choice is made. Identical-looking source rows are not silently deduped.
    """
    summary = {'direct_candidate_count': 0, 'complement_candidate_count': 0,
               'mapped_print_venue_ms': None, 'mapped_print_cache_ns': None,
               'mapped_print_price': None, 'api_minus_mapped_print_ms': None,
               'first_candidate_venue_ms': None, 'last_candidate_venue_ms': None,
               'print_link_status': 'no_matching_transaction_print',
               'selected_candidate_id': None}
    candidates = []
    for row in prints.to_dict('records'):
        direct = str(row['token_id']) == str(fill['token_id'])
        complementary = pd.notna(row['is_yes']) and pd.notna(fill['is_yes']) and bool(row['is_yes']) != bool(fill['is_yes'])
        if not direct and not complementary:
            continue
        price = None if pd.isna(row['price_micros']) else float(row['price_micros']) / 1e6
        mapped_price = None if price is None else price if direct else 1 - price
        venue_time = nullable_int(row['venue_ts_ms'])
        print_shares = None if pd.isna(row['size_micros']) else float(row['size_micros']) / 1e6
        candidate = {**row, 'wallet_row_id': fill['wallet_row_id'],
                     'wallet_token_id': str(fill['token_id']),
                     'candidate_link_type': 'direct_tx_token' if direct else 'complement_tx_opposite_token',
                     'price_in_wallet_outcome': mapped_price,
                     'api_price_difference': None if mapped_price is None else float(fill['price']) - mapped_price,
                     'price_within_tolerance': mapped_price is not None and abs(float(fill['price']) - mapped_price) <= price_tolerance + 1e-12,
                     'api_minus_venue_ms': None if venue_time is None else int(fill['timestamp']) * 1000 - venue_time,
                     'source_print_shares': print_shares,
                     'api_share_difference': None if print_shares is None else float(fill['size']) - print_shares}
        candidates.append(candidate)
    direct = [x for x in candidates if x['candidate_link_type'] == 'direct_tx_token']
    complementary = [x for x in candidates if x['candidate_link_type'] == 'complement_tx_opposite_token']
    summary['direct_candidate_count'] = len(direct)
    summary['complement_candidate_count'] = len(complementary)
    positive_times = [nullable_int(row['venue_ts_ms']) for row in candidates
                     if pd.notna(row['venue_ts_ms']) and nullable_int(row['venue_ts_ms']) > 0]
    if positive_times:
        summary['first_candidate_venue_ms'] = min(positive_times)
        summary['last_candidate_venue_ms'] = max(positive_times)
    preferred = direct if direct else complementary
    kind = 'direct' if direct else 'complement'
    if not preferred:
        return summary, candidates
    if len(preferred) != 1:
        summary['print_link_status'] = f'ambiguous_{kind}_prints'
        return summary, candidates
    candidate = preferred[0]
    if pd.isna(candidate['venue_ts_ms']) or int(candidate['venue_ts_ms']) <= 0:
        summary['print_link_status'] = f'{kind}_nonpositive_venue_timestamp'
        return summary, candidates
    if not candidate['price_within_tolerance']:
        summary['print_link_status'] = f'{kind}_single_price_mismatch'
        return summary, candidates
    summary.update(print_link_status=f'{kind}_single_price_consistent_candidate',
                   selected_candidate_id=candidate['candidate_id'],
                   mapped_print_venue_ms=int(candidate['venue_ts_ms']),
                   mapped_print_cache_ns=nullable_int(candidate['ts_ns']),
                   mapped_print_price=candidate['price_in_wallet_outcome'],
                   api_minus_mapped_print_ms=candidate['api_minus_venue_ms'])
    return summary, candidates


def run(args):
    fills_path = args.wallet_root / 'fresh_fills_two_days.parquet'
    assets_path = args.data_root / 'poly/catalog/assets.parquet'
    fills = pq.ParquetFile(fills_path).read().to_pandas()
    fills['wallet_row_id'] = range(len(fills))
    fills['token_id'] = fills.token_id.astype(str)
    fills['transaction_hash'] = fills.transaction_hash.str.lower()
    identity_path = args.wallet_root / 'fresh_order_fill_identity_two_days.parquet'
    identity_present = identity_path.exists()
    if identity_present:
        identity = pq.ParquetFile(identity_path).read(columns=[
            'transaction_hash', 'token_id', 'salt', 'decoded_role', 'order_ts_ms',
            'order_limit', 'requested_shares']).to_pandas()
        identity['transaction_hash'] = identity.transaction_hash.str.lower()
        identity['token_id'] = identity.token_id.astype(str)
        if identity.duplicated(['transaction_hash', 'token_id']).any():
            raise ValueError('Fresh signed identity is ambiguous; preserve rows and resolve identity separately')
        fills = fills.merge(identity, on=['transaction_hash', 'token_id'], how='left', validate='many_to_one')
    assets = pq.ParquetFile(assets_path).read(columns=[
        'token_id', 'market_key', 'contract_start_ms', 'contract_end_ms', 'is_yes']).to_pandas()
    assets['token_id'] = assets.token_id.astype(str)
    assets = assets.drop_duplicates()
    if assets.duplicated('token_id').any():
        raise ValueError('Catalog token has conflicting mappings')
    fills = fills.merge(assets, on='token_id', how='left', validate='many_to_one')
    fills['api_after_contract_end_ms'] = fills.timestamp * 1000 - fills.contract_end_ms
    # Unknown other-asset rows remain in the summary with an explicit scope state.
    results = {}
    all_candidates = []
    input_tapes = []
    groups = list(fills[fills.market_key.notna()].groupby(['market_key', 'contract_start_ms'], sort=True))
    for index, ((market_key, cs), group) in enumerate(groups):
        if args.max_contracts is not None and index >= args.max_contracts:
            for row in group.to_dict('records'):
                results[row['wallet_row_id']] = {'print_link_status': 'not_examined_bounded_run'}
            continue
        part = args.data_root / 'poly/trade_tape' / f'market_key={market_key}' / f'contract_start_ms={int(cs)}'
        paths = sorted(part.glob('session_id=*/*.parquet'))
        if not paths:
            for row in group.to_dict('records'):
                results[row['wallet_row_id']] = {'print_link_status': 'missing_downloaded_tape_partition'}
            continue
        pieces = []
        partition_first = None
        partition_last = None
        partition_rows = 0
        transactions = set(group.transaction_hash)
        valid_tokens = set(assets[(assets.market_key == market_key) &
                           (assets.contract_start_ms == cs)].token_id)
        for path in paths:
            file = pq.ParquetFile(path)
            frame = file.read(columns=TAPE_COLS).to_pandas()
            partition_rows += len(frame)
            valid_times = frame.loc[frame.venue_ts_ms.notna() & (frame.venue_ts_ms > 0), 'venue_ts_ms']
            if len(valid_times):
                first, last = int(valid_times.min()), int(valid_times.max())
                partition_first = first if partition_first is None else min(partition_first, first)
                partition_last = last if partition_last is None else max(partition_last, last)
            frame['source_row_ordinal'] = range(len(frame))
            frame['transaction_hash'] = frame.transaction_hash.str.lower()
            frame['token_id'] = frame.token_id.astype(str)
            frame = frame[frame.transaction_hash.isin(transactions) & frame.token_id.isin(valid_tokens)].copy()
            relative = path.relative_to(args.data_root).as_posix()
            frame['source_file'] = relative
            frame['candidate_id'] = [relative + '#' + str(value) for value in frame.source_row_ordinal]
            pieces.append(frame)
            input_tapes.append({'file': relative, 'sha256': digest(path),
                                'bytes': path.stat().st_size, 'rows': file.metadata.num_rows})
        tape = pd.concat(pieces, ignore_index=True)
        by_transaction = {key: frame for key, frame in tape.groupby('transaction_hash', sort=False)}
        for row in group.to_dict('records'):
            summary, candidates = link_one_fill(row, by_transaction.get(row['transaction_hash'], tape.iloc[0:0]), args.price_tolerance)
            api_ms = int(row['timestamp']) * 1000
            summary.update(partition_source_print_rows=partition_rows,
                           partition_first_venue_ms=partition_first,
                           partition_last_venue_ms=partition_last,
                           api_minus_partition_last_venue_ms=None if partition_last is None else api_ms - partition_last,
                           api_timestamp_after_partition_last_print=None if partition_last is None else api_ms > partition_last,
                           api_timestamp_before_partition_first_print=None if partition_first is None else api_ms < partition_first)
            results[row['wallet_row_id']] = summary
            all_candidates.extend(candidates)
    for row in fills[fills.market_key.isna()].to_dict('records'):
        results[row['wallet_row_id']] = {'print_link_status': 'outside_exported_token_catalog'}
    links = fills.merge(pd.DataFrame([{'wallet_row_id': row_id, **summary}
                 for row_id, summary in results.items()]), on='wallet_row_id', validate='one_to_one')
    candidates = pd.DataFrame(all_candidates)
    if candidates.empty:
        candidates = pd.DataFrame(columns=TAPE_COLS + ['wallet_row_id', 'candidate_id', 'candidate_link_type'])
    args.output.mkdir(parents=True, exist_ok=True)
    outputs = []
    for name, frame in [('fresh_fill_venue_links.parquet', links),
                         ('fresh_venue_print_candidates.parquet', candidates)]:
        path = args.output / name
        frame.to_parquet(path, index=False, compression='zstd')
        outputs.append({'file': name, 'sha256': digest(path), 'bytes': path.stat().st_size,
                        'rows': len(frame), 'columns': list(frame.columns)})
    mapped = links[links.mapped_print_venue_ms.notna()] if 'mapped_print_venue_ms' in links else links.iloc[0:0]
    lags = mapped.api_minus_mapped_print_ms.dropna() if 'api_minus_mapped_print_ms' in mapped else pd.Series(dtype=float)
    report = {'created_utc': datetime.now(timezone.utc).isoformat(), 'script_sha256': digest(Path(__file__)),
        'source_fresh_fills_sha256': digest(fills_path), 'source_assets_sha256': digest(assets_path),
        'source_fresh_identity_sha256': digest(identity_path) if identity_present else None,
        'price_tolerance': args.price_tolerance, 'max_contracts': args.max_contracts,
        'wallet_rows': len(fills), 'catalog_mapped_rows': int(fills.market_key.notna().sum()),
        'status_counts': links.print_link_status.value_counts().to_dict(),
        'mapped_rows': len(mapped), 'candidate_rows': len(candidates),
        'mapped_api_minus_print_ms': {'n': len(lags), 'negative': int((lags < 0).sum()),
            **{f'p{q}': float(lags.quantile(q/100)) if len(lags) else None for q in [0, 10, 50, 90, 99, 100]}},
        'latest_mapped_venue_ms': float(mapped.mapped_print_venue_ms.max()) if len(mapped) else None,
        'input_tapes': input_tapes, 'outputs': outputs,
        'unmatched_catalog_rows': links.loc[links.print_link_status.eq('no_matching_transaction_print'), [
            'wallet_row_id', 'transaction_hash', 'token_id', 'market_key', 'contract_start_ms',
            'contract_end_ms', 'timestamp', 'api_after_contract_end_ms', 'partition_first_venue_ms',
            'partition_last_venue_ms', 'api_minus_partition_last_venue_ms',
            'api_timestamp_after_partition_last_print']].to_dict('records'),
        'legacy_reference': {'file': 'context/legacy_gt_reference.py',
             'original_source': 'boneohio_20261002_PARITY/gt.py',
             'original_sha256': 'b982cb48ab5d80312babb773b1cb6e3e49afd3cd752516dc2fbf95490f6c69b7',
             'sanitized_sha256': '2f55e6e506174029a4b46dcf839dd08d7d0919f977d98d610ab2e2f45ee279b1'},
        'limits': ['Retrospective public print attribution only; no observed wallet decision/submit/match time.',
                  'Direct transaction/token candidates are preferred; otherwise opposite catalog token is explicitly complemented.',
                  'Multiple preferred candidates leave mapped time null even if timestamps are equal.',
                  'No candidate is selected by minimum API lag, maximum edge, outcome, fill quantity or profitability.',
                  'A single price-consistent complement candidate remains an attribution candidate, not exact signed-order proof.',
                  'Source venue time must be positive; normalized cache clock is never substituted as venue time.',
                  'Every source candidate, price/quantity discrepancy and missing partition is preserved.',
                  'Partition first/last positive venue times are computed before transaction filtering; they are observed print ranges, not certified continuous capture.',
                  'API-after-partition flags diagnose interval censoring only; API seconds already lag public prints and cannot prove the missing event occurred beyond the tail.',
                  'No print-minus40 matching proxy or reaction latency is fabricated.',
                  'API time selection may censor prints at the requested interval edges; all-asset rows outside BTC catalog remain explicit.',
                  'Pflow collector clocks follow the audited per-row vendor witness merge; no CHD source is read.']}
    (args.output / 'FRESH_VENUE_PROVENANCE.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps({key: report[key] for key in ['wallet_rows', 'catalog_mapped_rows',
          'status_counts', 'mapped_rows', 'candidate_rows', 'mapped_api_minus_print_ms', 'latest_mapped_venue_ms']}, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-root', type=Path, default=ROOT / 'data')
    parser.add_argument('--wallet-root', type=Path, default=ROOT / 'context/wallet')
    parser.add_argument('--output', type=Path, default=ROOT / 'results/fresh_wallet_match')
    parser.add_argument('--price-tolerance', type=float, default=.01)
    parser.add_argument('--max-contracts', type=int)
    run(parser.parse_args())
