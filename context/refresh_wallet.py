"""Bounded unauthenticated BoneOhio API export for the two-day study.

Public user-scoped keyset walks only. No secret/configuration is read. Raw
response bytes are fingerprinted; unrelated profile fields are not exported.
"""
from pathlib import Path
from datetime import datetime, timezone
import argparse
import hashlib
import json
import time
import urllib.parse
import urllib.request
import urllib.error

import pandas as pd

ADDRESS = '0x48ac40fc545cf327edd5365435c3a9f385614a7e'
BASE = 'https://data-api.polymarket.com'
START = int(datetime(2026, 9, 30, tzinfo=timezone.utc).timestamp())
END = int(datetime(2026, 10, 2, tzinfo=timezone.utc).timestamp())
FILL_COLUMNS = ['side', 'token_id', 'condition_id', 'size', 'price', 'timestamp',
                'slug', 'event_slug', 'outcome', 'outcome_index', 'transaction_hash']
ACTIVITY_COLUMNS = ['side', 'token_id', 'asset', 'condition_id', 'size', 'price',
                    'timestamp', 'slug', 'event_slug', 'outcome', 'outcome_index',
                    'transaction_hash', 'type', 'usdc_size', 'usdcSize']


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def request_json(url):
    for attempt in range(5):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'BoneOhio-public-research-bundle/1.0'})
            with urllib.request.urlopen(req, timeout=40) as response:
                raw = response.read()
            return raw, json.loads(raw)
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
            if attempt == 4:
                raise
            if isinstance(exc, urllib.error.HTTPError) and exc.code not in (429, 500, 502, 503, 504):
                raise
            time.sleep(min(8, 1 + attempt * 2))


def walk(endpoint, extra, max_pages):
    cursor = None
    seen = set()
    rows = []
    pages = []
    exhausted = False
    began = datetime.now(timezone.utc).isoformat()
    params_base = {'user': ADDRESS, 'start': START, 'end': END - 1, 'limit': 1000, **extra}
    for index in range(max_pages):
        params = dict(params_base)
        if cursor:
            params['cursor'] = cursor
        url = BASE + endpoint + '?' + urllib.parse.urlencode(params)
        raw, payload = request_json(url)
        data = payload.get('data')
        if not isinstance(data, list):
            raise ValueError(f'{endpoint}: expected list envelope, got {type(data).__name__}')
        pagination = payload.get('pagination') or {}
        next_cursor = pagination.get('next_cursor')
        pages.append({'page': index + 1, 'response_sha256': digest(raw), 'response_bytes': len(raw),
                      'rows': len(data), 'timestamp_min': min((x['timestamp'] for x in data if x.get('timestamp') is not None), default=None),
                      'timestamp_max': max((x['timestamp'] for x in data if x.get('timestamp') is not None), default=None),
                      'has_more': pagination.get('has_more'), 'next_cursor_present': bool(next_cursor)})
        rows.extend(data)
        print(json.dumps({'endpoint': endpoint, 'kind': extra, 'page': index + 1,
                          'rows_total': len(rows), 'has_next': bool(next_cursor)}), flush=True)
        if not next_cursor:
            exhausted = True
            break
        if next_cursor in seen:
            raise ValueError(f'{endpoint}: repeated cursor')
        seen.add(next_cursor)
        cursor = next_cursor
        time.sleep(.1)
    in_window = [row for row in rows if row.get('timestamp') is not None and START <= int(row['timestamp']) < END]
    return in_window, {'endpoint': BASE + endpoint, 'query_without_opaque_cursors': params_base,
                       'requested_utc_start': '2026-09-30T00:00:00Z',
                       'requested_utc_end_exclusive': '2026-10-02T00:00:00Z',
                       'pull_started_utc': began, 'pull_finished_utc': datetime.now(timezone.utc).isoformat(),
                       'cursor_exhausted': exhausted, 'max_pages': max_pages,
                       'raw_rows': len(rows), 'client_retained_rows': len(in_window),
                       'out_of_window_rows': len(rows) - len(in_window), 'pages': pages}


def key(row):
    return tuple(str(row.get(col)) for col in ['transaction_hash', 'token_id', 'side', 'timestamp', 'price', 'size'])


def save(rows, columns, path):
    frame = pd.DataFrame([{k: row.get(k) for k in columns} for row in rows], columns=columns)
    # Empty unknown-only columns are intentionally retained rather than invented.
    frame.to_parquet(path, index=False, compression='zstd')
    timestamps = pd.to_numeric(frame['timestamp'], errors='coerce') if 'timestamp' in frame else pd.Series(dtype=float)
    return {'path': path.name, 'sha256': digest(path.read_bytes()), 'bytes': path.stat().st_size,
            'rows': len(frame), 'columns': list(frame.columns),
            'timestamp_min': None if timestamps.empty else int(timestamps.min()),
            'timestamp_max': None if timestamps.empty else int(timestamps.max())}


def main(out, max_pages):
    out.mkdir(parents=True, exist_ok=True)
    all_rows, all_meta = walk('/v2/trades', {'taker_only': 'false'}, max_pages)
    taker_rows, taker_meta = walk('/v2/trades', {'taker_only': 'true'}, max_pages)
    taker_keys = {key(row) for row in taker_rows}
    all_keys = {key(row) for row in all_rows}
    for row in all_rows:
        row['api_role'] = 'TAKER' if key(row) in taker_keys else 'NOT_IN_TAKER_PULL'
        row['role_basis'] = 'exact six-field membership in separately exhausted taker-only walk'
    files = [save(all_rows, FILL_COLUMNS + ['api_role', 'role_basis'], out / 'fresh_fills_two_days.parquet'),
             save(taker_rows, FILL_COLUMNS, out / 'fresh_taker_fills_two_days.parquet')]
    activity_failure = None
    try:
        activity, activity_meta = walk('/v2/activity', {'exclude_deposits_withdrawals': 'false'}, max_pages)
        files.append(save(activity, ACTIVITY_COLUMNS, out / 'fresh_activity_two_days.parquet'))
        activity_counts = pd.Series([x.get('type') for x in activity]).value_counts(dropna=False).to_dict()
    except Exception as exc:
        activity_meta = {'endpoint': BASE + '/v2/activity', 'cursor_exhausted': False}
        activity_failure = type(exc).__name__ + ': ' + str(exc)
        activity_counts = {}
    status = None
    try:
        raw, status_payload = request_json(BASE + '/v2/status')
        # Public service freshness, not observer receipt or order-state evidence.
        status = {'checked_utc': datetime.now(timezone.utc).isoformat(),
                  'source_sha256': digest(raw), 'public_service_payload': status_payload}
    except Exception as exc:
        status = {'error_type': type(exc).__name__, 'available': False}
    report = {'target_public_wallet': ADDRESS, 'retrieved_utc': datetime.now(timezone.utc).isoformat(),
              'source_specification': 'https://data-api.polymarket.com/v2/docs',
              'walks': {'all_trades': all_meta, 'taker_trades': taker_meta, 'activity': activity_meta},
              'files': files, 'all_exact_duplicate_count': len(all_rows) - len(all_keys),
              'taker_exact_duplicate_count': len(taker_rows) - len(taker_keys),
              'taker_keys_missing_from_all': len(taker_keys - all_keys),
              'activity_type_counts': activity_counts, 'activity_failure': activity_failure,
              'public_service_status': status,
              'limits': ['All rows are public API representations, not a zero-fill submission ledger.',
                         'API seconds are not observed receipt, submission or matching time.',
                         'NOT_IN_TAKER_PULL is provisional API role inference, not guaranteed maker status.',
                         'No historical resolution, fee, PnL or order lifecycle is invented for fresh rows.',
                         'End is requested inclusive END-1; client selection is strict [START,END).',
                         'Cursor exhaustion is bounded endpoint coverage, not independent blockchain completeness.',
                         'Nontrade cashflow availability depends on the public activity endpoint; no exact capital inference.',
                         'Fresh tail transaction decoding/venue linking is a separate output if performed.']}
    (out / 'FRESH_PROVENANCE.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    print('COMPLETE', json.dumps({'files': files, 'exhausted': {k: v['cursor_exhausted'] for k, v in report['walks'].items()},
                                 'activity_types': activity_counts, 'activity_failure': activity_failure}), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--max-pages', type=int, default=100)
    args = parser.parse_args()
    main(args.out, args.max_pages)
