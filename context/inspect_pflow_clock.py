"""Bounded source-manifest audit for the exported Polymarket clock lineage."""
from pathlib import Path
from datetime import datetime, timezone, timedelta
import argparse
import hashlib
import json


def run(args):
    rows = []
    first = datetime(2026, 9, 29, 23, tzinfo=timezone.utc)
    for offset in range(50):
        instant = first + timedelta(hours=offset)
        relative = Path('raw/v3') / instant.strftime('%Y-%m-%d/%H') / 'manifest.json'
        path = args.mirror / relative
        if not path.exists():
            rows.append({'hour_utc': instant.isoformat(), 'source': relative.as_posix(), 'exists': False,
                         'converter_fallback_rule': 'plain'})
            continue
        raw = path.read_bytes()
        source = json.loads(raw)
        note = str(source.get('sequence_note') or '')
        rule = 'earliest' if 'EARLIEST' in note else ('last_active' if 'LAST ACTIVE' in note else 'plain')
        rows.append({'hour_utc': instant.isoformat(), 'source': relative.as_posix(), 'exists': True,
                     'source_sha256': hashlib.sha256(raw).hexdigest(), 'source_bytes': len(raw),
                     'sequence_note': note, 'converter_rule': rule,
                     'main_period': datetime(2026, 9, 30, tzinfo=timezone.utc) <= instant < datetime(2026, 10, 2, tzinfo=timezone.utc)})
    counts = {rule: sum(row.get('converter_rule') == rule for row in rows if row.get('main_period'))
              for rule in ['earliest', 'last_active', 'plain']}
    report = {'checked_utc': datetime.now(timezone.utc).isoformat(),
        'scope': '48 main hours plus one warmup and one lifecycle-tail source hour',
        'main_hour_rules': counts, 'hour_manifests': rows,
        'conversion': {'source_timestamp_received_unit': 'microseconds',
                       'source_venue_timestamp_unit': 'milliseconds',
                       'earliest_or_plain': 'ts_ns=max(timestamp_received_us*1000, venue_ts_ms*1000000)',
                       'last_active': 'ts_ns=max((timestamp_received_us-arrival_skew_us)*1000, venue_ts_ms*1000000)',
                       'branch_selection': 'hour manifest sequence_note: EARLIEST wins, else LAST ACTIVE, else plain; missing/unreadable manifest -> plain',
                       'adapter': 'pf2cache.pf_tables preserves supplied ts_ns. Its separate reclock function is not called by the pflow-only converter.',
                       'derived_prune_rows': 'venue-BBO prune rows reuse their parent timestamp and receive newly assigned seq; synthetic cleanup events, not separate receives'},
        'source_code': [
            {'source': 'vendor_20260930_PFCACHE/pfcache.py:119-155',
             'sha256': '67c89e5b305548264f29bbb3576610456e569d2b8d1935c3bd267f625dbb8ac2'},
            {'source': 'vendor_20261001_PFEXT/pfcache.py:122-168',
             'sha256': '68b75bb06e8b9acb36456a11ee975e41fa940ea61d761b0d0bdf85525e194096'},
            {'source': 'vendor_20260930_CACHECMP/pf2cache.py:110-138',
             'sha256': '0f7e487a32df8f0300fa4feabcc3379daf28d35b73f23a6990ada14394622f7c'}],
        'limits': ['Vendor merged/adjusted receive provenance, not BoneOhio receipt or our native recorder location.',
                   'Clock clipping can change source arrival for rows where venue timestamp is later.',
                   'No constant synthetic venue+median-lag conversion was found in the pflow-only source path.',
                   'The converter source header geography/latency comments are not independently certified target facts.',
                   'Derived depth_clock carries these timestamps but is not an independent source capture.']}
    args.out.write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps({'main_hour_rules': counts,
                      'missing_manifests': sum(not row['exists'] for row in rows),
                      'distinct_notes': sorted({row.get('sequence_note', '') for row in rows})}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--mirror', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    run(parser.parse_args())
