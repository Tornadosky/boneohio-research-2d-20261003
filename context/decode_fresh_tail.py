"""Decode missing public filled transactions; export target-only order links.

No signature, signer, opaque metadata, builder identifier or counterpart row
is exported. Historical ABI is retained and versioned as an explicit assumption.
"""
from pathlib import Path
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import sys
import time
import urllib.request

import pandas as pd

BONE = '0x48ac40fc545cf327edd5365435c3a9f385614a7e'
RPCS = ['https://polygon-bor-rpc.publicnode.com', 'https://polygon.drpc.org',
        'https://polygon-rpc.com']
SELECTOR = '0x3c2b4399'
ORDER_TYPE = '(uint256,address,address,uint256,uint256,uint256,uint8,uint8,uint256,bytes32,bytes32,bytes)'
ABI_TYPES = ['bytes32', ORDER_TYPE, ORDER_TYPE + '[]', 'uint256', 'uint256[]', 'uint256', 'uint256[]']


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def run(args):
    if args.abi_deps:
        sys.path.insert(0, str(args.abi_deps))
    from eth_abi import decode
    args.out.mkdir(parents=True, exist_ok=True)
    fresh = pd.read_parquet(args.fresh_fills)
    historical = pd.read_parquet(args.historical_links, columns=['tx'])
    wanted = sorted(set(fresh.transaction_hash.dropna()) - set(historical.tx.dropna()))
    if len(wanted) > args.max_transactions:
        raise ValueError(f'{len(wanted)} transactions exceed declared bound {args.max_transactions}')
    began = datetime.now(timezone.utc).isoformat()
    batches = [wanted[index:index + 20] for index in range(0, len(wanted), 20)]

    def fetch_batch(pair):
        index, hashes = pair
        request_body = json.dumps([{'jsonrpc': '2.0', 'id': n, 'method': 'eth_getTransactionByHash',
                                   'params': [h]} for n, h in enumerate(hashes)]).encode()
        last = None
        for attempt in range(6):
            rpc = RPCS[(index + attempt) % len(RPCS)]
            try:
                req = urllib.request.Request(rpc, data=request_body,
                        headers={'Content-Type': 'application/json', 'User-Agent': 'BoneOhio-public-research/1.0'})
                with urllib.request.urlopen(req, timeout=30) as response:
                    raw = response.read()
                payload = json.loads(raw)
                if not isinstance(payload, list):
                    raise ValueError('batch RPC response is not list')
                lookup = {row.get('id'): row.get('result') for row in payload}
                if any(lookup.get(n) is None for n in range(len(hashes))):
                    raise ValueError('missing RPC transaction response')
                return [lookup[n] for n in range(len(hashes))], {
                    'rpc': rpc, 'batch_index': index, 'transactions': len(hashes),
                    'response_sha256': digest(raw), 'response_bytes': len(raw)}
            except Exception as exc:
                last = type(exc).__name__ + ': ' + str(exc)
                time.sleep(min(6, 1 + attempt))
        return [], {'batch_index': index, 'transactions': len(hashes), 'error': last,
                    'failed_transaction_hashes': hashes}

    rows = []
    audit = []
    unknown = []
    decoded = set()
    with ThreadPoolExecutor(max_workers=4) as pool:
        for transactions, info in pool.map(fetch_batch, enumerate(batches)):
            audit.append(info)
            for tx in transactions:
                inp = tx.get('input') or ''
                if not inp.startswith(SELECTOR):
                    unknown.append({'transaction_hash': tx['hash'], 'selector': inp[:10], 'reason': 'unrecognized selector'})
                    continue
                try:
                    cond, taker, makers, taker_fill, maker_fills, taker_fee, maker_fees = decode(ABI_TYPES, bytes.fromhex(inp[10:]))
                    found = 0
                    appearances = [(taker, taker_fill, taker_fee)] + list(zip(makers, maker_fills, maker_fees))
                    for position, (order, fill, fee) in enumerate(appearances):
                        salt, maker, signer, token, maker_amount, taker_amount, side, sig_type, order_time, meta, builder, signature = order
                        if maker.lower() != BONE:
                            continue
                        found += 1
                        idx = position - 1
                        rows.append({'tx': tx['hash'], 'block': int(tx['blockNumber'], 16),
                            'cond': '0x' + cond.hex(), 'idx': idx, 'token': str(token),
                            'maker_amt': int(maker_amount), 'taker_amt': int(taker_amount),
                            'side': int(side), 'sig_type': int(sig_type), 'order_ts_ms': int(order_time),
                            'salt': str(salt), 'fill_amt': int(fill), 'fee_amt': int(fee),
                            'decoded_role': 'TAKER' if idx == -1 else 'MAKER',
                            'calldata_sha256': digest(bytes.fromhex(inp[2:])), 'fresh_tail': True})
                    if found:
                        decoded.add(tx['hash'])
                    else:
                        unknown.append({'transaction_hash': tx['hash'], 'reason': 'decoded but no target maker rows'})
                except Exception as exc:
                    unknown.append({'transaction_hash': tx['hash'], 'reason': type(exc).__name__ + ': ' + str(exc)})
            print(json.dumps({'batches_finished': len(audit), 'of': len(batches),
                              'target_order_rows': len(rows), 'decoded_transactions': len(decoded),
                              'errors': len(unknown)}), flush=True)
    frame = pd.DataFrame(rows)
    target = args.out / 'fresh_tail_chain_links.parquet'
    frame.to_parquet(target, index=False, compression='zstd')
    unresolved = sorted(set(wanted) - decoded)
    report = {'public_wallet': BONE, 'started_utc': began,
        'finished_utc': datetime.now(timezone.utc).isoformat(),
        'source_fresh_fills_sha256': digest(args.fresh_fills.read_bytes()),
        'source_historical_links_sha256': digest(args.historical_links.read_bytes()),
        'script_sha256': None, 'selector': SELECTOR, 'abi_types': ABI_TYPES,
        'abi_lineage': 'historical boneohio_20261001_ENTRY/decode_txs.py; exact source hash in context/PROVENANCE.json',
        'max_transactions': args.max_transactions, 'missing_historical_transactions': len(wanted),
        'successfully_decoded_target_transactions': len(decoded), 'target_order_appearances': len(frame),
        'unique_salts': int(frame.salt.nunique()) if len(frame) else 0,
        'rpc_batch_audit': audit, 'exceptions': unknown,
        'unresolved_transaction_hashes': unresolved,
        'output': {'path': target.name, 'sha256': digest(target.read_bytes()),
                   'bytes': target.stat().st_size, 'rows': len(frame), 'columns': list(frame.columns)},
        'limits': ['Public filled transaction calldata only; no zero-fill submissions or private user stream.',
                   'Role follows matchOrders argument position; a salt may have different roles across fills.',
                   'Raw fill amounts are maker-asset units; preserve exact conversion semantics.',
                   'order_ts_ms is observed calldata, not observed signing/submission time.',
                   'No original signatures, signers, builder metadata or counterpart details are exported.',
                   'Fresh venue-print/footprint/lifecycle/outcome joins are separate; none is invented here.']}
    (args.out / 'FRESH_CHAIN_PROVENANCE.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    print('COMPLETE', json.dumps({k: report[k] for k in ['missing_historical_transactions',
          'successfully_decoded_target_transactions', 'target_order_appearances', 'unique_salts']}), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--fresh-fills', type=Path, required=True)
    parser.add_argument('--historical-links', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--abi-deps', type=Path)
    parser.add_argument('--max-transactions', type=int, default=500)
    run(parser.parse_args())
