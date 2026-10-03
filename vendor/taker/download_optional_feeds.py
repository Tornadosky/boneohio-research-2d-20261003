"""Anonymous direct-provider hourly downloads into the user's private workspace.

Provider datasets never enter this repository/release. Byte-preserving downloads
with source URLs, SHA256 receipts, bounded retries and resumable hour cursor.
No API keys, JWTs, environment files or stored credentials are read or accepted.

Docs: https://www.cryptohftdata.com/docs/rest-authentication
Terms: https://www.cryptohftdata.com/terms
"""
import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import time
import urllib.error
import urllib.parse
import urllib.request

API = 'https://api.cryptohftdata.com/v1/download'
TERMS = 'https://www.cryptohftdata.com/terms'
DEFAULT_SERIES = [('binance_futures','BTCUSDT','orderbook'),
                  ('binance_futures','BTCUSDT','trades'),
                  # /v1/status verified 2026-10-03 advertises this actual key;
                  # the static exchange-doc page calls it kraken_futures.
                  ('kraken_derivatives','PF_XBTUSD','orderbook'),
                  ('kraken_derivatives','PF_XBTUSD','trades')]
PACKAGE = Path(__file__).resolve().parents[2]


def parse_hour(value):
    dt = datetime.fromisoformat(value.replace('Z','+00:00'))
    dt = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
    if dt.minute or dt.second or dt.microsecond:
        raise ValueError('start/end must be exact UTC hours')
    return dt


def source_url(exchange,symbol,kind,hour):
    if not all(re.fullmatch(r'[A-Za-z0-9_]+',x) for x in (exchange,symbol,kind)):
        raise ValueError('invalid provider file identifier')
    rel = f'{exchange}/{hour:%Y-%m-%d}/{hour:%H}/{symbol}_{kind}.parquet'
    return API+'?'+urllib.parse.urlencode({'file':rel}),rel


def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda:stream.read(1024*1024),b''):
            h.update(block)
    return h.hexdigest()


def receipt(path):
    if not path.is_file() or path.stat().st_size<8:
        return False
    with path.open('rb') as stream:
        first = stream.read(4)
        stream.seek(-4,2)
        last = stream.read(4)
    # Retain original zstd wrappers as bytes; the reader detects them separately.
    return (first==b'PAR1' and last==b'PAR1') or first==bytes([0x28,0xB5,0x2F,0xFD])


def download(url,destination,*,retries=4,timeout=60,interval=1.25):
    """Serial requests stay below the documented anonymous 60/minute limit."""
    part = destination.with_suffix('.parquet.part')
    for attempt in range(retries):
        time.sleep(interval)
        try:
            request = urllib.request.Request(url,headers={'User-Agent':'bonebundle-private-research/1.0'})
            with urllib.request.urlopen(request,timeout=timeout) as response:
                destination.parent.mkdir(parents=True,exist_ok=True)
                with part.open('wb') as output:
                    while True:
                        data = response.read(1024*1024)
                        if not data:
                            break
                        output.write(data)
            if not receipt(part):
                raise ValueError('response is not Parquet or wrapped zstd')
            part.replace(destination)
            return dict(status='DOWNLOADED',attempts=attempt+1,bytes=destination.stat().st_size,sha256=sha(destination))
        except urllib.error.HTTPError as error:
            if error.code==404:
                return dict(status='MISSING_404',attempts=attempt+1)
            if error.code in (401,403):
                return dict(status='ACCESS_REJECTED',http_status=error.code,attempts=attempt+1)
            if attempt+1==retries:
                return dict(status='FAILED_HTTP',http_status=error.code,attempts=attempt+1)
            retry_after = error.headers.get('Retry-After','') if error.headers else ''
            delay = min(30,float(retry_after)) if retry_after.isdigit() else min(20,2**attempt)
            time.sleep(max(0,delay))
        except (OSError,ValueError,TimeoutError):
            if attempt+1==retries:
                return dict(status='FAILED_NETWORK_OR_FORMAT',attempts=attempt+1)
            time.sleep(min(20,2**attempt))
    return dict(status='FAILED')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,default=Path.home()/'.cache/bonebundle/vendor_private')
    parser.add_argument('--start',default='2026-09-29T23:00:00Z')
    parser.add_argument('--end',default='2026-10-02T01:00:00Z',help='exclusive hour; includes the 00:00 end-tail hour')
    parser.add_argument('--series',action='append',help='exchange:symbol:type; repeat to override the four defaults')
    parser.add_argument('--dry-run',action='store_true')
    parser.add_argument('--retries',type=int,default=4)
    parser.add_argument('--timeout',type=int,default=60)
    parser.add_argument('--interval',type=float,default=1.25)
    parser.add_argument('--max-files',type=int,help='bounded smoke or partial run; resume reuses verified receipts')
    args = parser.parse_args()
    if not 1<=args.retries<=8 or not 1<=args.timeout<=120 or args.interval<1.1:
        parser.error('retries 1..8, timeout 1..120, interval >=1.1 seconds required')
    root = args.output.expanduser().resolve()
    if root==PACKAGE or PACKAGE in root.parents:
        parser.error('provider files must be outside the repository; choose a private workspace directory')
    start,end = parse_hour(args.start),parse_hour(args.end)
    if end<=start or end-start>timedelta(days=7):
        parser.error('positive window up to seven days required')
    series = [tuple(s.split(':')) for s in args.series] if args.series else DEFAULT_SERIES
    if any(len(s)!=3 for s in series):
        parser.error('each --series is exchange:symbol:type')
    objects = []
    hour = start
    while hour<end:
        for exchange,symbol,kind in series:
            url,rel = source_url(exchange,symbol,kind,hour)
            objects.append((url,rel))
        hour += timedelta(hours=1)
    if args.max_files is not None:
        if args.max_files<1:
            parser.error('--max-files must be positive')
        objects = objects[:args.max_files]
    if args.dry_run:
        print(json.dumps(dict(output=str(root),start=start.isoformat(),end_exclusive=end.isoformat(),
                              files=len(objects),series=series,terms=TERMS,example_urls=[u for u,r in objects[:4]]),indent=2))
        return 0
    root.mkdir(parents=True,exist_ok=True)
    index_path = root/'DOWNLOAD_MANIFEST.json'
    index = json.loads(index_path.read_text()) if index_path.exists() else dict(provider='CryptoHFTData',terms=TERMS,
               redistribution='Excluded from public bundle; provider standard terms govern personal/internal use',objects={})
    failures = 0
    for number,(url,rel) in enumerate(objects,1):
        destination = root/rel
        prior = index['objects'].get(rel,{})
        if prior.get('sha256') and receipt(destination) and sha(destination)==prior['sha256']:
            result = dict(prior,status='SKIPPED_VERIFIED')
        else:
            result = download(url,destination,retries=args.retries,timeout=args.timeout,interval=args.interval)
            result.update(source_url=url,download_utc=datetime.now(timezone.utc).isoformat(),file=rel)
        index['objects'][rel] = result
        index['cursor'] = dict(attempted=number,planned=len(objects),last_file=rel)
        temporary = index_path.with_suffix('.json.part')
        temporary.write_text(json.dumps(index,indent=2)+'\n')
        temporary.replace(index_path)
        print(json.dumps(dict(file=rel,status=result['status'],completed=number,total=len(objects))),flush=True)
        if result['status'] not in ('DOWNLOADED','SKIPPED_VERIFIED'):
            failures += 1
        if result['status']=='ACCESS_REJECTED':
            print('Anonymous access rejected by provider; no credential fallback attempted.',flush=True)
            break
    print(json.dumps(dict(files_attempted=number,non_success=failures,manifest=str(index_path))))
    return 2 if failures else 0


if __name__=='__main__':
    raise SystemExit(main())
