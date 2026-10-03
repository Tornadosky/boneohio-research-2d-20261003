#!/usr/bin/env python3
"""Download selected public release shards, verify hashes and extract safely."""
import argparse,hashlib,json,os,shutil,tarfile,urllib.request
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]

def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda:f.read(4*1024*1024),b''):h.update(chunk)
    return h.hexdigest()

def extract(archive,target):
    target=target.resolve()
    with tarfile.open(archive,'r:*') as tf:
        for member in tf.getmembers():
            dest=(target/member.name).resolve()
            if not dest.is_relative_to(target) or not member.isfile():
                raise ValueError('Unsafe archive member: '+member.name)
        # All members were restricted to regular files and resolved paths above.
        # This also works on Python 3.11 versions predating extraction filters.
        kwargs={'filter':'data'} if hasattr(tarfile,'data_filter') else {}
        tf.extractall(target,members=tf.getmembers(),**kwargs)
        return [m.name for m in tf.getmembers()]

def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--groups',default='metadata,features,recorder',help='Comma-separated groups, or all; use --list to inspect sizes')
    ap.add_argument('--tenor',choices=['both','btc_5m','btc_15m'],default='both')
    ap.add_argument('--day',choices=['both','2026-09-30','2026-10-01'],default='both')
    ap.add_argument('--list',action='store_true')
    ap.add_argument('--keep-archives',action='store_true')
    ap.add_argument('--force',action='store_true')
    args=ap.parse_args()
    manifest=json.loads((ROOT/'assets.json').read_text())
    wanted=set(args.groups.split(','));selected=[]
    for a in manifest['assets']:
        if 'all' not in wanted and a['group'] not in wanted:continue
        if a.get('tenor') and args.tenor!='both' and a['tenor']!=args.tenor:continue
        if a.get('day') and args.day!='both' and a['day']!=args.day:continue
        selected.append(a)
    if args.list:
        for a in manifest['assets']:print(f"{a['group']:14s} {a['bytes']/2**20:9.1f} MiB  {a['name']}")
        print(f"Selected download: {sum(a['bytes'] for a in selected)/2**30:.2f} GiB; extracted: {sum(a['extracted_bytes'] for a in selected)/2**30:.2f} GiB")
        return
    if not selected:raise SystemExit('No matching groups')
    (ROOT/'downloads').mkdir(exist_ok=True)
    receipts=ROOT/'data/.shards';receipts.mkdir(parents=True,exist_ok=True)
    available=shutil.disk_usage(ROOT).free
    required=sum(a['extracted_bytes'] for a in selected)+max(a['bytes'] for a in selected)
    if available<required and args.force is False:
        raise SystemExit(f'Need approximately {required/2**30:.2f} GiB free; have {available/2**30:.2f}. Select fewer groups/days, or --force if existing files account for the difference.')
    for a in selected:
        done=receipts/(a['name']+'.json')
        if done.exists() and not args.force:
            receipt=json.loads(done.read_text())
            if receipt.get('sha256')==a['sha256'] and receipt.get('paths') and all((ROOT/p).is_file() for p in receipt['paths']):
                print('Already extracted:',a['name'],flush=True);continue
        dst=ROOT/'downloads'/a['name'];tmp=dst.with_name(dst.name+'.partial')
        if not dst.exists() or dst.stat().st_size!=a['bytes'] or digest(dst)!=a['sha256']:
            print(f"Downloading {a['name']} ({a['bytes']/2**20:.1f} MiB)",flush=True)
            req=urllib.request.Request(a['url'],headers={'User-Agent':'boneohio-research-bundle'})
            with urllib.request.urlopen(req,timeout=120) as response,tmp.open('wb') as out:
                shutil.copyfileobj(response,out,4*1024*1024)
            if tmp.stat().st_size!=a['bytes'] or digest(tmp)!=a['sha256']:
                tmp.unlink(missing_ok=True);raise RuntimeError('Download hash/length mismatch: '+a['name'])
            tmp.replace(dst)
        names=extract(dst,ROOT)
        if len(names)!=a['files']:raise ValueError('Archive member count mismatch: '+a['name'])
        done.write_text(json.dumps({'sha256':a['sha256'],'files':a['files'],'paths':names}))
        if not args.keep_archives:dst.unlink()
        print('Verified and extracted:',a['name'],flush=True)
    print('Run python scripts/validate_bundle.py for per-file verification.')

if __name__=='__main__':main()
