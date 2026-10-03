"""Independently rebuild handoff catalogs from licensed PendulumFlow V3 metadata.

The original private cache catalogs are read only to select contracts, preserve
our integer local IDs, and compare identities. Their metadata is never used as
the public provenance or silently accepted as an independent verification.
Read only rare new_market row groups; no full-event scan or research simulation.
"""
import argparse
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import time

for name in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMBA_NUM_THREADS"):
    os.environ[name] = "1"

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

SOURCE_URL = "https://archive.pendulumflow.com"
DOWNLOAD_URL = "https://dl.pendulumflow.com"
LICENSE_URL = "https://creativecommons.org/licenses/by/4.0/"
_SLUGS = None


def sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def token(value):
    if isinstance(value, (bytes, bytearray)):
        return str(int.from_bytes(value, "big"))
    return str(value)


def condition(value):
    return "0x" + value.hex() if isinstance(value, (bytes, bytearray)) else str(value).lower()


def orientation(label):
    value = str(label).lower()
    if value in ("up", "yes"):
        return 0
    if value in ("down", "no"):
        return 1
    raise ValueError(f"nonbinary lifecycle outcome {label!r}")


def init_worker(slugs):
    global _SLUGS
    _SLUGS = pa.array(slugs, type=pa.string())


def scan_hour(path):
    """Parquet statistics skip every row group with no non-null slug values."""
    path = Path(path)
    f = pq.ParquetFile(path)
    if "slug" not in f.schema_arrow.names:
        return [], {"hour": path.stem, "metadata_groups": 0, "skip": "no_slug_column"}
    index = f.schema.names.index("slug")
    want = ("event_type", "market", "id", "assets_ids", "outcomes", "slug", "timestamp")
    missing = set(want) - set(f.schema_arrow.names)
    if missing:
        return [], {"hour": path.stem, "metadata_groups": 0, "skip": "missing_lifecycle_fields", "missing": sorted(missing)}
    rows, groups = [], 0
    for i in range(f.num_row_groups):
        stats = f.metadata.row_group(i).column(index).statistics
        # V3 stores event types together: in the sampled 108.6M-row hour only
        # one of 420 groups has any slug. Never decode a no-slug event group.
        if stats is not None and stats.num_values == 0:
            continue
        small = f.read_row_group(i, columns=["slug", "event_type"])
        mask = pc.and_(pc.equal(small["event_type"].cast(pa.string()), "new_market"), pc.is_in(small["slug"], _SLUGS))
        if not pc.any(mask).as_py():
            continue
        groups += 1
        metadata = f.read_row_group(i, columns=list(want)).filter(mask)
        for r in metadata.to_pylist():
            ids = r["assets_ids"] or []
            outcomes = r["outcomes"] or []
            if len(ids) != 2 or len(outcomes) != 2:
                raise ValueError(f"nonbinary metadata for target {r['slug']}")
            pairs = {token(t): orientation(o) for t, o in zip(ids, outcomes)}
            if len(pairs) != 2 or set(pairs.values()) != {0, 1}:
                raise ValueError(f"ambiguous metadata for target {r['slug']}")
            rows.append({"slug": r["slug"], "condition_id": condition(r["market"]),
                         "venue_market_id": str(r["id"]), "token_outcomes": pairs,
                         "event_type": "new_market", "event_venue_time": r["timestamp"].isoformat() if r["timestamp"] else None,
                         "source_hour": path.stem,
                         "source_url": f"{DOWNLOAD_URL}/v3/{path.parent.parent.name}/{path.parent.name}/{path.name}"})
    source_manifest = path.parent / "manifest.json"
    digest = None
    if source_manifest.exists():
        m = json.loads(source_manifest.read_text())
        digest = m.get("sha256") or m.get("file_sha256")
    for row in rows:
        row["source_hour_sha256"] = digest
        row["source_manifest_url"] = f"{DOWNLOAD_URL}/v3/{path.parent.parent.name}/{path.parent.name}/manifest.json"
    return rows, {"hour": path.stem, "metadata_groups": groups, "matched_lifecycle_rows": len(rows),
                  "file_bytes": path.stat().st_size, "source_url": f"{DOWNLOAD_URL}/v3/{path.parent.parent.name}/{path.parent.name}/{path.name}"}


def keys_from_partitions(root):
    result = set()
    for table in ("normalized_depth_events", "bbo_quotes", "trade_tape", "book_checkpoints"):
        for market in (Path(root) / table).glob("market_key=btc_*m"):
            mk = market.name.split("=", 1)[1]
            if mk not in ("btc_5m", "btc_15m"):
                continue
            for contract in market.glob("contract_start_ms=*"):
                result.add((mk, int(contract.name.split("=", 1)[1])))
    return result


def public_records(markets, assets, certified, main_keys):
    out_m, out_a, proofs = [], [], []
    index_a = {(str(k), int(s)): g for (k, s), g in assets.groupby(["market_key", "contract_start_ms"])}
    for r in markets.itertuples(index=False):
        key = (str(r.market_key), int(r.contract_start_ms))
        slug = f"btc-updown-{r.market_key.split('_')[1]}-{int(r.contract_start_ms)//1000}"
        proof = certified.get(slug)
        if proof is None:
            continue
        a = index_a[key]
        actual_pairs = {str(x.token_id): int(x.outcome) for x in a.itertuples()}
        if actual_pairs != proof["token_outcomes"]:
            raise ValueError(f"independent token/outcome mismatch {slug}")
        if str(r.condition_id).lower() != proof["condition_id"]:
            raise ValueError(f"independent condition mismatch {slug}")
        # Preserve the engine's identifier only if independently carried by V3.
        if str(r.market_id).lower() not in (proof["condition_id"], proof["venue_market_id"].lower()):
            raise ValueError(f"original market_id not carried by independent V3 metadata: {slug}")
        duration = 300_000 if key[0] == "btc_5m" else 900_000
        end = key[1] + duration
        if int(r.contract_end_ms) != end:
            raise ValueError(f"contract duration disagreement {slug}")
        independently_carried_market_id = (proof["condition_id"] if str(r.market_id).lower() == proof["condition_id"]
                                         else proof["venue_market_id"])
        market = {"market_local_id": int(r.market_local_id), "market_id": independently_carried_market_id, "slug": slug,
                  "asset": "btc", "tenor": key[0].split("_")[1], "market_key": key[0],
                  "contract_start_ms": key[1], "contract_end_ms": end, "condition_id": proof["condition_id"],
                  "session_id": "pflow", "end_ts_ms": end}
        target_m, target_a = out_m, out_a
        target_m.append(dict(market, core=key in main_keys))
        for x in a.itertuples(index=False):
            target_a.append({"asset_local_id": int(x.asset_local_id), "market_local_id": int(r.market_local_id),
                             "market_key": key[0], "contract_start_ms": key[1], "contract_end_ms": end,
                             "market_id": independently_carried_market_id, "token_id": str(x.token_id),
                             "is_yes": bool(proof["token_outcomes"][str(x.token_id)] == 0),
                             "outcome": int(proof["token_outcomes"][str(x.token_id)]), "session_id": "pflow", "core": key in main_keys})
        proofs.append(dict(proof, market_key=key[0], contract_start_ms=key[1], core=key in main_keys,
                           original_pair_condition_outcome_verified=True,
                           own_integer_local_ids_preserved=True))
    return pd.DataFrame(out_m), pd.DataFrame(out_a), proofs


def atomic_json(path, obj):
    path = Path(path)
    temp = path.with_suffix(path.suffix + ".publiccatalog.tmp")
    temp.write_text(json.dumps(obj, indent=2) + "\n", encoding="utf-8")
    temp.replace(path)


def apply_catalog(stage, bundle, task, marker):
    if not Path(marker).exists():
        raise RuntimeError(f"clock-fix completion marker absent; staged catalog retained, apply separately: {Path(marker).name}")
    provenance = json.loads((stage / "PROVENANCE.json").read_text())
    if provenance["missing_targets"]:
        raise RuntimeError("independent lifecycle metadata missing; no public catalog apply")
    private = task / "catalog_private_before_rebuild"
    private.mkdir(exist_ok=True)
    import shutil
    targets = []
    for sub in ("poly", "diagnostics/poly"):
        src = stage / sub / "catalog"
        if not src.exists():
            continue
        dest = bundle / "data" / sub / "catalog"
        dest.mkdir(parents=True, exist_ok=True)
        for f in src.glob("*.parquet"):
            output = dest / f.name
            if output.exists():
                save = private / sub / f.name
                save.parent.mkdir(parents=True, exist_ok=True)
                if not save.exists():
                    shutil.copy2(output, save)
            temporary = output.with_suffix(".publiccatalog.tmp")
            shutil.copy2(f, temporary)
            temporary.replace(output)
            targets.append(output)
    proof_path = bundle / "data/poly/catalog/PROVENANCE.json"
    shutil.copy2(stage / "PROVENANCE.json", proof_path)
    # Apply only after the clock fix, loading the latest manifests so its hashes
    # cannot be overwritten by a stale copy. Source/private export manifest stays.
    for manifest in (bundle / "data_manifest.json", task / "public_data_manifest.json"):
        if not manifest.exists():
            continue
        doc = json.loads(manifest.read_text())
        replacements = {}
        for p in targets:
            pf = pq.ParquetFile(p)
            relative = p.relative_to(bundle).as_posix()
            replacements[relative] = {"path": relative, "bytes": p.stat().st_size, "rows": pf.metadata.num_rows,
                         "sha256": sha(p), "columns": pf.schema_arrow.names,
                         "transformation": "independent licensed V3 lifecycle catalog; stable own local IDs; optional tick/size/neg_risk fields omitted",
                         "source_ref": "PendulumFlow V3 new_market metadata, CC BY 4.0",
                         "source_provenance": "data/poly/catalog/PROVENANCE.json", "license": LICENSE_URL}
        doc["files"] = [replacements.pop(x["path"], x) for x in doc["files"]] + list(replacements.values())
        doc["public_catalog"] = {"provenance": "data/poly/catalog/PROVENANCE.json", "sha256": sha(proof_path),
                                 "certified_contracts": provenance["certified_contracts"], "metadata_source": "licensed PendulumFlow V3 lifecycle"}
        atomic_json(manifest, doc)
    atomic_json(task / "PUBLIC_CATALOG_COMPLETE.json", {"complete": True, "core_markets": provenance["core_markets"],
                 "diagnostic_markets": provenance["diagnostic_markets"], "provenance_sha256": sha(proof_path),
                 "utc": datetime.now(timezone.utc).isoformat()})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-root", required=True)
    parser.add_argument("--mirror-v3-root", required=True)
    parser.add_argument("--bundle-root", required=True)
    parser.add_argument("--task-root", required=True)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--lookback-hours", type=int, default=72)
    parser.add_argument("--clock-marker", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--apply-only", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.workers <= 4:
        raise ValueError("bounded metadata I/O permits 1-4 workers")
    task, bundle = Path(args.task_root).resolve(), Path(args.bundle_root).resolve()
    if not bundle.is_relative_to(task):
        raise ValueError("bundle output outside explicitly named task")
    stage = task / "public_catalog_staged"
    if args.apply_only:
        apply_catalog(stage, bundle, task, args.clock_marker)
        return
    core = keys_from_partitions(bundle / "data/poly")
    diag = keys_from_partitions(bundle / "data/diagnostics/poly")
    if len(core) != 768 or len(diag) != 5:
        raise ValueError(f"expected 768 core +5 diagnostic contracts, found {len(core)}+{len(diag)}")
    keys = core | diag
    cache = Path(args.cache_root)
    m = pq.ParquetFile(cache / "catalog/markets.parquet").read().to_pandas()
    a = pq.ParquetFile(cache / "catalog/assets.parquet").read().to_pandas()
    m = m[[((str(k), int(s)) in keys) for k, s in zip(m.market_key, m.contract_start_ms)]].copy()
    a = a[[((str(k), int(s)) in keys) for k, s in zip(a.market_key, a.contract_start_ms)]].copy()
    if len(m) != len(keys) or len(a) != 2 * len(keys):
        raise ValueError("private cache selection does not have one binary pair per target")
    slugs = {f"btc-updown-{key.split('_')[1]}-{start//1000}" for key, start in keys}
    sources = Path(args.mirror_v3_root)
    wanted_hours, priority = set(), []
    for _, ms in sorted(keys, key=lambda x: x[1]):
        stamp = datetime.fromtimestamp(ms / 1000, timezone.utc).replace(minute=0, second=0, microsecond=0)
        for offset in (-24, -25, -23, 0):
            hour = stamp + timedelta(hours=offset)
            if hour not in priority:
                priority.append(hour)
        wanted_hours.update(stamp + timedelta(hours=offset) for offset in range(-args.lookback_hours, 2))
    all_hours = priority + sorted(wanted_hours - set(priority), reverse=True)
    paths = [sources / h.strftime("%Y-%m-%d/%H/%Y-%m-%dT%H.parquet") for h in all_hours]
    paths = [p for p in paths if p.exists()]
    found, audit = {}, []
    start = time.monotonic()
    it = iter(paths)
    with ProcessPoolExecutor(args.workers, initializer=init_worker, initargs=(sorted(slugs),)) as executor:
        pending = {executor.submit(scan_hour, p): p for p in [next(it, None) for _ in range(args.workers)] if p is not None}
        while pending:
            completed, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in completed:
                path = pending.pop(future)
                rows, report = future.result()
                audit.append(report)
                for row in rows:
                    old = found.get(row["slug"])
                    if old is not None and (old["condition_id"], old["token_outcomes"]) != (row["condition_id"], row["token_outcomes"]):
                        raise ValueError(f"conflicting licensed lifecycle rows for {row['slug']}")
                    found.setdefault(row["slug"], row)
                if len(audit) % 12 == 0 or len(found) == len(slugs):
                    print("CATALOG_METADATA", len(audit), "hours", len(found), "/", len(slugs), "targets", round(time.monotonic() - start, 1), "seconds", flush=True)
                if len(found) != len(slugs):
                    nxt = next(it, None)
                    if nxt is not None:
                        pending[executor.submit(scan_hour, nxt)] = nxt
            if len(found) == len(slugs):
                for future in pending:
                    future.cancel()
                break
    mm, aa, proofs = public_records(m, a, found, core)
    # The source mirror verifies downloads against the archive SHA256SUMS and
    # records successful checks. Check those records without re-reading 54
    # multi-GB raw event hours for this bounded metadata-only operation.
    ledger = sources.parent.parent / "state/ledger.tsv"
    if not ledger.exists():
        raise ValueError("source checksum-verifying mirror ledger missing")
    ledger_rows = pd.read_csv(ledger, sep="\t", names=["era", "path", "sha256", "bytes", "timestamp"],
                              dtype={"era": str, "path": str, "sha256": str})
    verified = {(str(x.path), str(x.sha256)): int(x.bytes) for x in ledger_rows.itertuples()
                if x.era == "v3"}
    source_hours = {}
    for proof in proofs:
        relative = proof["source_url"].split("/v3/", 1)[1]
        key = (relative, proof["source_hour_sha256"])
        if key not in verified:
            raise ValueError(f"source metadata digest does not match checksum-verified mirror ledger: {relative}")
        proof["source_sha256_download_verified"] = True
        source_hours[relative] = {"url": proof["source_url"], "sha256": proof["source_hour_sha256"],
                                  "bytes": verified[key], "sha256_download_verified": True,
                                  "manifest_url": proof["source_manifest_url"]}
    missing = sorted(slugs - set(found))
    stage.mkdir(parents=True, exist_ok=True)
    if len(mm):
        for sub, flag in (("poly", True), ("diagnostics/poly", False)):
            out = stage / sub / "catalog"
            out.mkdir(parents=True, exist_ok=True)
            mm[mm.core == flag].drop(columns="core").sort_values(["market_key", "contract_start_ms"]).to_parquet(out / "markets.parquet", index=False, compression="zstd")
            aa[aa.core == flag].drop(columns="core").sort_values("asset_local_id").to_parquet(out / "assets.parquet", index=False, compression="zstd")
    provenance = {"date_utc": datetime.now(timezone.utc).isoformat(), "task": task.name,
        "metadata_source": "PendulumFlow V3 new_market lifecycle rows; no Telonex metadata redistributed",
        "source_archive": SOURCE_URL, "source_schema": f"{SOURCE_URL}/v3/SCHEMA.json", "license": LICENSE_URL,
        "source_license_notice": f"{SOURCE_URL}/LICENSE.txt", "source_credit": "pendulumflow, Polymarket Orderbook Archive, V3",
        "source_checksum_verification": "archive-manifest SHA256 equals the existing checksum-verifying mirror download ledger; raw files not rehashed by this metadata-only scan",
        "source_hours": [source_hours[k] for k in sorted(source_hours)],
        "reconstruction_code_sha256": sha(Path(__file__)),
        "transforms": ["target BTC5/BTC15 selection", "contract descriptors derived from independently witnessed slug",
                       "units and binary outcome mapping", "stable integer local IDs generated by our cache builder",
                       "optional tick/minimum-size/neg_risk fields omitted", "Parquet ZSTD re-encoding"],
        "core_markets": int(mm.core.sum()) if len(mm) else 0, "diagnostic_markets": int((~mm.core).sum()) if len(mm) else 0,
        "certified_contracts": len(proofs), "missing_targets": missing, "missing_verdict": "unknown, not silently accepted",
        "workers": args.workers, "thread_environment": "OMP/OPENBLAS/MKL/NUMBA=1", "scope": "bounded rare metadata I/O; no full-event per-row scan",
        "hours_examined": audit, "contracts": proofs,
        "delivered_catalogs": [{"path": p.relative_to(stage).as_posix(), "sha256": sha(p),
                                 "bytes": p.stat().st_size, "rows": pq.ParquetFile(p).metadata.num_rows}
                                for p in sorted(stage.glob("**/*.parquet"))]}
    atomic_json(stage / "PROVENANCE.json", provenance)
    print("CATALOG_STAGED", len(proofs), "missing", len(missing), flush=True)
    if missing:
        print("MISSING", json.dumps(missing), flush=True)
        raise SystemExit(2)
    if args.apply:
        apply_catalog(stage, bundle, task, args.clock_marker)
        print("CATALOG_PUBLIC_APPLIED", flush=True)


if __name__ == "__main__":
    main()
