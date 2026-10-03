"""Bounded exporter of anonymized real captured-order regression fixtures.

Uses the original decision-replay functions; original experiment outputs are read
only. Output must be in the explicitly named task root. No wallet/source
account identifiers, original order IDs, or original run/ref fields are exported.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys

import numpy as np
import pandas as pd


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    parser.add_argument("--task-root", required=True)
    parser.add_argument("--original-code", required=True)
    parser.add_argument("--original-output-root", required=True)
    parser.add_argument("--account", required=True, help="private source alias; never serialized")
    args = parser.parse_args()
    out = Path(args.out)
    allowed = Path(args.task_root).resolve()
    if not out.resolve().is_relative_to(allowed):
        raise ValueError("fixture output outside named browser-bundle task")
    out.mkdir(parents=True, exist_ok=True)
    original = Path(args.original_code)
    os.environ["RP_OUT"] = str(out / "_import_only")
    spec = importlib.util.spec_from_file_location("frozen_q99_reference", original)
    ref = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = ref
    spec.loader.exec_module(ref)
    source_sha = hashlib.sha256(original.read_bytes()).hexdigest()
    exp = Path(args.original_output_root)
    archive = pd.read_parquet(exp / "replay/orders_scored.parquet")
    raw_steps = pd.read_parquet(exp / f"inputs_{args.account}/own_steps2_btc_5m.parquet")
    requested = [("2026-10-01T14:00:00Z", "DOWN", .92, "large_complement_sweep"),
                 ("2026-10-01T14:55:00Z", "UP", .97, "bounded_trade_through"),
                 ("2026-10-01T16:45:00Z", "UP", .94, "own_footprint_losing_fill")]
    items = []
    for index, (iso, side, price, why) in enumerate(requested, 1):
        slot = int(pd.Timestamp(iso).timestamp())
        group = archive[(archive.acct == args.account) & (archive.prof == "btc_5m") & (archive.slot == slot)
                        & (archive.side == side) & np.isclose(archive.px, price)]
        if not len(group):
            raise ValueError(f"diagnostic order missing at {iso}/{side}/{price}")
        group = group.sort_values("size", ascending=False).iloc[:1].copy()
        # Original identity is used exclusively inside the source runner and never serialized.
        row = group.iloc[0]
        depth, trades = ref.load_market("btc", "btc_5m", slot)
        steps = raw_steps[(raw_steps.contract_start_ms == slot * 1000) & (raw_steps.outcome == side)
                          & np.isclose(raw_steps.limit, price)].sort_values("venue_ms", kind="stable")
        ref.OWN[(args.account, "btc_5m")] = {(slot * 1000, side, round(price, 2)): (steps.venue_ms.to_numpy(np.float64), steps.delta.to_numpy(np.float64))}
        golden = ref.run_market(("btc_5m", slot, group))[0]
        identifier = f"fixture_order_{index:03d}"
        dest = out / identifier
        dest.mkdir(exist_ok=True)
        depth.to_parquet(dest / "depth.parquet", index=False, compression="zstd")
        trades.to_parquet(dest / "trades.parquet", index=False, compression="zstd")
        steps[["venue_ms", "delta"]].to_parquet(dest / "own_steps.parquet", index=False, compression="zstd")
        order = {"outcome": int(row.outcome), "price_micros": int(row.px_mic), "quantity": float(row["size"]),
                 "place_venue_ms": float(row.place_venue_ms), "cutoff_venue_ms": float(row.t_out),
                 "cancel_venue_ms": float(row.cancel_venue_ms) if pd.notna(row.cancel_venue_ms) else None,
                 "order_id": identifier}
        expected = {mode: {"simulated_shares": float(golden[f"{mode}_fs"]),
                           "first_fill_print_venue_ms": float(golden[f"{mode}_first"]) if pd.notna(golden[f"{mode}_first"]) else None,
                           "queue_ahead_shares": float(golden[f"{mode}_qa"]) if pd.notna(golden[f"{mode}_qa"]) else None}
                    for mode in ref.MODES}
        item = {"fixture": identifier, "market_key": "btc_5m", "contract_start_ms": slot * 1000, "reason": why,
                "order": order, "live_filled_shares": float(row.fs), "known_won": bool(row.won),
                "source_generated_expected": expected,
                "depth_rows": len(depth), "trade_rows": len(trades), "own_step_rows": len(steps)}
        (dest / "expected.json").write_text(json.dumps(item, indent=2) + "\n")
        print(identifier, len(depth), len(trades), row.fs, expected["eng"], expected["eng_ftfix"], flush=True)
        items.append(item)
    if hashlib.sha256(original.read_bytes()).hexdigest() != source_sha:
        raise RuntimeError("original source changed while exporting fixtures")
    manifest = {"source_replay_sha256": source_sha, "fixture_count": len(items),
                "original_archive": "Q99 decision replay 2026-10-03, captured accepted orders; anonymized subset",
                "data_version": "BTC pendulumflow pflow_v1, frozen 2026-10-03",
                "selection": "three known mechanism diagnostics; not held-out strategy validation",
                "files": [{"file": str(p.relative_to(out)), "bytes": p.stat().st_size,
                           "sha256": hashlib.sha256(p.read_bytes()).hexdigest()} for p in sorted(out.glob("fixture_order_*/*"))]}
    (out / "MANIFEST.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    main()
