"""Real captured-order parity against independently generated original-source outputs.

These are three selected Q99 mechanism regressions, not strategy qualification.
The original account/order identity was discarded during fixture extraction.
"""
import ast
import hashlib
import json
import math
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from bonebundle.maker import MODES, MakerOrder, MissingFootprintError, replay_order
from bonebundle.maker import _reference

ROOT = Path(__file__).resolve().parents[2]
VENDOR = ROOT / "vendor/maker"
FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="module", params=sorted(FIXTURES.glob("fixture_order_*")), ids=lambda p: p.name)
def real_order(request):
    path = request.param
    meta = json.loads((path / "expected.json").read_text(encoding="utf-8"))
    return (pd.read_parquet(path / "depth.parquet"), pd.read_parquet(path / "trades.parquet"),
            pd.read_parquet(path / "own_steps.parquet"), meta)


@pytest.mark.parametrize("mode", MODES)
def test_portable_modes_match_original_source_goldens(real_order, mode):
    depth, trades, own, meta = real_order
    result = replay_order(depth, trades, MakerOrder(**meta["order"]), mode=mode, own_steps=own, require_footprint=False)
    expected = meta["source_generated_expected"][mode]
    for field, value in expected.items():
        actual = getattr(result, field)
        if value is None:
            assert actual is None
        else:
            assert actual == pytest.approx(value, rel=1e-12, abs=1e-7), (meta["fixture"], mode, field)


def _source_functions(path, names, globals_dict):
    """Execute only named original functions, never module imports/output mkdir."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    funcs = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    assert len(funcs) == len(names)
    exec(compile(ast.Module(body=funcs, type_ignores=[]), str(path), "exec"), globals_dict)
    return globals_dict


def test_original_production_fp_simulate_agrees_on_actual_inputs(real_order):
    depth, trades, own, meta = real_order
    order = MakerOrder(**meta["order"])
    bids = _reference.token_bids(depth, order.outcome, order.price_micros)
    lvl = bids[bids.price_micros == order.price_micros]
    side = "UP" if order.outcome == 0 else "DOWN"
    dt = pd.DataFrame(dict(side=side, ts_ns=lvl.venue_ts_ms.to_numpy(np.int64) * 1_000_000,
                           seq=lvl.seq.to_numpy(np.int64), state_id=np.zeros(len(lvl), dtype=np.int64),
                           bid_level_size=lvl["size"].to_numpy(np.float64)))
    mp = np.where(trades.outcome == order.outcome, trades.price_micros, 1_000_000 - trades.price_micros)
    mask = ((trades.outcome == order.outcome) & (trades.trade_side == 1)) | ((trades.outcome != order.outcome) & (trades.trade_side == 0))
    tr = trades[mask].copy()
    tr["mapped_price"] = mp[mask.to_numpy()]
    tr = tr.sort_values(["venue_ts_ms", "seq"], kind="stable")
    tt = pd.DataFrame(dict(clock_ns=tr.venue_ts_ms.to_numpy(np.int64) * 1_000_000,
                           price=tr.mapped_price.to_numpy(np.float64) / 1_000_000,
                           size=tr.size_micros.to_numpy(np.float64) / 1_000_000, aggressor_side="SELL"))
    cfg = SimpleNamespace(queue_size_multiplier=1.0, trigger_bid=order.price_micros / 1_000_000,
                          trigger_tolerance=1e-6, polymarket_feed_delay_ms=13.0, polymarket_timestamp_clock="venue")
    env = {"np": np, "pd": pd, "math": math, "_FP_RULE": "sizematch", "_FP_THROUGH": True,
           "_FP_EFF_SHIFT_MS": 60.0, "_FP_OWN_VENUE": True, "_FP_CUT_SHIFTED": True,
           "_fp_own_steps": lambda *_: (own.venue_ms.to_numpy(np.int64), own.delta.to_numpy(np.float64))}
    source = _source_functions(VENDOR / "queue99_backtester.py", {"_fp_cancel", "_fp_simulate"}, env)
    filled, fills = source["_fp_simulate"](side=side, open_ns=int(order.place_venue_ms) * 1_000_000,
                 cutoff_ns=int(order.cutoff_venue_ms) * 1_000_000, requested=order.quantity,
                 causal_trades=tt, cfg=cfg, pre_open_depth=dt, contract_start_ms=meta["contract_start_ms"])
    result = replay_order(depth, trades, order, mode="eng", own_steps=own)
    assert filled == pytest.approx(result.simulated_shares, abs=1e-7)
    if fills:
        assert fills[0][0] / 1_000_000 == result.first_fill_print_venue_ms
    else:
        assert result.first_fill_print_venue_ms is None


def test_extracted_reference_functions_match_original_ast():
    source = ast.parse((VENDOR / "replay.py").read_text(encoding="utf-8"))
    port = ast.parse(Path(_reference.__file__).read_text(encoding="utf-8"))
    originals = {n.name: ast.dump(n) for n in source.body if isinstance(n, ast.FunctionDef)}
    extracted = {n.name: ast.dump(n) for n in port.body if isinstance(n, ast.FunctionDef)}
    assert extracted
    for name, code in extracted.items():
        assert code == originals[name], name


def test_canonical_fillpar3_queue_matches_new_fp3_on_actual_inputs(real_order):
    depth, trades, own, meta = real_order
    order = MakerOrder(**meta["order"])
    # Independently execute the older canonical queue used for BoneOhio FINAL.
    env = {"np": np, "pd": pd, "MIC": 1_000_000}
    original = _source_functions(VENDOR / "fillrep.py", {"token_bids", "above_asof", "_cancel", "replay"}, env)
    bids = original["token_bids"](depth, order.outcome, order.price_micros)
    level = bids[bids.price_micros == order.price_micros].copy()
    level["size"] = level.size_micros / 1_000_000
    before = level["size"].shift(1).fillna(0.0)
    candidates = level[(level["size"] - before >= order.quantity - 1e-6)
                       & level.venue_ts_ms.between(order.place_venue_ms - 3000, order.place_venue_ms + 3000)]
    assert len(candidates), "fixture must have a real footprint"
    k = int((candidates.venue_ts_ms - order.place_venue_ms).abs().argmin())
    foot = (float(candidates.venue_ts_ms.iloc[k]), int(candidates.seq.iloc[k]))
    mapped = np.where(trades.outcome == order.outcome, trades.price_micros, 1_000_000 - trades.price_micros)
    sell = ((trades.outcome == order.outcome) & (trades.trade_side == 1)) | ((trades.outcome != order.outcome) & (trades.trade_side == 0))
    mask = sell.to_numpy() & (mapped <= order.price_micros)
    p = trades[mask].copy()
    p["mapped"] = mapped[mask]
    p = p.sort_values(["venue_ts_ms", "seq"], kind="stable")
    tv = p.venue_ts_ms.to_numpy(np.float64)
    qty = p.size_micros.to_numpy(np.float64) / 1_000_000
    above = original["above_asof"](bids, order.price_micros, tv - 60.0 - 0.5)
    prints = [(tv[i] - 60.0, max(0, qty[i] - above[i]), bool(p.mapped.iloc[i] < order.price_micros), tv[i]) for i in range(len(p))]
    cutoff = order.cancel_venue_ms - 1 if order.cancel_venue_ms is not None else order.cutoff_venue_ms
    filled, first, fills = original["replay"](level, prints, order.quantity, foot, cutoff, "sizematch", 0)
    result = replay_order(depth, trades, order, mode="fp3")
    assert filled == pytest.approx(result.simulated_shares, abs=1e-7)
    assert first == result.first_fill_print_venue_ms


def test_distributed_reference_checksums():
    manifest = json.loads((VENDOR / "SOURCE_MANIFEST.json").read_text(encoding="utf-8"))
    for record in manifest["files"]:
        path = VENDOR / record["file"]
        assert path.stat().st_size == record["distributed_bytes"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == record["distributed_sha256"]
    assert hashlib.sha256(Path(_reference.__file__).read_bytes()).hexdigest() == manifest["extracted_sha256"]


def test_fixture_checksums():
    manifest = json.loads((FIXTURES / "MANIFEST.json").read_text(encoding="utf-8"))
    assert manifest["fixture_count"] == 3
    for record in manifest["files"]:
        path = FIXTURES / record["file"]
        assert path.stat().st_size == record["bytes"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == record["sha256"]


def test_bounded_trade_through_fixture_preserves_difference():
    path = FIXTURES / "fixture_order_002"
    depth, trades, own = (pd.read_parquet(path / f"{name}.parquet") for name in ("depth", "trades", "own_steps"))
    meta = json.loads((path / "expected.json").read_text(encoding="utf-8"))
    order = MakerOrder(**meta["order"])
    legacy = replay_order(depth, trades, order, mode="eng", own_steps=own)
    bounded = replay_order(depth, trades, order, mode="eng_ftfix", own_steps=own)
    assert legacy.simulated_shares == pytest.approx(748)
    assert bounded.simulated_shares == pytest.approx(55)
    # Actual fill was 25; even the improved replay is not exact on every order.
    assert meta["live_filled_shares"] == 25


def test_historical_footprint_required_by_default(real_order):
    depth, trades, own, meta = real_order
    values = dict(meta["order"], quantity=1e10)
    with pytest.raises(MissingFootprintError):
        replay_order(depth, trades, MakerOrder(**values), mode="fp3")
