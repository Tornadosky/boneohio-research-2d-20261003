"""Portable fixed-order maker replay over normalized Polymarket cache data.

This is an execution component, not a BoneOhio entry/cancel/selection strategy.
"""
from dataclasses import asdict, dataclass
import math
import os
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from ._reference import above_asof, sim_dm, sim_engine, sim_fp3, token_bids

MIC = 1_000_000
ENG_MODES = ("eng", "eng_book", "eng_sh60", "eng_noown", "eng_thr", "eng_own5", "eng_fix", "eng_ownft", "eng_ftfix")
MODES = ENG_MODES + ("fp3", "fp3_df1", "fp3_thr", "eng_dm", "fp3_dm")
DEPTH_COLUMNS = ("venue_ts_ms", "seq", "outcome", "event_type", "side_code", "price_micros", "size_micros", "bid_prices", "bid_sizes")
TRADE_COLUMNS = ("venue_ts_ms", "seq", "outcome", "price_micros", "size_micros", "trade_side")


@dataclass(frozen=True)
class MakerOrder:
    """One BUY order; binary outcome 0=UP/YES, 1=DOWN/NO.

    place_venue_ms and cutoff_venue_ms must already be on a declared venue clock.
    A maker replay quantity is the resting remainder after any initial taker sweep.
    footprint is an exact historical (venue_ms, seq) insertion, when known.
    """
    outcome: int
    price_micros: int
    quantity: float
    place_venue_ms: float
    cutoff_venue_ms: float
    cancel_venue_ms: float | None = None
    footprint: tuple[float, int] | None = None
    order_id: str = ""

    def __post_init__(self):
        if self.outcome not in (0, 1):
            raise ValueError("outcome must be 0 or 1")
        if isinstance(self.price_micros, bool) or int(self.price_micros) != self.price_micros or not 0 < self.price_micros < MIC:
            raise ValueError("price_micros must be an integer strictly between 0 and 1000000")
        if not math.isfinite(self.quantity) or self.quantity <= 0:
            raise ValueError("quantity must be finite and positive")
        if not all(math.isfinite(x) for x in (self.place_venue_ms, self.cutoff_venue_ms)) or self.cutoff_venue_ms <= self.place_venue_ms:
            raise ValueError("finite cutoff must be later than placement")
        if self.cancel_venue_ms is not None and not math.isfinite(self.cancel_venue_ms):
            raise ValueError("cancel_venue_ms must be finite or None")


@dataclass(frozen=True)
class MakerResult:
    order_id: str
    mode: str
    simulated_shares: float
    first_fill_print_venue_ms: float | None
    queue_ahead_shares: float | None
    level_rows: int
    print_shift_ms: float
    own_removal: bool
    footprint_found: bool | None
    footprint_delta_ms: float | None

    def to_dict(self):
        return asdict(self)


class MissingFootprintError(ValueError):
    pass


def _optional_float(value):
    return float(value) if np.isfinite(value) else None


def load_market(data_root: str | Path | None, market_key: str, contract_start_ms: int, *, session_id: str | None = None):
    """Read full market bid-depth/prints from a relocated standard cache layout.

    data_root is the directory containing normalized_depth_events and trade_tape;
    default is DATA_ROOT or ./data/poly. Multiple sessions require an explicit
    session_id, because witnesses must not be silently mixed/double-counted.
    """
    root = Path(data_root or os.environ.get("DATA_ROOT", "data/poly"))
    if market_key not in ("btc_5m", "btc_15m"):
        raise ValueError("this handoff is scoped to btc_5m and btc_15m")
    if int(contract_start_ms) != contract_start_ms or contract_start_ms < 0:
        raise ValueError("contract_start_ms must be a nonnegative integer")

    def files(table):
        base = root / table / f"market_key={market_key}" / f"contract_start_ms={int(contract_start_ms)}"
        if session_id is not None:
            if "/" in session_id or "\\" in session_id or session_id in (".", ".."):
                raise ValueError("invalid session_id")
            return sorted((base / f"session_id={session_id}").glob("*.parquet"))
        paths = sorted(base.glob("session_id=*/*.parquet"))
        if len({f.parent.name for f in paths}) > 1:
            raise ValueError(f"multiple {table} sessions; choose one explicitly: {base}")
        return paths

    df, tf = files("normalized_depth_events"), files("trade_tape")
    if not df:
        raise FileNotFoundError(f"missing normalized depth for {market_key}/{contract_start_ms} under {root}")
    depth = pd.concat([pq.ParquetFile(f).read(columns=list(DEPTH_COLUMNS)).to_pandas() for f in df], ignore_index=True)
    trades = pd.concat([pq.ParquetFile(f).read(columns=list(TRADE_COLUMNS)).to_pandas() for f in tf], ignore_index=True) if tf else pd.DataFrame(columns=TRADE_COLUMNS)
    return depth, trades


def _validate_frames(depth, trades):
    for name, frame, columns in (("depth", depth, DEPTH_COLUMNS), ("trades", trades, TRADE_COLUMNS)):
        missing = set(columns) - set(frame.columns)
        if missing:
            raise ValueError(f"{name} missing columns: {sorted(missing)}")
        if len(frame) and not frame.outcome.isin((0, 1)).all():
            raise ValueError(f"{name} has a nonbinary outcome")
        if len(frame) and (frame["venue_ts_ms"].isna().any() or frame["seq"].isna().any()):
            raise ValueError(f"{name} has missing venue timestamps/sequence")


def _own_views(own_steps, lt, ls):
    """Return original, blanket-5ms diagnostic, and footprint-aligned cumulative own size."""
    if own_steps is None or len(own_steps) == 0:
        return None, None, None
    if not {"venue_ms", "delta"}.issubset(own_steps.columns):
        raise ValueError("own_steps requires venue_ms and delta for this exact account/token/level")
    frame = own_steps.sort_values("venue_ms", kind="stable")
    ot, od = frame.venue_ms.to_numpy(np.float64), frame.delta.to_numpy(np.float64)
    if not np.isfinite(ot).all() or not np.isfinite(od).all():
        raise ValueError("own_steps must be finite")
    own = (ot, np.cumsum(od))
    t5 = np.where(od > 0, ot - 5.0, ot)
    ix = np.argsort(t5, kind="stable")
    lead5 = (t5[ix], np.cumsum(od[ix]))
    inc = ls - np.r_[0.0, ls[:-1]] if len(ls) else ls
    snapped = ot.copy()
    for j, dj in enumerate(od):
        if dj <= 0:
            continue
        m = np.flatnonzero((inc >= dj - 1e-6) & (lt >= snapped[j] - 10) & (lt <= snapped[j] + 2))
        if len(m):
            snapped[j] = lt[m[np.argmin(np.abs(lt[m] - ot[j]))]]
    ix = np.argsort(snapped, kind="stable")
    footprint = (snapped[ix], np.cumsum(od[ix]))
    return own, lead5, footprint


def replay_order(depth: pd.DataFrame, trades: pd.DataFrame, order: MakerOrder, *, mode: str = "eng_ftfix",
                 own_steps: pd.DataFrame | None = None, print_shift_ms: float | None = None,
                 require_footprint: bool = True) -> MakerResult:
    """Replay a fixed maker BUY order without inventing an entry/cancel strategy.

    eng is the validated production reference. eng_ftfix implements the latest
    decision-replay recommendation (bounded sweeps and snapped own steps).
    fp3 is forensic: inferred footprints use future observations. Missing fp3
    footprints fail by default; require_footprint=False enables the original
    synthetic-entry diagnostic. No other wallet's size may be removed as own.
    """
    if mode not in MODES:
        raise ValueError(f"unknown maker mode {mode!r}; supported: {MODES}")
    _validate_frames(depth, trades)
    oc, L, R0 = order.outcome, int(order.price_micros), float(order.quantity)
    bids = token_bids(depth, oc, L)
    lvl = bids[bids.price_micros == L][["venue_ts_ms", "seq", "size"]].reset_index(drop=True)
    lvl["venue_ts_ms"] = lvl.venue_ts_ms.astype(np.float64)
    lt, ls, lq = lvl.venue_ts_ms.to_numpy(), lvl["size"].to_numpy(), lvl.seq.to_numpy()
    t_open, t_cut = float(order.place_venue_ms), float(order.cutoff_venue_ms)
    if len(trades):
        t = trades.copy()
        t["size"] = t.size_micros.astype(np.float64) / MIC
        mp = np.where(t.outcome == oc, t.price_micros, MIC - t.price_micros)
        sell_like = ((t.outcome == oc) & (t.trade_side == 1)) | ((t.outcome != oc) & (t.trade_side == 0))
        mask = sell_like.to_numpy() & (mp <= L)
        p = t[mask].copy()
        p["mp"] = mp[mask]
        p = p.sort_values(["venue_ts_ms", "seq"], kind="stable")
    else:
        p = pd.DataFrame(columns=["venue_ts_ms", "seq", "size", "mp"])
    tv, sz, mpv = p.venue_ts_ms.to_numpy(np.float64), p["size"].to_numpy(np.float64), p.mp.to_numpy()
    default_shift = 60.0 if mode == "eng_sh60" or mode in ("fp3", "fp3_df1", "fp3_thr") else 47.0
    shift = default_shift if print_shift_ms is None else float(print_shift_ms)
    if not math.isfinite(shift) or shift < 0:
        raise ValueError("print_shift_ms must be finite and nonnegative")
    if mode in ("eng_dm", "fp3_dm") and shift != 47.0:
        raise ValueError("the rejected depth-matched diagnostic has a fixed 47ms fallback; override is unsupported")
    own, own5, ownft = _own_views(own_steps, lt, ls)
    footprint_found = footprint_dt = None
    own_removal = False

    if mode in ENG_MODES:
        book = mode in ("eng_book", "eng_thr", "eng_fix", "eng_ftfix")
        bounded = mode in ("eng_thr", "eng_fix", "eng_ftfix")
        selected_own = None if mode == "eng_noown" else own5 if mode in ("eng_own5", "eng_fix") else ownft if mode in ("eng_ownft", "eng_ftfix") else own
        own_removal = selected_own is not None
        amt = np.maximum(0.0, sz - above_asof(bids, L, tv - shift - 0.5)) if book and len(tv) else sz
        prints = [(tv[i] - shift, float(amt[i]), bool(mpv[i] < L and not bounded), tv[i])
                  for i in range(len(tv)) if t_open < tv[i] and tv[i] - shift < t_cut]
        filled, first, ahead = sim_engine(lt, ls, lq, selected_own, prints, t_open, t_cut, R0)
    else:
        lv3 = lvl
        foot = order.footprint
        if foot is None:
            prev = lvl["size"].shift(1).fillna(0.0)
            inc = lvl[(lvl["size"] - prev >= R0 - 1e-6) & lvl.venue_ts_ms.between(t_open - 3000, t_open + 3000)]
            if len(inc):
                k = int((inc.venue_ts_ms - t_open).abs().argmin())
                foot = (float(inc.venue_ts_ms.iloc[k]), int(inc.seq.iloc[k]))
        if foot is not None:
            foot = (float(foot[0]), int(foot[1]))
            if not ((lvl.venue_ts_ms == foot[0]) & (lvl.seq == foot[1])).any():
                raise MissingFootprintError("supplied historical footprint does not exist in this token/level")
            footprint_found, footprint_dt = True, float(foot[0] - t_open)
        elif mode != "eng_dm":
            if require_footprint:
                raise MissingFootprintError("no >=quantity footprint within placement +/-3000 ms; do not fabricate BoneOhio parity")
            footprint_found = False
            foot = (t_open, -1)
            lv3 = pd.concat([lvl, pd.DataFrame(dict(venue_ts_ms=[t_open], seq=[-1], size=[np.nan]))], ignore_index=True).sort_values(["venue_ts_ms", "seq"])
            lv3["size"] = lv3["size"].ffill().fillna(0.0)
            m = (lv3.venue_ts_ms == t_open) & (lv3.seq == -1)
            lv3.loc[m, "size"] = lv3.loc[m, "size"] + R0
        t_exit = float(order.cancel_venue_ms) - 1 if order.cancel_venue_ms is not None else t_cut
        if mode in ("eng_dm", "fp3_dm"):
            # Rejected 2026-10-03 proposal retained for explicit diagnostics only.
            prints = [(tv[i], float(sz[i]), bool(mpv[i] == L)) for i in range(len(tv))]
            if mode == "eng_dm":
                if ownft is not None and len(lt):
                    k = np.searchsorted(ownft[0], lt, side="right") - 1
                    adj = np.maximum(0.0, ls - np.where(k >= 0, np.maximum(0.0, ownft[1][np.clip(k, 0, None)]), 0.0))
                else:
                    adj = ls
                own_removal = ownft is not None
                filled, first, ahead = sim_dm(lt, adj, lq, prints, t_open, t_cut, R0)
            else:
                filled, first, ahead = sim_dm(lv3.venue_ts_ms.to_numpy(np.float64), lv3["size"].to_numpy(np.float64), lv3.seq.to_numpy(), prints, t_open, t_exit, R0, foot_in=foot)
        else:
            amt = np.maximum(0.0, sz - above_asof(bids, L, tv - shift - 0.5)) if len(tv) else np.zeros(0)
            prints = [(tv[i] - shift, float(amt[i]), bool(mpv[i] < L and mode != "fp3_thr"), tv[i]) for i in range(len(tv))]
            filled, first, ahead = sim_fp3(lv3, prints, R0, foot, t_exit, int(mode == "fp3_df1"))
    return MakerResult(order.order_id, mode, float(filled), _optional_float(first), _optional_float(ahead), len(lvl), shift,
                       own_removal, footprint_found, footprint_dt)


def replay_orders(depth, trades, orders: Iterable[MakerOrder], *, modes=("eng", "eng_ftfix", "fp3"), own_steps_by_level=None,
                  require_footprint=True):
    """Return one DataFrame row per order/mode; unknown lifecycle is caller-owned."""
    rows = []
    for order in orders:
        own = (own_steps_by_level or {}).get((order.outcome, order.price_micros))
        for mode in modes:
            rows.append(replay_order(depth, trades, order, mode=mode, own_steps=own, require_footprint=require_footprint).to_dict())
    return pd.DataFrame(rows)
