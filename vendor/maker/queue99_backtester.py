#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import sys
import tempfile
import time
import traceback
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, fields, replace
from multiprocessing import get_context
from pathlib import Path
from typing import Any, Iterable, Sequence, Mapping

# Prevent 128 worker processes from each creating native BLAS/Arrow pools.
for _thread_env_name in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS", "NUMEXPR_NUM_THREADS", "ARROW_NUM_THREADS"):
    os.environ.setdefault(_thread_env_name, "1")

import numpy as np
import pandas as pd

import queue99_common as q
from queue99_frame_cache import frame_cache, frame_token

_WORKER_CFG: dict[str, Any] | None = None
_WORKER_TOKEN_MAP: dict[str, str] | None = None
_WORKER_RESOLUTIONS: dict[int, str] | None = None
_WORKER_UNDERLYING: q.UnderlyingMidSeries | None = None

# Keep one venue-time Binance index reusable across ordinary feed-delay and
# maximum-age sensitivity runs.  One hour is far larger than a realistic
# Binance-to-VPS delay while adding little data relative to a multi-day run.
BTC_INDEX_PRESTART_BUFFER_MS = 3_600_000.0


class InternalContractError(RuntimeError):
    """Programming/output-contract error that must fail the run immediately.

    These failures cannot be repaired by trying a sibling cache session.  V23
    accidentally allowed malformed rows to survive until apply_balance, wasting
    the full market run before crashing.  V25 marks them explicitly so preflight
    and the first affected worker terminate the job early.
    """


_INTERNAL_PROGRAMMING_EXCEPTIONS = (InternalContractError, KeyError, AssertionError, TypeError, NameError, AttributeError, IndexError)


@dataclass
class Config:
    data_dir: str
    outdir: str
    start_ms: int
    end_ms: int
    market_key: str = "btc_5m"
    yes_outcome: str = "UP"
    trigger_bid: float = 0.99
    trigger_tolerance: float = 1e-6
    order_trigger_policy: str = "first_market"
    entry_retry_policy: str = "never"
    # BTC2: opt-in event-driven portfolio arms; historical strategies default off.
    portfolio_btc5_event_mode: bool = False
    # Opt-in generic suite; old strategy validators keep their historical contract.
    universal_market_mode: bool = False
    # Dedicated one-shot same-outcome retry after an early-momentum order is
    # rejected at actual placement because POST_ONLY would cross. A negative
    # delay disables the feature completely.
    post_only_cross_retry_delay_ms: float = -1.0
    # V43.18.20: causal, asset-normalized entry research. No settlement data enters these gates.
    entry_filter_audit_enabled: bool = False
    entry_require_directional_alignment: bool = False
    entry_distance_min_bps: float = -1.0
    entry_distance_min_usd: float = -1.0
    entry_momentum_min_usd: float = -1.0
    entry_momentum_min_bps: float = -1.0
    entry_risk_z_min: float = -1.0
    entry_filter_momentum_lookback_sec: float = 3.0
    entry_filter_vol_lookback_sec: int = 60
    entry_filter_vol_sample_ms: int = 1000
    entry_filter_vol_floor_bps_sqrt_sec: float = 0.1
    entry_market_age_start_sec: float = 0.0
    entry_market_age_end_sec: float = 300.0
    share_sizing_mode: str = "btc_move"
    btc_price_path: str = ""
    btc_symbol: str = "AUTO"
    btc_mid_max_age_ms: float = 2000.0
    binance_venue_to_vps_delay_ms: float = 0.0
    polymarket_timestamp_clock: str = "local"
    polymarket_feed_delay_ms: float = 13.0
    bid99_binance_momentum_strict_gt: bool = False
    btc_load_workers: int = 128
    btc_prune_by_time_stats: bool = True
    btc_load_log_every_files: int = 1000
    btc_index_cache_mode: str = "auto"
    btc_index_cache_dir: str = ""
    btc_index_cache_source_tag: str = ""
    btc_worker_index_mode: str = "local_copy"
    btc_worker_mmap_root: str = ""
    btc_move_divisor_usd: float = 100.0
    btc_shares_multiplier: float = 30.0
    min_shares_to_open: int = 5
    max_shares_to_open: int = 1000
    fixed_shares_to_open: float = 30.0
    min_btc_move_usd: float = 0.0
    # V43.18.29-r8 optional final-size multiplier. The base strategy first
    # chooses its ordinary quantity. A child variant then multiplies that final
    # quantity by a causal trailing-candle volatility factor. Signal and FIFO
    # shadows remain unchanged so the child can reuse them.
    final_sizing_volatility_enabled: bool = False
    final_sizing_candle_count: int = 5
    final_sizing_candle_sec: int = 60
    final_sizing_reference_pct: float = 0.05
    final_sizing_factor_min: float = 0.1
    final_sizing_factor_max: float = 1.0
    # Optional direction-aware trigger-time momentum gate. A negative minimum
    # disables the feature completely. When enabled, the net delayed-observable
    # Binance midpoint move over the lookback must support the triggered side.
    bid99_binance_momentum_min_usd: float = -1.0
    # Optional early-trigger-only threshold. None inherits the base threshold.
    # This lets the 0.98-style path be looser without weakening exact-0.99
    # confirmation or the momentum ladder.
    bid99_binance_momentum_early_min_usd: float | None = None
    bid99_binance_momentum_lookback_sec: float = 1.0
    # When false, exact-TRIGGER_BID candidates do not require the momentum
    # gate. The earlier momentum trigger (for example bid >= 0.98) still does.
    bid99_binance_momentum_apply_to_99: bool = True
    bid99_binance_momentum_early_trigger_bid: float = -1.0
    momentum_early_cross_match_delay_ms: float = 50.0

    # Optional additive momentum-ladder mode.  In V29 the legacy 0.99 entry
    # state machine can remain active unchanged while a lower-price ladder adds
    # passive orders below TRIGGER_BID.  This preserves the queue timing/fills
    # that the pre-ladder strategy already had at 0.99 instead of redirecting
    # those early signals into a lower-priced replacement state machine.
    momentum_ladder_enabled: bool = False
    # Keep the exact pre-ladder TRIGGER_BID engine active as an independent top
    # rung.  With the default true, ladder orders are restricted to prices
    # strictly below TRIGGER_BID and the legacy 0.99 orders/fills are merged
    # unchanged into the market result.
    momentum_ladder_preserve_legacy_99: bool = True
    # Make-before-break repricing: do not request cancellation of the current
    # lower rung merely because a higher-rung signal appeared.  First obtain a
    # successful passive placement for the replacement; only then start the
    # configured reprice cancellation delay on the old order.
    momentum_ladder_make_before_break: bool = True
    # Records how ladder mode was selected. The shell wrapper sets this to
    # explicit_flag, auto_from_ladder_config, or default_disabled so the
    # effective config proves which state machine actually ran.
    momentum_ladder_activation_source: str = "direct_config"
    momentum_ladder_start_bid: float = 0.95
    momentum_ladder_max_bid: float = 0.99
    momentum_ladder_tick: float = 0.01
    momentum_ladder_post_only_retry_ms: float = 100.0
    # V30 risk controls. A negative lower-ladder share override inherits the
    # normal strategy sizing. The placement-attempt cap prevents repeatedly
    # chasing a lower rung after many post-only rejections.
    momentum_ladder_shares_to_open: float = -1.0
    momentum_ladder_max_placement_attempts: int = 64
    momentum_ladder_reprice_cancel_delay_ms: float = -1.0
    momentum_ladder_stop_drop_ticks: int = 1
    momentum_ladder_continue_after_full_fill: bool = False
    momentum_ladder_max_orders_per_market: int = 8
    # V42 experiment controls. A "deal" is one fresh allocation cycle. Repricing
    # an unfilled remainder to a higher rung stays inside the same deal. A new
    # deal starts only after the previous allocation fully fills and the ladder
    # is allowed to continue. -1 means unlimited (subject to max orders).
    momentum_ladder_max_deals_per_market: int = -1
    # When false, a live ladder order is sticky: higher momentum signals do not
    # cancel/reprice it. A later fresh deal may still start after a full fill.
    momentum_ladder_reprice_enabled: bool = True
    momentum_ladder_write_event_audit: bool = False

    # V31 native serialized fixed-arm mode.  Each configured arm is evaluated
    # with the original single-limit Q99 execution path at its own fixed limit
    # price, then a single per-contract broker slot causally selects which arm
    # is allowed to send/open.  This is intentionally separate from the V30
    # dynamic ladder so the two architectures can be compared directly.
    serialized_fixed_arms_enabled: bool = False
    serialized_fixed_arm_prices: str = "0.97,0.98,0.99"
    serialized_fixed_arm_shares: str = "50,50,50"
    # Optional per-arm directional BTC momentum floors. Empty means inherit
    # BID99_BINANCE_MOMENTUM_MIN_USD for every arm. Values align 1:1 with
    # SERIALIZED_FIXED_ARM_PRICES and let the optimizer strengthen only one rung.
    serialized_fixed_arm_momentum_mins: str = ""
    # Optional per-arm early-trigger momentum floors. Empty inherits the
    # corresponding exact-trigger floor from SERIALIZED_FIXED_ARM_MOMENTUM_MINS.
    # V43.17 uses this to loosen exact 0.99 confirmation without also weakening
    # the earlier 0.98 path of the 0.99 arm.
    serialized_fixed_arm_early_momentum_mins: str = ""
    serialized_fixed_arm_early_offset: float = 0.01
    serialized_fixed_arm_ack_ms: float = 50.0
    serialized_fixed_arm_suppress_competing: bool = True
    # Sizing of each fixed arm. fixed uses SERIALIZED_FIXED_ARM_SHARES.
    # btc_move uses the normal causal BTC-distance sizing formula with
    # BTC_MOVE_DIVISOR_USD / BTC_SHARES_MULTIPLIER / MIN/MAX_SHARES_TO_OPEN.
    serialized_fixed_arm_sizing_mode: str = "fixed"
    # Optional per-arm BTC sizing divisors. Empty inherits BTC_MOVE_DIVISOR_USD.
    serialized_fixed_arm_btc_move_divisors: str = ""
    # block = original one-slot serializer. upshift_cancel_lower = when a
    # strictly higher arm becomes send-eligible while a lower arm is genuinely
    # still live, request cancellation of the lower order. The higher arm can
    # then claim the slot on a later causal attempt after cancel+ACK. This tests
    # 0.97 -> 0.98 -> 0.99 chasing without simultaneous independent fills.
    serialized_fixed_arm_busy_policy: str = "block"
    serialized_fixed_arm_upshift_cancel_delay_ms: float = 0.0
    # Optional V43.17 selective-upshift guards. A negative replacement-price
    # floor disables the price guard. When require_unfilled_lower is true, a
    # higher arm cannot evict a lower order after any causal fill has occurred.
    serialized_fixed_arm_upshift_min_replacement_price: float = -1.0
    # Optional owner-price guard. Negative disables it. When enabled, only a
    # currently live lower arm at or below this price may be evicted. V43.17
    # uses 0.97 to protect the empirically stronger 0.98 queue position.
    serialized_fixed_arm_upshift_max_owner_price: float = -1.0
    serialized_fixed_arm_upshift_require_unfilled_lower: bool = False
    # V43.18 blocked-signal persistence. When enabled, each fixed-arm shadow
    # keeps producing later same-outcome eligible send candidates after its
    # first independently openable order. The serializer may use those later
    # candidates only until that arm/outcome actually opens in the real one-slot
    # simulation. This models a live pending intent that keeps retrying after a
    # serializer-local busy-slot block without inventing a second execution slot.
    serialized_fixed_arm_retry_blocked_signals: bool = False
    # Optional pending-intent supersession: once a higher price arm has emitted
    # its first eligible send for the same outcome, stale continuation retries
    # from lower arms are retired. This implements "retry until a higher price
    # signal arrives" without cancelling an already-live order (that remains the
    # separate busy-policy/upshift decision).
    serialized_fixed_arm_retry_supersede_lower_on_higher_signal: bool = False
    # actual_send is the live-realistic mode: only an attempted venue send
    # (opened or placement-rejected) owns the broker slot. report_compatible
    # additionally lets pre-open filter rejections claim the slot, matching the
    # offline serializer from Q99_REPRODUCIBILITY_LIGHT for parity studies.
    serialized_fixed_arm_slot_claim_mode: str = "actual_send"
    # The research strategy gates both the early floor and exact-limit trigger
    # by Binance momentum. Keep this separate from the parent legacy setting.
    serialized_fixed_arm_momentum_apply_to_exact: bool = True
    # Optional per-arm exact-trigger momentum flags aligned 1:1 with prices.
    # Empty inherits SERIALIZED_FIXED_ARM_MOMENTUM_APPLY_TO_EXACT for every arm.
    # This is intentionally separate from the per-arm momentum floor so a 0.99
    # arm can reproduce legacy semantics: early 0.98 needs momentum, exact 0.99
    # bypasses it, while lower arms still require momentum at their exact price.
    serialized_fixed_arm_momentum_apply_exact_flags: str = ""
    serialized_fixed_arm_require_report_eligibility: bool = False
    serialized_fixed_arm_include_suppressed_audit: bool = True
    # Internal optimizer-only switch. Bid-disappearance diagnostics are report
    # telemetry and never affect trigger, placement, cancellation, FIFO fills or
    # PnL. Shared shadow engines can skip this O(n) per-order scan safely.
    optimizer_skip_nondecision_diagnostics: bool = False
    # Internal V43.18.4 continuation-query controls. They are never user-facing
    # strategy settings. A retry child can ask the normal causal engine for the
    # next independently openable same-outcome order whose broker send timestamp
    # is at/after this boundary, avoiding construction/FIFO simulation of every
    # blocked continuation in the market.
    optimizer_entry_not_before_send_ns: int = -1
    optimizer_entry_locked_outcome: str = ""
    # Internal V43.18.10 bounded persistent-continuation stream control.
    # A positive value caps placement-reached attempts produced by a shared
    # retry stream; -1 keeps ordinary single-strategy behavior unchanged.
    optimizer_max_reached_attempts: int = -1
    # Internal V43.18.11 optimizer stream mode. Placement chronology is fully
    # validated against depth, but trade/FIFO fill simulation is deferred until
    # the outer serializer actually selects an independently openable retry.
    optimizer_placement_only_stream: bool = False

    # V43 hard anti-lookahead instrumentation. When enabled, each decision
    # records its latest causal input and hard-fails if any source timestamp is
    # after the decision clock (strict-prior features must also be < trigger).
    strict_causal_audit: bool = False
    # Legacy compatibility switch only. CACHE_SCHEMA.json is optional: the
    # reader always uses the fixed built-in 1e6 price/size schema. When an
    # optional manifest exists it is checked for contradictions and reported.
    require_canonical_cache_schema: bool = False

    wait_after_bid99_sec: float = 0.0
    paper_signal_snapshot_delay_ms: float = 0.0
    paper_send_start_delay_ms: float = 12.5
    paper_shadow_order_open_delay_ms: float = 0.0
    limit_order_lifetime_sec: float = -1.0
    limit_order_lifetime_cancel_delay_ms: float = -1.0
    cancel_if_binance_mid_below_trigger: bool = False
    binance_mid_below_trigger_cancel_delay_ms: float = -1.0
    max_live_book_age_ms: float = 1000.0
    depth_lookback_ms: float = 1000.0
    # Legacy field retained for direct Config construction by older tests/tools.
    # POST_ONLY_ORDER is canonical; None falls back to this legacy value.
    post_only: bool = True
    post_only_order: bool | None = None
    queue_size_multiplier: float = 1.0
    # Optional causal queue-ahead reconstruction from exact-limit depth changes.
    # ``post_order_first`` treats size increases observed after our open as FIFO
    # volume behind us; later unexplained decreases remove that behind volume
    # before they are allowed to reduce the queue ahead.
    queue_ahead_reconstruction_mode: str = "off"
    cancel_if_queue_ahead_above_shares: float = -1.0
    queue_cancel_delay_ms: float = -1.0
    cancel_if_bid99_unexplained_drop_shares: float = -1.0
    bid99_unexplained_drop_cancel_delay_ms: float = -1.0
    write_bid99_volume_timeline: bool = True
    # The strategy always evaluates the raw event stream at full resolution.
    # These controls apply only to the persisted audit representation.
    bid99_volume_output_interval_ms: float = 5000.0
    bid99_volume_max_mib: float = 200.0
    bid99_volume_preview_rows: int = 5000
    fill_remaining_on_ask99_appear: bool = False
    fill_remaining_on_trade_below_limit: bool = False
    # Shared acknowledgement/decision delay for the two stronger fill
    # confirmations.  The order remains live during this interval; a
    # cancellation that becomes effective at or before the delayed fill blocks
    # that fill, while exact-limit FIFO trades may continue to execute.
    price_priority_fill_delay_ms: float = 50.0
    trade_clock_mode: str = "venue_with_local_guard"
    allow_local_trade_clock_fallback: bool = False
    allow_all_trades_when_side_missing: bool = False
    include_same_timestamp_trades: bool = False
    fee_rate: float = 0.0
    fee_exponent: float = 1.0
    starting_balance: float = 10000.0
    enforce_balance: bool = True
    require_resolution: bool = True
    # ``resolution_path`` becomes the effective selected path as soon as local
    # resolution discovery completes.  The raw user request is retained
    # separately so an asset/timeframe typo can be audited without poisoning
    # the final result contract.
    resolution_path: str = ""
    resolution_path_requested: str = ""
    resolution_path_selected: str = ""
    resolution_path_auto_corrected: bool = False
    resolution_path_auto_correction_reason: str = ""
    allow_btc_settlement: bool = False
    source_gap_policy: str = "exclude"
    max_source_gap_market_fraction: float = 0.03
    recover_alternate_sessions: bool = True
    workers: int = 128
    pool_chunksize: int = 1
    limit_markets: int = 0
    sample_markets: int = 0
    preflight_sample_markets: int = 12
    preflight_workers: int = 12
    preflight_mode: str = "parallel_full"
    fail_on_market_error: bool = True
    write_market_charts: bool = True
    market_charts_per_bucket: int = 10
    market_chart_dpi: int = 300
    market_chart_width_in: float = 20.0
    market_chart_height_in: float = 12.0
    market_chart_max_points: int = 0
    market_chart_save_data: bool = True
    # Disabled chart features perform no associated network or cache I/O.
    market_chart_png_compress_level: int = 1
    market_chart_fetch_polymarket_start_end: bool = True
    market_chart_boundary_workers: int = 30
    market_chart_boundary_request_timeout_sec: float = 3.0
    market_chart_boundary_total_timeout_sec: float = 12.0
    market_chart_boundary_retries: int = 1
    fail_on_market_chart_error: bool = True
    # The HTML is a navigation/summary artifact, not a duplicate of every CSV.
    # The writer adaptively reduces previews and fails closed above this cap.
    html_report_max_mib: float = 20.0
    html_report_order_rows: int = 300
    html_report_equity_max_points: int = 5000
    html_report_cell_max_chars: int = 160
    progress_interval_sec: float = 10.0

    @property
    def underlying_asset(self) -> str:
        return q.market_asset(self.market_key)

    @property
    def underlying_asset_label(self) -> str:
        return q.market_asset_label(self.market_key)

    @property
    def market_timeframe(self) -> str:
        return q.market_timeframe(self.market_key)

    @property
    def expected_binance_symbol(self) -> str:
        return q.default_binance_symbol(self.market_key)

    def __post_init__(self) -> None:
        # V43.18.26 is globally FIFO-only, including old launchers/registry names.
        # Decode historical flag names for saved-config compatibility, but never
        # permit a stale environment/config to reactivate ask or taker fills.
        self.fill_remaining_on_ask99_appear = False
        self.fill_remaining_on_trade_below_limit = False
        self.post_only = True
        self.post_only_order = True

    @property
    def post_only_order_enabled(self) -> bool:
        """Return the canonical post-only setting with backward compatibility."""
        return bool(self.post_only if self.post_only_order is None else self.post_only_order)

    @property
    def bid99_binance_momentum_configured(self) -> bool:
        return bool(self.bid99_binance_momentum_min_usd >= 0)

    @property
    def effective_early_momentum_min_usd(self) -> float:
        value = self.bid99_binance_momentum_early_min_usd
        return float(self.bid99_binance_momentum_min_usd if value is None else value)

    def momentum_threshold_for_trigger(self, trigger_source: str) -> float:
        return (
            self.effective_early_momentum_min_usd
            if str(trigger_source or "") == "momentum_early_bid"
            else float(self.bid99_binance_momentum_min_usd)
        )

    @property
    def momentum_early_trigger_enabled(self) -> bool:
        return bool(self.bid99_binance_momentum_early_trigger_bid >= 0)

    @property
    def momentum_ladder_active(self) -> bool:
        return bool(self.momentum_ladder_enabled)

    @property
    def legacy_99_preserved(self) -> bool:
        return bool(self.momentum_ladder_enabled and self.momentum_ladder_preserve_legacy_99)

    def _serialized_csv_floats(self, raw: str, *, name: str) -> list[float]:
        parts = [piece.strip() for piece in str(raw or "").split(",") if piece.strip()]
        if not parts:
            raise ValueError(f"{name} must contain at least one numeric value")
        values: list[float] = []
        for piece in parts:
            try:
                value = float(piece)
            except Exception as exc:
                raise ValueError(f"{name} contains non-numeric value {piece!r}") from exc
            if not math.isfinite(value):
                raise ValueError(f"{name} contains non-finite value {piece!r}")
            values.append(value)
        return values

    @property
    def serialized_fixed_arm_price_list(self) -> list[float]:
        return self._serialized_csv_floats(self.serialized_fixed_arm_prices, name="SERIALIZED_FIXED_ARM_PRICES")

    @property
    def serialized_fixed_arm_share_list(self) -> list[float]:
        return self._serialized_csv_floats(self.serialized_fixed_arm_shares, name="SERIALIZED_FIXED_ARM_SHARES")

    @property
    def serialized_fixed_arm_momentum_min_list(self) -> list[float]:
        raw = str(self.serialized_fixed_arm_momentum_mins or "").strip()
        if not raw:
            return [float(self.bid99_binance_momentum_min_usd)] * len(self.serialized_fixed_arm_price_list)
        return self._serialized_csv_floats(raw, name="SERIALIZED_FIXED_ARM_MOMENTUM_MINS")

    @property
    def serialized_fixed_arm_early_momentum_min_list(self) -> list[float]:
        raw = str(self.serialized_fixed_arm_early_momentum_mins or "").strip()
        if not raw:
            return list(self.serialized_fixed_arm_momentum_min_list)
        return self._serialized_csv_floats(raw, name="SERIALIZED_FIXED_ARM_EARLY_MOMENTUM_MINS")

    @property
    def serialized_fixed_arm_momentum_apply_exact_list(self) -> list[bool]:
        raw = str(self.serialized_fixed_arm_momentum_apply_exact_flags or "").strip()
        if not raw:
            return [bool(self.serialized_fixed_arm_momentum_apply_to_exact)] * len(self.serialized_fixed_arm_price_list)
        out: list[bool] = []
        for piece in raw.split(","):
            token = piece.strip().lower()
            if token in {"1", "true", "yes", "on"}:
                out.append(True)
            elif token in {"0", "false", "no", "off"}:
                out.append(False)
            else:
                raise ValueError(
                    "SERIALIZED_FIXED_ARM_MOMENTUM_APPLY_EXACT_FLAGS contains invalid boolean "
                    f"{piece!r}"
                )
        return out

    @property
    def serialized_fixed_arm_btc_move_divisor_list(self) -> list[float]:
        raw = str(self.serialized_fixed_arm_btc_move_divisors or "").strip()
        if not raw:
            return [float(self.btc_move_divisor_usd)] * len(self.serialized_fixed_arm_price_list)
        return self._serialized_csv_floats(raw, name="SERIALIZED_FIXED_ARM_BTC_MOVE_DIVISORS")

    def serialized_report_eligibility(self) -> dict[str, Any]:
        """Audit whether V31 matches the supplied BTC5 serialized V1 assumptions.

        This is deliberately an audit instead of a silent coercion: users can
        run other arm prices/sizes, while strict mode can fail closed when an
        exact reproduction experiment is desired.
        """
        mismatches: list[str] = []
        prices = self.serialized_fixed_arm_price_list
        shares = self.serialized_fixed_arm_share_list
        def close(a: float, b: float, tol: float = 1e-9) -> bool:
            return abs(float(a) - float(b)) <= tol
        if len(prices) != 3 or any(not close(a, b) for a, b in zip(prices, [0.97, 0.98, 0.99])):
            mismatches.append(f"prices={prices!r} expected=[0.97,0.98,0.99]")
        if len(shares) != 3 or any(not close(a, 50.0) for a in shares):
            mismatches.append(f"shares={shares!r} expected=[50,50,50]")
        if not close(self.serialized_fixed_arm_early_offset, 0.01):
            mismatches.append(f"early_offset={self.serialized_fixed_arm_early_offset} expected=0.01")
        if not close(self.serialized_fixed_arm_ack_ms, 50.0):
            mismatches.append(f"ack_ms={self.serialized_fixed_arm_ack_ms} expected=50")
        if self.serialized_fixed_arm_slot_claim_mode != "report_compatible":
            mismatches.append(f"slot_claim_mode={self.serialized_fixed_arm_slot_claim_mode!r} expected='report_compatible'")
        if not self.serialized_fixed_arm_suppress_competing:
            mismatches.append("suppress_competing=False expected=True")
        if not self.serialized_fixed_arm_momentum_apply_to_exact:
            mismatches.append("momentum_apply_to_exact=False expected=True")
        if any(not close(x, 0.01) for x in self.serialized_fixed_arm_momentum_min_list):
            mismatches.append(f"serialized_momentum_mins={self.serialized_fixed_arm_momentum_min_list!r} expected all 0.01")
        if any(not close(x, 0.01) for x in self.serialized_fixed_arm_early_momentum_min_list):
            mismatches.append(f"serialized_early_momentum_mins={self.serialized_fixed_arm_early_momentum_min_list!r} expected all 0.01")
        if any(not flag for flag in self.serialized_fixed_arm_momentum_apply_exact_list):
            mismatches.append(f"serialized_exact_flags={self.serialized_fixed_arm_momentum_apply_exact_list!r} expected all True")
        if self.share_sizing_mode != "fixed":
            mismatches.append(f"share_sizing_mode={self.share_sizing_mode!r} expected='fixed'")
        if self.serialized_fixed_arm_sizing_mode != "fixed":
            mismatches.append(f"serialized_arm_sizing_mode={self.serialized_fixed_arm_sizing_mode!r} expected='fixed'")
        if self.serialized_fixed_arm_busy_policy != "block":
            mismatches.append(f"serialized_busy_policy={self.serialized_fixed_arm_busy_policy!r} expected='block'")
        if not self.post_only_order_enabled:
            mismatches.append("post_only_order=False expected=True")
        if self.entry_retry_policy != "any_side":
            mismatches.append(f"entry_retry_policy={self.entry_retry_policy!r} expected='any_side'")
        if not close(self.post_only_cross_retry_delay_ms, 30.0):
            mismatches.append(f"post_only_cross_retry_delay_ms={self.post_only_cross_retry_delay_ms} expected=30")
        if not close(self.paper_send_start_delay_ms, 12.5):
            mismatches.append(f"paper_send_start_delay_ms={self.paper_send_start_delay_ms} expected=12.5")
        if not close(self.paper_shadow_order_open_delay_ms, 50.0):
            mismatches.append(f"paper_shadow_order_open_delay_ms={self.paper_shadow_order_open_delay_ms} expected=50")
        if not close(self.binance_venue_to_vps_delay_ms, 130.0):
            mismatches.append(f"binance_venue_to_vps_delay_ms={self.binance_venue_to_vps_delay_ms} expected=130")
        if not close(self.queue_size_multiplier, 1.0):
            mismatches.append(f"queue_size_multiplier={self.queue_size_multiplier} expected=1")
        if not self.cancel_if_binance_mid_below_trigger:
            mismatches.append("cancel_if_binance_mid_adverse_to_outcome=False expected=True")
        if not close(self.binance_mid_below_trigger_cancel_delay_ms, -1.0):
            mismatches.append(f"binance_adverse_cancel_delay_ms={self.binance_mid_below_trigger_cancel_delay_ms} expected=-1")
        if not self.fill_remaining_on_ask99_appear:
            mismatches.append("fill_remaining_on_ask99_appear=False expected=True")
        if self.fill_remaining_on_trade_below_limit:
            mismatches.append("fill_remaining_on_trade_below_limit=True expected=False")
        if not close(self.fee_rate, 0.0):
            mismatches.append(f"fee_rate={self.fee_rate} expected=0")
        if self.market_key == "btc_5m":
            if not close(self.bid99_binance_momentum_min_usd, 0.01):
                mismatches.append(f"momentum_min={self.bid99_binance_momentum_min_usd} expected=0.01 for btc_5m V1")
            if not close(self.bid99_binance_momentum_lookback_sec, 3.0):
                mismatches.append(f"momentum_lookback={self.bid99_binance_momentum_lookback_sec} expected=3 for btc_5m V1")
            if not close(self.entry_market_age_start_sec, 0.0) or not close(self.entry_market_age_end_sec, 300.0):
                mismatches.append(
                    f"entry_age={self.entry_market_age_start_sec}:{self.entry_market_age_end_sec} expected=0:300 for btc_5m V1"
                )
        return {
            "eligible": not mismatches,
            "mismatches": mismatches,
            "reference": "Q99_REPRODUCIBILITY_LIGHT_2026-09-14 BTC5 V1 full150 ACK50",
        }

    @property
    def exact_bid99_momentum_gate_enabled(self) -> bool:
        # In additive V29 ladder mode, exact-TRIGGER_BID remains a genuine
        # legacy scope.  Its old momentum behavior is preserved exactly instead
        # of being replaced by the lower-price ladder.
        return bool(
            self.bid99_binance_momentum_configured
            and (
                self.serialized_fixed_arm_momentum_apply_to_exact
                if self.serialized_fixed_arms_enabled
                else self.bid99_binance_momentum_apply_to_99
            )
            and (not self.momentum_ladder_enabled or self.momentum_ladder_preserve_legacy_99)
        )

    @property
    def bid99_binance_momentum_gate_enabled(self) -> bool:
        # The lookback/index is required when either exact-0.99 activation is
        # gated or the earlier momentum-trigger path is enabled.
        return bool(
            self.bid99_binance_momentum_configured
            and (
                self.bid99_binance_momentum_apply_to_99
                or self.momentum_early_trigger_enabled
                or self.momentum_ladder_enabled
                or self.serialized_fixed_arms_enabled
            )
        )

    def momentum_required_for_trigger(self, *, is_exact_bid99: bool) -> bool:
        if not self.bid99_binance_momentum_configured:
            return False
        return bool(self.bid99_binance_momentum_apply_to_99 if is_exact_bid99 else True)

    @property
    def entry_filters_enabled(self) -> bool:
        return bool(self.entry_require_directional_alignment or self.entry_distance_min_bps >= 0
                    or self.entry_momentum_min_bps >= 0 or self.entry_risk_z_min >= 0
                    or self.entry_distance_min_usd >= 0 or self.entry_momentum_min_usd >= 0)

    @property
    def entry_filter_data_needed(self) -> bool:
        return bool(self.entry_filter_audit_enabled or self.entry_filters_enabled)

    def validate(self) -> None:
        if self.portfolio_btc5_event_mode:
            from queue99_portfolio_btc5 import validate_parent
            validate_parent(self)
        if self.fill_remaining_on_ask99_appear or self.fill_remaining_on_trade_below_limit or not self.post_only_order_enabled:
            raise ValueError("FIFO-only release: ask/below-limit fills and non-post-only orders are unavailable")
        from queue99_entry_filters import validate_filter_config
        validate_filter_config(self)
        if self.polymarket_timestamp_clock not in {"local", "venue"}:
            raise ValueError("POLYMARKET_TIMESTAMP_CLOCK must be local or venue")
        if not math.isfinite(self.polymarket_feed_delay_ms) or self.polymarket_feed_delay_ms < 0:
            raise ValueError("POLYMARKET_FEED_DELAY_MS must be finite and >=0")
        if self.polymarket_timestamp_clock == "venue":
            if self.trade_clock_mode != "venue" or self.allow_local_trade_clock_fallback:
                raise ValueError("venue Polymarket mode requires TRADE_CLOCK_MODE=venue and no local fallback")
            total = self.wait_after_bid99_sec * 1000 + self.paper_signal_snapshot_delay_ms + self.paper_send_start_delay_ms + self.paper_shadow_order_open_delay_ms
            if not math.isfinite(total) or total < self.polymarket_feed_delay_ms:
                raise ValueError("venue-to-open total must be >= Polymarket observation delay")
        self.market_key = q.normalize_market_key(self.market_key)
        self.share_sizing_mode = normalize_sizing_mode(self.share_sizing_mode)
        self.btc_symbol = str(self.btc_symbol or "AUTO").strip().upper()
        if self.btc_symbol in {"", "AUTO", "DEFAULT"}:
            self.btc_symbol = self.expected_binance_symbol
        if self.end_ms <= self.start_ms:
            raise ValueError("END_UTC must be after START_UTC")
        period = q.market_period_sec(self.market_key)
        if not (0 <= self.entry_market_age_start_sec <= self.entry_market_age_end_sec <= period):
            raise ValueError(
                f"entry market-age window must satisfy 0 <= start <= end <= {period}; "
                f"got {self.entry_market_age_start_sec}:{self.entry_market_age_end_sec}"
            )
        if self.final_sizing_volatility_enabled:
            if int(self.final_sizing_candle_count) < 1:
                raise ValueError("FINAL_SIZING_CANDLE_COUNT must be >= 1")
            if int(self.final_sizing_candle_sec) < 1:
                raise ValueError("FINAL_SIZING_CANDLE_SEC must be >= 1")
            if (not math.isfinite(float(self.final_sizing_reference_pct))
                    or float(self.final_sizing_reference_pct) <= 0):
                raise ValueError("FINAL_SIZING_REFERENCE_PCT must be finite and > 0")
            if (not math.isfinite(float(self.final_sizing_factor_min))
                    or not math.isfinite(float(self.final_sizing_factor_max))
                    or float(self.final_sizing_factor_min) <= 0
                    or float(self.final_sizing_factor_max) < float(self.final_sizing_factor_min)
                    or float(self.final_sizing_factor_max) > 1.0 + 1e-12):
                raise ValueError(
                    "FINAL_SIZING_FACTOR_MIN/MAX must satisfy 0 < min <= max <= 1"
                )
        if self.entry_retry_policy not in {"never", "same_side", "any_side"}:
            raise ValueError("ENTRY_RETRY_POLICY must be never, same_side, or any_side")
        post_only_cross_retry_enabled = self.post_only_cross_retry_delay_ms >= 0 and (not self.momentum_ladder_enabled or self.momentum_ladder_preserve_legacy_99)
        if post_only_cross_retry_enabled:
            if not math.isfinite(float(self.post_only_cross_retry_delay_ms)):
                raise ValueError("POST_ONLY_CROSS_RETRY_DELAY_MS must be finite when enabled")
            if not self.post_only_order_enabled:
                raise ValueError("POST_ONLY_CROSS_RETRY_DELAY_MS requires POST_ONLY_ORDER=1")
        if self.trigger_bid <= 0 or self.trigger_bid >= 1:
            raise ValueError("TRIGGER_BID must be between 0 and 1")
        if self.serialized_fixed_arms_enabled:
            if self.momentum_ladder_enabled:
                raise ValueError("SERIALIZED_FIXED_ARMS_ENABLED=1 is mutually exclusive with MOMENTUM_LADDER_ENABLED=1")
            if self.share_sizing_mode != "fixed":
                raise ValueError("SERIALIZED_FIXED_ARMS_ENABLED=1 requires parent SHARE_SIZING_MODE=fixed; use SERIALIZED_FIXED_ARM_SIZING_MODE for arm sizing")
            if self.serialized_fixed_arm_sizing_mode not in {"fixed", "btc_move"}:
                raise ValueError("SERIALIZED_FIXED_ARM_SIZING_MODE must be fixed or btc_move")
            if self.serialized_fixed_arm_sizing_mode == "btc_move":
                if self.btc_move_divisor_usd <= 0 or not math.isfinite(float(self.btc_move_divisor_usd)):
                    raise ValueError("BTC_MOVE_DIVISOR_USD must be finite and > 0 for serialized btc_move sizing")
                if self.btc_shares_multiplier < 0 or not math.isfinite(float(self.btc_shares_multiplier)):
                    raise ValueError("BTC_SHARES_MULTIPLIER must be finite and >= 0 for serialized btc_move sizing")
            prices = self.serialized_fixed_arm_price_list
            shares = self.serialized_fixed_arm_share_list
            momentum_mins = self.serialized_fixed_arm_momentum_min_list
            early_momentum_mins = self.serialized_fixed_arm_early_momentum_min_list
            momentum_apply_exact = self.serialized_fixed_arm_momentum_apply_exact_list
            arm_divisors = self.serialized_fixed_arm_btc_move_divisor_list
            if len(prices) != len(shares):
                raise ValueError("SERIALIZED_FIXED_ARM_PRICES and SERIALIZED_FIXED_ARM_SHARES must have the same length")
            if len(prices) != len(momentum_mins):
                raise ValueError("SERIALIZED_FIXED_ARM_MOMENTUM_MINS must be empty or have one value per arm")
            if any(not math.isfinite(x) for x in momentum_mins):
                raise ValueError("SERIALIZED_FIXED_ARM_MOMENTUM_MINS values must be finite; negative disables that arm's momentum activation gate")
            if len(prices) != len(early_momentum_mins):
                raise ValueError("SERIALIZED_FIXED_ARM_EARLY_MOMENTUM_MINS must be empty or have one value per arm")
            if any(not math.isfinite(x) for x in early_momentum_mins):
                raise ValueError("SERIALIZED_FIXED_ARM_EARLY_MOMENTUM_MINS values must be finite; negative disables that arm's early momentum activation gate")
            if len(prices) != len(momentum_apply_exact):
                raise ValueError("SERIALIZED_FIXED_ARM_MOMENTUM_APPLY_EXACT_FLAGS must be empty or have one value per arm")
            if len(prices) != len(arm_divisors):
                raise ValueError("SERIALIZED_FIXED_ARM_BTC_MOVE_DIVISORS must be empty or have one value per arm")
            if any((not math.isfinite(x)) or x <= 0 for x in arm_divisors):
                raise ValueError("SERIALIZED_FIXED_ARM_BTC_MOVE_DIVISORS values must be finite and > 0")
            if len(prices) < 2:
                raise ValueError("SERIALIZED_FIXED_ARMS_ENABLED=1 requires at least two arms")
            if len(set(round(x, 12) for x in prices)) != len(prices):
                raise ValueError("SERIALIZED_FIXED_ARM_PRICES must be unique")
            if prices != sorted(prices):
                raise ValueError("SERIALIZED_FIXED_ARM_PRICES must be strictly ascending")
            if any(price <= 0 or price >= 1 for price in prices):
                raise ValueError("SERIALIZED_FIXED_ARM_PRICES values must be in (0,1)")
            if any((not math.isfinite(x)) or x <= 0 for x in shares):
                raise ValueError("SERIALIZED_FIXED_ARM_SHARES values must be finite and > 0")
            if not math.isfinite(float(self.serialized_fixed_arm_early_offset)) or self.serialized_fixed_arm_early_offset <= 0:
                raise ValueError("SERIALIZED_FIXED_ARM_EARLY_OFFSET must be finite and > 0")
            if any(price - self.serialized_fixed_arm_early_offset <= 0 for price in prices):
                raise ValueError("SERIALIZED_FIXED_ARM_EARLY_OFFSET makes an early floor <= 0")
            if not math.isfinite(float(self.serialized_fixed_arm_ack_ms)) or self.serialized_fixed_arm_ack_ms < 0:
                raise ValueError("SERIALIZED_FIXED_ARM_ACK_MS must be finite and >= 0")
            if self.serialized_fixed_arm_slot_claim_mode not in {"actual_send", "report_compatible"}:
                raise ValueError("SERIALIZED_FIXED_ARM_SLOT_CLAIM_MODE must be actual_send or report_compatible")
            if self.serialized_fixed_arm_busy_policy not in {"block", "upshift_cancel_lower"}:
                raise ValueError("SERIALIZED_FIXED_ARM_BUSY_POLICY must be block or upshift_cancel_lower")
            if self.serialized_fixed_arm_busy_policy == "upshift_cancel_lower" and self.serialized_fixed_arm_suppress_competing:
                raise ValueError("SERIALIZED_FIXED_ARM_BUSY_POLICY=upshift_cancel_lower requires SERIALIZED_FIXED_ARM_SUPPRESS_COMPETING=0")
            if self.serialized_fixed_arm_retry_supersede_lower_on_higher_signal and not self.serialized_fixed_arm_retry_blocked_signals:
                raise ValueError("SERIALIZED_FIXED_ARM_RETRY_SUPERSEDE_LOWER_ON_HIGHER_SIGNAL requires SERIALIZED_FIXED_ARM_RETRY_BLOCKED_SIGNALS=1")
            if not math.isfinite(float(self.serialized_fixed_arm_upshift_cancel_delay_ms)) or self.serialized_fixed_arm_upshift_cancel_delay_ms < 0:
                raise ValueError("SERIALIZED_FIXED_ARM_UPSHIFT_CANCEL_DELAY_MS must be finite and >= 0")
            upshift_floor = float(self.serialized_fixed_arm_upshift_min_replacement_price)
            if not math.isfinite(upshift_floor) or (upshift_floor >= 0 and not (0 < upshift_floor < 1)):
                raise ValueError("SERIALIZED_FIXED_ARM_UPSHIFT_MIN_REPLACEMENT_PRICE must be negative(disabled) or in (0,1)")
            upshift_owner_ceiling = float(self.serialized_fixed_arm_upshift_max_owner_price)
            if not math.isfinite(upshift_owner_ceiling) or (upshift_owner_ceiling >= 0 and not (0 < upshift_owner_ceiling < 1)):
                raise ValueError("SERIALIZED_FIXED_ARM_UPSHIFT_MAX_OWNER_PRICE must be negative(disabled) or in (0,1)")
            if not self.bid99_binance_momentum_configured:
                raise ValueError("SERIALIZED_FIXED_ARMS_ENABLED=1 requires BID99_BINANCE_MOMENTUM_MIN_USD >= 0")
            if self.serialized_fixed_arm_require_report_eligibility:
                eligibility = self.serialized_report_eligibility()
                if not eligibility["eligible"]:
                    raise ValueError("serialized fixed-arm report eligibility failed: " + "; ".join(eligibility["mismatches"]))
        if not str(self.momentum_ladder_activation_source or "").strip():
            raise ValueError("MOMENTUM_LADDER_ACTIVATION_SOURCE must be non-empty")
        if self.momentum_ladder_enabled:
            if self.order_trigger_policy != "first_market":
                raise ValueError("MOMENTUM_LADDER_ENABLED=1 currently requires ORDER_TRIGGER_POLICY=first_market")
            if not self.bid99_binance_momentum_configured:
                raise ValueError("MOMENTUM_LADDER_ENABLED=1 requires BID99_BINANCE_MOMENTUM_MIN_USD >= 0")
            if not (0 < self.momentum_ladder_start_bid < 1):
                raise ValueError("MOMENTUM_LADDER_START_BID must be in (0,1)")
            if not (self.momentum_ladder_start_bid < self.momentum_ladder_max_bid < 1):
                raise ValueError("MOMENTUM_LADDER_MAX_BID must be > start bid and < 1")
            if not (math.isfinite(float(self.momentum_ladder_tick)) and self.momentum_ladder_tick > 0):
                raise ValueError("MOMENTUM_LADDER_TICK must be finite and > 0")
            if self.momentum_ladder_max_bid + self.trigger_tolerance < self.momentum_ladder_start_bid + self.momentum_ladder_tick:
                raise ValueError("momentum ladder has no executable price above its start bid")
            if not (math.isfinite(float(self.momentum_ladder_post_only_retry_ms)) and self.momentum_ladder_post_only_retry_ms >= 0):
                raise ValueError("MOMENTUM_LADDER_POST_ONLY_RETRY_MS must be finite and >= 0")
            if not math.isfinite(float(self.momentum_ladder_shares_to_open)) or self.momentum_ladder_shares_to_open < -1:
                raise ValueError("MOMENTUM_LADDER_SHARES_TO_OPEN must be -1 (inherit) or non-negative")
            if 0 <= self.momentum_ladder_shares_to_open < 1e-12:
                raise ValueError("MOMENTUM_LADDER_SHARES_TO_OPEN=0 is invalid; use -1 to inherit or disable the ladder")
            if self.momentum_ladder_max_placement_attempts < 1 or self.momentum_ladder_max_placement_attempts > 64:
                raise ValueError("MOMENTUM_LADDER_MAX_PLACEMENT_ATTEMPTS must be in [1,64]")
            if self.momentum_ladder_reprice_cancel_delay_ms < -1:
                raise ValueError("MOMENTUM_LADDER_REPRICE_CANCEL_DELAY_MS must be -1 or non-negative")
            if self.momentum_ladder_stop_drop_ticks < 1:
                raise ValueError("MOMENTUM_LADDER_STOP_DROP_TICKS must be >= 1")
            if self.momentum_ladder_max_orders_per_market < 1:
                raise ValueError("MOMENTUM_LADDER_MAX_ORDERS_PER_MARKET must be >= 1")
            if self.momentum_ladder_max_deals_per_market == 0 or self.momentum_ladder_max_deals_per_market < -1:
                raise ValueError("MOMENTUM_LADDER_MAX_DEALS_PER_MARKET must be -1 or >= 1")
            # When legacy-0.99 preservation is enabled, legacy retry/cancel
            # controls remain active for that top rung.  The lower-price
            # overlay itself still avoids normalized-depth replay.
        if self.queue_size_multiplier < 0:
            raise ValueError("QUEUE_SIZE_MULTIPLIER must be non-negative")
        if str(self.queue_ahead_reconstruction_mode).strip().lower() not in {"off", "post_order_first"}:
            raise ValueError("QUEUE_AHEAD_RECONSTRUCTION_MODE must be off or post_order_first")
        if self.wait_after_bid99_sec < 0:
            raise ValueError("WAIT_AFTER_BID99_SEC must be non-negative")
        if self.limit_order_lifetime_sec >= 0 and self.limit_order_lifetime_cancel_delay_ms < -1:
            raise ValueError(
                "LIMIT_ORDER_LIFETIME_CANCEL_DELAY_MS must be -1 "
                "(reuse order-open delay) or non-negative when lifetime cancellation is enabled"
            )
        sweep_variable_workers = str(os.environ.get("QUEUE99_PARALLEL_SWEEP_MODE", "0")).strip().lower() in {"1","true","yes","y","on"}
        if sweep_variable_workers:
            if self.workers < 1 or self.workers > 128:
                raise ValueError("parallel sweep requires WORKERS in [1,128]")
        elif self.workers != 128:
            raise ValueError("this release requires WORKERS=128 outside QUEUE99_PARALLEL_SWEEP_MODE")
        if self.pool_chunksize != 1:
            raise ValueError("this release requires POOL_CHUNKSIZE=1")
        if self.source_gap_policy not in {"exclude", "fail"}:
            raise ValueError("SOURCE_GAP_POLICY must be exclude or fail")
        if self.source_gap_policy == "exclude" and not (0 <= self.max_source_gap_market_fraction <= 1):
            raise ValueError("MAX_SOURCE_GAP_MARKET_FRACTION must be in [0,1] when SOURCE_GAP_POLICY=exclude")
        if self.share_sizing_mode not in {"fixed", "btc_move"}:
            raise ValueError("SHARE_SIZING_MODE must be underlying_move/binance_move/btc_move or fixed")

        charts_enabled = bool(self.write_market_charts and self.market_charts_per_bucket > 0)
        btc_sizing_enabled = self.share_sizing_mode == "btc_move"
        btc_reversal_cancel_enabled = bool(self.cancel_if_binance_mid_below_trigger)
        if not math.isfinite(float(self.bid99_binance_momentum_min_usd)):
            raise ValueError("BID99_BINANCE_MOMENTUM_MIN_USD must be finite; use a negative value to disable")
        if self.bid99_binance_momentum_early_min_usd is not None:
            early_min = float(self.bid99_binance_momentum_early_min_usd)
            if not math.isfinite(early_min) or early_min < 0:
                raise ValueError("BID99_BINANCE_MOMENTUM_EARLY_MIN_USD must be finite and >= 0, or omitted to inherit")
        btc_momentum_configured = self.bid99_binance_momentum_configured
        btc_momentum_gate_enabled = self.bid99_binance_momentum_gate_enabled
        exact_bid99_momentum_gate_enabled = self.exact_bid99_momentum_gate_enabled
        if not math.isfinite(float(self.bid99_binance_momentum_early_trigger_bid)):
            raise ValueError("BID99_BINANCE_MOMENTUM_EARLY_TRIGGER_BID must be finite; use a negative value to disable")
        momentum_early_trigger_enabled = bool(self.momentum_early_trigger_enabled and (not self.momentum_ladder_enabled or self.momentum_ladder_preserve_legacy_99))
        post_only_order_enabled = self.post_only_order_enabled
        momentum_early_cross_match_enabled = bool(momentum_early_trigger_enabled and not post_only_order_enabled)
        entry_retry_enabled = self.entry_retry_policy != "never"
        if post_only_cross_retry_enabled:
            if not momentum_early_trigger_enabled:
                raise ValueError(
                    "POST_ONLY_CROSS_RETRY_DELAY_MS requires BID99_BINANCE_MOMENTUM_EARLY_TRIGGER_BID >= 0"
                )
            if not btc_momentum_configured:
                raise ValueError(
                    "POST_ONLY_CROSS_RETRY_DELAY_MS requires BID99_BINANCE_MOMENTUM_MIN_USD >= 0 "
                    "for its parent early-momentum trigger"
                )
        if momentum_early_trigger_enabled:
            if not btc_momentum_configured:
                raise ValueError("BID99_BINANCE_MOMENTUM_EARLY_TRIGGER_BID requires BID99_BINANCE_MOMENTUM_MIN_USD >= 0")
            if not (0 < self.bid99_binance_momentum_early_trigger_bid <= self.trigger_bid + self.trigger_tolerance):
                raise ValueError("BID99_BINANCE_MOMENTUM_EARLY_TRIGGER_BID must be > 0 and <= TRIGGER_BID")
            # Crossed-at-placement matching is reachable only when canonical
            # post-only is disabled. With POST_ONLY_ORDER=1 this control is inert.
            if (
                not self.post_only_order_enabled
                and (not math.isfinite(float(self.momentum_early_cross_match_delay_ms))
                     or self.momentum_early_cross_match_delay_ms < 0)
            ):
                raise ValueError("MOMENTUM_EARLY_CROSS_MATCH_DELAY_MS must be finite and non-negative when early entry is enabled and POST_ONLY_ORDER=0")
        btc_trading_enabled = (
            btc_sizing_enabled
            or btc_reversal_cancel_enabled
            or btc_momentum_gate_enabled
            or self.entry_filter_data_needed
        )
        btc_index_enabled = btc_trading_enabled or charts_enabled
        if btc_index_enabled and self.btc_symbol != self.expected_binance_symbol:
            raise ValueError(
                f"BINANCE_SYMBOL/BTC_SYMBOL={self.btc_symbol!r} does not match "
                f"MARKET_KEY={self.market_key!r}; expected {self.expected_binance_symbol!r}. "
                "Use BINANCE_SYMBOL=AUTO or the matching asset symbol."
            )
        # Binance is needed for trading only in btc_move mode, but it is also a
        # mandatory chart series whenever PNG generation is enabled. Fixed-size
        # runs postpone this chart-only load until after successful simulation.
        if btc_index_enabled:
            if not math.isfinite(float(self.binance_venue_to_vps_delay_ms)) or self.binance_venue_to_vps_delay_ms < 0:
                raise ValueError("BINANCE_VENUE_TO_VPS_DELAY_MS must be finite and non-negative when Binance is active")
            required_prestart_ms = float(self.binance_venue_to_vps_delay_ms) + (
                float(self.btc_mid_max_age_ms) if btc_trading_enabled else 0.0
            )
            if btc_momentum_gate_enabled:
                required_prestart_ms += float(self.bid99_binance_momentum_lookback_sec) * 1000.0
            if required_prestart_ms > BTC_INDEX_PRESTART_BUFFER_MS - 1000.0:
                raise ValueError(
                    "required Binance history (feed delay + max age + enabled momentum lookback) exceeds the "
                    f"fixed {BTC_INDEX_PRESTART_BUFFER_MS/1000:g}s venue-index pre-start buffer"
                )
            if not (1 <= self.btc_load_workers <= 128):
                raise ValueError("BTC_LOAD_WORKERS must be between 1 and 128 when Binance is active")
            if self.btc_load_log_every_files < 0:
                raise ValueError("BTC_LOAD_LOG_EVERY_FILES must be non-negative when Binance is active")
            if self.btc_index_cache_mode not in {"auto", "rebuild", "off"}:
                raise ValueError("BTC_INDEX_CACHE_MODE must be auto, rebuild, or off")
        if btc_trading_enabled:
            if self.btc_mid_max_age_ms < 0:
                raise ValueError("BTC_MID_MAX_AGE_MS must be non-negative when Binance trading logic is active")
            if self.btc_worker_index_mode not in {"local_copy", "shared"}:
                raise ValueError("BTC_WORKER_INDEX_MODE must be local_copy or shared when Binance trading logic is active")
        if btc_momentum_gate_enabled:
            if (
                not math.isfinite(float(self.bid99_binance_momentum_min_usd))
                or self.bid99_binance_momentum_min_usd < 0
            ):
                raise ValueError("BID99_BINANCE_MOMENTUM_MIN_USD must be finite and non-negative when enabled")
            if (
                not math.isfinite(float(self.bid99_binance_momentum_lookback_sec))
                or self.bid99_binance_momentum_lookback_sec <= 0
            ):
                raise ValueError("BID99_BINANCE_MOMENTUM_LOOKBACK_SEC must be finite and positive when the gate is enabled")
        if btc_sizing_enabled:
            if self.min_btc_move_usd < 0:
                raise ValueError("MIN_UNDERLYING_MOVE_USD/MIN_BTC_MOVE_USD must be non-negative")
            if self.btc_move_divisor_usd <= 0:
                raise ValueError("UNDERLYING_MOVE_DIVISOR_USD/BTC_MOVE_DIVISOR_USD must be > 0")
            if self.btc_shares_multiplier < 0:
                raise ValueError("UNDERLYING_SHARES_MULTIPLIER/BTC_SHARES_MULTIPLIER must be non-negative")
            if self.min_shares_to_open <= 0 or self.max_shares_to_open < self.min_shares_to_open:
                raise ValueError("share clipping must satisfy 0 < MIN_SHARES_TO_OPEN <= MAX_SHARES_TO_OPEN")
        elif self.fixed_shares_to_open <= 0:
            raise ValueError("FIXED_SHARES_TO_OPEN must be positive")

        # Optional cancellation rules validate only when enabled. Negative
        # thresholds disable the rule and make their delay controls inert.
        if self.cancel_if_queue_ahead_above_shares >= 0 and self.queue_cancel_delay_ms < -1:
            raise ValueError("QUEUE_CANCEL_DELAY_MS must be -1 (reuse open delay) or non-negative")
        if self.cancel_if_bid99_unexplained_drop_shares >= 0:
            if self.bid99_unexplained_drop_cancel_delay_ms < -1:
                raise ValueError(
                    "BID99_UNEXPLAINED_DROP_CANCEL_DELAY_MS must be -1 "
                    "(reuse open delay) or non-negative"
                )
        if self.write_bid99_volume_timeline:
            if (
                not math.isfinite(float(self.bid99_volume_output_interval_ms))
                or self.bid99_volume_output_interval_ms <= 0
            ):
                raise ValueError(
                    "BID99_VOLUME_OUTPUT_INTERVAL_MS must be finite and positive "
                    "when bid99 timeline output is enabled"
                )
            if (
                not math.isfinite(float(self.bid99_volume_max_mib))
                or not (0 < self.bid99_volume_max_mib <= 200)
            ):
                raise ValueError(
                    "BID99_VOLUME_MAX_MIB must be in (0,200] when bid99 timeline output is enabled"
                )
            if self.bid99_volume_preview_rows < 0:
                raise ValueError("BID99_VOLUME_PREVIEW_ROWS must be non-negative")
        if self.cancel_if_binance_mid_below_trigger:
            if self.binance_mid_below_trigger_cancel_delay_ms < -1:
                raise ValueError(
                    "BINANCE_ADVERSE_CANCEL_DELAY_MS (legacy BINANCE_MID_BELOW_TRIGGER_CANCEL_DELAY_MS) "
                    "must be -1 (reuse order-open delay) or non-negative"
                )

        price_priority_fill_enabled = bool(
            self.fill_remaining_on_ask99_appear
            or self.fill_remaining_on_trade_below_limit
        )
        # The delay is completely inert when both confirmation models are
        # disabled, including validation and execution.
        if price_priority_fill_enabled:
            if (
                not math.isfinite(float(self.price_priority_fill_delay_ms))
                or self.price_priority_fill_delay_ms < 0
            ):
                raise ValueError(
                    "PRICE_PRIORITY_FILL_DELAY_MS must be finite and non-negative "
                    "when ASK99 or below-limit fill confirmation is enabled"
                )

        preflight_enabled = self.preflight_mode != "off" and self.preflight_sample_markets > 0
        if self.preflight_mode not in {"parallel_full", "serial_full", "off"}:
            raise ValueError("PREFLIGHT_MODE must be parallel_full, serial_full, or off")
        if self.preflight_sample_markets < 0:
            raise ValueError("PREFLIGHT_SAMPLE_MARKETS must be non-negative")
        if preflight_enabled and not (1 <= self.preflight_workers <= 128):
            raise ValueError("PREFLIGHT_WORKERS must be between 1 and 128 when preflight is enabled")

        if self.market_charts_per_bucket < 0:
            raise ValueError("MARKET_CHARTS_PER_BUCKET must be non-negative")
        # Hard chart gate: when WRITE_MARKET_CHARTS=0 or PER_BUCKET=0, no
        # plotting module is imported and chart-only controls are inert.
        if charts_enabled:
            if self.market_chart_dpi <= 0 or self.market_chart_width_in <= 0 or self.market_chart_height_in <= 0:
                raise ValueError("market chart DPI/width/height must be positive")
            if self.market_chart_max_points < 0:
                raise ValueError("MARKET_CHART_MAX_POINTS must be non-negative")
            if not (0 <= self.market_chart_png_compress_level <= 9):
                raise ValueError("MARKET_CHART_PNG_COMPRESS_LEVEL must be between 0 and 9")
            # Chart-only HTTP controls are ignored unless that annotation is
            # enabled. Local PNG rendering never depends on this network path.
            if self.market_chart_fetch_polymarket_start_end:
                if not (1 <= self.market_chart_boundary_workers <= 30):
                    raise ValueError("MARKET_CHART_BOUNDARY_WORKERS must be between 1 and 30")
                if self.market_chart_boundary_request_timeout_sec <= 0:
                    raise ValueError("MARKET_CHART_BOUNDARY_REQUEST_TIMEOUT_SEC must be positive")
                if self.market_chart_boundary_total_timeout_sec <= 0:
                    raise ValueError("MARKET_CHART_BOUNDARY_TOTAL_TIMEOUT_SEC must be positive")
                if self.market_chart_boundary_retries < 1:
                    raise ValueError("MARKET_CHART_BOUNDARY_RETRIES must be at least 1")

        if (
            not math.isfinite(float(self.html_report_max_mib))
            or not (0 < self.html_report_max_mib <= 20)
        ):
            raise ValueError("HTML_REPORT_MAX_MIB must be in (0,20]")
        if self.html_report_order_rows < 0:
            raise ValueError("HTML_REPORT_ORDER_ROWS must be non-negative")
        if self.html_report_equity_max_points < 100:
            raise ValueError("HTML_REPORT_EQUITY_MAX_POINTS must be at least 100")
        if not (32 <= self.html_report_cell_max_chars <= 2000):
            raise ValueError("HTML_REPORT_CELL_MAX_CHARS must be between 32 and 2000")

    def feature_plan(self) -> dict[str, Any]:
        btc_sizing_enabled = (
            self.share_sizing_mode == "btc_move"
            or (self.serialized_fixed_arms_enabled and self.serialized_fixed_arm_sizing_mode == "btc_move")
        )
        btc_reversal_cancel_enabled = bool(self.cancel_if_binance_mid_below_trigger)
        btc_momentum_configured = self.bid99_binance_momentum_configured
        btc_momentum_gate_enabled = self.bid99_binance_momentum_gate_enabled
        exact_bid99_momentum_gate_enabled = self.exact_bid99_momentum_gate_enabled
        ladder_enabled = bool(self.momentum_ladder_enabled)
        serialized_fixed_arms_enabled = bool(self.serialized_fixed_arms_enabled)
        momentum_early_trigger_enabled = bool(
            serialized_fixed_arms_enabled
            or (self.momentum_early_trigger_enabled and (not ladder_enabled or self.momentum_ladder_preserve_legacy_99))
        )
        post_only_order_enabled = self.post_only_order_enabled
        momentum_early_cross_match_enabled = bool(momentum_early_trigger_enabled and not post_only_order_enabled)
        entry_retry_enabled = bool(self.entry_retry_policy != "never" and (not ladder_enabled or self.momentum_ladder_preserve_legacy_99))
        post_only_cross_retry_enabled = bool(self.post_only_cross_retry_delay_ms >= 0 and (not ladder_enabled or self.momentum_ladder_preserve_legacy_99))
        btc_trading_enabled = (
            btc_sizing_enabled
            or btc_reversal_cancel_enabled
            or btc_momentum_gate_enabled
            or self.entry_filter_data_needed
        )
        preflight_enabled = self.preflight_mode != "off" and self.preflight_sample_markets > 0
        charts_enabled = bool(self.write_market_charts and self.market_charts_per_bucket > 0)
        bid99_drop_cancel_enabled = self.cancel_if_bid99_unexplained_drop_shares >= 0
        exact_bid99_replay_enabled = bool(
            bid99_drop_cancel_enabled and (not ladder_enabled or self.momentum_ladder_preserve_legacy_99)
        )
        queue_cancel_enabled = self.cancel_if_queue_ahead_above_shares >= 0
        boundary_fetch_enabled = charts_enabled and bool(self.market_chart_fetch_polymarket_start_end)
        # Binance is a mandatory chart series whenever market charts are enabled.
        # Fixed-size trading still avoids all Binance work until the post-run chart phase.
        btc_chart_enabled = charts_enabled
        btc_index_enabled = btc_trading_enabled or btc_chart_enabled
        lifetime_cancel_enabled = self.limit_order_lifetime_sec >= 0
        return {
            "btc_mid_index_enabled": btc_index_enabled,
            "btc_source_discovery_enabled": btc_index_enabled,
            "btc_raw_shard_read_enabled": btc_index_enabled,
            "btc_used_by_sizing": btc_sizing_enabled,
            "btc_asof_age_guard_enabled": btc_trading_enabled,
            "btc_venue_clock_required": btc_index_enabled,
            "btc_venue_to_vps_delay_enabled": btc_index_enabled,
            "btc_mid_adverse_to_outcome_cancel_enabled": btc_reversal_cancel_enabled,
            "btc_mid_below_trigger_cancel_enabled": btc_reversal_cancel_enabled,
            "bid99_binance_momentum_configured": btc_momentum_configured,
            "bid99_binance_momentum_apply_to_99": bool(self.bid99_binance_momentum_apply_to_99),
            "bid99_binance_momentum_early_min_usd": float(self.effective_early_momentum_min_usd),
            "bid99_binance_momentum_gate_enabled": btc_momentum_gate_enabled,
            "bid99_binance_momentum_exact_bid99_gate_enabled": exact_bid99_momentum_gate_enabled,
            "bid99_binance_momentum_early_gate_enabled": bool(btc_momentum_configured and momentum_early_trigger_enabled),
            "bid99_binance_momentum_lookback_enabled": btc_momentum_gate_enabled,
            "bid99_binance_momentum_early_trigger_enabled": momentum_early_trigger_enabled,
            "bid99_binance_momentum_scope": (
                "serialized_fixed_arms_all_levels" if serialized_fixed_arms_enabled and btc_momentum_configured
                else "legacy_99_plus_ladder_lower" if ladder_enabled and self.momentum_ladder_preserve_legacy_99 and btc_momentum_configured
                else "ladder_all_levels" if ladder_enabled and btc_momentum_configured
                else "exact_and_early" if exact_bid99_momentum_gate_enabled and momentum_early_trigger_enabled
                else "exact_only" if exact_bid99_momentum_gate_enabled
                else "early_only" if btc_momentum_configured and momentum_early_trigger_enabled
                else "disabled"
            ),
            "serialized_fixed_arms_enabled": serialized_fixed_arms_enabled,
            "serialized_fixed_arm_prices": self.serialized_fixed_arm_price_list if serialized_fixed_arms_enabled else [],
            "serialized_fixed_arm_shares": self.serialized_fixed_arm_share_list if serialized_fixed_arms_enabled else [],
            "serialized_fixed_arm_early_offset": float(self.serialized_fixed_arm_early_offset) if serialized_fixed_arms_enabled else None,
            "serialized_fixed_arm_ack_ms": float(self.serialized_fixed_arm_ack_ms) if serialized_fixed_arms_enabled else None,
            "serialized_fixed_arm_suppress_competing": bool(self.serialized_fixed_arm_suppress_competing) if serialized_fixed_arms_enabled else False,
            "serialized_fixed_arm_sizing_mode": str(self.serialized_fixed_arm_sizing_mode) if serialized_fixed_arms_enabled else "disabled",
            "serialized_fixed_arm_momentum_mins": self.serialized_fixed_arm_momentum_min_list if serialized_fixed_arms_enabled else [],
            "serialized_fixed_arm_early_momentum_mins": self.serialized_fixed_arm_early_momentum_min_list if serialized_fixed_arms_enabled else [],
            "serialized_fixed_arm_momentum_apply_exact_flags": self.serialized_fixed_arm_momentum_apply_exact_list if serialized_fixed_arms_enabled else [],
            "serialized_fixed_arm_btc_move_divisors": self.serialized_fixed_arm_btc_move_divisor_list if serialized_fixed_arms_enabled else [],
            "serialized_fixed_arm_busy_policy": str(self.serialized_fixed_arm_busy_policy) if serialized_fixed_arms_enabled else "disabled",
            "serialized_fixed_arm_upshift_cancel_delay_ms": float(self.serialized_fixed_arm_upshift_cancel_delay_ms) if serialized_fixed_arms_enabled else None,
            "serialized_fixed_arm_upshift_min_replacement_price": float(self.serialized_fixed_arm_upshift_min_replacement_price) if serialized_fixed_arms_enabled else None,
            "serialized_fixed_arm_upshift_max_owner_price": float(self.serialized_fixed_arm_upshift_max_owner_price) if serialized_fixed_arms_enabled else None,
            "serialized_fixed_arm_upshift_require_unfilled_lower": bool(self.serialized_fixed_arm_upshift_require_unfilled_lower) if serialized_fixed_arms_enabled else False,
            "serialized_fixed_arm_retry_blocked_signals": bool(self.serialized_fixed_arm_retry_blocked_signals) if serialized_fixed_arms_enabled else False,
            "serialized_fixed_arm_retry_supersede_lower_on_higher_signal": bool(self.serialized_fixed_arm_retry_supersede_lower_on_higher_signal) if serialized_fixed_arms_enabled else False,
            "serialized_fixed_arm_slot_claim_mode": str(self.serialized_fixed_arm_slot_claim_mode) if serialized_fixed_arms_enabled else "disabled",
            "serialized_fixed_arm_momentum_apply_to_exact": bool(self.serialized_fixed_arm_momentum_apply_to_exact) if serialized_fixed_arms_enabled else False,
            "serialized_fixed_arm_report_eligibility": self.serialized_report_eligibility() if serialized_fixed_arms_enabled else {"eligible": False, "mismatches": ["mode_disabled"]},
            "momentum_ladder_enabled": ladder_enabled,
            "momentum_ladder_activation_source": str(self.momentum_ladder_activation_source),
            "momentum_ladder_preserve_legacy_99": bool(self.momentum_ladder_preserve_legacy_99) if ladder_enabled else False,
            "momentum_ladder_make_before_break": bool(self.momentum_ladder_make_before_break) if ladder_enabled else False,
            "momentum_ladder_fast_single_read_path": bool(ladder_enabled and not self.momentum_ladder_preserve_legacy_99),
            "momentum_ladder_overlay_single_read_path": ladder_enabled,
            "momentum_ladder_start_bid": float(self.momentum_ladder_start_bid) if ladder_enabled else None,
            "momentum_ladder_max_bid": float(self.momentum_ladder_max_bid) if ladder_enabled else None,
            "momentum_ladder_effective_lower_max_bid": (
                _ladder_price(min(float(self.momentum_ladder_max_bid), float(self.trigger_bid) - float(self.momentum_ladder_tick)), float(self.momentum_ladder_tick))
                if ladder_enabled and self.momentum_ladder_preserve_legacy_99 else float(self.momentum_ladder_max_bid) if ladder_enabled else None
            ),
            "momentum_ladder_tick": float(self.momentum_ladder_tick) if ladder_enabled else None,
            "momentum_ladder_post_only_retry_ms": float(self.momentum_ladder_post_only_retry_ms) if ladder_enabled else None,
            "momentum_ladder_shares_to_open": float(self.momentum_ladder_shares_to_open) if ladder_enabled else None,
            "momentum_ladder_max_placement_attempts": int(self.momentum_ladder_max_placement_attempts) if ladder_enabled else None,
            "momentum_ladder_continue_after_full_fill": bool(self.momentum_ladder_continue_after_full_fill) if ladder_enabled else False,
            "momentum_ladder_max_deals_per_market": int(self.momentum_ladder_max_deals_per_market) if ladder_enabled else None,
            "momentum_ladder_reprice_enabled": bool(self.momentum_ladder_reprice_enabled) if ladder_enabled else False,
            "legacy_entry_state_machine_bypassed": bool(ladder_enabled and not self.momentum_ladder_preserve_legacy_99),
            "legacy_99_entry_state_machine_preserved": bool(ladder_enabled and self.momentum_ladder_preserve_legacy_99),
            "post_only_order_enabled": post_only_order_enabled,
            "entry_retry_policy": self.entry_retry_policy,
            "entry_retry_enabled": entry_retry_enabled,
            "post_only_cross_retry_enabled": post_only_cross_retry_enabled,
            "post_only_cross_retry_delay_ms": (
                float(self.post_only_cross_retry_delay_ms) if post_only_cross_retry_enabled else None
            ),
            "post_only_cross_retry_same_outcome_only": post_only_cross_retry_enabled,
            "post_only_cross_retry_exact_bid99_required": post_only_cross_retry_enabled,
            "momentum_early_cross_match_enabled": momentum_early_cross_match_enabled,
            "btc_used_by_market_charts": btc_chart_enabled,
            "btc_worker_mmap_enabled": btc_trading_enabled,
            "underlying_asset": self.underlying_asset,
            "binance_symbol": self.btc_symbol,
            "underlying_mid_index_enabled": btc_index_enabled,
            "underlying_used_by_sizing": btc_sizing_enabled,
            "underlying_used_by_market_charts": btc_chart_enabled,
            "underlying_worker_mmap_enabled": btc_trading_enabled,
            "preflight_enabled": preflight_enabled,
            "alternate_session_recovery_enabled": bool(self.recover_alternate_sessions),
            "post_open_ask99_cross_monitor_enabled": bool(self.fill_remaining_on_ask99_appear),
            "trade_below_limit_price_priority_enabled": bool(self.fill_remaining_on_trade_below_limit),
            "price_priority_fill_delay_enabled": bool(
                self.fill_remaining_on_ask99_appear
                or self.fill_remaining_on_trade_below_limit
            ),
            "queue_ahead_cancel_enabled": queue_cancel_enabled,
            "queue_ahead_reconstruction_enabled": str(self.queue_ahead_reconstruction_mode).strip().lower() != "off",
            "queue_ahead_reconstruction_mode": str(self.queue_ahead_reconstruction_mode).strip().lower(),
            "limit_order_lifetime_cancel_enabled": lifetime_cancel_enabled,
            "bid99_unexplained_drop_cancel_enabled": bid99_drop_cancel_enabled,
            "bid99_volume_timeline_write_enabled": bool(self.write_bid99_volume_timeline),
            "bid99_volume_timeline_compact_output": bool(self.write_bid99_volume_timeline),
            "bid99_volume_output_interval_ms": (
                float(self.bid99_volume_output_interval_ms)
                if self.write_bid99_volume_timeline else None
            ),
            "bid99_volume_output_hard_cap_mib": (
                float(self.bid99_volume_max_mib)
                if self.write_bid99_volume_timeline else None
            ),
            "html_report_compact_mode": True,
            "html_report_hard_cap_mib": float(self.html_report_max_mib),
            # Exact normalized-depth replay is a hard-gated trading input only
            # for the unexplained external bid-0.99 cancellation rule.
            "exact_bid99_level_replay_enabled": exact_bid99_replay_enabled,
            "normalized_depth_events_strategy_load_enabled": bool(exact_bid99_replay_enabled or momentum_early_cross_match_enabled),
            "normalized_depth_events_ask_ladder_load_is_conditional": momentum_early_cross_match_enabled,
            "market_charts_enabled": charts_enabled,
            "market_chart_source_reload_enabled": charts_enabled,
            "market_chart_source_sidecars_enabled": charts_enabled and bool(self.market_chart_save_data),
            "market_chart_polymarket_start_end_fetch_enabled": boundary_fetch_enabled,
            "inactive_control_groups": [
                name
                for name, active in (
                    ("serialized_fixed_arms", serialized_fixed_arms_enabled),
                    ("momentum_ladder_fast_path", ladder_enabled),
                    ("btc_move_sizing", btc_sizing_enabled),
                    ("binance_mid_adverse_to_outcome_cancellation", btc_reversal_cancel_enabled),
                    ("bid99_binance_directional_momentum_gate", btc_momentum_gate_enabled),
                    ("bid99_binance_momentum_exact_bid99_gate", exact_bid99_momentum_gate_enabled),
                    ("bid99_binance_momentum_early_trigger", momentum_early_trigger_enabled),
                    ("post_only_entry", post_only_order_enabled),
                    ("entry_retry", entry_retry_enabled),
                    ("post_only_cross_retry", post_only_cross_retry_enabled),
                    ("momentum_early_crossed_ask_match", momentum_early_cross_match_enabled),
                    ("binance_market_chart_series", btc_chart_enabled),
                    ("integrated_preflight", preflight_enabled),
                    ("queue_ahead_cancellation", queue_cancel_enabled),
                    ("queue_ahead_depth_reconstruction", str(self.queue_ahead_reconstruction_mode).strip().lower() != "off"),
                    ("limit_order_lifetime_cancellation", lifetime_cancel_enabled),
                    ("bid99_unexplained_drop_cancellation", bid99_drop_cancel_enabled),
                    ("bid99_volume_timeline_output", bool(self.write_bid99_volume_timeline)),
                    ("market_charts_and_chart_cache_reads", charts_enabled),
                    ("chart_only_polymarket_start_end_network", boundary_fetch_enabled),
                    ("chart_source_sidecar_writes", charts_enabled and bool(self.market_chart_save_data)),
                    ("alternate_session_directory_search", bool(self.recover_alternate_sessions)),
                    ("post_open_ask99_cross_monitor", bool(self.fill_remaining_on_ask99_appear)),
                    ("trade_below_limit_price_priority", bool(self.fill_remaining_on_trade_below_limit)),
                )
                if not active
            ],
        }

    def to_dict(self) -> dict[str, Any]:
        return {field.name: getattr(self, field.name) for field in fields(self)} | {
            "cancel_if_binance_mid_adverse_to_outcome": bool(self.cancel_if_binance_mid_below_trigger),
            "binance_adverse_cancel_delay_ms": float(self.binance_mid_below_trigger_cancel_delay_ms),
            "version": q.VERSION,
            "post_only_order": self.post_only_order_enabled,
            "post_only": self.post_only_order_enabled,
            "schema": "queue99-effective-config-v43-8-reverse-book-index",
            "underlying_asset": self.underlying_asset,
            "underlying_asset_label": self.underlying_asset_label,
            "market_timeframe": self.market_timeframe,
            "market_period_sec": q.market_period_sec(self.market_key),
            "binance_symbol": self.btc_symbol,
            "underlying_price_path": self.btc_price_path,
            "underlying_mid_max_age_ms": self.btc_mid_max_age_ms,
            "underlying_move_divisor_usd": self.btc_move_divisor_usd,
            "underlying_shares_multiplier": self.btc_shares_multiplier,
            "min_underlying_move_usd": self.min_btc_move_usd,
            "allow_binance_settlement": bool(self.allow_btc_settlement),
            "legacy_btc_setting_names_retained": True,
            "entry_market_age_window_inclusive": True,
            "polymarket_start_end_scope": "chart_only_after_backtest",
            "polymarket_start_end_used_by_trading": False,
            "feature_plan": self.feature_plan(),
        }


def parse_utc_ms(value: str) -> int:
    parsed = pd.to_datetime(value, utc=True, errors="raise")
    return int(pd.Timestamp(parsed).value // 1_000_000)


def _half_up_nonnegative(value: float) -> int:
    """Stable half-up rounding for non-negative share quantities."""
    value = max(0.0, float(value))
    half = math.floor(value) + 0.5
    stable = half if abs(value - half) <= 1e-9 else value
    return int(math.floor(stable + 0.5))


def apply_final_sizing_volatility_multiplier(
    *, cfg: Config, underlying: Any, decision_ns: int, base_requested_shares: float,
) -> tuple[float | None, dict[str, Any], str]:
    """Apply the causal trailing-candle final-size multiplier.

    The caller has already selected the strategy's ordinary final quantity.
    This helper only shrinks that quantity; it never changes signal eligibility,
    order price, queue position, or the maximum execution shadow. The original
    minimum-share clip is deliberately not re-applied: with a 0.1 factor, a
    ten-share base order is allowed to become one share.
    """
    base = float(base_requested_shares)
    if not cfg.final_sizing_volatility_enabled:
        return base, {
            "final_sizing_volatility_enabled": False,
            "final_sizing_base_requested_shares": base,
            "final_sizing_multiplier": 1.0,
            "final_sizing_requested_shares": base,
            "final_sizing_status": "disabled",
        }, ""
    if underlying is None:
        return None, {
            "final_sizing_volatility_enabled": True,
            "final_sizing_base_requested_shares": base,
            "final_sizing_status": "missing_underlying_series",
        }, "missing_final_sizing_underlying_series"
    stats = underlying.trailing_closed_candle_abs_change_stats(
        decision_ns=int(decision_ns),
        venue_to_vps_delay_ms=float(cfg.binance_venue_to_vps_delay_ms),
        candle_sec=int(cfg.final_sizing_candle_sec),
        candle_count=int(cfg.final_sizing_candle_count),
    )
    if stats is None:
        return None, {
            "final_sizing_volatility_enabled": True,
            "final_sizing_base_requested_shares": base,
            "final_sizing_candle_count": int(cfg.final_sizing_candle_count),
            "final_sizing_candle_sec": int(cfg.final_sizing_candle_sec),
            "final_sizing_reference_pct": float(cfg.final_sizing_reference_pct),
            "final_sizing_factor_min": float(cfg.final_sizing_factor_min),
            "final_sizing_factor_max": float(cfg.final_sizing_factor_max),
            "final_sizing_status": "missing_complete_candles",
        }, "missing_complete_final_sizing_candles"
    mean_pct = float(stats["mean_abs_change_pct"])
    raw_factor = mean_pct / float(cfg.final_sizing_reference_pct)
    factor = max(
        float(cfg.final_sizing_factor_min),
        min(float(cfg.final_sizing_factor_max), raw_factor),
    )
    scaled_raw = max(0.0, base * factor)
    rounded = _half_up_nonnegative(scaled_raw)
    base_cap = max(1, _half_up_nonnegative(base))
    final = float(max(1, min(base_cap, rounded)))
    fields: dict[str, Any] = {
        "final_sizing_volatility_enabled": True,
        "final_sizing_base_requested_shares": base,
        "final_sizing_candle_count": int(cfg.final_sizing_candle_count),
        "final_sizing_candle_sec": int(cfg.final_sizing_candle_sec),
        "final_sizing_reference_pct": float(cfg.final_sizing_reference_pct),
        "final_sizing_factor_min": float(cfg.final_sizing_factor_min),
        "final_sizing_factor_max": float(cfg.final_sizing_factor_max),
        "final_sizing_mean_abs_change_pct": mean_pct,
        "final_sizing_factor_raw": float(raw_factor),
        "final_sizing_multiplier": float(factor),
        "final_sizing_scaled_raw_shares": float(scaled_raw),
        "final_sizing_scaled_rounded_shares": int(rounded),
        "final_sizing_requested_shares": final,
        "final_sizing_candle_window_start_ns": int(stats["window_start_ns"]),
        "final_sizing_candle_window_end_ns": int(stats["window_end_ns"]),
        "final_sizing_max_input_venue_ts_ns": int(stats["max_input_venue_ts_ns"]),
        "final_sizing_max_input_observed_ts_ns": int(stats["max_input_observed_ts_ns"]),
        "final_sizing_candle_abs_change_pcts_json": json.dumps(
            stats["abs_change_pcts"], separators=(",", ":")
        ),
        "final_sizing_candle_opens_json": json.dumps(
            stats["opens"], separators=(",", ":")
        ),
        "final_sizing_candle_closes_json": json.dumps(
            stats["closes"], separators=(",", ":")
        ),
        "final_sizing_status": "applied",
    }
    return final, fields, ""


def normalize_sizing_mode(value: Any) -> str:
    raw = str(value or "btc_move").strip().lower().replace("-", "_")
    aliases = {
        "binance_move": "btc_move",
        "underlying_move": "btc_move",
        "asset_move": "btc_move",
        "move": "btc_move",
        "fixed_shares": "fixed",
    }
    raw = aliases.get(raw, raw)
    if raw not in {"btc_move", "fixed"}:
        raise ValueError("SHARE_SIZING_MODE must be underlying_move/binance_move/btc_move or fixed")
    return raw

_CONFIG_FIELD_NAMES = frozenset(field.name for field in fields(Config))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Queue99 FIFO backtester for BTC/ETH/SOL/DOGE/XRP/HYPE markets with Binance underlying data and chart-only Polymarket boundaries")
    p.add_argument("--data-dir", required=True)
    p.add_argument("--outdir", required=True)
    p.add_argument("--start-utc", required=True)
    p.add_argument("--end-utc", required=True)
    p.add_argument("--market-key", default="btc_5m", help="Explicit asset/timeframe key such as btc_5m, eth_15m, sol_15m, doge_5m, xrp_5m, or hype_5m")
    p.add_argument("--yes-outcome", default="UP")
    p.add_argument("--trigger-bid", type=float, default=0.99)
    p.add_argument("--trigger-tolerance", type=float, default=1e-6)
    p.add_argument("--order-trigger-policy", choices=["first_market", "first_per_outcome"], default="first_market")
    p.add_argument(
        "--entry-retry-policy",
        choices=["never", "same_side", "any_side"],
        default="never",
        help=(
            "Retry policy after a terminal candidate rejection. never consumes the selected opportunity; "
            "same_side retries only the original outcome; any_side may switch to a later qualifying outcome."
        ),
    )
    p.add_argument(
        "--post-only-cross-retry-delay-ms",
        type=float,
        default=-1.0,
        help=(
            "One-shot same-outcome retry after a momentum-early attempt is rejected because POST_ONLY would cross. "
            "The retry waits this many milliseconds after the rejection, then requires a later exact 0.99 bid. "
            "Fresh directional Binance momentum is required only when BID99_BINANCE_MOMENTUM_APPLY_TO_99=1. "
            "Negative disables the dedicated retry."
        ),
    )
    p.add_argument("--entry-market-age-start-sec", type=float, default=0.0)
    p.add_argument("--entry-market-age-end-sec", type=float, default=300.0)
    p.add_argument("--share-sizing-mode", choices=["btc_move", "binance_move", "underlying_move", "fixed"], default="btc_move")
    p.add_argument("--btc-price-path", "--binance-price-path", "--underlying-price-path", dest="btc_price_path", default="")
    p.add_argument("--btc-symbol", "--binance-symbol", "--underlying-symbol", dest="btc_symbol", default="AUTO")
    p.add_argument("--btc-mid-max-age-ms", "--underlying-mid-max-age-ms", dest="btc_mid_max_age_ms", type=float, default=2000.0)
    p.add_argument(
        "--binance-venue-to-vps-delay-ms",
        type=float,
        default=0.0,
        help=(
            "Simulated one-way Binance venue-to-VPS feed delay. Binance rows are indexed by venue_ts; "
            "a row is observable at venue_ts + this delay."
        ),
    )
    p.add_argument("--polymarket-timestamp-clock", choices=["local", "venue"], default=os.environ.get("POLYMARKET_TIMESTAMP_CLOCK", "local"))
    p.add_argument("--polymarket-feed-delay-ms", type=float, default=float(os.environ.get("POLYMARKET_FEED_DELAY_MS", "13")))
    p.add_argument("--btc-load-workers", type=int, default=128)
    p.add_argument("--btc-prune-by-time-stats", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--btc-load-log-every-files", type=int, default=1000)
    p.add_argument("--btc-index-cache-mode", choices=["auto", "rebuild", "off"], default="auto")
    p.add_argument("--btc-index-cache-dir", default="")
    p.add_argument("--btc-index-cache-source-tag", default="")
    p.add_argument("--btc-worker-index-mode", choices=["local_copy", "shared"], default="local_copy")
    p.add_argument("--btc-worker-mmap-root", default="")
    p.add_argument("--btc-move-divisor-usd", "--underlying-move-divisor-usd", dest="btc_move_divisor_usd", type=float, default=100.0)
    p.add_argument("--btc-shares-multiplier", "--underlying-shares-multiplier", dest="btc_shares_multiplier", type=float, default=30.0)
    p.add_argument("--min-shares-to-open", type=int, default=5)
    p.add_argument("--max-shares-to-open", type=int, default=1000)
    p.add_argument("--fixed-shares-to-open", type=float, default=30.0)
    p.add_argument("--min-btc-move-usd", "--min-underlying-move-usd", dest="min_btc_move_usd", type=float, default=0.0)
    p.add_argument(
        "--bid99-binance-momentum-min-usd",
        type=float,
        default=-1.0,
        help=(
            "Direction-aware Binance move required for each momentum-gated entry candidate. "
            "UP requires current-reference >= this value; DOWN requires "
            "reference-current >= this value. Negative disables momentum gating."
        ),
    )
    p.add_argument(
        "--bid99-binance-momentum-early-min-usd",
        type=float,
        default=None,
        help=(
            "Optional momentum threshold used only by the earlier bid trigger (for example 0.98). "
            "When omitted it inherits --bid99-binance-momentum-min-usd. This does not change "
            "exact-TRIGGER_BID or ladder thresholds."
        ),
    )
    p.add_argument(
        "--bid99-binance-momentum-lookback-sec",
        type=float,
        default=1.0,
        help=(
            "Lookback from the candidate Polymarket trigger timestamp for the optional "
            "direction-aware Binance momentum gate."
        ),
    )
    p.add_argument(
        "--bid99-binance-momentum-apply-to-99",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "When enabled, exact-TRIGGER_BID activation requires the Binance momentum gate. "
            "When disabled, exact-0.99 activation bypasses momentum while the configured "
            "earlier bid trigger (for example 0.98) continues to require momentum."
        ),
    )
    p.add_argument("--bid99-binance-momentum-early-trigger-bid", type=float, default=-1.0,
                   help="Optional earlier bid floor such as 0.98; negative disables")
    p.add_argument("--momentum-early-cross-match-delay-ms", type=float, default=50.0,
                   help="Additional delay after normal arrival before matching crossing asks for an early-momentum order")
    p.add_argument("--momentum-ladder-enabled", action=argparse.BooleanOptionalAction, default=False,
                   help="Enable the additive passive momentum ladder")
    p.add_argument("--momentum-ladder-preserve-legacy-99", action=argparse.BooleanOptionalAction, default=True,
                   help="Keep the original TRIGGER_BID/0.99 state machine unchanged and add only lower ladder orders")
    p.add_argument("--momentum-ladder-make-before-break", action=argparse.BooleanOptionalAction, default=True,
                   help="Place the higher replacement successfully before starting cancellation of the lower rung")
    p.add_argument("--momentum-ladder-activation-source", default="direct_cli",
                   help="Audit label describing how ladder mode was selected")
    p.add_argument("--momentum-ladder-start-bid", type=float, default=0.95,
                   help="Initial observed Polymarket bid level that arms the ladder")
    p.add_argument("--momentum-ladder-max-bid", type=float, default=0.99,
                   help="Highest passive limit price used by the ladder")
    p.add_argument("--momentum-ladder-tick", type=float, default=0.01,
                   help="Price step used when advancing a ladder order")
    p.add_argument("--momentum-ladder-post-only-retry-ms", type=float, default=100.0,
                   help="Wait after a post-only rejection before resubmitting the same target price")
    p.add_argument("--momentum-ladder-shares-to-open", type=float, default=-1.0,
                   help="Lower-ladder fixed shares per fresh cycle; -1 inherits normal sizing and never changes legacy 0.99 size")
    p.add_argument("--momentum-ladder-max-placement-attempts", type=int, default=64,
                   help="Maximum send attempts for one lower-rung placement, including post-only rejections (1..64)")
    p.add_argument("--momentum-ladder-reprice-cancel-delay-ms", type=float, default=-1.0,
                   help="Cancel delay before moving an unfilled remainder higher; -1 reuses order-open delay")
    p.add_argument("--momentum-ladder-stop-drop-ticks", type=int, default=1,
                   help="Stop same-price post-only retries after best bid falls this many ticks below the ladder anchor")
    p.add_argument("--momentum-ladder-continue-after-full-fill", action=argparse.BooleanOptionalAction, default=False,
                   help="After a full fill, allow a fresh full-size order at the next ladder level")
    p.add_argument("--momentum-ladder-max-orders-per-market", type=int, default=8,
                   help="Safety bound on successfully opened ladder orders per market")
    p.add_argument("--momentum-ladder-max-deals-per-market", type=int, default=-1,
                   help="Maximum fresh ladder allocation cycles per market; reprices stay in the same deal; -1 unlimited")
    p.add_argument("--momentum-ladder-reprice-enabled", action=argparse.BooleanOptionalAction, default=True,
                   help="Allow a live ladder order to reprice higher on a later momentum signal; disable for sticky-order tests")
    p.add_argument("--momentum-ladder-write-event-audit", action=argparse.BooleanOptionalAction, default=False,
                   help="Persist extra ladder decision diagnostics; disabled by default for speed")
    p.add_argument("--serialized-fixed-arms-enabled", action=argparse.BooleanOptionalAction, default=False,
                   help="Enable native serialized fixed-price arms using the original single-limit Q99 engine")
    p.add_argument("--serialized-fixed-arm-prices", default="0.97,0.98,0.99",
                   help="Ascending comma-separated fixed arm limits")
    p.add_argument("--serialized-fixed-arm-shares", default="50,50,50",
                   help="Comma-separated requested shares aligned with SERIALIZED_FIXED_ARM_PRICES")
    p.add_argument("--serialized-fixed-arm-momentum-mins", default="",
                   help="Optional comma-separated per-arm exact-trigger BTC momentum floors aligned with prices; empty inherits BID99_BINANCE_MOMENTUM_MIN_USD")
    p.add_argument("--serialized-fixed-arm-early-momentum-mins", default="",
                   help="Optional comma-separated per-arm early-trigger BTC momentum floors; empty inherits each arm exact floor")
    p.add_argument("--serialized-fixed-arm-early-offset", type=float, default=0.01,
                   help="Each arm L uses early trigger floor L-offset")
    p.add_argument("--serialized-fixed-arm-ack-ms", type=float, default=50.0,
                   help="Broker-slot acknowledgement delay after attempt/order release")
    p.add_argument("--serialized-fixed-arm-suppress-competing", action=argparse.BooleanOptionalAction, default=True,
                   help="If another arm attempts while the slot is busy, suppress that arm for the rest of the contract")
    p.add_argument("--serialized-fixed-arm-sizing-mode", choices=["fixed","btc_move"], default="fixed",
                   help="fixed uses SERIALIZED_FIXED_ARM_SHARES; btc_move uses the causal BTC-distance sizing formula")
    p.add_argument("--serialized-fixed-arm-btc-move-divisors", default="",
                   help="Optional comma-separated BTC sizing divisor per arm; empty inherits BTC_MOVE_DIVISOR_USD")
    p.add_argument("--serialized-fixed-arm-busy-policy", choices=["block","upshift_cancel_lower"], default="block",
                   help="block waits for the live arm; upshift_cancel_lower cancels a lower live arm when a higher arm becomes send-eligible")
    p.add_argument("--serialized-fixed-arm-upshift-cancel-delay-ms", type=float, default=0.0,
                   help="Cancel request-to-effective delay used by upshift_cancel_lower")
    p.add_argument("--serialized-fixed-arm-upshift-min-replacement-price", type=float, default=-1.0,
                   help="Only replacement arms at or above this price may upshift-cancel; negative disables the guard")
    p.add_argument("--serialized-fixed-arm-upshift-max-owner-price", type=float, default=-1.0,
                   help="Only live lower arms at or below this price may be evicted; negative disables the guard")
    p.add_argument("--serialized-fixed-arm-upshift-require-unfilled-lower", action=argparse.BooleanOptionalAction, default=False,
                   help="Only upshift-cancel a lower live order if it has zero causally observed fill before the request")
    p.add_argument("--serialized-fixed-arm-retry-blocked-signals", action=argparse.BooleanOptionalAction, default=False,
                   help="Keep later same-outcome eligible arm candidates after the first shadow open so serializer-blocked pending intents can retry until one really opens")
    p.add_argument("--serialized-fixed-arm-retry-supersede-lower-on-higher-signal", action=argparse.BooleanOptionalAction, default=False,
                   help="With blocked-signal retry enabled, retire lower-price continuation retries after a higher arm first signals for the same outcome")
    p.add_argument("--serialized-fixed-arm-slot-claim-mode", choices=["actual_send","report_compatible"], default="actual_send",
                   help="actual_send claims the slot only for venue send attempts; report_compatible reproduces the supplied offline serializer")
    p.add_argument("--serialized-fixed-arm-momentum-apply-to-exact", action=argparse.BooleanOptionalAction, default=True,
                   help="Apply Binance momentum to each arm's exact-limit trigger as in the supplied serialized study")
    p.add_argument("--serialized-fixed-arm-momentum-apply-exact-flags", default="",
                   help="Optional comma-separated per-arm exact-trigger momentum booleans; empty inherits SERIALIZED_FIXED_ARM_MOMENTUM_APPLY_TO_EXACT")
    p.add_argument("--serialized-fixed-arm-require-report-eligibility", action=argparse.BooleanOptionalAction, default=False,
                   help="Fail configuration validation unless settings match the supplied BTC5 V1 report assumptions")
    p.add_argument("--serialized-fixed-arm-include-suppressed-audit", action=argparse.BooleanOptionalAction, default=True,
                   help="Retain zero-execution rows for blocked/suppressed shadow-arm attempts in queue99_orders.csv")
    p.add_argument("--wait-after-bid99-sec", type=float, default=0.0)
    p.add_argument("--paper-signal-snapshot-delay-ms", type=float, default=0.0)
    p.add_argument("--paper-send-start-delay-ms", type=float, default=12.5)
    p.add_argument("--paper-shadow-order-open-delay-ms", type=float, default=0.0)
    p.add_argument("--limit-order-lifetime-sec", type=float, default=-1.0)
    p.add_argument("--limit-order-lifetime-cancel-delay-ms", type=float, default=-1.0)
    p.add_argument(
        "--cancel-if-binance-mid-below-trigger",
        "--cancel-if-binance-mid-adverse-to-outcome",
        dest="cancel_if_binance_mid_below_trigger",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Outcome-aware adverse Binance cancellation. UP orders cancel below the saved "
            "trigger mid; DOWN orders cancel above it. The old option name is retained as an alias."
        ),
    )
    p.add_argument(
        "--binance-mid-below-trigger-cancel-delay-ms",
        "--binance-adverse-cancel-delay-ms",
        dest="binance_mid_below_trigger_cancel_delay_ms",
        type=float,
        default=-1.0,
        help="Outcome-aware cancel request-to-effective delay; -1 reuses PAPER_SHADOW_ORDER_OPEN_DELAY_MS.",
    )
    p.add_argument("--max-live-book-age-ms", type=float, default=1000.0)
    p.add_argument("--depth-lookback-ms", type=float, default=1000.0)
    p.add_argument(
        "--strict-causal-audit",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Hard-fail if any feature/input timestamp is after its decision timestamp; emits ns audit fields.",
    )
    p.add_argument(
        "--require-canonical-cache-schema",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Legacy compatibility switch; CACHE_SCHEMA.json is optional and the fixed built-in 1e6/1e6 schema is always used.",
    )
    p.add_argument(
        "--post-only-order", "--post-only",
        dest="post_only_order",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "At the delayed placement timestamp reject the attempted limit order when best ask <= limit. "
            "A later ask cross is allowed because the order was already resting."
        ),
    )
    p.add_argument("--entry-distance-min-usd", type=float, default=-1.0)
    p.add_argument("--entry-momentum-min-usd", type=float, default=-1.0)
    p.add_argument("--queue-size-multiplier", type=float, default=1.0)
    p.add_argument(
        "--queue-ahead-reconstruction-mode",
        choices=("off", "post_order_first"),
        default="off",
        help=(
            "Causally reduce queue ahead when the exact-limit displayed level shrinks without "
            "queue-consuming trades. post_order_first assigns post-open growth behind our order "
            "and removes that volume before reducing the queue ahead."
        ),
    )
    p.add_argument("--cancel-if-queue-ahead-above-shares", type=float, default=-1.0)
    p.add_argument("--queue-cancel-delay-ms", type=float, default=-1.0)
    p.add_argument("--cancel-if-bid99-unexplained-drop-shares", type=float, default=-1.0)
    p.add_argument("--bid99-unexplained-drop-cancel-delay-ms", type=float, default=-1.0)
    p.add_argument("--write-bid99-volume-timeline", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument(
        "--bid99-volume-output-interval-ms",
        type=float,
        default=5000.0,
        help=(
            "Persist at most one ordinary bid99 audit summary per interval per order. "
            "Cancellation calculations still use every raw event."
        ),
    )
    p.add_argument(
        "--bid99-volume-max-mib",
        type=float,
        default=200.0,
        help="Hard compressed size cap for queue99_bid99_volume_events.csv.gz (maximum 200 MiB).",
    )
    p.add_argument(
        "--bid99-volume-preview-rows",
        type=int,
        default=5000,
        help="Maximum uncompressed preview rows written for quick inspection.",
    )
    p.add_argument("--fill-remaining-on-ask99-appear", action=argparse.BooleanOptionalAction, default=False)
    p.add_argument("--fill-remaining-on-trade-below-limit", action=argparse.BooleanOptionalAction, default=False)
    p.add_argument(
        "--price-priority-fill-delay-ms",
        type=float,
        default=50.0,
        help=(
            "Delay from ASK<=limit or trade<limit evidence to the inferred full fill. "
            "A cancellation effective at or before the delayed fill blocks it."
        ),
    )
    p.add_argument("--trade-clock-mode", choices=["venue", "venue_with_local_guard", "local"], default="venue_with_local_guard")
    p.add_argument("--allow-local-trade-clock-fallback", action=argparse.BooleanOptionalAction, default=False)
    p.add_argument("--allow-all-trades-when-side-missing", action=argparse.BooleanOptionalAction, default=False)
    p.add_argument("--include-same-timestamp-trades", action=argparse.BooleanOptionalAction, default=False)
    p.add_argument("--fee-rate", type=float, default=0.0)
    p.add_argument("--fee-exponent", type=float, default=1.0)
    p.add_argument("--starting-balance", type=float, default=10000.0)
    p.add_argument("--enforce-balance", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--require-resolution", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--resolution-path", default="", help="Explicit cached settlement winner sidecar file or directory")
    p.add_argument("--allow-btc-settlement", "--allow-binance-settlement", dest="allow_btc_settlement", action=argparse.BooleanOptionalAction, default=False)
    p.add_argument("--source-gap-policy", choices=["exclude", "fail"], default="exclude")
    p.add_argument("--max-source-gap-market-fraction", type=float, default=0.03)
    p.add_argument("--recover-alternate-sessions", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--workers", type=int, default=128)
    p.add_argument("--pool-chunksize", type=int, default=1)
    p.add_argument("--limit-markets", type=int, default=0)
    p.add_argument("--sample-markets", type=int, default=0)
    p.add_argument("--preflight-sample-markets", type=int, default=12)
    p.add_argument("--preflight-workers", type=int, default=12)
    p.add_argument("--preflight-mode", choices=["parallel_full", "serial_full", "off"], default="parallel_full")
    p.add_argument("--fail-on-market-error", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--write-market-charts", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--market-charts-per-bucket", type=int, default=10)
    p.add_argument("--market-chart-dpi", type=int, default=300)
    p.add_argument("--market-chart-width-in", type=float, default=20.0)
    p.add_argument("--market-chart-height-in", type=float, default=12.0)
    p.add_argument("--market-chart-max-points", type=int, default=0)
    p.add_argument("--market-chart-save-data", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--market-chart-png-compress-level", type=int, default=1)
    p.add_argument("--market-chart-fetch-polymarket-start-end", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--market-chart-boundary-workers", type=int, default=30)
    p.add_argument("--market-chart-boundary-request-timeout-sec", type=float, default=3.0)
    p.add_argument("--market-chart-boundary-total-timeout-sec", type=float, default=12.0)
    p.add_argument("--market-chart-boundary-retries", type=int, default=1)
    p.add_argument("--fail-on-market-chart-error", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--html-report-max-mib", type=float, default=20.0)
    p.add_argument("--html-report-order-rows", type=int, default=300)
    p.add_argument("--html-report-equity-max-points", type=int, default=5000)
    p.add_argument("--html-report-cell-max-chars", type=int, default=160)
    p.add_argument("--progress-interval-sec", type=float, default=10.0)
    p.add_argument("--validate-config-only", action="store_true")
    return p


def _normalize_resolution_request(value: Any) -> tuple[str, str]:
    """Return (effective_initial_path, exact_user_request).

    AUTO/DEFAULT/empty deliberately leave discovery to the asset/timeframe-aware
    local resolver.  No network source is involved.
    """
    requested = str(value or "").strip()
    if requested.upper() in {"", "AUTO", "DEFAULT", "NONE", "-"}:
        return "", requested
    return str(Path(requested).expanduser().resolve()), requested


def _apply_resolution_selection(cfg: Config, resolution_meta: dict[str, Any]) -> None:
    """Promote the resolver-selected sidecar into the effective configuration."""
    selected = str(resolution_meta.get("path") or "").strip()
    cfg.resolution_path_selected = selected
    requested = str(cfg.resolution_path_requested or cfg.resolution_path or "").strip()
    initial_effective = str(cfg.resolution_path or "").strip()
    corrected = bool(
        selected
        and initial_effective
        and not q.resolution_paths_equivalent(initial_effective, selected)
    )
    cfg.resolution_path_auto_corrected = corrected
    cfg.resolution_path_auto_correction_reason = ""
    if corrected:
        requested_key = q.resolution_market_key_from_path(requested)
        if requested_key and requested_key != cfg.market_key:
            cfg.resolution_path_auto_correction_reason = (
                f"requested_resolution_declares_{requested_key}_but_market_key_is_{cfg.market_key}; "
                "selected_matching_local_sidecar"
            )
        else:
            cfg.resolution_path_auto_correction_reason = (
                "asset_timeframe_matching_local_sidecar_with_better_selected_market_coverage"
            )
    if selected:
        cfg.resolution_path = selected


def config_from_args(ns: argparse.Namespace) -> Config:
    sizing = normalize_sizing_mode(ns.share_sizing_mode)
    market_key = q.normalize_market_key(ns.market_key)
    resolution_path, resolution_path_requested = _normalize_resolution_request(ns.resolution_path)
    raw_symbol = str(ns.btc_symbol or "AUTO").strip().upper()
    resolved_symbol = q.default_binance_symbol(market_key) if raw_symbol in {"", "AUTO", "DEFAULT"} else raw_symbol
    cfg = Config(
        data_dir=str(Path(ns.data_dir).expanduser().resolve()),
        outdir=str(Path(ns.outdir).expanduser().resolve()),
        start_ms=parse_utc_ms(ns.start_utc),
        end_ms=parse_utc_ms(ns.end_utc),
        market_key=market_key,
        yes_outcome=ns.yes_outcome,
        trigger_bid=ns.trigger_bid,
        trigger_tolerance=ns.trigger_tolerance,
        order_trigger_policy=ns.order_trigger_policy,
        entry_retry_policy=ns.entry_retry_policy,
        post_only_cross_retry_delay_ms=ns.post_only_cross_retry_delay_ms,
        entry_distance_min_usd=getattr(ns, "entry_distance_min_usd", -1.0),
        entry_momentum_min_usd=getattr(ns, "entry_momentum_min_usd", -1.0),
        entry_market_age_start_sec=ns.entry_market_age_start_sec,
        entry_market_age_end_sec=ns.entry_market_age_end_sec,
        share_sizing_mode=sizing,
        btc_price_path=ns.btc_price_path,
        btc_symbol=resolved_symbol,
        btc_mid_max_age_ms=ns.btc_mid_max_age_ms,
        binance_venue_to_vps_delay_ms=ns.binance_venue_to_vps_delay_ms,
        polymarket_timestamp_clock=ns.polymarket_timestamp_clock,
        polymarket_feed_delay_ms=ns.polymarket_feed_delay_ms,
        btc_load_workers=ns.btc_load_workers,
        btc_prune_by_time_stats=ns.btc_prune_by_time_stats,
        btc_load_log_every_files=ns.btc_load_log_every_files,
        btc_index_cache_mode=ns.btc_index_cache_mode,
        btc_index_cache_dir=(str(Path(ns.btc_index_cache_dir).expanduser().resolve()) if str(ns.btc_index_cache_dir).strip() else ""),
        btc_index_cache_source_tag=ns.btc_index_cache_source_tag,
        btc_worker_index_mode=ns.btc_worker_index_mode,
        btc_worker_mmap_root=(str(Path(ns.btc_worker_mmap_root).expanduser().resolve()) if str(ns.btc_worker_mmap_root).strip() else ""),
        btc_move_divisor_usd=ns.btc_move_divisor_usd,
        btc_shares_multiplier=ns.btc_shares_multiplier,
        min_shares_to_open=ns.min_shares_to_open,
        max_shares_to_open=ns.max_shares_to_open,
        fixed_shares_to_open=ns.fixed_shares_to_open,
        min_btc_move_usd=ns.min_btc_move_usd,
        bid99_binance_momentum_min_usd=ns.bid99_binance_momentum_min_usd,
        bid99_binance_momentum_early_min_usd=ns.bid99_binance_momentum_early_min_usd,
        bid99_binance_momentum_lookback_sec=ns.bid99_binance_momentum_lookback_sec,
        bid99_binance_momentum_apply_to_99=ns.bid99_binance_momentum_apply_to_99,
        bid99_binance_momentum_early_trigger_bid=ns.bid99_binance_momentum_early_trigger_bid,
        momentum_early_cross_match_delay_ms=ns.momentum_early_cross_match_delay_ms,
        momentum_ladder_enabled=ns.momentum_ladder_enabled,
        momentum_ladder_preserve_legacy_99=ns.momentum_ladder_preserve_legacy_99,
        momentum_ladder_make_before_break=ns.momentum_ladder_make_before_break,
        momentum_ladder_activation_source=ns.momentum_ladder_activation_source,
        momentum_ladder_start_bid=ns.momentum_ladder_start_bid,
        momentum_ladder_max_bid=ns.momentum_ladder_max_bid,
        momentum_ladder_tick=ns.momentum_ladder_tick,
        momentum_ladder_post_only_retry_ms=ns.momentum_ladder_post_only_retry_ms,
        momentum_ladder_shares_to_open=ns.momentum_ladder_shares_to_open,
        momentum_ladder_max_placement_attempts=ns.momentum_ladder_max_placement_attempts,
        momentum_ladder_reprice_cancel_delay_ms=ns.momentum_ladder_reprice_cancel_delay_ms,
        momentum_ladder_stop_drop_ticks=ns.momentum_ladder_stop_drop_ticks,
        momentum_ladder_continue_after_full_fill=ns.momentum_ladder_continue_after_full_fill,
        momentum_ladder_max_orders_per_market=ns.momentum_ladder_max_orders_per_market,
        momentum_ladder_max_deals_per_market=ns.momentum_ladder_max_deals_per_market,
        momentum_ladder_reprice_enabled=ns.momentum_ladder_reprice_enabled,
        momentum_ladder_write_event_audit=ns.momentum_ladder_write_event_audit,
        serialized_fixed_arms_enabled=ns.serialized_fixed_arms_enabled,
        serialized_fixed_arm_prices=ns.serialized_fixed_arm_prices,
        serialized_fixed_arm_shares=ns.serialized_fixed_arm_shares,
        serialized_fixed_arm_momentum_mins=ns.serialized_fixed_arm_momentum_mins,
        serialized_fixed_arm_early_momentum_mins=ns.serialized_fixed_arm_early_momentum_mins,
        serialized_fixed_arm_early_offset=ns.serialized_fixed_arm_early_offset,
        serialized_fixed_arm_ack_ms=ns.serialized_fixed_arm_ack_ms,
        serialized_fixed_arm_suppress_competing=ns.serialized_fixed_arm_suppress_competing,
        serialized_fixed_arm_sizing_mode=ns.serialized_fixed_arm_sizing_mode,
        serialized_fixed_arm_btc_move_divisors=ns.serialized_fixed_arm_btc_move_divisors,
        serialized_fixed_arm_busy_policy=ns.serialized_fixed_arm_busy_policy,
        serialized_fixed_arm_upshift_cancel_delay_ms=ns.serialized_fixed_arm_upshift_cancel_delay_ms,
        serialized_fixed_arm_upshift_min_replacement_price=ns.serialized_fixed_arm_upshift_min_replacement_price,
        serialized_fixed_arm_upshift_max_owner_price=ns.serialized_fixed_arm_upshift_max_owner_price,
        serialized_fixed_arm_upshift_require_unfilled_lower=ns.serialized_fixed_arm_upshift_require_unfilled_lower,
        serialized_fixed_arm_retry_blocked_signals=ns.serialized_fixed_arm_retry_blocked_signals,
        serialized_fixed_arm_retry_supersede_lower_on_higher_signal=ns.serialized_fixed_arm_retry_supersede_lower_on_higher_signal,
        serialized_fixed_arm_slot_claim_mode=ns.serialized_fixed_arm_slot_claim_mode,
        serialized_fixed_arm_momentum_apply_to_exact=ns.serialized_fixed_arm_momentum_apply_to_exact,
        serialized_fixed_arm_momentum_apply_exact_flags=ns.serialized_fixed_arm_momentum_apply_exact_flags,
        serialized_fixed_arm_require_report_eligibility=ns.serialized_fixed_arm_require_report_eligibility,
        serialized_fixed_arm_include_suppressed_audit=ns.serialized_fixed_arm_include_suppressed_audit,
        wait_after_bid99_sec=ns.wait_after_bid99_sec,
        paper_signal_snapshot_delay_ms=ns.paper_signal_snapshot_delay_ms,
        paper_send_start_delay_ms=ns.paper_send_start_delay_ms,
        paper_shadow_order_open_delay_ms=ns.paper_shadow_order_open_delay_ms,
        limit_order_lifetime_sec=ns.limit_order_lifetime_sec,
        limit_order_lifetime_cancel_delay_ms=ns.limit_order_lifetime_cancel_delay_ms,
        cancel_if_binance_mid_below_trigger=ns.cancel_if_binance_mid_below_trigger,
        binance_mid_below_trigger_cancel_delay_ms=ns.binance_mid_below_trigger_cancel_delay_ms,
        max_live_book_age_ms=ns.max_live_book_age_ms,
        depth_lookback_ms=ns.depth_lookback_ms,
        strict_causal_audit=ns.strict_causal_audit,
        require_canonical_cache_schema=ns.require_canonical_cache_schema,
        post_only=ns.post_only_order,
        post_only_order=ns.post_only_order,
        queue_size_multiplier=ns.queue_size_multiplier,
        queue_ahead_reconstruction_mode=ns.queue_ahead_reconstruction_mode,
        cancel_if_queue_ahead_above_shares=ns.cancel_if_queue_ahead_above_shares,
        queue_cancel_delay_ms=ns.queue_cancel_delay_ms,
        cancel_if_bid99_unexplained_drop_shares=ns.cancel_if_bid99_unexplained_drop_shares,
        bid99_unexplained_drop_cancel_delay_ms=ns.bid99_unexplained_drop_cancel_delay_ms,
        write_bid99_volume_timeline=ns.write_bid99_volume_timeline,
        bid99_volume_output_interval_ms=ns.bid99_volume_output_interval_ms,
        bid99_volume_max_mib=ns.bid99_volume_max_mib,
        bid99_volume_preview_rows=ns.bid99_volume_preview_rows,
        fill_remaining_on_ask99_appear=ns.fill_remaining_on_ask99_appear,
        fill_remaining_on_trade_below_limit=ns.fill_remaining_on_trade_below_limit,
        price_priority_fill_delay_ms=ns.price_priority_fill_delay_ms,
        trade_clock_mode=ns.trade_clock_mode,
        allow_local_trade_clock_fallback=ns.allow_local_trade_clock_fallback,
        allow_all_trades_when_side_missing=ns.allow_all_trades_when_side_missing,
        include_same_timestamp_trades=ns.include_same_timestamp_trades,
        fee_rate=ns.fee_rate,
        fee_exponent=ns.fee_exponent,
        starting_balance=ns.starting_balance,
        enforce_balance=ns.enforce_balance,
        require_resolution=ns.require_resolution,
        resolution_path=resolution_path,
        resolution_path_requested=resolution_path_requested,
        allow_btc_settlement=ns.allow_btc_settlement,
        source_gap_policy=ns.source_gap_policy,
        max_source_gap_market_fraction=ns.max_source_gap_market_fraction,
        recover_alternate_sessions=ns.recover_alternate_sessions,
        workers=ns.workers,
        pool_chunksize=ns.pool_chunksize,
        limit_markets=ns.limit_markets,
        sample_markets=ns.sample_markets,
        preflight_sample_markets=ns.preflight_sample_markets,
        preflight_workers=ns.preflight_workers,
        preflight_mode=ns.preflight_mode,
        fail_on_market_error=ns.fail_on_market_error,
        write_market_charts=ns.write_market_charts,
        market_charts_per_bucket=ns.market_charts_per_bucket,
        market_chart_dpi=ns.market_chart_dpi,
        market_chart_width_in=ns.market_chart_width_in,
        market_chart_height_in=ns.market_chart_height_in,
        market_chart_max_points=ns.market_chart_max_points,
        market_chart_save_data=ns.market_chart_save_data,
        market_chart_png_compress_level=ns.market_chart_png_compress_level,
        market_chart_fetch_polymarket_start_end=ns.market_chart_fetch_polymarket_start_end,
        market_chart_boundary_workers=ns.market_chart_boundary_workers,
        market_chart_boundary_request_timeout_sec=ns.market_chart_boundary_request_timeout_sec,
        market_chart_boundary_total_timeout_sec=ns.market_chart_boundary_total_timeout_sec,
        market_chart_boundary_retries=ns.market_chart_boundary_retries,
        fail_on_market_chart_error=ns.fail_on_market_chart_error,
        html_report_max_mib=ns.html_report_max_mib,
        html_report_order_rows=ns.html_report_order_rows,
        html_report_equity_max_points=ns.html_report_equity_max_points,
        html_report_cell_max_chars=ns.html_report_cell_max_chars,
        progress_interval_sec=ns.progress_interval_sec,
    )
    cfg.validate()
    return cfg


def fee_for_shares(shares: float, price: float, cfg: Config) -> float:
    if shares <= 0 or price <= 0 or price >= 1 or cfg.fee_rate <= 0:
        return 0.0
    return float(shares) * float(cfg.fee_rate) * ((float(price) * (1.0 - float(price))) ** float(cfg.fee_exponent))


def _row_time_ns(row: dict[str, Any], ns_key: str, ms_key: str, default: int = np.iinfo(np.int64).max) -> int:
    value = row.get(ns_key)
    try:
        if value is not None and not pd.isna(value):
            return int(value)
    except Exception:
        pass
    ms = q.as_float(row.get(ms_key), math.nan)
    return q.ms_to_ns(ms) if math.isfinite(ms) else int(default)


def _delay_ns(ms: float) -> int:
    return int(round(float(ms) * 1_000_000.0))


def _normalize_causal_frame_ns(frame: pd.DataFrame | None) -> pd.DataFrame:
    """Normalize explicitly named report-millisecond columns to int64 ns once.

    Production loaders already provide authoritative nanoseconds.  The ms
    fallbacks exist only for older synthetic/unit-test callers and are never
    selected by inspecting values.  After this boundary conversion all causal
    code orders and compares integer nanoseconds.
    """
    if frame is None:
        return pd.DataFrame()
    if bool(getattr(frame, "attrs", {}).get("_queue99_causal_ns_normalized", False)):
        return frame
    out = frame.copy()
    pairs = (("ts_ns","ts_ms"),("clock_ns","clock_ms"),("local_ts_ns","local_ts_ms"),("venue_ts_ns","venue_ts_ms"))
    for ns_col, ms_col in pairs:
        if ns_col not in out.columns and ms_col in out.columns:
            vals = pd.to_numeric(out[ms_col], errors="coerce")
            out[ns_col] = q.timestamp_to_ns(vals, ms_col)
        elif ns_col in out.columns:
            out[ns_col] = pd.to_numeric(out[ns_col], errors="coerce").fillna(-1).astype("int64")
    for col in ("seq","state_id"):
        if col not in out.columns:
            out[col] = 0
        else:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0).astype("int64")
    out.attrs["_queue99_causal_ns_normalized"] = True
    return out


def _causal_audit_fields(*, decision_ns: int, sources: dict[str, int | None], strict_prior: set[str] | None = None, enabled: bool = False) -> dict[str, Any]:
    clean = {name: int(ts) for name, ts in sources.items() if ts is not None and int(ts) >= 0}
    maximum = max(clean.values(), default=-1)
    if enabled:
        if maximum > int(decision_ns):
            raise InternalContractError(f"STRICT_CAUSAL_AUDIT future input: decision_ns={decision_ns} maximum_input_ns={maximum} sources={clean}")
        for name in strict_prior or set():
            if name in clean and clean[name] >= int(decision_ns):
                raise InternalContractError(f"STRICT_CAUSAL_AUDIT strict-prior violation: feature={name} feature_ns={clean[name]} decision_ns={decision_ns}")
    out={"signal_ts_ns":int(decision_ns),"maximum_input_ts_ns":int(maximum)}
    for name,ts in clean.items(): out[f"{name}_ts_ns"]=int(ts)
    return out


def _worker_init(cfg: dict[str, Any], token_map: dict[str, str], resolutions: dict[int, str], mmap_dir: str | None) -> None:
    global _WORKER_CFG, _WORKER_TOKEN_MAP, _WORKER_RESOLUTIONS, _WORKER_UNDERLYING
    _WORKER_CFG = cfg
    _WORKER_TOKEN_MAP = token_map
    _WORKER_RESOLUTIONS = {int(k): v for k, v in resolutions.items()}
    _WORKER_UNDERLYING = None
    if mmap_dir:
        root = Path(mmap_dir)
        times_ns = np.load(root / "times_ns.npy", mmap_mode="r")
        prices = np.load(root / "prices.npy", mmap_mode="r")
        meta = json.loads((root / "metadata.json").read_text(encoding="utf-8"))
        # times_ns.npy is itself an explicit unit contract. Older generated
        # synthetic indexes may omit timestamp_unit in metadata; accept that
        # without inspecting values and normalize the metadata in memory.
        unit = str(meta.get("timestamp_unit", "") or "nanoseconds")
        if unit != "nanoseconds":
            raise RuntimeError(f"underlying mmap index is not nanosecond schema: {unit!r}")
        meta["timestamp_unit"] = "nanoseconds"
        _WORKER_UNDERLYING = q.UnderlyingMidSeries(None, prices, meta["source"], meta["path"], meta, times_ns=times_ns)


def source_gap_result(task: q.ContractTask, reason: str, stage: str, session_attempts: list[dict[str, Any]], details: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "ok": True,
        "source_gap": True,
        "source_gap_reason": reason,
        "source_gap_stage": stage,
        "market": {
            "contract_start_ms": task.contract_start_ms,
            "market_start_utc": q.market_iso(task.contract_start_ms),
            "status": f"source_gap_{reason}",
            "simulation_valid": False,
            "source_session_id": task.session_id,
            "bbo_dir": task.bbo_dir,
            "source_session_attempts": json.dumps(session_attempts, separators=(",", ":"), default=str),
            "source_gap_details": json.dumps(details or {}, separators=(",", ":"), default=str),
        },
        "orders": [], "fills": [], "queue_trades": [], "bid99_volume_events": [],
    }



def _side_depth_after(depth: pd.DataFrame, *, side: str, open_ns: int) -> pd.DataFrame:
    """Return immutable same-side depth rows after open, cached per market."""
    if depth is None or depth.empty:
        return pd.DataFrame(columns=list(depth.columns) if isinstance(depth, pd.DataFrame) else [])
    cache = frame_cache(depth, "_queue99_side_depth_after_cache")
    key = (str(side), int(open_ns))
    cached = cache.get(key)
    if cached is not None:
        return cached
    ts = pd.to_numeric(depth["ts_ns"], errors="raise").to_numpy(dtype=np.int64, copy=False)
    sides = depth["side"].astype(str).to_numpy(copy=False)
    mask = (sides == str(side)) & (ts > int(open_ns))
    out = depth.loc[mask].copy()
    out.attrs["_queue99_causal_ns_normalized"] = bool(depth.attrs.get("_queue99_causal_ns_normalized", False))
    cache[key] = out
    return out



def _causal_order_trades(
    trades: pd.DataFrame, *, side: str, cfg: Config,
    open_ns: int | None = None, end_ns: int | None = None,
    open_ms: float | None = None, end_ms: float | None = None,
) -> pd.DataFrame:
    """Return causal same-outcome trades using authoritative int64 ns clocks."""
    if trades is None or trades.empty:
        return pd.DataFrame(columns=list(trades.columns) if isinstance(trades, pd.DataFrame) else [])
    if open_ns is None:
        if open_ms is None: raise ValueError("open_ns is required")
        open_ns=q.ms_to_ns(open_ms)
    if end_ns is None:
        if end_ms is None: raise ValueError("end_ns is required")
        end_ns=q.ms_to_ns(end_ms)
    open_ns,end_ns=int(open_ns),int(end_ns)
    cache = frame_cache(trades, "_queue99_causal_order_trades_cache")
    cache_key = (
        str(side), open_ns, end_ns, round(float(cfg.trigger_bid), 12),
        round(float(cfg.trigger_tolerance), 12), bool(cfg.include_same_timestamp_trades),
        str(cfg.trade_clock_mode),
    )
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    price = pd.to_numeric(trades.get("price"), errors="coerce")
    selected = trades[(trades["outcome"] == side) & (price <= cfg.trigger_bid + cfg.trigger_tolerance + 1e-12)].copy()
    if selected.empty: return selected
    clock_ns = pd.to_numeric(selected["clock_ns"], errors="coerce").astype("int64")
    selected = selected[clock_ns >= open_ns] if cfg.include_same_timestamp_trades else selected[clock_ns > open_ns]
    selected = selected[pd.to_numeric(selected["clock_ns"], errors="coerce").astype("int64") < end_ns]
    if cfg.trade_clock_mode == "venue_with_local_guard":
        local_ns = pd.to_numeric(selected["local_ts_ns"], errors="coerce").astype("int64")
        selected = selected[local_ns >= open_ns] if cfg.include_same_timestamp_trades else selected[local_ns > open_ns]
        selected = selected[pd.to_numeric(selected["local_ts_ns"], errors="coerce").astype("int64") < end_ns]
    if not selected.empty:
        selected.sort_values(["clock_ns","local_ts_ns","seq","state_id"], inplace=True, kind="stable")
    selected.attrs["_queue99_causal_ns_normalized"] = True
    cache[cache_key] = selected
    return selected

def _queue_consuming_exact99_trades(
    causal_trades: pd.DataFrame,
    *,
    cfg: Config,
    order_id: str,
) -> pd.DataFrame:
    """Return validated exact-limit SELL prints that consume FIFO queue ahead."""
    if causal_trades is None or causal_trades.empty:
        return pd.DataFrame(columns=list(causal_trades.columns) if isinstance(causal_trades, pd.DataFrame) else [])
    cache = frame_cache(causal_trades, "_queue99_exact_queue_trades_cache")
    cache_key = (
        round(float(cfg.trigger_bid), 12), round(float(cfg.trigger_tolerance), 12),
        bool(cfg.allow_all_trades_when_side_missing),
    )
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    price = pd.to_numeric(causal_trades.get("price"), errors="coerce")
    exact = causal_trades[
        np.abs(price - cfg.trigger_bid) <= cfg.trigger_tolerance + 1e-12
    ].copy()
    if exact.empty:
        return exact
    if bool(exact["side_conflict"].any()):
        raise RuntimeError(f"conflicting trade-side columns for relevant exact-limit trades: {order_id}")
    unresolved = exact[~exact["aggressor_side"].isin(["BUY", "SELL"])]
    if not unresolved.empty and not cfg.allow_all_trades_when_side_missing:
        raise RuntimeError(
            f"unresolved side for {len(unresolved)} relevant exact-limit trades: {order_id}"
        )
    eligible = exact[
        (exact["aggressor_side"] == "SELL")
        | (
            ~exact["aggressor_side"].isin(["BUY", "SELL"])
            & cfg.allow_all_trades_when_side_missing
        )
    ].copy()
    if not eligible.empty:
        eligible.sort_values(["clock_ns", "local_ts_ns", "seq", "state_id"], inplace=True, kind="stable")
    eligible.attrs["_queue99_causal_ns_normalized"] = True
    cache[cache_key] = eligible
    return eligible


def _build_bid99_volume_timeline(
    *,
    order_id: str,
    contract_start_ms: int,
    side: str,
    open_ns: int | None = None,
    open_ms: float | None = None,
    raw_queue: float = 0.0,
    side_depth: pd.DataFrame,
    exact_queue_trades: pd.DataFrame,
    cfg: Config,
    volume_source: str,
    scan_end_ns: int | None = None,
    scan_end_ms: float | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any] | None]:
    """Reconcile external bid-0.99 volume at full resolution.

    Strategy decisions always consume every raw depth and trade event.  The
    returned audit rows are a *compact interval ledger*: ordinary events are
    aggregated into configurable time buckets, while order-open, cancellation
    threshold, known/unknown transitions and final-summary rows are preserved.
    This keeps the simulation identical while preventing multi-gigabyte CSVs
    and enormous multiprocessing payloads.
    """
    if open_ns is None:
        if open_ms is None: raise ValueError("open_ns is required")
        open_ns=q.ms_to_ns(open_ms)
    if scan_end_ns is None:
        if scan_end_ms is None: raise ValueError("scan_end_ns is required")
        scan_end_ns=q.ms_to_ns(scan_end_ms)
    open_ns=int(open_ns); initial_scan_end_ns=int(scan_end_ns)
    open_ms=q.ns_to_ms(open_ns); initial_scan_end=q.ns_to_ms(initial_scan_end_ns)
    threshold = float(cfg.cancel_if_bid99_unexplained_drop_shares)
    output_enabled = bool(cfg.write_bid99_volume_timeline)
    need_scan = threshold >= 0 or output_enabled
    initial_queue = max(0.0, float(raw_queue))
    empty_stats = {
        "max_external_bid99_shares_seen": initial_queue,
        "total_sell_trade_shares_at_99": 0.0,
        "total_bought_shares_at_99": 0.0,
        "max_unexplained_drop_from_peak_shares": 0.0,
        "max_unexplained_drop_step_shares": 0.0,
        "cumulative_unexplained_drop_shares": 0.0,
        "last_external_bid99_shares": initial_queue,
        "bid99_volume_source": str(volume_source),
        "bid99_volume_raw_depth_events_scanned": 0,
        "bid99_volume_raw_trade_events_scanned": 0,
        "bid99_volume_output_rows": 0,
        "bid99_volume_output_interval_ms": (
            float(cfg.bid99_volume_output_interval_ms) if output_enabled else math.nan
        ),
    }
    # Hard disable: neither the cancellation model nor persisted audit needs the
    # event stream.  Do not inspect depth/trades and do not validate inactive
    # output controls.
    if not need_scan:
        return [], empty_stats, None

    side_depth=_normalize_causal_frame_ns(side_depth)
    exact_queue_trades=_normalize_causal_frame_ns(exact_queue_trades)
    events: list[dict[str, Any]] = []
    raw_depth_events = 0
    raw_trade_events = 0
    if side_depth is not None and not side_depth.empty:
        for row in side_depth.to_dict("records"):
            ts_ns=int(row.get("ts_ns",-1) or -1)
            if open_ns < ts_ns < initial_scan_end_ns:
                raw_depth_events += 1
                events.append({"kind":"depth","ts_ns":ts_ns,"priority":1,"row":row})

    if isinstance(exact_queue_trades, pd.DataFrame) and not exact_queue_trades.empty:
        price = pd.to_numeric(exact_queue_trades.get("price"), errors="coerce")
        exact = exact_queue_trades[
            np.abs(price - cfg.trigger_bid) <= cfg.trigger_tolerance + 1e-12
        ].copy()
        if not exact.empty:
            exact["_observation_ts_ns"] = pd.to_numeric(exact.get("local_ts_ns"), errors="coerce")
            missing_local = ~np.isfinite(exact["_observation_ts_ns"])
            exact.loc[missing_local, "_observation_ts_ns"] = pd.to_numeric(exact.loc[missing_local,"clock_ns"],errors="coerce")
            exact=exact[np.isfinite(exact["_observation_ts_ns"])]
            for row in exact.to_dict("records"):
                ts_ns=int(row.get("_observation_ts_ns",-1) or -1)
                if open_ns < ts_ns < initial_scan_end_ns:
                    raw_trade_events += 1
                    events.append({"kind":"trade","ts_ns":ts_ns,"priority":0,"row":row})

    events.sort(
        key=lambda item: (
            int(item["ts_ns"]),
            int(item["priority"]),
            q.as_int(item["row"].get("seq"), 0),
            q.as_int(item["row"].get("state_id"), 0),
        )
    )

    timeline: list[dict[str, Any]] = []
    peak = initial_queue
    current = peak
    previous_depth_size = current
    exact_sell_since_peak = 0.0
    exact_sell_since_previous_depth = 0.0
    total_sell_99 = 0.0
    cumulative_unexplained = 0.0
    max_peak_unexplained = 0.0
    max_step_unexplained = 0.0
    cancel_candidate: dict[str, Any] | None = None
    effective_scan_end_ns = initial_scan_end_ns
    interval_ns = max(1, _delay_ns(float(cfg.bid99_volume_output_interval_ms))) if output_enabled else 2**63-1
    bucket: dict[str, Any] | None = None
    last_known: bool | None = True

    def base_row(*, ts_ns: int, event_kind: str, priority: int) -> dict[str, Any]:
        ts_ms=q.ns_to_ms(ts_ns)
        peak_unexplained = max(0.0, peak - current - exact_sell_since_peak)
        return {
            "order_id": order_id,
            "contract_start_ms": int(contract_start_ms),
            "outcome": side,
            "ts_ns": int(ts_ns),
            "ts_ms": float(ts_ms),
            "event_kind": event_kind,
            "output_priority": int(priority),
            "bid99_volume_source": str(volume_source),
            "external_bid99_known": True,
            "external_bid99_shares": float(current),
            "external_bid99_min_shares": float(current),
            "external_bid99_max_shares": float(current),
            "max_external_bid99_shares_seen": float(peak),
            "exact99_sell_trade_shares_this_event": 0.0,
            "exact99_sell_trade_count_this_event": 0,
            "total_sell_trade_shares_at_99": float(total_sell_99),
            "total_bought_shares_at_99": float(total_sell_99),
            "observed_bid99_drop_shares": 0.0,
            "trade_explained_drop_shares": 0.0,
            "unexplained_drop_step_shares": 0.0,
            "max_unexplained_drop_step_shares": 0.0,
            "cumulative_unexplained_drop_shares": float(cumulative_unexplained),
            "unexplained_drop_from_peak_shares": float(peak_unexplained),
            "cancel_threshold_shares": float(threshold) if threshold >= 0 else math.nan,
            "cancel_threshold_crossed": False,
            "depth_event_count": 0,
            "trade_event_count": 0,
            "best_bid": math.nan,
            "best_bid_size": math.nan,
            "best_ask": math.nan,
            "best_ask_size": math.nan,
            "ask_crossed_limit": False,
        }

    def emit_direct(*, ts_ns: int, event_kind: str, priority: int, known: bool = True) -> None:
        if not output_enabled:
            return
        row = base_row(ts_ns=ts_ns, event_kind=event_kind, priority=priority)
        row["external_bid99_known"] = bool(known)
        if not known:
            row["external_bid99_shares"] = math.nan
            row["external_bid99_min_shares"] = math.nan
            row["external_bid99_max_shares"] = math.nan
        timeline.append(row)

    def flush_bucket(*, forced_kind: str | None = None, priority: int | None = None) -> None:
        nonlocal bucket
        if not output_enabled or bucket is None:
            bucket = None
            return
        known_values = bucket["known_values"]
        row = base_row(
            ts_ns=int(bucket["last_ts_ns"]),
            event_kind=str(forced_kind or bucket.get("event_kind") or "interval_summary"),
            priority=int(bucket["priority"] if priority is None else priority),
        )
        row.update({
            "interval_start_ts_ns": int(bucket["start_ts_ns"]),
            "interval_end_ts_ns": int(bucket["last_ts_ns"]),
            "interval_start_ts_ms": q.ns_to_ms(int(bucket["start_ts_ns"])),
            "interval_end_ts_ms": q.ns_to_ms(int(bucket["last_ts_ns"])),
            "external_bid99_known": bool(bucket["last_known"]),
            "external_bid99_shares": (
                float(bucket["last_external_bid99_shares"])
                if bucket["last_known"] else math.nan
            ),
            "external_bid99_min_shares": min(known_values) if known_values else math.nan,
            "external_bid99_max_shares": max(known_values) if known_values else math.nan,
            "max_external_bid99_shares_seen": float(bucket["max_peak"]),
            "exact99_sell_trade_shares_this_event": float(bucket["trade_shares"]),
            "exact99_sell_trade_count_this_event": int(bucket["trade_count"]),
            "total_sell_trade_shares_at_99": float(bucket["total_sell_99"]),
            "total_bought_shares_at_99": float(bucket["total_sell_99"]),
            "observed_bid99_drop_shares": float(bucket["observed_drop"]),
            "trade_explained_drop_shares": float(bucket["explained_drop"]),
            "unexplained_drop_step_shares": float(bucket["unexplained_drop"]),
            "max_unexplained_drop_step_shares": float(bucket["max_unexplained_step"]),
            "cumulative_unexplained_drop_shares": float(bucket["cumulative_unexplained"]),
            "unexplained_drop_from_peak_shares": float(bucket["max_peak_unexplained"]),
            "cancel_threshold_crossed": bool(bucket["threshold_crossed"]),
            "depth_event_count": int(bucket["depth_count"]),
            "trade_event_count": int(bucket["trade_count"]),
            "best_bid": bucket["best_bid"],
            "best_bid_size": bucket["best_bid_size"],
            "best_ask": bucket["best_ask"],
            "best_ask_size": bucket["best_ask_size"],
            "ask_crossed_limit": bool(bucket["ask_crossed_limit"]),
        })
        timeline.append(row)
        bucket = None

    def record_bucket(
        *,
        ts_ns: int,
        known: bool,
        trade_shares: float = 0.0,
        observed_drop: float = 0.0,
        explained_drop: float = 0.0,
        unexplained_step: float = 0.0,
        threshold_crossed: bool = False,
        best_bid: float = math.nan,
        best_bid_size: float = math.nan,
        best_ask: float = math.nan,
        best_ask_size: float = math.nan,
        ask_crossed_limit: bool = False,
        force_flush: bool = False,
        event_kind: str = "interval_summary",
        priority: int = 0,
    ) -> None:
        nonlocal bucket
        if not output_enabled:
            return
        bucket_index = max(0, int(ts_ns)-open_ns) // interval_ns
        bucket_start_ns = open_ns + bucket_index * interval_ns
        if bucket is not None and int(bucket["index"]) != bucket_index:
            flush_bucket()
        if bucket is None:
            bucket = {
                "index": bucket_index,
                "start_ts_ns": bucket_start_ns,
                "last_ts_ns": int(ts_ns),
                "event_kind": event_kind,
                "priority": int(priority),
                "last_known": bool(known),
                "last_external_bid99_shares": float(current),
                "known_values": [],
                "max_peak": float(peak),
                "trade_shares": 0.0,
                "trade_count": 0,
                "depth_count": 0,
                "observed_drop": 0.0,
                "explained_drop": 0.0,
                "unexplained_drop": 0.0,
                "max_unexplained_step": 0.0,
                "cumulative_unexplained": float(cumulative_unexplained),
                "max_peak_unexplained": max(0.0, peak - current - exact_sell_since_peak),
                "total_sell_99": float(total_sell_99),
                "threshold_crossed": False,
                "best_bid": math.nan,
                "best_bid_size": math.nan,
                "best_ask": math.nan,
                "best_ask_size": math.nan,
                "ask_crossed_limit": False,
            }
        bucket["last_ts_ns"] = int(ts_ns)
        bucket["last_known"] = bool(known)
        bucket["last_external_bid99_shares"] = float(current)
        if known:
            bucket["known_values"].append(float(current))
        bucket["max_peak"] = max(float(bucket["max_peak"]), float(peak))
        bucket["trade_shares"] += max(0.0, float(trade_shares))
        bucket["trade_count"] += int(trade_shares > 0)
        bucket["depth_count"] += int(trade_shares <= 0)
        bucket["observed_drop"] += max(0.0, float(observed_drop))
        bucket["explained_drop"] += max(0.0, float(explained_drop))
        bucket["unexplained_drop"] += max(0.0, float(unexplained_step))
        bucket["max_unexplained_step"] = max(
            float(bucket["max_unexplained_step"]), max(0.0, float(unexplained_step))
        )
        bucket["cumulative_unexplained"] = float(cumulative_unexplained)
        bucket["max_peak_unexplained"] = max(
            float(bucket["max_peak_unexplained"]),
            max(0.0, peak - current - exact_sell_since_peak),
        )
        bucket["total_sell_99"] = float(total_sell_99)
        bucket["threshold_crossed"] = bool(bucket["threshold_crossed"] or threshold_crossed)
        bucket["priority"] = max(int(bucket["priority"]), int(priority))
        if math.isfinite(best_bid):
            bucket["best_bid"] = float(best_bid)
        if math.isfinite(best_bid_size):
            bucket["best_bid_size"] = float(best_bid_size)
        if math.isfinite(best_ask):
            bucket["best_ask"] = float(best_ask)
        if math.isfinite(best_ask_size):
            bucket["best_ask_size"] = float(best_ask_size)
        bucket["ask_crossed_limit"] = bool(ask_crossed_limit)
        if force_flush:
            flush_bucket(forced_kind=event_kind, priority=priority)

    emit_direct(ts_ns=open_ns, event_kind="order_open", priority=2, known=True)

    for event in events:
        ts_ns = int(event["ts_ns"])
        if ts_ns >= effective_scan_end_ns:
            break
        row = event["row"]
        if event["kind"] == "trade":
            qty = max(0.0, q.as_float(row.get("size"), 0.0))
            if qty <= 0:
                continue
            total_sell_99 += qty
            exact_sell_since_peak += qty
            exact_sell_since_previous_depth += qty
            record_bucket(ts_ns=ts_ns, known=True, trade_shares=qty)
            continue

        raw_current = q.as_float(
            row.get("bid_level_size", row.get("bid99_size")),
            math.nan,
        )
        best_bid = q.as_float(row.get("best_bid"), math.nan)
        best_bid_size = q.as_float(row.get("best_bid_size"), math.nan)
        best_ask = q.as_float(row.get("best_ask"), math.nan)
        best_ask_size = q.as_float(row.get("best_ask_size"), math.nan)
        ask_crossed = bool(row.get("ask_crossed_limit", False))
        if not math.isfinite(raw_current):
            transition = last_known is not False
            record_bucket(
                ts_ns=ts_ns,
                known=False,
                best_bid=best_bid,
                best_bid_size=best_bid_size,
                best_ask=best_ask,
                best_ask_size=best_ask_size,
                ask_crossed_limit=ask_crossed,
                force_flush=transition,
                event_kind="bid99_unknown_transition" if transition else "interval_summary",
                priority=1 if transition else 0,
            )
            last_known = False
            continue

        new_current = max(0.0, float(raw_current))
        observed_drop = max(0.0, previous_depth_size - new_current)
        explained_drop = min(observed_drop, exact_sell_since_previous_depth)
        unexplained_step = max(0.0, observed_drop - explained_drop)
        cumulative_unexplained += unexplained_step
        max_step_unexplained = max(max_step_unexplained, unexplained_step)
        current = new_current

        if current > peak + 1e-12:
            peak = current
            exact_sell_since_peak = 0.0
        peak_unexplained = max(0.0, peak - current - exact_sell_since_peak)
        max_peak_unexplained = max(max_peak_unexplained, peak_unexplained)
        crossed = threshold >= 0 and peak_unexplained + 1e-12 >= threshold
        first_cross = bool(crossed and cancel_candidate is None)
        if first_cross:
            delay = (
                cfg.bid99_unexplained_drop_cancel_delay_ms
                if cfg.bid99_unexplained_drop_cancel_delay_ms >= 0
                else cfg.paper_shadow_order_open_delay_ms
            )
            delay_ns=_delay_ns(max(0.0,float(delay)))
            cancel_candidate = {
                "reason": "bid99_unexplained_drop_threshold",
                "request_ts_ns": int(ts_ns), "effective_ts_ns": int(ts_ns)+delay_ns,
                "request_ts_ms": q.ns_to_ms(ts_ns), "effective_ts_ms": q.ns_to_ms(ts_ns+delay_ns),
                "delay_ms": delay_ns/1e6,
                "observed_value": float(peak_unexplained),
                "threshold": float(threshold),
                "max_external_bid99_shares_seen": float(peak),
                "current_external_bid99_shares": float(current),
                "sell_trade_shares_since_peak": float(exact_sell_since_peak),
                "total_sell_trade_shares_at_99": float(total_sell_99),
                "cumulative_unexplained_drop_shares": float(cumulative_unexplained),
            }
            effective_scan_end_ns = min(effective_scan_end_ns, int(cancel_candidate["effective_ts_ns"]))

        known_transition = last_known is False
        record_bucket(
            ts_ns=ts_ns,
            known=True,
            observed_drop=observed_drop,
            explained_drop=explained_drop,
            unexplained_step=unexplained_step,
            threshold_crossed=crossed,
            best_bid=best_bid,
            best_bid_size=best_bid_size,
            best_ask=best_ask,
            best_ask_size=best_ask_size,
            ask_crossed_limit=ask_crossed,
            force_flush=bool(first_cross or known_transition),
            event_kind=(
                "cancel_threshold_crossed" if first_cross
                else "bid99_known_transition" if known_transition
                else "interval_summary"
            ),
            priority=2 if first_cross else 1 if known_transition else 0,
        )
        last_known = True
        previous_depth_size = current
        exact_sell_since_previous_depth = 0.0

    flush_bucket()
    if output_enabled:
        emit_direct(
            ts_ns=min(int(effective_scan_end_ns), int(initial_scan_end_ns)),
            event_kind="order_scan_summary",
            priority=2,
            known=bool(last_known is not False),
        )

    stats = {
        "max_external_bid99_shares_seen": float(peak),
        "total_sell_trade_shares_at_99": float(total_sell_99),
        "total_bought_shares_at_99": float(total_sell_99),
        "max_unexplained_drop_from_peak_shares": float(max_peak_unexplained),
        "max_unexplained_drop_step_shares": float(max_step_unexplained),
        "cumulative_unexplained_drop_shares": float(cumulative_unexplained),
        "last_external_bid99_shares": float(current),
        "bid99_volume_source": str(volume_source),
        "bid99_volume_raw_depth_events_scanned": int(raw_depth_events),
        "bid99_volume_raw_trade_events_scanned": int(raw_trade_events),
        "bid99_volume_output_rows": int(len(timeline)),
        "bid99_volume_output_interval_ms": (
            float(cfg.bid99_volume_output_interval_ms) if output_enabled else math.nan
        ),
    }
    return timeline, stats, cancel_candidate

def _disappearance_diagnostics(
    *, side_depth: pd.DataFrame, causal_trades: pd.DataFrame,
    open_ns: int | None = None, open_ms: float | None = None, cfg: Config,
) -> dict[str, Any]:
    """Record bid-disappearance evidence without using float epoch ordering."""
    if open_ns is None:
        if open_ms is None: raise ValueError("open_ns is required")
        open_ns=q.ms_to_ns(open_ms)
    open_ns=int(open_ns)
    from queue99_entry_filters import attrs_free_frame
    side_depth=_normalize_causal_frame_ns(attrs_free_frame(side_depth))
    causal_trades=_normalize_causal_frame_ns(attrs_free_frame(causal_trades))
    result={"bid99_disappearance_ts_ns":-1,"bid99_disappearance_ts_ms":math.nan,"bid99_disappearance_time_utc":"","bid99_disappearance_best_bid":math.nan,"bid99_disappearance_bid99_size":math.nan,"best_ask_at_disappearance":math.nan,"best_ask_size_at_disappearance":math.nan,"ask_existed_at_disappearance":False,"ask_crossed_limit":False,"bid99_reappeared":False,"bid99_reappearance_delay_ms":math.nan,"sell_trade_volume_at_99_before_disappearance":0.0,"sell_trade_volume_at_or_below_99_after_disappearance":0.0}
    if side_depth is None or side_depth.empty:return result
    future=side_depth[pd.to_numeric(side_depth["ts_ns"],errors="raise").astype("int64")>open_ns].copy()
    if future.empty:return result
    future.sort_values(["ts_ns","seq","state_id"],inplace=True,kind="stable")
    disappeared=future[(pd.to_numeric(future["bid99_size"],errors="coerce")<=1e-12)&(pd.to_numeric(future["best_bid"],errors="coerce")<cfg.trigger_bid-cfg.trigger_tolerance)]
    if disappeared.empty:return result
    row=disappeared.iloc[0].to_dict(); ts_ns=int(disappeared["ts_ns"].iloc[0]); ts_ms=q.ns_to_ms(ts_ns)
    later=future[(pd.to_numeric(future["ts_ns"],errors="raise").astype("int64")>ts_ns)&(pd.to_numeric(future["bid99_size"],errors="coerce")>1e-12)&(np.abs(pd.to_numeric(future["best_bid"],errors="coerce")-cfg.trigger_bid)<=cfg.trigger_tolerance+1e-12)]
    reappearance_delay=(int(later["ts_ns"].iloc[0])-ts_ns)/1e6 if not later.empty else math.nan
    trades=causal_trades if isinstance(causal_trades,pd.DataFrame) else pd.DataFrame()
    if trades.empty:before_99=after_le=0.0
    else:
        sell=trades[(trades["aggressor_side"]=="SELL")|(~trades["aggressor_side"].isin(["BUY","SELL"])&cfg.allow_all_trades_when_side_missing)].copy(); prices=pd.to_numeric(sell["price"],errors="coerce"); clocks=pd.to_numeric(sell["clock_ns"],errors="raise").astype("int64"); sizes=pd.to_numeric(sell["size"],errors="coerce").fillna(0).clip(lower=0)
        before_99=float(sizes[(clocks<ts_ns)&(np.abs(prices-cfg.trigger_bid)<=cfg.trigger_tolerance+1e-12)].sum()); after_le=float(sizes[(clocks>ts_ns)&(prices<=cfg.trigger_bid+cfg.trigger_tolerance+1e-12)].sum())
    best_ask=q.as_float(row.get("best_ask"),math.nan); best_ask_size=q.as_float(row.get("best_ask_size"),math.nan)
    result.update({"bid99_disappearance_ts_ns":ts_ns,"bid99_disappearance_ts_ms":ts_ms,"bid99_disappearance_time_utc":q.market_iso(ts_ms),"bid99_disappearance_best_bid":q.as_float(row.get("best_bid"),math.nan),"bid99_disappearance_bid99_size":q.as_float(row.get("bid99_size"),math.nan),"best_ask_at_disappearance":best_ask,"best_ask_size_at_disappearance":best_ask_size,"ask_existed_at_disappearance":bool(math.isfinite(best_ask) and best_ask_size>0),"ask_crossed_limit":bool(math.isfinite(best_ask) and best_ask_size>0 and best_ask<=cfg.trigger_bid+cfg.trigger_tolerance+1e-12),"bid99_reappeared":bool(not later.empty),"bid99_reappearance_delay_ms":reappearance_delay,"sell_trade_volume_at_99_before_disappearance":before_99,"sell_trade_volume_at_or_below_99_after_disappearance":after_le})
    return result



def _queue_ahead_depth_reconstruction_events(
    *,
    open_ns: int,
    cutoff_ns: int,
    queue_ahead: float,
    side_depth: pd.DataFrame,
    exact_queue_trades: pd.DataFrame,
    cfg: Config,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Build causal displayed-level queue adjustments for one resting order.

    The reconstruction is deliberately conservative:

    * a post-open size increase is assigned behind our order;
    * later cancellation removes that behind bucket before reducing queue ahead;
    * exact-limit queue-consuming trades are added back to the observed depth
      change before non-trade addition/cancellation is inferred, preventing the
      same traded shares from being counted twice;
    * only the residual displayed change is treated as addition or cancellation.

    The returned events are observations, not executions.  The fill simulator
    applies them at their depth receive timestamp before later FIFO evidence.
    """
    mode = str(cfg.queue_ahead_reconstruction_mode or "off").strip().lower()
    stats: dict[str, Any] = {
        "queue_ahead_reconstruction_mode": mode,
        "queue_ahead_reconstruction_enabled": mode != "off",
        "queue_depth_rows_considered": 0,
        "queue_depth_intervals_with_exact_trades": 0,
        "queue_depth_addition_events": 0,
        "queue_depth_cancellation_events": 0,
        "queue_depth_planned_added_behind_shares": 0.0,
        "queue_depth_planned_cancel_shares": 0.0,
    }
    if mode == "off" or side_depth is None or side_depth.empty:
        return [], stats
    if mode != "post_order_first":
        raise InternalContractError(f"unsupported queue reconstruction mode: {mode!r}")

    depth = _normalize_causal_frame_ns(side_depth)
    trades = _normalize_causal_frame_ns(exact_queue_trades)
    ts = pd.to_numeric(depth.get("ts_ns"), errors="coerce")
    depth = depth[np.isfinite(ts)].copy()
    if depth.empty:
        return [], stats
    depth["_q99_depth_ts_ns"] = pd.to_numeric(depth["ts_ns"], errors="raise").astype("int64")
    depth = depth[(depth["_q99_depth_ts_ns"] > int(open_ns)) & (depth["_q99_depth_ts_ns"] < int(cutoff_ns))]
    if depth.empty:
        return [], stats
    depth.sort_values(["_q99_depth_ts_ns", "seq", "state_id"], inplace=True, kind="stable")

    trade_times = np.empty(0, dtype=np.int64)
    trade_prefix = np.zeros(1, dtype=np.float64)
    if trades is not None and not trades.empty:
        clock = pd.to_numeric(trades.get("clock_ns"), errors="coerce")
        sizes = pd.to_numeric(trades.get("size"), errors="coerce").fillna(0.0).clip(lower=0.0)
        mask = np.isfinite(clock) & (clock > int(open_ns)) & (clock < int(cutoff_ns)) & (sizes > 0)
        if cfg.trade_clock_mode == "venue_with_local_guard" and "local_ts_ns" in trades.columns:
            local = pd.to_numeric(trades.get("local_ts_ns"), errors="coerce")
            mask &= np.isfinite(local) & (local < int(cutoff_ns))
        active = trades.loc[mask].copy()
        if not active.empty:
            active.sort_values(["clock_ns", "local_ts_ns", "seq", "state_id"], inplace=True, kind="stable")
            trade_times = pd.to_numeric(active["clock_ns"], errors="raise").to_numpy(dtype=np.int64, copy=False)
            trade_sizes = pd.to_numeric(active["size"], errors="coerce").fillna(0.0).clip(lower=0.0).to_numpy(dtype=np.float64, copy=False)
            trade_prefix = np.concatenate((np.zeros(1, dtype=np.float64), np.cumsum(trade_sizes, dtype=np.float64)))

    def interval_trade_shares(start_ns: int, end_ns: int) -> float:
        if trade_times.size == 0 or end_ns <= start_ns:
            return 0.0
        lo = int(np.searchsorted(trade_times, int(start_ns), side="right"))
        hi = int(np.searchsorted(trade_times, int(end_ns), side="right"))
        return float(trade_prefix[hi] - trade_prefix[lo])

    events: list[dict[str, Any]] = []
    previous_size: float | None = max(0.0, float(queue_ahead))
    previous_ts = int(open_ns)
    multiplier = float(cfg.queue_size_multiplier)
    eps = 1e-9
    for row in depth.to_dict("records"):
        current_ts = int(row.get("_q99_depth_ts_ns", row.get("ts_ns", -1)))
        raw = q.as_float(row.get("bid_level_size", row.get("bid99_size")), math.nan)
        stats["queue_depth_rows_considered"] += 1
        if not math.isfinite(raw):
            previous_size = None
            previous_ts = current_ts
            continue
        current_size = max(0.0, float(raw) * multiplier)
        if previous_size is None:
            previous_size = current_size
            previous_ts = current_ts
            continue
        trade_shares = interval_trade_shares(previous_ts, current_ts)
        if trade_shares > eps:
            stats["queue_depth_intervals_with_exact_trades"] += 1

        # Aggregate displayed size should have fallen by the exact-limit trade
        # volume.  The residual change is therefore the causal net of non-trade
        # additions/cancellations observed at this depth update:
        #
        #   non_trade_delta = current - previous + exact_trade_shares
        #
        # Positive residual is new volume (necessarily behind our resting
        # order). Negative residual is cancellation; cancellation always removes
        # the post-order/behind bucket first and only then reduces queue ahead.
        non_trade_delta = current_size - float(previous_size) + float(trade_shares)
        if non_trade_delta > eps:
            amount = float(non_trade_delta)
            events.append({
                "ts_ns": current_ts,
                "kind": "add_behind",
                "shares": amount,
                "previous_level_shares": float(previous_size),
                "current_level_shares": float(current_size),
                "interval_exact_trade_shares": float(trade_shares),
                "inference": (
                    "post_open_level_growth"
                    if trade_shares <= eps
                    else "trade_adjusted_net_addition"
                ),
            })
            stats["queue_depth_addition_events"] += 1
            stats["queue_depth_planned_added_behind_shares"] += amount
        elif non_trade_delta < -eps:
            amount = float(-non_trade_delta)
            events.append({
                "ts_ns": current_ts,
                "kind": "cancel_post_order_first",
                "shares": amount,
                "previous_level_shares": float(previous_size),
                "current_level_shares": float(current_size),
                "interval_exact_trade_shares": float(trade_shares),
                "inference": (
                    "displayed_drop_without_exact_trade"
                    if trade_shares <= eps
                    else "trade_adjusted_net_cancellation"
                ),
            })
            stats["queue_depth_cancellation_events"] += 1
            stats["queue_depth_planned_cancel_shares"] += amount
        previous_size = current_size
        previous_ts = current_ts
    events.sort(key=lambda row: (int(row["ts_ns"]), 0 if row["kind"] == "add_behind" else 1))
    return events, stats

# ---- FILLPARITY (q99_20260930_FILLPARITY): env-gated execution model calibrated on live q99 orders ----
_FP_ON = os.environ.get("Q99_FILLPAR", "0") == "1"
_FP_COMP = os.environ.get("Q99_FILLPAR_COMPLEMENT", "1") == "1"
_FP_THROUGH = os.environ.get("Q99_FILLPAR_THROUGH", "1") == "1"
_FP_EFF_SHIFT_MS = float(os.environ.get("Q99_FILLPAR_EFFECTIVE_SHIFT_MS", "60"))
_FP_RULE = os.environ.get("Q99_FILLPAR_CANCEL_RULE", "sizematch")
_FP_OTHER = {"UP": "DOWN", "DOWN": "UP"}
_FP_OWN_PATH = os.environ.get("Q99_FILLPAR_OWN_STEPS", "")
_FP_OWN_VENUE = os.environ.get("Q99_FILLPAR_OWN_VENUE", "0") == "1"
_FP_CUT_SHIFTED = os.environ.get("Q99_FILLPAR_CUT_SHIFTED", "0") == "1"
_FP_OWN = None


def _fp_own_steps(contract_start_ms, outcome, limit):
    """Signed venue-ms steps of the live account's own resting size at this level (own-order removal)."""
    global _FP_OWN
    if not _FP_OWN_PATH:
        return None
    if _FP_OWN is None:
        df = pd.read_parquet(_FP_OWN_PATH)
        _FP_OWN = {k: (g["venue_ms"].to_numpy(np.int64), g["delta"].to_numpy(np.float64))
                   for k, g in df.sort_values("venue_ms").groupby(["contract_start_ms", "outcome", "limit"])}
    return _FP_OWN.get((int(contract_start_ms), str(outcome), round(float(limit), 2)))


def _fp_augment_trades(trades, *, task, data_dir, cfg, token_map, start_ms, end_ms, outcomes):
    """Add complement-token BUY prints (mapped to our outcome at 1-p, as SELL) to the loaded tape."""
    if not (_FP_ON and _FP_COMP) or not outcomes:
        return trades
    comp = sorted({_FP_OTHER[str(o)] for o in outcomes if str(o) in _FP_OTHER})
    lo = 1.0 - float(cfg.trigger_bid)
    kw = {"min_price": lo, "target_price": None} if _FP_THROUGH else {"target_price": lo}
    extra, _meta = q.load_trades(
        task, data_dir=data_dir, market_key=cfg.market_key, token_map=token_map, yes_outcome=cfg.yes_outcome,
        start_ms=start_ms, end_ms=end_ms, trade_clock_mode=cfg.trade_clock_mode,
        allow_local_fallback=cfg.allow_local_trade_clock_fallback, tolerance=cfg.trigger_tolerance,
        outcomes=comp, **kw,
    )
    if extra is None or extra.empty:
        return trades
    extra = extra[extra["aggressor_side"] == "BUY"].copy()
    if extra.empty:
        return trades
    extra["outcome"] = extra["outcome"].map(_FP_OTHER)
    extra["price"] = np.round(1.0 - pd.to_numeric(extra["price"], errors="coerce"), 6)
    extra["aggressor_side"] = "SELL"
    extra["side_source"] = "fillpar_complement_buy"
    extra["side_conflict"] = False
    extra["trade_id"] = "c:" + extra["trade_id"].astype(str)
    out = pd.concat([trades, extra], ignore_index=True) if trades is not None and not trades.empty else extra
    out.sort_values(["clock_ns", "seq", "state_id"], inplace=True, kind="stable")
    return out.reset_index(drop=True)


def _fp_cancel(queue, amount):
    others = [i for i, x in enumerate(queue) if not x[1]]
    if _FP_RULE in ("sizematch", "sizefifo") and others:
        cand = [i for i in others if abs(queue[i][0] - amount) < 1e-6]
        if cand:
            queue.pop(cand[-1] if _FP_RULE == "sizematch" else cand[0])
            return 0.0
    for i in reversed(others):
        if amount <= 1e-9:
            break
        c = min(queue[i][0], amount); queue[i][0] -= c; amount -= c
    queue[:] = [x for x in queue if x[0] > 1e-9]
    return amount


def _fp_simulate(*, side, open_ns, cutoff_ns, requested, causal_trades, cfg, pre_open_depth, contract_start_ms=None):
    """Order-list FIFO at level L on the engine clock. Our BT order is virtual (not in the displayed level)."""
    mult = float(cfg.queue_size_multiplier); L = float(cfg.trigger_bid); tol = float(cfg.trigger_tolerance)
    feed = float(cfg.polymarket_feed_delay_ms) if str(cfg.polymarket_timestamp_clock) == "venue" else 0.0
    shift_ns = int(round(max(0.0, _FP_EFF_SHIFT_MS - feed) * 1e6))
    ev = []
    if pre_open_depth is not None and not pre_open_depth.empty:
        d = pre_open_depth[pre_open_depth["side"].astype(str) == str(side)]
        d = d[pd.to_numeric(d["ts_ns"], errors="coerce") < int(cutoff_ns)]
        own = _fp_own_steps(contract_start_ms, side, L) if contract_start_ms is not None else None
        if own is not None:
            own_t = (own[0] + (0.0 if _FP_OWN_VENUE else feed)) * 1_000_000; own_c = np.cumsum(own[1])
        for t, sq, st, v in zip(d["ts_ns"].to_numpy(np.int64), d["seq"].to_numpy(np.int64),
                                d["state_id"].to_numpy(np.int64), d["bid_level_size"].to_numpy(np.float64)):
            if own is not None and math.isfinite(v):
                k = int(np.searchsorted(own_t, t, side="right")) - 1
                if k >= 0:
                    v = max(0.0, v - max(0.0, float(own_c[k])))
            ev.append((int(t), 1, int(sq), float(v) * mult if math.isfinite(v) else math.nan))
    ev.append((int(open_ns), 2, 0, None))                                      # our (virtual) join
    if causal_trades is not None and not causal_trades.empty:
        tr = causal_trades
        ck = pd.to_numeric(tr["clock_ns"], errors="coerce").to_numpy(np.int64)
        px = pd.to_numeric(tr["price"], errors="coerce").to_numpy(np.float64)
        sz = pd.to_numeric(tr["size"], errors="coerce").fillna(0.0).to_numpy(np.float64)
        ag = tr["aggressor_side"].astype(str).to_numpy()
        for c, p_, z, a in zip(ck, px, sz, ag):
            if a != "SELL" or z <= 0 or not (int(open_ns) < c and (c - shift_ns if _FP_CUT_SHIFTED else c) < int(cutoff_ns)):
                continue
            if abs(p_ - L) <= tol + 1e-12:
                ev.append((int(c) - shift_ns, 0, 0, (float(z), False, int(c))))
            elif _FP_THROUGH and p_ < L - tol - 1e-12:
                ev.append((int(c) - shift_ns, 0, 0, (float(z), True, int(c))))
    ev.sort(key=lambda e: (e[0], e[1], e[2]))
    queue = []; others_tot = 0.0; joined = False; R = float(requested); filled = 0.0; fills = []
    for t, typ, sq, val in ev:
        if typ == 2:
            if int(open_ns) < int(cutoff_ns):
                queue.append([R, True]); joined = True
            continue
        if t >= int(cutoff_ns) and typ == 1:
            break
        if typ == 0:
            amt, thr, orig = val
            if thr:
                amt = 1e18
            while amt > 1e-9 and queue:
                c = min(queue[0][0], amt)
                if queue[0][1]:
                    fills.append((orig, c)); filled += c; R -= c
                else:
                    others_tot -= c
                queue[0][0] -= c; amt -= c
                if queue[0][0] <= 1e-9:
                    queue.pop(0)
            others_tot = max(0.0, others_tot)
            if joined and R <= 1e-9:
                break
            continue
        new = val
        if not math.isfinite(new):
            continue
        delta = new - others_tot
        if delta > 1e-9:
            queue.append([delta, False]); others_tot += delta
        elif delta < -1e-9:
            left = _fp_cancel(queue, -delta)
            others_tot = max(0.0, others_tot - (-delta - left))
    return filled, fills


def _simulate_order_fills(
    *,
    order_id: str,
    contract_start_ms: int,
    side: str,
    open_ns: int | None = None,
    cutoff_ns: int | None = None,
    open_ms: float | None = None,
    cutoff_ms: float | None = None,
    requested: float,
    queue_ahead: float,
    side_depth: pd.DataFrame,
    causal_trades: pd.DataFrame,
    exact_queue_trades: pd.DataFrame,
    cfg: Config,
    queue_reconstruction_depth: pd.DataFrame | None = None,
    initial_cross_match: dict[str, Any] | None = None,
    pre_open_depth: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Apply only exact-limit tape FIFO on an int64 nanosecond clock.

    The ``*_ms`` arguments are compatibility-only.  They are converted once at
    the boundary; every causal comparison, tie, cutoff, and dedup thereafter is
    integer nanoseconds.  A fill at exactly the cancellation cutoff is blocked.
    """
    if initial_cross_match and initial_cross_match.get("scheduled", False):
        raise ValueError("FIFO-only execution does not accept an initial ask-cross match")
    if open_ns is None:
        if open_ms is None: raise ValueError("open_ns is required")
        open_ns = q.ms_to_ns(open_ms)
    if cutoff_ns is None:
        if cutoff_ms is None: raise ValueError("cutoff_ns is required")
        cutoff_ns = q.ms_to_ns(cutoff_ms)
    open_ns = int(open_ns); cutoff_ns = int(cutoff_ns)
    side_depth = _normalize_causal_frame_ns(side_depth)
    queue_reconstruction_depth = _normalize_causal_frame_ns(
        queue_reconstruction_depth if queue_reconstruction_depth is not None else side_depth
    )
    causal_trades = _normalize_causal_frame_ns(causal_trades)
    exact_queue_trades = _normalize_causal_frame_ns(exact_queue_trades)
    fill_cache = None
    fill_cache_key = None
    if initial_cross_match is None and isinstance(side_depth, pd.DataFrame):
        fill_cache = frame_cache(side_depth, "_queue99_fill_sim_cache")
        fill_cache_key = (
            str(side), int(open_ns), int(cutoff_ns), round(float(requested), 9),
            round(float(queue_ahead), 9), frame_token(causal_trades), frame_token(exact_queue_trades),
            frame_token(queue_reconstruction_depth), bool(cfg.include_same_timestamp_trades),
            round(float(cfg.trigger_bid), 12), round(float(cfg.trigger_tolerance), 12),
            bool(cfg.fill_remaining_on_ask99_appear), bool(cfg.fill_remaining_on_trade_below_limit),
            round(float(cfg.price_priority_fill_delay_ms), 9), str(cfg.trade_clock_mode),
            str(cfg.queue_ahead_reconstruction_mode).strip().lower(),
        )
        cached_fill = fill_cache.get(fill_cache_key)
        if cached_fill is not None:
            out = dict(cached_fill)
            out["fill_events"] = [dict(row, order_id=order_id) for row in cached_fill.get("fill_events", [])]
            out["queue_trades"] = [dict(row, order_id=order_id) for row in cached_fill.get("queue_trades", [])]
            return out
    confirmation_delay_ns = 0
    confirmation_delay_ms = 0.0

    def _empty_like(frame: pd.DataFrame | None) -> pd.DataFrame:
        return pd.DataFrame(columns=list(frame.columns)) if isinstance(frame, pd.DataFrame) else pd.DataFrame()

    def _active_trades(frame: pd.DataFrame | None) -> pd.DataFrame:
        if frame is None or frame.empty: return _empty_like(frame)
        clock = pd.to_numeric(frame["clock_ns"], errors="raise").astype("int64")
        after_open = clock >= open_ns if cfg.include_same_timestamp_trades else clock > open_ns
        out = frame[after_open & (clock < cutoff_ns)].copy()
        if cfg.trade_clock_mode == "venue_with_local_guard" and not out.empty:
            local = pd.to_numeric(out["local_ts_ns"], errors="raise").astype("int64")
            out = out[local < cutoff_ns].copy()
        if not out.empty:
            out.sort_values(["clock_ns", "local_ts_ns", "seq", "state_id"], inplace=True, kind="stable")
        return out

    trades_active = _active_trades(causal_trades)
    exact_active = _active_trades(exact_queue_trades)
    # Defensive boundary: even a direct caller cannot label BUY/through prints as FIFO.
    if not exact_active.empty:
        price = pd.to_numeric(exact_active["price"], errors="coerce")
        exact_active = exact_active[np.abs(price - cfg.trigger_bid) <= cfg.trigger_tolerance + 1e-12].copy()
        if "side_conflict" in exact_active and bool(exact_active["side_conflict"].any()):
            raise RuntimeError(f"conflicting exact-limit FIFO sides: {order_id}")
        sides = exact_active["aggressor_side"]
        allowed = (sides == "SELL") | (~sides.isin(["BUY", "SELL"]) & cfg.allow_all_trades_when_side_missing)
        exact_active = exact_active[allowed].copy()
    queue_adjustments, queue_reconstruction_plan = _queue_ahead_depth_reconstruction_events(
        open_ns=open_ns,
        cutoff_ns=cutoff_ns,
        queue_ahead=float(queue_ahead),
        side_depth=queue_reconstruction_depth,
        exact_queue_trades=exact_active,
        cfg=cfg,
    )

    # No displayed-ask, trade-through, or reverse-book evidence can generate fills.
    confirmation = None
    evidence_ns = confirmation_ns = -1
    confirmation_effective_before_cutoff = False
    exact_cutoff_ns = cutoff_ns

    exact = exact_active[pd.to_numeric(exact_active["clock_ns"], errors="raise").astype("int64") < exact_cutoff_ns].copy() if not exact_active.empty else _empty_like(exact_active)
    if cfg.trade_clock_mode == "venue_with_local_guard" and not exact.empty:
        exact=exact[pd.to_numeric(exact["local_ts_ns"], errors="raise").astype("int64") < exact_cutoff_ns].copy()

    queue_remaining=float(queue_ahead); queue_behind=0.0; filled=0.0; cumulative_exact99=0.0
    fill_events:list[dict[str,Any]]=[]; queue_trades:list[dict[str,Any]]=[]
    adjustment_index=0
    reconstruction_added_behind=0.0
    reconstruction_cancelled_behind=0.0
    reconstruction_cancelled_ahead=0.0
    reconstruction_applied_events=0

    def apply_queue_adjustments_until(limit_ns:int)->None:
        nonlocal adjustment_index,queue_remaining,queue_behind
        nonlocal reconstruction_added_behind,reconstruction_cancelled_behind
        nonlocal reconstruction_cancelled_ahead,reconstruction_applied_events
        while adjustment_index < len(queue_adjustments):
            event=queue_adjustments[adjustment_index]
            ts_ns=int(event["ts_ns"])
            if ts_ns>int(limit_ns):
                break
            adjustment_index+=1
            amount=max(0.0,q.as_float(event.get("shares"),0.0))
            if amount<=0:
                continue
            queue_before=float(queue_remaining); behind_before=float(queue_behind)
            cancel_behind=0.0; cancel_ahead=0.0; added_behind=0.0
            if str(event.get("kind"))=="add_behind":
                queue_behind+=amount; added_behind=amount
                reconstruction_added_behind+=amount
            else:
                cancel_behind=min(queue_behind,amount); queue_behind-=cancel_behind
                residual=max(0.0,amount-cancel_behind)
                cancel_ahead=min(queue_remaining,residual); queue_remaining-=cancel_ahead
                reconstruction_cancelled_behind+=cancel_behind
                reconstruction_cancelled_ahead+=cancel_ahead
            reconstruction_applied_events+=1
            queue_trades.append({
                "order_id":order_id,"contract_start_ms":contract_start_ms,"outcome":side,
                "trade_ts_ns":ts_ns,"trade_ts_ms":q.ns_to_ms(ts_ns),
                "trade_local_ts_ns":ts_ns,"trade_local_ts_ms":q.ns_to_ms(ts_ns),
                "trade_venue_ts_ns":-1,"trade_venue_ts_ms":math.nan,
                "trade_price":float(cfg.trigger_bid),
                "trade_price_relation":"depth_queue_reconstruction",
                "trade_shares":0.0,"aggressor_side":"","side_source":"depth_clock_level_change",
                "queue_before":queue_before,"queue_consumed":cancel_ahead,"queue_after":float(queue_remaining),
                "queue_behind_before":behind_before,"queue_behind_added":added_behind,
                "queue_behind_cancelled":cancel_behind,"queue_behind_after":float(queue_behind),
                "queue_ahead_cancelled":cancel_ahead,
                "fill_before":float(filled),"fill_shares":0.0,"fill_after":float(filled),
                "cumulative_eligible_trade_shares":float(cumulative_exact99),
                "price_priority_full_fill_confirmation":False,
                "queue_reconstruction_kind":str(event.get("kind","")),
                "queue_reconstruction_inference":str(event.get("inference","")),
                "queue_reconstruction_previous_level_shares":q.as_float(event.get("previous_level_shares"),math.nan),
                "queue_reconstruction_current_level_shares":q.as_float(event.get("current_level_shares"),math.nan),
                "queue_reconstruction_interval_exact_trade_shares":q.as_float(event.get("interval_exact_trade_shares"),0.0),
            })

    cross = {}
    cross_scheduled = cross_effective = False
    cross_match_ns = -1
    cross_levels = []
    cross_fill = cross_cost = 0.0
    cross_levels_used = 0

    def process_exact_row(row:dict[str,Any])->None:
        nonlocal queue_remaining,queue_behind,filled,cumulative_exact99
        qty=max(0.0,q.as_float(row.get("size"),0.0)); ts_ns=int(row["clock_ns"])
        if qty<=0 or filled>=requested-1e-9 or ts_ns>=cutoff_ns:return
        apply_queue_adjustments_until(ts_ns)
        queue_before=queue_remaining; behind_before=queue_behind; fill_before=filled
        queue_consumed=min(queue_remaining,qty); queue_remaining-=queue_consumed
        after_ahead=max(0.0,qty-queue_consumed)
        order_fill=min(max(0.0,requested-filled),after_ahead); filled+=order_fill
        residual=max(0.0,after_ahead-order_fill)
        behind_consumed=min(queue_behind,residual); queue_behind-=behind_consumed
        cumulative_exact99+=qty
        queue_trades.append({"order_id":order_id,"contract_start_ms":contract_start_ms,"outcome":side,"trade_ts_ns":ts_ns,"trade_ts_ms":q.ns_to_ms(ts_ns),"trade_local_ts_ns":int(row.get("local_ts_ns",-1)),"trade_local_ts_ms":row.get("local_ts_ms"),"trade_venue_ts_ns":int(row.get("venue_ts_ns",-1)),"trade_venue_ts_ms":row.get("venue_ts_ms"),"trade_price":row["price"],"trade_price_relation":"at_limit","trade_shares":qty,"aggressor_side":row["aggressor_side"],"side_source":row["side_source"],"queue_before":queue_before,"queue_consumed":queue_consumed,"queue_after":queue_remaining,"queue_behind_before":behind_before,"queue_behind_consumed":behind_consumed,"queue_behind_after":queue_behind,"fill_before":fill_before,"fill_shares":order_fill,"fill_after":filled,"cumulative_eligible_trade_shares":cumulative_exact99,"price_priority_full_fill_confirmation":False})
        if order_fill>0:
            ms=q.ns_to_ms(ts_ns); fill_events.append({"order_id":order_id,"contract_start_ms":contract_start_ms,"outcome":side,"fill_ts_ns":ts_ns,"fill_ts_ms":ms,"fill_time_utc":q.market_iso(ms),"fill_shares":order_fill,"fill_price":cfg.trigger_bid,"fill_source":"trade_tape_fifo_at_limit","cumulative_filled_shares":filled})

    exact_records = exact.to_dict("records")
    for row in exact_records:
        process_exact_row(row)
        if filled >= requested - 1e-9:
            break

    cross_match_ms=q.ns_to_ms(cross_match_ns) if cross_match_ns>=0 else math.nan
    initial_cross_fields={"momentum_early_cross_match_scheduled":cross_scheduled,"momentum_early_cross_match_ts_ns":cross_match_ns,"momentum_early_cross_match_ts_ms":cross_match_ms,"momentum_early_cross_match_time_utc":q.market_iso(cross_match_ms) if cross_match_ns>=0 else "","momentum_early_cross_match_delay_ms":q.as_float(cross.get("delay_ms"),math.nan),"momentum_early_cross_match_still_crossed":bool(cross_levels),"momentum_early_cross_match_available_ask_shares":float(sum(max(0.0,q.as_float(x.get("shares"),0.0)) for x in cross_levels)),"momentum_early_cross_match_level_count":len(cross_levels),"momentum_early_cross_match_effective_before_cutoff":cross_effective,"momentum_early_cross_match_blocked_by_cutoff":bool(cross_scheduled and not cross_effective),"momentum_early_cross_match_filled_shares":float(cross_fill),"momentum_early_cross_match_cost_usdc":float(cross_cost),"momentum_early_cross_match_average_price":cross_cost/cross_fill if cross_fill>0 else math.nan,"momentum_early_cross_match_levels_used":int(cross_levels_used),"momentum_early_cross_match_levels_json":json.dumps(cross_levels,separators=(",",":"),sort_keys=True)}

    evidence_ms=q.ns_to_ms(evidence_ns) if evidence_ns>=0 else math.nan; confirmation_ms=q.ns_to_ms(confirmation_ns) if confirmation_ns>=0 else math.nan
    ask_fill=0.0; below_fill=0.0
    confirmation_fields={"fill_confirmation_kind":"","fill_confirmation_ts_ns":confirmation_ns,"fill_confirmation_ts_ms":math.nan,"fill_confirmation_time_utc":"","fill_confirmation_signal_ts_ns":evidence_ns,"fill_confirmation_signal_ts_ms":math.nan,"fill_confirmation_signal_time_utc":"","fill_confirmation_effective_ts_ns":confirmation_ns,"fill_confirmation_effective_ts_ms":math.nan,"fill_confirmation_effective_time_utc":"","fill_confirmation_delay_ms":math.nan,"fill_confirmation_effective_before_cutoff":False,"fill_confirmation_blocked_by_cutoff":False,"ask99_appearance_ts_ns":-1,"ask99_appearance_ts_ms":math.nan,"ask99_appearance_time_utc":"","ask99_appearance_best_ask":math.nan,"ask99_appearance_best_ask_size":math.nan,"ask99_appearance_best_bid":math.nan,"ask99_appearance_best_bid_size":math.nan,"ask99_appearance_bid99_size":math.nan,"ask99_appearance_ask_crossed_limit":False,"ask99_appearance_executable_shares":0.0,"ask99_appearance_queue_before":math.nan,"ask99_appearance_queue_consumed":0.0,"ask99_appearance_queue_after":math.nan,"ask99_appearance_order_fill_shares":0.0,"below_limit_trade_ts_ns":-1,"below_limit_trade_ts_ms":math.nan,"below_limit_trade_time_utc":"","below_limit_trade_price":math.nan,"below_limit_trade_shares":math.nan,"below_limit_trade_aggressor_side":""}
    # Apply any remaining observed depth changes before the cancellation/market
    # cutoff so the final queue state and diagnostics are complete.  They cannot
    # create a fill without later execution evidence.
    if filled < requested - 1e-9 and cutoff_ns > open_ns:
        apply_queue_adjustments_until(cutoff_ns - 1)

    full_fill_ns=max((int(row.get("fill_ts_ns",-1)) for row in fill_events),default=-1) if filled>=requested-1e-9 else -1
    exact99_total=float(pd.to_numeric(exact_active.get("size"),errors="coerce").fillna(0).clip(lower=0).sum()) if isinstance(exact_active,pd.DataFrame) and not exact_active.empty else 0.0
    below_total=float(pd.to_numeric(trades_active.loc[pd.to_numeric(trades_active["price"],errors="coerce")<cfg.trigger_bid-cfg.trigger_tolerance-1e-12,"size"],errors="coerce").fillna(0).clip(lower=0).sum()) if isinstance(trades_active,pd.DataFrame) and not trades_active.empty else 0.0
    result = {
        "filled":float(filled),
        "queue_remaining":float(queue_remaining),
        "queue_behind_remaining":float(queue_behind),
        "fill_events":fill_events,
        "queue_trades":queue_trades,
        "full_fill_ts_ns":full_fill_ns,
        "full_fill_ts_ms":q.ns_to_ms(full_fill_ns) if full_fill_ns>=0 else math.nan,
        "exact99_trade_shares_processed":float(cumulative_exact99),
        "total_sell_trade_shares_at_99":exact99_total,
        "total_bought_shares_at_99":exact99_total,
        "total_trade_shares_below_limit":below_total,
        "total_price_priority_evidence_trade_shares_at_or_below_limit":exact99_total+below_total,
        "ask99_confirmation_fill_shares":float(ask_fill),
        "below_limit_price_priority_fill_shares":float(below_fill),
        "price_priority_confirmed_fill_shares":float(ask_fill+below_fill),
        **queue_reconstruction_plan,
        "queue_depth_reconstruction_applied_events":int(reconstruction_applied_events),
        "queue_depth_added_behind_shares":float(reconstruction_added_behind),
        "queue_depth_cancelled_behind_shares":float(reconstruction_cancelled_behind),
        "queue_depth_cancelled_ahead_shares":float(reconstruction_cancelled_ahead),
        **initial_cross_fields,
        **confirmation_fields,
    }
    if _FP_ON:
        fp_filled, fp_fills = _fp_simulate(side=side, open_ns=open_ns, cutoff_ns=cutoff_ns, requested=float(requested),
                                           causal_trades=causal_trades, cfg=cfg, pre_open_depth=pre_open_depth,
                                           contract_start_ms=contract_start_ms)
        cum = 0.0; fp_events = []
        for ts_ns, shares in fp_fills:
            cum += shares; ms = q.ns_to_ms(ts_ns)
            fp_events.append({"order_id": order_id, "contract_start_ms": contract_start_ms, "outcome": side, "fill_ts_ns": int(ts_ns),
                              "fill_ts_ms": ms, "fill_time_utc": q.market_iso(ms), "fill_shares": float(shares),
                              "fill_price": cfg.trigger_bid, "fill_source": "fillpar_orderlist_fifo", "cumulative_filled_shares": cum})
        full_ns = max((e["fill_ts_ns"] for e in fp_events), default=-1) if fp_filled >= float(requested) - 1e-9 else -1
        result.update({"filled": float(fp_filled), "fill_events": fp_events, "full_fill_ts_ns": int(full_ns),
                       "full_fill_ts_ms": q.ns_to_ms(full_ns) if full_ns >= 0 else math.nan, "fillpar_model": True,
                       "fifo_legacy_filled": float(filled)})
    if fill_cache is not None and fill_cache_key is not None:
        fill_cache[fill_cache_key] = {
            **result,
            "fill_events": [dict(row) for row in fill_events],
            "queue_trades": [dict(row) for row in queue_trades],
        }
    return result


def _bid99_binance_momentum_gate(
    *,
    trigger_ms: float,
    outcome: str,
    cfg: Config,
    underlying: q.UnderlyingMidSeries | None,
    current_lookup: dict[str, Any] | None,
    required_for_candidate: bool = True,
    trigger_source: str = "",
    trigger_ns: int | None = None,
) -> tuple[bool, dict[str, Any], str]:
    """Evaluate the direction-aware Binance momentum gate for one candidate.

    ``BID99_BINANCE_MOMENTUM_APPLY_TO_99=0`` bypasses this gate only for
    exact-0.99 activation (including the dedicated later exact-0.99 retry).
    Earlier momentum entries still require the same venue-time, delayed-
    observable lookback calculation.
    """
    configured = cfg.bid99_binance_momentum_configured
    required = bool(configured and required_for_candidate)
    side = str(outcome or "").strip().upper()
    source = str(trigger_source or "")
    threshold = float(cfg.momentum_threshold_for_trigger(source))
    defaults: dict[str, Any] = {
        "bid99_binance_momentum_configured": bool(configured),
        "bid99_binance_momentum_apply_to_99": bool(cfg.bid99_binance_momentum_apply_to_99),
        "bid99_binance_momentum_required_for_candidate": bool(required),
        "bid99_binance_momentum_evaluated": False,
        "bid99_binance_momentum_bypassed_for_exact_bid99": bool(
            configured and not required and source == "exact_bid99"
        ),
        # Backward-compatible field now means active for this candidate, not
        # merely configured globally.
        "bid99_binance_momentum_enabled": bool(required),
        "bid99_binance_momentum_trigger_source": source,
        "bid99_binance_momentum_outcome": side,
        "bid99_binance_momentum_required_direction": (
            "up" if side == "UP" else "down" if side == "DOWN" else ""
        ),
        "bid99_binance_momentum_min_usd": threshold,
        "bid99_binance_momentum_base_min_usd": float(cfg.bid99_binance_momentum_min_usd),
        "bid99_binance_momentum_early_min_usd": float(cfg.effective_early_momentum_min_usd),
        "bid99_binance_momentum_lookback_sec": float(cfg.bid99_binance_momentum_lookback_sec),
        "bid99_binance_momentum_reference_query_ts_ns": -1,
        "bid99_binance_momentum_reference_query_ts_ms": math.nan,
        "bid99_binance_momentum_reference_mid": math.nan,
        "bid99_binance_momentum_reference_venue_ts_ns": -1,
        "bid99_binance_momentum_reference_observed_ts_ns": -1,
        "bid99_binance_momentum_reference_venue_ts_ms": math.nan,
        "bid99_binance_momentum_reference_observed_ts_ms": math.nan,
        "bid99_binance_momentum_reference_observed_age_ms": math.nan,
        "bid99_binance_momentum_reference_series_index": -1,
        "bid99_binance_momentum_current_mid": math.nan,
        "bid99_binance_momentum_current_venue_ts_ns": -1,
        "bid99_binance_momentum_current_observed_ts_ns": -1,
        "bid99_binance_momentum_current_venue_ts_ms": math.nan,
        "bid99_binance_momentum_current_observed_ts_ms": math.nan,
        "bid99_binance_momentum_current_observed_age_ms": math.nan,
        "bid99_binance_momentum_current_series_index": -1,
        "bid99_binance_momentum_signed_move_usd": math.nan,
        "bid99_binance_momentum_directional_move_usd": math.nan,
        "bid99_binance_momentum_passed": math.nan,
    }
    if not required:
        return True, defaults, ""
    if underlying is None:
        raise RuntimeError(
            "enabled Binance momentum path requires the Binance venue-time midpoint index"
        )
    if side not in q.OUTCOMES:
        raise RuntimeError(f"enabled bid99 Binance momentum gate has invalid outcome={side!r}")
    evaluated = defaults | {"bid99_binance_momentum_evaluated": True}
    if current_lookup is None:
        return False, evaluated, "missing_or_stale_btc_trigger_mid"

    decision_ns = int(trigger_ns) if trigger_ns is not None else q.ms_to_ns(trigger_ms)
    reference_query_ns = decision_ns - int(round(float(cfg.bid99_binance_momentum_lookback_sec) * 1_000_000_000))
    reference_query_ms = q.ns_to_ms(reference_query_ns)
    reference_lookup = underlying.asof_observed_ns(
        reference_query_ns,
        cfg.btc_mid_max_age_ms,
        cfg.binance_venue_to_vps_delay_ms,
    )
    current_mid = float(current_lookup["price"])
    current_fields = {
        "bid99_binance_momentum_reference_query_ts_ns": reference_query_ns,
        "bid99_binance_momentum_reference_query_ts_ms": reference_query_ms,
        "bid99_binance_momentum_current_mid": current_mid,
        "bid99_binance_momentum_current_venue_ts_ns": int(current_lookup.get("venue_ts_ns", -1)),
        "bid99_binance_momentum_current_observed_ts_ns": int(current_lookup.get("observed_ts_ns", -1)),
        "bid99_binance_momentum_current_venue_ts_ms": float(current_lookup["venue_ts_ms"]),
        "bid99_binance_momentum_current_observed_ts_ms": float(current_lookup["observed_ts_ms"]),
        "bid99_binance_momentum_current_observed_age_ms": float(current_lookup["observed_age_ms"]),
        "bid99_binance_momentum_current_series_index": int(current_lookup["index"]),
    }
    if reference_lookup is None:
        return (
            False,
            evaluated | current_fields,
            "missing_or_stale_bid99_binance_momentum_reference",
        )

    reference_mid = float(reference_lookup["price"])
    signed_move = current_mid - reference_mid
    directional_move = signed_move if side == "UP" else -signed_move
    # Reject nominal equality in strict mode despite subtraction round-off.
    # Two input ULPs are far smaller than a tradable ETHUSDT/BTCUSDT price tick.
    boundary_tolerance = 2.0 * max(math.ulp(current_mid), math.ulp(reference_mid), math.ulp(threshold))
    passed = bool(directional_move > threshold + boundary_tolerance) if cfg.bid99_binance_momentum_strict_gt else bool(directional_move + 1e-12 >= threshold)
    fields = evaluated | current_fields | {
        "bid99_binance_momentum_reference_mid": reference_mid,
        "bid99_binance_momentum_reference_venue_ts_ns": int(reference_lookup.get("venue_ts_ns", -1)),
        "bid99_binance_momentum_reference_observed_ts_ns": int(reference_lookup.get("observed_ts_ns", -1)),
        "bid99_binance_momentum_reference_venue_ts_ms": float(reference_lookup["venue_ts_ms"]),
        "bid99_binance_momentum_reference_observed_ts_ms": float(reference_lookup["observed_ts_ms"]),
        "bid99_binance_momentum_reference_observed_age_ms": float(reference_lookup["observed_age_ms"]),
        "bid99_binance_momentum_reference_series_index": int(reference_lookup["index"]),
        "bid99_binance_momentum_signed_move_usd": float(signed_move),
        "bid99_binance_momentum_directional_move_usd": float(directional_move),
        "bid99_binance_momentum_passed": passed,
        "bid99_binance_momentum_comparison": ">" if cfg.bid99_binance_momentum_strict_gt else ">=",
    }
    return passed, fields, "" if passed else "bid99_binance_momentum_not_confirmed"


def _binance_mid_below_trigger_cancel_candidate(
    *, order: dict[str, Any], open_ns: int | None = None, market_end_ns: int | None = None,
    open_ms: float | None = None, market_end_ms: float | None = None,
    cfg: Config, underlying: q.UnderlyingMidSeries | None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """Build outcome-aware adverse-Binance cancellation on int64 nanoseconds."""
    if open_ns is None:
        if open_ms is None: raise ValueError("open_ns is required")
        open_ns=q.ms_to_ns(open_ms)
    if market_end_ns is None:
        if market_end_ms is None: raise ValueError("market_end_ns is required")
        market_end_ns=q.ms_to_ns(market_end_ms)
    open_ns=int(open_ns); market_end_ns=int(market_end_ns)
    outcome=str(order.get("outcome","UP")).strip().upper() or "UP"; direction="below_trigger" if outcome=="UP" else "above_trigger" if outcome=="DOWN" else ""
    enabled=bool(cfg.cancel_if_binance_mid_below_trigger); reference_mid=q.as_float(order.get("btc_trigger_mid"),math.nan); reference_index=q.as_int(order.get("btc_trigger_series_index"),-1)
    reference_venue_ns=int(order.get("btc_trigger_venue_ts_ns",-1) or -1); reference_observed_ns=int(order.get("btc_trigger_observed_ts_ns",-1) or -1)
    reference_venue_ms=q.ns_to_ms(reference_venue_ns) if reference_venue_ns>=0 else q.as_float(order.get("btc_trigger_venue_ts_ms"),math.nan)
    reference_observed_ms=q.ns_to_ms(reference_observed_ns) if reference_observed_ns>=0 else q.as_float(order.get("btc_trigger_observed_ts_ms"),math.nan)
    defaults={
        "binance_adverse_cancel_enabled":enabled,"binance_adverse_outcome":outcome,"binance_adverse_direction":direction,"binance_adverse_reference_mid":reference_mid,
        "binance_adverse_reference_venue_ts_ns":reference_venue_ns,"binance_adverse_reference_observed_ts_ns":reference_observed_ns,
        "binance_adverse_reference_venue_ts_ms":reference_venue_ms,"binance_adverse_reference_observed_ts_ms":reference_observed_ms,"binance_adverse_reference_series_index":reference_index,
        "binance_adverse_detected":False,"binance_adverse_detected_mid":math.nan,"binance_adverse_detected_venue_ts_ns":-1,"binance_adverse_detected_observed_ts_ns":-1,"binance_adverse_detected_venue_ts_ms":math.nan,"binance_adverse_detected_observed_ts_ms":math.nan,"binance_adverse_move_usd":math.nan,"binance_adverse_detected_before_order_open":False,"binance_adverse_cancel_scheduled":False,"binance_adverse_cancel_request_ts_ns":-1,"binance_adverse_cancel_request_ts_ms":math.nan,"binance_adverse_cancel_request_time_utc":"","binance_adverse_cancel_effective_ts_ns":-1,"binance_adverse_cancel_effective_ts_ms":math.nan,"binance_adverse_cancel_effective_time_utc":"","binance_adverse_cancel_delay_ms":math.nan,"binance_adverse_cancel_selected":False,
        "binance_mid_below_trigger_cancel_enabled":enabled,"binance_mid_below_trigger_reference_mid":reference_mid,"binance_mid_below_trigger_reference_venue_ts_ms":reference_venue_ms,"binance_mid_below_trigger_reference_observed_ts_ms":reference_observed_ms,"binance_mid_below_trigger_reference_series_index":reference_index,"binance_mid_below_trigger_detected":False,"binance_mid_below_trigger_detected_mid":math.nan,"binance_mid_below_trigger_detected_venue_ts_ms":math.nan,"binance_mid_below_trigger_detected_observed_ts_ms":math.nan,"binance_mid_below_trigger_drop_usd":math.nan,"binance_mid_below_trigger_detected_before_order_open":False,"binance_mid_below_trigger_cancel_scheduled":False,"binance_mid_below_trigger_cancel_request_ts_ms":math.nan,"binance_mid_below_trigger_cancel_request_time_utc":"","binance_mid_below_trigger_cancel_effective_ts_ms":math.nan,"binance_mid_below_trigger_cancel_effective_time_utc":"","binance_mid_below_trigger_cancel_delay_ms":math.nan,"binance_mid_below_trigger_cancel_selected":False,
    }
    if not enabled:return None,defaults
    if underlying is None:raise RuntimeError("CANCEL_IF_BINANCE_MID_ADVERSE_TO_OUTCOME=1 requires the Binance venue-time midpoint index")
    if outcome not in q.OUTCOMES:raise RuntimeError(f"enabled Binance adverse cancellation has invalid outcome={outcome!r}")
    trigger_ns=int(order.get("trigger_ts_ns",-1) or -1)
    if trigger_ns<0:
        trigger_ms=q.as_float(order.get("trigger_ts_ms"),math.nan); trigger_ns=q.ms_to_ns(trigger_ms) if math.isfinite(trigger_ms) else -1
    if not math.isfinite(reference_mid) or reference_index<0 or trigger_ns<0:raise RuntimeError("enabled Binance adverse cancellation is missing its trigger-time reference midpoint")
    common=dict(reference_price=reference_mid,after_index=reference_index,after_observed_ns=trigger_ns,end_observed_ns=market_end_ns,venue_to_vps_delay_ms=float(cfg.binance_venue_to_vps_delay_ms))
    if outcome=="UP":event=underlying.first_below_after_observed_ns(**common); adverse_move=q.as_float(event.get("drop_usd"),math.nan) if event else math.nan
    else:event=underlying.first_above_after_observed_ns(**common); adverse_move=q.as_float(event.get("rise_usd"),math.nan) if event else math.nan
    if event is None:return None,defaults
    observed_ns=int(event["observed_ts_ns"]); request_ns=max(open_ns,observed_ns); delay_ms=float(cfg.binance_mid_below_trigger_cancel_delay_ms) if cfg.binance_mid_below_trigger_cancel_delay_ms>=0 else float(cfg.paper_shadow_order_open_delay_ms); delay_ms=_vania_lat_sample_ms("CANCEL",(int(observed_ns)*1000003)^int(open_ns),delay_ms); delay_ns=_delay_ns(max(0.0,delay_ms)); effective_ns=request_ns+delay_ns
    detected_mid=float(event["price"]); before_open=observed_ns<open_ns
    request_ms=q.ns_to_ms(request_ns); effective_ms=q.ns_to_ms(effective_ns); observed_ms=q.ns_to_ms(observed_ns); venue_ns=int(event["venue_ts_ns"]); venue_ms=q.ns_to_ms(venue_ns)
    common_fields={"binance_adverse_detected":True,"binance_adverse_detected_mid":detected_mid,"binance_adverse_detected_venue_ts_ns":venue_ns,"binance_adverse_detected_observed_ts_ns":observed_ns,"binance_adverse_detected_venue_ts_ms":venue_ms,"binance_adverse_detected_observed_ts_ms":observed_ms,"binance_adverse_move_usd":float(adverse_move),"binance_adverse_detected_before_order_open":before_open,"binance_adverse_cancel_request_ts_ns":request_ns,"binance_adverse_cancel_request_ts_ms":request_ms,"binance_adverse_cancel_request_time_utc":q.market_iso(request_ms),"binance_adverse_cancel_effective_ts_ns":effective_ns,"binance_adverse_cancel_effective_ts_ms":effective_ms,"binance_adverse_cancel_effective_time_utc":q.market_iso(effective_ms),"binance_adverse_cancel_delay_ms":delay_ns/1e6,"binance_mid_below_trigger_detected":True,"binance_mid_below_trigger_detected_mid":detected_mid,"binance_mid_below_trigger_detected_venue_ts_ms":venue_ms,"binance_mid_below_trigger_detected_observed_ts_ms":observed_ms,"binance_mid_below_trigger_drop_usd":float(adverse_move),"binance_mid_below_trigger_detected_before_order_open":before_open,"binance_mid_below_trigger_cancel_request_ts_ms":request_ms,"binance_mid_below_trigger_cancel_request_time_utc":q.market_iso(request_ms),"binance_mid_below_trigger_cancel_effective_ts_ms":effective_ms,"binance_mid_below_trigger_cancel_effective_time_utc":q.market_iso(effective_ms),"binance_mid_below_trigger_cancel_delay_ms":delay_ns/1e6}
    fields=defaults|common_fields
    if request_ns>=market_end_ns:return None,fields
    fields["binance_adverse_cancel_scheduled"]=True; fields["binance_mid_below_trigger_cancel_scheduled"]=True
    candidate={"reason":"binance_mid_adverse_to_outcome","request_ts_ns":request_ns,"effective_ts_ns":effective_ns,"request_ts_ms":request_ms,"effective_ts_ms":effective_ms,"delay_ms":delay_ns/1e6,"observed_value":detected_mid,"threshold":reference_mid,"binance_outcome":outcome,"binance_adverse_direction":direction,"binance_venue_ts_ns":venue_ns,"binance_observed_ts_ns":observed_ns,"binance_venue_ts_ms":venue_ms,"binance_observed_ts_ms":observed_ms,"binance_adverse_move_usd":float(adverse_move)}
    return candidate,fields


def _apply_entry_retry_candidate_policy(
    rows: list[dict[str, Any]], *, cfg: Config
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Return BBO opportunities needed by the broad and dedicated retry models.

    ``ENTRY_RETRY_POLICY`` retains its established broad-policy meaning.  In addition,
    ``POST_ONLY_CROSS_RETRY_DELAY_MS >= 0`` keeps later exact-limit rows for the
    same outcome even when the broad policy is ``never``.  Those rows are only a
    candidate pool: the placement state machine may reach one of them only after
    a momentum-early attempt was actually rejected by post-only at its delayed
    placement timestamp.
    """
    ordered = sorted(
        (dict(row) for row in rows),
        key=lambda row: (
            _row_time_ns(row, "ts_ns", "ts_ms"),
            int(row.get("seq", 0)),
            str(row.get("side", "")),
        ),
    )
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, int, int]] = set()
    for row in ordered:
        key = (
            str(row.get("side", "")),
            _row_time_ns(row, "ts_ns", "ts_ms", 0),
            int(row.get("seq", 0)),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    dedicated_enabled = cfg.post_only_cross_retry_delay_ms >= 0
    if not deduped:
        return [], {
            "entry_retry_policy": cfg.entry_retry_policy,
            "entry_candidate_rows_before_retry_policy": 0,
            "entry_candidate_rows_after_retry_policy": 0,
            "entry_retry_locked_outcome": "",
            "entry_first_candidate_outcome": "",
            "post_only_cross_retry_enabled": dedicated_enabled,
            "post_only_cross_retry_candidate_pool_rows": 0,
        }

    def is_exact(row: dict[str, Any]) -> bool:
        if "entry_trigger_is_exact_bid99" in row:
            return bool(row.get("entry_trigger_is_exact_bid99"))
        bid = q.as_float(row.get("best_bid"), math.nan)
        return bool(
            math.isfinite(bid)
            and abs(bid - cfg.trigger_bid) <= cfg.trigger_tolerance + 1e-12
        )

    locked_outcome = ""
    dedicated_locked_outcome = ""
    selected: list[dict[str, Any]]
    if cfg.entry_retry_policy == "never":
        first_by_side: dict[str, dict[str, Any]] = {}
        for row in deduped:
            side = str(row.get("side", ""))
            if side in q.OUTCOMES and side not in first_by_side:
                first_by_side[side] = row
        per_side = sorted(
            first_by_side.values(),
            key=lambda row: (_row_time_ns(row, "ts_ns", "ts_ms"), int(row.get("seq", 0)), str(row["side"])),
        )
        if cfg.order_trigger_policy == "first_market":
            selected = per_side[:1]
            if selected and dedicated_enabled:
                first = selected[0]
                dedicated_locked_outcome = str(first.get("side", ""))
                first_key = (
                    str(first.get("side", "")),
                    _row_time_ns(first, "ts_ns", "ts_ms", 0),
                    int(first.get("seq", 0)),
                )
                selected.extend(
                    row for row in deduped
                    if str(row.get("side", "")) == dedicated_locked_outcome
                    and is_exact(row)
                    and (
                        str(row.get("side", "")),
                        _row_time_ns(row, "ts_ns", "ts_ms", 0),
                        int(row.get("seq", 0)),
                    ) != first_key
                )
        else:
            selected = list(per_side)
            if dedicated_enabled:
                first_keys = {
                    side: (
                        str(row.get("side", "")),
                        _row_time_ns(row, "ts_ns", "ts_ms", 0),
                        int(row.get("seq", 0)),
                    )
                    for side, row in first_by_side.items()
                }
                selected.extend(
                    row for row in deduped
                    if str(row.get("side", "")) in first_keys
                    and is_exact(row)
                    and (
                        str(row.get("side", "")),
                        _row_time_ns(row, "ts_ns", "ts_ms", 0),
                        int(row.get("seq", 0)),
                    ) != first_keys[str(row.get("side", ""))]
                )
    elif cfg.entry_retry_policy == "same_side" and cfg.order_trigger_policy == "first_market":
        locked_outcome = str(deduped[0].get("side", ""))
        selected = [row for row in deduped if str(row.get("side", "")) == locked_outcome]
    else:
        # any_side with first_market, or either broad retry mode with
        # first_per_outcome.
        selected = deduped

    # The dedicated pool can overlap the broad retry pool. Deduplicate after
    # expansion while retaining chronological order.
    selected = sorted(
        selected,
        key=lambda row: (
            _row_time_ns(row, "ts_ns", "ts_ms"),
            int(row.get("seq", 0)),
            str(row.get("side", "")),
        ),
    )
    unique_selected: list[dict[str, Any]] = []
    selected_seen: set[tuple[str, int, int]] = set()
    for row in selected:
        key = (
            str(row.get("side", "")),
            _row_time_ns(row, "ts_ns", "ts_ms", 0),
            int(row.get("seq", 0)),
        )
        if key in selected_seen:
            continue
        selected_seen.add(key)
        unique_selected.append(row)
    selected = unique_selected

    dedicated_pool_rows = 0
    for index, row in enumerate(selected, start=1):
        row["entry_candidate_index"] = index
        row["entry_retry_policy"] = cfg.entry_retry_policy
        row["entry_retry_locked_outcome"] = locked_outcome
        row["post_only_cross_retry_enabled"] = dedicated_enabled
        row["post_only_cross_retry_candidate_pool"] = bool(
            dedicated_enabled and is_exact(row)
        )
        if row["post_only_cross_retry_candidate_pool"]:
            dedicated_pool_rows += 1

    return selected, {
        "entry_retry_policy": cfg.entry_retry_policy,
        "entry_candidate_rows_before_retry_policy": len(deduped),
        "entry_candidate_rows_after_retry_policy": len(selected),
        "entry_retry_locked_outcome": locked_outcome,
        "entry_first_candidate_outcome": str(deduped[0].get("side", "")),
        "post_only_cross_retry_enabled": dedicated_enabled,
        "post_only_cross_retry_delay_ms": (
            float(cfg.post_only_cross_retry_delay_ms) if dedicated_enabled else math.nan
        ),
        "post_only_cross_retry_candidate_pool_rows": dedicated_pool_rows,
        "post_only_cross_retry_locked_outcome": dedicated_locked_outcome,
    }

def _missing_book_components(*, best_bid: float, best_bid_size: float, best_ask: float, best_ask_size: float) -> list[str]:
    """Return book components that are unavailable for a trading decision.

    Missing raw price/size sentinels decode to NaN.  A zero size remains a real
    zero and is therefore not considered missing.
    """
    missing: list[str] = []
    if not math.isfinite(float(best_bid)):
        missing.append("bid_price")
    if not math.isfinite(float(best_bid_size)):
        missing.append("bid_size")
    if not math.isfinite(float(best_ask)):
        missing.append("ask_price")
    if not math.isfinite(float(best_ask_size)):
        missing.append("ask_size")
    return missing


def _buy_limit_open_book_semantics(
    *,
    best_bid: float,
    best_bid_size: float,
    best_ask: float,
    best_ask_size: float,
    limit_price: float,
    tolerance: float,
) -> dict[str, Any]:
    """Interpret a canonical top-of-book snapshot for BUY-limit placement.

    Canonical raw price ``-1`` decodes to NaN and means *the level is absent*.
    It does not mean the entire book state is unknown.  Therefore:

    * no ask => nothing can cross the BUY at placement;
    * no bid => the BUY establishes a new bid and queue-ahead is zero;
    * bid size is required only when the existing best bid is exactly our
      limit and therefore forms FIFO queue ahead;
    * ask size is required only when the ask price is marketable against our
      limit and immediate/post-only crossing semantics depend on that size.

    A present price paired with a missing size remains unsafe whenever that
    size is actually needed.  This preserves the strict cache contract while
    avoiding the old false rejection of known-empty book sides.
    """
    bid = float(best_bid)
    ask = float(best_ask)
    bid_size = float(best_bid_size)
    ask_size = float(best_ask_size)
    limit = float(limit_price)
    tol = float(tolerance) + 1e-12

    bid_present = math.isfinite(bid)
    ask_present = math.isfinite(ask)
    bid_at_limit = bool(bid_present and abs(bid - limit) <= tol)
    ask_marketable = bool(ask_present and ask <= limit + tol)

    missing: list[str] = []
    if bid_at_limit and not math.isfinite(bid_size):
        missing.append("bid_size")
    if ask_marketable and not math.isfinite(ask_size):
        missing.append("ask_size")

    crossed = bool(
        ask_marketable
        and math.isfinite(ask_size)
        and ask_size > 0.0
    )
    raw_queue = (
        max(0.0, bid_size)
        if bid_at_limit and math.isfinite(bid_size)
        else 0.0
    )
    return {
        "missing_components": missing,
        "bid_present": bid_present,
        "ask_present": ask_present,
        "bid_at_limit": bid_at_limit,
        "ask_marketable": ask_marketable,
        "crossed": crossed,
        "raw_queue_shares": float(raw_queue),
        "absent_bid_allowed": not bid_present,
        "absent_ask_allowed": not ask_present,
    }


def _missing_signal_bbo_components(*, best_bid: float) -> list[str]:
    """Return BBO components actually required to form a price/momentum signal.

    Legacy and serialized entry signals are price-triggered.  They consume the
    causal best-bid price (plus Binance momentum when configured), but they do
    not consume BBO bid/ask sizes.  Queue size and executable ask liquidity are
    validated later against the causal depth snapshot at actual order-open time.

    This separation is important for caches whose BBO stream intentionally
    carries price-only observations with size=-1.  Missing optional BBO sizes
    are diagnostic information, not permission to trade on unknown liquidity.
    The order-open depth gate remains strict and still requires all four book
    components before an order can open.
    """
    return [] if math.isfinite(float(best_bid)) else ["bid_price"]


def _optional_signal_bbo_missing_components(
    *, best_bid: float, best_bid_size: float, best_ask: float, best_ask_size: float
) -> list[str]:
    blocking = set(_missing_signal_bbo_components(best_bid=best_bid))
    return [
        name for name in _missing_book_components(
            best_bid=best_bid, best_bid_size=best_bid_size,
            best_ask=best_ask, best_ask_size=best_ask_size,
        )
        if name not in blocking
    ]


def _missing_book_fields(stage: str, components: list[str]) -> dict[str, Any]:
    return {
        "blocked_by_missing_market_data": bool(components),
        "missing_market_data_stage": str(stage) if components else "",
        "missing_market_data_components": ",".join(components),
        "missing_market_data_component_count": len(components),
    }


def _update_missing_market_data_counters(
    market: dict[str, Any],
    rows: Iterable[Mapping[str, Any]],
    *,
    add_to_existing: bool = False,
) -> None:
    """Persist compact-safe counts for candidates blocked by missing book data."""
    stage = Counter()
    component = Counter()
    total = 0
    for row in rows:
        if not q.as_bool(row.get("blocked_by_missing_market_data", False)):
            continue
        total += 1
        st = str(row.get("missing_market_data_stage", "") or "unknown")
        stage[st] += 1
        for name in str(row.get("missing_market_data_components", "") or "").split(","):
            name = name.strip()
            if name:
                component[name] += 1
    if add_to_existing:
        total += q.as_int(market.get("candidate_trades_blocked_missing_market_data"), 0)
        stage.update({
            str(k): q.as_int(v, 0)
            for k, v in (market.get("blocked_missing_market_data_by_stage") or {}).items()
        })
        component.update({
            str(k): q.as_int(v, 0)
            for k, v in (market.get("blocked_missing_market_data_by_component") or {}).items()
        })
    market["candidate_trades_blocked_missing_market_data"] = int(total)
    market["blocked_missing_market_data_by_stage"] = dict(stage)
    market["blocked_missing_market_data_by_component"] = dict(component)


def _select_momentum_early_entry_candidates(
    rows: list[dict[str, Any]], *, cfg: Config, underlying: q.UnderlyingMidSeries | None
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Select retry-relevant BBO rows first, then evaluate Binance momentum.

    Candidate policy is resolved before any filter result, preserving the
    first-opportunity semantics of ``ENTRY_RETRY_POLICY=never``.  This ordering
    also makes disabled retry paths cheap: when broad retry and the dedicated
    post-only-cross retry are both disabled, only the first policy-selected BBO
    row (or first row per outcome) receives Binance lookback queries.
    """
    if cfg.bid99_binance_momentum_early_trigger_bid < 0:
        raise RuntimeError("early-entry selector called while disabled")
    if underlying is None:
        raise RuntimeError("momentum early entry requires Binance midpoint index")

    basic_rows: list[dict[str, Any]] = []
    for row in sorted(
        rows,
        key=lambda item: (
            _row_time_ns(item, "ts_ns", "ts_ms"),
            int(item.get("seq", 0)),
            str(item.get("side", "")),
        ),
    ):
        side = str(row.get("side", ""))
        if side not in q.OUTCOMES:
            continue
        observed_bid = q.as_float(row.get("best_bid"), math.nan)
        exact = bool(
            math.isfinite(observed_bid)
            and abs(observed_bid - cfg.trigger_bid)
            <= cfg.trigger_tolerance + 1e-12
        )
        chosen = dict(row)
        chosen.update({
            "entry_trigger_source": "exact_bid99" if exact else "momentum_early_bid",
            "entry_trigger_is_exact_bid99": exact,
            "entry_trigger_observed_best_bid": observed_bid,
            "entry_trigger_bid_floor": float(cfg.bid99_binance_momentum_early_trigger_bid),
        })
        basic_rows.append(chosen)

    selected, retry_meta = _apply_entry_retry_candidate_policy(basic_rows, cfg=cfg)
    rejected: Counter[str] = Counter()
    evaluated_rows: list[dict[str, Any]] = []
    for row in selected:
        event_ns = _row_time_ns(row, "ts_ns", "ts_ms")
        event_ms = q.ns_to_ms(event_ns)
        side = str(row["side"])
        is_exact = bool(row.get("entry_trigger_is_exact_bid99", False))
        momentum_required = cfg.momentum_required_for_trigger(is_exact_bid99=is_exact)
        current = (
            underlying.asof_observed_ns(
                event_ns, cfg.btc_mid_max_age_ms, cfg.binance_venue_to_vps_delay_ms
            )
            if momentum_required else None
        )
        passed, fields, reason = _bid99_binance_momentum_gate(
            trigger_ms=event_ms, trigger_ns=event_ns,
            outcome=side,
            cfg=cfg,
            underlying=underlying,
            current_lookup=current,
            required_for_candidate=momentum_required,
            trigger_source=str(row.get("entry_trigger_source", "")),
        )
        if not passed:
            rejected[str(reason or "momentum_not_confirmed")] += 1
        chosen = dict(row)
        chosen.update({
            "_momentum_evaluated": bool(fields.get("bid99_binance_momentum_evaluated", False)),
            "_momentum_required": bool(momentum_required),
            "_momentum_passed": bool(passed),
            "_momentum_skip_reason": str(reason or ""),
            "_trigger_lookup": current,
            "_momentum_fields": fields,
        })
        evaluated_rows.append(chosen)

    passed_selected = sum(
        bool(row.get("_momentum_passed")) and bool(row.get("_momentum_required"))
        for row in evaluated_rows
    )
    bypassed_exact = sum(
        not bool(row.get("_momentum_required"))
        and bool(row.get("entry_trigger_is_exact_bid99", False))
        for row in evaluated_rows
    )
    required_rows = sum(bool(row.get("_momentum_required")) for row in evaluated_rows)
    return evaluated_rows, {
        "early_trigger_bbo_rows": len(rows),
        "early_trigger_rows_before_retry_policy": len(basic_rows),
        "early_trigger_rows_momentum_evaluated": sum(bool(row.get("_momentum_evaluated")) for row in evaluated_rows),
        "exact_bid99_rows_momentum_bypassed": bypassed_exact,
        "momentum_required_candidate_rows": required_rows,
        "early_trigger_momentum_evaluation_pruned_rows": max(0, len(basic_rows) - len(evaluated_rows)),
        "early_trigger_momentum_rejection_reasons": dict(rejected),
        "entry_triggers_before_policy": len(basic_rows),
        "entry_triggers_after_policy": len(evaluated_rows),
        "entry_triggers_dropped_by_policy": max(0, len(basic_rows) - len(evaluated_rows)),
        "entry_selected_momentum_passed": passed_selected,
        "entry_selected_momentum_failed": max(0, required_rows - passed_selected),
        "entry_trigger_source_counts": dict(
            Counter(str(row.get("entry_trigger_source")) for row in evaluated_rows)
        ),
        **retry_meta,
    }

def _select_reached_entry_attempts(
    attempts: list[dict[str, Any]],
    *,
    depth: pd.DataFrame,
    cfg: Config,
    market_end_ms: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    """Apply entry chronology and validate the executable book at placement.

    The broad ``ENTRY_RETRY_POLICY`` behaves as before.  The dedicated
    ``POST_ONLY_CROSS_RETRY_DELAY_MS`` path is narrower: exactly one same-outcome
    retry may be submitted after a *momentum-early* attempt was rejected because
    post-only would cross.  The retry signal must be a later exact-0.99 BBO row,
    must occur no earlier than rejection + configured delay, and must already
    either pass a fresh directional Binance-momentum lookup at its own timestamp
    when ``BID99_BINANCE_MOMENTUM_APPLY_TO_99=1``, or bypass that exact-price
    lookup when the switch is off. Normal placement latency is then applied again
    and post-only is checked again.
    """
    market_end_ns = q.ms_to_ns(market_end_ms)
    ordered = sorted(
        (dict(row) for row in attempts),
        key=lambda row: (
            _row_time_ns(row, "trigger_ts_ns", "trigger_ts_ms"),
            int(row.get("entry_candidate_index", 0)),
            str(row.get("outcome", "")),
        ),
    )
    opened: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    opened_sides: set[str] = set()
    closed_sides: set[str] = set()
    # V43.18 serializer-pending research mode. A fixed-arm child is normally a
    # one-order engine. For serializer retry studies we keep later causal
    # same-outcome candidates after the first independently openable shadow
    # order, tag them as continuations, and let the outer one-slot serializer
    # decide whether any continuation is actually allowed to send.
    shadow_retry_enabled = bool(cfg.serialized_fixed_arm_retry_blocked_signals)
    shadow_retry_anchor_side = ""
    shadow_retry_continuation_rows = 0
    shadow_retry_other_side_suppressed = 0
    global_not_before_ns = -1
    side_not_before_ns = {side: -1 for side in q.OUTCOMES}
    first_candidate_outcome = str(ordered[0].get("outcome", "")) if ordered else ""
    locked_side = (
        first_candidate_outcome
        if ordered and cfg.entry_retry_policy == "same_side" and cfg.order_trigger_policy == "first_market"
        else ""
    )
    global_attempt_no = 0
    side_attempt_no: Counter[str] = Counter()
    suppressed_before_decision = 0
    suppressed_by_side_lock = 0
    suppressed_after_success = 0
    post_only_rejections = 0

    dedicated_enabled = cfg.post_only_cross_retry_delay_ms >= 0
    dedicated_states: dict[str, dict[str, Any]] = {
        side: {
            "armed": False,
            "consumed": False,
            "eligible_ts_ns": np.iinfo(np.int64).max,
            "eligible_ts_ms": math.inf,
            "parent_attempt_number": 0,
            "parent_order_id": "",
            "parent_reject_ts_ns": -1,
            "parent_reject_ts_ms": math.nan,
        }
        for side in q.OUTCOMES
    }
    dedicated_market_side = ""
    dedicated_armed = 0
    dedicated_attempts = 0
    dedicated_opened = 0
    dedicated_rejected_again = 0
    dedicated_suppressed_before_delay = 0
    dedicated_suppressed_other_side = 0
    dedicated_suppressed_non_exact = 0
    dedicated_revalidation_rejections = 0
    dedicated_revalidation_reasons: Counter[str] = Counter()

    def active_dedicated_state(side: str) -> dict[str, Any] | None:
        if not dedicated_enabled:
            return None
        if cfg.order_trigger_policy == "first_market":
            if not dedicated_market_side or side != dedicated_market_side:
                return None
            state = dedicated_states[dedicated_market_side]
            return state if state["armed"] and not state["consumed"] else None
        state = dedicated_states.get(side)
        return state if state and state["armed"] and not state["consumed"] else None

    for row in ordered:
        side = str(row.get("outcome", ""))
        trigger_ns = _row_time_ns(row, "trigger_ts_ns", "trigger_ts_ms")
        trigger_ms = q.ns_to_ms(trigger_ns)
        row["trigger_ts_ns"] = trigger_ns
        if side not in q.OUTCOMES:
            continue
        shadow_continuation = bool(
            shadow_retry_enabled
            and cfg.order_trigger_policy == "first_market"
            and bool(opened)
        )
        if cfg.order_trigger_policy == "first_market" and opened:
            if not shadow_retry_enabled:
                suppressed_after_success += 1
                continue
            if shadow_retry_anchor_side and side != shadow_retry_anchor_side:
                shadow_retry_other_side_suppressed += 1
                continue
            shadow_retry_continuation_rows += 1
        if cfg.order_trigger_policy != "first_market" and (side in opened_sides or side in closed_sides):
            suppressed_after_success += 1
            continue

        any_active_side = ""
        if cfg.order_trigger_policy == "first_market" and dedicated_market_side:
            state = dedicated_states[dedicated_market_side]
            if state["armed"] and not state["consumed"]:
                any_active_side = dedicated_market_side
        state = active_dedicated_state(side)
        dedicated_attempt = False

        if any_active_side and side != any_active_side:
            dedicated_suppressed_other_side += 1
            continue
        if state is not None:
            if trigger_ns < int(state["eligible_ts_ns"]):
                dedicated_suppressed_before_delay += 1
                continue
            if not bool(row.get("entry_trigger_is_exact_bid99", False)):
                dedicated_suppressed_non_exact += 1
                continue
            # When APPLY_TO_99 is enabled, revalidation is performed during
            # candidate preparation at this exact row's timestamp. With the
            # switch off, the exact-price row carries an explicit bypass. A
            # failed/stale required candidate is an observation, not a submitted
            # retry attempt, so continue searching.
            if str(row.get("status", "")) != "prepared":
                dedicated_revalidation_rejections += 1
                dedicated_revalidation_reasons[str(row.get("skip_reason", "") or row.get("status", "") or "unknown")] += 1
                continue
            dedicated_attempt = True
            state["consumed"] = True
            dedicated_attempts += 1
            row.update({
                "post_only_cross_retry_attempt": True,
                "post_only_cross_retry_parent_attempt_number": int(state["parent_attempt_number"]),
                "post_only_cross_retry_parent_order_id": str(state["parent_order_id"]),
                "post_only_cross_retry_parent_reject_ts_ns": int(state["parent_reject_ts_ns"]),
                "post_only_cross_retry_parent_reject_ts_ms": float(state["parent_reject_ts_ms"]),
                "post_only_cross_retry_parent_reject_time_utc": q.market_iso(float(state["parent_reject_ts_ms"])),
                "post_only_cross_retry_eligible_ts_ns": int(state["eligible_ts_ns"]),
                "post_only_cross_retry_eligible_ts_ms": float(state["eligible_ts_ms"]),
                "post_only_cross_retry_eligible_time_utc": q.market_iso(float(state["eligible_ts_ms"])),
                "post_only_cross_retry_signal_ts_ns": trigger_ns,
                "post_only_cross_retry_signal_ts_ms": trigger_ms,
                "post_only_cross_retry_signal_time_utc": q.market_iso(trigger_ms),
                "post_only_cross_retry_wait_elapsed_ms": trigger_ms - float(state["parent_reject_ts_ms"]),
                "post_only_cross_retry_after_eligibility_ms": trigger_ms - float(state["eligible_ts_ms"]),
                "post_only_cross_retry_requires_exact_bid99": True,
                "post_only_cross_retry_same_outcome": True,
                "post_only_cross_retry_momentum_required": bool(
                    row.get("bid99_binance_momentum_required_for_candidate", False)
                ),
                "post_only_cross_retry_momentum_revalidated": bool(
                    row.get("bid99_binance_momentum_evaluated", False)
                ),
                "post_only_cross_retry_momentum_bypassed_by_apply_to_99_off": bool(
                    row.get("bid99_binance_momentum_bypassed_for_exact_bid99", False)
                ),
            })
        else:
            if cfg.order_trigger_policy == "first_market":
                if locked_side and side != locked_side:
                    suppressed_by_side_lock += 1
                    continue
                not_before_ns = global_not_before_ns
            else:
                not_before_ns = side_not_before_ns[side]
            if trigger_ns < not_before_ns:
                suppressed_before_decision += 1
                continue

        optimizer_max_reached = int(getattr(cfg, "optimizer_max_reached_attempts", -1))
        if optimizer_max_reached > 0 and global_attempt_no >= optimizer_max_reached:
            break

        global_attempt_no += 1
        side_attempt_no[side] += 1
        row.update({
            "entry_attempt_reached": True,
            "entry_attempt_number": global_attempt_no,
            "entry_attempt_number_for_outcome": int(side_attempt_no[side]),
            "entry_retry_policy": cfg.entry_retry_policy,
            "entry_retry_locked_outcome": locked_side,
            "entry_first_candidate_outcome": first_candidate_outcome,
            "entry_retry_switched_outcome": bool(
                global_attempt_no > 1 and side != first_candidate_outcome
            ),
            "serialized_retry_shadow_continuation": bool(shadow_continuation),
            "serialized_retry_shadow_anchor_outcome": str(shadow_retry_anchor_side or side),
            "post_only_order": cfg.post_only_order_enabled,
            "post_only_rejected_at_placement": False,
            "order_was_opened": False,
            "post_only_cross_retry_enabled": dedicated_enabled,
            "post_only_cross_retry_delay_ms": (
                float(cfg.post_only_cross_retry_delay_ms) if dedicated_enabled else math.nan
            ),
            "post_only_cross_retry_attempt": bool(dedicated_attempt),
            "post_only_cross_retry_armed": False,
        })

        if str(row.get("status", "")) != "prepared":
            row.update({
                "entry_attempt_result": "filter_rejected",
                "entry_attempt_failure_stage": "pre_open_filter",
                "entry_attempt_decision_ts_ns": trigger_ns,
                "entry_attempt_decision_ts_ms": trigger_ms,
                "entry_attempt_decision_time_utc": q.market_iso(trigger_ms),
                "post_only_cross_retry_result": (
                    "retry_filter_rejected" if dedicated_attempt else "not_applicable"
                ),
            })
            rejected.append(row)
            decision_after_ns = trigger_ns + 1
            if dedicated_attempt and cfg.order_trigger_policy == "first_market":
                dedicated_market_side = ""
            if cfg.entry_retry_policy == "never":
                if cfg.order_trigger_policy == "first_market":
                    break
                closed_sides.add(side)
            elif cfg.order_trigger_policy == "first_market":
                global_not_before_ns = decision_after_ns
            else:
                side_not_before_ns[side] = decision_after_ns
            continue

        open_ns = _row_time_ns(row, "order_open_ts_ns", "order_open_ts_ms")
        open_ms = q.ns_to_ms(open_ns)
        row["order_open_ts_ns"] = open_ns
        failure_status = ""
        failure_reason = ""
        open_fields: dict[str, Any] = {}
        if open_ns >= market_end_ns:
            failure_status = "skipped_order_open_after_market_end"
            failure_reason = "order_open_after_market_end"
        else:
            snapshot = q.depth_asof_ns(
                depth, side, open_ns, strictly_before=True, max_age_ms=cfg.max_live_book_age_ms
            )
            if snapshot is None:
                failure_status = "skipped_missing_depth_state_at_open"
                failure_reason = "missing_depth_state_at_open"
            else:
                best_bid = q.as_float(snapshot.get("best_bid"), math.nan)
                best_ask = q.as_float(snapshot.get("best_ask"), math.nan)
                best_bid_size = q.as_float(snapshot.get("best_bid_size"), math.nan)
                best_ask_size = q.as_float(snapshot.get("best_ask_size"), math.nan)
                open_fields = {
                    "depth_state_ts_ns": int(snapshot["ts_ns"]),
                    "depth_state_ts_ms": snapshot["ts_ms"],
                    "depth_state_age_ms": snapshot["age_ms"],
                    "best_bid_at_open": best_bid,
                    "best_bid_size_at_open": best_bid_size,
                    "best_ask_at_open": best_ask,
                    "best_ask_size_at_open": best_ask_size,
                }
                open_semantics = _buy_limit_open_book_semantics(
                    best_bid=best_bid, best_bid_size=best_bid_size,
                    best_ask=best_ask, best_ask_size=best_ask_size,
                    limit_price=cfg.trigger_bid, tolerance=cfg.trigger_tolerance,
                )
                missing_open = list(open_semantics["missing_components"])
                open_fields.update({
                    **_missing_book_fields("order_open_depth", missing_open),
                    "order_open_bid_level_present": bool(open_semantics["bid_present"]),
                    "order_open_ask_level_present": bool(open_semantics["ask_present"]),
                    "order_open_absent_bid_allowed": bool(open_semantics["absent_bid_allowed"]),
                    "order_open_absent_ask_allowed": bool(open_semantics["absent_ask_allowed"]),
                })
                if missing_open:
                    failure_status = "skipped_missing_book_component_at_open"
                    failure_reason = "missing_book_component_at_open"
                elif math.isfinite(best_bid) and best_bid > cfg.trigger_bid + cfg.trigger_tolerance:
                    failure_status = "skipped_best_bid_above_limit_at_open"
                    failure_reason = "best_bid_above_limit_at_open"
                else:
                    crossed = bool(open_semantics["crossed"])
                    if cfg.post_only_order_enabled and crossed:
                        failure_status = "skipped_post_only_would_cross"
                        failure_reason = "post_only_would_cross"
                        post_only_rejections += 1

        if failure_status:
            qualifies_for_dedicated_retry = bool(
                dedicated_enabled
                and not dedicated_attempt
                and failure_reason == "post_only_would_cross"
                and str(row.get("entry_trigger_source", "")) == "momentum_early_bid"
                and not dedicated_states[side]["armed"]
                and not dedicated_states[side]["consumed"]
            )
            row.update({
                "status": failure_status,
                "skip_reason": failure_reason,
                **open_fields,
                "entry_attempt_result": "placement_rejected",
                "entry_attempt_failure_stage": "actual_placement",
                "entry_attempt_decision_ts_ns": open_ns,
                "entry_attempt_decision_ts_ms": open_ms,
                "entry_attempt_decision_time_utc": q.market_iso(open_ms),
                "post_only_rejected_at_placement": failure_reason == "post_only_would_cross",
                "post_only_reject_ts_ms": open_ms if failure_reason == "post_only_would_cross" else math.nan,
                "post_only_reject_time_utc": q.market_iso(open_ms) if failure_reason == "post_only_would_cross" else "",
                "momentum_early_cross_exception_used": False,
                "post_only_cross_retry_result": (
                    "retry_placement_rejected" if dedicated_attempt
                    else "armed" if qualifies_for_dedicated_retry
                    else "not_applicable"
                ),
            })
            if qualifies_for_dedicated_retry:
                eligible_ns = open_ns + _delay_ns(cfg.post_only_cross_retry_delay_ms)
                eligible_ms = q.ns_to_ms(eligible_ns)
                dedicated_states[side].update({
                    "armed": True,
                    "consumed": False,
                    "eligible_ts_ns": eligible_ns,
                    "eligible_ts_ms": eligible_ms,
                    "parent_attempt_number": global_attempt_no,
                    "parent_order_id": str(row.get("order_id", "")),
                    "parent_reject_ts_ns": open_ns,
                    "parent_reject_ts_ms": open_ms,
                })
                dedicated_armed += 1
                if cfg.order_trigger_policy == "first_market":
                    dedicated_market_side = side
                    global_not_before_ns = eligible_ns
                else:
                    side_not_before_ns[side] = eligible_ns
                row.update({
                    "post_only_cross_retry_armed": True,
                    "post_only_cross_retry_eligible_ts_ns": eligible_ns,
                    "post_only_cross_retry_eligible_ts_ms": eligible_ms,
                    "post_only_cross_retry_eligible_time_utc": q.market_iso(eligible_ms),
                    "post_only_cross_retry_parent_attempt_number": global_attempt_no,
                    "post_only_cross_retry_parent_order_id": str(row.get("order_id", "")),
                    "post_only_cross_retry_parent_reject_ts_ns": open_ns,
                    "post_only_cross_retry_parent_reject_ts_ms": open_ms,
                    "post_only_cross_retry_parent_reject_time_utc": q.market_iso(open_ms),
                })
                rejected.append(row)
                continue

            rejected.append(row)
            if dedicated_attempt:
                dedicated_rejected_again += 1
                if cfg.order_trigger_policy == "first_market":
                    dedicated_market_side = ""
            decision_after_ns = open_ns + 1
            if cfg.entry_retry_policy == "never":
                if cfg.order_trigger_policy == "first_market":
                    break
                closed_sides.add(side)
            elif cfg.order_trigger_policy == "first_market":
                global_not_before_ns = decision_after_ns
            else:
                side_not_before_ns[side] = decision_after_ns
            continue

        raw_open_audit = _causal_audit_fields(
            decision_ns=open_ns,
            sources={"source_depth": int(open_fields.get("depth_state_ts_ns", -1))},
            strict_prior={"source_depth"},
            enabled=cfg.strict_causal_audit,
        )
        open_audit = {f"order_open_{k}": v for k, v in raw_open_audit.items()}
        open_audit["depth_preprocessing_window_end_ns"] = int(open_ns)
        row.update({
            **open_fields,
            **open_audit,
            "entry_attempt_result": "opened",
            "entry_attempt_failure_stage": "",
            "entry_attempt_decision_ts_ms": open_ms,
            "entry_attempt_decision_time_utc": q.market_iso(open_ms),
            "order_was_opened": True,
            "post_only_rejected_at_placement": False,
            "post_only_reject_ts_ms": math.nan,
            "post_only_reject_time_utc": "",
            "post_only_cross_retry_result": (
                "retry_opened" if dedicated_attempt else "not_applicable"
            ),
            "_placement_validated": True,
        })
        if dedicated_attempt:
            dedicated_opened += 1
            if cfg.order_trigger_policy == "first_market":
                dedicated_market_side = ""
        opened.append(row)
        opened_sides.add(side)
        if not shadow_retry_anchor_side:
            shadow_retry_anchor_side = side
        if cfg.order_trigger_policy == "first_market" and not shadow_retry_enabled:
            break

    armed_not_attempted = sum(
        1 for state in dedicated_states.values()
        if state["armed"] and not state["consumed"]
    )
    return opened, rejected, {
        "entry_retry_policy": cfg.entry_retry_policy,
        "entry_retry_locked_outcome": locked_side,
        "entry_first_candidate_outcome": first_candidate_outcome,
        "entry_attempts_reached": global_attempt_no,
        "optimizer_max_reached_attempts": int(getattr(cfg, "optimizer_max_reached_attempts", -1)),
        "entry_retry_attempts": max(0, global_attempt_no - (1 if cfg.order_trigger_policy == "first_market" else min(2, len(side_attempt_no)))),
        "entry_attempts_suppressed_before_previous_decision": suppressed_before_decision,
        "entry_attempts_suppressed_by_same_side_lock": suppressed_by_side_lock,
        "entry_attempts_suppressed_after_success": suppressed_after_success,
        "serialized_retry_shadow_enabled": bool(shadow_retry_enabled),
        "serialized_retry_shadow_anchor_outcome": str(shadow_retry_anchor_side),
        "serialized_retry_shadow_continuation_rows": int(shadow_retry_continuation_rows),
        "serialized_retry_shadow_other_side_suppressed": int(shadow_retry_other_side_suppressed),
        "post_only_placement_rejections": post_only_rejections,
        "orders_opened_after_entry_attempts": len(opened),
        "post_only_cross_retry_enabled": dedicated_enabled,
        "post_only_cross_retry_delay_ms": (
            float(cfg.post_only_cross_retry_delay_ms) if dedicated_enabled else math.nan
        ),
        "post_only_cross_retry_armed": dedicated_armed,
        "post_only_cross_retry_attempts": dedicated_attempts,
        "post_only_cross_retry_opened_orders": dedicated_opened,
        "post_only_cross_retry_rejected_again": dedicated_rejected_again,
        "post_only_cross_retry_armed_without_qualifying_candidate": armed_not_attempted,
        "post_only_cross_retry_candidates_suppressed_before_delay": dedicated_suppressed_before_delay,
        "post_only_cross_retry_candidates_suppressed_other_side": dedicated_suppressed_other_side,
        "post_only_cross_retry_candidates_suppressed_non_exact": dedicated_suppressed_non_exact,
        "post_only_cross_retry_revalidation_rejections": dedicated_revalidation_rejections,
        "post_only_cross_retry_revalidation_rejection_reasons": dict(dedicated_revalidation_reasons),
    }


def _ladder_price(value: float, tick: float) -> float:
    if not math.isfinite(float(value)):
        return math.nan
    units = round(float(value) / float(tick))
    return float(round(units * float(tick), 10))


def _ladder_depth_arrays(depth: pd.DataFrame, side: str) -> dict[str, np.ndarray]:
    depth = _normalize_causal_frame_ns(depth)
    part = depth[depth["side"].eq(str(side))].sort_values(["ts_ns", "seq", "state_id"], kind="stable")
    return {
        "ts_ns": pd.to_numeric(part.get("ts_ns"), errors="coerce").to_numpy(dtype=np.int64),
        "bid": pd.to_numeric(part.get("best_bid"), errors="coerce").to_numpy(dtype=np.float64),
        "ask": pd.to_numeric(part.get("best_ask"), errors="coerce").to_numpy(dtype=np.float64),
        "bid_size": pd.to_numeric(part.get("best_bid_size"), errors="coerce").to_numpy(dtype=np.float64),
        "ask_size": pd.to_numeric(part.get("best_ask_size"), errors="coerce").to_numpy(dtype=np.float64),
        "seq": pd.to_numeric(part.get("seq"), errors="coerce").fillna(0).to_numpy(dtype=np.int64),
        "state_id": pd.to_numeric(part.get("state_id"), errors="coerce").fillna(0).to_numpy(dtype=np.int64),
    }


def _ladder_depth_snapshot(
    arrays: dict[str, np.ndarray], query_ns: int, *, max_age_ms: float, strictly_before: bool = True,
) -> dict[str, Any] | None:
    times = arrays["ts_ns"]
    if not len(times): return None
    pos = int(np.searchsorted(times, int(query_ns), side="left" if strictly_before else "right") - 1)
    if pos < 0: return None
    ts_ns = int(times[pos]); age_ns = int(query_ns) - ts_ns
    if age_ns < 0 or (max_age_ms >= 0 and age_ns > _delay_ns(max_age_ms)): return None
    return {
        "index": pos, "ts_ns": ts_ns, "ts_ms": q.ns_to_ms(ts_ns), "age_ns": age_ns, "age_ms": age_ns/1_000_000.0,
        "best_bid": float(arrays["bid"][pos]), "best_ask": float(arrays["ask"][pos]),
        "best_bid_size": float(arrays["bid_size"][pos]), "best_ask_size": float(arrays["ask_size"][pos]),
        "seq": int(arrays["seq"][pos]), "state_id": int(arrays["state_id"][pos]),
    }


def _ladder_momentum_events(*, arrays: dict[str,np.ndarray], side: str, task: q.ContractTask, market_end_ms: float, cfg: Config, underlying: q.UnderlyingMidSeries) -> dict[str,np.ndarray]:
    """Vectorized ladder features on a single authoritative int64-ns clock."""
    start_ns = q.ms_to_ns(task.contract_start_ms) + _delay_ns(cfg.entry_market_age_start_sec*1000.0)
    end_ns = min(q.ms_to_ns(market_end_ms), q.ms_to_ns(task.contract_start_ms)+_delay_ns(cfg.entry_market_age_end_sec*1000.0))
    delay_ns=_delay_ns(cfg.binance_venue_to_vps_delay_ms); lookback_ns=_delay_ns(cfg.bid99_binance_momentum_lookback_sec*1000.0)
    venue=np.asarray(underlying.times_ns,dtype=np.int64); prices=np.asarray(underlying.prices,dtype=np.float64)
    left=int(np.searchsorted(venue,start_ns-delay_ns,side="left")); right=int(np.searchsorted(venue,end_ns-delay_ns,side="right"))
    observed=venue[left:right]+delay_ns if right>left else np.empty(0,dtype=np.int64)
    depth_times=arrays["ts_ns"]; dmask=(depth_times>=start_ns)&(depth_times<=end_ns)
    clocks=np.unique(np.concatenate([depth_times[dmask],observed])).astype(np.int64,copy=False)
    if not len(clocks):
        return {"ts_ns":np.empty(0,dtype=np.int64),"bid":np.empty(0),"mid":np.empty(0),"reference_mid":np.empty(0),"directional_move":np.empty(0),"mid_index":np.empty(0,dtype=np.int64),"mid_observed_ns":np.empty(0,dtype=np.int64),"reference_observed_ns":np.empty(0,dtype=np.int64),"missing_book_candidate_count":0,"missing_book_by_component":{},"optional_missing_book_candidate_count":0,"optional_missing_book_by_component":{}}
    dpos=np.searchsorted(depth_times,clocks,side="right")-1; depth_ok=dpos>=0
    bids=np.full(len(clocks),np.nan); asks=np.full(len(clocks),np.nan)
    bid_sizes=np.full(len(clocks),np.nan); ask_sizes=np.full(len(clocks),np.nan)
    bids[depth_ok]=arrays["bid"][dpos[depth_ok]]
    asks[depth_ok]=arrays["ask"][dpos[depth_ok]]
    bid_sizes[depth_ok]=arrays["bid_size"][dpos[depth_ok]]
    ask_sizes[depth_ok]=arrays["ask_size"][dpos[depth_ok]]
    cur_pos=np.searchsorted(venue,clocks-delay_ns,side="right")-1
    ref_query=clocks-lookback_ns; ref_pos=np.searchsorted(venue,ref_query-delay_ns,side="right")-1
    valid=depth_ok&(cur_pos>=0)&(ref_pos>=0); cur=np.full(len(clocks),np.nan); ref=np.full(len(clocks),np.nan)
    cur_obs=np.full(len(clocks),-1,dtype=np.int64); ref_obs=np.full(len(clocks),-1,dtype=np.int64)
    if valid.any():
        idx=np.flatnonzero(valid); cur[idx]=prices[cur_pos[idx]]; ref[idx]=prices[ref_pos[idx]]
        cur_obs[idx]=venue[cur_pos[idx]]+delay_ns; ref_obs[idx]=venue[ref_pos[idx]]+delay_ns
        cur_age=clocks[idx]-cur_obs[idx]; ref_age=ref_query[idx]-ref_obs[idx]
        age_ok=(cur_age>=0)&(ref_age>=0)&((cfg.btc_mid_max_age_ms<0)|(cur_age<=_delay_ns(cfg.btc_mid_max_age_ms)))&((cfg.btc_mid_max_age_ms<0)|(ref_age<=_delay_ns(cfg.btc_mid_max_age_ms)))
        valid[idx[~age_ok]]=False
    directional=(cur-ref) if str(side).upper()=="UP" else -(cur-ref)
    valid &= np.isfinite(cur)&np.isfinite(ref)&np.isfinite(directional)
    valid &= directional+1e-12>=float(cfg.bid99_binance_momentum_min_usd)
    valid &= bids>=float(cfg.momentum_ladder_start_bid)-cfg.trigger_tolerance-1e-12
    valid &= bids<=float(cfg.momentum_ladder_max_bid)+cfg.trigger_tolerance+1e-12
    # Signal-time ladder logic is price + BTC-momentum based.  Canonical -1
    # on the opposite/size components means that level/component is absent,
    # not that the bid-price signal itself is unknowable.  Do not require a
    # complete four-component book here.  Queue/cross sizes are validated
    # later, at the actual placement timestamp, by
    # _buy_limit_open_book_semantics().
    pre_book_valid = valid.copy()
    optional_component_ok = {
        "bid_size": np.isfinite(bid_sizes),
        "ask_price": np.isfinite(asks),
        "ask_size": np.isfinite(ask_sizes),
    }
    optional_missing_mask = np.zeros(len(clocks), dtype=bool)
    for mask in optional_component_ok.values():
        optional_missing_mask |= ~mask
    optional_missing_book_candidate_count = int(np.sum(pre_book_valid & optional_missing_mask))
    optional_missing_book_by_component = {
        name: int(np.sum(pre_book_valid & ~mask))
        for name, mask in optional_component_ok.items()
        if int(np.sum(pre_book_valid & ~mask)) > 0
    }
    # No signal candidate is blocked solely because bid/ask sizes or the ask
    # level are absent.  A missing bid price already fails the bid-range test
    # above and therefore cannot form a signal.
    missing_book_candidate_count = 0
    missing_book_by_component: dict[str, int] = {}
    idx=np.flatnonzero(valid)
    if cfg.strict_causal_audit and len(idx):
        if np.any(cur_obs[idx]>clocks[idx]) or np.any(ref_obs[idx]>=clocks[idx]):
            raise InternalContractError("STRICT_CAUSAL_AUDIT ladder Binance feature timestamp violation")
    return {"ts_ns":clocks[idx],"bid":bids[idx],"mid":cur[idx],"reference_mid":ref[idx],"directional_move":directional[idx],"mid_index":cur_pos[idx].astype(np.int64),"mid_observed_ns":cur_obs[idx],"reference_observed_ns":ref_obs[idx],"missing_book_candidate_count":missing_book_candidate_count,"missing_book_by_component":missing_book_by_component,"optional_missing_book_candidate_count":optional_missing_book_candidate_count,"optional_missing_book_by_component":optional_missing_book_by_component}


def _next_ladder_signal(events: dict[str,np.ndarray], *, after_ns: int | None = None, after_ms: float | None = None, anchor_bid: float, cfg: Config) -> dict[str,Any] | None:
    if after_ns is None:
        if after_ms is None: raise ValueError("after_ns is required")
        after_ns=q.ms_to_ns(after_ms)
    times=events.get("ts_ns")
    if times is None and "ts" in events:
        # Compatibility for pre-V43 synthetic tests only; V43 production events
        # always carry authoritative ts_ns. Convert once, then order in ns.
        times=q.milliseconds_to_ns_array(pd.Series(np.asarray(events["ts"])), field="legacy_ladder_ts_ms")
    if times is None: times=np.empty(0,dtype=np.int64)
    if not len(times) or anchor_bid>=cfg.momentum_ladder_max_bid-cfg.trigger_tolerance: return None
    pos=int(np.searchsorted(times,int(after_ns),side="right")); tick=float(cfg.momentum_ladder_tick)
    for i in range(pos,len(times)):
        bid=float(events["bid"][i])
        if bid<float(anchor_bid)-cfg.trigger_tolerance-1e-12 or bid>float(cfg.momentum_ladder_max_bid)+cfg.trigger_tolerance+1e-12: continue
        next_tick=_ladder_price(float(anchor_bid)+tick,tick); observed=_ladder_price(bid,tick); target=min(_ladder_price(cfg.momentum_ladder_max_bid,tick),max(next_tick,observed))
        if target<=float(anchor_bid)+cfg.trigger_tolerance: continue
        ts_ns=int(times[i])
        return {"signal_ts_ns":ts_ns,"signal_ts_ms":q.ns_to_ms(ts_ns),"observed_bid":bid,"target_price":float(target),"binance_mid":float(events["mid"][i]),"binance_reference_mid":float(events["reference_mid"][i]),"binance_directional_move_usd":float(events["directional_move"][i]),"binance_mid_index":int(events["mid_index"][i]),"btc_feature_ts_ns":int(events.get("mid_observed_ns",times)[i]),"prior_feature_ts_ns":int(events.get("reference_observed_ns",times)[i])}
    return None

def _ladder_place_with_retries(
    *, task: q.ContractTask, cfg: Config, arrays: dict[str,np.ndarray], side: str,
    order_id: str, signal: dict[str,Any], anchor_bid: float, requested: float,
    not_before_ns: int | None = None, market_end_ns: int | None = None, sequence: int,
    not_before_ms: float | None = None, market_end_ms: float | None = None,
) -> dict[str,Any]:
    if not_before_ns is None:
        if not_before_ms is None: raise ValueError("not_before_ns is required")
        not_before_ns=q.ms_to_ns(not_before_ms)
    if market_end_ns is None:
        if market_end_ms is None: raise ValueError("market_end_ns is required")
        market_end_ns=q.ms_to_ns(market_end_ms)
    if "signal_ts_ns" not in signal:
        if "signal_ts_ms" not in signal: raise ValueError("signal requires signal_ts_ns")
        signal=dict(signal); signal["signal_ts_ns"]=q.ms_to_ns(signal["signal_ts_ms"])
        signal.setdefault("btc_feature_ts_ns",int(signal["signal_ts_ns"])-1)
        signal.setdefault("prior_feature_ts_ns",int(signal["signal_ts_ns"])-1)
    target=float(signal["target_price"])
    base_delay_ns=_delay_ns(float(cfg.wait_after_bid99_sec)*1000.0+float(cfg.paper_signal_snapshot_delay_ms)+float(cfg.paper_send_start_delay_ms)+float(cfg.paper_shadow_order_open_delay_ms))
    attempt_signal_ns=max(int(signal["signal_ts_ns"]),int(not_before_ns)); retry_ns=_delay_ns(cfg.momentum_ladder_post_only_retry_ms)
    market_end_ns=int(market_end_ns); stop_bid=float(anchor_bid)-float(cfg.momentum_ladder_stop_drop_ticks)*float(cfg.momentum_ladder_tick)
    attempts=rejects=0; first_attempt_open_ns=-1; last_snapshot=None; stop_reason=""
    while attempts<int(cfg.momentum_ladder_max_placement_attempts):
        attempts+=1; open_ns=attempt_signal_ns+base_delay_ns
        if first_attempt_open_ns<0: first_attempt_open_ns=open_ns
        if open_ns>=market_end_ns: stop_reason="order_open_after_market_end"; break
        snapshot=_ladder_depth_snapshot(arrays,open_ns,max_age_ms=float(cfg.max_live_book_age_ms),strictly_before=True)
        if snapshot is None: stop_reason="missing_depth_state_at_open"; break
        last_snapshot=snapshot; best_bid=float(snapshot["best_bid"]); best_ask=float(snapshot["best_ask"]); best_bid_size=float(snapshot["best_bid_size"]); best_ask_size=float(snapshot["best_ask_size"])
        open_semantics = _buy_limit_open_book_semantics(
            best_bid=best_bid, best_bid_size=best_bid_size,
            best_ask=best_ask, best_ask_size=best_ask_size,
            limit_price=target, tolerance=cfg.trigger_tolerance,
        )
        missing_open = list(open_semantics["missing_components"])
        if missing_open:
            stop_reason="missing_book_component_at_open"
            break
        if math.isfinite(best_bid) and best_bid<=stop_bid+cfg.trigger_tolerance+1e-12: stop_reason="ladder_retry_stop_bid_dropped"; break
        if math.isfinite(best_bid) and best_bid>target+cfg.trigger_tolerance+1e-12: stop_reason="ladder_target_below_best_at_open"; break
        crossed=bool(open_semantics["crossed"])
        if crossed and cfg.post_only_order_enabled:
            rejects+=1; attempt_signal_ns=open_ns+retry_ns; continue
        raw_queue=float(open_semantics["raw_queue_shares"])
        queue_ahead=raw_queue*float(cfg.queue_size_multiplier)
        raw_open_audit=_causal_audit_fields(decision_ns=open_ns,sources={"source_depth":int(snapshot["ts_ns"]),"btc_feature":int(signal.get("btc_feature_ts_ns",-1)),"prior_feature":int(signal.get("prior_feature_ts_ns",-1))},strict_prior={"source_depth","prior_feature"},enabled=cfg.strict_causal_audit)
        open_audit={f"order_open_{k}":v for k,v in raw_open_audit.items()}
        open_audit["depth_preprocessing_window_end_ns"]=int(open_ns)
        return {"opened":True,"order_id":order_id,"order_open_ts_ns":open_ns,"order_open_ts_ms":q.ns_to_ms(open_ns),"order_open_time_utc":q.market_iso(q.ns_to_ms(open_ns)),"requested_shares":float(requested),"limit_price":target,"queue_ahead_shares":queue_ahead,"queue_ahead_raw_shares":raw_queue,"placement_attempts":attempts,"post_only_rejections_before_open":rejects,"first_placement_open_ts_ns":first_attempt_open_ns,"first_placement_open_ts_ms":q.ns_to_ms(first_attempt_open_ns),"last_placement_signal_ts_ns":attempt_signal_ns,"last_placement_signal_ts_ms":q.ns_to_ms(attempt_signal_ns),"depth_state_ts_ns":int(snapshot["ts_ns"]),"depth_state_ts_ms":float(snapshot["ts_ms"]),"depth_state_age_ms":float(snapshot["age_ms"]),"best_bid_at_open":best_bid,"best_bid_size_at_open":float(snapshot["best_bid_size"]),"best_ask_at_open":best_ask,"best_ask_size_at_open":best_ask_size,"order_open_bid_level_present":bool(open_semantics["bid_present"]),"order_open_ask_level_present":bool(open_semantics["ask_present"]),"order_open_absent_bid_allowed":bool(open_semantics["absent_bid_allowed"]),"order_open_absent_ask_allowed":bool(open_semantics["absent_ask_allowed"]),"crossed_at_open":crossed,"stop_reason":"",**open_audit}
    snapshot=last_snapshot or {}; decision_ns=int(snapshot.get("ts_ns",attempt_signal_ns)) if stop_reason=="missing_depth_state_at_open" else min(market_end_ns,attempt_signal_ns+base_delay_ns); decision_ms=q.ns_to_ms(decision_ns)
    return {"opened":False,"order_id":order_id,"order_open_ts_ns":decision_ns,"order_open_ts_ms":decision_ms,"order_open_time_utc":q.market_iso(decision_ms),"requested_shares":float(requested),"limit_price":target,"queue_ahead_shares":0.0,"queue_ahead_raw_shares":0.0,"placement_attempts":attempts,"post_only_rejections_before_open":rejects,"first_placement_open_ts_ns":first_attempt_open_ns,"first_placement_open_ts_ms":q.ns_to_ms(first_attempt_open_ns) if first_attempt_open_ns>=0 else math.nan,"last_placement_signal_ts_ns":attempt_signal_ns,"last_placement_signal_ts_ms":q.ns_to_ms(attempt_signal_ns),"depth_state_ts_ns":int(snapshot.get("ts_ns",-1)),"depth_state_ts_ms":q.as_float(snapshot.get("ts_ms"),math.nan),"depth_state_age_ms":q.as_float(snapshot.get("age_ms"),math.nan),"best_bid_at_open":q.as_float(snapshot.get("best_bid"),math.nan),"best_bid_size_at_open":q.as_float(snapshot.get("best_bid_size"),math.nan),"best_ask_at_open":q.as_float(snapshot.get("best_ask"),math.nan),"best_ask_size_at_open":q.as_float(snapshot.get("best_ask_size"),math.nan),"crossed_at_open":False,"stop_reason":stop_reason or "ladder_post_only_retry_limit"}

def _ladder_causal_trades(
    trades: pd.DataFrame, *, side: str, open_ns: int, end_ns: int, limit_price: float, cfg: Config,
) -> pd.DataFrame:
    """Return only trade observations causally available during an order lifetime."""
    if trades is None or trades.empty:
        return pd.DataFrame(columns=list(trades.columns) if isinstance(trades, pd.DataFrame) else [])
    trades = _normalize_causal_frame_ns(trades)
    price = pd.to_numeric(trades.get("price"), errors="coerce")
    selected = trades[(trades["outcome"] == side) & (price <= limit_price + cfg.trigger_tolerance + 1e-12)].copy()
    if selected.empty:
        return selected
    clock_ns = pd.to_numeric(selected["clock_ns"], errors="raise").astype("int64")
    selected = selected[clock_ns >= int(open_ns)] if cfg.include_same_timestamp_trades else selected[clock_ns > int(open_ns)]
    if selected.empty:
        return selected
    selected = selected[pd.to_numeric(selected["clock_ns"], errors="raise").astype("int64") < int(end_ns)]
    if cfg.trade_clock_mode == "venue_with_local_guard" and not selected.empty:
        local_ns = pd.to_numeric(selected["local_ts_ns"], errors="raise").astype("int64")
        selected = selected[local_ns >= int(open_ns)] if cfg.include_same_timestamp_trades else selected[local_ns > int(open_ns)]
        if not selected.empty:
            selected = selected[pd.to_numeric(selected["local_ts_ns"], errors="raise").astype("int64") < int(end_ns)]
    if not selected.empty:
        selected.sort_values(["clock_ns", "local_ts_ns", "seq", "state_id"], inplace=True, kind="stable")
    return selected


def _ladder_simulate_fills(
    *,
    order_id: str,
    contract_start_ms: int,
    side: str,
    open_ns: int,
    cutoff_ns: int,
    requested: float,
    queue_ahead: float,
    limit_price: float,
    side_arrays: dict[str, np.ndarray],
    trades: pd.DataFrame,
    cfg: Config,
    initial_cross_levels: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Only exact-limit trade FIFO; no ask snapshot or initial-cross fills."""
    if initial_cross_levels:
        raise ValueError("FIFO-only execution does not accept initial ask-cross levels")
    open_ns = int(open_ns); cutoff_ns = int(cutoff_ns)
    trades = _normalize_causal_frame_ns(trades)
    if cutoff_ns <= open_ns:
        return {
            "filled": 0.0, "queue_remaining": max(0.0, float(queue_ahead)),
            "fill_events": [], "queue_trades": [],
            "first_fill_ts_ns": -1, "full_fill_ts_ns": -1,
            "first_fill_ts_ms": math.nan, "full_fill_ts_ms": math.nan,
        }
    causal = _ladder_causal_trades(
        trades, side=side, open_ns=open_ns, end_ns=cutoff_ns, limit_price=limit_price, cfg=cfg
    )
    exact = causal[
        np.abs(pd.to_numeric(causal.get("price"), errors="coerce") - limit_price)
        <= cfg.trigger_tolerance + 1e-12
    ].copy() if not causal.empty else causal
    if not exact.empty:
        if bool(exact["side_conflict"].any()):
            raise RuntimeError(f"conflicting trade-side columns for ladder exact-limit trades: {order_id}")
        unresolved = exact[~exact["aggressor_side"].isin(["BUY", "SELL"])]
        if not unresolved.empty and not cfg.allow_all_trades_when_side_missing:
            raise RuntimeError(f"unresolved side for {len(unresolved)} ladder exact-limit trades: {order_id}")
        exact = exact[
            (exact["aggressor_side"] == "SELL")
            | (~exact["aggressor_side"].isin(["BUY", "SELL"]) & cfg.allow_all_trades_when_side_missing)
        ].copy()

    # (effective_ns, priority, deterministic_tie, event_kind, payload)
    events: list[tuple[int, int, tuple[int, int], str, dict[str, Any]]] = []
    for row in exact.to_dict("records") if isinstance(exact, pd.DataFrame) else []:
        ts_ns = int(row.get("clock_ns", -1))
        if open_ns <= ts_ns < cutoff_ns:
            tie = (int(row.get("seq", 0) or 0), int(row.get("state_id", 0) or 0))
            events.append((ts_ns, 1, tie, "exact", row))

    events.sort(key=lambda x: (x[0], x[1], x[2]))
    queue = max(0.0, float(queue_ahead)); filled = 0.0
    fill_events: list[dict[str, Any]] = []; queue_trades: list[dict[str, Any]] = []
    first_fill_ns = -1; full_fill_ns = -1

    def add_fill(ts_ns: int, amount: float, price: float, source: str, **extra: Any) -> None:
        nonlocal filled, first_fill_ns, full_fill_ns
        amount = min(max(0.0, requested - filled), max(0.0, amount))
        if amount <= 0:
            return
        filled += amount
        ts_ms = q.ns_to_ms(ts_ns)
        fill_events.append({
            "order_id": order_id, "contract_start_ms": contract_start_ms, "outcome": side,
            "fill_ts_ns": int(ts_ns), "fill_ts_ms": ts_ms, "fill_time_utc": q.market_iso(ts_ms),
            "fill_shares": amount, "fill_price": float(price), "fill_source": source,
            "cumulative_filled_shares": filled, **extra,
        })
        if first_fill_ns < 0: first_fill_ns = int(ts_ns)
        if filled >= requested - 1e-9: full_fill_ns = int(ts_ns)

    for ts_ns, _priority, _tie, kind, row in events:
        if filled >= requested - 1e-9:
            break
        if ts_ns >= cutoff_ns:
            break
        if kind == "exact":
            qty = max(0.0, q.as_float(row.get("size"), 0.0)); queue_before = queue
            consumed = min(queue, qty); queue -= consumed
            amount = min(max(0.0, requested - filled), max(0.0, qty - consumed))
            add_fill(ts_ns, amount, limit_price, "ladder_trade_fifo_at_limit")
            queue_trades.append({
                "order_id": order_id, "contract_start_ms": contract_start_ms, "outcome": side,
                "trade_ts_ns": int(ts_ns), "trade_ts_ms": q.ns_to_ms(ts_ns),
                "trade_price": limit_price, "trade_shares": qty,
                "queue_before": queue_before, "queue_consumed": consumed, "queue_after": queue,
                "fill_shares": amount, "fill_after": filled, "aggressor_side": str(row.get("aggressor_side", "")),
                "side_source": str(row.get("side_source", "")), "ladder_limit_price": limit_price,
            })
    return {
        "filled": float(filled), "queue_remaining": float(queue),
        "fill_events": fill_events, "queue_trades": queue_trades,
        "first_fill_ts_ns": int(first_fill_ns), "full_fill_ts_ns": int(full_fill_ns),
        "first_fill_ts_ms": q.ns_to_ms(first_fill_ns) if first_fill_ns >= 0 else math.nan,
        "full_fill_ts_ms": q.ns_to_ms(full_fill_ns) if full_fill_ns >= 0 else math.nan,
    }


def _ladder_adverse_cancel(
    *, side: str, signal: dict[str, Any], open_ns: int, market_end_ns: int,
    cfg: Config, underlying: q.UnderlyingMidSeries,
) -> dict[str, Any] | None:
    if not cfg.cancel_if_binance_mid_below_trigger:
        return None
    kwargs = dict(
        reference_price=float(signal["binance_mid"]),
        after_index=int(signal["binance_mid_index"]),
        after_observed_ns=int(signal["signal_ts_ns"]),
        end_observed_ns=int(market_end_ns),
        venue_to_vps_delay_ms=float(cfg.binance_venue_to_vps_delay_ms),
    )
    adverse = (
        underlying.first_below_after_observed_ns(**kwargs)
        if str(side).upper() == "UP"
        else underlying.first_above_after_observed_ns(**kwargs)
    )
    if adverse is None:
        return None
    delay_ms = (
        float(cfg.binance_mid_below_trigger_cancel_delay_ms)
        if cfg.binance_mid_below_trigger_cancel_delay_ms >= 0
        else float(cfg.paper_shadow_order_open_delay_ms)
    )
    delay_ms = _vania_lat_sample_ms("CANCEL", (int(adverse["observed_ts_ns"]) * 1000003) ^ int(open_ns), delay_ms)
    delay_ns = _delay_ns(max(0.0, delay_ms))
    request_ns = max(int(open_ns), int(adverse["observed_ts_ns"])); effective_ns = request_ns + delay_ns
    return {
        "reason": "binance_mid_adverse_to_outcome",
        "request_ts_ns": request_ns, "effective_ts_ns": effective_ns,
        "request_ts_ms": q.ns_to_ms(request_ns), "effective_ts_ms": q.ns_to_ms(effective_ns),
        "delay_ms": delay_ns / 1_000_000.0,
        "observed_value": float(adverse["price"]), "threshold": float(signal["binance_mid"]),
        "adverse": adverse,
    }


def _evaluate_ladder_session(
    task: q.ContractTask,
    cfg: Config,
    token_map: dict[str, str],
    resolutions: dict[int, str],
    underlying: q.UnderlyingMidSeries | None,
    *,
    legacy_top_open_by_side: dict[str, list[float]] | None = None,
) -> dict[str, Any]:
    """Fast single-read momentum ladder evaluator.

    A single compact depth_clock timeline supplies the ladder trigger floor,
    placement books, FIFO queue snapshots and post-open ask state.  A single
    bounded trade-tape read then serves all ladder prices, retries and reprices
    in memory.  BBO and normalized-depth datasets are not opened on this path.
    """
    if underlying is None:
        raise RuntimeError("momentum ladder requires the Binance underlying venue-time index")
    legacy_top_open_by_side = {
        str(side).upper(): sorted(float(x) for x in (times or []) if math.isfinite(float(x)))
        for side, times in (legacy_top_open_by_side or {}).items()
    }
    data_dir = Path(cfg.data_dir)
    market_end_ms = float(task.contract_start_ms + q.market_period_sec(cfg.market_key) * 1000)
    market_end_ns = q.ms_to_ns(market_end_ms)
    contract_start_ns = q.ms_to_ns(task.contract_start_ms)

    # Fast path: depth_clock is the canonical compact top-of-book timeline and
    # is required anyway for post-only placement, FIFO queue snapshots and
    # later ask-cross confirmation.  V23 first read BBO and then read depth for
    # almost every market, doubling shared-filesystem metadata/I/O.  V25 reads
    # depth_clock once and performs the >= ladder-floor prefilter in memory.
    depth_start = max(
        float(task.contract_start_ms) - float(cfg.max_live_book_age_ms) - 1.0,
        float(task.contract_start_ms) - 5000.0,
    )
    depth, depth_meta = q.load_depth_clock_timeline_fast(
        task,
        data_dir=data_dir,
        market_key=cfg.market_key,
        yes_outcome=cfg.yes_outcome,
        start_ms=depth_start,
        end_ms=market_end_ms,
        trigger_bid=cfg.trigger_bid,
        tolerance=cfg.trigger_tolerance,
    )
    side_arrays = {side: _ladder_depth_arrays(depth, side) for side in q.OUTCOMES}
    entry_start_ns = contract_start_ns + _delay_ns(float(cfg.entry_market_age_start_sec) * 1000.0)
    entry_end_ns = min(market_end_ns, contract_start_ns + _delay_ns(float(cfg.entry_market_age_end_sec) * 1000.0))
    floor_sides: set[str] = set()
    for side, arrays in side_arrays.items():
        times = arrays["ts_ns"]
        bids = arrays["bid"]
        if not len(times):
            continue
        mask = (
            (times >= entry_start_ns)
            & (times <= entry_end_ns)
            & (bids >= float(cfg.momentum_ladder_start_bid) - float(cfg.trigger_tolerance) - 1e-12)
            & (bids <= float(cfg.momentum_ladder_max_bid) + float(cfg.trigger_tolerance) + 1e-12)
        )
        if bool(mask.any()):
            floor_sides.add(side)
    bbo_meta = {
        "source": "depth_clock_ladder_floor_prefilter",
        "files_read": 0,
        "raw_rows": 0,
        "bbo_read_skipped": True,
    }
    market = {
        "contract_start_ms": task.contract_start_ms,
        "market_start_utc": q.market_iso(task.contract_start_ms),
        "market_end_utc": q.market_iso(market_end_ms),
        "source_session_id": task.session_id,
        "bbo_dir": task.bbo_dir,
        "status": "no_ladder_floor_trigger" if not floor_sides else "ladder_floor_seen",
        "simulation_valid": True,
        "momentum_ladder_enabled": True,
        "momentum_ladder_fast_path": True,
        "momentum_ladder_overlay": True,
        "momentum_ladder_make_before_break": bool(cfg.momentum_ladder_make_before_break),
        "momentum_ladder_bbo_read_skipped": True,
        "momentum_ladder_start_bid": float(cfg.momentum_ladder_start_bid),
        "momentum_ladder_max_bid": float(cfg.momentum_ladder_max_bid),
        "momentum_ladder_tick": float(cfg.momentum_ladder_tick),
        "momentum_ladder_shares_to_open": float(cfg.momentum_ladder_shares_to_open),
        "momentum_ladder_max_placement_attempts": int(cfg.momentum_ladder_max_placement_attempts),
    }
    if not floor_sides:
        market.update({f"depth_{k}": v for k, v in depth_meta.items()})
        return {
            "ok": True, "source_gap": False, "market": market, "orders": [], "fills": [],
            "queue_trades": [], "bid99_volume_events": [], "bbo_meta": bbo_meta, "depth_meta": depth_meta,
        }
    side_arrays = {side: arrays for side, arrays in side_arrays.items() if side in floor_sides}
    momentum_events = {
        side: _ladder_momentum_events(
            arrays=arrays, side=side, task=task, market_end_ms=market_end_ms, cfg=cfg, underlying=underlying
        )
        for side, arrays in side_arrays.items()
    }
    ladder_missing_signal_count = sum(int(events.get("missing_book_candidate_count", 0)) for events in momentum_events.values())
    ladder_missing_components: Counter[str] = Counter()
    for events in momentum_events.values():
        ladder_missing_components.update({k: int(v) for k, v in dict(events.get("missing_book_by_component", {})).items()})
    market["candidate_trades_blocked_missing_market_data"] = int(ladder_missing_signal_count)
    market["blocked_missing_market_data_by_stage"] = ({"ladder_signal_depth": int(ladder_missing_signal_count)} if ladder_missing_signal_count else {})
    market["blocked_missing_market_data_by_component"] = dict(ladder_missing_components)
    ladder_optional_missing_signal_count = sum(
        int(events.get("optional_missing_book_candidate_count", 0)) for events in momentum_events.values()
    )
    ladder_optional_missing_components: Counter[str] = Counter()
    for events in momentum_events.values():
        ladder_optional_missing_components.update({
            k: int(v) for k, v in dict(events.get("optional_missing_book_by_component", {})).items()
        })
    market["ladder_signal_candidates_with_optional_missing_data"] = int(ladder_optional_missing_signal_count)
    market["ladder_signal_optional_missing_by_component"] = dict(ladder_optional_missing_components)
    first_signals: list[tuple[int, str, dict[str, Any]]] = []
    for side, events in momentum_events.items():
        sig = _next_ladder_signal(events, after_ns=entry_start_ns - 1,
                                  anchor_bid=float(cfg.momentum_ladder_start_bid), cfg=cfg)
        if sig is not None:
            first_signals.append((int(sig["signal_ts_ns"]), side, sig))
    if not first_signals:
        market["status"] = "no_ladder_momentum_signal"
        market.update({f"depth_{k}": v for k, v in depth_meta.items()})
        return {
            "ok": True, "source_gap": False, "market": market, "orders": [], "fills": [],
            "queue_trades": [], "bid99_volume_events": [], "bbo_meta": bbo_meta, "depth_meta": depth_meta,
        }
    first_signals.sort(key=lambda x: (x[0], x[1]))
    _first_ts, side, signal = first_signals[0]
    arrays = side_arrays[side]
    events = momentum_events[side]
    # In additive mode, the preserved legacy top order is authoritative.  The
    # lower overlay may work before that top order is resting, but must never
    # create a fresh lower-priced order after the legacy TRIGGER_BID order has
    # already opened.  An already-resting lower order is cancelled only after
    # that successful top placement (plus the configured reprice cancel delay).
    legacy_top_times_for_side = legacy_top_open_by_side.get(str(side).upper(), [])
    earliest_legacy_top_open_ns = (
        q.ms_to_ns(float(legacy_top_times_for_side[0])) if legacy_top_times_for_side else market_end_ns
    )
    lower_placement_deadline_ns = min(market_end_ns, earliest_legacy_top_open_ns)
    lower_placement_deadline_ms = q.ns_to_ms(lower_placement_deadline_ns)

    # One bounded trade read serves every lower-ladder order.  When below-limit
    # confirmation is disabled, a lower price bound avoids irrelevant trades.
    trade_min = None if cfg.fill_remaining_on_trade_below_limit else _ladder_price(
        cfg.momentum_ladder_start_bid + cfg.momentum_ladder_tick, cfg.momentum_ladder_tick
    )
    trades, trade_meta = q.load_trades(
        task,
        data_dir=data_dir,
        market_key=cfg.market_key,
        token_map=token_map,
        yes_outcome=cfg.yes_outcome,
        start_ms=float(task.contract_start_ms),
        end_ms=market_end_ms,
        trade_clock_mode=cfg.trade_clock_mode,
        allow_local_fallback=cfg.allow_local_trade_clock_fallback,
        min_price=trade_min,
        max_price=float(cfg.momentum_ladder_max_bid),
        tolerance=cfg.trigger_tolerance,
        outcomes=[side],
    )

    # Resolution is deliberately introduced only after signal generation and source loading.
    winner = resolutions.get(task.contract_start_ms, "")
    market["winner"] = winner
    orders: list[dict[str, Any]] = []
    fills: list[dict[str, Any]] = []
    queue_trades: list[dict[str, Any]] = []
    ladder_audit: list[dict[str, Any]] = []
    anchor = float(cfg.momentum_ladder_start_bid)
    cursor_ns = int(signal["signal_ts_ns"]) - 1
    pending_signal: dict[str, Any] | None = signal
    pending_qty: float | None = None
    # When make-before-break uses a non-zero cancel latency, the replacement
    # is physically resting before the old order disappears.  To avoid
    # double-counting the same trade volume in the short overlap, the lower
    # overlay conservatively defers replacement fill eligibility until the old
    # cancellation is effective while retaining the queue snapshot from the
    # true placement time.
    pending_fill_guard_until_ns: int | None = None
    reverse_book_consumed: dict[tuple[str, int, int], float] = {}
    full_cycle_size: float | None = None
    opened_count = 0
    order_seq = 0
    deal_count = 0
    active_deal_seq = 0
    cycle_needs_count = True

    while opened_count < int(cfg.momentum_ladder_max_orders_per_market):
        if (
            cycle_needs_count
            and int(cfg.momentum_ladder_max_deals_per_market) >= 1
            and deal_count >= int(cfg.momentum_ladder_max_deals_per_market)
        ):
            break
        if pending_signal is None:
            pending_signal = _next_ladder_signal(events, after_ns=cursor_ns, anchor_bid=anchor, cfg=cfg)
            if pending_signal is None:
                break
        sig = pending_signal
        pending_signal = None
        if int(sig["signal_ts_ns"]) >= lower_placement_deadline_ns:
            break

        if pending_qty is None:
            if cfg.momentum_ladder_shares_to_open >= 0:
                requested = float(cfg.momentum_ladder_shares_to_open)
                sizing = {"raw_shares": requested, "rounded_shares": int(math.floor(requested + 0.5)),
                          "requested_shares": requested, "shares_clipped_low": False, "shares_clipped_high": False,
                          "ladder_size_source": "momentum_ladder_shares_to_open"}
            elif cfg.share_sizing_mode == "fixed":
                requested = float(cfg.fixed_shares_to_open)
                sizing = {"raw_shares": requested, "rounded_shares": int(math.floor(requested + 0.5)),
                          "requested_shares": requested, "shares_clipped_low": False, "shares_clipped_high": False,
                          "ladder_size_source": "inherited_fixed_shares_to_open"}
            else:
                start_lookup = underlying.asof_observed(
                    float(task.contract_start_ms), cfg.btc_mid_max_age_ms, cfg.binance_venue_to_vps_delay_ms
                )
                if start_lookup is None:
                    break
                move = abs(float(sig["binance_mid"]) - float(start_lookup["price"]))
                if move + 1e-12 < cfg.min_btc_move_usd:
                    cursor_ns = int(sig["signal_ts_ns"])
                    continue
                sizing = q.share_size_from_move(
                    move,
                    divisor_usd=cfg.btc_move_divisor_usd,
                    multiplier=cfg.btc_shares_multiplier,
                    min_shares=cfg.min_shares_to_open,
                    max_shares=cfg.max_shares_to_open,
                )
                requested = float(sizing["requested_shares"])
                sizing["ladder_size_source"] = "inherited_btc_move_sizing"
            full_cycle_size = requested
        else:
            requested = float(pending_qty)
            sizing = {"raw_shares": requested, "rounded_shares": int(math.floor(requested + 0.5)),
                      "requested_shares": requested, "shares_clipped_low": False, "shares_clipped_high": False,
                      "ladder_size_source": "carried_unfilled_remainder"}

        order_seq += 1
        order_id = f"{task.contract_start_ms}:{side}:ladder:{order_seq}:{int(sig['signal_ts_ns'])}"
        placement = _ladder_place_with_retries(
            task=task, cfg=cfg, arrays=arrays, side=side, order_id=order_id, signal=sig,
            anchor_bid=anchor, requested=requested, not_before_ns=max(cursor_ns, int(sig["signal_ts_ns"])),
            market_end_ns=lower_placement_deadline_ns, sequence=order_seq,
        )
        raw_signal_audit = _causal_audit_fields(
            decision_ns=int(sig["signal_ts_ns"]),
            sources={
                "btc_feature": int(sig.get("btc_feature_ts_ns", -1)),
                "prior_feature": int(sig.get("prior_feature_ts_ns", -1)),
            },
            strict_prior={"prior_feature"}, enabled=cfg.strict_causal_audit,
        )
        raw_signal_audit["bbo_preprocessing_window_end_ns"] = int(sig["signal_ts_ns"])
        raw_signal_audit["btc_preprocessing_window_end_ns"] = int(sig["signal_ts_ns"])
        base = {
            "order_id": order_id,
            "contract_start_ms": task.contract_start_ms,
            "market_start_utc": q.market_iso(task.contract_start_ms),
            "market_end_utc": q.market_iso(market_end_ms),
            "outcome": side,
            "winner": winner,
            "simulation_valid": True,
            "balance_accepted": False,
            "entry_trigger_source": "momentum_ladder",
            "entry_attempt_number": order_seq,
            "momentum_ladder_enabled": True,
            # Execution policy is part of the immutable order contract.  Do not
            # let downstream integrity validation guess this from a default.
            "post_only_order": bool(cfg.post_only_order_enabled),
            "post_only": bool(cfg.post_only_order_enabled),
            "momentum_ladder_order_sequence": order_seq,
            "momentum_ladder_deal_sequence": active_deal_seq if active_deal_seq > 0 else deal_count + 1,
            "entry_trigger_observed_best_bid": float(sig["observed_bid"]),
            "entry_trigger_bid_floor": float(cfg.momentum_ladder_start_bid),
            "ladder_anchor_bid": float(anchor),
            "ladder_signal_ts_ns": int(sig["signal_ts_ns"]),
            "ladder_signal_ts_ms": float(sig["signal_ts_ms"]),
            "ladder_signal_time_utc": q.market_iso(sig["signal_ts_ms"]),
            "ladder_signal_best_bid": float(sig["observed_bid"]),
            "ladder_target_price": float(sig["target_price"]),
            "limit_price": float(sig["target_price"]),
            "requested_shares": requested,
            "btc_trigger_mid": float(sig["binance_mid"]),
            "btc_trigger_series_index": int(sig["binance_mid_index"]),
            "bid99_binance_momentum_reference_mid": float(sig["binance_reference_mid"]),
            "bid99_binance_momentum_current_mid": float(sig["binance_mid"]),
            "bid99_binance_momentum_signed_move_usd": float(sig["binance_mid"] - sig["binance_reference_mid"]),
            "bid99_binance_momentum_directional_move_usd": float(sig["binance_directional_move_usd"]),
            "bid99_binance_momentum_min_usd": float(cfg.bid99_binance_momentum_min_usd),
            "bid99_binance_momentum_configured": True,
            "bid99_binance_momentum_required_for_candidate": True,
            "bid99_binance_momentum_evaluated": True,
            "bid99_binance_momentum_passed": True,
            "potential_filled_shares": 0.0,
            "filled_shares": 0.0,
            "potential_pnl_usdc": 0.0,
            "net_pnl_usdc": 0.0,
            **sizing,
            **raw_signal_audit,
            **{k: v for k, v in placement.items() if k != "order_id"},
        }
        if not placement["opened"]:
            missing_placement = str(placement.get("stop_reason", "")) == "missing_book_component_at_open"
            placement_missing_components = list(_buy_limit_open_book_semantics(
                best_bid=q.as_float(placement.get("best_bid_at_open"), math.nan),
                best_bid_size=q.as_float(placement.get("best_bid_size_at_open"), math.nan),
                best_ask=q.as_float(placement.get("best_ask_at_open"), math.nan),
                best_ask_size=q.as_float(placement.get("best_ask_size_at_open"), math.nan),
                limit_price=q.as_float(placement.get("limit_price"), q.as_float(base.get("limit_price"), cfg.trigger_bid)),
                tolerance=cfg.trigger_tolerance,
            )["missing_components"]) if missing_placement else []
            base.update({
                "status": f"skipped_{placement['stop_reason']}",
                "skip_reason": placement["stop_reason"],
                "order_was_opened": False,
                "post_only_rejected_at_placement": bool(placement["post_only_rejections_before_open"] > 0),
                **_missing_book_fields("ladder_order_open_depth", placement_missing_components),
            })
            orders.append(base)
            if cfg.momentum_ladder_write_event_audit:
                ladder_audit.append({"order_id": order_id, "event": "placement_failed", "reason": placement["stop_reason"]})
            # A stale target can recover only after fresh momentum; a bid-drop
            # stop or market-end condition terminates this ladder.
            if placement["stop_reason"] == "ladder_target_below_best_at_open":
                anchor = float(sig["target_price"])
                cursor_ns = int(placement["order_open_ts_ns"])
                pending_qty = requested
                continue
            break

        opened_count += 1
        if cycle_needs_count:
            deal_count += 1
            active_deal_seq = deal_count
            cycle_needs_count = False
        base["momentum_ladder_deal_sequence"] = int(active_deal_seq)
        open_ns = int(placement["order_open_ts_ns"])
        open_ms = q.ns_to_ms(open_ns)
        fill_open_ns = max(
            open_ns,
            int(pending_fill_guard_until_ns) if pending_fill_guard_until_ns is not None else open_ns,
        )
        fill_open_ms = q.ns_to_ms(fill_open_ns)
        pending_fill_guard_until_ns = None
        limit = float(sig["target_price"])
        queue = float(placement["queue_ahead_shares"])
        base.update({
            "status": "ladder_open",
            "skip_reason": "",
            "order_was_opened": True,
            "post_only_rejected_at_placement": False,
            "queue_ahead_shares": queue,
            "ladder_physical_open_ts_ms": open_ms,
            "ladder_fill_eligibility_ts_ns": fill_open_ns,
            "ladder_fill_eligibility_ts_ms": fill_open_ms,
            "ladder_fill_guard_ms": max(0, fill_open_ns - open_ns) / 1_000_000.0,
        })

        next_sig = (
            _next_ladder_signal(events, after_ns=open_ns, anchor_bid=limit, cfg=cfg)
            if cfg.momentum_ladder_reprice_enabled
            else None
        )
        reprice_candidate: dict[str, Any] | None = None
        reprice_delay = (
            float(cfg.momentum_ladder_reprice_cancel_delay_ms)
            if cfg.momentum_ladder_reprice_cancel_delay_ms >= 0
            else float(cfg.paper_shadow_order_open_delay_ms)
        )
        replacement_candidates: list[dict[str, Any]] = []
        if next_sig is not None:
            if cfg.momentum_ladder_make_before_break:
                probe_id = (
                    f"{task.contract_start_ms}:{side}:ladder_probe:{order_seq + 1}:"
                    f"{int(next_sig['signal_ts_ns'])}"
                )
                replacement_probe = _ladder_place_with_retries(
                    task=task, cfg=cfg, arrays=arrays, side=side, order_id=probe_id, signal=next_sig,
                    anchor_bid=limit, requested=requested,
                    not_before_ns=max(open_ns, int(next_sig["signal_ts_ns"])),
                    market_end_ns=lower_placement_deadline_ns, sequence=order_seq + 1,
                )
                base.update({
                    "ladder_replacement_probe_attempted": True,
                    "ladder_replacement_probe_opened": bool(replacement_probe.get("opened", False)),
                    "ladder_replacement_probe_target_price": float(next_sig["target_price"]),
                    "ladder_replacement_probe_attempts": q.as_int(replacement_probe.get("placement_attempts"), 0),
                    "ladder_replacement_probe_post_only_rejections": q.as_int(
                        replacement_probe.get("post_only_rejections_before_open"), 0
                    ),
                    "ladder_replacement_probe_stop_reason": str(replacement_probe.get("stop_reason", "")),
                })
                if replacement_probe.get("opened", False):
                    replacement_open_ns = int(replacement_probe["order_open_ts_ns"])
                    delay_ns = _delay_ns(max(0.0, reprice_delay))
                    replacement_candidates.append({
                        "reason": "momentum_ladder_reprice",
                        "request_ts_ns": replacement_open_ns,
                        "effective_ts_ns": replacement_open_ns + delay_ns,
                        "request_ts_ms": q.ns_to_ms(replacement_open_ns),
                        "effective_ts_ms": q.ns_to_ms(replacement_open_ns + delay_ns),
                        "delay_ms": delay_ns / 1_000_000.0,
                        "next_signal": next_sig,
                        "replacement_kind": "ladder",
                        "replacement_open_ts_ns": replacement_open_ns,
                        "replacement_open_ts_ms": q.ns_to_ms(replacement_open_ns),
                        "replacement_limit_price": float(next_sig["target_price"]),
                    })
            else:
                request_ns = int(next_sig["signal_ts_ns"]); delay_ns = _delay_ns(max(0.0, reprice_delay))
                replacement_candidates.append({
                    "reason": "momentum_ladder_reprice",
                    "request_ts_ns": request_ns,
                    "effective_ts_ns": request_ns + delay_ns,
                    "request_ts_ms": q.ns_to_ms(request_ns),
                    "effective_ts_ms": q.ns_to_ms(request_ns + delay_ns),
                    "delay_ms": delay_ns / 1_000_000.0,
                    "next_signal": next_sig,
                    "replacement_kind": "ladder_signal",
                    "replacement_open_ts_ns": -1,
                    "replacement_open_ts_ms": math.nan,
                    "replacement_limit_price": float(next_sig["target_price"]),
                })

        # In additive V29 mode, the original TRIGGER_BID engine is an
        # independent top rung.  Once a real legacy 0.99 order for this side is
        # resting, it can replace the highest lower ladder order without ever
        # delaying or changing that legacy order itself.
        legacy_times = legacy_top_open_by_side.get(str(side).upper(), [])
        if legacy_times and float(cfg.trigger_bid) > limit + float(cfg.trigger_tolerance):
            legacy_open_ns = next((q.ms_to_ns(t) for t in legacy_times if q.ms_to_ns(t) >= open_ns), None)
            if legacy_open_ns is not None and legacy_open_ns < market_end_ns:
                request_ns = max(open_ns, int(legacy_open_ns)); delay_ns = _delay_ns(max(0.0, reprice_delay))
                replacement_candidates.append({
                    "reason": "momentum_ladder_reprice_to_legacy99",
                    "request_ts_ns": request_ns,
                    "effective_ts_ns": request_ns + delay_ns,
                    "request_ts_ms": q.ns_to_ms(request_ns),
                    "effective_ts_ms": q.ns_to_ms(request_ns + delay_ns),
                    "delay_ms": delay_ns / 1_000_000.0,
                    "next_signal": None,
                    "replacement_kind": "legacy_99",
                    "replacement_open_ts_ns": int(legacy_open_ns),
                    "replacement_open_ts_ms": q.ns_to_ms(int(legacy_open_ns)),
                    "replacement_limit_price": float(cfg.trigger_bid),
                })

        if replacement_candidates:
            reprice_candidate = min(
                replacement_candidates,
                key=lambda x: (int(x["request_ts_ns"]), float(x["replacement_limit_price"])),
            )
            base.update({
                "ladder_replacement_kind": str(reprice_candidate.get("replacement_kind", "")),
                "ladder_replacement_open_ts_ms": q.as_float(reprice_candidate.get("replacement_open_ts_ms"), math.nan),
                "ladder_replacement_limit_price": q.as_float(reprice_candidate.get("replacement_limit_price"), math.nan),
                "ladder_reprice_request_basis": (
                    "replacement_open" if cfg.momentum_ladder_make_before_break
                    else "higher_signal"
                ),
            })
        adverse = _ladder_adverse_cancel(
            side=side, signal=sig, open_ns=open_ns, market_end_ns=market_end_ns,
            cfg=cfg, underlying=underlying,
        )
        if adverse is not None:
            adv = dict(adverse.get("adverse") or {})
            base.update({
                "binance_adverse_cancel_enabled": True,
                "binance_adverse_detected": True,
                "binance_adverse_direction": "below_trigger" if side == "UP" else "above_trigger",
                "binance_adverse_reference_mid": float(sig["binance_mid"]),
                "binance_adverse_detected_mid": q.as_float(adv.get("price"), math.nan),
                "binance_adverse_detected_venue_ts_ms": q.as_float(adv.get("venue_ts_ms"), math.nan),
                "binance_adverse_detected_observed_ts_ms": q.as_float(adv.get("observed_ts_ms"), math.nan),
                "binance_adverse_cancel_scheduled": True,
                "binance_adverse_cancel_request_ts_ms": q.as_float(adverse.get("request_ts_ms"), math.nan),
                "binance_adverse_cancel_effective_ts_ms": q.as_float(adverse.get("effective_ts_ms"), math.nan),
                "binance_adverse_cancel_delay_ms": q.as_float(adverse.get("delay_ms"), math.nan),
                "binance_adverse_detected_before_order_open": int(adv.get("observed_ts_ns", 2**63-1)) <= open_ns,
            })
        else:
            base.update({
                "binance_adverse_cancel_enabled": bool(cfg.cancel_if_binance_mid_below_trigger),
                "binance_adverse_detected": False,
                "binance_adverse_cancel_scheduled": False,
            })
        lifetime: dict[str, Any] | None = None
        if cfg.limit_order_lifetime_sec >= 0:
            delay_ms = float(cfg.limit_order_lifetime_cancel_delay_ms) if cfg.limit_order_lifetime_cancel_delay_ms >= 0 else float(cfg.paper_shadow_order_open_delay_ms)
            req_ns = open_ns + _delay_ns(float(cfg.limit_order_lifetime_sec) * 1000.0); delay_ns = _delay_ns(max(0.0, delay_ms))
            if req_ns < market_end_ns:
                lifetime = {"reason":"limit_order_lifetime_expired","request_ts_ns":req_ns,"effective_ts_ns":req_ns+delay_ns,"request_ts_ms":q.ns_to_ms(req_ns),"effective_ts_ms":q.ns_to_ms(req_ns+delay_ns),"delay_ms":delay_ns/1e6}
        queue_cancel: dict[str, Any] | None = None
        if cfg.cancel_if_queue_ahead_above_shares >= 0 and queue > cfg.cancel_if_queue_ahead_above_shares + 1e-12:
            delay_ms = cfg.queue_cancel_delay_ms if cfg.queue_cancel_delay_ms >= 0 else cfg.paper_shadow_order_open_delay_ms; delay_ns=_delay_ns(max(0.0,float(delay_ms)))
            queue_cancel={"reason":"queue_ahead_above_threshold","request_ts_ns":open_ns,"effective_ts_ns":open_ns+delay_ns,"request_ts_ms":open_ms,"effective_ts_ms":q.ns_to_ms(open_ns+delay_ns),"delay_ms":delay_ns/1e6}
        cancels=[x for x in (reprice_candidate,adverse,lifetime,queue_cancel) if x is not None]
        selected_cancel=min(cancels,key=lambda x:(int(x["effective_ts_ns"]),int(x["request_ts_ns"]))) if cancels else None
        cutoff_ns=min(market_end_ns,int(selected_cancel["effective_ts_ns"]) if selected_cancel else market_end_ns)

        initial_cross_levels: list[dict[str,Any]]=[]; reverse_state_ns=-1
        if not cfg.post_only_order_enabled and bool(placement.get("crossed_at_open",False)):
            levels, ask_meta=q.load_ask_ladder_at_time_fast(task,data_dir=data_dir,market_key=cfg.market_key,token_map=token_map,yes_outcome=cfg.yes_outcome,outcome=side,query_ns=int(open_ns),max_price=limit,tolerance=cfg.trigger_tolerance)
            reverse_state_ns=int(ask_meta.get("state_ts_ns",-1))
            for level in levels:
                raw_price=int(round(float(level["price"])*q.PRICE_SCALE)); key=(str(side),reverse_state_ns,raw_price)
                remaining_level=max(0.0,float(level["shares"])-reverse_book_consumed.get(key,0.0))
                if remaining_level>0: initial_cross_levels.append({"price":float(level["price"]),"shares":remaining_level})

        base["reverse_book_ts_ns"] = int(reverse_state_ns)
        base["reverse_book_query_ts_ns"] = int(open_ns) if reverse_state_ns >= 0 else -1
        base["execution_preprocessing_window_end_ns"] = int(cutoff_ns)
        execution=_ladder_simulate_fills(order_id=order_id,contract_start_ms=task.contract_start_ms,side=side,open_ns=fill_open_ns,cutoff_ns=cutoff_ns,requested=requested,queue_ahead=queue,limit_price=limit,side_arrays=arrays,trades=trades,cfg=cfg,initial_cross_levels=initial_cross_levels)
        filled=float(execution["filled"]); remaining=max(0.0,requested-filled); full_ns=int(execution.get("full_fill_ts_ns",-1))
        if reverse_state_ns>=0:
            for fill in execution["fill_events"]:
                if fill.get("fill_source")=="ladder_reverse_book_at_open":
                    raw_price=int(round(float(fill["fill_price"])*q.PRICE_SCALE)); key=(str(side),reverse_state_ns,raw_price)
                    reverse_book_consumed[key]=reverse_book_consumed.get(key,0.0)+float(fill["fill_shares"])
        # A fill strictly before the cancel request makes that candidate non-actionable.
        if selected_cancel is not None and full_ns>=0 and full_ns < int(selected_cancel["request_ts_ns"]):
            selected_cancel=None; cutoff_ns=market_end_ns
            execution=_ladder_simulate_fills(order_id=order_id,contract_start_ms=task.contract_start_ms,side=side,open_ns=fill_open_ns,cutoff_ns=cutoff_ns,requested=requested,queue_ahead=queue,limit_price=limit,side_arrays=arrays,trades=trades,cfg=cfg,initial_cross_levels=initial_cross_levels)
            filled=float(execution["filled"]); remaining=max(0.0,requested-filled); full_ns=int(execution.get("full_fill_ts_ns",-1))

        fills.extend(execution["fill_events"])
        queue_trades.extend(execution["queue_trades"])
        cost = sum(q.as_float(row.get("fill_shares"), 0.0) * q.as_float(row.get("fill_price"), limit) for row in execution["fill_events"])
        fee = sum(fee_for_shares(q.as_float(row.get("fill_shares"), 0.0), q.as_float(row.get("fill_price"), limit), cfg) for row in execution["fill_events"])
        payout = filled if winner == side else 0.0 if winner in q.OUTCOMES else 0.0
        pnl = payout - cost - fee
        if filled >= requested - 1e-9:
            status = "fully_filled_before_cancel" if selected_cancel else "fully_filled"
        elif selected_cancel is not None and int(selected_cancel["effective_ts_ns"]) < market_end_ns:
            is_reprice = str(selected_cancel["reason"]).startswith("momentum_ladder_reprice")
            status = "partially_filled_then_repriced" if is_reprice and filled > 0 else \
                     "unfilled_repriced" if is_reprice else \
                     "partially_filled_then_cancelled" if filled > 0 else f"cancelled_{selected_cancel['reason']}"
        elif filled > 0:
            status = "partially_filled_at_market_end"
        else:
            status = "unfilled_at_market_end"
        base.update({
            "status": status,
            "potential_filled_shares": filled,
            "fill_fraction": filled / requested if requested > 0 else 0.0,
            "queue_remaining_shares": float(execution["queue_remaining"]),
            "potential_cost_usdc": cost,
            "potential_fee_usdc": fee,
            "potential_average_fill_price": cost / filled if filled > 0 else math.nan,
            "potential_payout_usdc": payout,
            "potential_pnl_usdc": pnl,
            "first_fill_ts_ns": int(execution.get("first_fill_ts_ns", -1)),
            "first_fill_ts_ms": q.as_float(execution.get("first_fill_ts_ms"), math.nan),
            "last_fill_ts_ms": max([q.as_float(x.get("fill_ts_ms"), math.nan) for x in execution["fill_events"]], default=math.nan),
            "cancel_requested": selected_cancel is not None,
            "cancel_reason": str(selected_cancel.get("reason", "")) if selected_cancel else "",
            "cancel_request_ts_ns": int(selected_cancel.get("request_ts_ns", -1)) if selected_cancel else -1,
            "cancel_effective_ts_ns": int(selected_cancel.get("effective_ts_ns", -1)) if selected_cancel else -1,
            "cancel_request_ts_ms": q.as_float(selected_cancel.get("request_ts_ms"), math.nan) if selected_cancel else math.nan,
            "cancel_effective_ts_ms": q.as_float(selected_cancel.get("effective_ts_ms"), math.nan) if selected_cancel else math.nan,
            "cancel_delay_ms": q.as_float(selected_cancel.get("delay_ms"), math.nan) if selected_cancel else math.nan,
            "cancel_effective_before_market_end": bool(selected_cancel and int(selected_cancel["effective_ts_ns"]) < market_end_ns),
            "ladder_reprice_selected": bool(selected_cancel and str(selected_cancel["reason"]).startswith("momentum_ladder_reprice")),
            "ladder_reprice_to_legacy99": bool(selected_cancel and selected_cancel["reason"] == "momentum_ladder_reprice_to_legacy99"),
            "binance_adverse_cancel_selected": bool(selected_cancel and selected_cancel["reason"] == "binance_mid_adverse_to_outcome"),
        })
        orders.append(base)
        if cfg.momentum_ladder_write_event_audit:
            ladder_audit.append({
                "order_id": order_id, "event": "order_complete", "status": status,
                "limit_price": limit, "filled": filled, "remaining": remaining,
                "cancel_reason": base["cancel_reason"],
            })

        if filled >= requested - 1e-9:
            if not cfg.momentum_ladder_continue_after_full_fill or limit >= cfg.momentum_ladder_max_bid - cfg.trigger_tolerance:
                break
            if (
                int(cfg.momentum_ladder_max_deals_per_market) >= 1
                and deal_count >= int(cfg.momentum_ladder_max_deals_per_market)
            ):
                break
            anchor = limit
            cursor_ns = full_ns if full_ns >= 0 else open_ns
            pending_qty = None
            pending_signal = None
            cycle_needs_count = True
            active_deal_seq = 0
            continue

        if selected_cancel and selected_cancel["reason"] == "momentum_ladder_reprice" and remaining > 1e-9:
            anchor = limit
            next_pending = dict(selected_cancel["next_signal"])
            # Re-run the deterministic replacement placement on the next loop
            # using the actual remaining quantity.  Its physical open remains
            # the same as the probe.  Fill eligibility is guarded until the old
            # order's cancellation becomes effective to avoid double-consuming
            # overlap trade volume.
            cursor_ns = int(next_pending["signal_ts_ns"]) - 1
            pending_qty = remaining
            pending_signal = next_pending
            pending_fill_guard_until_ns = (
                int(selected_cancel["effective_ts_ns"])
                if cfg.momentum_ladder_make_before_break else None
            )
            continue
        # A successful legacy 0.99 placement is the terminal top replacement:
        # the preserved old engine already owns that order and its fills.
        break

    market.update({
        "status": "evaluated_ladder",
        "ladder_selected_outcome": side,
        "orders": len(orders),
        "ladder_opened_orders": opened_count,
        "ladder_deals_started": deal_count,
        "ladder_max_deals_per_market": int(cfg.momentum_ladder_max_deals_per_market),
        "ladder_reprice_enabled": bool(cfg.momentum_ladder_reprice_enabled),
        "potential_filled_orders": sum(q.as_float(x.get("potential_filled_shares"), 0.0) > 0 for x in orders),
        "potential_filled_shares": sum(q.as_float(x.get("potential_filled_shares"), 0.0) for x in orders),
        "depth_rows": len(depth),
        "trade_rows": len(trades),
        "normalized_depth_events_loaded": False,
        "normalized_depth_events_load_reason": "ladder_fast_path_disabled",
        "ladder_event_audit_rows": len(ladder_audit),
    })
    market.update({f"depth_{k}": v for k, v in depth_meta.items()})
    market.update({f"trade_{k}": v for k, v in trade_meta.items()})
    if cfg.momentum_ladder_write_event_audit:
        market["ladder_event_audit_json"] = json.dumps(ladder_audit, separators=(",", ":"), default=str)
    _update_missing_market_data_counters(market, orders, add_to_existing=True)
    return {
        "ok": True,
        "source_gap": False,
        "market": market,
        "orders": orders,
        "fills": fills,
        "queue_trades": queue_trades,
        "bid99_volume_events": [],
        "bbo_meta": bbo_meta,
        "depth_meta": depth_meta,
        "trade_meta": trade_meta,
    }


def _effective_lower_ladder_max_bid(cfg: Config) -> float:
    if not (cfg.momentum_ladder_enabled and cfg.momentum_ladder_preserve_legacy_99):
        return float(cfg.momentum_ladder_max_bid)
    tick = float(cfg.momentum_ladder_tick)
    top_lower = _ladder_price(float(cfg.trigger_bid) - tick, tick)
    return min(float(cfg.momentum_ladder_max_bid), float(top_lower))


def _legacy_top_open_times(result: dict[str, Any], cfg: Config) -> dict[str, list[float]]:
    out: dict[str, list[float]] = defaultdict(list)
    for row in result.get("orders", []) or []:
        if not q.as_bool(row.get("order_was_opened", False)):
            continue
        limit_price = q.as_float(row.get("limit_price"), math.nan)
        if not math.isfinite(limit_price) or abs(limit_price - float(cfg.trigger_bid)) > float(cfg.trigger_tolerance) + 1e-12:
            continue
        ts = q.as_float(row.get("order_open_ts_ms"), math.nan)
        side = str(row.get("outcome", "")).upper()
        if side in q.OUTCOMES and math.isfinite(ts):
            out[side].append(float(ts))
    return {side: sorted(times) for side, times in out.items()}


def _empty_ladder_overlay_result(
    task: q.ContractTask, cfg: Config, resolutions: dict[int, str], *, status: str
) -> dict[str, Any]:
    end_ms = float(task.contract_start_ms + q.market_period_sec(cfg.market_key) * 1000)
    return {
        "ok": True,
        "source_gap": False,
        "market": {
            "contract_start_ms": task.contract_start_ms,
            "market_start_utc": q.market_iso(task.contract_start_ms),
            "market_end_utc": q.market_iso(end_ms),
            "source_session_id": task.session_id,
            "bbo_dir": task.bbo_dir,
            "status": status,
            "simulation_valid": True,
                "momentum_ladder_enabled": True,
            "momentum_ladder_overlay": True,
            "ladder_opened_orders": 0,
            "potential_filled_orders": 0,
            "potential_filled_shares": 0.0,
        },
        "orders": [],
        "fills": [],
        "queue_trades": [],
        "bid99_volume_events": [],
        "bbo_meta": {"source": "additive_overlay_empty", "files_read": 0, "raw_rows": 0},
        "depth_meta": {},
        "trade_meta": {},
    }


def _merge_additive_ladder_results(
    legacy: dict[str, Any], ladder: dict[str, Any], cfg: Config, *, effective_lower_max_bid: float
) -> dict[str, Any]:
    # Preserve the legacy rows byte-for-byte at the dict level: no 0.99 order,
    # trigger timestamp, queue snapshot, fill event, retry, or cancellation is
    # rewritten by the lower overlay.  The only operation is list concatenation.
    legacy_orders = list(legacy.get("orders", []) or [])
    legacy_fills = list(legacy.get("fills", []) or [])
    legacy_queue = list(legacy.get("queue_trades", []) or [])
    ladder_orders = list(ladder.get("orders", []) or [])
    ladder_fills = list(ladder.get("fills", []) or [])
    ladder_queue = list(ladder.get("queue_trades", []) or [])

    market = dict(legacy.get("market", {}) or {})
    lmarket = dict(ladder.get("market", {}) or {})
    missing_total = q.as_int(market.get("candidate_trades_blocked_missing_market_data"), 0) + q.as_int(lmarket.get("candidate_trades_blocked_missing_market_data"), 0)
    missing_stage = Counter({str(k): q.as_int(v, 0) for k, v in (market.get("blocked_missing_market_data_by_stage") or {}).items()})
    missing_stage.update({str(k): q.as_int(v, 0) for k, v in (lmarket.get("blocked_missing_market_data_by_stage") or {}).items()})
    missing_component = Counter({str(k): q.as_int(v, 0) for k, v in (market.get("blocked_missing_market_data_by_component") or {}).items()})
    missing_component.update({str(k): q.as_int(v, 0) for k, v in (lmarket.get("blocked_missing_market_data_by_component") or {}).items()})
    market.update({
        "status": "evaluated_legacy99_plus_ladder",
        "momentum_ladder_enabled": True,
        "momentum_ladder_overlay": True,
        "momentum_ladder_preserve_legacy_99": True,
        "momentum_ladder_make_before_break": bool(cfg.momentum_ladder_make_before_break),
        "legacy_99_status": str((legacy.get("market") or {}).get("status", "")),
        "momentum_ladder_overlay_status": str(lmarket.get("status", "")),
        "momentum_ladder_overlay_evaluated": True,
        "momentum_ladder_user_max_bid": float(cfg.momentum_ladder_max_bid),
        "momentum_ladder_effective_lower_max_bid": float(effective_lower_max_bid),
        "ladder_selected_outcome": str(lmarket.get("ladder_selected_outcome", "")),
        "ladder_opened_orders": q.as_int(lmarket.get("ladder_opened_orders"), 0),
        "ladder_overlay_order_rows": len(ladder_orders),
        "legacy_99_order_rows": len(legacy_orders),
        "orders": len(legacy_orders) + len(ladder_orders),
        "potential_filled_orders": sum(
            q.as_float(x.get("potential_filled_shares"), 0.0) > 0
            for x in legacy_orders + ladder_orders
        ),
        "potential_filled_shares": sum(
            q.as_float(x.get("potential_filled_shares"), 0.0)
            for x in legacy_orders + ladder_orders
        ),
        "ladder_depth_rows": q.as_int(lmarket.get("depth_rows"), 0),
        "ladder_trade_rows": q.as_int(lmarket.get("trade_rows"), 0),
        "candidate_trades_blocked_missing_market_data": int(missing_total),
        "blocked_missing_market_data_by_stage": dict(missing_stage),
        "blocked_missing_market_data_by_component": dict(missing_component),
    })
    source_gap = bool(legacy.get("source_gap", False) or ladder.get("source_gap", False))
    source_gap_reason = ladder.get("source_gap_reason") or legacy.get("source_gap_reason")
    source_gap_stage = ladder.get("source_gap_stage") or legacy.get("source_gap_stage")
    source_gap_details = ladder.get("source_gap_details") or legacy.get("source_gap_details")
    if source_gap:
        market["momentum_ladder_overlay_source_gap"] = bool(ladder.get("source_gap", False))
        market["momentum_ladder_overlay_source_gap_reason"] = str(ladder.get("source_gap_reason") or "")
        market["momentum_ladder_overlay_source_gap_stage"] = str(ladder.get("source_gap_stage") or "")
    return {
        "ok": bool(legacy.get("ok", True) and ladder.get("ok", True)),
        "source_gap": source_gap,
        "source_gap_reason": source_gap_reason,
        "source_gap_stage": source_gap_stage,
        "source_gap_details": source_gap_details,
        "market": market,
        "orders": legacy_orders + ladder_orders,
        "fills": legacy_fills + ladder_fills,
        "queue_trades": legacy_queue + ladder_queue,
        "bid99_volume_events": list(legacy.get("bid99_volume_events", []) or [])
            + list(ladder.get("bid99_volume_events", []) or []),
        "bbo_meta": legacy.get("bbo_meta", {}) or ladder.get("bbo_meta", {}),
        "depth_meta": legacy.get("depth_meta", {}) or ladder.get("depth_meta", {}),
        "trade_meta": legacy.get("trade_meta", {}) or ladder.get("trade_meta", {}),
    }



def _signal_to_send_delay_ns(cfg: Config) -> int:
    setup = _delay_ns(cfg.wait_after_bid99_sec * 1000.0 + cfg.paper_signal_snapshot_delay_ms + cfg.paper_send_start_delay_ms)
    if cfg.polymarket_timestamp_clock == "venue":
        return max(0, setup - _delay_ns(cfg.polymarket_feed_delay_ms))
    return setup


# ---- DEPLOYGAP latency distributions (env-gated; default off) ----
_VANIA_LAT_CACHE: dict = {}


def _vania_lat_sample_ms(kind, key, default_ms):
    """Deterministic per-order draw from the live latency sample file in env Q99_LAT_SAMPLES_<kind>; default when unset."""
    path = os.environ.get("Q99_LAT_SAMPLES_" + kind, "")
    if not path:
        return default_ms
    arr = _VANIA_LAT_CACHE.get(path)
    if arr is None:
        with open(path) as fh:
            arr = [float(x) for x in fh.read().split() if x.strip()]
        if not arr:
            raise InternalContractError(f"empty latency sample file {path}")
        _VANIA_LAT_CACHE[path] = arr
    z = (int(key) + 0x9E3779B97F4A7C15) & 0xFFFFFFFFFFFFFFFF
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & 0xFFFFFFFFFFFFFFFF
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & 0xFFFFFFFFFFFFFFFF
    z ^= z >> 31
    return arr[z % len(arr)]


def _entry_order_timing(candidate: Mapping[str, Any], decision_ns: int, cfg: Config) -> dict[str, Any]:
    """Venue-anchored total, without counting observation latency twice.

    Send cannot precede the observation. The realized send-to-open interval is
    reported separately: configured stage values specify the original total.
    """
    setup = _delay_ns(cfg.wait_after_bid99_sec * 1000.0 + cfg.paper_signal_snapshot_delay_ms + cfg.paper_send_start_delay_ms)
    total = setup + _delay_ns(cfg.paper_shadow_order_open_delay_ms)
    anchor = int(decision_ns)
    if cfg.polymarket_timestamp_clock == "venue":
        if "venue_ts_ns" not in candidate or int(candidate["venue_ts_ns"]) < 0:
            raise InternalContractError("venue-mode candidate has no authoritative venue timestamp")
        source_venue_ns = int(candidate["venue_ts_ns"])
        anchor = source_venue_ns
        state_venue_ns = candidate.get("state_venue_ts_ns")
        if state_venue_ns is not None and not pd.isna(state_venue_ns):
            clock = candidate.get("candidate_clock")
            if clock not in {"raw_venue_event", "source_sequence_prefixmax_venue"}:
                raise InternalContractError("state timestamp requires documented venue-clock provenance")
            anchor = int(state_venue_ns)
            if clock == "raw_venue_event" and anchor != source_venue_ns:
                raise InternalContractError("raw venue event state timestamp must equal source timestamp")
            if anchor < source_venue_ns:
                raise InternalContractError("cumulative state publication precedes raw source venue timestamp")
        if int(decision_ns) != anchor + _delay_ns(cfg.polymarket_feed_delay_ms):
            raise InternalContractError("candidate observation must equal venue/state publication timestamp plus feed delay")
    _lat_open = _vania_lat_sample_ms("OPEN", anchor, None)
    if _lat_open is not None:
        total = max(setup, _delay_ns(_lat_open))
    send = max(int(decision_ns), anchor + setup)
    opened = anchor + total
    if opened < send:
        raise InternalContractError("configured venue-to-open total would open before an observable send")
    return {"order_timing_anchor_ts_ns": anchor, "order_send_ts_ns": send,
            "order_open_ts_ns": opened, "venue_to_open_total_ms": total / 1e6,
            "realized_send_to_open_ms": (opened - send) / 1e6,
            "send_clamped_to_observation": send > anchor + setup}


def _serialized_arm_send_ts_ns(row: dict[str, Any], cfg: Config) -> int:
    trigger_ns=int(row.get("trigger_ts_ns",-1) or -1)
    if trigger_ns<0:
        trigger_ms=q.as_float(row.get("trigger_ts_ms"),math.nan)
        if not math.isfinite(trigger_ms): return -1
        trigger_ns=q.ms_to_ns(trigger_ms)
    return trigger_ns + _signal_to_send_delay_ns(cfg)

def _serialized_arm_send_ts_ms(row: dict[str, Any], cfg: Config) -> float:
    ns=_serialized_arm_send_ts_ns(row,cfg); return q.ns_to_ms(ns) if ns>=0 else math.nan


def _serialized_arm_claims_slot(row: dict[str, Any], cfg: Config) -> bool:
    if not q.as_bool(row.get("entry_attempt_reached", False)):
        return False
    if cfg.serialized_fixed_arm_slot_claim_mode == "report_compatible":
        return True
    # Live-realistic mode: pre-open signal/filter rejections never generated a
    # venue send, therefore they cannot own or block the broker execution slot.
    return str(row.get("entry_attempt_result", "")) in {"opened", "placement_rejected"}


def _serialized_arm_release_ts_ns(row: dict[str, Any], cfg: Config, *, market_end_ns: int) -> int:
    send_ns=_serialized_arm_send_ts_ns(row,cfg)
    if send_ns<0:
        trigger_ns=int(row.get("trigger_ts_ns",-1) or -1); send_ns=trigger_ns if trigger_ns>=0 else int(market_end_ns)
    if q.as_bool(row.get("order_was_opened",False)):
        candidates=[int(market_end_ns)]
        cancel_ns=int(row.get("cancel_effective_ts_ns",-1) or -1)
        if cancel_ns<0:
            cancel_ms=q.as_float(row.get("cancel_effective_ts_ms"),math.nan); cancel_ns=q.ms_to_ns(cancel_ms) if math.isfinite(cancel_ms) else -1
        if cancel_ns>=0:candidates.append(cancel_ns)
        requested=max(0.0,q.as_float(row.get("requested_shares"),0.0)); filled=max(0.0,q.as_float(row.get("potential_filled_shares"),0.0))
        full_ns=int(row.get("full_fill_ts_ns",-1) or -1)
        if full_ns<0:
            full_ms=q.as_float(row.get("full_fill_ts_ms"),math.nan); full_ns=q.ms_to_ns(full_ms) if math.isfinite(full_ms) else -1
        if requested>0 and filled+1e-9>=requested and full_ns>=0:candidates.append(full_ns)
        release_ns=min(candidates)
    else:
        release_ns=int(row.get("entry_attempt_decision_ts_ns",-1) or -1)
        if release_ns<0:
            release_ns=int(row.get("order_open_ts_ns",-1) or -1)
        if release_ns<0:release_ns=send_ns
    return max(int(send_ns),int(release_ns))

def _serialized_arm_release_ts_ms(row: dict[str, Any], cfg: Config, *, market_end_ms: float) -> float:
    return q.ns_to_ms(_serialized_arm_release_ts_ns(row,cfg,market_end_ns=q.ms_to_ns(market_end_ms)))


def _serialized_remap_arm_result(
    result: dict[str, Any], *, arm_price: float, arm_shares: float
) -> dict[str, Any]:
    """Give every independent arm globally unique IDs and explicit audit tags."""
    arm_code = int(round(float(arm_price) * 100))
    mapping: dict[str, str] = {}
    for row in result.get("orders", []) or []:
        old = str(row.get("order_id", ""))
        if old:
            mapping[old] = f"{old}:SERL{arm_code:02d}"

    def map_row(source: dict[str, Any]) -> dict[str, Any]:
        row = dict(source)
        old = str(row.get("order_id", ""))
        if old in mapping:
            row["order_id"] = mapping[old]
        parent = str(row.get("post_only_cross_retry_parent_order_id", "") or "")
        if parent in mapping:
            row["post_only_cross_retry_parent_order_id"] = mapping[parent]
        row.update({
            "serialized_fixed_arms_enabled": True,
            "serialized_arm_price": float(arm_price),
            "serialized_arm_configured_fixed_shares": float(arm_shares),
            "serialized_arm_requested_shares": q.as_float(source.get("requested_shares"), arm_shares),
            "serialized_shadow_status": str(source.get("status", "")),
            "serialized_shadow_order_was_opened": q.as_bool(source.get("order_was_opened", False)),
            "serialized_shadow_potential_filled_shares": q.as_float(source.get("potential_filled_shares"), 0.0),
            "serialized_shadow_potential_pnl_usdc": q.as_float(source.get("potential_pnl_usdc"), 0.0),
        })
        return row

    orders = [map_row(row) for row in result.get("orders", []) or []]
    fills: list[dict[str, Any]] = []
    for source in result.get("fills", []) or []:
        row = dict(source)
        old = str(row.get("order_id", ""))
        if old in mapping:
            row["order_id"] = mapping[old]
        row["serialized_arm_price"] = float(arm_price)
        fills.append(row)
    queue_trades: list[dict[str, Any]] = []
    for source in result.get("queue_trades", []) or []:
        row = dict(source)
        old = str(row.get("order_id", ""))
        if old in mapping:
            row["order_id"] = mapping[old]
        row["serialized_arm_price"] = float(arm_price)
        queue_trades.append(row)
    volume_events: list[dict[str, Any]] = []
    for source in result.get("bid99_volume_events", []) or []:
        row = dict(source)
        old = str(row.get("order_id", ""))
        if old in mapping:
            row["order_id"] = mapping[old]
        row["serialized_arm_price"] = float(arm_price)
        volume_events.append(row)
    return dict(result) | {
        "orders": orders,
        "fills": fills,
        "queue_trades": queue_trades,
        "bid99_volume_events": volume_events,
    }


def _serialized_zero_execution_row(
    row: dict[str, Any], *, status: str, reason: str, blocked_by_arm: float | None = None
) -> dict[str, Any]:
    out = dict(row)
    out.update({
        "status": status,
        "skip_reason": reason,
        "order_was_opened": False,
        "balance_accepted": False,
        "serialized_selected_execution": False,
        "serialized_suppressed": status.startswith("skipped_serialized_arm_suppressed"),
        "serialized_blocked_by_arm_price": float(blocked_by_arm) if blocked_by_arm is not None else math.nan,
        "potential_filled_shares": 0.0,
        "potential_cost_usdc": 0.0,
        "potential_fee_usdc": 0.0,
        "potential_payout_usdc": 0.0,
        "potential_pnl_usdc": 0.0,
        "filled_shares": 0.0,
        "cost_usdc": 0.0,
        "fee_usdc": 0.0,
        "payout_usdc": 0.0,
        "net_pnl_usdc": 0.0,
        "fill_fraction": 0.0,
        "first_fill_ts_ms": math.nan,
        "last_fill_ts_ms": math.nan,
    })
    return out


def _evaluate_serialized_fixed_arms_session(
    task: q.ContractTask,
    cfg: Config,
    token_map: dict[str, str],
    resolutions: dict[int, str],
    underlying: q.UnderlyingMidSeries | None,
) -> dict[str, Any]:
    """Run fixed-price Q99 shadow arms and a native causal one-slot arbiter.

    Every arm uses the normal single-limit evaluator, including the mature FIFO,
    post-only, retry, cancellation and fill code.  The serializer controls only
    whether an arm is permitted to own the broker slot; it does not invent a
    second fill model.
    """
    prices = list(cfg.serialized_fixed_arm_price_list)
    shares = list(cfg.serialized_fixed_arm_share_list)
    momentum_mins = list(cfg.serialized_fixed_arm_momentum_min_list)
    early_momentum_mins = list(cfg.serialized_fixed_arm_early_momentum_min_list)
    momentum_apply_exact = list(cfg.serialized_fixed_arm_momentum_apply_exact_list)
    arm_divisors = list(cfg.serialized_fixed_arm_btc_move_divisor_list)
    market_end_ms = task.contract_start_ms + q.market_period_sec(cfg.market_key) * 1000
    arm_results: list[dict[str, Any]] = []
    market_start_lookup = None
    if cfg.final_sizing_volatility_enabled and underlying is not None:
        market_start_lookup = underlying.asof_observed(
            task.contract_start_ms, cfg.btc_mid_max_age_ms, cfg.binance_venue_to_vps_delay_ms
        )

    for arm_price, arm_shares, arm_momentum_min, arm_early_momentum_min, arm_apply_exact, arm_divisor in zip(
        prices, shares, momentum_mins, early_momentum_mins, momentum_apply_exact, arm_divisors
    ):
        arm_cfg = replace(
            cfg,
            serialized_fixed_arms_enabled=False,
            serialized_fixed_arm_require_report_eligibility=False,
            momentum_ladder_enabled=False,
            momentum_ladder_preserve_legacy_99=False,
            momentum_ladder_make_before_break=False,
            momentum_ladder_activation_source=f"serialized_fixed_arm_{arm_price:.2f}",
            trigger_bid=float(arm_price),
            bid99_binance_momentum_early_trigger_bid=float(arm_price - cfg.serialized_fixed_arm_early_offset),
            # Each fixed arm may have its own causal momentum threshold.
            bid99_binance_momentum_min_usd=float(arm_momentum_min),
            bid99_binance_momentum_early_min_usd=float(arm_early_momentum_min),
            bid99_binance_momentum_apply_to_99=bool(arm_apply_exact),
            share_sizing_mode=("fixed" if cfg.final_sizing_volatility_enabled else str(cfg.serialized_fixed_arm_sizing_mode)),
            fixed_shares_to_open=(float(cfg.max_shares_to_open) if cfg.final_sizing_volatility_enabled else float(arm_shares)),
            btc_move_divisor_usd=float(arm_divisor),
            final_sizing_volatility_enabled=False,
        )
        child = _evaluate_session(task, arm_cfg, token_map, resolutions, underlying)
        if not child.get("ok", True) or child.get("source_gap", False):
            # Fail closed for the whole serialized market.  Allowing only the
            # arms whose historical source happens to be present would create a
            # selection advantage that cannot exist in a complete live feed.
            market = dict(child.get("market", {}) or {})
            market.update({
                "serialized_fixed_arms_enabled": True,
                "serialized_fixed_arm_source_gap_arm_price": float(arm_price),
                "serialized_fixed_arm_source_complete": False,
            })
            return dict(child) | {"market": market, "orders": [], "fills": [], "queue_trades": []}
        remapped = _serialized_remap_arm_result(
            child, arm_price=float(arm_price), arm_shares=float(cfg.max_shares_to_open if cfg.final_sizing_volatility_enabled else arm_shares)
        )
        if cfg.final_sizing_volatility_enabled:
            # Lazy import avoids a module cycle at import time. This canonical
            # fallback follows the same max-shadow truncation as the optimizer.
            import queue99_multistrategy as multi
            remapped = multi._resized_arm_result(
                remapped, cfg=cfg, arm_price=float(arm_price),
                arm_divisor=float(arm_divisor), market_start_lookup=market_start_lookup,
                underlying=underlying,
            )
        arm_results.append(remapped)

    return _serialize_fixed_arm_results(task, cfg, arm_results)


def _serialize_fixed_arm_results(
    task: q.ContractTask,
    cfg: Config,
    arm_results: Sequence[dict[str, Any]],
    *, coordinator_order: bool = False,
) -> dict[str, Any]:
    """Serialize already-evaluated fixed-price arm results through one broker slot.

    This helper is intentionally source-I/O free. Multi-strategy optimizers can
    prepare each price arm once per market, derive causally smaller requested
    sizes from that maximum-size FIFO trace, and reuse the same arm results
    across many ACK/arm-set/sizing variants without rereading market files.
    """
    prices = list(cfg.serialized_fixed_arm_price_list)
    shares = list(cfg.serialized_fixed_arm_share_list)
    momentum_mins = list(cfg.serialized_fixed_arm_momentum_min_list)
    early_momentum_mins = list(cfg.serialized_fixed_arm_early_momentum_min_list)
    momentum_apply_exact = list(cfg.serialized_fixed_arm_momentum_apply_exact_list)
    arm_divisors = list(cfg.serialized_fixed_arm_btc_move_divisor_list)
    market_end_ms = task.contract_start_ms + q.market_period_sec(cfg.market_key) * 1000
    market_end_ns = q.ms_to_ns(market_end_ms)
    if len(arm_results) != len(prices):
        raise InternalContractError(
            f"serialized_precomputed_arm_count mismatch={len(arm_results)} expected={len(prices)}"
        )
    for arm_price, arm_result in zip(prices, arm_results):
        if not arm_result.get("ok", True) or arm_result.get("source_gap", False):
            market = dict(arm_result.get("market", {}) or {})
            market.update({
                "serialized_fixed_arms_enabled": True,
                "serialized_fixed_arm_source_gap_arm_price": float(arm_price),
                "serialized_fixed_arm_source_complete": False,
            })
            return dict(arm_result) | {"market": market, "orders": [], "fills": [], "queue_trades": []}
    attempts: list[dict[str, Any]] = []
    all_orders: list[dict[str, Any]] = []
    fill_by_id: dict[str, list[dict[str, Any]]] = defaultdict(list)
    queue_by_id: dict[str, list[dict[str, Any]]] = defaultdict(list)
    volume_by_id: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for arm_result in arm_results:
        for fill in arm_result.get("fills", []) or []:
            fill_by_id[str(fill.get("order_id", ""))].append(fill)
        for qr in arm_result.get("queue_trades", []) or []:
            queue_by_id[str(qr.get("order_id", ""))].append(qr)
        for ve in arm_result.get("bid99_volume_events", []) or []:
            volume_by_id[str(ve.get("order_id", ""))].append(ve)
        for row in arm_result.get("orders", []) or []:
            all_orders.append(row)
            if not q.as_bool(row.get("entry_attempt_reached", False)):
                continue
            tagged = dict(row)
            tagged["serialized_slot_send_ts_ns"] = _serialized_arm_send_ts_ns(tagged, cfg)
            tagged["serialized_slot_send_ts_ms"] = q.ns_to_ms(tagged["serialized_slot_send_ts_ns"]) if tagged["serialized_slot_send_ts_ns"] >= 0 else math.nan
            tagged["serialized_slot_claimed"] = _serialized_arm_claims_slot(tagged, cfg)
            tagged["serialized_slot_release_ts_ns"] = _serialized_arm_release_ts_ns(tagged, cfg, market_end_ns=market_end_ns)
            tagged["serialized_slot_release_ts_ms"] = q.ns_to_ms(tagged["serialized_slot_release_ts_ns"])
            attempts.append(tagged)

    if coordinator_order:
        # Fresh-event coordination has already ordered distinct source events,
        # and chosen high-arm priority within each event. Preserve that order
        # for equal sends; independent-arm streams retain their old policy.
        source_keys = {}
        for row in attempts:
            fields = ("entry_attempt_number", "portfolio_source_event_ts_ns",
                      "portfolio_source_event_seq", "portfolio_source_state_id",
                      "portfolio_source_event_ordinal")
            if not q.as_bool(row.get("portfolio_btc5_event_mode", False)) or any(
                not isinstance(row.get(name), (int, np.integer))
                or isinstance(row.get(name), (bool, np.bool_)) for name in fields
            ):
                raise InternalContractError("coordinator serializer missing or mixed source-order provenance")
            attempt = int(row["entry_attempt_number"])
            source_ns = int(row["portfolio_source_event_ts_ns"])
            if (attempt <= 0 or source_ns < 0
                    or source_ns > int(row["serialized_slot_send_ts_ns"])
                    or int(row["portfolio_source_event_ordinal"]) < 0
                    or attempt in source_keys):
                raise InternalContractError("coordinator serializer invalid or duplicate source-order provenance")
            source_keys[attempt] = (
                source_ns, int(row["portfolio_source_event_seq"]),
                int(row["portfolio_source_state_id"]),
                -int(round(float(row["serialized_arm_price"]) * 100)),
                0 if str(row.get("outcome", "")).upper() == "DOWN" else 1,
                int(row["portfolio_source_event_ordinal"]),
            )
        ordered_keys = [source_keys[index] for index in sorted(source_keys)]
        if ordered_keys != sorted(ordered_keys):
            raise InternalContractError("coordinator serializer offer order contradicts source-event order")
        attempts.sort(key=lambda row: (
            int(row["serialized_slot_send_ts_ns"]), int(row["entry_attempt_number"]),
        ))
    else:
        # Legacy independent-arm same-send policy remains higher-price-first.
        attempts.sort(key=lambda row: (
            int(row.get("serialized_slot_send_ts_ns", 2**63-1)),
            -q.as_float(row.get("serialized_arm_price"), -math.inf),
            q.as_int(row.get("entry_attempt_number"), 0),
            str(row.get("order_id", "")),
        ))

    suppressed_arms: set[float] = set()
    busy_until_ns = -1
    busy_release_ns = -1
    busy_arm: float | None = None
    busy_order_id: str | None = None
    selected_ids: set[str] = set()
    selected_orders: list[dict[str, Any]] = []
    selected_by_id: dict[str, dict[str, Any]] = {}
    forced_cancel_by_id: dict[str, dict[str, int | float]] = {}
    audit_rows: list[dict[str, Any]] = []
    counters = Counter()
    by_arm_slot_claims: Counter[str] = Counter()
    by_arm_blocked: Counter[str] = Counter()
    by_arm_selected_attempts: Counter[str] = Counter()
    by_arm_upshift_requests: Counter[str] = Counter()
    by_arm_upshift_candidates: Counter[str] = Counter()
    by_arm_upshift_rejected_price: Counter[str] = Counter()
    by_arm_upshift_rejected_owner_price: Counter[str] = Counter()
    by_arm_upshift_rejected_filled: Counter[str] = Counter()
    by_owner_upshift_requests: Counter[str] = Counter()
    by_arm_retry_continuations: Counter[str] = Counter()
    by_arm_retry_selected: Counter[str] = Counter()
    by_arm_retry_fulfilled_suppressed: Counter[str] = Counter()
    by_arm_retry_higher_superseded: Counter[str] = Counter()
    by_arm_last_blocked_busy_until_ns: dict[str, int] = {}
    by_arm_last_blocked_send_ns: dict[str, int] = {}
    by_arm_last_blocked_outcome: dict[str, str] = {}
    retry_fulfilled_arm_outcomes: set[tuple[float, str]] = set()
    highest_activated_arm_by_outcome: dict[str, float] = {}

    for source in attempts:
        row = dict(source)
        arm = float(row.get("serialized_arm_price"))
        oid = str(row.get("order_id", ""))
        send_ns = int(row.get("serialized_slot_send_ts_ns", -1)); send_ms = q.ns_to_ms(send_ns) if send_ns >= 0 else math.inf
        claims = q.as_bool(row.get("serialized_slot_claimed", False))
        outcome = str(row.get("outcome", ""))
        retry_continuation = bool(row.get("serialized_retry_shadow_continuation", False))
        retry_key = (round(float(arm), 12), outcome)
        if arm in suppressed_arms:
            counters["suppressed_rows"] += 1
            if cfg.serialized_fixed_arm_include_suppressed_audit:
                audit_rows.append(_serialized_zero_execution_row(
                    row,
                    status="skipped_serialized_arm_suppressed_for_contract",
                    reason="serialized_arm_suppressed_for_contract",
                    blocked_by_arm=busy_arm,
                ))
            continue
        if not claims:
            counters["pre_send_filter_rows"] += 1
            if cfg.serialized_fixed_arm_include_suppressed_audit:
                audit = _serialized_zero_execution_row(
                    row,
                    status="skipped_serialized_pre_send_filter_no_slot_claim",
                    reason="serialized_pre_send_filter_no_slot_claim",
                    blocked_by_arm=None,
                )
                audit["serialized_shadow_status"] = str(row.get("status", ""))
                audit_rows.append(audit)
            continue
        counters["slot_claim_attempts"] += 1
        arm_key = f"{arm:.2f}"
        by_arm_slot_claims[arm_key] += 1
        if not retry_continuation:
            previous_high = highest_activated_arm_by_outcome.get(outcome, -math.inf)
            if arm > previous_high:
                highest_activated_arm_by_outcome[outcome] = float(arm)
        else:
            counters["retry_continuation_claim_attempts"] += 1
            by_arm_retry_continuations[arm_key] += 1
            if retry_key in retry_fulfilled_arm_outcomes:
                counters["retry_continuation_suppressed_after_real_open"] += 1
                by_arm_retry_fulfilled_suppressed[arm_key] += 1
                if cfg.serialized_fixed_arm_include_suppressed_audit:
                    audit = _serialized_zero_execution_row(
                        row,
                        status="skipped_serialized_retry_already_fulfilled",
                        reason="serialized_pending_retry_stopped_after_real_open",
                        blocked_by_arm=None,
                    )
                    audit["serialized_retry_highest_activated_arm_price"] = float(highest_activated_arm_by_outcome.get(outcome, math.nan))
                    audit_rows.append(audit)
                continue
            highest_seen = highest_activated_arm_by_outcome.get(outcome, -math.inf)
            if (
                cfg.serialized_fixed_arm_retry_supersede_lower_on_higher_signal
                and math.isfinite(highest_seen)
                and arm < float(highest_seen) - 1e-9
            ):
                counters["retry_continuation_suppressed_by_higher_signal"] += 1
                by_arm_retry_higher_superseded[arm_key] += 1
                if cfg.serialized_fixed_arm_include_suppressed_audit:
                    audit = _serialized_zero_execution_row(
                        row,
                        status="skipped_serialized_retry_superseded_by_higher_signal",
                        reason="serialized_pending_lower_retry_retired_after_higher_signal",
                        blocked_by_arm=None,
                    )
                    audit["serialized_retry_highest_activated_arm_price"] = float(highest_seen)
                    audit_rows.append(audit)
                continue
        if busy_until_ns >= 0 and send_ns < busy_until_ns:
            counters["blocked_attempts"] += 1
            by_arm_blocked[arm_key] += 1
            different_arm = busy_arm is not None and abs(float(busy_arm) - arm) > 1e-9
            higher_arm = different_arm and busy_arm is not None and arm > float(busy_arm) + 1e-9
            # Live-realistic upshift experiment: a higher arm may request an
            # early cancellation of the currently live lower arm. The higher
            # attempt itself remains blocked; a later causal attempt may claim
            # the slot only after the lower cancel becomes effective plus ACK.
            owner = selected_by_id.get(busy_order_id) if busy_order_id is not None else None
            owner_open_ns = _row_time_ns((owner or {}), "order_open_ts_ns", "order_open_ts_ms")
            live_higher_candidate = bool(
                cfg.serialized_fixed_arm_busy_policy == "upshift_cancel_lower"
                and higher_arm
                and busy_order_id is not None
                and send_ns < busy_release_ns
                and owner_open_ns <= send_ns
            )
            if live_higher_candidate:
                counters["upshift_candidate_attempts"] += 1
                by_arm_upshift_candidates[arm_key] += 1
                floor = float(cfg.serialized_fixed_arm_upshift_min_replacement_price)
                price_allowed = floor < 0 or arm + 1e-12 >= floor
                owner_ceiling = float(cfg.serialized_fixed_arm_upshift_max_owner_price)
                owner_price_allowed = (
                    owner_ceiling < 0
                    or (busy_arm is not None and float(busy_arm) <= owner_ceiling + 1e-12)
                )
                lower_filled_before = 0.0
                if cfg.serialized_fixed_arm_upshift_require_unfilled_lower:
                    for fill in fill_by_id.get(str(busy_order_id), []):
                        fill_ns = int(fill.get("fill_ts_ns", -1) or -1)
                        if fill_ns < 0:
                            fill_ms = q.as_float(fill.get("fill_ts_ms"), math.nan)
                            fill_ns = q.ms_to_ns(fill_ms) if math.isfinite(fill_ms) else -1
                        if fill_ns >= 0 and fill_ns < send_ns:
                            lower_filled_before += max(0.0, q.as_float(fill.get("fill_shares"), 0.0))
                unfilled_allowed = (
                    not cfg.serialized_fixed_arm_upshift_require_unfilled_lower
                    or lower_filled_before <= 1e-9
                )
                if not price_allowed:
                    counters["upshift_rejected_replacement_price"] += 1
                    by_arm_upshift_rejected_price[arm_key] += 1
                    status = "skipped_serialized_arm_blocked_upshift_price_guard"
                    reason = "serialized_higher_arm_below_upshift_replacement_floor"
                elif not owner_price_allowed:
                    counters["upshift_rejected_owner_price"] += 1
                    by_arm_upshift_rejected_owner_price[arm_key] += 1
                    status = "skipped_serialized_arm_blocked_upshift_owner_price_guard"
                    reason = "serialized_lower_arm_above_upshift_owner_price_ceiling"
                elif not unfilled_allowed:
                    counters["upshift_rejected_lower_already_filled"] += 1
                    by_arm_upshift_rejected_filled[arm_key] += 1
                    status = "skipped_serialized_arm_blocked_lower_already_filled"
                    reason = "serialized_lower_order_already_partially_filled"
                else:
                    request_ns = int(send_ns)
                    effective_ns = min(int(busy_release_ns), request_ns + _delay_ns(float(cfg.serialized_fixed_arm_upshift_cancel_delay_ms)))
                    request_ms=q.ns_to_ms(request_ns); effective_ms=q.ns_to_ms(effective_ns)
                    if effective_ns < busy_release_ns:
                        if owner is not None:
                            previous = forced_cancel_by_id.get(busy_order_id)
                            if previous is None or effective_ns < int(previous["effective_ns"]):
                                forced_cancel_by_id[busy_order_id] = {
                                    "request_ns": request_ns, "effective_ns": effective_ns,
                                    "request_ms": request_ms, "effective_ms": effective_ms,
                                    "replacement_arm": float(arm),
                                }
                                owner.update({
                                    "serialized_upshift_cancel_requested": True,
                                    "serialized_upshift_cancel_request_ts_ns": request_ns,
                                    "serialized_upshift_cancel_effective_ts_ns": effective_ns,
                                    "serialized_upshift_cancel_request_ts_ms": request_ms,
                                    "serialized_upshift_cancel_effective_ts_ms": effective_ms,
                                    "serialized_upshift_requested_by_arm_price": float(arm),
                                    "serialized_upshift_lower_filled_before_request": float(lower_filled_before),
                                    "serialized_slot_release_ts_ns": effective_ns,
                                    "serialized_slot_release_ts_ms": effective_ms,
                                    "serialized_slot_busy_until_ts_ns": effective_ns + _delay_ns(cfg.serialized_fixed_arm_ack_ms),
                                    "serialized_slot_busy_until_ts_ms": q.ns_to_ms(effective_ns + _delay_ns(cfg.serialized_fixed_arm_ack_ms)),
                                })
                                busy_release_ns = effective_ns
                                busy_until_ns = effective_ns + _delay_ns(cfg.serialized_fixed_arm_ack_ms)
                                counters["upshift_cancel_requests"] += 1
                                by_arm_upshift_requests[arm_key] += 1
                                if busy_arm is not None:
                                    by_owner_upshift_requests[f"{float(busy_arm):.2f}"] += 1
                        status = "skipped_serialized_higher_arm_requested_upshift_cancel"
                        reason = "serialized_higher_arm_requested_lower_cancel"
                    else:
                        status = "skipped_serialized_arm_blocked_busy_slot"
                        reason = "serialized_arm_attempted_while_busy"
            elif different_arm and cfg.serialized_fixed_arm_suppress_competing:
                suppressed_arms.add(arm)
                counters["arms_suppressed_on_busy"] += 1
                status = "skipped_serialized_arm_suppressed_busy_slot"
                reason = "serialized_competing_arm_attempted_while_busy"
            else:
                if different_arm:
                    counters["different_arm_busy_attempts"] += 1
                else:
                    counters["same_arm_busy_attempts"] += 1
                status = "skipped_serialized_arm_blocked_busy_slot"
                reason = "serialized_arm_attempted_while_busy"
            # Compact continuation scheduler hint. Record the *resulting*
            # busy-until boundary after any upshift cancellation adjustment; a
            # lazy retry needs no candidate whose send occurs before this time.
            by_arm_last_blocked_busy_until_ns[arm_key] = int(busy_until_ns)
            by_arm_last_blocked_send_ns[arm_key] = int(send_ns)
            by_arm_last_blocked_outcome[arm_key] = str(outcome)
            if cfg.serialized_fixed_arm_include_suppressed_audit:
                audit_rows.append(_serialized_zero_execution_row(
                    row, status=status, reason=reason, blocked_by_arm=busy_arm
                ))
            continue

        # Slot is free at this send timestamp.
        busy_until_ns = -1
        busy_release_ns = -1
        busy_arm = None
        busy_order_id = None
        release_ns = int(row.get("serialized_slot_release_ts_ns", send_ns))
        busy_release_ns = release_ns
        busy_until_ns = release_ns + _delay_ns(cfg.serialized_fixed_arm_ack_ms)
        busy_arm = arm
        busy_order_id = oid if q.as_bool(row.get("order_was_opened", False)) else None
        row.update({
            "serialized_selected_attempt": True,
            "serialized_selected_execution": q.as_bool(row.get("order_was_opened", False)),
            "serialized_suppressed": False,
            "serialized_blocked_by_arm_price": math.nan,
            "serialized_slot_busy_until_ts_ns": int(busy_until_ns),
            "serialized_slot_busy_until_ts_ms": q.ns_to_ms(busy_until_ns),
            "serialized_slot_ack_ms": float(cfg.serialized_fixed_arm_ack_ms),
            "serialized_slot_claim_mode": str(cfg.serialized_fixed_arm_slot_claim_mode),
            "serialized_busy_policy": str(cfg.serialized_fixed_arm_busy_policy),
        })
        counters["selected_attempts"] += 1
        by_arm_selected_attempts[arm_key] += 1
        if q.as_bool(row.get("order_was_opened", False)):
            selected_ids.add(oid)
            selected_by_id[oid] = row
            retry_fulfilled_arm_outcomes.add(retry_key)
            if retry_continuation:
                counters["retry_continuation_selected_opened"] += 1
                by_arm_retry_selected[arm_key] += 1
            counters["selected_opened_orders"] += 1
        else:
            counters["selected_rejected_attempts"] += 1
        selected_orders.append(row)

    # If an upshift requested an earlier cancel, remove any shadow fills/trades
    # that occurred at or after that effective cancel and recompute the lower
    # order's potential PnL. This preserves one live execution slot and avoids
    # the double-counting problem of independently simulated overlapping orders.
    def _before_forced_cancel(oid: str, ts_ns: int) -> bool:
        cut = forced_cancel_by_id.get(oid)
        return cut is None or int(ts_ns) < int(cut["effective_ns"])

    selected_fills: list[dict[str, Any]] = []
    selected_queue: list[dict[str, Any]] = []
    for oid in selected_ids:
        selected_fills.extend([
            dict(fill) for fill in fill_by_id.get(oid, [])
            if _before_forced_cancel(oid, int(fill.get("fill_ts_ns", 2**63-1)))
        ])
        selected_queue.extend([
            dict(qr) for qr in queue_by_id.get(oid, [])
            if _before_forced_cancel(oid, int(qr.get("trade_ts_ns", 2**63-1)))
        ])
    selected_volume = [ve for oid in selected_ids for ve in volume_by_id.get(oid, [])]

    fills_by_selected: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for fill in selected_fills:
        fills_by_selected[str(fill.get("order_id", ""))].append(fill)
    for oid, cut in forced_cancel_by_id.items():
        row = selected_by_id.get(oid)
        if row is None:
            continue
        kept = sorted(fills_by_selected.get(oid, []), key=lambda x: int(x.get("fill_ts_ns", 2**63-1)))
        filled = float(sum(max(0.0, q.as_float(x.get("fill_shares"), 0.0)) for x in kept))
        cost = float(sum(
            max(0.0, q.as_float(x.get("fill_shares"), 0.0)) * q.as_float(x.get("fill_price"), row.get("limit_price", cfg.trigger_bid))
            for x in kept
        ))
        fee = float(sum(
            fee_for_shares(
                max(0.0, q.as_float(x.get("fill_shares"), 0.0)),
                q.as_float(x.get("fill_price"), row.get("limit_price", cfg.trigger_bid)),
                cfg,
            )
            for x in kept
        ))
        winner = str(row.get("winner", ""))
        outcome = str(row.get("outcome", ""))
        payout = filled if winner == outcome else 0.0
        pnl = payout - cost - fee
        requested = max(0.0, q.as_float(row.get("requested_shares"), 0.0))
        row.update({
            "cancel_reason": "serialized_higher_arm_upshift",
            "cancel_request_ts_ns": int(cut["request_ns"]),
            "cancel_effective_ts_ns": int(cut["effective_ns"]),
            "cancel_request_ts_ms": float(cut["request_ms"]),
            "cancel_effective_ts_ms": float(cut["effective_ms"]),
            "cancel_effective_before_market_end": int(cut["effective_ns"]) < market_end_ns,
            "status": "partially_filled_then_serialized_upshift_cancel" if filled > 0 else "cancelled_serialized_upshift",
            "potential_filled_shares": filled,
            "fill_fraction": filled / requested if requested > 0 else 0.0,
            "potential_cost_usdc": cost,
            "potential_fee_usdc": fee,
            "potential_average_fill_price": cost / filled if filled > 0 else math.nan,
            "potential_payout_usdc": payout,
            "potential_pnl_usdc": pnl,
            "first_fill_ts_ns": min([int(x.get("fill_ts_ns", 2**63-1)) for x in kept], default=-1),
            "first_fill_ts_ms": min([q.as_float(x.get("fill_ts_ms"), math.inf) for x in kept], default=math.nan),
            "last_fill_ts_ms": max([q.as_float(x.get("fill_ts_ms"), -math.inf) for x in kept], default=math.nan),
            "full_fill_ts_ns": -1, "full_fill_ts_ms": math.nan,
        })

    orders_out = selected_orders + audit_rows
    orders_out.sort(key=lambda row: (
        _row_time_ns(row, "trigger_ts_ns", "trigger_ts_ms"),
        -q.as_float(row.get("serialized_arm_price"), -math.inf),
        str(row.get("order_id", "")),
    ))

    # Hard causal invariant: selected slot-owning attempts cannot overlap.
    selected_slot_rows = sorted(
        [row for row in selected_orders if q.as_bool(row.get("serialized_selected_attempt", False))],
        key=lambda row: int(row.get("serialized_slot_send_ts_ns", 2**63-1)),
    )
    previous_busy_until_ns = -1
    overlap_violations = 0
    for row in selected_slot_rows:
        send_ns = int(row.get("serialized_slot_send_ts_ns", 2**63-1))
        if previous_busy_until_ns >= 0 and send_ns < previous_busy_until_ns:
            overlap_violations += 1
        previous_busy_until_ns = max(previous_busy_until_ns, int(row.get("serialized_slot_busy_until_ts_ns", send_ns)))
    if overlap_violations:
        raise InternalContractError(
            f"serialized_fixed_arm_slot_overlap contract_start_ms={task.contract_start_ms} violations={overlap_violations}"
        )

    by_arm_selected = Counter(
        f"{q.as_float(row.get('serialized_arm_price'), math.nan):.2f}"
        for row in selected_orders
        if q.as_bool(row.get("order_was_opened", False))
    )
    market_template = dict((arm_results[-1].get("market", {}) if arm_results else {}) or {})
    missing_total = sum(
        q.as_int((result.get("market") or {}).get("candidate_trades_blocked_missing_market_data"), 0)
        for result in arm_results
    )
    missing_stage = Counter()
    missing_component = Counter()
    for result in arm_results:
        arm_market = result.get("market") or {}
        missing_stage.update({
            str(k): q.as_int(v, 0)
            for k, v in (arm_market.get("blocked_missing_market_data_by_stage") or {}).items()
        })
        missing_component.update({
            str(k): q.as_int(v, 0)
            for k, v in (arm_market.get("blocked_missing_market_data_by_component") or {}).items()
        })
    market_template.update({
        "status": "evaluated_serialized_fixed_arms",
        "serialized_fixed_arms_enabled": True,
        "serialized_fixed_arm_source_complete": True,
        "serialized_fixed_arm_attempt_order": "coordinator_source_order" if coordinator_order else "independent_arm_high_price_ties",
        "serialized_fixed_arm_prices": list(prices),
        "serialized_fixed_arm_shares": list(shares),
        "serialized_fixed_arm_momentum_mins": list(momentum_mins),
        "serialized_fixed_arm_early_momentum_mins": list(early_momentum_mins),
        "serialized_fixed_arm_sizing_mode": str(cfg.serialized_fixed_arm_sizing_mode),
        "serialized_fixed_arm_btc_move_divisors": list(arm_divisors),
        "serialized_fixed_arm_busy_policy": str(cfg.serialized_fixed_arm_busy_policy),
        "serialized_fixed_arm_upshift_cancel_delay_ms": float(cfg.serialized_fixed_arm_upshift_cancel_delay_ms),
        "serialized_fixed_arm_upshift_min_replacement_price": float(cfg.serialized_fixed_arm_upshift_min_replacement_price),
        "serialized_fixed_arm_upshift_max_owner_price": float(cfg.serialized_fixed_arm_upshift_max_owner_price),
        "serialized_fixed_arm_upshift_require_unfilled_lower": bool(cfg.serialized_fixed_arm_upshift_require_unfilled_lower),
        "serialized_fixed_arm_ack_ms": float(cfg.serialized_fixed_arm_ack_ms),
        "serialized_fixed_arm_slot_claim_mode": str(cfg.serialized_fixed_arm_slot_claim_mode),
        "serialized_fixed_arm_suppress_competing": bool(cfg.serialized_fixed_arm_suppress_competing),
        "serialized_fixed_arm_suppressed_prices": sorted(suppressed_arms),
        "serialized_fixed_arm_overlap_violations": int(overlap_violations),
        "serialized_fixed_arm_selected_opened_by_price": dict(by_arm_selected),
        "serialized_fixed_arm_slot_claim_attempts_by_price": dict(by_arm_slot_claims),
        "serialized_fixed_arm_blocked_attempts_by_price": dict(by_arm_blocked),
        "serialized_fixed_arm_selected_attempts_by_price": dict(by_arm_selected_attempts),
        "serialized_fixed_arm_upshift_cancel_requests_by_price": dict(by_arm_upshift_requests),
        "serialized_fixed_arm_upshift_candidate_attempts_by_price": dict(by_arm_upshift_candidates),
        "serialized_fixed_arm_upshift_rejected_replacement_price_by_price": dict(by_arm_upshift_rejected_price),
        "serialized_fixed_arm_upshift_rejected_owner_price_by_price": dict(by_arm_upshift_rejected_owner_price),
        "serialized_fixed_arm_upshift_rejected_lower_already_filled_by_price": dict(by_arm_upshift_rejected_filled),
        "serialized_fixed_arm_upshift_cancel_requests_by_owner_price": dict(by_owner_upshift_requests),
        "serialized_fixed_arm_retry_blocked_signals": bool(cfg.serialized_fixed_arm_retry_blocked_signals),
        "serialized_fixed_arm_retry_supersede_lower_on_higher_signal": bool(cfg.serialized_fixed_arm_retry_supersede_lower_on_higher_signal),
        "serialized_fixed_arm_retry_continuation_claim_attempts_by_price": dict(by_arm_retry_continuations),
        "serialized_fixed_arm_retry_selected_opened_by_price": dict(by_arm_retry_selected),
        "serialized_fixed_arm_retry_fulfilled_suppressed_by_price": dict(by_arm_retry_fulfilled_suppressed),
        "serialized_fixed_arm_retry_higher_superseded_by_price": dict(by_arm_retry_higher_superseded),
        "serialized_fixed_arm_last_blocked_busy_until_ns_by_price": dict(by_arm_last_blocked_busy_until_ns),
        "serialized_fixed_arm_last_blocked_send_ns_by_price": dict(by_arm_last_blocked_send_ns),
        "serialized_fixed_arm_last_blocked_outcome_by_price": dict(by_arm_last_blocked_outcome),
        "serialized_fixed_arm_retry_highest_activated_arm_by_outcome": dict(highest_activated_arm_by_outcome),
        "serialized_fixed_arm_upshift_cancel_requests": int(counters.get("upshift_cancel_requests", 0)),
        "serialized_fixed_arm_upshift_candidate_attempts": int(counters.get("upshift_candidate_attempts", 0)),
        "serialized_fixed_arm_upshift_rejected_replacement_price": int(counters.get("upshift_rejected_replacement_price", 0)),
        "serialized_fixed_arm_upshift_rejected_owner_price": int(counters.get("upshift_rejected_owner_price", 0)),
        "serialized_fixed_arm_upshift_rejected_lower_already_filled": int(counters.get("upshift_rejected_lower_already_filled", 0)),
        "serialized_fixed_arm_same_arm_busy_attempts": int(counters.get("same_arm_busy_attempts", 0)),
        "serialized_fixed_arm_different_arm_busy_attempts": int(counters.get("different_arm_busy_attempts", 0)),
        "serialized_fixed_arm_momentum_apply_exact_flags": list(momentum_apply_exact),
        "candidate_trades_blocked_missing_market_data": int(missing_total),
        "blocked_missing_market_data_by_stage": dict(missing_stage),
        "blocked_missing_market_data_by_component": dict(missing_component),
        **{f"serialized_fixed_arm_{key}": int(value) for key, value in counters.items()},
    })
    return {
        "ok": True,
        "source_gap": False,
        "market": market_template,
        "orders": orders_out,
        "fills": selected_fills,
        "queue_trades": selected_queue,
        "bid99_volume_events": selected_volume,
        "bbo_meta": arm_results[-1].get("bbo_meta", {}) if arm_results else {},
        "depth_meta": arm_results[-1].get("depth_meta", {}) if arm_results else {},
        "trade_meta": arm_results[-1].get("trade_meta", {}) if arm_results else {},
    }


def _vania_block_event(cfg: Config) -> bool:
    """REPRO (env-gated): serialized busy_policy=block (keep_queue) replayed with the live Go block-mode event lifecycle."""
    if not bool(cfg.serialized_fixed_arms_enabled) or cfg.portfolio_btc5_event_mode:
        return False
    pol = str(cfg.serialized_fixed_arm_busy_policy)
    return ((os.environ.get("Q99_LIVE_BLOCK_EVENTS", "0") == "1" and pol == "block")
            or (os.environ.get("Q99_LIVE_RB_EVENTS", "0") == "1" and pol == "upshift_cancel_lower"))


def _evaluate_session(task: q.ContractTask, cfg: Config, token_map: dict[str, str], resolutions: dict[int, str], underlying: q.UnderlyingMidSeries | None, *, candidate_override: Mapping[str, Any] | None = None) -> dict[str, Any]:
    q.set_polymarket_clock(cfg.polymarket_timestamp_clock, cfg.polymarket_feed_delay_ms)
    if cfg.polymarket_timestamp_clock == "venue" and cfg.momentum_ladder_enabled:
        raise ValueError("venue-clock mode supports the legacy and serialized engines in this release; dynamic ladder mode is not selected by the ETH suite")
    if cfg.portfolio_btc5_event_mode:
        if candidate_override is not None:
            raise InternalContractError("portfolio parent cannot accept a single-limit override")
        from queue99_portfolio_btc5 import evaluate_session
        return evaluate_session(task, cfg, token_map, resolutions, underlying)
    if _vania_block_event(cfg) and candidate_override is None:
        from queue99_portfolio_btc5 import evaluate_session
        return evaluate_session(task, cfg, token_map, resolutions, underlying, block_mode=True,
                                rb_mode=str(cfg.serialized_fixed_arm_busy_policy) == "upshift_cancel_lower")
    if cfg.serialized_fixed_arms_enabled:
        return _evaluate_serialized_fixed_arms_session(task, cfg, token_map, resolutions, underlying)
    if cfg.momentum_ladder_enabled and cfg.momentum_ladder_preserve_legacy_99:
        # Run the exact pre-ladder state machine first with only the ladder flag
        # disabled.  All user controls for 0.99 (early trigger, retries, momentum
        # scope, post-only, adverse cancellation, FIFO fill logic, etc.) remain
        # untouched, which makes the top-rung output reproducible with a
        # ladder-disabled run using the same settings.
        legacy_cfg = replace(
            cfg,
            momentum_ladder_enabled=False,
            momentum_ladder_preserve_legacy_99=False,
            momentum_ladder_make_before_break=False,
            momentum_ladder_activation_source="additive_legacy99_child",
        )
        legacy = _evaluate_session(task, legacy_cfg, token_map, resolutions, underlying)
        if not legacy.get("ok", True) or legacy.get("source_gap", False):
            return legacy

        # The overlay owns only prices strictly below TRIGGER_BID.  The highest
        # valid lower rung is exactly one configured tick below the preserved
        # legacy top price, preventing duplicate 0.99 orders.
        tick = float(cfg.momentum_ladder_tick)
        effective_lower_max = _effective_lower_ladder_max_bid(cfg)
        if effective_lower_max + float(cfg.trigger_tolerance) < float(cfg.momentum_ladder_start_bid) + tick:
            ladder = _empty_ladder_overlay_result(
                task, cfg, resolutions, status="no_executable_lower_ladder_rung"
            )
        else:
            ladder_cfg = replace(
                cfg,
                momentum_ladder_max_bid=effective_lower_max,
                momentum_ladder_preserve_legacy_99=False,
            )
            ladder = _evaluate_ladder_session(
                task, ladder_cfg, token_map, resolutions, underlying,
                legacy_top_open_by_side=_legacy_top_open_times(legacy, cfg),
            )
        if not ladder.get("ok", True) and not ladder.get("source_gap", False):
            return ladder
        # A lower-overlay source gap must never erase a valid preserved legacy
        # 0.99 result.  Keep the legacy orders/fills byte-for-byte, mark the
        # market as ladder-incomplete, and let SOURCE_GAP_POLICY account for the
        # missing lower-layer opportunity separately.
        return _merge_additive_ladder_results(
            legacy, ladder, cfg, effective_lower_max_bid=effective_lower_max
        )
    if cfg.momentum_ladder_enabled:
        return _evaluate_ladder_session(task, cfg, token_map, resolutions, underlying)
    data_dir = Path(cfg.data_dir)
    period_ms = q.market_period_sec(cfg.market_key) * 1000
    market_end_ms = task.contract_start_ms + period_ms
    market_end_ns = q.ms_to_ns(market_end_ms)
    momentum_early_trigger_enabled = cfg.momentum_early_trigger_enabled
    retry_enabled = cfg.entry_retry_policy != "never"
    post_only_cross_retry_enabled = cfg.post_only_cross_retry_delay_ms >= 0
    optimizer_send_not_before_ns = int(cfg.optimizer_entry_not_before_send_ns)
    optimizer_send_delay_ns = _signal_to_send_delay_ns(cfg)
    optimizer_trigger_not_before_ns = (
        optimizer_send_not_before_ns - optimizer_send_delay_ns
        if optimizer_send_not_before_ns >= 0 else -1
    )
    effective_age_start_sec = float(cfg.entry_market_age_start_sec)
    if optimizer_trigger_not_before_ns >= 0:
        effective_age_start_sec = max(
            effective_age_start_sec,
            (optimizer_trigger_not_before_ns - q.ms_to_ns(task.contract_start_ms)) / 1_000_000_000.0,
        )
    if candidate_override is None:
        raw_candidates, bbo_meta = q.load_trigger_candidates_fast(
            task, data_dir=data_dir, market_key=cfg.market_key, token_map=token_map,
            yes_outcome=cfg.yes_outcome, trigger_bid=cfg.trigger_bid, tolerance=cfg.trigger_tolerance,
            age_start_sec=effective_age_start_sec, age_end_sec=cfg.entry_market_age_end_sec,
            order_trigger_policy=cfg.order_trigger_policy,
            minimum_trigger_bid=(cfg.bid99_binance_momentum_early_trigger_bid if momentum_early_trigger_enabled else None),
            # Later rows are retained only for an enabled broad retry policy or
            # the dedicated post-only-cross retry. With both disabled, the loader
            # returns only the first policy-selected opportunity and avoids the
            # associated BBO retention and Binance momentum revalidation work.
            return_all_candidates=bool(retry_enabled or post_only_cross_retry_enabled),
        )
    else:
        # A coordinator supplies exactly the event it is processing, never a
        # future candidate list. All existing signal/placement/FIFO gates below
        # still execute unchanged. This private hook is not a CLI setting.
        row = dict(candidate_override)
        event_ns = _row_time_ns(row, "ts_ns", "ts_ms")
        age = (event_ns - int(task.contract_start_ms) * 1_000_000) / 1e9
        if str(row.get("side", "")) not in q.OUTCOMES or not (cfg.entry_market_age_start_sec <= age <= cfg.entry_market_age_end_sec):
            raise InternalContractError("single-event override outside configured outcome/age")
        # Don't trust cached decision results from another arm/configuration.
        row = {key: value for key, value in row.items() if not str(key).startswith("_")}
        raw_candidates = [row]
        bbo_meta = {"source": "portfolio_single_causal_event", "raw_rows": 1,
                    "triggers_before_policy": 1, "triggers": 1}
    optimizer_locked_outcome = str(cfg.optimizer_entry_locked_outcome or "").upper()
    if optimizer_locked_outcome:
        raw_candidates = [
            row for row in raw_candidates
            if str(row.get("side", "")).upper() == optimizer_locked_outcome
        ]
    if optimizer_send_not_before_ns >= 0:
        raw_candidates = [
            row for row in raw_candidates
            if _row_time_ns(row, "ts_ns", "ts_ms") + optimizer_send_delay_ns >= optimizer_send_not_before_ns
        ]
    if momentum_early_trigger_enabled:
        candidates, early_meta = _select_momentum_early_entry_candidates(
            raw_candidates, cfg=cfg, underlying=underlying
        )
        bbo_meta = dict(bbo_meta) | early_meta
        bbo_meta.update({
            "triggers_before_policy": early_meta["entry_triggers_before_policy"],
            "triggers": early_meta["entry_triggers_after_policy"],
            "triggers_dropped_by_policy": early_meta["entry_triggers_dropped_by_policy"],
        })
    else:
        annotated: list[dict[str, Any]] = []
        for candidate in raw_candidates:
            row = dict(candidate)
            row.setdefault("entry_trigger_source", "exact_bid99")
            row.setdefault("entry_trigger_is_exact_bid99", True)
            row.setdefault("entry_trigger_observed_best_bid", q.as_float(row.get("best_bid"), math.nan))
            row.setdefault("entry_trigger_bid_floor", cfg.trigger_bid)
            annotated.append(row)
        candidates, retry_meta = _apply_entry_retry_candidate_policy(annotated, cfg=cfg)
        bbo_meta = dict(bbo_meta) | retry_meta
        bbo_meta.update({
            "triggers_before_policy": retry_meta["entry_candidate_rows_before_retry_policy"],
            "triggers": len(candidates),
            "triggers_dropped_by_policy": max(
                0,
                int(retry_meta["entry_candidate_rows_before_retry_policy"]) - len(candidates),
            ),
        })
    candidates_all_count = int(bbo_meta.get("triggers_before_policy", len(candidates)))
    market = {
        "contract_start_ms": task.contract_start_ms,
        "market_start_utc": q.market_iso(task.contract_start_ms),
        "market_end_utc": q.market_iso(market_end_ms),
        "source_session_id": task.session_id,
        "bbo_dir": task.bbo_dir,
        "status": ("no_momentum_qualified_entry_trigger" if momentum_early_trigger_enabled and not candidates else "no_bid99_trigger" if not candidates else "triggered"),
        "simulation_valid": True,
        "bid99_candidates_before_policy": candidates_all_count,
        "bid99_candidates_after_policy": len(candidates),
        "momentum_early_trigger_enabled": momentum_early_trigger_enabled,
        "bid99_binance_momentum_apply_to_99": bool(cfg.bid99_binance_momentum_apply_to_99),
        "exact_bid99_momentum_gate_enabled": bool(cfg.exact_bid99_momentum_gate_enabled),
        "momentum_early_trigger_bid": cfg.bid99_binance_momentum_early_trigger_bid if momentum_early_trigger_enabled else math.nan,
        "post_only_order": cfg.post_only_order_enabled,
        "entry_retry_policy": cfg.entry_retry_policy,
        "post_only_cross_retry_enabled": post_only_cross_retry_enabled,
        "post_only_cross_retry_delay_ms": (
            float(cfg.post_only_cross_retry_delay_ms) if post_only_cross_retry_enabled else math.nan
        ),
        "entry_trigger_source_counts": dict(Counter(str(r.get("entry_trigger_source", "")) for r in candidates)),
        "entry_market_age_start_sec": cfg.entry_market_age_start_sec,
        "entry_market_age_end_sec": cfg.entry_market_age_end_sec,
        "bbo_rows": int(bbo_meta.get("raw_rows", 0)),
        "optimizer_entry_not_before_send_ns": int(optimizer_send_not_before_ns),
        "optimizer_entry_locked_outcome": optimizer_locked_outcome,
    }
    if not candidates:
        _update_missing_market_data_counters(market, [])
        return {
            "ok": True, "source_gap": False, "market": market, "orders": [], "fills": [],
            "queue_trades": [], "bid99_volume_events": [], "bbo_meta": bbo_meta,
        }

    prepared: list[dict[str, Any]] = []
    skipped_orders: list[dict[str, Any]] = []
    for candidate_position, candidate in enumerate(candidates, start=1):
        trigger_ns = int(candidate.get("ts_ns", q.ms_to_ns(float(candidate["ts_ms"]))))
        trigger_ms = q.ns_to_ms(trigger_ns)
        side = str(candidate["side"])
        base = {
            "order_id": f"{task.contract_start_ms}:{side}:{trigger_ns}",
            "contract_start_ms": task.contract_start_ms,
            "market_start_utc": q.market_iso(task.contract_start_ms),
            "market_end_utc": q.market_iso(market_end_ms),
            "outcome": side,
            "trigger_ts_ns": trigger_ns,
            "trigger_venue_ts_ns": int(candidate.get("venue_ts_ns", trigger_ns)),
            "trigger_state_venue_ts_ns": (
                int(candidate["state_venue_ts_ns"])
                if candidate.get("state_venue_ts_ns") is not None and not pd.isna(candidate.get("state_venue_ts_ns"))
                else int(candidate.get("venue_ts_ns", trigger_ns))),
            "trigger_candidate_clock": str(candidate.get("candidate_clock", "raw_venue_event")),
            "trigger_observed_ts_ns": trigger_ns,
            "polymarket_timestamp_clock": cfg.polymarket_timestamp_clock,
            "polymarket_feed_delay_ms": cfg.polymarket_feed_delay_ms if cfg.polymarket_timestamp_clock == "venue" else 0.0,
            "trigger_ts_ms": trigger_ms,
            "trigger_time_utc": q.market_iso(trigger_ms),
            "trigger_bid": cfg.trigger_bid,
            "limit_price": cfg.trigger_bid,
            "entry_trigger_source": str(candidate.get("entry_trigger_source", "exact_bid99")),
            "entry_trigger_is_exact_bid99": bool(candidate.get("entry_trigger_is_exact_bid99", True)),
            "entry_trigger_observed_best_bid": q.as_float(candidate.get("best_bid"), math.nan),
            "entry_trigger_observed_bid_size": q.as_float(candidate.get("bid_size"), math.nan),
            "entry_trigger_observed_best_ask": q.as_float(candidate.get("best_ask"), math.nan),
            "entry_trigger_observed_ask_size": q.as_float(candidate.get("ask_size"), math.nan),
            "entry_trigger_bid_floor": q.as_float(candidate.get("entry_trigger_bid_floor"), cfg.trigger_bid),
            "entry_candidate_index": int(candidate.get("entry_candidate_index", candidate_position)),
            "entry_retry_policy": cfg.entry_retry_policy,
            "post_only_cross_retry_enabled": post_only_cross_retry_enabled,
            "post_only_cross_retry_delay_ms": (
                float(cfg.post_only_cross_retry_delay_ms) if post_only_cross_retry_enabled else math.nan
            ),
            "post_only_order": cfg.post_only_order_enabled,
            "market_age_at_trigger_sec": (trigger_ms - task.contract_start_ms) / 1000.0,
            "source_session_id": task.session_id,
            "simulation_valid": True,
            "balance_accepted": False,
            "filled_shares": 0.0,
            "potential_filled_shares": 0.0,
            "net_pnl_usdc": 0.0,
            "potential_pnl_usdc": 0.0,
        }
        signal_best_bid = q.as_float(candidate.get("best_bid"), math.nan)
        signal_best_bid_size = q.as_float(candidate.get("bid_size"), math.nan)
        signal_best_ask = q.as_float(candidate.get("best_ask"), math.nan)
        signal_best_ask_size = q.as_float(candidate.get("ask_size"), math.nan)
        signal_missing = _missing_signal_bbo_components(best_bid=signal_best_bid)
        optional_signal_missing = _optional_signal_bbo_missing_components(
            best_bid=signal_best_bid, best_bid_size=signal_best_bid_size,
            best_ask=signal_best_ask, best_ask_size=signal_best_ask_size,
        )
        base.update({
            "signal_bbo_optional_missing_components": ",".join(optional_signal_missing),
            "signal_bbo_optional_missing_component_count": len(optional_signal_missing),
            "signal_bbo_has_optional_missing_data": bool(optional_signal_missing),
        })
        if optional_signal_missing:
            market["signal_bbo_candidates_with_optional_missing_data"] = (
                q.as_int(market.get("signal_bbo_candidates_with_optional_missing_data"), 0) + 1
            )
            optional_counts = Counter({
                str(k): q.as_int(v, 0)
                for k, v in (market.get("signal_bbo_optional_missing_by_component") or {}).items()
            })
            optional_counts.update(optional_signal_missing)
            market["signal_bbo_optional_missing_by_component"] = dict(optional_counts)
        if signal_missing:
            skipped_orders.append(base | _missing_book_fields("signal_bbo", signal_missing) | {
                "status": "skipped_missing_bbo_component_at_trigger",
                "skip_reason": "missing_bbo_component_at_trigger",
            })
            continue

        start_lookup: dict[str, Any] | None = None
        trigger_lookup: dict[str, Any] | None = candidate.get("_trigger_lookup")
        is_exact_bid99 = bool(candidate.get("entry_trigger_is_exact_bid99", True))
        momentum_required = cfg.momentum_required_for_trigger(is_exact_bid99=is_exact_bid99)
        btc_trading_needed = (
            cfg.share_sizing_mode == "btc_move"
            or cfg.cancel_if_binance_mid_below_trigger
            or momentum_required
            or cfg.entry_filter_data_needed
        )
        if btc_trading_needed:
            if underlying is None:
                raise RuntimeError("Binance underlying mid series was not loaded for enabled trading logic")
            if trigger_lookup is None:
                trigger_lookup = underlying.asof_observed_ns(
                    trigger_ns, cfg.btc_mid_max_age_ms, cfg.binance_venue_to_vps_delay_ms
                )
            if trigger_lookup is None:
                skipped_orders.append(base | {
                    "status": "skipped_missing_or_stale_btc_trigger_mid",
                    "skip_reason": "missing_or_stale_btc_trigger_mid",
                    "btc_mid_source": underlying.source,
                    "btc_timestamp_clock": "venue",
                    "binance_venue_to_vps_delay_ms": cfg.binance_venue_to_vps_delay_ms,
                    "btc_trigger_lookup_required_by": "+".join(
                        name
                        for name, active in (
                            ("sizing", cfg.share_sizing_mode == "btc_move"),
                            ("binance_adverse_cancel", cfg.cancel_if_binance_mid_below_trigger),
                            ("bid99_binance_momentum_gate", momentum_required),
                        )
                        if active
                    ),
                })
                continue

        if candidate.get("_momentum_evaluated"):
            momentum_passed = bool(candidate.get("_momentum_passed"))
            momentum_fields = dict(candidate.get("_momentum_fields") or {})
            momentum_skip_reason = str(candidate.get("_momentum_skip_reason") or "")
        else:
            momentum_passed, momentum_fields, momentum_skip_reason = _bid99_binance_momentum_gate(
                trigger_ms=trigger_ms, trigger_ns=trigger_ns,
                outcome=side,
                cfg=cfg,
                underlying=underlying,
                current_lookup=trigger_lookup,
                required_for_candidate=momentum_required,
                trigger_source=str(candidate.get("entry_trigger_source", "exact_bid99")),
            )
        if not momentum_passed:
            status = (
                "skipped_missing_or_stale_bid99_binance_momentum_reference"
                if momentum_skip_reason == "missing_or_stale_bid99_binance_momentum_reference"
                else "skipped_bid99_binance_momentum_not_confirmed"
            )
            skipped_orders.append(base | momentum_fields | {
                "status": status,
                "skip_reason": momentum_skip_reason,
                "btc_mid_source": underlying.source if underlying is not None else "missing",
                "btc_timestamp_clock": "venue",
                "binance_venue_to_vps_delay_ms": cfg.binance_venue_to_vps_delay_ms,
                "btc_trigger_mid": (
                    float(trigger_lookup["price"]) if trigger_lookup is not None else math.nan
                ),
            })
            continue

        # Run at the actual source candidate, BEFORE order preparation and slot arbitration.
        # An rejected candidate cannot consume FIFO priority, reserve cash, or receive a fill.
        filter_fields: dict[str, Any] = {}
        if cfg.entry_filter_data_needed:
            from queue99_entry_filters import evaluate_entry_filters
            filter_ok, filter_fields, filter_reason = evaluate_entry_filters(
                trigger_ns=int(trigger_ns), contract_start_ns=int(task.contract_start_ms) * 1_000_000,
                market_period_sec=q.market_period_sec(cfg.market_key), outcome=side,
                cfg=cfg, underlying=underlying,
            )
            counts = market.setdefault("entry_filter_candidate_counts", {})
            counts["evaluated"] = int(counts.get("evaluated", 0)) + 1
            label = "passed" if filter_ok else filter_reason
            counts[label] = int(counts.get(label, 0)) + 1
            if not filter_ok:
                skipped_orders.append(base | momentum_fields | filter_fields | {
                    "status": "skipped_entry_filter", "skip_reason": filter_reason,
                })
                continue

        if cfg.share_sizing_mode == "fixed":
            requested = float(cfg.fixed_shares_to_open)
            start_price = signed_move = move = math.nan
            trigger_price = (
                float(trigger_lookup["price"])
                if trigger_lookup is not None else math.nan
            )
            sizing = {
                "raw_shares": requested,
                "rounded_shares": int(math.floor(requested + 0.5)),
                "requested_shares": requested,
                "shares_clipped_low": False,
                "shares_clipped_high": False,
            }
        else:
            assert underlying is not None and trigger_lookup is not None
            start_lookup = underlying.asof_observed(
                task.contract_start_ms,
                cfg.btc_mid_max_age_ms,
                cfg.binance_venue_to_vps_delay_ms,
            )
            if start_lookup is None:
                skipped_orders.append(base | {
                    "status": "skipped_missing_or_stale_btc_market_start_mid",
                    "skip_reason": "missing_or_stale_btc_market_start_mid",
                    "btc_mid_source": underlying.source,
                    "btc_timestamp_clock": "venue",
                    "binance_venue_to_vps_delay_ms": cfg.binance_venue_to_vps_delay_ms,
                })
                continue
            start_price = float(start_lookup["price"])
            trigger_price = float(trigger_lookup["price"])
            signed_move = trigger_price - start_price
            move = abs(signed_move)
            if move + 1e-12 < cfg.min_btc_move_usd:
                skipped_orders.append(base | {
                    "status": "skipped_btc_move_below_minimum",
                    "skip_reason": "btc_move_below_minimum",
                    "btc_mid_source": underlying.source,
                    "btc_timestamp_clock": "venue",
                    "binance_venue_to_vps_delay_ms": cfg.binance_venue_to_vps_delay_ms,
                    "btc_market_start_mid": start_price,
                    "btc_trigger_mid": trigger_price,
                    "btc_move_signed_usd": signed_move,
                    "btc_move_abs_usd": move,
                    "min_btc_move_usd": cfg.min_btc_move_usd,
                })
                continue
            sizing = q.share_size_from_move(
                move,
                divisor_usd=cfg.btc_move_divisor_usd,
                multiplier=cfg.btc_shares_multiplier,
                min_shares=cfg.min_shares_to_open,
                max_shares=cfg.max_shares_to_open,
            )
            requested = float(sizing["requested_shares"])
        signal_sources = {"source_bbo": int(trigger_ns)}
        if filter_fields:
            signal_sources["normalized_entry_features"] = int(filter_fields.get("entry_filter_max_input_ts_ns", -1))
        strict_signal_prior: set[str] = set()
        if trigger_lookup is not None:
            signal_sources["btc_feature"] = int(trigger_lookup.get("observed_ts_ns", -1))
        prior_ns = int(momentum_fields.get("bid99_binance_momentum_reference_observed_ts_ns", -1) or -1)
        if prior_ns > 0:
            signal_sources["prior_feature"] = prior_ns
            strict_signal_prior.add("prior_feature")
        signal_audit = _causal_audit_fields(
            decision_ns=trigger_ns, sources=signal_sources, strict_prior=strict_signal_prior,
            enabled=cfg.strict_causal_audit,
        )
        signal_audit.update({
            "bbo_preprocessing_window_end_ns": int(trigger_ns),
            "btc_preprocessing_window_end_ns": int(trigger_ns),
        })
        timing = _entry_order_timing(candidate, trigger_ns, cfg)
        base.update(timing)
        order_open_ns = timing["order_open_ts_ns"]
        order_open_ms = q.ns_to_ms(order_open_ns)
        prepared.append(base | sizing | momentum_fields | filter_fields | signal_audit | {
            "status": "prepared",
            "skip_reason": "",
            "btc_mid_source": underlying.source if underlying is not None else "fixed",
            "btc_timestamp_clock": "venue" if underlying is not None else "not_used",
            "binance_venue_to_vps_delay_ms": cfg.binance_venue_to_vps_delay_ms,
            "btc_market_start_mid": start_price,
            "btc_trigger_mid": trigger_price,
            "btc_market_start_venue_ts_ms": (
                float(start_lookup["venue_ts_ms"]) if start_lookup is not None else math.nan
            ),
            "btc_market_start_observed_ts_ms": (
                float(start_lookup["observed_ts_ms"]) if start_lookup is not None else math.nan
            ),
            "btc_market_start_observed_age_ms": (
                float(start_lookup["observed_age_ms"]) if start_lookup is not None else math.nan
            ),
            "btc_market_start_venue_age_ms": (
                float(start_lookup["venue_age_ms"]) if start_lookup is not None else math.nan
            ),
            "btc_trigger_venue_ts_ms": (
                float(trigger_lookup["venue_ts_ms"]) if trigger_lookup is not None else math.nan
            ),
            "btc_trigger_observed_ts_ms": (
                float(trigger_lookup["observed_ts_ms"]) if trigger_lookup is not None else math.nan
            ),
            "btc_trigger_observed_age_ms": (
                float(trigger_lookup["observed_age_ms"]) if trigger_lookup is not None else math.nan
            ),
            "btc_trigger_venue_age_ms": (
                float(trigger_lookup["venue_age_ms"]) if trigger_lookup is not None else math.nan
            ),
            "btc_trigger_series_index": (
                int(trigger_lookup["index"]) if trigger_lookup is not None else -1
            ),
            "btc_move_signed_usd": signed_move,
            "btc_move_abs_usd": move,
            "min_btc_move_usd": cfg.min_btc_move_usd,
            "wait_after_bid99_sec": cfg.wait_after_bid99_sec,
            "paper_signal_snapshot_delay_ms": cfg.paper_signal_snapshot_delay_ms,
            "paper_send_start_delay_ms": cfg.paper_send_start_delay_ms,
            "paper_shadow_order_open_delay_ms": cfg.paper_shadow_order_open_delay_ms,
            "limit_order_lifetime_sec": cfg.limit_order_lifetime_sec,
            "limit_order_lifetime_cancel_delay_ms": cfg.limit_order_lifetime_cancel_delay_ms,
            "order_open_ts_ns": order_open_ns,
            "order_open_ts_ms": order_open_ms,
            "order_open_time_utc": q.market_iso(order_open_ms),
            "requested_shares": requested,
            "limit_price": cfg.trigger_bid,
            "momentum_early_trigger_enabled": momentum_early_trigger_enabled,
            "momentum_early_cross_match_delay_ms": (
                cfg.momentum_early_cross_match_delay_ms
                if momentum_early_trigger_enabled and not cfg.post_only_order_enabled
                else math.nan
            ),
        })
    all_attempt_rows = skipped_orders + prepared
    if not prepared:
        _, reached_rejections, retry_meta = _select_reached_entry_attempts(
            all_attempt_rows,
            depth=pd.DataFrame(),
            cfg=cfg,
            market_end_ms=float(market_end_ms),
        )
        market.update(retry_meta)
        market["status"] = "triggers_skipped_before_order_open"
        _update_missing_market_data_counters(market, reached_rejections)
        return {
            "ok": True, "source_gap": False, "market": market, "orders": reached_rejections,
            "fills": [], "queue_trades": [], "bid99_volume_events": [], "bbo_meta": bbo_meta,
        }

    opens_before_end_ns = [
        int(row["order_open_ts_ns"])
        for row in prepared
        if int(row["order_open_ts_ns"]) < int(market_end_ns)
    ]
    if not opens_before_end_ns:
        _, reached_rejections, retry_meta = _select_reached_entry_attempts(
            all_attempt_rows,
            depth=pd.DataFrame(),
            cfg=cfg,
            market_end_ms=float(market_end_ms),
        )
        market.update(retry_meta)
        market["status"] = "order_open_after_market_end"
        _update_missing_market_data_counters(market, reached_rejections)
        return {
            "ok": True, "source_gap": False, "market": market, "orders": reached_rejections,
            "fills": [], "queue_trades": [], "bid99_volume_events": [], "bbo_meta": bbo_meta,
        }

    earliest_open_ns = min(opens_before_end_ns)
    earliest_open = q.ns_to_ms(earliest_open_ns)

    # One canonical compact depth_clock timeline is used for every fill-policy
    # configuration. Optional fill flags can change interpretation only; they
    # can never change the opening snapshot or post-only eligibility.
    depth_window_start = max(
        float(task.contract_start_ms),
        float(earliest_open) - max(float(cfg.max_live_book_age_ms), float(cfg.depth_lookback_ms), 0.0) - 1.0,
    )
    depth_window_end = float(market_end_ms)
    depth, depth_meta = q.load_depth_clock_timeline_fast(
        task,
        data_dir=data_dir,
        market_key=cfg.market_key,
        yes_outcome=cfg.yes_outcome,
        start_ms=depth_window_start,
        end_ms=depth_window_end,
        trigger_bid=cfg.trigger_bid,
        tolerance=cfg.trigger_tolerance,
    )

    # Resolve candidate chronology and perform the canonical placement-time
    # post-only check before queue creation or any fill simulation.
    prepared, skipped_orders, retry_meta = _select_reached_entry_attempts(
        all_attempt_rows,
        depth=depth,
        cfg=cfg,
        market_end_ms=float(market_end_ms),
    )
    market.update(retry_meta)
    if not prepared:
        market["status"] = "entry_attempts_rejected"
        market.update({f"depth_{key}": value for key, value in depth_meta.items()})
        _update_missing_market_data_counters(market, skipped_orders)
        return {
            "ok": True, "source_gap": False, "market": market, "orders": skipped_orders,
            "fills": [], "queue_trades": [], "bid99_volume_events": [],
            "bbo_meta": bbo_meta, "depth_meta": depth_meta,
        }

    if bool(getattr(cfg, "optimizer_placement_only_stream", False)):
        market["status"] = "optimizer_placement_only_stream"
        market["optimizer_placement_only_stream"] = True
        market["optimizer_placement_only_opened_orders"] = int(len(prepared))
        market.update({f"depth_{key}": value for key, value in depth_meta.items()})
        _update_missing_market_data_counters(market, skipped_orders)
        return {
            "ok": True, "source_gap": False, "market": market,
            "orders": list(skipped_orders) + list(prepared),
            "fills": [], "queue_trades": [], "bid99_volume_events": [],
            "bbo_meta": bbo_meta, "depth_meta": depth_meta,
        }

    # The compact depth_clock is sufficient for the invariant post-only check,
    # ask-at/below-limit confirmation, and the shared full-market timeline.
    # Exact external 0.99 volume can be hidden when a better bid is above 0.99,
    # so the cancellation rule alone hard-enables a single-level normalized-L2
    # replay. With the rule disabled, normalized_depth_events is never opened.
    exact_bid99_depth: pd.DataFrame | None = None
    exact_bid99_meta: dict[str, Any] = {
        "enabled": False,
        "reason": "exact_level_features_disabled",
    }
    need_exact_bid_level = bool(
        cfg.cancel_if_bid99_unexplained_drop_shares >= 0
        or str(cfg.queue_ahead_reconstruction_mode).strip().lower() != "off"
    )
    if need_exact_bid_level:
        exact_bid99_depth, exact_bid99_meta = q.load_bid_level_timeline_fast(
            task,
            data_dir=data_dir,
            market_key=cfg.market_key,
            token_map=token_map,
            yes_outcome=cfg.yes_outcome,
            start_ms=float(task.contract_start_ms),
            end_ms=float(market_end_ms),
            level_price=cfg.trigger_bid,
        )
        exact_bid99_meta = dict(exact_bid99_meta)
        exact_bid99_meta["enabled"] = True
        exact_bid99_meta["reason"] = "+".join(
            reason for reason, active in (
                ("exact_bid99_unexplained_drop_cancellation", cfg.cancel_if_bid99_unexplained_drop_shares >= 0),
                ("queue_ahead_depth_reconstruction", str(cfg.queue_ahead_reconstruction_mode).strip().lower() != "off"),
            ) if active
        )

    # Lower-price execution evidence is loaded only when that rule is enabled.
    # Otherwise the exact-price Parquet pushdown used by the old FIFO model is
    # retained. Normalized depth is independently hard-gated by the optional
    # unexplained external bid-0.99 volume cancellation rule above.
    trade_kwargs = (
        {"max_price": cfg.trigger_bid, "target_price": None}
        if (cfg.fill_remaining_on_trade_below_limit or (_FP_ON and _FP_THROUGH))
        else {"target_price": cfg.trigger_bid, "max_price": None}
    )
    trades, trade_meta = q.load_trades(
        task,
        data_dir=data_dir,
        market_key=cfg.market_key,
        token_map=token_map,
        yes_outcome=cfg.yes_outcome,
        start_ms=earliest_open,
        end_ms=market_end_ms,
        trade_clock_mode=cfg.trade_clock_mode,
        allow_local_fallback=cfg.allow_local_trade_clock_fallback,
        tolerance=cfg.trigger_tolerance,
        outcomes=[str(x["outcome"]) for x in prepared],
        **trade_kwargs,
    )
    trades = _fp_augment_trades(trades, task=task, data_dir=data_dir, cfg=cfg, token_map=token_map,
                                start_ms=earliest_open, end_ms=market_end_ms, outcomes=[str(x["outcome"]) for x in prepared])
    market.update({f"depth_{key}": value for key, value in depth_meta.items()})
    market.update({f"trade_{key}": value for key, value in trade_meta.items()})
    market.update({f"bid99_exact_level_{key}": value for key, value in exact_bid99_meta.items()})
    market["depth_clock_full_market_timeline"] = True
    market["normalized_depth_events_loaded"] = bool(exact_bid99_depth is not None)
    market["normalized_depth_events_load_reason"] = (
        str(exact_bid99_meta.get("reason", "enabled"))
        if exact_bid99_depth is not None else "disabled"
    )

    orders: list[dict[str, Any]] = list(skipped_orders)
    fill_events: list[dict[str, Any]] = []
    queue_trades: list[dict[str, Any]] = []
    bid99_volume_events: list[dict[str, Any]] = []
    momentum_early_ask_ladder_loads = 0
    momentum_early_ask_ladder_rows = 0

    for order in prepared:
        side = str(order["outcome"])
        open_ns = int(order.get("order_open_ts_ns", q.ms_to_ns(float(order["order_open_ts_ms"]))))
        open_ms = q.ns_to_ms(open_ns)
        requested = float(order["requested_shares"])
        snapshot = q.depth_asof_ns(depth, side, open_ns, strictly_before=True, max_age_ms=cfg.max_live_book_age_ms)
        if snapshot is None:
            order.update(status="skipped_missing_depth_state_at_open", skip_reason="missing_depth_state_at_open")
            orders.append(order)
            continue
        best_bid = q.as_float(snapshot.get("best_bid"), math.nan)
        best_ask = q.as_float(snapshot.get("best_ask"), math.nan)
        best_bid_size = q.as_float(snapshot.get("best_bid_size"), math.nan)
        best_ask_size = q.as_float(snapshot.get("best_ask_size"), math.nan)
        open_book_fields = {
            "depth_state_ts_ns": int(snapshot["ts_ns"]),
            "depth_state_ts_ms": snapshot["ts_ms"],
            "depth_state_age_ms": snapshot["age_ms"],
            "best_bid_at_open": best_bid,
            "best_bid_size_at_open": best_bid_size,
            "best_ask_at_open": best_ask,
            "best_ask_size_at_open": best_ask_size,
        }
        open_semantics = _buy_limit_open_book_semantics(
            best_bid=best_bid, best_bid_size=best_bid_size,
            best_ask=best_ask, best_ask_size=best_ask_size,
            limit_price=cfg.trigger_bid, tolerance=cfg.trigger_tolerance,
        )
        missing_open = list(open_semantics["missing_components"])
        open_book_fields.update({
            **_missing_book_fields("order_open_depth", missing_open),
            "order_open_bid_level_present": bool(open_semantics["bid_present"]),
            "order_open_ask_level_present": bool(open_semantics["ask_present"]),
            "order_open_absent_bid_allowed": bool(open_semantics["absent_bid_allowed"]),
            "order_open_absent_ask_allowed": bool(open_semantics["absent_ask_allowed"]),
        })
        if missing_open:
            order.update(
                status="skipped_missing_book_component_at_open",
                skip_reason="missing_book_component_at_open",
                **open_book_fields,
            )
            orders.append(order)
            continue
        if math.isfinite(best_bid) and best_bid > cfg.trigger_bid + cfg.trigger_tolerance:
            order.update(
                status="skipped_best_bid_above_limit_at_open",
                skip_reason="best_bid_above_limit_at_open",
                **open_book_fields,
            )
            orders.append(order)
            continue
        crossed_at_open = bool(open_semantics["crossed"])
        no_external_bid99_at_open = bool(
            (not math.isfinite(best_bid))
            or best_bid < cfg.trigger_bid - cfg.trigger_tolerance - 1e-12
        )
        momentum_early_order = str(order.get("entry_trigger_source", "")) == "momentum_early_bid"
        early_cross_exception = bool(
            (not cfg.post_only_order_enabled)
            and momentum_early_order
            and no_external_bid99_at_open
            and crossed_at_open
        )
        if cfg.post_only_order_enabled and crossed_at_open:
            order.update(
                status="skipped_post_only_would_cross",
                skip_reason="post_only_would_cross",
                **open_book_fields,
                momentum_early_cross_exception_used=False,
            )
            orders.append(order)
            continue

        initial_cross_match: dict[str, Any] | None = None
        if early_cross_exception:
            match_ns = open_ns + _delay_ns(float(cfg.momentum_early_cross_match_delay_ms)); match_ms=q.ns_to_ms(match_ns)
            levels: list[dict[str, float]] = []
            ask_meta: dict[str, Any] = {
                "source": "normalized_depth_events_ask_ladder_at_time",
                "query_ns": match_ns, "query_ms": match_ms,
                "skipped_after_market_end": bool(match_ns >= market_end_ns),
            }
            if match_ns < market_end_ns:
                levels, ask_meta = q.load_ask_ladder_at_time_fast(
                    task, data_dir=data_dir, market_key=cfg.market_key, token_map=token_map,
                    yes_outcome=cfg.yes_outcome, outcome=side, query_ms=match_ms,
                    max_price=cfg.trigger_bid, tolerance=cfg.trigger_tolerance,
                )
                momentum_early_ask_ladder_loads += 1
                momentum_early_ask_ladder_rows += int(ask_meta.get("rows_retained", 0) or 0)
            reverse_state_ns = int(ask_meta.get("state_ts_ns", -1) or -1)
            initial_cross_match = {
                "scheduled": True,
                "match_ts_ns": match_ns,
                "match_ts_ms": match_ms,
                "delay_ms": float(cfg.momentum_early_cross_match_delay_ms),
                "levels": levels,
                "meta": ask_meta,
            }
            open_book_fields.update({
                "reverse_book_ts_ns": reverse_state_ns,
                "reverse_book_query_ts_ns": int(match_ns),
                "momentum_early_cross_exception_used": True,
                "momentum_early_crossed_at_open": True,
                "momentum_early_no_external_bid99_at_open": True,
                "momentum_early_cross_match_ts_ns": match_ns,
                "momentum_early_cross_match_ts_ms": match_ms,
                "momentum_early_cross_match_time_utc": q.market_iso(match_ms),
                "momentum_early_cross_match_delay_ms": float(cfg.momentum_early_cross_match_delay_ms),
                "momentum_early_cross_match_ask_ladder_source": str(ask_meta.get("source", "")),
                "momentum_early_cross_match_ask_ladder_levels": len(levels),
                "momentum_early_cross_match_available_ask_shares": float(sum(x.get("shares", 0.0) for x in levels)),
            })
        else:
            open_book_fields.update({
                "momentum_early_cross_exception_used": False,
                "momentum_early_crossed_at_open": crossed_at_open,
                "momentum_early_no_external_bid99_at_open": no_external_bid99_at_open,
            })

        compact_raw_queue = float(open_semantics["raw_queue_shares"])
        volume_source = "depth_clock_top_bid99"
        raw_queue = compact_raw_queue
        exact_side_depth: pd.DataFrame | None = None
        if exact_bid99_depth is not None:
            exact_snapshot = q.depth_asof_ns(
                exact_bid99_depth, side, open_ns, strictly_before=True, max_age_ms=-1
            )
            if exact_snapshot is None:
                raise q.SourceDataGap(
                    "missing_exact_bid99_level_state_at_open",
                    "depth",
                    exact_bid99_meta.get("path", ""),
                    {
                        "contract_start_ms": task.contract_start_ms,
                        "outcome": side,
                        "order_open_ts_ms": open_ms,
                    },
                )
            raw_queue = max(0.0, q.as_float(exact_snapshot.get("bid_level_size"), 0.0))
            exact_side_depth = _side_depth_after(exact_bid99_depth, side=side, open_ns=open_ns)
            volume_source = "normalized_depth_events_exact_bid99"
        queue_ahead = raw_queue * cfg.queue_size_multiplier
        order.update({
            "status": "opened",
            "skip_reason": "",
            **open_book_fields,
            "raw_bid99_queue_shares": raw_queue,
            "compact_depth_clock_bid99_queue_shares": compact_raw_queue,
            "bid99_volume_source": volume_source,
            "queue_ahead_shares": queue_ahead,
            "queue_size_multiplier": cfg.queue_size_multiplier,
            "queue_ahead_reconstruction_mode": str(cfg.queue_ahead_reconstruction_mode).strip().lower(),
            "queue_ahead_reconstruction_exact_level_source": bool(exact_side_depth is not None),
            "fill_remaining_on_ask99_appear": cfg.fill_remaining_on_ask99_appear,
            "fill_remaining_on_trade_below_limit": cfg.fill_remaining_on_trade_below_limit,
            "price_priority_fill_delay_ms": (
                cfg.price_priority_fill_delay_ms
                if (cfg.fill_remaining_on_ask99_appear or cfg.fill_remaining_on_trade_below_limit)
                else math.nan
            ),
            "cancel_if_bid99_unexplained_drop_shares": cfg.cancel_if_bid99_unexplained_drop_shares,
        })

        side_depth = _side_depth_after(depth, side=side, open_ns=open_ns)
        causal_trades = _causal_order_trades(
            trades,
            side=side,
            open_ns=open_ns,
            end_ns=market_end_ns,
            cfg=cfg,
        )
        exact_queue_trades = _queue_consuming_exact99_trades(
            causal_trades,
            cfg=cfg,
            order_id=str(order["order_id"]),
        )
        queue_cancel_candidate: dict[str, Any] | None = None
        if (
            cfg.cancel_if_queue_ahead_above_shares >= 0
            and queue_ahead > cfg.cancel_if_queue_ahead_above_shares + 1e-12
        ):
            delay = (
                cfg.queue_cancel_delay_ms
                if cfg.queue_cancel_delay_ms >= 0
                else cfg.paper_shadow_order_open_delay_ms
            )
            delay_ns=_delay_ns(max(0.0,delay))
            queue_cancel_candidate = {
                "reason": "queue_ahead_above_threshold",
                "request_ts_ns": open_ns, "effective_ts_ns": open_ns+delay_ns,
                "request_ts_ms": open_ms, "effective_ts_ms": q.ns_to_ms(open_ns+delay_ns),
                "delay_ms": delay_ns/1e6,
                "observed_value": queue_ahead,
                "threshold": cfg.cancel_if_queue_ahead_above_shares,
            }

        # Optional time-in-force cancellation. The request is sent only after
        # the order has been active for LIMIT_ORDER_LIFETIME_SEC. The order
        # remains live during the configured cancellation delay, so FIFO,
        # ask-cross, and below-limit price-priority fills can still occur.
        lifetime_cancel_candidate: dict[str, Any] | None = None
        lifetime_request_ms = math.nan
        lifetime_effective_ms = math.nan
        lifetime_delay_ms = math.nan
        if cfg.limit_order_lifetime_sec >= 0:
            lifetime_delay_ms = (
                cfg.limit_order_lifetime_cancel_delay_ms
                if cfg.limit_order_lifetime_cancel_delay_ms >= 0
                else cfg.paper_shadow_order_open_delay_ms
            )
            lifetime_request_ns = open_ns + _delay_ns(float(cfg.limit_order_lifetime_sec) * 1000.0)
            lifetime_delay_ns = _delay_ns(max(0.0, float(lifetime_delay_ms)))
            lifetime_effective_ns = lifetime_request_ns + lifetime_delay_ns
            lifetime_request_ms = q.ns_to_ms(lifetime_request_ns); lifetime_effective_ms=q.ns_to_ms(lifetime_effective_ns)
            # A request at or after settlement has no executable effect.
            if lifetime_request_ns < market_end_ns:
                lifetime_cancel_candidate = {
                    "reason": "limit_order_lifetime_expired",
                    "request_ts_ns": lifetime_request_ns, "effective_ts_ns": lifetime_effective_ns,
                    "request_ts_ms": lifetime_request_ms, "effective_ts_ms": lifetime_effective_ms,
                    "delay_ms": lifetime_delay_ns/1e6,
                    "observed_value": float(cfg.limit_order_lifetime_sec),
                    "threshold": float(cfg.limit_order_lifetime_sec),
                }
        order.update({
            "limit_order_lifetime_enabled": bool(cfg.limit_order_lifetime_sec >= 0),
            "limit_order_lifetime_sec": float(cfg.limit_order_lifetime_sec),
            "lifetime_cancel_scheduled": bool(lifetime_cancel_candidate is not None),
            "lifetime_cancel_request_ts_ms": float(lifetime_request_ms) if math.isfinite(lifetime_request_ms) else math.nan,
            "lifetime_cancel_request_time_utc": q.market_iso(lifetime_request_ms) if math.isfinite(lifetime_request_ms) else "",
            "lifetime_cancel_effective_ts_ms": float(lifetime_effective_ms) if math.isfinite(lifetime_effective_ms) else math.nan,
            "lifetime_cancel_effective_time_utc": q.market_iso(lifetime_effective_ms) if math.isfinite(lifetime_effective_ms) else "",
            "lifetime_cancel_delay_ms": float(lifetime_delay_ms) if math.isfinite(lifetime_delay_ms) else math.nan,
            "lifetime_cancel_selected": False,
        })

        binance_reversal_cancel_candidate, binance_reversal_fields = (
            _binance_mid_below_trigger_cancel_candidate(
                order=order,
                open_ns=open_ns,
                market_end_ns=market_end_ns,
                cfg=cfg,
                underlying=underlying,
            )
            if cfg.cancel_if_binance_mid_below_trigger
            else (
                None,
                {
                    "binance_adverse_cancel_enabled": False,
                    "binance_adverse_cancel_selected": False,
                    "binance_mid_below_trigger_cancel_enabled": False,
                    "binance_mid_below_trigger_cancel_selected": False,
                },
            )
        )
        order.update(binance_reversal_fields)

        static_cancel_candidates = [
            candidate
            for candidate in (
                queue_cancel_candidate,
                lifetime_cancel_candidate,
                binance_reversal_cancel_candidate,
            )
            if candidate is not None
        ]
        static_scan_end_ns = min([market_end_ns] + [int(candidate["effective_ts_ns"]) for candidate in static_cancel_candidates])
        static_scan_end_ms = q.ns_to_ms(static_scan_end_ns)

        timeline, volume_stats, drop_cancel = _build_bid99_volume_timeline(
            order_id=str(order["order_id"]),
            contract_start_ms=task.contract_start_ms,
            side=side,
            open_ns=open_ns,
            raw_queue=raw_queue,
            side_depth=(exact_side_depth if exact_side_depth is not None else side_depth),
            exact_queue_trades=exact_queue_trades,
            cfg=cfg,
            volume_source=volume_source,
            scan_end_ns=static_scan_end_ns,
        )
        bid99_volume_events.extend(timeline)
        diagnostics = (
            {}
            if cfg.optimizer_skip_nondecision_diagnostics
            else _disappearance_diagnostics(
                side_depth=side_depth,
                causal_trades=causal_trades,
                open_ns=open_ns,
                cfg=cfg,
            )
        )

        cancel_candidates: list[dict[str, Any]] = list(static_cancel_candidates)
        if drop_cancel is not None:
            cancel_candidates.append(drop_cancel)

        no_cancel = _simulate_order_fills(
            order_id=str(order["order_id"]),
            contract_start_ms=task.contract_start_ms,
            side=side,
            open_ns=open_ns,
            cutoff_ns=market_end_ns,
            requested=requested,
            queue_ahead=queue_ahead,
            side_depth=side_depth,
            causal_trades=causal_trades,
            exact_queue_trades=exact_queue_trades,
            cfg=cfg,
            queue_reconstruction_depth=(exact_side_depth if exact_side_depth is not None else side_depth),
            initial_cross_match=initial_cross_match,
            pre_open_depth=exact_bid99_depth,
        )
        uncancelled_full_ns = int(no_cancel.get("full_fill_ts_ns", -1))
        if uncancelled_full_ns >= 0:
            cancel_candidates = [candidate for candidate in cancel_candidates if int(candidate["request_ts_ns"]) < uncancelled_full_ns]
        cancel = min(cancel_candidates, key=lambda x: (int(x["effective_ts_ns"]), int(x["request_ts_ns"]))) if cancel_candidates else None
        cancel_effective_before_market_end = bool(cancel is not None and int(cancel["effective_ts_ns"]) < market_end_ns)
        cancel_effective_ns = int(cancel["effective_ts_ns"]) if cancel_effective_before_market_end else market_end_ns
        cancel_effective_ms = q.ns_to_ms(cancel_effective_ns)
        execution = (
            _simulate_order_fills(
                order_id=str(order["order_id"]),
                contract_start_ms=task.contract_start_ms,
                side=side,
                open_ns=open_ns,
                cutoff_ns=cancel_effective_ns,
                requested=requested,
                queue_ahead=queue_ahead,
                side_depth=side_depth,
                causal_trades=causal_trades,
                exact_queue_trades=exact_queue_trades,
                cfg=cfg,
                queue_reconstruction_depth=(exact_side_depth if exact_side_depth is not None else side_depth),
                initial_cross_match=initial_cross_match,
                pre_open_depth=exact_bid99_depth,
            )
            if cancel else no_cancel
        )
        if cancel:
            order.update({
                "cancel_requested": True,
                "cancel_reason": cancel["reason"],
                "lifetime_cancel_selected": cancel["reason"] == "limit_order_lifetime_expired",
                "binance_adverse_cancel_selected": cancel["reason"] == "binance_mid_adverse_to_outcome",
                "binance_mid_below_trigger_cancel_selected": cancel["reason"] == "binance_mid_adverse_to_outcome",
                "cancel_request_ts_ns": int(cancel["request_ts_ns"]),
                "cancel_effective_ts_ns": int(cancel["effective_ts_ns"]),
                "cancel_request_ts_ms": cancel["request_ts_ms"],
                "cancel_effective_ts_ms": float(cancel["effective_ts_ms"]),
                "cancel_delay_ms": cancel["delay_ms"],
                "cancel_effective_before_market_end": bool(cancel_effective_before_market_end),
                "cancel_pending_at_market_end": bool(not cancel_effective_before_market_end),
                "cancel_observed_value": cancel["observed_value"],
                "cancel_threshold": cancel["threshold"],
                "bid99_unexplained_drop_cancel_observed_shares": (
                    cancel["observed_value"]
                    if cancel["reason"] == "bid99_unexplained_drop_threshold" else math.nan
                ),
            })
        else:
            order.update({
                "cancel_requested": False,
                "cancel_reason": "",
                "cancel_request_ts_ns": -1,
                "cancel_effective_ts_ns": -1,
                "cancel_request_ts_ms": math.nan,
                "cancel_effective_ts_ms": math.nan,
                "cancel_delay_ms": math.nan,
                "cancel_effective_before_market_end": False,
                "cancel_pending_at_market_end": False,
                "cancel_observed_value": math.nan,
                "cancel_threshold": math.nan,
                "bid99_unexplained_drop_cancel_observed_shares": math.nan,
                "lifetime_cancel_selected": False,
                "binance_adverse_cancel_selected": False,
                "binance_mid_below_trigger_cancel_selected": False,
            })

        fill_events.extend(execution["fill_events"])
        queue_trades.extend(execution["queue_trades"])
        filled = float(execution["filled"])
        queue_remaining = float(execution["queue_remaining"])
        winner = resolutions.get(task.contract_start_ms, "")
        market["winner"] = winner
        if cfg.require_resolution and winner not in q.OUTCOMES and filled > 0:
            order.update(status="skipped_missing_resolution", skip_reason="missing_resolution", simulation_valid=False)
            orders.append(order)
            continue
        execution_fill_events = list(execution.get("fill_events", []))
        cost = float(sum(
            max(0.0, q.as_float(event.get("fill_shares"), 0.0))
            * q.as_float(event.get("fill_price"), cfg.trigger_bid)
            for event in execution_fill_events
        ))
        fee = float(sum(
            fee_for_shares(
                max(0.0, q.as_float(event.get("fill_shares"), 0.0)),
                q.as_float(event.get("fill_price"), cfg.trigger_bid),
                cfg,
            )
            for event in execution_fill_events
        ))
        average_fill_price = cost / filled if filled > 0 else math.nan
        payout = filled if winner == side else 0.0 if winner in q.OUTCOMES else 0.0
        pnl = payout - cost - fee
        if filled >= requested - 1e-9:
            status = "fully_filled_before_cancel" if cancel else "fully_filled"
        elif filled > 0 and cancel_effective_before_market_end:
            status = "partially_filled_then_cancelled"
        elif filled > 0:
            status = "partially_filled_at_market_end"
        elif cancel_effective_before_market_end:
            status = {
                "queue_ahead_above_threshold": "cancelled_queue_ahead_rule",
                "bid99_unexplained_drop_threshold": "cancelled_bid99_unexplained_drop_rule",
                "limit_order_lifetime_expired": "cancelled_limit_order_lifetime",
                "binance_mid_adverse_to_outcome": "cancelled_binance_mid_adverse_to_outcome",
            }.get(str(cancel.get("reason", "")), "cancelled_unfilled_remainder")
        else:
            status = "unfilled_at_market_end"

        order.update({
            "status": status,
            "queue_trade_shares": execution["exact99_trade_shares_processed"],
            "queue_remaining_shares": queue_remaining,
            "potential_filled_shares": filled,
            "price_priority_confirmed_fill_shares": execution["price_priority_confirmed_fill_shares"],
            "ask99_confirmation_fill_shares": execution["ask99_confirmation_fill_shares"],
            "below_limit_price_priority_fill_shares": execution["below_limit_price_priority_fill_shares"],
            "ask99_appearance_confirmation_enabled": cfg.fill_remaining_on_ask99_appear,
            "ask99_appearance_confirmation_applied": execution["ask99_confirmation_fill_shares"] > 0,
            "below_limit_trade_fill_enabled": cfg.fill_remaining_on_trade_below_limit,
            "below_limit_trade_fill_applied": execution["below_limit_price_priority_fill_shares"] > 0,
            **volume_stats,
            **execution,
            **diagnostics,
            "bid99_disappearance_used_for_fill": False,
            "fill_fraction": filled / requested if requested > 0 else 0.0,
            "winner": winner,
            "potential_cost_usdc": cost,
            "potential_fee_usdc": fee,
            "potential_average_fill_price": average_fill_price,
            "potential_payout_usdc": payout,
            "potential_pnl_usdc": pnl,
            "first_fill_ts_ms": min(
                [e["fill_ts_ms"] for e in execution["fill_events"]], default=math.nan
            ),
            "last_fill_ts_ms": max(
                [e["fill_ts_ms"] for e in execution["fill_events"]], default=math.nan
            ),
        })
        # Do not embed nested row lists in queue99_orders.csv.
        order.pop("fill_events", None)
        order.pop("queue_trades", None)
        orders.append(order)

    market.update({
        "status": "evaluated",
        "orders": len(orders),
        "potential_filled_orders": sum(q.as_float(x.get("potential_filled_shares"), 0.0) > 0 for x in orders),
        "potential_filled_shares": sum(q.as_float(x.get("potential_filled_shares"), 0.0) for x in orders),
        "depth_rows": len(depth),
        "trade_rows": len(trades),
        "bid99_volume_event_rows": len(bid99_volume_events),
        "momentum_early_ask_ladder_loads": int(momentum_early_ask_ladder_loads),
        "momentum_early_ask_ladder_rows": int(momentum_early_ask_ladder_rows),
        "normalized_depth_events_loaded": bool(
            exact_bid99_depth is not None or momentum_early_ask_ladder_loads > 0
        ),
        "normalized_depth_events_load_reason": "+".join(
            reason for reason, active in (
                ("exact_bid99_unexplained_drop_cancellation", cfg.cancel_if_bid99_unexplained_drop_shares >= 0 and exact_bid99_depth is not None),
                ("queue_ahead_depth_reconstruction", str(cfg.queue_ahead_reconstruction_mode).strip().lower() != "off" and exact_bid99_depth is not None),
                ("momentum_early_crossed_ask_match", momentum_early_ask_ladder_loads > 0),
            ) if active
        ) or "disabled",
    })
    _update_missing_market_data_counters(market, orders)
    return {
        "ok": True,
        "source_gap": False,
        "market": market,
        "orders": orders,
        "fills": fill_events,
        "queue_trades": queue_trades,
        "bid99_volume_events": bid99_volume_events,
        "bbo_meta": bbo_meta,
        "depth_meta": depth_meta,
        "trade_meta": trade_meta,
    }

def evaluate_market_worker(payload: dict[str, Any]) -> dict[str, Any]:
    started = time.monotonic()
    task = q.ContractTask(**payload["task"])
    session_attempts: list[dict[str, Any]] = []
    try:
        assert _WORKER_CFG is not None and _WORKER_TOKEN_MAP is not None and _WORKER_RESOLUTIONS is not None
        cfg = Config(**{k: v for k, v in _WORKER_CFG.items() if k in _CONFIG_FIELD_NAMES})

        # Hot path: evaluate the manifest-selected session without listing its
        # sibling directories. The sibling metadata lookup is paid only after
        # a genuine selected-session gap/error makes recovery necessary.
        candidates: list[q.ContractTask] = [task]
        alternates_discovered = False
        last_gap: q.SourceDataGap | None = None
        candidate_errors: list[tuple[q.ContractTask, Exception, str]] = []
        index = 0
        while index < len(candidates):
            candidate = candidates[index]
            index += 1
            try:
                result = _evaluate_session(candidate, cfg, _WORKER_TOKEN_MAP, _WORKER_RESOLUTIONS, _WORKER_UNDERLYING)
                _validate_order_row_contract(
                    result.get("orders", []),
                    context=f"worker_market={task.contract_start_ms}:session={candidate.session_id}",
                )
                known_ids = {str(row["order_id"]) for row in result.get("orders", [])}
                for fill_index, fill in enumerate(result.get("fills", [])):
                    oid = str(fill.get("order_id", ""))
                    if not oid or oid not in known_ids:
                        raise InternalContractError(
                            f"fill_contract_unknown_order_id market={task.contract_start_ms} "
                            f"fill_index={fill_index} order_id={oid!r}"
                        )
                result["market"]["source_session_recovered"] = candidate.session_id != task.session_id
                result["market"]["source_session_original"] = task.session_id
                result["market"]["source_session_recovery_attempted"] = bool(alternates_discovered)
                result["market"]["source_session_attempts"] = json.dumps(
                    session_attempts + [{"session_id": candidate.session_id, "bbo_dir": candidate.bbo_dir, "status": "used"}],
                    separators=(",", ":"),
                    default=str,
                )
                result["elapsed_sec"] = time.monotonic() - started
                return result
            except q.SourceDataGap as exc:
                session_attempts.append({
                    "session_id": candidate.session_id,
                    "bbo_dir": candidate.bbo_dir,
                    "status": "source_gap",
                    **exc.as_dict(),
                })
                last_gap = exc
            except FileNotFoundError as exc:
                reason = str(exc)
                stage = "bbo" if "bbo" in reason else "depth" if "depth" in reason else "trade" if "trade" in reason else "unknown"
                gap = q.SourceDataGap(reason, stage, candidate.bbo_dir)
                session_attempts.append({
                    "session_id": candidate.session_id,
                    "bbo_dir": candidate.bbo_dir,
                    "status": "source_gap",
                    **gap.as_dict(),
                })
                last_gap = gap
            except _INTERNAL_PROGRAMMING_EXCEPTIONS:
                raise
            except Exception as exc:
                tb = traceback.format_exc()
                session_attempts.append({
                    "session_id": candidate.session_id,
                    "bbo_dir": candidate.bbo_dir,
                    "status": "candidate_error",
                    "error": repr(exc),
                })
                candidate_errors.append((candidate, exc, tb))

            if cfg.recover_alternate_sessions and not alternates_discovered:
                alternates_discovered = True
                for alternate in q.session_candidates(task, Path(cfg.data_dir), cfg.market_key):
                    if str(Path(alternate.bbo_dir)) != str(Path(task.bbo_dir)):
                        candidates.append(alternate)

        if candidate_errors:
            sample = [
                {"session_id": c.session_id, "bbo_dir": c.bbo_dir, "error": repr(exc)}
                for c, exc, _tb in candidate_errors[:10]
            ]
            first_candidate, first_error, _first_tb = candidate_errors[0]
            raise RuntimeError(
                "all coherent session candidates failed with at least one genuine source/parser error; "
                f"first_session={first_candidate.session_id}; first_error={first_error!r}; sample={sample}"
            ) from first_error
        if last_gap is not None:
            result = source_gap_result(
                task,
                last_gap.reason,
                last_gap.stage,
                session_attempts,
                details=last_gap.as_dict(),
            )
            result["market"]["source_session_recovery_attempted"] = bool(alternates_discovered)
            result["elapsed_sec"] = time.monotonic() - started
            return result
        raise RuntimeError("no session candidate evaluated")
    except _INTERNAL_PROGRAMMING_EXCEPTIONS as exc:
        return {
            "ok": False, "source_gap": False, "fatal_internal_contract": True, "market": {
                "contract_start_ms": task.contract_start_ms, "market_start_utc": q.market_iso(task.contract_start_ms),
                "status": "internal_contract_error", "simulation_valid": False,
                "source_session_recovery_attempted": bool(locals().get("alternates_discovered", False)),
                "source_session_attempts": json.dumps(session_attempts, separators=(",", ":"), default=str),
            },
            "orders": [], "fills": [], "queue_trades": [], "bid99_volume_events": [], "error": repr(exc),
            "traceback": traceback.format_exc(), "elapsed_sec": time.monotonic() - started,
        }
    except Exception as exc:
        return {
            "ok": False, "source_gap": False, "market": {
                "contract_start_ms": task.contract_start_ms, "market_start_utc": q.market_iso(task.contract_start_ms),
                "status": "worker_error", "simulation_valid": False,
                "source_session_recovery_attempted": bool(locals().get("alternates_discovered", False)),
                "source_session_attempts": json.dumps(session_attempts, separators=(",", ":"), default=str),
            },
            "orders": [], "fills": [], "queue_trades": [], "bid99_volume_events": [], "error": repr(exc),
            "traceback": traceback.format_exc(), "elapsed_sec": time.monotonic() - started,
        }



def _open_btc_mmap(root: Path) -> q.UnderlyingMidSeries:
    times_ns = np.load(root / "times_ns.npy", mmap_mode="r", allow_pickle=False)
    prices = np.load(root / "prices.npy", mmap_mode="r", allow_pickle=False)
    metadata = json.loads((root / "metadata.json").read_text(encoding="utf-8"))
    if len(times_ns) != len(prices) or len(times_ns) <= 0:
        raise RuntimeError(f"invalid Binance underlying mmap index at {root}: times={len(times_ns)} prices={len(prices)}")
    unit = str(metadata.get("timestamp_unit", "") or "nanoseconds")
    if str(metadata.get("timestamp_clock", "")) != "binance_venue" or unit != "nanoseconds":
        raise RuntimeError(
            f"Binance underlying mmap index at {root} is not a venue-time multi-asset index; "
            f"timestamp_clock={metadata.get('timestamp_clock')!r} timestamp_unit={unit!r}"
        )
    metadata["timestamp_unit"] = "nanoseconds"
    return q.UnderlyingMidSeries(None, prices, str(metadata.get("source", "binance")), str(metadata.get("path", "")), metadata, times_ns=times_ns)


def _btc_index_key(
    cfg: Config,
    paths: Sequence[Path],
    *,
    index_lookback_ms: float | None = None,
) -> tuple[str, dict[str, Any]]:
    required_prestart_ms = float(
        cfg.btc_mid_max_age_ms + cfg.binance_venue_to_vps_delay_ms
        if index_lookback_ms is None
        else index_lookback_ms
    )
    if required_prestart_ms > BTC_INDEX_PRESTART_BUFFER_MS - 1000.0:
        raise ValueError(
            f"required Binance pre-start history {required_prestart_ms:g}ms exceeds "
            f"the fixed venue-index buffer {BTC_INDEX_PRESTART_BUFFER_MS:g}ms"
        )
    payload = {
        "schema": "queue99-underlying-mid-venue-index-v4-ns",
        "source_paths": [str(Path(path).expanduser().resolve()) for path in paths],
        "asset": cfg.underlying_asset,
        "market_key": cfg.market_key,
        "symbol": cfg.btc_symbol,
        "start_ms": cfg.start_ms - int(BTC_INDEX_PRESTART_BUFFER_MS),
        "end_ms": cfg.end_ms,
        "source_tag": cfg.btc_index_cache_source_tag,
        "timestamp_clock": "binance_venue",
        "timestamp_unit": "nanoseconds",
        "prestart_buffer_ms": float(BTC_INDEX_PRESTART_BUFFER_MS),
        "loader_contract": (
            "venue_timestamp_stats+venue_time_symbol_pushdown+sequence_order+"
            "exact_event_dedup_v3_int64_ns"
        ),
    }
    # The simulated feed delay and max-age guard are intentionally absent from
    # the key: both are applied at lookup time against the same venue index.
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return digest[:24], payload


def prepare_btc_index(
    cfg: Config,
    paths: Sequence[Path],
    outdir: Path,
    started: float,
    *,
    index_lookback_ms: float | None = None,
) -> tuple[q.UnderlyingMidSeries, str, dict[str, Any]]:
    """Reuse a persistent read-only midpoint index or build it once.

    AUTO deliberately treats the historical source as immutable for the exact
    path/window/symbol/source-tag key. Set BTC_INDEX_CACHE_MODE=rebuild or change
    BTC_INDEX_CACHE_SOURCE_TAG after modifying the underlying cache.
    """
    prepare_started = time.monotonic()
    mode = str(cfg.btc_index_cache_mode)
    required_prestart_ms = float(
        cfg.btc_mid_max_age_ms + cfg.binance_venue_to_vps_delay_ms
        if index_lookback_ms is None
        else index_lookback_ms
    )
    key, key_payload = _btc_index_key(cfg, paths, index_lookback_ms=required_prestart_ms)
    if mode == "off":
        # OFF means one run-local index only. Never point it at, or delete, a
        # configured shared cache root.
        cache_root = outdir / "input"
        target = cache_root / "btc_mid_mmap"
    else:
        if cfg.btc_index_cache_dir:
            cache_root = Path(cfg.btc_index_cache_dir).expanduser().resolve()
        elif outdir.parent.name == "results":
            cache_root = outdir.parent.parent / "runtime_cache" / "binance_mid_indexes"
        else:
            cache_root = outdir.parent / "runtime_cache" / "binance_mid_indexes"
        target = cache_root / key
    reference_path = outdir / "input" / "btc_mid_index_reference.json"
    neutral_reference_path = outdir / "input" / "underlying_mid_index_reference.json"
    if mode != "off":
        target_ready = target / "READY"
        if mode == "auto" and target_ready.exists():
            try:
                series = _open_btc_mmap(target)
                meta = dict(series.metadata) | {
                    "underlying_asset": cfg.underlying_asset,
                    "market_key": cfg.market_key,
                    "symbol": cfg.btc_symbol,
                    "index_cache_hit": True,
                    "index_cache_mode": mode,
                    "index_cache_key": key,
                    "index_cache_path": str(target),
                    "source_immutability_assumption": True,
                    "current_run_source_load_elapsed_sec": 0.0,
                    "current_run_index_write_elapsed_sec": 0.0,
                    "current_run_index_prepare_elapsed_sec": float(time.monotonic() - prepare_started),
                    "index_bytes": int(sum((target / name).stat().st_size for name in ("times_ns.npy", "prices.npy") if (target / name).exists())),
                    "required_prestart_ms_for_this_run": float(required_prestart_ms),
                    "binance_venue_to_vps_delay_ms_for_this_run": float(cfg.binance_venue_to_vps_delay_ms),
                    "lookup_clock_contract": "venue_ts_ns + configured_delay_ns <= strategy_vps_time_ns",
                }
                series.metadata = meta
                q.atomic_json(reference_path, meta | {"key_payload": key_payload})
                q.atomic_json(neutral_reference_path, meta | {"key_payload": key_payload})
                q.log(f"BTC_MID_INDEX_CACHE_HIT rows={len(series.times_ms):,} path={target} key={key}", started)
                return series, str(target), meta
            except Exception as exc:
                q.log(f"BTC_MID_INDEX_CACHE_INVALID path={target} error={exc!r} action=rebuild", started)
                # READY exists but the index is unreadable or inconsistent.
                # Remove only this key, never the shared cache root.
                if target.exists():
                    shutil.rmtree(target)
        # The build is committed by an atomic directory rename. A target
        # without READY is therefore stale/incomplete and must not block a
        # rebuild or be mistaken for a valid cache hit.
        if target.exists() and not (target / "READY").exists():
            q.log(f"BTC_MID_INDEX_CACHE_INCOMPLETE path={target} action=remove", started)
            shutil.rmtree(target)
        if mode == "rebuild" and target.exists():
            shutil.rmtree(target)
        cache_root.mkdir(parents=True, exist_ok=True)
    else:
        if target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True, exist_ok=True)

    q.log(f"BTC_MID_INDEX_CACHE_BUILD_START mode={mode} key={key} target={target}", started)
    loaded = q.load_btc_mid_series(
        paths,
        start_ms=cfg.start_ms - int(BTC_INDEX_PRESTART_BUFFER_MS),
        end_ms=cfg.end_ms,
        symbol=cfg.btc_symbol,
        workers=cfg.btc_load_workers,
        prune_by_time_stats=cfg.btc_prune_by_time_stats,
        progress_interval_sec=cfg.progress_interval_sec,
        progress_every_files=cfg.btc_load_log_every_files,
        run_started=started,
    )
    build_started = time.monotonic()
    if mode == "off":
        build_dir = target
    else:
        build_dir = Path(tempfile.mkdtemp(prefix=f".{key}.build-", dir=str(cache_root)))
    np.save(build_dir / "times_ns.npy", np.asarray(loaded.times_ns, dtype=np.int64), allow_pickle=False)
    np.save(build_dir / "prices.npy", np.asarray(loaded.prices, dtype=np.float64), allow_pickle=False)
    index_bytes = int(sum((build_dir / name).stat().st_size for name in ("times_ns.npy", "prices.npy")))
    write_elapsed_before_metadata = time.monotonic() - build_started
    meta = dict(loaded.metadata) | {
        "source": loaded.source,
        "path": loaded.path,
        "underlying_asset": cfg.underlying_asset,
        "market_key": cfg.market_key,
        "symbol": cfg.btc_symbol,
        "index_cache_hit": False,
        "index_cache_mode": mode,
        "index_cache_key": key,
        "index_cache_path": str(target),
        "source_immutability_assumption": mode != "off",
        "key_payload": key_payload,
        "current_run_source_load_elapsed_sec": float(loaded.metadata.get("elapsed_sec", 0.0) or 0.0),
        "current_run_index_write_elapsed_sec": float(write_elapsed_before_metadata),
        "current_run_index_prepare_elapsed_sec": float(time.monotonic() - prepare_started),
        "index_bytes": int(index_bytes),
        "required_prestart_ms_for_this_run": float(required_prestart_ms),
        "binance_venue_to_vps_delay_ms_for_this_run": float(cfg.binance_venue_to_vps_delay_ms),
        "timestamp_clock": "binance_venue",
        "timestamp_unit": "nanoseconds",
        "lookup_clock_contract": "venue_ts_ns + configured_delay_ns <= strategy_vps_time_ns",
        "delay_is_part_of_index_cache_key": False,
    }
    q.atomic_json(build_dir / "metadata.json", meta)
    (build_dir / "READY").write_text("ready\n", encoding="utf-8")
    if mode != "off":
        if target.exists():
            shutil.rmtree(build_dir)
        else:
            build_dir.replace(target)
    write_elapsed = time.monotonic() - build_started
    meta["current_run_index_write_elapsed_sec"] = float(write_elapsed)
    meta["current_run_index_prepare_elapsed_sec"] = float(time.monotonic() - prepare_started)
    q.atomic_json(target / "metadata.json", meta)
    series = _open_btc_mmap(target)
    series.metadata = meta
    q.atomic_json(reference_path, meta)
    q.atomic_json(neutral_reference_path, meta)
    q.log(
        f"BTC_MID_INDEX_CACHE_BUILD_DONE rows={len(series.times_ms):,} "
        f"write_elapsed_sec={write_elapsed:.3f} bytes={int(np.asarray(series.times_ns).nbytes + np.asarray(series.prices).nbytes):,} "
        f"path={target} key={key}",
        started,
    )
    return series, str(target), meta


def stage_btc_worker_index(
    index_dir: str | Path,
    cfg: Config,
    *,
    started: float,
) -> tuple[tempfile.TemporaryDirectory[str], str, dict[str, Any]]:
    """Copy the compact index once to node-local storage for worker mmap.

    Mapping a persistent index directly from the original shared filesystem caused 128 workers to
    fault the same ~279 MB through the shared filesystem. The V4 fast run used
    /tmp instead. This stage restores that topology while retaining a persistent
    normalized index between jobs.
    """
    source = Path(index_dir)
    root_text = (
        cfg.btc_worker_mmap_root
        or os.environ.get("SLURM_TMPDIR")
        or os.environ.get("TMPDIR")
        or "/tmp"
    )
    root = Path(root_text)
    if not root.is_dir():
        root = Path("/tmp")
    temp = tempfile.TemporaryDirectory(prefix="queue99_underlying_worker_", dir=str(root))
    target = Path(temp.name)
    started_stage = time.monotonic()
    total_bytes = 0
    methods: dict[str, str] = {}
    for name in ("times_ns.npy", "prices.npy", "metadata.json", "READY"):
        src = source / name
        if not src.exists():
            if name == "READY":
                continue
            temp.cleanup()
            raise RuntimeError(f"BTC worker index source is incomplete: missing {src}")
        dst = target / name
        # A hardlink would still point at the same shared-filesystem inode and
        # therefore would not provide the node-local page-fault isolation this
        # mode promises. Always materialize a real copy under the selected
        # node-local root. The compact index is roughly 279 MB for the supplied
        # 20-day range, so this is a short sequential copy rather than a raw
        # 20,528-shard reload.
        shutil.copyfile(src, dst)
        methods[name] = "copy"
        total_bytes += int(src.stat().st_size)
    # Open once before creating workers so an incomplete local copy fails here.
    _open_btc_mmap(target)
    elapsed = time.monotonic() - started_stage
    meta = {
        "source_index_path": str(source),
        "worker_index_path": str(target),
        "temporary_root": str(root),
        "bytes": int(total_bytes),
        "stage_elapsed_sec": float(elapsed),
        "file_methods": methods,
        "cleaned_after_workers": False,
    }
    q.log(
        f"BTC_MID_WORKER_STAGE_DONE bytes={total_bytes:,} elapsed_sec={elapsed:.3f} "
        f"source={source} target={target}",
        started,
    )
    return temp, str(target), meta


def integrated_preflight(tasks: Sequence[q.ContractTask], cfg: Config, token_map: dict[str, str], resolutions: dict[int, str], underlying: q.UnderlyingMidSeries | None, outdir: Path, started: float | None = None) -> dict[str, Any]:
    phase_started = time.monotonic()
    count = min(max(0, cfg.preflight_sample_markets), len(tasks))
    if cfg.preflight_mode == "off":
        count = 0
    if count <= 0:
        payload = {"ok": True, "sampled": 0, "warnings": [], "errors": [], "mode": cfg.preflight_mode, "elapsed_sec": time.monotonic() - phase_started}
        q.atomic_json(outdir / "preflight" / "queue99_preflight.json", payload)
        return payload
    indices = np.linspace(0, len(tasks) - 1, count).round().astype(int)
    sample = [tasks[int(i)] for i in indices]
    warnings: list[str] = []
    errors: list[str] = []
    rows: list[dict[str, Any]] = []
    q.log(f"QUEUE99_INTEGRATED_PREFLIGHT_START mode={cfg.preflight_mode} markets={count} workers={min(cfg.preflight_workers,count)}", started)

    def run_one(task: q.ContractTask) -> tuple[q.ContractTask, dict[str, Any] | None, Exception | None]:
        try:
            result = _evaluate_session(task, cfg, token_map, resolutions, underlying)
            _validate_order_row_contract(result.get("orders", []), context=f"preflight_market={task.contract_start_ms}")
            known_ids = {str(row["order_id"]) for row in result.get("orders", [])}
            for fill_index, fill in enumerate(result.get("fills", [])):
                oid = str(fill.get("order_id", ""))
                if not oid or oid not in known_ids:
                    raise InternalContractError(
                        f"fill_contract_unknown_order_id preflight_market={task.contract_start_ms} "
                        f"fill_index={fill_index} order_id={oid!r}"
                    )
            return task, result, None
        except Exception as exc:
            return task, None, exc

    completed = 0
    if cfg.preflight_mode == "serial_full":
        iterable = [run_one(task) for task in sample]
    else:
        iterable = []
        with ThreadPoolExecutor(max_workers=min(cfg.preflight_workers, count), thread_name_prefix="queue99-preflight") as executor:
            futures = [executor.submit(run_one, task) for task in sample]
            for future in as_completed(futures):
                iterable.append(future.result())
    for task, result, exc in iterable:
        completed += 1
        if exc is None and result is not None:
            rows.append({
                "contract_start_ms": task.contract_start_ms,
                "status": result["market"].get("status"),
                "orders": len(result.get("orders", [])),
                "source_gap": False,
            })
        elif isinstance(exc, q.SourceDataGap):
            warnings.append(f"{task.contract_start_ms}:{exc.reason}")
            rows.append({
                "contract_start_ms": task.contract_start_ms,
                "status": "source_gap",
                "source_gap": True,
                "reason": exc.reason,
                "stage": exc.stage,
                "path": exc.path,
                "details": exc.details,
            })
        else:
            errors.append(f"{task.contract_start_ms}:{exc!r}")
            rows.append({"contract_start_ms": task.contract_start_ms, "status": "error", "error": repr(exc)})
    rows.sort(key=lambda row: int(row.get("contract_start_ms", 0)))
    payload = {
        "ok": not errors,
        "sampled": count,
        "warnings": warnings,
        "errors": errors,
        "rows": rows,
        "mode": cfg.preflight_mode,
        "workers": min(cfg.preflight_workers, count),
        "btc_mid_source": underlying.source if underlying else "fixed",
        "btc_mid_path": underlying.path if underlying else "",
        "underlying_mid_source": underlying.source if underlying else "fixed",
        "underlying_mid_path": underlying.path if underlying else "",
        "underlying_asset": cfg.underlying_asset,
        "binance_symbol": cfg.btc_symbol,
        "chart_boundary_network_access_during_preflight": False,
        "elapsed_sec": time.monotonic() - phase_started,
    }
    q.atomic_json(outdir / "preflight" / "queue99_preflight.json", payload)
    return payload

def classify_run_status(
    *,
    market_error_count: int,
    source_gap_market_count: int,
    markets_selected: int,
    source_gap_policy: str,
    max_source_gap_market_fraction: float,
) -> tuple[str, float]:
    """Return the terminal run status from disjoint coverage classes."""
    selected = max(0, int(markets_selected))
    errors = max(0, int(market_error_count))
    gaps = max(0, int(source_gap_market_count))
    gap_fraction = gaps / max(1, selected)
    if errors:
        return "MARKET_WORKER_ERRORS", gap_fraction
    if gaps and (
        str(source_gap_policy) == "fail"
        or gap_fraction > float(max_source_gap_market_fraction) + 1e-12
    ):
        return "SOURCE_GAP_LIMIT_EXCEEDED", gap_fraction
    if gaps:
        return "COMPLETE_WITH_SOURCE_GAPS", gap_fraction
    return "COMPLETE", gap_fraction


def _validate_order_row_contract(orders: Sequence[dict[str, Any]], *, context: str) -> None:
    """Fail early with a precise contract error instead of a late KeyError.

    Ladder mode can emit multiple audit/placement rows per market, so every row
    must carry a stable identity even when the order never becomes live.
    """
    required = ("order_id", "contract_start_ms", "outcome", "status", "simulation_valid")
    seen: set[str] = set()
    for index, row in enumerate(orders):
        missing = [name for name in required if name not in row or row.get(name) in (None, "")]
        if missing:
            raise InternalContractError(
                f"order_contract_missing_fields context={context} index={index} "
                f"missing={missing} keys={sorted(row)[:80]}"
            )
        oid = str(row.get("order_id"))
        if oid in seen:
            raise InternalContractError(f"order_contract_duplicate_order_id context={context} order_id={oid}")
        seen.add(oid)


def _validate_execution_price_integrity(
    orders: Sequence[dict[str, Any]],
    fills: Sequence[dict[str, Any]],
    queue_trades: Sequence[dict[str, Any]] | None,
    *,
    context: str,
) -> dict[str, Any]:
    """Fail closed when an execution price is detached from its order price.

    The V24 incident was configuration-driven (the legacy state machine was
    silently selected), but this validator prevents a second class of failure:
    a ladder order at 0.96 must never be costed or emitted as a 0.99 fill.
    """
    _validate_order_row_contract(orders, context=context)
    by_id = {str(row["order_id"]): row for row in orders}
    ladder_orders = 0
    serialized_orders = 0
    checked_fills = 0
    checked_queue_rows = 0

    for row in orders:
        is_ladder = q.as_bool(row.get("momentum_ladder_enabled", False))
        is_serialized = q.as_bool(row.get("serialized_fixed_arms_enabled", False))
        if not (is_ladder or is_serialized):
            continue
        if is_ladder:
            ladder_orders += 1
        if is_serialized:
            serialized_orders += 1
        oid = str(row["order_id"])
        limit_price = q.as_float(row.get("limit_price"), math.nan)
        target_price = q.as_float(row.get("ladder_target_price"), limit_price)
        if not (math.isfinite(limit_price) and 0 < limit_price < 1):
            raise InternalContractError(
                f"ladder_price_integrity_invalid_limit context={context} order_id={oid} limit_price={limit_price!r}"
            )
        if math.isfinite(target_price) and abs(target_price - limit_price) > 1e-9:
            raise InternalContractError(
                f"ladder_price_integrity_target_mismatch context={context} order_id={oid} "
                f"target={target_price} limit={limit_price}"
            )
        filled = q.as_float(row.get("potential_filled_shares"), 0.0)
        potential_cost = q.as_float(row.get("potential_cost_usdc"), 0.0)
        avg = q.as_float(row.get("potential_average_fill_price"), math.nan)
        if "post_only_order" not in row:
            raise InternalContractError(
                f"execution_price_integrity_missing_post_only_policy context={context} order_id={oid}"
            )
        post_only = q.as_bool(row.get("post_only_order"))
        tolerance = max(1e-7, max(0.0, filled) * 1e-9)
        if filled > 1e-12:
            worst_cost = filled * limit_price
            if post_only:
                # A resting maker BUY executes at its own limit price.
                if abs(potential_cost - worst_cost) > tolerance:
                    raise InternalContractError(
                        f"ladder_price_integrity_cost_mismatch context={context} order_id={oid} "
                        f"filled={filled} limit={limit_price} potential_cost={potential_cost}"
                    )
                if math.isfinite(avg) and abs(avg - limit_price) > 1e-9:
                    raise InternalContractError(
                        f"ladder_price_integrity_average_mismatch context={context} order_id={oid} "
                        f"average={avg} limit={limit_price}"
                    )
            else:
                # A marketable non-post-only BUY may consume asks strictly BELOW
                # its limit.  That is legitimate price improvement.  Fail only
                # if simulated cost/average is worse than the BUY limit.
                if potential_cost < -tolerance or potential_cost > worst_cost + tolerance:
                    raise InternalContractError(
                        f"ladder_price_integrity_cost_above_limit context={context} order_id={oid} "
                        f"filled={filled} limit={limit_price} potential_cost={potential_cost} worst_cost={worst_cost}"
                    )
                if math.isfinite(avg) and (avg <= 0 or avg > limit_price + 1e-9):
                    raise InternalContractError(
                        f"ladder_price_integrity_average_above_limit context={context} order_id={oid} "
                        f"average={avg} limit={limit_price}"
                    )
                if math.isfinite(avg) and abs(potential_cost - filled * avg) > tolerance:
                    raise InternalContractError(
                        f"ladder_price_integrity_cost_average_mismatch context={context} order_id={oid} "
                        f"filled={filled} average={avg} potential_cost={potential_cost}"
                    )

    for fill in fills:
        oid = str(fill.get("order_id") or "").strip()
        shares = q.as_float(fill.get("fill_shares"), 0.0)
        if shares <= 0:
            continue
        if oid not in by_id:
            raise InternalContractError(
                f"fill_price_integrity_unknown_order context={context} order_id={oid!r}"
            )
        price = q.as_float(fill.get("fill_price"), math.nan)
        if not (math.isfinite(price) and 0 < price < 1):
            raise InternalContractError(
                f"fill_price_integrity_missing_or_invalid context={context} order_id={oid} fill_price={fill.get('fill_price')!r}"
            )
        checked_fills += 1
        order = by_id[oid]
        is_ladder_order = q.as_bool(order.get("momentum_ladder_enabled", False))
        is_serialized_order = q.as_bool(order.get("serialized_fixed_arms_enabled", False))
        if is_ladder_order or is_serialized_order:
            limit_price = q.as_float(order.get("limit_price"), math.nan)
            if "post_only_order" not in order:
                raise InternalContractError(
                    f"execution_price_integrity_missing_post_only_policy context={context} order_id={oid}"
                )
            post_only = q.as_bool(order.get("post_only_order"))
            invalid_price = (
                abs(price - limit_price) > 1e-9
                if post_only
                else price > limit_price + 1e-9
            )
            if invalid_price:
                contract = "ladder_fill_price_mismatch" if is_ladder_order else "serialized_fill_price_mismatch"
                raise InternalContractError(
                    f"{contract} context={context} order_id={oid} "
                    f"fill_price={price} limit_price={limit_price} post_only={int(post_only)} source={fill.get('fill_source','')}"
                )

    for row in queue_trades or []:
        oid = str(row.get("order_id") or "").strip()
        if not oid or oid not in by_id or not q.as_bool(by_id[oid].get("momentum_ladder_enabled", False)):
            continue
        checked_queue_rows += 1
        limit_price = q.as_float(by_id[oid].get("limit_price"), math.nan)
        recorded_limit = q.as_float(row.get("ladder_limit_price"), limit_price)
        if abs(recorded_limit - limit_price) > 1e-9:
            raise InternalContractError(
                f"ladder_queue_price_mismatch context={context} order_id={oid} "
                f"queue_limit={recorded_limit} order_limit={limit_price}"
            )

    return {
        "status": "PASS",
        "ladder_orders_checked": ladder_orders,
        "serialized_fixed_arm_orders_checked": serialized_orders,
        "positive_fill_rows_checked": checked_fills,
        "ladder_queue_rows_checked": checked_queue_rows,
    }


def apply_balance(orders: list[dict[str, Any]], fills: list[dict[str, Any]], cfg: Config) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    _validate_order_row_contract(orders, context="apply_balance")
    by_id = {str(order["order_id"]): order for order in orders}
    fill_cost_by_id: dict[str, float] = defaultdict(float)
    fill_fee_by_id: dict[str, float] = defaultdict(float)
    fill_shares_by_id: dict[str, float] = defaultdict(float)
    for fill in fills:
        oid = str(fill.get("order_id", ""))
        shares = max(0.0, q.as_float(fill.get("fill_shares"), 0.0))
        price = q.as_float(fill.get("fill_price"), math.nan)
        if not oid or shares <= 0:
            continue
        if not math.isfinite(price) or price <= 0 or price >= 1:
            raise InternalContractError(
                f"fill_price_missing_or_invalid context=apply_balance order_id={oid!r} fill_price={fill.get('fill_price')!r}"
            )
        if oid not in by_id:
            raise InternalContractError(f"fill_references_unknown_order context=apply_balance order_id={oid!r}")
        fill_shares_by_id[oid] += shares
        fill_cost_by_id[oid] += shares * price
        fill_fee_by_id[oid] += fee_for_shares(shares, price, cfg)
    events: list[tuple[float, int, str, str]] = []
    for order in orders:
        if order.get("status", "").startswith("skipped") or not order.get("simulation_valid", True):
            continue
        oid = str(order.get("order_id", ""))
        if not oid:
            raise RuntimeError("order_contract_missing_order_id context=apply_balance_event_build")
        open_ms = q.as_float(order.get("order_open_ts_ms"), math.nan)
        if not math.isfinite(open_ms):
            continue
        cancel_eff = q.as_float(order.get("cancel_effective_ts_ms"), math.nan)
        end_ms = order["contract_start_ms"] + q.market_period_sec(cfg.market_key) * 1000
        release_ms = cancel_eff if math.isfinite(cancel_eff) else end_ms
        events.append((open_ms, 1, "open", oid))
        events.append((release_ms, 0, "release", oid))
        events.append((end_ms, 2, "settle", oid))
    events.sort()
    cash = float(cfg.starting_balance)
    reserved: dict[str, float] = {}
    accepted: set[str] = set()
    released: set[str] = set()
    equity_rows: list[dict[str, Any]] = [{"ts_ms": cfg.start_ms, "time_utc": q.market_iso(cfg.start_ms), "cash_usdc": cash, "reserved_usdc": 0.0, "equity_usdc": cash, "event": "start"}]
    peak_reserved = 0.0
    for ts_ms, _, kind, oid in events:
        order = by_id[oid]
        requested = q.as_float(order.get("requested_shares"), 0.0)
        filled = q.as_float(order.get("potential_filled_shares"), 0.0)
        # Fill rows are authoritative for execution price.  The reservation
        # remains limit-price worst case, while actual settlement uses weighted
        # ask prices for the momentum-early crossed-book path.
        event_filled = float(fill_shares_by_id.get(oid, 0.0))
        if abs(event_filled - filled) > 1e-7:
            raise RuntimeError(
                f"fill share reconciliation failed order={oid} order_filled={filled} event_filled={event_filled}"
            )
        limit_price = q.as_float(order.get("limit_price"), math.nan)
        if not math.isfinite(limit_price):
            if q.as_bool(order.get("momentum_ladder_enabled", False)):
                raise InternalContractError(
                    f"ladder_limit_price_missing context=apply_balance order_id={oid}"
                )
            # Legacy synthetic/direct callers historically omitted limit_price;
            # retain TRIGGER_BID only for those non-ladder rows. Ladder rows are
            # price-safe and never receive this fallback.
            limit_price = float(cfg.trigger_bid)
        if limit_price <= 0 or limit_price >= 1:
            raise RuntimeError(f"invalid order limit_price for balance reservation order={oid}: {limit_price!r}")
        reserve_amount = requested * limit_price + fee_for_shares(requested, limit_price, cfg)
        execution_cost = float(fill_cost_by_id.get(oid, 0.0))
        execution_fee = float(fill_fee_by_id.get(oid, 0.0))
        actual_cost = execution_cost + execution_fee
        if kind == "open":
            if not cfg.enforce_balance or cash + 1e-9 >= reserve_amount:
                accepted.add(oid)
                cash -= reserve_amount
                reserved[oid] = reserve_amount
                order["balance_accepted"] = True
                order["balance_at_open_before_usdc"] = cash + reserve_amount
                order["reserved_at_open_usdc"] = reserve_amount
            else:
                order["balance_accepted"] = False
                order["status"] = "rejected_insufficient_balance"
                order["skip_reason"] = "insufficient_balance"
                order["potential_filled_shares_before_balance"] = filled
        elif kind == "release" and oid in accepted and oid not in released:
            refund = max(0.0, reserved.get(oid, 0.0) - actual_cost)
            cash += refund
            reserved[oid] = actual_cost
            released.add(oid)
            order["unfilled_reservation_released_usdc"] = refund
        elif kind == "settle" and oid in accepted:
            if oid not in released:
                refund = max(0.0, reserved.get(oid, 0.0) - actual_cost)
                cash += refund
                reserved[oid] = actual_cost
                released.add(oid)
                order["unfilled_reservation_released_usdc"] = refund
            winner = str(order.get("winner", ""))
            payout = filled if winner == order.get("outcome") else 0.0
            cash += payout
            reserved.pop(oid, None)
            order["filled_shares"] = filled
            order["cost_usdc"] = execution_cost
            order["fee_usdc"] = execution_fee
            order["average_fill_price"] = execution_cost / filled if filled > 0 else math.nan
            order["payout_usdc"] = payout
            order["net_pnl_usdc"] = payout - actual_cost
            order["settled"] = winner in q.OUTCOMES
            order["balance_after_settlement_usdc"] = cash
        peak_reserved = max(peak_reserved, sum(reserved.values()))
        equity_rows.append({
            "ts_ms": ts_ms, "time_utc": q.market_iso(ts_ms), "cash_usdc": cash,
            "reserved_usdc": sum(reserved.values()), "equity_usdc": cash + sum(reserved.values()),
            "event": kind, "order_id": oid,
        })
    accepted_fills = [row for row in fills if str(row.get("order_id")) in accepted]
    for order in orders:
        oid = str(order.get("order_id", ""))
        if oid not in accepted:
            order["filled_shares"] = 0.0
            order["net_pnl_usdc"] = 0.0
            order["cost_usdc"] = 0.0
            order["fee_usdc"] = 0.0
            order["average_fill_price"] = math.nan
            order["payout_usdc"] = 0.0
    # Rebuild mark-to-settlement equity from actual cash events; maximum drawdown on available cash.
    values = np.array([float(row["equity_usdc"]) for row in equity_rows], dtype=float)
    peaks = np.maximum.accumulate(values) if len(values) else np.array([])
    drawdowns = peaks - values if len(values) else np.array([])
    if len(drawdowns):
        drawdowns[np.abs(drawdowns) < 1e-8] = 0.0
    for row, dd in zip(equity_rows, drawdowns):
        row["drawdown_usdc"] = float(dd)
    metrics = {
        "initial_balance_usdc": cfg.starting_balance,
        "final_balance_usdc": cash,
        "net_pnl_usdc": cash - cfg.starting_balance,
        "peak_reserved_usdc": peak_reserved,
        "max_drawdown_usdc": float(drawdowns.max()) if len(drawdowns) else 0.0,
        "orders_accepted_by_balance": len(accepted),
        "orders_rejected_by_balance": sum(order.get("status") == "rejected_insufficient_balance" for order in orders),
    }
    return orders, accepted_fills, equity_rows, metrics


def select_tasks(tasks: list[q.ContractTask], cfg: Config) -> list[q.ContractTask]:
    if cfg.limit_markets > 0:
        tasks = tasks[: cfg.limit_markets]
    if cfg.sample_markets > 0 and len(tasks) > cfg.sample_markets:
        idx = np.linspace(0, len(tasks) - 1, cfg.sample_markets).round().astype(int)
        tasks = [tasks[int(i)] for i in idx]
    return tasks


def _add_multi_asset_output_aliases(rows: list[dict[str, Any]], cfg: Config) -> None:
    """Add outcome-neutral aliases without breaking V20 result consumers.

    Historical CSV columns keep their ``btc_*`` names for backward
    compatibility.  V22 writes equivalent ``underlying_*`` columns so ETH and
    SOL reports are not semantically mislabeled.
    """
    aliases = {
        "btc_mid_source": "underlying_mid_source",
        "btc_timestamp_clock": "underlying_timestamp_clock",
        "btc_market_start_mid": "underlying_market_start_mid",
        "btc_market_start_venue_ts_ms": "underlying_market_start_venue_ts_ms",
        "btc_market_start_observed_ts_ms": "underlying_market_start_observed_ts_ms",
        "btc_market_start_observed_age_ms": "underlying_market_start_observed_age_ms",
        "btc_market_start_venue_age_ms": "underlying_market_start_venue_age_ms",
        "btc_trigger_mid": "underlying_trigger_mid",
        "btc_trigger_venue_ts_ms": "underlying_trigger_venue_ts_ms",
        "btc_trigger_observed_ts_ms": "underlying_trigger_observed_ts_ms",
        "btc_trigger_observed_age_ms": "underlying_trigger_observed_age_ms",
        "btc_trigger_venue_age_ms": "underlying_trigger_venue_age_ms",
        "btc_trigger_series_index": "underlying_trigger_series_index",
        "btc_move_signed_usd": "underlying_move_signed_usd",
        "btc_move_abs_usd": "underlying_move_abs_usd",
        "min_btc_move_usd": "min_underlying_move_usd",
        "btc_sizing_raw_shares": "underlying_sizing_raw_shares",
        "btc_sizing_rounded_shares": "underlying_sizing_rounded_shares",
        "btc_sizing_was_clipped_low": "underlying_sizing_was_clipped_low",
        "btc_sizing_was_clipped_high": "underlying_sizing_was_clipped_high",
    }
    for row in rows:
        row.setdefault("market_key", cfg.market_key)
        row.setdefault("underlying_asset", cfg.underlying_asset)
        row.setdefault("binance_symbol", cfg.btc_symbol)
        for old, new in aliases.items():
            if old in row and new not in row:
                row[new] = row.get(old)


def _legacy_entry_summary_counts(orders: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    """Return counters owned by the legacy entry/retry state machine only.

    Ladder order sequence numbers and ladder placement retries are distinct
    concepts and must never leak into the legacy result-contract counters.
    """
    legacy_orders = [
        row for row in orders
        if not q.as_bool(row.get("momentum_ladder_enabled", False))
        and not q.as_bool(row.get("serialized_fixed_arms_enabled", False))
    ]
    return {
        "post_only_placement_rejections": sum(
            q.as_bool(row.get("post_only_rejected_at_placement", False))
            for row in legacy_orders
        ),
        "retry_opened_orders": sum(
            q.as_bool(row.get("order_was_opened", False))
            and q.as_int(row.get("entry_attempt_number"), 0) > 1
            for row in legacy_orders
        ),
        "retry_switched_outcome_orders": sum(
            q.as_bool(row.get("order_was_opened", False))
            and q.as_bool(row.get("entry_retry_switched_outcome", False))
            for row in legacy_orders
        ),
    }


def _write_underlying_source_summary(outdir: Path, payload: Mapping[str, Any]) -> None:
    """Write the V22 neutral source summary and the legacy BTC-named alias."""
    data = dict(payload)
    q.atomic_json(Path(outdir) / "underlying_mid_source_summary.json", data)
    q.atomic_json(Path(outdir) / "btc_mid_source_summary.json", data)


def run(cfg: Config) -> int:
    started = time.monotonic()
    run_started_utc = pd.Timestamp.now(tz="UTC").isoformat().replace("+00:00", "Z")
    outdir = Path(cfg.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    q.atomic_json(outdir / "effective_config.json", cfg.to_dict())
    q.log("QUEUE99_FEATURE_PLAN " + json.dumps(cfg.feature_plan(), sort_keys=True), started)
    data_dir = Path(cfg.data_dir)
    cache_schema_meta = q.schema.validate_cache_manifest(data_dir, require=bool(cfg.require_canonical_cache_schema))
    q.atomic_json(outdir / "cache_schema_validation.json", cache_schema_meta)
    q.log(
        "QUEUE99_CACHE_SCHEMA "
        f"source={cache_schema_meta.get('schema_source')} manifest_present={int(bool(cache_schema_meta.get('present')))} "
        f"price_scale={cache_schema_meta.get('price_scale')} size_scale={cache_schema_meta.get('size_scale')} "
        f"missing_price_raw={cache_schema_meta.get('missing_price_raw')} missing_size_raw={cache_schema_meta.get('missing_size_raw')} "
        f"timestamp_unit={cache_schema_meta.get('timestamp_unit')}",
        started,
    )
    tasks, task_meta = q.discover_contract_tasks(data_dir, cfg.market_key, cfg.start_ms, cfg.end_ms)
    tasks = select_tasks(tasks, cfg)
    if not tasks:
        raise RuntimeError("no markets selected")
    token_map = q.resolve_asset_map(data_dir, cfg.market_key)
    selected_starts = {int(task.contract_start_ms) for task in tasks}
    resolutions, resolution_meta = q.resolution_map(
        data_dir,
        cfg.market_key,
        cfg.yes_outcome,
        explicit_path=cfg.resolution_path,
        selected_starts=selected_starts,
        allow_btc_settlement=cfg.allow_btc_settlement,
    )
    covered_selected = selected_starts.intersection(resolutions)
    resolution_meta["require_resolution"] = bool(cfg.require_resolution)
    resolution_meta["explicit_path_requested"] = str(cfg.resolution_path_requested or cfg.resolution_path or "")
    resolution_meta["selected_markets"] = int(len(selected_starts))
    resolution_meta["covered_selected_markets"] = int(len(covered_selected))
    resolution_meta["missing_selected_markets"] = int(len(selected_starts - set(resolutions)))
    resolution_meta["selected_coverage_fraction"] = float(len(covered_selected) / max(1, len(selected_starts)))
    requested_resolution_path = str(cfg.resolution_path_requested or cfg.resolution_path or "")
    _apply_resolution_selection(cfg, resolution_meta)
    resolution_meta["configured_path_requested"] = requested_resolution_path
    resolution_meta["effective_config_resolution_path"] = str(cfg.resolution_path or "")
    resolution_meta["resolution_path_auto_corrected"] = bool(cfg.resolution_path_auto_corrected)
    resolution_meta["resolution_path_auto_correction_reason"] = str(cfg.resolution_path_auto_correction_reason or "")
    # Rewrite before any expensive Binance loading or market work.  The file
    # used by the report and final Slurm validator therefore reflects the
    # actual settlement sidecar, while preserving the original request.
    q.atomic_json(outdir / "effective_config.json", cfg.to_dict())
    q.atomic_json(outdir / "resolution_source_summary.json", resolution_meta)
    if cfg.resolution_path_auto_corrected:
        q.log(
            "QUEUE99_RESOLUTION_PATH_AUTO_CORRECTED "
            f"requested={requested_resolution_path or '-'} "
            f"selected={cfg.resolution_path or '-'} "
            f"reason={cfg.resolution_path_auto_correction_reason}",
            started,
        )
    q.log(
        "QUEUE99_RESOLUTION_SOURCE "
        f"path={resolution_meta.get('path') or '-'} "
        f"covered={len(covered_selected)}/{len(selected_starts)} "
        f"records={len(resolutions)} "
        f"reasons={resolution_meta.get('reason_counts', {})}",
        started,
    )
    if cfg.require_resolution and len(covered_selected) != len(selected_starts):
        attempted = [
            item.get("path") for item in resolution_meta.get("attempted_paths", [])
            if item.get("exists") or item.get("explicit")
        ]
        missing_sample = sorted(selected_starts - set(resolutions))[:20]
        raise RuntimeError(
            "REQUIRE_RESOLUTION=1 but the settlement winner sidecar does not cover every selected market; "
            f"covered={len(covered_selected)}/{len(selected_starts)}; "
            f"missing_sample={missing_sample}; explicit_requested={requested_resolution_path or '-'}; "
            f"attempted={attempted}; errors={resolution_meta.get('errors', [])[:8]}; "
            f"see {outdir / 'resolution_source_summary.json'}"
        )
    underlying: q.UnderlyingMidSeries | None = None
    persistent_mmap_dir: str | None = None
    mmap_dir: str | None = None
    mmap_temp: tempfile.TemporaryDirectory[str] | None = None
    worker_mmap_meta: dict[str, Any] = {}
    btc_index_meta: dict[str, Any] = {"enabled": False}
    btc_sizing_enabled = cfg.share_sizing_mode == "btc_move"
    btc_reversal_cancel_enabled = bool(cfg.cancel_if_binance_mid_below_trigger)
    btc_momentum_gate_enabled = cfg.bid99_binance_momentum_gate_enabled
    btc_trading_enabled = (
        btc_sizing_enabled
        or btc_reversal_cancel_enabled
        or btc_momentum_gate_enabled
        or cfg.entry_filter_data_needed
    )
    if btc_trading_enabled:
        q.log(
            f"QUEUE99_BTC_SOURCE_DISCOVERY_START explicit={cfg.btc_price_path or '-'} data_dir={data_dir}",
            started,
        )
        paths = q.discover_binance_path(data_dir, cfg.btc_price_path)
        q.log(
            "QUEUE99_BTC_SOURCE_DISCOVERY_DONE candidates="
            + (",".join(str(path) for path in paths) if paths else "none"),
            started,
        )
        required_btc_prestart_ms = (
            float(cfg.btc_mid_max_age_ms)
            + float(cfg.binance_venue_to_vps_delay_ms)
            + (
                float(cfg.bid99_binance_momentum_lookback_sec) * 1000.0
                if btc_momentum_gate_enabled else 0.0
            )
        )
        underlying, persistent_mmap_dir, btc_index_meta = prepare_btc_index(
            cfg, paths, outdir, started, index_lookback_ms=required_btc_prestart_ms
        )
        _write_underlying_source_summary(outdir, underlying.metadata | {
            "network_access": False,
            "timestamp_clock": "binance_venue",
            "binance_venue_to_vps_delay_ms": float(cfg.binance_venue_to_vps_delay_ms),
            "used_by_sizing": bool(btc_sizing_enabled),
            "used_by_binance_adverse_cancel": bool(btc_reversal_cancel_enabled),
            "used_by_binance_mid_below_trigger_cancel": bool(btc_reversal_cancel_enabled),
            "used_by_bid99_binance_momentum_gate": bool(btc_momentum_gate_enabled),
            "used_by_market_charts": bool(cfg.write_market_charts and cfg.market_charts_per_bucket > 0),
            "note": (
                "Trading uses cached Binance venue timestamps only. A source row is observable on the "
                "simulated VPS at venue_ts_ms + BINANCE_VENUE_TO_VPS_DELAY_MS. The persistent venue-time "
                "index is reused across delay settings; the delay is applied only at lookup time."
            ),
        })
    preflight = integrated_preflight(tasks, cfg, token_map, resolutions, underlying, outdir, started)
    q.log(
        f"QUEUE99_INTEGRATED_PREFLIGHT_DONE ok={int(preflight['ok'])} "
        f"sampled={preflight['sampled']}/{len(tasks):,} mode={preflight.get('mode')} "
        f"elapsed_sec={float(preflight.get('elapsed_sec', 0.0)):.3f}",
        started,
    )
    if not preflight["ok"]:
        return 3
    if persistent_mmap_dir is not None:
        if cfg.btc_worker_index_mode == "shared":
            mmap_dir = str(persistent_mmap_dir)
            index_root = Path(persistent_mmap_dir)
            worker_mmap_meta = {
                "mode": "shared",
                "source_index_path": str(index_root),
                "worker_index_path": str(index_root),
                "bytes": int(sum((index_root / name).stat().st_size for name in ("times_ns.npy", "prices.npy") if (index_root / name).exists())),
                "stage_elapsed_sec": 0.0,
                "file_methods": {},
                "cleanup_required": False,
                "cleaned_after_workers": True,
                "warning": "All workers mmap the persistent shared-filesystem index directly; this avoids the per-run local copy but may increase shared-storage page-fault latency.",
            }
            q.log(
                f"BTC_MID_WORKER_INDEX_SHARED rows={len(underlying.times_ms):,} "
                f"path={mmap_dir} stage_elapsed_sec=0.000",
                started,
            )
        else:
            q.log(
                f"BTC_MID_WORKER_STAGE_START source={persistent_mmap_dir} "
                f"root={cfg.btc_worker_mmap_root or os.environ.get('SLURM_TMPDIR') or os.environ.get('TMPDIR') or '/tmp'}",
                started,
            )
            mmap_temp, mmap_dir, worker_mmap_meta = stage_btc_worker_index(
                persistent_mmap_dir, cfg, started=started
            )
            worker_mmap_meta["mode"] = "local_copy"
        note = (
            "The persistent normalized Binance underlying index avoids rescanning raw shards. Workers map the same persistent index directly; no per-run worker copy was made."
            if cfg.btc_worker_index_mode == "shared"
            else "The persistent normalized Binance underlying index avoids rescanning raw shards; one node-local copy avoids 128-worker page faults from shared storage."
        )
        underlying.metadata = dict(underlying.metadata) | {"worker_sharing": worker_mmap_meta}
        _write_underlying_source_summary(outdir, dict(underlying.metadata) | {
            "network_access": False,
            "timestamp_clock": "binance_venue",
            "binance_venue_to_vps_delay_ms": float(cfg.binance_venue_to_vps_delay_ms),
            "used_by_sizing": bool(btc_sizing_enabled),
            "used_by_binance_adverse_cancel": bool(btc_reversal_cancel_enabled),
            "used_by_binance_mid_below_trigger_cancel": bool(btc_reversal_cancel_enabled),
            "used_by_bid99_binance_momentum_gate": bool(btc_momentum_gate_enabled),
            "used_by_market_charts": bool(cfg.write_market_charts and cfg.market_charts_per_bucket > 0),
            "note": note + " Venue rows become observable only after the configured Binance-to-VPS delay.",
        })
    workers_used = min(int(cfg.workers), max(1, len(tasks)))
    q.log(
        f"QUEUE99_START version={q.VERSION} market_key={cfg.market_key} asset={cfg.underlying_asset_label} symbol={cfg.btc_symbol} markets={len(tasks):,} workers={workers_used}/128 "
        f"entry_age={cfg.entry_market_age_start_sec:g}:{cfg.entry_market_age_end_sec:g}s "
        f"min_underlying_move={cfg.min_btc_move_usd:g} sizing={cfg.share_sizing_mode} "
        f"binance_venue_to_vps_delay_ms={cfg.binance_venue_to_vps_delay_ms:g} "
        f"btc_reversal_cancel={int(cfg.cancel_if_binance_mid_below_trigger)} "
        f"bid99_binance_momentum_min={cfg.bid99_binance_momentum_min_usd:g} "
        f"bid99_binance_momentum_lookback={cfg.bid99_binance_momentum_lookback_sec:g}s "
        f"bid99_binance_momentum_apply_to_99={int(cfg.bid99_binance_momentum_apply_to_99)} "
        f"post_only={int(cfg.post_only_order_enabled)} retry={cfg.entry_retry_policy}",
        started,
    )
    cfg_dict = cfg.to_dict()
    payloads = [{"task": asdict(task)} for task in tasks]
    market_phase_started = time.monotonic()
    # Stream worker payloads into their final row collections instead of
    # retaining every nested worker result until the pool finishes.  This keeps
    # RSS bounded when thousands of markets return multiple ladder attempts.
    markets_acc: list[dict[str, Any]] = []
    orders_acc: list[dict[str, Any]] = []
    fills_acc: list[dict[str, Any]] = []
    queue_trades_acc: list[dict[str, Any]] = []
    bid99_volume_events_acc: list[dict[str, Any]] = []
    source_gaps: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    worker_elapsed_samples: list[float] = []
    source_gap_completed = 0
    completed = 0
    last_log = time.monotonic()
    context_name = "forkserver" if sys.platform.startswith("linux") else "spawn"
    ctx = get_context(context_name)
    try:
        with ctx.Pool(
            processes=workers_used,
            initializer=_worker_init,
            initargs=(cfg_dict, token_map, resolutions, mmap_dir),
        ) as pool:
            iterator = pool.imap_unordered(evaluate_market_worker, payloads, chunksize=1)
            for result in iterator:
                completed += 1
                try:
                    worker_elapsed_samples.append(float(result.get("elapsed_sec", 0.0) or 0.0))
                except Exception:
                    worker_elapsed_samples.append(0.0)
                if result.get("ok"):
                    market_row = result.get("market", {})
                    markets_acc.append(market_row)
                    orders_acc.extend(result.get("orders", []))
                    fills_acc.extend(result.get("fills", []))
                    queue_trades_acc.extend(result.get("queue_trades", []))
                    bid99_volume_events_acc.extend(result.get("bid99_volume_events", []))
                    if result.get("source_gap"):
                        source_gap_completed += 1
                        source_gaps.append(
                            market_row
                            | {
                                "source_gap_reason": result.get("source_gap_reason"),
                                "source_gap_stage": result.get("source_gap_stage"),
                            }
                        )
                else:
                    if result.get("fatal_internal_contract"):
                        raise InternalContractError(
                            "worker_internal_contract_fail_fast "
                            f"market={result.get('market', {}).get('contract_start_ms')} "
                            f"error={result.get('error')}"
                        )
                    errors.append(result)
                    markets_acc.append(result.get("market", {}))
                now = time.monotonic()
                if completed == len(payloads) or now - last_log >= cfg.progress_interval_sec:
                    elapsed = max(1e-9, now - market_phase_started)
                    rate = completed / elapsed
                    eta = (len(payloads) - completed) / rate if rate else math.nan
                    q.log(
                        f"QUEUE99_PROGRESS markets={completed:,}/{len(payloads):,} "
                        f"source_gaps={source_gap_completed:,} errors={len(errors)} "
                        f"rate={rate:.1f}markets/s eta={q.fmt_duration(eta)}",
                        started,
                    )
                    last_log = now
    finally:
        if mmap_temp is not None:
            mmap_temp.cleanup()
            worker_mmap_meta["cleaned_after_workers"] = True
            if underlying is not None:
                underlying.metadata = dict(underlying.metadata) | {"worker_sharing": worker_mmap_meta}
            source_summary_path = next(
                (path for path in (outdir / "underlying_mid_source_summary.json", outdir / "btc_mid_source_summary.json") if path.exists()),
                outdir / "underlying_mid_source_summary.json",
            )
            try:
                payload = json.loads(source_summary_path.read_text(encoding="utf-8"))
                payload["worker_sharing"] = worker_mmap_meta
                _write_underlying_source_summary(outdir, payload)
            except Exception:
                pass
    market_phase_elapsed = time.monotonic() - market_phase_started
    markets = markets_acc
    orders = orders_acc
    fills = fills_acc
    queue_trades = queue_trades_acc
    bid99_volume_events = bid99_volume_events_acc
    price_integrity = _validate_execution_price_integrity(
        orders, fills, queue_trades, context="pre_balance_full_run"
    )
    orders, fills, equity, balance = apply_balance(orders, fills, cfg)
    _add_multi_asset_output_aliases(markets, cfg)
    _add_multi_asset_output_aliases(orders, cfg)
    _add_multi_asset_output_aliases(fills, cfg)
    _add_multi_asset_output_aliases(queue_trades, cfg)
    _add_multi_asset_output_aliases(bid99_volume_events, cfg)
    # Aggregate actual market PnL after balance.
    pnl_by_market: dict[int, float] = defaultdict(float)
    for order in orders:
        pnl_by_market[q.as_int(order.get("contract_start_ms"), 0)] += q.as_float(order.get("net_pnl_usdc"), 0.0)
    for row in markets:
        row["net_pnl_usdc"] = pnl_by_market.get(q.as_int(row.get("contract_start_ms"), 0), 0.0)
    source_complete = len(tasks) - len(source_gaps) - len(errors)
    status, gap_fraction = classify_run_status(
        market_error_count=len(errors),
        source_gap_market_count=len(source_gaps),
        markets_selected=len(tasks),
        source_gap_policy=cfg.source_gap_policy,
        max_source_gap_market_fraction=cfg.max_source_gap_market_fraction,
    )
    valid_orders = [o for o in orders if o.get("simulation_valid", True) and o.get("balance_accepted", False)]
    filled_orders = [o for o in valid_orders if q.as_float(o.get("filled_shares"), 0.0) > 0]
    wins = [o for o in filled_orders if q.as_float(o.get("net_pnl_usdc"), 0.0) > 0]
    losses = [o for o in filled_orders if q.as_float(o.get("net_pnl_usdc"), 0.0) < 0]
    queue_reconstruction_opened_orders = [
        o for o in valid_orders
        if q.as_bool(o.get("order_was_opened", False))
        and str(o.get("queue_ahead_reconstruction_mode", "off")).strip().lower() != "off"
    ]
    queue_reconstruction_affected_orders = [
        o for o in queue_reconstruction_opened_orders
        if q.as_int(o.get("queue_depth_reconstruction_applied_events"), 0) > 0
    ]
    queue_reconstruction_filled_orders = [
        o for o in queue_reconstruction_affected_orders
        if q.as_float(o.get("filled_shares"), 0.0) > 0
    ]
    legacy_entry_counts = _legacy_entry_summary_counts(orders)
    serialized_valid_orders = [o for o in valid_orders if q.as_bool(o.get("serialized_fixed_arms_enabled", False))]
    if cfg.serialized_fixed_arms_enabled:
        legacy_valid_orders = [
            o for o in serialized_valid_orders
            if abs(q.as_float(o.get("serialized_arm_price"), math.nan) - 0.99) <= 1e-9
        ]
        ladder_valid_orders = []
    else:
        legacy_valid_orders = [o for o in valid_orders if not q.as_bool(o.get("momentum_ladder_enabled", False))]
        ladder_valid_orders = [o for o in valid_orders if q.as_bool(o.get("momentum_ladder_enabled", False))]
    legacy_filled_orders = [o for o in legacy_valid_orders if q.as_float(o.get("filled_shares"), 0.0) > 0]
    ladder_filled_orders = [o for o in ladder_valid_orders if q.as_float(o.get("filled_shares"), 0.0) > 0]
    ladder_pnl_by_price: dict[str, float] = defaultdict(float)
    ladder_filled_orders_by_price: dict[str, int] = defaultdict(int)
    ladder_filled_shares_by_price: dict[str, float] = defaultdict(float)
    for order in ladder_valid_orders:
        price = q.as_float(order.get("limit_price"), math.nan)
        if not math.isfinite(price):
            continue
        price_key = f"{price:.2f}"
        ladder_pnl_by_price[price_key] += q.as_float(order.get("net_pnl_usdc"), 0.0)
        if q.as_float(order.get("filled_shares"), 0.0) > 0:
            ladder_filled_orders_by_price[price_key] += 1
            ladder_filled_shares_by_price[price_key] += q.as_float(order.get("filled_shares"), 0.0)
    serialized_pnl_by_price: dict[str, float] = defaultdict(float)
    serialized_filled_orders_by_price: dict[str, int] = defaultdict(int)
    serialized_filled_shares_by_price: dict[str, float] = defaultdict(float)
    for order in serialized_valid_orders:
        price = q.as_float(order.get("serialized_arm_price"), q.as_float(order.get("limit_price"), math.nan))
        if not math.isfinite(price):
            continue
        key = f"{price:.2f}"
        serialized_pnl_by_price[key] += q.as_float(order.get("net_pnl_usdc"), 0.0)
        if q.as_float(order.get("filled_shares"), 0.0) > 0:
            serialized_filled_orders_by_price[key] += 1
            serialized_filled_shares_by_price[key] += q.as_float(order.get("filled_shares"), 0.0)

    summary = {
        "schema": q.SCHEMA,
        "version": q.VERSION,
        "status": status,
        "execution_price_integrity": price_integrity,
        "cache_schema": cache_schema_meta,
        "market_key": cfg.market_key,
        "underlying_asset": cfg.underlying_asset,
        "underlying_asset_label": cfg.underlying_asset_label,
        "market_timeframe": cfg.market_timeframe,
        "market_period_sec": q.market_period_sec(cfg.market_key),
        "binance_symbol": cfg.btc_symbol,
        "started_utc": run_started_utc,
        "ended_utc": pd.Timestamp.now(tz="UTC").isoformat().replace("+00:00", "Z"),
        "elapsed_sec": time.monotonic() - started,
        "markets_selected": len(tasks),
        "markets_processed": len(markets),
        "source_complete_markets": source_complete,
        "source_complete_market_fraction": source_complete / max(1, len(tasks)),
        "source_gap_market_count": len(source_gaps),
        "source_gap_market_fraction": gap_fraction,
        "source_gap_reason_counts": dict(Counter(str(x.get("source_gap_reason")) for x in source_gaps)),
        "source_gap_stage_counts": dict(Counter(str(x.get("source_gap_stage")) for x in source_gaps)),
        "source_session_recovered_markets": sum(bool(m.get("source_session_recovered")) for m in markets),
        "source_session_recovery_attempted_markets": sum(
            str(m.get("source_session_attempts", "")).count('"status":"source_gap"') > 0
            for m in markets
        ),
        "market_error_count": len(errors),
        "market_errors": [{"contract_start_ms": e["market"].get("contract_start_ms"), "error": e.get("error"), "traceback": e.get("traceback")} for e in errors[:200]],
        "source_coverage_reconciliation": {
            "selected": len(tasks), "source_complete": source_complete, "source_gaps": len(source_gaps),
            "worker_errors": len(errors), "matches": source_complete + len(source_gaps) + len(errors) == len(tasks),
        },
        "triggers_found": sum(q.as_int(m.get("bid99_candidates_before_policy"), 0) for m in markets),
        "queue_ahead_reconstruction_mode": str(cfg.queue_ahead_reconstruction_mode).strip().lower(),
        "queue_ahead_reconstruction_enabled": str(cfg.queue_ahead_reconstruction_mode).strip().lower() != "off",
        "queue_ahead_reconstruction_opened_orders": len(queue_reconstruction_opened_orders),
        "queue_ahead_reconstruction_affected_orders": len(queue_reconstruction_affected_orders),
        "queue_ahead_reconstruction_filled_orders": len(queue_reconstruction_filled_orders),
        "queue_depth_reconstruction_applied_events": sum(
            q.as_int(o.get("queue_depth_reconstruction_applied_events"), 0)
            for o in queue_reconstruction_opened_orders
        ),
        "queue_depth_added_behind_shares": float(sum(
            q.as_float(o.get("queue_depth_added_behind_shares"), 0.0)
            for o in queue_reconstruction_opened_orders
        )),
        "queue_depth_cancelled_behind_shares": float(sum(
            q.as_float(o.get("queue_depth_cancelled_behind_shares"), 0.0)
            for o in queue_reconstruction_opened_orders
        )),
        "queue_depth_cancelled_ahead_shares": float(sum(
            q.as_float(o.get("queue_depth_cancelled_ahead_shares"), 0.0)
            for o in queue_reconstruction_opened_orders
        )),
        "serialized_fixed_arms_enabled": bool(cfg.serialized_fixed_arms_enabled),
        "serialized_fixed_arm_prices": cfg.serialized_fixed_arm_price_list if cfg.serialized_fixed_arms_enabled else [],
        "serialized_fixed_arm_shares": cfg.serialized_fixed_arm_share_list if cfg.serialized_fixed_arms_enabled else [],
        "serialized_fixed_arm_momentum_mins": cfg.serialized_fixed_arm_momentum_min_list if cfg.serialized_fixed_arms_enabled else [],
        "serialized_fixed_arm_sizing_mode": str(cfg.serialized_fixed_arm_sizing_mode) if cfg.serialized_fixed_arms_enabled else "disabled",
        "serialized_fixed_arm_btc_move_divisors": cfg.serialized_fixed_arm_btc_move_divisor_list if cfg.serialized_fixed_arms_enabled else [],
        "serialized_fixed_arm_busy_policy": str(cfg.serialized_fixed_arm_busy_policy) if cfg.serialized_fixed_arms_enabled else "disabled",
        "serialized_fixed_arm_upshift_cancel_delay_ms": float(cfg.serialized_fixed_arm_upshift_cancel_delay_ms) if cfg.serialized_fixed_arms_enabled else None,
        "serialized_fixed_arm_ack_ms": float(cfg.serialized_fixed_arm_ack_ms) if cfg.serialized_fixed_arms_enabled else None,
        "serialized_fixed_arm_slot_claim_mode": str(cfg.serialized_fixed_arm_slot_claim_mode) if cfg.serialized_fixed_arms_enabled else "disabled",
        "serialized_fixed_arm_suppress_competing": bool(cfg.serialized_fixed_arm_suppress_competing) if cfg.serialized_fixed_arms_enabled else False,
        "serialized_fixed_arm_report_eligibility": cfg.serialized_report_eligibility() if cfg.serialized_fixed_arms_enabled else {"eligible": False, "mismatches": ["mode_disabled"]},
        "serialized_fixed_arm_selected_attempts": sum(q.as_int(m.get("serialized_fixed_arm_selected_attempts"), 0) for m in markets),
        "serialized_fixed_arm_selected_opened_orders": sum(q.as_int(m.get("serialized_fixed_arm_selected_opened_orders"), 0) for m in markets),
        "serialized_fixed_arm_selected_rejected_attempts": sum(q.as_int(m.get("serialized_fixed_arm_selected_rejected_attempts"), 0) for m in markets),
        "serialized_fixed_arm_blocked_attempts": sum(q.as_int(m.get("serialized_fixed_arm_blocked_attempts"), 0) for m in markets),
        "serialized_fixed_arm_suppressed_rows": sum(q.as_int(m.get("serialized_fixed_arm_suppressed_rows"), 0) for m in markets),
        "serialized_fixed_arm_arms_suppressed_on_busy": sum(q.as_int(m.get("serialized_fixed_arm_arms_suppressed_on_busy"), 0) for m in markets),
        "serialized_fixed_arm_pre_send_filter_rows": sum(q.as_int(m.get("serialized_fixed_arm_pre_send_filter_rows"), 0) for m in markets),
        "serialized_fixed_arm_overlap_violations": sum(q.as_int(m.get("serialized_fixed_arm_overlap_violations"), 0) for m in markets),
        "serialized_fixed_arm_upshift_cancel_requests": sum(q.as_int(m.get("serialized_fixed_arm_upshift_cancel_requests"), 0) for m in markets),
        "serialized_fixed_arm_retry_blocked_signals": bool(cfg.serialized_fixed_arm_retry_blocked_signals) if cfg.serialized_fixed_arms_enabled else False,
        "serialized_fixed_arm_retry_supersede_lower_on_higher_signal": bool(cfg.serialized_fixed_arm_retry_supersede_lower_on_higher_signal) if cfg.serialized_fixed_arms_enabled else False,
        "serialized_fixed_arm_retry_continuation_claim_attempts": sum(q.as_int(m.get("serialized_fixed_arm_retry_continuation_claim_attempts"), 0) for m in markets),
        "serialized_fixed_arm_retry_continuation_selected_opened": sum(q.as_int(m.get("serialized_fixed_arm_retry_continuation_selected_opened"), 0) for m in markets),
        "serialized_fixed_arm_retry_continuation_suppressed_after_real_open": sum(q.as_int(m.get("serialized_fixed_arm_retry_continuation_suppressed_after_real_open"), 0) for m in markets),
        "serialized_fixed_arm_retry_continuation_suppressed_by_higher_signal": sum(q.as_int(m.get("serialized_fixed_arm_retry_continuation_suppressed_by_higher_signal"), 0) for m in markets),
        "serialized_fixed_arm_net_pnl_usdc": float(sum(serialized_pnl_by_price.values())),
        "serialized_fixed_arm_filled_orders": int(sum(serialized_filled_orders_by_price.values())),
        "serialized_fixed_arm_filled_shares": float(sum(serialized_filled_shares_by_price.values())),
        "serialized_fixed_arm_pnl_by_limit_price": dict(sorted(serialized_pnl_by_price.items())),
        "serialized_fixed_arm_filled_orders_by_limit_price": dict(sorted(serialized_filled_orders_by_price.items())),
        "serialized_fixed_arm_filled_shares_by_limit_price": dict(sorted(serialized_filled_shares_by_price.items())),
        "momentum_ladder_enabled": bool(cfg.momentum_ladder_enabled),
        "momentum_ladder_preserve_legacy_99": bool(cfg.momentum_ladder_enabled and cfg.momentum_ladder_preserve_legacy_99),
        "momentum_ladder_make_before_break": bool(cfg.momentum_ladder_enabled and cfg.momentum_ladder_make_before_break),
        "momentum_ladder_fast_single_read_path": bool(cfg.feature_plan()["momentum_ladder_fast_single_read_path"]),
        "momentum_ladder_overlay_single_read_path": bool(cfg.feature_plan().get("momentum_ladder_overlay_single_read_path", False)),
        "momentum_ladder_bbo_reads_skipped_markets": sum(q.as_bool(m.get("momentum_ladder_bbo_read_skipped", False)) for m in markets),
        "momentum_ladder_depth_rows_loaded": sum(q.as_int(m.get("ladder_depth_rows", m.get("depth_rows", 0)), 0) for m in markets if q.as_bool(m.get("momentum_ladder_enabled", False))),
        "momentum_ladder_trade_rows_loaded": sum(q.as_int(m.get("ladder_trade_rows", m.get("trade_rows", 0)), 0) for m in markets if q.as_bool(m.get("momentum_ladder_enabled", False))),
        "momentum_ladder_selected_markets": sum(
            q.as_bool(m.get("momentum_ladder_overlay_evaluated", False))
            or str(m.get("status", "")).startswith("evaluated_ladder")
            for m in markets
        ),
        "momentum_ladder_opened_orders": sum(q.as_bool(o.get("order_was_opened", False)) and q.as_bool(o.get("momentum_ladder_enabled", False)) for o in orders),
        "momentum_ladder_post_only_rejections": sum(q.as_int(o.get("post_only_rejections_before_open"), 0) for o in orders if q.as_bool(o.get("momentum_ladder_enabled", False))),
        "momentum_ladder_reprice_orders": sum(q.as_bool(o.get("ladder_reprice_selected", False)) for o in orders),
        "momentum_ladder_full_fills": sum(str(o.get("status", "")).startswith("fully_filled") for o in orders if q.as_bool(o.get("momentum_ladder_enabled", False))),
        "momentum_ladder_deals_started": sum(q.as_int(m.get("ladder_deals_started"), 0) for m in markets),
        "momentum_ladder_max_deals_per_market": int(cfg.momentum_ladder_max_deals_per_market) if cfg.momentum_ladder_enabled else None,
        "momentum_ladder_reprice_enabled": bool(cfg.momentum_ladder_reprice_enabled) if cfg.momentum_ladder_enabled else False,
        "legacy_99_opened_orders": sum(q.as_bool(o.get("order_was_opened", False)) for o in legacy_valid_orders),
        "legacy_99_filled_orders": len(legacy_filled_orders),
        "legacy_99_filled_shares": sum(q.as_float(o.get("filled_shares"), 0.0) for o in legacy_filled_orders),
        "legacy_99_net_pnl_usdc": sum(q.as_float(o.get("net_pnl_usdc"), 0.0) for o in legacy_valid_orders),
        "momentum_ladder_lower_filled_orders": len(ladder_filled_orders),
        "momentum_ladder_lower_filled_shares": sum(q.as_float(o.get("filled_shares"), 0.0) for o in ladder_filled_orders),
        "momentum_ladder_lower_net_pnl_usdc": sum(q.as_float(o.get("net_pnl_usdc"), 0.0) for o in ladder_valid_orders),
        "momentum_ladder_pnl_by_limit_price": dict(sorted(ladder_pnl_by_price.items())),
        "momentum_ladder_filled_orders_by_limit_price": dict(sorted(ladder_filled_orders_by_price.items())),
        "momentum_ladder_filled_shares_by_limit_price": dict(sorted(ladder_filled_shares_by_price.items())),
        "exact_bid99_trigger_orders": sum(str(o.get("entry_trigger_source", "")) == "exact_bid99" for o in orders),
        "momentum_early_trigger_orders": sum(str(o.get("entry_trigger_source", "")) == "momentum_early_bid" for o in orders),
        "bid99_binance_momentum_apply_to_99": bool(cfg.bid99_binance_momentum_apply_to_99),
        "exact_bid99_momentum_gate_enabled": bool(cfg.exact_bid99_momentum_gate_enabled),
        "exact_bid99_momentum_bypassed_orders": sum(
            q.as_bool(o.get("bid99_binance_momentum_bypassed_for_exact_bid99", False)) for o in orders
        ),
        "momentum_early_cross_match_orders": sum(q.as_bool(o.get("momentum_early_cross_match_scheduled", False)) for o in orders),
        "momentum_early_cross_match_filled_orders": sum(q.as_float(o.get("momentum_early_cross_match_filled_shares"), 0.0) > 0 for o in orders),
        "momentum_early_cross_match_filled_shares": sum(q.as_float(o.get("momentum_early_cross_match_filled_shares"), 0.0) for o in orders),
        "post_only_order_enabled": bool(cfg.post_only_order_enabled),
        "entry_retry_policy": cfg.entry_retry_policy,
        "post_only_cross_retry_enabled": bool(
            cfg.post_only_cross_retry_delay_ms >= 0
            and (not cfg.momentum_ladder_enabled or cfg.momentum_ladder_preserve_legacy_99)
        ),
        "post_only_cross_retry_delay_ms": (
            float(cfg.post_only_cross_retry_delay_ms)
            if cfg.post_only_cross_retry_delay_ms >= 0
            and (not cfg.momentum_ladder_enabled or cfg.momentum_ladder_preserve_legacy_99)
            else None
        ),
        "post_only_cross_retry_armed": sum(q.as_int(m.get("post_only_cross_retry_armed"), 0) for m in markets),
        "post_only_cross_retry_attempts": sum(q.as_int(m.get("post_only_cross_retry_attempts"), 0) for m in markets),
        "post_only_cross_retry_opened_orders": sum(q.as_int(m.get("post_only_cross_retry_opened_orders"), 0) for m in markets),
        "post_only_cross_retry_rejected_again": sum(q.as_int(m.get("post_only_cross_retry_rejected_again"), 0) for m in markets),
        "post_only_cross_retry_armed_without_qualifying_candidate": sum(
            q.as_int(m.get("post_only_cross_retry_armed_without_qualifying_candidate"), 0) for m in markets
        ),
        "post_only_cross_retry_revalidation_rejections": sum(
            q.as_int(m.get("post_only_cross_retry_revalidation_rejections"), 0) for m in markets
        ),
        "post_only_cross_retry_candidates_suppressed_before_delay": sum(
            q.as_int(m.get("post_only_cross_retry_candidates_suppressed_before_delay"), 0) for m in markets
        ),
        "post_only_cross_retry_candidates_suppressed_other_side": sum(
            q.as_int(m.get("post_only_cross_retry_candidates_suppressed_other_side"), 0) for m in markets
        ),
        "post_only_cross_retry_candidates_suppressed_non_exact": sum(
            q.as_int(m.get("post_only_cross_retry_candidates_suppressed_non_exact"), 0) for m in markets
        ),
        "post_only_cross_retry_revalidation_rejection_reasons": dict(Counter(
            reason
            for market_row in markets
            for reason, count in (market_row.get("post_only_cross_retry_revalidation_rejection_reasons") or {}).items()
            for _ in range(q.as_int(count, 0))
        )),
        "entry_attempts_reached": sum(q.as_int(m.get("entry_attempts_reached"), 0) for m in markets),
        "entry_retry_attempts": sum(q.as_int(m.get("entry_retry_attempts"), 0) for m in markets),
        "post_only_placement_rejections": legacy_entry_counts["post_only_placement_rejections"],
        "orders_opened_after_entry_attempts": sum(
            q.as_bool(o.get("order_was_opened", False)) for o in orders
        ),
        "retry_opened_orders": legacy_entry_counts["retry_opened_orders"],
        "retry_switched_outcome_orders": legacy_entry_counts["retry_switched_outcome_orders"],
        "entry_attempt_result_counts": dict(Counter(
            str(o.get("entry_attempt_result", ""))
            for o in orders if o.get("entry_attempt_result")
        )),
        "entry_attempt_failure_stage_counts": dict(Counter(
            str(o.get("entry_attempt_failure_stage", ""))
            for o in orders if o.get("entry_attempt_failure_stage")
        )),
        "orders_total": len(orders),
        "orders_accepted_by_balance": balance["orders_accepted_by_balance"],
        "orders_filled_any": len(filled_orders),
        "filled_shares": sum(q.as_float(o.get("filled_shares"), 0.0) for o in filled_orders),
        "filled_cost_usdc": sum(q.as_float(o.get("cost_usdc"), 0.0) for o in filled_orders),
        "average_fill_price": (
            sum(q.as_float(o.get("cost_usdc"), 0.0) for o in filled_orders)
            / sum(q.as_float(o.get("filled_shares"), 0.0) for o in filled_orders)
            if sum(q.as_float(o.get("filled_shares"), 0.0) for o in filled_orders) > 0 else 0.0
        ),
        "wins": len(wins), "losses": len(losses),
        "win_rate": len(wins) / len(filled_orders) if filled_orders else 0.0,
        "gross_profit_usdc": sum(max(0.0, q.as_float(o.get("net_pnl_usdc"), 0.0)) for o in filled_orders),
        "gross_loss_usdc": sum(min(0.0, q.as_float(o.get("net_pnl_usdc"), 0.0)) for o in filled_orders),
        "initial_balance_usdc": balance["initial_balance_usdc"],
        "final_balance_usdc": balance["final_balance_usdc"],
        "net_pnl_usdc": balance["net_pnl_usdc"],
        "roi_initial": balance["net_pnl_usdc"] / cfg.starting_balance if cfg.starting_balance else 0.0,
        "max_drawdown_usdc": balance["max_drawdown_usdc"],
        "peak_reserved_usdc": balance["peak_reserved_usdc"],
        "order_status_counts": dict(Counter(str(o.get("status", "")) for o in orders)),
        "order_skip_reason_counts": dict(Counter(str(o.get("skip_reason", "")) for o in orders if o.get("skip_reason"))),
        "signal_bbo_required_components": ["bid_price"],
        "signal_bbo_optional_components": ["bid_size", "ask_price", "ask_size"],
        "order_open_depth_required_components": [
            "bid_size_if_existing_best_bid_equals_limit",
            "ask_size_if_existing_ask_is_marketable",
        ],
        "order_open_absent_level_semantics": {
            "missing_bid_price": "known_empty_bid_side_queue_ahead_zero",
            "missing_ask_price": "known_empty_ask_side_not_crossed",
            "present_bid_at_limit_missing_size": "hard_block",
            "present_marketable_ask_missing_size": "hard_block",
        },
        "orders_opened_with_absent_bid": sum(
            1 for o in orders
            if q.as_bool(o.get("order_was_opened", False))
            and q.as_bool(o.get("order_open_absent_bid_allowed", False))
        ),
        "orders_opened_with_absent_ask": sum(
            1 for o in orders
            if q.as_bool(o.get("order_was_opened", False))
            and q.as_bool(o.get("order_open_absent_ask_allowed", False))
        ),
        "signal_bbo_candidates_with_optional_missing_data": sum(
            q.as_int(m.get("signal_bbo_candidates_with_optional_missing_data"), 0) for m in markets
        ),
        "signal_bbo_optional_missing_by_component": dict(Counter({
            component: sum(q.as_int((m.get("signal_bbo_optional_missing_by_component") or {}).get(component), 0) for m in markets)
            for component in {component for m in markets for component in (m.get("signal_bbo_optional_missing_by_component") or {}).keys()}
        })),
        "ladder_signal_candidates_with_optional_missing_data": sum(
            q.as_int(m.get("ladder_signal_candidates_with_optional_missing_data"), 0) for m in markets
        ),
        "ladder_signal_optional_missing_by_component": dict(Counter({
            component: sum(q.as_int((m.get("ladder_signal_optional_missing_by_component") or {}).get(component), 0) for m in markets)
            for component in {component for m in markets for component in (m.get("ladder_signal_optional_missing_by_component") or {}).keys()}
        })),
        "candidate_trades_blocked_missing_market_data": sum(
            q.as_int(m.get("candidate_trades_blocked_missing_market_data"), 0) for m in markets
        ),
        "blocked_missing_market_data_by_stage": dict(Counter({
            stage: sum(q.as_int((m.get("blocked_missing_market_data_by_stage") or {}).get(stage), 0) for m in markets)
            for stage in {stage for m in markets for stage in (m.get("blocked_missing_market_data_by_stage") or {}).keys()}
        })),
        "blocked_missing_market_data_by_component": dict(Counter({
            component: sum(q.as_int((m.get("blocked_missing_market_data_by_component") or {}).get(component), 0) for m in markets)
            for component in {component for m in markets for component in (m.get("blocked_missing_market_data_by_component") or {}).keys()}
        })),
        "orders_with_ask99_appearance_confirmation": sum(
            bool(o.get("ask99_appearance_confirmation_applied")) for o in valid_orders
        ),
        "ask99_appearance_confirmed_fill_shares": sum(
            q.as_float(o.get("ask99_confirmation_fill_shares"), 0.0) for o in valid_orders
        ),
        "orders_with_below_limit_price_priority_confirmation": sum(
            bool(o.get("below_limit_trade_fill_applied")) for o in valid_orders
        ),
        "below_limit_price_priority_fill_shares": sum(
            q.as_float(o.get("below_limit_price_priority_fill_shares"), 0.0) for o in valid_orders
        ),
        "price_priority_confirmed_fill_shares": sum(
            q.as_float(o.get("price_priority_confirmed_fill_shares"), 0.0) for o in valid_orders
        ),
        "bid99_unexplained_drop_cancel_orders": sum(
            str(o.get("cancel_reason", "")) == "bid99_unexplained_drop_threshold" for o in valid_orders
        ),
        "bid99_volume_event_rows": len(bid99_volume_events),
        "bid99_volume_raw_depth_events_scanned": sum(
            q.as_int(o.get("bid99_volume_raw_depth_events_scanned"), 0) for o in orders
        ),
        "bid99_volume_raw_trade_events_scanned": sum(
            q.as_int(o.get("bid99_volume_raw_trade_events_scanned"), 0) for o in orders
        ),
        "bid99_volume_output_interval_ms": (
            float(cfg.bid99_volume_output_interval_ms)
            if cfg.write_bid99_volume_timeline else None
        ),
        "queue_ahead_threshold_cancel_orders": sum(
            str(o.get("cancel_reason", "")) == "queue_ahead_above_threshold" for o in valid_orders
        ),
        "limit_order_lifetime_cancel_requests": sum(
            str(o.get("cancel_reason", "")) == "limit_order_lifetime_expired" for o in valid_orders
        ),
        "limit_order_lifetime_cancel_orders": sum(
            str(o.get("cancel_reason", "")) == "limit_order_lifetime_expired"
            and q.as_bool(o.get("cancel_effective_before_market_end", False))
            for o in valid_orders
        ),
        "binance_adverse_detected_orders": sum(
            q.as_bool(o.get("binance_adverse_detected", False)) for o in valid_orders
        ),
        "binance_adverse_cancel_requests": sum(
            str(o.get("cancel_reason", "")) == "binance_mid_adverse_to_outcome" for o in valid_orders
        ),
        "binance_adverse_cancel_orders": sum(
            str(o.get("cancel_reason", "")) == "binance_mid_adverse_to_outcome"
            and q.as_bool(o.get("cancel_effective_before_market_end", False))
            for o in valid_orders
        ),
        "binance_adverse_detected_before_open_orders": sum(
            q.as_bool(o.get("binance_adverse_detected_before_order_open", False))
            for o in valid_orders
        ),
        # Legacy aliases for downstream scripts written against V13.
        "binance_mid_below_trigger_detected_orders": sum(
            q.as_bool(o.get("binance_adverse_detected", False)) for o in valid_orders
        ),
        "binance_mid_below_trigger_cancel_requests": sum(
            str(o.get("cancel_reason", "")) == "binance_mid_adverse_to_outcome" for o in valid_orders
        ),
        "binance_mid_below_trigger_cancel_orders": sum(
            str(o.get("cancel_reason", "")) == "binance_mid_adverse_to_outcome"
            and q.as_bool(o.get("cancel_effective_before_market_end", False))
            for o in valid_orders
        ),
        "binance_mid_below_trigger_detected_before_open_orders": sum(
            q.as_bool(o.get("binance_adverse_detected_before_order_open", False))
            for o in valid_orders
        ),
        "bid99_binance_momentum_configured": bool(cfg.bid99_binance_momentum_configured),
        "bid99_binance_momentum_apply_to_99": bool(cfg.bid99_binance_momentum_apply_to_99),
        "bid99_binance_momentum_gate_enabled": bool(btc_momentum_gate_enabled),
        "bid99_binance_momentum_exact_bid99_gate_enabled": bool(
            cfg.feature_plan()["bid99_binance_momentum_exact_bid99_gate_enabled"]
        ),
        "bid99_binance_momentum_early_gate_enabled": bool(
            cfg.feature_plan()["bid99_binance_momentum_early_gate_enabled"]
        ),
        "bid99_binance_momentum_scope": cfg.feature_plan()["bid99_binance_momentum_scope"],
        "bid99_binance_momentum_candidates_evaluated": sum(
            q.as_bool(o.get("bid99_binance_momentum_evaluated", False)) for o in orders
        ),
        "bid99_binance_momentum_exact_bid99_bypassed_orders": sum(
            q.as_bool(o.get("bid99_binance_momentum_bypassed_for_exact_bid99", False)) for o in orders
        ),
        "bid99_binance_momentum_exact_bid99_evaluated_orders": sum(
            str(o.get("entry_trigger_source", "")) == "exact_bid99"
            and q.as_bool(o.get("bid99_binance_momentum_evaluated", False))
            for o in orders
        ),
        "bid99_binance_momentum_early_evaluated_orders": sum(
            str(o.get("entry_trigger_source", "")) == "momentum_early_bid"
            and q.as_bool(o.get("bid99_binance_momentum_evaluated", False))
            for o in orders
        ),
        "bid99_binance_momentum_passed_orders": sum(
            q.as_bool(o.get("bid99_binance_momentum_passed", False)) for o in orders
        ),
        "bid99_binance_momentum_rejected_orders": sum(
            str(o.get("skip_reason", "")) == "bid99_binance_momentum_not_confirmed"
            for o in orders
        ),
        "bid99_binance_momentum_missing_reference_orders": sum(
            str(o.get("skip_reason", "")) == "missing_or_stale_bid99_binance_momentum_reference"
            for o in orders
        ),
        "btc_mid_source": underlying.metadata if underlying else {"source": "fixed", "enabled": False},
        "underlying_mid_source": underlying.metadata if underlying else {"source": "fixed", "enabled": False},
        "btc_mid_index_cache": btc_index_meta,
        "underlying_mid_index_cache": btc_index_meta,
        "task_index": task_meta,
        "resolution": resolution_meta,
        "effective_config": cfg.to_dict(),
        "runtime": {
            "preflight_elapsed_sec": float(preflight.get("elapsed_sec", 0.0) or 0.0),
            "market_phase_elapsed_sec": float(market_phase_elapsed),
            "market_phase_markets_per_sec": float(len(tasks) / max(1e-9, market_phase_elapsed)),
            "market_worker_elapsed_sec_p50": float(np.quantile(worker_elapsed_samples, 0.50)) if worker_elapsed_samples else 0.0,
            "market_worker_elapsed_sec_p90": float(np.quantile(worker_elapsed_samples, 0.90)) if worker_elapsed_samples else 0.0,
            "market_worker_elapsed_sec_p99": float(np.quantile(worker_elapsed_samples, 0.99)) if worker_elapsed_samples else 0.0,
            "btc_index_cache_hit": bool(btc_index_meta.get("index_cache_hit", False)),
            "btc_index_cache_path": str(btc_index_meta.get("index_cache_path", "")),
            "btc_source_load_elapsed_sec": float(btc_index_meta.get("current_run_source_load_elapsed_sec", 0.0) or 0.0),
            "btc_index_write_elapsed_sec": float(btc_index_meta.get("current_run_index_write_elapsed_sec", 0.0) or 0.0),
            "btc_index_prepare_elapsed_sec": float(btc_index_meta.get("current_run_index_prepare_elapsed_sec", 0.0) or 0.0),
            "btc_index_bytes": int(btc_index_meta.get("index_bytes", 0) or 0),
            "btc_worker_index_mode": cfg.btc_worker_index_mode,
            "btc_worker_stage_elapsed_sec": float(worker_mmap_meta.get("stage_elapsed_sec", 0.0) or 0.0),
            "underlying_index_cache_hit": bool(btc_index_meta.get("index_cache_hit", False)),
            "underlying_index_cache_path": str(btc_index_meta.get("index_cache_path", "")),
            "underlying_source_load_elapsed_sec": float(btc_index_meta.get("current_run_source_load_elapsed_sec", 0.0) or 0.0),
            "underlying_index_write_elapsed_sec": float(btc_index_meta.get("current_run_index_write_elapsed_sec", 0.0) or 0.0),
            "underlying_index_prepare_elapsed_sec": float(btc_index_meta.get("current_run_index_prepare_elapsed_sec", 0.0) or 0.0),
            "underlying_index_bytes": int(btc_index_meta.get("index_bytes", 0) or 0),
            "underlying_worker_index_mode": cfg.btc_worker_index_mode,
            "underlying_worker_stage_elapsed_sec": float(worker_mmap_meta.get("stage_elapsed_sec", 0.0) or 0.0),
        },
        "runtime_contract": {"workers_requested": int(cfg.workers), "workers_used": workers_used, "pool_chunksize": 1, "runtime_allowed_cpu_count": len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None, "parallel_sweep_mode": bool(str(os.environ.get("QUEUE99_PARALLEL_SWEEP_MODE", "0")).strip().lower() in {"1","true","yes","y","on"})},
    }
    q.write_csv(outdir / "queue99_markets.csv", markets)
    q.write_csv(outdir / "queue99_orders.csv", orders)
    q.write_csv(outdir / "queue99_fill_events.csv", fills)
    q.write_csv(outdir / "queue99_queue_trades.csv", queue_trades)
    bid99_output_meta: dict[str, Any] = {
        "enabled": False,
        "source_rows": 0,
        "written_rows": 0,
        "size_bytes": 0,
        "max_bytes": int(max(0.0, float(cfg.bid99_volume_max_mib)) * 1024 * 1024),
        "hard_cap_satisfied": True,
    }
    if cfg.write_bid99_volume_timeline:
        q.log(
            f"QUEUE99_BID99_VOLUME_OUTPUT_START source_rows={len(bid99_volume_events):,} "
            f"interval_ms={cfg.bid99_volume_output_interval_ms:g} "
            f"max_mib={cfg.bid99_volume_max_mib:g}",
            started,
        )
        bid99_output_started = time.monotonic()
        bid99_output_meta = q.write_bounded_csv_gzip(
            outdir / "queue99_bid99_volume_events.csv.gz",
            bid99_volume_events,
            max_bytes=int(float(cfg.bid99_volume_max_mib) * 1024 * 1024),
            preview_path=outdir / "queue99_bid99_volume_events_preview.csv",
            preview_rows=int(cfg.bid99_volume_preview_rows),
            compression_level=6,
        ) | {
            "enabled": True,
            "output_interval_ms": float(cfg.bid99_volume_output_interval_ms),
            "simulation_used_full_resolution": True,
            "persistence_is_compact_interval_ledger": True,
        }
        bid99_output_meta["elapsed_sec"] = float(time.monotonic() - bid99_output_started)
        q.atomic_json(outdir / "queue99_bid99_volume_output.json", bid99_output_meta)
        q.log(
            f"QUEUE99_BID99_VOLUME_OUTPUT_DONE written_rows={int(bid99_output_meta.get('written_rows', 0)):,} "
            f"omitted_rows={int(bid99_output_meta.get('omitted_rows', 0)):,} "
            f"size_mib={float(bid99_output_meta.get('size_bytes', 0))/(1024*1024):.3f} "
            f"elapsed_sec={float(bid99_output_meta.get('elapsed_sec', 0)):.3f}",
            started,
        )
    summary["bid99_volume_output"] = bid99_output_meta
    summary["bid99_volume_event_rows_persisted"] = int(bid99_output_meta.get("written_rows", 0) or 0)
    summary["bid99_volume_event_rows_omitted_by_size_cap"] = int(bid99_output_meta.get("omitted_rows", 0) or 0)
    q.write_csv(outdir / "queue99_source_gaps.csv", source_gaps)
    q.write_csv(outdir / "queue99_equity_curve.csv", equity)
    q.atomic_json(outdir / "queue99_summary.json", summary)
    q.atomic_json(outdir / "queue99_diagnostics.json", {
        "preflight": preflight, "task_index": task_meta, "resolution": resolution_meta,
        "worker_errors": summary["market_errors"], "source_gaps": source_gaps,
    })
    # Reports and charts are intentionally generated after the simulation from the exact persisted rows.
    report_started = time.monotonic()
    q.log("QUEUE99_REPORT_START", started)
    try:
        from queue99_report import write_html_report
        write_html_report(outdir, summary, cfg.to_dict(), markets, orders, fills, equity)
        report_meta_path = outdir / "queue99_report_size.json"
        if report_meta_path.exists():
            summary["html_report"] = json.loads(report_meta_path.read_text(encoding="utf-8"))
    except Exception as exc:
        summary["report_error"] = repr(exc)
        q.atomic_json(outdir / "queue99_summary.json", summary)
        if cfg.fail_on_market_chart_error:
            status = "REPORT_ERROR"
    report_elapsed = time.monotonic() - report_started
    summary["runtime"]["report_elapsed_sec"] = float(report_elapsed)
    q.log(f"QUEUE99_REPORT_DONE elapsed_sec={report_elapsed:.3f}", started)
    chart_elapsed = 0.0
    charts_enabled = bool(cfg.write_market_charts and cfg.market_charts_per_bucket > 0)
    if charts_enabled and status in {"COMPLETE", "COMPLETE_WITH_SOURCE_GAPS"}:
        chart_started = time.monotonic()
        q.log(
            f"QUEUE99_MARKET_CHARTS_START per_bucket={cfg.market_charts_per_bucket} "
            f"dpi={cfg.market_chart_dpi} max_points={cfg.market_chart_max_points}",
            started,
        )
        try:
            from queue99_market_charts import generate_market_charts, market_chart_selection_plan

            chart_plan = market_chart_selection_plan(
                markets, orders, int(cfg.market_charts_per_bucket)
            )
            summary["market_chart_selection"] = {
                key: value
                for key, value in chart_plan.items()
                if key != "groups"
            }
            chart_underlying = underlying
            if int(chart_plan.get("selected_chart_count", 0) or 0) <= 0:
                # A run may legitimately produce only filtered/rejected attempts and
                # therefore have no simulation-valid, balance-accepted order market
                # that can be ranked by settled PnL. Do not load Binance solely for
                # an empty chart set, and do not reinterpret this as a render error.
                q.log(
                    "QUEUE99_MARKET_CHARTS_NO_ELIGIBLE_MARKETS "
                    f"eligible={chart_plan.get('eligible_market_count', 0)} "
                    f"orders={len(orders)} reason={chart_plan.get('reason')!r}",
                    started,
                )
            elif chart_underlying is None:
                # Binance underlying midpoint is mandatory on every chart that is
                # actually selected. In move-sizing mode the trading index is reused.
                # In fixed mode it is prepared only after successful simulation.
                q.log(
                    f"QUEUE99_CHART_BINANCE_SOURCE_DISCOVERY_START explicit={cfg.btc_price_path or '-'} data_dir={data_dir}",
                    started,
                )
                chart_paths = q.discover_binance_path(data_dir, cfg.btc_price_path)
                q.log(
                    "QUEUE99_CHART_BINANCE_SOURCE_DISCOVERY_DONE candidates="
                    + (",".join(str(path) for path in chart_paths) if chart_paths else "none"),
                    started,
                )
                chart_underlying, chart_index_dir, chart_btc_meta = prepare_btc_index(
                    cfg,
                    chart_paths,
                    outdir,
                    started,
                    index_lookback_ms=float(cfg.binance_venue_to_vps_delay_ms),
                )
                btc_index_meta = chart_btc_meta
                summary["btc_mid_index_cache"] = chart_btc_meta
                summary["btc_mid_source"] = chart_underlying.metadata
                summary["runtime"].update({
                    "btc_index_cache_hit": bool(chart_btc_meta.get("index_cache_hit", False)),
                    "btc_index_cache_path": str(chart_btc_meta.get("index_cache_path", chart_index_dir)),
                    "btc_source_load_elapsed_sec": float(chart_btc_meta.get("current_run_source_load_elapsed_sec", 0.0) or 0.0),
                    "btc_index_write_elapsed_sec": float(chart_btc_meta.get("current_run_index_write_elapsed_sec", 0.0) or 0.0),
                    "btc_index_prepare_elapsed_sec": float(chart_btc_meta.get("current_run_index_prepare_elapsed_sec", 0.0) or 0.0),
                    "btc_index_bytes": int(chart_btc_meta.get("index_bytes", 0) or 0),
                    "btc_worker_index_mode": "not_used_chart_only",
                    "btc_worker_stage_elapsed_sec": 0.0,
                })
                _write_underlying_source_summary(outdir, dict(chart_underlying.metadata) | {
                    "network_access": False,
                    "timestamp_clock": "binance_venue",
                    "binance_venue_to_vps_delay_ms": float(cfg.binance_venue_to_vps_delay_ms),
                    "used_by_sizing": False,
                    "used_by_binance_adverse_cancel": False,
                    "used_by_binance_mid_below_trigger_cancel": False,
                    "used_by_market_charts": True,
                    "note": (
                        "Fixed-size trading did not use Binance. The venue-time Binance midpoint index was "
                        "opened or built only after successful simulation because every selected market PNG "
                        "requires the series; chart times use venue_ts_ms plus the configured delay."
                    ),
                })
                q.atomic_json(outdir / "queue99_summary.json", summary)
            chart_summary = generate_market_charts(
                cfg, tasks, token_map, markets, orders, fills, queue_trades,
                bid99_volume_events, chart_underlying, outdir
            )
            summary["market_charts"] = chart_summary
            # Rewrite after PNG generation so the HTML contains direct chart links.
            from queue99_report import write_html_report
            write_html_report(outdir, summary, cfg.to_dict(), markets, orders, fills, equity)
            report_meta_path = outdir / "queue99_report_size.json"
            if report_meta_path.exists():
                summary["html_report"] = json.loads(report_meta_path.read_text(encoding="utf-8"))
        except Exception as exc:
            summary["market_chart_error"] = repr(exc)
            if cfg.fail_on_market_chart_error:
                status = "MARKET_CHART_ERROR"
        chart_elapsed = time.monotonic() - chart_started
        summary["runtime"]["market_charts_elapsed_sec"] = float(chart_elapsed)
        q.log(f"QUEUE99_MARKET_CHARTS_DONE elapsed_sec={chart_elapsed:.3f}", started)
    else:
        summary["runtime"]["market_charts_elapsed_sec"] = 0.0
    summary["status"] = status
    q.atomic_json(outdir / "queue99_summary.json", summary)
    outputs = []
    for path in sorted(p for p in outdir.rglob("*") if p.is_file() and "btc_mid_mmap" not in str(p) and "underlying_mid_mmap" not in str(p)):
        outputs.append({"path": str(path.relative_to(outdir)), "size": path.stat().st_size, "sha256": q.file_sha256(path)})
    q.atomic_json(outdir / "queue99_output_manifest.json", {"version": q.VERSION, "files": outputs})
    if status in {"COMPLETE", "COMPLETE_WITH_SOURCE_GAPS"}:
        (outdir / "RUN_COMPLETE.txt").write_text(f"status={status}\nversion={q.VERSION}\nended_utc={summary['ended_utc']}\n", encoding="utf-8")
    q.log(
        f"QUEUE99_DONE status={status} markets={len(tasks):,} source_gaps={len(source_gaps):,} "
        f"errors={len(errors):,} orders={len(orders):,} "
        f"filled_orders={len(filled_orders):,} filled_shares={summary['filled_shares']:.4f} "
        f"net_pnl={summary['net_pnl_usdc']:.6f} elapsed={q.fmt_duration(time.monotonic()-started)} "
        f"report={outdir/'queue99_report.html'}",
        started,
    )
    if status in {"COMPLETE", "COMPLETE_WITH_SOURCE_GAPS"}:
        return 0
    if status == "MARKET_WORKER_ERRORS":
        return 2
    return 3


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    ns = parser.parse_args(argv)
    cfg = config_from_args(ns)
    outdir = Path(cfg.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    q.atomic_json(outdir / "effective_config.json", cfg.to_dict())
    if ns.validate_config_only:
        print(json.dumps(cfg.to_dict(), indent=2, sort_keys=True))
        return 0
    return run(cfg)


if __name__ == "__main__":
    raise SystemExit(main())
