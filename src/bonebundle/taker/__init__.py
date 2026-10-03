"""Portable, explicitly scoped execution research references; no live transport."""
from .depth import BookView, DepthReplay
from .binance import BinanceL2View, read_binance_l2_hour, reconstruct_binance_l2
from .execution import (
    ConsumptionLedger, MatchResult, crypto_hold_ms, fixed_share_benchmark,
    match_budget_fak, match_time_ms, mapped_complement_price, settlement_pnl,
)

__all__ = ['BookView', 'DepthReplay', 'ConsumptionLedger', 'MatchResult',
           'crypto_hold_ms', 'match_time_ms', 'match_budget_fak',
           'fixed_share_benchmark', 'mapped_complement_price', 'settlement_pnl']
__all__ += ['BinanceL2View', 'read_binance_l2_hour', 'reconstruct_binance_l2']
