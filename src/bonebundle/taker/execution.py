"""A2 principal-budget FAK mechanics, full-depth extension and explicit benchmarks.

No order submission, hidden outcomes in sizing, latency grid snapping, implicit
complement double-sweep, or live-wallet certification. Fees are per price leg.
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
import math

from .depth import BookView

HOLD50 = int(datetime(2026,8,17,11,tzinfo=timezone.utc).timestamp()*1000)
HOLD150 = int(datetime(2026,9,4,14,tzinfo=timezone.utc).timestamp()*1000)


def _integer(value, name):
    if isinstance(value,bool) or not isinstance(value,int) or value<0:
        raise ValueError(f'{name}: nonnegative exact integer required')
    return value


def crypto_hold_ms(venue_ingress_ms):
    t = _integer(venue_ingress_ms,'venue_ingress_ms')
    return 250 if t<HOLD50 else 50 if t<HOLD150 else 150


def match_time_ms(decision_ms, *, total_latency_ms=None, ingress_latency_ms=None, applicable=True):
    decision_ms = _integer(decision_ms,'decision_ms')
    if (total_latency_ms is None)==(ingress_latency_ms is None):
        raise ValueError('Supply exactly one of total latency or ingress latency')
    if total_latency_ms is not None:
        return decision_ms+_integer(total_latency_ms,'total_latency_ms')
    ingress = decision_ms+_integer(ingress_latency_ms,'ingress_latency_ms')
    return ingress+(crypto_hold_ms(ingress) if applicable else 0)


def mapped_complement_price(price):
    price = float(price)
    if not math.isfinite(price) or not 0<=price<=1:
        raise ValueError('binary price outside [0,1]')
    return round(1.-price,6)


@dataclass
class ConsumptionLedger:
    """A2 simsize conservative nonreplenishment scenario for one contract.

    Historical future books omit hypothetical own fills. This subtracts prior
    modeled fills by token/price until contract end. Public L2 cannot identify
    true refill/cancel identity, so this is an explicit sensitivity assumption.
    """
    contract_id: str
    used: dict = field(default_factory=dict)
    last_match_ms: int = -1


@dataclass(frozen=True)
class MatchResult:
    status: str
    reason: str
    matched_shares: float = 0.
    principal_cost: float = 0.
    fee_cash_value: float = 0.
    legs: tuple = ()
    depth_mode: str = 'full'
    sizing_mode: str = 'PRINCIPAL_BUDGET'
    limitations: tuple = ()

    @property
    def vwap(self):
        return self.principal_cost/self.matched_shares if self.matched_shares>0 else None


def _match(book, request, limit_price, *, fee_rate, depth_mode, depletion, shares=False):
    mode = 'FIXED_SHARE_BENCHMARK' if shares else 'PRINCIPAL_BUDGET'
    def unknown(reason):
        return MatchResult('UNQUALIFIED',reason,depth_mode=depth_mode,sizing_mode=mode)
    if not isinstance(book,BookView) or not book.qualified:
        return unknown(getattr(book,'reason','INVALID_BOOK'))
    if book.clock_basis!='venue':
        return unknown('RECEIPT_BOOK_IS_NOT_VENUE_MATCH_BOOK')
    if not book.strict_before:
        return unknown('ARRIVAL_REQUIRES_STRICT_BEFORE_VIEW')
    if book.source_ms is None or book.source_ms>=book.match_ms:
        return unknown('FUTURE_OR_SAME_MS_COMPONENT')
    if not book.complete:
        return unknown('INCOMPLETE_DEPTH')
    if depth_mode not in ('full','a2_four'):
        raise ValueError('depth_mode must be full or a2_four')
    try:
        if any(isinstance(x,bool) for x in (request,limit_price,fee_rate)):
            return unknown('INVALID_ORDER_TERMS')
        request,limit_price,fee_rate = float(request),float(limit_price),float(fee_rate)
        if not all(math.isfinite(x) for x in (request,limit_price,fee_rate)) or request<=0 or not 0<limit_price<1 or fee_rate<0:
            return unknown('INVALID_ORDER_TERMS')
        levels = [(float(p),float(q)) for p,q in book.asks]
        if any(not math.isfinite(p) or not math.isfinite(q) or not 0<p<=1 or q<0 for p,q in levels):
            return unknown('INVALID_LEVEL')
        if any(levels[i][0]>=levels[i+1][0] for i in range(len(levels)-1)):
            return unknown('NONINCREASING_OR_DUPLICATE_ASKS')
    except (TypeError,ValueError,OverflowError):
        return unknown('INVALID_ORDER_OR_LADDER')
    if depletion is not None:
        if depletion.contract_id!=book.contract_id or not book.contract_id:
            return unknown('DEPLETION_CONTRACT_MISMATCH')
        if book.match_ms<depletion.last_match_ms:
            return unknown('DEPLETION_TIME_REVERSED')
    limits = ('EXPLORATORY_EXECUTION_WITHOUT_SIGNED_PAYLOAD_OR_WALLET_LEDGER',)
    if depth_mode=='a2_four':
        levels = levels[:4]
        limits += ('DATED_A2_FOUR_LEVEL_CAP',)
    if shares:
        limits += ('FIXED_SHARES_DO_NOT_ESTABLISH_POLYMARKET_BUY_FAK_SEMANTICS',)
    if depletion is not None:
        limits += ('OWN_DEPTH_NONREPLENISHMENT_SCENARIO',)
    got = cost = fee = 0.
    legs = []
    staged = {}
    for price,quantity in levels:
        # Historic A2 arrival arrays were rounded to 4 dp; full replay preserves
        # integer-micro price precision and dynamically observed .001 ticks.
        p = round(price,4) if depth_mode=='a2_four' else price
        if p>limit_price+1e-9:
            break
        if quantity<=0:
            if depth_mode=='a2_four':
                break
            continue
        key = (book.is_yes,round(price*1_000_000))
        available = quantity-(depletion.used.get(key,0.) if depletion is not None else 0.)
        if available<=1e-9:
            continue
        take = min(available,request-got if shares else (request-cost)/p)
        if take<=1e-9:
            break
        legfee = fee_rate*p*(1-p)*take
        got += take
        cost += take*p
        fee += legfee
        staged[key] = staged.get(key,0.)+take
        legs.append(dict(price=p,shares=take,principal=take*p,fee_cash_value=legfee))
        if (shares and got>=request-1e-9) or (not shares and cost>=request-1e-6):
            break
    if depletion is not None:
        for key,take in staged.items():
            depletion.used[key] = depletion.used.get(key,0.)+take
        depletion.last_match_ms = book.match_ms
    satisfied = got>=request-1e-9 if shares else cost>=request-1e-6
    status = 'NO_FILL' if got<=1e-9 else 'FILLED' if satisfied else 'PARTIAL'
    return MatchResult(status,'MODELED_FAK_MATCH',got,cost,fee,tuple(legs),depth_mode,mode,limits)


def match_budget_fak(book, principal_usd, limit_price, *, fee_rate=.07, depth_mode='full', depletion=None):
    """BUY FAK spends principal at compliant asks; better prices buy more shares.

    Budget excludes fee valuation, matching the dated A2 convention. This is a
    diagnostic price-level model; SDK cents/4dp rounding remains a known gap.
    """
    return _match(book,principal_usd,limit_price,fee_rate=fee_rate,depth_mode=depth_mode,depletion=depletion)


def fixed_share_benchmark(book, requested_shares, limit_price, *, fee_rate=.07, depth_mode='full', depletion=None):
    """Separate quantity-limited comparison, not a certified venue BUY FAK."""
    return _match(book,requested_shares,limit_price,fee_rate=fee_rate,depth_mode=depth_mode,depletion=depletion,shares=True)


def settlement_pnl(result, token_won):
    """Contract-end payout proxy; missing outcome remains unknown (None)."""
    if result.status=='UNQUALIFIED' or token_won is None:
        return None
    if token_won not in (0,1,False,True):
        raise ValueError('binary official outcome required')
    return result.matched_shares*int(token_won)-result.principal_cost-result.fee_cash_value
