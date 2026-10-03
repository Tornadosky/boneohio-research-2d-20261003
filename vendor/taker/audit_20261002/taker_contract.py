"""Small, deliberately strict simulated taker matcher; not exchange certification.

The caller owns provenance and synchronization. Only confirmed simulated fills
commit cash, inventory, reservations and liquidity depletion. Terminal no-fills
commit idempotence/chronology metadata. See
CONTRACT.md for the required explicit metadata and unsupported observations.
"""
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_HALF_UP
import hashlib
import json
import math


_AUG17 = int(datetime(2026, 8, 17, 11, tzinfo=timezone.utc).timestamp()) * 10**9
_SEP4 = int(datetime(2026, 9, 4, 14, tzinfo=timezone.utc).timestamp()) * 10**9
_ROUNDINGS = {"ROUND_DOWN": ROUND_DOWN, "ROUND_HALF_UP": ROUND_HALF_UP}


class _Unqualified(ValueError):
    pass


def _need(condition, reason):
    if not condition:
        raise _Unqualified(reason)


def _number(value, name, *, positive=False):
    _need(not isinstance(value, bool) and isinstance(value, (int, float, str, Decimal)), "INVALID_" + name)
    try:
        value = Decimal(str(value))
        valid = value.is_finite() and math.isfinite(float(value))
    except (ValueError, OverflowError, InvalidOperation):
        valid = False
    _need(valid and (value > 0 if positive else value >= 0), "INVALID_" + name)
    return value


def _clock(value, name):
    _need(type(value) is int and value >= 0, "INVALID_" + name)
    return value


def _identifier(value, name):
    _need(isinstance(value, str) and bool(value.strip()), "INVALID_" + name)
    return value


def crypto_hold_ms(venue_ingress_ns, applicable=True):
    """Dated crypto hold at venue ingress, with exact nanosecond boundaries."""
    _clock(venue_ingress_ns, "VENUE_INGRESS_NS")
    _need(type(applicable) is bool, "INVALID_APPLICABILITY")
    if not applicable:
        return 0
    return 250 if venue_ingress_ns < _AUG17 else 50 if venue_ingress_ns < _SEP4 else 150


def match_time_ns(decision_ns, *, total_latency_ms=None, ingress_latency_ms=None, applicable=True):
    """Use exactly one latency budget. Total already includes any venue hold."""
    _clock(decision_ns, "DECISION_NS")
    _need(type(applicable) is bool, "INVALID_APPLICABILITY")
    _need((total_latency_ms is None) != (ingress_latency_ms is None), "AMBIGUOUS_LATENCY_BUDGET")
    delay = _number(total_latency_ms if total_latency_ms is not None else ingress_latency_ms, "LATENCY_MS") * 10**6
    _need(delay == delay.to_integral_value(), "SUB_NANOSECOND_LATENCY")
    result = decision_ns + int(delay)
    if ingress_latency_ms is not None:
        result += crypto_hold_ms(result, applicable) * 10**6
    return result


def _round(value, quantum, rounding=ROUND_DOWN):
    return (value / quantum).to_integral_value(rounding=rounding) * quantum


def _float(value):
    result = float(value)
    _need(math.isfinite(result), "NUMERIC_OVERFLOW")
    return result


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False, default=str).encode()).hexdigest()


def _result(status, reason, **extra):
    return dict(status=status, reason=reason, filled_quantity=0.0, net_quantity=0.0,
                vwap=None, gross_notional=0.0, fee_cash=0.0, fee_shares=0.0,
                cash_delta=0.0, position_delta=0.0, legs=[], replayed=False, **extra)


def execute(intent: dict, book: dict, account: dict) -> dict:
    """Evaluate an explicit FAK/FOK intent and atomically commit a simulated fill.

    Missing/unsupported evidence returns UNQUALIFIED. A valid but unmatchable
    order returns NO_FILL. Never creates/releases timeout reservations, guesses
    fee policy, clips limits or sweeps a second complementary book.
    """
    try:
        return _execute(intent, book, account)
    except _Unqualified as error:
        return _result("UNQUALIFIED", str(error))
    except (KeyError, TypeError, InvalidOperation, ValueError, OverflowError) as error:
        return _result("UNQUALIFIED", "MISSING_OR_INVALID_INPUT:" + str(error))


def _execute(intent, book, account):
    _need(isinstance(intent, dict) and isinstance(account, dict), "MISSING_INTENT_OR_ACCOUNT")
    venue_id = _identifier(intent["venue"], "VENUE")
    _need(venue_id in ("GENERIC_SHARE_TEST", "POLYMARKET"), "UNSUPPORTED_VENUE")
    _need(intent["sizing_mode"] == "SHARES", "UNSUPPORTED_SIZING_MODE")
    _need(intent["sizing_semantics_established"] is True, "UNESTABLISHED_SIZING_SEMANTICS")
    _need(not (venue_id == "POLYMARKET" and intent.get("side") == "BUY"
               and intent.get("order_type") in ("FAK", "FOK")),
          "UNSUPPORTED_POLYMARKET_BUY_BUDGET_SEMANTICS")
    attempt = _identifier(intent["attempt_id"], "ATTEMPT_ID")
    fingerprint = _digest(intent)
    state = account.get("_taker_state", {})
    _need(isinstance(state, dict), "INVALID_ACCOUNT_STATE")
    attempts = state.get("attempts", {})
    _need(isinstance(attempts, dict), "INVALID_ATTEMPTS_STATE")
    if attempt in attempts:
        previous = attempts[attempt]
        _need(isinstance(previous, dict), "INVALID_ATTEMPT_STATE")
        _need(previous["fingerprint"] == fingerprint, "ATTEMPT_ID_COLLISION")
        result = deepcopy(previous["result"])
        result["replayed"] = True
        return result

    _need(intent["order_type"] in ("FAK", "FOK"), "UNSUPPORTED_ORDER_TYPE")
    side = intent["side"]
    _need(side in ("BUY", "SELL"), "UNSUPPORTED_SIDE")
    asset = _identifier(intent["asset_id"], "ASSET_ID")
    decision = _clock(intent["decision_ns"], "DECISION_NS")
    match = _clock(intent["match_ns"], "MATCH_NS")
    _need(match >= decision, "MATCH_BEFORE_DECISION")
    opening = _clock(account["asof_ns"], "ACCOUNT_ASOF_NS")
    watermark = _clock(state.get("last_match_ns", opening), "ACCOUNT_WATERMARK_NS")
    _need(match >= max(opening, watermark), "ACCOUNT_TIME_REVERSAL")
    max_age = _clock(intent["max_book_age_ns"], "MAX_BOOK_AGE_NS")
    step = _number(intent["quantity_step"], "QUANTITY_STEP", positive=True)
    tick = _number(intent["tick_size"], "TICK_SIZE", positive=True)
    limit = _number(intent["limit_price"], "LIMIT_PRICE", positive=True)
    _need(limit < 1 and tick < 1, "PRICE_OUT_OF_RANGE")
    _need(limit % tick == 0, "LIMIT_OFF_TICK")
    requested = _round(_number(intent["quantity"], "QUANTITY", positive=True), step)
    minimum = _number(intent["min_order_size"], "MIN_ORDER_SIZE")
    min_notional = _number(intent["min_notional"], "MIN_NOTIONAL")
    _need(requested > 0 and requested >= minimum and requested * limit >= min_notional, "BELOW_MINIMUM")

    policy = intent["fee"]
    rate = _number(policy["rate"], "FEE_RATE")
    _need(rate <= 1, "INVALID_FEE_RATE")
    mode = policy["mode"]
    _need(mode in ("CASH_ON_TOP", "SHARES_DEDUCTED"), "UNKNOWN_FEE_MODE")
    _need(policy["scope"] == "PER_LEG", "UNSUPPORTED_FEE_SCOPE")
    _need(policy["rounding"] in _ROUNDINGS, "UNKNOWN_FEE_ROUNDING")
    rounding = _ROUNDINGS[policy["rounding"]]
    fee_quantum = _number(policy["quantum"], "FEE_QUANTUM", positive=True)
    shares_quantum = None
    if side == "BUY" and mode == "SHARES_DEDUCTED":
        shares_quantum = _number(policy["shares_quantum"], "SHARE_FEE_QUANTUM", positive=True)

    _need(isinstance(book, dict), "MISSING_BOOK")
    _need(book["asset_id"] == asset, "ASSET_MISMATCH")
    revision = _identifier(book["revision"], "REVISION")
    _need(book["clock_basis"] == "venue", "UNSUPPORTED_BOOK_CLOCK")
    venue = _clock(book["venue_ns"], "BOOK_VENUE_NS")
    component = _clock(book["component_max_ns"], "COMPONENT_MAX_NS")
    _need(venue <= match and component <= match, "FUTURE_BOOK_COMPONENT")
    _need(match - venue <= max_age, "STALE_BOOK")
    _need(book["normalized"] is True, "UNNORMALIZED_BOOK")
    ladders = {}
    ids = set()
    for name in ("asks", "bids"):
        levels = book[name]
        _need(isinstance(levels, list), "INVALID_LADDER")
        ladders[name] = []
        for level in levels:
            lid = _identifier(level["liquidity_id"], "LIQUIDITY_ID")
            _need(lid not in ids, "DUPLICATE_LIQUIDITY_ID")
            ids.add(lid)
            price = _number(level["price"], "LEVEL_PRICE", positive=True)
            quantity = _number(level["quantity"], "LEVEL_QUANTITY")
            _need(price < 1, "PRICE_OUT_OF_RANGE")
            _need(price % tick == 0, "BOOK_OFF_TICK")
            ladders[name].append((lid, price, quantity))
    content = _digest({name: sorted([(lid,str(p),str(q)) for lid,p,q in levels]) for name,levels in ladders.items()})
    revision_key = json.dumps([asset, revision], separators=(",", ":"))
    revisions = state.get("revisions", {})
    _need(isinstance(revisions, dict), "INVALID_REVISIONS_STATE")
    prior = revisions.get(revision_key, {})
    _need(isinstance(prior, dict), "INVALID_REVISION_STATE")
    _need(not prior or prior["content"] == content, "REVISION_CONTENT_CHANGED")
    book_used = book.get("_depletion", {})
    _need(isinstance(book_used, dict), "INVALID_BOOK_DEPLETION")
    if book_used:
        _need(book.get("_depletion_revision") == revision, "DEPLETION_REVISION_MISMATCH")
    used = {}
    for source in (book_used, prior.get("used", {})):
        _need(isinstance(source, dict), "INVALID_DEPLETION")
        for lid, quantity in source.items():
            _need(lid in ids, "UNKNOWN_DEPLETION_ID")
            used[lid] = max(used.get(lid, Decimal(0)), _number(quantity, "DEPLETION"))
    liquidity = state.get("liquidity", {})
    _need(isinstance(liquidity, dict), "INVALID_LIQUIDITY_STATE")
    identities = {}
    for ladder, levels in ladders.items():
        for lid, price, quantity in levels:
            identity = [asset, ladder, str(price), str(quantity)]
            identities[lid] = identity
            if lid in liquidity:
                known = liquidity[lid]
                _need(isinstance(known, dict), "INVALID_LIQUIDITY_STATE")
                _need(known["identity"] == identity, "LIQUIDITY_CHANGED_WITHOUT_PROVENANCE")
                used[lid] = max(used.get(lid, Decimal(0)), _number(known["used"], "DEPLETION"))
            _need(used.get(lid, Decimal(0)) <= quantity, "EXCESS_DEPLETION")
    initial_used = used.copy()

    def terminal_state(result, committed_used):
        next_state = deepcopy(state)
        next_state.setdefault("attempts", {})[attempt] = dict(fingerprint=fingerprint, result=deepcopy(result))
        next_state.setdefault("revisions", {})[revision_key] = dict(content=content, used={key:str(q) for key,q in committed_used.items()})
        for lid, identity in identities.items():
            next_state.setdefault("liquidity", {})[lid] = dict(identity=identity, used=str(committed_used.get(lid, Decimal(0))))
        next_state["last_match_ns"] = match
        return next_state

    cash = _number(account["cash"], "CASH")
    positions = account["positions"]
    _need(isinstance(positions, dict) and asset in positions, "MISSING_POSITION_BALANCE")
    holdings = {key: _number(value, "POSITION") for key,value in positions.items()}
    reservations = account["reservations"]
    _need(isinstance(reservations, dict), "MISSING_RESERVATIONS")
    reserved_cash = Decimal(0)
    reserved_positions = {}
    other_cash = Decimal(0)
    other_shares = Decimal(0)
    for key, reservation in reservations.items():
        _identifier(key, "RESERVATION_ID")
        reserve_asset = _identifier(reservation["asset_id"], "RESERVATION_ASSET")
        rc = _number(reservation["cash"], "RESERVED_CASH")
        rq = _number(reservation["quantity"], "RESERVED_QUANTITY")
        status = reservation["status"]
        _need(status in ("RESERVED", "PENDING", "TIMEOUT"), "UNKNOWN_RESERVATION_STATUS")
        if key == attempt:
            _need(status == "RESERVED", "PENDING_ATTEMPT")
            _need(reserve_asset == asset, "RESERVATION_ASSET_MISMATCH")
        else:
            other_cash += rc
            if reserve_asset == asset:
                other_shares += rq
        reserved_cash += rc
        reserved_positions[reserve_asset] = reserved_positions.get(reserve_asset, Decimal(0)) + rq
    _need(reserved_cash <= cash, "RESERVATIONS_EXCEED_CASH")
    _need(all(q <= holdings.get(key, Decimal(0)) for key,q in reserved_positions.items()), "RESERVATIONS_EXCEED_INVENTORY")
    available_cash = cash - other_cash
    available_shares = holdings[asset] - other_shares

    def cost(qty, price):
        notional = qty * price
        fee = _round(qty * rate * price * (1 - price), fee_quantum, rounding)
        share_fee = _round(fee / price, shares_quantum, rounding) if shares_quantum is not None else Decimal(0)
        _need(share_fee <= qty, "FEE_EXCEEDS_FILL")
        _need(side != "SELL" or fee <= notional, "FEE_EXCEEDS_PROCEEDS")
        debit = notional + fee if mode == "CASH_ON_TOP" else notional
        return notional, fee, share_fee, debit

    legs = []
    gross = notional_total = fees = share_fees = debit_total = Decimal(0)
    levels = sorted(ladders["asks" if side == "BUY" else "bids"], key=lambda x:x[1], reverse=side=="SELL")
    for lid, price, quantity in levels:
        if (side == "BUY" and price > limit) or (side == "SELL" and price < limit):
            break
        quantity = min(quantity - used.get(lid, Decimal(0)), requested - gross)
        if side == "SELL":
            quantity = min(quantity, available_shares - gross)
        quantity = _round(quantity, step)
        if quantity <= 0:
            continue
        # Binary search whole quantity steps: rounded fees make algebraic division
        # unsafe near the cash boundary, and FAK must never overdraw reservations.
        if side == "BUY" and cost(quantity, price)[3] > available_cash - debit_total:
            lo, hi = 0, int(quantity / step)
            while lo < hi:
                mid = (lo + hi + 1) // 2
                if cost(mid * step, price)[3] <= available_cash - debit_total:
                    lo = mid
                else:
                    hi = mid - 1
            quantity = lo * step
        if quantity <= 0:
            continue
        notional, fee, share_fee, debit = cost(quantity, price)
        legs.append(dict(liquidity_id=lid, price=_float(price), quantity=_float(quantity),
                         gross_notional=_float(notional), fee_cash=_float(fee), fee_shares=_float(share_fee)))
        used[lid] = used.get(lid, Decimal(0)) + quantity
        gross += quantity; notional_total += notional; fees += fee; share_fees += share_fee; debit_total += debit
        if gross == requested:
            break
    if gross == 0 or (intent["order_type"] == "FOK" and gross != requested):
        result = _result("NO_FILL", "FOK_NOT_FULL" if intent["order_type"] == "FOK" else "NO_EXECUTABLE_LIQUIDITY_OR_BALANCE")
        account["_taker_state"] = terminal_state(result, initial_used)
        return result
    net = gross - share_fees if side == "BUY" else gross
    cash_delta = -debit_total if side == "BUY" else notional_total - fees
    position_delta = net if side == "BUY" else -gross
    result = dict(status="FILLED" if gross == requested else "PARTIAL", reason="SIMULATED_MATCH",
                  filled_quantity=_float(gross), net_quantity=_float(net), vwap=_float(notional_total/gross),
                  gross_notional=_float(notional_total), fee_cash=_float(fees), fee_shares=_float(share_fees),
                  cash_delta=_float(cash_delta), position_delta=_float(position_delta), legs=legs,
                  requested_quantity=_float(requested), match_ns=match, replayed=False)
    next_cash = _float(cash + cash_delta)
    next_position = _float(holdings[asset] + position_delta)
    next_state = terminal_state(result, used)
    # All validation/arithmetic precedes this commit; callers serialize executions.
    account["cash"] = next_cash
    account["positions"][asset] = next_position
    account["reservations"].pop(attempt, None)
    account["_taker_state"] = next_state
    book["_depletion"] = {key:str(q) for key,q in used.items()}
    book["_depletion_revision"] = revision
    return result
