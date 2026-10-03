# Reference taker execution contract

`taker_contract.py` is a stdlib, generic share-sized simulated FAK/FOK matcher with explicit evidence
requirements. It is a reference for candidate adapters, not an exchange emulator,
live-order API, or certification of any strategy. All accompanying matcher tests
are **synthetic semantic cases**, not claimed reconstructions of live fills.

The source-local Tail executor inspected in `tailtaker/exec_exact.py` sweeps four
levels selected by snapped latency, does not retain wallet/reservation or own
depletion state, and stops on invalid ladder entries without marking missing
evidence. Its fees are already computed per level. The separate reported VWAP-fee
defect must not be attributed to this Tail source. The contract keeps all supplied
levels and rejects invalid inputs rather than treating them as observed no-fills.

## API

```python
execute(intent: dict, book: dict, account: dict) -> dict
crypto_hold_ms(venue_ingress_ns, applicable=True) -> int
match_time_ns(decision_ns, *, total_latency_ms=None,
              ingress_latency_ms=None, applicable=True) -> int
```

Callers must serialize `execute` calls sharing a wallet or liquidity state. The
function stages its match and changes book liquidity, balances, inventory and
reservations only when returning a new `FILLED` or `PARTIAL` result. Terminal
`NO_FILL` writes account idempotence/chronology metadata without changing those
economic fields. `UNQUALIFIED` leaves both inputs unchanged. This is in-process
staging, not a durable database transaction.

### Intent fields (all required unless specified)

| Field | Meaning |
|---|---|
| `venue` | Explicit `GENERIC_SHARE_TEST` for synthetic reference semantics, or `POLYMARKET`; other venues are unsupported |
| `sizing_mode` | Only `SHARES` is implemented; spend/budget matching is unsupported |
| `sizing_semantics_established` | Must be literal `True`, backed by venue/order-type-specific sizing evidence; never a default |
| `attempt_id` | Unique nonempty string within the shared wallet state |
| `order_type` | `FAK` or `FOK`; GTC and other types are unqualified |
| `side`, `asset_id` | `BUY` or `SELL`, and exact token/asset string |
| `decision_ns`, `match_ns` | Exact nonnegative integer timestamps in the same venue-aligned clock; match cannot precede decision |
| `max_book_age_ns` | Explicit maximum age of the supplied venue snapshot |
| `quantity`, `quantity_step` | Gross submitted shares, rounded **down** to this market/order-specific quantum |
| `limit_price`, `tick_size` | Binary-outcome prices strictly between zero and one; limit must be on the supplied tick |
| `min_order_size`, `min_notional` | Explicit submission minima; checked against rounded requested shares and requested shares × limit |
| `fee` | Explicit fee policy object described below |

Numeric monetary inputs may be finite numbers, numeric strings, or Decimals.
Boolean, nonfinite, negative and invalid values fail closed. A supplied zero
minimum/rate means the caller has established zero, not that a default was used.
Minimum-order rules apply to submission; a valid FAK match may be smaller than
the submission minimum. The contract currently supports share-sized limit
orders. Spend-sized order construction and matching require an explicit adapter.
**Polymarket BUY FAK/FOK is rejected even when the caller asserts share semantics.**
Captured `misscalc/execution_reference.py` explicitly describes those BUY orders
as spending the signed maker-dollar budget; better prices can return more shares
than the signed taker quantity. One captured order with nominal 5 shares and a
.20 cap returned 33.333334 shares at .03 for approximately $1 principal. The
exact earlier deployed-release join is still unresolved, so this observation
must not be expanded into an unaudited universal historical rule. It does prove
that generic fixed-share arithmetic cannot silently certify these orders.
`GENERIC_SHARE_TEST` marks synthetic semantics and is never a replacement venue
label for real Polymarket data. No spend-sized matcher was added. Real adapters
must preserve signed maker/taker units and establish applicable semantics before
claiming venue compatibility; this reference remains fail-closed for those BUYs.

### Fee policy

`fee = {rate, mode, quantum, rounding, scope}` has no defaults. Supported scope
is `PER_LEG`; rounding is `ROUND_HALF_UP` or `ROUND_DOWN`. Each supplied liquidity
leg uses cash-denominated fee `quantity * rate * price * (1-price)`, rounded to
`quantum`. `CASH_ON_TOP` debits BUY principal plus fee. `SHARES_DEDUCTED` debits
BUY principal and deducts fee shares; that mode additionally requires
`shares_quantum`, using the same rounding rule after converting the rounded cash
fee to shares at the leg price. Both modes deduct SELL fees from cash proceeds.
An unsupported mode, scope, precision or formula is unqualified.

`fee_cash` always reports cash-denominated fee valuation, including when
collected in shares; `cash_delta` reports the actual cash change. For example,
five shares at .20 plus five at .80 at rate .07 produces fee .112 and gross VWAP
.50; calculating the fee at VWAP would produce .175 and is incorrect under this
explicit per-leg policy.

The adapter must establish policy by market and match date. Current generic
documentation does not prove historical collection mode or rounding allocation.
If multiple maker fills are aggregated into one public price level, per-maker
fee-rounding allocation may be unknowable; a price-level replay must not claim
exact fee qualification in that situation.

### Book fields

| Field | Meaning |
|---|---|
| `asset_id`, `revision` | Exact asset and nonempty immutable snapshot revision |
| `clock_basis` | Must equal `venue` |
| `venue_ns` | Snapshot venue timestamp used for freshness |
| `component_max_ns` | Latest timestamp of **any** component used to construct this book |
| `normalized` | Must be literal `True`; adapter certifies complement normalization happened once |
| `asks`, `bids` | Lists of `{liquidity_id, price, quantity}`; known empty lists are allowed |

Both snapshot and latest component must be no later than `match_ns`. Snapshot
age must satisfy `max_book_age_ns`. No receive timestamp or grid point is
silently substituted. The adapter remains responsible for complete causal
construction, clock uncertainty and stale/phantom depth; a scalar maximum cannot
prove the underlying feed was complete or synchronized.

Liquidity IDs must identify stable economic liquidity globally across assets,
revisions and both ladders, including complementary representations. Duplicate
IDs in one book are rejected, not summed or guessed away. Reusing an existing ID
under another asset/side/price is unqualified: the contract does not guess the
necessary cross-asset accounting transformation. The
contract performs one normalized ladder sweep, preserving all supplied levels
and best-price order. It never constructs/sweeps a second complement book.

Confirmed fills store depletion in `book['_depletion']` with
`book['_depletion_revision']`, and persist it in account state by stable global
liquidity ID. Recreating a snapshot or changing its revision cannot restore
depleted size. Different contents under the same revision are unqualified.
Changing the price/quantity/asset/side of a previously known ID is also
unqualified, even in another revision. This small version deliberately has no
implicit replenishment/reset operation. An adapter may introduce a new ID only
when source evidence identifies genuinely new economic liquidity; assigning IDs
by snapshot revision is invalid. Public aggregate L2 changes often cannot
establish this identity, so those dynamic episodes remain unqualified. The
contract does **not** infer replenishment, cancellation, own-footprint removal or
market impact from prices. Fresh book objects must omit old depletion markers.

### Account fields

`account = {asof_ns, cash, positions: {asset_id: quantity}, reservations: {...}}`. The
traded asset requires an explicit position balance, including zero. `cash` and
positions are confirmed **total** balances, before subtracting reservations.
`asof_ns` is a required exact integer clock for the opening/current externally
reconciled balances; it must not be after the proposed match. An internal
`last_match_ns` watermark also rejects backward event processing, so proceeds
from a later SELL cannot fund an earlier BUY. External transfers, settlement and
reconciliation events must preserve this shared chronology; do not clear state
to admit an older order. Equal timestamps are serialized in the caller's known
event order; unknown ordering requires upstream qualification limits.
Every reservation is `{cash, quantity, asset_id, status}` keyed by attempt ID;
status must be `RESERVED`, `PENDING` or `TIMEOUT`. Other attempts' cash/inventory
remain unavailable for all three statuses. Overreserved accounts are unqualified.
An order cannot be resubmitted while its own reservation is PENDING/TIMEOUT.
A known RESERVED reservation is usable by that attempt and is removed only
after a confirmed simulated match. Timeout does not imply cancellation or free
capital. An external lifecycle ledger must resolve unknown posts and late fills.

The matcher writes `_taker_state` after terminal simulated fills and no-fills.
Keep this state with the wallet across strategy calls. A repeated terminal
attempt with identical intent returns the cached result with `replayed=True`,
without balance/depth changes;
reusing that ID with different intent returns `ATTEMPT_ID_COLLISION`. No-fill
attempts cannot become later fills merely because funds or a book changed after
the first call. Never assign a new ID to an unresolved retry merely to bypass
pending protection. The caller still needs a real lifecycle ledger; these cached
receipts describe simulated outcomes only.

FAK takes available price-compliant quantity within cash/inventory bounds and
the supplied quantity step. BUY affordability includes cash fees where relevant.
FOK commits only the entire rounded requested quantity; insufficient liquidity,
cash or inventory rolls back all staged economic changes and caches its terminal
no-fill receipt. Account admission rules
that reject the whole request before matching, allowance checks, settlement,
redemptions, cooldowns and risk limits require an explicit upstream adapter.

### Results and clocks

Results have `status`, `reason`, `filled_quantity` (gross), `net_quantity`,
`vwap` (gross), `gross_notional`, `fee_cash`, `fee_shares`, `cash_delta`,
`position_delta`, `legs`, and `replayed`. Successful matches also carry rounded
`requested_quantity` and `match_ns`. SELL `net_quantity` is gross quantity sold;
`position_delta` carries the negative sign. `NO_FILL` means known inputs admit
no simulated fill or an incomplete FOK; `UNQUALIFIED` means missing, invalid or
unsupported evidence. Neither is an observation of the real venue outcome.

Crypto holds are 250 ms before 2026-08-17 11:00:00 UTC, 50 ms through before
2026-09-04 14:00:00 UTC, then 150 ms. Selection uses **venue ingress** time;
`applicable=False` explicitly disables the crypto rule. With ingress latency,
the helper computes ingress and then adds its dated hold. With total latency,
the helper adds only the provided total, which already includes any hold. Both
budgets or neither budget raise ValueError. Fractional nanoseconds are rejected;
no grid snapping occurs. Public second-resolution timestamps, response RTT and
chain confirmation are not valid substitutes for exact matching timestamps.

## Verification and qualification scope

```bash
python -m unittest discover -s tests -p test_taker_contract.py -v
```

Initial tests were written before the implementation; the first run failed on
the missing module. Later malformed-state and independent-review regressions
were observed failing by assertion, then fixed. Independent review exposed a
revision-reset depletion defect, missing no-fill idempotence, backward account
cash reuse and cross-asset complement reuse; each now has an adversarial case.
The semantic suite covers fee/VWAP separation, precision,
limits, FAK/FOK, inventory/cash reservations, unknown posts, idempotence, duplicate
liquidity, same-revision depletion, missing/stale/future books and exact hold
boundaries. Passing it does not establish source provenance, held-out parity,
fee-policy applicability, or complete strategy/account replay. Qualification
requires those separate source-backed comparisons and coverage denominators.
