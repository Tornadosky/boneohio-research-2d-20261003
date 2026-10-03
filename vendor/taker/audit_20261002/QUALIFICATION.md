# Taker qualification gate

`qualification.py` is an offline, stdlib evidence gate for **one named validation
cohort**, wallet, deployment, engine/config pair and dated venue regime. It is not
an exchange simulator and is not a certification of future strategy performance.
It does not submit orders, use credentials, or change a source ledger.

```bash
PYTHONPATH=. python -m unittest discover -s tests -p test_qualification.py -v
python qualification.py audited_dossier.json --out qualification_result.json
```

The CLI exits zero only for `QUALIFIED_WITHIN_TOLERANCE`; both `FAIL` and
`UNQUALIFIED` exit one. `qualify(dossier)` exposes the same pure-function API.
The executable, entirely synthetic input example is `fixture()` in
`tests/test_qualification.py`. Its passing result is a schema/semantic test,
**not live evidence**. No recovered strategy has been passed through this gate
with a complete qualifying live dossier in this experiment.

## Verdicts

| Verdict | Meaning |
|---|---|
| `QUALIFIED_WITHIN_TOLERANCE` | Every required attestation, membership/clock check and supplied numerical comparison passes for this cohort. |
| `FAIL` | At least one known terminal-status, signed-intent, fill/no-fill or numerical counterexample. Missing evidence is also retained. |
| `UNQUALIFIED` | An input, proof, complete denominator, terminal outcome, or causal/account prerequisite is missing or inconsistent, with no known numerical counterexample taking precedence. |

The output always includes `universal_identity: false`, scoped identifiers,
attempt verdicts, all failure/unknown paths and the declared tolerances. A
per-attempt `WITHIN_TOLERANCE` result does not override a missing global
population or accounting prerequisite. Passing a filled-only API sample is
forbidden, and an empty cohort cannot pass vacuously.

## Input contract

Every source reference has a nonempty `path` and a 64-digit hexadecimal
`sha256`. These identify the immutable audited source and must not refer to
credentials. The gate validates references **structurally**; it does not open or
hash those files. Use the archive verifier and source-specific audits to prove
content, identities and completeness before establishing an attestation.

| Top-level field | Required content |
|---|---|
| `scope` | `venue` (`GENERIC_SHARE_TEST` or `POLYMARKET`), `family`, `wallet`, `deployment_id`, actual `engine_sha256` and `config_sha256`; half-open decision window `start_ns/end_ns`; `regime` containing `id/start_ns/end_ns` for venue ingress. |
| `cohort` | `id`, `kind: validation`, `declared_before_comparison: true`, `prior_label_exposure: none` or `prior_analysis`, explicit `development_attempt_ids`, boolean `held_out_family`. Validation IDs cannot overlap development IDs. |
| `population` | `exhaustive: true`, nonempty unique `attempt_ids` and source references proving this denominator. Membership must exactly equal the attempt rows, including rejects, no-fills, timeouts and not-sent intents. An unresolved timeout remains a row and blocks qualification. |
| `evidence` | Each prerequisite below has `established: true` plus nonempty `sources`. Missing is unknown, never an inferred default. |
| `attempts` | Actual/replayed intent, request size, source, causal clocks, and observed/predicted terminal outcomes described below. |
| `wallet_checkpoints` | Chronologically ordered, source-backed observed/predicted wallet states, beginning with `OPENING` and ending with `CLOSING`. The complete event stream must include reservations, transfers, settlements and redemptions relevant to reusable capital. |

The twelve required evidence attestations are:

- `deployment_wallet_regime`: dated source/config/mode and wallet/family attribution.
- `sizing_semantics`: signed maker/taker units and venue/order-type-specific share versus principal semantics.
- `signed_intents`: exact placed terms, including sizing mode and signed cap.
- `terminal_lifecycle`: complete posting, retries, cancellation and eventual result.
- `identified_taker_legs`: actual economic leg identities and authenticated/receipt role allocation; neither BUY nor FAK establishes taker role.
- `historical_fee_policy`: market/date, collection currency, precision and per-leg allocation.
- `causal_venue_depth`: complete causal depth, complement normalization, depletion and replenishment evidence.
- `venue_clock_alignment`: component clocks, offsets and timing identification; public seconds and response RTT are insufficient.
- `complete_wallet_events`: ordered balances, inventory, transfers, fees, settlement and redemption history for the shared wallet.
- `reservations_allowance_and_risk`: pending capital, unknown posts, inventory reservations, allowance and strategy risk constraints.
- `strategy_config_signal_replay`: independent end-to-end replay with the deployed signal source, scan phase, config and complete attempt population.
- `fixed_latency_policy`: a predeclared dated latency policy; per-order fitting to the live answer is disallowed.

An attestation is an audit assertion, not something this module can infer from a
boolean. A fabricated or incorrectly audited assertion can fool any such gate.
Preserve its evidence and reviewer, and publish unknowns instead of setting a
checkbox to make the result green.

## Attempt semantics

`intent` and `replayed_intent` require `order_type`, `side`, `asset_id`,
`sizing_mode`, `quantity`, `limit_price`, `tick_size`, `quantity_step`,
`fee_policy_id` and `signed_payload_sha256`. Numeric formatting differences do
not change semantics; actual numeric differences do. USD sizing additionally
requires explicit matching `principal`. `requested_quantity` supplies a proven
share denominator; signed SHARES quantity must respect its explicit quantity step, and it is not guessed by dividing principal by the signed cap.
For share sizing it must equal the captured requested quantity. This conservative
version requires a captured signed payload even for not-sent records; earlier
pre-sign aborts remain unqualified until an upstream intent-provenance adapter
establishes equivalent semantics. Do not invent a signature to fill this gap.

Each `clock` requires exact integer `decision_ns`, `venue_ingress_ns`,
`match_ns`, `book_ns`, `component_max_ns`, `max_book_age_ns`, and
`account_asof_ns`. The book and every contributing component must precede match,
the book must meet the explicit age budget, and usable account state cannot come
from after decision. For an outcome without an identifiable venue evaluation
clock, leave the prerequisite unknown; never label a cancellation/response time
as match time. This deliberately leaves many historically rejected or timed-out
rows unqualified. It is not permissible to exclude them to pass the cohort.

Each observed/predicted outcome contains terminal `status` (`FILLED`, `PARTIAL`,
`NO_FILL`, `REJECTED`, `NOT_SENT`), explicit `gross_quantity`, `net_quantity`,
`vwap`, `fee_cash`, `cash_delta`, and a `legs` array. Zero quantity requires null
VWAP, zero net quantity/fee/cash delta and an explicit empty leg array. Nontrade
cash movements belong in a separate audited accounting bridge. Filled outcomes require individually
preserved economic legs with gross/net quantity, price, cash fee valuation and
cash delta; observed legs additionally require globally unique `leg_id` and
`source`. Construct leg IDs from actual trade/allocation or transaction/log-index
identities, not `(order, second, price, quantity)`. That tuple destroys real
same-valued legs in the recovered q99 control.

Leg totals and VWAP must agree with the aggregate outcome. Comparison preserves
leg sequence and partition. A differing partition is unqualified until a
source-backed allocation bridge is available; agreeing VWAP alone does not
qualify fee or depth allocation. Leg/order quantities allow
`max(0.01 shares, 1% requested)`, price allows one recorded tick, and fee/cash
allow $0.01. A positive-versus-zero fill discrepancy **always fails**, however
small. Terminal-status and forced-intent differences also fail.

`POLYMARKET` BUY FAK/FOK cannot qualify with `sizing_mode: SHARES`, regardless
of attestation flags. Captured execution source and an actual improved-price
budget fill show why nominal signed taker shares are not a fixed share ceiling.
USD/principal comparison in this gate is only dossier validation; it does not
implement spend-sized matching or make the generic reference matcher compatible.
The synthetic venue label must never be applied to real venue evidence. Each
predicted BUY leg above its signed cap, or SELL leg below its signed floor, fails;
an observed leg contradicting attributed signed terms is unqualified.

The gate recognizes GTC/GTD labels so a future audited lifecycle simulator can
supply them; recognition is not support by `taker_contract.py`, whose matcher
currently supports only FAK/FOK. Supplying a GTC order as FAK fails this gate.

## Wallet checkpoints and remaining limits

Each checkpoint contains unique `event_id`, exact `timestamp_ns`, `kind`,
`source`, `observed` and `predicted`. Each state requires nonnegative total
`cash`, `reserved_cash`, `positions` and `reserved_positions`. Every traded
asset needs an explicit position, including zero; reservations need the same
asset universe and cannot exceed total cash/inventory. Compare total, reserved
and available cash within $0.01 and position/reserved shares within 0.01 share.
Opening state must precede the first decision; closing state must follow the
last outcome. Equal physical timestamps require a future explicit event-order
adapter and currently remain unqualified. Never increment a timestamp to invent
ordering.

The complete wallet-event attestation must be backed by the full cash-flow and
inventory reconciliation, not only two convenient snapshots. This module
compares supplied checkpoints; it does not itself reconstruct receipt cash flows
or prove that omitted intraperiod reservations did not exist. It also does not
recompute strategy signals, recover missing venue clocks, authenticate API
history, verify fee schedules, or create held-out data. Those remain distinct
upstream audit/replay responsibilities. Existing prior-analysis validation is
reported as such and is not pristine unseen data.

The synthetic regression suite covers missing fees/denominators, unknown posts,
future book components and account balances, cash and inventory reservations,
signed caps/order types/sizing, duplicate fill identities, held-out overlap,
nonfinite inputs, omitted asset balances, known no-fill outcomes, mismatched leg
partitions, and tiny false fills. Initial missing-module failure and subsequent
omitted-inventory/numeric-format/reservation failures were observed before fixes.

Independent review additionally reproduced zero-fill inventory/fee/cash creation, Boolean USD principal, signed-limit violations and off-step signed share quantity. New regressions failed before the fixes and now pass. The explicit venue/sizing scope guard also failed before its implementation.
