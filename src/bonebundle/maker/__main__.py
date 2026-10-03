"""Run a declared fixed-order CSV on portable market cache shards."""
import argparse
from pathlib import Path
import pandas as pd

from .api import MODES, MakerOrder, load_market, replay_order


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", default="data/poly")
    parser.add_argument("--orders", required=True, help="CSV: market_key,contract_start_ms and MakerOrder fields")
    parser.add_argument("--own-steps", help="optional parquet: contract_start_ms,outcome,limit,venue_ms,delta; exact historical wallet only")
    parser.add_argument("--modes", default="eng,eng_ftfix", help="comma separated: " + ",".join(MODES))
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    frame = pd.read_csv(args.orders)
    steps = pd.read_parquet(args.own_steps) if args.own_steps else None
    rows = []
    for (market, start), group in frame.groupby(["market_key", "contract_start_ms"]):
        depth, trades = load_market(args.data_root, market, int(start))
        for values in group.to_dict("records"):
            cancel = values.get("cancel_venue_ms")
            foot = None
            if pd.notna(values.get("footprint_seq")) and pd.notna(values.get("footprint_venue_ms")):
                foot = (float(values["footprint_venue_ms"]), int(values["footprint_seq"]))
            order = MakerOrder(outcome=int(values["outcome"]), price_micros=int(values["price_micros"]), quantity=float(values["quantity"]),
                               place_venue_ms=float(values["place_venue_ms"]), cutoff_venue_ms=float(values["cutoff_venue_ms"]),
                               cancel_venue_ms=float(cancel) if pd.notna(cancel) else None, footprint=foot,
                               order_id=str(values.get("order_id", "")))
            own = None
            if steps is not None:
                side = "UP" if order.outcome == 0 else "DOWN"
                own = steps[(steps.contract_start_ms == int(start)) & (steps.outcome == side) & (steps.limit.round(6) == order.price_micros / 1_000_000)]
            for mode in args.modes.split(","):
                row = replay_order(depth, trades, order, mode=mode, own_steps=own).to_dict()
                row.update(market_key=market, contract_start_ms=int(start))
                rows.append(row)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    result = pd.DataFrame(rows)
    if out.suffix == ".parquet":
        result.to_parquet(out, index=False)
    else:
        result.to_csv(out, index=False)
    print(f"wrote {len(rows)} declared order/mode rows to {out}")


if __name__ == "__main__":
    main()
