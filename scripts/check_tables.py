"""Read marketrec tables with pyarrow, independently of the Rust code that wrote them.

Checks every day directory under a symbol directory: each table's parts are readable, have
the columns SCHEMA.md lists, carry the contract version, and add up to the row counts in
manifest.json. Exits with 1 on the first disagreement.

    python3 scripts/check_tables.py data/binance/BTCUSDT
"""

import json
import sys
from pathlib import Path

import pyarrow.parquet as pq

COLUMNS = {
    "depth_diff": ["recv_ts", "event_ts", "first_update_id", "final_update_id", "side", "price", "qty", "source"],
    "book_ticker": ["recv_ts", "update_id", "bid_px", "bid_qty", "ask_px", "ask_qty", "source"],
    "trades": ["recv_ts", "event_ts", "trade_ts", "trade_id", "price", "qty", "is_buyer_maker", "source"],
    "book_l20": ["recv_ts", "last_update_id", "bid_px", "bid_qty", "ask_px", "ask_qty", "source"],
    "quality": ["ts", "kind", "detail", "source"],
}


def check_day(day: Path) -> str:
    m = json.loads((day / "manifest.json").read_text())
    counts = []
    for table, columns in COLUMNS.items():
        rows = 0
        for rel in m["files"].get(table, []):
            path = day / rel
            meta = pq.read_metadata(path).metadata
            version = int(meta[b"marketrec.schema_version"])
            if version != m["schema_version"]:
                raise SystemExit(f"{path}: schema version {version}, manifest {m['schema_version']}")
            t = pq.read_table(path)
            if t.column_names != columns:
                raise SystemExit(f"{path}: columns {t.column_names}")
            rows += t.num_rows
        listed = sorted(str(p.relative_to(day)) for p in (day / table).glob("part-*.parquet"))
        if listed != sorted(m["files"].get(table, [])):
            raise SystemExit(f"{day / table}: parts {listed} are not the manifest's")
        if rows != m["rows"][table]:
            raise SystemExit(f"{day / table}: {rows} rows, manifest says {m['rows'][table]}")
        counts.append(f"{table} {rows}")
    return f"{day.name}: " + ", ".join(counts)


def main() -> None:
    root = Path(sys.argv[1])
    days = sorted(p.parent for p in root.glob("*/manifest.json"))
    if not days:
        raise SystemExit(f"no day directories in {root}")
    for day in days:
        print(check_day(day))


if __name__ == "__main__":
    main()
