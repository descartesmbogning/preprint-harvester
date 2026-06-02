import duckdb
import json
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

con = duckdb.connect()
con.execute("PRAGMA threads=2;")
con.execute("PRAGMA preserve_insertion_order=false;")
con.execute("PRAGMA memory_limit='2GB';")
con.execute("PRAGMA temp_directory='data/tmp_duckdb';")

ROOT = Path.cwd()
if ROOT.name == "notebooks": ROOT = ROOT.parent
BASE = ROOT / "data" / "by_server"

con.execute(f"""
    CREATE OR REPLACE VIEW all_backends AS
    SELECT * FROM read_parquet('{(BASE / "*" / "*.parquet").as_posix()}',
        hive_partitioning=true, union_by_name=true)
""")

# ── Servers to skip ───────────────────────────────────────────────────────────
EXCLUDE_SERVERS = {
    "HAL",
    "SSRN",
    "NutriXiv",
    "PREPRINTS.RU",           # truncated, adjust
    "PoolText",
    "Prepublicaciones OpenCiencia",
    "Qeios",
    "RePec: Research Papers in Economics",
    "Research Square",
    "EconStor Preprints",
    "RePEc: Research Papers in Economics",
    "bioRxiv",
    "medRxiv",
    # add more as needed ↓
}

# ── Helpers ───────────────────────────────────────────────────────────────────
def clean_date(raw_dt: str) -> str:
    if not raw_dt:
        return None
    try:
        return datetime.fromisoformat(
            raw_dt.replace("Z", "+00:00")
        ).strftime("%Y-%m-%d")
    except Exception:
        return raw_dt[:10]

def parse_dates(raw: str):
    try:
        obj = json.loads(raw)
        dates = obj.get("dates", [])
        if not dates:
            return None

        submitted = [
            d["date"] for d in dates
            if d.get("dateType") == "Submitted" and "date" in d
        ]
        date_submitted = clean_date(submitted[0]) if submitted else None
        date_first     = clean_date(dates[0]["date"]) if "date" in dates[0] else None
        raw_dates_str  = json.dumps(dates, ensure_ascii=False)

        return date_submitted, date_first, raw_dates_str
    except Exception:
        return None

# ── Schema ────────────────────────────────────────────────────────────────────
out = ROOT / "data" / "submitted_dates.parquet"
schema = pa.schema([
    ("record_id",      pa.string()),
    ("server_name",    pa.string()),
    ("backend",        pa.string()),
    ("date_submitted", pa.string()),
    ("date_first",     pa.string()),
    ("raw_dates",      pa.string()),
])

# ── Stream per server ─────────────────────────────────────────────────────────
all_servers = con.execute("""
    SELECT DISTINCT CAST(server_name AS VARCHAR)
    FROM all_backends
""").fetchall()

servers = [(srv,) for (srv,) in all_servers if srv not in EXCLUDE_SERVERS]
skipped = len(all_servers) - len(servers)
print(f"Total servers : {len(all_servers)}")
print(f"Skipped       : {skipped}")
print(f"To process    : {len(servers)}\n")

writer = None
total  = 0

for (srv,) in servers:
    print(f"  → {srv}", end=" ... ")

    rows = con.execute("""
        SELECT
            CAST(record_id   AS VARCHAR),
            CAST(server_name AS VARCHAR),
            CAST(backend     AS VARCHAR),
            CAST(raw_json    AS VARCHAR)
        FROM all_backends
        WHERE CAST(server_name AS VARCHAR) = $1
    """, [srv]).fetchall()

    out_rows = {
        "record_id":      [],
        "server_name":    [],
        "backend":        [],
        "date_submitted": [],
        "date_first":     [],
        "raw_dates":      [],
    }

    for record_id, server_name, backend, raw in rows:
        result = parse_dates(raw)
        if result is None:
            continue
        date_submitted, date_first, raw_dates_str = result

        out_rows["record_id"].append(record_id)
        out_rows["server_name"].append(server_name)
        out_rows["backend"].append(backend)
        out_rows["date_submitted"].append(date_submitted)
        out_rows["date_first"].append(date_first)
        out_rows["raw_dates"].append(raw_dates_str)

    if not out_rows["record_id"]:
        print("(no matches)")
        continue

    batch = pa.table(out_rows, schema=schema)
    if writer is None:
        writer = pq.ParquetWriter(str(out), schema, compression="snappy")
    writer.write_table(batch)
    total += len(out_rows["record_id"])
    print(f"{len(out_rows['record_id'])} rows")

if writer:
    writer.close()

print(f"\n✅ Done — {total} total rows → {out}")