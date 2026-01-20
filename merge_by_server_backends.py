#!/usr/bin/env python3
"""
Memory-safe merge + export for huge per-server parquet datasets (50GB+).

Key upgrades for memory:
- Column selection controls (EXCLUDE_COLUMNS / INCLUDE_COLUMNS).
- Export as a PARTITIONED parquet dataset (streaming-safe; avoids LIMIT/OFFSET OOM spikes).
- Merge tables stored as ALL VARCHAR (schema drift safe).

Reads:
  data/by_server/<server_safe>/*_{backend}.parquet

Writes:
  data/all_backends/
    all_{backend}/  (partitioned parquet dataset by server_name by default)
    merge_tmp.duckdb
"""

from __future__ import annotations

import os
import sys
import traceback
from pathlib import Path
from typing import List, Optional, Tuple

import duckdb

# ---------------------------
# CONFIG
# ---------------------------

BY_SERVER_DIR = Path("data/by_server")
OUT_DIR = Path("data/all_backends")
TMP_DIR = Path("data/tmp_duckdb")

BACKENDS = ["crossref", "datacite", "openalex"]
BACKEND_SUFFIX = {
    "crossref": "_crossref.parquet",
    "datacite": "_datacite.parquet",
    "openalex": "_openalex.parquet",
}

# Canonical columns (same across all backends, per your screenshot)
CANONICAL_COLUMNS: List[str] = [
    "record_id",
    "server_name",
    "backend",
    "source_work_id",
    "doi",
    "doi_url",
    "landing_page_url",
    "url_best",
    "prefix",
    "member_id",
    "client_id",
    "provider_id",
    "source_registry",
    "publisher",
    "container_title",
    "institution_name",
    "group_title",
    "issn",
    "title",
    "original_title",
    "short_title",
    "subtitle",
    "language",
    "type_backend_raw",
    "subtype_backend_raw",
    "type_canonical",
    "is_paratext",
    "is_preprint_candidate",
    "date_created",
    "date_posted",
    "date_deposited",
    "date_indexed",
    "date_updated",
    "date_issued",
    "date_registered",
    "date_published",
    "date_published_online",
    "publication_year",
    "date_published_source",
    "date_posted_source",
    "is_oa",
    "oa_status",
    "license",
    "license_url_best",
    "abstract_raw",
    "abstract_text",
    "links_json_best",
    "fulltext_pdf_url",
    "authors_flat",
    "institutions_flat",
    "countries_flat",
    "authors_json",
    "contributors_json",
    "editors_json",
    "funders_json",
    "funders_flat",
    "funders_count",
    "subjects_json",
    "concepts_json",
    "topics_json",
    "cited_by_count",
    "cited_by_count_datacite",
    "cited_by_count_openalex",
    "is_referenced_by_count_crossref",
    "reference_count",
    "references_json",
    "relations_json",
    "has_preprint",
    "is_preprint_of",
    "has_published_version",
    "published_version_ids_json",
    "is_version_of",
    "version_of_ids_json",
    "version_label",
    "has_review",
    "update_to_json",
    "parent_doi",
    "update_policy",
    "rule_tokens",
    "rule_row_id",
    "raw_relationships_json",
    "raw_json",
]

# ---------------------------
# COLUMN SELECTION (NEW)
# ---------------------------
# Choose ONE mode:
#  A) INCLUDE_COLUMNS = [...]   -> export only these columns
#  B) EXCLUDE_COLUMNS = [...]   -> export all canonical minus these
# If both empty, exports ALL columns.

INCLUDE_COLUMNS: List[str] = [
    "record_id",
    "server_name",
    "backend",
    # "source_work_id",
    "doi",
    "doi_url",
    "landing_page_url",
    # "url_best",
    # "prefix",
    # "member_id",
    # "client_id",
    # "provider_id",
    # "source_registry",
    # "publisher",
    # "container_title",
    # "institution_name",
    # "group_title",
    # "issn",
    "title",
    # "original_title",
    # "short_title",
    # "subtitle",
    # "language",
    "type_backend_raw",
    "subtype_backend_raw",
    "type_canonical",
    # "is_paratext",
    "is_preprint_candidate",
    "date_created",
    "date_posted",
    "date_deposited",
    "date_indexed",
    "date_updated",
    "date_issued",
    "date_registered",
    "date_published",
    "date_published_online",
    "publication_year",
    "date_published_source",
    "date_posted_source",
    # "is_oa",
    # "oa_status",
    # "license",
    # "license_url_best",
    # "abstract_raw",
    # "abstract_text",
    "links_json_best",
    # "fulltext_pdf_url",
    # "authors_flat",
    # "institutions_flat",
    # "countries_flat",
    # "authors_json",
    # "contributors_json",
    # "editors_json",
    # "funders_json",
    # "funders_flat",
    # "funders_count",
    # "subjects_json",
    # "concepts_json",
    # "topics_json",
    # "cited_by_count",
    # "cited_by_count_datacite",
    # "cited_by_count_openalex",
    # "is_referenced_by_count_crossref",
    # "reference_count",
    # "references_json",
    "relations_json",
    "has_preprint",
    "is_preprint_of",
    "has_published_version",
    "published_version_ids_json",
    "is_version_of",
    "version_of_ids_json",
    "version_label",
    "has_review",
    "update_to_json",
    "parent_doi",
    # "update_policy",
    # "rule_tokens",
    # "rule_row_id",
    "raw_relationships_json",
    # "raw_json",
    ]  # e.g. ["record_id","server_name","backend","doi","title","publication_year"]

EXCLUDE_COLUMNS: List[str] = [
    # # Heaviest fields first (often huge and not always needed for analytics)
    # # "raw_json",
    # # "raw_relationships_json",
    # "authors_json",
    # "contributors_json",
    # "editors_json",
    # "funders_json",
    # "subjects_json",
    # "concepts_json",
    # "topics_json",
    # "references_json",
    # # "relations_json",
    # "links_json_best",
    # "abstract_raw",
    # # keep abstract_text if you want a cleaner text field; otherwise exclude it too:
    # # "abstract_text",
    # 'fulltext_pdf_url',
    # 'is_oa',
    # 'oa_status',
    # 'date_posted_source',
    # 'date_published_source',
    # 'is_paratext',
    # 'subtitle',
    # 'short_title',
    # 'original_title',
    # 'source_registry',
    # 'url_best',

    # 'rule_row_id',
    # "update_policy",
    # "reference_count",
    # "cited_by_count",
    # "cited_by_count_datacite",
    # "cited_by_count_openalex",
    # "is_referenced_by_count_crossref",
    # "funders_count",
    # "institutions_flat",
]

# ---------------------------
# EXPORT STRATEGY (NEW)
# ---------------------------
# Partitioned export is much more memory-stable than LIMIT/OFFSET chunking.
# Options: None, "server_name", "backend", "publication_year"
EXPORT_PARTITION_BY: Optional[str] = "backend"  # e.g. "server_name"

# Parquet settings
PARQUET_COMPRESSION = os.environ.get("DUCKDB_PARQUET_COMPRESSION", "ZSTD")
PARQUET_ROW_GROUP_SIZE = int(os.environ.get("DUCKDB_PARQUET_ROW_GROUP_SIZE", "10000"))

# Dedupe (optional)
DEDUPE = False
DEDUP_KEY = {
    "crossref": "record_id",
    "datacite": "record_id",
    "openalex": "record_id",
}

# DuckDB resource tuning
MERGE_THREADS = int(os.environ.get("DUCKDB_THREADS", "4"))
MERGE_MEM_LIMIT = os.environ.get("DUCKDB_MEM_LIMIT", "2GB")

EXPORT_THREADS = int(os.environ.get("DUCKDB_EXPORT_THREADS", "1"))
EXPORT_MEM_LIMIT = os.environ.get("DUCKDB_EXPORT_MEM_LIMIT", "768MB")

CHECKPOINT_EVERY_FILES = 50

# ---------------------------
# HELPERS
# ---------------------------

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

def list_backend_files(backend: str) -> List[Tuple[str, Path]]:
    suffix = BACKEND_SUFFIX[backend]
    if not BY_SERVER_DIR.exists():
        raise FileNotFoundError(f"Missing directory: {BY_SERVER_DIR}")

    out: List[Tuple[str, Path]] = []
    for server_dir in sorted(BY_SERVER_DIR.iterdir()):
        if not server_dir.is_dir():
            continue
        server_safe = server_dir.name
        for p in sorted(server_dir.glob(f"*{suffix}")):
            if p.is_file():
                out.append((server_safe, p))
    return out

def open_connection(db_path: Path, threads: int, mem_limit: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=str(db_path))
    con.execute(f"PRAGMA threads={threads};")
    con.execute(f"PRAGMA memory_limit='{mem_limit}';")
    con.execute(f"PRAGMA temp_directory='{str(TMP_DIR)}';")
    con.execute("PRAGMA preserve_insertion_order=false;")
    con.execute("PRAGMA enable_progress_bar=false;")
    return con

def table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    q = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?"
    return con.execute(q, [name]).fetchone()[0] > 0

def log_fail(log_path: Path, parquet_path: Path, errmsg: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(str(parquet_path) + "\n")
        f.write(errmsg.strip() + "\n")
        f.write("-" * 80 + "\n")

def parquet_has_any_row(con: duckdb.DuckDBPyConnection, parquet_path: Path) -> bool:
    try:
        x = con.execute(f"SELECT 1 FROM read_parquet('{str(parquet_path)}') LIMIT 1").fetchone()
        return x is not None
    except Exception:
        # unreadable -> let insert path handle logging
        return True

# ---------------------------
# COLUMN SELECTION LOGIC
# ---------------------------

def resolve_export_columns() -> List[str]:
    canonical = list(CANONICAL_COLUMNS)

    if INCLUDE_COLUMNS:
        keep = []
        missing = []
        for c in INCLUDE_COLUMNS:
            if c in canonical:
                keep.append(c)
            else:
                missing.append(c)
        if missing:
            raise SystemExit(f"[ERROR] INCLUDE_COLUMNS contains unknown columns: {missing}")
        return keep

    if EXCLUDE_COLUMNS:
        unknown = [c for c in EXCLUDE_COLUMNS if c not in canonical]
        if unknown:
            raise SystemExit(f"[ERROR] EXCLUDE_COLUMNS contains unknown columns: {unknown}")
        return [c for c in canonical if c not in set(EXCLUDE_COLUMNS)]

    return canonical

# ---------------------------
# SCHEMA CREATION (ALL VARCHAR)
# ---------------------------

def create_backend_table_all_varchar(con: duckdb.DuckDBPyConnection, backend: str) -> None:
    cols_sql = ",\n  ".join([f"{c} VARCHAR" for c in CANONICAL_COLUMNS])
    con.execute(f"CREATE TABLE {backend} (\n  {cols_sql}\n);")

# ---------------------------
# INSERT (NO t.* ; OVERRIDE backend)
# ---------------------------

def build_insert_select_sql(backend: str, server_safe: str, parquet_path: Path) -> str:
    exprs: List[str] = []
    for col in CANONICAL_COLUMNS:
        if col == "backend":
            exprs.append(f"'{backend}'::VARCHAR AS backend")
        elif col == "server_name":
            exprs.append(f"COALESCE(CAST(t.server_name AS VARCHAR), '{server_safe}') AS server_name")
        else:
            exprs.append(f"CAST(t.{col} AS VARCHAR) AS {col}")

    select_list = ",\n      ".join(exprs)
    return f"""
    INSERT INTO {backend}
    SELECT
      {select_list}
    FROM read_parquet('{str(parquet_path)}', union_by_name=TRUE) AS t
    """

def insert_file(con: duckdb.DuckDBPyConnection, backend: str, server_safe: str, parquet_path: Path) -> None:
    con.execute(build_insert_select_sql(backend, server_safe, parquet_path))

# ---------------------------
# MERGE PER BACKEND
# ---------------------------

def merge_backend(backend: str, con: duckdb.DuckDBPyConnection) -> None:
    files = list_backend_files(backend)
    log_path = OUT_DIR / f"merge_failures_{backend}.log"
    if log_path.exists():
        log_path.unlink()

    print(f"\n[BACKEND] {backend}")
    print(f"  files: {len(files)}")

    if not files:
        return

    if not table_exists(con, backend):
        create_backend_table_all_varchar(con, backend)

    processed = inserted = empty = failed = 0

    for server_safe, p in files:
        processed += 1

        if not parquet_has_any_row(con, p):
            empty += 1
            continue

        try:
            insert_file(con, backend, server_safe, p)
            inserted += 1
        except Exception as e:
            failed += 1
            tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            log_fail(log_path, p, tb)

        if processed % CHECKPOINT_EVERY_FILES == 0:
            con.execute("CHECKPOINT;")
            print(f"  ... processed {processed}/{len(files)} | inserted={inserted} | empty={empty} | failed={failed}")

    con.execute("CHECKPOINT;")
    print(f"  [OK] processed={processed} | inserted={inserted} | empty={empty} | failed={failed}")
    if failed:
        print(f"  [LOG] failures: {log_path}")

# ---------------------------
# EXPORT (PARTITIONED, STREAMING-SAFE)
# ---------------------------

def export_partitioned_parquet_dataset(
    con: duckdb.DuckDBPyConnection,
    table: str,
    out_dir: Path,
    export_cols: List[str],
    partition_by: Optional[str],
) -> None:
    """
    Export as a partitioned parquet dataset:
      COPY (SELECT ...) TO 'out_dir' (FORMAT PARQUET, PARTITION_BY (...))

    This is usually more memory-stable than LIMIT/OFFSET chunking for wide tables.
    """
    ensure_dir(out_dir)

    # Validate partition column
    if partition_by is not None:
        if partition_by not in export_cols:
            raise SystemExit(f"[ERROR] partition column '{partition_by}' must be in exported columns: {export_cols}")

    cols_sql = ", ".join(export_cols)
    partition_sql = f", PARTITION_BY ({partition_by})" if partition_by else ""

    # Important: exporting to a DIRECTORY (not a file)
    con.execute(f"""
        COPY (
            SELECT {cols_sql}
            FROM {table}
        )
        TO '{str(out_dir)}'
        (FORMAT PARQUET,
         COMPRESSION {PARQUET_COMPRESSION},
         ROW_GROUP_SIZE {PARQUET_ROW_GROUP_SIZE}
         {partition_sql});
    """)

# ---------------------------
# OPTIONAL: DEDUPE QUERY
# ---------------------------

def create_dedup_query(con: duckdb.DuckDBPyConnection, backend: str) -> str:
    cols = [r[0] for r in con.execute(f"DESCRIBE {backend}").fetchall()]
    key = DEDUP_KEY.get(backend)

    if key and key in cols:
        # if backend == "openalex":
        #     key_expr = "COALESCE(NULLIF(source_work_id,''), NULLIF(doi,''))" if "doi" in cols else "NULLIF(source_work_id,'')"
        # else:
        #     key_expr = f"NULLIF({key},'')"
        
        key_expr = f"NULLIF({key},'')"

        return f"""
            SELECT * EXCLUDE(rn)
            FROM (
                SELECT *, row_number() OVER (PARTITION BY {key_expr} ORDER BY record_id) AS rn
                FROM {backend}
            )
            WHERE rn = 1
        """

    return f"SELECT * FROM {backend}"

# ---------------------------
# MAIN
# ---------------------------

def main() -> None:
    ensure_dir(OUT_DIR)
    ensure_dir(TMP_DIR)

    if not BY_SERVER_DIR.exists():
        raise SystemExit(f"[ERROR] missing {BY_SERVER_DIR}")

    db_path = OUT_DIR / "merge_tmp.duckdb"

    # reset DB by default (avoids stale schema)
    if os.environ.get("DUCKDB_RESET_DB", "1") == "1" and db_path.exists():
        print(f"[RESET] Removing old DB: {db_path}")
        db_path.unlink()

    # 1) MERGE
    con = open_connection(db_path, threads=MERGE_THREADS, mem_limit=MERGE_MEM_LIMIT)
    for b in BACKENDS:
        merge_backend(b, con)
    con.close()

    # 2) EXPORT
    export_cols = resolve_export_columns()
    print(f"\n[EXPORT] columns={len(export_cols)} (excluded={len(CANONICAL_COLUMNS)-len(export_cols)})")
    if EXPORT_PARTITION_BY:
        print(f"[EXPORT] partition_by={EXPORT_PARTITION_BY}")

    conx = open_connection(db_path, threads=EXPORT_THREADS, mem_limit=EXPORT_MEM_LIMIT)

    for b in BACKENDS:
        if not table_exists(conx, b):
            continue

        out_dataset_dir = OUT_DIR / f"all_{b}"
        print(f"\n[EXPORT DATASET] {b} -> {out_dataset_dir}")
        export_partitioned_parquet_dataset(
            conx, b, out_dataset_dir, export_cols, EXPORT_PARTITION_BY
        )
        print(f"  [DONE] {b} dataset exported.")

        if DEDUPE:
            dq = create_dedup_query(conx, b)
            dedup_out = OUT_DIR / f"all_{b}_dedup"
            print(f"[EXPORT DEDUP DATASET] {b} -> {dedup_out}")
            conx.execute(f"""
                COPY (
                    SELECT {", ".join(export_cols)}
                    FROM ({dq}) q
                )
                TO '{str(dedup_out)}'
                (FORMAT PARQUET,
                 COMPRESSION {PARQUET_COMPRESSION},
                 ROW_GROUP_SIZE {PARQUET_ROW_GROUP_SIZE}
                 {", PARTITION_BY (" + EXPORT_PARTITION_BY + ")" if EXPORT_PARTITION_BY else ""});
            """)
            print(f"  [DONE] {b} dedup dataset exported.")

    conx.close()
    print("\n[DONE] Merge + partitioned export finished successfully.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[STOP] Interrupted by user.")
        sys.exit(130)
