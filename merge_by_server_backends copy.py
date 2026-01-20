#!/usr/bin/env python3
"""
Merge per-server backend files (Crossref / DataCite / OpenAlex)
into one file per backend, using data already stored in data/by_server.

- Expects a directory structure like:
    data/by_server/
        <server_safe_name>/
            <server_safe>_YYYY-MM-DD_YYYY-MM-DD_crossref.parquet
            <server_safe>_YYYY-MM-DD_YYYY-MM-DD_datacite.parquet
            <server_safe>_YYYY-MM-DD_YYYY-MM-DD_openalex.parquet
        ...

- Only reads .parquet files (ignores server-level CSVs to avoid duplicates).
- Adds:
    - `server_safe` column: name of the server folder
    - `backend` column: "crossref", "datacite", or "openalex"

- Outputs merged files in MERGED_OUTPUT_DIR:
    data/all_backends/
        all_crossref.parquet / all_crossref.csv
        all_datacite.parquet / all_datacite.csv
        all_openalex.parquet / all_openalex.csv
"""

import os
import sys
from pathlib import Path
import pandas as pd


# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------

# Root folder where per-server data are stored
BY_SERVER_DIR = Path("data/by_server")  # ← change if your folder is different

# Where to store merged backend files
MERGED_OUTPUT_DIR = Path("data/all_backends")

# File suffixes that identify each backend
BACKEND_SUFFIXES = {
    "crossref": "_crossref.parquet",
    "datacite": "_datacite.parquet",
    "openalex": "_openalex.parquet",
}


# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------

def find_backend_from_name(filename: str) -> str | None:
    """
    Infer backend from filename suffix.
    Example:
      foo_2000-01-01_2025-10-11_crossref.parquet -> "crossref"
    """
    for backend, suffix in BACKEND_SUFFIXES.items():
        if filename.endswith(suffix):
            return backend
    return None


def drop_duplicates_backend(df: pd.DataFrame, backend: str) -> pd.DataFrame:
    """
    Drop duplicates by a sensible key for each backend.
    """
    subset = None

    if backend in ("crossref", "datacite"):
        if "doi" in df.columns:
            subset = ["doi"]
    elif backend == "openalex":
        if "openalex_id" in df.columns:
            subset = ["openalex_id"]
        elif "doi" in df.columns:
            subset = ["doi"]

    if subset:
        df = df.drop_duplicates(subset=subset)
    else:
        df = df.drop_duplicates()

    return df


# -------------------------------------------------------------------
# MAIN MERGE LOGIC
# -------------------------------------------------------------------

def merge_by_server(backends=("crossref", "datacite", "openalex")):
    BY_SERVER_DIR.mkdir(parents=True, exist_ok=True)
    MERGED_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Accumulators per backend
    acc = {b: [] for b in backends}

    if not BY_SERVER_DIR.exists():
        print(f"[ERROR] Directory does not exist: {BY_SERVER_DIR}")
        sys.exit(1)

    # Walk servers
    for server_dir in BY_SERVER_DIR.iterdir():
        if not server_dir.is_dir():
            continue

        server_safe = server_dir.name
        print(f"\n[SERVER] {server_safe}")

        # Scan parquet files inside this server directory
        for parquet_path in server_dir.glob("*.parquet"):
            backend = find_backend_from_name(parquet_path.name)
            if backend is None or backend not in backends:
                # Not a recognized backend file
                continue

            print(f"  - reading {backend} file: {parquet_path}")
            try:
                df = pd.read_parquet(parquet_path)
            except Exception as e:
                print(f"    [WARN] Failed to read {parquet_path}: {e}")
                continue

            if df.empty:
                print("    [INFO] File is empty; skipping.")
                continue

            # Tag server & backend
            df = df.copy()
            df["server_safe"] = server_safe
            df["backend"] = backend

            acc[backend].append(df)

    # After scan: merge & save per backend
    for backend in backends:
        dfs = acc[backend]
        if not dfs:
            print(f"\n[INFO] No data found for backend: {backend}")
            continue

        print(f"\n[MERGE] Concatenating {len(dfs)} chunks for backend: {backend}")
        merged = pd.concat(dfs, ignore_index=True)

        # Drop duplicates
        merged = drop_duplicates_backend(merged, backend)

        # Save
        base = f"all_{backend}"
        parquet_out = MERGED_OUTPUT_DIR / f"{base}.parquet"
        csv_out = MERGED_OUTPUT_DIR / f"{base}.csv"

        merged.to_parquet(parquet_out, index=False)
        merged.to_csv(csv_out, index=False, encoding="utf-8-sig")

        print(f"[DONE] {backend} merged:")
        print(f"   rows: {len(merged)}")
        print(f"   parquet: {parquet_out}")
        print(f"   csv:     {csv_out}")


if __name__ == "__main__":
    merge_by_server()
