#!/usr/bin/env python3
"""
Standalone script to label a merged all_datacite file with a server_name column,
using DataCite client metadata fetched from the DataCite /clients API.

Input:
    data/all_backends/all_datacite.parquet

Output:
    data/all_backends/all_datacite_labeled.parquet
    data/all_backends/all_datacite_labeled.csv
    data/output/clients_catalog.csv   <-- metadata about DataCite clients

Requires: pandas, requests, pyarrow
"""

import time
import json
import requests
import pandas as pd
from pathlib import Path

# ----------------------------- CONFIG -----------------------------
INPUT_FILE  = Path("data/all_backends/all_datacite.parquet")
OUTPUT_FILE = Path("data/all_backends/all_datacite_labeled.parquet")
CATALOG_CSV = Path("data/output/clients_catalog.csv")

MAILTO = "your.real.email@example.com"  # <<-- PUT YOUR EMAIL HERE
DATACITE_CLIENTS = "https://api.datacite.org/clients"


# ------------------------- HTTP UTILITIES -------------------------
def fetch_client_metadata(client_id: str) -> dict:
    """
    Fetch metadata for a DataCite client_id.
    Returns dict with fields: id, name, displayName, raw_json
    """
    url = f"{DATACITE_CLIENTS}/{client_id}"
    headers = {
        "Accept": "application/vnd.api+json",
        "User-Agent": f"DataCite-Labeler (mailto:{MAILTO})"
    }

    try:
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code == 200:
            data = r.json().get("data", {})
            attributes = data.get("attributes", {}) or {}
            return {
                "id": client_id,
                "name": attributes.get("name", ""),
                "displayName": attributes.get("displayName", attributes.get("name", "")),
                "raw_json": json.dumps(data, ensure_ascii=False)
            }
        else:
            # fallback to search endpoint
            r = requests.get(
                DATACITE_CLIENTS,
                params={"query": client_id},
                headers=headers,
                timeout=30,
            )
            arr = r.json().get("data", []) or []
            if arr:
                a = arr[0]
                attributes = a.get("attributes", {}) or {}
                return {
                    "id": client_id,
                    "name": attributes.get("name", ""),
                    "displayName": attributes.get("displayName", attributes.get("name", "")),
                    "raw_json": json.dumps(a, ensure_ascii=False)
                }
    except Exception as e:
        print(f"[WARN] Could not fetch client_id={client_id}: {e}")

    # default fallback
    return {"id": client_id, "name": "", "displayName": "", "raw_json": ""}


# ------------------------- CATALOG HANDLING -----------------------
def load_catalog(path: Path) -> pd.DataFrame:
    if path.exists():
        try:
            return pd.read_csv(path, dtype=str).fillna("")
        except:
            pass
    return pd.DataFrame(columns=["id", "name", "displayName", "raw_json"])


def save_catalog(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df = df.drop_duplicates(subset=["id"])
    df.to_csv(path, index=False, encoding="utf-8")


# ------------------------------ MAIN ------------------------------
def main():
    print("\n[label_all_datacite] Reading:", INPUT_FILE)
    df = pd.read_parquet(INPUT_FILE)

    if "client_id" not in df.columns:
        raise SystemExit("ERROR: all_datacite file has no `client_id` column.")

    # Unique client IDs
    client_ids = sorted(set(df["client_id"].dropna().astype(str)))
    print(f"Found {len(client_ids):,} unique client_id values.")

    # Load existing catalog
    catalog = load_catalog(CATALOG_CSV)
    known_ids = set(catalog["id"].tolist())
    to_fetch = [cid for cid in client_ids if cid not in known_ids]

    print(f"New client_ids to fetch from DataCite: {len(to_fetch):,}")

    # Fetch missing client metadata
    new_rows = []
    for i, cid in enumerate(to_fetch, 1):
        if i % 10 == 0:
            time.sleep(0.2)  # polite pool
        print(f"  [{i}/{len(to_fetch)}] Fetching {cid}...")
        md = fetch_client_metadata(cid)
        new_rows.append(md)

    # Update catalog
    if new_rows:
        catalog = pd.concat([catalog, pd.DataFrame(new_rows)], ignore_index=True)
    save_catalog(catalog, CATALOG_CSV)

    # Build mapping dict
    id_to_name = dict(zip(catalog["id"], catalog["displayName"]))

    # Add server_name
    df["server_name"] = df["client_id"].astype(str).map(id_to_name).fillna("")

    # Save output
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_FILE, index=False)
    df.to_csv(OUTPUT_FILE.with_suffix(".csv"), index=False, encoding="utf-8-sig")

    print(f"\n[label_all_datacite] DONE")
    print(f"Labeled file written to:\n  {OUTPUT_FILE}")
    print(f"Catalog updated at:\n  {CATALOG_CSV}\n")


if __name__ == "__main__":
    main()
