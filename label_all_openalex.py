#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

IN_PATH  = Path("data/all_backends/all_openalex.parquet")
OUT_PATH = Path("data/all_backends/all_openalex_labeled.parquet")

def main():
    df = pd.read_parquet(IN_PATH)

    # Create server columns if not present
    if "server_id" not in df.columns:
        df["server_id"] = df.get("primary_location_source_id")
    if "server_name" not in df.columns:
        df["server_name"] = df.get("primary_location_source_display_name")

    # Optional: keep a flag of where the name comes from
    if "server_name_source" not in df.columns:
        df["server_name_source"] = "openalex_source_display_name"

    # Optional: enforce backend column
    if "backend" not in df.columns:
        df["backend"] = "openalex"

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)
    df.to_csv(OUT_PATH.with_suffix(".csv"), index=False, encoding="utf-8-sig")
    print(f"Saved labeled OpenAlex to: {OUT_PATH}")

if __name__ == "__main__":
    main()
