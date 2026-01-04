#!/usr/bin/env python3

from pathlib import Path
import pandas as pd

# ----------------------- CONFIG -----------------------
DATA_DIR = Path("data/all_backends")

all_backends = DATA_DIR / "all_backends_merged_fullschema.parquet" 
CROSSREF = DATA_DIR / "all_crossref_labeled.parquet"  
DATACITE = DATA_DIR / "all_datacite_labeled.parquet"
OPENALEX = DATA_DIR / "all_openalex_labeled.parquet"

SAMPLE_DIR = DATA_DIR / "samples"
SAMPLE_DIR.mkdir(parents=True, exist_ok=True)


def preview(name: str, path: Path):
    print(f"\n\n================ {name.upper()} ================")

    if not path.exists():
        print(f"[ERROR] File not found: {path}")
        return

    # Load only first part
    df = pd.read_parquet(path)

    print(f"[INFO] Loaded: {path}")
    print(f"Shape: {df.shape[0]:,} rows × {df.shape[1]:,} columns")

    # Print list of columns
    print("\nColumns:")
    for col in df.columns:
        print("  -", col)

    # Print head
    print("\nHead sample (5 rows):")
    print(df.head(5))

    # Save a small sample (100 rows)
    sample_path = SAMPLE_DIR / f"{name}_sample.csv"
    df.head(100).to_csv(sample_path, index=False, encoding="utf-8-sig")
    print(f"[Saved] Sample → {sample_path}")


def main():
    preview("all_backends", all_backends)
    preview("crossref", CROSSREF)
    preview("datacite", DATACITE)
    preview("openalex", OPENALEX)


if __name__ == "__main__":
    main()
