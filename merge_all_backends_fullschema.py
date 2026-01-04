#!/usr/bin/env python3
"""
Merge Crossref, DataCite and OpenAlex preprint data into a unified table
WITHOUT losing any backend-specific columns you care about, but dropping
heavy / unused JSON columns and harmonising some names.

We keep ALL remaining original columns from each backend.
We only ADD harmonised helpers:

    - backend          ('crossref' | 'datacite' | 'openalex')
    - server_name      (from backend-specific fields / labelers)
    - server_id        (client_id / prefix / source_id, depending on backend)
    - created_date     (harmonized)
    - registered_date  (harmonized)
    - posted_date      (harmonized)
    - publication_date (harmonized)
    - publication_year (harmonized)
    - primary_url      (best landing page)
    - is_oa, oa_status (from OpenAlex, NA elsewhere)

Inputs (adjust paths as needed):

    data/all_backends/all_crossref_labeled.parquet
    data/all_backends/all_datacite_labeled.parquet
    data/all_backends/all_openalex_labeled.parquet

Outputs:

    data/all_backends/all_backends_merged_fullschema.parquet
    data/all_backends/all_backends_merged_fullschema.csv
"""

from pathlib import Path
import pandas as pd
import numpy as np

# ----------------------------- CONFIG -----------------------------
DATA_DIR      = Path("data/all_backends")
OUT_PARQUET   = DATA_DIR / "all_backends_merged_fullschema.parquet"
OUT_CSV       = DATA_DIR / "all_backends_merged_fullschema.csv"

CROSSREF_FILE = DATA_DIR / "all_crossref_labeled.parquet"
DATACITE_FILE = DATA_DIR / "all_datacite_labeled.parquet"
OPENALEX_FILE = DATA_DIR / "all_openalex_labeled.parquet"

# Columns we want to guarantee exist in the final DF
CANONICAL_COLS = [
    "doi",
    "backend",
    "server_name",
    "server_id",
    "title",
    "publisher",
    "language",
    "created_date",
    "registered_date",
    "posted_date",
    "publication_date",
    "publication_year",
    "primary_url",
    "is_oa",
    "oa_status",
]


def _ensure_columns(df: pd.DataFrame, cols) -> pd.DataFrame:
    """Add missing columns filled with NA, without dropping anything."""
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df


# -------------------------- PREPARE CROSSREF ----------------------
def prepare_crossref(path: Path) -> pd.DataFrame:
    print(f"[Crossref] Loading {path}")
    df = pd.read_parquet(path)

    # Drop unwanted heavy / debug columns
    crossref_drop_cols = [
        "primary_url",
        "original_title",
        "short_title",
        "subtitle",
        "container_title",
        "short_container_title",
        "authors",
        "editors_json",
        "translators_json",
        "chairs_json",
        "contributors_json",
        "license_url",
        "links_json",
        "subjects",
        "issn_json",
        "issn_type_json",
        "isbn_type_json",
        "alternative_id_json",
        "reference_count",
        "is_referenced_by_count",
        "references_json",
        "update_type",
        "update_policy",
        "update_to_json",
        "archive_json",
        "content_domain_json",
        "assertion_json",
        "source",
        "score",
        "abstract_raw",
        "server_safe",
        "type",
    ]
    df = df.drop(columns=[c for c in crossref_drop_cols if c in df.columns], errors="ignore")

    # Backend flag
    df["backend"] = "crossref"

    # Rename year -> publication_year if requested
    if "year" in df.columns and "publication_year" not in df.columns:
        df = df.rename(columns={"year": "publication_year"})

    # server_name: should have been added during labeling step
    if "server_name" not in df.columns:
        df["server_name"] = pd.NA

    # server_id: use prefix (or member if you prefer)
    if "server_id" not in df.columns:
        df["server_id"] = df.get("prefix")

    # type: use prefix (or member if you prefer)
    if "type" not in df.columns:
        df["type"] = df.get("subtype")

    # Harmonized dates (we do not remove any existing date columns)
    if "created_date" not in df.columns:
        df["created_date"] = df.get("created_date")  # if present
    if "posted_date" not in df.columns:
        df["posted_date"] = df.get("posted_date")

    if "publication_date" not in df.columns:
        # often something like 'issued_date' from flattening
        df["publication_date"] = df.get("issued_date")

    # Publication year helper – if not already set via rename above
    if "publication_year" not in df.columns:
        df["publication_year"] = (
            df["publication_date"]
            .astype("string")
            .str.slice(0, 4)
        )

    # Crossref doesn't really have a "registered" date in same sense
    if "registered_date" not in df.columns:
        df["registered_date"] = pd.NA

    # Primary URL helper – Crossref raw primary_url was dropped,
    # so just fall back to 'url'.
    if "primary_url" not in df.columns:
        df["primary_url"] = df.get("url")

    # OA info not available directly from Crossref
    if "is_oa" not in df.columns:
        df["is_oa"] = pd.NA
    if "oa_status" not in df.columns:
        df["oa_status"] = pd.NA

    df = _ensure_columns(df, CANONICAL_COLS)
    return df


# -------------------------- PREPARE DATACITE ----------------------
def prepare_datacite(path: Path) -> pd.DataFrame:
    print(f"[DataCite] Loading {path}")
    df = pd.read_parquet(path)

    # Rename columns BEFORE harmonisation
    datacite_rename_map = {
        "creators_json":         "authorships_json",
        "created":               "created_date",
        "funding_refs_json":     "funder_json",
        "rights_list_json":      "licenses_json",
        "resource_type_general": "subtype",
        "resource_type":         "type",
    }
    # Only rename those that actually exist
    rename_subset = {k: v for k, v in datacite_rename_map.items() if k in df.columns}
    if rename_subset:
        df = df.rename(columns=rename_subset)

    # Drop unwanted heavy / debug columns
    datacite_drop_cols = [
        "titles_json",
        "sizes_json",
        "identifiers_json",
        "references_json",
        "citations_json",
        "schema_version",
        "state",
        "types_json",
        "descriptions_json",
        "alternate_ids_json",
        "related_ids_json",
        "container_json",
        "formats_json",
        "geo_locations_json",
        "url_alternate_json",
        "raw_attributes_json",
        "server_safe",
    ]
    df = df.drop(columns=[c for c in datacite_drop_cols if c in df.columns], errors="ignore")

    df["backend"] = "datacite"

    # server_name should already be present after labeling
    if "server_name" not in df.columns:
        df["server_name"] = pd.NA

    # server_id from client_id (but keep client_id itself as-is)
    if "server_id" not in df.columns:
        df["server_id"] = df.get("client_id")

    # Harmonized dates – we also keep original created/registered
    if "created_date" not in df.columns:
        # maybe the original 'created' still exists if not renamed
        df["created_date"] = df.get("created")
    if "registered_date" not in df.columns:
        df["registered_date"] = df.get("registered")
    if "posted_date" not in df.columns:
        df["posted_date"] = pd.NA  # not a native field in DataCite

    if "publication_date" not in df.columns:
        df["publication_date"] = df.get("published")

    # Publication year helper
    if "publication_year" not in df.columns:
        if "published_year" in df.columns:
            df["publication_year"] = df["published_year"].astype("string")
        else:
            df["publication_year"] = (
                df["publication_date"]
                .astype("string")
                .str.slice(0, 4)
            )

    # Primary URL helper
    if "primary_url" not in df.columns:
        df["primary_url"] = df.get("url")

    # OA info not coming from DataCite directly
    if "is_oa" not in df.columns:
        df["is_oa"] = pd.NA
    if "oa_status" not in df.columns:
        df["oa_status"] = pd.NA

    df = _ensure_columns(df, CANONICAL_COLS)
    return df


# -------------------------- PREPARE OPENALEX ----------------------
def prepare_openalex(path: Path) -> pd.DataFrame:
    print(f"[OpenAlex] Loading {path}")
    df = pd.read_parquet(path)

    df["backend"] = "openalex"

    # --- Renames requested ---
    if "authorships_json" in df.columns and "authors_json" not in df.columns:
        df = df.rename(columns={"authorships_json": "authors_json"})

    # --- First derive canonical helpers from raw OpenAlex fields ---
    # server_name from primary_location_source_display_name
    if "server_name" not in df.columns:
        if "primary_location_source_display_name" in df.columns:
            df["server_name"] = df["primary_location_source_display_name"]
        else:
            df["server_name"] = pd.NA

    # server_id from primary_location_source_id
    if "server_id" not in df.columns:
        df["server_id"] = df.get("primary_location_source_id")

    # Harmonized dates (OpenAlex doesn't really expose all of these as we need)
    if "created_date" not in df.columns:
        df["created_date"] = pd.NA
    if "registered_date" not in df.columns:
        df["registered_date"] = pd.NA
    if "posted_date" not in df.columns:
        df["posted_date"] = pd.NA

    if "publication_date" not in df.columns:
        df["publication_date"] = df.get("publication_date")  # usually already present

    if "publication_year" not in df.columns:
        if "publication_year" in df.columns:
            df["publication_year"] = df["publication_year"].astype("string")
        else:
            df["publication_year"] = (
                df["publication_date"]
                .astype("string")
                .str.slice(0, 4)
            )

    # Primary URL helper
    if "primary_url" not in df.columns:
        if "primary_location_landing_page_url" in df.columns:
            df["primary_url"] = df["primary_location_landing_page_url"]
        else:
            df["primary_url"] = pd.NA

    # OA info – use OpenAlex primary_location fields when available
    if "is_oa" not in df.columns:
        if "primary_location_is_oa" in df.columns:
            df["is_oa"] = df["primary_location_is_oa"]
        else:
            df["is_oa"] = pd.NA
    if "oa_status" not in df.columns:
        if "primary_location_oa_status" in df.columns:
            df["oa_status"] = df["primary_location_oa_status"]
        else:
            df["oa_status"] = pd.NA

    # Now drop the heavy OpenAlex raw / helper columns
    openalex_drop_cols = [
        "primary_location_landing_page_url",
        "primary_location_source_id",
        "primary_location_source_display_name",
        "raw_openalex_json",
        "server_safe",
        "server_id_source",   # just in case, some variants
        "server_name_source",
        "cited_by_count",
        "primary_location_is_oa",
        "primary_location_oa_status",
    ]
    df = df.drop(columns=[c for c in openalex_drop_cols if c in df.columns], errors="ignore")

    df = _ensure_columns(df, CANONICAL_COLS)
    return df


# ------------------------------- MAIN ------------------------------
def main():
    frames = []

    if CROSSREF_FILE.exists():
        frames.append(prepare_crossref(CROSSREF_FILE))
    else:
        print(f"[WARN] Crossref file not found: {CROSSREF_FILE}")

    if DATACITE_FILE.exists():
        frames.append(prepare_datacite(DATACITE_FILE))
    else:
        print(f"[WARN] DataCite file not found: {DATACITE_FILE}")

    if OPENALEX_FILE.exists():
        frames.append(prepare_openalex(OPENALEX_FILE))
    else:
        print(f"[WARN] OpenAlex file not found: {OPENALEX_FILE}")

    if not frames:
        raise SystemExit("No backend files found. Nothing to merge.")

    # Concatenate with sort=False → union of all columns, keeping them as-is
    df_merged = pd.concat(frames, ignore_index=True, sort=False)

    # Optional: drop exact duplicates by (backend, doi)
    if "doi" in df_merged.columns:
        df_merged.drop_duplicates(subset=["backend", "doi"], inplace=True)

    # 🔹 FIX: enforce a consistent dtype for publication_year
    if "publication_year" in df_merged.columns:
        df_merged["publication_year"] = (
            pd.to_numeric(df_merged["publication_year"], errors="coerce")
              .astype("Int64")   # nullable integer type
        )

    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    df_merged.to_parquet(OUT_PARQUET, index=False)
    df_merged.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    print(f"\nMerged shape: {df_merged.shape}")
    print(f"Written:\n  {OUT_PARQUET}\n  {OUT_CSV}")


if __name__ == "__main__":
    main()
