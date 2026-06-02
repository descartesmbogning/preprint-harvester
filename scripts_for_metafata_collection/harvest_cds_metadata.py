from __future__ import annotations
from tqdm.auto import tqdm

import json
import re
from pathlib import Path
from typing import Any
import time
import traceback
import pandas as pd
import gc
from sickle import Sickle

pd.set_option('display.max_columns', None)

CDS_OAI_ENDPOINT = "https://cds.cern.ch/oai2d"


BIG_CANON_COLS = [
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


DOI_RE = re.compile(r"(10\.\d{4,9}/[-._;()/:A-Z0-9]+)", re.I)


def _json(obj: Any) -> str | None:
    if obj is None:
        return None
    try:
        return json.dumps(obj, ensure_ascii=False, sort_keys=True)
    except Exception:
        return None


def _norm_doi(doi: str | None) -> str | None:
    if not doi:
        return None
    doi = str(doi).strip()
    doi = doi.replace("https://doi.org/", "").replace("http://doi.org/", "")
    doi = doi.replace("doi:", "").strip()
    return doi.lower() if doi else None


def _doi_url(doi: str | None) -> str | None:
    return f"https://doi.org/{doi}" if doi else None


def _derive_prefix_from_doi(doi: str | None) -> str | None:
    if not doi or "/" not in doi:
        return None
    return doi.split("/", 1)[0]


def _year_from_date(d: str | None) -> int | None:
    if not d:
        return None
    try:
        return int(str(d)[:4])
    except Exception:
        return None


def _safe_server_dir_name(name: str) -> str:
    name = str(name).strip()
    name = name.replace(" ", "_").replace("/", "_")
    name = re.sub(r'[<>:"\\|?*]', "_", name)
    name = re.sub(r"\.{3,}", "", name)
    name = re.sub(r"_+", "_", name)
    return (name or "server")[:50]


def normalize_list(x: Any) -> list[str]:
    if x is None:
        return []
    if isinstance(x, list):
        return [str(v).strip() for v in x if v is not None and str(v).strip()]
    return [str(x).strip()] if str(x).strip() else []


def extract_doi(identifiers: list[str] | None) -> str | None:
    if not identifiers:
        return None

    for value in identifiers:
        if not value:
            continue
        match = DOI_RE.search(str(value))
        if match:
            return _norm_doi(match.group(1))

    return None


def extract_best_url(identifiers: list[str] | None, doi: str | None) -> str | None:
    if identifiers:
        for value in identifiers:
            text = str(value).strip()
            if text.startswith(("http://", "https://")) and "doi.org/" not in text.lower():
                return text

        for value in identifiers:
            text = str(value).strip()
            if text.startswith(("http://", "https://")):
                return text

    return _doi_url(doi)


def authors_flat_from_list(authors: list[str]) -> str | None:
    authors = [a.strip() for a in authors if a and str(a).strip()]
    return "; ".join(authors) if authors else None

def _year_from_date(d: str | None) -> int | None:
    """
    Extract year from messy CDS OAI date values.

    Examples:
    - 2002
    - 2002-05-01
    - 2002-05
    - 2002/05/01
    - 2002-05-01T00:00:00Z
    """

    if not d:
        return None

    d = str(d).strip()

    match = re.search(r"\b(19|20)\d{2}\b", d)

    if match:
        return int(match.group(0))

    return None


def harvest_cds_raw(
    max_records: int | None = None,
    estimated_total: int | None = None,
    progress_every: int = 1000,
    save_every: int = 5000,
    checkpoint_dir: str = "data/checkpoints/cds",
    sleep_seconds: float = 0.2,
    date_start: int = 1990,
    date_end: int = 2025,
) -> pd.DataFrame:

    checkpoint_dir = Path(checkpoint_dir)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    sickle = Sickle(
        CDS_OAI_ENDPOINT,
        timeout=120,
    )

    records = sickle.ListRecords(metadataPrefix="oai_dc")

    rows = []

    total = max_records if max_records else estimated_total

    processed = 0
    chunk_id = 0
    retry_count = 0
    max_retries = 10
    with tqdm(total=total, desc="Harvesting CDS records", unit="record") as pbar:

        while True:
            try:
                record = next(records)

                if getattr(record, "deleted", False):
                    continue

                meta = record.metadata or {}

                title_list = normalize_list(meta.get("title"))
                creator_list = normalize_list(meta.get("creator"))
                date_list = normalize_list(meta.get("date"))

                record_year = None
                
                if date_list:
                    record_year = _year_from_date(date_list[0])
                
                if record_year is not None:
                    if record_year < date_start or record_year > date_end:
                        continue
                identifier_list = normalize_list(meta.get("identifier"))
                description_list = normalize_list(meta.get("description"))
                subject_list = normalize_list(meta.get("subject"))
                publisher_list = normalize_list(meta.get("publisher"))
                contributor_list = normalize_list(meta.get("contributor"))
                type_list = normalize_list(meta.get("type"))
                format_list = normalize_list(meta.get("format"))
                source_list = normalize_list(meta.get("source"))
                language_list = normalize_list(meta.get("language"))
                relation_list = normalize_list(meta.get("relation"))
                coverage_list = normalize_list(meta.get("coverage"))
                rights_list = normalize_list(meta.get("rights"))

                doi = extract_doi(identifier_list)
                landing_page_url = extract_best_url(identifier_list, doi)

                raw_meta = {
                    "oai_identifier": getattr(record.header, "identifier", None),
                    "datestamp": getattr(record.header, "datestamp", None),
                    "set_specs": normalize_list(getattr(record.header, "setSpecs", [])),
                    "title": title_list,
                    "creator": creator_list,
                    "date": date_list,
                    "identifier": identifier_list,
                    "description": description_list,
                    "subject": subject_list,
                    "publisher": publisher_list,
                    "contributor": contributor_list,
                    "type": type_list,
                    "format": format_list,
                    "source": source_list,
                    "language": language_list,
                    "relation": relation_list,
                    "coverage": coverage_list,
                    "rights": rights_list,
                }

                rows.append(
                    {
                        "oai_identifier": getattr(record.header, "identifier", None),
                        "datestamp": getattr(record.header, "datestamp", None),
                        "set_specs": normalize_list(getattr(record.header, "setSpecs", [])),
                        "title": title_list[0] if title_list else None,
                        "authors": creator_list,
                        "date": date_list[0] if date_list else None,
                        "identifier": identifier_list,
                        "abstract": description_list[0] if description_list else None,
                        "subject": subject_list,
                        "publisher": publisher_list[0] if publisher_list else None,
                        "contributor": contributor_list,
                        "type": type_list[0] if type_list else None,
                        "format": format_list[0] if format_list else None,
                        "source": source_list[0] if source_list else None,
                        "language": language_list[0] if language_list else None,
                        "relation": relation_list,
                        "coverage": coverage_list,
                        "rights": rights_list[0] if rights_list else None,
                        "rights_url": rights_list[1] if len(rights_list) > 1 else None,
                        "doi": doi,
                        "landing_page_url": landing_page_url,
                        "raw_json": _json(raw_meta),
                    }
                )

                processed += 1
                retry_count = 0

                pbar.update(1)

                if processed % progress_every == 0:
                    pbar.set_postfix(
                        {
                            "processed": f"{processed:,}",
                            "chunks": chunk_id,
                        }
                    )

                # checkpoint save
                if processed % save_every == 0:
                
                    raw_chunk_df = pd.DataFrame(rows)
                
                    canon_chunk_df = build_big_canon_cds(
                        raw_chunk_df
                    )
                
                    canon_parquet_path, canon_csv_path = save_cds_chunk_outputs(
                        canon_chunk_df,
                        chunk_id=chunk_id,
                        output_root="data/by_server",
                        date_start=str(date_start),
                        date_end=str(date_end),
                    )
                
                    print(f"\nSaved canon parquet: {canon_parquet_path}")
                    print(f"Saved canon csv.gz: {canon_csv_path}")
                
                    rows = []
                    chunk_id += 1
                
                    gc.collect()


                
                if max_records and processed >= max_records:
                    break

                time.sleep(sleep_seconds)

            except StopIteration:
                break

            
            except Exception as e:
            
                retry_count += 1
            
                print("\nERROR DETECTED")
                print(type(e).__name__, e)
            
                traceback.print_exc()
            
                if rows:
                    chunk_path = checkpoint_dir / f"cds_emergency_chunk_{chunk_id:05d}.parquet"
            
                    pd.DataFrame(rows).to_parquet(chunk_path, index=False)
            
                    print(f"\nEmergency checkpoint saved: {chunk_path}")
            
                if retry_count >= max_retries:
                    print("\nMaximum retries reached. Stopping harvest.")
                    break
            
                sleep_time = min(300, 30 * retry_count)
            
                print(f"\nSleeping {sleep_time} seconds before retry...")
                time.sleep(sleep_time)
            
                continue

    harvest_metadata = {
        "server_name": "CERN Document Server",
        "backend": "cds",
        "endpoint": CDS_OAI_ENDPOINT,
        "harvest_timestamp": pd.Timestamp.utcnow().isoformat(),
        "record_year": record_year,
        # "records_count": int(len(final_df)),
        "estimated_total": estimated_total,
        "checkpoint_files": len(list(checkpoint_dir.glob("*.parquet"))),
    }
    
    with open(checkpoint_dir / "harvest_metadata.json", "w") as f:
        json.dump(harvest_metadata, f, indent=2)
    
    # save final remaining rows
    if rows:
    
        raw_chunk_df = pd.DataFrame(rows)
    
        canon_chunk_df = build_big_canon_cds(
            raw_chunk_df
        )
    
        canon_parquet_path, canon_csv_path = save_cds_chunk_outputs(
            canon_chunk_df,
            chunk_id=chunk_id,
            output_root="data/by_server",
            date_start=str(date_start),
            date_end=str(date_end),
        )
    
        print(f"\nSaved final canon parquet: {canon_parquet_path}")
        print(f"Saved final canon csv.gz: {canon_csv_path}")

    return None

def build_big_canon_cds(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=BIG_CANON_COLS)

    out = pd.DataFrame()

    out["server_name"] = "CERN Document Server"
    out["backend"] = "cds"

    out["doi"] = df["doi"].apply(_norm_doi)
    out["doi_url"] = out["doi"].apply(_doi_url)
    out["source_work_id"] = df.get("oai_identifier")

    out["landing_page_url"] = df.get("landing_page_url")
    out["url_best"] = out["landing_page_url"].fillna(out["doi_url"])
    out["prefix"] = out["doi"].apply(_derive_prefix_from_doi)

    out["member_id"] = None
    out["client_id"] = None
    out["provider_id"] = None
    out["source_registry"] = "cds_oai_pmh"

    out["publisher"] = df.get("publisher")
    out["container_title"] = None
    out["institution_name"] = "CERN"
    out["group_title"] = None
    out["issn"] = None

    out["title"] = df.get("title")
    out["original_title"] = None
    out["short_title"] = None
    out["subtitle"] = None
    out["language"] = df.get("language")

    out["type_backend_raw"] = df.get("type")
    out["subtype_backend_raw"] = df.get("format")
    out["type_canonical"] = "unknown"
    out["is_paratext"] = None
    out["is_preprint_candidate"] = None

    out["date_created"] = None
    out["date_posted"] = df.get("date")
    out["date_deposited"] = None
    out["date_indexed"] = None
    out["date_updated"] = df.get("datestamp")
    out["date_issued"] = None
    out["date_registered"] = None
    out["date_published"] = None
    out["date_published_online"] = None

    out["publication_year"] = out["date_posted"].apply(_year_from_date)
    out["date_published_source"] = None
    out["date_posted_source"] = out["date_posted"].apply(
        lambda x: "cds_oai_dc:date" if x else None
    )

    out["is_oa"] = None
    out["oa_status"] = None
    out["license"] = df.get("rights")
    out["license_url_best"] = df.get("rights_url")

    out["abstract_raw"] = df.get("abstract")
    out["abstract_text"] = df.get("abstract")

    out["links_json_best"] = df.get("identifier").apply(_json)
    out["fulltext_pdf_url"] = None

    out["authors_flat"] = df.get("authors").apply(authors_flat_from_list)
    out["institutions_flat"] = None
    out["countries_flat"] = None
    out["authors_json"] = df.get("authors").apply(_json)
    out["contributors_json"] = df.get("contributor").apply(_json)
    out["editors_json"] = None

    out["funders_json"] = None
    out["funders_flat"] = None
    out["funders_count"] = None

    out["subjects_json"] = df.get("subject").apply(_json)
    out["concepts_json"] = None
    out["topics_json"] = None

    out["cited_by_count"] = None
    out["cited_by_count_datacite"] = None
    out["cited_by_count_openalex"] = None
    out["is_referenced_by_count_crossref"] = None
    out["reference_count"] = None
    out["references_json"] = None

    out["relations_json"] = df.get("relation").apply(_json)
    out["has_preprint"] = None
    out["is_preprint_of"] = None
    out["has_published_version"] = None
    out["published_version_ids_json"] = None
    out["is_version_of"] = None
    out["version_of_ids_json"] = None
    out["version_label"] = None
    out["has_review"] = None
    out["update_to_json"] = None
    out["parent_doi"] = None
    out["update_policy"] = None

    out["rule_tokens"] = "cds_oai_dc"
    out["rule_row_id"] = None
    out["raw_relationships_json"] = None
    out["raw_json"] = df.get("raw_json")

    def build_record_id(row):
        doi = row.get("doi") 
        oai_identifier = row.get("source_work_id")
        url = row.get("landing_page_url")
        if doi:
            return f"cds::{doi}"
        if oai_identifier:
            return f"cds::{oai_identifier.lower()}"
        if url:
            return f"cds::{url}"
        return None

    out["record_id"] = out.apply(build_record_id, axis=1)

    for col in BIG_CANON_COLS:
        if col not in out.columns:
            out[col] = None

    return out[BIG_CANON_COLS].copy()
    

def save_cds_chunk_outputs(
    canon_df: pd.DataFrame,
    *,
    chunk_id: int,
    output_root: str = "data/by_server",
    date_start: str = 1990,
    date_end: str = 2025,
) -> tuple[Path, Path]:

    safe_server = _safe_server_dir_name("CERN Document Server")

    server_dir = Path(output_root) / safe_server
    server_dir.mkdir(parents=True, exist_ok=True)

    base = (
        f"{safe_server}_{date_start}_{date_end}"
        f"_cds_chunk_{chunk_id:05d}"
    )

    canon_parquet_path = server_dir / f"{base}.parquet"
    canon_csv_gz_path = server_dir / f"{base}.csv.gz"

    canon_df["server_name"] = "CERN Document Server"
    canon_df["backend"] = "cds"

    canon_df.to_parquet(
        canon_parquet_path,
        index=False,
    )

    canon_df.to_csv(
        canon_csv_gz_path,
        index=False,
        compression="gzip",
        encoding="utf-8-sig",
    )

    return canon_parquet_path, canon_csv_gz_path
    
def main():
    raw_df = harvest_cds_raw(
        max_records=None,
        estimated_total=140000,  # approximate CDS preprint collection size
        progress_every=500,
        save_every=2000,
        sleep_seconds=0.5,
        date_start=1990,
        date_end=2025,
    )


    print("\nCDS harvest completed.")
    print("Chunk canonical parquet and csv.gz files saved.")

if __name__ == "__main__":
    main()