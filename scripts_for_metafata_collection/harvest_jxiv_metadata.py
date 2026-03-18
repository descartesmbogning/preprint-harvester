from __future__ import annotations

import json
import gzip
import re
from pathlib import Path
from typing import Any

import pandas as pd
from sickle import Sickle


JXIV_OAI_ENDPOINT = "https://jxiv.jst.go.jp/index.php/jxiv/oai"


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
URL_RE = re.compile(r"https?://[^\s<>\"']+", re.I)


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
            if not value:
                continue
            text = str(value).strip()
            if text.startswith("http://") or text.startswith("https://"):
                # avoid using doi.org if a platform page exists
                if "doi.org/" not in text.lower():
                    return text
        for value in identifiers:
            if not value:
                continue
            text = str(value).strip()
            if text.startswith("http://") or text.startswith("https://"):
                return text
    return _doi_url(doi)


def normalize_list(x: Any) -> list[str]:
    if x is None:
        return []
    if isinstance(x, list):
        return [str(v).strip() for v in x if str(v).strip()]
    return [str(x).strip()] if str(x).strip() else []


def authors_flat_from_list(authors: list[str]) -> str | None:
    authors = [a.strip() for a in authors if a and str(a).strip()]
    return "; ".join(authors) if authors else None


def harvest_jxiv_raw() -> pd.DataFrame:
    sickle = Sickle(JXIV_OAI_ENDPOINT)
    records = sickle.ListRecords(metadataPrefix="oai_dc")

    rows = []
    for record in records:
        if getattr(record, "deleted", False):
            continue

        meta = record.metadata or {}

        title_list = normalize_list(meta.get("title"))
        creator_list = normalize_list(meta.get("creator"))
        date_list = normalize_list(meta.get("date"))
        identifier_list = normalize_list(meta.get("identifier"))
        description_list = normalize_list(meta.get("description"))
        subject_list = normalize_list(meta.get("subject"))
        contributor_list = normalize_list(meta.get("contributor"))
        relation_list = normalize_list(meta.get("relation"))
        coverage_list = normalize_list(meta.get("coverage"))
        language_list = normalize_list(meta.get("language"))

        doi = extract_doi(identifier_list)
        landing_page_url = extract_best_url(identifier_list, doi)

        raw_meta = {
            "title": title_list,
            "creator": creator_list,
            "date": date_list,
            "identifier": identifier_list,
            "description": description_list,
            "subject": subject_list,
            "publisher": meta.get("publisher"),
            "contributor": contributor_list,
            "type": meta.get("type"),
            "format": meta.get("format"),
            "source": meta.get("source"),
            "language": language_list,
            "relation": relation_list,
            "coverage": coverage_list,
            "rights": meta.get("rights"),
        }

        rows.append(
            {
                "title": title_list[0] if title_list else None,
                "authors": creator_list,
                "date": date_list[0] if date_list else None,
                "identifier": identifier_list,
                "abstract": description_list[0] if description_list else None,
                "subject": subject_list,
                "publisher": normalize_list(meta.get("publisher"))[0] if normalize_list(meta.get("publisher")) else None,
                "contributor": contributor_list,
                "type": normalize_list(meta.get("type"))[0] if normalize_list(meta.get("type")) else None,
                "format": normalize_list(meta.get("format"))[0] if normalize_list(meta.get("format")) else None,
                "source": normalize_list(meta.get("source"))[0] if normalize_list(meta.get("source")) else None,
                "language": language_list[0] if language_list else None,
                "relation": relation_list,
                "coverage": coverage_list,
                "rights": normalize_list(meta.get("rights"))[0] if normalize_list(meta.get("rights")) else None,
                "rights_url": normalize_list(meta.get("rights"))[1] if normalize_list(meta.get("rights")) else None,
                "doi": doi,
                "landing_page_url": landing_page_url,
                "raw_json": _json(raw_meta),
            }
        )

    return pd.DataFrame(rows)


def build_big_canon_jxiv(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=BIG_CANON_COLS)

    out = pd.DataFrame()
    out["server_name"] = "Jxiv"
    out["backend"] = "jxiv"

    out["doi"] = df["doi"].apply(_norm_doi)
    out["doi_url"] = out["doi"].apply(_doi_url)
    out["source_work_id"] = out["doi"]

    out["landing_page_url"] = df.get("landing_page_url")
    out["url_best"] = out["landing_page_url"].fillna(out["doi_url"])
    out["prefix"] = out["doi"].apply(_derive_prefix_from_doi)

    out["member_id"] = None
    out["client_id"] = None
    out["provider_id"] = None
    out["source_registry"] = "jxiv_oai_pmh"

    out["publisher"] = df.get("publisher")
    out["container_title"] = None
    out["institution_name"] = None
    out["group_title"] = None
    out["issn"] = None

    out["title"] = df.get("title")
    out["original_title"] = None
    out["short_title"] = None
    out["subtitle"] = None
    out["language"] = df.get("language")

    out["type_backend_raw"] = df.get("type")
    out["subtype_backend_raw"] = df.get("format")
    out["type_canonical"] = "preprint"
    out["is_paratext"] = None
    out["is_preprint_candidate"] = None

    # jxiv OAI dc:date mapped conservatively
    
    # out["date"] = df.get("date")
    out["date_created"] = None
    out["date_posted"] = df.get("date")
    out["date_deposited"] = None
    out["date_indexed"] = None
    out["date_updated"] = None
    out["date_issued"] = None
    out["date_registered"] = None
    out["date_published"] = None
    out["date_published_online"] = None

    out["publication_year"] = out["date_published"].apply(_year_from_date)
    out["date_published_source"] = out["date_published"].apply(lambda x: "jxiv_oai_dc:date" if x else None)
    out["date_posted_source"] = out["date_posted"].apply(lambda x: "jxiv_oai_dc:date" if x else None)

    out["is_oa"] = None #True
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

    out["rule_tokens"] = "jxiv_oai_dc"
    out["rule_row_id"] = None
    out["raw_relationships_json"] = None
    out["raw_json"] = df.get("raw_json")

    def build_record_id(row):
        doi = row.get("doi")
        url = row.get("landing_page_url")
        if doi:
            return f"jxiv::{doi}"
        if url:
            return f"jxiv::{url}"
        return None

    out["record_id"] = out.apply(build_record_id, axis=1)

    for col in BIG_CANON_COLS:
        if col not in out.columns:
            out[col] = None

    return out[BIG_CANON_COLS].copy()


def save_jxiv_outputs(
    canon_df: pd.DataFrame,
    output_root: str = "data/by_server",
    date_start: str = "1990-01-01",
    date_end: str = "2025-12-31",
) -> tuple[Path, Path]:
    safe_server = _safe_server_dir_name("Jxiv")
    server_dir = Path(output_root) / safe_server
    server_dir.mkdir(parents=True, exist_ok=True)

    base = f"{safe_server}_{date_start}_{date_end}_jxiv"
    parquet_path = server_dir / f"{base}.parquet"
    csv_gz_path = server_dir / f"{base}.csv.gz"

    canon_df.to_parquet(parquet_path, index=False)
    canon_df.to_csv(csv_gz_path, index=False, compression="gzip", encoding="utf-8-sig")

    return parquet_path, csv_gz_path


def main():
    raw_df = harvest_jxiv_raw()
    canon_df = build_big_canon_jxiv(raw_df)

    parquet_path, csv_gz_path = save_jxiv_outputs(
        canon_df,
        output_root="data/by_server",
        date_start="1990-01-01",
        date_end="2025-12-31",
    )

    print(f"Harvested raw rows: {len(raw_df)}")
    print(f"Canonical rows: {len(canon_df)}")
    print(f"Saved parquet: {parquet_path}")
    print(f"Saved csv.gz: {csv_gz_path}")


if __name__ == "__main__":
    main()