#!/usr/bin/env python3
"""
Crossref preprint server labeling pipeline (VS Code / CLI friendly).

Main usage (with defaults):

    python crossref_server_labeler.py

Defaults:
    --input-parquet  data/all_backends/all_crossref.parquet
    --output-dir     data/all_backends
    --prefix-csv     your Google Sheet (prefix rules)
    --domain-csv     your Google Sheet (domain rules)
    --subtype-filter preprint

Outputs (in output-dir):
    - all_crossref_labeled.parquet                (CLEAN, with `server_name`)
    - all_crossref_labeled_filtered.parquet       (subset, with `server_name`)
    - manual_review_crossref_preprints_<date>.parquet  (FULL, for inspection)
    - summary_crossref_preprints_<field>_<date>.csv    (FULL, diagnostics)
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
from urllib.parse import urlparse
import re
import unicodedata

# -------------------------------------------------------------------
# 1. CLI ARGUMENTS
# -------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Crossref preprint server labeling pipeline (VS Code / CLI friendly)."
    )
    p.add_argument(
        "--input-parquet",
        default="data/all_backends/all_crossref.parquet",
        help="Path to input Crossref parquet (default: data/all_backends/all_crossref.parquet).",
    )
    p.add_argument(
        "--prefix-csv",
        default=(
            "https://docs.google.com/spreadsheets/d/"
            "10_7FdcpZjntqFsEHIii7bAM72uF__of_iUohSD5w8w4/export?format=csv&gid=174743897"
        ),
        help="Prefix rules CSV or Google Sheet export URL "
             "(default: your prefix sheet).",
    )
    p.add_argument(
        "--domain-csv",
        default=(
            "https://docs.google.com/spreadsheets/d/"
            "10_7FdcpZjntqFsEHIii7bAM72uF__of_iUohSD5w8w4/export?format=csv&gid=143048761"
        ),
        help="Domain rules CSV or Google Sheet export URL "
             "(default: your domain sheet).",
    )
    p.add_argument(
        "--output-dir",
        default="data/all_backends",
        help="Output directory (default: data/all_backends).",
    )
    p.add_argument(
        "--subtype-filter",
        default="preprint",
        help="Filter on subtype (default: 'preprint'; use '' to disable).",
    )
    p.add_argument(
        "--examples-k",
        type=int,
        default=10,
        help="Number of example URLs/DOIs to store per group in summaries (default: 10).",
    )
    return p.parse_args()


# -------------------------------------------------------------------
# 2. NORMALIZATION HELPERS
# -------------------------------------------------------------------

DOI_CORE_RE = re.compile(r"(10\.\d{4,9}/\S+)", re.IGNORECASE)


def _normalize_doi_raw(x: str) -> str | None:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    s = str(x).strip()
    s = unicodedata.normalize("NFKC", s).lower()
    m = DOI_CORE_RE.search(s)  # extract from URL/“doi:” etc.
    if m:
        return m.group(1)
    if s.startswith("10.") and "/" in s:
        return s
    return None


def clean_preprint_fields(df: pd.DataFrame, *, numeric_keep: int = 2, add_bucket: bool = True) -> pd.DataFrame:
    """
    Clean + enrich preprint fields.
    - Builds gold_server_name
    - Normalizes DOI and builds:
        * doi_prefix_from_text
        * doi_suffix
        * doi_prefix_first_token
    - Builds primary_domain and primary_domain_extend
    - Extracts year from posted_date.
    """
    df = df.drop_duplicates().copy()

    # --- gold_server_name
    df["gold_server_name"] = (
        df.get("institution_name")
          .fillna(df.get("group_title"))
          .fillna(df.get("publisher"))
    )

    # --- Normalize DOI
    if "doi" in df.columns:
        df["doi_lc"] = df["doi"].map(_normalize_doi_raw)
    else:
        df["doi_lc"] = pd.Series(pd.NA, index=df.index, dtype="object")

    # --- prefix as given
    if "prefix" in df.columns:
        df["prefix_lc"] = df["prefix"].astype(str).str.strip().str.lower()
        df.loc[df["prefix_lc"].isin(["", "nan", "none"]), "prefix_lc"] = pd.NA
    else:
        df["prefix_lc"] = pd.Series(pd.NA, index=df.index, dtype="object")

    # --- Extract prefix/suffix from normalized DOI
    doi_parts = df["doi_lc"].str.extract(r"^(10\.\d{4,9})/(.+)$")
    df["doi_prefix_from_text"] = doi_parts[0]
    df["doi_suffix"] = doi_parts[1]
    df["prefix_lc"] = df["prefix_lc"].where(df["prefix_lc"].notna(), df["doi_prefix_from_text"])

    # --- Build first segment of suffix
    starts_with_letter = df["doi_suffix"].str.match(r"^[a-z]", na=False)

    first_seg_letters = df["doi_suffix"].str.extract(
        r"^([a-z\-]+)(?=\d|[.\-_/:]|$)", expand=False
    )
    first_seg_default = df["doi_suffix"].str.split(r"[.\-_/:\s]", n=1, regex=True).str[0]

    first_seg = pd.Series(
        np.where(starts_with_letter, first_seg_letters, first_seg_default),
        index=df.index,
        dtype="object"
    )

    need_fallback = first_seg.isna() & df["doi_suffix"].notna()
    first_seg.loc[need_fallback] = df.loc[need_fallback, "doi_suffix"].str.extract(
        r"^([a-z0-9\-]+)", expand=False
    )

    # compress numeric tokens
    if numeric_keep and numeric_keep > 0:
        numeric_only = first_seg.str.fullmatch(r"\d+", na=False)
        first_seg.loc[numeric_only] = first_seg.loc[numeric_only].str[:numeric_keep]

    df["doi_prefix_first_token"] = pd.Series(pd.NA, index=df.index, dtype="object")
    ok = df["prefix_lc"].notna() & first_seg.notna() & (first_seg.astype(str) != "")
    df.loc[ok, "doi_prefix_first_token"] = (
        df.loc[ok, "prefix_lc"].astype(str) + "/" + first_seg.loc[ok].astype(str)
    )

    def domain_and_first_path(u):
        try:
            parsed = urlparse(str(u).lower())
            host = parsed.netloc
            if host.startswith("www."):
                host = host[4:]
            parts = re.split(r"[/=]", parsed.path)
            first_part = parts[1] if len(parts) > 1 and parts[1] else None
            return f"{host}/{first_part}" if host and first_part else (host or None)
        except Exception:
            return None

    if "primary_url" in df.columns:
        df["primary_domain"] = df["primary_url"].apply(
            lambda u: urlparse(str(u)).netloc.lower().replace("www.", "") if pd.notna(u) else None
        )
        df["primary_domain_extend"] = df["primary_url"].apply(domain_and_first_path)
    else:
        df["primary_domain"] = pd.Series(pd.NA, index=df.index, dtype="object")
        df["primary_domain_extend"] = pd.Series(pd.NA, index=df.index, dtype="object")

    if "posted_date" in df.columns:
        df["posted_date"] = pd.to_datetime(df["posted_date"], errors="coerce")
        df["year"] = df["posted_date"].dt.year

    return df


# -------------------------------------------------------------------
# 3. SUMMARY HELPERS
# -------------------------------------------------------------------

MISSING_TOKEN = "MISSING"


def _top_k_counts(s: pd.Series, k: int | None = None) -> List[str]:
    vc = s.fillna(MISSING_TOKEN).value_counts(dropna=False)
    if k is not None:
        vc = vc.head(k)
    return [f"{str(name)} ({int(cnt)})" for name, cnt in vc.items()]


def _sample_unique(s: pd.Series, k: int = 10) -> List[str]:
    if s is None or s.empty:
        return []
    vals = s.dropna().unique()
    if len(vals) == 0:
        return []
    return [str(v) for v in vals[:k]]


def _build_sharing_maps(df: pd.DataFrame) -> Dict[str, Dict[str, List[str]]]:
    maps: Dict[str, Dict[str, List[str]]] = {}

    if "prefix" in df.columns and "validated_server_name" in df.columns:
        maps["prefix_to_server_names"] = (
            df.dropna(subset=["prefix"])
              .groupby("prefix")["validated_server_name"]
              .apply(lambda x: sorted(pd.Series(x.dropna().unique())))
              .to_dict()
        )
    else:
        maps["prefix_to_server_names"] = {}

    if "member" in df.columns and "validated_server_name" in df.columns:
        maps["member_to_server_names"] = (
            df.dropna(subset=["member"])
              .groupby("member")["validated_server_name"]
              .apply(lambda x: sorted(pd.Series(x.dropna().unique())))
              .to_dict()
        )
    else:
        maps["member_to_server_names"] = {}

    if "primary_domain" in df.columns and "validated_server_name" in df.columns:
        maps["domain_to_server_names"] = (
            df.dropna(subset=["primary_domain"])
              .groupby("primary_domain")["validated_server_name"]
              .apply(lambda x: sorted(pd.Series(x.dropna().unique())))
              .to_dict()
        )
    else:
        maps["domain_to_server_names"] = {}

    if "group_title" in df.columns and "validated_server_name" in df.columns:
        maps["group_title_to_server_names"] = (
            df.dropna(subset=["group_title"])
              .groupby("group_title")["validated_server_name"]
              .apply(lambda x: sorted(pd.Series(x.dropna().unique())))
              .to_dict()
        )
    else:
        maps["group_title_to_server_names"] = {}

    if "institution_name" in df.columns and "validated_server_name" in df.columns:
        maps["institution_name_to_server_names"] = (
            df.dropna(subset=["institution_name"])
              .groupby("institution_name")["validated_server_name"]
              .apply(lambda x: sorted(pd.Series(x.dropna().unique())))
              .to_dict()
        )
    else:
        maps["institution_name_to_server_names"] = {}

    if "doi_prefix_first_token" in df.columns and "validated_server_name" in df.columns:
        maps["doi_prefix_first_token_to_server_names"] = (
            df.dropna(subset=["doi_prefix_first_token"])
              .groupby("doi_prefix_first_token")["validated_server_name"]
              .apply(lambda x: sorted(pd.Series(x.dropna().unique())))
              .to_dict()
        )
    else:
        maps["doi_prefix_first_token_to_server_names"] = {}

    return maps


def summarize_by_field(
    df: pd.DataFrame,
    field: str,
    examples_k: int = 10,
    preprint_subtype_value: str = "preprint",
) -> pd.DataFrame:
    if field not in df.columns:
        raise KeyError(f"Field {field!r} not in DataFrame")

    maps = _build_sharing_maps(df)
    prefix_map = maps["prefix_to_server_names"]
    member_map = maps["member_to_server_names"]
    domain_map = maps["domain_to_server_names"]
    group_title_map = maps["group_title_to_server_names"]
    institution_name_map = maps["institution_name_to_server_names"]
    doi_prefix_first_token_map = maps["doi_prefix_first_token_to_server_names"]

    work = df.copy()
    work[field] = work[field].fillna(MISSING_TOKEN)
    g = work.groupby(field, dropna=False)

    rows: List[Dict] = []
    for server_key, group in g:
        prefixes_u = group["prefix"].dropna().unique() if "prefix" in group else []
        members_u = group["member"].dropna().unique() if "member" in group else []
        domains_u = group["primary_domain"].dropna().unique() if "primary_domain" in group else []
        group_titles_u = group["group_title"].dropna().unique() if "group_title" in group else []
        institutions_u = group["institution_name"].dropna().unique() if "institution_name" in group else []
        doi_prefixes_u = group["doi_prefix_first_token"].dropna().unique() if "doi_prefix_first_token" in group else []

        server_names_sharing_prefix = sorted(
            set().union(*(set(prefix_map.get(px, [])) for px in prefixes_u))
        ) if len(prefixes_u) else []

        server_names_sharing_member = sorted(
            set().union(*(set(member_map.get(mb, [])) for mb in members_u))
        ) if len(members_u) else []

        server_names_sharing_domain = sorted(
            set().union(*(set(domain_map.get(dom, [])) for dom in domains_u))
        ) if len(domains_u) else []

        server_names_sharing_group = sorted(
            set().union(*(set(group_title_map.get(gt, [])) for gt in group_titles_u))
        ) if len(group_titles_u) else []

        server_names_sharing_institution = sorted(
            set().union(*(set(institution_name_map.get(ins, [])) for ins in institutions_u))
        ) if len(institutions_u) else []

        server_names_sharing_doi_prefix = sorted(
            set().union(*(set(doi_prefix_first_token_map.get(dp, [])) for dp in doi_prefixes_u))
        ) if len(doi_prefixes_u) else []

        if "subtype" in group.columns:
            n_preprints = int((group["subtype"] == preprint_subtype_value).sum())
        else:
            n_preprints = 0

        row = {
            f"Field_{field}": server_key,
            "Publishers": _top_k_counts(group.get("publisher", pd.Series(index=group.index))),
            "Prefixes": _top_k_counts(group.get("prefix", pd.Series(index=group.index))),
            "Members": _top_k_counts(group.get("member", pd.Series(index=group.index))),
            "institution_name": _top_k_counts(group.get("institution_name", pd.Series(index=group.index))),
            "group_title": _top_k_counts(group.get("group_title", pd.Series(index=group.index))),
            "primary_domain": _top_k_counts(group.get("primary_domain", pd.Series(index=group.index))),
            "primary_domain_extend": _top_k_counts(group.get("primary_domain_extend", pd.Series(index=group.index))),
            "doi_prefix_first_token": _top_k_counts(group.get("doi_prefix_first_token", pd.Series(index=group.index))),
            "gold_server_name": _top_k_counts(group.get("gold_server_name", pd.Series(index=group.index))),
            "validation_status": _top_k_counts(group.get("validation_status", pd.Series(index=group.index))),
            "rule_id": _top_k_counts(group.get("rule_id", pd.Series(index=group.index))),
            "year": _top_k_counts(group.get("year", pd.Series(index=group.index))),
            "Associated with Institution": bool(
                group.get("institution_name", pd.Series(index=group.index)).notna().any()
            ),
            "institution_name_count": int(
                group.get("institution_name", pd.Series()).fillna(MISSING_TOKEN).nunique()
            ),
            "group_title_count": int(
                group.get("group_title", pd.Series()).fillna(MISSING_TOKEN).nunique()
            ),
            "Example URLs": _sample_unique(group.get("url", pd.Series()), examples_k),
            "Example Primary URLs": _sample_unique(group.get("primary_url", pd.Series()), examples_k),
            "Example DOIs": _sample_unique(group.get("doi", pd.Series()), examples_k),
            "Number of Preprint Works": n_preprints,
            "Server Sharing Prefix": server_names_sharing_prefix,
            "Server Sharing Prefix Count": len(server_names_sharing_prefix),
            "Server Sharing Member": server_names_sharing_member,
            "Server Sharing Member Count": len(server_names_sharing_member),
            "Server Sharing Primary Domain": server_names_sharing_domain,
            "Server Sharing Primary Domain Count": len(server_names_sharing_domain),
            "Server Sharing Group Title's": server_names_sharing_group,
            "Server Sharing Group Title's Count": len(server_names_sharing_group),
            "Server Sharing Institution": server_names_sharing_institution,
            "Server Sharing Institution Count": len(server_names_sharing_institution),
            "Server Sharing DOI Prefix and Token": server_names_sharing_doi_prefix,
            "Server Sharing DOI Prefix and Token Count": len(server_names_sharing_doi_prefix),
        }
        rows.append(row)

    summary = pd.DataFrame(rows)
    summary = summary.sort_values(
        by=["Number of Preprint Works", f"Field_{field}"],
        ascending=[False, True],
        kind="mergesort",
    ).reset_index(drop=True)
    return summary


# -------------------------------------------------------------------
# 4. VALIDATION PIPELINE
# -------------------------------------------------------------------

OSF_AMS_DOMAINS = {"osf.io", "ams.org"}
IBICT_DOMAIN = "preprints.ibict.br"

PREFIX_OVERRIDE_DOMAINS = {
    "vimeo.com",
    "experience.arcgis.com",
    "researchcatalogue.net",
    "cambridge.org",
    "scholarcommons.usf.edu",
}

DOMAIN_OVERRIDE_DOMAINS = {
    "biorxiv.org",
    "engrxiv.org",
    "eartharxiv.org",
    "saemobilus.sae.org",
    "21docs.com",
    "ecoevorxiv.org",
    "datacite.org",
    "protocols.io",
    "jsr.org",
    "crossref.org",
    "ihp-wins-dev.geo-solutions.it",
    "techrxiv.org",
}

DOI_PREFIX_OVERRIDE_TOKENS = {
    "10.25159/unisarxiv",
    "10.54120/jost",
    "10.22541/21docs",
    "10.5194/hess-",
    "10.5194/amt-",
    "10.14293/11",
    "10.14293/newpsychology",
    "10.35948/crusca",
    "10.47952/gro-publ-",
    "10.15763/11",
    "10.22541/techrxiv",
    "10.1590/scielopreprintstest",
    "10.5555/dspace",
}

PUBPUB_SUFFIX = ".pubpub.org"


def normalize_name(s):
    if pd.isna(s):
        return None
    s = str(s).strip()
    if not s:
        return None

    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


def row_match_domain(row):
    d = row["norm_domain"]
    if not isinstance(d, str) or not d:
        return False
    for col in ["norm_group", "norm_inst", "norm_pub"]:
        val = row[col]
        if isinstance(val, str) and d in val:
            return True
    return False


def row_match_prefix(row):
    p = row["norm_prefix"]
    if not isinstance(p, str) or not p:
        return False
    for col in ["norm_group", "norm_inst", "norm_pub"]:
        val = row[col]
        if isinstance(val, str) and p in val:
            return True
    return False


def validate_server_names(df: pd.DataFrame, inplace: bool = False):
    if not inplace:
        df = df.copy()

    required_cols = [
        "domain_server_name",
        "prefix_server_name",
        "group_title",
        "institution_name",
        "publisher",
        "primary_domain",
        "doi_prefix_first_token",
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = pd.NA

    df["norm_domain"] = df["domain_server_name"].apply(normalize_name)
    df["norm_prefix"] = df["prefix_server_name"].apply(normalize_name)
    df["norm_group"] = df["group_title"].apply(normalize_name)
    df["norm_inst"] = df["institution_name"].apply(normalize_name)
    df["norm_pub"] = df["publisher"].apply(normalize_name)

    df["validated_server_name"] = pd.NA
    df["validation_status"] = pd.NA
    df["confidence_score"] = pd.NA
    df["rule_id"] = pd.NA

    df["match_dom_text"] = df.apply(row_match_domain, axis=1)
    df["match_pref_text"] = df.apply(row_match_prefix, axis=1)

    dom = df["norm_domain"]
    pref = df["norm_prefix"]

    same_dom_pref = dom.notna() & pref.notna() & (dom == pref)
    diff_dom_pref = dom.notna() & pref.notna() & (dom != pref)

    def unvalidated():
        return df["validated_server_name"].isna()

    # R1
    mask1 = unvalidated() & same_dom_pref & (df["match_dom_text"] | df["match_pref_text"])
    df.loc[mask1, "validated_server_name"] = df.loc[mask1, "domain_server_name"]
    df.loc[mask1, "validation_status"] = "MATCH_STRONG"
    df.loc[mask1, "confidence_score"] = 1.0
    df.loc[mask1, "rule_id"] = "R1_MATCH_STRONG"

    # R2
    mask2 = unvalidated() & same_dom_pref & ~df["match_dom_text"] & ~df["match_pref_text"]
    df.loc[mask2, "validated_server_name"] = df.loc[mask2, "domain_server_name"]
    df.loc[mask2, "validation_status"] = "MATCH_WEAK"
    df.loc[mask2, "confidence_score"] = 0.8
    df.loc[mask2, "rule_id"] = "R2_MATCH_WEAK"

    # R3
    remaining = unvalidated() & diff_dom_pref
    mask3_dom = remaining & df["match_dom_text"] & ~df["match_pref_text"]
    mask3_pref = remaining & df["match_pref_text"] & ~df["match_dom_text"]

    df.loc[mask3_dom, "validated_server_name"] = df.loc[mask3_dom, "domain_server_name"]
    df.loc[mask3_dom, "validation_status"] = "MATCH_DOMAIN"
    df.loc[mask3_dom, "confidence_score"] = 0.9
    df.loc[mask3_dom, "rule_id"] = "R3_MATCH_DOMAIN"

    df.loc[mask3_pref, "validated_server_name"] = df.loc[mask3_pref, "prefix_server_name"]
    df.loc[mask3_pref, "validation_status"] = "MATCH_PREFIX"
    df.loc[mask3_pref, "confidence_score"] = 0.9
    df.loc[mask3_pref, "rule_id"] = "R3_MATCH_PREFIX"

    # R4
    remaining = unvalidated() & diff_dom_pref
    mask4 = remaining & ~df["match_dom_text"] & ~df["match_pref_text"]
    df.loc[mask4, "validation_status"] = "LOW_CONFIDENCE_MANUAL"
    df.loc[mask4, "confidence_score"] = 0.3
    df.loc[mask4, "rule_id"] = "R4_LOW_CONFIDENCE"

    # R5a
    mask5_group = df["primary_domain"].isin(OSF_AMS_DOMAINS) & df["group_title"].notna()
    df.loc[mask5_group, "validated_server_name"] = df.loc[mask5_group, "group_title"]
    df.loc[mask5_group, "validation_status"] = "MATCH_RULE5_GROUP_TITLE"
    df.loc[mask5_group, "confidence_score"] = 0.98
    df.loc[mask5_group, "rule_id"] = "R5A_OSF_AMS_GROUP_TITLE"

    # R5b
    mask5_ibict = (df["primary_domain"] == IBICT_DOMAIN) & df["institution_name"].notna()
    df.loc[mask5_ibict, "validated_server_name"] = df.loc[mask5_ibict, "institution_name"]
    df.loc[mask5_ibict, "validation_status"] = "MATCH_RULE5_INSTITUTION"
    df.loc[mask5_ibict, "confidence_score"] = 0.98
    df.loc[mask5_ibict, "rule_id"] = "R5B_IBICT_INSTITUTION"

    # R5c PubPub
    mask_pubpub = (
        unvalidated()
        & df["primary_domain"].notna()
        & df["primary_domain"].astype(str).str.endswith(PUBPUB_SUFFIX)
    )

    def get_pubpub_label(row):
        dom_val = row["primary_domain"]
        if not isinstance(dom_val, str):
            return None
        gt = row.get("group_title")
        if isinstance(gt, str) and gt.strip():
            label = gt.strip()
        else:
            sub = dom_val.split(PUBPUB_SUFFIX)[0]
            sub = sub.split(".")[0]
            label = sub.replace("-", " ").replace("_", " ").strip()
        return f"{label.title()} (PubPub)" if label else None

    if mask_pubpub.any():
        pubpub_labels = df.loc[mask_pubpub].apply(get_pubpub_label, axis=1)
        has_pubpub_label = pubpub_labels.notna()
        idx_pubpub = df.loc[mask_pubpub].index[has_pubpub_label]

        df.loc[idx_pubpub, "validated_server_name"] = pubpub_labels[has_pubpub_label]
        df.loc[idx_pubpub, "validation_status"] = "MATCH_RULE5_PUBPUB"
        df.loc[idx_pubpub, "confidence_score"] = 0.98
        df.loc[idx_pubpub, "rule_id"] = "R5C_PUBPUB"

    # R5d
    mask5_prefix = (
        unvalidated()
        & df["primary_domain"].isin(PREFIX_OVERRIDE_DOMAINS)
        & df["prefix_server_name"].notna()
    )
    df.loc[mask5_prefix, "validated_server_name"] = df.loc[mask5_prefix, "prefix_server_name"]
    df.loc[mask5_prefix, "validation_status"] = "MATCH_RULE5_PREFIX_OVERRIDE"
    df.loc[mask5_prefix, "confidence_score"] = 0.97
    df.loc[mask5_prefix, "rule_id"] = "R5D_PREFIX_OVERRIDE"

    # R5e
    mask5_domain = (
        unvalidated()
        & df["primary_domain"].isin(DOMAIN_OVERRIDE_DOMAINS)
        & df["domain_server_name"].notna()
    )
    df.loc[mask5_domain, "validated_server_name"] = df.loc[mask5_domain, "domain_server_name"]
    df.loc[mask5_domain, "validation_status"] = "MATCH_RULE5_DOMAIN_OVERRIDE"
    df.loc[mask5_domain, "confidence_score"] = 0.97
    df.loc[mask5_domain, "rule_id"] = "R5E_DOMAIN_OVERRIDE"

    # R5f
    mask5_prefix_doi = (
        unvalidated()
        & df["doi_prefix_first_token"].isin(DOI_PREFIX_OVERRIDE_TOKENS)
        & df["prefix_server_name"].notna()
    )
    df.loc[mask5_prefix_doi, "validated_server_name"] = df.loc[mask5_prefix_doi, "prefix_server_name"]
    df.loc[mask5_prefix_doi, "validation_status"] = "MATCH_RULE5_DOI_PREFIX_OVERRIDE"
    df.loc[mask5_prefix_doi, "confidence_score"] = 0.97
    df.loc[mask5_prefix_doi, "rule_id"] = "R5F_DOI_PREFIX_OVERRIDE"

    manual_df = df[df["validation_status"] == "LOW_CONFIDENCE_MANUAL"].copy()
    return df, manual_df


# -------------------------------------------------------------------
# 5. SERVER NAME CORRECTIONS
# -------------------------------------------------------------------

SERVER_NAME_CORRECTIONS = {
    "Techrxiv": "TechRxiv",
    "agriRxiv": "AgriRxiv",
    "AgriXiv": "AgriRxiv",
    "elife": "eLife",
    "eLife": "eLife",
    "ESS Open Archive": "Earth and Space Science Open Archive",
    "LawArXiv": "Law Archive",
    "Instituto Brasileiro de Informação em Ciência e Tecnologia Ibict":
        "Instituto Brasileiro de Informação em Ciência e Tecnologia (Ibict)",
    "EMERI": "EmeRI",
    "Life Sciences": "EcoEvoRxiv",
    "Physical Sciences and Mathematics": "EcoEvoRxiv",
    "Social and Behavioral Sciences": "EcoEvoRxiv",
}


def apply_server_name_corrections(df_valid: pd.DataFrame) -> pd.DataFrame:
    df_valid = df_valid.copy()
    df_valid["validated_server_name_old"] = df_valid["validated_server_name"]
    df_valid["validated_server_name"] = df_valid["validated_server_name"].astype(str).str.strip()
    df_valid["validated_server_name"] = df_valid["validated_server_name"].replace(SERVER_NAME_CORRECTIONS)
    df_valid["validated_server_name"] = (
        df_valid["validated_server_name"]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    return df_valid


# -------------------------------------------------------------------
# 6. RULE TABLES
# -------------------------------------------------------------------

def load_rule_tables(prefix_csv: str, domain_csv: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    print(f"[INFO] Loading prefix rules from: {prefix_csv}")
    prefix_server_name = (
        pd.read_csv(prefix_csv)[["prefix_server_name", "Field_doi_prefix_first_token"]]
        .dropna(how="all")
        .drop_duplicates()
    )

    print(f"[INFO] Loading domain rules from: {domain_csv}")
    domain_server_name = (
        pd.read_csv(domain_csv)[["domain_server_name", "Field_primary_domain_ok"]]
        .dropna(how="all")
        .drop_duplicates()
    )
    return prefix_server_name, domain_server_name


def merge_rules_into_df(
    df: pd.DataFrame,
    prefix_server_name: pd.DataFrame,
    domain_server_name: pd.DataFrame,
) -> pd.DataFrame:
    df_merged = df.merge(
        domain_server_name,
        left_on="primary_domain",
        right_on="Field_primary_domain_ok",
        how="left",
    )
    df_merged = df_merged.merge(
        prefix_server_name,
        left_on="doi_prefix_first_token",
        right_on="Field_doi_prefix_first_token",
        how="left",
    )
    df_merged = df_merged.drop(columns=["Field_primary_domain_ok", "Field_doi_prefix_first_token"])
    return df_merged


# -------------------------------------------------------------------
# 7. MAIN PIPELINE
# -------------------------------------------------------------------

def main():
    args = parse_args()

    input_parquet = Path(args.input_parquet)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not input_parquet.exists():
        raise FileNotFoundError(f"Input parquet not found: {input_parquet}")

    print(f"[INFO] Reading Crossref parquet: {input_parquet}")
    df_all = pd.read_parquet(input_parquet)
    n_before = len(df_all)
    print(f"[INFO] Loaded {n_before:,} rows from input.")

    # Optional filter on subtype
    if args.subtype_filter:
        if "subtype" not in df_all.columns:
            print(f"[WARN] subtype_filter='{args.subtype_filter}' but column 'subtype' is missing. No filter applied.")
        else:
            df_all = df_all[df_all["subtype"] == args.subtype_filter].copy()
            print(f"[INFO] After subtype filter ({args.subtype_filter}): {len(df_all):,} rows.")

    df_all = df_all.drop_duplicates()
    print(f"[INFO] After dropping exact duplicates: {len(df_all):,} rows.")

    # posted_date / latest date
    if "posted_date" in df_all.columns:
        df_all["posted_date"] = pd.to_datetime(df_all["posted_date"], errors="coerce")
        latest_date = df_all["posted_date"].max()
        latest_date_str = latest_date.strftime("%Y-%m-%d") if pd.notna(latest_date) else "unknown_date"
    else:
        latest_date_str = "unknown_date"
    print(f"[INFO] Most recent posted_date: {latest_date_str}")

    # Normalize fields
    print("[INFO] Cleaning / normalizing preprint fields...")
    df_norm = clean_preprint_fields(df_all, numeric_keep=2, add_bucket=True)
    print(f"[INFO] After normalization: {df_norm.shape[0]:,} rows, {df_norm.shape[1]} columns.")

    # Rules
    prefix_server_name, domain_server_name = load_rule_tables(args.prefix_csv, args.domain_csv)
    print(f"[INFO] prefix_server_name rows: {len(prefix_server_name)}, "
          f"domain_server_name rows: {len(domain_server_name)}")

    print("[INFO] Merging mapping rules into main DataFrame...")
    df_domain = merge_rules_into_df(df_norm, prefix_server_name, domain_server_name)
    print(f"[INFO] After merging rules: {df_domain.shape[0]:,} rows, {df_domain.shape[1]} columns.")

    # Validate
    print("[INFO] Running server validation rules...")
    df_valid, manual_df = validate_server_names(df_domain)
    print("[INFO] Validation rule_id distribution:")
    print(df_valid["rule_id"].value_counts(dropna=False).head(20))
    print("[INFO] validation_status distribution:")
    print(df_valid["validation_status"].value_counts(dropna=False).head(20))
    print(f"[INFO] Manual review rows: {manual_df.shape[0]:,}")

    # Corrections
    print("[INFO] Applying server name corrections...")
    df_valid = apply_server_name_corrections(df_valid)
    print("[INFO] Top validated_server_name values after correction:")
    print(df_valid["validated_server_name"].value_counts().head(20))

    # FULL version kept for manual review + summaries
    df_valid_full = df_valid.copy()

    # Save manual review subset (FULL)
    manual_out = output_dir / f"manual_review_crossref_preprints_{latest_date_str}.parquet"
    manual_df.to_parquet(manual_out, index=False)
    print(f"[INFO] Saved manual review parquet to: {manual_out}")

    # ----------------------------------------------------------------
    # CREATE CLEAN EXPORT VERSION:
    #   - server_name = validated_server_name
    #   - drop helper / normalization columns you listed
    # ----------------------------------------------------------------
    df_for_export = df_valid_full.copy()
    df_for_export["server_name"] = df_for_export["validated_server_name"]

    DROP_COLS = [
        "gold_server_name",
        "doi_lc",
        "prefix_lc",
        "doi_prefix_from_text",
        "doi_suffix",
        "doi_prefix_first_token",
        "primary_domain",
        "primary_domain_extend",
        "domain_server_name",
        "prefix_server_name",
        "norm_domain",
        "norm_prefix",
        "norm_group",
        "norm_inst",
        "norm_pub",
        "validation_status",
        "confidence_score",
        "rule_id",
        "match_dom_text",
        "match_pref_text",
        "validated_server_name_old",
        "validated_server_name",  # we keep only `server_name`
    ]
    cols_to_drop = [c for c in DROP_COLS if c in df_for_export.columns]
    df_for_export = df_for_export.drop(columns=cols_to_drop)

    # Save main full labeled file (CLEAN, with server_name)
    full_out = output_dir / "all_crossref_labeled.parquet"
    df_for_export.to_parquet(full_out, index=False)
    print(f"[INFO] Saved full labeled parquet (clean, with server_name) to: {full_out}")

    # Save filtered labeled file (subset)
    cols_filtered = [
        "doi",
        "posted_date",
        "year",
        "publisher",
        "member",
        "prefix",
        "institution_name",
        "group_title",
        "server_name",
    ]
    cols_present = [c for c in cols_filtered if c in df_for_export.columns]
    df_valid_filtered = df_for_export[cols_present].copy()

    filtered_out = output_dir / "all_crossref_labeled_filtered.parquet"
    df_valid_filtered.to_parquet(filtered_out, index=False)
    print(f"[INFO] Saved filtered labeled parquet to: {filtered_out}")

    # ----------------------------------------------------------------
    # Summaries (use FULL df_valid_full that still has helper columns)
    # ----------------------------------------------------------------
    fields_to_summarize = [
        "validated_server_name",
        "validated_server_name_old",
        "primary_domain",
        "doi_prefix_first_token",
    ]
    print("[INFO] Building summary tables...")
    for fld in fields_to_summarize:
        if fld not in df_valid_full.columns:
            print(f"[WARN] skip summary for field {fld!r} (missing in df).")
            continue
        print(f"  - summarizing by field: {fld}")
        sdf = summarize_by_field(df_valid_full, fld, examples_k=args.examples_k)
        summary_out = output_dir / f"summary_crossref_preprints_{fld}_{latest_date_str}.csv"
        sdf.to_csv(summary_out, index=False, encoding="utf-8-sig")
        print(f"    saved: {summary_out}")

    print("[DONE] Crossref server labeling pipeline complete.")


if __name__ == "__main__":
    main()
