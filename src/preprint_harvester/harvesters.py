import os
import time
import json
import re
import requests
import pandas as pd
from datetime import datetime
from urllib.parse import urlparse, parse_qs, quote, unquote
from typing import Any, Dict, Optional, Union
from html import unescape

# ============================================================
#  Constants
# ============================================================
CROSSREF_WORKS = "https://api.crossref.org/works"
DATACITE_DOIS  = "https://api.datacite.org/dois"
OPENALEX_WORKS = "https://api.openalex.org/works"

DEFAULT_MAILTO = "your.email@example.com"
UA = f"Crossref-PreprintHarvester/3.1 (mailto:{DEFAULT_MAILTO})"

try:
    from tqdm import tqdm
    _HAVE_TQDM = True
except Exception:
    _HAVE_TQDM = False

# Timestamp to avoid overwriting on reruns
ts = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
# ============================================================
#  Generic helpers
# ============================================================
def _set_mailto(mailto: str):
    """Update global mailto + User-Agent for Crossref."""
    global UA, DEFAULT_MAILTO
    DEFAULT_MAILTO = mailto
    UA = f"Crossref-PreprintHarvester/3.1 (mailto:{mailto})"


def _date_from_parts(d):
    """Crossref date-parts → YYYY-MM-DD."""
    try:
        parts = (d or {}).get("date-parts", [[]])[0]
        if not parts:
            return None
        y = f"{parts[0]:04d}"
        m = f"{(parts[1] if len(parts) > 1 else 1):02d}"
        dd = f"{(parts[2] if len(parts) > 2 else 1):02d}"
        return f"{y}-{m}-{dd}"
    except Exception:
        return None


def _first(x, key=None):
    if not x:
        return None
    v = x[0]
    return v.get(key) if key and isinstance(v, dict) else v


def _json(obj):
    if obj is None:
        return None
    try:
        return json.dumps(obj, ensure_ascii=False, sort_keys=True)
    except Exception:
        return None


def _validate_date(date_str: str, name: str) -> str:
    """
    Ensure date_str is YYYY-MM-DD and return it normalized.
    Raise ValueError if invalid.
    """
    if not date_str:
        raise ValueError(f"{name} is empty or None")
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"{name} must be in YYYY-MM-DD format, got: {date_str}")
    return dt.date().isoformat()  # normalized "YYYY-MM-DD"



def _safe_server_dir_name(name: str) -> str:
    """
    Turn an arbitrary server name into a safe directory name.

    - Replaces spaces with '_'
    - Replaces or removes characters invalid on Windows: <>:"\\|?*
    - Collapses repeated underscores
    """
    if not isinstance(name, str):
        name = str(name)

    name = name.strip()
    # basic replacements
    name = name.replace(" ", "_")
    name = name.replace("/", "_")

    # remove/replace Windows-forbidden characters
    name = re.sub(r'[<>:"\\|?*]', "_", name)

    # 🔹 remove sequences of 3+ dots (the "...")
    name = re.sub(r"\.{3,}", "", name)

    # collapse multiple underscores
    name = re.sub(r"_+", "_", name)

    # avoid empty name
    name = name or "server"

    # optionally limit length
    return name[:50]

def _stamp_df(
    df: pd.DataFrame,
    *,
    server_name: str,
    backend: str,
    rule_tokens=None,
    rule_row_id=None
) -> pd.DataFrame:
    """
    Add server_name/backend columns so we don't need post-labeling later.
    Keeps provenance in the harvested outputs (and helps merging).
    """
    if df is None or df.empty:
        return df
    df = df.copy()

    # Put them first for visibility
    if "server_name" not in df.columns:
        df.insert(0, "server_name", server_name)
    else:
        df["server_name"] = server_name

    if "backend" not in df.columns:
        df.insert(1, "backend", backend)
    else:
        df["backend"] = backend

    if rule_tokens is not None and "rule_tokens" not in df.columns:
        df["rule_tokens"] = "/".join(sorted(set(rule_tokens)))

    if rule_row_id is not None and "rule_row_id" not in df.columns:
        df["rule_row_id"] = int(rule_row_id)

    return df

def _normalize_issn(x: str) -> str | None:
    if not x:
        return None
    s = str(x).strip().upper().replace(" ", "")
    s = s.strip('"').strip("'")
    s = s.replace("–", "-").replace("—", "-")
    # accept ####-#### only
    m = re.fullmatch(r"\d{4}-\d{3}[\dX]", s)
    return s if m else None

def _parse_issn_cell(val):
    # works with: "2516-2314", ' "2516-2314" ', "[2516-2314, 2050-084X]" etc
    raw = _parse_list_cell(val)
    out = []
    for r in raw:
        n = _normalize_issn(r)
        if n:
            out.append(n)
    # also handle when the cell is a single value (not comma-separated)
    if not out and pd.notna(val):
        n = _normalize_issn(str(val))
        if n:
            out = [n]
    return sorted(set(out))


def _parse_records_types_cell(val):
    """
    Parse records_types cell into normalized tokens.
    Accepts: "all", "preprint", "posted-content", "text", "preprint/text", "preprint, text"
    """
    if pd.isna(val) or val is None:
        return ["all"]
    s = str(val).strip().lower()
    if not s:
        return ["all"]

    # allow separators: / or , or ;
    s = s.replace("[", "").replace("]", "")
    parts = re.split(r"[/,;]", s)
    out = []
    for p in parts:
        p = p.strip().strip("'\"")
        if p:
            out.append(p)
    return out or ["all"]


def _resolve_record_types_for_backend(record_tokens, backend: str):
    tokens = [t.strip().lower() for t in (record_tokens or []) if str(t).strip()]
    if not tokens:
        tokens = ["all"]
    if "all" in tokens:
        tokens = ["all"]

    # ---- Crossref ----
    if backend == "crossref":
        alias = {
            "preprint": "posted-content",
            "posted-content": "posted-content",
            "article": "journal-article",
            "journal-article": "journal-article",
            "conference-paper": "proceedings-article",
            "conference-proceeding": "proceedings-article",
            "report": "report",
        }
        # ignore tokens Crossref can't support
        crossref_types = []
        for t in tokens:
            if t == "all":
                return (["all"], None, None)  # preserve your default
            if t in alias:
                crossref_types.append(alias[t])
        crossref_types = sorted(set(crossref_types)) or ["posted-content"]
        return (crossref_types, None, None)

    # ---- OpenAlex ----
    if backend == "openalex":
        alias = {
            "preprint": "preprint",
            "posted-content": "preprint",
            "article": "article",
            "journal-article": "article",
            "report": "report",
            "conference-paper": "proceedings-article",
            "conference-proceeding": "proceedings-article",
        }
        openalex_types = []
        for t in tokens:
            if t == "all":
                return (None, None, None)  # preserve your default
            if t in alias:
                openalex_types.append(alias[t])
        # openalex_types = sorted(set(openalex_types)) or ["preprint"]        
        openalex_types = sorted(set(openalex_types))
        if not openalex_types:
            openalex_types = ["preprint"]  # default when blank/unknown
            
        return (None, None, openalex_types)

    # ---- DataCite ----
    if backend == "datacite":
        if "all" in tokens:
            return (None, ["all"], None)
        return (None, tokens, None)

    return (None, None, None)

###########################################
_PUNCT_RE = re.compile(r"\s+([,.;:!?])")
_WS_RE = re.compile(r"\s+")
_QUOTE_FIX_RE = re.compile(r'\s+([”’"])')
_OPEN_QUOTE_FIX_RE = re.compile(r'([“‘"])\\s+')

def _cleanup_text(text: str | None) -> str | None:
    if not text or not isinstance(text, str):
        return None
    text = _WS_RE.sub(" ", text).strip()
    text = _PUNCT_RE.sub(r"\1", text)
    text = re.sub(r"\s+\)", ")", text)
    text = re.sub(r"\(\s+", "(", text)
    text = _QUOTE_FIX_RE.sub(r"\1", text)
    text = _OPEN_QUOTE_FIX_RE.sub(r"\1", text)
    return text or None

def abstract_inverted_index_to_text(abstract_inverted_index):
    """
    Convert OpenAlex 'abstract_inverted_index' to clean abstract text.

    Accepts:
      - dict: {"word":[pos,...], ...}
      - str: JSON string encoding that dict
      - str: already-plain text (returns cleaned version)
    """
    if abstract_inverted_index is None:
        return None

    # string input: try JSON -> else treat as plain text
    if isinstance(abstract_inverted_index, str):
        s = abstract_inverted_index.strip()
        if not s:
            return None
        try:
            abstract_inverted_index = json.loads(s)
        except Exception:
            return _cleanup_text(s)

    if not isinstance(abstract_inverted_index, dict) or not abstract_inverted_index:
        return None

    # Build list of (pos, word). Keep first word if duplicates at same pos.
    pos_to_word = {}
    for word, positions in abstract_inverted_index.items():
        if not positions:
            continue
        for p in positions:
            if p not in pos_to_word:
                pos_to_word[p] = word

    if not pos_to_word:
        return None

    # Safer reconstruction: sort positions (handles sparse or huge max_pos)
    ordered = [pos_to_word[p] for p in sorted(pos_to_word.keys())]
    text = " ".join(w for w in ordered if w)

    return _cleanup_text(text)


def _openalex_extract_abstract_inverted_index(r: pd.Series):
    """
    Return abstract_inverted_index as either dict or JSON string, or None.
    Looks in:
      1) abstract_inverted_index (preferred)
      2) abstract_inverted_index_json (if you stored it like that)
      3) raw_json (if abstract only lives there)
    """
    v = r.get("abstract_inverted_index")
    if v is not None and not (isinstance(v, float) and pd.isna(v)):
        return v

    v = r.get("abstract_inverted_index_json")
    if isinstance(v, str) and v.strip():
        return v

    raw = r.get("raw_json")
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None

    try:
        obj = raw if isinstance(raw, dict) else json.loads(raw) if isinstance(raw, str) else None
        if isinstance(obj, dict):
            return obj.get("abstract_inverted_index")
    except Exception:
        return None

    return None

def _openalex_get_abstract_idx_from_row(r: pd.Series):
    """
    Robustly return OpenAlex abstract_inverted_index from:
      1) r["abstract_inverted_index"] (dict or JSON string)
      2) r["abstract_inverted_index_json"] (JSON string)
      3) r["raw_json"] (dict or JSON string)
    """
    # 1) direct column
    v = r.get("abstract_inverted_index")
    if v is not None and not (isinstance(v, float) and pd.isna(v)):
        return v

    # 2) alternate name
    v = r.get("abstract_inverted_index_json")
    if isinstance(v, str) and v.strip():
        return v

    # 3) raw_json fallback
    raw = r.get("raw_json")
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None

    try:
        obj = raw if isinstance(raw, dict) else json.loads(raw) if isinstance(raw, str) else None
        if isinstance(obj, dict):
            return obj.get("abstract_inverted_index")
    except Exception:
        return None

    return None


def _json_or_none(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    if isinstance(x, str):
        return x.strip() or None
    try:
        return json.dumps(x, ensure_ascii=False)
    except Exception:
        return str(x)
# //////////////////
def _datacite_type_query_from_tokens(tokens: list[str]) -> str | None:
    """
    Build a DataCite query fragment for requested record types WITHOUT causing
    "everything is Text" duplication.

    Rules:
    - "text" => resourceTypeGeneral:"Text"
    - "report" => resourceType:"Report"
    - "journal-article"/"article" => resourceType:"Journal article"
    - "conference-paper" => resourceType:"Conference paper"
    - "conference-proceeding" => resourceType:"Conference proceeding" OR "Proceedings"
    - "preprint" is handled elsewhere via resource-type-id=Preprint (recommended)
    - "all" / "other" => no clause
    """
    if not tokens:
        return None

    # normalize
    norm = []
    for t in tokens:
        if t is None:
            continue
        t = str(t).strip().lower()
        if not t:
            continue
        if t in ("journal-article",):
            t = "article"
        norm.append(t)

    # if "all" present, no restriction
    if "all" in norm:
        return None

    clauses = []

    if "text" in norm:
        clauses.append('types.resourceType:"Text"')

    if "report" in norm:
        clauses.append('types.resourceType:"Report"')

    if "article" in norm:
        clauses.append('types.resourceType:"Journal article"')

    if "conference-paper" in norm:
        clauses.append('types.resourceType:"Conference paper"')

    if "conference-proceeding" in norm:
        clauses.append('types.resourceType:"Conference proceeding"')
        clauses.append('types.resourceType:"Proceedings"')

    if "other" in norm:
        clauses.append('types.resourceType:"Other"')

    # do not attempt to query "other"
    # do not handle "preprint" here (you already have resource-type-id=Preprint)

    clauses = sorted(set(clauses))
    if not clauses:
        return None

    return "(" + " OR ".join(clauses) + ")"


def _datacite_norm_resource_type_id(rt: str) -> str | None:
    """
    Normalize rule tokens into DataCite resource-type-id tokens.
    Expect: lowercase, hyphenated (already in your sheet).
    DataCite resource-type-id supports: text, report, preprint, journal-article,
    conference-paper, conference-proceeding, other, etc.
    """
    if not rt:
        return None
    s = str(rt).strip().lower()
    if not s:
        return None
    return s

def _datacite_join_resource_type_ids(resource_types: list[str]) -> str | None:
    """
    Build comma-separated resource-type-id list (DataCite accepts this).
    """
    norm = []
    seen = set()
    for rt in resource_types or []:
        t = _datacite_norm_resource_type_id(rt)
        if not t:
            continue
        if t in seen:
            continue
        seen.add(t)
        norm.append(t)
    return ",".join(norm) if norm else None


def _datacite_extract_abstract(descriptions_json_str: str | None) -> str | None:
    """
    DataCite descriptions: list of {description, descriptionType, lang, ...}
    We prefer Abstract, then first description.
    """
    if not descriptions_json_str:
        return None
    try:
        arr = json.loads(descriptions_json_str)
    except Exception:
        return None
    if not isinstance(arr, list) or not arr:
        return None

    # Prefer descriptionType == "Abstract"
    preferred = None
    fallback = None
    for d in arr:
        if not isinstance(d, dict):
            continue
        text = d.get("description")
        if isinstance(text, str):
            text = unescape(text).strip()
        if not text:
            continue

        if fallback is None:
            fallback = text

        if str(d.get("descriptionType") or "").lower() == "abstract":
            preferred = text
            break

    # Clean spacing a bit
    out = preferred or fallback
    if out:
        out = re.sub(r"\s+", " ", out).strip()
    return out or None


def _safe_lower(x):
    return str(x).strip().lower() if x is not None else ""

def _crossref_extract_license(licenses_json_str: str | None) -> tuple[str | None, str | None]:
    """
    Crossref 'license' is usually a list of dicts like:
      [{"URL": "...", "start": {...}, "delay-in-days": 0, "content-version": "vor"}]
    We pick the first URL; label is usually not provided, so we use URL as label fallback.
    """
    if not licenses_json_str:
        return (None, None)
    try:
        arr = json.loads(licenses_json_str)
    except Exception:
        return (None, None)

    if not isinstance(arr, list) or not arr:
        return (None, None)

    # Prefer any dict with URL
    for it in arr:
        if isinstance(it, dict):
            url = it.get("URL") or it.get("url")
            if isinstance(url, str) and url.strip():
                url = url.strip()
                # Crossref often has no explicit label; keep url as fallback label
                return (url, url)

    return (None, None)


def _datacite_extract_license(rights_list_json: str | None, rights_fallback=None) -> tuple[str | None, str | None]:
    """
    DataCite:
      - attributes.rightsList: list of dicts with possible keys:
        rights, rightsUri, schemeUri, rightsIdentifier, rightsIdentifierScheme, lang
      - attributes.rights (deprecated string)
    We pick the best label + URL we can find.
    """
    label = None
    url = None

    # 1) rightsList
    if rights_list_json:
        try:
            arr = json.loads(rights_list_json)
        except Exception:
            arr = None

        if isinstance(arr, list):
            # pick first item that has rightsUri (best)
            for it in arr:
                if not isinstance(it, dict):
                    continue
                cand_url = it.get("rightsUri") or it.get("rightsURI") or it.get("url")
                cand_label = it.get("rights") or it.get("rightsIdentifier") or it.get("rightsIdentifierScheme")
                if cand_url and str(cand_url).strip():
                    url = str(cand_url).strip()
                    if cand_label and str(cand_label).strip():
                        label = str(cand_label).strip()
                    else:
                        label = url
                    return (label, url)

            # otherwise pick first label-only
            for it in arr:
                if not isinstance(it, dict):
                    continue
                cand_label = it.get("rights") or it.get("rightsIdentifier") or it.get("rightsIdentifierScheme")
                if cand_label and str(cand_label).strip():
                    label = str(cand_label).strip()
                    return (label, None)

    # 2) deprecated single string fallback
    if rights_fallback and str(rights_fallback).strip():
        label = str(rights_fallback).strip()

    return (label, url)


def _openalex_extract_license(license_val) -> tuple[str | None, str | None]:
    """
    OpenAlex primary_location.license is often a URL or a short identifier.
    If it looks like a URL, put it in license_url_best as well.
    """
    if license_val is None:
        return (None, None)
    s = str(license_val).strip()
    if not s:
        return (None, None)
    if s.startswith("http://") or s.startswith("https://"):
        return (s, s)
    return (s, None)

def _funders_from_crossref(funder_json_str: str | None) -> tuple[str | None, int | None]:
    arr = _loads_json_safe(funder_json_str) or []
    if not isinstance(arr, list) or not arr:
        return None, None
    names = []
    for f in arr:
        if not isinstance(f, dict):
            continue
        n = f.get("name")
        if n:
            names.append(str(n).strip())
    flat = _join_unique(names)
    return flat, (len(names) if names else None)

def _funders_from_datacite(funding_refs_json_str: str | None) -> tuple[str | None, int | None]:
    arr = _loads_json_safe(funding_refs_json_str) or []
    if not isinstance(arr, list) or not arr:
        return None, None
    names = []
    for fr in arr:
        if not isinstance(fr, dict):
            continue
        # DataCite fundingReferences often has: funderName, funderIdentifier, awardNumber, awardTitle
        n = fr.get("funderName")
        if n:
            names.append(str(n).strip())
    flat = _join_unique(names)
    return flat, (len(names) if names else None)

def _funders_from_openalex(funders_json_str: str | None) -> tuple[str | None, int | None]:
    arr = _loads_json_safe(funders_json_str) or []
    if not isinstance(arr, list) or not arr:
        return None, None
    names = []
    for f in arr:
        if not isinstance(f, dict):
            continue
        # OpenAlex funders often has keys like: funder_display_name (and/or an id)
        n = f.get("funder_display_name") or f.get("display_name")
        if n:
            names.append(str(n).strip())
    flat = _join_unique(names)
    return flat, (len(names) if names else None)


# DOI normalization and merging -----------------------
_DOI_RE = re.compile(r"(10\.\d{4,9}/[^\s\"<>]+)", re.IGNORECASE)

def normalize_doi(x: str) -> str | None:
    if not x:
        return None
    x = str(x).strip()
    x = unquote(x)
    x = x.replace("https://doi.org/", "").replace("http://doi.org/", "")
    x = x.replace("doi:", "").strip()
    m = _DOI_RE.search(x)
    if not m:
        return None
    return m.group(1).lower()

def merge_dois(*values) -> str:
    """Merge any number of DOI containers/strings into one ';'-joined string."""
    out = []
    seen = set()

    def add_one(v):
        d = normalize_doi(v)
        if d and d not in seen:
            seen.add(d)
            out.append(d)

    for v in values:
        if not v:
            continue
        if isinstance(v, str):
            # allow ';' separated strings
            parts = [p.strip() for p in v.split(";")] if ";" in v else [v]
            for p in parts:
                add_one(p)
        elif isinstance(v, (list, tuple, set)):
            for item in v:
                add_one(item)
        else:
            add_one(v)

    return ";".join(out)

# Relation extraction ---------------------------------------
VERSION_REL_KEYS = {
    # classic version relations
    "is-version-of",
    "has-version",

    # Crossref "updates" relations (your key requirement)
    "updated-by",
    "updated-to",
    "updated-from",
    "update-to",
    "update-from",

    # optional: sometimes appears as "is-updated-by" / "updates" in some metadata
    "is-updated-by",
    "updates",
}

PREPRINT_REL_KEYS = {"is-preprint-of", "has-preprint"}
REVIEW_REL_KEYS    = {"has-review", "is-reviewed-by"}  # keep what you use

def _extract_dois_from_relation_value(rel_value):
    """rel_value can be list/dict/str; returns list of normalized dois."""
    dois = []

    def add(v):
        d = normalize_doi(v)
        if d:
            dois.append(d)

    if not rel_value:
        return dois

    if isinstance(rel_value, str):
        add(rel_value)
        return dois

    if isinstance(rel_value, dict):
        # sometimes a single object
        _id = rel_value.get("id") or rel_value.get("DOI") or rel_value.get("doi")
        add(_id)
        return dois

    if isinstance(rel_value, list):
        for item in rel_value:
            if isinstance(item, str):
                add(item)
                continue
            if isinstance(item, dict):
                _id = item.get("id") or item.get("DOI") or item.get("doi")
                # if id-type exists, respect it, but still try to parse
                add(_id)
            else:
                add(item)
        return dois

    # fallback
    add(rel_value)
    return dois


def _extract_relations_crossref(relation_obj: dict | None):
    """
    Returns: (is_preprint_of, has_preprint, is_version_of, has_review)
    Each output is a ';' joined DOI string (deduped).
    """
    if not isinstance(relation_obj, dict):
        return ("", "", "", "")

    preprint_of = []
    has_preprint = []
    version_of = []
    has_review = []

    for key, value in relation_obj.items():
        k = str(key).strip().lower()

        if k in PREPRINT_REL_KEYS:
            if k == "is-preprint-of":
                preprint_of += _extract_dois_from_relation_value(value)
            elif k == "has-preprint":
                has_preprint += _extract_dois_from_relation_value(value)

        elif k in VERSION_REL_KEYS:
            version_of += _extract_dois_from_relation_value(value)

        elif k in REVIEW_REL_KEYS:
            has_review += _extract_dois_from_relation_value(value)

    # dedupe + join
    return (
        merge_dois(preprint_of),
        merge_dois(has_preprint),
        merge_dois(version_of),
        merge_dois(has_review),
    )

# ============================================================
#  BIG CANONICAL SCHEMA (exact columns you drafted)
# ============================================================

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
    'raw_relationships_json',
    'raw_json',
]

def _coalesce(*vals):
    for v in vals:
        if v is None:
            continue
        if isinstance(v, float) and pd.isna(v):
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return v
    return None

def _norm_doi(doi: str | None) -> str | None:
    if not doi:
        return None
    d = str(doi).strip()
    d = d.replace("https://doi.org/", "").replace("http://doi.org/", "")
    d = d.replace("doi:", "").strip()
    return d.lower() if d else None

def _doi_url(doi_norm: str | None) -> str | None:
    return f"https://doi.org/{doi_norm}" if doi_norm else None

def _derive_prefix_from_doi(doi_norm: str | None) -> str | None:
    if not doi_norm or "/" not in doi_norm:
        return None
    return doi_norm.split("/", 1)[0]

def _year_from_date(d: str | None) -> int | None:
    if not d:
        return None
    try:
        return int(str(d)[:4])
    except Exception:
        return None

def _strip_jats(abstract_raw: str | None) -> str | None:
    # Crossref abstracts often are JATS XML; keep it simple & robust.
    if not abstract_raw or not isinstance(abstract_raw, str):
        return None
    txt = unescape(abstract_raw)
    txt = re.sub(r"<[^>]+>", " ", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt or None

def _loads_json_safe(s):
    if not s or not isinstance(s, str):
        return None
    try:
        return json.loads(s)
    except Exception:
        return None

def _join_unique(vals):
    vals = [v.strip() for v in (vals or []) if isinstance(v, str) and v.strip()]
    # preserve order while unique
    seen = set()
    out = []
    for v in vals:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return "; ".join(out) if out else None

def _flat_from_crossref(authors_json_str):
    authors = _loads_json_safe(authors_json_str) or []
    names, insts, countries = [], [], []

    for a in authors if isinstance(authors, list) else []:
        given = (a or {}).get("given")
        family = (a or {}).get("family")
        literal = (a or {}).get("name") or (a or {}).get("literal")
        nm = ", ".join([x for x in [family, given] if x]) or literal
        if nm:
            names.append(nm)

        # affiliations (names only)
        for aff in (a or {}).get("affiliation") or []:
            an = (aff or {}).get("name")
            if an:
                insts.append(an)

    return _join_unique(names), _join_unique(insts), _join_unique(countries)

def _flat_from_datacite(creators_json_str):
    creators = _loads_json_safe(creators_json_str) or []
    names, insts, countries = [], [], []

    for c in creators if isinstance(creators, list) else []:
        nm = (c or {}).get("name")
        if not nm:
            gn = (c or {}).get("givenName")
            fn = (c or {}).get("familyName")
            nm = ", ".join([x for x in [gn, fn] if x]) or None
        if nm:
            names.append(nm)

        aff = (c or {}).get("affiliation")
        if isinstance(aff, str):
            insts.append(aff)
        elif isinstance(aff, list):
            for a in aff:
                if isinstance(a, str):
                    insts.append(a)
                elif isinstance(a, dict):
                    # DataCite sometimes uses {"name": "..."}
                    an = a.get("name")
                    if an:
                        insts.append(an)

    return _join_unique(names), _join_unique(insts), _join_unique(countries)

def _flat_from_openalex(authorships_json_str):
    authorships = _loads_json_safe(authorships_json_str) or []
    names, insts, countries = [], [], []

    for au in authorships if isinstance(authorships, list) else []:
        author = (au or {}).get("author") or {}
        nm = author.get("display_name")
        if nm:
            names.append(nm)

        for inst in (au or {}).get("institutions") or []:
            iname = (inst or {}).get("display_name")
            if iname:
                insts.append(iname)
            ctry = (inst or {}).get("country_code")
            if ctry:
                countries.append(ctry)

    return _join_unique(names), _join_unique(insts), _join_unique(countries)

def add_flat_columns(canon_df: pd.DataFrame, backend: str) -> pd.DataFrame:
    if canon_df is None or canon_df.empty:
        return canon_df
    backend = (backend or "").lower()
    df = canon_df.copy()

    authors_flat = []
    insts_flat = []
    countries_flat = []

    for _, r in df.iterrows():
        if backend == "crossref":
            a, i, c = _flat_from_crossref(r.get("authors_json"))
        elif backend == "datacite":
            a, i, c = _flat_from_datacite(r.get("authors_json"))  # note: your canon maps creators_json -> authors_json
        elif backend == "openalex":
            a, i, c = _flat_from_openalex(r.get("authors_json"))  # canon maps authorships_json -> authors_json
        else:
            a, i, c = (None, None, None)

        authors_flat.append(a)
        insts_flat.append(i)
        countries_flat.append(c)

    df["authors_flat"] = authors_flat
    df["institutions_flat"] = insts_flat
    df["countries_flat"] = countries_flat
    return df

def _pick_crossref_date_published(r: pd.Series) -> tuple[str|None, str|None]:
    # prefer peer-reviewed-ish dates first, fallback to posted/created
    for col, src in [
        ("issued_date", "issued_date"),
        ("published_date", "published_date"),
        ("published_print_date", "published_print_date"),
        ("posted_date", "posted_date"),
        ("created_date", "created_date"), # created
        ("deposited_date", "deposited_date"),
        ("indexed_date", "indexed_date"),
    ]:
        v = r.get(col)
        if v:
            return v, src
    return None, None

def _pick_datacite_date_published(r: pd.Series) -> tuple[str|None, str|None]:
    # published can be year/int or string; normalize if possible
    pub = r.get("published")
    if isinstance(pub, str) and len(pub) >= 10:
        return pub[:10], "published"
    if r.get("published_year"):
        # store year only as YYYY-01-01? better: keep date_published None and year separately
        return None, "published_year"
    if r.get("registered"):
        return r.get("registered"), "registered"
    if r.get("created"):
        return r.get("created"), "created"
    return None, None

def _infer_is_oa_and_status(backend: str, r: pd.Series) -> tuple[bool|None, str|None]:
    if backend == "openalex":
        # you already store open_access bits
        is_oa = r.get("primary_location_is_oa")
        oa_status = r.get("primary_location_oa_status")
        return (bool(is_oa) if is_oa is not None else None), oa_status
    # Crossref/DataCite: not always explicit; leave None unless you have strong signal
    return None, None

def _infer_fulltext_pdf_url(backend: str, r: pd.Series) -> str | None:
    # Crossref has "links_json" from m.get("link"); DataCite has urlAlternate/citations/etc; OpenAlex sometimes has open_access
    if backend == "crossref":
        links_json = r.get("links_json")
        if links_json:
            try:
                lst = json.loads(links_json)
                if isinstance(lst, list):
                    # pick first PDF if present
                    for it in lst:
                        if (it or {}).get("content-type", "").lower() == "application/pdf":
                            return it.get("URL")
            except Exception:
                pass
        return None
    if backend == "openalex":
        # OpenAlex often provides landing page; PDF is not always direct
        return None
    if backend == "datacite":
        return None
    return None

def _type_canonical_from_backend(backend: str, r: pd.Series) -> str | None:
    if backend == "crossref":
        # t = (r.get("type") or "").lower()
        # if t == "posted-content":
        #     return "preprint"
        # if t == "journal-article":
        #     return "journal-article"
        # return t or None

        # prefer subtype if present; else type
        return _coalesce(r.get("subtype"), r.get("type"))

    if backend == "openalex":
        return (r.get("type") or None)

    if backend == "datacite":
        # prefer resource_type_general if present; else resource_type
        return _coalesce(r.get("resource_type_general"), r.get("resource_type"))
    return None

def _is_preprint_candidate(backend: str, r: pd.Series) -> bool | None:
    if backend == "crossref":
        t = (r.get("type") or "").lower()
        return True if t == "posted-content" else False if t else None
    if backend == "openalex":
        t = (r.get("type") or "").lower()
        return True if t == "preprint" else False if t else None
    if backend == "datacite":
        # DataCite: if resource-type-id included preprint OR resourceTypeGeneral indicates it
        rtg = (r.get("resource_type_general") or "").lower()
        rt = (r.get("resource_type") or "").lower()
        if "preprint" in rtg or "preprint" in rt:
            return True
        return None
    return None

def _build_big_canon_crossref(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=BIG_CANON_COLS)

    out = pd.DataFrame()
    out["server_name"] = df.get("server_name")
    out["backend"] = "crossref"

    out["doi"] = df.get("doi").apply(_norm_doi)
    out["doi_url"] = out["doi"].apply(_doi_url)

    out["source_work_id"] = out["doi"]  # Crossref: DOI-centric

    out["landing_page_url"] = df.apply(lambda r: _coalesce(r.get("primary_url"), r.get("url"), _doi_url(_norm_doi(r.get("doi")))), axis=1)
    out["url_best"] = out["landing_page_url"]

    out["prefix"] = df.apply(lambda r: _coalesce(r.get("prefix"), _derive_prefix_from_doi(_norm_doi(r.get("doi")))), axis=1)
    out["member_id"] = df.get("member")
    out["client_id"] = None
    out["provider_id"] = None
    out["source_registry"] = "crossref"

    out["publisher"] = df.get("publisher")
    out["container_title"] = df.get("container_title")
    out["institution_name"] = df.get("institution_name")
    out["group_title"] = df.get("group_title")
    out["issn"] = df.get("issn")

    out["title"] = df.get("title")
    out["original_title"] = df.get("original_title")
    out["short_title"] = df.get("short_title")
    out["subtitle"] = df.get("subtitle")
    out["language"] = df.get("language")

    out["type_backend_raw"] = df.get("type")
    out["subtype_backend_raw"] = df.get("subtype")
    out["type_canonical"] = df.apply(lambda r: _type_canonical_from_backend("crossref", r), axis=1)

    out["is_paratext"] = None
    out["is_preprint_candidate"] = df.apply(lambda r: _is_preprint_candidate("crossref", r), axis=1)

    out["date_created"] = df.get("created_date")
    out["date_posted"] = df.get("posted_date")
    out["date_deposited"] = df.get("deposited_date")
    out["date_indexed"] = df.get("indexed_date")
    out["date_updated"] = None  # Crossref doesn't have "updated" like DataCite
    out["date_registered"] = None
    out["date_issued"] = df.get("issued_date")
    out["date_published_online"] = df.get("published_online_date")

    dp, dpsrc = zip(*df.apply(_pick_crossref_date_published, axis=1))
    # out["date_published"] = list(dp)
    out["date_published"] = df.get("published_date")
    out["date_published_source"] = list(dpsrc)

    out["publication_year"] = out["date_published"].apply(_year_from_date)
    out["date_posted_source"] = out["date_posted"].apply(lambda x: "posted_date" if x else None)

    out["is_oa"], out["oa_status"] = None, None
    # Crossref: parse licenses_json if available
    license_label, license_url = zip(*df.get("licenses_json", pd.Series([None]*len(df))).apply(_crossref_extract_license))
    out["license"] = list(license_label)
    out["license_url_best"] = list(license_url)

    # fallback if parsing didn't find anything
    out["license"] = out["license"].fillna(df.get("license_url"))
    out["license_url_best"] = out["license_url_best"].fillna(df.get("license_url"))

    out["abstract_raw"] = df.get("abstract_raw")
    out["abstract_text"] = df.get("abstract_raw").apply(_strip_jats)

    out["links_json_best"] = df.get("links_json")
    out["fulltext_pdf_url"] = df.apply(lambda r: _infer_fulltext_pdf_url("crossref", r), axis=1)

    out["authors_json"] = df.get("authors_json")
    out["contributors_json"] = df.get("contributors_json")
    out["editors_json"] = df.get("editors_json")
    # in _build_big_canon_crossref()
    out["funders_json"] = df.get("funder_json")

    flat_count = df.get("funder_json", pd.Series([None]*len(df))).apply(_funders_from_crossref)
    out["funders_flat"] = flat_count.apply(lambda x: x[0])
    out["funders_count"] = flat_count.apply(lambda x: x[1])

    out["subjects_json"] = df.get("subjects_json")
    out["concepts_json"] = None
    out["topics_json"] = None

    out["cited_by_count"] = df.get("is_referenced_by_count")
    out["cited_by_count_datacite"] = None
    out["cited_by_count_openalex"] = None
    out["is_referenced_by_count_crossref"] = df.get("is_referenced_by_count")
    out["reference_count"] = df.get("reference_count")
    out["references_json"] = df.get("references_json")

    out["relations_json"] = df.get("relation_json")
    out["has_preprint"] = df.get("has_preprint")
    out["is_preprint_of"] = df.get("is_preprint_of")
    out["has_review"] = df.get("has_review")   # ✅ NEW

    # "has_published_version" is not directly provided; infer if is_preprint_of exists
    out["has_published_version"] = out["is_preprint_of"].apply(lambda x: True if isinstance(x, str) and x.strip() else False if x is not None else None)
    out["published_version_ids_json"] = None

    out["is_version_of"] = df.get("is_version_of")
    out["version_of_ids_json"] = df.get("version_of_ids_json")

    out["version_label"] = df.get("version_label")
    # ---------------------------------------
    # Parent DOI extraction from update-to relationships
    # ---------------------------------------
    def _extract_update_to_children(update_to_json):
        """Return list of child DOIs from update-to JSON."""
        if update_to_json is None or (isinstance(update_to_json, float) and pd.isna(update_to_json)):
            return []
        try:
            arr = json.loads(update_to_json) if isinstance(update_to_json, str) else update_to_json
        except Exception:
            return []
        if not isinstance(arr, list):
            return []
        out = []
        for u in arr:
            if isinstance(u, dict):
                d = u.get("DOI") or u.get("doi")
                if d:
                    out.append(str(d).strip().lower())
        return out

    # parent = the current DOI row
    df2 = df.copy()
    df2["doi_norm"] = df2["doi"].astype(str).str.strip().str.lower()

    # Build edge list parent -> child
    edges = []
    for parent_doi, update_to_json in zip(df2["doi_norm"], df2.get("update_to_json", [None]*len(df2))):
        for child_doi in _extract_update_to_children(update_to_json):
            edges.append((child_doi, parent_doi))

    # Invert: child -> parent (if multiple parents exist, keep first or join)
    child_to_parent = {}
    for child, parent in edges:
        child_to_parent.setdefault(child, parent)  # keep first seen

    # Now assign parent_doi to each DOI row (child rows will match)
    df2["parent_doi"] = df2["doi_norm"].map(child_to_parent)

    # then in your canonical 'out':
    out["parent_doi"] = df2["parent_doi"]
    #---------------------------------------

    out["update_to_json"] = df.get("update_to_json")
    out["update_policy"] = df.get("update_policy")

    out["rule_tokens"] = df.get("rule_tokens")
    out["rule_row_id"] = df.get("rule_row_id")

    out["raw_relationships_json"] = None
    out["raw_json"] = df.get("raw_json")

    # record_id = stable row id
    out["record_id"] = out.apply(lambda r: f"crossref::{r.get('doi')}" if r.get("doi") else None, axis=1)

    # Ensure all columns exist + order
    for c in BIG_CANON_COLS:
        if c not in out.columns:
            out[c] = None
    return out[BIG_CANON_COLS].copy()

# ------------------------------------------------------------
# DataCite
# ------------------------------------------------------------
def _build_big_canon_datacite(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=BIG_CANON_COLS)

    out = pd.DataFrame()
    out["server_name"] = df.get("server_name")
    out["backend"] = "datacite"

    out["doi"] = df.get("doi").apply(_norm_doi)
    out["doi_url"] = out["doi"].apply(_doi_url)
    out["source_work_id"] = out["doi"]

    out["landing_page_url"] = df.apply(lambda r: _coalesce(r.get("url"), _doi_url(_norm_doi(r.get("doi")))), axis=1)
    out["url_best"] = out["landing_page_url"]

    out["prefix"] = df.apply(lambda r: _coalesce(r.get("prefix"), _derive_prefix_from_doi(_norm_doi(r.get("doi")))), axis=1)
    out["member_id"] = None
    out["client_id"] = df.get("client_id")
    out["provider_id"] = df.get("provider_id")
    out["source_registry"] = "datacite"

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

    # backend raw type fields
    out["type_backend_raw"] = df.get("resource_type_general")
    out["subtype_backend_raw"] = df.get("resource_type")

    out["type_canonical"] = df.apply(lambda r: _type_canonical_from_backend("datacite", r), axis=1)
    out["is_paratext"] = None
    out["is_preprint_candidate"] = df.apply(lambda r: _is_preprint_candidate("datacite", r), axis=1)

    out["date_created"] = df.get("created")
    out["date_posted"] = None
    out["date_deposited"] = None
    out["date_indexed"] = None
    out["date_updated"] = df.get("updated")
    out["date_registered"] = df.get("registered")
    out["date_issued"] = None
    out["date_published_online"] = None

    dp, dpsrc = zip(*df.apply(_pick_datacite_date_published, axis=1))
    # out["date_published"] = list(dp)
    out["date_published"] = df.get("published")
    out["date_published_source"] = list(dpsrc)

    out["publication_year"] = df.get("published_year")
    out["date_posted_source"] = None

    out["is_oa"], out["oa_status"] = None, None
    # out["license"] = df.get("rights")
    # out["license_url_best"] = df.get("rights_list_json")  # you could parse rights_list_json later
    lic_pairs = df.apply(
        lambda r: _datacite_extract_license(r.get("rights_list_json"), r.get("rights")),
        axis=1
    )
    out["license"] = lic_pairs.apply(lambda x: x[0])
    out["license_url_best"] = lic_pairs.apply(lambda x: x[1])

    out["abstract_raw"] = df.get("descriptions_json")
    out["abstract_text"] = df.get("descriptions_json").apply(_datacite_extract_abstract)

    out["links_json_best"] = df.get("url_alternate_json")
    out["fulltext_pdf_url"] = None

    out["authors_json"] = df.get("creators_json")
    out["contributors_json"] = df.get("contributors_json")
    out["editors_json"] = None
    # in _build_big_canon_datacite()
    out["funders_json"] = df.get("funding_refs_json")

    flat_count = df.get("funding_refs_json", pd.Series([None]*len(df))).apply(_funders_from_datacite)
    out["funders_flat"] = flat_count.apply(lambda x: x[0])
    out["funders_count"] = flat_count.apply(lambda x: x[1])

    out["subjects_json"] = df.get("subjects_json")
    out["concepts_json"] = None
    out["topics_json"] = None

    out["cited_by_count"] = df.get("citation_count")
    out["cited_by_count_datacite"] = df.get("citation_count")
    out["cited_by_count_openalex"] = None
    out["is_referenced_by_count_crossref"] = None
    out["reference_count"] = df.get("reference_count")
    out["references_json"] = df.get("references_json")

    # relations: DataCite relatedIdentifiers exist, but not parsed into has_preprint/is_preprint_of yet
    out["relations_json"] = df.get("related_ids_json")
    out["has_preprint"] = None
    out["is_preprint_of"] = None

    out["has_published_version"] = None
    out["published_version_ids_json"] = None

    out["is_version_of"] = None
    out["version_of_ids_json"] = None

    out["update_to_json"] = None
    out["update_policy"] = None
    out["version_label"] = df.get("version_label")
    out["parent_doi"] = None #df.get("parent_doi")

    out["rule_tokens"] = df.get("rule_tokens")
    out["rule_row_id"] = df.get("rule_row_id")
    out["raw_relationships_json"] = df.get("raw_relationships_json")
    out["raw_json"] = df.get("raw_json")

    out["record_id"] = out.apply(lambda r: f"datacite::{r.get('doi')}" if r.get("doi") else None, axis=1)

    for c in BIG_CANON_COLS:
        if c not in out.columns:
            out[c] = None
    return out[BIG_CANON_COLS].copy()

def _build_big_canon_openalex(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=BIG_CANON_COLS)

    out = pd.DataFrame()
    out["server_name"] = df.get("server_name")
    out["backend"] = "openalex"

    out["doi"] = df.get("doi").apply(_norm_doi)
    out["doi_url"] = out["doi"].apply(_doi_url)

    out["source_work_id"] = df.get("openalex_id")  # OpenAlex ID
    out["landing_page_url"] = df.apply(lambda r: _coalesce(r.get("primary_location_landing_page_url"), _doi_url(_norm_doi(r.get("doi")))), axis=1)
    out["url_best"] = out["landing_page_url"]

    out["prefix"] = out["doi"].apply(_derive_prefix_from_doi)
    out["member_id"] = None
    out["client_id"] = None
    out["provider_id"] = None
    out["source_registry"] = "openalex"

    out["publisher"] = None
    out["container_title"] = None
    out["institution_name"] = None
    out["group_title"] = None
    out["issn"] = df.get("issn")

    out["title"] = df.get("title")
    out["original_title"] = None
    out["short_title"] = None
    out["subtitle"] = None
    out["language"] = df.get("language")  # OpenAlex language

    out["type_backend_raw"] = df.get("type")
    out["subtype_backend_raw"] = None
    out["type_canonical"] = df.get("type")

    out["is_paratext"] = df.get("is_paratext")
    out["is_preprint_candidate"] = df.apply(lambda r: _is_preprint_candidate("openalex", r), axis=1)

    out["date_created"] = df.get("created_date") #
    out["date_posted"] = None
    out["date_deposited"] = None
    out["date_indexed"] = None
    out["date_updated"] = df.get("updated_date") # OpenAlex updated_date
    out["date_registered"] = None
    out["date_issued"] = None
    out["date_published_online"] = None

    out["date_published"] = df.get("publication_date")
    out["publication_year"] = df.get("publication_year")
    out["date_published_source"] = out["date_published"].apply(lambda x: "openalex.publication_date" if x else None)
    out["date_posted_source"] = None

    is_oa, oa_status = zip(*df.apply(lambda r: _infer_is_oa_and_status("openalex", r), axis=1))
    out["is_oa"] = list(is_oa) # is_oa from primary location
    out["oa_status"] = list(oa_status) # oa status from primary location
    # out["license"] = df.get("license")  # license from primary location
    # out["license_url_best"] = df.get("license_id") # license URL from primary location
    lic_pairs = df.get("license", pd.Series([None]*len(df))).apply(_openalex_extract_license)
    out["license"] = lic_pairs.apply(lambda x: x[0])
    # out["license_url_best"] = lic_pairs.apply(lambda x: x[1])
    out["license_url_best"] = df.get("license_id") 

    # --- abstract (robust) ---
    # Prefer the JSON string column (survives CSV/parquet better), then dict, then raw_json fallback.
    abs_src = df.get("abstract_inverted_index_json")

    if abs_src is None:
        abs_src = df.get("abstract_inverted_index")

    if abs_src is None and "raw_json" in df.columns:
        # fallback: extract from raw_json
        def _abs_from_raw(x):
            if x is None or (isinstance(x, float) and pd.isna(x)):
                return None
            try:
                obj = x if isinstance(x, dict) else json.loads(x) if isinstance(x, str) else None
                if isinstance(obj, dict):
                    return obj.get("abstract_inverted_index")
            except Exception:
                return None
            return None
        abs_src = df["raw_json"].apply(_abs_from_raw)

    out["abstract_raw"] = abs_src
    out["abstract_text"] = abs_src.apply(abstract_inverted_index_to_text) if abs_src is not None else None

    out["links_json_best"] = None
    out["fulltext_pdf_url"] = df.get("primary_location_fulltext_pdf_url") #

    out["authors_json"] = df.get("authorships_json")
    out["contributors_json"] = None
    out["editors_json"] = None
    # in _build_big_canon_openalex()
    out["funders_json"] = df.get("funders_json")

    flat_count = df.get("funders_json", pd.Series([None]*len(df))).apply(_funders_from_openalex)
    out["funders_flat"] = flat_count.apply(lambda x: x[0])
    out["funders_count"] = flat_count.apply(lambda x: x[1])

    out["subjects_json"] = None
    out["concepts_json"] = df.get("concepts_json")
    out["topics_json"] = df.get("topics_json") # to verify for primary topics

    out["cited_by_count"] = df.get("cited_by_count")
    out["cited_by_count_datacite"] = None
    out["cited_by_count_openalex"] = df.get("cited_by_count")
    out["is_referenced_by_count_crossref"] = None
    out["reference_count"] = df.get("referenced_works_count") #
    out["references_json"] = df.get("references_json") # 

    out["relations_json"] = None
    out["has_preprint"] = None
    out["is_preprint_of"] = None

    out["has_published_version"] = None
    out["published_version_ids_json"] = None

    out["is_version_of"] = None
    out["version_of_ids_json"] = None

    out["update_to_json"] = None
    out["update_policy"] = None
    out["version_label"] = None

    out["rule_tokens"] = df.get("rule_tokens")
    out["rule_row_id"] = df.get("rule_row_id")
    out["raw_relationships_json"] = None
    out["raw_json"] = df.get("raw_json")

    out["record_id"] = out.apply(
        lambda r: f"openalex::{r.get('source_work_id').split('/')[-1]}" if r.get("source_work_id") else (f"openalex::{r.get('doi')}" if r.get("doi") else None),
        axis=1
    )

    for c in BIG_CANON_COLS:
        if c not in out.columns:
            out[c] = None
    return out[BIG_CANON_COLS].copy()


def build_big_canonical(df: pd.DataFrame, backend: str) -> pd.DataFrame:
    backend = (backend or "").lower()
    if backend == "crossref":
        return _build_big_canon_crossref(df)
    if backend == "datacite":
        return _build_big_canon_datacite(df)
    if backend == "openalex":
        return _build_big_canon_openalex(df)
    return pd.DataFrame(columns=BIG_CANON_COLS)


def union_big_by_doi(canon_frames: list[pd.DataFrame], prefer=("crossref", "datacite", "openalex")) -> pd.DataFrame:
    """
    1 row per DOI (canonical union). Prefers backend order when multiple present.
    """
    frames = [d for d in (canon_frames or []) if d is not None and not d.empty]
    if not frames:
        return pd.DataFrame(columns=BIG_CANON_COLS)

    all_df = pd.concat(frames, ignore_index=True)

    all_df["doi"] = all_df["doi"].apply(_norm_doi)
    all_df = all_df[all_df["doi"].notna() & (all_df["doi"].astype(str).str.len() > 0)].copy()

    rank = {b: i for i, b in enumerate(prefer)}
    all_df["_pref"] = all_df["backend"].map(lambda x: rank.get(str(x).lower(), 999))

    all_df.sort_values(by=["doi", "_pref"], ascending=[True, True], inplace=True)
    winners = all_df.drop_duplicates(subset=["doi"], keep="first").copy()
    winners.drop(columns=["_pref"], inplace=True, errors="ignore")

    return winners[BIG_CANON_COLS].copy()

def finalize_for_export(canon_df: pd.DataFrame, backend: str) -> pd.DataFrame:
    backend = (backend or "").lower()
    keep = BIG_CANON_COLS
    out = canon_df.copy()

    for c in keep:
        if c not in out.columns:
            out[c] = None

    # ✅ ensure raw_json is serialized text
    if "raw_json" in out.columns:
        out["raw_json"] = out["raw_json"].apply(lambda x: x if isinstance(x, str) else _json(x))

    return out[keep].copy()


def add_canonical_columns(df: pd.DataFrame, backend: str) -> pd.DataFrame:
    """
    Backward-compatible wrapper.
    Converts a backend-specific harvested df into the BIG canonical schema,
    then adds flat columns and ensures export-ready columns exist.
    """
    canon = build_big_canonical(df, backend=backend)
    canon = add_flat_columns(canon, backend=backend)
    canon = finalize_for_export(canon, backend=backend)
    return canon

# ============================================================
#  Example URL builders (for summary)
# ============================================================
def _build_crossref_example_url(date_start, date_end, params, mailto):
    """
    Build a human-readable Crossref example URL for the summary.
    Uses first prefix/member/group-title/issn when available.

    IMPORTANT:
      - If crossref_types == ["posted-content"] -> use posted-date + type filter
      - Else (including ["all"]) -> use deposited-date and no type filter
    """
    if not date_start or not date_end:
        return None

    crossref_types = params.get("crossref_types") or ["posted-content"]

    only_posted_content = (
        len(crossref_types) == 1 and crossref_types[0] == "posted-content"
    )

    if only_posted_content:
        filters = [
            f"from-posted-date:{date_start}",
            f"until-posted-date:{date_end}",
            "type:posted-content",
        ]
    # else:
    #     filters = [
    #         f"from-deposited-date:{date_start}",
    #         f"until-deposited-date:{date_end}",
    #     ]
    #     # only add type if not "all"
    #     if crossref_types != ["all"]:
    #         filters.append(f"type:{crossref_types[0]}")

    else:
        filters = [
            f"from-deposit-date:{date_start}",
            f"until-deposit-date:{date_end}",
        ]
        if crossref_types != ["all"]:
            filters.append(f"type:{crossref_types[0]}")



    prefixes = params.get("prefixes") or []
    if prefixes:
        filters.append(f"prefix:{prefixes[0]}")

    members = params.get("members") or []
    if members:
        filters.append(f"member:{members[0]}")

    gts = params.get("group_titles_exact") or []
    if gts:
        filters.append(f"group-title:{gts[0]}")

    issns = params.get("issns") or []
    if issns:
        filters.append(f"issn:{issns[0]}")

    flt_str = ",".join(filters)
    return (
        f"{CROSSREF_WORKS}"
        f"?filter={flt_str}"
        f"&rows=5"
        f"&cursor=*"
        f"&mailto={requests.utils.quote(mailto)}"
    )




def _build_datacite_example_url(
    date_start,
    date_end,
    client_ids,
    resource_types,
    mailto,
    types_query_override: str | None = None,
    doi_prefix_query: str | None = None,
):
    client_ids = [c for c in (client_ids or []) if c]
    if not client_ids:
        return None
    cid = client_ids[0]

    params = [f"client-id={quote(cid)}"]

    # resource-type-id (only if not 'all')
    resource_types = [str(rt).strip().lower() for rt in (resource_types or []) if rt]
    want_all = ("all" in resource_types) or (len(resource_types) == 0)
    if not want_all:
        rtid = _datacite_join_resource_type_ids(resource_types)
        if rtid:
            params.append("resource-type-id=" + quote(rtid))

    # ---- Build query in the order you want: DOI first, then registered ----
    q_parts = []

    # 1) optional type override first (if you ever use it)
    if types_query_override:
        q_parts.append(types_query_override)

    # 2) DOI prefix query first (this is your goal)
    if doi_prefix_query:
        # if doi_prefix_query already has parentheses, keep it
        q_parts.append(doi_prefix_query if doi_prefix_query.startswith("(") else f"({doi_prefix_query})")

    # 3) registered date window second
    if date_start and date_end:
        q_parts.append(f"registered:[{date_start} TO {date_end}]")
    elif date_start:
        q_parts.append(f"registered:[{date_start} TO *]")
    elif date_end:
        q_parts.append(f"registered:[* TO {date_end}]")

    if q_parts:
        q = " AND ".join(q_parts)

        # Encode spaces as %20, keep []:()".* readable
        params.append("query=" + quote(q, safe='[]:()".*'))

    # ✅ your requested form: totals-only
    params.append("page[size]=0")

    return DATACITE_DOIS + "?" + "&".join(params)



def _build_openalex_example_url(date_start, date_end, source_ids, mailto, openalex_types=None):
    """
    Build a human-readable OpenAlex example URL for the summary.
    - openalex_types=None means "no type filter" (i.e., all)
    - openalex_types=["article","preprint"] will generate type:article|preprint
    """
    source_ids = [s for s in (source_ids or []) if s]
    if not source_ids:
        return None

    filter_parts = []

    # If openalex_types is None -> no type filter (meaning: all)
    if openalex_types is not None:
        openalex_types = [t.strip().lower() for t in openalex_types if str(t).strip()]
        if len(openalex_types) == 0:
            openalex_types = ["preprint"]  # safety fallback
        filter_parts.append("type:" + "|".join(openalex_types))

    filter_parts.append("primary_location.source.id:" + "|".join(source_ids))

    if date_start:
        filter_parts.append(f"from_publication_date:{date_start}")
    if date_end:
        filter_parts.append(f"to_publication_date:{date_end}")

    filter_str = ",".join(filter_parts)

    params = [
        "filter=" + requests.utils.quote(filter_str, safe=":,|"),
        "per-page=5",
        "cursor=*",
        f"mailto={requests.utils.quote(mailto)}",
    ]
    return OPENALEX_WORKS + "?" + "&".join(params)



# ============================================================
#  OpenAlex auth helper (optional API key support)
# ============================================================
def _get_openalex_headers(api_key: str | None = None) -> dict:
    """
    Build headers for OpenAlex requests.

    Priority:
      1. explicit api_key argument
      2. environment variable OPENALEX_API_KEY
      3. no key → empty headers (public / free tier)

    Users who have an API key can set:
      export OPENALEX_API_KEY="sk_...."
    or pass api_key="sk_..." to harvest_openalex_for_source_ids().
    """
    key = api_key or os.getenv("OPENALEX_API_KEY")
    headers = {}

    if key:
        # If OpenAlex expects a header-based key; adjust if spec changes.
        headers["Authorization"] = f"Bearer {key}"

    return headers



# ============================================================
#  Crossref helpers
# ============================================================
def _fetch_page(params, max_retries=6, base_sleep=0.5):
    """Low-level Crossref /works page fetch with retries."""
    headers = {"User-Agent": UA}
    last_exc = None
    for attempt in range(max_retries):
        try:
            r = requests.get(CROSSREF_WORKS, params=params, headers=headers, timeout=60)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(base_sleep * (2 ** attempt))
                continue
            try:
                print("Crossref error payload:", r.json())
            except Exception:
                print("Crossref error text:", r.text[:800])
            r.raise_for_status()
        except requests.RequestException as e:
            last_exc = e
            time.sleep(base_sleep * (2 ** attempt))
    if last_exc:
        raise last_exc


def _total_results(filters, sort_key="deposited"):
    """Get Crossref total-results for given filters."""
    params = {
        "filter": ",".join(filters),
        "rows": 0,
        "cursor": "*",
        "mailto": DEFAULT_MAILTO,
        "sort": sort_key,
        "order": "asc",
    }
    try:
        data = _fetch_page(params)
        return int(data.get("message", {}).get("total-results", 0))
    except Exception:
        params["rows"] = 1
        data = _fetch_page(params)
        return int(data.get("message", {}).get("total-results", 0))


def _stream_with_filters(filters, rows=1000, cursor="*", sort_key="deposited"):
    """Stream all Crossref items for given filter set (cursor API)."""
    assert rows <= 1000
    params = {
        "filter": ",".join(filters),
        "rows": rows,
        "cursor": cursor,
        "mailto": DEFAULT_MAILTO,
        "sort": sort_key,
        "order": "asc",
    }

    total = None
    yielded = 0
    last_cursors = set()
    pbar = None

    while True:
        data = _fetch_page(params)
        msg = data.get("message", {})

        if total is None:
            total = msg.get("total-results") or 0
            if _HAVE_TQDM and isinstance(total, int) and total > 0:
                pbar = tqdm(total=total, desc="Fetching preprints", unit="rec")

        items = msg.get("items") or []
        if not items:
            cur = msg.get("next-cursor")
            if cur and cur not in last_cursors:
                params["cursor"] = cur
                last_cursors.add(cur)
                continue
            break

        for it in items:
            yielded += 1
            if pbar:
                pbar.update(1)
            yield it

        if isinstance(total, int) and total > 0 and yielded >= total:
            break

        cur = msg.get("next-cursor")
        if not cur:
            break
        if cur in last_cursors:
            params["cursor"] = cur
            continue
        last_cursors.add(cur)
        params["cursor"] = cur

    if pbar:
        pbar.close()


def _fetch_work_by_doi(doi, max_retries=6, base_sleep=0.5):
    """Fetch one Crossref work by DOI."""
    headers = {"User-Agent": UA}
    url = f"{CROSSREF_WORKS}/{requests.utils.quote(doi)}"
    last_exc = None
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=headers, timeout=60)
            if r.status_code == 200:
                j = r.json()
                return j.get("message") or {}
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(base_sleep * (2 ** attempt))
                continue
            try:
                print("Crossref /works/{doi} error payload:", r.json())
            except Exception:
                print("Crossref /works/{doi} error text:", r.text[:800])
            r.raise_for_status()
        except requests.RequestException as e:
            last_exc = e
            time.sleep(base_sleep * (2 ** attempt))
    if last_exc:
        raise last_exc


# def _extract_relations(rel):
#     """Extract is-preprint-of / has-preprint / is-version-of / has-review lists from Crossref."""
#     if not rel or not isinstance(rel, dict):
#         return (None, None, None, None)

#     def pick(kind):
#         items = rel.get(kind) or []
#         dois = []
#         for it in items:
#             doi = it.get("id")
#             if doi and isinstance(doi, str) and doi.lower().startswith("https://doi.org/"):
#                 doi = doi.split("org/", 1)[1]
#             if doi:
#                 dois.append(str(doi).strip())
#         return "; ".join(sorted(set(dois))) if dois else None

#     return (
#         pick("is-preprint-of"),
#         pick("has-preprint"),
#         pick("is-version-of"),
#         pick("has-review"),   # ✅ NEW
#     )


def _one_row_wide(m):
    """Flatten Crossref message into a rich, wide dict."""
    title                 = _first(m.get("title"))
    original_title        = _first(m.get("original-title"))
    short_title           = _first(m.get("short-title"))
    subtitle              = _first(m.get("subtitle"))
    container_title       = _first(m.get("container-title"))
    short_container_title = _first(m.get("short-container-title"))

    created_date          = _date_from_parts(m.get("created"))
    posted_date           = _date_from_parts(m.get("posted"))
    deposited_date        = _date_from_parts(m.get("deposited"))
    indexed_date          = _date_from_parts(m.get("indexed"))
    issued_date           = _date_from_parts(m.get("issued"))
    published_date        = _date_from_parts(m.get("published"))
    published_online_date = _date_from_parts(m.get("published-online"))
    published_print_date  = _date_from_parts(m.get("published-print"))
    accepted_date         = _date_from_parts(m.get("accepted"))
    approved_date         = _date_from_parts(m.get("approved"))

    is_preprint_of, has_preprint, is_version_of, has_review = _extract_relations_crossref(m.get("relation"))
    relation_json          = _json(m.get("relation"))

    # # then ALWAYS merge into final column (not overwrite)
    # existing_is_version_of = df.get("is_version_of")  # could be None or a string

    # # ut["is_preprint_of"] = merge_dois(df.get("is_preprint_of"), is_preprint_of)
    # # ut["has_preprint"]   = merge_dois(df.get("has_preprint"), has_preprint)
    # # ut["has_review"]     = merge_dois(df.get("has_review"), has_review)

    # # IMPORTANT: include your old behavior + relation-derived versions (incl updated-*)
    # ut["is_version_of"]  = merge_dois(existing_is_version_of, is_version_of_rel)

    authors_pretty = None
    if m.get("author"):
        names = []
        for a in m.get("author"):
            nm = " ".join(filter(None, [a.get("given"), a.get("family")])).strip()
            if not nm:
                nm = a.get("name") or a.get("literal")
            if nm:
                names.append(nm)
        authors_pretty = "; ".join(names) if names else None

    authors_json           = _json(m.get("author"))
    editors_json           = _json(m.get("editor"))
    translators_json       = _json(m.get("translator"))
    chairs_json            = _json(m.get("chair"))
    contributors_json      = _json(m.get("container-contributor") or m.get("contributor"))

    license_url            = _first(m.get("license"), "URL")
    licenses_json          = _json(m.get("license"))
    links_json             = _json(m.get("link"))
    primary_url            = (m.get("resource", {}) or {}).get("primary", {}).get("URL")

    issn_json              = _json(m.get("ISSN"))
    issn = "; ".join(m.get("ISSN") or []) if m.get("ISSN") else None
    issn_type_json         = _json(m.get("issn-type"))
    isbn_type_json         = _json(m.get("isbn-type"))
    alternative_id_json    = _json(m.get("alternative-id"))

    subjects               = "; ".join(m.get("subject") or []) if m.get("subject") else None
    subjects_json          = _json(m.get("subject"))
    language               = m.get("language")

    funders_json           = _json(m.get("funder"))
    reference_count        = m.get("reference-count")
    is_referenced_by_count = m.get("is-referenced-by-count")
    references_json        = _json(m.get("reference"))  # to delect

    update_to = m.get("update-to") or []
    labels = []
    if isinstance(update_to, list):
        for u in update_to:
            if isinstance(u, dict):
                lab = u.get("label")
                if lab:
                    labels.append(str(lab).strip())

    version_label = "; ".join(sorted(set(labels))) if labels else None

    update_to_json         = _json(m.get("update-to"))  # to delect
    update_policy          = m.get("update-policy") # to delect
    update_type            = _first(m.get("update-to"), "type") # to delect

    publisher              = m.get("publisher")
    member                 = m.get("member")
    prefix                 = m.get("prefix")
    doi                    = m.get("DOI")
    url                    = m.get("URL")
    type_                  = m.get("type")
    subtype                = m.get("subtype")

    archive_json           = _json(m.get("archive"))
    content_domain_json    = _json(m.get("content-domain"))
    assertion_json         = _json(m.get("assertion"))

    institution_name       = _first(m.get("institution"), "name")
    institution_json       = _json(m.get("institution"))

    group_title            = m.get("group-title")
    source                 = m.get("source")
    score                  = m.get("score")
    abstract_raw           = m.get("abstract")

    return {
        "doi": doi,
        "url": url,
        "primary_url": primary_url,
        "title": title,
        "original_title": original_title,
        "short_title": short_title,
        "subtitle": subtitle,
        "type": type_,
        "subtype": subtype,
        "prefix": prefix,
        "publisher": publisher,
        "container_title": container_title,
        "short_container_title": short_container_title,
        "institution_name": institution_name,
        "created_date": created_date,
        "posted_date": posted_date,
        "deposited_date": deposited_date,
        "indexed_date": indexed_date,
        "issued_date": issued_date,
        "published_date": published_date,
        "published_online_date": published_online_date,
        "published_print_date": published_print_date,
        "accepted_date": accepted_date,
        "approved_date": approved_date,
        "authors": authors_pretty,
        "authors_json": authors_json,
        "editors_json": editors_json,
        "translators_json": translators_json,
        "chairs_json": chairs_json,
        "contributors_json": contributors_json,
        "license_url": license_url,
        "licenses_json": licenses_json,
        "links_json": links_json,
        "subjects": subjects,
        "subjects_json": subjects_json,
        "language": language,
        "issn_json": issn_json,
        "issn": issn,
        "issn_type_json": issn_type_json,
        "isbn_type_json": isbn_type_json,
        "alternative_id_json": alternative_id_json,
        "funder_json": funders_json,
        "reference_count": reference_count,
        "is_referenced_by_count": is_referenced_by_count,
        "references_json": references_json,
        "is_preprint_of": is_preprint_of,
        "has_preprint": has_preprint,
        "is_version_of": is_version_of,
        "has_review": has_review,
        "relation_json": relation_json,
        "update_type": update_type,
        "update_policy": update_policy,
        "update_to_json": update_to_json,
        "version_label": version_label,
        "archive_json": archive_json,
        "content_domain_json": content_domain_json,
        "assertion_json": assertion_json,
        "institution_json": institution_json,
        "group_title": group_title,
        "member": member,
        "source": source,
        "score": score,
        "abstract_raw": abstract_raw,
        "raw_json": _json(m),
    }


def _filters_base(from_iso, until_iso, crossref_types=None):
    """
    Choose the correct Crossref date filter depending on record types.

    - If strictly posted-content → posted-date window
    - Otherwise (including 'all') → deposit-date window (works broadly)
    """
    crossref_types = [t.strip() for t in (crossref_types or []) if str(t).strip()]
    if not crossref_types:
        crossref_types = ["posted-content"]

    only_posted_content = (
        len(crossref_types) == 1 and crossref_types[0] == "posted-content"
    )

    if only_posted_content:
        base = [
            f"from-posted-date:{from_iso}",
            f"until-posted-date:{until_iso}",
        ]
    else:
        # ✅ Crossref uses "deposit" (not "deposited")
        base = [
            f"from-deposit-date:{from_iso}",
            f"until-deposit-date:{until_iso}",
        ]

    return base, crossref_types




def _fanout_api_filters(from_iso, until_iso, prefixes=None, members=None, group_titles_exact=None, issns=None, crossref_types=None):
    base_date, crossref_types = _filters_base(from_iso, until_iso, crossref_types=crossref_types)
    sets = []

    prefixes = [str(p).strip() for p in (prefixes or []) if p]
    members  = [str(m).strip() for m in (members  or []) if m]
    gtitles  = [str(g).strip() for g in (group_titles_exact or []) if g]
    issns    = [str(i).strip() for i in (issns or []) if i]

    issn_space = issns if issns else [None]

    for rt in crossref_types:
        if prefixes or members or gtitles or issns:
            for issn in issn_space:
                for p in (prefixes or [None]):
                    for m in (members or [None]):
                        for g in (gtitles or [None]):
                            flt = list(base_date)
                            if rt != "all":
                                flt.append(f"type:{rt}")
                            if issn: flt.append(f"issn:{issn}")
                            if p:    flt.append(f"prefix:{p}")
                            if m:    flt.append(f"member:{m}")
                            if g:    flt.append(f"group-title:{g}")
                            sets.append(flt)
        else:
            if rt != "all":
                sets.append(list(base_date) + [f"type:{rt}"])
            else:
                sets.append(list(base_date))

    return sets


def _eval_predicate_on_item(
    item,
    group_title_contains=None,
    institution_contains=None,
    url_contains=None,
    doi_startswith=None,
    doi_contains=None,
    require_all=False,
):
    """Apply client-level predicates to a Crossref item."""
    group_title_contains = [s.lower() for s in (group_title_contains or []) if s]
    institution_contains = [s.lower() for s in (institution_contains or []) if s]
    url_contains         = [s.lower() for s in (url_contains or []) if s]
    doi_startswith       = [s.lower() for s in (doi_startswith or []) if s]
    doi_contains         = [s.lower() for s in (doi_contains or []) if s]

    doi = (item.get("DOI") or "").lower()
    url = (item.get("URL") or "").lower()
    try:
        primary_url = ((item.get("resource") or {}).get("primary") or {}).get("URL") or ""
    except Exception:
        primary_url = ""
    primary_url = primary_url.lower()

    gt = item.get("group-title") or ""
    gt_l = gt.lower() if isinstance(gt, str) else ""

    insts = item.get("institution") or []
    inst_names = []
    if isinstance(insts, list):
        for ins in insts:
            nm = (ins or {}).get("name")
            if nm:
                inst_names.append(str(nm))
    inst_blob_l = " | ".join(inst_names).lower() if inst_names else ""

    checks = []

    if group_title_contains:
        checks.append(any(s in gt_l for s in group_title_contains))
    if institution_contains:
        checks.append(any(s in inst_blob_l for s in institution_contains))
    if url_contains:
        checks.append(any(s in url or s in primary_url for s in url_contains))
    if doi_startswith:
        checks.append(any(doi.startswith(s) for s in doi_startswith))
    if doi_contains:
        checks.append(any(s in doi for s in doi_contains))

    if not checks:
        return True
    return all(checks) if require_all else any(checks)


def _datacite_post_filter_df(
    df: pd.DataFrame,
    *,
    doi_startswith=None,
    url_contains=None,
    require_all: bool = False,
) -> pd.DataFrame:
    """
    Apply rule-based post-filters to a harvested DataCite dataframe.
    - doi_startswith: list[str] -> keep rows where doi startswith any token
    - url_contains: list[str] -> keep rows where url contains any token
    """
    if df is None or df.empty:
        return df

    doi_startswith = [s.lower() for s in (doi_startswith or []) if s]
    url_contains   = [s.lower() for s in (url_contains   or []) if s]

    if not doi_startswith and not url_contains:
        return df

    ddoi = df.get("doi")
    durl = df.get("url")

    doi_series = ddoi.astype(str).str.lower() if ddoi is not None else pd.Series([""] * len(df))
    url_series = durl.astype(str).str.lower() if durl is not None else pd.Series([""] * len(df))

    checks = []

    if doi_startswith:
        checks.append(doi_series.apply(lambda x: any(x.startswith(p) for p in doi_startswith)))

    if url_contains:
        checks.append(url_series.apply(lambda x: any(p in x for p in url_contains)))

    mask = None
    if checks:
        mask = checks[0]
        for c in checks[1:]:
            mask = (mask & c) if require_all else (mask | c)

    out = df[mask].copy() if mask is not None else df.copy()
    if "doi" in out.columns:
        out.drop_duplicates(subset=["doi"], inplace=True)
    return out


def harvest_preprints_filtered(
    date_start,
    date_end,
    mailto=DEFAULT_MAILTO,
    prefixes=None,
    members=None,
    group_titles_exact=None, 
    issns=None,
    group_title_contains=None,
    institution_contains=None,
    url_contains=None,
    doi_startswith=None,
    doi_contains=None,
    require_all=False,
    dois_exact=None,
    enrich_by_doi=False,
    crossref_types=None,
    count_only=False,
    rows_per_call=1000,
    sort_key="deposited",
    polite_sleep_s=0.0,
):
    """
    High-level Crossref harvested, used by the sheet-driven function.
    """
    _set_mailto(mailto)

    # start_dt = datetime.fromisoformat(date_start).replace(hour=0, minute=0, second=0)
    # end_dt   = datetime.fromisoformat(date_end).replace(hour=23, minute=59, second=59)
    # from_iso  = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
    # until_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%S")

    from_iso = date_start
    until_iso = date_end


    dois_exact = [d for d in (dois_exact or []) if d]

    # ----- Direct DOI mode -----
    if dois_exact:
        rows = []
        kept = 0
        for d in dois_exact:
            try:
                m = _fetch_work_by_doi(d)
                if not m:
                    continue
                if _eval_predicate_on_item(
                    m,
                    group_title_contains=group_title_contains,
                    institution_contains=institution_contains,
                    url_contains=url_contains,
                    doi_startswith=doi_startswith,
                    doi_contains=doi_contains,
                    require_all=require_all,
                ):
                    kept += 1
                    if not count_only:
                        rows.append(_one_row_wide(m))
                if polite_sleep_s > 0:
                    time.sleep(polite_sleep_s)
            except Exception as e:
                print(f"[warn] DOI fetch failed for {d}: {e}")
        return kept if count_only else pd.DataFrame(rows)

    # ----- Filter fan-out -----
    filter_sets = _fanout_api_filters(
        from_iso, until_iso,
        prefixes=prefixes,
        members=members,
        group_titles_exact=group_titles_exact,
        issns=issns,
        crossref_types=crossref_types,
    )

    has_client_predicates = any([
        group_title_contains, institution_contains, url_contains, doi_startswith, doi_contains
    ])

    # COUNT-ONLY, API-level filters only
    if count_only:
        if not has_client_predicates:
            total = 0
            for flt in filter_sets:
                total += _total_results(flt, sort_key=sort_key)
            return total
        else:
            seen = set()
            kept = 0
            for flt in filter_sets:
                for item in _stream_with_filters(flt, rows=rows_per_call, cursor="*", sort_key=sort_key):
                    doi = (item.get("DOI") or "").lower()
                    if not doi or doi in seen:
                        continue
                    if _eval_predicate_on_item(
                        item,
                        group_title_contains=group_title_contains,
                        institution_contains=institution_contains,
                        url_contains=url_contains,
                        doi_startswith=doi_startswith,
                        doi_contains=doi_contains,
                        require_all=require_all,
                    ):
                        kept += 1
                    seen.add(doi)
            return kept

    # FULL DATA, no extra predicates → pure filter-based stream
    rows = []
    if not has_client_predicates:
        for flt in filter_sets:
            for item in _stream_with_filters(flt, rows=rows_per_call, cursor="*", sort_key=sort_key):
                rows.append(_one_row_wide(item))
        df = pd.DataFrame(rows)
        if not df.empty:
            df.drop_duplicates(subset=["doi"], inplace=True)
        return df


    # FULL DATA, with client predicates → stream and keep matching items (NO DOI enrich)
    rows = []
    seen = set()

    for flt in filter_sets:
        for item in _stream_with_filters(flt, rows=rows_per_call, cursor="*", sort_key=sort_key):
            doi = (item.get("DOI") or "").lower()
            if not doi or doi in seen:
                continue

            if _eval_predicate_on_item(
                item,
                group_title_contains=group_title_contains,
                institution_contains=institution_contains,
                url_contains=url_contains,
                doi_startswith=doi_startswith,
                doi_contains=doi_contains,
                require_all=require_all,
            ):
                rows.append(_one_row_wide(item))

            seen.add(doi)

    df = pd.DataFrame(rows)
    if not df.empty:
        df.drop_duplicates(subset=["doi"], inplace=True)
    return df



# ============================================================
#  DataCite helpers
# ============================================================
def _parse_next_cursor(next_url: str):
    """Parse page[cursor] from Datacite 'links.next' URL."""
    try:
        qs = parse_qs(urlparse(next_url).query)
        return qs.get("page[cursor]", [None])[0]
    except Exception:
        return None


def _norm_date(iso: str):
    if not iso:
        return None
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        return str(iso)[:10]


def _datacite_request(params, mailto, max_retries=6, base_sleep=0.5):
    headers = {
        "User-Agent": f"DataCite-SheetHarvester/1.0 (mailto:{mailto})",
        "Accept": "application/vnd.api+json",
    }
    last_exc = None
    for attempt in range(max_retries):
        try:
            r = requests.get(DATACITE_DOIS, params=params, headers=headers, timeout=60)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(base_sleep * (2 ** attempt))
                continue
            try:
                print("DataCite error payload:", r.json())
            except Exception:
                print("DataCite error text:", r.text[:800])
            r.raise_for_status()
        except requests.RequestException as e:
            last_exc = e
            time.sleep(base_sleep * (2 ** attempt))
    if last_exc:
        raise last_exc

def _datacite_parent_doi(related_identifiers):
    """
    Return parent DOI for a child version (DataCite relatedIdentifiers).
    Prioritize child -> parent relations.
    """
    if not isinstance(related_identifiers, list):
        return None

    preferred = {"IsNewVersionOf", "IsVersionOf", "IsDerivedFrom", "IsPreviousVersionOf"}

    for rel_type in preferred:
        for r in related_identifiers:
            if not isinstance(r, dict):
                continue
            if r.get("relationType") != rel_type:
                continue
            if str(r.get("relatedIdentifierType") or "").upper() == "DOI":
                rid = r.get("relatedIdentifier")
                if rid:
                    return _norm_doi(str(rid))
    return None

def _datacite_one_row(item):
    """Flatten one DataCite /dois item into a wide row."""
    if not item or "attributes" not in item:
        return None

    a = item["attributes"]
    rel = item.get("relationships") or {}

    doi       = a.get("doi")
    url       = a.get("url")
    publisher = a.get("publisher")
    language  = a.get("language")
    version   = a.get("version")
    schema_version = a.get("schemaVersion")
    state     = a.get("state")

    prefix    = a.get("prefix") or (doi.split("/", 1)[0] if doi and "/" in doi else None)

    client_id = (rel.get("client") or {}).get("data", {}).get("id") or a.get("clientId")
    provider_id = (rel.get("provider") or {}).get("data", {}).get("id") or a.get("providerId")
    if not provider_id and client_id and "." in client_id:
        provider_id = client_id.split(".", 1)[0]

    types          = a.get("types") or {}
    types_json     = _json(types)
    type_resource  = types.get("resourceType")
    type_general   = types.get("resourceTypeGeneral")

    titles       = a.get("titles") or []
    title_raw    = _first(titles, "title")
    # from html import unescape
    title        = unescape(title_raw) if isinstance(title_raw, str) else title_raw
    titles_json  = _json(titles)

    created    = _norm_date(a.get("created"))
    registered = _norm_date(a.get("registered"))
    updated    = _norm_date(a.get("updated"))

    published       = a.get("published")
    published_year  = a.get("publicationYear")

    creators_json       = _json(a.get("creators"))
    contributors_json   = _json(a.get("contributors"))
    subjects_json       = _json(a.get("subjects"))
    descriptions_json   = _json(a.get("descriptions"))
    identifiers_json    = _json(a.get("identifiers"))
    alt_ids_json        = _json(a.get("alternateIdentifiers"))
    related_ids_raw = a.get("relatedIdentifiers") or []
    related_ids_json = _json(related_ids_raw)
    # NEW: parent DOI from DataCite relatedIdentifiers
    parent_doi = _datacite_parent_doi(related_ids_raw)

    container_json      = _json(a.get("container"))
    funding_refs_json   = _json(a.get("fundingReferences"))
    rights              = a.get("rights")  # deprecated single string
    rights_list_json    = _json(a.get("rightsList"))
    # rightsIdentifiers = a.get("rightsIdentifiers")
    
    sizes_json          = _json(a.get("sizes"))
    formats_json        = _json(a.get("formats"))
    geo_locations_json  = _json(a.get("geoLocations"))
    references_json     = _json(rel.get("references"))
    referenceCount      = a.get("referenceCount")
    citations_json      = _json(a.get("citations"))
    citationCount       = a.get("citationCount")
    url_alternate_json  = _json(a.get("urlAlternate"))
    raw_attributes_json = _json(a)
    raw_relationships_json = _json(rel)

    return {
        "doi": doi, "url": url, "publisher": publisher, "language": language,
        "version": version, "schema_version": schema_version, "state": state,
        "prefix": prefix,
        "client_id": client_id, "provider_id": provider_id,
        "resource_type": type_resource, "resource_type_general": type_general,
        "types_json": types_json,
        "title": title, "titles_json": titles_json,
        "created": created, "registered": registered, "updated": updated,
        "published": published, "published_year": published_year,
        "creators_json": creators_json, "contributors_json": contributors_json,
        "subjects_json": subjects_json, "descriptions_json": descriptions_json,
        "identifiers_json": identifiers_json, "alternate_ids_json": alt_ids_json,
        "related_ids_json": related_ids_json, "container_json": container_json,
        "funding_refs_json": funding_refs_json, 
        "rights": rights,"parent_doi": parent_doi,
        "rights_list_json": rights_list_json,
        "sizes_json": sizes_json, "formats_json": formats_json,
        "geo_locations_json": geo_locations_json, "references_json": references_json,
        "citations_json": citations_json, "url_alternate_json": url_alternate_json,
        "reference_count": referenceCount, "citation_count": citationCount,
        "raw_json": raw_attributes_json,
        "raw_relationships_json": raw_relationships_json,
    }


def harvest_datacite_for_client_ids(
    mailto: str,
    client_ids,
    resource_types,
    date_start: str,
    date_end: str,
    rows_per_call: int = 1000,
    types_query_override: str | None = None,
    doi_prefix_query: str | None = None,
):
    """
    Thin wrapper around the DataCite API for per-server harvesting.

    - client_ids: list[str], e.g. ["cern.cds"]
    - resource_types: e.g. ["preprint"] or ["preprint", "text"]
      * 'preprint' is treated specially: we do BOTH:
          resource-type-id=Preprint AND query=types.resourceType:"preprint"
    - date_start/date_end: strings YYYY-MM-DD (applied on 'registered' via query)
      The date filter is:
          query = 'registered:[date_start TO date_end]'
      possibly combined with type query via AND.
    """
    if date_start:
        date_start = _validate_date(date_start, "date_start")
    if date_end:
        date_end = _validate_date(date_end, "date_end")

    # Build the date query string once
    date_query = None
    if date_start and date_end:
        date_query = f"registered:[{date_start} TO {date_end}]"
    elif date_start:
        date_query = f"registered:[{date_start} TO *]"
    elif date_end:
        date_query = f"registered:[* TO {date_end}]"

    all_rows = []

    client_ids = [str(c).strip() for c in (client_ids or []) if str(c).strip()]
    resource_types = [str(rt).strip().lower() for rt in (resource_types or []) if str(rt).strip()]

    if not client_ids or not resource_types:
        return pd.DataFrame()

    for cid in client_ids:
        print(f"\n  [DataCite] client_id={cid}")

        # If "all" is present, do NOT send resource-type-id (means: all types)
        want_all = any(str(x).strip().lower() == "all" for x in (resource_types or []))

        rtid = None if want_all else _datacite_join_resource_type_ids(resource_types)

        base_params = {
            "client-id": cid,
            "disable-facets": "true",
            "affiliation": "true",
        }

        if rtid:
            # DataCite accepts comma-separated list
            base_params["resource-type-id"] = rtid

        # Build query: start from date_query, then AND doi_prefix_query (if any)
        combined_query = date_query  # can be None

        if doi_prefix_query:
            combined_query = f"({combined_query}) AND ({doi_prefix_query})" if combined_query else f"({doi_prefix_query})"

        if combined_query:
            base_params["query"] = combined_query


        print("[DataCite combined_query]", base_params.get("query"))

        # ---- total count (exact) ----
        total_params = dict(base_params)
        total_params["page[size]"] = 0
        total_params["page[cursor]"] = "1"

        debug_total = {k: total_params.get(k) for k in ("client-id", "resource-type-id", "query") if k in total_params}
        print("[DataCite total_params]", debug_total)

        js_total = _datacite_request(total_params, mailto=mailto)
        meta = js_total.get("meta") or {}
        total = int(meta.get("total") or 0)
        print(f"    combined slice → total={total}")

        pbar = None
        if _HAVE_TQDM and total > 0:
            pbar = tqdm(total=total, desc="Fetching records (DataCite)", unit="rec")

        cursor = "1"
        first_page_logged = False
        while cursor:
            params = dict(base_params)
            params["page[size]"] = rows_per_call
            params["page[cursor]"] = cursor

            if not first_page_logged:
                debug_params = {k: params.get(k) for k in ("client-id", "resource-type-id", "query") if k in params}
                print("[DataCite page_params]", debug_params)
                first_page_logged = True

            js = _datacite_request(params, mailto=mailto)
            items = js.get("data") or []
            if not items:
                break

            for it in items:
                if pbar:
                    pbar.update(1)
                row = _datacite_one_row(it)
                if row:
                    all_rows.append(row)

            links = js.get("links") or {}
            nxt = links.get("next")
            cursor = _parse_next_cursor(nxt) if nxt else None

        if pbar:
            pbar.close()


    df = pd.DataFrame(all_rows)
    if not df.empty and "doi" in df.columns:
        df.drop_duplicates(subset=["doi"], inplace=True)

        if "registered" in df.columns:
            print(f"[DataCite sanity-check] registered min={df['registered'].min()} max={df['registered'].max()}")

    return df


# ============================================================
#  OpenAlex helpers (NEW)
# ============================================================
def _openalex_one_row(work: dict) -> dict:
    """Flatten one OpenAlex work into a simple row."""
    primary = work.get("primary_location") or {}
    source  = primary.get("source") or {}
    oa      = work.get("open_access") or {}

    return {
        "openalex_id": work.get("id"),
        "doi": work.get("doi"),
        "title": work.get("display_name"),
        "publication_year": work.get("publication_year"),
        "publication_date": work.get("publication_date"),
        "cited_by_count": work.get("cited_by_count"),
        "type": work.get("type"),
        'abstract_inverted_index_json': _json(work.get("abstract_inverted_index")),
        "abstract_inverted_index": work.get("abstract_inverted_index"),
        "is_paratext": work.get("is_paratext"),
        "primary_location_landing_page_url": primary.get("landing_page_url"),
        "primary_location_source_id": source.get("id"),
        "primary_location_source_display_name": source.get("display_name"),
        "issn": source.get("issn_l"),
        "primary_location_is_oa": oa.get("is_oa"),
        "primary_location_oa_status": oa.get("oa_status"),
        "authorships_json": _json(work.get("authorships")),
        "concepts_json": _json(work.get("concepts")),
        "topics_json": _json(work.get("topics")),
        "language": work.get("language"),
        "license": primary.get("license"),
        "license_id": primary.get("license_id"),
        "created_date": work.get("created_date"),
        "version_label": primary.get("version"),
        "updated_date": work.get("updated_date"),
        "primary_location_fulltext_pdf_url": primary.get("pdf_url"),
        "referenced_works_count": work.get("referenced_works_count"),
        "references_json": _json(work.get("referenced_works")),
        'funders_json': _json(work.get("funders")),
        "raw_json": _json(work),
    }


def harvest_openalex_for_source_ids(
    source_ids,
    date_start: str,
    date_end: str,
    mailto: str,
    per_page: int = 200,
    max_results: int = 3000000,
    only_preprints: bool = True,
    openalex_types: list[str] | None = None,
    openalex_api_key: str | None = None,   # ← optional API key
) -> pd.DataFrame:
    """
    Harvest OpenAlex works for one or more source_ids over a date range.

    - source_ids: list like ["s3006283864", "s4306401238"]
    - date_start/date_end: YYYY-MM-DD (same window as Crossref/DataCite)
    - only_preprints: if True, add type:preprint to the filter
    """
    source_ids = [s.strip() for s in (source_ids or []) if str(s).strip()]
    if not source_ids:
        return pd.DataFrame()

    if date_start:
        date_start = _validate_date(date_start, "date_start")
    if date_end:
        date_end = _validate_date(date_end, "date_end")

    filter_parts = []

    # at the top of harvest_openalex_for_source_ids()
    if openalex_types is not None:
        openalex_types = [t.strip().lower() for t in openalex_types if str(t).strip()]

    if openalex_types is None:
        # no type filter
        pass
    elif len(openalex_types) == 0:
        openalex_types = ["preprint"]
        filter_parts.append("type:" + "|".join(openalex_types))
    else:
        filter_parts.append("type:" + "|".join(openalex_types))


    # Restrict to the given sources
    filter_parts.append(f"primary_location.source.id:{'|'.join(source_ids)}")

    # Publication date range
    if date_start:
        filter_parts.append(f"from_publication_date:{date_start}")
    if date_end:
        filter_parts.append(f"to_publication_date:{date_end}")

    filter_str = ",".join(filter_parts)

    works = []
    cursor = "*"
    per_page = min(per_page, 200)

    print("Resolved OpenAlex parameters:")
    print(f"  - source_ids: {source_ids}")
    print(f"  - filter: {filter_str}")

    # 👇 NEW: build headers & progress bar
    headers = _get_openalex_headers(openalex_api_key)
    pbar = tqdm(desc="Fetching preprints (OpenAlex)", unit="rec") if _HAVE_TQDM else None

    while True:
        url = (
            f"{OPENALEX_WORKS}"
            f"?filter={filter_str}"
            f"&per-page={per_page}"
            f"&cursor={cursor}"
            f"&mailto={mailto}"
        )
        try:
            r = requests.get(url, timeout=60, headers=headers)
            r.raise_for_status()
            data = r.json()
        except requests.RequestException as e:
            print(f"[OpenAlex] Error for {source_ids} cursor={cursor}: {e}")
            break

        results = data.get("results") or []
        if not results:
            break

        # 👇 update progress bar per record
        for w in results:
            works.append(w)
            if pbar is not None:
                pbar.update(1)

        if len(works) >= max_results:
            print(f"[OpenAlex] Reached max_results={max_results}, stopping.")
            break

        cursor = data.get("meta", {}).get("next_cursor")
        if not cursor:
            break

    if pbar is not None:
        pbar.close()

    rows = [_openalex_one_row(w) for w in works]
    df = pd.DataFrame(rows)

    if openalex_types and not df.empty and "type" in df.columns:
        allowed = set(openalex_types)
        df = df[df["type"].astype(str).str.lower().isin(allowed)].copy()


    if not df.empty and "openalex_id" in df.columns:
        df.drop_duplicates(subset=["openalex_id"], inplace=True)

    print(f"OpenAlex: harvested {len(df)} records")
    return df


# ============================================================
#  Rules sheet helpers
# ============================================================
def _parse_list_cell(val):
    """Parse "[a,b]" or "a,b" or single value into list of strings."""
    if pd.isna(val):
        return []
    s = str(val)
    s = s.replace("[", "").replace("]", "")
    parts = s.split(",")
    out = []
    for p in parts:
        cleaned = p.strip().strip("'\"")
        if cleaned:
            out.append(cleaned)
    return out


def _parse_rules_tokens(row: pd.Series):
    """
    Combine `rules` and optional rule_1..rule_7 columns into a set of tokens.
    E.g. "client_id/text/source_id" → {"client_id","text","source_id"}.
    """
    tokens = set()

    raw_rules = row.get("rules", None)
    if pd.notna(raw_rules):
        for tok in str(raw_rules).split("/"):
            tok = tok.strip()
            if tok and tok != "/":
                tokens.add(tok)

    for i in range(1, 8):
        col = f"rule_{i}"
        if col in row.index:
            v = row[col]
            if pd.notna(v):
                tok = str(v).strip()
                if tok and tok != "/":
                    tokens.add(tok)

    return {t for t in tokens if t}


def _build_params_from_rule_row(row: pd.Series):
    """
    Build Crossref parameter dict from one row of the Google Sheet.
    Only used when backend='crossref'.
    """
    rule_tokens = _parse_rules_tokens(row)

    prefixes             = None
    members              = None
    group_titles_exact   = None
    group_title_contains = None
    institution_contains = None
    url_contains         = None
    doi_startswith       = None
    doi_contains         = None
    issns = None

    if "prefix" in rule_tokens:
        prefixes_list = _parse_list_cell(row.get("doi_prefixes"))
        prefixes = prefixes_list or None

    if "member" in rule_tokens:
        members_list = _parse_list_cell(row.get("crossref_members"))
        members = members_list or None

    if "group_title" in rule_tokens:
        gt_list = _parse_list_cell(row.get("group_title"))
        group_titles_exact = gt_list or None

    if "institution_name" in rule_tokens:
        inst_list = _parse_list_cell(row.get("institution_name"))
        institution_contains = inst_list or None

    if "primary_domain" in rule_tokens:
        dom1 = _parse_list_cell(row.get("primary_domain"))
        dom2 = _parse_list_cell(row.get("primary_domain_extend"))
        doms = dom1 + dom2
        url_contains = doms or None

    if "doi_prefix_first_token" in rule_tokens:
        doi_list = _parse_list_cell(row.get("doi_prefix_first_token"))
        doi_startswith = doi_list or None

    if "issn" in rule_tokens:
        issns = _parse_issn_cell(row.get("ISSN")) or None  # NEW (your column is 'ISSN')

    return dict(
        prefixes=prefixes,
        members=members,
        group_titles_exact=group_titles_exact,
        group_title_contains=group_title_contains,
        institution_contains=institution_contains,
        url_contains=url_contains,
        doi_startswith=doi_startswith,
        doi_contains=doi_contains,
        issns=issns, 
    )

# ============================================================
#  Main: Harvest per server from rules sheet
# ============================================================
def harvest_servers_from_rules_sheet(
    sheet_csv_path_or_url: str,
    servers=None,              # list of server names (Field_server_name) or None for all include==yes
    date_start: str = "2000-01-01",
    date_end: str = "2025-10-11",
    mailto: str = DEFAULT_MAILTO,
    output_root: str = "data/by_server_datacite_crossref_openalex",
    rows_per_call: int = 1000,
    dry_run: bool = False,
    openalex_api_key: str | None = None, 
):
    """
    - sheet_csv_path_or_url: path to downloaded CSV or Google Sheets CSV URL
    - servers: list of server names (Field_server_name) to harvest.
               If None, harvest ALL rows with include == 'yes'.
    - date_start/date_end: 'YYYY-MM-DD'
    - mailto: your contact email for Crossref/DataCite/OpenAlex
    - output_root: directory where per-server subfolders are created.
    - dry_run: if True, only prints parameters; no API calls, no files.

    Summary CSV includes:
      - backend (crossref/datacite/none)
      - rows / parquet_path / csv_path for main backend
      - openalex_rows / openalex_parquet_path / openalex_csv_path / openalex_note
      - crossref_example_url / datacite_example_url / openalex_example_url
      - details JSON with crossref/datacite/openalex params and example URLs.
    """
    os.makedirs(output_root, exist_ok=True)

    df_rules = pd.read_csv(sheet_csv_path_or_url, header=0)
    print(f"Columns found in CSV: {df_rules.columns.tolist()}")

    # normalize include
    norm_cols = {c.strip().lower(): c for c in df_rules.columns}
    include_col_name = norm_cols.get("include")
    if include_col_name:
        df_rules["include_norm"] = (
            df_rules[include_col_name].astype(str).str.strip().str.lower()
        )
    else:
        print("Warning: no 'include' column found, assuming all rows are included.")
        df_rules["include_norm"] = "yes"

    df_rules = df_rules[df_rules["include_norm"] == "yes"]

    # restrict to selected servers (if provided)
    if servers is not None:
        servers_set = set(servers)
        df_rules = df_rules[df_rules["Field_server_name"].isin(servers_set)]

    # mapping for client_id and source_id columns
    client_id_col = norm_cols.get("datacite_client_id", "datacite_client_id")
    source_id_col = norm_cols.get("source_id", "source_id")

    ts = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    summary_rows = []
    summary_path = os.path.join(
        output_root,
        f"harvest_summary_{date_start}_{date_end}_{ts}_{'dry' if dry_run else 'real'}.csv"
    )

    # crossref-related rule tokens
    crossref_tokens = {
        "prefix",
        "member",
        "group_title",
        "institution_name",
        "primary_domain",
        "doi_prefix_first_token",
        "issn",  
    }

    for rid, row in df_rules.reset_index(drop=True).iterrows():
        server_name = row["Field_server_name"]

        # ---- parse rules once ----
        rule_tokens = _parse_rules_tokens(row)

        base_details = {
            "server": server_name,
            "date_start": date_start,
            "date_end": date_end,
            "rule_tokens": sorted(list(rule_tokens)) if rule_tokens else [],
        }

        # default outputs
        backend = "none"
        main_rows = 0
        main_parquet_path = None
        main_csv_path = None
        main_note = ""

        openalex_rows = None
        openalex_parquet_path = None
        openalex_csv_path = None
        openalex_note = None

        # >>> ADDED: example URLs default
        crossref_example_url = None
        datacite_example_url = None
        openalex_example_url = None

        details_dict = dict(base_details)

        # --------------------------------------------------
        # Case 1: no rules at all → only log, no harvesting
        # --------------------------------------------------
        if not rule_tokens:
            print("\n" + "=" * 70)
            print(f"Server: {server_name}  (backend: none)")
            print("=" * 70)
            print("  -> No rules defined in 'rules' / rule_1..rule_7.")
            details_dict["backend"] = "none"
            details_dict["note"] = "no_rules_defined"

            # also include example URLs (None) for schema consistency
            details_dict["crossref_example_url"] = crossref_example_url
            details_dict["datacite_example_url"] = datacite_example_url
            details_dict["openalex_example_url"] = openalex_example_url

            details_json = json.dumps(details_dict, ensure_ascii=False)
            summary_rows.append(
                {
                    "server": server_name,
                    "backend": "none",
                    "rows": 0,
                    "parquet_path": None,
                    "csv_path": None,
                    "dry_run": dry_run,
                    "openalex_rows": openalex_rows,
                    "openalex_parquet_path": openalex_parquet_path,
                    "openalex_csv_path": openalex_csv_path,
                    "openalex_note": openalex_note,
                    "crossref_example_url": crossref_example_url,
                    "datacite_example_url": datacite_example_url,
                    "openalex_example_url": openalex_example_url,
                    "details": details_json,
                    "note": "no_rules_defined",
                }
            )
            continue

        has_client_id = "client_id" in rule_tokens
        has_crossref  = bool(rule_tokens & crossref_tokens)
        has_openalex  = "source_id" in rule_tokens

        if has_client_id:
            backend = "datacite"
        elif has_crossref:
            backend = "crossref"
        elif has_openalex:
            backend = "openalex"
        else:
            backend = "none"

        print("\n" + "=" * 70)
        print(f"Server: {server_name}  (backend: {backend})")
        print("=" * 70)
        print(f"Date window: {date_start} → {date_end}")
        print(f"Rules tokens: {rule_tokens}")

        details_dict["backend"] = backend

        records_types_tokens = _parse_records_types_cell(row.get("records_types"))
        details_dict["records_types_tokens"] = records_types_tokens

        crossref_types, _, _ = _resolve_record_types_for_backend(records_types_tokens, backend="crossref")
        _, datacite_types, _ = _resolve_record_types_for_backend(records_types_tokens, backend="datacite")
        _, _, openalex_types = _resolve_record_types_for_backend(records_types_tokens, backend="openalex")


        details_dict["resolved_record_types"] = {
            "crossref_types": crossref_types,
            "datacite_types": datacite_types,
            "openalex_types": openalex_types,
        }

        # ------------------------------------------------------------------
        # MAIN BACKEND: CROSSREF
        # ------------------------------------------------------------------
        if backend == "crossref":
            params = _build_params_from_rule_row(row)
            params["crossref_types"] = crossref_types
            params_display = {
                k: v for k, v in params.items()
                if v not in (None, [], "", {})
            }

            print("Resolved Crossref parameters:")
            if not params_display:
                print("  (none)")
            else:
                for k, v in params_display.items():
                    print(f"  - {k}: {v}")

            details_dict["crossref_params"] = params_display

            # >>> ADDED: build example URL for Crossref
            crossref_example_url = _build_crossref_example_url(
                date_start=date_start,
                date_end=date_end,
                params=params,
                mailto=mailto,
            )

            if dry_run:
                print("DRY RUN: No Crossref API calls, no files written.")
                main_rows = None
                main_note = "dry_run_crossref"
            else:
                df_server = harvest_preprints_filtered(
                    date_start,
                    date_end,
                    mailto=mailto,
                    prefixes=params["prefixes"],
                    members=params["members"],
                    group_titles_exact=params["group_titles_exact"],
                    group_title_contains=params["group_title_contains"],
                    institution_contains=params["institution_contains"],
                    url_contains=params["url_contains"],
                    doi_startswith=params["doi_startswith"],
                    doi_contains=params["doi_contains"],
                    issns=params["issns"],
                    crossref_types=crossref_types, 
                    count_only=False,
                    rows_per_call=rows_per_call,
                ) 

                df_server = _stamp_df(
                    df_server,
                    server_name=server_name,
                    backend="crossref",
                    rule_tokens=rule_tokens,
                    rule_row_id=rid,
                )

                df_server = add_canonical_columns(df_server, backend="crossref")
                main_rows = len(df_server)
                print(f"\n{server_name}: harvested {main_rows} records from crossref")


                safe_server = _safe_server_dir_name(server_name)
                server_dir = os.path.join(output_root, safe_server)
                os.makedirs(server_dir, exist_ok=True)

                # base = f"{safe_server}_{date_start}_{date_end}_crossref"
                base = f"{safe_server}_rule{rid}_{date_start}_{date_end}_crossref"

                main_parquet_path = os.path.join(server_dir, f"{base}.parquet")
                main_csv_path     = os.path.join(server_dir, f"{base}.csv")

                df_server.to_parquet(main_parquet_path, index=False)
                # df_server.to_csv(main_csv_path, index=False, encoding="utf-8-sig")
                df_server.to_csv(main_csv_path + ".gz", index=False, compression="gzip", encoding="utf-8-sig")


        # ------------------------------------------------------------------
        # MAIN BACKEND: DATACITE
        # ------------------------------------------------------------------
        elif backend == "datacite":
            client_ids_raw = row.get(client_id_col)
            client_ids = _parse_list_cell(client_ids_raw)

            # default fallback keeps old behavior
            resource_types = ["preprint"]
            if "text" in rule_tokens:
                resource_types.append("text")

            # ✅ de-dupe while preserving order
            seen = set()
            resource_types = [x for x in resource_types if not (x in seen or seen.add(x))]


            # ✅ if records_types says something else, use it
            types_query_override = None
            if datacite_types:
                resource_types = datacite_types
                # types_query_override = _datacite_type_query_from_tokens(resource_types)
                # details_dict["datacite_types_query_override"] = types_query_override

            # NEW: capture datacite post-filters from rule tokens
            datacite_doi_startswith = None
            if "doi_prefix_first_token" in rule_tokens:
                datacite_doi_startswith = _parse_list_cell(row.get("doi_prefix_first_token")) or None
            details_dict["datacite_doi_startswith"] = datacite_doi_startswith

            doi_prefix_query = None
            if datacite_doi_startswith:
                # parts = [f'doi:"{p}*"' for p in datacite_doi_startswith]
                # doi_prefix_query = "(" + " OR ".join(parts) + ")"
                # p already like "10.11588/artdok." so p + "*" => "10.11588/artdok.*"
                parts = [f"doi:{p}*" for p in datacite_doi_startswith]
                doi_prefix_query = "(" + " OR ".join(parts) + ")"

            details_dict["datacite_doi_prefix_query"] = doi_prefix_query


            print("Resolved DataCite parameters:")
            print(f"  - client_ids: {client_ids if client_ids else 'MISSING'}")
            # print(f"  - resource_types: ['preprint'] + {resource_types[1:]}")
            print(f"  - resource_types: {resource_types}")


            details_dict["client_ids"] = client_ids
            details_dict["resource_types"] = resource_types

            # >>> ADDED: build example URL for DataCite
            datacite_example_url = _build_datacite_example_url(
                date_start=date_start,
                date_end=date_end,
                client_ids=client_ids,
                resource_types=resource_types,
                mailto=mailto,
                types_query_override=types_query_override,
                doi_prefix_query=doi_prefix_query,
            )

            if not client_ids:
                print("  -> No client_id found even though 'client_id' is in rules.")
                main_rows = 0
                main_note = "missing_client_id"
            elif dry_run:
                print("DRY RUN: No DataCite API calls, no files written.")
                main_rows = None
                main_note = "dry_run_datacite"
            else:
                df_dc = harvest_datacite_for_client_ids(
                    mailto=mailto,
                    client_ids=client_ids,
                    resource_types=resource_types,
                    date_start=date_start,
                    date_end=date_end,
                    rows_per_call=rows_per_call,
                    # types_query_override=types_query_override,
                    doi_prefix_query=doi_prefix_query,
                )

                df_dc = _stamp_df(
                    df_dc,
                    server_name=server_name,
                    backend="datacite",
                    rule_tokens=rule_tokens,
                    rule_row_id=rid,
                )

                before = len(df_dc)

                df_dc = _datacite_post_filter_df(
                    df_dc,
                    doi_startswith=datacite_doi_startswith,
                    # optionally support domain filter for DataCite too if you want:
                    url_contains=_parse_list_cell(row.get("primary_domain")) if "primary_domain" in rule_tokens else None,
                    require_all=False,
                )
                df_dc = add_canonical_columns(df_dc, backend="datacite")

                after = len(df_dc)
                if datacite_doi_startswith:
                    print(f"[DataCite post-filter] doi_prefix_first_token kept {after}/{before}")


                main_rows = len(df_dc)
                print(f"{server_name}: harvested {main_rows} records from datacite")

                safe_server = _safe_server_dir_name(server_name)
                server_dir = os.path.join(output_root, safe_server)
                os.makedirs(server_dir, exist_ok=True)

                base = f"{safe_server}_rule{rid}_{date_start}_{date_end}_datacite"
                main_parquet_path = os.path.join(server_dir, f"{base}.parquet")
                main_csv_path     = os.path.join(server_dir, f"{base}.csv")

                df_dc.to_parquet(main_parquet_path, index=False)
                # df_dc.to_csv(main_csv_path, index=False, encoding="utf-8-sig")
                df_dc.to_csv(main_csv_path + ".gz", index=False, compression="gzip", encoding="utf-8-sig")
            

        # ------------------------------------------------------------------
        # MAIN BACKEND: NONE (but maybe OpenAlex-only)
        # ------------------------------------------------------------------
        # else:
        #     print("  -> No 'client_id' or Crossref-specific tokens. No main backend.")
        #     main_note = "no_main_backend"
        # MAIN BACKEND: OPENALEX
        elif backend == "openalex":
            print("  -> OpenAlex selected as main backend.")
            main_note = "main_backend_openalex"

        # MAIN BACKEND: NONE
        else:
            print("  -> No 'client_id', no Crossref tokens, and no OpenAlex source_id. No backend.")
            main_note = "no_backend"

        # ------------------------------------------------------------------
        # OPTIONAL: OPENALEX (IF source_id IN RULES)
        # ------------------------------------------------------------------
        if has_openalex:
            source_ids_raw = row.get(source_id_col)
            source_ids = _parse_list_cell(source_ids_raw)

            details_dict["openalex_params"] = {
                "source_ids": source_ids,
                "date_start": date_start,
                "date_end": date_end,
                "openalex_types": openalex_types,
            }

            # Build example URL (for debug / summary)
            openalex_example_url = _build_openalex_example_url(
                date_start=date_start,
                date_end=date_end,
                source_ids=source_ids,
                mailto=mailto,
                openalex_types=openalex_types,
            )

            if not source_ids:
                print("  -> 'source_id' token present but no source_id values. Skipping OpenAlex.")
                openalex_note = "missing_source_id"

            elif dry_run:
                print("DRY RUN: No OpenAlex API calls, no files written.")
                openalex_note = "dry_run_openalex"

                # still set paths for consistency (optional)
                safe_server = _safe_server_dir_name(server_name)
                server_dir = os.path.join(output_root, safe_server)
                os.makedirs(server_dir, exist_ok=True)

                ts2 = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
                src_token_raw = source_ids[0] if source_ids else "nosource"
                src_token = _safe_server_dir_name(src_token_raw)[:20]
                base_oa = f"{safe_server}_{src_token}_{date_start}_{date_end}_{ts2}_openalex"
                # if len(base_oa) > 80:
                #     base_oa = base_oa[:80]

                openalex_parquet_path = os.path.join(server_dir, f"{base_oa}.parquet")
                openalex_csv_path = os.path.join(server_dir, f"{base_oa}.csv")

            else:
                df_oa = harvest_openalex_for_source_ids(
                    source_ids=source_ids,
                    date_start=date_start,
                    date_end=date_end,
                    mailto=mailto,
                    per_page=200,
                    max_results=2000000,
                    openalex_types=openalex_types,
                    openalex_api_key=openalex_api_key,
                )

                df_oa = _stamp_df(
                    df_oa,
                    server_name=server_name,
                    backend="openalex",
                    rule_tokens=rule_tokens,
                    rule_row_id=rid,
                )

                df_oa = add_canonical_columns(df_oa, backend="openalex")
                openalex_rows = len(df_oa)

                # Build output paths FIRST (before writing)
                safe_server = _safe_server_dir_name(server_name)
                server_dir = os.path.join(output_root, safe_server)
                os.makedirs(server_dir, exist_ok=True)

                ts2 = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
                src_token_raw = source_ids[0] if source_ids else "nosource"
                src_token = _safe_server_dir_name(src_token_raw)[:20]

                base_oa = f"{safe_server}_{src_token}_{date_start}_{date_end}_{ts2}_openalex"
                # if len(base_oa) > 80:
                #     base_oa = base_oa[:80]

                openalex_parquet_path = os.path.join(server_dir, f"{base_oa}.parquet")
                openalex_csv_path     = os.path.join(server_dir, f"{base_oa}.csv")

                # Now write safely
                df_oa.to_parquet(openalex_parquet_path, index=False)
                df_oa.to_csv(openalex_csv_path + ".gz", index=False, compression="gzip", encoding="utf-8-sig")
                openalex_note = ""

                # If OpenAlex is the chosen backend, treat OpenAlex outputs as "main"
                if backend == "openalex":
                    main_rows = openalex_rows
                    main_parquet_path = openalex_parquet_path
                    main_csv_path = openalex_csv_path
                    main_note = "main_backend_openalex"


                # # Safe, short directory name for the server
                # safe_server = _safe_server_dir_name(server_name)
                # server_dir = os.path.join(output_root, safe_server)
                # os.makedirs(server_dir, exist_ok=True)

                # # Timestamp to avoid overwriting on reruns
                # ts = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

                # # Build a short token for source_ids (only first source_id, truncated)
                # if source_ids:
                #     src_token_raw = source_ids[0]
                # else:
                #     src_token_raw = "nosource"

                # src_token = _safe_server_dir_name(src_token_raw)[:20]

                # # Final *short* base name – no server name repetition, no list repr
                # base_oa = f"{safe_server}_{src_token}_{date_start}_{date_end}_{ts}_openalex"
                

                # # Extra safety: cap the length of the base name
                # # max_base_len = 80
                # # if len(base_oa) > max_base_len:
                # #     base_oa = base_oa[:max_base_len]

                # openalex_parquet_path = os.path.join(server_dir, f"{base_oa}.parquet")
                # openalex_csv_path     = os.path.join(server_dir, f"{base_oa}.csv")

                # df_oa.to_parquet(openalex_parquet_path, index=False)
                # # df_oa.to_csv(openalex_csv_path, index=False, encoding="utf-8-sig")
                # df_oa.to_csv(openalex_csv_path + ".gz", index=False, compression="gzip", encoding="utf-8-sig")
                # openalex_note = ""

        # store example URLs in details as well
        details_dict["crossref_example_url"] = crossref_example_url
        details_dict["datacite_example_url"] = datacite_example_url
        details_dict["openalex_example_url"] = openalex_example_url

        # build details JSON and summary row
        details_json = json.dumps(details_dict, ensure_ascii=False)

        summary_rows.append(
            {
                "server": server_name,
                "backend": backend,
                "rows": main_rows,
                "parquet_path": main_parquet_path,
                "csv_path": main_csv_path,
                "dry_run": dry_run,
                "openalex_rows": openalex_rows,
                "openalex_parquet_path": openalex_parquet_path,
                "openalex_csv_path": openalex_csv_path,
                "openalex_note": openalex_note,
                "crossref_example_url": crossref_example_url,
                "datacite_example_url": datacite_example_url,
                "openalex_example_url": openalex_example_url,
                "details": details_json,
                "note": main_note,
            }
        )

    # ---- save summary at the end ----
    if summary_rows:
        df_summary = pd.DataFrame(summary_rows)
        df_summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
        print(f"\nSummary saved to: {summary_path}")
        return df_summary

    print("\nNo servers processed.")
    return pd.DataFrame()
