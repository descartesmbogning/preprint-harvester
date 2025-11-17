import os
import time
import json
import requests
import pandas as pd
from datetime import datetime
from urllib.parse import urlparse, parse_qs

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


# ============================================================
#  Example URL builders (for summary)
# ============================================================
def _build_crossref_example_url(date_start, date_end, params, mailto):
    """
    Build a human-readable Crossref example URL for the summary.
    Uses first prefix/member/group-title when available.
    """
    if not date_start or not date_end:
        return None

    filters = [
        f"from-posted-date:{date_start}",
        f"until-posted-date:{date_end}",
        "type:posted-content",
    ]

    prefixes = params.get("prefixes") or []
    if prefixes:
        filters.append(f"prefix:{prefixes[0]}")

    members = params.get("members") or []
    if members:
        filters.append(f"member:{members[0]}")

    gts = params.get("group_titles_exact") or []
    if gts:
        filters.append(f"group-title:{gts[0]}")

    flt_str = ",".join(filters)
    return (
        f"{CROSSREF_WORKS}"
        f"?filter={flt_str}"
        f"&rows=5"
        f"&cursor=*"
        f"&mailto={requests.utils.quote(mailto)}"
    )


def _build_datacite_example_url(date_start, date_end, client_ids, resource_types, mailto):
    """
    Build a human-readable DataCite example URL for the summary.
    Uses first client_id and first resource_type.
    """
    client_ids = [c for c in (client_ids or []) if c]
    resource_types = [rt.lower() for rt in (resource_types or []) if rt]

    if not client_ids or not resource_types:
        return None

    cid = client_ids[0]
    rt = resource_types[0]

    params = [f"client-id={requests.utils.quote(cid)}"]

    if rt == "preprint":
        params.append("resource-type-id=Preprint")
    else:
        params.append(f"resource-type-id={requests.utils.quote(rt)}")

    # Build the registered date query like in harvest_datacite_for_client_ids
    if date_start or date_end:
        if date_start and date_end:
            q = f"registered:[{date_start} TO {date_end}]"
        elif date_start:
            q = f"registered:[{date_start} TO *]"
        else:
            q = f"registered:[* TO {date_end}]"
        params.append(
            "query=" + requests.utils.quote(q, safe="[]: ")
        )

    params.append("page[size]=5")
    params.append("page[cursor]=1")

    return DATACITE_DOIS + "?" + "&".join(params)


def _build_openalex_example_url(date_start, date_end, source_ids, mailto, only_preprints=True):
    """
    Build a human-readable OpenAlex example URL for the summary.
    Uses all source_ids from the row.
    """
    source_ids = [s for s in (source_ids or []) if s]
    if not source_ids:
        return None

    filter_parts = []
    if only_preprints:
        filter_parts.append("type:preprint")

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


def _extract_relations(rel):
    """Extract is-preprint-of / has-preprint / is-version-of lists from Crossref."""
    if not rel or not isinstance(rel, dict):
        return (None, None, None)

    def pick(kind):
        items = rel.get(kind) or []
        dois = []
        for it in items:
            doi = it.get("id")
            if doi and doi.lower().startswith("https://doi.org/"):
                doi = doi.split("org/", 1)[1]
            if doi:
                dois.append(doi)
        return "; ".join(sorted(set(dois))) if dois else None

    return (pick("is-preprint-of"), pick("has-preprint"), pick("is-version-of"))


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
    published_online_date = _date_from_parts(m.get("published-online"))
    published_print_date  = _date_from_parts(m.get("published-print"))
    accepted_date         = _date_from_parts(m.get("accepted"))
    approved_date         = _date_from_parts(m.get("approved"))

    is_preprint_of, has_preprint, is_version_of = _extract_relations(m.get("relation"))
    relation_json          = _json(m.get("relation"))

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
    issn_type_json         = _json(m.get("issn-type"))
    isbn_type_json         = _json(m.get("isbn-type"))
    alternative_id_json    = _json(m.get("alternative-id"))

    subjects               = "; ".join(m.get("subject") or []) if m.get("subject") else None
    subjects_json          = _json(m.get("subject"))
    language               = m.get("language")

    funders_json           = _json(m.get("funder"))
    reference_count        = m.get("reference-count")
    is_referenced_by_count = m.get("is-referenced-by-count")
    references_json        = _json(m.get("reference"))

    update_to_json         = _json(m.get("update-to"))
    update_policy          = m.get("update-policy")
    update_type            = _first(m.get("update-to"), "type")

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
        "relation_json": relation_json,
        "update_type": update_type,
        "update_policy": update_policy,
        "update_to_json": update_to_json,
        "archive_json": archive_json,
        "content_domain_json": content_domain_json,
        "assertion_json": assertion_json,
        "institution_json": institution_json,
        "group_title": group_title,
        "member": member,
        "source": source,
        "score": score,
        "abstract_raw": abstract_raw,
    }


def _filters_base(from_iso, until_iso):
    return [
        f"from-posted-date:{from_iso}",
        f"until-posted-date:{until_iso}",
        "type:posted-content",
    ]


def _fanout_api_filters(from_iso, until_iso, prefixes=None, members=None, group_titles_exact=None):
    base = _filters_base(from_iso, until_iso)
    sets = []

    prefixes = [str(p).strip() for p in (prefixes or []) if p]
    members  = [str(m).strip() for m in (members  or []) if m]
    gtitles  = [str(g).strip() for g in (group_titles_exact or []) if g]

    if prefixes or members or gtitles:
        for p in (prefixes or [None]):
            for m in (members or [None]):
                for g in (gtitles or [None]):
                    flt = list(base)
                    if p: flt.append(f"prefix:{p}")
                    if m: flt.append(f"member:{m}")
                    if g: flt.append(f"group-title:{g}")
                    sets.append(flt)
    else:
        sets.append(base)

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


def harvest_preprints_filtered(
    date_start,
    date_end,
    mailto=DEFAULT_MAILTO,
    prefixes=None,
    members=None,
    group_titles_exact=None,
    group_title_contains=None,
    institution_contains=None,
    url_contains=None,
    doi_startswith=None,
    doi_contains=None,
    require_all=False,
    dois_exact=None,
    count_only=False,
    rows_per_call=1000,
    sort_key="deposited",
    polite_sleep_s=0.0,
):
    """
    High-level Crossref harvested, used by the sheet-driven function.
    """
    _set_mailto(mailto)

    start_dt = datetime.fromisoformat(date_start).replace(hour=0, minute=0, second=0)
    end_dt   = datetime.fromisoformat(date_end).replace(hour=23, minute=59, second=59)
    from_iso  = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
    until_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%S")

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
        group_titles_exact=group_titles_exact
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

    # FULL DATA, but with client predicates → 2-pass (collect DOIs then enrich)
    keep_dois = []
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
                keep_dois.append(doi)
            seen.add(doi)

    for doi in keep_dois:
        try:
            m = _fetch_work_by_doi(doi)
            if m:
                rows.append(_one_row_wide(m))
                if polite_sleep_s > 0:
                    time.sleep(polite_sleep_s)
        except Exception as e:
            print(f"[warn] DOI fetch failed for {doi}: {e}")

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
    from html import unescape
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
    related_ids_json    = _json(a.get("relatedIdentifiers"))
    container_json      = _json(a.get("container"))
    funding_refs_json   = _json(a.get("fundingReferences"))
    rights_list_json    = _json(a.get("rightsList"))
    sizes_json          = _json(a.get("sizes"))
    formats_json        = _json(a.get("formats"))
    geo_locations_json  = _json(a.get("geoLocations"))
    references_json     = _json(a.get("references"))
    citations_json      = _json(a.get("citations"))
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
        "funding_refs_json": funding_refs_json, "rights_list_json": rights_list_json,
        "sizes_json": sizes_json, "formats_json": formats_json,
        "geo_locations_json": geo_locations_json, "references_json": references_json,
        "citations_json": citations_json, "url_alternate_json": url_alternate_json,
        "raw_attributes_json": raw_attributes_json,
        "raw_relationships_json": raw_relationships_json,
    }


def harvest_datacite_for_client_ids(
    mailto: str,
    client_ids,
    resource_types,
    date_start: str,
    date_end: str,
    rows_per_call: int = 1000,
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

        for rt in resource_types:
            if rt == "preprint":
                passes = [
                    {"resource_type_id": "Preprint", "query": None},
                    {"resource_type_id": None, "query": 'types.resourceType:"preprint"'},
                ]
            else:
                passes = [
                    {"resource_type_id": rt, "query": None},
                ]

            for sel in passes:
                base_params = {
                    "client-id": cid,
                    "disable-facets": "true",
                    "affiliation": "true",
                }
                if sel["resource_type_id"]:
                    base_params["resource-type-id"] = sel["resource_type_id"]

                existing_q = sel["query"]
                combined_query = None
                if existing_q and date_query:
                    combined_query = f"{existing_q} AND {date_query}"
                elif existing_q:
                    combined_query = existing_q
                elif date_query:
                    combined_query = date_query

                if combined_query:
                    base_params["query"] = combined_query

                total_params = dict(base_params)
                total_params["page[size]"] = 0
                total_params["page[cursor]"] = "1"

                debug_total = {
                    k: v for k, v in total_params.items()
                    if k in ("client-id", "resource-type-id", "query")
                }
                print("[DataCite total_params]", debug_total)

                js_total = _datacite_request(total_params, mailto=mailto)
                meta = js_total.get("meta") or {}
                total = int(meta.get("total") or 0)
                print(f"    slice {sel} → total={total}")

                pbar = None
                if _HAVE_TQDM and total > 0:
                    pbar = tqdm(total=total, desc="Fetching preprints", unit="rec")

                cursor = "1"
                first_page_logged = False
                while cursor:
                    params = dict(base_params)
                    params["page[size]"] = rows_per_call
                    params["page[cursor]"] = cursor

                    if not first_page_logged:
                        debug_params = {
                            k: v for k, v in params.items()
                            if k in ("client-id", "resource-type-id", "query")
                        }
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
        "is_paratext": work.get("is_paratext"),
        "primary_location_landing_page_url": primary.get("landing_page_url"),
        "primary_location_source_id": source.get("id"),
        "primary_location_source_display_name": source.get("display_name"),
        "primary_location_is_oa": oa.get("is_oa"),
        "primary_location_oa_status": oa.get("oa_status"),
        "authorships_json": _json(work.get("authorships")),
        "concepts_json": _json(work.get("concepts")),
        "topics_json": _json(work.get("topics")),
        "raw_openalex_json": _json(work),
    }


def harvest_openalex_for_source_ids(
    source_ids,
    date_start: str,
    date_end: str,
    mailto: str,
    per_page: int = 200,
    max_results: int = 200000,
    only_preprints: bool = True,
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

    # ✅ Restrict to preprints
    if only_preprints:
        filter_parts.append("type:preprint")

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

    while True:
        url = (
            f"{OPENALEX_WORKS}"
            f"?filter={filter_str}"
            f"&per-page={per_page}"
            f"&cursor={cursor}"
            f"&mailto={mailto}"
        )
        try:
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            data = r.json()
        except requests.RequestException as e:
            print(f"[OpenAlex] Error for {source_ids} cursor={cursor}: {e}")
            break

        results = data.get("results") or []
        if not results:
            break

        works.extend(results)
        if len(works) >= max_results:
            break

        cursor = data.get("meta", {}).get("next_cursor")
        if not cursor:
            break

    rows = [_openalex_one_row(w) for w in works]
    df = pd.DataFrame(rows)

    # Extra safety: keep only type == "preprint" if requested
    if only_preprints and not df.empty and "type" in df.columns:
        df = df[df["type"] == "preprint"].copy()

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

    if "prefix" in rule_tokens:
        prefixes_list = _parse_list_cell(row.get("Prefixes"))
        prefixes = prefixes_list or None

    if "member" in rule_tokens:
        members_list = _parse_list_cell(row.get("Members"))
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

    return dict(
        prefixes=prefixes,
        members=members,
        group_titles_exact=group_titles_exact,
        group_title_contains=group_title_contains,
        institution_contains=institution_contains,
        url_contains=url_contains,
        doi_startswith=doi_startswith,
        doi_contains=doi_contains,
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

    df_rules = pd.read_csv(sheet_csv_path_or_url, header=1)
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
    client_id_col = norm_cols.get("client_id", "client_id")
    source_id_col = norm_cols.get("source_id", "source_id")

    summary_rows = []
    summary_path = os.path.join(
        output_root,
        f"harvest_summary_{date_start}_{date_end}_{'dry' if dry_run else 'real'}.csv"
    )

    # crossref-related rule tokens
    crossref_tokens = {
        "prefix",
        "member",
        "group_title",
        "institution_name",
        "primary_domain",
        "doi_prefix_first_token",
    }

    for _, row in df_rules.iterrows():
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
        else:
            backend = "none"

        print("\n" + "=" * 70)
        print(f"Server: {server_name}  (backend: {backend})")
        print("=" * 70)
        print(f"Date window: {date_start} → {date_end}")
        print(f"Rules tokens: {rule_tokens}")

        details_dict["backend"] = backend

        # ------------------------------------------------------------------
        # MAIN BACKEND: CROSSREF
        # ------------------------------------------------------------------
        if backend == "crossref":
            params = _build_params_from_rule_row(row)
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
                    count_only=False,
                    rows_per_call=rows_per_call,
                )

                main_rows = len(df_server)
                print(f"\n{server_name}: harvested {main_rows} records from crossref")

                safe_server = (
                    str(server_name).strip()
                    .replace(" ", "_")
                    .replace("/", "_")
                )
                server_dir = os.path.join(output_root, safe_server)
                os.makedirs(server_dir, exist_ok=True)

                base = f"{safe_server}_{date_start}_{date_end}_crossref"
                main_parquet_path = os.path.join(server_dir, f"{base}.parquet")
                main_csv_path     = os.path.join(server_dir, f"{base}.csv")

                df_server.to_parquet(main_parquet_path, index=False)
                df_server.to_csv(main_csv_path, index=False, encoding="utf-8-sig")

        # ------------------------------------------------------------------
        # MAIN BACKEND: DATACITE
        # ------------------------------------------------------------------
        elif backend == "datacite":
            client_ids_raw = row.get(client_id_col)
            client_ids = _parse_list_cell(client_ids_raw)

            resource_types = ["preprint"]
            if "text" in rule_tokens:
                resource_types.append("text")

            print("Resolved DataCite parameters:")
            print(f"  - client_ids: {client_ids if client_ids else 'MISSING'}")
            print(f"  - resource_types: ['preprint'] + {resource_types[1:]}")

            details_dict["client_ids"] = client_ids
            details_dict["resource_types"] = resource_types

            # >>> ADDED: build example URL for DataCite
            datacite_example_url = _build_datacite_example_url(
                date_start=date_start,
                date_end=date_end,
                client_ids=client_ids,
                resource_types=resource_types,
                mailto=mailto,
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
                )

                main_rows = len(df_dc)
                print(f"{server_name}: harvested {main_rows} records from datacite")

                safe_server = (
                    str(server_name).strip()
                    .replace(" ", "_")
                    .replace("/", "_")
                )
                server_dir = os.path.join(output_root, safe_server)
                os.makedirs(server_dir, exist_ok=True)

                base = f"{safe_server}_{date_start}_{date_end}_datacite"
                main_parquet_path = os.path.join(server_dir, f"{base}.parquet")
                main_csv_path     = os.path.join(server_dir, f"{base}.csv")

                df_dc.to_parquet(main_parquet_path, index=False)
                df_dc.to_csv(main_csv_path, index=False, encoding="utf-8-sig")

        # ------------------------------------------------------------------
        # MAIN BACKEND: NONE (but maybe OpenAlex-only)
        # ------------------------------------------------------------------
        else:
            print("  -> No 'client_id' or Crossref-specific tokens. No main backend.")
            main_note = "no_main_backend"

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
            }

            # >>> ADDED: build example URL for OpenAlex
            openalex_example_url = _build_openalex_example_url(
                date_start=date_start,
                date_end=date_end,
                source_ids=source_ids,
                mailto=mailto,
                only_preprints=True,
            )

            if not source_ids:
                print("  -> 'source_id' token present but no source_id values. Skipping OpenAlex.")
                openalex_note = "missing_source_id"
            elif dry_run:
                print("DRY RUN: No OpenAlex API calls, no files written.")
                openalex_note = "dry_run_openalex"
            else:
                df_oa = harvest_openalex_for_source_ids(
                    source_ids=source_ids,
                    date_start=date_start,
                    date_end=date_end,
                    mailto=mailto,
                    per_page=200,
                    max_results=200000,
                )
                openalex_rows = len(df_oa)

                safe_server = (
                    str(server_name).strip()
                    .replace(" ", "_")
                    .replace("/", "_")
                )
                server_dir = os.path.join(output_root, safe_server)
                os.makedirs(server_dir, exist_ok=True)

                base_oa = f"{safe_server}_{date_start}_{date_end}_openalex"
                openalex_parquet_path = os.path.join(server_dir, f"{base_oa}.parquet")
                openalex_csv_path     = os.path.join(server_dir, f"{base_oa}.csv")

                df_oa.to_parquet(openalex_parquet_path, index=False)
                df_oa.to_csv(openalex_csv_path, index=False, encoding="utf-8-sig")
                openalex_note = ""

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
