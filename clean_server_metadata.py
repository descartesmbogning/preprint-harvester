import pandas as pd
import numpy as np
import re
from pathlib import Path


# ============================================================
# CONFIG
# ============================================================
INPUT_FILE = "data/server_metadata_raw.xlsx"   # change if needed
OUTPUT_FILE = "data/server_metadata_clean.csv"


# ============================================================
# BASIC HELPERS
# ============================================================
def norm_text(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()


def lower(x) -> str:
    return norm_text(x).lower()


def yes_no_unknown(x) -> str:
    txt = lower(x)
    if txt in {"yes", "y", "true", "1"}:
        return "Yes"
    if txt in {"no", "n", "false", "0"}:
        return "No"
    return "Unknown"


def yes_no_from_text(x) -> str:
    txt = lower(x)
    if txt == "":
        return "Unknown"
    if any(k in txt for k in ["yes", "available", "true"]):
        return "Yes"
    if any(k in txt for k in ["no", "not available", "false"]):
        return "No"
    return "Unknown"


def combine_text(*vals) -> str:
    parts = [norm_text(v) for v in vals if norm_text(v) != ""]
    return " | ".join(parts)


def contains_any(txt: str, keywords: list[str]) -> bool:
    return any(k in txt for k in keywords)


# ============================================================
# CLASSIFIERS
# ============================================================
def classify_scope(x: str) -> str:
    txt = lower(x)

    if txt == "":
        return "Other / unclear"

    if contains_any(txt, [
        "all scientific fields",
        "multiple scientific fields",
        "all fields of research",
        "multidisciplinary",
        "broad"
    ]):
        return "Multidisciplinary"

    if contains_any(txt, [
        "biomedical", "health", "medical", "clinical",
        "life sciences", "biology", "medicine", "translational"
    ]):
        return "Life sciences & biomedicine"

    if contains_any(txt, [
        "physics", "chemistry", "engineering",
        "systems science", "quantitative finance"
    ]):
        return "Physical sciences"

    if contains_any(txt, [
        "mathematics", "computer science", "statistics"
    ]):
        return "Math & computer science"

    if contains_any(txt, [
        "social sciences", "political science", "social care", "behavioural"
    ]):
        return "Social sciences"

    if contains_any(txt, [
        "economics", "business", "applied economics"
    ]):
        return "Economics & business"

    if contains_any(txt, [
        "humanities", "art", "photography", "design"
    ]):
        return "Humanities & arts"

    if contains_any(txt, [
        "agriculture", "agricultural"
    ]):
        return "Agriculture"

    if contains_any(txt, [
        "african", "arab", "regional", "community"
    ]):
        return "Region/community-specific"

    return "Other / unclear"


def classify_ownership(x: str) -> str:
    txt = lower(x)

    if txt == "":
        return "Other / unclear"

    if contains_any(txt, ["publisher", "publishing organisation", "technology provider"]):
        return "Publisher"

    if contains_any(txt, ["academic institution", "university", "library"]):
        return "Academic institution"

    if contains_any(txt, ["community group"]):
        return "Academic community group"

    if contains_any(txt, ["scientific society", "society"]):
        return "Scientific society"

    if contains_any(txt, ["funder", "funding organisation", "membership organisation"]):
        return "Funder"

    if contains_any(txt, ["independent", "community-led"]):
        return "Independent / community-led"

    return "Other / unclear"


def classify_submission(x: str) -> str:
    txt = lower(x)

    if txt == "":
        return "Mixed / unclear"

    if "not active" in txt:
        return "Not active"

    if "no direct submission" in txt:
        return "No direct submission"

    if contains_any(txt, ["organization", "organisation", "institution"]):
        return "Organization / institution"

    if contains_any(txt, ["researcher (specific", "specific eligibility", "researcher (specific eligibility)"]):
        return "Researcher (specific eligibility)"

    if "researcher" in txt:
        return "Researcher"

    return "Mixed / unclear"


def classify_platform_model(x: str) -> str:
    txt = lower(x)

    if txt in {"", "na", "n/a", "no", "false"}:
        return "Preprint only"

    if all(k in txt for k in ["publish", "review", "curate"]):
        return "Publish-review-curate"

    return "Hybrid / mixed"


def classify_platform_type(self_def: str, def_main: str, def_non_main: str) -> str:
    txt = combine_text(self_def, def_main, def_non_main).lower()

    if txt == "":
        return "Other / unclear"

    if "preprint server" in txt:
        return "Preprint server"

    if "e-print archive" in txt:
        return "E-print archive"

    if "community-led digital archive" in txt or "community archive" in txt:
        return "Community archive"

    if contains_any(txt, ["repository", "web repository", "digital archive", "archive"]):
        return "Repository"

    if contains_any(txt, ["publishing venue", "publishing platform", "open research publishing venue", "open research"]):
        return "Publishing venue / platform"

    if "prepublication platform" in txt:
        return "Prepublication platform"

    return "Other / unclear"


def classify_screening(processes: str, notes: str) -> str:
    txt = combine_text(processes, notes).lower()

    if txt == "" or txt in {"n/a", "na", "no information"}:
        return "None / not described"

    strong_terms = [
        "ethical", "legal", "harm", "misconduct", "reporting standards",
        "data is available", "code is available", "ethics", "irb"
    ]
    moderate_terms = [
        "plagiarism", "text overlap", "author screening", "endorsement"
    ]
    basic_terms = [
        "scope", "format", "basic quality", "metadata complete",
        "manuscript is complete", "complete", "formatting", "suitability"
    ]

    if contains_any(txt, strong_terms):
        return "Strong screening"

    if contains_any(txt, moderate_terms):
        return "Moderate screening"

    if contains_any(txt, basic_terms):
        return "Basic screening"

    return "Basic screening"


def classify_review_type(x: str) -> str:
    txt = lower(x)

    if txt in {"", "na", "n/a", "none", "no"}:
        return "None"

    if "open" in txt:
        return "Open peer review"

    if contains_any(txt, ["public", "community"]):
        return "Public/community review"

    if contains_any(txt, ["single-blind", "single blind"]):
        return "Single-blind"

    return "Other"


def classify_versioning(x: str) -> str:
    txt = lower(x)

    if txt in {"", "na", "n/a"}:
        return "Unknown"

    if contains_any(txt, ["yes", "version", "update", "updated", "authors can update"]):
        return "Yes"

    return "No"


def classify_publication_status(x: str) -> str:
    txt = lower(x)

    if txt in {"", "na", "n/a"}:
        return "Unknown"

    if contains_any(txt, ["yes", "published", "peer-reviewed version", "doi"]):
        return "Yes"

    return "No"


def classify_metrics_text(x: str) -> str:
    txt = lower(x)
    if txt in {"", "na", "n/a"}:
        return "No"
    return "Yes"


def classify_permanence(x: str) -> str:
    txt = lower(x)

    if txt == "":
        return "Unclear"

    if "no removal" in txt:
        return "Permanent, no removal"

    if "withdraw" in txt:
        return "Permanent with withdrawal marking"

    if contains_any(txt, ["some removal", "exceptional circumstances", "special circumstances"]):
        return "Permanent with limited removal exceptions"

    if "permanent" in txt:
        return "Permanent with limited removal exceptions"

    return "Unclear"


def classify_charges(x: str) -> str:
    txt = lower(x)

    if txt in {"", "na", "n/a", "no information"}:
        return "Unknown"

    if contains_any(txt, ["no fee", "no fees", "free of charge", "free"]):
        return "Free"

    if contains_any(txt, ["apc", "article processing charge", "payable upon acceptance"]):
        return "APC / publication charges"

    if contains_any(txt, ["small charge", "fee", "fees", "usd per paper"]):
        return "Some fees"

    return "Unknown"


def detect_non_peer_review_notice(*texts: str) -> str:
    txt = combine_text(*texts).lower()
    patterns = [
        "not peer reviewed",
        "not been peer reviewed",
        "non-reviewed",
        "has not been peer-reviewed",
        "not peer-reviewed"
    ]
    return "Yes" if contains_any(txt, patterns) else "No"


def detect_warning_label(row: pd.Series) -> str:
    fields = [
        "Any Warning message on main page",
        "Any Warning on a single preprint page",
        "Do they have a general warning label",
    ]
    vals = [lower(row.get(c, "")) for c in fields]
    return "Yes" if any(v in {"yes", "y"} for v in vals) else "No"


def detect_indexed_flag(x: str, keyword: str) -> str:
    txt = lower(x)
    return "Yes" if keyword in txt else "No"


def classify_indexing_group(row: pd.Series) -> str:
    flags = sum([
        row["indexed_google_scholar"] == "Yes",
        row["indexed_crossref"] == "Yes",
        row["indexed_europe_pmc"] == "Yes",
        row["indexed_pubmed_pathway"] == "Yes",
    ])
    if flags >= 3:
        return "Broadly indexed"
    if flags >= 1:
        return "Selectively indexed"
    return "Minimal / unclear indexing"


# ============================================================
# COLUMN CHECKER
# ============================================================
REQUIRED_COLUMNS = [
    "source_id",
    "Server Name",
    "Server Name Rule File",
    "Server Main Page Link",
    "Active (in 2026)",
    "Who can submit text",
    "Researcher can submit",
    "Accepts Preprints ONLY",
    "Publish - Review - Curate models",
    "Definition of server/platform on the main page (Y/N)",
    "Text of the Definition of the server/platform on the main page ",
    "SelfDefinitiononMainPage",
    "Definition on non-main page",
    "Review Type",
    "Versioning",
    "Publication Status",
    "can identify peer reviewed version",
    "Disciplinary scope",
    "Ownership type",
    "Screening processes",
    "Additional notes on screening process",
    "External content indexing",
    "Permanence of content",
    "Charges for Services",
    "is statistics available ?",
    "metrics to track their preprints",
    "Any Warning message on main page",
    "Text of message",
    "Any Warning on a single preprint page",
    "Text of single page warning",
    "Any warning on the text of COVID 19 page",
    "Text of COVID page warning",
    "Do they have a general warning label",
    "Is there a separate warning label for medical preprints",
    "Text of Preprint definition",
]


def warn_missing_columns(df: pd.DataFrame):
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        print("\n[WARNING] Missing columns:")
        for c in missing:
            print(f" - {c}")
        print("\nThe script will still run if those fields are not critical, but some outputs may be 'Unknown'.\n")


# ============================================================
# MAIN CLEANING FUNCTION
# ============================================================
def build_clean_metadata(raw: pd.DataFrame) -> pd.DataFrame:
    raw = raw.copy()
    raw.columns = [str(c).strip() for c in raw.columns]

    def col(name: str) -> pd.Series:
        if name in raw.columns:
            return raw[name]
        return pd.Series([""] * len(raw), index=raw.index)

    clean = pd.DataFrame({
        "server_name": col("Server Name").map(norm_text),
        "server_name_rule_file": col("Server Name Rule File").map(norm_text),
        "source_id": col("source_id").map(norm_text),
        "server_main_page_link": col("Server Main Page Link").map(norm_text),

        "is_active_2026": col("Active (in 2026)").map(yes_no_unknown),
        "scope_group": col("Disciplinary scope").map(classify_scope),
        "ownership_group": col("Ownership type").map(classify_ownership),
        "submission_group": col("Who can submit text").map(classify_submission),
        "researcher_can_submit": col("Researcher can submit").map(yes_no_unknown),
        "is_preprint_only": col("Accepts Preprints ONLY").map(yes_no_unknown),
        "platform_model": col("Publish - Review - Curate models").map(classify_platform_model),

        "review_type_group": col("Review Type").map(classify_review_type),
        "versioning_available": col("Versioning").map(classify_versioning),
        "publication_status_tracking": col("Publication Status").map(classify_publication_status),
        "can_identify_peer_reviewed_version": col("can identify peer reviewed version").map(yes_no_unknown),

        "has_medical_warning": col("Is there a separate warning label for medical preprints").map(yes_no_unknown),
        "has_server_statistics": col("is statistics available ?").map(yes_no_unknown),
        "has_preprint_metrics": col("metrics to track their preprints").map(classify_metrics_text),

        "raw_disciplinary_scope": col("Disciplinary scope").map(norm_text),
        "raw_ownership_type": col("Ownership type").map(norm_text),
        "raw_submission_text": col("Who can submit text").map(norm_text),
        "raw_screening_processes": col("Screening processes").map(norm_text),
        "raw_external_content_indexing": col("External content indexing").map(norm_text),
        "raw_permanence": col("Permanence of content").map(norm_text),
        "raw_charges": col("Charges for Services").map(norm_text),
        "raw_self_definition": col("SelfDefinitiononMainPage").map(norm_text),
        "raw_platform_model": col("Publish - Review - Curate models").map(norm_text),
        "raw_versioning": col("Versioning").map(norm_text),
        "raw_publication_status": col("Publication Status").map(norm_text),
    })

    clean["platform_type_group"] = raw.apply(
        lambda r: classify_platform_type(
            r.get("SelfDefinitiononMainPage", ""),
            r.get("Text of the Definition of the server/platform on the main page ", ""),
            r.get("Definition on non-main page", "")
        ),
        axis=1
    )

    clean["screening_level"] = raw.apply(
        lambda r: classify_screening(
            r.get("Screening processes", ""),
            r.get("Additional notes on screening process", "")
        ),
        axis=1
    )

    clean["has_warning_label"] = raw.apply(detect_warning_label, axis=1)

    clean["has_non_peer_review_notice"] = raw.apply(
        lambda r: detect_non_peer_review_notice(
            r.get("Text of message", ""),
            r.get("Text of single page warning", ""),
            r.get("Text of COVID page warning", ""),
            r.get("Text of Preprint definition", ""),
            r.get("SelfDefinitiononMainPage", "")
        ),
        axis=1
    )

    clean["indexed_google_scholar"] = col("External content indexing").map(
        lambda x: detect_indexed_flag(x, "google scholar")
    )
    clean["indexed_crossref"] = col("External content indexing").map(
        lambda x: detect_indexed_flag(x, "crossref")
    )
    clean["indexed_europe_pmc"] = col("External content indexing").map(
        lambda x: detect_indexed_flag(x, "europe pmc")
    )
    clean["indexed_pubmed_pathway"] = col("External content indexing").map(
        lambda x: "Yes" if contains_any(lower(x), ["pubmed", "prepubmed"]) else "No"
    )

    clean["indexing_group"] = clean.apply(classify_indexing_group, axis=1)
    clean["permanence_group"] = col("Permanence of content").map(classify_permanence)
    clean["charges_group"] = col("Charges for Services").map(classify_charges)

    # remove empty server names
    clean = clean[clean["server_name"] != ""].copy()

    # deduplicate by server_name, keep first
    clean = clean.drop_duplicates(subset=["server_name"]).reset_index(drop=True)

    return clean


# ============================================================
# OPTIONAL: SUMMARY TABLES FOR QA
# ============================================================
def print_category_summary(df: pd.DataFrame, columns: list[str], top_n: int = 20):
    print("\n=== CATEGORY SUMMARY ===")
    for c in columns:
        if c not in df.columns:
            continue
        print(f"\n--- {c} ---")
        print(df[c].value_counts(dropna=False).head(top_n).to_string())


def export_qc_tables(df: pd.DataFrame, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)

    qa_cols = [
        "is_active_2026",
        "scope_group",
        "ownership_group",
        "submission_group",
        "researcher_can_submit",
        "is_preprint_only",
        "platform_model",
        "platform_type_group",
        "screening_level",
        "has_warning_label",
        "has_medical_warning",
        "has_non_peer_review_notice",
        "review_type_group",
        "versioning_available",
        "publication_status_tracking",
        "can_identify_peer_reviewed_version",
        "indexing_group",
        "has_server_statistics",
        "has_preprint_metrics",
        "permanence_group",
        "charges_group",
    ]

    freq_rows = []
    for c in qa_cols:
        if c not in df.columns:
            continue
        vc = df[c].value_counts(dropna=False)
        for k, v in vc.items():
            freq_rows.append({
                "column": c,
                "category": k,
                "count": int(v)
            })

    freq_df = pd.DataFrame(freq_rows)
    freq_df.to_csv(out_dir / "server_metadata_clean_frequencies.csv", index=False)

    df.to_csv(out_dir / "server_metadata_clean_preview.csv", index=False)


# ============================================================
# RUN
# ============================================================
def read_input_file(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    if p.suffix.lower() == ".csv":
        return pd.read_csv(p)
    elif p.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(p)
    else:
        raise ValueError("Input file must be CSV or Excel.")


def main():
    raw = read_input_file(INPUT_FILE)
    warn_missing_columns(raw)

    clean = build_clean_metadata(raw)
    clean.to_csv(OUTPUT_FILE, index=False)

    print(f"\nSaved cleaned metadata: {OUTPUT_FILE}")
    print(f"Rows: {len(clean):,}")
    print(f"Columns: {len(clean.columns):,}")

    print_category_summary(clean, [
        "is_active_2026",
        "scope_group",
        "ownership_group",
        "submission_group",
        "is_preprint_only",
        "platform_model",
        "platform_type_group",
        "screening_level",
        "indexing_group",
        "permanence_group",
        "charges_group",
    ])

    export_qc_tables(clean, Path("qc_server_metadata"))
    print("\nQC files saved to: qc_server_metadata/")


if __name__ == "__main__":
    main()