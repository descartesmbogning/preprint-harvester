# Preprint Harvester — Pipeline and Repository Structure

## Overview

This project builds a global dataset of preprints (1990–2025).

The pipeline follows five main stages:

1. Data collection + cleaning  
2. Date processing  
3. Deduplication  
4. Parent selection  
5. Final datasets  


---

## Pipeline Stages

### Stage 1 — Data collection + cleaning

This stage collects metadata from multiple sources and prepares it in a common format.

It includes:
- querying APIs and OAI-PMH sources  
- assigning server and backend names  
- basic field formatting  
- storing records by server  

**Main code**
- `src/preprint_harvester/harvesters.py`
- `scripts/run_harvest_from_sheet.py`
- `scripts/harvest_jxiv.py`

**Main folders**
- `data/by_server/`

**Outputs**
- server-level files in `data/by_server/`
- harvest summaries (`harvest_summary_*.csv`)

---

### Stage 2 — Date processing

This stage estimates when each record first appeared publicly.

It includes:
- checking multiple date fields  
- selecting the earliest valid date  
- correcting SSRN records when needed  

**Main notebooks**
- `notebooks/00_ssrn_date_exploration.ipynb`
- `notebooks/1_compute_earliest_public_appearance.ipynb`

**Outputs**
- processed data in `notebooks/outputs_new/`

---

### Stage 3 — Deduplication

This stage groups records that represent the same work.

It includes:
- matching by title and authors  
- matching using version relationships  

**Main notebooks**
- `notebooks/2_identify_duplicate_and_versioned_records___title_author.ipynb`
- `notebooks/3_identify_versioned_records___version_relationships.ipynb`

**Outputs**
- cluster files and metrics in `notebooks/outputs_new/`

---

### Stage 4 — Parent selection

This stage selects one main record per group.

It includes:
- harmonizing cluster identifiers  
- selecting a parent record  
- linking duplicates to the parent  

**Main notebook**
- `notebooks/4_harmonize_cluster_identifiers_and_select_parent_records.ipynb`

**Outputs**
- updated datasets in `notebooks/outputs_new/`

---

### Stage 5 — Final datasets

This stage produces the final datasets and tracking outputs.

It includes:
- building the final dataset  
- generating parent index  
- generating tracker files  
- updating Google Sheet outputs  

**Main notebooks**
- `notebooks/5_generate_final_datasets.ipynb`
- `notebooks/5_generate_tracker_files.ipynb`
- `notebooks/5_update_google_sheet_file.ipynb`

**Outputs**
- `parent_version_index.csv`
- `parent_version_index.parquet`
- `tracker_data/`
- `parent/`
- `packets/`

All stored in:
- `notebooks/outputs_new/`

---
## Repository Structure

The repository is organized to match the pipeline stages.

```
preprint-harvester/
├── README.md
├── data/
│   ├── SSRNData/
│   │   └── SSRNData.txt
│   ├── SSRNData.zip
│   └── by_server/
│       ├── AIJR_Preprints/
│       ├── arXiv/
│       ├── bioRxiv/
│       ├── Jxiv/
│       ├── medRxiv/
│       └── ...
├── src/
│   └── preprint_harvester/
│       ├── __init__.py
│       └── harvesters.py
├── scripts/
│   ├── run_harvest_from_sheet.py
│   └── harvest_jxiv.py
├── notebooks/
│   ├── 00_explore_metadata_patterns_and_identify_review_partof_records_to_exclude.ipynb
│   ├── 00_ssrn_date_exploration.ipynb
│   ├── 1_compute_earliest_public_appearance.ipynb
│   ├── 2_identify_duplicate_and_versioned_records___title_author.ipynb
│   ├── 3_identify_versioned_records___version_relationships.ipynb
│   ├── 4_harmonize_cluster_identifiers_and_select_parent_records.ipynb
│   ├── 5_generate_final_datasets.ipynb
│   ├── 5_generate_tracker_files.ipynb
│   ├── 5_update_google_sheet_file.ipynb
│   └── outputs_new/
├── merge_by_server_backends.py
├── merge_all_backends_fullschema.py
├── preview_backends.py
├── requirements.txt
└── structure.txt
```

---

## How Code Maps to the Pipeline

| Stage | Description | Code / Notebook | Output |
|------|------------|----------------|--------|
| Stage 1 | Data collection + cleaning | `harvesters.py`, `run_harvest_from_sheet.py`, `harvest_jxiv.py` | `data/by_server/` |
| Stage 2 | Date processing | `00_ssrn_date_exploration.ipynb`, `1_compute_earliest_public_appearance.ipynb` | `outputs_new/` |
| Stage 3 | Deduplication | `2_*.ipynb`, `3_*.ipynb` | `outputs_new/` |
| Stage 4 | Parent selection | `4_*.ipynb` | `outputs_new/` |
| Stage 5 | Final datasets | `5_*.ipynb` | `outputs_new/` |

---

## Summary

- Raw data is stored in: `data/by_server/`  
- Processed data is stored in: `notebooks/outputs_new/`  
- Each stage is clearly separated in notebooks and scripts  



---

---
## How to Use This Repository

This section shows how to use the repository step by step.

The pipeline has 5 stages. You should follow them in order.

---

## Quick Start

If you want a fast overview, follow these steps:

1. Run data collection scripts  
2. Open notebook for date processing  
3. Run deduplication notebooks  
4. Run parent selection notebook  
5. Run final dataset notebooks  

Main outputs will be in:
- `notebooks/outputs_new/`

---

## Step-by-Step Usage

### Step 1 — Collect metadata

This step collects records from multiple sources and stores them by server.

**Main files**
- `src/preprint_harvester/harvesters.py`
- `scripts/run_harvest_from_sheet.py`
- `scripts/harvest_jxiv.py`

**Input**
- API access (configured in `.env`)
- server rules (from sheet or script)

**Output**
- data stored in:
  - `data/by_server/`
- summary files:
  - `data/by_server/harvest_summary_*.csv`

---

### Step 2 — Process dates

This step finds the earliest date when each record appeared online.

**Main notebooks**
- `notebooks/00_ssrn_date_exploration.ipynb`
- `notebooks/1_compute_earliest_public_appearance.ipynb`

**Input**
- harvested data from `data/by_server/`
- SSRN data from `data/SSRNData/`

**Output**
- records with `date_first_seen`
- stored in:
  - `notebooks/outputs_new/`

---

### Step 3 — Identify duplicate and versioned records

This step groups records that refer to the same work.

It uses:
- title and author similarity
- version relationships

**Main notebooks**
- `notebooks/2_identify_duplicate_and_versioned_records___title_author.ipynb`
- `notebooks/3_identify_versioned_records___version_relationships.ipynb`

**Output**
- grouped records
- cluster files
- metrics and reports

Stored in:
- `notebooks/outputs_new/`

---

### Step 4 — Select parent records

This step selects one main record for each group.

**Main notebook**
- `notebooks/4_harmonize_cluster_identifiers_and_select_parent_records.ipynb`

**Output**
- parent record assigned for each group
- updated dataset with hierarchy

Stored in:
- `notebooks/outputs_new/`

---

### Step 5 — Generate final datasets

This step builds the final datasets and tracking outputs.

**Main notebooks**
- `notebooks/5_generate_final_datasets.ipynb`
- `notebooks/5_generate_tracker_files.ipynb`
- `notebooks/5_update_google_sheet_file.ipynb`

**Output folder**
- `notebooks/outputs_new/`

**Main outputs**
- `parent_version_index.csv`
- `parent_version_index.parquet`
- `tracker_data/`
- `parent/`
- `packets/`

---

## How the Pipeline Flows
```
Stage 1 → data/by_server/
↓
Stage 2 → outputs_new/ (with dates)
↓
Stage 3 → outputs_new/ (clusters)
↓
Stage 4 → outputs_new/ (parent records)
↓
Stage 5 → outputs_new/ (final datasets)
```

---

## Recommended Order for New Users

If you are new, follow this order:

1. Read this README  
2. Run:
   - `scripts/run_harvest_from_sheet.py`
3. Open:
   - `notebooks/1_compute_earliest_public_appearance.ipynb`
4. Run:
   - `notebooks/2_identify_duplicate_and_versioned_records___title_author.ipynb`
5. Then:
   - `notebooks/3_identify_versioned_records___version_relationships.ipynb`
6. Then:
   - `notebooks/4_harmonize_cluster_identifiers_and_select_parent_records.ipynb`
7. Finally:
   - `notebooks/5_generate_final_datasets.ipynb`
   - `notebooks/5_generate_tracker_files.ipynb`

---

## Summary Table

| Step | Goal | Main files | Output |
|------|------|-----------|--------|
| 1 | Collect data | `harvesters.py`, `run_harvest_from_sheet.py`, `harvest_jxiv.py` | `data/by_server/` |
| 2 | Process dates | `1_compute_earliest_public_appearance.ipynb` | `outputs_new/` |
| 3 | Deduplicate | `2_*.ipynb`, `3_*.ipynb` | `outputs_new/` |
| 4 | Select parent | `4_*.ipynb` | `outputs_new/` |
| 5 | Final datasets | `5_*.ipynb` | `outputs_new/` |

---

## Notes

- Raw data is stored in: `data/by_server/`  
- All processed outputs are stored in: `notebooks/outputs_new/`  
- Each stage builds on the previous one  
- Notebooks should be run in order  

---


