# 🧬 Preprint Harvester

### **Crossref × DataCite × OpenAlex — Unified Metadata Harvester for Preprint Servers**

**Author:** .....  
**License:** MIT  
**Status:** Public Release v1.0

---

## ⭐ Overview

**Preprint Harvester** is a unified Python toolkit designed to automatically collect preprint metadata across **Crossref**, **DataCite**, and **OpenAlex**.

The toolkit reads a **Google Sheet–like rules table** that specifies how each preprint server should be queried, depending on:

* DOI prefixes  
* Crossref group titles  
* Members  
* Institution names  
* Domain-based URL patterns  
* DataCite client IDs  
* OpenAlex source IDs  

It supports:

✔ Multi-provider harvesting  
✔ Automatic output per server  
✔ Date-range filtering  
✔ Dry-run mode (no API calls)  
✔ Export to Parquet and CSV  
✔ Summary report generation  
✔ Reproducible metadata pipelines  

---

## 📌 Key Features

### 🔍 1. Multi-backend harvesting

* **Crossref**: prefix / member / group-title / domain / DOI-first-token  
* **DataCite**: client-id + resource-type filters  
* **OpenAlex**: primary_location.source.id + optional preprint filter  

### 🗂 2. Sheet-driven rules

Supports a full rule specification from a spreadsheet with columns:

```text
Field_server_name
include
rules
rule_1 … rule_7
prefixes
members
group_title
institution_name
primary_domain
primary_domain_extend
doi_prefix_first_token
client_id
source_id
```

### 📁 3. Reproducible outputs

For each server:

```text
/data/by_server/<SERVER_NAME>/
    server_YYYY-MM-DD_YYYY-MM-DD_crossref.parquet
    server_YYYY-MM-DD_YYYY-MM-DD_crossref.csv
    server_YYYY-MM-DD_YYYY-MM-DD_datacite.parquet
    server_YYYY-MM-DD_YYYY-MM-DD_openalex.parquet
```

Plus a global summary CSV.

### 🧪 4. Dry-run mode

Prints **exact API parameters** without calling the APIs.

### 🛡 5. Compliant with API best practices

* Uses polite `mailto=`  
* Retries for rate limits  
* Cursor-based pagination  

---

# 📦 Installation

### 1. Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/preprint-harvester.git
cd preprint-harvester
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. (Optional) Install in development mode

```bash
pip install -e .
```

---

# 📊 Input: Rules Sheet Format

The program expects a **CSV downloaded from Google Sheets**.

Example minimal structure:

| Field_server_name | include | rules              | client_id    | source_id     | prefixes   | group_title              | primary_domain |
| ----------------- | ------- | ------------------ | ------------ | ------------- | ---------- | ------------------------ | -------------- |
| Preprints.org     | yes     | prefix             |              |               | [10.20944] |                          |                |
| OSF Preprints     | yes     | group_title        |              |               |            | [Open Science Framework] |                |
| CERN Doc Server   | yes     | client_id/preprint | ["cern.cds"] |               |            |                          |                |
| engrxiv           | yes     | source_id          |              | ["s4306401442"] |            |                          |                |

Fields may contain: `"a,b"`, `"[a,b]"`, or a single value.

---

# 🚀 Usage

## 🔧 Running the harvester

The main entry point is:

```text
scripts/run_harvest_from_sheet.py
```

or directly via Python:

```python
from preprint_harvester.harvesters import harvest_servers_from_rules_sheet

harvest_servers_from_rules_sheet(
    sheet_csv_path_or_url="rules.csv",
    date_start="2025-01-01",
    date_end="2025-01-15",
    mailto="your.email@domain.com",
    dry_run=False
)
```

---

# 🧪 Dry Run (No API calls)

To print all constructed API parameters *without querying APIs*:

```python
harvest_servers_from_rules_sheet(
    "rules.csv",
    date_start="2025-01-01",
    date_end="2025-01-15",
    mailto="your.email@example.com",
    dry_run=True
)
```

You will see printed:

* Resolved Crossref filters  
* DataCite query + resource_type  
* OpenAlex API query  
* API URL examples  

---

# 🔍 Example API Calls (Automatically printed in summary)

### **Crossref Example**

```text
https://api.crossref.org/works?filter=from-posted-date:2025-01-01T00:00:00,
until-posted-date:2025-01-15T23:59:59,prefix:10.20944&type:posted-content
&rows=1000&cursor=*&mailto=you@example.com
```

### **DataCite Example**

```text
https://api.datacite.org/dois
  ?client-id=cern.cds
  &resource-type-id=Preprint
  &query=registered:[2025-01-01 TO 2025-01-15]
  &page[size]=1000
  &page[cursor]=1
```

### **OpenAlex Example**

```text
https://api.openalex.org/works
  ?filter=type:preprint,
           primary_location.source.id:s4306402450,
           from_publication_date:2025-01-01,
           to_publication_date:2025-01-15
  &per-page=200
  &cursor=*
  &mailto=you@example.com
```

---

# 📁 Output Structure

After running the harvester:

```text
data/
└── by_server/
    ├── Preprints.org/
    │   ├── Preprints.org_2025-01-01_2025-01-15_crossref.parquet
    │   └── Preprints.org_2025-01-01_2025-01-15_crossref.csv
    ├── OSF Preprints/
    ├── CERN Document Server/
    └── engrxiv/
harvest_summary_2025-01-01_2025-01-15_real.csv
```

Each folder contains **Crossref**, **DataCite**, and/or **OpenAlex** results depending on rules.

---

# 🧱 Directory Layout

```text
preprint-harvester/
│
├── README.md
├── LICENSE
├── requirements.txt
│
├── src/
│   └── preprint_harvester/
│       ├── __init__.py
│       └── harvesters.py
│
├── scripts/
│   └── run_harvest_from_sheet.py
│
├── examples/
│   ├── example_rules_sheet.csv
│   └── example_usage.ipynb
│
├── tests/
│
└── data/
```

---

# 📖 Example Notebook

See:

```text
/examples/example_usage.ipynb
```

It includes:

* Loading rules  
* Running dry-run  
* Fetching Crossref, DataCite, OpenAlex  
* Merging results  
* Displaying metadata  

---

# 🧪 Testing

Minimal example:

```bash
pytest tests/
```

You can add mock-based tests for:

* Parameter building  
* Filtering  
* Fake API responses  

---

# ⚠️ Troubleshooting

### ❗ 1. Crossref returns 429 Too Many Requests

Solution:

* Reduce `rows_per_call`  
* Add sleep time via polite_sleep  
* Ensure correct `mailto=`  

### ❗ 2. DataCite returns zero results

Check:

* `client_id` spelling  
* date range  
* resource-type combination  

### ❗ 3. OpenAlex returns no preprints

Make sure:

* The source actually deposits preprints  
* Remove `only_preprints=True` to test  

---

# 🤝 Contributing

All contributions are welcome!

1. Fork the repo  
2. Create a new branch  
3. Submit a pull request  

Please format code with:

```bash
black .
isort .
```

---

# 📚 Citation

If you use this tool in research:

```text
...... (2025).
Preprint Harvester: Unified Crossref–DataCite–OpenAlex Metadata Harvester.
GitHub Repository.
```
