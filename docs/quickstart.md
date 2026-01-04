# Quickstart

This page shows how to run the harvester end to end with a minimal setup.

## 1. Install dependencies

Create and activate a virtual environment, then install your Python deps
(including `pandas`, `requests`, and `pyarrow`):

```bash
python -m venv .venv
source .venv/bin/activate  # on Windows: .venv\Scripts\activate
pip install pandas requests pyarrow
```

For the docs site:

```bash
npm install
```

## 2. Prepare the rules sheet

Export your Google Sheet as CSV (or download it) so that you have something
like:

- `rules_sheet.csv`

The sheet must at least contain:

- `Field_server_name`
- `include`
- `rules` and/or `rule_1`..`rule_7`

Plus the parameter columns referenced by rules (e.g. `Prefixes`, `Members`,
`client_id`, `source_id`, `primary_domain`, etc.).

See [Rules sheet](./rules-sheet.md) for the full schema.

## 3. Call the main function

The main orchestrator is:

```python
from your_module import harvest_servers_from_rules_sheet

df_summary = harvest_servers_from_rules_sheet(
    sheet_csv_path_or_url="rules_sheet.csv",
    servers=None,  # or a list like ["bioRxiv", "medRxiv"]
    date_start="2020-01-01",
    date_end="2025-10-11",
    mailto="you@example.org",
    output_root="data/by_server_datacite_crossref_openalex",
    rows_per_call=1000,
    dry_run=False,
)
```

This will:

1. Read the rules sheet.
2. Decide per row which backend(s) to use:
   - Crossref (based on `prefix`, `member`, `group_title`, etc.),
   - DataCite (based on `client_id`),
   - OpenAlex (based on `source_id`).
3. Create per-server subfolders with Parquet + CSV.
4. Write a global summary CSV listing what was harvested.

## 4. Example API calls

These are the kinds of API calls issued internally.

### Crossref example

```text
https://api.crossref.org/works
  ?filter=from-posted-date:2020-01-01T00:00:00,until-posted-date:2025-10-11T23:59:59,type:posted-content,prefix:10.1101
  &rows=1000
  &cursor=*
  &sort=deposited
  &order=asc
  &mailto=you@example.org
```

### DataCite example

```text
https://api.datacite.org/dois
  ?client-id=cern.cds
  &resource-type-id=Preprint
  &page[size]=1000
  &page[cursor]=1
  &query=registered:[2020-01-01 TO 2025-10-11]
```

Or using the text `"preprint"` in the `types.resourceType` field:

```text
https://api.datacite.org/dois
  ?client-id=cern.cds
  &page[size]=1000
  &page[cursor]=1
  &query=types.resourceType:"preprint" AND registered:[2020-01-01 TO 2025-10-11]
```

### OpenAlex example

```text
https://api.openalex.org/works
  ?filter=type:preprint,primary_location.source.id:s3006283864|s4306401238,from_publication_date:2020-01-01,to_publication_date:2025-10-11
  &per-page=200
  &cursor=*
  &mailto=you@example.org
```

Only `type:preprint` works are kept by default.
