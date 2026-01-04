# File layout

A typical run will produce:

```text
data/by_server_datacite_crossref_openalex/
  bioRxiv/
    bioRxiv_2020-01-01_2025-10-11_crossref.parquet
    bioRxiv_2020-01-01_2025-10-11_crossref.csv
    bioRxiv_2020-01-01_2025-10-11_datacite.parquet
    bioRxiv_2020-01-01_2025-10-11_datacite.csv
    bioRxiv_2020-01-01_2025-10-11_openalex.parquet
    bioRxiv_2020-01-01_2025-10-11_openalex.csv
  medRxiv/
    ...
  ...
  harvest_summary_2020-01-01_2025-10-11_real.csv
```

The **summary CSV** includes one row per server with columns like:

- `server`
- `backend` (crossref / datacite / none)
- `rows` (count from main backend)
- `parquet_path` (path to main backend file)
- `csv_path`
- `openalex_rows`
- `openalex_parquet_path`
- `openalex_csv_path`
- `details` (JSON with parameters used)
- `note` (diagnostic note, e.g. `no_rules_defined`, `missing_client_id`)
