# CLI / script usage

The main entry point in Python is the function:

```python
harvest_servers_from_rules_sheet(
    sheet_csv_path_or_url: str,
    servers=None,
    date_start: str = "2000-01-01",
    date_end: str = "2025-10-11",
    mailto: str = "you@example.org",
    output_root: str = "data/by_server_datacite_crossref_openalex",
    rows_per_call: int = 1000,
    dry_run: bool = False,
)
```

## Minimal script

Create `scripts/run_harvest.py` in your codebase:

```python
from your_module import harvest_servers_from_rules_sheet

if __name__ == "__main__":
    df_summary = harvest_servers_from_rules_sheet(
        sheet_csv_path_or_url="rules_sheet.csv",
        date_start="2020-01-01",
        date_end="2025-10-11",
        mailto="you@example.org",
        output_root="data/by_server_datacite_crossref_openalex",
        rows_per_call=1000,
        dry_run=False,
    )
    print(df_summary.head())
```

Then run:

```bash
python scripts/run_harvest.py
```

You can also wrap this into a CLI using `argparse` if needed.
