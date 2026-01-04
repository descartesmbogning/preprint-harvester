# FAQ

## Why do you use `mailto` everywhere?

Crossref, DataCite, and OpenAlex ask clients to provide a contact email in
their requests. This is both polite and useful for debugging rate-limit
issues. Always set a real email that you monitor.

## How do I restrict to a subset of servers?

Use the `servers` argument in `harvest_servers_from_rules_sheet`, passing a
list of values from `Field_server_name`:

```python
harvest_servers_from_rules_sheet(
    sheet_csv_path_or_url="rules_sheet.csv",
    servers=["bioRxiv", "medRxiv"],
    ...
)
```

## Can I run in dry-run mode first?

Yes, set `dry_run=True`. This will print resolved parameters but will not
call any API or write files.

## How do I only use OpenAlex?

Set rules so that `source_id` is the only token and leave `client_id`,
`prefix`, `member`, etc. empty. The main backend will be `"none"`, but the
OpenAlex block will still run for those rows.
