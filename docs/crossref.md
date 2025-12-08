# Crossref backend

The Crossref backend queries the `/works` API using cursor-based pagination.
It focuses on `type:posted-content` and uses `from-posted-date` /
`until-posted-date` as the date window.

## Filters

For a given server, filters are built from the rules sheet via:

```python
def _filters_base(from_iso, until_iso):
    return [
        f"from-posted-date:{from_iso}",
        f"until-posted-date:{until_iso}",
        "type:posted-content",
    ]
```

Additional filters are added for:

- `prefix` (from `Prefixes` column)
- `member` (from `Members` column)
- `group-title` (from `group_title` column)

The code can fan out across combinations of prefix, member, and group_title.

## Predicates (client-side filters)

Beyond API filters, extra predicates are applied client-side:

- `group_title_contains`
- `institution_contains`
- `url_contains`
- `doi_startswith`
- `doi_contains`

These use the helper:

```python
_eval_predicate_on_item(item, ...)
```

If any predicate is present, the harvester performs a **two-pass** approach:

1. Stream all candidate records with cursor API, apply predicates, collect DOIs.
2. Re-fetch each DOI individually to construct a rich, wide row with `_one_row_wide`.

## Flattened schema

Each Crossref item is converted to one row with `doi`, `url`, `primary_url`,
titles, dates, authors, subjects, funding, relations (`is-preprint-of`,
`has-preprint`, `is-version-of`), and several JSON columns capturing the raw
lists and nested objects.

Output is written as:

- `<server>_<date_start>_<date_end>_crossref.parquet`
- `<server>_<date_start>_<date_end>_crossref.csv`
