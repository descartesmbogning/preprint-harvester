# DataCite backend

The DataCite backend queries `/dois` per **client-id** and resource type.

## Resource types

The `resource_types` list typically contains:

- `"preprint"` (always)
- optionally `"text"` if the `text` token is present in rules

For `"preprint"`, the harvester performs **two passes**:

1. `resource-type-id=Preprint`
2. `query=types.resourceType:"preprint"`

This captures both structured and textual encodings of preprints.

## Date filter

Date filtering uses the `registered` field via a query such as:

```text
registered:[2020-01-01 TO 2025-10-11]
```

combined with the resource-type query where needed.

## Pagination

Pagination uses `page[size]` and `page[cursor]`, with `links.next` inspected
to extract the next cursor. The helper `_parse_next_cursor` handles that.

## Flattened schema

Each `/dois` item is flattened to one row with:

- basic identifiers (`doi`, `url`, `prefix`)
- provider metadata (`client_id`, `provider_id`)
- resource types (`resource_type`, `resource_type_general`)
- title and publication dates
- a collection of JSON fields (creators, contributors, subjects, rights,
  funding references, etc.)

Output files are named:

- `<server>_<date_start>_<date_end>_datacite.parquet`
- `<server>_<date_start>_<date_end>_datacite.csv`
