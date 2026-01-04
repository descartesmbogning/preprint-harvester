# OpenAlex backend

The OpenAlex backend fetches works from `/works` using `primary_location.source.id`
and a publication date window.

## Preprint-only by default

By default, the backend restricts to `type:preprint`:

```text
filter=type:preprint,primary_location.source.id:s3006283864|s4306401238,from_publication_date:YYYY-MM-DD,to_publication_date:YYYY-MM-DD
```

You can change the behaviour in `harvest_openalex_for_source_ids` by toggling
the `only_preprints` flag.

## Source IDs

Source IDs (`sXXXXXXX`) are read from the `source_id` column of the rules sheet,
parsed as a list (e.g. `[s3006283864,s4306401238]`). Multiple IDs are OR-ed via `|`.

## Flattened schema

A minimal, stable schema is extracted:

- `openalex_id`
- `doi`
- `title`
- `publication_year`
- `publication_date`
- `cited_by_count`
- `type`
- `is_paratext`
- `primary_location_landing_page_url`
- `primary_location_source_id`
- `primary_location_source_display_name`
- `primary_location_is_oa`
- `primary_location_oa_status`
- `authorships_json`
- `concepts_json`
- `topics_json`
- `raw_openalex_json`

Outputs are named:

- `<server>_<date_start>_<date_end>_openalex.parquet`
- `<server>_<date_start>_<date_end>_openalex.csv`
