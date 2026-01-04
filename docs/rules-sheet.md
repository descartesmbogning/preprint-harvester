# Rules sheet

The **rules sheet** is the control panel for the whole harvester. Each row
corresponds to one logical preprint server. Columns indicate which backends
to use and how to query them.

At minimum, you need:

- `Field_server_name`: human-readable server name (e.g. "bioRxiv")
- `include`: rows with `"yes"` will be harvested
- `rules` and/or `rule_1`..`rule_7`: tokens describing which columns to use

## Rule tokens

Some examples of tokens you can use in `rules` or `rule_i` columns:

- `client_id` → use DataCite backend, column `client_id`
- `prefix` → use Crossref filter `prefix:...` (from `Prefixes` column)
- `member` → use Crossref filter `member:...` (from `Members` column)
- `group_title` → use Crossref filter `group-title:...` (from `group_title` column)
- `institution_name` → client-side filter on institution names
- `primary_domain` → client-side filter on primary URL domains
- `doi_prefix_first_token` → client-side filter on DOI prefix + first token
- `source_id` → use OpenAlex backend, column `source_id`

Internally, the code parses tokens with:

```python
def _parse_rules_tokens(row: pd.Series):
    tokens = set()
    # from "rules"
    # and from rule_1..rule_7
    return tokens
```

These tokens feed into `_build_params_from_rule_row` for Crossref and into
backend selection in `harvest_servers_from_rules_sheet`.

## Backend selection logic

For each row:

- If `client_id` is in tokens → **DataCite** is the main backend.
- Else if any of `{prefix, member, group_title, institution_name,
  primary_domain, doi_prefix_first_token}` is present → **Crossref** is the
  main backend.
- Else → `"backend": "none"`.

Independently, if `source_id` is in tokens, an **OpenAlex** harvest is also
attempted for that server.

## Example row

| Field_server_name | include | rules                                      | Prefixes      | client_id | source_id         |
|-------------------|---------|--------------------------------------------|---------------|-----------|-------------------|
| bioRxiv           | yes     | prefix/client_id/source_id                 | ["10.1101"]   | cshl.biorxiv | [s3006283864]  |
| medRxiv           | yes     | prefix/client_id/source_id                 | ["10.1101"]   | cshl.medrxiv | [s4306401238] |

This row will:

- Harvest from Crossref (prefix 10.1101, `type:posted-content` and date window).
- Harvest from DataCite (client `cshl.biorxiv`, resource type `"preprint"`).
- Harvest from OpenAlex (source id `s3006283864`, `type:preprint`).

All outputs go into a `data/by_server_datacite_crossref_openalex/bioRxiv/`
folder (name sanitized). A global summary row is added to the summary CSV.
