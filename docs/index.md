# Preprint Server Harvester

This documentation site describes a Python toolkit that harvests preprint
metadata from three major infrastructures:

- **Crossref** (via `/works`)
- **DataCite** (via `/dois`)
- **OpenAlex** (via `/works`)

All harvesting logic is driven by a single **rules sheet** (typically a
Google Sheet exported as CSV), where each row represents a preprint server
(e.g., *bioRxiv*, *OSF Preprints*, *arXiv mirror*).

## Main features

- Rules-driven harvesting from a tabular sheet
- Crossref backend (prefix / member / group-title / institution / DOI rules)
- DataCite backend (per–`client-id`, `resourceType`)
- OpenAlex backend (per–`source_id`)
- Consistent wide-table outputs in **Parquet** and **CSV**
- Per-server output folders + global summary file

If you are reading this locally:

```bash
npm install
npm run start
```

Then open the local URL printed by Docusaurus in your browser.
