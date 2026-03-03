# Changelog

## Unreleased

### Bugfixes
- Fixed ENCODE bulk update processing sync so metadata-derived processing steps are generated and persisted per experiment accession.
- Added ENCODE backfill logic to sync processing-step JSON files for existing ENCODE accessions during bulk update, not only newly fetched records.
- Fixed ENCODE pipeline insertion by resolving a valid PMID per accession before writing to `analysis_pipelines`, and tracking skipped rows when no PMID can be resolved.
- Added ENCODE processing extraction tests covering metadata parsing and PMID resolution behavior.
