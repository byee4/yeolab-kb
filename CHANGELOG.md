# Changelog

## Unreleased

### Bugfixes
- Fixed ENCODE bulk update processing sync so metadata-derived processing steps are generated and persisted per experiment accession.
- Added ENCODE backfill logic to sync processing-step JSON files for existing ENCODE accessions during bulk update, not only newly fetched records.
- Fixed ENCODE pipeline insertion by resolving a valid PMID per accession before writing to `analysis_pipelines`, and tracking skipped rows when no PMID can be resolved.
- Added ENCODE processing extraction tests covering metadata parsing and PMID resolution behavior.
- Fixed ENCODE grant search handling to fall back across multiple query variants when one filter returns 403.

### Added
- Added admin ENCODE JSON upload/import flow for pre-downloaded ENCODE Experiment search payloads (`@graph`), including dataset import, publication linking, processing-step extraction, analysis pipeline sync, and code_examples sync.
- Added resumable ENCODE JSON batch import for uploaded payloads to avoid long blocking requests/timeouts (e.g., HTTP 524), with persisted checkpoint state by upload ID and automatic resume from the last completed batch.
- Added ENCODE upload progress details in admin status, including batch counters and a rolling list of recently imported experiment accessions during import.
