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
- Added direct ENCSR experiment JSON upload support (multi-file `ENCSR*.json`) that parses embedded experiment metadata and file-level processing steps without live ENCODE API fetch.
- Extended `sync_code_examples` to also read `ENCODEPROJECT_metadata/` from GitHub and populate ENC* dataset processing metadata from those JSON files.
- Added resumable ENCODE JSON batch import for uploaded payloads to avoid long blocking requests/timeouts (e.g., HTTP 524), with persisted checkpoint state by upload ID and automatic resume from the last completed batch.
- Added ENCODE upload progress details in admin status, including batch counters and a rolling list of recently imported experiment accessions during import.
- Updated ENCODE upload import parsing to fetch each uploaded accession from live ENCODE experiment JSON (`/experiments/<accession>/?format=json`) plus file metadata before deriving processing steps.
- Refined ENCODE upload progress tracking to checkpoint and publish status per dataset (per accession), including `completed_experiments/total_experiments`, current accession, and recently parsed datasets.
- Fixed ENCODE upload progress polling in multi-instance deployments by persisting upload state in DB and allowing status polling by `upload_id` to avoid stale `0/-` progress.
- Refined ENCODE processing extraction to generate one metadata-rich processing line per file (output type, assembly, replicates, step name, software, QC), modeled after `parse_encode_metadata.py`.
- Added an ENCODE upload `override_existing` option to force replacement of previously stored ENCODE processing steps/pipelines during grant JSON import.
- Fixed ENCODE upload override behavior so batch imports restart from experiment 0 (instead of resuming checkpoints) when `override_existing` is enabled.
- Ensured refined one-line-per-file ENCODE metadata steps are always retained in processing output (not filtered out by keyword matching).
- Fixed ENCODE file parsing fallback to expand `files` URI references (`@id`) into full file metadata when search rows are missing/incomplete.
- Updated ENCODE flattened processing lines to emit script-style rows (`FILE | TYPE | ASM | REP | STEP | SOFTWARE | QC`) and ignore unresolved placeholder file entries.
- Added admin `Stop Parsing` control for ENCODE JSON imports with cooperative cancellation by `upload_id`, preserving resumable checkpoint state.
