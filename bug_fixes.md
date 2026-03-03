# Bug Fix Documentation: `fetch_geo_sra_metadata.py`

## Overview

Three bugs were identified and fixed in `get_sra_for_geo()` and `parse_sra_runinfo_csv()` that together caused the GEO → GSM → SRX → SRR lookup chain to return no results. The root issue was that SRA searches were using an unreliable query strategy while the correct accession data had already been extracted and was sitting unused.

**Verified example:** GSE107895 → GSM2883063 → SRX3461030 → SRR6365478 now resolves correctly.

---

## Bug 1 — Root Cause: SRP Accessions Extracted But Never Used

**Function:** `get_sra_for_geo()`
**Severity:** Critical — directly caused zero results

### What was happening

The function correctly parsed GEO SOFT `!Series_relation` fields and extracted SRP study accessions into a `srp_to_gse` dictionary. However, this dictionary was never consulted when building the SRA search query. Every GSE was searched using:

```python
# Before — always used [All Fields], ignoring srp_to_gse entirely
handle = Entrez.esearch(db="sra", term=f"{gse}[All Fields]", retmax=2000)
```

`[All Fields]` performs fuzzy full-text matching and frequently returns zero results when given a GEO accession like `GSE107895`, because SRA records don't always contain the GSE string in their free-text fields.

### Fix

The search now uses a priority chain, falling back to broader strategies only when narrower ones fail:

```python
# After — prioritized search strategy
if gse in gse_to_srp:
    search_term = f"{gse_to_srp[gse]}[All Fields]"   # e.g. SRP123456
elif gse in gse_to_prjna:
    search_term = f"{gse_to_prjna[gse]}[BioProject]"  # e.g. PRJNA420478
else:
    search_term = f"{gse}[GSEL]"                       # SRA's GEO series field

# [All Fields] retained as last resort if primary search returns nothing
if not sra_ids:
    handle = Entrez.esearch(db="sra", term=f"{gse}[All Fields]", retmax=2000)
    ...
```

The `[GSEL]` field is the dedicated SRA Entrez index for GEO series accessions and is far more reliable than `[All Fields]` when no SRP or PRJNA is available.

---

## Bug 2 — Incomplete Relation Parsing: PRJNA Accessions Missed

**Function:** `get_sra_for_geo()`
**Severity:** High — caused SRP-based search to be skipped for most datasets

### What was happening

GEO SOFT `!Series_relation` lines come in several formats:

```
SRA: https://www.ncbi.nlm.nih.gov/sra?term=SRP123456
BioProject: https://www.ncbi.nlm.nih.gov/bioproject/PRJNA420478
SuperSeries of: GSE99999
```

The original code only matched `SRP\d+` and also required the string `"SRA:"` to be present as a guard:

```python
# Before — missed PRJNA entirely, and gated SRP on "SRA:" string
for rel in detail.get("relations", []):
    if "SRA:" in rel or "SRP" in rel:
        srp_match = re.search(r'(SRP\d+)', rel)
        if srp_match:
            srp_to_gse[srp] = gse
```

In practice, many GEO datasets only expose a PRJNA BioProject accession in their relations, not an SRP. These datasets would silently fall through to the broken `[All Fields]` search.

### Fix

Both SRP and PRJNA are now extracted from every relation line, without gating on a prefix string:

```python
# After — extracts both SRP and PRJNA from all relation lines
for rel in detail.get("relations", []):
    srp_match = re.search(r'(SRP\d+)', rel)
    if srp_match:
        srp = srp_match.group(1)
        srp_to_gse[srp] = gse
        gse_to_srp[gse] = srp
    prjna_match = re.search(r'(PRJNA\d+)', rel)
    if prjna_match:
        prjna = prjna_match.group(1)
        prjna_to_gse[prjna] = gse
        gse_to_prjna[gse] = prjna
```

Reverse lookup dictionaries (`gse_to_srp`, `gse_to_prjna`) are also built here so Bug 1's fix can look up the right accession by GSE during the search loop.

---

## Bug 3 — Fragile CSV Parsing Breaks on Fields Containing Commas

**Function:** `parse_sra_runinfo_csv()`
**Severity:** Medium — caused silent data loss in the RunInfo fallback path

### What was happening

The SRA RunInfo CSV fallback used a naive `str.split(",")` to parse each row:

```python
# Before — breaks when any field contains a comma
headers = lines[0].split(",")
for line in lines[1:]:
    values = line.split(",")
    if len(values) != len(headers):
        continue   # silently drops the row
    record = dict(zip(headers, values))
```

SRA RunInfo fields such as `SampleName`, `LibraryName`, and `experiment_title` routinely contain commas. When a row produced more values than headers after splitting, it was silently dropped rather than parsed correctly.

### Fix

Replaced with Python's `csv.DictReader`, which correctly handles RFC 4180 quoting and embedded commas:

```python
# After — handles commas in fields correctly
import csv, io

def parse_sra_runinfo_csv(text, gse):
    runs = []
    if not text or not text.strip():
        return runs
    try:
        reader = csv.DictReader(io.StringIO(text.strip()))
        for record in reader:
            record["source_gse"] = gse
            runs.append(dict(record))
    except Exception:
        pass
    return runs
```

---

## Summary Table

| # | Function | Bug | Impact | Fix |
|---|----------|-----|--------|-----|
| 1 | `get_sra_for_geo` | `srp_to_gse` built but never used; all searches used unreliable `[All Fields]` | **Critical** — zero SRR results returned | Priority chain: SRP → PRJNA → `[GSEL]` → `[All Fields]` fallback |
| 2 | `get_sra_for_geo` | Only `SRP\d+` extracted from relations; `PRJNA` accessions missed entirely | **High** — SRP-based search skipped for most datasets | Extract both `SRP` and `PRJNA` from all relation lines; build reverse GSE lookup |
| 3 | `parse_sra_runinfo_csv` | Naive `str.split(",")` broke on fields containing commas; rows silently dropped | **Medium** — data loss in RunInfo fallback path | Replace with `csv.DictReader` |

---

## 2026-03-03 — Dataset View/Analysis Step Clobber Fix

### Symptom

Navigating between the dataset detail view (`/dataset/<id>/`) and analysis view (`/analysis/dataset/<accession>/`) appeared to replace or "clobber" processing steps for the same dataset.

### Root Cause

`publications/code_examples.py` maintained an in-memory registry (`_REGISTRY`, `_PATHS`) loaded at import time. In multi-worker deployments (e.g., gunicorn), each worker keeps its own copy of this cache. After a JSON edit or extract/sync operation handled by one worker, other workers could still serve stale step data, creating the appearance that page navigation overwrote pipeline steps.

Additionally, registry load had a write-on-read side effect: when `locked` was missing, loading a file would rewrite it to disk. This made normal reads unexpectedly mutate files.

### Fix

1. Added a refresh-on-read guard (`_ensure_fresh_registry`) and invoked it in all registry read accessors:
   - `get_registry`
   - `list_datasets`
   - `list_datasets_with_paths`
   - `get_dataset_rel_path`
   - `get_dataset_content`
   - `get_steps_for_dataset`
   - `is_dataset_locked`
   - `get_dataset_raw_text`

2. Removed write-on-read behavior in `_load_registry` that persisted `locked=true` during registry loading.

### Result

- Dataset and analysis views now read consistent, current JSON-backed processing steps.
- Viewing pages no longer causes implicit JSON rewrites.
- Step data is no longer "clobbered" by cross-worker stale cache reads.

---

## 2026-03-03 — Globus Login Recovery on Token Endpoint 5xx

### Symptom

Some normal browser sessions failed during `/complete/globus/` with provider-side token endpoint 5xx errors.

### Fix

Updated `yeolab_search.middleware.GlobusDebugMiddleware` to add a safe recovery path:

- If `/complete/globus/` raises `requests.exceptions.HTTPError` with status `>= 500`,
  automatically:
  1. logs out the user
  2. flushes session state
  3. redirects to `/login/globus/?oauth_retry=1`

### Result

Users can recover from transient provider-side failures by being routed into a clean re-auth flow, instead of landing on a hard 500 page.

---

## 2026-03-03 — Chat "Network Error" Mitigation (SSE Timeout)

### Symptom

Chat UI intermittently showed generic network errors during long responses/tool calls.

### Root Cause

The chat endpoint uses streaming HTTP responses (`text/event-stream`). With gunicorn
worker timeout set to 120 seconds, long-running AI/tool rounds could exceed timeout,
terminating the worker and surfacing as browser-side network failure.

### Fix

Made gunicorn runtime tunable in `scripts/start_web.sh` and raised default timeout:

- `GUNICORN_TIMEOUT` default changed to `600`
- Added env-driven settings:
  - `GUNICORN_BIND` (default `0.0.0.0:8000`)
  - `GUNICORN_WORKERS` (default `4`)

Updated deployment/env documentation to include `GUNICORN_TIMEOUT`.

---

## 2026-03-03 — Chat Transcript Download (.txt)

### Feature

Added a `Download .txt` button to the chat interface toolbar.

### Behavior

- Exports the current conversation (`conversationHistory`) to a local text file.
- Includes export timestamp, provider, and model metadata at the top.
- Prevents export while streaming is in progress.
- Shows a helpful message if no messages are present yet.

---

## 2026-03-03 — Remove Runtime Default Pipeline Generation in Dataset/Analysis Views

### Problem

Dataset/analysis pages could show fallback-generated steps (from metadata templates) when curated `code_examples` steps were missing or stale, which made step output appear clobbered/inconsistent.

### Removed Logic

In `publications/views.py`:

1. `dataset_detail` no longer calls `generate_pipeline_from_metadata(...)` when `code_examples` steps are empty.
2. `_build_code_example_pipelines` no longer auto-adds missing GSE accessions by generating template pipelines from DB metadata.
3. `analysis_detail_by_accession` no longer falls back to generated steps; it now returns 404 if no curated `code_examples` entry exists.

### Result

Runtime display now uses only curated `code_examples` JSON for dataset-level analysis steps, eliminating default-template fallback in user-facing views.

### Documentation Follow-up

Added explicit in-code documentation to `publications/code_examples.py`:

- marked `generate_pipeline_from_metadata(...)` as deprecated for user-facing runtime rendering
- documented that `_PIPELINE_TEMPLATES` / `_DEFAULT_TEMPLATE` are retained only for manual/admin/offline workflows
