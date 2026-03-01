# Yeo Lab Publications Database — Claude Context

## What This Project Is

An SQLite3 database (`yeolab_publications.db`) of all publications by Gene Yeo's lab at UCSD, with a Django web app for searching/browsing. Built from PubMed metadata via MCP tools, with GEO/SRA dataset linkage fetched locally via Biopython.

## Current Counts (Feb 2026)

- 308 publications (2004–2026), 2,326 authors, 4,682 author-paper links
- 760 grants, 1,879 paper-grant links
- 528 dataset accessions (GSE, SRX, etc.), 8,064 data files
- Top journals: Molecular Cell (25), bioRxiv (24), Nature Comms (16), Nature (12), Cell Reports (12)

## Architecture

### Database (`yeolab_publications.db`)
- **SQLite with DELETE journal mode** (not WAL — Cowork mounted filesystem corrupts WAL on `cp`; use `iterdump()` → `executescript()` to transfer)
- **12 tables**: publications, authors, publication_authors, affiliations, publication_affiliations, dataset_accessions, publication_datasets, dataset_files, grants, publication_grants, publication_summaries, update_log
- **FTS5 virtual table** (`publications_fts`) for full-text search. FTS internal tables (`_config`, `_content`, `_data`) must be filtered during dump/restore
- Primary key on publications is `pmid` (TEXT). Junction tables use composite PKs

### Data pipeline
1. **PubMed MCP tools** → metadata JSON (batches of 20) → `build_yeolab_db.py` → SQLite
2. **fetch_geo_sra_metadata.py** (runs locally, needs NCBI API access) → `yeolab_geo_sra_results.json` (ELink → GEO SOFT → SRA XML → PMC full text scanning)
   - Output now separates strong links (`pmid_datasets`) from text-mined candidates (`pmid_potential_datasets`)
   - Text-mined links are flagged as `potentially_related_dataset`
   - Text-mined `GSE*` entries are enriched with GEO metadata/citations and SRA metadata when available
3. **import_geo_sra_results.py** → populates dataset tables (creates schema if missing; requires publications populated first)
4. **fetch_encodeproject_metadata.py** (runs locally, needs internet access) → `yeolab_encode_results.json` (ENCODE REST API → experiments, files, annotations for grants U41HG009889 and U54HG007005)
   - Output mirrors GEO/SRA JSON structure: `pmid_datasets`, `dataset_accessions`, `encode_files`, `annotations`, `all_accessions`
   - Cross-references experiment PMIDs with local publications DB
   - Supports `--skip-files`, `--skip-details` for faster partial fetches
5. **update_yeolab_db.py** — self-contained master updater using Biopython directly. Modes: `--pubmed-only`, `--geo-only`, `--summary`, `--search`

### SRA parsing
- SRX-first: `parse_sra_xml()` extracts EXPERIMENT_PACKAGE hierarchy (SRX → SRS → SRP → RUN_SET → SRR)
- SRA search fallback chain: SRP[All Fields] → PRJNA[BioProject] → GSE[GSEL] → GSE[All Fields]
- Original file names from: `SRAFile@filename`, `CloudFile@filename`, `RUN@alias`, sample attributes (`source_name`, `submitted_file_name`)
- Dual output: hierarchical (`srx_experiments` keyed by SRX) + flat (`sra_runs` keyed by SRR)

### Django web app (`yeolab_search/`)
- **Unmanaged models** (`managed = False`) mapping to existing SQLite tables. Junction tables use `db_column="rowid"` as Django PK (workaround for composite PKs)
- **Settings**: DB path defaults to `../yeolab_publications.db` relative to `yeolab_search/`, overridable via `YEOLAB_DB_PATH` env var. Uses Tailwind CDN + Chart.js CDN (no npm build). Globus OAuth2 via `django-globus-portal-framework` + `social-auth-app-django`
- **Views**: home (dashboard + chart), search (FTS5 with LIKE fallback), publication_detail, author_list, author_detail, dataset_list, dataset_detail, admin_panel (with add/remove preview+confirm), chat_page + chat_message (AI chat with SSE streaming), plus REST API views
- **AI Chat** (`/chat/`): Claude-powered Q&A about the database. User provides their own Anthropic API key (stored in browser localStorage only). Backend uses `ai_tools.py` (8 pre-defined tool functions for DB queries) and `chat_service.py` (system prompt + Anthropic SDK streaming with tool-use loop). Responses stream via SSE (`StreamingHttpResponse`). Tools: `search_publications`, `get_publication`, `search_authors`, `get_author`, `search_datasets`, `get_dataset`, `get_database_stats`, `search_grants`. Requires `anthropic` package.
- **Services layer** (`publications/services.py`): Refactored update logic from `update_yeolab_db.py` for use within Django. Provides `submit_single_pmid()` (sync), `start_full_update()` (background thread, modes: full/pubmed/geo/encode), `start_encode_update()` (fetches experiments, files, and annotations from ENCODE REST API for Yeo Lab grants), `preview_pmid()` (fetch metadata without inserting), `preview_remove_pmid()` (show what would be removed), and `remove_pmid()` (cascade delete with junction-table cleanup). Uses raw SQL via `django.db.connection` cursor with `%s` placeholders (Django SQLite backend). PubMed/GEO modes require biopython + requests; ENCODE mode requires only requests.
- **Admin panel** (`/admin/`): Three sections — bulk update (with real-time progress polling), add publication (preview→confirm via AJAX), and remove publication (preview→confirm via AJAX). Both add and remove show a summary before executing. Supports full, pubmed-only, geo-only, and encode update modes. ENCODE update fetches experiments for grants U41HG009889 and U54HG007005 from encodeproject.org and imports them into `dataset_accessions` and `dataset_files`.
- **PMID management**: Add and remove publications are admin-only (no standalone `/submit/` page). API endpoints: `POST /api/submit/` and `POST /api/remove/` with JSON `{"pmid": "..."}`.
- **CSRF**: `CsrfViewMiddleware` is enabled. POST forms use `{% csrf_token %}`, AJAX calls use `X-CSRFToken` header from cookie.
- **Authentication**: Globus OAuth2 via `django-globus-portal-framework`. Login at `/login/globus/`, logout at `/logout/`. All admin views and write API endpoints require `@login_required`. Public pages (search, browse, read-only API) remain accessible without login. Requires `GLOBUS_CLIENT_ID` and `GLOBUS_CLIENT_SECRET` env vars. Redirect URL: `http://localhost:8000/complete/globus/`.
- **FTS5 fallback**: `_fts_search()` in views.py catches exceptions if FTS table doesn't exist and falls back to `LIKE` queries
- **Migrations required**: `python manage.py migrate` creates auth, sessions, and social_django tables. Publication models remain unmanaged
- Run with: `cd yeolab_search && python manage.py runserver`

## Known Issues & Potential Improvements

- **FTS5 table** may not exist in all copies of the DB (built by `build_yeolab_db.py` in Cowork but lost during some transfers). The Django app handles this gracefully via LIKE fallback
- **publication_summaries** table is empty — placeholder for AI-generated summaries
- **publication_affiliations** junction is empty — affiliations stored per-author in `publication_authors.affiliation` instead
- **Potential dataset links** from PMC full text are intentionally lower-confidence and labeled `potentially_related_dataset`
- **GEO citations** are stored in `dataset_accessions.citation_pmids` and surfaced in dataset detail pages (with linked PubMed entries and authors when the PMID exists in the local publications table)
- **Cowork sandbox blocks NCBI** — `fetch_geo_sra_metadata.py` and `update_yeolab_db.py` must run on local machine. PubMed MCP tools work through separate channel

## File Inventory

```
yeolab_publications.db          # SQLite database
fetch_geo_sra_metadata.py       # Run locally → yeolab_geo_sra_results.json
import_geo_sra_results.py       # Import JSON → SQLite (creates schema if needed)
update_yeolab_db.py             # Master updater (PubMed + GEO/SRA, runs locally)
build_yeolab_db.py              # Original builder from MCP JSON files
yeolab_search/                  # Django web app
  manage.py
  yeolab_search/settings.py     # DB path config, minimal middleware
  publications/models.py        # Unmanaged models for all 12 tables
  publications/services.py      # Update logic (Entrez fetching, PMID add/remove, preview)
  publications/ai_tools.py      # AI chat tool definitions and DB query functions
  publications/chat_service.py  # Chat orchestration (system prompt, Anthropic API, tool loop)
  publications/views.py         # Web UI + REST API views (admin panel, add/remove, chat, etc.)
  publications/urls.py          # URL routing
  publications/templates/       # Tailwind CSS templates (incl. chat.html)
  publications/templatetags/    # query_string and get_item helpers
README.md                       # User-facing documentation
fetch_encodeproject_metadata.py  # ENCODE dataset fetcher → yeolab_encode_results.json
CLAUDE.md                       # This file
```
