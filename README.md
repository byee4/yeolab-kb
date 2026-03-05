# Yeo Lab Publications Database

SQLite3 database of all Yeo Lab (Gene Yeo, UCSD) publications with PubMed metadata, author networks, grant information, and GEO/SRA dataset linkage. Includes a Django web app with search, browsing, bulk download scripts, and a REST API.

## Current State (Mar 2026)

| Table | Rows | Notes |
|---|---|---|
| publications | 308 | All Yeo GW[Author] from PubMed, 2004–2026 |
| authors | 2,326 | Unique author records |
| publication_authors | 4,682 | Author–paper links with position (first/last) |
| grants | 760 | NIH/NSF/etc grant numbers |
| publication_grants | 1,879 | Paper–grant links |
| dataset_accessions | 2,476 | GEO/SRA/ENCODE accessions with full metadata |
| sra_experiments | 11,764 | SRX experiments with library/sample metadata |
| sra_runs | 14,173 | SRR runs with spots, bases, sizes, file names |
| dataset_files | 96,404 | Data files with rich metadata from ENCODE/GEO/SRA |
| publication_datasets | 2,383 | Paper–dataset links (strong + potentially related) |
| analysis_pipelines | 289 | Extracted analysis pipeline records |
| pipeline_steps | 2,039 | Parsed processing steps across pipelines |
| computational_methods | 43 | Curated methods/tool canonical table |
| publication_summaries | 0 | AI-generated summaries (placeholder) |

## Quick Start

### 1. Build from scratch (run locally)

```bash
pip install biopython requests

# Step 1: Create database and populate publications from PubMed
python update_yeolab_db.py --pubmed-only

# Step 2: Fetch GEO/SRA metadata from NCBI (~30-60 min)
python fetch_geo_sra_metadata.py
# → produces yeolab_geo_sra_results.json

# Step 3: Import GEO/SRA data into the database
python import_geo_sra_results.py --db yeolab_publications.db --input yeolab_geo_sra_results.json

# Step 4 (optional): Fetch ENCODE Project metadata for Yeo Lab grants
python fetch_encodeproject_metadata.py
# → produces yeolab_encode_results.json
# → use --grants to specify different grant numbers
# → use --skip-files for a faster partial fetch
```

Note: Step 1 is required before Step 3. `import_geo_sra_results.py` will create the schema if missing, but it needs publications already loaded to link datasets to papers. Steps 1 and 2 can run in parallel. Use `--clear` on step 3 for a clean reimport.

### 2. Incremental updates (run periodically)

```bash
# Full update: new papers + GEO/SRA
python update_yeolab_db.py

# PubMed metadata only (faster)
python update_yeolab_db.py --pubmed-only

# GEO/SRA only
python update_yeolab_db.py --geo-only

# Summary
python update_yeolab_db.py --summary

# Full-text search
python update_yeolab_db.py --search "eCLIP"
```

Set `NCBI_API_KEY` env var for 10 req/sec (free at https://www.ncbi.nlm.nih.gov/account/settings/).

## Environment Variables & Secrets

Use `.env.example` as the template. Do not commit real secrets.

### Runtime (web app)

| Variable | Required | Secret | Where to get / how to set |
|---|---|---|---|
| `DJANGO_SECRET_KEY` | Yes (production) | Yes | Generate with `python -c "from django.core.management.utils import get_random_secret_key; print(get_random_secret_key())"` |
| `DJANGO_DEBUG` | Yes | No | Set `False` in production, `True` for local dev |
| `DJANGO_ALLOWED_HOSTS` | Yes (production) | No | Comma-separated host/domain list for your deployment |
| `DATABASE_URL` | Yes (production) | Yes | PostgreSQL connection string from your DB provider (AWS RDS, DO Managed DB, etc.) |
| `YEOLAB_DB_PATH` | Optional | No | Local SQLite path override (defaults to `yeolab_publications.db`) |
| `CSRF_TRUSTED_ORIGINS` | Yes (production) | No | Comma-separated HTTPS origins serving this app |
| `SECURE_SSL_REDIRECT` | Recommended | No | `True` if app should force HTTPS; set `False` only if TLS termination is handled upstream |

### Globus authentication and admin gating

| Variable | Required | Secret | Where to get / how to set |
|---|---|---|---|
| `GLOBUS_CLIENT_ID` | Yes (if login enabled) | No | Register app at [app.globus.org/settings/developers](https://app.globus.org/settings/developers) |
| `GLOBUS_CLIENT_SECRET` | Yes (if login enabled) | Yes | Same Globus app registration page |
| `SOCIAL_AUTH_GLOBUS_REDIRECT_URI` | Recommended | No | Must exactly match registered callback (example: `https://yourdomain/complete/globus/`) |
| `GLOBUS_ADMIN_GROUP` | Yes (for admin lock-down) | No | Globus Group UUID for admin users (copy from group details/URL in Globus Groups) |

### GitHub sync for Code Examples Editor

| Variable | Required | Secret | Where to get / how to set |
|---|---|---|---|
| `GITHUB_PAT` | Required for private repo or write/push | Yes | Create token in GitHub settings (fine-grained token recommended, repo contents read/write) |
| `GITHUB_REPO` | Optional | No | Repo slug containing `code_examples/` (default `byee4/yeolab-publications-db`) |
| `GITHUB_BRANCH` | Optional | No | Branch to fetch/push (default `main`) |
| `CODE_EXAMPLES_REPO_DIR` | Optional | No | Local path to cloned repo (deployment default `/app/yeolab-publications-db`) |
| `CODE_EXAMPLES_DIR` | Optional | No | Direct override for `code_examples` directory path |
| `CODE_EXAMPLES_REFRESH_INTERVAL_SEC` | Optional | No | Registry refresh interval for code examples cache |

### Data pipeline and processing knobs

| Variable | Required | Secret | Where to get / how to set |
|---|---|---|---|
| `NCBI_API_KEY` | Optional but recommended | Yes | Generate from [NCBI account settings](https://www.ncbi.nlm.nih.gov/account/settings/) |
| `ENTREZ_EMAIL` | Recommended | No | Contact email used for NCBI Entrez usage etiquette |
| `ENCODE_SOFTWARE_RESOLVE_DELAY` | Optional | No | Delay (seconds) between ENCODE software resolution calls |

### Deployment / CI secrets (outside app runtime)

| Variable | Required | Secret | Where to get / how to set |
|---|---|---|---|
| `AWS_ACCESS_KEY_ID` | AWS deploy workflow only | Yes | IAM user/role with EB/ECR deployment permissions |
| `AWS_SECRET_ACCESS_KEY` | AWS deploy workflow only | Yes | IAM credential pair for the above identity |
| `DIGITALOCEAN_ACCESS_TOKEN` | DO deploy workflow only | Yes | Personal access token from DigitalOcean API settings |
| `DO_APP_ID` | DO deploy workflow only | No | App ID from DigitalOcean App Platform |
| `GUNICORN_TIMEOUT` | Optional | No | Gunicorn worker timeout seconds (default in this repo: `600`) |

### 3. Web interface + API

A Django app is included for browsing, searching, and programmatic access.

```bash
pip install django django-globus-portal-framework anthropic
cd yeolab_search
python manage.py migrate   # creates auth/session/social_auth tables
python manage.py runserver
# → Web UI:  http://127.0.0.1:8000
# → API:     http://127.0.0.1:8000/api/stats/
# → Chat:    http://127.0.0.1:8000/chat/  (login required)
```

Features: full-text search across titles/abstracts, filter by year/journal/author, publication detail pages with authors and datasets (with potentially related datasets shown separately), author profiles with co-author networks, dataset browser with supplementary files and SRA/ENCODE metadata, bulk download script generation, Analysis views, a full REST API, Globus-based authentication, an AI chat interface powered by Claude or ChatGPT (BYOK — bring your own Anthropic/OpenAI API key), and an admin panel (login required) with update controls plus add/remove publication management with preview confirmation. The app uses `DATABASE_URL` for PostgreSQL when set, otherwise falls back to `yeolab_publications.db` (override with `YEOLAB_DB_PATH`).

### 4. Admin panel & updates (via web UI)

The Django app includes a built-in admin panel for updating the database without running command-line scripts. Requires `biopython`, `requests`, and Globus authentication. The admin panel and all write operations require login.

```bash
pip install django django-globus-portal-framework biopython requests
cd yeolab_search
python manage.py migrate
python manage.py runserver
# → Admin:   http://127.0.0.1:8000/admin/  (login required)
# → Login:   http://127.0.0.1:8000/login/globus/
```

**Globus authentication setup**: Register an application at https://app.globus.org/settings/developers (select "Register a portal, science gateway, or other application"). Set the redirect URL to `http://localhost:8000/complete/globus/` (or your production URL). Then set the environment variables `GLOBUS_CLIENT_ID` and `GLOBUS_CLIENT_SECRET` before starting the server. Public pages (search, browse, API) remain accessible without login.

**One-click update** (`/admin/`): Click update modes to run background jobs. Supported modes are `full`, `pubmed`, `geo`, `encode`, `methods`, and `analysis`. Progress and logs stream in real time.

**Code Examples Editor** (`/admin/code-editor/`): Edit per-dataset JSON files used by the Analysis views. In deployment, the web container clones/pulls `byee4/yeolab-publications-db` on startup and the editor reads/writes files under `/app/yeolab-publications-db/code_examples/{year}/{Mon}/{ACCESSION}.json`. Save operations return the full local file path. Files default to `locked: true` to prevent future Extract Analysis overwrites.

Runtime note: analysis/dataset pages now use dataset-keyed JSON first, and the legacy metadata fallback builder (`generate_pipeline_from_metadata`) is deprecated for runtime rendering and retained only for backfill/admin workflows.

**Add a publication** (`/admin/`): Enter a PubMed ID and click "Preview" to fetch metadata from PubMed. A summary (title, authors, journal, year, grants) is displayed for confirmation before inserting. If the PMID already exists, you're shown a link to the existing record.

**Remove a publication** (`/admin/`): Enter a PubMed ID and click "Preview" to see what would be removed (title, author links, grant links, dataset links). Confirm to delete the publication and all its junction-table links. Shared authors, grants, and datasets are not deleted — only the links to this publication.

**API endpoints**: `POST /api/submit/` with JSON `{"pmid": "12345678"}` to add a paper, or `POST /api/remove/` with JSON `{"pmid": "12345678"}` to remove one.

### 5. AI Chat (`/chat/`)

Logged-in users can ask natural language questions about publications, authors, datasets, and grants. Powered by Claude (Anthropic) or ChatGPT (OpenAI) with tool-calling that queries the database directly.

```bash
pip install anthropic openai   # if not already installed
```

Users provide their own Anthropic or OpenAI API key (stored in browser localStorage only — never sent to or saved on the server). You can select provider and model in the chat UI. Both providers have access to 9 tools: `search_publications`, `get_publication`, `search_authors`, `get_author`, `search_datasets`, `get_dataset`, `get_database_stats`, `search_grants`, and `search_pipelines`. Responses stream in real time via Server-Sent Events.

How to get a key:
- Anthropic: Sign in at [console.anthropic.com](https://console.anthropic.com/), open [API Keys](https://console.anthropic.com/settings/keys), create a key, and copy the `sk-ant-...` value.
- OpenAI: Sign in at [platform.openai.com](https://platform.openai.com/), open [API keys](https://platform.openai.com/api-keys), create a secret key, and copy the `sk-...` value.

Example questions: "What are the lab's most recent publications?", "Which datasets use eCLIP?", "Who are Gene Yeo's top collaborators?", "Summarize the lab's work on TDP-43".

## Testing

Run the Django test suite from repo root:

```bash
/Users/brianyee/miniconda3/bin/python yeolab_search/manage.py test \
  publications.tests.test_code_examples_registry \
  publications.tests.test_view_integration \
  publications.tests.test_encode_processing \
  publications.tests.test_bulk_updates
```

Current focused coverage additions include:
- code_examples registry refresh throttling behavior
- analysis list cache path (avoids rebuilding code_examples index per request)
- ENCODE processing extraction and bulk update/stop controls

## REST API

All read endpoints return JSON without authentication. Write endpoints (`POST /api/submit/` and `POST /api/remove/`) require Globus login. Base URL: `http://localhost:8000/api/`

### Endpoints

#### `GET /api/stats/`

Summary statistics for the entire database: counts, top journals, publications by year, library strategy breakdown, organism distribution.

```bash
curl http://localhost:8000/api/stats/ | python -m json.tool
```

#### `GET /api/publications/`

Search and list publications. Returns paginated results.

| Parameter | Description |
|---|---|
| `q` | Full-text search query (uses FTS5) |
| `year` | Filter by publication year |
| `journal` | Filter by journal name (substring match) |
| `author` | Filter by author name (substring match) |
| `page` | Page number (default: 1) |
| `per_page` | Results per page (default: 25, max: 100) |

```bash
# Search for eCLIP papers
curl "http://localhost:8000/api/publications/?q=eCLIP"

# All papers from 2024
curl "http://localhost:8000/api/publications/?year=2024"

# Filter by author
curl "http://localhost:8000/api/publications/?author=Van+Nostrand"
```

Response:

```json
{
  "count": 42,
  "page": 1,
  "per_page": 25,
  "total_pages": 2,
  "results": [
    {
      "pmid": "32728249",
      "title": "A large-scale binding and functional map of human RNA-binding proteins",
      "doi": "10.1038/s41586-020-2077-3",
      "journal": "Nature",
      "pub_year": 2020,
      "authors": null,
      "pubmed_url": "https://pubmed.ncbi.nlm.nih.gov/32728249/",
      "..."
    }
  ]
}
```

#### `GET /api/publications/<pmid>/`

Full detail for a single publication, including authors, datasets, potentially related datasets, and grants.

```bash
curl http://localhost:8000/api/publications/32728249/
```

Response includes `authors`, `datasets` (strong links), `potentially_related_datasets` (text-mined, lower confidence), and `grants` arrays.

#### `GET /api/datasets/`

Search and list dataset accessions. Returns paginated results.

| Parameter | Description |
|---|---|
| `q` | Search accession, title, organism, or summary |
| `type` | Filter by accession type (GSE, SRX, ENCSR, etc.) |
| `page` | Page number (default: 1) |
| `per_page` | Results per page (default: 25, max: 100) |

```bash
# All GSE datasets
curl "http://localhost:8000/api/datasets/?type=GSE"

# Search for CLIP-seq datasets
curl "http://localhost:8000/api/datasets/?q=CLIP"
```

#### `GET /api/datasets/<accession_id>/`

Full detail for a dataset, including linked publications, SRA experiments with nested runs, and data files. The `supplementary_files` field contains parsed GEO supplementary file URLs. The `sra_experiments` array contains the full SRX→SRR hierarchy with library metadata, sample attributes, and per-run spots/bases/sizes.

```bash
curl http://localhost:8000/api/datasets/5/
```

#### `GET /api/authors/`

List authors sorted by publication count.

| Parameter | Description |
|---|---|
| `q` | Filter by name (substring match) |
| `page` | Page number (default: 1) |
| `per_page` | Results per page (default: 50, max: 200) |

```bash
curl "http://localhost:8000/api/authors/?q=Yeo"
```

#### `POST /api/submit/`

Add a single publication by PubMed ID. Send a JSON body with the `pmid` field. Returns the added publication info, or an error if the PMID is not found. If the PMID already exists, returns `already_exists: true`.

```bash
curl -X POST http://localhost:8000/api/submit/ \
  -H "Content-Type: application/json" \
  -d '{"pmid": "32728249"}'
```

#### `POST /api/remove/`

Remove a publication by PubMed ID. Deletes the publication and all junction-table links (authors, grants, datasets). Shared authors, grants, and datasets are preserved. Returns counts of removed links.

```bash
curl -X POST http://localhost:8000/api/remove/ \
  -H "Content-Type: application/json" \
  -d '{"pmid": "32728249"}'
```

#### `POST /api/update/start/`

Start a background database update (login required). Send `mode` as form data: `full`, `pubmed`, `geo`, `encode`, `methods`, or `analysis`.

```bash
curl -X POST http://localhost:8000/api/update/start/ -d "mode=pubmed"
```

#### `GET /api/update/status/`

Poll the current background update status. Returns `running`, `progress`, `log`, `stats`, and `error` fields.

```bash
curl http://localhost:8000/api/update/status/
```

### Bulk Download

Every dataset page includes a "Bulk Download .sh" button that generates a shell script with `wget`/`curl`/`fasterq-dump` commands for all associated files (supplementary files, SRA runs, data files). The script can also be accessed programmatically:

```bash
# Download the bash script for a dataset
curl -o download_GSE120023.sh "http://localhost:8000/dataset/5/download.sh"
chmod +x download_GSE120023.sh
./download_GSE120023.sh

# Or get just the URL list
curl "http://localhost:8000/dataset/5/download.sh?format=urls"
```

### Python example

```python
import requests

BASE = "http://localhost:8000/api"

# Get all eCLIP publications
pubs = requests.get(f"{BASE}/publications/", params={"q": "eCLIP"}).json()
for p in pubs["results"]:
    print(f'{p["pmid"]}: {p["title"][:80]}')

# Get datasets for a specific paper
detail = requests.get(f"{BASE}/publications/32728249/").json()
for ds in detail["datasets"]:
    print(f'  {ds["accession"]} ({ds["type"]}): {ds["title"][:60] if ds["title"] else ""}')

# Get full SRA experiment metadata for a dataset
ds_detail = requests.get(f"{BASE}/datasets/{detail['datasets'][0]['accession_id']}/").json()
for exp in ds_detail["sra_experiments"]:
    print(f'  {exp["srx_accession"]}: {exp["library_strategy"]} {exp["organism"]}')
    for run in exp["runs"]:
        print(f'    {run["srr_accession"]}: {run["total_spots"]} spots, {run["size_mb"]} MB')
```

## Files

| File | Purpose |
|---|---|
| `yeolab_publications.db` | SQLite3 database |
| `fetch_geo_sra_metadata.py` | Fetches GEO/SRA datasets from NCBI (run locally) |
| `fetch_encodeproject_metadata.py` | Fetches ENCODE datasets for Yeo Lab grants (run locally) |
| `import_geo_sra_results.py` | Imports fetched JSON into SQLite (creates schema if needed) |
| `update_yeolab_db.py` | Master update script (PubMed + GEO/SRA) |
| `build_yeolab_db.py` | Original builder from PubMed MCP JSON files |
| `yeolab_search/` | Django web app with UI + REST API |

## Database Schema

### Core tables

**publications** — One row per paper. Primary key: `pmid`. Contains title, abstract, journal, dates, DOI, PMC ID, MeSH terms, keywords, publication types, language, word count.

**authors** / **publication_authors** — Normalized author data. `publication_authors` tracks position in author list, first/last author flags, and per-paper affiliation text.

**grants** / **publication_grants** — Funding sources extracted from PubMed metadata.

### Dataset tables

**dataset_accessions** — One row per unique accession (GSE, GSM, SRR, SRX, PRJNA, ENCSR, etc.). Stores title, organism, platform, summary, overall design, sample count, supplementary file URLs (JSON), submission/update dates, contact info, experiment types (JSON), relations (JSON, e.g. SRP/PRJNA links), sample IDs (JSON list of GSM accessions), and citation PMIDs (`citation_pmids`, JSON list).

**sra_experiments** — One row per SRX experiment. Full library metadata: strategy, source, selection, layout, platform, instrument model. Sample metadata: accession (SRS), name, alias, BioSample, BioProject, organism, sample attributes (JSON dict with cell type, source, etc.), and original file names (JSON). Links to parent GSE via `parent_accession_id`.

**sra_runs** — One row per SRR run. Stores total spots, total bases, size in MB, published date, SRA download URL, cloud URLs (JSON with AWS/GCP providers), and original file names (JSON, e.g. `.fq.gz` names). Links to parent SRX via `experiment_id`.

**dataset_files** — Original file names and URLs per accession. Populated from SRA RunInfo and XML metadata. Captures library name, strategy, layout, file size, and download paths.

**publication_datasets** — Many-to-many link between papers and datasets. `source` column indicates how the link was found: `ncbi_elink`, `geo_pubmed_id`, `abstract`, `title`, or `potentially_related_dataset` (text-mined from PMC full text; lower-confidence and kept explicitly flagged). The web UI and API separate these into distinct sections.

For `GSE*` accessions mined from PMC full text, the fetch/import/update scripts now attempt to enrich metadata in the same shape as linked GEO series (title/summary/design/contact fields, sample/platform metadata, citation PMIDs, and SRA runs when resolvable).

### Supporting tables

**affiliations** / **publication_affiliations** — Unique affiliation strings linked to papers.

**publication_summaries** — Placeholder for AI-generated one-line summaries, key findings, methods, data types, and model systems.

**update_log** — Tracks every update run with timestamps, counts, and notes.

### Full-text search

An FTS5 virtual table (`publications_fts`) indexes pmid, title, abstract, and journal_name for fast keyword search.

## Example Queries

```sql
-- Papers by year
SELECT pub_year, COUNT(*) FROM publications GROUP BY pub_year ORDER BY pub_year;

-- All eCLIP papers
SELECT pmid, title, pub_year FROM publications
WHERE abstract LIKE '%eCLIP%' OR title LIKE '%eCLIP%';

-- Top collaborators (non-Yeo authors with most co-authored papers)
SELECT a.fore_name, a.last_name, COUNT(*) as papers
FROM publication_authors pa
JOIN authors a ON pa.author_id = a.author_id
WHERE a.last_name != 'Yeo'
GROUP BY a.author_id ORDER BY papers DESC LIMIT 20;

-- Papers with GEO datasets (after import)
SELECT p.pmid, p.title, da.accession, da.title as dataset_title
FROM publications p
JOIN publication_datasets pd ON p.pmid = pd.pmid
JOIN dataset_accessions da ON pd.accession_id = da.accession_id
WHERE da.accession_type = 'GSE';

-- SRA experiments with library metadata
SELECT se.srx_accession, se.library_strategy, se.organism, se.instrument_model,
       da.accession as gse, da.title
FROM sra_experiments se
JOIN dataset_accessions da ON se.parent_accession_id = da.accession_id
WHERE se.library_strategy = 'RNA-Seq'
LIMIT 20;

-- Total data volume per GSE
SELECT da.accession, da.title, COUNT(sr.run_id) as runs,
       SUM(sr.total_spots) as total_spots, SUM(sr.size_mb) as total_mb
FROM dataset_accessions da
JOIN sra_experiments se ON se.parent_accession_id = da.accession_id
JOIN sra_runs sr ON sr.experiment_id = se.experiment_id
GROUP BY da.accession_id ORDER BY total_mb DESC LIMIT 20;

-- Full-text search
SELECT pmid, title FROM publications_fts WHERE publications_fts MATCH 'TDP-43 AND splicing';
```

## Deployment

The application supports production deployment with Docker and PostgreSQL on either AWS Elastic Beanstalk or DigitalOcean App Platform.

**Quick start with Docker:**

```bash
# Start PostgreSQL + Django locally
docker compose up -d --build

# Migrate existing SQLite data to PostgreSQL (one-time)
docker compose --profile migrate run --rm migrate-data
```

**To refresh your Docker instance with all the new changes (code examples system, sync command, schema updates), run:**
```bash
# Rebuild the image to pick up new files
docker compose build

# Restart with the updated image
docker compose up -d

# Run schema migration + backfill code examples
docker compose exec web python manage.py ensure_schema
docker compose exec web python manage.py backfill_code_examples --force
```

See [DEPLOYMENT.md](DEPLOYMENT.md) for complete instructions covering AWS EB, DigitalOcean, environment variables, data migration, CI/CD, and the production checklist.

## Troubleshooting (Docker Compose + PostgreSQL)

### Symptom: `web` returns 500 with `server closed the connection unexpectedly`

If you see logs like:
- `db ... received fast shutdown request`
- `web ... psycopg2.OperationalError: server closed the connection unexpectedly`

this usually means PostgreSQL restarted or was stopped while Django still had open DB connections.

### Safe recovery (no data loss)

Run from the project root:

```bash
# 1) Stop web first so no requests hit a restarting DB
docker compose stop web

# 2) Back up Postgres volume before recovery
docker run --rm -v yeolab_postgres_data:/data -v "$PWD":/backup alpine \
  sh -c 'cd /data && tar czf /backup/postgres_data_backup_$(date +%Y%m%d_%H%M%S).tgz .'

# 3) Start DB and wait until healthy
docker compose up -d db
docker compose ps
docker compose logs -f db

# 4) Verify DB connectivity
docker compose exec db pg_isready -U yeolab -d yeolab_publications
docker compose exec db psql -U yeolab -d yeolab_publications -c "select now();"

# 5) Start web and verify endpoint health
docker compose up -d web
docker compose ps
curl -i http://localhost:8000/healthz/
```

### Hardening now included in this repo

- `db` and `web` use `restart: unless-stopped` in `docker-compose.yml`
- `web` has a Compose healthcheck that probes `http://127.0.0.1:8000/healthz/`
- Django DB settings enable connection health checks (`CONN_HEALTH_CHECKS=True`) when using `DATABASE_URL`

These reduce user-facing 500s after transient DB restarts by recycling stale connections.

### Symptom: "Save Locally" appears to work but files are not visible on host

`Save Locally` writes to the container's resolved `code_examples` directory. In this repo, the expected host path is:

- `<repo>/yeolab-publications-db/code_examples`

`<repo>/code_examples` is a symlink to that directory.

If files are not showing up, recreate the `web` container so updated bind mounts are applied:

```bash
docker compose up -d --force-recreate web
```

### Symptom: "Fetch from GitHub" returns 404

`Fetch from GitHub` reads from:

- `GITHUB_REPO` (default `byee4/yeolab-publications-db`)
- `GITHUB_BRANCH` (default `main`)

and attempts both nested (`code_examples/<year>/<Mon>/<accession>.json`) and root paths.

If it still returns 404:

1. The file may not exist on that repo/branch yet.
2. `GITHUB_PAT` may be missing for private repo access.
3. `GITHUB_REPO`/`GITHUB_BRANCH` may target the wrong remote.

Set these in `.env` and recreate web:

```bash
GITHUB_PAT=ghp_...
GITHUB_REPO=byee4/yeolab-publications-db
GITHUB_BRANCH=main
docker compose up -d --force-recreate web
```

### Last resort (rebuild Postgres data volume)

Only do this if the DB volume is corrupted and you have a backup:

```bash
docker compose down
docker volume rm yeolab_postgres_data
docker compose up -d db
docker compose --profile migrate run --rm migrate-data
docker compose up -d web
```
