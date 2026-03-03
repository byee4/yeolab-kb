# Deployment Guide

This guide covers deploying the Yeo Lab Publications Database to production using Docker with PostgreSQL. Both AWS Elastic Beanstalk and DigitalOcean App Platform are supported.

## Prerequisites

- Docker and Docker Compose installed locally
- A GitHub account with the repository pushed
- For AWS: an AWS account with EB CLI installed
- For DigitalOcean: a DO account with `doctl` installed

## Local Development with Docker

**Quick start** (PostgreSQL backend):

```bash
# Start PostgreSQL + Django
docker compose up -d --build

# The app will be available at http://localhost:8000
```

**Migrate existing SQLite data to PostgreSQL:**

```bash
# One-time data migration (requires yeolab_publications.db in project root)
docker compose --profile migrate run --rm migrate-data
```

**Local development without Docker** (SQLite backend):

```bash
pip install -r requirements.txt
cd yeolab_search
python manage.py migrate
python manage.py runserver
```

When no `DATABASE_URL` is set, the app automatically uses SQLite at `../yeolab_publications.db`.

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | Production | (SQLite fallback) | PostgreSQL connection string |
| `DJANGO_SECRET_KEY` | Production | insecure dev key | Django secret key |
| `DJANGO_DEBUG` | No | `True` | Set to `False` in production |
| `DJANGO_ALLOWED_HOSTS` | Production | `*` | Comma-separated hostnames |
| `CSRF_TRUSTED_ORIGINS` | Production | (empty) | Comma-separated origins for CSRF |
| `GLOBUS_CLIENT_ID` | For auth | (empty) | Globus OAuth2 client ID |
| `GLOBUS_CLIENT_SECRET` | For auth | (empty) | Globus OAuth2 client secret |
| `GITHUB_PAT` | Optional | (empty) | PAT for Code Examples Editor GitHub fetch/push |
| `GITHUB_REPO` | Optional | `byee4/yeolab-publications-db` | Repo containing `code_examples/` JSON |
| `GITHUB_BRANCH` | Optional | `main` | Branch containing `code_examples/` JSON |
| `CODE_EXAMPLES_REPO_DIR` | Optional | `/app/yeolab-publications-db` | Local clone target for code examples repo |
| `CODE_EXAMPLES_DIR` | Optional | auto-discovered | Override local `code_examples` directory |
| `GUNICORN_TIMEOUT` | Optional | `600` | Worker timeout in seconds (increase to avoid chat SSE disconnects) |
| `SECURE_SSL_REDIRECT` | No | `True` (prod) | Set `False` if LB handles SSL |

Copy `.env.example` to `.env` and fill in your values for local Docker development.

## Option A: AWS Elastic Beanstalk

### 1. Create RDS PostgreSQL Instance

```bash
aws rds create-db-instance \
  --db-instance-identifier yeolab-db \
  --db-instance-class db.t3.micro \
  --engine postgres \
  --engine-version 16 \
  --master-username yeolab \
  --master-user-password YOUR_PASSWORD \
  --allocated-storage 20
```

### 2. Create ECR Repository

```bash
aws ecr create-repository --repository-name yeolab-search
```

### 3. Initialize Elastic Beanstalk

```bash
eb init -p docker yeolab-search --region us-west-2
eb create yeolab-search-production --single
```

### 4. Set Environment Variables

```bash
eb setenv \
  DATABASE_URL=postgresql://yeolab:PASSWORD@your-rds-endpoint:5432/yeolab_publications \
  DJANGO_SECRET_KEY=$(python -c "from django.core.management.utils import get_random_secret_key; print(get_random_secret_key())") \
  DJANGO_DEBUG=False \
  DJANGO_ALLOWED_HOSTS=your-eb-domain.elasticbeanstalk.com \
  CSRF_TRUSTED_ORIGINS=https://your-eb-domain.elasticbeanstalk.com \
  GLOBUS_CLIENT_ID=your-client-id \
  GLOBUS_CLIENT_SECRET=your-client-secret \
  SECURE_SSL_REDIRECT=False
```

### 5. Initialize the Database Schema

Connect to RDS and run the schema:

```bash
psql $DATABASE_URL -f schema/postgres_schema.sql
```

### 6. Migrate Data

```bash
python scripts/migrate_sqlite_to_postgres.py \
  --sqlite yeolab_publications.db \
  --postgres "$DATABASE_URL"
```

### 7. Deploy

```bash
eb deploy
```

Or push to `main` to trigger the GitHub Actions workflow (requires `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` secrets).

## Option B: DigitalOcean App Platform

### 1. Create the App

This repo is already configured in `.do/app.yaml` as `byee4/yeolab-kb` on `main`.
Create the app:

```bash
doctl apps create --spec .do/app.yaml
```

This creates both the web service and a managed PostgreSQL database.

### 2. Set Secrets

In the DO dashboard (or via CLI), set these app-level environment variables:
- `DJANGO_SECRET_KEY`
- `GLOBUS_CLIENT_ID`
- `GLOBUS_CLIENT_SECRET`
- `GITHUB_PAT` (optional but recommended for Code Examples Editor GitHub sync/push)

The `DATABASE_URL` is automatically injected by the managed database component.

### Code Examples Repo Sync on Startup

The web container startup script now performs:

1. `git clone` of `GITHUB_REPO` (default `byee4/yeolab-publications-db`) into `CODE_EXAMPLES_REPO_DIR` if missing.
2. `git fetch + reset --hard origin/<GITHUB_BRANCH>` if already cloned.
3. Sets `CODE_EXAMPLES_DIR` to `<CODE_EXAMPLES_REPO_DIR>/code_examples` for runtime/editor reads and writes.

This makes the Code Examples Editor operate on the checked-out repository copy inside the running app container.

### 3. Initialize the Database Schema

Get the database connection string from the DO dashboard, then:

```bash
psql $DATABASE_URL -f schema/postgres_schema.sql
```

### 4. Migrate Data

```bash
python scripts/migrate_sqlite_to_postgres.py \
  --sqlite yeolab_publications.db \
  --postgres "$DATABASE_URL"
```

### 5. Deploy

Push to `main` to trigger automatic deployment. Or manually:

```bash
doctl apps create-deployment YOUR_APP_ID
```

### 6. Release v1.0.0 (this repo)

```bash
git checkout main
git pull origin main
git tag -a 1.0.0 -m "Release 1.0.0"
git push origin main
git push origin 1.0.0
```

## Data Migration from SQLite

The migration script transfers all data from the existing SQLite database to PostgreSQL:

```bash
python scripts/migrate_sqlite_to_postgres.py \
  --sqlite yeolab_publications.db \
  --postgres "postgresql://user:pass@host:5432/yeolab_publications" \
  --schema schema/postgres_schema.sql
```

The `--schema` flag is optional if you've already created the schema. The script:

1. Creates the PostgreSQL schema (if `--schema` provided)
2. Migrates all core tables in dependency order
3. Resets PostgreSQL sequences (auto-increment counters)
4. Populates full-text search vectors
5. Validates row counts match between databases

## Data Migration from Local Postgres to Remote

**Project:** `yeolab-kb` / `yeolab_search`
**Scope:** Local Docker (`yeolab_publications`) to DigitalOcean Managed DB

### 1. Local Database Health Check (Docker)

Before exporting, ensure all sequences are synchronized with the data to prevent primary key collisions on the remote server.

#### Step A: Reset Sequences

Create a file named `fix_seqs.sql` on your Mac and run it inside the container:

```bash
docker compose exec -T db psql -d yeolab_publications -U yeolab < fix_seqs.sql

```

* **Purpose:** Updates `last_value` for all sequences to match `MAX(id)` for every table in the `public` schema.

#### Step B: Generate the "Perfect" Dump

```bash
docker compose exec db pg_dump -Fc --no-acl --no-owner -d yeolab_publications -U yeolab > yeolab_perfect_sync.dump

```

* **Flag `-Fc`:** Uses the compressed Custom format for faster, flexible restores.
* **Flag `--no-owner`:** Prevents local username conflicts on DigitalOcean.

### 2. Remote Database Preparation (DigitalOcean)

To resolve the **"public" vs "yeolab"** schema discrepancy, the remote target must be completely cleared.

### Step A: Wipe and Re-initialize

Run this command using your `DATABASE_URL`:

```bash
psql "DATABASE_URL" -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"

```

* **Critical:** This removes existing tables (like `authors`) that cause "relation already exists" errors during import.

### 3. The Migration (Restore)

Push the local binary dump to the remote server using `pg_restore`.

```bash
pg_restore -v --no-owner --no-privileges -d "DATABASE_URL" yeolab_perfect_sync.dump

```

* **Outcome:** All tables are restored into the `public` schema, which is where the web app expects them by default.

### 4. Troubleshooting Reference

| Issue | Likely Cause | Resolution |
| --- | --- | --- |
| **"relation already exists"** | Residual tables in `public` schema. | Run `DROP SCHEMA public CASCADE`. |
| **"column does not exist"** | Schema drift/outdated dump. | Verify columns (e.g., `source_gse`) or re-export. |
| **"OCI runtime failed"** | Docker volume mount conflict. | Check `docker-compose.yml` for file vs. folder mount errors. |
| **Missing SRA Data** | Different schemas (`yeolab` vs `public`). | Restore specifically into the `public` schema. |

### 5. Final Verification Query

Run this to confirm that **GSE72502** now correctly shows 30 experiments in production:

```bash
psql "DATABASE_URL" -c "SELECT count(*) FROM sra_experiments WHERE source_gse = 'GSE72502';"

```

## Post-Deploy Schema Safety

This app uses unmanaged models for most domain tables, plus Django-managed auth/session tables. After deploy, run:

```bash
cd /app/yeolab_search
python manage.py migrate --noinput
python manage.py ensure_schema
```

`ensure_schema` is idempotent and adds expected late-added columns in unmanaged tables.

## Updating Globus OAuth2 Redirect URL

After deployment, update your Globus app registration at `https://app.globus.org/settings/developers` to add your production redirect URL:

```
https://your-domain.com/complete/globus/
```

## CI/CD

GitHub Actions workflows are included:

- **ci.yml** — runs on every push/PR: creates a test PostgreSQL database, runs schema + migrations + Django checks
- **deploy-aws.yml** — deploys to AWS EB on push to `main` (requires AWS secrets)
- **deploy-do.yml** — deploys to DO App Platform on push to `main` (requires `DIGITALOCEAN_ACCESS_TOKEN` and `DO_APP_ID` secrets)

## Production Checklist

- [ ] `DJANGO_SECRET_KEY` is a random key (not the default)
- [ ] `DJANGO_DEBUG=False`
- [ ] `DJANGO_ALLOWED_HOSTS` lists only your domain(s)
- [ ] `CSRF_TRUSTED_ORIGINS` lists your https origin(s)
- [ ] Globus OAuth2 redirect URL updated for production domain
- [ ] Database backups configured (RDS automated backups / DO managed backups)
- [ ] SSL/TLS configured (ACM certificate for AWS / DO auto-SSL)
- [ ] `SECURE_SSL_REDIRECT=False` if load balancer terminates SSL
