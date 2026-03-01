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
docker compose up --build

# The app will be available at http://localhost:8000
```

**Migrate existing SQLite data to PostgreSQL:**

```bash
# One-time data migration (requires yeolab_publications.db in project root)
docker compose --profile migrate run migrate-data
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

Edit `.do/app.yaml` — replace `<your-github-username>` with your GitHub username, then:

```bash
doctl apps create --spec .do/app.yaml
```

This creates both the web service and a managed PostgreSQL database.

### 2. Set Secrets

In the DO dashboard (or via CLI), set these app-level environment variables:
- `DJANGO_SECRET_KEY`
- `GLOBUS_CLIENT_ID`
- `GLOBUS_CLIENT_SECRET`

The `DATABASE_URL` is automatically injected by the managed database component.

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
2. Migrates all 12 tables in dependency order
3. Resets PostgreSQL sequences (auto-increment counters)
4. Populates full-text search vectors
5. Validates row counts match between databases

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
