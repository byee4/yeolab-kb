#!/usr/bin/env python3
"""
Migrate data from the SQLite yeolab_publications.db to a PostgreSQL database.

Usage:
    python scripts/migrate_sqlite_to_postgres.py \
        --sqlite yeolab_publications.db \
        --postgres "postgresql://user:pass@localhost:5432/yeolab_publications" \
        [--schema schema/postgres_schema.sql]

Requires: psycopg2, sqlite3 (stdlib)
"""
import argparse
import sqlite3
import sys
import os

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    sys.exit("psycopg2 is required: pip install psycopg2-binary")


# Tables in dependency order (parents before children)
TABLE_ORDER = [
    "publications",
    "authors",
    "publication_authors",
    "affiliations",
    "publication_affiliations",
    "dataset_accessions",
    "publication_datasets",
    "dataset_files",
    "grants",
    "publication_grants",
    "sra_experiments",
    "sra_runs",
    "publication_summaries",
    "update_log",
]

# Tables that are FTS5 virtual tables or internal — skip during migration
SKIP_TABLES = {
    "publications_fts",
    "publications_fts_config",
    "publications_fts_content",
    "publications_fts_data",
    "publications_fts_docsize",
    "publications_fts_idx",
}

BATCH_SIZE = 1000


def get_sqlite_tables(sqlite_conn):
    """Return list of real table names from SQLite."""
    cur = sqlite_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    return [row[0] for row in cur.fetchall() if row[0] not in SKIP_TABLES]


def get_columns(sqlite_conn, table):
    """Return column names for a table (excluding SQLite implicit rowid)."""
    cur = sqlite_conn.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cur.fetchall()]


def migrate_table(sqlite_conn, pg_conn, table, columns):
    """Copy all rows from a SQLite table to PostgreSQL."""
    sqlite_cur = sqlite_conn.cursor()
    sqlite_cur.execute(f"SELECT {', '.join(columns)} FROM {table}")

    pg_cur = pg_conn.cursor()
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

    total = 0
    while True:
        rows = sqlite_cur.fetchmany(BATCH_SIZE)
        if not rows:
            break
        psycopg2.extras.execute_batch(pg_cur, insert_sql, rows, page_size=BATCH_SIZE)
        total += len(rows)

    pg_conn.commit()
    return total


def reset_sequences(pg_conn):
    """Reset PostgreSQL serial sequences to max(id) + 1 for all tables."""
    seq_queries = [
        "SELECT setval('authors_author_id_seq', COALESCE((SELECT MAX(author_id) FROM authors), 0) + 1, false)",
        "SELECT setval('publication_authors_id_seq', COALESCE((SELECT MAX(id) FROM publication_authors), 0) + 1, false)",
        "SELECT setval('dataset_accessions_accession_id_seq', COALESCE((SELECT MAX(accession_id) FROM dataset_accessions), 0) + 1, false)",
        "SELECT setval('publication_datasets_id_seq', COALESCE((SELECT MAX(id) FROM publication_datasets), 0) + 1, false)",
        "SELECT setval('dataset_files_file_id_seq', COALESCE((SELECT MAX(file_id) FROM dataset_files), 0) + 1, false)",
        "SELECT setval('grants_grant_id_seq', COALESCE((SELECT MAX(grant_id) FROM grants), 0) + 1, false)",
        "SELECT setval('publication_grants_id_seq', COALESCE((SELECT MAX(id) FROM publication_grants), 0) + 1, false)",
        "SELECT setval('sra_experiments_experiment_id_seq', COALESCE((SELECT MAX(experiment_id) FROM sra_experiments), 0) + 1, false)",
        "SELECT setval('sra_runs_run_id_seq', COALESCE((SELECT MAX(run_id) FROM sra_runs), 0) + 1, false)",
        "SELECT setval('update_log_id_seq', COALESCE((SELECT MAX(id) FROM update_log), 0) + 1, false)",
    ]
    cur = pg_conn.cursor()
    for q in seq_queries:
        try:
            cur.execute(q)
        except Exception as e:
            print(f"  Warning: sequence reset failed: {e}")
            pg_conn.rollback()
            continue
    pg_conn.commit()


def update_search_vectors(pg_conn):
    """Populate the search_vector column for all publications."""
    cur = pg_conn.cursor()
    cur.execute("""
        UPDATE publications SET search_vector =
            to_tsvector('english', COALESCE(title, '') || ' ' ||
                                   COALESCE(abstract, '') || ' ' ||
                                   COALESCE(journal_name, ''))
    """)
    pg_conn.commit()
    print(f"  Updated search_vector for all publications")


def validate(sqlite_conn, pg_conn):
    """Compare row counts between SQLite and PostgreSQL."""
    sqlite_cur = sqlite_conn.cursor()
    pg_cur = pg_conn.cursor()
    all_ok = True

    for table in TABLE_ORDER:
        sqlite_cur.execute(f"SELECT COUNT(*) FROM {table}")
        sq_count = sqlite_cur.fetchone()[0]

        try:
            pg_cur.execute(f"SELECT COUNT(*) FROM {table}")
            pg_count = pg_cur.fetchone()[0]
        except Exception:
            pg_conn.rollback()
            pg_count = "N/A"

        match = "OK" if sq_count == pg_count else "MISMATCH"
        if sq_count != pg_count:
            all_ok = False
        print(f"  {table}: SQLite={sq_count}, PostgreSQL={pg_count} [{match}]")

    return all_ok


def main():
    parser = argparse.ArgumentParser(description="Migrate SQLite → PostgreSQL")
    parser.add_argument("--sqlite", required=True, help="Path to SQLite database")
    parser.add_argument("--postgres", required=True, help="PostgreSQL connection string")
    parser.add_argument("--schema", default=None, help="Path to postgres_schema.sql (optional)")
    args = parser.parse_args()

    if not os.path.exists(args.sqlite):
        sys.exit(f"SQLite database not found: {args.sqlite}")

    print(f"Connecting to SQLite: {args.sqlite}")
    sqlite_conn = sqlite3.connect(args.sqlite)

    print(f"Connecting to PostgreSQL...")
    pg_conn = psycopg2.connect(args.postgres)

    # Step 1: Create schema if provided
    if args.schema:
        print(f"Creating schema from {args.schema}...")
        with open(args.schema) as f:
            schema_sql = f.read()
        pg_cur = pg_conn.cursor()
        pg_cur.execute(schema_sql)
        pg_conn.commit()
        print("  Schema created successfully")

    # Step 2: Migrate data table by table
    sqlite_tables = get_sqlite_tables(sqlite_conn)
    print(f"\nMigrating {len(sqlite_tables)} tables...")

    for table in TABLE_ORDER:
        if table not in sqlite_tables:
            print(f"  Skipping {table} (not in SQLite)")
            continue

        columns = get_columns(sqlite_conn, table)
        # Skip 'search_vector' column which is PG-only
        columns = [c for c in columns if c != 'search_vector']

        count = migrate_table(sqlite_conn, pg_conn, table, columns)
        print(f"  {table}: {count} rows migrated")

    # Step 3: Reset sequences
    print("\nResetting PostgreSQL sequences...")
    reset_sequences(pg_conn)

    # Step 4: Populate search vectors
    print("\nPopulating full-text search vectors...")
    update_search_vectors(pg_conn)

    # Step 5: Validate
    print("\nValidating migration...")
    ok = validate(sqlite_conn, pg_conn)

    sqlite_conn.close()
    pg_conn.close()

    if ok:
        print("\nMigration completed successfully!")
    else:
        print("\nMigration completed with MISMATCHES — please investigate.")
        sys.exit(1)


if __name__ == "__main__":
    main()
