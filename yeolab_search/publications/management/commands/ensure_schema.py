"""
Management command to ensure the database schema has all expected columns.

Runs idempotent ALTER TABLE statements for columns added after the initial
schema was deployed. Safe to run on every startup.
"""
from django.core.management.base import BaseCommand
from django.db import connection


# (table, column, type) tuples for columns that may be missing
_COLUMN_ADDITIONS = [
    ("pipeline_steps", "code_example", "TEXT"),
    ("pipeline_steps", "code_language", "TEXT"),
    ("pipeline_steps", "github_url", "TEXT"),
]


class Command(BaseCommand):
    help = "Ensure all expected columns exist in unmanaged tables."

    def handle(self, *args, **options):
        cur = connection.cursor()

        # Check which columns already exist
        is_postgres = connection.vendor == "postgresql"

        for table, column, col_type in _COLUMN_ADDITIONS:
            if is_postgres:
                cur.execute(
                    "SELECT 1 FROM information_schema.columns "
                    "WHERE table_name = %s AND column_name = %s",
                    [table, column],
                )
            else:
                cur.execute(f"PRAGMA table_info({table})")
                existing_cols = [row[1] for row in cur.fetchall()]
                if column in existing_cols:
                    continue
                # For SQLite, just try the ALTER
                try:
                    cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
                    self.stdout.write(f"  Added {table}.{column}")
                except Exception:
                    pass
                continue

            if cur.fetchone():
                continue  # Column already exists

            cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
            self.stdout.write(f"  Added {table}.{column}")

        self.stdout.write(self.style.SUCCESS("Schema check complete."))
