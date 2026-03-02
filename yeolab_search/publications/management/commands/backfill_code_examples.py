"""
Management command to backfill code_example, code_language, and github_url
for all existing pipeline_steps rows from the dataset-keyed code_examples/ registry.

Joins pipeline_steps → analysis_pipelines → dataset_accessions to get the
accession string, then looks up code examples by accession + step_order
(or accession + tool_name as fallback).

Safe to run multiple times — only updates rows where code_example IS NULL.

Usage:
    python manage.py backfill_code_examples          # backfill missing
    python manage.py backfill_code_examples --force   # overwrite all
    python manage.py backfill_code_examples --dry-run  # preview only
"""
from django.core.management.base import BaseCommand
from django.db import connection

from publications.code_examples import get_code_example, get_code_example_by_tool


class Command(BaseCommand):
    help = "Backfill code examples from dataset-keyed code_examples/ for existing pipeline steps."

    def add_arguments(self, parser):
        parser.add_argument(
            "--force",
            action="store_true",
            help="Overwrite existing code_example values (default: only fill NULLs)",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Preview changes without writing to the database",
        )

    def handle(self, *args, **options):
        force = options["force"]
        dry_run = options["dry_run"]
        cur = connection.cursor()

        # Join to get accession string for each step
        where_clause = "" if force else "AND ps.code_example IS NULL"
        cur.execute(f"""
            SELECT ps.step_id, ps.step_order, ps.tool_name, ps.tool_version,
                   ps.description, da.accession
            FROM pipeline_steps ps
            JOIN analysis_pipelines ap ON ps.pipeline_id = ap.pipeline_id
            LEFT JOIN dataset_accessions da ON ap.accession_id = da.accession_id
            WHERE ps.tool_name IS NOT NULL {where_clause}
        """)

        rows = cur.fetchall()
        self.stdout.write(f"Found {len(rows)} steps to process.")

        updated = 0
        matched = 0

        for step_id, step_order, tool_name, tool_version, description, accession in rows:
            if not accession:
                continue

            # Try by step_order first, then by tool_name
            code, lang, github = get_code_example(accession, step_order)
            if not code:
                code, lang, github = get_code_example_by_tool(accession, tool_name)

            if not code and not github:
                continue

            matched += 1

            if not dry_run:
                cur.execute(
                    "UPDATE pipeline_steps "
                    "SET code_example = %s, code_language = %s, github_url = %s "
                    "WHERE step_id = %s",
                    [code, lang, github, step_id],
                )
            else:
                self.stdout.write(
                    f"  [dry-run] Step {step_id} ({accession} #{step_order}): "
                    f"{tool_name} → {lang or 'github-only'} "
                    f"({len(code) if code else 0} chars)"
                )

            updated += 1

        if not dry_run:
            connection.connection.commit()

        self.stdout.write(
            self.style.SUCCESS(
                f"Done. {updated} steps updated, "
                f"{len(rows) - matched} had no matching entry in code_examples/."
            )
        )
