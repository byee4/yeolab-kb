"""
Management command to migrate flat code_examples/*.json files into
year/month subdirectories and backfill ALL GSE accessions with
pipeline steps generated from DB metadata (library_strategy + methods).

Steps:
1. Migrate existing flat files (e.g. GSE120023.json) into year/month dirs
   based on their linked publication date.
2. For each GSE accession in the DB that doesn't already have a JSON file,
   generate a pipeline from SRA library_strategy + computational_methods.

Usage:
    python manage.py migrate_code_examples                   # migrate + backfill
    python manage.py migrate_code_examples --migrate-only     # only move flat files
    python manage.py migrate_code_examples --backfill-only    # only create stubs
    python manage.py migrate_code_examples --force            # overwrite existing files
    python manage.py migrate_code_examples --dry-run          # preview only
"""
import json
import os
import shutil

from django.core.management.base import BaseCommand
from django.db import connection


class Command(BaseCommand):
    help = "Migrate code examples into year/month folders and backfill GSE with pipeline steps from metadata."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Preview changes without writing",
        )
        parser.add_argument(
            "--migrate-only",
            action="store_true",
            help="Only migrate existing flat files",
        )
        parser.add_argument(
            "--backfill-only",
            action="store_true",
            help="Only backfill new GSE pipelines",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Overwrite existing files during backfill",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        migrate_only = options["migrate_only"]
        backfill_only = options["backfill_only"]
        force = options["force"]

        from publications.code_examples import _find_dir, reload_registry

        base_dir = _find_dir()
        if not base_dir:
            self.stderr.write(self.style.ERROR("code_examples/ directory not found"))
            return

        self.stdout.write(f"Base directory: {base_dir}")

        if not backfill_only:
            self._migrate_flat_files(base_dir, dry_run)

        if not migrate_only:
            self._backfill_gse(base_dir, dry_run, force)

        if not dry_run:
            reload_registry()
            self.stdout.write(self.style.SUCCESS("Registry reloaded."))

    def _migrate_flat_files(self, base_dir, dry_run):
        """Move flat files (code_examples/GSE*.json) into year/month subdirs."""
        import glob as g
        flat_files = []
        for fp in g.glob(os.path.join(base_dir, "*.json")):
            fn = os.path.basename(fp)
            if fn.startswith("_") or fn.startswith("."):
                continue
            flat_files.append(fp)

        if not flat_files:
            self.stdout.write("  No flat files to migrate.")
            return

        self.stdout.write(f"\n--- Migrating {len(flat_files)} flat file(s) ---")

        cur = connection.cursor()
        moved = 0

        for filepath in flat_files:
            accession = os.path.splitext(os.path.basename(filepath))[0]

            cur.execute("""
                SELECT p.pub_year, p.pub_month
                FROM publications p
                JOIN publication_datasets pd ON p.pmid = pd.pmid
                JOIN dataset_accessions da ON pd.accession_id = da.accession_id
                WHERE da.accession = %s AND p.pub_year IS NOT NULL
                ORDER BY p.pub_year ASC, p.pub_month ASC
                LIMIT 1
            """, [accession])
            row = cur.fetchone()

            if not row or not row[0]:
                self.stdout.write(
                    self.style.WARNING(f"  {accession}: no pub date found, skipping migration")
                )
                continue

            year, month = row[0], row[1] or 1
            from publications.code_examples import _MONTH_ABBR
            month_str = _MONTH_ABBR.get(month, "Unknown")

            dest_dir = os.path.join(base_dir, str(year), month_str)
            dest_path = os.path.join(dest_dir, os.path.basename(filepath))

            if dry_run:
                self.stdout.write(f"  [dry-run] MOVE: {accession} -> {year}/{month_str}/")
            else:
                os.makedirs(dest_dir, exist_ok=True)
                shutil.move(filepath, dest_path)
                self.stdout.write(f"  MOVED: {accession} -> {year}/{month_str}/")

            moved += 1

        self.stdout.write(f"  Migrated {moved} file(s).")

    def _backfill_gse(self, base_dir, dry_run, force):
        """Generate pipeline JSON files for all GSE accessions from DB metadata."""
        from publications.code_examples import (
            _MONTH_ABBR, generate_pipeline_from_metadata,
        )

        cur = connection.cursor()

        # Get all GSE accessions with their earliest pub date
        cur.execute("""
            SELECT da.accession,
                   MIN(p.pub_year) as pub_year,
                   MIN(CASE WHEN p.pub_year = (
                       SELECT MIN(p2.pub_year)
                       FROM publications p2
                       JOIN publication_datasets pd2 ON p2.pmid = pd2.pmid
                       WHERE pd2.accession_id = da.accession_id
                       AND p2.pub_year IS NOT NULL
                   ) THEN p.pub_month ELSE 9999 END) as pub_month
            FROM dataset_accessions da
            JOIN publication_datasets pd ON da.accession_id = pd.accession_id
            JOIN publications p ON pd.pmid = p.pmid
            WHERE da.accession_type = 'GSE' AND p.pub_year IS NOT NULL
            GROUP BY da.accession
            ORDER BY da.accession
        """)

        gse_rows = cur.fetchall()
        self.stdout.write(f"\n--- Backfilling {len(gse_rows)} GSE accession(s) ---")

        # Build a set of existing files (scan recursively)
        import glob as g
        existing = set()
        for fp in g.glob(os.path.join(base_dir, "**", "*.json"), recursive=True):
            acc = os.path.splitext(os.path.basename(fp))[0]
            existing.add(acc)

        created = 0
        updated = 0
        skipped_existing = 0
        skipped_no_pipeline = 0

        for accession, pub_year, pub_month in gse_rows:
            if accession in existing and not force:
                skipped_existing += 1
                continue

            if pub_month is None or pub_month > 12:
                pub_month = 1

            # Generate pipeline from DB metadata
            pipeline = generate_pipeline_from_metadata(accession)
            if not pipeline or not pipeline.get("steps"):
                skipped_no_pipeline += 1
                continue

            month_str = _MONTH_ABBR.get(pub_month, "Jan")
            dest_dir = os.path.join(base_dir, str(pub_year), month_str)
            dest_path = os.path.join(dest_dir, f"{accession}.json")

            step_count = len(pipeline["steps"])
            strategy = pipeline.get("library_strategy", "unknown")
            is_update = accession in existing

            if dry_run:
                action = "UPDATE" if is_update else "CREATE"
                self.stdout.write(
                    f"  [dry-run] {action}: {pub_year}/{month_str}/{accession}.json "
                    f"({step_count} steps, {strategy})"
                )
            else:
                os.makedirs(dest_dir, exist_ok=True)
                # Save only the steps (keep format consistent)
                save_data = {"steps": pipeline["steps"]}
                with open(dest_path, "w") as fh:
                    json.dump(save_data, fh, indent=2)

            if is_update:
                updated += 1
            else:
                created += 1

        action = "Would" if dry_run else "Done"
        self.stdout.write(
            self.style.SUCCESS(
                f"  {action}: created {created}, updated {updated}. "
                f"Skipped {skipped_existing} existing, "
                f"{skipped_no_pipeline} with no pipeline data."
            )
        )
