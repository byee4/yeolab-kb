"""
Management command to sync per-dataset code example files from GitHub.

Fetches the recursive list of dataset JSON files from the remote
code_examples/ directory (year/month subdirs), compares with local,
and syncs added/changed datasets.

Usage:
    python manage.py sync_code_examples
    python manage.py sync_code_examples --backfill
    python manage.py sync_code_examples --backfill --force
    python manage.py sync_code_examples --dry-run
"""
import json

from django.core.management import call_command
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Fetch per-dataset code example files from GitHub and update the local registry."

    def add_arguments(self, parser):
        parser.add_argument(
            "--backfill",
            action="store_true",
            help="Re-run backfill_code_examples after syncing",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="With --backfill, overwrite existing code examples",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Preview what would change without writing",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        backfill = options["backfill"]
        force = options["force"]

        from publications.github_sync import list_remote_datasets, fetch_dataset
        from publications.github_sync import list_remote_json_files, fetch_remote_json_file
        from publications.code_examples import (
            list_datasets_with_paths, get_dataset_content, save_dataset_content,
            delete_dataset, reload_registry,
        )
        from publications import services

        # ── List remote datasets ──────────────────────────────────────
        self.stdout.write("Listing remote datasets on GitHub (recursive)...")

        try:
            remote_datasets = list_remote_datasets()
        except RuntimeError as e:
            self.stderr.write(self.style.ERROR(str(e)))
            return

        remote_by_acc = {d["accession"]: d for d in remote_datasets}
        remote_accessions = set(remote_by_acc.keys())

        local_datasets = list_datasets_with_paths()
        local_accessions = {d["accession"] for d in local_datasets}

        self.stdout.write(f"  Remote: {len(remote_accessions)} dataset(s)")
        self.stdout.write(f"  Local:  {len(local_accessions)} dataset(s)")

        # ── Compare ───────────────────────────────────────────────────
        added = remote_accessions - local_accessions
        removed = local_accessions - remote_accessions
        common = remote_accessions & local_accessions

        self.stdout.write(
            f"  New: {len(added)}, Local-only: {len(removed)}, Common: {len(common)}"
        )

        if added:
            self.stdout.write(f"  New datasets: {', '.join(sorted(added)[:20])}")

        # ── Fetch and compare ─────────────────────────────────────────
        to_fetch = sorted(added | common)
        fetched = 0
        changed = 0
        errors = 0

        for accession in to_fetch:
            remote_info = remote_by_acc[accession]
            rel_path = remote_info.get("rel_path", "")

            try:
                remote_content, sha = fetch_dataset(accession, rel_path=rel_path)
            except Exception as e:
                self.stderr.write(f"  Error fetching {accession}: {e}")
                errors += 1
                continue

            local_content = get_dataset_content(accession)

            # Normalize for comparison
            try:
                remote_norm = json.dumps(json.loads(remote_content), sort_keys=True)
                local_norm = json.dumps(json.loads(local_content), sort_keys=True) if local_content else None
            except (json.JSONDecodeError, TypeError):
                remote_norm = remote_content
                local_norm = local_content

            if remote_norm == local_norm:
                continue

            action = "ADD" if accession in added else "UPDATE"
            if accession not in added:
                changed += 1

            # Parse year/month from rel_path (e.g. "2020/Oct")
            year, month = None, None
            if rel_path:
                parts = rel_path.split("/")
                if len(parts) >= 2:
                    try:
                        year = int(parts[0])
                        month = parts[1]
                    except (ValueError, IndexError):
                        pass

            if dry_run:
                path_label = f"{rel_path}/{accession}" if rel_path else accession
                self.stdout.write(f"  [dry-run] {action}: {path_label}")
            else:
                try:
                    save_dataset_content(accession, remote_content, year=year, month=month)
                    self.stdout.write(f"  {action}: {accession}")
                    fetched += 1
                except ValueError as e:
                    self.stderr.write(f"  Error saving {accession}: {e}")
                    errors += 1

        # ── Report local-only ─────────────────────────────────────────
        if removed:
            self.stdout.write(
                self.style.WARNING(
                    f"  {len(removed)} local-only dataset(s): "
                    f"{', '.join(sorted(removed)[:10])}"
                )
            )

        if dry_run:
            self.stdout.write(self.style.WARNING("  [dry-run] No files written."))
            return

        if fetched == 0 and errors == 0:
            self.stdout.write(self.style.SUCCESS("  Already up to date."))
        else:
            reload_registry()
            self.stdout.write(
                self.style.SUCCESS(
                    f"  Sync complete. {fetched} file(s) written, {errors} error(s)."
                )
            )

        # ── ENCODEPROJECT_metadata import ────────────────────────────
        self.stdout.write("Syncing ENCODEPROJECT_metadata JSON files...")
        try:
            remote_meta = list_remote_json_files("ENCODEPROJECT_metadata")
        except Exception as e:
            self.stderr.write(self.style.WARNING(f"  Skipped ENCODEPROJECT_metadata sync: {e}"))
            remote_meta = []

        enc_meta_count = 0
        enc_meta_errors = 0
        for meta in remote_meta:
            name = str(meta.get("name", ""))
            if not name.upper().startswith("ENCSR") or not name.lower().endswith(".json"):
                continue
            path = meta.get("path", "")
            accession = name[:-5]
            try:
                content, _sha = fetch_remote_json_file(path)
                payload = json.loads(content)
                if not isinstance(payload, dict):
                    raise ValueError("Payload is not a JSON object")
                result = services.import_encode_experiment_detail_payloads(
                    payloads=[payload],
                    grant_label="github_encodeproject_metadata",
                    override_existing=force,
                )
                if result.get("errors"):
                    raise RuntimeError(result["errors"][0])
                enc_meta_count += 1
                self.stdout.write(f"  ENCODE metadata: {accession}")
            except Exception as e:
                enc_meta_errors += 1
                self.stderr.write(f"  Error importing {accession} from {path}: {e}")

        if remote_meta:
            self.stdout.write(
                self.style.SUCCESS(
                    f"  ENCODEPROJECT_metadata import complete: {enc_meta_count} imported, {enc_meta_errors} errors."
                )
            )

        # ── Optional backfill ─────────────────────────────────────────
        if backfill:
            self.stdout.write("  Running backfill_code_examples...")
            call_command("backfill_code_examples", force=force)
