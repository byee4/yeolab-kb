#!/usr/bin/env python3
"""
Import GEO/SRA results into the Yeo Lab publications SQLite database.
Run this after fetch_geo_sra_metadata.py has produced yeolab_geo_sra_results.json.

This script imports ALL metadata captured by the fetcher, including:
  - Full GEO series details (title, summary, design, types, contacts, relations, samples)
  - SRX experiment-level metadata (library info, platform, sample attributes, biosample, bioproject)
  - SRR run-level metadata (spots, bases, size, original file names, cloud URLs)
  - Text-mined accessions from PMC full text (imported as potentially related datasets)

Schema additions over the original:
  - dataset_accessions: new columns for last_update_date, contact_name, contact_institute,
    experiment_types, relations, sample_ids
  - sra_experiments: one row per SRX with full library/sample metadata
  - sra_runs: one row per SRR with spots/bases/size and original file names

Usage:
    python import_geo_sra_results.py [--db yeolab_publications.db] [--input yeolab_geo_sra_results.json]
    python import_geo_sra_results.py --clear   # wipe dataset tables before importing

Note: The database must already contain publications (run `update_yeolab_db.py --pubmed-only` first
if starting from scratch). This script will create the dataset tables if they don't exist.
"""

import sqlite3
import json
import argparse
import os
import sys
from datetime import datetime


# ============================================================
# SCHEMA — full schema including new SRA experiment/run tables
# ============================================================
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS publications (
    pmid TEXT PRIMARY KEY,
    pmc_id TEXT, doi TEXT, pii TEXT,
    title TEXT NOT NULL, abstract TEXT,
    journal_name TEXT, journal_iso TEXT,
    pub_date TEXT, pub_year INTEGER, pub_month INTEGER, pub_day INTEGER,
    volume TEXT, issue TEXT, pages TEXT,
    pub_types TEXT, mesh_terms TEXT, keywords TEXT,
    language TEXT, is_open_access INTEGER DEFAULT 0,
    citation_count INTEGER, abstract_word_count INTEGER,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS authors (
    author_id INTEGER PRIMARY KEY AUTOINCREMENT,
    last_name TEXT, fore_name TEXT, initials TEXT, orcid TEXT,
    UNIQUE(last_name, fore_name, initials)
);
CREATE TABLE IF NOT EXISTS publication_authors (
    pmid TEXT NOT NULL, author_id INTEGER NOT NULL,
    author_position INTEGER NOT NULL,
    is_first_author INTEGER DEFAULT 0, is_last_author INTEGER DEFAULT 0,
    affiliation TEXT,
    PRIMARY KEY (pmid, author_id, author_position),
    FOREIGN KEY (pmid) REFERENCES publications(pmid),
    FOREIGN KEY (author_id) REFERENCES authors(author_id)
);
CREATE TABLE IF NOT EXISTS affiliations (
    affiliation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    affiliation_text TEXT UNIQUE,
    institution TEXT, department TEXT, city TEXT, state TEXT, country TEXT
);
CREATE TABLE IF NOT EXISTS publication_affiliations (
    pmid TEXT NOT NULL, affiliation_id INTEGER NOT NULL,
    PRIMARY KEY (pmid, affiliation_id),
    FOREIGN KEY (pmid) REFERENCES publications(pmid),
    FOREIGN KEY (affiliation_id) REFERENCES affiliations(affiliation_id)
);
CREATE TABLE IF NOT EXISTS dataset_accessions (
    accession_id INTEGER PRIMARY KEY AUTOINCREMENT,
    accession TEXT NOT NULL UNIQUE,
    accession_type TEXT NOT NULL, database TEXT NOT NULL,
    title TEXT, organism TEXT, platform TEXT,
    summary TEXT, overall_design TEXT,
    num_samples INTEGER, submission_date TEXT, status TEXT,
    supplementary_files TEXT,
    last_update_date TEXT,
    contact_name TEXT,
    contact_institute TEXT,
    experiment_types TEXT,
    relations TEXT,
    sample_ids TEXT,
    citation_pmids TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS publication_datasets (
    pmid TEXT NOT NULL, accession_id INTEGER NOT NULL,
    source TEXT DEFAULT 'abstract',
    PRIMARY KEY (pmid, accession_id),
    FOREIGN KEY (pmid) REFERENCES publications(pmid),
    FOREIGN KEY (accession_id) REFERENCES dataset_accessions(accession_id)
);
CREATE TABLE IF NOT EXISTS dataset_files (
    file_id INTEGER PRIMARY KEY AUTOINCREMENT,
    accession_id INTEGER NOT NULL,
    file_name TEXT NOT NULL, file_type TEXT,
    file_size_bytes INTEGER, file_url TEXT, md5_checksum TEXT,
    FOREIGN KEY (accession_id) REFERENCES dataset_accessions(accession_id)
);
CREATE TABLE IF NOT EXISTS sra_experiments (
    experiment_id INTEGER PRIMARY KEY AUTOINCREMENT,
    srx_accession TEXT NOT NULL UNIQUE,
    parent_accession_id INTEGER,
    source_gse TEXT,
    title TEXT,
    alias TEXT,
    sample_accession TEXT,
    sample_name TEXT,
    sample_alias TEXT,
    study_accession TEXT,
    bioproject TEXT,
    biosample TEXT,
    library_name TEXT,
    library_strategy TEXT,
    library_source TEXT,
    library_selection TEXT,
    library_layout TEXT,
    platform TEXT,
    instrument_model TEXT,
    organism TEXT,
    sample_attributes TEXT,
    original_file_names TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (parent_accession_id) REFERENCES dataset_accessions(accession_id)
);
CREATE TABLE IF NOT EXISTS sra_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    srr_accession TEXT NOT NULL UNIQUE,
    experiment_id INTEGER,
    srx_accession TEXT,
    alias TEXT,
    total_spots INTEGER,
    total_bases INTEGER,
    size_mb REAL,
    published_date TEXT,
    sra_url TEXT,
    cloud_urls TEXT,
    file_names TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (experiment_id) REFERENCES sra_experiments(experiment_id)
);
CREATE TABLE IF NOT EXISTS grants (
    grant_id INTEGER PRIMARY KEY AUTOINCREMENT,
    grant_number TEXT, agency TEXT, country TEXT,
    UNIQUE(grant_number, agency)
);
CREATE TABLE IF NOT EXISTS publication_grants (
    pmid TEXT NOT NULL, grant_id INTEGER NOT NULL,
    PRIMARY KEY (pmid, grant_id),
    FOREIGN KEY (pmid) REFERENCES publications(pmid),
    FOREIGN KEY (grant_id) REFERENCES grants(grant_id)
);
CREATE TABLE IF NOT EXISTS publication_summaries (
    pmid TEXT PRIMARY KEY,
    one_line_summary TEXT, key_findings TEXT,
    methods_summary TEXT, data_types TEXT, model_systems TEXT,
    generated_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (pmid) REFERENCES publications(pmid)
);
CREATE TABLE IF NOT EXISTS update_log (
    update_id INTEGER PRIMARY KEY AUTOINCREMENT,
    update_time TEXT DEFAULT (datetime('now')),
    total_pmids_in_pubmed INTEGER, new_pmids_added INTEGER,
    metadata_updated INTEGER, accessions_found INTEGER, notes TEXT
);
CREATE INDEX IF NOT EXISTS idx_pub_year ON publications(pub_year);
CREATE INDEX IF NOT EXISTS idx_pub_doi ON publications(doi);
CREATE INDEX IF NOT EXISTS idx_pub_pmc ON publications(pmc_id);
CREATE INDEX IF NOT EXISTS idx_author_name ON authors(last_name, fore_name);
CREATE INDEX IF NOT EXISTS idx_pubauth_pmid ON publication_authors(pmid);
CREATE INDEX IF NOT EXISTS idx_dataset_type ON dataset_accessions(accession_type);
CREATE INDEX IF NOT EXISTS idx_pubdata_pmid ON publication_datasets(pmid);
CREATE INDEX IF NOT EXISTS idx_files_acc ON dataset_files(accession_id);
CREATE INDEX IF NOT EXISTS idx_sra_exp_gse ON sra_experiments(source_gse);
CREATE INDEX IF NOT EXISTS idx_sra_exp_parent ON sra_experiments(parent_accession_id);
CREATE INDEX IF NOT EXISTS idx_sra_exp_strategy ON sra_experiments(library_strategy);
CREATE INDEX IF NOT EXISTS idx_sra_exp_organism ON sra_experiments(organism);
CREATE INDEX IF NOT EXISTS idx_sra_runs_exp ON sra_runs(experiment_id);
CREATE INDEX IF NOT EXISTS idx_sra_runs_srx ON sra_runs(srx_accession);
"""

# Columns to add to dataset_accessions if they don't exist yet (for upgrading older DBs)
DATASET_ACCESSIONS_NEW_COLS = [
    ("last_update_date", "TEXT"),
    ("contact_name", "TEXT"),
    ("contact_institute", "TEXT"),
    ("experiment_types", "TEXT"),
    ("relations", "TEXT"),
    ("sample_ids", "TEXT"),
    ("citation_pmids", "TEXT"),
]


def ensure_db(db_path):
    """Create tables if they don't exist. Add new columns if upgrading. Returns connection."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA_SQL)
    conn.commit()

    # Add new columns to dataset_accessions if they don't exist (upgrade path)
    existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(dataset_accessions)")}
    for col_name, col_type in DATASET_ACCESSIONS_NEW_COLS:
        if col_name not in existing_cols:
            print(f"  Adding column dataset_accessions.{col_name} ({col_type})")
            conn.execute(f"ALTER TABLE dataset_accessions ADD COLUMN {col_name} {col_type}")

    # Backward-compat migration: preserve old text-mined links but flag as potential.
    cur = conn.execute("""
        UPDATE publication_datasets
        SET source='potentially_related_dataset'
        WHERE source='pmc_full_text'
    """)
    migrated = cur.rowcount if cur.rowcount is not None else 0
    if migrated > 0:
        print(f"  Migrated {migrated} publication_datasets rows from pmc_full_text to potentially_related_dataset")
    conn.commit()
    return conn


def classify_database(acc_type):
    mapping = {
        'GSE': 'GEO', 'GSM': 'GEO', 'GDS': 'GEO', 'GPL': 'GEO',
        'SRP': 'SRA', 'SRR': 'SRA', 'SRX': 'SRA', 'SRS': 'SRA',
        'PRJNA': 'BioProject', 'PRJEB': 'BioProject',
        'ERP': 'ENA', 'DRP': 'DDBJ',
        'ENCSR': 'ENCODE', 'ENCFF': 'ENCODE',
        'E-MTAB': 'ArrayExpress', 'E-GEOD': 'ArrayExpress',
        'MASSIVE': 'MassIVE', 'PXD': 'PRIDE',
    }
    return mapping.get(acc_type, 'Unknown')


def json_or_none(obj):
    """Convert a list/dict to JSON string, or None if empty."""
    if not obj:
        return None
    return json.dumps(obj, default=str)


def safe_int(val):
    """Convert a value to int, returning None on failure."""
    if val is None or val == "":
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def safe_float(val):
    """Convert a value to float, returning None on failure."""
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def clear_dataset_tables(conn):
    """Wipe all dataset-related tables for a clean reimport."""
    tables = ['sra_runs', 'sra_experiments', 'dataset_files', 'publication_datasets', 'dataset_accessions']
    for table in tables:
        try:
            conn.execute(f"DELETE FROM {table}")
            print(f"  Cleared {table}")
        except sqlite3.OperationalError:
            pass  # table may not exist yet
    conn.commit()


def import_results(db_path, json_path, clear_first=False):
    print(f"Importing GEO/SRA results into {db_path}")
    print(f"Source: {json_path}")

    with open(json_path) as f:
        data = json.load(f)

    # Ensure all tables exist (uses CREATE IF NOT EXISTS, safe to re-run)
    conn = ensure_db(db_path)

    if clear_first:
        print("\nClearing existing dataset tables...")
        clear_dataset_tables(conn)

    # Check if publications table has data
    pub_count = conn.execute("SELECT COUNT(*) FROM publications").fetchone()[0]
    if pub_count == 0:
        print("\nWARNING: The publications table is empty!")
        print("You need to populate it first. Run:")
        print("  python update_yeolab_db.py --pubmed-only")
        print("\nThen re-run this import script.")
        print("(Continuing anyway — PMIDs without matching publications will be skipped.)")

    metadata = data.get("metadata", {})
    print(f"\nSource metadata:")
    for k, v in metadata.items():
        print(f"  {k}: {v}")

    # ========================================================
    # PHASE 1: Import all GEO series from geo_details
    # ========================================================
    print(f"\n--- Phase 1: Importing GEO series details ---")
    geo_details = data.get("geo_details", {})
    gse_inserted = 0
    gse_updated = 0

    for gse, detail in geo_details.items():
        if detail.get("error"):
            continue

        accession = detail.get("accession", gse)
        if not accession:
            continue

        title = detail.get("title", "")
        summary = (detail.get("summary", "") or "").strip()
        overall_design = (detail.get("overall_design", "") or "").strip()
        organisms = detail.get("organisms", [])
        organism = organisms[0] if organisms else ""
        platforms = detail.get("platforms", [])
        platform = ", ".join(platforms) if platforms else ""
        n_samples = detail.get("n_samples", 0)
        supp_files = detail.get("supplementary_files", [])
        submission_date = detail.get("submission_date", "")
        last_update_date = detail.get("last_update_date", "")
        status = detail.get("status", "")
        contact_name = detail.get("contact_name", "")
        contact_institute = detail.get("contact_institute", "")
        exp_types = detail.get("types", [])
        relations = detail.get("relations", [])
        sample_ids = detail.get("samples", [])
        citation_pmids = detail.get("pubmed_ids", [])

        existing = conn.execute(
            "SELECT accession_id FROM dataset_accessions WHERE accession=?",
            (accession,)
        ).fetchone()

        if not existing:
            conn.execute("""
                INSERT INTO dataset_accessions
                (accession, accession_type, database, title, organism, platform,
                 summary, overall_design, num_samples, supplementary_files,
                 submission_date, last_update_date, status,
                 contact_name, contact_institute, experiment_types,
                 relations, sample_ids, citation_pmids)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                accession, "GSE", "GEO", title, organism, platform,
                summary, overall_design, n_samples,
                json_or_none(supp_files),
                submission_date, last_update_date, status,
                contact_name, contact_institute,
                json_or_none(exp_types),
                json_or_none(relations),
                json_or_none(sample_ids),
                json_or_none(citation_pmids),
            ))
            gse_inserted += 1
        else:
            # Update existing GSE with richer metadata from geo_details
            conn.execute("""
                UPDATE dataset_accessions SET
                    title = COALESCE(NULLIF(?, ''), title),
                    organism = COALESCE(NULLIF(?, ''), organism),
                    platform = COALESCE(NULLIF(?, ''), platform),
                    summary = COALESCE(NULLIF(?, ''), summary),
                    overall_design = COALESCE(NULLIF(?, ''), overall_design),
                    num_samples = COALESCE(?, num_samples),
                    supplementary_files = COALESCE(?, supplementary_files),
                    submission_date = COALESCE(NULLIF(?, ''), submission_date),
                    last_update_date = COALESCE(NULLIF(?, ''), last_update_date),
                    status = COALESCE(NULLIF(?, ''), status),
                    contact_name = COALESCE(NULLIF(?, ''), contact_name),
                    contact_institute = COALESCE(NULLIF(?, ''), contact_institute),
                    experiment_types = COALESCE(?, experiment_types),
                    relations = COALESCE(?, relations),
                    sample_ids = COALESCE(?, sample_ids),
                    citation_pmids = COALESCE(?, citation_pmids)
                WHERE accession = ?
            """, (
                title, organism, platform,
                summary, overall_design, n_samples if n_samples else None,
                json_or_none(supp_files),
                submission_date, last_update_date, status,
                contact_name, contact_institute,
                json_or_none(exp_types),
                json_or_none(relations),
                json_or_none(sample_ids),
                json_or_none(citation_pmids),
                accession,
            ))
            gse_updated += 1

    conn.commit()
    print(f"  GSE series inserted: {gse_inserted}, updated: {gse_updated}")

    # ========================================================
    # PHASE 2: Import SRX experiments from srx_experiments
    # ========================================================
    print(f"\n--- Phase 2: Importing SRX experiments ---")
    srx_experiments = data.get("srx_experiments", {})
    srx_inserted = 0
    srx_skipped = 0

    # Build a cache of accession -> accession_id for linking
    acc_id_cache = {}
    for row in conn.execute("SELECT accession, accession_id FROM dataset_accessions"):
        acc_id_cache[row[0]] = row[1]

    for srx, exp in srx_experiments.items():
        if not srx:
            continue

        existing = conn.execute(
            "SELECT experiment_id FROM sra_experiments WHERE srx_accession=?",
            (srx,)
        ).fetchone()
        if existing:
            srx_skipped += 1
            continue

        source_gse = exp.get("source_gse", "")
        parent_acc_id = acc_id_cache.get(source_gse)

        conn.execute("""
            INSERT OR IGNORE INTO sra_experiments
            (srx_accession, parent_accession_id, source_gse,
             title, alias, sample_accession, sample_name, sample_alias,
             study_accession, bioproject, biosample,
             library_name, library_strategy, library_source,
             library_selection, library_layout,
             platform, instrument_model, organism,
             sample_attributes, original_file_names)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            srx, parent_acc_id, source_gse,
            exp.get("title", ""),
            exp.get("alias", ""),
            exp.get("sample_accession", ""),
            exp.get("sample_name", ""),
            exp.get("sample_alias", ""),
            exp.get("study_accession", ""),
            exp.get("bioproject", ""),
            exp.get("biosample", ""),
            exp.get("library_name", ""),
            exp.get("library_strategy", ""),
            exp.get("library_source", ""),
            exp.get("library_selection", ""),
            exp.get("library_layout", ""),
            exp.get("platform", ""),
            exp.get("instrument_model", ""),
            exp.get("organism", ""),
            json_or_none(exp.get("sample_attributes", {})),
            json_or_none(exp.get("original_file_names", [])),
        ))
        srx_inserted += 1

    conn.commit()
    print(f"  SRX experiments inserted: {srx_inserted}, skipped (existing): {srx_skipped}")

    # Build SRX -> experiment_id cache
    srx_id_cache = {}
    for row in conn.execute("SELECT srx_accession, experiment_id FROM sra_experiments"):
        srx_id_cache[row[0]] = row[1]

    # ========================================================
    # PHASE 3: Import SRR runs from srx_experiments (hierarchical)
    # and sra_runs (flat, for any extras)
    # ========================================================
    print(f"\n--- Phase 3: Importing SRR runs ---")
    srr_inserted = 0
    srr_skipped = 0
    seen_srrs = set()

    # First pass: import from srx_experiments (preserves SRX→SRR hierarchy)
    for srx, exp in srx_experiments.items():
        experiment_id = srx_id_cache.get(srx)
        for run in exp.get("runs", []):
            srr = run.get("accession", "")
            if not srr or srr in seen_srrs:
                continue
            seen_srrs.add(srr)

            existing = conn.execute(
                "SELECT run_id FROM sra_runs WHERE srr_accession=?", (srr,)
            ).fetchone()
            if existing:
                srr_skipped += 1
                continue

            conn.execute("""
                INSERT OR IGNORE INTO sra_runs
                (srr_accession, experiment_id, srx_accession, alias,
                 total_spots, total_bases, size_mb,
                 published_date, sra_url, cloud_urls, file_names)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                srr, experiment_id, srx,
                run.get("alias", ""),
                safe_int(run.get("total_spots")),
                safe_int(run.get("total_bases")),
                safe_float(run.get("size_mb")),
                run.get("published", ""),
                run.get("sra_url", ""),
                json_or_none(run.get("cloud_urls", [])),
                json_or_none(run.get("file_names", [])),
            ))
            srr_inserted += 1

    # Second pass: import from sra_runs (flat) for anything missed
    sra_runs = data.get("sra_runs", {})
    for srr, run_data in sra_runs.items():
        if not srr or srr in seen_srrs:
            continue
        seen_srrs.add(srr)

        srx = run_data.get("Experiment", "")
        experiment_id = srx_id_cache.get(srx)

        existing = conn.execute(
            "SELECT run_id FROM sra_runs WHERE srr_accession=?", (srr,)
        ).fetchone()
        if existing:
            srr_skipped += 1
            continue

        size_mb = safe_float(run_data.get("size_MB"))
        conn.execute("""
            INSERT OR IGNORE INTO sra_runs
            (srr_accession, experiment_id, srx_accession, alias,
             total_spots, total_bases, size_mb,
             published_date, sra_url, cloud_urls, file_names)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            srr, experiment_id, srx,
            run_data.get("run_alias", ""),
            safe_int(run_data.get("spots")),
            safe_int(run_data.get("bases")),
            size_mb,
            "",  # no published date in flat format
            run_data.get("download_path", ""),
            None,  # no cloud_urls in flat format
            json_or_none(run_data.get("run_file_names", [])),
        ))
        srr_inserted += 1

    conn.commit()
    print(f"  SRR runs inserted: {srr_inserted}, skipped (existing): {srr_skipped}")

    # ========================================================
    # PHASE 4: Import per-PMID dataset links and text-mined accessions
    # ========================================================
    print(f"\n--- Phase 4: Importing PMID-dataset links ---")
    pmid_datasets = data.get("pmid_datasets", {})
    pmid_potential_datasets = data.get("pmid_potential_datasets", {})

    acc_inserted = 0
    link_inserted = 0
    potential_link_inserted = 0
    file_inserted = 0
    pmids_skipped = 0
    pmids_matched = 0
    pmid_seen_status = {}

    # Refresh acc_id_cache after phase 1 insertions
    acc_id_cache.clear()
    for row in conn.execute("SELECT accession, accession_id FROM dataset_accessions"):
        acc_id_cache[row[0]] = row[1]

    for dataset_map, force_potential in (
        (pmid_datasets, False),
        (pmid_potential_datasets, True),
    ):
        for pmid, datasets in dataset_map.items():
            # Verify PMID exists in publications table (count each PMID once)
            if pmid in pmid_seen_status:
                exists = pmid_seen_status[pmid]
            else:
                exists = bool(conn.execute("SELECT 1 FROM publications WHERE pmid=?", (pmid,)).fetchone())
                pmid_seen_status[pmid] = exists
                if exists:
                    pmids_matched += 1
                else:
                    pmids_skipped += 1
            if not exists:
                continue

            for ds in datasets:
                accession = ds.get("accession", "")
                acc_type = ds.get("type", "")
                database = ds.get("database", classify_database(acc_type))
                source = ds.get("source", "unknown")
                if force_potential or source == "pmc_full_text":
                    source = "potentially_related_dataset"

                if not accession:
                    continue

                # Insert accession if not already present (for text-mined accessions)
                if accession not in acc_id_cache:
                    title = ds.get("title", "")
                    summary = ds.get("summary", "")
                    overall_design = ds.get("overall_design", "")
                    organisms = ds.get("organisms", [])
                    organism = organisms[0] if organisms else ""
                    platforms = ds.get("platforms", [])
                    platform = platforms[0] if platforms else ""
                    n_samples = ds.get("n_samples", 0)
                    supp_files = ds.get("supplementary_files", [])
                    citation_pmids = ds.get("pubmed_ids", [])

                    try:
                        conn.execute("""
                            INSERT INTO dataset_accessions
                            (accession, accession_type, database, title, organism, platform,
                             summary, overall_design, num_samples, supplementary_files, citation_pmids)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            accession, acc_type, database, title, organism, platform,
                            summary, overall_design, n_samples,
                            json_or_none(supp_files),
                            json_or_none(citation_pmids),
                        ))
                        acc_inserted += 1
                        # Update cache
                        row = conn.execute(
                            "SELECT accession_id FROM dataset_accessions WHERE accession=?",
                            (accession,)
                        ).fetchone()
                        if row:
                            acc_id_cache[accession] = row[0]
                    except sqlite3.IntegrityError:
                        # Race condition or duplicate — fetch existing ID
                        row = conn.execute(
                            "SELECT accession_id FROM dataset_accessions WHERE accession=?",
                            (accession,)
                        ).fetchone()
                        if row:
                            acc_id_cache[accession] = row[0]

                acc_id = acc_id_cache.get(accession)
                if not acc_id:
                    continue

                # Backfill metadata when present in incoming record (helps upgrade old sparse rows).
                ds_title = ds.get("title", "")
                ds_summary = ds.get("summary", "")
                ds_overall_design = ds.get("overall_design", "")
                ds_organisms = ds.get("organisms", [])
                ds_organism = ds_organisms[0] if ds_organisms else ""
                ds_platforms = ds.get("platforms", [])
                ds_platform = ds_platforms[0] if ds_platforms else ""
                ds_n_samples = ds.get("n_samples", 0)
                ds_supp_files = ds.get("supplementary_files", [])
                ds_submission_date = ds.get("submission_date", "")
                ds_last_update_date = ds.get("last_update_date", "")
                ds_status = ds.get("status", "")
                ds_contact_name = ds.get("contact_name", "")
                ds_contact_institute = ds.get("contact_institute", "")
                ds_types = ds.get("types", [])
                ds_relations = ds.get("relations", [])
                ds_samples = ds.get("samples", [])
                ds_citation_pmids = ds.get("pubmed_ids", [])
                conn.execute("""
                    UPDATE dataset_accessions SET
                        title = COALESCE(NULLIF(?, ''), title),
                        organism = COALESCE(NULLIF(?, ''), organism),
                        platform = COALESCE(NULLIF(?, ''), platform),
                        summary = COALESCE(NULLIF(?, ''), summary),
                        overall_design = COALESCE(NULLIF(?, ''), overall_design),
                        num_samples = COALESCE(?, num_samples),
                        supplementary_files = COALESCE(?, supplementary_files),
                        submission_date = COALESCE(NULLIF(?, ''), submission_date),
                        last_update_date = COALESCE(NULLIF(?, ''), last_update_date),
                        status = COALESCE(NULLIF(?, ''), status),
                        contact_name = COALESCE(NULLIF(?, ''), contact_name),
                        contact_institute = COALESCE(NULLIF(?, ''), contact_institute),
                        experiment_types = COALESCE(?, experiment_types),
                        relations = COALESCE(?, relations),
                        sample_ids = COALESCE(?, sample_ids),
                        citation_pmids = COALESCE(?, citation_pmids)
                    WHERE accession_id = ?
                """, (
                    ds_title, ds_organism, ds_platform,
                    ds_summary, ds_overall_design, ds_n_samples if ds_n_samples else None,
                    json_or_none(ds_supp_files),
                    ds_submission_date, ds_last_update_date, ds_status,
                    ds_contact_name, ds_contact_institute,
                    json_or_none(ds_types), json_or_none(ds_relations),
                    json_or_none(ds_samples), json_or_none(ds_citation_pmids),
                    acc_id,
                ))

                # Insert publication-dataset link
                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO publication_datasets (pmid, accession_id, source)
                        VALUES (?, ?, ?)
                    """, (pmid, acc_id, source))
                    link_inserted += 1
                    if source == "potentially_related_dataset":
                        potential_link_inserted += 1
                except sqlite3.IntegrityError:
                    pass

                # Import SRA files from sra_experiments (hierarchical) in pmid_datasets
                for exp in ds.get("sra_experiments", []):
                    for run in exp.get("runs", []):
                        srr = run.get("srr", "")
                        if not srr:
                            continue
                        sample_name = exp.get("sample_name", "")
                        strategy = exp.get("library_strategy", "")
                        size_mb = run.get("size_mb", "")
                        sra_url = run.get("sra_url", "")
                        file_names = run.get("file_names", [])

                        # Use original file names if available, otherwise SRR accession
                        display_name = file_names[0] if file_names else f"{srr}_{sample_name}" if sample_name else srr
                        file_type = strategy if strategy else "FASTQ"

                        try:
                            size_bytes = int(float(size_mb) * 1024 * 1024) if size_mb else None
                        except (ValueError, TypeError):
                            size_bytes = None

                        conn.execute("""
                            INSERT OR IGNORE INTO dataset_files
                            (accession_id, file_name, file_type, file_size_bytes, file_url)
                            VALUES (?, ?, ?, ?, ?)
                        """, (acc_id, display_name, file_type, size_bytes, sra_url))
                        file_inserted += 1

                # Also import from flat sra_runs if sra_experiments is empty
                if not ds.get("sra_experiments"):
                    for run in ds.get("sra_runs", []):
                        run_acc = run.get("run_accession", "")
                        if not run_acc:
                            continue
                        sample_name = run.get("sample_name", "")
                        strategy = run.get("library_strategy", "")
                        size_mb = run.get("size_mb", "")
                        download = run.get("download_path", "")
                        run_file_names = run.get("run_file_names", [])

                        display_name = run_file_names[0] if run_file_names else (
                            f"{run_acc}_{sample_name}" if sample_name else run_acc
                        )
                        file_type = strategy if strategy else "FASTQ"

                        try:
                            size_bytes = int(float(size_mb) * 1024 * 1024) if size_mb else None
                        except (ValueError, TypeError):
                            size_bytes = None

                        conn.execute("""
                            INSERT OR IGNORE INTO dataset_files
                            (accession_id, file_name, file_type, file_size_bytes, file_url)
                            VALUES (?, ?, ?, ?, ?)
                        """, (acc_id, display_name, file_type, size_bytes, download))
                        file_inserted += 1

    conn.commit()

    # ========================================================
    # PHASE 5: Cross-link GSE pubmed_ids from geo_details
    # ========================================================
    print(f"\n--- Phase 5: Cross-linking GEO pubmed_ids ---")
    crosslink_inserted = 0
    for gse, detail in geo_details.items():
        pubmed_ids = detail.get("pubmed_ids", [])
        acc_id = acc_id_cache.get(gse)
        if not acc_id:
            continue
        for pmid in pubmed_ids:
            exists = conn.execute("SELECT 1 FROM publications WHERE pmid=?", (pmid,)).fetchone()
            if not exists:
                continue
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO publication_datasets (pmid, accession_id, source)
                    VALUES (?, ?, ?)
                """, (pmid, acc_id, "geo_pubmed_id"))
                crosslink_inserted += 1
            except sqlite3.IntegrityError:
                pass
    conn.commit()
    print(f"  Cross-links from GEO pubmed_ids: {crosslink_inserted}")

    # ========================================================
    # Log and report
    # ========================================================
    conn.execute("""
        INSERT INTO update_log (total_pmids_in_pubmed, new_pmids_added,
                                accessions_found, notes)
        VALUES (?, 0, ?, ?)
    """, (
        metadata.get("total_pmids", 0),
        gse_inserted + acc_inserted,
        (f"Full import from {os.path.basename(json_path)}: "
         f"{gse_inserted} GSE, {srx_inserted} SRX, {srr_inserted} SRR, "
         f"{acc_inserted} text-mined accessions, "
         f"{potential_link_inserted} potentially related links, "
         f"{link_inserted} total pub-dataset links, {file_inserted} dataset_files, "
         f"{crosslink_inserted} GEO cross-links"),
    ))
    conn.commit()

    print(f"\n{'=' * 60}")
    print(f"Import complete:")
    print(f"  PMIDs matched in DB:    {pmids_matched}")
    print(f"  PMIDs skipped:          {pmids_skipped}")
    print(f"  GSE series inserted:    {gse_inserted}")
    print(f"  GSE series updated:     {gse_updated}")
    print(f"  SRX experiments:        {srx_inserted}")
    print(f"  SRR runs:               {srr_inserted}")
    print(f"  Text-mined accessions:  {acc_inserted}")
    print(f"  Potential links:        {potential_link_inserted}")
    print(f"  Pub-dataset links:      {link_inserted}")
    print(f"  Dataset files:          {file_inserted}")
    print(f"  GEO cross-links:        {crosslink_inserted}")

    if pmids_skipped > 0 and pub_count == 0:
        print(f"\n  All {pmids_skipped} PMIDs were skipped because the publications table is empty.")
        print("  Run `python update_yeolab_db.py --pubmed-only` first, then re-run this import.")

    # Print summary
    print(f"\nDatabase totals:")
    for table in ['publications', 'dataset_accessions', 'publication_datasets',
                   'dataset_files', 'sra_experiments', 'sra_runs']:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table}: {count}")
        except sqlite3.OperationalError:
            print(f"  {table}: (table not found)")

    # Show SRX stats
    print(f"\nSRA experiment breakdown:")
    for row in conn.execute("""
        SELECT library_strategy, COUNT(*) FROM sra_experiments
        GROUP BY library_strategy ORDER BY COUNT(*) DESC LIMIT 10
    """):
        print(f"  {row[0] or '(unknown)'}: {row[1]}")

    print(f"\nOrganisms in SRX experiments:")
    for row in conn.execute("""
        SELECT organism, COUNT(*) FROM sra_experiments
        GROUP BY organism ORDER BY COUNT(*) DESC LIMIT 5
    """):
        print(f"  {row[0] or '(unknown)'}: {row[1]}")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Import GEO/SRA results into the Yeo Lab publications database.",
        epilog="If the publications table is empty, run `python update_yeolab_db.py --pubmed-only` first."
    )
    parser.add_argument("--db", default="yeolab_publications.db",
                        help="Path to SQLite database (default: yeolab_publications.db)")
    parser.add_argument("--input", default="yeolab_geo_sra_results.json",
                        help="Path to JSON from fetch_geo_sra_metadata.py (default: yeolab_geo_sra_results.json)")
    parser.add_argument("--clear", action="store_true",
                        help="Clear existing dataset tables before importing (clean reimport)")
    args = parser.parse_args()
    import_results(args.db, args.input, clear_first=args.clear)
