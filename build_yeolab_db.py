#!/usr/bin/env python3
"""
Yeo Lab Publications Database Builder
======================================
Builds an SQLite3 database from PubMed metadata JSON files.
Extracts GEO/SRA accessions from abstracts and full text.
Designed for incremental updates.

Usage:
    python3 build_yeolab_db.py --build       # Initial build from JSON files
    python3 build_yeolab_db.py --summary      # Print database summary
"""

import sqlite3
import json
import re
import os
import sys
import hashlib
from datetime import datetime
from pathlib import Path

# ============================================================
# CONFIGURATION
# ============================================================
DB_PATH = "yeolab_publications.db"
METADATA_DIR = "/sessions/quirky-gracious-galileo/mnt/.claude/projects/-sessions-quirky-gracious-galileo/af403e63-782d-4b7a-80f0-14ea4f1e06d3/tool-results"

# Regex patterns for accession extraction
ACCESSION_PATTERNS = {
    # GEO accessions
    'GSE': re.compile(r'\b(GSE\d{3,8})\b'),
    'GSM': re.compile(r'\b(GSM\d{3,8})\b'),
    'GDS': re.compile(r'\b(GDS\d{3,6})\b'),
    'GPL': re.compile(r'\b(GPL\d{2,6})\b'),
    # SRA accessions
    'SRP': re.compile(r'\b(SRP\d{5,9})\b'),
    'SRR': re.compile(r'\b(SRR\d{5,12})\b'),
    'SRX': re.compile(r'\b(SRX\d{5,9})\b'),
    'SRS': re.compile(r'\b(SRS\d{5,9})\b'),
    'PRJNA': re.compile(r'\b(PRJNA\d{3,9})\b'),
    'PRJEB': re.compile(r'\b(PRJEB\d{3,9})\b'),
    # ENA / DDBJ
    'ERP': re.compile(r'\b(ERP\d{5,9})\b'),
    'DRP': re.compile(r'\b(DRP\d{5,9})\b'),
    # ENCODE
    'ENCSR': re.compile(r'\b(ENCSR\d{3}[A-Z]{3})\b'),
    'ENCFF': re.compile(r'\b(ENCFF\d{3}[A-Z]{3})\b'),
    # ArrayExpress
    'E-MTAB': re.compile(r'\b(E-MTAB-\d{3,6})\b'),
    'E-GEOD': re.compile(r'\b(E-GEOD-\d{3,6})\b'),
}

# ============================================================
# DATABASE SCHEMA
# ============================================================
SCHEMA_SQL = """
-- Publications table: core publication metadata
CREATE TABLE IF NOT EXISTS publications (
    pmid TEXT PRIMARY KEY,
    pmc_id TEXT,
    doi TEXT,
    pii TEXT,
    title TEXT NOT NULL,
    abstract TEXT,
    journal_name TEXT,
    journal_iso TEXT,
    pub_date TEXT,         -- ISO format YYYY-MM-DD or YYYY-MM or YYYY
    pub_year INTEGER,
    pub_month INTEGER,
    pub_day INTEGER,
    volume TEXT,
    issue TEXT,
    pages TEXT,
    pub_types TEXT,         -- JSON array of publication types
    mesh_terms TEXT,        -- JSON array of MeSH terms
    keywords TEXT,          -- JSON array of keywords
    language TEXT,
    is_open_access INTEGER DEFAULT 0,
    citation_count INTEGER,
    abstract_word_count INTEGER,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

-- Authors table: individual authors
CREATE TABLE IF NOT EXISTS authors (
    author_id INTEGER PRIMARY KEY AUTOINCREMENT,
    last_name TEXT,
    fore_name TEXT,
    initials TEXT,
    orcid TEXT,
    UNIQUE(last_name, fore_name, initials)
);

-- Publication-Author junction table
CREATE TABLE IF NOT EXISTS publication_authors (
    pmid TEXT NOT NULL,
    author_id INTEGER NOT NULL,
    author_position INTEGER NOT NULL,  -- 1-indexed position in author list
    is_first_author INTEGER DEFAULT 0,
    is_last_author INTEGER DEFAULT 0,
    affiliation TEXT,
    PRIMARY KEY (pmid, author_id, author_position),
    FOREIGN KEY (pmid) REFERENCES publications(pmid),
    FOREIGN KEY (author_id) REFERENCES authors(author_id)
);

-- Affiliations table
CREATE TABLE IF NOT EXISTS affiliations (
    affiliation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    affiliation_text TEXT UNIQUE,
    institution TEXT,
    department TEXT,
    city TEXT,
    state TEXT,
    country TEXT
);

-- Publication-Affiliation junction
CREATE TABLE IF NOT EXISTS publication_affiliations (
    pmid TEXT NOT NULL,
    affiliation_id INTEGER NOT NULL,
    PRIMARY KEY (pmid, affiliation_id),
    FOREIGN KEY (pmid) REFERENCES publications(pmid),
    FOREIGN KEY (affiliation_id) REFERENCES affiliations(affiliation_id)
);

-- GEO/SRA/Dataset accessions found in publications
CREATE TABLE IF NOT EXISTS dataset_accessions (
    accession_id INTEGER PRIMARY KEY AUTOINCREMENT,
    accession TEXT NOT NULL UNIQUE,
    accession_type TEXT NOT NULL,   -- GSE, GSM, SRP, SRR, PRJNA, ENCSR, etc.
    database TEXT NOT NULL,          -- GEO, SRA, ENCODE, ArrayExpress, etc.
    title TEXT,
    organism TEXT,
    platform TEXT,
    summary TEXT,
    overall_design TEXT,
    num_samples INTEGER,
    submission_date TEXT,
    status TEXT,
    supplementary_files TEXT,        -- JSON array of file names/URLs
    created_at TEXT DEFAULT (datetime('now'))
);

-- Publication-Dataset junction (many-to-many)
CREATE TABLE IF NOT EXISTS publication_datasets (
    pmid TEXT NOT NULL,
    accession_id INTEGER NOT NULL,
    source TEXT DEFAULT 'abstract',  -- where the accession was found: abstract, title, full_text, ncbi_link
    PRIMARY KEY (pmid, accession_id),
    FOREIGN KEY (pmid) REFERENCES publications(pmid),
    FOREIGN KEY (accession_id) REFERENCES dataset_accessions(accession_id)
);

-- Original file names associated with datasets
CREATE TABLE IF NOT EXISTS dataset_files (
    file_id INTEGER PRIMARY KEY AUTOINCREMENT,
    accession_id INTEGER NOT NULL,
    file_name TEXT NOT NULL,
    file_type TEXT,           -- FASTQ, BAM, BED, bigWig, etc.
    file_size_bytes INTEGER,
    file_url TEXT,
    md5_checksum TEXT,
    FOREIGN KEY (accession_id) REFERENCES dataset_accessions(accession_id)
);

-- Grants/funding information
CREATE TABLE IF NOT EXISTS grants (
    grant_id INTEGER PRIMARY KEY AUTOINCREMENT,
    grant_number TEXT,
    agency TEXT,
    country TEXT,
    UNIQUE(grant_number, agency)
);

-- Publication-Grant junction
CREATE TABLE IF NOT EXISTS publication_grants (
    pmid TEXT NOT NULL,
    grant_id INTEGER NOT NULL,
    PRIMARY KEY (pmid, grant_id),
    FOREIGN KEY (pmid) REFERENCES publications(pmid),
    FOREIGN KEY (grant_id) REFERENCES grants(grant_id)
);

-- AI-generated summaries
CREATE TABLE IF NOT EXISTS publication_summaries (
    pmid TEXT PRIMARY KEY,
    one_line_summary TEXT,
    key_findings TEXT,
    methods_summary TEXT,
    data_types TEXT,         -- JSON array: eCLIP, RNA-seq, CRISPR, etc.
    model_systems TEXT,      -- JSON array: HepG2, K562, iPSC, mouse, etc.
    generated_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (pmid) REFERENCES publications(pmid)
);

-- Update tracking
CREATE TABLE IF NOT EXISTS update_log (
    update_id INTEGER PRIMARY KEY AUTOINCREMENT,
    update_time TEXT DEFAULT (datetime('now')),
    total_pmids_in_pubmed INTEGER,
    new_pmids_added INTEGER,
    metadata_updated INTEGER,
    accessions_found INTEGER,
    notes TEXT
);

-- Useful indices
CREATE INDEX IF NOT EXISTS idx_pub_year ON publications(pub_year);
CREATE INDEX IF NOT EXISTS idx_pub_doi ON publications(doi);
CREATE INDEX IF NOT EXISTS idx_pub_pmc ON publications(pmc_id);
CREATE INDEX IF NOT EXISTS idx_author_name ON authors(last_name, fore_name);
CREATE INDEX IF NOT EXISTS idx_pubauth_pmid ON publication_authors(pmid);
CREATE INDEX IF NOT EXISTS idx_pubauth_author ON publication_authors(author_id);
CREATE INDEX IF NOT EXISTS idx_dataset_type ON dataset_accessions(accession_type);
CREATE INDEX IF NOT EXISTS idx_dataset_db ON dataset_accessions(database);
CREATE INDEX IF NOT EXISTS idx_pubdata_pmid ON publication_datasets(pmid);
CREATE INDEX IF NOT EXISTS idx_pubdata_acc ON publication_datasets(accession_id);
CREATE INDEX IF NOT EXISTS idx_files_acc ON dataset_files(accession_id);
CREATE INDEX IF NOT EXISTS idx_grants_pmid ON publication_grants(pmid);

-- Full-text search virtual table
CREATE VIRTUAL TABLE IF NOT EXISTS publications_fts USING fts5(
    pmid, title, abstract, journal_name,
    content='publications',
    content_rowid='rowid'
);

-- Triggers to keep FTS in sync
CREATE TRIGGER IF NOT EXISTS publications_ai AFTER INSERT ON publications BEGIN
    INSERT INTO publications_fts(rowid, pmid, title, abstract, journal_name)
    VALUES (new.rowid, new.pmid, new.title, new.abstract, new.journal_name);
END;

CREATE TRIGGER IF NOT EXISTS publications_ad AFTER DELETE ON publications BEGIN
    INSERT INTO publications_fts(publications_fts, rowid, pmid, title, abstract, journal_name)
    VALUES ('delete', old.rowid, old.pmid, old.title, old.abstract, old.journal_name);
END;

CREATE TRIGGER IF NOT EXISTS publications_au AFTER UPDATE ON publications BEGIN
    INSERT INTO publications_fts(publications_fts, rowid, pmid, title, abstract, journal_name)
    VALUES ('delete', old.rowid, old.pmid, old.title, old.abstract, old.journal_name);
    INSERT INTO publications_fts(rowid, pmid, title, abstract, journal_name)
    VALUES (new.rowid, new.pmid, new.title, new.abstract, new.journal_name);
END;
"""

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def classify_database(accession_type):
    """Classify which database an accession belongs to."""
    mapping = {
        'GSE': 'GEO', 'GSM': 'GEO', 'GDS': 'GEO', 'GPL': 'GEO',
        'SRP': 'SRA', 'SRR': 'SRA', 'SRX': 'SRA', 'SRS': 'SRA',
        'PRJNA': 'BioProject', 'PRJEB': 'BioProject',
        'ERP': 'ENA', 'DRP': 'DDBJ',
        'ENCSR': 'ENCODE', 'ENCFF': 'ENCODE',
        'E-MTAB': 'ArrayExpress', 'E-GEOD': 'ArrayExpress',
    }
    return mapping.get(accession_type, 'Unknown')


def extract_accessions(text):
    """Extract all dataset accessions from text."""
    if not text:
        return []
    results = []
    for acc_type, pattern in ACCESSION_PATTERNS.items():
        matches = pattern.findall(text)
        for m in set(matches):
            results.append({
                'accession': m,
                'type': acc_type,
                'database': classify_database(acc_type),
            })
    return results


def safe_int(val):
    """Safely convert a value to int."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def parse_pub_date(article):
    """Extract publication date from article metadata."""
    pub_date = article.get('publication_date', {})
    if isinstance(pub_date, dict):
        year = safe_int(pub_date.get('year'))
        month = safe_int(pub_date.get('month'))
        day = safe_int(pub_date.get('day'))
    elif isinstance(pub_date, str):
        parts = pub_date.replace('/', '-').split('-')
        year = safe_int(parts[0]) if len(parts) > 0 else None
        month = safe_int(parts[1]) if len(parts) > 1 else None
        day = safe_int(parts[2]) if len(parts) > 2 else None
    else:
        year = month = day = None

    # Build ISO date string
    date_str = None
    if year:
        date_str = str(year)
        if month:
            date_str += f"-{int(month):02d}"
            if day:
                date_str += f"-{int(day):02d}"

    return date_str, year, month, day


def parse_affiliation(aff_text):
    """Attempt to parse institution/department from affiliation string."""
    if not aff_text:
        return {}
    result = {'text': aff_text}
    # Try to extract country (last part after comma often)
    parts = [p.strip().rstrip('.') for p in aff_text.split(',')]
    if len(parts) > 1:
        result['country'] = parts[-1]
    return result


def load_metadata_files(metadata_dir):
    """Load all JSON metadata files from the tool-results directory."""
    articles = []
    seen_pmids = set()

    for fname in sorted(os.listdir(metadata_dir)):
        if not fname.endswith('.txt'):
            continue
        fpath = os.path.join(metadata_dir, fname)
        try:
            with open(fpath, 'r') as f:
                data = json.load(f)
            if 'articles' in data:
                for art in data['articles']:
                    pmid = art.get('identifiers', {}).get('pmid', '')
                    if pmid and pmid not in seen_pmids:
                        seen_pmids.add(pmid)
                        articles.append(art)
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            print(f"  Warning: Could not parse {fname}: {e}")
            continue

    return articles


# ============================================================
# DATABASE OPERATIONS
# ============================================================

def create_database(db_path):
    """Create or initialize the database with schema."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    return conn


def insert_publication(conn, article):
    """Insert a single publication into the database."""
    ids = article.get('identifiers', {})
    pmid = ids.get('pmid', '')
    if not pmid:
        return None

    # Check if already exists
    existing = conn.execute("SELECT pmid FROM publications WHERE pmid = ?", (pmid,)).fetchone()

    title = article.get('title', '')
    abstract = article.get('abstract', '')
    journal = article.get('journal', {})
    journal_name = journal.get('title', '') if isinstance(journal, dict) else str(journal)
    journal_iso = journal.get('iso_abbreviation', '') if isinstance(journal, dict) else ''

    date_str, year, month, day = parse_pub_date(article)

    # Publication types
    pub_types = article.get('publication_types', [])
    if isinstance(pub_types, list):
        pub_types_json = json.dumps(pub_types)
    else:
        pub_types_json = json.dumps([])

    # MeSH terms
    mesh = article.get('mesh_terms', [])
    if isinstance(mesh, list):
        mesh_json = json.dumps(mesh)
    else:
        mesh_json = json.dumps([])

    # Keywords
    keywords = article.get('keywords', [])
    if isinstance(keywords, list):
        keywords_json = json.dumps(keywords)
    else:
        keywords_json = json.dumps([])

    abstract_wc = len(abstract.split()) if abstract else 0

    if existing:
        conn.execute("""
            UPDATE publications SET
                pmc_id=?, doi=?, pii=?, title=?, abstract=?,
                journal_name=?, journal_iso=?, pub_date=?,
                pub_year=?, pub_month=?, pub_day=?,
                volume=?, issue=?, pages=?,
                pub_types=?, mesh_terms=?, keywords=?,
                language=?, abstract_word_count=?,
                updated_at=datetime('now')
            WHERE pmid=?
        """, (
            ids.get('pmc', ''), ids.get('doi', ''), ids.get('pii', ''),
            title, abstract, journal_name, journal_iso,
            date_str, year, month, day,
            article.get('volume', ''), article.get('issue', ''), article.get('pages', ''),
            pub_types_json, mesh_json, keywords_json,
            article.get('language', 'eng'), abstract_wc,
            pmid
        ))
    else:
        conn.execute("""
            INSERT INTO publications (
                pmid, pmc_id, doi, pii, title, abstract,
                journal_name, journal_iso, pub_date,
                pub_year, pub_month, pub_day,
                volume, issue, pages,
                pub_types, mesh_terms, keywords,
                language, abstract_word_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            pmid, ids.get('pmc', ''), ids.get('doi', ''), ids.get('pii', ''),
            title, abstract, journal_name, journal_iso,
            date_str, year, month, day,
            article.get('volume', ''), article.get('issue', ''), article.get('pages', ''),
            pub_types_json, mesh_json, keywords_json,
            article.get('language', 'eng'), abstract_wc
        ))

    return pmid


def insert_authors(conn, pmid, article):
    """Insert authors for a publication."""
    authors_list = article.get('authors', [])
    if not isinstance(authors_list, list):
        return

    num_authors = len(authors_list)
    for i, author in enumerate(authors_list):
        last_name = author.get('last_name', '')
        fore_name = author.get('fore_name', '')
        initials = author.get('initials', '')

        if not last_name:
            continue

        # Insert or get author
        conn.execute("""
            INSERT OR IGNORE INTO authors (last_name, fore_name, initials)
            VALUES (?, ?, ?)
        """, (last_name, fore_name, initials))

        author_row = conn.execute("""
            SELECT author_id FROM authors
            WHERE last_name=? AND fore_name=? AND initials=?
        """, (last_name, fore_name, initials)).fetchone()

        if author_row:
            author_id = author_row[0]
            # Get affiliation
            affiliations = author.get('affiliations', [])
            aff_text = affiliations[0] if affiliations else ''

            conn.execute("""
                INSERT OR REPLACE INTO publication_authors
                (pmid, author_id, author_position, is_first_author, is_last_author, affiliation)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                pmid, author_id, i + 1,
                1 if i == 0 else 0,
                1 if i == num_authors - 1 else 0,
                aff_text
            ))

            # Insert unique affiliations
            if aff_text:
                conn.execute("""
                    INSERT OR IGNORE INTO affiliations (affiliation_text)
                    VALUES (?)
                """, (aff_text,))
                aff_row = conn.execute(
                    "SELECT affiliation_id FROM affiliations WHERE affiliation_text=?",
                    (aff_text,)
                ).fetchone()
                if aff_row:
                    conn.execute("""
                        INSERT OR IGNORE INTO publication_affiliations (pmid, affiliation_id)
                        VALUES (?, ?)
                    """, (pmid, aff_row[0]))


def insert_grants(conn, pmid, article):
    """Insert grant/funding information."""
    grants = article.get('grants', [])
    if not isinstance(grants, list):
        return
    for grant in grants:
        grant_num = grant.get('grant_id', '') or grant.get('id', '')
        agency = grant.get('agency', '')
        country = grant.get('country', '')
        if grant_num or agency:
            conn.execute("""
                INSERT OR IGNORE INTO grants (grant_number, agency, country)
                VALUES (?, ?, ?)
            """, (grant_num, agency, country))
            row = conn.execute("""
                SELECT grant_id FROM grants WHERE grant_number=? AND agency=?
            """, (grant_num, agency)).fetchone()
            if row:
                conn.execute("""
                    INSERT OR IGNORE INTO publication_grants (pmid, grant_id)
                    VALUES (?, ?)
                """, (pmid, row[0]))


def extract_and_insert_accessions(conn, pmid, article):
    """Extract dataset accessions from abstract and title, insert into DB."""
    title = article.get('title', '') or ''
    abstract = article.get('abstract', '') or ''
    combined_text = f"{title} {abstract}"

    accessions = extract_accessions(combined_text)
    for acc in accessions:
        conn.execute("""
            INSERT OR IGNORE INTO dataset_accessions (accession, accession_type, database)
            VALUES (?, ?, ?)
        """, (acc['accession'], acc['type'], acc['database']))

        row = conn.execute(
            "SELECT accession_id FROM dataset_accessions WHERE accession=?",
            (acc['accession'],)
        ).fetchone()

        if row:
            source = 'title' if acc['accession'] in title else 'abstract'
            conn.execute("""
                INSERT OR IGNORE INTO publication_datasets (pmid, accession_id, source)
                VALUES (?, ?, ?)
            """, (pmid, row[0], source))


# ============================================================
# MAIN BUILD LOGIC
# ============================================================

def build_database(db_path, metadata_dir):
    """Main function to build the database from metadata files."""
    print(f"{'='*60}")
    print(f"YEO LAB PUBLICATIONS DATABASE BUILDER")
    print(f"{'='*60}")
    print(f"Database: {db_path}")
    print(f"Metadata dir: {metadata_dir}")
    print()

    # Step 1: Create database
    print("[1/5] Creating database schema...")
    conn = create_database(db_path)
    print("  ✓ Schema created with 12 tables, FTS5 search, and indices")

    # Step 2: Load metadata
    print("[2/5] Loading metadata from JSON files...")
    articles = load_metadata_files(metadata_dir)
    print(f"  ✓ Loaded {len(articles)} unique articles")

    # Step 3: Insert publications
    print("[3/5] Inserting publications, authors, grants...")
    inserted = 0
    accession_count = 0
    for art in articles:
        pmid = insert_publication(conn, art)
        if pmid:
            insert_authors(conn, pmid, art)
            insert_grants(conn, pmid, art)
            extract_and_insert_accessions(conn, pmid, art)
            inserted += 1
    conn.commit()
    print(f"  ✓ Inserted/updated {inserted} publications")

    # Step 4: Count accessions
    acc_count = conn.execute("SELECT COUNT(*) FROM dataset_accessions").fetchone()[0]
    link_count = conn.execute("SELECT COUNT(*) FROM publication_datasets").fetchone()[0]
    print(f"  ✓ Found {acc_count} unique dataset accessions across {link_count} publication-dataset links")

    # Step 5: Log update
    conn.execute("""
        INSERT INTO update_log (total_pmids_in_pubmed, new_pmids_added, accessions_found, notes)
        VALUES (?, ?, ?, ?)
    """, (308, inserted, acc_count, f"Initial build from {len(articles)} metadata files"))
    conn.commit()

    print(f"\n[4/5] Database statistics:")
    print_summary(conn)

    print(f"\n[5/5] Integrity checks...")
    run_integrity_checks(conn)

    conn.close()
    print(f"\n{'='*60}")
    print(f"Database saved to: {db_path}")
    print(f"{'='*60}")


def print_summary(conn):
    """Print database summary statistics."""
    stats = {
        'Publications': conn.execute("SELECT COUNT(*) FROM publications").fetchone()[0],
        'Unique Authors': conn.execute("SELECT COUNT(*) FROM authors").fetchone()[0],
        'Author-Paper Links': conn.execute("SELECT COUNT(*) FROM publication_authors").fetchone()[0],
        'Unique Affiliations': conn.execute("SELECT COUNT(*) FROM affiliations").fetchone()[0],
        'Dataset Accessions': conn.execute("SELECT COUNT(*) FROM dataset_accessions").fetchone()[0],
        'Pub-Dataset Links': conn.execute("SELECT COUNT(*) FROM publication_datasets").fetchone()[0],
        'Grants': conn.execute("SELECT COUNT(*) FROM grants").fetchone()[0],
    }
    for k, v in stats.items():
        print(f"  {k}: {v}")

    # Year distribution
    print("\n  Publications by year:")
    rows = conn.execute("""
        SELECT pub_year, COUNT(*) as cnt
        FROM publications
        WHERE pub_year IS NOT NULL
        GROUP BY pub_year ORDER BY pub_year
    """).fetchall()
    for year, cnt in rows:
        bar = '█' * cnt
        print(f"    {year}: {bar} ({cnt})")

    # Top journals
    print("\n  Top 10 journals:")
    rows = conn.execute("""
        SELECT journal_name, COUNT(*) as cnt
        FROM publications
        WHERE journal_name != ''
        GROUP BY journal_name ORDER BY cnt DESC LIMIT 10
    """).fetchall()
    for j, cnt in rows:
        print(f"    {cnt:3d} | {j[:70]}")

    # Accession types
    print("\n  Dataset accessions by type:")
    rows = conn.execute("""
        SELECT accession_type, database, COUNT(*) as cnt
        FROM dataset_accessions
        GROUP BY accession_type, database ORDER BY cnt DESC
    """).fetchall()
    for atype, db, cnt in rows:
        print(f"    {atype:8s} ({db:12s}): {cnt}")


def run_integrity_checks(conn):
    """Run integrity and consistency checks."""
    checks_passed = 0
    checks_total = 0

    # Check 1: No duplicate PMIDs
    checks_total += 1
    dup = conn.execute("""
        SELECT pmid, COUNT(*) FROM publications GROUP BY pmid HAVING COUNT(*) > 1
    """).fetchall()
    if not dup:
        print("  ✓ No duplicate PMIDs")
        checks_passed += 1
    else:
        print(f"  ✗ Found {len(dup)} duplicate PMIDs!")

    # Check 2: All publications have titles
    checks_total += 1
    no_title = conn.execute("SELECT COUNT(*) FROM publications WHERE title IS NULL OR title = ''").fetchone()[0]
    if no_title == 0:
        print("  ✓ All publications have titles")
        checks_passed += 1
    else:
        print(f"  ✗ {no_title} publications missing titles")

    # Check 3: Publication-author referential integrity
    checks_total += 1
    orphan_pa = conn.execute("""
        SELECT COUNT(*) FROM publication_authors pa
        LEFT JOIN publications p ON pa.pmid = p.pmid
        WHERE p.pmid IS NULL
    """).fetchone()[0]
    if orphan_pa == 0:
        print("  ✓ No orphaned publication_authors records")
        checks_passed += 1
    else:
        print(f"  ✗ {orphan_pa} orphaned publication_authors records")

    # Check 4: Dataset accession format validation
    checks_total += 1
    bad_acc = conn.execute("""
        SELECT COUNT(*) FROM dataset_accessions
        WHERE accession NOT GLOB '[A-Z]*[0-9]*'
          AND accession NOT LIKE 'E-%'
    """).fetchone()[0]
    if bad_acc == 0:
        print("  ✓ All accessions match expected format")
        checks_passed += 1
    else:
        print(f"  ⚠ {bad_acc} accessions with unexpected format")

    # Check 5: Year range sanity
    checks_total += 1
    year_range = conn.execute("""
        SELECT MIN(pub_year), MAX(pub_year) FROM publications WHERE pub_year IS NOT NULL
    """).fetchone()
    if year_range[0] and year_range[0] >= 2000 and year_range[1] <= 2026:
        print(f"  ✓ Year range {year_range[0]}-{year_range[1]} is plausible for Yeo lab")
        checks_passed += 1
    else:
        print(f"  ⚠ Year range {year_range[0]}-{year_range[1]} may include non-Yeo-lab papers")

    print(f"\n  Integrity: {checks_passed}/{checks_total} checks passed")


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Yeo Lab Publications Database')
    parser.add_argument('--build', action='store_true', help='Build database from JSON files')
    parser.add_argument('--summary', action='store_true', help='Print database summary')
    parser.add_argument('--db', default=DB_PATH, help='Database path')
    args = parser.parse_args()

    if args.build:
        build_database(args.db, METADATA_DIR)
    elif args.summary:
        conn = sqlite3.connect(args.db)
        print_summary(conn)
        conn.close()
    else:
        # Default: build
        build_database(args.db, METADATA_DIR)
