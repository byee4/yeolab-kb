#!/usr/bin/env python3
"""
Yeo Lab Publications Database - Full Update Script
====================================================
This is the master script for updating the publications database.
Run it periodically to pull new publications and associated datasets.

Requirements:
    pip install biopython requests

Usage:
    python update_yeolab_db.py                    # Full update
    python update_yeolab_db.py --pubmed-only       # Only update PubMed metadata
    python update_yeolab_db.py --geo-only           # Only update GEO/SRA data
    python update_yeolab_db.py --summary            # Print database summary
    python update_yeolab_db.py --search "eCLIP"     # Full-text search

Environment:
    NCBI_API_KEY  - Optional NCBI API key for faster queries (10/sec vs 3/sec)
                    Get one free at: https://www.ncbi.nlm.nih.gov/account/settings/
"""

import sqlite3
import json
import re
import time
import sys
import os
import argparse
import xml.etree.ElementTree as ET
from datetime import datetime
from collections import defaultdict

try:
    from Bio import Entrez
except ImportError:
    print("ERROR: Install biopython: pip install biopython")
    sys.exit(1)

try:
    import requests
except ImportError:
    print("ERROR: Install requests: pip install requests")
    sys.exit(1)

# ============================================================
# CONFIGURATION
# ============================================================
Entrez.email = "brian.alan.yee@gmail.com"
Entrez.api_key = os.environ.get("NCBI_API_KEY", None)
RATE_LIMIT = 0.34 if Entrez.api_key else 0.5

DB_PATH = "yeolab_publications.db"
AUTHOR_QUERY = "Yeo GW[Author]"

ACCESSION_PATTERNS = {
    'GSE': re.compile(r'\b(GSE\d{3,8})\b'),
    'GSM': re.compile(r'\b(GSM\d{3,8})\b'),
    'GDS': re.compile(r'\b(GDS\d{3,6})\b'),
    'GPL': re.compile(r'\b(GPL\d{2,6})\b'),
    'SRP': re.compile(r'\b(SRP\d{5,9})\b'),
    'SRR': re.compile(r'\b(SRR\d{5,12})\b'),
    'SRX': re.compile(r'\b(SRX\d{5,9})\b'),
    'SRS': re.compile(r'\b(SRS\d{5,9})\b'),
    'PRJNA': re.compile(r'\b(PRJNA\d{3,9})\b'),
    'PRJEB': re.compile(r'\b(PRJEB\d{3,9})\b'),
    'ENCSR': re.compile(r'\b(ENCSR\d{3}[A-Z]{3})\b'),
    'ENCFF': re.compile(r'\b(ENCFF\d{3}[A-Z]{3})\b'),
    'E-MTAB': re.compile(r'\b(E-MTAB-\d{3,6})\b'),
    'MASSIVE': re.compile(r'\b(MSV\d{6,9})\b'),
    'PXD': re.compile(r'\b(PXD\d{5,9})\b'),
}

DB_MAP = {
    'GSE': 'GEO', 'GSM': 'GEO', 'GDS': 'GEO', 'GPL': 'GEO',
    'SRP': 'SRA', 'SRR': 'SRA', 'SRX': 'SRA', 'SRS': 'SRA',
    'PRJNA': 'BioProject', 'PRJEB': 'BioProject',
    'ENCSR': 'ENCODE', 'ENCFF': 'ENCODE',
    'E-MTAB': 'ArrayExpress', 'MASSIVE': 'MassIVE', 'PXD': 'PRIDE',
}


def rate_limit():
    time.sleep(RATE_LIMIT)


# ============================================================
# SCHEMA (same as build_yeolab_db.py)
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
    supplementary_files TEXT, last_update_date TEXT,
    contact_name TEXT, contact_institute TEXT,
    experiment_types TEXT, relations TEXT,
    sample_ids TEXT, citation_pmids TEXT,
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
"""

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
    """Create or open the database."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA_SQL)
    existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(dataset_accessions)")}
    for col_name, col_type in DATASET_ACCESSIONS_NEW_COLS:
        if col_name not in existing_cols:
            conn.execute(f"ALTER TABLE dataset_accessions ADD COLUMN {col_name} {col_type}")
    # Backward-compat migration: keep old full-text links, but mark as potential.
    conn.execute("""
        UPDATE publication_datasets
        SET source='potentially_related_dataset'
        WHERE source='pmc_full_text'
    """)
    conn.commit()
    return conn


# ============================================================
# PUBMED UPDATE
# ============================================================
def safe_int(val):
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def update_pubmed(conn):
    """Fetch all PMIDs and update metadata for new/changed publications."""
    print("\n" + "=" * 60)
    print("UPDATING PUBMED METADATA")
    print("=" * 60)

    # Get current PMIDs
    handle = Entrez.esearch(db="pubmed", term=AUTHOR_QUERY, retmax=1000, sort="pub_date")
    record = Entrez.read(handle)
    handle.close()
    rate_limit()

    all_pmids = record["IdList"]
    total_in_pubmed = int(record["Count"])
    print(f"PubMed reports {total_in_pubmed} results, retrieved {len(all_pmids)} PMIDs")

    # Find new PMIDs
    existing = set(r[0] for r in conn.execute("SELECT pmid FROM publications").fetchall())
    new_pmids = [p for p in all_pmids if p not in existing]
    print(f"Existing in DB: {len(existing)}, New: {len(new_pmids)}")

    # Fetch metadata for new PMIDs
    if new_pmids:
        print(f"Fetching metadata for {len(new_pmids)} new publications...")
        for i in range(0, len(new_pmids), 20):
            batch = new_pmids[i:i + 20]
            try:
                handle = Entrez.efetch(db="pubmed", id=batch, rettype="xml", retmode="xml")
                records = Entrez.read(handle)
                handle.close()
                rate_limit()

                articles = records.get("PubmedArticle", [])
                for article in articles:
                    insert_pubmed_article(conn, article)
            except Exception as e:
                print(f"  Warning: Batch fetch failed: {e}")
                rate_limit()

        conn.commit()
        print(f"  Inserted {len(new_pmids)} new publications")

    # Log update
    conn.execute("""
        INSERT INTO update_log (total_pmids_in_pubmed, new_pmids_added, notes)
        VALUES (?, ?, ?)
    """, (total_in_pubmed, len(new_pmids),
          f"PubMed update: {len(new_pmids)} new papers"))
    conn.commit()

    return all_pmids


def insert_pubmed_article(conn, article_data):
    """Parse and insert a PubmedArticle XML record."""
    try:
        medline = article_data.get("MedlineCitation", {})
        pmid = str(medline.get("PMID", ""))
        if not pmid:
            return

        article = medline.get("Article", {})
        title = str(article.get("ArticleTitle", ""))
        abstract_parts = article.get("Abstract", {}).get("AbstractText", [])
        abstract = " ".join(str(p) for p in abstract_parts)

        journal = article.get("Journal", {})
        journal_name = journal.get("Title", "")
        journal_iso = journal.get("ISOAbbreviation", "")

        # Date
        journal_issue = journal.get("JournalIssue", {})
        pub_date = journal_issue.get("PubDate", {})
        year = safe_int(pub_date.get("Year"))
        month_str = pub_date.get("Month", "")
        day = safe_int(pub_date.get("Day"))

        # Convert month name to number
        month_map = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
                     "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}
        month = month_map.get(month_str, safe_int(month_str))

        date_str = None
        if year:
            date_str = str(year)
            if month:
                date_str += f"-{month:02d}"
                if day:
                    date_str += f"-{day:02d}"

        volume = journal_issue.get("Volume", "")
        issue = journal_issue.get("Issue", "")

        pagination = article.get("Pagination", {})
        pages = pagination.get("MedlinePgn", "") if isinstance(pagination, dict) else ""

        # IDs
        pmc_id = ""
        doi = ""
        pii = ""
        for aid in article_data.get("PubmedData", {}).get("ArticleIdList", []):
            id_type = aid.attributes.get("IdType", "") if hasattr(aid, 'attributes') else ""
            if id_type == "pmc":
                pmc_id = str(aid)
            elif id_type == "doi":
                doi = str(aid)
            elif id_type == "pii":
                pii = str(aid)

        # Pub types
        pub_types = []
        for pt in article.get("PublicationTypeList", []):
            pub_types.append(str(pt))

        # MeSH
        mesh_terms = []
        for mh in medline.get("MeshHeadingList", []):
            desc = mh.get("DescriptorName", "")
            mesh_terms.append(str(desc))

        # Keywords
        keywords = []
        for kw_list in medline.get("KeywordList", []):
            for kw in kw_list:
                keywords.append(str(kw))

        abstract_wc = len(abstract.split()) if abstract else 0

        conn.execute("""
            INSERT OR REPLACE INTO publications
            (pmid, pmc_id, doi, pii, title, abstract,
             journal_name, journal_iso, pub_date, pub_year, pub_month, pub_day,
             volume, issue, pages, pub_types, mesh_terms, keywords,
             language, abstract_word_count, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    datetime('now'))
        """, (
            pmid, pmc_id, doi, pii, title, abstract,
            journal_name, journal_iso, date_str, year, month, day,
            volume, issue, pages,
            json.dumps(pub_types), json.dumps(mesh_terms), json.dumps(keywords),
            article.get("Language", ["eng"])[0] if article.get("Language") else "eng",
            abstract_wc
        ))

        # Authors
        author_list = article.get("AuthorList", [])
        num_authors = len(author_list)
        for pos, author in enumerate(author_list):
            last = author.get("LastName", "")
            fore = author.get("ForeName", "")
            initials = author.get("Initials", "")
            if not last:
                continue

            conn.execute("INSERT OR IGNORE INTO authors (last_name, fore_name, initials) VALUES (?, ?, ?)",
                         (last, fore, initials))
            row = conn.execute(
                "SELECT author_id FROM authors WHERE last_name=? AND fore_name=? AND initials=?",
                (last, fore, initials)).fetchone()
            if row:
                affs = author.get("AffiliationInfo", [])
                aff_text = affs[0].get("Affiliation", "") if affs else ""
                conn.execute("""
                    INSERT OR REPLACE INTO publication_authors
                    (pmid, author_id, author_position, is_first_author, is_last_author, affiliation)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (pmid, row[0], pos + 1, 1 if pos == 0 else 0,
                      1 if pos == num_authors - 1 else 0, aff_text))

        # Grants
        for grant in article.get("GrantList", []):
            gid = grant.get("GrantID", "")
            agency = grant.get("Agency", "")
            country = grant.get("Country", "")
            if gid or agency:
                conn.execute("INSERT OR IGNORE INTO grants (grant_number, agency, country) VALUES (?, ?, ?)",
                             (gid, agency, country))
                grow = conn.execute("SELECT grant_id FROM grants WHERE grant_number=? AND agency=?",
                                    (gid, agency)).fetchone()
                if grow:
                    conn.execute("INSERT OR IGNORE INTO publication_grants (pmid, grant_id) VALUES (?, ?)",
                                 (pmid, grow[0]))

    except Exception as e:
        print(f"  Warning: Failed to parse article: {e}")


# ============================================================
# GEO/SRA UPDATE
# ============================================================
def update_geo_sra(conn, pmids):
    """Find and update GEO/SRA datasets linked to publications."""
    print("\n" + "=" * 60)
    print("UPDATING GEO/SRA DATASETS")
    print("=" * 60)

    # ELink: pubmed -> gds
    print("Finding GEO links via NCBI ELink...")
    pmid_to_gse = defaultdict(set)
    batch_size = 50

    for i in range(0, len(pmids), batch_size):
        batch = pmids[i:i + batch_size]
        pct = min((i + batch_size), len(pmids)) / len(pmids) * 100
        print(f"  ELink batch {i // batch_size + 1} ({pct:.0f}%)...", end="\r")

        try:
            handle = Entrez.elink(dbfrom="pubmed", db="gds", id=batch, linkname="pubmed_gds")
            records = Entrez.read(handle)
            handle.close()
            rate_limit()

            for rec in records:
                pmid = rec["IdList"][0] if rec["IdList"] else None
                if not pmid:
                    continue
                gds_ids = []
                for linkset in rec.get("LinkSetDb", []):
                    for link in linkset.get("Link", []):
                        gds_ids.append(link["Id"])

                if gds_ids:
                    # Get GSE accessions from GDS summaries
                    try:
                        handle2 = Entrez.esummary(db="gds", id=",".join(gds_ids[:100]))
                        summaries = Entrez.read(handle2)
                        handle2.close()
                        rate_limit()

                        for s in summaries:
                            gse_num = s.get("GSE", "")
                            acc = s.get("Accession", "")
                            if gse_num:
                                pmid_to_gse[pmid].add(f"GSE{gse_num}")
                            elif acc.startswith("GSE"):
                                pmid_to_gse[pmid].add(acc)
                    except Exception:
                        rate_limit()
        except Exception as e:
            print(f"\n  Warning: ELink failed: {e}")
            rate_limit()

    all_gse = set()
    for gse_set in pmid_to_gse.values():
        all_gse.update(gse_set)
    print(f"\n  Found {len(all_gse)} unique GSE series from {len(pmid_to_gse)} PMIDs")

    # Fetch GEO series details
    for gse in all_gse:
        existing = conn.execute(
            "SELECT accession_id FROM dataset_accessions WHERE accession=?", (gse,)
        ).fetchone()
        if existing:
            continue  # Already have this one

        try:
            url = f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={gse}&targ=self&form=text&view=brief"
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200:
                detail = parse_geo_soft(resp.text, gse)
                conn.execute("""
                    INSERT OR IGNORE INTO dataset_accessions
                    (accession, accession_type, database, title, organism, platform,
                     summary, overall_design, num_samples, supplementary_files,
                     submission_date, last_update_date, status, contact_name,
                     contact_institute, experiment_types, relations, sample_ids, citation_pmids)
                    VALUES (?, 'GSE', 'GEO', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    gse, detail.get("title", ""),
                    ", ".join(detail.get("organisms", [])),
                    ", ".join(detail.get("platforms", [])),
                    detail.get("summary", ""),
                    detail.get("overall_design", ""),
                    detail.get("n_samples", 0),
                    json.dumps(detail.get("supplementary_files", [])),
                    detail.get("submission_date", ""),
                    detail.get("last_update_date", ""),
                    detail.get("status", ""),
                    detail.get("contact_name", ""),
                    detail.get("contact_institute", ""),
                    json.dumps(detail.get("types", [])),
                    json.dumps(detail.get("relations", [])),
                    json.dumps(detail.get("samples", [])),
                    json.dumps(detail.get("pubmed_ids", [])),
                ))
                conn.execute("""
                    UPDATE dataset_accessions SET
                        title=COALESCE(NULLIF(?, ''), title),
                        organism=COALESCE(NULLIF(?, ''), organism),
                        platform=COALESCE(NULLIF(?, ''), platform),
                        summary=COALESCE(NULLIF(?, ''), summary),
                        overall_design=COALESCE(NULLIF(?, ''), overall_design),
                        num_samples=COALESCE(?, num_samples),
                        supplementary_files=COALESCE(?, supplementary_files),
                        submission_date=COALESCE(NULLIF(?, ''), submission_date),
                        last_update_date=COALESCE(NULLIF(?, ''), last_update_date),
                        status=COALESCE(NULLIF(?, ''), status),
                        contact_name=COALESCE(NULLIF(?, ''), contact_name),
                        contact_institute=COALESCE(NULLIF(?, ''), contact_institute),
                        experiment_types=COALESCE(?, experiment_types),
                        relations=COALESCE(?, relations),
                        sample_ids=COALESCE(?, sample_ids),
                        citation_pmids=COALESCE(?, citation_pmids)
                    WHERE accession=?
                """, (
                    detail.get("title", ""),
                    ", ".join(detail.get("organisms", [])),
                    ", ".join(detail.get("platforms", [])),
                    detail.get("summary", ""),
                    detail.get("overall_design", ""),
                    detail.get("n_samples", 0) or None,
                    json.dumps(detail.get("supplementary_files", [])),
                    detail.get("submission_date", ""),
                    detail.get("last_update_date", ""),
                    detail.get("status", ""),
                    detail.get("contact_name", ""),
                    detail.get("contact_institute", ""),
                    json.dumps(detail.get("types", [])),
                    json.dumps(detail.get("relations", [])),
                    json.dumps(detail.get("samples", [])),
                    json.dumps(detail.get("pubmed_ids", [])),
                    gse,
                ))

                # Get SRA runs for this GSE
                fetch_sra_runs(conn, gse)

            rate_limit()
        except Exception as e:
            print(f"  Warning: Failed to fetch {gse}: {e}")
            rate_limit()

    # Link GSE to PMIDs
    for pmid, gse_set in pmid_to_gse.items():
        for gse in gse_set:
            acc_row = conn.execute(
                "SELECT accession_id FROM dataset_accessions WHERE accession=?",
                (gse,)
            ).fetchone()
            if acc_row:
                conn.execute("""
                    INSERT OR IGNORE INTO publication_datasets (pmid, accession_id, source)
                    VALUES (?, ?, 'ncbi_elink')
                """, (pmid, acc_row[0]))

    # Scan PMC full text for additional accessions
    scan_pmc_fulltext(conn, pmids)

    conn.commit()
    acc_count = conn.execute("SELECT COUNT(*) FROM dataset_accessions").fetchone()[0]
    link_count = conn.execute("SELECT COUNT(*) FROM publication_datasets").fetchone()[0]
    print(f"\n  Database now has {acc_count} accessions, {link_count} pub-dataset links")


def parse_geo_soft(text, accession):
    """Parse GEO SOFT format."""
    result = {"accession": accession, "samples": [], "supplementary_files": [],
              "organisms": [], "platforms": []}
    for line in text.split("\n"):
        line = line.strip()
        if not line.startswith("!Series_"):
            continue
        if "=" not in line:
            continue
        key = line.split("=")[0].replace("!Series_", "").strip()
        val = "=".join(line.split("=")[1:]).strip()

        if key == "title":
            result["title"] = val
        elif key == "summary":
            result.setdefault("summary", "")
            result["summary"] += val + " "
        elif key == "overall_design":
            result.setdefault("overall_design", "")
            result["overall_design"] += val + " "
        elif key == "sample_id":
            result["samples"].append(val)
        elif key == "platform_id":
            result["platforms"].append(val)
        elif key == "supplementary_file":
            result["supplementary_files"].append(val)
        elif key.startswith("sample_taxid") or key == "sample_organism":
            result.setdefault("organisms", set())
            result["organisms"].add(val)
        elif key == "type":
            result.setdefault("types", [])
            result["types"].append(val)
        elif key == "pubmed_id":
            result.setdefault("pubmed_ids", [])
            result["pubmed_ids"].append(val)
        elif key == "relation":
            result.setdefault("relations", [])
            result["relations"].append(val)
        elif key in ("submission_date", "last_update_date", "status",
                     "contact_name", "contact_institute"):
            result[key] = val

    if "organisms" in result:
        result["organisms"] = list(result["organisms"])
    result["n_samples"] = len(result["samples"])
    return result


def fetch_sra_runs(conn, gse):
    """Fetch SRA run info for a GEO series."""
    try:
        handle = Entrez.esearch(db="sra", term=f"{gse}[All Fields]", retmax=500)
        record = Entrez.read(handle)
        handle.close()
        rate_limit()

        sra_ids = record.get("IdList", [])
        if not sra_ids:
            return

        handle = Entrez.efetch(db="sra", id=sra_ids[:200], rettype="runinfo", retmode="text")
        runinfo_text = handle.read()
        handle.close()
        rate_limit()

        acc_row = conn.execute(
            "SELECT accession_id FROM dataset_accessions WHERE accession=?", (gse,)
        ).fetchone()
        if not acc_row:
            return
        acc_id = acc_row[0]

        lines = runinfo_text.strip().split("\n")
        if len(lines) < 2:
            return
        headers = lines[0].split(",")
        for line in lines[1:]:
            if not line.strip():
                continue
            values = line.split(",")
            if len(values) != len(headers):
                continue
            rec = dict(zip(headers, values))
            run_acc = rec.get("Run", "")
            if run_acc:
                sample_name = rec.get("SampleName", "")
                strategy = rec.get("LibraryStrategy", "FASTQ")
                size_mb = rec.get("size_MB", "")
                download = rec.get("download_path", "")
                try:
                    size_bytes = int(float(size_mb) * 1024 * 1024) if size_mb else None
                except (ValueError, TypeError):
                    size_bytes = None

                file_name = f"{run_acc}_{sample_name}" if sample_name else run_acc
                conn.execute("""
                    INSERT OR IGNORE INTO dataset_files
                    (accession_id, file_name, file_type, file_size_bytes, file_url)
                    VALUES (?, ?, ?, ?, ?)
                """, (acc_id, file_name, strategy, size_bytes, download))
    except Exception:
        rate_limit()


def scan_pmc_fulltext(conn, pmids):
    """Scan PMC full text for potentially related accessions not found via ELink."""
    print("\n  Scanning PMC full text for potentially related accessions...")

    # Get PMC IDs
    pmid_to_pmcid = {}
    for i in range(0, len(pmids), 100):
        batch = pmids[i:i + 100]
        try:
            handle = Entrez.elink(dbfrom="pubmed", db="pmc", id=batch, linkname="pubmed_pmc")
            records = Entrez.read(handle)
            handle.close()
            rate_limit()
            for rec in records:
                pmid = rec["IdList"][0] if rec["IdList"] else None
                if not pmid:
                    continue
                for ls in rec.get("LinkSetDb", []):
                    for link in ls.get("Link", []):
                        pmid_to_pmcid[pmid] = link["Id"]
        except Exception:
            rate_limit()

    print(f"  {len(pmid_to_pmcid)} papers have PMC full text")

    new_accessions = 0
    gse_metadata_cache = {}
    for i, (pmid, pmcid) in enumerate(pmid_to_pmcid.items()):
        if (i + 1) % 20 == 0:
            print(f"  Scanning {i + 1}/{len(pmid_to_pmcid)}...", end="\r")
        try:
            handle = Entrez.efetch(db="pmc", id=pmcid, rettype="xml", retmode="xml")
            xml_bytes = handle.read()
            handle.close()
            rate_limit()

            if isinstance(xml_bytes, bytes):
                text = xml_bytes.decode('utf-8', errors='replace')
            else:
                text = str(xml_bytes)
            clean = re.sub(r'<[^>]+>', ' ', text)

            for acc_type, pattern in ACCESSION_PATTERNS.items():
                for m in set(pattern.findall(clean)):
                    db = DB_MAP.get(acc_type, 'Unknown')
                    conn.execute("""
                        INSERT OR IGNORE INTO dataset_accessions
                        (accession, accession_type, database)
                        VALUES (?, ?, ?)
                    """, (m, acc_type, db))
                    acc_row = conn.execute(
                        "SELECT accession_id FROM dataset_accessions WHERE accession=?",
                        (m,)
                    ).fetchone()
                    if acc_row:
                        if acc_type == "GSE":
                            if m not in gse_metadata_cache:
                                try:
                                    url = f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={m}&targ=self&form=text&view=brief"
                                    resp = requests.get(url, timeout=30)
                                    if resp.status_code == 200:
                                        gse_metadata_cache[m] = parse_geo_soft(resp.text, m)
                                    else:
                                        gse_metadata_cache[m] = None
                                except Exception:
                                    gse_metadata_cache[m] = None
                                rate_limit()
                            detail = gse_metadata_cache.get(m)
                            if detail:
                                conn.execute("""
                                    UPDATE dataset_accessions SET
                                        title=COALESCE(NULLIF(?, ''), title),
                                        organism=COALESCE(NULLIF(?, ''), organism),
                                        platform=COALESCE(NULLIF(?, ''), platform),
                                        summary=COALESCE(NULLIF(?, ''), summary),
                                        overall_design=COALESCE(NULLIF(?, ''), overall_design),
                                        num_samples=COALESCE(?, num_samples),
                                        supplementary_files=COALESCE(?, supplementary_files),
                                        submission_date=COALESCE(NULLIF(?, ''), submission_date),
                                        last_update_date=COALESCE(NULLIF(?, ''), last_update_date),
                                        status=COALESCE(NULLIF(?, ''), status),
                                        contact_name=COALESCE(NULLIF(?, ''), contact_name),
                                        contact_institute=COALESCE(NULLIF(?, ''), contact_institute),
                                        experiment_types=COALESCE(?, experiment_types),
                                        relations=COALESCE(?, relations),
                                        sample_ids=COALESCE(?, sample_ids),
                                        citation_pmids=COALESCE(?, citation_pmids)
                                    WHERE accession=?
                                """, (
                                    detail.get("title", ""),
                                    ", ".join(detail.get("organisms", [])),
                                    ", ".join(detail.get("platforms", [])),
                                    detail.get("summary", ""),
                                    detail.get("overall_design", ""),
                                    detail.get("n_samples", 0) or None,
                                    json.dumps(detail.get("supplementary_files", [])),
                                    detail.get("submission_date", ""),
                                    detail.get("last_update_date", ""),
                                    detail.get("status", ""),
                                    detail.get("contact_name", ""),
                                    detail.get("contact_institute", ""),
                                    json.dumps(detail.get("types", [])),
                                    json.dumps(detail.get("relations", [])),
                                    json.dumps(detail.get("samples", [])),
                                    json.dumps(detail.get("pubmed_ids", [])),
                                    m,
                                ))
                                fetch_sra_runs(conn, m)
                        conn.execute("""
                            INSERT OR IGNORE INTO publication_datasets (pmid, accession_id, source)
                            VALUES (?, ?, 'potentially_related_dataset')
                        """, (pmid, acc_row[0]))
                        new_accessions += 1
        except Exception:
            rate_limit()

    print(f"\n  Found {new_accessions} potentially related accession-publication links from full text")


# ============================================================
# SUMMARY
# ============================================================
def print_summary(conn):
    """Print database summary."""
    print("\n" + "=" * 60)
    print("YEO LAB PUBLICATIONS DATABASE SUMMARY")
    print("=" * 60)

    stats = {
        'Publications': conn.execute("SELECT COUNT(*) FROM publications").fetchone()[0],
        'Unique Authors': conn.execute("SELECT COUNT(*) FROM authors").fetchone()[0],
        'Author-Paper Links': conn.execute("SELECT COUNT(*) FROM publication_authors").fetchone()[0],
        'Dataset Accessions': conn.execute("SELECT COUNT(*) FROM dataset_accessions").fetchone()[0],
        'Pub-Dataset Links': conn.execute("SELECT COUNT(*) FROM publication_datasets").fetchone()[0],
        'Dataset Files': conn.execute("SELECT COUNT(*) FROM dataset_files").fetchone()[0],
        'Grants': conn.execute("SELECT COUNT(*) FROM grants").fetchone()[0],
    }
    for k, v in stats.items():
        print(f"  {k}: {v}")

    # Year distribution
    print("\n  Publications by year:")
    rows = conn.execute("""
        SELECT pub_year, COUNT(*) FROM publications
        WHERE pub_year IS NOT NULL GROUP BY pub_year ORDER BY pub_year
    """).fetchall()
    for year, cnt in rows:
        bar = '█' * cnt
        print(f"    {year}: {bar} ({cnt})")

    # Top journals
    print("\n  Top 10 journals:")
    rows = conn.execute("""
        SELECT journal_name, COUNT(*) FROM publications
        WHERE journal_name != '' GROUP BY journal_name ORDER BY COUNT(*) DESC LIMIT 10
    """).fetchall()
    for j, cnt in rows:
        print(f"    {cnt:3d} | {j[:70]}")

    # Accession types
    print("\n  Dataset accessions by type:")
    rows = conn.execute("""
        SELECT accession_type, database, COUNT(*) FROM dataset_accessions
        GROUP BY accession_type, database ORDER BY COUNT(*) DESC
    """).fetchall()
    for atype, db, cnt in rows:
        print(f"    {atype:8s} ({db:12s}): {cnt}")

    # Recent updates
    print("\n  Recent updates:")
    rows = conn.execute("""
        SELECT update_time, new_pmids_added, notes FROM update_log
        ORDER BY update_id DESC LIMIT 5
    """).fetchall()
    for t, n, notes in rows:
        print(f"    {t}: +{n or 0} PMIDs - {notes or ''}")


def search_db(conn, query):
    """Full-text search across publications."""
    print(f"\nSearching for: {query}")
    try:
        rows = conn.execute("""
            SELECT p.pmid, p.title, p.pub_year, p.journal_name
            FROM publications p
            WHERE p.title LIKE ? OR p.abstract LIKE ?
            ORDER BY p.pub_year DESC
        """, (f"%{query}%", f"%{query}%")).fetchall()

        print(f"Found {len(rows)} results:\n")
        for pmid, title, year, journal in rows:
            print(f"  [{year}] PMID {pmid}")
            print(f"    {title[:100]}")
            print(f"    {journal}")
            print()
    except Exception as e:
        print(f"Search error: {e}")


# ============================================================
# MAIN
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="Yeo Lab Publications DB Updater")
    parser.add_argument("--db", default=DB_PATH, help="Database path")
    parser.add_argument("--pubmed-only", action="store_true", help="Only update PubMed")
    parser.add_argument("--geo-only", action="store_true", help="Only update GEO/SRA")
    parser.add_argument("--summary", action="store_true", help="Print summary")
    parser.add_argument("--search", type=str, help="Search publications")
    args = parser.parse_args()

    conn = ensure_db(args.db)

    if args.summary:
        print_summary(conn)
    elif args.search:
        search_db(conn, args.search)
    elif args.pubmed_only:
        update_pubmed(conn)
        print_summary(conn)
    elif args.geo_only:
        pmids = [r[0] for r in conn.execute("SELECT pmid FROM publications").fetchall()]
        update_geo_sra(conn, pmids)
        print_summary(conn)
    else:
        # Full update
        pmids = update_pubmed(conn)
        update_geo_sra(conn, pmids)
        print_summary(conn)

    conn.close()
    print(f"\nDatabase: {args.db}")
    print(f"Done at {datetime.now().isoformat()}")


if __name__ == "__main__":
    main()
