"""
Service layer for updating the Yeo Lab publications database.
Wraps the logic from update_yeolab_db.py for use within the Django app.

Requirements: pip install biopython requests
These views only work when the Django app is run locally (not in Cowork sandbox)
because NCBI Entrez API access is needed.
"""

import json
import re
import time
import os
import threading
import xml.etree.ElementTree as ET
from datetime import datetime
from collections import defaultdict

from django.db import connection


def _is_postgres():
    """Check if the current database backend is PostgreSQL."""
    return connection.vendor == 'postgresql'


def _now_sql():
    """Return the SQL expression for 'current timestamp' for the active backend."""
    return "NOW()" if _is_postgres() else "datetime('now')"


# Django's SQLite backend uses %s placeholders (not ?).
# All raw SQL in this module uses %s accordingly.
# INSERT OR REPLACE/IGNORE → ON CONFLICT for PostgreSQL compatibility.

# ============================================================
# Try importing dependencies — fail gracefully
# ============================================================
try:
    from Bio import Entrez

    _HAS_BIOPYTHON = True
except ImportError:
    _HAS_BIOPYTHON = False

try:
    import requests as _requests

    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False


def _check_deps():
    """Raise a clear error if dependencies are missing."""
    missing = []
    if not _HAS_BIOPYTHON:
        missing.append("biopython")
    if not _HAS_REQUESTS:
        missing.append("requests")
    if missing:
        raise RuntimeError(
            f"Missing packages: {', '.join(missing)}. "
            f"Install with: pip install {' '.join(missing)}"
        )


# ============================================================
# Configuration
# ============================================================
ENTREZ_EMAIL = "brian.alan.yee@gmail.com"
AUTHOR_QUERY = "Yeo GW[Author]"

ACCESSION_PATTERNS = {
    "GSE": re.compile(r"\b(GSE\d{3,8})\b"),
    "GSM": re.compile(r"\b(GSM\d{3,8})\b"),
    "GDS": re.compile(r"\b(GDS\d{3,6})\b"),
    "GPL": re.compile(r"\b(GPL\d{2,6})\b"),
    "SRP": re.compile(r"\b(SRP\d{5,9})\b"),
    "SRR": re.compile(r"\b(SRR\d{5,12})\b"),
    "SRX": re.compile(r"\b(SRX\d{5,9})\b"),
    "SRS": re.compile(r"\b(SRS\d{5,9})\b"),
    "PRJNA": re.compile(r"\b(PRJNA\d{3,9})\b"),
    "PRJEB": re.compile(r"\b(PRJEB\d{3,9})\b"),
    "ENCSR": re.compile(r"\b(ENCSR\d{3}[A-Z]{3})\b"),
    "ENCFF": re.compile(r"\b(ENCFF\d{3}[A-Z]{3})\b"),
    "E-MTAB": re.compile(r"\b(E-MTAB-\d{3,6})\b"),
    "MASSIVE": re.compile(r"\b(MSV\d{6,9})\b"),
    "PXD": re.compile(r"\b(PXD\d{5,9})\b"),
}

DB_MAP = {
    "GSE": "GEO",
    "GSM": "GEO",
    "GDS": "GEO",
    "GPL": "GEO",
    "SRP": "SRA",
    "SRR": "SRA",
    "SRX": "SRA",
    "SRS": "SRA",
    "PRJNA": "BioProject",
    "PRJEB": "BioProject",
    "ENCSR": "ENCODE",
    "ENCFF": "ENCODE",
    "E-MTAB": "ArrayExpress",
    "MASSIVE": "MassIVE",
    "PXD": "PRIDE",
}


def _setup_entrez():
    """Configure Entrez with email and optional API key."""
    Entrez.email = ENTREZ_EMAIL
    Entrez.api_key = os.environ.get("NCBI_API_KEY", None)
    rate = 0.34 if Entrez.api_key else 0.5
    return rate


def _rate_limit(rate):
    time.sleep(rate)


def _safe_int(val):
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


# ============================================================
# Background task status tracking
# ============================================================
_update_status = {
    "running": False,
    "progress": "",
    "log": [],
    "started_at": None,
    "finished_at": None,
    "error": None,
    "stats": {},
}
_status_lock = threading.Lock()


def get_update_status():
    """Return a copy of the current update status."""
    with _status_lock:
        return dict(_update_status)


def _set_status(**kwargs):
    with _status_lock:
        _update_status.update(kwargs)


def _log(msg):
    with _status_lock:
        _update_status["log"].append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
        _update_status["progress"] = msg


# ============================================================
# Preview a PMID (fetch metadata without inserting)
# ============================================================
def preview_pmid(pmid_str):
    """
    Fetch metadata for a PMID from PubMed without inserting.
    Returns a preview dict with title, authors, journal, etc.
    """
    _check_deps()
    rate = _setup_entrez()
    pmid_str = pmid_str.strip()

    result = {
        "success": False,
        "pmid": pmid_str,
        "already_exists": False,
        "error": None,
    }

    try:
        # Check if already exists
        with connection.cursor() as cur:
            cur.execute(
                "SELECT title, journal_name, pub_year FROM publications WHERE pmid = %s",
                [pmid_str],
            )
            row = cur.fetchone()
            if row:
                result["already_exists"] = True
                result["title"] = row[0]
                result["journal"] = row[1]
                result["year"] = row[2]
                result["success"] = True
                return result

        # Fetch from PubMed
        handle = Entrez.efetch(db="pubmed", id=[pmid_str], rettype="xml", retmode="xml")
        records = Entrez.read(handle)
        handle.close()
        _rate_limit(rate)

        articles = records.get("PubmedArticle", [])
        if not articles:
            result["error"] = f"PMID {pmid_str} not found on PubMed"
            return result

        article = articles[0]
        medline = article.get("MedlineCitation", {})
        art = medline.get("Article", {})

        title = str(art.get("ArticleTitle", ""))
        journal = art.get("Journal", {})
        journal_name = journal.get("Title", "")
        pub_date = journal.get("JournalIssue", {}).get("PubDate", {})
        year = pub_date.get("Year", "")

        authors = []
        for a in art.get("AuthorList", []):
            last = a.get("LastName", "")
            fore = a.get("ForeName", "")
            if last:
                authors.append(f"{fore} {last}".strip())

        grants = []
        for g in art.get("GrantList", []):
            gid = g.get("GrantID", "")
            agency = g.get("Agency", "")
            if gid or agency:
                grants.append(f"{gid} ({agency})" if gid else agency)

        abstract_parts = art.get("Abstract", {}).get("AbstractText", [])
        abstract = " ".join(str(p) for p in abstract_parts)

        result.update({
            "success": True,
            "title": title,
            "journal": journal_name,
            "year": year,
            "authors": authors,
            "author_count": len(authors),
            "grants": grants,
            "grant_count": len(grants),
            "has_abstract": bool(abstract.strip()),
            "abstract_words": len(abstract.split()) if abstract else 0,
        })

    except Exception as e:
        result["error"] = str(e)

    return result


# ============================================================
# Single PMID submission (synchronous)
# ============================================================
def submit_single_pmid(pmid_str):
    """
    Fetch a single PubMed ID and insert it into the database.
    Returns a dict with status information.
    """
    _check_deps()
    rate = _setup_entrez()
    pmid_str = pmid_str.strip()

    result = {
        "success": False,
        "pmid": pmid_str,
        "title": None,
        "authors_added": 0,
        "grants_added": 0,
        "already_exists": False,
        "error": None,
    }

    try:
        # Check if already exists
        with connection.cursor() as cur:
            cur.execute("SELECT title FROM publications WHERE pmid = %s", [pmid_str])
            row = cur.fetchone()
            if row:
                result["already_exists"] = True
                result["title"] = row[0]
                result["success"] = True
                return result

        # Fetch from PubMed
        handle = Entrez.efetch(db="pubmed", id=[pmid_str], rettype="xml", retmode="xml")
        records = Entrez.read(handle)
        handle.close()
        _rate_limit(rate)

        articles = records.get("PubmedArticle", [])
        if not articles:
            result["error"] = f"PMID {pmid_str} not found on PubMed"
            return result

        article = articles[0]
        info = _insert_pubmed_article_raw(article)
        result["title"] = info.get("title", "")
        result["authors_added"] = info.get("authors_added", 0)
        result["grants_added"] = info.get("grants_added", 0)
        result["success"] = True

    except Exception as e:
        result["error"] = str(e)

    return result


# ============================================================
# Preview removal of a PMID
# ============================================================
def preview_remove_pmid(pmid_str):
    """
    Look up what would be removed if a PMID is deleted.
    Returns a summary dict without actually deleting.
    """
    pmid_str = pmid_str.strip()
    result = {
        "success": False,
        "pmid": pmid_str,
        "exists": False,
        "error": None,
    }

    try:
        with connection.cursor() as cur:
            cur.execute(
                "SELECT title, journal_name, pub_year FROM publications WHERE pmid = %s",
                [pmid_str],
            )
            row = cur.fetchone()
            if not row:
                result["error"] = f"PMID {pmid_str} is not in the database"
                return result

            result["exists"] = True
            result["title"] = row[0]
            result["journal"] = row[1]
            result["year"] = row[2]

            # Count related records
            cur.execute(
                "SELECT COUNT(*) FROM publication_authors WHERE pmid = %s", [pmid_str]
            )
            result["author_links"] = cur.fetchone()[0]

            cur.execute(
                "SELECT COUNT(*) FROM publication_grants WHERE pmid = %s", [pmid_str]
            )
            result["grant_links"] = cur.fetchone()[0]

            cur.execute(
                "SELECT COUNT(*) FROM publication_datasets WHERE pmid = %s", [pmid_str]
            )
            result["dataset_links"] = cur.fetchone()[0]

            # Get author names for display
            cur.execute(
                """SELECT a.fore_name, a.last_name
                   FROM publication_authors pa
                   JOIN authors a ON a.author_id = pa.author_id
                   WHERE pa.pmid = %s
                   ORDER BY pa.author_position""",
                [pmid_str],
            )
            result["authors"] = [
                f"{r[0]} {r[1]}".strip() for r in cur.fetchall()
            ]

            result["success"] = True

    except Exception as e:
        result["error"] = str(e)

    return result


# ============================================================
# Remove a PMID (synchronous)
# ============================================================
def remove_pmid(pmid_str):
    """
    Delete a publication and its junction-table links from the database.
    Does NOT delete orphaned authors/grants (they may be shared).
    Returns a dict with removal status.
    """
    pmid_str = pmid_str.strip()
    result = {
        "success": False,
        "pmid": pmid_str,
        "error": None,
        "removed": {},
    }

    try:
        with connection.cursor() as cur:
            # Verify it exists
            cur.execute("SELECT title FROM publications WHERE pmid = %s", [pmid_str])
            row = cur.fetchone()
            if not row:
                result["error"] = f"PMID {pmid_str} is not in the database"
                return result

            result["title"] = row[0]

            # Delete from junction tables first (FK order)
            for table in (
                "publication_authors",
                "publication_grants",
                "publication_datasets",
                "publication_affiliations",
                "publication_summaries",
            ):
                try:
                    cur.execute(f"DELETE FROM {table} WHERE pmid = %s", [pmid_str])
                    result["removed"][table] = cur.rowcount
                except Exception:
                    pass  # Table may not exist or be empty

            # Delete from FTS5 index
            try:
                cur.execute(
                    "DELETE FROM publications_fts WHERE pmid = %s", [pmid_str]
                )
            except Exception:
                pass  # FTS table may not exist

            # Delete the publication itself
            cur.execute("DELETE FROM publications WHERE pmid = %s", [pmid_str])
            result["removed"]["publications"] = cur.rowcount

            # Log the removal
            cur.execute(
                """INSERT INTO update_log (new_pmids_added, notes)
                   VALUES (%s, %s)""",
                [-1, f"Removed PMID {pmid_str}: {result['title'][:80]}"],
            )

            result["success"] = True

    except Exception as e:
        result["error"] = str(e)

    return result


def _insert_pubmed_article_raw(article_data):
    """Parse and insert a PubmedArticle into the DB using raw SQL. Returns info dict."""
    info = {"title": "", "authors_added": 0, "grants_added": 0}

    medline = article_data.get("MedlineCitation", {})
    pmid = str(medline.get("PMID", ""))
    if not pmid:
        return info

    article = medline.get("Article", {})
    title = str(article.get("ArticleTitle", ""))
    info["title"] = title

    abstract_parts = article.get("Abstract", {}).get("AbstractText", [])
    abstract = " ".join(str(p) for p in abstract_parts)

    journal = article.get("Journal", {})
    journal_name = journal.get("Title", "")
    journal_iso = journal.get("ISOAbbreviation", "")

    journal_issue = journal.get("JournalIssue", {})
    pub_date = journal_issue.get("PubDate", {})
    year = _safe_int(pub_date.get("Year"))
    month_str = pub_date.get("Month", "")
    day = _safe_int(pub_date.get("Day"))

    month_map = {
        "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
        "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
    }
    month = month_map.get(month_str, _safe_int(month_str))

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

    pmc_id = ""
    doi = ""
    pii = ""
    for aid in article_data.get("PubmedData", {}).get("ArticleIdList", []):
        id_type = aid.attributes.get("IdType", "") if hasattr(aid, "attributes") else ""
        if id_type == "pmc":
            pmc_id = str(aid)
        elif id_type == "doi":
            doi = str(aid)
        elif id_type == "pii":
            pii = str(aid)

    pub_types = [str(pt) for pt in article.get("PublicationTypeList", [])]
    mesh_terms = [str(mh.get("DescriptorName", "")) for mh in medline.get("MeshHeadingList", [])]
    keywords = []
    for kw_list in medline.get("KeywordList", []):
        for kw in kw_list:
            keywords.append(str(kw))

    abstract_wc = len(abstract.split()) if abstract else 0
    lang = article.get("Language", ["eng"])
    lang_str = lang[0] if lang else "eng"

    with connection.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO publications
            (pmid, pmc_id, doi, pii, title, abstract,
             journal_name, journal_iso, pub_date, pub_year, pub_month, pub_day,
             volume, issue, pages, pub_types, mesh_terms, keywords,
             language, abstract_word_count, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    {_now_sql()})
            ON CONFLICT (pmid) DO UPDATE SET
                pmc_id=EXCLUDED.pmc_id, doi=EXCLUDED.doi, pii=EXCLUDED.pii,
                title=EXCLUDED.title, abstract=EXCLUDED.abstract,
                journal_name=EXCLUDED.journal_name, journal_iso=EXCLUDED.journal_iso,
                pub_date=EXCLUDED.pub_date, pub_year=EXCLUDED.pub_year,
                pub_month=EXCLUDED.pub_month, pub_day=EXCLUDED.pub_day,
                volume=EXCLUDED.volume, issue=EXCLUDED.issue, pages=EXCLUDED.pages,
                pub_types=EXCLUDED.pub_types, mesh_terms=EXCLUDED.mesh_terms,
                keywords=EXCLUDED.keywords, language=EXCLUDED.language,
                abstract_word_count=EXCLUDED.abstract_word_count,
                updated_at=EXCLUDED.updated_at
            """,
            [
                pmid, pmc_id, doi, pii, title, abstract,
                journal_name, journal_iso, date_str, year, month, day,
                volume, issue, pages,
                json.dumps(pub_types), json.dumps(mesh_terms), json.dumps(keywords),
                lang_str, abstract_wc,
            ],
        )

        # Authors
        author_list = article.get("AuthorList", [])
        num_authors = len(author_list)
        for pos, author in enumerate(author_list):
            last = author.get("LastName", "")
            fore = author.get("ForeName", "")
            initials = author.get("Initials", "")
            if not last:
                continue

            cur.execute(
                """INSERT INTO authors (last_name, fore_name, initials) VALUES (%s, %s, %s)
                   ON CONFLICT DO NOTHING""",
                [last, fore, initials],
            )
            cur.execute(
                "SELECT author_id FROM authors WHERE last_name=%s AND fore_name=%s AND initials=%s",
                [last, fore, initials],
            )
            row = cur.fetchone()
            if row:
                affs = author.get("AffiliationInfo", [])
                aff_text = affs[0].get("Affiliation", "") if affs else ""
                cur.execute(
                    """
                    INSERT INTO publication_authors
                    (pmid, author_id, author_position, is_first_author, is_last_author, affiliation)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (pmid, author_id, author_position) DO UPDATE SET
                        is_first_author=EXCLUDED.is_first_author,
                        is_last_author=EXCLUDED.is_last_author,
                        affiliation=EXCLUDED.affiliation
                    """,
                    [pmid, row[0], pos + 1, 1 if pos == 0 else 0,
                     1 if pos == num_authors - 1 else 0, aff_text],
                )
                info["authors_added"] += 1

        # Grants
        for grant in article.get("GrantList", []):
            gid = grant.get("GrantID", "")
            agency = grant.get("Agency", "")
            country = grant.get("Country", "")
            if gid or agency:
                cur.execute(
                    """INSERT INTO grants (grant_number, agency, country) VALUES (%s, %s, %s)
                       ON CONFLICT DO NOTHING""",
                    [gid, agency, country],
                )
                cur.execute(
                    "SELECT grant_id FROM grants WHERE grant_number=%s AND agency=%s",
                    [gid, agency],
                )
                grow = cur.fetchone()
                if grow:
                    cur.execute(
                        """INSERT INTO publication_grants (pmid, grant_id) VALUES (%s, %s)
                           ON CONFLICT DO NOTHING""",
                        [pmid, grow[0]],
                    )
                    info["grants_added"] += 1

        # Rebuild full-text search entry for this PMID
        try:
            if _is_postgres():
                # PostgreSQL: update the search_vector column (trigger handles this on insert,
                # but we also update explicitly here for safety)
                cur.execute(
                    """UPDATE publications SET search_vector =
                       to_tsvector('english', COALESCE(title,'') || ' ' || COALESCE(abstract,'') || ' ' || COALESCE(journal_name,''))
                       WHERE pmid = %s""",
                    [pmid],
                )
            else:
                # SQLite FTS5
                cur.execute(
                    """INSERT OR REPLACE INTO publications_fts (pmid, title, abstract, journal_name)
                       VALUES (%s, %s, %s, %s)""",
                    [pmid, title, abstract, journal_name],
                )
        except Exception:
            pass  # FTS table may not exist

    return info


# ============================================================
# Full update (runs in background thread)
# ============================================================
def start_full_update(mode="full"):
    """
    Start a background update. mode: 'full', 'pubmed', 'geo', or 'encode'.
    Returns True if started, False if already running.
    """
    if mode == "encode":
        return start_encode_update()
    _check_deps()
    with _status_lock:
        if _update_status["running"]:
            return False
        _update_status.update({
            "running": True,
            "progress": "Starting...",
            "log": [],
            "started_at": datetime.now().isoformat(),
            "finished_at": None,
            "error": None,
            "stats": {},
        })

    t = threading.Thread(target=_run_update, args=(mode,), daemon=True)
    t.start()
    return True


def _run_update(mode):
    """Background worker for full update."""
    try:
        rate = _setup_entrez()
        _log(f"Starting {mode} update...")

        if mode in ("full", "pubmed"):
            pmids = _update_pubmed(rate)
        else:
            # Get existing PMIDs for geo-only mode
            with connection.cursor() as cur:
                cur.execute("SELECT pmid FROM publications")
                pmids = [r[0] for r in cur.fetchall()]

        if mode in ("full", "geo"):
            _update_geo_sra(rate, pmids)

        # Collect final stats
        with connection.cursor() as cur:
            stats = {}
            for table in ("publications", "authors", "dataset_accessions",
                          "publication_datasets", "dataset_files", "grants",
                          "sra_experiments", "sra_runs"):
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                stats[table] = cur.fetchone()[0]

        _set_status(stats=stats)
        _log(f"Update complete! {stats.get('publications', '?')} publications in DB.")

    except Exception as e:
        _log(f"ERROR: {e}")
        _set_status(error=str(e))
    finally:
        _set_status(running=False, finished_at=datetime.now().isoformat())


def _update_pubmed(rate):
    """Fetch all PMIDs and insert new publications."""
    _log("Searching PubMed for Yeo GW[Author]...")

    handle = Entrez.esearch(db="pubmed", term=AUTHOR_QUERY, retmax=1000, sort="pub_date")
    record = Entrez.read(handle)
    handle.close()
    _rate_limit(rate)

    all_pmids = record["IdList"]
    total_in_pubmed = int(record["Count"])
    _log(f"PubMed reports {total_in_pubmed} results, retrieved {len(all_pmids)} PMIDs")

    with connection.cursor() as cur:
        cur.execute("SELECT pmid FROM publications")
        existing = set(r[0] for r in cur.fetchall())

    new_pmids = [p for p in all_pmids if p not in existing]
    _log(f"Existing: {len(existing)}, New: {len(new_pmids)}")

    if new_pmids:
        _log(f"Fetching metadata for {len(new_pmids)} new publications...")
        for i in range(0, len(new_pmids), 20):
            batch = new_pmids[i:i + 20]
            try:
                handle = Entrez.efetch(db="pubmed", id=batch, rettype="xml", retmode="xml")
                records = Entrez.read(handle)
                handle.close()
                _rate_limit(rate)

                articles = records.get("PubmedArticle", [])
                for article in articles:
                    _insert_pubmed_article_raw(article)
                _log(f"  Inserted batch {i // 20 + 1} ({min(i + 20, len(new_pmids))}/{len(new_pmids)})")
            except Exception as e:
                _log(f"  Warning: Batch failed: {e}")
                _rate_limit(rate)

        _log(f"Inserted {len(new_pmids)} new publications")
    else:
        _log("No new publications found")

    # Log update
    with connection.cursor() as cur:
        cur.execute(
            """INSERT INTO update_log (total_pmids_in_pubmed, new_pmids_added, notes)
               VALUES (%s, %s, %s)""",
            [total_in_pubmed, len(new_pmids), f"Web UI update: {len(new_pmids)} new"],
        )

    return all_pmids


def _parse_geo_soft(text, accession):
    """Parse GEO SOFT format."""
    result = {
        "accession": accession, "samples": [], "supplementary_files": [],
        "organisms": [], "platforms": [],
    }
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

    result["n_samples"] = len(result["samples"])
    return result


def _update_geo_sra(rate, pmids):
    """Find and update GEO/SRA datasets linked to publications."""
    _log("Finding GEO links via NCBI ELink...")
    pmid_to_gse = defaultdict(set)

    for i in range(0, len(pmids), 50):
        batch = pmids[i:i + 50]
        pct = min(i + 50, len(pmids)) / len(pmids) * 100
        _log(f"  ELink batch {i // 50 + 1} ({pct:.0f}%)...")

        try:
            handle = Entrez.elink(dbfrom="pubmed", db="gds", id=batch, linkname="pubmed_gds")
            records = Entrez.read(handle)
            handle.close()
            _rate_limit(rate)

            for rec in records:
                pm = rec["IdList"][0] if rec["IdList"] else None
                if not pm:
                    continue
                gds_ids = []
                for linkset in rec.get("LinkSetDb", []):
                    for link in linkset.get("Link", []):
                        gds_ids.append(link["Id"])

                if gds_ids:
                    try:
                        h2 = Entrez.esummary(db="gds", id=",".join(gds_ids[:100]))
                        summaries = Entrez.read(h2)
                        h2.close()
                        _rate_limit(rate)
                        for s in summaries:
                            gse_num = s.get("GSE", "")
                            acc = s.get("Accession", "")
                            if gse_num:
                                pmid_to_gse[pm].add(f"GSE{gse_num}")
                            elif acc.startswith("GSE"):
                                pmid_to_gse[pm].add(acc)
                    except Exception:
                        _rate_limit(rate)
        except Exception as e:
            _log(f"  Warning: ELink failed: {e}")
            _rate_limit(rate)

    all_gse = set()
    for gse_set in pmid_to_gse.values():
        all_gse.update(gse_set)
    _log(f"Found {len(all_gse)} unique GSE series from {len(pmid_to_gse)} PMIDs")

    # Fetch new GEO series details
    new_gse = 0
    for gse in all_gse:
        with connection.cursor() as cur:
            cur.execute("SELECT accession_id FROM dataset_accessions WHERE accession=%s", [gse])
            if cur.fetchone():
                continue  # Already have this one

        try:
            url = f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={gse}&targ=self&form=text&view=brief"
            resp = _requests.get(url, timeout=30)
            if resp.status_code == 200:
                detail = _parse_geo_soft(resp.text, gse)
                with connection.cursor() as cur:
                    cur.execute(
                        """INSERT INTO dataset_accessions
                           (accession, accession_type, database, title, organism, platform,
                            summary, overall_design, num_samples, supplementary_files)
                           VALUES (%s, 'GSE', 'GEO', %s, %s, %s, %s, %s, %s, %s)
                           ON CONFLICT (accession) DO NOTHING""",
                        [
                            gse, detail.get("title", ""),
                            ", ".join(detail.get("organisms", [])),
                            ", ".join(detail.get("platforms", [])),
                            detail.get("summary", ""),
                            detail.get("overall_design", ""),
                            detail.get("n_samples", 0),
                            json.dumps(detail.get("supplementary_files", [])),
                        ],
                    )
                _fetch_sra_runs(rate, gse)
                new_gse += 1
            _rate_limit(rate)
        except Exception as e:
            _log(f"  Warning: Failed to fetch {gse}: {e}")
            _rate_limit(rate)

    _log(f"Added {new_gse} new GSE series")

    # Link GSE to PMIDs
    with connection.cursor() as cur:
        for pm, gse_set in pmid_to_gse.items():
            for gse in gse_set:
                cur.execute(
                    "SELECT accession_id FROM dataset_accessions WHERE accession=%s", [gse]
                )
                acc_row = cur.fetchone()
                if acc_row:
                    cur.execute(
                        """INSERT INTO publication_datasets (pmid, accession_id, source)
                           VALUES (%s, %s, 'ncbi_elink')
                           ON CONFLICT DO NOTHING""",
                        [pm, acc_row[0]],
                    )

    # Scan PMC full text
    _scan_pmc_fulltext(rate, pmids)

    with connection.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM dataset_accessions")
        acc_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM publication_datasets")
        link_count = cur.fetchone()[0]
    _log(f"Database now has {acc_count} accessions, {link_count} pub-dataset links")


def _fetch_sra_runs(rate, gse):
    """Fetch SRA run info for a GEO series."""
    try:
        handle = Entrez.esearch(db="sra", term=f"{gse}[All Fields]", retmax=500)
        record = Entrez.read(handle)
        handle.close()
        _rate_limit(rate)

        sra_ids = record.get("IdList", [])
        if not sra_ids:
            return

        handle = Entrez.efetch(db="sra", id=sra_ids[:200], rettype="runinfo", retmode="text")
        runinfo_text = handle.read()
        handle.close()
        _rate_limit(rate)

        with connection.cursor() as cur:
            cur.execute(
                "SELECT accession_id FROM dataset_accessions WHERE accession=%s", [gse]
            )
            acc_row = cur.fetchone()
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
                    cur.execute(
                        """INSERT INTO dataset_files
                           (accession_id, file_name, file_type, file_size_bytes, file_url)
                           VALUES (%s, %s, %s, %s, %s)
                           ON CONFLICT DO NOTHING""",
                        [acc_id, file_name, strategy, size_bytes, download],
                    )
    except Exception:
        _rate_limit(rate)


# ============================================================
# ENCODE Project update (runs in background thread)
# ============================================================
ENCODE_BASE_URL = "https://www.encodeproject.org"
ENCODE_RATE_LIMIT = 0.15  # seconds between requests
ENCODE_TIMEOUT = 60.0
ENCODE_MAX_RETRIES = 3
ENCODE_DEFAULT_GRANTS = ["U41HG009889", "U54HG007005"]
ENCODE_HEADERS = {
    "accept": "application/json",
    "User-Agent": "YeoLabDjango/1.0 (brian.alan.yee@gmail.com)",
}


def start_encode_update(grants=None, skip_files=False, skip_details=False):
    """
    Start a background ENCODE update.
    Returns True if started, False if already running.
    """
    if not _HAS_REQUESTS:
        raise RuntimeError("Missing package: requests. Install with: pip install requests")

    with _status_lock:
        if _update_status["running"]:
            return False
        _update_status.update({
            "running": True,
            "progress": "Starting ENCODE update...",
            "log": [],
            "started_at": datetime.now().isoformat(),
            "finished_at": None,
            "error": None,
            "stats": {},
        })

    grant_list = grants or ENCODE_DEFAULT_GRANTS
    t = threading.Thread(
        target=_run_encode_update,
        args=(grant_list, skip_files, skip_details),
        daemon=True,
    )
    t.start()
    return True


def _encode_api_get(url, params=None, label="request"):
    """Make a GET request to the ENCODE API with retries."""
    for attempt in range(ENCODE_MAX_RETRIES + 1):
        try:
            time.sleep(ENCODE_RATE_LIMIT)
            resp = _requests.get(
                url,
                params=params,
                headers=ENCODE_HEADERS,
                timeout=ENCODE_TIMEOUT,
            )
            resp.raise_for_status()
            return resp.json()
        except _requests.exceptions.RequestException as e:
            if attempt < ENCODE_MAX_RETRIES:
                wait = 2.0 * (2 ** attempt)
                _log(f"  [RETRY {attempt+1}/{ENCODE_MAX_RETRIES}] {label}: {e} — waiting {wait:.1f}s")
                time.sleep(wait)
            else:
                raise


def _encode_search(search_type, extra_params=None):
    """Search the ENCODE portal. Returns list of result objects."""
    params = {"type": search_type, "format": "json", "limit": "all"}
    if extra_params:
        params.update(extra_params)
    data = _encode_api_get(
        f"{ENCODE_BASE_URL}/search/",
        params=params,
        label=f"search {search_type}",
    )
    return data.get("@graph", [])


def _extract_encode_control_accessions(control_values):
    """Extract ENCSR control accessions from ENCODE `possible_controls` values."""
    controls = []
    pattern = re.compile(r"(ENCSR\d{3}[A-Z]{3})")
    for ctrl in control_values or []:
        if isinstance(ctrl, dict):
            acc = str(ctrl.get("accession", "")).strip().upper()
            if pattern.fullmatch(acc):
                controls.append(acc)
            continue
        if isinstance(ctrl, str):
            for match in pattern.findall(ctrl.upper()):
                controls.append(match)
    # Preserve order while de-duplicating
    return list(dict.fromkeys(controls))


def _parse_relations_json(relations_text):
    """Parse dataset_accessions.relations JSON into a list of strings."""
    if not relations_text:
        return []
    try:
        parsed = json.loads(relations_text)
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item).strip() for item in parsed if str(item).strip()]


def _merge_control_relations(existing_relations, controls):
    """Merge `control:ENCSR...` tags into existing relations list."""
    merged = list(existing_relations or [])
    seen = set(merged)
    for control_acc in controls:
        relation = f"control:{control_acc}"
        if relation not in seen:
            merged.append(relation)
            seen.add(relation)
    return merged


def _run_encode_update(grant_list, skip_files, skip_details):
    """Background worker for ENCODE update."""
    try:
        _log(f"Starting ENCODE update for grants: {', '.join(grant_list)}")
        total_experiments = 0
        total_files_added = 0
        total_links = 0

        # Step 1: Find experiments for each grant
        all_experiments = {}  # accession -> experiment dict
        for grant in grant_list:
            _log(f"[1/4] Searching ENCODE experiments for {grant}...")
            results = _encode_search("Experiment", {"award.name": grant})
            if not results:
                results = _encode_search("Experiment", {"award.project_num": grant})
            _log(f"  Found {len(results)} experiments for {grant}")

            for exp in results:
                acc = exp.get("accession", "")
                if not acc or acc in all_experiments:
                    continue
                all_experiments[acc] = _parse_encode_experiment(exp, grant)

        total_experiments = len(all_experiments)
        _log(f"  Total unique experiments: {total_experiments}")

        # Step 2: Fetch experiment details (replicates, references with PMIDs)
        if not skip_details and all_experiments:
            _log(f"[2/4] Fetching experiment details...")
            count = 0
            for acc, exp in all_experiments.items():
                count += 1
                if count % 50 == 0 or count == total_experiments:
                    _log(f"  Details: {count}/{total_experiments}...")
                try:
                    detail = _encode_api_get(
                        f"{ENCODE_BASE_URL}/experiments/{acc}/",
                        params={"format": "json", "frame": "embedded"},
                        label=f"GET /experiments/{acc}/",
                    )
                    # Extract PMIDs from embedded references
                    for ref in detail.get("references", []):
                        if isinstance(ref, dict):
                            for ident in ref.get("identifiers", []):
                                if isinstance(ident, str) and ident.startswith("PMID:"):
                                    pmid = ident.replace("PMID:", "")
                                    if pmid not in exp["pmids"]:
                                        exp["pmids"].append(pmid)
                    # Extract controls from detailed payload (fallback when search frame omits them)
                    for control_acc in _extract_encode_control_accessions(
                        detail.get("possible_controls", [])
                    ):
                        if control_acc not in exp["controls"]:
                            exp["controls"].append(control_acc)
                    # Extract organism from replicates
                    organisms = set()
                    for rep in detail.get("replicates", []):
                        if not isinstance(rep, dict):
                            continue
                        lib = rep.get("library", {}) or {}
                        bio = lib.get("biosample", {}) or {}
                        org = bio.get("organism", {})
                        if isinstance(org, dict):
                            name = org.get("scientific_name", "")
                            if name:
                                organisms.add(name)
                    if organisms:
                        exp["organisms"] = sorted(organisms)
                except Exception as e:
                    pass  # Non-fatal
        else:
            _log(f"[2/4] Skipping experiment details")

        # Step 3: Insert experiments into dataset_accessions and link to PMIDs
        _log(f"[3/4] Importing {total_experiments} experiments into database...")
        with connection.cursor() as cur:
            for acc, exp in all_experiments.items():
                # Insert into dataset_accessions
                organism = ", ".join(exp.get("organisms", []))
                summary = f"{exp.get('assay_title', '')} of {exp.get('target', 'N/A')} in {exp.get('biosample_summary', exp.get('biosample_term', 'N/A'))}"
                assay_title = (exp.get("assay_title", "") or "").strip()
                is_eclip = "eclip" in assay_title.lower()
                controls = exp.get("controls", []) if is_eclip else []
                experiment_types_json = json.dumps([assay_title]) if assay_title else ""

                cur.execute(
                    "SELECT accession_id, relations FROM dataset_accessions WHERE accession = %s",
                    [acc],
                )
                existing = cur.fetchone()
                if existing:
                    existing_relations = _parse_relations_json(existing[1])
                    merged_relations = _merge_control_relations(existing_relations, controls)
                    relations_json = json.dumps(merged_relations) if merged_relations else ""
                    # Update existing record with richer metadata
                    cur.execute(
                        """UPDATE dataset_accessions
                           SET title = COALESCE(NULLIF(%s, ''), title),
                               organism = COALESCE(NULLIF(%s, ''), organism),
                               summary = COALESCE(NULLIF(%s, ''), summary),
                               status = COALESCE(NULLIF(%s, ''), status),
                               submission_date = COALESCE(NULLIF(%s, ''), submission_date),
                               last_update_date = COALESCE(NULLIF(%s, ''), last_update_date),
                               contact_name = COALESCE(NULLIF(%s, ''), contact_name),
                               experiment_types = COALESCE(NULLIF(%s, ''), experiment_types),
                               relations = COALESCE(NULLIF(%s, ''), relations)
                           WHERE accession_id = %s""",
                        [
                            exp.get("description", ""),
                            organism,
                            summary,
                            exp.get("status", ""),
                            exp.get("date_submitted", ""),
                            exp.get("date_released", ""),
                            exp.get("lab", ""),
                            experiment_types_json,
                            relations_json,
                            existing[0],
                        ],
                    )
                    acc_id = existing[0]
                else:
                    relations_json = json.dumps([f"control:{c}" for c in controls]) if controls else ""
                    if _is_postgres():
                        cur.execute(
                            """INSERT INTO dataset_accessions
                               (accession, accession_type, database, title, organism,
                                summary, overall_design, status, submission_date,
                                last_update_date, contact_name, experiment_types, relations,
                                citation_pmids)
                               VALUES (%s, 'ENCSR', 'ENCODE', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                               RETURNING accession_id""",
                            [
                                acc, exp.get("description", ""), organism, summary,
                                exp.get("description", ""), exp.get("status", ""),
                                exp.get("date_submitted", ""), exp.get("date_released", ""),
                                exp.get("lab", ""), experiment_types_json,
                                relations_json, json.dumps(exp.get("pmids", [])),
                            ],
                        )
                        acc_id = cur.fetchone()[0]
                    else:
                        cur.execute(
                            """INSERT INTO dataset_accessions
                               (accession, accession_type, database, title, organism,
                                summary, overall_design, status, submission_date,
                                last_update_date, contact_name, experiment_types, relations,
                                citation_pmids)
                               VALUES (%s, 'ENCSR', 'ENCODE', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                            [
                                acc, exp.get("description", ""), organism, summary,
                                exp.get("description", ""), exp.get("status", ""),
                                exp.get("date_submitted", ""), exp.get("date_released", ""),
                                exp.get("lab", ""), experiment_types_json,
                                relations_json, json.dumps(exp.get("pmids", [])),
                            ],
                        )
                        acc_id = cur.lastrowid

                # Ensure referenced control experiments exist as ENCODE accessions.
                # This keeps control links resolvable even if a control was not returned by grant search.
                for control_acc in controls:
                    cur.execute(
                        """INSERT INTO dataset_accessions
                           (accession, accession_type, database, title)
                           VALUES (%s, 'ENCSR', 'ENCODE', %s)
                           ON CONFLICT (accession) DO NOTHING""",
                        [control_acc, "ENCODE control experiment"],
                    )

                # Link to publications
                for pmid in exp.get("pmids", []):
                    cur.execute(
                        "SELECT pmid FROM publications WHERE pmid = %s", [pmid]
                    )
                    if cur.fetchone():
                        cur.execute(
                            """INSERT INTO publication_datasets
                               (pmid, accession_id, source)
                               VALUES (%s, %s, 'encode_api')
                               ON CONFLICT DO NOTHING""",
                            [pmid, acc_id],
                        )
                        total_links += 1

        _log(f"  Created {total_links} publication-dataset links")

        # Step 4: Fetch and insert file metadata
        if not skip_files and all_experiments:
            _log(f"[4/4] Fetching file metadata...")
            count = 0
            for acc in all_experiments:
                count += 1
                if count % 50 == 0 or count == total_experiments:
                    _log(f"  Files: {count}/{total_experiments} experiments...")
                try:
                    files = _encode_search("File", {
                        "dataset": f"/experiments/{acc}/",
                        "field": [
                            "accession", "file_format", "output_type", "file_size", "href",
                            "assembly", "genome_annotation", "mapped_by",
                        ],
                    })
                    with connection.cursor() as cur:
                        cur.execute(
                            "SELECT accession_id FROM dataset_accessions WHERE accession = %s",
                            [acc],
                        )
                        acc_row = cur.fetchone()
                        if not acc_row:
                            continue
                        acc_id = acc_row[0]

                        for f in files:
                            file_acc = f.get("accession", "")
                            if not file_acc:
                                continue
                            file_format = f.get("file_format", "")
                            output_type = f.get("output_type", "")
                            file_size = f.get("file_size")
                            assembly = f.get("assembly", "")
                            mapping_assembly = f.get("mapping_assembly", "")
                            genome_annotation = f.get("genome_annotation", "")
                            md5sum = f.get("md5sum", "")
                            href = f.get("href", "")
                            download_url = f"{ENCODE_BASE_URL}{href}" if href and not href.startswith("http") else href

                            file_name = f"{file_acc}.{file_format}" if file_format else file_acc
                            file_type = f"{file_format} ({output_type})" if output_type else file_format
                            metadata_tags = []
                            if mapping_assembly:
                                metadata_tags.append(f"mapping assembly: {mapping_assembly}")
                            elif assembly:
                                metadata_tags.append(f"assembly: {assembly}")
                            if genome_annotation:
                                metadata_tags.append(f"genome annotation: {genome_annotation}")
                            if metadata_tags:
                                file_type = f"{file_type}; {'; '.join(metadata_tags)}" if file_type else "; ".join(metadata_tags)

                            cur.execute(
                                """SELECT file_id
                                   FROM dataset_files
                                   WHERE accession_id = %s
                                     AND file_name = %s
                                     AND COALESCE(file_url, '') = %s
                                   ORDER BY file_id
                                   LIMIT 1""",
                                [acc_id, file_name, (download_url or "")],
                            )
                            existing_file = cur.fetchone()
                            if existing_file:
                                cur.execute(
                                    """UPDATE dataset_files
                                       SET file_type = COALESCE(NULLIF(%s, ''), file_type),
                                           file_size_bytes = COALESCE(%s, file_size_bytes),
                                           md5_checksum = COALESCE(NULLIF(%s, ''), md5_checksum)
                                       WHERE file_id = %s""",
                                    [file_type, file_size, md5sum, existing_file[0]],
                                )
                            else:
                                cur.execute(
                                    """INSERT INTO dataset_files
                                       (accession_id, file_name, file_type, file_size_bytes, file_url, md5_checksum)
                                       VALUES (%s, %s, %s, %s, %s, %s)""",
                                    [acc_id, file_name, file_type, file_size, download_url, md5sum],
                                )
                                total_files_added += 1
                except Exception as e:
                    pass  # Non-fatal
            _log(f"  Added {total_files_added} file records")
        else:
            _log(f"[4/4] Skipping file metadata")

        # Collect final stats
        with connection.cursor() as cur:
            stats = {}
            for table in ("publications", "authors", "dataset_accessions",
                          "publication_datasets", "dataset_files", "grants"):
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                stats[table] = cur.fetchone()[0]
            # ENCODE-specific counts
            cur.execute(
                "SELECT COUNT(*) FROM dataset_accessions WHERE database = 'ENCODE'"
            )
            stats["encode_datasets"] = cur.fetchone()[0]

        _set_status(stats=stats)
        _log(f"ENCODE update complete! {total_experiments} experiments, {total_files_added} files, {total_links} links.")

        # Log the update
        with connection.cursor() as cur:
            cur.execute(
                """INSERT INTO update_log (new_pmids_added, notes)
                   VALUES (%s, %s)""",
                [0, f"ENCODE update: {total_experiments} experiments, {total_files_added} files from {', '.join(grant_list)}"],
            )

    except Exception as e:
        _log(f"ERROR: {e}")
        _set_status(error=str(e))
    finally:
        _set_status(running=False, finished_at=datetime.now().isoformat())


def _parse_encode_experiment(exp, grant):
    """Parse an ENCODE experiment search result into a standard dict."""
    # Extract target
    target = exp.get("target", {})
    if isinstance(target, dict):
        target_label = target.get("label", target.get("name", ""))
    elif isinstance(target, str):
        target_label = target.split("/")[-2] if "/" in target else target
    else:
        target_label = ""

    # Extract biosample
    biosample = exp.get("biosample_ontology", {})
    if isinstance(biosample, dict):
        biosample_term = biosample.get("term_name", "")
    elif isinstance(biosample, list) and biosample:
        first = biosample[0] if isinstance(biosample[0], dict) else {}
        biosample_term = first.get("term_name", "")
    else:
        biosample_term = ""

    # Extract lab
    lab = exp.get("lab", {})
    if isinstance(lab, dict):
        lab_name = lab.get("title", lab.get("name", ""))
    elif isinstance(lab, str):
        lab_name = lab.split("/")[-2] if "/" in lab else lab
    else:
        lab_name = ""

    # Extract PMIDs from references
    pmids = []
    for ref in exp.get("references", []):
        if isinstance(ref, dict):
            for ident in ref.get("identifiers", []):
                if isinstance(ident, str) and ident.startswith("PMID:"):
                    pmids.append(ident.replace("PMID:", ""))
    controls = _extract_encode_control_accessions(exp.get("possible_controls", []))

    return {
        "accession": exp.get("accession", ""),
        "status": exp.get("status", ""),
        "assay_title": exp.get("assay_title", ""),
        "target": target_label,
        "biosample_term": biosample_term,
        "biosample_summary": exp.get("biosample_summary", ""),
        "description": exp.get("description", ""),
        "date_released": exp.get("date_released", ""),
        "date_submitted": exp.get("date_submitted", ""),
        "lab": lab_name,
        "grants": [grant],
        "pmids": pmids,
        "controls": controls,
        "organisms": [],
    }


def _scan_pmc_fulltext(rate, pmids):
    """Scan PMC full text for potentially related accessions."""
    _log("Scanning PMC full text for potentially related accessions...")

    pmid_to_pmcid = {}
    for i in range(0, len(pmids), 100):
        batch = pmids[i:i + 100]
        try:
            handle = Entrez.elink(dbfrom="pubmed", db="pmc", id=batch, linkname="pubmed_pmc")
            records = Entrez.read(handle)
            handle.close()
            _rate_limit(rate)
            for rec in records:
                pm = rec["IdList"][0] if rec["IdList"] else None
                if not pm:
                    continue
                for ls in rec.get("LinkSetDb", []):
                    for link in ls.get("Link", []):
                        pmid_to_pmcid[pm] = link["Id"]
        except Exception:
            _rate_limit(rate)

    _log(f"{len(pmid_to_pmcid)} papers have PMC full text")

    new_links = 0
    for idx, (pm, pmcid) in enumerate(pmid_to_pmcid.items()):
        if (idx + 1) % 20 == 0:
            _log(f"  Scanning {idx + 1}/{len(pmid_to_pmcid)}...")
        try:
            handle = Entrez.efetch(db="pmc", id=pmcid, rettype="xml", retmode="xml")
            xml_bytes = handle.read()
            handle.close()
            _rate_limit(rate)

            text = xml_bytes.decode("utf-8", errors="replace") if isinstance(xml_bytes, bytes) else str(xml_bytes)
            clean = re.sub(r"<[^>]+>", " ", text)

            with connection.cursor() as cur:
                for acc_type, pattern in ACCESSION_PATTERNS.items():
                    for m in set(pattern.findall(clean)):
                        db = DB_MAP.get(acc_type, "Unknown")
                        cur.execute(
                            """INSERT INTO dataset_accessions
                               (accession, accession_type, database) VALUES (%s, %s, %s)
                               ON CONFLICT (accession) DO NOTHING""",
                            [m, acc_type, db],
                        )
                        cur.execute(
                            "SELECT accession_id FROM dataset_accessions WHERE accession=%s",
                            [m],
                        )
                        acc_row = cur.fetchone()
                        if acc_row:
                            cur.execute(
                                """INSERT INTO publication_datasets
                                   (pmid, accession_id, source)
                                   VALUES (%s, %s, 'potentially_related_dataset')
                                   ON CONFLICT DO NOTHING""",
                                [pm, acc_row[0]],
                            )
                            new_links += 1
        except Exception:
            _rate_limit(rate)

    _log(f"Found {new_links} potentially related accession-publication links from full text")
