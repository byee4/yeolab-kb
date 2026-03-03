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
        status = dict(_update_status)
        # Return defensive copies for nested mutable fields.
        status["log"] = list(_update_status.get("log", []))
        status["stats"] = dict(_update_status.get("stats", {}))
        return status


def _set_status(**kwargs):
    with _status_lock:
        _update_status.update(kwargs)


def _log(msg):
    with _status_lock:
        _update_status["log"].append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
        _update_status["progress"] = msg


def _init_status_locked(progress_msg):
    """Initialize shared background-update status. Caller must hold _status_lock."""
    _update_status.update({
        "running": True,
        "progress": progress_msg,
        "log": [],
        "started_at": datetime.now().isoformat(),
        "finished_at": None,
        "error": None,
        "stats": {},
    })


def _start_background_worker(target, args, progress_msg):
    """
    Start a daemon thread for a background update with consistent status handling.

    Returns:
        True if started.
        False if an update is already running or worker startup fails.
    """
    with _status_lock:
        if _update_status["running"]:
            return False
        _init_status_locked(progress_msg)

    try:
        t = threading.Thread(target=target, args=args, daemon=True)
        t.start()
    except Exception as exc:
        # If thread startup fails, surface a clean terminal status instead of
        # leaving callers with ambiguous state.
        _set_status(
            running=False,
            finished_at=datetime.now().isoformat(),
            error=f"Failed to start background worker: {exc}",
        )
        _log(f"ERROR: Failed to start background worker: {exc}")
        return False
    return True


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
    return _start_background_worker(
        target=_run_update,
        args=(mode,),
        progress_msg="Starting...",
    )


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

    grant_list = grants or ENCODE_DEFAULT_GRANTS
    return _start_background_worker(
        target=_run_encode_update,
        args=(grant_list, skip_files, skip_details),
        progress_msg="Starting ENCODE update...",
    )


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


def _encode_search_experiments_for_grant(grant):
    """
    Robust ENCODE experiment search for a grant identifier.
    Some filters can return 403 depending on portal policy/index state.
    Try multiple query variants and degrade gracefully.
    """
    variants = [
        ("award.name", {"award.name": grant}),
        ("award.project_num", {"award.project_num": grant}),
        ("award.project", {"award.project": grant}),
        ("searchTerm", {"searchTerm": grant}),
    ]
    for label, params in variants:
        try:
            results = _encode_search("Experiment", params)
        except Exception as e:
            _log(f"  Warning: {label} query failed for {grant}: {e}")
            continue
        if results:
            _log(f"  Grant {grant}: matched {len(results)} experiments via {label}")
            return results
    return []


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


_ENCODE_PROCESSING_KEYWORDS = re.compile(
    r"(process|pipeline|workflow|align|map|assembly|annotat|quant|normaliz|"
    r"peak|idr|trim|adapter|qc|quality control|count|expression|call)",
    flags=re.IGNORECASE,
)


def _split_text_into_processing_sentences(text):
    """Split free text into sentence-like processing steps."""
    if not text:
        return []
    normalized = re.sub(r"\s+", " ", str(text).strip())
    if not normalized:
        return []
    parts = re.split(r"(?<=[.!?])\s+(?=[A-Z0-9])", normalized)
    out = []
    for part in parts:
        sent = part.strip(" ;")
        if sent:
            out.append(sent)
    return out


def _extract_encode_processing_steps(exp, detail=None, files=None):
    """
    Derive ordered processing steps from ENCODE experiment/file metadata.
    Returns (steps, raw_text).
    """
    candidate_sentences = []

    def _add_sentence(text):
        for sentence in _split_text_into_processing_sentences(text):
            if _ENCODE_PROCESSING_KEYWORDS.search(sentence):
                candidate_sentences.append(sentence)

    assay_title = (exp.get("assay_title", "") or "").strip()
    if assay_title:
        _add_sentence(f"Assay type: {assay_title}.")

    description = (exp.get("description", "") or "").strip()
    if description:
        _add_sentence(description)

    biosample_summary = (exp.get("biosample_summary", "") or "").strip()
    if biosample_summary:
        _add_sentence(f"Biosample summary: {biosample_summary}.")

    # Mine selected detail fields when available.
    if isinstance(detail, dict):
        for key in (
            "analysis_step_versions",
            "possible_controls",
            "notes",
            "description",
            "assay_title",
            "biosample_summary",
            "status",
            "target",
        ):
            value = detail.get(key)
            if isinstance(value, str):
                _add_sentence(value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, str):
                        _add_sentence(item)
                    elif isinstance(item, dict):
                        for subkey in ("description", "title", "status"):
                            if isinstance(item.get(subkey), str):
                                _add_sentence(item.get(subkey))

    # Mine file-level metadata: mapped_by/output_type/assembly frequently encode processing context.
    mapped_bys = set()
    output_types = set()
    assemblies = set()
    genome_annotations = set()
    file_formats = set()
    for f in files or []:
        if not isinstance(f, dict):
            continue
        for key, target_set in (
            ("mapped_by", mapped_bys),
            ("output_type", output_types),
            ("assembly", assemblies),
            ("mapping_assembly", assemblies),
            ("genome_annotation", genome_annotations),
            ("file_format", file_formats),
        ):
            val = (f.get(key) or "").strip()
            if val:
                target_set.add(val)

    for tool in sorted(mapped_bys):
        _add_sentence(f"Mapped reads were processed using {tool}.")
    if output_types:
        _add_sentence(f"Generated output types include: {', '.join(sorted(output_types))}.")
    if assemblies:
        _add_sentence(f"Mapping assembly references include: {', '.join(sorted(assemblies))}.")
    if genome_annotations:
        _add_sentence(f"Genome annotations include: {', '.join(sorted(genome_annotations))}.")
    if file_formats:
        _add_sentence(f"Produced file formats include: {', '.join(sorted(file_formats))}.")

    # De-duplicate while preserving order.
    ordered = []
    seen = set()
    for sentence in candidate_sentences:
        norm = sentence.lower()
        if norm in seen:
            continue
        seen.add(norm)
        ordered.append(sentence)

    # Provide a deterministic fallback so ENCODE accessions still have an analysis page.
    if not ordered:
        fallback = f"ENCODE metadata processing summary for {exp.get('accession', '')}."
        if assay_title:
            fallback += f" Assay: {assay_title}."
        ordered = [fallback.strip()]

    steps = [
        {"step_order": idx + 1, "description": desc}
        for idx, desc in enumerate(ordered)
    ]
    raw_text = "\n".join(ordered)
    return steps, raw_text


def _write_encode_steps_to_code_examples(accession, raw_text, steps, method_ids):
    """
    Create/overwrite a dataset JSON file for ENCODE accessions using extracted metadata steps.
    """
    try:
        from publications.code_examples import (
            save_dataset_content,
            lookup_pub_date,
            is_dataset_locked,
            get_registry,
        )
    except Exception as e:
        _log(f"  Warning: could not import code_examples helpers for {accession}: {e}")
        return

    source_name = "encode_metadata_processing"
    registry = get_registry() or {}
    existing = registry.get(accession, {}) if isinstance(registry, dict) else {}
    existing_locked = bool(existing.get("locked", True))
    has_prior_extract = (
        str(existing.get("raw_text_source", "")).strip() == source_name
        and bool(str(existing.get("raw_text", "")).strip())
    )

    if has_prior_extract and is_dataset_locked(accession):
        _log(f"  Skipping code_examples overwrite for {accession} (locked=true)")
        return

    json_steps = []
    for step in steps:
        desc = step.get("description", "").strip()
        if not desc:
            continue
        tool_name, tool_version, _ = _detect_tool_in_step(desc, method_ids)
        json_steps.append({
            "step_order": len(json_steps) + 1,
            "description": desc,
            "tool_name": tool_name or "",
            "tool_version": tool_version or "",
            "code_example": "",
            "code_language": "bash",
            "github_url": "",
        })

    payload = {
        "steps": json_steps,
        "raw_text": raw_text,
        "raw_text_source": source_name,
        "locked": existing_locked if existing else True,
    }
    content = json.dumps(payload, indent=2)
    year, month = lookup_pub_date(accession)
    kwargs = {}
    if year and month:
        kwargs["year"] = year
        kwargs["month"] = month
    try:
        save_dataset_content(accession, content, **kwargs)
    except Exception as e:
        _log(f"  Warning: failed to write code_examples for {accession}: {e}")


def _resolve_encode_pipeline_pmid(cur, accession_id, pmids):
    """
    Resolve a valid PMID for an ENCODE accession so we can satisfy
    analysis_pipelines.pmid NOT NULL + FK constraints.
    """
    # Prefer PMIDs attached from ENCODE references if they exist in publications.
    for pmid in pmids or []:
        cur.execute("SELECT pmid FROM publications WHERE pmid = %s", [pmid])
        row = cur.fetchone()
        if row:
            return row[0]

    # Fall back to any already linked publication for this accession.
    cur.execute(
        """SELECT pd.pmid
           FROM publication_datasets pd
           WHERE pd.accession_id = %s
           ORDER BY pd.pmid
           LIMIT 1""",
        [accession_id],
    )
    row = cur.fetchone()
    if row:
        return row[0]
    return None


def _load_encode_backfill_candidates():
    """
    Load existing ENCODE experiment accessions from DB for metadata backfill.
    This lets ENCODE bulk update backfill processing steps for prior ENCSR rows.
    """
    candidates = {}
    with connection.cursor() as cur:
        cur.execute(
            """SELECT accession_id, accession, title, summary, experiment_types
               FROM dataset_accessions
               WHERE database = 'ENCODE' AND accession LIKE 'ENCSR%'"""
        )
        rows = cur.fetchall()
        for accession_id, accession, title, summary, experiment_types in rows:
            assay_title = ""
            if experiment_types:
                try:
                    parsed = json.loads(experiment_types)
                    if isinstance(parsed, list) and parsed:
                        assay_title = str(parsed[0]).strip()
                    elif isinstance(parsed, str):
                        assay_title = parsed.strip()
                except Exception:
                    assay_title = str(experiment_types).strip()
            candidates[accession] = {
                "accession": accession,
                "accession_id": accession_id,
                "assay_title": assay_title,
                "description": (summary or title or "").strip(),
                "biosample_summary": "",
                "pmids": [],
                "_files": [],
            }

        if not candidates:
            return candidates

        ids = [item["accession_id"] for item in candidates.values()]
        placeholders = ",".join(["%s"] * len(ids))
        cur.execute(
            f"""SELECT accession_id, file_name, file_type
                FROM dataset_files
                WHERE accession_id IN ({placeholders})""",
            ids,
        )
        for accession_id, file_name, file_type in cur.fetchall():
            target = None
            for item in candidates.values():
                if item["accession_id"] == accession_id:
                    target = item
                    break
            if target is None:
                continue
            entry = {}
            file_name = (file_name or "").strip()
            file_type = (file_type or "").strip()
            if file_name and "." in file_name:
                entry["file_format"] = file_name.rsplit(".", 1)[-1]
            if file_type:
                entry["output_type"] = file_type.split(";", 1)[0].strip()
                if "mapping assembly:" in file_type.lower():
                    for part in file_type.split(";"):
                        if part.lower().strip().startswith("mapping assembly:"):
                            entry["mapping_assembly"] = part.split(":", 1)[-1].strip()
                elif "assembly:" in file_type.lower():
                    for part in file_type.split(";"):
                        if part.lower().strip().startswith("assembly:"):
                            entry["assembly"] = part.split(":", 1)[-1].strip()
            target["_files"].append(entry)

    return candidates


def _run_encode_update(grant_list, skip_files, skip_details):
    """Background worker for ENCODE update."""
    try:
        _log(f"Starting ENCODE update for grants: {', '.join(grant_list)}")
        total_experiments = 0
        total_files_added = 0
        total_links = 0
        encode_pipeline_count = 0
        encode_step_count = 0
        encode_pipeline_skipped_no_pmid = 0
        encode_json_synced = 0

        # Ensure analysis/method tables are present when ENCODE update runs standalone.
        _ensure_pipeline_tables()
        _ensure_methods_tables()

        # Step 1: Find experiments for each grant
        all_experiments = {}  # accession -> experiment dict
        for grant in grant_list:
            _log(f"[1/5] Searching ENCODE experiments for {grant}...")
            results = _encode_search_experiments_for_grant(grant)
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
            _log(f"[2/5] Fetching experiment details...")
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
                    exp["_detail"] = detail
                except Exception as e:
                    pass  # Non-fatal
        else:
            _log(f"[2/5] Skipping experiment details")

        # Step 3: Insert experiments into dataset_accessions and link to PMIDs
        _log(f"[3/5] Importing {total_experiments} experiments into database...")
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
                exp["accession_id"] = acc_id

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
            _log(f"[4/5] Fetching file metadata...")
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
                    all_experiments.get(acc, {})["_files"] = files
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
            _log(f"[4/5] Skipping file metadata")

        # Step 5: Build ENCODE processing pipelines from metadata.
        _log(f"[5/5] Building ENCODE metadata processing pipelines...")
        try:
            method_ids = _get_method_ids()
        except Exception:
            method_ids = {}

        # Include existing ENCODE rows so processing steps are backfilled, not only newly fetched rows.
        processing_experiments = dict(all_experiments)
        for acc, existing_exp in _load_encode_backfill_candidates().items():
            processing_experiments.setdefault(acc, existing_exp)

        with connection.cursor() as cur:
            for acc, exp in processing_experiments.items():
                acc_id = exp.get("accession_id")
                if not acc_id:
                    cur.execute(
                        "SELECT accession_id FROM dataset_accessions WHERE accession = %s",
                        [acc],
                    )
                    row = cur.fetchone()
                    acc_id = row[0] if row else None
                if not acc_id:
                    continue
                steps, raw_text = _extract_encode_processing_steps(
                    exp,
                    detail=exp.get("_detail"),
                    files=exp.get("_files") or [],
                )
                if not steps:
                    continue

                _write_encode_steps_to_code_examples(
                    accession=acc,
                    raw_text=raw_text,
                    steps=steps,
                    method_ids=method_ids,
                )
                encode_json_synced += 1

                pipeline_pmid = _resolve_encode_pipeline_pmid(cur, acc_id, exp.get("pmids", []))
                if not pipeline_pmid:
                    encode_pipeline_skipped_no_pmid += 1
                    continue

                # Refresh ENCODE-derived pipeline for this accession on each bulk update.
                cur.execute(
                    """DELETE FROM pipeline_steps
                       WHERE pipeline_id IN (
                           SELECT pipeline_id FROM analysis_pipelines
                           WHERE accession_id = %s AND source = 'encode_metadata_processing'
                       )""",
                    [acc_id],
                )
                cur.execute(
                    """DELETE FROM analysis_pipelines
                       WHERE accession_id = %s AND source = 'encode_metadata_processing'""",
                    [acc_id],
                )

                pipeline_title = f"{acc} Data Processing"
                pid = _insert_pipeline(
                    cur=cur,
                    pmid=pipeline_pmid,
                    accession_id=acc_id,
                    assay_type=exp.get("assay_title", ""),
                    title=pipeline_title,
                    source="encode_metadata_processing",
                    raw_text=raw_text,
                    steps=steps,
                    method_ids=method_ids,
                    accession_str=acc,
                )
                if pid:
                    encode_pipeline_count += 1
                    encode_step_count += len(steps)

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
            stats["encode_pipelines"] = encode_pipeline_count
            stats["encode_pipeline_steps"] = encode_step_count
            stats["encode_pipeline_skipped_no_pmid"] = encode_pipeline_skipped_no_pmid
            stats["encode_json_synced"] = encode_json_synced

        _set_status(stats=stats)
        _log(
            f"ENCODE update complete! {total_experiments} experiments, "
            f"{total_files_added} files, {total_links} links, "
            f"{encode_pipeline_count} processing pipelines, "
            f"{encode_json_synced} JSON files synced."
        )

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


# ============================================================
# Computational Methods Extraction
# ============================================================

# Comprehensive bioinformatics software/method dictionary
# Format: canonical_name -> {aliases, category, url}
_SOFTWARE_DICT = {
    # --- Alignment / Mapping ---
    "STAR": {"aliases": ["STAR aligner", "STARsolo", "STAR-Fusion"], "category": "Alignment", "url": "https://github.com/alexdobin/STAR"},
    "Bowtie": {"aliases": ["Bowtie1", "bowtie 1"], "category": "Alignment", "url": "http://bowtie-bio.sourceforge.net"},
    "Bowtie2": {"aliases": ["bowtie 2", "Bowtie 2"], "category": "Alignment", "url": "http://bowtie-bio.sourceforge.net/bowtie2"},
    "BWA": {"aliases": ["bwa-mem", "BWA-MEM", "BWA-MEM2", "bwa mem"], "category": "Alignment", "url": "https://github.com/lh3/bwa"},
    "HISAT2": {"aliases": ["HISAT", "hisat2"], "category": "Alignment", "url": "http://daehwankimlab.github.io/hisat2"},
    "TopHat": {"aliases": ["TopHat2", "tophat2"], "category": "Alignment", "url": ""},
    "minimap2": {"aliases": ["Minimap2"], "category": "Alignment", "url": "https://github.com/lh3/minimap2"},
    "Novoalign": {"aliases": ["novoalign"], "category": "Alignment", "url": ""},
    "GSNAP": {"aliases": ["gsnap"], "category": "Alignment", "url": ""},
    "Salmon": {"aliases": ["salmon"], "category": "Quantification", "url": "https://combine-lab.github.io/salmon"},
    "kallisto": {"aliases": ["Kallisto"], "category": "Quantification", "url": "https://pachterlab.github.io/kallisto"},
    "RSEM": {"aliases": ["rsem"], "category": "Quantification", "url": "https://github.com/deweylab/RSEM"},
    "featureCounts": {"aliases": ["Subread", "subread"], "category": "Quantification", "url": "http://subread.sourceforge.net"},
    "HTSeq": {"aliases": ["htseq-count", "HTSeq-count"], "category": "Quantification", "url": "https://htseq.readthedocs.io"},
    "Cufflinks": {"aliases": ["Cuffdiff", "Cuffquant", "Cuffcompare"], "category": "Quantification", "url": ""},
    # --- Differential Expression ---
    "DESeq2": {"aliases": ["DESeq", "DEseq2", "DESeq 2"], "category": "Differential Expression", "url": "https://bioconductor.org/packages/DESeq2"},
    "edgeR": {"aliases": ["EdgeR"], "category": "Differential Expression", "url": "https://bioconductor.org/packages/edgeR"},
    "limma": {"aliases": ["limma-voom", "limma voom", "voom"], "category": "Differential Expression", "url": "https://bioconductor.org/packages/limma"},
    "StringTie": {"aliases": ["Stringtie", "stringtie"], "category": "Transcript Assembly", "url": "https://ccb.jhu.edu/software/stringtie"},
    # --- CLIP / RBP Analysis ---
    "CLIPper": {"aliases": ["clipper", "CLIPPER"], "category": "CLIP Analysis", "url": "https://github.com/YeoLab/clipper"},
    "Skipper": {"aliases": ["skipper"], "category": "CLIP Analysis", "url": "https://github.com/YeoLab/skipper"},
    "PureCLIP": {"aliases": ["pureclip", "PureClip"], "category": "CLIP Analysis", "url": "https://github.com/skrakau/PureCLIP"},
    "Piranha": {"aliases": ["piranha"], "category": "CLIP Analysis", "url": ""},
    "CLAM": {"aliases": ["clam"], "category": "CLIP Analysis", "url": ""},
    "CTK": {"aliases": ["CIMS", "CITS"], "category": "CLIP Analysis", "url": ""},
    "MANDALORION": {"aliases": ["Mandalorion"], "category": "CLIP Analysis", "url": ""},
    "rMAPS": {"aliases": ["rMAPS2", "rmaps"], "category": "Splicing Analysis", "url": ""},
    "MISO": {"aliases": ["miso"], "category": "Splicing Analysis", "url": ""},
    "rMATS": {"aliases": ["rmats", "rMATS-turbo"], "category": "Splicing Analysis", "url": "https://github.com/Xinglab/rmats-turbo"},
    "SUPPA": {"aliases": ["SUPPA2", "suppa"], "category": "Splicing Analysis", "url": ""},
    "Leafcutter": {"aliases": ["leafcutter", "LeafCutter"], "category": "Splicing Analysis", "url": ""},
    "MAJIQ": {"aliases": ["majiq"], "category": "Splicing Analysis", "url": ""},
    "Whippet": {"aliases": ["whippet"], "category": "Splicing Analysis", "url": ""},
    "SplAdder": {"aliases": ["spladder"], "category": "Splicing Analysis", "url": ""},
    "RBNS": {"aliases": ["rbns"], "category": "RBP Binding", "url": ""},
    "RBPmap": {"aliases": ["rbpmap"], "category": "RBP Binding", "url": ""},
    # --- Peak Calling ---
    "MACS2": {"aliases": ["MACS", "MACS 2", "macs2", "MACS3"], "category": "Peak Calling", "url": "https://github.com/macs3-project/MACS"},
    "HOMER": {"aliases": ["Homer", "homer"], "category": "Peak Calling / Motif", "url": "http://homer.ucsd.edu"},
    "IDR": {"aliases": ["idr", "irreproducible discovery rate"], "category": "Peak Calling", "url": ""},
    # --- Single-Cell ---
    "Cell Ranger": {"aliases": ["CellRanger", "cellranger", "Cell Ranger ARC", "cellranger-arc", "cellranger-atac"], "category": "Single-Cell", "url": "https://support.10xgenomics.com"},
    "Seurat": {"aliases": ["seurat"], "category": "Single-Cell", "url": "https://satijalab.org/seurat"},
    "Scanpy": {"aliases": ["scanpy", "scverse"], "category": "Single-Cell", "url": "https://scanpy.readthedocs.io"},
    "scVI": {"aliases": ["scvi-tools", "scvi", "scANVI", "totalVI", "PeakVI", "MultiVI", "DestVI"], "category": "Single-Cell", "url": "https://scvi-tools.org"},
    "Velocyto": {"aliases": ["velocyto", "scVelo", "scvelo", "RNA velocity"], "category": "Single-Cell", "url": "http://velocyto.org"},
    "Monocle": {"aliases": ["monocle2", "Monocle3", "monocle3", "Monocle 3"], "category": "Single-Cell", "url": ""},
    "Harmony": {"aliases": ["harmony"], "category": "Single-Cell", "url": ""},
    "LIGER": {"aliases": ["liger", "rliger"], "category": "Single-Cell", "url": ""},
    "ArchR": {"aliases": ["archr"], "category": "Single-Cell", "url": ""},
    "Signac": {"aliases": ["signac"], "category": "Single-Cell", "url": ""},
    "CellChat": {"aliases": ["cellchat"], "category": "Single-Cell", "url": ""},
    "CellTypist": {"aliases": ["celltypist"], "category": "Single-Cell", "url": ""},
    "scArches": {"aliases": ["scarches"], "category": "Single-Cell", "url": ""},
    # --- QC / Preprocessing ---
    "FastQC": {"aliases": ["fastqc"], "category": "QC", "url": "https://www.bioinformatics.babraham.ac.uk/projects/fastqc"},
    "MultiQC": {"aliases": ["multiqc"], "category": "QC", "url": "https://multiqc.info"},
    "Trimmomatic": {"aliases": ["trimmomatic"], "category": "Read Processing", "url": ""},
    "cutadapt": {"aliases": ["Cutadapt", "CutAdapt"], "category": "Read Processing", "url": "https://cutadapt.readthedocs.io"},
    "Trim Galore": {"aliases": ["TrimGalore", "trim_galore", "trim galore"], "category": "Read Processing", "url": ""},
    "fastp": {"aliases": ["Fastp"], "category": "Read Processing", "url": "https://github.com/OpenGene/fastp"},
    "UMI-tools": {"aliases": ["umi_tools", "UMI tools", "umi-tools"], "category": "Read Processing", "url": ""},
    # --- Variant / Editing ---
    "GATK": {"aliases": ["HaplotypeCaller", "Mutect2", "MuTect"], "category": "Variant Calling", "url": "https://gatk.broadinstitute.org"},
    "SAILOR": {"aliases": ["sailor"], "category": "RNA Editing", "url": "https://github.com/YeoLab/sailor"},
    "MARINE": {"aliases": ["marine"], "category": "RNA Editing", "url": ""},
    "REDItools": {"aliases": ["reditools"], "category": "RNA Editing", "url": ""},
    "JACUSA": {"aliases": ["jacusa", "JACUSA2"], "category": "RNA Editing", "url": ""},
    # --- Genome / Sequence Tools ---
    "samtools": {"aliases": ["SAMtools", "Samtools", "htslib"], "category": "Genomic Utilities", "url": "http://www.htslib.org"},
    "bedtools": {"aliases": ["BEDTools", "Bedtools"], "category": "Genomic Utilities", "url": "https://bedtools.readthedocs.io"},
    "Picard": {"aliases": ["picard"], "category": "Genomic Utilities", "url": "https://broadinstitute.github.io/picard"},
    "deepTools": {"aliases": ["deeptools", "DeepTools"], "category": "Genomic Utilities", "url": "https://deeptools.readthedocs.io"},
    "UCSC tools": {"aliases": ["UCSC Genome Browser", "liftOver", "bigWig", "bigBed", "bedGraphToBigWig", "wigToBigWig"], "category": "Genomic Utilities", "url": "https://genome.ucsc.edu"},
    "IGV": {"aliases": ["Integrative Genomics Viewer"], "category": "Visualization", "url": "https://igv.org"},
    # --- Motif Analysis ---
    "MEME": {"aliases": ["MEME Suite", "MEME-ChIP", "meme-chip"], "category": "Motif Analysis", "url": "https://meme-suite.org"},
    "DREME": {"aliases": ["dreme"], "category": "Motif Analysis", "url": "https://meme-suite.org"},
    "FIMO": {"aliases": ["fimo"], "category": "Motif Analysis", "url": "https://meme-suite.org"},
    "DeepBind": {"aliases": ["deepbind"], "category": "Motif Analysis", "url": ""},
    # --- Pathway / Enrichment ---
    "GSEA": {"aliases": ["Gene Set Enrichment Analysis", "gsea", "fgsea"], "category": "Pathway Analysis", "url": "https://www.gsea-msigdb.org"},
    "DAVID": {"aliases": ["david"], "category": "Pathway Analysis", "url": "https://david.ncifcrf.gov"},
    "Enrichr": {"aliases": ["enrichr"], "category": "Pathway Analysis", "url": "https://maayanlab.cloud/Enrichr"},
    "clusterProfiler": {"aliases": ["ClusterProfiler", "clusterprofiler"], "category": "Pathway Analysis", "url": ""},
    "Gene Ontology": {"aliases": ["GO analysis", "GO enrichment", "gene ontology"], "category": "Pathway Analysis", "url": "http://geneontology.org"},
    "KEGG": {"aliases": ["kegg", "KEGG pathway"], "category": "Pathway Analysis", "url": "https://www.genome.jp/kegg"},
    "Reactome": {"aliases": ["reactome"], "category": "Pathway Analysis", "url": "https://reactome.org"},
    "Metascape": {"aliases": ["metascape"], "category": "Pathway Analysis", "url": "https://metascape.org"},
    "PANTHER": {"aliases": ["panther"], "category": "Pathway Analysis", "url": "http://pantherdb.org"},
    # --- Machine Learning / Deep Learning ---
    "TensorFlow": {"aliases": ["tensorflow", "Tensorflow"], "category": "Machine Learning", "url": "https://www.tensorflow.org"},
    "PyTorch": {"aliases": ["pytorch", "Pytorch"], "category": "Machine Learning", "url": "https://pytorch.org"},
    "Keras": {"aliases": ["keras"], "category": "Machine Learning", "url": "https://keras.io"},
    "scikit-learn": {"aliases": ["sklearn", "scikit learn", "Scikit-learn"], "category": "Machine Learning", "url": "https://scikit-learn.org"},
    "XGBoost": {"aliases": ["xgboost"], "category": "Machine Learning", "url": ""},
    "Random Forest": {"aliases": ["random forest", "RandomForest"], "category": "Machine Learning", "url": ""},
    "SVM": {"aliases": ["support vector machine", "Support Vector Machine"], "category": "Machine Learning", "url": ""},
    "Neural Network": {"aliases": ["neural network", "deep neural network", "DNN", "CNN", "convolutional neural network", "RNN", "recurrent neural network", "transformer", "LSTM"], "category": "Machine Learning", "url": ""},
    "AlphaFold": {"aliases": ["alphafold", "AlphaFold2", "AlphaFold 2"], "category": "Structure Prediction", "url": "https://alphafold.ebi.ac.uk"},
    "RoseTTAFold": {"aliases": ["rosettafold"], "category": "Structure Prediction", "url": ""},
    # --- Statistical / Dimensionality ---
    "PCA": {"aliases": ["principal component analysis", "Principal Component Analysis"], "category": "Statistics", "url": ""},
    "t-SNE": {"aliases": ["tSNE", "t-distributed stochastic neighbor embedding"], "category": "Statistics", "url": ""},
    "UMAP": {"aliases": ["umap"], "category": "Statistics", "url": "https://umap-learn.readthedocs.io"},
    "Leiden clustering": {"aliases": ["Leiden", "leiden algorithm", "Leiden algorithm"], "category": "Statistics", "url": ""},
    "Louvain clustering": {"aliases": ["Louvain", "louvain algorithm"], "category": "Statistics", "url": ""},
    # --- Workflow / Pipeline ---
    "Nextflow": {"aliases": ["nextflow", "nf-core"], "category": "Workflow", "url": "https://nextflow.io"},
    "Snakemake": {"aliases": ["snakemake"], "category": "Workflow", "url": "https://snakemake.readthedocs.io"},
    "WDL": {"aliases": ["Cromwell", "cromwell"], "category": "Workflow", "url": ""},
    # --- Languages / Frameworks ---
    "R": {"aliases": ["R language", "R/Bioconductor", "Bioconductor"], "category": "Language", "url": "https://www.r-project.org"},
    "Python": {"aliases": ["python", "Python 3"], "category": "Language", "url": "https://python.org"},
    "MATLAB": {"aliases": ["matlab", "Matlab"], "category": "Language", "url": ""},
    # --- Databases / References ---
    "GENCODE": {"aliases": ["gencode", "Gencode"], "category": "Reference", "url": "https://www.gencodegenes.org"},
    "Ensembl": {"aliases": ["ensembl"], "category": "Reference", "url": "https://ensembl.org"},
    "RefSeq": {"aliases": ["refseq", "NCBI RefSeq"], "category": "Reference", "url": ""},
    "UniProt": {"aliases": ["uniprot", "UniProtKB", "Swiss-Prot"], "category": "Reference", "url": "https://www.uniprot.org"},
    "BLAST": {"aliases": ["blast", "BLASTN", "BLASTP", "BLASTX", "BLASTn", "BLASTp"], "category": "Sequence Search", "url": "https://blast.ncbi.nlm.nih.gov"},
    "HMMER": {"aliases": ["hmmer"], "category": "Sequence Search", "url": "http://hmmer.org"},
    # --- Assembly ---
    "Trinity": {"aliases": ["trinity"], "category": "Assembly", "url": "https://github.com/trinityrnaseq/trinityrnaseq"},
    "SPAdes": {"aliases": ["spades"], "category": "Assembly", "url": "https://github.com/ablab/spades"},
    "FLAIR": {"aliases": ["flair"], "category": "Long-Read Analysis", "url": "https://github.com/BrooksLabUCSC/flair"},
    "IsoSeq": {"aliases": ["Iso-Seq", "iso-seq", "IsoSeq3"], "category": "Long-Read Analysis", "url": ""},
    "TALON": {"aliases": ["talon"], "category": "Long-Read Analysis", "url": ""},
    "Bambu": {"aliases": ["bambu"], "category": "Long-Read Analysis", "url": ""},
    # --- Yeo Lab specific tools ---
    "ENCODE pipeline": {"aliases": ["ENCODE RNA-seq pipeline", "ENCODE eCLIP pipeline", "ENCODE CLIP pipeline", "ENCODE processing pipeline"], "category": "Pipeline", "url": "https://www.encodeproject.org"},
    "Mudskipper": {"aliases": ["mudskipper"], "category": "CLIP Analysis", "url": ""},
    "Oligomap": {"aliases": ["oligomap"], "category": "Sequence Analysis", "url": ""},
    "FLAM-seq": {"aliases": ["flam-seq", "FLAM-Seq"], "category": "Sequencing Method", "url": ""},
}

# Assay type patterns (detected from abstracts + GEO)
_ASSAY_PATTERNS = {
    "eCLIP": {"aliases": ["eCLIP-seq", "enhanced CLIP"], "category": "Assay"},
    "iCLIP": {"aliases": ["iCLIP2", "individual-nucleotide resolution CLIP"], "category": "Assay"},
    "CLIP-seq": {"aliases": ["CLIP sequencing", "UV crosslinking and immunoprecipitation"], "category": "Assay"},
    "HITS-CLIP": {"aliases": ["HITS-CLIP"], "category": "Assay"},
    "PAR-CLIP": {"aliases": ["PAR-CLIP"], "category": "Assay"},
    "irCLIP": {"aliases": ["infrared CLIP"], "category": "Assay"},
    "RNA-seq": {"aliases": ["RNA sequencing", "RNAseq", "mRNA-seq", "mRNA sequencing", "bulk RNA-seq"], "category": "Assay"},
    "scRNA-seq": {"aliases": ["single-cell RNA-seq", "single cell RNA-seq", "10x Genomics", "Drop-seq", "Smart-seq", "Smart-seq2", "10X Chromium"], "category": "Assay"},
    "snRNA-seq": {"aliases": ["single-nucleus RNA-seq", "single nucleus RNA-seq"], "category": "Assay"},
    "ATAC-seq": {"aliases": ["ATAC sequencing", "ATACseq", "scATAC-seq", "single-cell ATAC-seq"], "category": "Assay"},
    "ChIP-seq": {"aliases": ["ChIP sequencing", "ChIPseq", "CUT&RUN", "CUT&Tag"], "category": "Assay"},
    "Ribo-seq": {"aliases": ["ribosome profiling", "Ribo-Seq", "ribosome footprinting"], "category": "Assay"},
    "SHAPE-seq": {"aliases": ["SHAPE-MaP", "DMS-seq", "icSHAPE", "SHAPE sequencing"], "category": "Assay"},
    "Nanopore sequencing": {"aliases": ["Oxford Nanopore", "ONT sequencing", "nanopore", "MinION", "PromethION"], "category": "Assay"},
    "PacBio sequencing": {"aliases": ["PacBio", "SMRT sequencing", "Iso-Seq", "HiFi sequencing", "long-read sequencing"], "category": "Assay"},
    "Multiome": {"aliases": ["multiome", "10x Multiome", "RNA+ATAC", "joint profiling"], "category": "Assay"},
    "Spatial transcriptomics": {"aliases": ["spatial transcriptomics", "MERFISH", "Slide-seq", "Visium", "FISH"], "category": "Assay"},
    "Hi-C": {"aliases": ["Hi-C", "BL-Hi-C", "HiC"], "category": "Assay"},
    "Mass spectrometry": {"aliases": ["mass spectrometry", "LC-MS", "MS/MS", "proteomics", "TMT", "iTRAQ", "SILAC"], "category": "Assay"},
    "Whole genome sequencing": {"aliases": ["WGS", "whole genome sequencing", "whole-genome sequencing"], "category": "Assay"},
    "Whole exome sequencing": {"aliases": ["WES", "whole exome sequencing", "whole-exome sequencing", "exome sequencing"], "category": "Assay"},
    "Microarray": {"aliases": ["microarray", "gene expression array", "splicing array", "Affymetrix", "Agilent"], "category": "Assay"},
}

# Merge assay patterns into the main dict
for _name, _info in _ASSAY_PATTERNS.items():
    _SOFTWARE_DICT[_name] = {**_info, "url": ""}

_VERSION_RE = re.compile(
    r'(?:v(?:ersion)?\s*)?(\d+\.\d+(?:\.\d+)?(?:[a-z])?)',
    re.IGNORECASE
)

_SRA_STRATEGY_MAP = {
    "RNA-Seq": "RNA-seq",
    "CLIP-Seq": "CLIP-seq",
    "ChIP-Seq": "ChIP-seq",
    "ATAC-seq": "ATAC-seq",
    "ATAC-Seq": "ATAC-seq",
    "Bisulfite-Seq": "Bisulfite sequencing",
    "Hi-C": "Hi-C",
    "WGS": "Whole genome sequencing",
    "WXS": "Whole exome sequencing",
    "miRNA-Seq": "miRNA-seq",
}

_SRA_PLATFORM_MAP = {
    "ILLUMINA": ("Illumina", "Sequencing Platform"),
    "OXFORD_NANOPORE": ("Nanopore sequencing", "Assay"),
    "PACBIO_SMRT": ("PacBio sequencing", "Assay"),
    "ION_TORRENT": ("Ion Torrent", "Sequencing Platform"),
}


def _extract_methods_from_text(text, source_type="abstract"):
    """Scan text for software/method mentions. Returns list of dicts."""
    if not text:
        return []

    found = []
    seen = set()

    for canonical, info in _SOFTWARE_DICT.items():
        all_names = [canonical] + info.get("aliases", [])

        for alias in all_names:
            if len(alias) <= 3 and alias.isupper():
                pattern = r'\b' + re.escape(alias) + r'\b'
                flags = 0
            elif len(alias) <= 2:
                if alias == "R":
                    pattern = r'\bR\s*/\s*Bioconductor|\bR\s+\(|\bR\s+package|\bR\s+software|\bin R\b'
                    flags = 0
                else:
                    continue
            else:
                pattern = r'\b' + re.escape(alias) + r'\b'
                flags = re.IGNORECASE if len(alias) > 4 else 0

            match = re.search(pattern, text, flags)
            if match and canonical not in seen:
                seen.add(canonical)

                version = None
                after_text = text[match.end():match.end() + 40]
                ver_match = _VERSION_RE.search(after_text)
                if ver_match and ver_match.start() < 15:
                    version = ver_match.group(1)

                found.append({
                    "name": canonical,
                    "version": version,
                    "category": info.get("category", "Unknown"),
                    "source_type": source_type,
                    "matched_alias": alias if alias != canonical else None,
                    "url": info.get("url", ""),
                })
                break

    return found


def _extract_from_keywords(keywords_str, mesh_str):
    """Extract method hints from keywords and MeSH terms."""
    try:
        kw_list = json.loads(keywords_str) if keywords_str else []
    except (json.JSONDecodeError, TypeError):
        kw_list = [k.strip() for k in (keywords_str or "").split(";") if k.strip()]

    try:
        mesh_list = json.loads(mesh_str) if mesh_str else []
    except (json.JSONDecodeError, TypeError):
        mesh_list = [m.strip() for m in (mesh_str or "").split(";") if m.strip()]

    all_terms = " ".join(kw_list + mesh_list)
    return _extract_methods_from_text(all_terms, source_type="keywords_mesh")


def _extract_from_sra(library_strategy, platform, instrument):
    """Extract assay/platform info from SRA metadata."""
    found = []

    if library_strategy and library_strategy in _SRA_STRATEGY_MAP and _SRA_STRATEGY_MAP[library_strategy]:
        found.append({
            "name": _SRA_STRATEGY_MAP[library_strategy],
            "version": None,
            "category": "Assay",
            "source_type": "sra_metadata",
            "matched_alias": library_strategy,
            "url": "",
        })

    if platform and platform in _SRA_PLATFORM_MAP:
        name, cat = _SRA_PLATFORM_MAP[platform]
        found.append({
            "name": name,
            "version": None,
            "category": cat,
            "source_type": "sra_metadata",
            "matched_alias": platform,
            "url": "",
        })

    return found


def _ensure_methods_tables():
    """Create computational_methods and publication_methods tables if they don't exist."""
    with connection.cursor() as cur:
        if _is_postgres():
            cur.execute("""
                CREATE TABLE IF NOT EXISTS computational_methods (
                    method_id SERIAL PRIMARY KEY,
                    canonical_name TEXT NOT NULL UNIQUE,
                    category TEXT NOT NULL,
                    url TEXT,
                    description TEXT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS publication_methods (
                    id SERIAL PRIMARY KEY,
                    pmid TEXT NOT NULL REFERENCES publications(pmid),
                    method_id INTEGER NOT NULL REFERENCES computational_methods(method_id),
                    version TEXT,
                    source_type TEXT NOT NULL,
                    matched_text TEXT,
                    UNIQUE(pmid, method_id, source_type)
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pub_methods_pmid ON publication_methods(pmid)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pub_methods_method ON publication_methods(method_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_methods_category ON computational_methods(category)")
        else:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS computational_methods (
                    method_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    canonical_name TEXT NOT NULL,
                    category TEXT NOT NULL,
                    url TEXT,
                    description TEXT,
                    UNIQUE(canonical_name)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS publication_methods (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pmid TEXT NOT NULL REFERENCES publications(pmid),
                    method_id INTEGER NOT NULL REFERENCES computational_methods(method_id),
                    version TEXT,
                    source_type TEXT NOT NULL,
                    matched_text TEXT,
                    UNIQUE(pmid, method_id, source_type)
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pub_methods_pmid ON publication_methods(pmid)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pub_methods_method ON publication_methods(method_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_methods_category ON computational_methods(category)")


def start_methods_update():
    """
    Start a background methods extraction update.
    Returns True if started, False if already running.
    No external dependencies required — works from existing DB data.
    """
    return _start_background_worker(
        target=_run_methods_update,
        args=(),
        progress_msg="Starting methods extraction...",
    )


def _run_methods_update():
    """Background worker for methods extraction."""
    try:
        _log("Starting computational methods extraction...")

        # Ensure tables exist
        _log("Ensuring computational_methods and publication_methods tables exist...")
        _ensure_methods_tables()

        # Clear existing data for a clean re-extraction
        with connection.cursor() as cur:
            cur.execute("DELETE FROM publication_methods")
            cur.execute("DELETE FROM computational_methods")
        _log("Cleared existing methods data for fresh extraction")

        all_methods = defaultdict(list)  # pmid -> [method_dicts]

        # Step 1: Extract from abstracts + keywords/MeSH
        _log("[1/4] Extracting methods from publication abstracts and keywords...")
        with connection.cursor() as cur:
            cur.execute("SELECT pmid, abstract, keywords, mesh_terms FROM publications")
            pubs = cur.fetchall()

        for pmid, abstract, keywords, mesh_terms in pubs:
            methods = _extract_methods_from_text(abstract, "abstract")
            all_methods[pmid].extend(methods)

            kw_methods = _extract_from_keywords(keywords, mesh_terms)
            all_methods[pmid].extend(kw_methods)

        abstract_count = sum(len(v) for v in all_methods.values())
        pubs_with = len([k for k, v in all_methods.items() if v])
        _log(f"  Found {abstract_count} method mentions across {pubs_with} publications")

        # Step 2: Extract from GEO dataset metadata
        _log("[2/4] Extracting methods from GEO dataset metadata...")
        with connection.cursor() as cur:
            cur.execute("""
                SELECT da.accession_id, da.accession, da.title, da.summary,
                       da.overall_design, pd.pmid
                FROM dataset_accessions da
                JOIN publication_datasets pd ON pd.accession_id = da.accession_id
                WHERE da.accession_type = 'GSE'
                  AND (da.summary IS NOT NULL OR da.overall_design IS NOT NULL)
            """)
            geo_rows = cur.fetchall()

        geo_count = 0
        for acc_id, accession, title, summary, design, pmid in geo_rows:
            text = " ".join(filter(None, [title, summary, design]))
            methods = _extract_methods_from_text(text, "geo_metadata")
            for m in methods:
                m["matched_alias"] = m.get("matched_alias") or accession
            all_methods[pmid].extend(methods)
            geo_count += len(methods)

        _log(f"  Found {geo_count} method mentions from {len(geo_rows)} GEO datasets")

        # Step 3: Extract from SRA experiment metadata
        _log("[3/4] Extracting methods from SRA experiment metadata...")
        with connection.cursor() as cur:
            cur.execute("""
                SELECT se.experiment_id, se.srx_accession, se.library_strategy,
                       se.library_source, se.platform, se.instrument_model,
                       se.source_gse, da.accession, pd.pmid
                FROM sra_experiments se
                LEFT JOIN dataset_accessions da ON se.parent_accession_id = da.accession_id
                LEFT JOIN publication_datasets pd ON pd.accession_id = da.accession_id
                WHERE pd.pmid IS NOT NULL
            """)
            sra_rows = cur.fetchall()

        sra_count = 0
        seen_pmid_sra = set()
        for row in sra_rows:
            exp_id, srx, strategy, source, platform, instrument, gse, accession, pmid = row
            if (pmid, strategy, platform) in seen_pmid_sra:
                continue
            seen_pmid_sra.add((pmid, strategy, platform))

            methods = _extract_from_sra(strategy, platform, instrument)
            all_methods[pmid].extend(methods)
            sra_count += len(methods)

        _log(f"  Found {sra_count} method mentions from {len(sra_rows)} SRA experiments")

        # Step 4: Deduplicate and insert
        _log("[4/4] Deduplicating and inserting into database...")

        # Insert all unique methods
        all_canonical = {}
        for pmid, methods in all_methods.items():
            for m in methods:
                name = m["name"]
                if name not in all_canonical:
                    all_canonical[name] = {
                        "category": m["category"],
                        "url": m.get("url", ""),
                    }

        with connection.cursor() as cur:
            for name, info in all_canonical.items():
                cur.execute(
                    """INSERT INTO computational_methods (canonical_name, category, url)
                       VALUES (%s, %s, %s)
                       ON CONFLICT (canonical_name) DO NOTHING""",
                    [name, info["category"], info.get("url", "")],
                )

        _log(f"  Inserted {len(all_canonical)} unique methods/tools")

        # Build name->id map
        method_ids = {}
        with connection.cursor() as cur:
            cur.execute("SELECT method_id, canonical_name FROM computational_methods")
            for row in cur.fetchall():
                method_ids[row[1]] = row[0]

        # Insert publication_methods (deduplicated)
        inserted = 0
        with connection.cursor() as cur:
            for pmid, methods in all_methods.items():
                seen = set()
                for m in methods:
                    key = (pmid, m["name"], m["source_type"])
                    if key in seen:
                        continue
                    seen.add(key)

                    mid = method_ids.get(m["name"])
                    if mid:
                        try:
                            cur.execute(
                                """INSERT INTO publication_methods
                                   (pmid, method_id, version, source_type, matched_text)
                                   VALUES (%s, %s, %s, %s, %s)
                                   ON CONFLICT (pmid, method_id, source_type) DO NOTHING""",
                                [pmid, mid, m.get("version"),
                                 m["source_type"],
                                 m.get("matched_alias") or m.get("geo_accession")],
                            )
                            inserted += 1
                        except Exception:
                            pass

        _log(f"  Inserted {inserted} publication-method links")

        # Collect final stats
        with connection.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM computational_methods")
            total_methods = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM publication_methods")
            total_links = cur.fetchone()[0]

            cur.execute("SELECT COUNT(DISTINCT pmid) FROM publication_methods")
            pubs_with_methods = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM publications")
            total_pubs = cur.fetchone()[0]

            cur.execute("SELECT COUNT(DISTINCT version) FROM publication_methods WHERE version IS NOT NULL")
            version_count = cur.fetchone()[0]

            stats = {
                "methods": total_methods,
                "method_links": total_links,
                "pubs_with_methods": pubs_with_methods,
                "total_publications": total_pubs,
                "versions_found": version_count,
            }

        _set_status(stats=stats)
        coverage_pct = round(100 * pubs_with_methods / total_pubs) if total_pubs else 0
        _log(f"Methods extraction complete! {total_methods} methods, {total_links} links, "
             f"{pubs_with_methods}/{total_pubs} publications ({coverage_pct}% coverage)")

        # Log the update
        with connection.cursor() as cur:
            cur.execute(
                """INSERT INTO update_log (new_pmids_added, notes)
                   VALUES (%s, %s)""",
                [0, f"Methods extraction: {total_methods} methods, {total_links} links, "
                    f"{pubs_with_methods} publications"],
            )

    except Exception as e:
        _log(f"ERROR (methods): {e}")
        _set_status(error=str(e))
    finally:
        _set_status(running=False, finished_at=datetime.now().isoformat())


# ============================================================
# Analysis Pipeline Extraction
# ============================================================

# Metadata-only lines in GEO Data Processing (not actual analysis steps)
_GEO_DP_METADATA_PREFIXES = [
    "genome_build:",
    "genome build:",
    "supplementary_files_format_and_content:",
    "supplementary files format and content:",
    "assembly:",
]


def _ensure_pipeline_tables():
    """Create analysis_pipelines and pipeline_steps tables if they don't exist."""
    with connection.cursor() as cur:
        if _is_postgres():
            cur.execute("""
                CREATE TABLE IF NOT EXISTS analysis_pipelines (
                    pipeline_id SERIAL PRIMARY KEY,
                    pmid TEXT NOT NULL REFERENCES publications(pmid) ON DELETE CASCADE,
                    accession_id INTEGER REFERENCES dataset_accessions(accession_id) ON DELETE SET NULL,
                    assay_type TEXT,
                    pipeline_title TEXT,
                    source TEXT NOT NULL,
                    raw_text TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_steps (
                    step_id SERIAL PRIMARY KEY,
                    pipeline_id INTEGER NOT NULL REFERENCES analysis_pipelines(pipeline_id) ON DELETE CASCADE,
                    step_order INTEGER NOT NULL,
                    description TEXT NOT NULL,
                    method_id INTEGER REFERENCES computational_methods(method_id) ON DELETE SET NULL,
                    tool_name TEXT,
                    tool_version TEXT,
                    parameters TEXT,
                    UNIQUE(pipeline_id, step_order)
                )
            """)
        else:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS analysis_pipelines (
                    pipeline_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pmid TEXT NOT NULL REFERENCES publications(pmid) ON DELETE CASCADE,
                    accession_id INTEGER REFERENCES dataset_accessions(accession_id) ON DELETE SET NULL,
                    assay_type TEXT,
                    pipeline_title TEXT,
                    source TEXT NOT NULL,
                    raw_text TEXT,
                    created_at TEXT DEFAULT (datetime('now'))
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_steps (
                    step_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pipeline_id INTEGER NOT NULL REFERENCES analysis_pipelines(pipeline_id) ON DELETE CASCADE,
                    step_order INTEGER NOT NULL,
                    description TEXT NOT NULL,
                    method_id INTEGER REFERENCES computational_methods(method_id) ON DELETE SET NULL,
                    tool_name TEXT,
                    tool_version TEXT,
                    parameters TEXT,
                    UNIQUE(pipeline_id, step_order)
                )
            """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pipelines_pmid ON analysis_pipelines(pmid)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pipelines_accession ON analysis_pipelines(accession_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pipelines_assay ON analysis_pipelines(assay_type)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_steps_pipeline ON pipeline_steps(pipeline_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_steps_method ON pipeline_steps(method_id)")

        # --- Add code_example columns (idempotent for existing tables) ---
        for col, typ in [("code_example", "TEXT"), ("code_language", "TEXT"), ("github_url", "TEXT")]:
            try:
                cur.execute(f"ALTER TABLE pipeline_steps ADD COLUMN {col} {typ}")
            except Exception:
                pass  # Column already exists


def _classify_assay_from_text(text):
    """Detect assay type from text using _ASSAY_PATTERNS."""
    if not text:
        return None
    for assay_name, info in _ASSAY_PATTERNS.items():
        all_names = [assay_name] + info.get("aliases", [])
        for alias in all_names:
            if len(alias) <= 3:
                pattern = r'\b' + re.escape(alias) + r'\b'
            else:
                pattern = r'\b' + re.escape(alias) + r'\b'
            if re.search(pattern, text, re.IGNORECASE if len(alias) > 3 else 0):
                return assay_name
    return None


def _detect_tool_in_step(step_text, method_ids):
    """Detect a single software/method mention in a pipeline step.
    Returns (name, version, method_id) or (None, None, None)."""
    methods = _extract_methods_from_text(step_text, source_type="pipeline")
    # Filter out assay types and languages — we want actual tools
    skip_cats = {"Assay", "Language", "Reference", "Statistics", "Sequencing Platform"}
    for m in methods:
        if m["category"] not in skip_cats:
            mid = method_ids.get(m["name"])
            return m["name"], m.get("version"), mid
    # Fall back to assay/any match
    for m in methods:
        mid = method_ids.get(m["name"])
        return m["name"], m.get("version"), mid
    return None, None, None


def _parse_geo_data_processing(lines):
    """Parse GEO !Sample_data_processing lines into ordered step dicts.
    Returns list of {step_order, description}."""
    def _split_line_into_sentences(text):
        """Split a line into sentence-like steps while preserving URLs."""
        if not text:
            return []
        # Normalize whitespace first
        normalized = re.sub(r"\s+", " ", text.strip())
        if not normalized:
            return []
        # Split on sentence terminators followed by whitespace + capital/number.
        # Keeps common method text intact while breaking multi-sentence lines.
        parts = re.split(r"(?<=[.!?])\s+(?=[A-Z0-9])", normalized)
        sentences = []
        for part in parts:
            sent = part.strip(" ;")
            if sent:
                sentences.append(sent)
        return sentences

    steps = []
    order = 0
    for line in lines:
        line = line.strip()
        if not line:
            continue
        is_meta = any(line.lower().startswith(prefix) for prefix in _GEO_DP_METADATA_PREFIXES)
        if is_meta:
            continue
        for sentence in _split_line_into_sentences(line):
            order += 1
            steps.append({"step_order": order, "description": sentence})
    return steps


def _write_geo_steps_to_code_examples(accession, raw_text, steps, method_ids):
    """
    Create/overwrite the dataset JSON in code_examples/ for a GEO accession.
    One pipeline step is written per sentence-level step.
    """
    try:
        from publications.code_examples import (
            save_dataset_content,
            lookup_pub_date,
            is_dataset_locked,
            get_registry,
        )
    except Exception as e:
        _log(f"  Warning: could not import code_examples helpers for {accession}: {e}")
        return
    registry = get_registry() or {}
    existing = registry.get(accession, {}) if isinstance(registry, dict) else {}
    existing_locked = bool(existing.get("locked", True))
    has_prior_extract = (
        str(existing.get("raw_text_source", "")).strip() == "geo_data_processing"
        and bool(str(existing.get("raw_text", "")).strip())
    )

    # First run for an accession should sync JSON to extracted processing steps
    # even if default lock=true was inferred for legacy files.
    if has_prior_extract and is_dataset_locked(accession):
        _log(f"  Skipping code_examples overwrite for {accession} (locked=true)")
        return

    json_steps = []
    for step in steps:
        desc = step.get("description", "").strip()
        if not desc:
            continue
        tool_name, tool_version, _ = _detect_tool_in_step(desc, method_ids)
        json_steps.append({
            "step_order": len(json_steps) + 1,
            "description": desc,
            "tool_name": tool_name or "",
            "tool_version": tool_version or "",
            "code_example": "",
            "code_language": "bash",
            "github_url": "",
        })

    payload = {
        "steps": json_steps,
        "raw_text": raw_text,
        "raw_text_source": "geo_data_processing",
        # Keep existing lock setting if present; otherwise default locked=true.
        "locked": existing_locked if existing else True,
    }
    content = json.dumps(payload, indent=2)

    # Save in the existing location if present; otherwise use publication year/month.
    year, month = lookup_pub_date(accession)
    kwargs = {}
    if year and month:
        kwargs = {"year": int(year), "month": int(month)}

    save_path = save_dataset_content(accession, content, **kwargs)
    _log(f"  Updated code_examples JSON for {accession}: {save_path}")


def _parse_methods_text_into_pipelines(text, section_title=None):
    """Parse a Methods section text into sub-pipelines by heading.
    Returns list of {title, assay_type, steps: [{step_order, description}]}."""
    pipelines = []

    # Try splitting by sub-headings (common in STAR Methods)
    heading_pattern = re.compile(
        r'\n\s*([A-Z][A-Za-z0-9 /\-()]+(?:analysis|processing|sequencing|quantification|'
        r'alignment|mapping|calling|profiling|detection|annotation|identification|assembly|'
        r'clustering|normalization|filtering|visualization|statistics|quality control|QC))\s*\n',
        re.IGNORECASE
    )

    parts = heading_pattern.split(text)

    if len(parts) > 1:
        for i in range(1, len(parts), 2):
            heading = parts[i].strip()
            content = parts[i + 1].strip() if i + 1 < len(parts) else ""
            if not content or len(content) < 30:
                continue
            assay = _classify_assay_from_text(heading + " " + content[:200])
            steps = _split_text_into_steps(content)
            if steps:
                pipelines.append({"title": heading, "assay_type": assay, "steps": steps})
    else:
        title = section_title or "Computational analysis"
        assay = _classify_assay_from_text(text[:500])
        steps = _split_text_into_steps(text)
        if steps:
            pipelines.append({"title": title, "assay_type": assay, "steps": steps})

    return pipelines


def _split_text_into_steps(text):
    """Split a methods text block into ordered steps, grouping by tool context."""
    sentences = re.split(r'(?<=\.)\s+(?=[A-Z])', text.strip())
    sentences = [s.strip() for s in sentences if s.strip() and len(s.strip()) > 15]

    if not sentences:
        return []

    steps = []
    current_step = []
    current_tool = None

    for sent in sentences:
        methods = _extract_methods_from_text(sent, "pipeline")
        tools = [m["name"] for m in methods if m["category"] not in {"Assay", "Language", "Reference", "Statistics"}]
        primary_tool = tools[0] if tools else None

        if not current_step:
            current_step = [sent]
            current_tool = primary_tool
        elif primary_tool and primary_tool != current_tool:
            steps.append(" ".join(current_step))
            current_step = [sent]
            current_tool = primary_tool
        else:
            current_step.append(sent)
            if len(current_step) >= 3:
                steps.append(" ".join(current_step))
                current_step = []
                current_tool = None

    if current_step:
        steps.append(" ".join(current_step))

    return [{"step_order": i + 1, "description": s} for i, s in enumerate(steps)]


def _extract_methods_section(full_text):
    """Extract computational Methods sections from PMC full text.
    Returns list of (section_title, section_text) tuples."""
    sections = []

    patterns = [
        (r'QUANTIFICATION AND STATISTICAL ANALYSIS\s*\n([\s\S]*?)(?=\n[A-Z]{2,}[A-Z\s]*\n|DATA AND CODE|KEY RESOURCES|$)', "Quantification and Statistical Analysis"),
        (r'METHOD DETAILS\s*\n([\s\S]*?)(?=\nQUANTIFICATION|$)', "Method Details"),
        (r'(?:Materials and |Experimental )?Methods?\s*\n([\s\S]*?)(?=\n(?:Results|Discussion|Acknowledgement|References|Supplementary|Author Contributions|Data Availability|Funding)\b)', "Methods"),
    ]

    for pat, default_title in patterns:
        for m in re.finditer(pat, full_text, re.IGNORECASE):
            text = m.group(1).strip()
            if len(text) > 100:
                sections.append((default_title, text))

    if not sections:
        star_match = re.search(
            r'STAR\s+METHODS\s*\n([\s\S]*?)(?=\nSUPPLEMENTAL|$)',
            full_text, re.IGNORECASE
        )
        if star_match:
            text = star_match.group(1).strip()
            comp_start = re.search(
                r'(?:RNA-seq|eCLIP|ChIP-seq|ATAC-seq|scRNA|Bioinformatic|Computational|Data |Sequencing)',
                text, re.IGNORECASE
            )
            if comp_start:
                text = text[comp_start.start():]
                if len(text) > 100:
                    sections.append(("STAR Methods — Computational", text))

    return sections


def _get_method_ids():
    """Build name->method_id map from computational_methods table."""
    method_ids = {}
    with connection.cursor() as cur:
        cur.execute("SELECT method_id, canonical_name FROM computational_methods")
        for row in cur.fetchall():
            method_ids[row[1]] = row[0]
    return method_ids


def _insert_pipeline(cur, pmid, accession_id, assay_type, title, source, raw_text, steps, method_ids, accession_str=None):
    """Insert a pipeline and its steps. Returns pipeline_id or None.

    Parameters
    ----------
    accession_str : str or None
        The dataset accession string (e.g. "GSE137810") used to look up
        curated code examples from tools.json.  If None, code examples
        are populated from tools.json by matching accession_id → accession.
    """
    if not steps:
        return None

    cur.execute(
        """INSERT INTO analysis_pipelines (pmid, accession_id, assay_type, pipeline_title, source, raw_text)
           VALUES (%s, %s, %s, %s, %s, %s)""",
        [pmid, accession_id, assay_type, title, source, raw_text],
    )
    if _is_postgres():
        cur.execute("SELECT currval(pg_get_serial_sequence('analysis_pipelines', 'pipeline_id'))")
    else:
        cur.execute("SELECT last_insert_rowid()")
    pipeline_id = cur.fetchone()[0]

    # Lazy import to avoid circular dependency
    from publications.code_examples import get_code_example, get_code_example_by_tool

    # Resolve accession string from DB if not provided
    if not accession_str and accession_id:
        cur.execute("SELECT accession FROM dataset_accessions WHERE accession_id = %s", [accession_id])
        row = cur.fetchone()
        if row:
            accession_str = row[0]

    for step in steps:
        tool_name, tool_version, method_id = _detect_tool_in_step(step["description"], method_ids)

        # Look up code example from the dataset-keyed registry
        code_example = None
        code_language = None
        github_url = None

        if accession_str:
            # Try by step_order first, then by tool_name
            code_example, code_language, github_url = get_code_example(
                accession_str, step["step_order"],
            )
            if not code_example and tool_name:
                code_example, code_language, github_url = get_code_example_by_tool(
                    accession_str, tool_name,
                )

        cur.execute(
            """INSERT INTO pipeline_steps
               (pipeline_id, step_order, description, method_id, tool_name, tool_version,
                code_example, code_language, github_url)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT DO NOTHING""",
            [pipeline_id, step["step_order"], step["description"], method_id,
             tool_name, tool_version, code_example, code_language, github_url],
        )

    return pipeline_id


# ============================================================
# GEO Data Processing Fetcher
# ============================================================

def _fetch_gsm_data_processing_lines(gsm_accession, rate):
    """Fetch a single GSM's SOFT record and extract !Sample_data_processing lines."""
    url = f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={gsm_accession}&targ=self&form=text&view=quick"
    try:
        resp = _requests.get(url, timeout=30)
        resp.raise_for_status()
        lines = []
        for line in resp.text.split("\n"):
            line = line.strip()
            if line.startswith("!Sample_data_processing"):
                val = "=".join(line.split("=")[1:]).strip() if "=" in line else ""
                if val:
                    lines.append(val)
        _rate_limit(rate)
        return lines
    except Exception as e:
        _log(f"  Warning: failed to fetch {gsm_accession}: {e}")
        _rate_limit(rate)
        return []


def _run_geo_pipeline_extraction(method_ids):
    """Phase 1: Extract pipelines from GEO Data Processing sections."""
    _log("[1/2] Extracting pipelines from GEO Data Processing sections...")

    if not _HAS_REQUESTS:
        _log("  Skipping GEO — requests library not available")
        return 0

    rate = _setup_entrez() if _HAS_BIOPYTHON else 0.5

    with connection.cursor() as cur:
        cur.execute("""
            SELECT da.accession_id, da.accession, da.sample_ids, pd.pmid,
                   da.title, da.overall_design
            FROM dataset_accessions da
            JOIN publication_datasets pd ON pd.accession_id = da.accession_id
            WHERE da.accession_type = 'GSE'
              AND da.sample_ids IS NOT NULL
              AND da.sample_ids != ''
              AND da.sample_ids != '[]'
        """)
        gse_rows = cur.fetchall()

    _log(f"  Found {len(gse_rows)} GSE datasets linked to publications")
    pipeline_count = 0
    step_count = 0

    for idx, (acc_id, accession, sample_ids_raw, pmid, title, design) in enumerate(gse_rows):
        if (idx + 1) % 10 == 0:
            _log(f"  Processing {idx + 1}/{len(gse_rows)} GSEs...")

        try:
            samples = json.loads(sample_ids_raw) if sample_ids_raw.startswith("[") else [s.strip() for s in sample_ids_raw.split(",")]
        except (json.JSONDecodeError, TypeError):
            samples = [s.strip() for s in sample_ids_raw.split(",")]

        if not samples:
            continue

        gsm = samples[0].strip()
        if not gsm.startswith("GSM"):
            continue

        dp_lines = _fetch_gsm_data_processing_lines(gsm, rate)
        if not dp_lines:
            continue

        steps = _parse_geo_data_processing(dp_lines)
        if not steps:
            continue

        context_text = " ".join(filter(None, [title, design])) + " " + " ".join(dp_lines)
        assay = _classify_assay_from_text(context_text)

        ptitle = f"{accession} Data Processing"
        if assay:
            ptitle = f"{accession} {assay} Data Processing"

        raw_text = "\n".join(dp_lines)

        with connection.cursor() as cur:
            pid = _insert_pipeline(
                cur, pmid, acc_id, assay, ptitle,
                "geo_data_processing", raw_text, steps, method_ids,
                accession_str=accession,
            )
            if pid:
                pipeline_count += 1
                step_count += len(steps)

        # Save/overwrite dataset JSON in code_examples/ from sentence-level GEO steps
        try:
            _write_geo_steps_to_code_examples(accession, raw_text, steps, method_ids)
        except Exception as e:
            _log(f"  Warning: failed writing code_examples JSON for {accession}: {e}")

    _log(f"  Created {pipeline_count} pipelines with {step_count} steps from GEO Data Processing")
    return pipeline_count


# ============================================================
# PMC Methods Extraction
# ============================================================

def _run_pmc_pipeline_extraction(method_ids):
    """Phase 2: Extract pipelines from PMC full-text Methods sections."""
    _log("[2/2] Extracting pipelines from PMC Methods sections...")

    if not _HAS_BIOPYTHON:
        _log("  Skipping PMC — biopython not available")
        return 0

    rate = _setup_entrez()

    with connection.cursor() as cur:
        cur.execute("""
            SELECT pmid, pmc_id FROM publications
            WHERE pmc_id IS NOT NULL AND pmc_id != ''
        """)
        pmc_pubs = cur.fetchall()

    _log(f"  Found {len(pmc_pubs)} publications with PMC IDs")
    pipeline_count = 0
    step_count = 0
    errors = 0

    for idx, (pmid, pmc_id) in enumerate(pmc_pubs):
        if (idx + 1) % 10 == 0:
            _log(f"  Processing {idx + 1}/{len(pmc_pubs)} PMC articles...")

        pmc_num = pmc_id.replace("PMC", "").strip()

        try:
            handle = Entrez.efetch(db="pmc", id=pmc_num, rettype="xml")
            xml_text = handle.read()
            handle.close()
            _rate_limit(rate)

            full_text = _extract_text_from_pmc_xml(xml_text)
            if not full_text or len(full_text) < 200:
                continue

            method_sections = _extract_methods_section(full_text)
            if not method_sections:
                continue

            # Look up GSE accessions for this PMID (for raw_text saving)
            pmid_accessions = []
            try:
                with connection.cursor() as cur:
                    cur.execute("""
                        SELECT da.accession FROM dataset_accessions da
                        JOIN publication_datasets pd ON da.accession_id = pd.accession_id
                        WHERE pd.pmid = %s AND da.accession_type = 'GSE'
                    """, [pmid])
                    pmid_accessions = [r[0] for r in cur.fetchall()]
            except Exception:
                pass

            combined_raw = "\n\n".join(
                f"[{st}]\n{stxt}" for st, stxt in method_sections
            )

            for section_title, section_text in method_sections:
                pipelines = _parse_methods_text_into_pipelines(section_text, section_title)

                for p in pipelines:
                    if not p["steps"]:
                        continue
                    raw = "\n".join(s["description"] for s in p["steps"])
                    with connection.cursor() as cur:
                        pid = _insert_pipeline(
                            cur, pmid, None, p.get("assay_type"),
                            p["title"], "pmc_methods", raw,
                            p["steps"], method_ids,
                        )
                        if pid:
                            pipeline_count += 1
                            step_count += len(p["steps"])

            # Save raw methods text into JSON files for linked GSE accessions
            if pmid_accessions and combined_raw:
                try:
                    from publications.code_examples import update_dataset_raw_text
                    for acc in pmid_accessions:
                        update_dataset_raw_text(acc, combined_raw, source="pmc_methods")
                except Exception:
                    pass

        except Exception as e:
            errors += 1
            if errors <= 5:
                _log(f"  Warning: failed to process PMC{pmc_num}: {e}")
            _rate_limit(rate)
            continue

    _log(f"  Created {pipeline_count} pipelines with {step_count} steps from PMC Methods")
    if errors > 5:
        _log(f"  ({errors} total errors during PMC extraction)")
    return pipeline_count


def _extract_text_from_pmc_xml(xml_bytes):
    """Extract plain text from PMC XML (JATS format)."""
    try:
        if isinstance(xml_bytes, bytes):
            xml_text = xml_bytes.decode("utf-8", errors="replace")
        else:
            xml_text = xml_bytes

        root = ET.fromstring(xml_text)
        body = root.find(".//body")
        if body is None:
            return ""

        parts = []
        for elem in body.iter():
            if elem.text:
                parts.append(elem.text)
            if elem.tail:
                parts.append(elem.tail)

        return " ".join(parts)
    except ET.ParseError:
        return ""


def import_pmc_methods_json(json_path):
    """Import pipeline data from a JSON file (produced by extract_pmc_methods.py).
    JSON format: [{pmid, pmc_id, sections: [{title, text}]}, ...]"""
    with open(json_path) as f:
        data = json.load(f)

    method_ids = _get_method_ids()
    pipeline_count = 0

    for entry in data:
        pmid = entry.get("pmid")
        if not pmid:
            continue

        for section in entry.get("sections", []):
            title = section.get("title", "Methods")
            text = section.get("text", "")
            if len(text) < 50:
                continue

            pipelines = _parse_methods_text_into_pipelines(text, title)
            for p in pipelines:
                if not p["steps"]:
                    continue
                raw = "\n".join(s["description"] for s in p["steps"])
                with connection.cursor() as cur:
                    pid = _insert_pipeline(
                        cur, pmid, None, p.get("assay_type"),
                        p["title"], "pmc_methods", raw,
                        p["steps"], method_ids,
                    )
                    if pid:
                        pipeline_count += 1

    return pipeline_count


# ============================================================
# Pipeline Update Orchestrator
# ============================================================

def start_pipeline_update():
    """Start a background pipeline extraction update.
    Returns True if started, False if already running.
    Requires biopython and requests (runs locally only)."""
    return _start_background_worker(
        target=_run_pipeline_update,
        args=(),
        progress_msg="Starting pipeline extraction...",
    )


def _run_pipeline_update():
    """Background worker for pipeline extraction."""
    try:
        _log("Starting analysis pipeline extraction...")

        _log("Ensuring analysis_pipelines and pipeline_steps tables exist...")
        _ensure_pipeline_tables()
        _ensure_methods_tables()

        with connection.cursor() as cur:
            cur.execute("DELETE FROM pipeline_steps")
            cur.execute("DELETE FROM analysis_pipelines")
        _log("Cleared existing pipeline data for fresh extraction")

        method_ids = _get_method_ids()
        if not method_ids:
            _log("Warning: No computational methods found. Run 'Extract Methods' first for tool linking.")

        geo_count = _run_geo_pipeline_extraction(method_ids)
        pmc_count = _run_pmc_pipeline_extraction(method_ids)

        with connection.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM analysis_pipelines")
            total_pipelines = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM pipeline_steps")
            total_steps = cur.fetchone()[0]
            cur.execute("SELECT COUNT(DISTINCT pmid) FROM analysis_pipelines")
            pubs_with_pipelines = cur.fetchone()[0]
            cur.execute("SELECT COUNT(DISTINCT assay_type) FROM analysis_pipelines WHERE assay_type IS NOT NULL")
            assay_types = cur.fetchone()[0]
            cur.execute("SELECT COUNT(DISTINCT method_id) FROM pipeline_steps WHERE method_id IS NOT NULL")
            tools_linked = cur.fetchone()[0]

            stats = {
                "pipelines": total_pipelines,
                "steps": total_steps,
                "pubs_with_pipelines": pubs_with_pipelines,
                "assay_types": assay_types,
                "tools_linked": tools_linked,
                "geo_pipelines": geo_count,
                "pmc_pipelines": pmc_count,
            }

        _set_status(stats=stats)
        _log(f"Pipeline extraction complete! {total_pipelines} pipelines, {total_steps} steps, "
             f"{pubs_with_pipelines} publications, {tools_linked} tools linked")

        with connection.cursor() as cur:
            cur.execute(
                """INSERT INTO update_log (new_pmids_added, notes)
                   VALUES (%s, %s)""",
                [0, f"Pipeline extraction: {total_pipelines} pipelines, {total_steps} steps, "
                    f"GEO={geo_count}, PMC={pmc_count}"],
            )

    except Exception as e:
        import traceback
        _log(f"ERROR (pipelines): {e}")
        _log(traceback.format_exc())
        _set_status(error=str(e))
    finally:
        _set_status(running=False, finished_at=datetime.now().isoformat())
