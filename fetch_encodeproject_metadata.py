#!/usr/bin/env python3
"""
Yeo Lab ENCODE Project Dataset Fetcher
=======================================
Run this script OUTSIDE the Cowork sandbox on a machine with internet access.
It will:
  1. Query the ENCODE REST API for all experiments funded by specified grants
  2. Fetch detailed metadata for each experiment (biosample, assay, target, etc.)
  3. Fetch all file metadata (accession, format, output type, download URL, etc.)
  4. Cross-reference experiments with PubMed publications in the local DB
  5. Output everything as a JSON file compatible with the GEO/SRA import pipeline

Requirements:
    pip install requests

Usage:
    python fetch_encodeproject_metadata.py
    python fetch_encodeproject_metadata.py --grants U41HG009889 U54HG007005
    python fetch_encodeproject_metadata.py --output yeolab_encode_results.json
    python fetch_encodeproject_metadata.py --verbose
    # Produces: yeolab_encode_results.json

Author: Generated for Brian Yee / Yeo Lab
"""

import json
import re
import time
import sys
import os
import argparse
import sqlite3
from datetime import datetime
from collections import defaultdict

try:
    import requests
except ImportError:
    print("ERROR: requests required. Install with: pip install requests")
    sys.exit(1)

# ============================================================
# CONFIGURATION
# ============================================================
ENCODE_BASE_URL = "https://www.encodeproject.org"
OUTPUT_FILE = "yeolab_encode_results.json"

# Yeo Lab ENCODE grants
DEFAULT_GRANTS = [
    "U41HG009889",  # ENCODE4 - Yeo lab data production
    "U54HG007005",  # ENCODE3 - Yeo lab data production
]

# Rate limiting: ENCODE allows max 10 requests/second
RATE_LIMIT = 0.15  # seconds between requests (generous margin)
REQUEST_TIMEOUT = 60.0
MAX_RETRIES = 3
RETRY_BASE_WAIT = 2.0

# Path to local publications DB for PMID cross-referencing
DB_PATH = os.environ.get("YEOLAB_DB_PATH", "yeolab_publications.db")

# Headers for ENCODE API
HEADERS = {
    "accept": "application/json",
    "User-Agent": "YeoLabFetcher/1.0 (brian.alan.yee@gmail.com)",
}


def rate_limit():
    """Respect ENCODE rate limits."""
    time.sleep(RATE_LIMIT)


def run_with_retries(fn, retries=MAX_RETRIES, label="request", base_wait=RETRY_BASE_WAIT):
    """Run a callable with retry/backoff for transient request failures."""
    for attempt in range(retries + 1):
        try:
            return fn()
        except requests.exceptions.RequestException as e:
            if attempt < retries:
                wait = base_wait * (2 ** attempt)
                print(f"  [RETRY {attempt+1}/{retries}] {label}: {e} — waiting {wait:.1f}s")
                time.sleep(wait)
            else:
                print(f"  [FAILED] {label}: {e}")
                raise


# ============================================================
# ENCODE API HELPERS
# ============================================================

def encode_search(search_type, extra_params=None, limit="all"):
    """
    Search the ENCODE portal.

    Args:
        search_type: Object type (e.g., "Experiment", "File", "Award")
        extra_params: Dict of additional query parameters
        limit: Number of results or "all"

    Returns:
        List of result objects (the @graph array)
    """
    params = {
        "type": search_type,
        "format": "json",
        "limit": limit,
    }
    if extra_params:
        params.update(extra_params)

    url = f"{ENCODE_BASE_URL}/search/"

    def _do_search():
        resp = requests.get(url, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    rate_limit()
    data = run_with_retries(_do_search, label=f"search {search_type}")

    results = data.get("@graph", [])
    total = data.get("total", len(results))

    if isinstance(limit, str) and limit == "all" and len(results) < total:
        print(f"  WARNING: Got {len(results)}/{total} results for {search_type} search")

    return results


def encode_get(path, frame="embedded"):
    """
    Get a single object from the ENCODE portal.

    Args:
        path: Object path (e.g., "/experiments/ENCSR000AAA/")
        frame: Response frame ("object", "embedded", "page")

    Returns:
        Object dict
    """
    if not path.startswith("/"):
        path = f"/{path}"
    if not path.endswith("/"):
        path += "/"

    url = f"{ENCODE_BASE_URL}{path}"
    params = {"format": "json", "frame": frame}

    def _do_get():
        resp = requests.get(url, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    rate_limit()
    return run_with_retries(_do_get, label=f"GET {path}")


# ============================================================
# CORE FETCHERS
# ============================================================

def fetch_award_details(grant_numbers, verbose=False):
    """Fetch ENCODE award objects for the given grant numbers."""
    awards = {}
    for grant in grant_numbers:
        if verbose:
            print(f"\n  Fetching award details for {grant}...")

        # Search for the award by project number
        results = encode_search("Award", {"project_num": grant})

        if not results:
            # Try alternate search patterns
            results = encode_search("Award", {"name": grant})

        if results:
            award = results[0]
            award_id = award.get("@id", "")
            awards[grant] = {
                "award_id": award_id,
                "name": award.get("name", grant),
                "project": award.get("project", ""),
                "rfa": award.get("rfa", ""),
                "title": award.get("title", ""),
                "pi": award.get("pi", {}).get("title", "") if isinstance(award.get("pi"), dict) else str(award.get("pi", "")),
                "status": award.get("status", ""),
                "start_date": award.get("start_date", ""),
                "end_date": award.get("end_date", ""),
                "url": f"{ENCODE_BASE_URL}{award_id}" if award_id else "",
            }
            if verbose:
                print(f"    Found: {awards[grant]['title']}")
                print(f"    PI: {awards[grant]['pi']}")
                print(f"    RFA: {awards[grant]['rfa']}")
        else:
            print(f"  WARNING: No award found for {grant}")
            awards[grant] = {"name": grant, "award_id": "", "title": "", "pi": ""}

    return awards


def fetch_experiments_for_grants(grant_numbers, verbose=False):
    """
    Fetch all experiments associated with the given grant numbers.

    Returns:
        Dict mapping experiment accession -> experiment metadata
    """
    experiments = {}

    for grant in grant_numbers:
        if verbose:
            print(f"\n  Searching experiments for grant {grant}...")

        # Search experiments by award
        results = encode_search("Experiment", {
            "award.name": grant,
            "field": [
                "@id", "accession", "status", "assay_title", "assay_term_name",
                "target", "biosample_ontology", "biosample_summary",
                "description", "date_released", "date_submitted",
                "lab", "award", "replicates", "files",
                "dbxrefs", "references", "aliases",
                "possible_controls", "related_series",
            ],
        })

        if not results:
            # Try alternate field name
            results = encode_search("Experiment", {
                "award.project_num": grant,
            })

        if verbose:
            print(f"    Found {len(results)} experiments for {grant}")

        for exp in results:
            acc = exp.get("accession", "")
            if not acc:
                continue
            if acc in experiments:
                # Already seen from another grant — merge grant info
                if grant not in experiments[acc].get("grants", []):
                    experiments[acc]["grants"].append(grant)
                continue

            experiments[acc] = _parse_experiment(exp, grant)

    return experiments


def _parse_experiment(exp, grant):
    """Parse an experiment search result into our standard format."""
    # Extract target info
    target = exp.get("target", {})
    if isinstance(target, dict):
        target_label = target.get("label", target.get("name", ""))
        target_genes = target.get("genes", [])
        if isinstance(target_genes, list):
            target_gene_names = [g.get("symbol", "") for g in target_genes if isinstance(g, dict)]
        else:
            target_gene_names = []
    elif isinstance(target, str):
        target_label = target.split("/")[-2] if "/" in target else target
        target_gene_names = []
    else:
        target_label = ""
        target_gene_names = []

    # Extract biosample info
    biosample = exp.get("biosample_ontology", {})
    if isinstance(biosample, dict):
        biosample_term = biosample.get("term_name", "")
        biosample_type = biosample.get("classification", "")
    elif isinstance(biosample, list) and biosample:
        first = biosample[0] if isinstance(biosample[0], dict) else {}
        biosample_term = first.get("term_name", "")
        biosample_type = first.get("classification", "")
    else:
        biosample_term = ""
        biosample_type = ""

    # Extract lab info
    lab = exp.get("lab", {})
    if isinstance(lab, dict):
        lab_name = lab.get("title", lab.get("name", ""))
    elif isinstance(lab, str):
        lab_name = lab.split("/")[-2] if "/" in lab else lab
    else:
        lab_name = ""

    # Extract award info
    award = exp.get("award", {})
    if isinstance(award, dict):
        award_name = award.get("name", award.get("project_num", ""))
    elif isinstance(award, str):
        award_name = award.split("/")[-2] if "/" in award else award
    else:
        award_name = ""

    # Extract references (publications)
    references = exp.get("references", [])
    pmids = []
    for ref in references:
        if isinstance(ref, dict):
            identifiers = ref.get("identifiers", [])
            for ident in identifiers:
                if isinstance(ident, str) and ident.startswith("PMID:"):
                    pmids.append(ident.replace("PMID:", ""))
        elif isinstance(ref, str):
            # Reference might be a path like /publications/UUID/
            pass

    # Extract dbxrefs (GEO accessions, etc.)
    dbxrefs = exp.get("dbxrefs", [])
    geo_accessions = []
    other_xrefs = []
    for xref in dbxrefs:
        if isinstance(xref, str):
            if xref.startswith("GEO:"):
                geo_accessions.append(xref.replace("GEO:", ""))
            elif xref.startswith("UCSC-ENCODE-"):
                other_xrefs.append(xref)
            else:
                other_xrefs.append(xref)

    # Extract control experiments
    controls = _extract_control_accessions(exp.get("possible_controls", []))

    return {
        "accession": exp.get("accession", ""),
        "status": exp.get("status", ""),
        "assay_title": exp.get("assay_title", ""),
        "assay_term_name": exp.get("assay_term_name", ""),
        "target": target_label,
        "target_genes": target_gene_names,
        "biosample_term": biosample_term,
        "biosample_type": biosample_type,
        "biosample_summary": exp.get("biosample_summary", ""),
        "description": exp.get("description", ""),
        "date_released": exp.get("date_released", ""),
        "date_submitted": exp.get("date_submitted", ""),
        "lab": lab_name,
        "award": award_name,
        "grants": [grant],
        "pmids": pmids,
        "geo_accessions": geo_accessions,
        "dbxrefs": other_xrefs,
        "controls": controls,
        "aliases": exp.get("aliases", []),
        "url": f"{ENCODE_BASE_URL}/experiments/{exp.get('accession', '')}/",
    }


def _extract_control_accessions(control_values):
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
    return list(dict.fromkeys(controls))


def fetch_experiment_details(experiments, verbose=False):
    """
    Fetch detailed metadata for each experiment (including replicates,
    biosamples, and references with PMIDs).
    """
    total = len(experiments)
    for i, (acc, exp) in enumerate(experiments.items()):
        if verbose and (i % 50 == 0 or i == total - 1):
            print(f"  Fetching experiment details: {i+1}/{total}...")

        try:
            detail = encode_get(f"/experiments/{acc}/", frame="embedded")
        except Exception as e:
            if verbose:
                print(f"    WARNING: Could not fetch details for {acc}: {e}")
            continue

        # Update with richer data from embedded frame
        # Replicates
        replicates = []
        for rep in detail.get("replicates", []):
            if not isinstance(rep, dict):
                continue
            library = rep.get("library", {}) or {}
            biosample = library.get("biosample", {}) or {}

            rep_data = {
                "biological_replicate_number": rep.get("biological_replicate_number"),
                "technical_replicate_number": rep.get("technical_replicate_number"),
                "library_accession": library.get("accession", ""),
                "library_size_range": library.get("size_range", ""),
                "library_nucleic_acid_term": library.get("nucleic_acid_term_name", ""),
                "library_depleted_in": library.get("depleted_in_term_name", []),
                "library_strand_specificity": library.get("strand_specificity", ""),
                "biosample_accession": biosample.get("accession", ""),
                "biosample_organism": "",
                "biosample_life_stage": biosample.get("life_stage", ""),
                "biosample_age": biosample.get("age", ""),
            }
            organism = biosample.get("organism", {})
            if isinstance(organism, dict):
                rep_data["biosample_organism"] = organism.get("scientific_name", "")
            elif isinstance(organism, str):
                rep_data["biosample_organism"] = organism.split("/")[-2] if "/" in organism else organism

            replicates.append(rep_data)

        exp["replicates"] = replicates

        # Better PMID extraction from embedded references
        for ref in detail.get("references", []):
            if isinstance(ref, dict):
                for ident in ref.get("identifiers", []):
                    if isinstance(ident, str) and ident.startswith("PMID:"):
                        pmid = ident.replace("PMID:", "")
                        if pmid not in exp["pmids"]:
                            exp["pmids"].append(pmid)

        # Controls can be missing from search payload; merge from detail payload.
        detail_controls = _extract_control_accessions(detail.get("possible_controls", []))
        if detail_controls:
            existing = exp.get("controls", [])
            exp["controls"] = list(dict.fromkeys(existing + detail_controls))

        # Organisms from replicates
        organisms = set()
        for rep in replicates:
            org = rep.get("biosample_organism", "")
            if org:
                organisms.add(org)
        exp["organisms"] = sorted(organisms)

    return experiments


def fetch_files_for_experiments(experiments, verbose=False):
    """
    Fetch all file metadata for each experiment.

    Returns:
        Dict mapping experiment accession -> list of file metadata dicts
    """
    experiment_files = {}
    total = len(experiments)

    for i, acc in enumerate(experiments):
        if verbose and (i % 50 == 0 or i == total - 1):
            print(f"  Fetching files: {i+1}/{total} experiments...")

        try:
            files = encode_search("File", {
                "dataset": f"/experiments/{acc}/",
                "field": [
                    "@id", "accession", "file_format", "file_type",
                    "output_type", "output_category",
                    "assembly", "genome_annotation",
                    "file_size", "md5sum", "href",
                    "status", "date_created",
                    "replicate", "biological_replicates",
                    "technical_replicates",
                    "derived_from", "step_run",
                    "cloud_metadata",
                    "s3_uri", "azure_uri",
                ],
            })
        except Exception as e:
            if verbose:
                print(f"    WARNING: Could not fetch files for {acc}: {e}")
            experiment_files[acc] = []
            continue

        parsed_files = []
        for f in files:
            file_acc = f.get("accession", "")

            # Extract replicate info
            replicate = f.get("replicate", {})
            if isinstance(replicate, dict):
                bio_rep = replicate.get("biological_replicate_number")
                tech_rep = replicate.get("technical_replicate_number")
            else:
                bio_rep = None
                tech_rep = None

            # Cloud URIs
            s3_uri = f.get("s3_uri", "")
            azure_uri = f.get("azure_uri", "")
            cloud_metadata = f.get("cloud_metadata", {})

            # Download URL
            href = f.get("href", "")
            download_url = f"{ENCODE_BASE_URL}{href}" if href and not href.startswith("http") else href

            # Derived from
            derived_from = []
            for df in f.get("derived_from", []):
                if isinstance(df, dict):
                    derived_from.append(df.get("accession", ""))
                elif isinstance(df, str):
                    match = re.search(r"(ENCFF\w+)", df)
                    if match:
                        derived_from.append(match.group(1))

            parsed_files.append({
                "accession": file_acc,
                "file_format": f.get("file_format", ""),
                "file_type": f.get("file_type", ""),
                "output_type": f.get("output_type", ""),
                "output_category": f.get("output_category", ""),
                "assembly": f.get("assembly", ""),
                "genome_annotation": f.get("genome_annotation", ""),
                "file_size": f.get("file_size"),
                "md5sum": f.get("md5sum", ""),
                "status": f.get("status", ""),
                "date_created": f.get("date_created", ""),
                "biological_replicates": f.get("biological_replicates", []),
                "technical_replicates": f.get("technical_replicates", []),
                "bio_rep": bio_rep,
                "tech_rep": tech_rep,
                "derived_from": derived_from,
                "download_url": download_url,
                "s3_uri": s3_uri,
                "azure_uri": azure_uri,
                "cloud_metadata": cloud_metadata if isinstance(cloud_metadata, dict) else {},
            })

        experiment_files[acc] = parsed_files

    return experiment_files


def cross_reference_pmids(experiments, db_path=None, verbose=False):
    """
    Cross-reference experiment PMIDs with local publications database.

    Returns:
        Dict mapping PMID -> list of ENCODE experiment accessions
    """
    pmid_experiments = defaultdict(list)

    # Collect all PMIDs from experiments
    for acc, exp in experiments.items():
        for pmid in exp.get("pmids", []):
            pmid_experiments[pmid].append(acc)

    if not db_path or not os.path.exists(db_path):
        if verbose:
            print(f"  No local DB at {db_path} — skipping cross-reference")
        return dict(pmid_experiments), {}, {}

    if verbose:
        print(f"\n  Cross-referencing {len(pmid_experiments)} PMIDs with local DB...")

    # Look up which PMIDs exist in our local DB
    local_pmids = set()
    local_publications = {}
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        if pmid_experiments:
            placeholders = ",".join("?" for _ in pmid_experiments)
            cursor.execute(
                f"SELECT pmid, title, journal_name, pub_year FROM publications WHERE pmid IN ({placeholders})",
                list(pmid_experiments.keys()),
            )
            for row in cursor.fetchall():
                pmid = str(row["pmid"])
                local_pmids.add(pmid)
                local_publications[pmid] = {
                    "title": row["title"],
                    "journal": row["journal_name"],
                    "year": row["pub_year"],
                }

        # Also search for ENCODE accessions mentioned in publication text
        # (future enhancement)

        conn.close()
    except Exception as e:
        if verbose:
            print(f"    WARNING: DB cross-reference failed: {e}")

    if verbose:
        print(f"    {len(local_pmids)}/{len(pmid_experiments)} PMIDs found in local DB")

    return dict(pmid_experiments), local_publications, {
        pmid: pmid in local_pmids for pmid in pmid_experiments
    }


# ============================================================
# ANNOTATION SEARCH (find Annotations, not just Experiments)
# ============================================================

def fetch_annotations_for_grants(grant_numbers, verbose=False):
    """
    Fetch Annotation datasets (e.g., peak calls, gene quantifications)
    associated with the given grants.
    """
    annotations = {}

    for grant in grant_numbers:
        if verbose:
            print(f"\n  Searching annotations for grant {grant}...")

        results = encode_search("Annotation", {
            "award.name": grant,
        })

        if not results:
            results = encode_search("Annotation", {
                "award.project_num": grant,
            })

        if verbose:
            print(f"    Found {len(results)} annotations for {grant}")

        for ann in results:
            acc = ann.get("accession", "")
            if not acc or acc in annotations:
                continue

            annotations[acc] = {
                "accession": acc,
                "status": ann.get("status", ""),
                "annotation_type": ann.get("annotation_type", ""),
                "description": ann.get("description", ""),
                "biosample_ontology": _extract_biosample_info(ann.get("biosample_ontology")),
                "organism": "",
                "software_used": [],
                "date_released": ann.get("date_released", ""),
                "lab": _extract_lab(ann.get("lab")),
                "award": grant,
                "url": f"{ENCODE_BASE_URL}/annotations/{acc}/",
            }

    return annotations


def _extract_biosample_info(biosample):
    """Extract biosample info from various formats."""
    if isinstance(biosample, dict):
        return biosample.get("term_name", "")
    elif isinstance(biosample, list) and biosample:
        terms = []
        for b in biosample:
            if isinstance(b, dict):
                terms.append(b.get("term_name", ""))
        return ", ".join(t for t in terms if t)
    return ""


def _extract_lab(lab):
    """Extract lab name from various formats."""
    if isinstance(lab, dict):
        return lab.get("title", lab.get("name", ""))
    elif isinstance(lab, str):
        return lab.split("/")[-2] if "/" in lab else lab
    return ""


# ============================================================
# OUTPUT BUILDER
# ============================================================

def build_output(
    awards, experiments, experiment_files, annotations,
    pmid_experiments, local_publications, pmid_in_db,
    grant_numbers,
):
    """
    Build the output JSON in a format similar to yeolab_geo_sra_results.json.
    """
    # Compute summary stats
    total_files = sum(len(files) for files in experiment_files.values())
    assay_counts = defaultdict(int)
    organism_counts = defaultdict(int)
    biosample_counts = defaultdict(int)
    status_counts = defaultdict(int)

    for acc, exp in experiments.items():
        assay_counts[exp.get("assay_title", "unknown")] += 1
        status_counts[exp.get("status", "unknown")] += 1
        for org in exp.get("organisms", []):
            organism_counts[org] += 1
        bs = exp.get("biosample_term", "")
        if bs:
            biosample_counts[bs] += 1

    # File format breakdown
    format_counts = defaultdict(int)
    output_type_counts = defaultdict(int)
    for files in experiment_files.values():
        for f in files:
            format_counts[f.get("file_format", "unknown")] += 1
            output_type_counts[f.get("output_type", "unknown")] += 1

    # Build pmid_datasets mapping (mirroring GEO/SRA format)
    pmid_datasets = {}
    for pmid, accs in pmid_experiments.items():
        entries = []
        for enc_acc in accs:
            exp = experiments.get(enc_acc, {})
            entries.append({
                "accession": enc_acc,
                "type": "ENCSR",
                "database": "ENCODE",
                "source": "encode_api",
                "title": exp.get("description", ""),
                "assay": exp.get("assay_title", ""),
                "target": exp.get("target", ""),
                "biosample": exp.get("biosample_summary", exp.get("biosample_term", "")),
                "organisms": exp.get("organisms", []),
                "n_files": len(experiment_files.get(enc_acc, [])),
            })
        pmid_datasets[pmid] = entries

    # Build dataset_accessions (mirroring GEO/SRA format)
    dataset_accessions = {}
    for acc, exp in experiments.items():
        dataset_accessions[acc] = {
            "accession": acc,
            "type": "ENCSR",
            "database": "ENCODE",
            "source": "encode_api",
            "title": exp.get("description", ""),
            "summary": f"{exp.get('assay_title', '')} of {exp.get('target', 'N/A')} in {exp.get('biosample_summary', exp.get('biosample_term', 'N/A'))}",
            "overall_design": exp.get("description", ""),
            "organisms": exp.get("organisms", []),
            "assay": exp.get("assay_title", ""),
            "target": exp.get("target", ""),
            "biosample_term": exp.get("biosample_term", ""),
            "biosample_type": exp.get("biosample_type", ""),
            "biosample_summary": exp.get("biosample_summary", ""),
            "status": exp.get("status", ""),
            "date_released": exp.get("date_released", ""),
            "date_submitted": exp.get("date_submitted", ""),
            "lab": exp.get("lab", ""),
            "award": exp.get("award", ""),
            "grants": exp.get("grants", []),
            "geo_accessions": exp.get("geo_accessions", []),
            "controls": exp.get("controls", []),
            "aliases": exp.get("aliases", []),
            "pmids": exp.get("pmids", []),
            "citation_pmids": exp.get("pmids", []),
            "url": exp.get("url", ""),
            "n_files": len(experiment_files.get(acc, [])),
            "replicates": exp.get("replicates", []),
            "files": experiment_files.get(acc, []),
        }

    # Build all_accessions list
    all_accessions = sorted(set(
        list(experiments.keys()) +
        list(annotations.keys())
    ))

    # Build file-level index (similar to sra_runs)
    encode_files = {}
    for acc, files in experiment_files.items():
        for f in files:
            file_acc = f.get("accession", "")
            if file_acc:
                encode_files[file_acc] = {
                    **f,
                    "experiment_accession": acc,
                    "experiment_assay": experiments[acc].get("assay_title", ""),
                    "experiment_target": experiments[acc].get("target", ""),
                    "experiment_biosample": experiments[acc].get("biosample_summary", ""),
                }

    output = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "source": "ENCODE Project (encodeproject.org)",
            "grants": grant_numbers,
            "total_experiments": len(experiments),
            "total_annotations": len(annotations),
            "total_files": total_files,
            "total_unique_file_accessions": len(encode_files),
            "pmids_with_experiments": len(pmid_experiments),
            "pmids_in_local_db": sum(1 for v in pmid_in_db.values() if v) if pmid_in_db else 0,
            "assay_breakdown": dict(sorted(assay_counts.items(), key=lambda x: -x[1])),
            "organism_breakdown": dict(sorted(organism_counts.items(), key=lambda x: -x[1])),
            "biosample_breakdown": dict(sorted(biosample_counts.items(), key=lambda x: -x[1])[:20]),
            "status_breakdown": dict(sorted(status_counts.items(), key=lambda x: -x[1])),
            "file_format_breakdown": dict(sorted(format_counts.items(), key=lambda x: -x[1])),
            "output_type_breakdown": dict(sorted(output_type_counts.items(), key=lambda x: -x[1])[:20]),
        },
        "awards": awards,
        "pmid_datasets": pmid_datasets,
        "dataset_accessions": dataset_accessions,
        "annotations": annotations,
        "encode_files": encode_files,
        "all_accessions": all_accessions,
        "local_publication_matches": local_publications,
        "pmid_in_local_db": pmid_in_db,
    }

    return output


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Fetch ENCODE Project dataset metadata for Yeo Lab grants"
    )
    parser.add_argument(
        "--grants",
        nargs="+",
        default=DEFAULT_GRANTS,
        help=f"ENCODE grant/award numbers (default: {' '.join(DEFAULT_GRANTS)})",
    )
    parser.add_argument(
        "--output", "-o",
        default=OUTPUT_FILE,
        help=f"Output JSON file (default: {OUTPUT_FILE})",
    )
    parser.add_argument(
        "--db",
        default=DB_PATH,
        help=f"Path to local publications DB for cross-referencing (default: {DB_PATH})",
    )
    parser.add_argument(
        "--skip-files",
        action="store_true",
        help="Skip fetching individual file metadata (faster, less complete)",
    )
    parser.add_argument(
        "--skip-details",
        action="store_true",
        help="Skip fetching detailed experiment metadata (faster, less complete)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print progress details",
    )
    args = parser.parse_args()

    print(f"=" * 60)
    print(f"ENCODE Project Metadata Fetcher")
    print(f"Grants: {', '.join(args.grants)}")
    print(f"Output: {args.output}")
    print(f"=" * 60)

    # 1. Fetch award details
    print(f"\n[1/6] Fetching award details...")
    awards = fetch_award_details(args.grants, verbose=args.verbose)
    for grant, info in awards.items():
        print(f"  {grant}: {info.get('title', 'N/A')}")

    # 2. Fetch experiments
    print(f"\n[2/6] Fetching experiments...")
    experiments = fetch_experiments_for_grants(args.grants, verbose=args.verbose)
    print(f"  Found {len(experiments)} unique experiments")

    # 3. Fetch detailed experiment metadata
    if not args.skip_details:
        print(f"\n[3/6] Fetching experiment details (replicates, biosamples, references)...")
        experiments = fetch_experiment_details(experiments, verbose=args.verbose)
    else:
        print(f"\n[3/6] Skipping experiment details (--skip-details)")

    # 4. Fetch file metadata
    if not args.skip_files:
        print(f"\n[4/6] Fetching file metadata...")
        experiment_files = fetch_files_for_experiments(experiments, verbose=args.verbose)
        total_files = sum(len(f) for f in experiment_files.values())
        print(f"  Found {total_files} files across {len(experiment_files)} experiments")
    else:
        print(f"\n[4/6] Skipping file metadata (--skip-files)")
        experiment_files = {acc: [] for acc in experiments}

    # 5. Fetch annotations
    print(f"\n[5/6] Fetching annotation datasets...")
    annotations = fetch_annotations_for_grants(args.grants, verbose=args.verbose)
    print(f"  Found {len(annotations)} annotations")

    # 6. Cross-reference with local DB
    print(f"\n[6/6] Cross-referencing with local publications DB...")
    pmid_experiments, local_publications, pmid_in_db = cross_reference_pmids(
        experiments, db_path=args.db, verbose=args.verbose,
    )

    # Build output
    print(f"\nBuilding output...")
    output = build_output(
        awards, experiments, experiment_files, annotations,
        pmid_experiments, local_publications, pmid_in_db,
        args.grants,
    )

    # Write JSON
    with open(args.output, "w") as f:
        json.dump(output, f, indent=2, default=str)

    file_size_mb = os.path.getsize(args.output) / (1024 * 1024)
    print(f"\n{'=' * 60}")
    print(f"Done! Wrote {args.output} ({file_size_mb:.1f} MB)")
    print(f"  Experiments: {output['metadata']['total_experiments']}")
    print(f"  Annotations: {output['metadata']['total_annotations']}")
    print(f"  Files: {output['metadata']['total_files']}")
    print(f"  PMIDs with experiments: {output['metadata']['pmids_with_experiments']}")
    print(f"\nAssay breakdown:")
    for assay, count in list(output['metadata']['assay_breakdown'].items())[:10]:
        print(f"  {assay}: {count}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
