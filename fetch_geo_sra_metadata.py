#!/usr/bin/env python3
"""
Yeo Lab GEO/SRA Dataset Fetcher
================================
Run this script OUTSIDE the Cowork sandbox on a machine with NCBI API access.
It will:
  1. Query PubMed for all Yeo GW[Author] PMIDs
  2. Use NCBI ELink to find GEO datasets linked to each PMID
  3. Get full GEO series metadata (title, organism, platform, samples, files)
  4. Get SRA run metadata and original file names
  5. Scan PMC full text for potentially related accessions
  6. Output everything as a JSON file to bring back into Cowork

Requirements:
    pip install biopython requests

Usage:
    python fetch_geo_sra_metadata.py
    python fetch_geo_sra_metadata.py --retry 5
    python fetch_geo_sra_metadata.py --retry 5 --retry-wait 2.0 --timeout 120
    # Produces: yeolab_geo_sra_results.json

Author: Generated for Brian Yee / Yeo Lab
"""

import json
import re
import time
import sys
import os
import argparse
import socket
import xml.etree.ElementTree as ET
from datetime import datetime
from collections import defaultdict

try:
    from Bio import Entrez
except ImportError:
    print("ERROR: Biopython required. Install with: pip install biopython")
    sys.exit(1)

try:
    import requests
except ImportError:
    print("ERROR: requests required. Install with: pip install requests")
    sys.exit(1)

# ============================================================
# CONFIGURATION
# ============================================================
Entrez.email = "brian.alan.yee@gmail.com"  # Required by NCBI
Entrez.api_key = '9e09d5d38c680a8358426f7fac6d154b4f08'  # Optional: set for 10 req/s instead of 3

OUTPUT_FILE = "yeolab_geo_sra_results.json"
AUTHOR_QUERY = "Yeo GW[Author]"
RATE_LIMIT = 0.34 if Entrez.api_key else 0.5  # seconds between requests
DEFAULT_RETRIES = 3
REQUEST_TIMEOUT = 120.0
RETRY_BASE_WAIT = 2.0

# Regex patterns for accession extraction from full text
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
    'ERP': re.compile(r'\b(ERP\d{5,9})\b'),
    'DRP': re.compile(r'\b(DRP\d{5,9})\b'),
    'ENCSR': re.compile(r'\b(ENCSR\d{3}[A-Z]{3})\b'),
    'ENCFF': re.compile(r'\b(ENCFF\d{3}[A-Z]{3})\b'),
    'E-MTAB': re.compile(r'\b(E-MTAB-\d{3,6})\b'),
    'E-GEOD': re.compile(r'\b(E-GEOD-\d{3,6})\b'),
    'MASSIVE': re.compile(r'\b(MSV\d{6,9})\b'),
    'PXD': re.compile(r'\b(PXD\d{5,9})\b'),
}


def rate_limit():
    """Respect NCBI rate limits."""
    time.sleep(RATE_LIMIT)


def run_with_retries(
    fn,
    retries=DEFAULT_RETRIES,
    label="request",
    command=None,
    base_wait=RETRY_BASE_WAIT,
):
    """Run a callable with retry/backoff for transient request failures."""
    def _partial_read_details(exc):
        """Return debug text for partial payloads (e.g., IncompleteRead.partial)."""
        partial = getattr(exc, "partial", None)
        if partial is None:
            return ""
        if isinstance(partial, (bytes, bytearray)):
            raw = bytes(partial)
            preview = raw[:512]
            decoded = preview.decode("utf-8", errors="replace")
            return (
                f"\n    partial_bytes_len: {len(raw)}"
                f"\n    partial_bytes_hex_preview: {preview.hex()}"
                f"\n    partial_bytes_utf8_preview: {decoded}"
            )
        if isinstance(partial, str):
            preview = partial[:512]
            return (
                f"\n    partial_text_len: {len(partial)}"
                f"\n    partial_text_preview: {preview}"
            )
        return f"\n    partial_value: {repr(partial)}"

    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            result = fn()
            if attempt > 1:
                cmd_text = f"\n    command: {command}" if command else ""
                print(
                    f"\n  Success: {label} recovered after {attempt} attempts"
                    f"{cmd_text}"
                )
            return result
        except Exception as exc:
            last_exc = exc
            if attempt >= retries:
                break
            wait_s = max(RATE_LIMIT, base_wait * (2 ** (attempt - 1)))
            cmd_text = f"\n    command: {command}" if command else ""
            partial_text = _partial_read_details(exc)
            print(
                f"\n  Warning: {label} failed (attempt {attempt}/{retries})"
                f"{cmd_text}\n    error: {type(exc).__name__}: {exc}"
                f"{partial_text}"
                f"\n    retrying in {wait_s:.2f}s..."
            )
            time.sleep(wait_s)
    raise last_exc


# ============================================================
# STEP 1: Get all PMIDs
# ============================================================
def get_all_pmids(query=AUTHOR_QUERY):
    """Fetch all PMIDs for a PubMed query."""
    print(f"[1/6] Searching PubMed for: {query}")
    handle = Entrez.esearch(db="pubmed", term=query, retmax=1000, sort="pub_date")
    record = Entrez.read(handle)
    handle.close()
    pmids = record["IdList"]
    total = int(record["Count"])
    print(f"  Found {total} publications, retrieved {len(pmids)} PMIDs")
    rate_limit()
    return pmids


# ============================================================
# STEP 2: ELink PMIDs -> GEO datasets
# ============================================================
def get_geo_links_for_pmids(pmids, batch_size=50, retries=DEFAULT_RETRIES, retry_wait=RETRY_BASE_WAIT):
    """Use NCBI ELink to find GEO datasets linked to PMIDs."""
    print(f"\n[2/6] Querying NCBI ELink for GEO links ({len(pmids)} PMIDs)...")
    pmid_to_gds = defaultdict(set)  # PMID -> set of GDS IDs
    pmid_to_gse = defaultdict(set)  # PMID -> set of GSE accessions

    for i in range(0, len(pmids), batch_size):
        batch = pmids[i:i + batch_size]
        pct = (i + len(batch)) / len(pmids) * 100
        print(f"  Processing batch {i // batch_size + 1} ({pct:.0f}%)...", end="\r")
        cmd_desc = (
            "Entrez.elink(dbfrom='pubmed', db='gds', linkname='pubmed_gds', "
            f"id_count={len(batch)}, first_id={batch[0]}, last_id={batch[-1]})"
        )

        try:
            # Link pubmed -> gds (GEO DataSets)
            def _elink_batch():
                handle = Entrez.elink(dbfrom="pubmed", db="gds", id=batch, linkname="pubmed_gds")
                try:
                    return Entrez.read(handle)
                finally:
                    handle.close()

            records = run_with_retries(
                _elink_batch,
                retries=retries,
                label=f"ELink pubmed->gds batch {i // batch_size + 1}",
                command=cmd_desc,
                base_wait=retry_wait,
            )
            rate_limit()

            for rec in records:
                pmid = rec["IdList"][0] if rec["IdList"] else None
                if not pmid:
                    continue
                for linkset in rec.get("LinkSetDb", []):
                    for link in linkset.get("Link", []):
                        gds_id = link["Id"]
                        pmid_to_gds[pmid].add(gds_id)
        except Exception as e:
            print(
                f"\n  Warning: ELink pubmed->gds batch {i // batch_size + 1} failed after {retries} attempts"
                f"\n    command: {cmd_desc}\n    error: {type(e).__name__}: {e}"
            )
            rate_limit()

    # Convert GDS IDs to GSE accessions using ESummary
    all_gds_ids = set()
    for gds_set in pmid_to_gds.values():
        all_gds_ids.update(gds_set)

    print(f"\n  Found {len(all_gds_ids)} unique GDS IDs across {len(pmid_to_gds)} PMIDs")

    gds_to_gse = {}
    if all_gds_ids:
        gds_list = list(all_gds_ids)
        for i in range(0, len(gds_list), 100):
            batch = gds_list[i:i + 100]
            try:
                handle = Entrez.esummary(db="gds", id=",".join(batch))
                records = Entrez.read(handle)
                handle.close()
                rate_limit()

                for rec in records:
                    gds_id = str(rec.get("Id", ""))
                    accession = rec.get("Accession", "")
                    # GDS records have GSE in the "GSE" field or Accession
                    gse = rec.get("GSE", "")
                    if gse:
                        gse_acc = f"GSE{gse}"
                    elif accession.startswith("GSE"):
                        gse_acc = accession
                    else:
                        gse_acc = accession
                    gds_to_gse[gds_id] = {
                        'accession': gse_acc,
                        'title': rec.get("title", ""),
                        'summary': rec.get("summary", ""),
                        'gpl': rec.get("GPL", ""),
                        'gse': rec.get("GSE", ""),
                        'taxon': rec.get("taxon", ""),
                        'gdsType': rec.get("gdsType", ""),
                        'ptechType': rec.get("ptechType", ""),
                        'n_samples': rec.get("n_samples", 0),
                        'pdat': rec.get("PDAT", ""),
                        'suppFile': rec.get("suppFile", ""),
                        'ftpLink': rec.get("FTPLink", ""),
                        'entryType': rec.get("entryType", ""),
                    }
            except Exception as e:
                print(f"  Warning: ESummary batch failed: {e}")
                rate_limit()

    # Map back to PMIDs
    for pmid, gds_ids in pmid_to_gds.items():
        for gds_id in gds_ids:
            if gds_id in gds_to_gse:
                acc = gds_to_gse[gds_id].get('accession', '')
                pmid_to_gse[pmid].add(acc)

    return pmid_to_gds, pmid_to_gse, gds_to_gse


# ============================================================
# STEP 3: Get detailed GEO series metadata
# ============================================================
def get_geo_series_details(gse_accessions):
    """Fetch detailed metadata for GSE series from GEO."""
    print(f"\n[3/6] Fetching detailed GEO series metadata ({len(gse_accessions)} series)...")
    geo_details = {}

    for i, gse in enumerate(gse_accessions):
        pct = (i + 1) / len(gse_accessions) * 100
        print(f"  Fetching {gse} ({i + 1}/{len(gse_accessions)}, {pct:.0f}%)...", end="\r")

        try:
            # Use GEO SOFT format for detailed metadata
            gse_num = gse.replace("GSE", "")
            url = f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={gse}&targ=self&form=text&view=brief"
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                text = resp.text
                detail = parse_geo_soft(text, gse)
                geo_details[gse] = detail

            rate_limit()
        except Exception as e:
            print(f"\n  Warning: Failed to fetch {gse}: {e}")
            geo_details[gse] = {"error": str(e)}
            rate_limit()

    print(f"\n  Retrieved details for {len(geo_details)} GEO series")
    return geo_details


def parse_geo_soft(text, accession):
    """Parse GEO SOFT format text into a dictionary."""
    result = {"accession": accession, "samples": [], "supplementary_files": []}
    current_key = None

    for line in text.split("\n"):
        line = line.strip()
        if line.startswith("!Series_"):
            key = line.split("=")[0].replace("!Series_", "").strip()
            val = "=".join(line.split("=")[1:]).strip() if "=" in line else ""

            if key == "title":
                result["title"] = val
            elif key == "summary":
                result.setdefault("summary", "")
                result["summary"] += val + " "
            elif key == "overall_design":
                result.setdefault("overall_design", "")
                result["overall_design"] += val + " "
            elif key == "type":
                result.setdefault("types", [])
                result["types"].append(val)
            elif key == "sample_id":
                result["samples"].append(val)
            elif key == "platform_id":
                result.setdefault("platforms", [])
                result["platforms"].append(val)
            elif key == "supplementary_file":
                result["supplementary_files"].append(val)
            elif key in ("submission_date", "last_update_date", "status",
                         "pubmed_id", "relation", "contact_name",
                         "contact_institute", "geo_accession"):
                if key == "pubmed_id":
                    result.setdefault("pubmed_ids", [])
                    result["pubmed_ids"].append(val)
                elif key == "relation":
                    result.setdefault("relations", [])
                    result["relations"].append(val)
                else:
                    result[key] = val
            elif key.startswith("sample_taxid") or key == "sample_organism":
                result.setdefault("organisms", set())
                result["organisms"].add(val)

    # Convert sets to lists for JSON serialization
    if "organisms" in result:
        result["organisms"] = list(result["organisms"])

    result["n_samples"] = len(result["samples"])
    return result


# ============================================================
# STEP 4: Get SRA metadata and file names
# ============================================================
def get_sra_for_geo(gse_accessions, geo_details):
    """Find SRA experiments (SRX) linked to GEO series, then resolve SRR runs per SRX."""
    print(f"\n[4/6] Fetching SRA metadata for GEO-linked projects...")
    sra_data = {}        # SRR -> run record
    srx_data = {}        # SRX -> experiment record (with nested runs)

    # Collect SRA project links from GEO relations
    # GEO SOFT relations look like:
    #   "SRA: https://www.ncbi.nlm.nih.gov/sra?term=SRP123456"
    #   "BioProject: https://www.ncbi.nlm.nih.gov/bioproject/PRJNA123456"
    #   "SuperSeries of: GSE..."
    srp_to_gse = {}   # SRP accession -> GSE
    prjna_to_gse = {} # PRJNA accession -> GSE
    gse_to_srp = {}   # GSE -> SRP (for fast lookup in search loop)
    gse_to_prjna = {} # GSE -> PRJNA

    for gse, detail in geo_details.items():
        for rel in detail.get("relations", []):
            srp_match = re.search(r'(SRP\d+)', rel)
            if srp_match:
                srp = srp_match.group(1)
                srp_to_gse[srp] = gse
                gse_to_srp[gse] = srp
            prjna_match = re.search(r'(PRJNA\d+)', rel)
            if prjna_match:
                prjna = prjna_match.group(1)
                prjna_to_gse[prjna] = gse
                gse_to_prjna[gse] = prjna

    # Search SRA for each GSE → get SRX experiments → resolve SRR runs
    for idx, gse in enumerate(gse_accessions):
        pct = (idx + 1) / len(gse_accessions) * 100
        try:
            # Build the best possible search term:
            # 1. Prefer SRP (most direct link to SRA study)
            # 2. Fall back to PRJNA (BioProject)
            # 3. Fall back to GSE accession using the dedicated SRA field [GSEL]
            #    (GSEL = GEO series link field in SRA Entrez)
            # Using [All Fields] alone is unreliable because it does fuzzy text matching.
            if gse in gse_to_srp:
                search_term = f"{gse_to_srp[gse]}[All Fields]"
            elif gse in gse_to_prjna:
                search_term = f"{gse_to_prjna[gse]}[BioProject]"
            else:
                # [GSEL] is the SRA Entrez field for GEO series accessions
                search_term = f"{gse}[GSEL]"

            handle = Entrez.esearch(db="sra", term=search_term, retmax=2000)
            record = Entrez.read(handle)
            handle.close()
            rate_limit()

            sra_ids = record.get("IdList", [])

            # If primary search returned nothing, try [All Fields] as last resort
            if not sra_ids:
                handle = Entrez.esearch(db="sra", term=f"{gse}[All Fields]", retmax=2000)
                record = Entrez.read(handle)
                handle.close()
                rate_limit()
                sra_ids = record.get("IdList", [])

            if not sra_ids:
                continue

            print(f"  {gse}: {len(sra_ids)} SRA records ({pct:.0f}% of series done)")

            # Fetch full SRA XML for these IDs (contains SRX + SRR hierarchy)
            for j in range(0, len(sra_ids), 100):
                batch = sra_ids[j:j + 100]
                try:
                    handle = Entrez.efetch(db="sra", id=batch, rettype="full", retmode="xml")
                    xml_bytes = handle.read()
                    handle.close()
                    rate_limit()

                    experiments = parse_sra_xml(xml_bytes, gse)
                    for exp in experiments:
                        srx = exp.get("srx", "")
                        if srx:
                            srx_data[srx] = exp
                        for run in exp.get("runs", []):
                            srr = run.get("accession", "")
                            if srr:
                                # Flatten into sra_data keyed by SRR
                                sra_data[srr] = {
                                    "Run": srr,
                                    "Experiment": srx,
                                    "Sample": exp.get("sample_accession", ""),
                                    "SampleName": exp.get("sample_name", ""),
                                    "SRAStudy": exp.get("study_accession", ""),
                                    "BioProject": exp.get("bioproject", ""),
                                    "BioSample": exp.get("biosample", ""),
                                    "LibraryName": exp.get("library_name", ""),
                                    "LibraryStrategy": exp.get("library_strategy", ""),
                                    "LibrarySource": exp.get("library_source", ""),
                                    "LibrarySelection": exp.get("library_selection", ""),
                                    "LibraryLayout": exp.get("library_layout", ""),
                                    "Platform": exp.get("platform", ""),
                                    "Model": exp.get("instrument_model", ""),
                                    "ScientificName": exp.get("organism", ""),
                                    "spots": run.get("total_spots", ""),
                                    "bases": run.get("total_bases", ""),
                                    "size_MB": run.get("size_mb", ""),
                                    "download_path": run.get("sra_url", ""),
                                    # Preserve original file names
                                    "run_alias": run.get("alias", ""),
                                    "run_file_names": run.get("file_names", []),
                                    "experiment_alias": exp.get("alias", ""),
                                    "experiment_title": exp.get("title", ""),
                                    "source_gse": gse,
                                }
                except Exception as e:
                    print(f"    Warning: SRA XML fetch failed for {gse} batch: {e}")
                    rate_limit()

            # Also fetch RunInfo CSV as fallback for any missing fields
            try:
                handle = Entrez.efetch(db="sra", id=sra_ids[:500],
                                       rettype="runinfo", retmode="text")
                runinfo_text = handle.read()
                handle.close()
                rate_limit()

                csv_runs = parse_sra_runinfo_csv(runinfo_text, gse)
                for run in csv_runs:
                    srr = run.get("Run", "")
                    if srr and srr not in sra_data:
                        sra_data[srr] = run
                    elif srr and srr in sra_data:
                        # Merge in any fields missing from XML
                        for k, v in run.items():
                            if v and not sra_data[srr].get(k):
                                sra_data[srr][k] = v
            except Exception:
                rate_limit()

        except Exception as e:
            print(f"  Warning: SRA search failed for {gse}: {e}")
            rate_limit()

    print(f"  Retrieved {len(srx_data)} SRX experiments, {len(sra_data)} SRR runs")
    return sra_data, srx_data


def parse_sra_xml(xml_bytes, source_gse):
    """Parse SRA full XML to extract SRX experiments with nested SRR runs.

    The SRA XML hierarchy is:
        <EXPERIMENT_PACKAGE>
            <EXPERIMENT accession="SRX..." alias="...">
                <DESIGN>
                    <LIBRARY_DESCRIPTOR>...</LIBRARY_DESCRIPTOR>
                </DESIGN>
                <PLATFORM>...</PLATFORM>
            </EXPERIMENT>
            <SAMPLE accession="SRS..." alias="...">
                <SAMPLE_ATTRIBUTES>...</SAMPLE_ATTRIBUTES>
            </SAMPLE>
            <STUDY accession="SRP...">...</STUDY>
            <RUN_SET>
                <RUN accession="SRR..." alias="..." total_spots="..." total_bases="..." size="...">
                    <SRAFiles>
                        <SRAFile filename="..." url="..."/>
                    </SRAFiles>
                </RUN>
            </RUN_SET>
        </EXPERIMENT_PACKAGE>
    """
    experiments = []

    try:
        if isinstance(xml_bytes, bytes):
            xml_text = xml_bytes.decode("utf-8", errors="replace")
        else:
            xml_text = str(xml_bytes)

        root = ET.fromstring(xml_text)
    except ET.ParseError:
        # Sometimes NCBI wraps multiple packages without a root
        try:
            if isinstance(xml_bytes, bytes):
                xml_text = xml_bytes.decode("utf-8", errors="replace")
            else:
                xml_text = str(xml_bytes)
            xml_text = f"<ROOT>{xml_text}</ROOT>"
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            return experiments

    for pkg in root.iter("EXPERIMENT_PACKAGE"):
        exp = {
            "source_gse": source_gse,
            "runs": [],
        }

        # --- EXPERIMENT ---
        exp_elem = pkg.find("EXPERIMENT")
        if exp_elem is not None:
            exp["srx"] = exp_elem.get("accession", "")
            exp["alias"] = exp_elem.get("alias", "")

            title_elem = exp_elem.find("TITLE")
            if title_elem is not None and title_elem.text:
                exp["title"] = title_elem.text.strip()

            # DESIGN / LIBRARY_DESCRIPTOR
            lib = exp_elem.find(".//LIBRARY_DESCRIPTOR")
            if lib is not None:
                for field, tag in [
                    ("library_name", "LIBRARY_NAME"),
                    ("library_strategy", "LIBRARY_STRATEGY"),
                    ("library_source", "LIBRARY_SOURCE"),
                    ("library_selection", "LIBRARY_SELECTION"),
                ]:
                    elem = lib.find(tag)
                    if elem is not None and elem.text:
                        exp[field] = elem.text.strip()

                layout_elem = lib.find("LIBRARY_LAYOUT")
                if layout_elem is not None:
                    if layout_elem.find("PAIRED") is not None:
                        exp["library_layout"] = "PAIRED"
                    elif layout_elem.find("SINGLE") is not None:
                        exp["library_layout"] = "SINGLE"

            # PLATFORM
            plat_elem = exp_elem.find("PLATFORM")
            if plat_elem is not None:
                for child in plat_elem:
                    exp["platform"] = child.tag  # e.g. ILLUMINA
                    model_elem = child.find("INSTRUMENT_MODEL")
                    if model_elem is not None and model_elem.text:
                        exp["instrument_model"] = model_elem.text.strip()
                    break

        # --- SAMPLE ---
        sample_elem = pkg.find("SAMPLE")
        if sample_elem is not None:
            exp["sample_accession"] = sample_elem.get("accession", "")
            exp["sample_alias"] = sample_elem.get("alias", "")

            # Sample name from TITLE or alias
            stitle = sample_elem.find("TITLE")
            if stitle is not None and stitle.text:
                exp["sample_name"] = stitle.text.strip()
            else:
                exp["sample_name"] = exp.get("sample_alias", "")

            # Organism
            sci_name = sample_elem.find(".//SCIENTIFIC_NAME")
            if sci_name is not None and sci_name.text:
                exp["organism"] = sci_name.text.strip()

            # BioSample
            ext_ids = sample_elem.findall(".//EXTERNAL_ID")
            for eid in ext_ids:
                if eid.get("namespace", "").upper() == "BIOSAMPLE":
                    exp["biosample"] = eid.text.strip() if eid.text else ""

            # Sample attributes (often contain original file names, cell line, etc.)
            sample_attrs = {}
            for attr in sample_elem.findall(".//SAMPLE_ATTRIBUTE"):
                tag = attr.find("TAG")
                val = attr.find("VALUE")
                if tag is not None and tag.text and val is not None and val.text:
                    sample_attrs[tag.text.strip()] = val.text.strip()
            exp["sample_attributes"] = sample_attrs

            # Try to find original file name from common attribute keys
            for key in ["source_name", "original_file", "filename", "file_name",
                        "submitted_file_name", "raw_file", "fastq_file"]:
                if key in sample_attrs:
                    exp.setdefault("original_file_names", []).append(sample_attrs[key])

        # --- STUDY ---
        study_elem = pkg.find("STUDY")
        if study_elem is not None:
            exp["study_accession"] = study_elem.get("accession", "")
            # BioProject
            ext_ids = study_elem.findall(".//EXTERNAL_ID")
            for eid in ext_ids:
                if eid.get("namespace", "").upper() == "BIOPROJECT":
                    exp["bioproject"] = eid.text.strip() if eid.text else ""

        # --- RUN_SET ---
        for run_elem in pkg.findall(".//RUN"):
            run = {
                "accession": run_elem.get("accession", ""),
                "alias": run_elem.get("alias", ""),
                "total_spots": run_elem.get("total_spots", ""),
                "total_bases": run_elem.get("total_bases", ""),
                "published": run_elem.get("published", ""),
                "file_names": [],
            }

            # Size in bytes → MB
            size_bytes = run_elem.get("size", "")
            if size_bytes:
                try:
                    run["size_mb"] = f"{int(size_bytes) / (1024 * 1024):.2f}"
                except (ValueError, TypeError):
                    run["size_mb"] = ""

            # SRA file URLs and original file names
            for sra_file in run_elem.findall(".//SRAFile"):
                fname = sra_file.get("filename", "")
                url = sra_file.get("url", "")
                if fname:
                    run["file_names"].append(fname)
                if url and not run.get("sra_url"):
                    run["sra_url"] = url

            # Cloud file URIs (AWS/GCP)
            for cloud_file in run_elem.findall(".//CloudFile"):
                fname = cloud_file.get("filename", "")
                ftype = cloud_file.get("filetype", "")
                provider = cloud_file.get("provider", "")
                loc = cloud_file.get("location", "")
                if fname:
                    run["file_names"].append(fname)
                if loc:
                    run.setdefault("cloud_urls", []).append({
                        "provider": provider,
                        "location": loc,
                        "filetype": ftype,
                    })

            exp["runs"].append(run)

        experiments.append(exp)

    return experiments


def parse_sra_runinfo_csv(text, gse):
    """Parse SRA RunInfo CSV as fallback. Returns list of dicts keyed by column name."""
    import csv, io
    runs = []
    if not text or not text.strip():
        return runs
    try:
        reader = csv.DictReader(io.StringIO(text.strip()))
        for record in reader:
            record["source_gse"] = gse
            runs.append(dict(record))
    except Exception:
        pass
    return runs


# ============================================================
# STEP 5: Scan PMC full text for additional accessions
# ============================================================
def scan_pmc_for_accessions(pmids, retries=DEFAULT_RETRIES, retry_wait=RETRY_BASE_WAIT):
    """Fetch PMC full text and scan for potentially related dataset accessions."""
    print(f"\n[5/6] Scanning PMC full text for dataset accessions...")

    # First, convert PMIDs to PMCIDs
    pmid_to_pmcid = {}
    for i in range(0, len(pmids), 100):
        batch = pmids[i:i + 100]
        cmd_desc = (
            "Entrez.elink(dbfrom='pubmed', db='pmc', linkname='pubmed_pmc', "
            f"id_count={len(batch)}, first_id={batch[0]}, last_id={batch[-1]})"
        )
        try:
            def _pmc_elink_batch():
                handle = Entrez.elink(dbfrom="pubmed", db="pmc", id=batch, linkname="pubmed_pmc")
                try:
                    return Entrez.read(handle)
                finally:
                    handle.close()

            records = run_with_retries(
                _pmc_elink_batch,
                retries=retries,
                label=f"ELink pubmed->pmc batch {i // 100 + 1}",
                command=cmd_desc,
                base_wait=retry_wait,
            )
            rate_limit()

            for rec in records:
                pmid = rec["IdList"][0] if rec["IdList"] else None
                if not pmid:
                    continue
                for linkset in rec.get("LinkSetDb", []):
                    for link in linkset.get("Link", []):
                        pmcid = link["Id"]
                        pmid_to_pmcid[pmid] = f"PMC{pmcid}"
        except Exception as e:
            print(
                f"\n  Warning: ELink pubmed->pmc batch {i // 100 + 1} failed after {retries} attempts"
                f"\n    command: {cmd_desc}\n    error: {type(e).__name__}: {e}"
            )
            rate_limit()

    print(f"  Found {len(pmid_to_pmcid)} papers with PMC full text")

    # Fetch full text and extract accessions
    text_accessions = defaultdict(lambda: defaultdict(set))  # pmid -> accession_type -> set(accessions)
    pmcids = list(pmid_to_pmcid.items())

    for i, (pmid, pmcid) in enumerate(pmcids):
        pct = (i + 1) / len(pmcids) * 100
        if (i + 1) % 10 == 0:
            print(f"  Scanning {i + 1}/{len(pmcids)} ({pct:.0f}%)...", end="\r")

        try:
            handle = Entrez.efetch(db="pmc", id=pmcid.replace("PMC", ""),
                                   rettype="xml", retmode="xml")
            xml_text = handle.read()
            handle.close()
            rate_limit()

            # Extract all text content from XML
            full_text = extract_text_from_pmc_xml(xml_text)

            # Search for accessions
            for acc_type, pattern in ACCESSION_PATTERNS.items():
                matches = pattern.findall(full_text)
                for m in set(matches):
                    text_accessions[pmid][acc_type].add(m)

        except Exception as e:
            # Skip errors silently
            rate_limit()

    # Convert sets to lists
    result = {}
    total_acc = 0
    for pmid, acc_dict in text_accessions.items():
        result[pmid] = {}
        for acc_type, accs in acc_dict.items():
            result[pmid][acc_type] = list(accs)
            total_acc += len(accs)

    print(f"\n  Extracted {total_acc} accession mentions from {len(result)} papers")
    return result


def extract_text_from_pmc_xml(xml_bytes):
    """Extract all text content from PMC XML."""
    try:
        if isinstance(xml_bytes, bytes):
            text = xml_bytes.decode('utf-8', errors='replace')
        else:
            text = str(xml_bytes)

        # Simple approach: strip XML tags to get text
        clean = re.sub(r'<[^>]+>', ' ', text)
        clean = re.sub(r'\s+', ' ', clean)
        return clean
    except Exception:
        return ""


# ============================================================
# STEP 6: Assemble and output results
# ============================================================
def assemble_results(pmids, pmid_to_gds, pmid_to_gse, gds_to_gse,
                     geo_details, sra_data, srx_data, text_accessions):
    """Assemble all results into a single JSON structure."""
    print(f"\n[6/6] Assembling results...")

    # Build GSE -> SRX experiments mapping (preserves SRX→SRR hierarchy)
    gse_to_experiments = defaultdict(list)
    for srx, exp in srx_data.items():
        gse = exp.get("source_gse", "")
        gse_to_experiments[gse].append({
            "srx": srx,
            "alias": exp.get("alias", ""),
            "title": exp.get("title", ""),
            "sample_accession": exp.get("sample_accession", ""),
            "sample_name": exp.get("sample_name", ""),
            "sample_attributes": exp.get("sample_attributes", {}),
            "original_file_names": exp.get("original_file_names", []),
            "study_accession": exp.get("study_accession", ""),
            "bioproject": exp.get("bioproject", ""),
            "biosample": exp.get("biosample", ""),
            "library_name": exp.get("library_name", ""),
            "library_strategy": exp.get("library_strategy", ""),
            "library_source": exp.get("library_source", ""),
            "library_selection": exp.get("library_selection", ""),
            "library_layout": exp.get("library_layout", ""),
            "platform": exp.get("platform", ""),
            "instrument_model": exp.get("instrument_model", ""),
            "organism": exp.get("organism", ""),
            "runs": [
                {
                    "srr": run.get("accession", ""),
                    "alias": run.get("alias", ""),
                    "total_spots": run.get("total_spots", ""),
                    "total_bases": run.get("total_bases", ""),
                    "size_mb": run.get("size_mb", ""),
                    "published": run.get("published", ""),
                    "file_names": run.get("file_names", []),
                    "sra_url": run.get("sra_url", ""),
                    "cloud_urls": run.get("cloud_urls", []),
                }
                for run in exp.get("runs", [])
            ],
        })

    # Also build flat GSE -> SRR mapping from sra_data for backward compat
    gse_to_runs = defaultdict(list)
    for srr, run_data in sra_data.items():
        gse = run_data.get("source_gse", "")
        gse_to_runs[gse].append({
            "run_accession": run_data.get("Run", ""),
            "experiment_accession": run_data.get("Experiment", ""),
            "sample_accession": run_data.get("Sample", ""),
            "sample_name": run_data.get("SampleName", ""),
            "study_accession": run_data.get("SRAStudy", ""),
            "bioproject": run_data.get("BioProject", ""),
            "biosample": run_data.get("BioSample", ""),
            "library_name": run_data.get("LibraryName", ""),
            "library_strategy": run_data.get("LibraryStrategy", ""),
            "library_source": run_data.get("LibrarySource", ""),
            "library_selection": run_data.get("LibrarySelection", ""),
            "library_layout": run_data.get("LibraryLayout", ""),
            "platform": run_data.get("Platform", ""),
            "model": run_data.get("Model", ""),
            "organism": run_data.get("ScientificName", ""),
            "spots": run_data.get("spots", ""),
            "bases": run_data.get("bases", ""),
            "size_mb": run_data.get("size_MB", ""),
            "download_path": run_data.get("download_path", ""),
            "run_alias": run_data.get("run_alias", ""),
            "run_file_names": run_data.get("run_file_names", []),
            "experiment_alias": run_data.get("experiment_alias", ""),
            "experiment_title": run_data.get("experiment_title", ""),
        })

    # Collect all unique accessions from text scanning
    all_text_accessions = defaultdict(lambda: {"pmids": [], "type": ""})
    for pmid, acc_dict in text_accessions.items():
        for acc_type, accs in acc_dict.items():
            for acc in accs:
                all_text_accessions[acc]["pmids"].append(pmid)
                all_text_accessions[acc]["type"] = acc_type

    # Build per-PMID dataset links
    pmid_datasets = {}
    pmid_potential_datasets = {}
    potential_dataset_count = 0
    for pmid in pmids:
        datasets = []
        potential_datasets = []

        # From ELink
        for gse in pmid_to_gse.get(pmid, set()):
            detail = geo_details.get(gse, {})
            experiments = gse_to_experiments.get(gse, [])
            runs_flat = gse_to_runs.get(gse, [])
            datasets.append({
                "accession": gse,
                "type": "GSE",
                "database": "GEO",
                "source": "ncbi_elink",
                "title": detail.get("title", ""),
                "summary": detail.get("summary", ""),
                "overall_design": detail.get("overall_design", ""),
                "organisms": detail.get("organisms", []),
                "platforms": detail.get("platforms", []),
                "n_samples": detail.get("n_samples", 0),
                "supplementary_files": detail.get("supplementary_files", []),
                "sra_experiments": experiments,   # SRX→SRR hierarchy
                "sra_runs": runs_flat,            # flat SRR list for compat
            })

        # From text scanning (potentially related; keep separate from strong links)
        if pmid in text_accessions:
            for acc_type, accs in text_accessions[pmid].items():
                for acc in accs:
                    # Avoid duplicating GSEs already found via ELink
                    if acc_type == "GSE" and acc in pmid_to_gse.get(pmid, set()):
                        continue
                    db_map = {
                        'GSE': 'GEO', 'GSM': 'GEO', 'GDS': 'GEO', 'GPL': 'GEO',
                        'SRP': 'SRA', 'SRR': 'SRA', 'SRX': 'SRA', 'SRS': 'SRA',
                        'PRJNA': 'BioProject', 'PRJEB': 'BioProject',
                        'ERP': 'ENA', 'DRP': 'DDBJ',
                        'ENCSR': 'ENCODE', 'ENCFF': 'ENCODE',
                        'E-MTAB': 'ArrayExpress', 'E-GEOD': 'ArrayExpress',
                        'MASSIVE': 'MassIVE', 'PXD': 'PRIDE',
                    }
                    potential_entry = {
                        "accession": acc,
                        "type": acc_type,
                        "database": db_map.get(acc_type, "Unknown"),
                        "source": "potentially_related_dataset",
                    }
                    # For text-mined GSE accessions, attach GEO/SRA metadata when available.
                    if acc_type == "GSE":
                        detail = geo_details.get(acc, {})
                        experiments = gse_to_experiments.get(acc, [])
                        runs_flat = gse_to_runs.get(acc, [])
                        potential_entry.update({
                            "title": detail.get("title", ""),
                            "summary": detail.get("summary", ""),
                            "overall_design": detail.get("overall_design", ""),
                            "organisms": detail.get("organisms", []),
                            "platforms": detail.get("platforms", []),
                            "n_samples": detail.get("n_samples", 0),
                            "supplementary_files": detail.get("supplementary_files", []),
                            "submission_date": detail.get("submission_date", ""),
                            "last_update_date": detail.get("last_update_date", ""),
                            "status": detail.get("status", ""),
                            "contact_name": detail.get("contact_name", ""),
                            "contact_institute": detail.get("contact_institute", ""),
                            "types": detail.get("types", []),
                            "relations": detail.get("relations", []),
                            "samples": detail.get("samples", []),
                            "pubmed_ids": detail.get("pubmed_ids", []),
                            "sra_experiments": experiments,
                            "sra_runs": runs_flat,
                        })
                    potential_datasets.append(potential_entry)
                    potential_dataset_count += 1

        if datasets:
            pmid_datasets[pmid] = datasets
        if potential_datasets:
            pmid_potential_datasets[pmid] = potential_datasets

    result = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "author_query": AUTHOR_QUERY,
            "total_pmids": len(pmids),
            "pmids_with_datasets": len(pmid_datasets),
            "total_geo_series": len(geo_details),
            "total_srx_experiments": len(srx_data),
            "total_sra_runs": len(sra_data),
            "total_text_accessions": len(all_text_accessions),
            "pmids_with_potential_datasets": len(pmid_potential_datasets),
            "total_potential_dataset_links": potential_dataset_count,
        },
        "pmids": pmids,
        "pmid_datasets": pmid_datasets,
        "pmid_potential_datasets": pmid_potential_datasets,
        "geo_details": {k: make_serializable(v) for k, v in geo_details.items()},
        "srx_experiments": {k: make_serializable(v) for k, v in srx_data.items()},
        "all_accessions": {k: make_serializable(v) for k, v in all_text_accessions.items()},
        "sra_runs": {k: make_serializable(v) for k, v in sra_data.items()},
    }

    return result


def make_serializable(obj):
    """Convert sets and other non-serializable types to lists."""
    if isinstance(obj, set):
        return list(obj)
    elif isinstance(obj, dict):
        return {k: make_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_serializable(v) for v in obj]
    return obj


# ============================================================
# MAIN
# ============================================================
def main():
    global REQUEST_TIMEOUT, RETRY_BASE_WAIT
    parser = argparse.ArgumentParser(description="Yeo Lab GEO/SRA dataset fetcher")
    parser.add_argument(
        "--retry",
        type=int,
        default=DEFAULT_RETRIES,
        help=f"Retry attempts for transient NCBI request failures (default: {DEFAULT_RETRIES})",
    )
    parser.add_argument(
        "--retry-wait",
        type=float,
        default=RETRY_BASE_WAIT,
        help=f"Initial retry backoff in seconds (exponential). Default: {RETRY_BASE_WAIT}",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=REQUEST_TIMEOUT,
        help=f"Network socket timeout in seconds. Default: {REQUEST_TIMEOUT}",
    )
    args = parser.parse_args()
    retries = max(1, int(args.retry))
    retry_wait = max(0.1, float(args.retry_wait))
    REQUEST_TIMEOUT = max(5.0, float(args.timeout))
    RETRY_BASE_WAIT = retry_wait
    socket.setdefaulttimeout(REQUEST_TIMEOUT)

    print("=" * 60)
    print("YEO LAB GEO/SRA DATASET FETCHER")
    print("=" * 60)
    print(f"Email: {Entrez.email}")
    print(f"API Key: {'Set' if Entrez.api_key else 'Not set (will be slower)'}")
    print(f"Rate limit: {RATE_LIMIT}s between requests")
    print(f"Socket timeout: {REQUEST_TIMEOUT:.1f}s")
    print(f"Retry backoff: {RETRY_BASE_WAIT:.2f}s base (exponential), retries={retries}")
    print()

    if Entrez.api_key is None:
        print("TIP: Set NCBI_API_KEY env var for 10 req/sec instead of 3.")
        print("     Get a free key at: https://www.ncbi.nlm.nih.gov/account/settings/")
        print()

    # Step 1: Get all PMIDs
    pmids = get_all_pmids()

    # Step 2: Find GEO links via ELink
    pmid_to_gds, pmid_to_gse, gds_to_gse = get_geo_links_for_pmids(
        pmids,
        retries=retries,
        retry_wait=retry_wait,
    )

    # Collect all unique GSE accessions
    all_gse = set()
    for gse_set in pmid_to_gse.values():
        all_gse.update(gse_set)
    # Also add GSE from gds_to_gse
    for gds_id, info in gds_to_gse.items():
        acc = info.get("accession", "")
        if acc.startswith("GSE"):
            all_gse.add(acc)

    print(f"\n  Total unique GSE series: {len(all_gse)}")

    # Step 3: Get detailed GEO metadata
    geo_details = get_geo_series_details(list(all_gse))

    # Step 4: Get SRA metadata (SRX experiments → SRR runs)
    sra_data, srx_data = get_sra_for_geo(list(all_gse), geo_details)

    # Step 5: Scan PMC full text for additional accessions
    text_accessions = scan_pmc_for_accessions(
        pmids,
        retries=retries,
        retry_wait=retry_wait,
    )

    # Enrich text-mined GSE accessions with GEO/SRA metadata wherever possible
    text_mined_gse = set()
    for pmid, acc_dict in text_accessions.items():
        for gse in acc_dict.get("GSE", []):
            text_mined_gse.add(gse)
    extra_gse = sorted(g for g in text_mined_gse if g not in all_gse)
    if extra_gse:
        print(f"\n  Enriching {len(extra_gse)} text-mined GSE accessions with GEO/SRA metadata...")
        extra_geo_details = get_geo_series_details(extra_gse)
        geo_details.update(extra_geo_details)
        extra_sra_data, extra_srx_data = get_sra_for_geo(extra_gse, extra_geo_details)
        sra_data.update(extra_sra_data)
        srx_data.update(extra_srx_data)
        all_gse.update(extra_gse)

    # Step 6: Assemble and output
    results = assemble_results(pmids, pmid_to_gds, pmid_to_gse, gds_to_gse,
                               geo_details, sra_data, srx_data, text_accessions)

    # Save to JSON
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(results, f, indent=2, default=str)

    print(f"\n{'=' * 60}")
    print(f"RESULTS SAVED TO: {OUTPUT_FILE}")
    print(f"{'=' * 60}")
    print(f"\nSummary:")
    for k, v in results["metadata"].items():
        print(f"  {k}: {v}")
    print(f"\nFile size: {os.path.getsize(OUTPUT_FILE) / 1024:.1f} KB")
    print(f"\nNext step: Bring {OUTPUT_FILE} back into Cowork to populate the database.")


if __name__ == "__main__":
    main()
