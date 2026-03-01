import json
import re
from collections import defaultdict
from django.shortcuts import render, get_object_or_404
from django.db.models import Q, Count, Sum
from django.db import connection
from django.core.paginator import Paginator
from django.http import JsonResponse, HttpResponse, StreamingHttpResponse
from django.views.decorators.http import require_GET, require_POST
from django.views.decorators.csrf import csrf_protect, csrf_exempt
from django.contrib.auth.decorators import login_required
from .models import (
    Publication, Author, PublicationAuthor, DatasetAccession,
    PublicationDataset, DatasetFile, Grant, PublicationGrant,
    SraExperiment, SraRun,
)


def _fts_search(query, limit=500):
    """Full-text search. Uses PostgreSQL tsvector or SQLite FTS5 depending on backend."""
    with connection.cursor() as cur:
        try:
            if connection.vendor == 'postgresql':
                # PostgreSQL: use tsvector/tsquery via the search_vector column
                cur.execute(
                    """SELECT pmid FROM publications
                       WHERE search_vector @@ plainto_tsquery('english', %s)
                       ORDER BY ts_rank(search_vector, plainto_tsquery('english', %s)) DESC
                       LIMIT %s""",
                    [query, query, limit],
                )
            else:
                # SQLite: use FTS5 MATCH
                safe_q = query.replace('"', '""')
                fts_query = f'"{safe_q}"'
                cur.execute(
                    "SELECT pmid FROM publications_fts WHERE publications_fts MATCH ? LIMIT ?",
                    [fts_query, limit],
                )
            return [row[0] for row in cur.fetchall()]
        except Exception:
            return None


def home(request):
    """Landing page with search bar + stats."""
    stats = {}
    stats["publications"] = Publication.objects.count()
    stats["authors"] = Author.objects.count()
    stats["datasets"] = DatasetAccession.objects.count()
    stats["files"] = DatasetFile.objects.count()
    stats["grants"] = Grant.objects.count()
    stats["sra_experiments"] = SraExperiment.objects.count()
    stats["sra_runs"] = SraRun.objects.count()

    year_counts = (
        Publication.objects.values("pub_year")
        .annotate(count=Count("pmid"))
        .order_by("pub_year")
    )
    stats["year_data"] = json.dumps(
        [{"year": yc["pub_year"], "count": yc["count"]}
         for yc in year_counts if yc["pub_year"]]
    )

    top_journals = (
        Publication.objects.values("journal_name")
        .annotate(count=Count("pmid"))
        .order_by("-count")[:10]
    )
    stats["top_journals"] = top_journals

    return render(request, "publications/home.html", {"stats": stats})


def search(request):
    """Search publications via FTS5 or LIKE fallback."""
    query = request.GET.get("q", "").strip()
    year = request.GET.get("year", "")
    journal = request.GET.get("journal", "")
    author_q = request.GET.get("author", "")
    page_num = request.GET.get("page", 1)

    results = Publication.objects.all()

    if query:
        pmids = _fts_search(query)
        if pmids is not None:
            results = results.filter(pmid__in=pmids)
        else:
            results = results.filter(
                Q(title__icontains=query)
                | Q(abstract__icontains=query)
                | Q(journal_name__icontains=query)
            )

    if year:
        try:
            results = results.filter(pub_year=int(year))
        except ValueError:
            pass

    if journal:
        results = results.filter(journal_name=journal)

    if author_q:
        matching_authors = Author.objects.filter(
            Q(last_name__icontains=author_q) | Q(fore_name__icontains=author_q)
        ).values_list("author_id", flat=True)
        matching_pmids = (
            PublicationAuthor.objects.filter(author_id__in=matching_authors)
            .values_list("pmid", flat=True)
            .distinct()
        )
        results = results.filter(pmid__in=matching_pmids)

    results = results.order_by("-pub_year", "-pub_month", "-pub_day")

    paginator = Paginator(results, 25)
    page = paginator.get_page(page_num)

    years = (
        Publication.objects.values_list("pub_year", flat=True)
        .distinct()
        .order_by("-pub_year")
    )
    journals = (
        Publication.objects.values("journal_name")
        .annotate(count=Count("pmid"))
        .order_by("-count")[:30]
    )

    ctx = {
        "query": query,
        "year": year,
        "journal": journal,
        "author_q": author_q,
        "page": page,
        "total": paginator.count,
        "years": [y for y in years if y],
        "journals": journals,
    }
    return render(request, "publications/search.html", ctx)


def publication_detail(request, pmid):
    """Detail page for a single publication."""
    pub = get_object_or_404(Publication, pmid=pmid)

    authors = (
        PublicationAuthor.objects.filter(pmid=pmid)
        .select_related("author")
        .order_by("author_position")
    )

    all_datasets = (
        PublicationDataset.objects.filter(pmid=pmid)
        .select_related("accession")
    )

    # Separate strong links from potentially related datasets
    datasets = [d for d in all_datasets if d.source != "potentially_related_dataset"]
    potential_datasets = [d for d in all_datasets if d.source == "potentially_related_dataset"]

    grants = (
        PublicationGrant.objects.filter(pmid=pmid)
        .select_related("grant")
    )

    # Get files for each dataset (both strong and potential)
    dataset_ids = [d.accession_id for d in all_datasets]
    files = DatasetFile.objects.filter(accession_id__in=dataset_ids).order_by("file_name")

    files_by_acc = {}
    for f in files:
        files_by_acc.setdefault(f.accession_id, []).append(f)

    ctx = {
        "pub": pub,
        "authors": authors,
        "datasets": datasets,
        "potential_datasets": potential_datasets,
        "grants": grants,
        "files_by_acc": files_by_acc,
    }
    return render(request, "publications/detail.html", ctx)


def author_detail(request, author_id):
    """Author page showing all their publications."""
    author = get_object_or_404(Author, author_id=author_id)

    pub_links = (
        PublicationAuthor.objects.filter(author=author)
        .select_related("pmid")
        .order_by("-pmid__pub_year", "-pmid__pub_month")
    )

    publications = [pa.pmid for pa in pub_links]

    pmids = [pa.pmid_id for pa in pub_links]
    coauthors = (
        PublicationAuthor.objects.filter(pmid__in=pmids)
        .exclude(author=author)
        .values("author__author_id", "author__fore_name", "author__last_name")
        .annotate(shared=Count("pmid"))
        .order_by("-shared")[:20]
    )

    first_author_count = sum(1 for pa in pub_links if pa.is_first_author)
    last_author_count = sum(1 for pa in pub_links if pa.is_last_author)

    ctx = {
        "author": author,
        "publications": publications,
        "coauthors": coauthors,
        "total_pubs": len(publications),
        "first_author_count": first_author_count,
        "last_author_count": last_author_count,
    }
    return render(request, "publications/author_detail.html", ctx)


def author_list(request):
    """Browse all authors, sorted by publication count."""
    page_num = request.GET.get("page", 1)
    q = request.GET.get("q", "").strip()

    authors = (
        Author.objects.annotate(pub_count=Count("publicationauthor"))
        .filter(pub_count__gt=0)
        .order_by("-pub_count")
    )

    if q:
        authors = authors.filter(
            Q(last_name__icontains=q) | Q(fore_name__icontains=q)
        )

    paginator = Paginator(authors, 50)
    page = paginator.get_page(page_num)

    return render(request, "publications/author_list.html", {
        "page": page, "q": q, "total": paginator.count,
    })


def dataset_list(request):
    """Browse datasets with filters."""
    page_num = request.GET.get("page", 1)
    q = request.GET.get("q", "").strip()
    acc_type = request.GET.get("type", "")

    datasets = DatasetAccession.objects.all()

    if q:
        sra_exp_matches = SraExperiment.objects.filter(
            Q(srx_accession__icontains=q)
            | Q(source_gse__icontains=q)
            | Q(title__icontains=q)
            | Q(alias__icontains=q)
            | Q(sample_accession__icontains=q)
            | Q(sample_name__icontains=q)
            | Q(sample_alias__icontains=q)
            | Q(study_accession__icontains=q)
            | Q(bioproject__icontains=q)
            | Q(biosample__icontains=q)
            | Q(library_name__icontains=q)
            | Q(library_strategy__icontains=q)
            | Q(library_source__icontains=q)
            | Q(library_selection__icontains=q)
            | Q(library_layout__icontains=q)
            | Q(platform__icontains=q)
            | Q(instrument_model__icontains=q)
            | Q(organism__icontains=q)
            | Q(sample_attributes__icontains=q)
            | Q(original_file_names__icontains=q)
        ).values_list("parent_accession_id", flat=True)

        sra_run_matches = SraRun.objects.filter(
            Q(srr_accession__icontains=q)
            | Q(srx_accession__icontains=q)
            | Q(alias__icontains=q)
            | Q(sra_url__icontains=q)
            | Q(cloud_urls__icontains=q)
            | Q(file_names__icontains=q)
        ).values_list("experiment__parent_accession_id", flat=True)

        datasets = datasets.filter(
            Q(accession__icontains=q)
            | Q(title__icontains=q)
            | Q(organism__icontains=q)
            | Q(platform__icontains=q)
            | Q(summary__icontains=q)
            | Q(overall_design__icontains=q)
            | Q(experiment_types__icontains=q)
            | Q(relations__icontains=q)
            | Q(sample_ids__icontains=q)
            | Q(accession_id__in=sra_exp_matches)
            | Q(accession_id__in=sra_run_matches)
        )

    if acc_type:
        datasets = datasets.filter(accession_type=acc_type)

    datasets = datasets.order_by("-accession_id")

    paginator = Paginator(datasets, 25)
    page = paginator.get_page(page_num)

    types = (
        DatasetAccession.objects.values("accession_type")
        .annotate(count=Count("accession_id"))
        .order_by("-count")
    )

    return render(request, "publications/dataset_list.html", {
        "page": page, "q": q, "acc_type": acc_type,
        "types": types, "total": paginator.count,
    })


def _parse_json_field(value):
    """Safely parse a JSON text field into a Python object."""
    if not value:
        return None
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return None


def dataset_detail(request, accession_id):
    """Detail view for a single dataset accession."""
    dataset = get_object_or_404(DatasetAccession, accession_id=accession_id)

    pub_links = (
        PublicationDataset.objects.filter(accession=dataset)
        .select_related("pmid")
    )
    publications = [pl.pmid for pl in pub_links]

    files = DatasetFile.objects.filter(accession=dataset).order_by("file_type", "file_name")
    is_encode = (
        (dataset.database or "").upper() == "ENCODE"
        or (dataset.accession or "").upper().startswith("ENC")
        or (dataset.accession_type or "").upper().startswith("ENC")
    )
    encode_portal_url = ""
    accession_upper = (dataset.accession or "").upper()
    if accession_upper.startswith("ENCSR"):
        encode_portal_url = f"https://www.encodeproject.org/experiments/{dataset.accession}/"
    elif accession_upper.startswith("ENCFF"):
        encode_portal_url = f"https://www.encodeproject.org/files/{dataset.accession}/"
    elif accession_upper.startswith("ENCAT"):
        encode_portal_url = f"https://www.encodeproject.org/annotations/{dataset.accession}/"
    elif is_encode and dataset.accession:
        encode_portal_url = f"https://www.encodeproject.org/search/?searchTerm={dataset.accession}"

    # Parse supplementary files JSON
    supp_files_raw = _parse_json_field(dataset.supplementary_files)
    supplementary_files = []
    if isinstance(supp_files_raw, list):
        for url in supp_files_raw:
            if isinstance(url, str) and url.strip():
                fname = url.rstrip("/").split("/")[-1]
                supplementary_files.append({"url": url.strip(), "name": fname})

    # Parse relations JSON
    relations_list = _parse_json_field(dataset.relations) or []

    # Parse experiment_types JSON
    experiment_types_list = _parse_json_field(dataset.experiment_types) or []

    # Parse sample_ids JSON
    sample_ids_list = _parse_json_field(dataset.sample_ids) or []
    # Parse citation PMIDs JSON
    citation_pmids = _parse_json_field(dataset.citation_pmids) or []
    if not isinstance(citation_pmids, list):
        citation_pmids = []
    citation_pmids = [str(p).strip() for p in citation_pmids if str(p).strip()]

    citation_map = {pub.pmid: pub for pub in Publication.objects.filter(pmid__in=citation_pmids)}
    citation_author_links = (
        PublicationAuthor.objects.filter(pmid_id__in=citation_pmids)
        .select_related("author")
        .order_by("pmid_id", "author_position")
    )
    authors_by_citation = defaultdict(list)
    for pa in citation_author_links:
        if pa.author and pa.author.display_name:
            authors_by_citation[pa.pmid_id].append(pa.author.display_name)

    citation_items = []
    for pmid in citation_pmids:
        pub = citation_map.get(pmid)
        citation_items.append({
            "pmid": pmid,
            "pub": pub,
            "authors": authors_by_citation.get(pmid, []),
            "pubmed_url": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
        })

    sra_experiments = list(
        SraExperiment.objects.filter(
            Q(parent_accession=dataset) | Q(srx_accession=dataset.accession)
        )
        .order_by("srx_accession")
    )
    experiment_ids = [exp.experiment_id for exp in sra_experiments]
    runs_by_experiment = defaultdict(list)

    def _hydrate_run_metadata(run):
        run.file_names_list = []
        run.cloud_urls_list = []
        if run.file_names:
            try:
                run.file_names_list = json.loads(run.file_names)
            except json.JSONDecodeError:
                run.file_names_list = [run.file_names]
        if run.cloud_urls:
            try:
                run.cloud_urls_list = json.loads(run.cloud_urls)
            except json.JSONDecodeError:
                run.cloud_urls_list = [run.cloud_urls]
        return run

    if experiment_ids:
        runs = (
            SraRun.objects.filter(experiment_id__in=experiment_ids)
            .order_by("experiment_id", "srr_accession")
        )
        for run in runs:
            _hydrate_run_metadata(run)
            runs_by_experiment[run.experiment_id].append(run)

    direct_sra_runs = []
    if dataset.accession_type == "SRR":
        direct_sra_runs = list(
            SraRun.objects.filter(srr_accession=dataset.accession).order_by("srr_accession")
        )
        for run in direct_sra_runs:
            _hydrate_run_metadata(run)

    for exp in sra_experiments:
        exp.sample_attributes_map = {}
        exp.original_file_names_list = []
        if exp.sample_attributes:
            try:
                parsed_attrs = json.loads(exp.sample_attributes)
                if isinstance(parsed_attrs, dict):
                    exp.sample_attributes_map = parsed_attrs
            except json.JSONDecodeError:
                pass
        if exp.original_file_names:
            try:
                parsed_files = json.loads(exp.original_file_names)
                if isinstance(parsed_files, list):
                    exp.original_file_names_list = parsed_files
                elif isinstance(parsed_files, str):
                    exp.original_file_names_list = [parsed_files]
            except json.JSONDecodeError:
                exp.original_file_names_list = [exp.original_file_names]
        exp.runs = runs_by_experiment.get(exp.experiment_id, [])

    # Compute total size for download info
    all_runs = []
    for exp in sra_experiments:
        all_runs.extend(exp.runs)
    all_runs.extend(direct_sra_runs)
    total_size_mb = sum(r.size_mb or 0 for r in all_runs)

    # Collect available file types for the download filter
    download_file_types = set()
    # From supplementary files: extract extensions
    for sf in supplementary_files:
        name = sf.get("name", "")
        if name:
            # Handle double extensions like .bed.gz, .bigWig, etc.
            ext = _extract_file_extension(name)
            if ext:
                download_file_types.add(ext)
    # From dataset_files
    for f in files:
        if f.file_type:
            download_file_types.add(f.file_type)
        elif f.file_name:
            ext = _extract_file_extension(f.file_name)
            if ext:
                download_file_types.add(ext)
    # SRA runs are always FASTQ/SRA
    if all_runs:
        download_file_types.add("sra/fastq")
    download_file_types = sorted(download_file_types)

    # ENCODE-specific metadata parsing
    encode_summary_parts = {}
    encode_file_rows = []
    detailed_file_rows = []
    encode_file_formats = defaultdict(int)
    encode_output_types = defaultdict(int)
    encode_file_accessions = set()
    encode_total_file_size_bytes = 0

    summary = (dataset.summary or "").strip()
    if summary and " of " in summary and " in " in summary:
        try:
            assay, rest = summary.split(" of ", 1)
            target, biosample = rest.split(" in ", 1)
            if assay.strip():
                encode_summary_parts["assay"] = assay.strip()
            if target.strip():
                encode_summary_parts["target"] = target.strip()
            if biosample.strip():
                encode_summary_parts["biosample"] = biosample.strip()
        except ValueError:
            pass

    def _infer_assembly(text):
        """Best-effort assembly parsing from file metadata strings."""
        if not text:
            return ""
        match = re.search(
            r"\b(hg\d{2}|mm\d{1,2}|rn\d{1,2}|dm\d{1,2}|ce\d{1,2}|GRCh\d{2}|GRCm\d{2}|T2T-CHM13v?\d*|CHM13v?\d*)\b",
            str(text),
            flags=re.IGNORECASE,
        )
        if match:
            return match.group(1)
        return ""

    def _extract_mapping_assembly(*values):
        """
        Prefer explicit 'mapping assembly' metadata, with fallback to assembly hints.
        """
        explicit_patterns = [
            re.compile(r"mapping assembly[:=\s]+([A-Za-z0-9._-]+)", flags=re.IGNORECASE),
            re.compile(r"\bassembly[:=\s]+([A-Za-z0-9._-]+)", flags=re.IGNORECASE),
        ]
        for value in values:
            if not value:
                continue
            text = str(value)
            for pattern in explicit_patterns:
                match = pattern.search(text)
                if match:
                    return match.group(1)
        for value in values:
            assembly = _infer_assembly(value)
            if assembly:
                return assembly
        return ""

    for f in files:
        file_name = (f.file_name or "").strip()
        file_type = (f.file_type or "").strip()
        file_type_core = file_type.split(";", 1)[0].strip() if file_type else ""

        file_accession = ""
        if file_name:
            token = file_name.split(".", 1)[0]
            if token.upper().startswith("ENC"):
                file_accession = token
                encode_file_accessions.add(token)

        file_format = ""
        output_type = ""
        if file_type_core:
            if " (" in file_type_core and file_type_core.endswith(")"):
                file_format, output_type = file_type_core[:-1].split(" (", 1)
                file_format = file_format.strip()
                output_type = output_type.strip()
            else:
                file_format = file_type_core
        if not file_format:
            file_format = _extract_file_extension(file_name)

        if file_format:
            encode_file_formats[file_format] += 1
        if output_type:
            encode_output_types[output_type] += 1
        if f.file_size_bytes:
            encode_total_file_size_bytes += f.file_size_bytes

        encode_file_rows.append({
            "model": f,
            "file_accession": file_accession,
            "file_format": file_format,
            "output_type": output_type,
            "portal_url": f"https://www.encodeproject.org/files/{file_accession}/" if file_accession else "",
        })
        url_host = ""
        if f.file_url:
            url_no_scheme = f.file_url.split("://", 1)[-1]
            url_host = url_no_scheme.split("/", 1)[0]
        mapping_assembly = _extract_mapping_assembly(file_type, file_name, f.file_url)

        detailed_file_rows.append({
            "model": f,
            "file_accession": file_accession,
            "file_format": file_format,
            "output_type": output_type,
            "extension": _extract_file_extension(file_name or (f.file_url or "")),
            "portal_url": f"https://www.encodeproject.org/files/{file_accession}/" if file_accession else "",
            "url_host": url_host,
            "mapping_assembly": mapping_assembly,
        })

    encode_file_formats = sorted(
        encode_file_formats.items(), key=lambda item: (-item[1], item[0].lower())
    )
    encode_output_types = sorted(
        encode_output_types.items(), key=lambda item: (-item[1], item[0].lower())
    )

    return render(request, "publications/dataset_detail.html", {
        "dataset": dataset,
        "is_encode": is_encode,
        "encode_portal_url": encode_portal_url,
        "publications": publications,
        "files": files,
        "supplementary_files": supplementary_files,
        "relations_list": relations_list,
        "experiment_types_list": experiment_types_list,
        "sample_ids_list": sample_ids_list,
        "citation_items": citation_items,
        "sra_experiments": sra_experiments,
        "direct_sra_runs": direct_sra_runs,
        "sra_experiment_count": len(sra_experiments),
        "sra_run_count": sum(len(exp.runs) for exp in sra_experiments) + len(direct_sra_runs),
        "total_size_mb": total_size_mb,
        "download_file_types": download_file_types,
        "encode_summary_parts": encode_summary_parts,
        "encode_file_rows": encode_file_rows,
        "detailed_file_rows": detailed_file_rows,
        "encode_file_formats": encode_file_formats,
        "encode_output_types": encode_output_types,
        "encode_file_accession_count": len(encode_file_accessions),
        "encode_total_file_size_bytes": encode_total_file_size_bytes,
    })


# ============================================================
# File type extraction helper
# ============================================================
def _extract_file_extension(filename):
    """
    Extract a meaningful file type/extension from a filename.
    Handles compound extensions like .bed.gz, .bigWig, .tar.gz, etc.
    """
    if not filename:
        return ""
    name = filename.rstrip("/").split("/")[-1]  # basename
    name = name.split("?")[0]  # strip query strings
    lower = name.lower()

    # Common compound extensions
    compound = [
        ".bed.gz", ".narrowpeak.gz", ".broadpeak.gz", ".bigwig",
        ".bigbed", ".bam.bai", ".tar.gz", ".fastq.gz", ".fq.gz",
        ".vcf.gz", ".gtf.gz", ".gff3.gz", ".wig.gz", ".tsv.gz",
        ".csv.gz", ".txt.gz", ".sam.gz",
    ]
    for ext in compound:
        if lower.endswith(ext):
            return ext.lstrip(".")

    # ENCODE-style file_type field (e.g. "bam (alignments)")
    if "(" in name:
        return name.split("(")[0].strip()

    # Standard single extension
    parts = name.rsplit(".", 1)
    if len(parts) == 2 and len(parts[1]) <= 10:
        return parts[1].lower()
    return ""


def _file_matches_types(filename, file_type, url, types_filter):
    """
    Check if a file matches any of the requested type filters.
    types_filter is a set of lowercase type strings.
    """
    if not types_filter:
        return True  # No filter = include everything

    # Check file_type field directly (for dataset_files)
    if file_type:
        ft_lower = file_type.lower()
        # Match the file_type itself or its base (e.g., "bam (alignments)" matches "bam")
        for t in types_filter:
            if t in ft_lower or ft_lower.startswith(t):
                return True

    # Check filename extension
    ext = _extract_file_extension(filename or url or "")
    if ext:
        ext_lower = ext.lower()
        for t in types_filter:
            if t in ext_lower or ext_lower.startswith(t):
                return True

    # Check URL extension as fallback
    if url:
        url_ext = _extract_file_extension(url)
        if url_ext:
            for t in types_filter:
                if t in url_ext.lower():
                    return True

    return False


# ============================================================
# Bulk download script generation
# ============================================================
@require_GET
def dataset_download_script(request, accession_id):
    """Generate a shell script to bulk-download all files for a dataset.

    Query params:
        format: 'bash' (default) or 'urls'
        types: comma-separated file types to include (e.g., 'bam,bed.gz,bigWig')
               if omitted, all files are included
        file_ids: comma-separated dataset_files.file_id values; when provided,
                  only those selected dataset files are included
    """
    dataset = get_object_or_404(DatasetAccession, accession_id=accession_id)
    fmt = request.GET.get("format", "bash")  # bash or urls
    types_param = request.GET.get("types", "").strip()
    types_filter = set(t.strip().lower() for t in types_param.split(",") if t.strip()) if types_param else set()
    include_sra = not types_filter or "sra/fastq" in types_filter or "sra" in types_filter or "fastq" in types_filter
    file_ids_param = request.GET.get("file_ids", "").strip()
    selected_file_ids = set()
    if file_ids_param:
        for token in file_ids_param.split(","):
            token = token.strip()
            if token.isdigit():
                selected_file_ids.add(int(token))

    # Collect all downloadable URLs as (category, url, filename, file_type, file_id)
    all_items = []

    # 1. Supplementary files (GEO FTP)
    supp_files = _parse_json_field(dataset.supplementary_files)
    if isinstance(supp_files, list):
        for url in supp_files:
            if isinstance(url, str) and url.strip():
                fname = url.strip().rstrip("/").split("/")[-1]
                all_items.append(("supplementary", url.strip(), fname, "", None))

    # 2. SRA run URLs
    experiments = SraExperiment.objects.filter(
        Q(parent_accession=dataset) | Q(srx_accession=dataset.accession)
    )
    exp_ids = [e.experiment_id for e in experiments]
    runs = SraRun.objects.filter(experiment_id__in=exp_ids).order_by("srr_accession") if exp_ids else SraRun.objects.none()

    # Also check direct runs for SRR-type accessions
    if dataset.accession_type == "SRR":
        runs = SraRun.objects.filter(srr_accession=dataset.accession)

    for run in runs:
        if run.sra_url:
            all_items.append(("sra", run.sra_url, run.srr_accession or "", "sra/fastq", None))

    # 3. dataset_files URLs
    dataset_file_qs = DatasetFile.objects.filter(accession=dataset).order_by("file_type", "file_name")
    if selected_file_ids:
        dataset_file_qs = dataset_file_qs.filter(file_id__in=selected_file_ids)
    for f in dataset_file_qs:
        if f.file_url:
            all_items.append(("file", f.file_url, f.file_name or "", f.file_type or "", f.file_id))

    # If explicit file IDs are selected, generate script only from those dataset_files.
    if selected_file_ids:
        all_items = [item for item in all_items if item[0] == "file"]

    # Apply type filter
    if types_filter:
        filtered_items = []
        for cat, url, fname, ftype, file_id in all_items:
            if cat == "sra" and include_sra:
                filtered_items.append((cat, url, fname, ftype, file_id))
            elif _file_matches_types(fname, ftype, url, types_filter):
                filtered_items.append((cat, url, fname, ftype, file_id))
        urls = [(cat, url) for cat, url, _, _, _ in filtered_items]
    else:
        urls = [(cat, url) for cat, url, _, _, _ in all_items]

    type_label = f" (types: {types_param})" if types_param else ""

    if fmt == "urls":
        # Plain list of URLs
        content = "\n".join(url for _, url in urls)
        response = HttpResponse(content, content_type="text/plain")
        response["Content-Disposition"] = f'attachment; filename="{dataset.accession}_urls.txt"'
        return response

    # Generate bash script
    lines = [
        "#!/usr/bin/env bash",
        f"# Bulk download script for {dataset.accession}{type_label}",
        f"# Generated from Yeo Lab Publications Database",
        f"# Total files: {len(urls)}",
        "",
        f'OUTDIR="{dataset.accession}"',
        'mkdir -p "$OUTDIR"',
        'cd "$OUTDIR"',
        "",
    ]

    if any(cat == "supplementary" for cat, _ in urls):
        lines.append("# --- GEO supplementary files ---")
        for cat, url in urls:
            if cat == "supplementary":
                fname = url.rstrip("/").split("/")[-1]
                # Use wget for FTP, curl for HTTP
                if url.startswith("ftp://"):
                    lines.append(f'wget -nc "{url}" -O "{fname}"')
                else:
                    lines.append(f'curl -L -O -C - "{url}"')
        lines.append("")

    sra_urls = [(cat, url) for cat, url in urls if cat == "sra"]
    if sra_urls:
        lines.append("# --- SRA run files ---")
        lines.append("# Tip: use 'fasterq-dump' from SRA Toolkit for FASTQ conversion:")
        lines.append("#   fasterq-dump --split-files SRR_ACCESSION")
        lines.append("")

        # Collect SRR accessions for fasterq-dump
        srr_accessions = []
        for run in runs:
            if run.srr_accession:
                srr_accessions.append(run.srr_accession)

        if srr_accessions:
            lines.append("# Option A: Download via SRA Toolkit (recommended)")
            for srr in srr_accessions:
                lines.append(f"fasterq-dump --split-files {srr}")
            lines.append("")
            lines.append("# Option B: Direct download (larger .sra files)")
            for cat, url in sra_urls:
                fname = url.rstrip("/").split("/")[-1]
                lines.append(f'curl -L -O -C - "{url}"')
            lines.append("")

    file_urls = [(cat, url) for cat, url in urls if cat == "file"]
    if file_urls:
        lines.append("# --- Additional data files ---")
        for cat, url in file_urls:
            fname = url.rstrip("/").split("/")[-1]
            lines.append(f'curl -L -O -C - "{url}"')
        lines.append("")

    lines.append('echo "Download complete. Files saved to $OUTDIR"')

    content = "\n".join(lines)
    response = HttpResponse(content, content_type="text/x-shellscript")
    response["Content-Disposition"] = f'attachment; filename="download_{dataset.accession}.sh"'
    return response


# ============================================================
# REST API views (JSON)
# ============================================================

def _pub_to_dict(pub):
    """Serialize a Publication to a dict."""
    return {
        "pmid": pub.pmid,
        "pmc_id": pub.pmc_id,
        "doi": pub.doi,
        "title": pub.title,
        "abstract": pub.abstract,
        "journal": pub.journal_name,
        "journal_iso": pub.journal_iso,
        "pub_year": pub.pub_year,
        "pub_date": pub.pub_date,
        "volume": pub.volume,
        "issue": pub.issue,
        "pages": pub.pages,
        "pub_types": pub.pub_types_list,
        "keywords": pub.keywords_list,
        "mesh_terms": pub.mesh_terms_list,
        "is_open_access": bool(pub.is_open_access),
        "pubmed_url": pub.pubmed_url,
        "doi_url": pub.doi_url,
    }


def _dataset_to_dict(ds):
    """Serialize a DatasetAccession to a dict."""
    result = {
        "accession_id": ds.accession_id,
        "accession": ds.accession,
        "type": ds.accession_type,
        "database": ds.database,
        "title": ds.title,
        "organism": ds.organism,
        "platform": ds.platform,
        "summary": ds.summary,
        "overall_design": ds.overall_design,
        "num_samples": ds.num_samples,
        "submission_date": ds.submission_date,
        "last_update_date": ds.last_update_date,
        "status": ds.status,
        "contact_name": ds.contact_name,
        "contact_institute": ds.contact_institute,
    }
    # Parse JSON fields
    for field in ("supplementary_files", "experiment_types", "relations", "sample_ids"):
        raw = getattr(ds, field, None)
        result[field] = _parse_json_field(raw)
    if ds.geo_url:
        result["geo_url"] = ds.geo_url
    if ds.sra_url:
        result["sra_url"] = ds.sra_url
    return result


def _experiment_to_dict(exp):
    """Serialize an SraExperiment to a dict."""
    attrs = _parse_json_field(exp.sample_attributes)
    orig_files = _parse_json_field(exp.original_file_names)
    return {
        "experiment_id": exp.experiment_id,
        "srx_accession": exp.srx_accession,
        "source_gse": exp.source_gse,
        "title": exp.title,
        "alias": exp.alias,
        "sample_accession": exp.sample_accession,
        "sample_name": exp.sample_name,
        "study_accession": exp.study_accession,
        "bioproject": exp.bioproject,
        "biosample": exp.biosample,
        "library_name": exp.library_name,
        "library_strategy": exp.library_strategy,
        "library_source": exp.library_source,
        "library_selection": exp.library_selection,
        "library_layout": exp.library_layout,
        "platform": exp.platform,
        "instrument_model": exp.instrument_model,
        "organism": exp.organism,
        "sample_attributes": attrs if isinstance(attrs, dict) else None,
        "original_file_names": orig_files if isinstance(orig_files, list) else None,
    }


def _run_to_dict(run):
    """Serialize an SraRun to a dict."""
    files = _parse_json_field(run.file_names)
    clouds = _parse_json_field(run.cloud_urls)
    return {
        "run_id": run.run_id,
        "srr_accession": run.srr_accession,
        "srx_accession": run.srx_accession,
        "alias": run.alias,
        "total_spots": run.total_spots,
        "total_bases": run.total_bases,
        "size_mb": run.size_mb,
        "published_date": run.published_date,
        "sra_url": run.sra_url,
        "file_names": files if isinstance(files, list) else None,
        "cloud_urls": clouds if isinstance(clouds, list) else None,
    }


@require_GET
def api_publications(request):
    """
    GET /api/publications/
    Query params: q, year, journal, author, page, per_page
    """
    q = request.GET.get("q", "").strip()
    year = request.GET.get("year", "")
    journal = request.GET.get("journal", "")
    author_q = request.GET.get("author", "")
    page_num = int(request.GET.get("page", 1))
    per_page = min(int(request.GET.get("per_page", 25)), 100)

    qs = Publication.objects.all()

    if q:
        pmids = _fts_search(q)
        if pmids is not None:
            qs = qs.filter(pmid__in=pmids)
        else:
            qs = qs.filter(
                Q(title__icontains=q) | Q(abstract__icontains=q) | Q(journal_name__icontains=q)
            )

    if year:
        try:
            qs = qs.filter(pub_year=int(year))
        except ValueError:
            pass

    if journal:
        qs = qs.filter(journal_name__icontains=journal)

    if author_q:
        matching = Author.objects.filter(
            Q(last_name__icontains=author_q) | Q(fore_name__icontains=author_q)
        ).values_list("author_id", flat=True)
        pmid_list = (
            PublicationAuthor.objects.filter(author_id__in=matching)
            .values_list("pmid", flat=True).distinct()
        )
        qs = qs.filter(pmid__in=pmid_list)

    qs = qs.order_by("-pub_year", "-pub_month", "-pub_day")
    paginator = Paginator(qs, per_page)
    page = paginator.get_page(page_num)

    return JsonResponse({
        "count": paginator.count,
        "page": page.number,
        "per_page": per_page,
        "total_pages": paginator.num_pages,
        "results": [_pub_to_dict(p) for p in page],
    })


@require_GET
def api_publication_detail(request, pmid):
    """GET /api/publications/<pmid>/"""
    pub = get_object_or_404(Publication, pmid=pmid)
    data = _pub_to_dict(pub)

    # Authors
    authors = (
        PublicationAuthor.objects.filter(pmid=pmid)
        .select_related("author")
        .order_by("author_position")
    )
    data["authors"] = [
        {
            "author_id": pa.author.author_id,
            "name": pa.author.display_name,
            "position": pa.author_position,
            "is_first_author": bool(pa.is_first_author),
            "is_last_author": bool(pa.is_last_author),
            "affiliation": pa.affiliation,
        }
        for pa in authors
    ]

    # Datasets
    pub_datasets = PublicationDataset.objects.filter(pmid=pmid).select_related("accession")
    data["datasets"] = [
        {
            "accession": pd.accession.accession,
            "type": pd.accession.accession_type,
            "database": pd.accession.database,
            "title": pd.accession.title,
            "source": pd.source,
            "accession_id": pd.accession.accession_id,
        }
        for pd in pub_datasets if pd.source != "potentially_related_dataset"
    ]
    data["potentially_related_datasets"] = [
        {
            "accession": pd.accession.accession,
            "type": pd.accession.accession_type,
            "database": pd.accession.database,
            "source": pd.source,
            "accession_id": pd.accession.accession_id,
        }
        for pd in pub_datasets if pd.source == "potentially_related_dataset"
    ]

    # Grants
    grants = PublicationGrant.objects.filter(pmid=pmid).select_related("grant")
    data["grants"] = [
        {
            "grant_number": pg.grant.grant_number,
            "agency": pg.grant.agency,
            "country": pg.grant.country,
        }
        for pg in grants
    ]

    return JsonResponse(data)


@require_GET
def api_datasets(request):
    """
    GET /api/datasets/
    Query params: q, type, page, per_page
    """
    q = request.GET.get("q", "").strip()
    acc_type = request.GET.get("type", "")
    page_num = int(request.GET.get("page", 1))
    per_page = min(int(request.GET.get("per_page", 25)), 100)

    qs = DatasetAccession.objects.all()

    if q:
        qs = qs.filter(
            Q(accession__icontains=q) | Q(title__icontains=q)
            | Q(organism__icontains=q) | Q(summary__icontains=q)
        )

    if acc_type:
        qs = qs.filter(accession_type=acc_type)

    qs = qs.order_by("-accession_id")
    paginator = Paginator(qs, per_page)
    page = paginator.get_page(page_num)

    return JsonResponse({
        "count": paginator.count,
        "page": page.number,
        "per_page": per_page,
        "total_pages": paginator.num_pages,
        "results": [_dataset_to_dict(ds) for ds in page],
    })


@require_GET
def api_dataset_detail(request, accession_id):
    """GET /api/datasets/<accession_id>/"""
    ds = get_object_or_404(DatasetAccession, accession_id=accession_id)
    data = _dataset_to_dict(ds)

    # Linked publications
    links = PublicationDataset.objects.filter(accession=ds).select_related("pmid")
    data["publications"] = [
        {"pmid": pl.pmid.pmid, "title": pl.pmid.title, "source": pl.source}
        for pl in links
    ]

    # SRA experiments + runs
    experiments = SraExperiment.objects.filter(
        Q(parent_accession=ds) | Q(srx_accession=ds.accession)
    ).order_by("srx_accession")
    exp_list = []
    for exp in experiments:
        ed = _experiment_to_dict(exp)
        runs = SraRun.objects.filter(experiment_id=exp.experiment_id).order_by("srr_accession")
        ed["runs"] = [_run_to_dict(r) for r in runs]
        exp_list.append(ed)
    data["sra_experiments"] = exp_list

    # Dataset files
    files = DatasetFile.objects.filter(accession=ds).order_by("file_name")
    data["files"] = [
        {
            "file_name": f.file_name,
            "file_type": f.file_type,
            "file_size_bytes": f.file_size_bytes,
            "file_url": f.file_url,
        }
        for f in files
    ]

    return JsonResponse(data)


@require_GET
def api_authors(request):
    """
    GET /api/authors/
    Query params: q, page, per_page
    """
    q = request.GET.get("q", "").strip()
    page_num = int(request.GET.get("page", 1))
    per_page = min(int(request.GET.get("per_page", 50)), 200)

    qs = Author.objects.annotate(
        pub_count=Count("publicationauthor")
    ).filter(pub_count__gt=0).order_by("-pub_count")

    if q:
        qs = qs.filter(Q(last_name__icontains=q) | Q(fore_name__icontains=q))

    paginator = Paginator(qs, per_page)
    page = paginator.get_page(page_num)

    return JsonResponse({
        "count": paginator.count,
        "page": page.number,
        "per_page": per_page,
        "total_pages": paginator.num_pages,
        "results": [
            {
                "author_id": a.author_id,
                "name": a.display_name,
                "last_name": a.last_name,
                "fore_name": a.fore_name,
                "orcid": a.orcid,
                "pub_count": a.pub_count,
            }
            for a in page
        ],
    })


# ============================================================
# Admin panel & PMID submission
# ============================================================

@login_required
def admin_panel(request):
    """Admin page with update button and status display."""
    from . import services

    deps_ok = services._HAS_BIOPYTHON and services._HAS_REQUESTS
    status = services.get_update_status()

    # Recent update log
    with connection.cursor() as cur:
        cur.execute(
            "SELECT update_time, new_pmids_added, notes FROM update_log ORDER BY update_id DESC LIMIT 10"
        )
        recent_updates = [
            {"time": r[0], "new_pmids": r[1], "notes": r[2]}
            for r in cur.fetchall()
        ]

    return render(request, "publications/admin.html", {
        "deps_ok": deps_ok,
        "status": status,
        "recent_updates": recent_updates,
    })


@login_required
@require_POST
@csrf_protect
def admin_start_update(request):
    """Start a background update via POST."""
    from . import services

    mode = request.POST.get("mode", "full")
    if mode not in ("full", "pubmed", "geo", "encode"):
        return JsonResponse({"error": "Invalid mode"}, status=400)

    try:
        started = services.start_full_update(mode=mode)
        if started:
            return JsonResponse({"ok": True, "message": f"Started {mode} update"})
        else:
            return JsonResponse({"ok": False, "message": "Update already running"})
    except RuntimeError as e:
        return JsonResponse({"ok": False, "message": str(e)}, status=500)


@require_GET
def admin_update_status(request):
    """Poll endpoint for update progress."""
    from . import services
    return JsonResponse(services.get_update_status())


@login_required
@require_POST
@csrf_protect
def admin_preview_add(request):
    """Preview what will be added for a PMID (fetches from PubMed without inserting)."""
    from . import services

    pmid = request.POST.get("pmid", "").strip()
    if not pmid:
        return JsonResponse({"error": "Missing pmid"}, status=400)
    try:
        result = services.preview_pmid(pmid)
        return JsonResponse(result)
    except RuntimeError as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
@require_POST
@csrf_protect
def admin_confirm_add(request):
    """Actually insert a PMID after user confirmation."""
    from . import services

    pmid = request.POST.get("pmid", "").strip()
    if not pmid:
        return JsonResponse({"error": "Missing pmid"}, status=400)
    try:
        result = services.submit_single_pmid(pmid)
        return JsonResponse(result)
    except RuntimeError as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
@require_POST
@csrf_protect
def admin_preview_remove(request):
    """Preview what will be removed for a PMID."""
    from . import services

    pmid = request.POST.get("pmid", "").strip()
    if not pmid:
        return JsonResponse({"error": "Missing pmid"}, status=400)
    try:
        result = services.preview_remove_pmid(pmid)
        return JsonResponse(result)
    except RuntimeError as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
@require_POST
@csrf_protect
def admin_confirm_remove(request):
    """Actually remove a PMID after user confirmation."""
    from . import services

    pmid = request.POST.get("pmid", "").strip()
    if not pmid:
        return JsonResponse({"error": "Missing pmid"}, status=400)
    try:
        result = services.remove_pmid(pmid)
        return JsonResponse(result)
    except RuntimeError as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
@require_POST
@csrf_protect
def api_submit_pmid(request):
    """API endpoint: submit a single PMID via POST JSON."""
    from . import services

    try:
        body = json.loads(request.body)
        pmid = body.get("pmid", "").strip()
    except (json.JSONDecodeError, AttributeError):
        pmid = request.POST.get("pmid", "").strip()

    if not pmid:
        return JsonResponse({"error": "Missing pmid parameter"}, status=400)

    try:
        result = services.submit_single_pmid(pmid)
        return JsonResponse(result)
    except RuntimeError as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
@require_POST
@csrf_protect
def api_remove_pmid(request):
    """API endpoint: remove a PMID via POST JSON."""
    from . import services

    try:
        body = json.loads(request.body)
        pmid = body.get("pmid", "").strip()
    except (json.JSONDecodeError, AttributeError):
        pmid = request.POST.get("pmid", "").strip()

    if not pmid:
        return JsonResponse({"error": "Missing pmid parameter"}, status=400)

    try:
        result = services.remove_pmid(pmid)
        return JsonResponse(result)
    except RuntimeError as e:
        return JsonResponse({"error": str(e)}, status=500)


@require_GET
def api_stats(request):
    """GET /api/stats/ — summary statistics."""
    return JsonResponse({
        "publications": Publication.objects.count(),
        "authors": Author.objects.count(),
        "datasets": DatasetAccession.objects.count(),
        "sra_experiments": SraExperiment.objects.count(),
        "sra_runs": SraRun.objects.count(),
        "data_files": DatasetFile.objects.count(),
        "grants": Grant.objects.count(),
        "top_journals": list(
            Publication.objects.values("journal_name")
            .annotate(count=Count("pmid"))
            .order_by("-count")[:10]
        ),
        "publications_by_year": list(
            Publication.objects.values("pub_year")
            .annotate(count=Count("pmid"))
            .order_by("pub_year")
        ),
        "library_strategies": list(
            SraExperiment.objects.values("library_strategy")
            .annotate(count=Count("experiment_id"))
            .order_by("-count")[:15]
        ),
        "organisms": list(
            SraExperiment.objects.values("organism")
            .annotate(count=Count("experiment_id"))
            .order_by("-count")[:10]
        ),
    })


# ============================================================
# Chat views
# ============================================================

@login_required
def chat_page(request):
    """Render the AI chat interface."""
    return render(request, "publications/chat.html")


@login_required
@require_POST
@csrf_protect
def chat_message(request):
    """SSE streaming endpoint for AI chat messages."""
    from .chat_service import stream_chat

    try:
        body = json.loads(request.body)
    except (json.JSONDecodeError, ValueError):
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    api_key = body.get("api_key", "").strip()
    message = body.get("message", "").strip()
    history = body.get("history", [])
    provider = body.get("provider", "claude").strip().lower()
    model = body.get("model", "").strip()

    if not api_key:
        return JsonResponse({"error": "API key is required"}, status=400)
    if not message:
        return JsonResponse({"error": "Message is required"}, status=400)
    if provider not in {"claude", "openai"}:
        return JsonResponse({"error": "Invalid provider. Must be 'claude' or 'openai'."}, status=400)
    if not model:
        return JsonResponse({"error": "Model is required"}, status=400)

    # Basic API key format validation by provider
    if provider == "claude" and not (api_key.startswith("sk-ant-") or api_key.startswith("sk-")):
        return JsonResponse(
            {"error": "Invalid API key format. Anthropic keys start with 'sk-ant-' or 'sk-'."},
            status=400,
        )
    if provider == "openai" and not api_key.startswith("sk-"):
        return JsonResponse(
            {"error": "Invalid API key format. OpenAI keys typically start with 'sk-'."},
            status=400,
        )

    # Validate history format
    clean_history = []
    for msg in history:
        if isinstance(msg, dict) and "role" in msg and "content" in msg:
            if msg["role"] in ("user", "assistant"):
                content = msg["content"]
                if not isinstance(content, str):
                    content = str(content)
                clean_history.append({
                    "role": msg["role"],
                    "content": content,
                })

    response = StreamingHttpResponse(
        stream_chat(api_key, message, clean_history, provider=provider, model=model),
        content_type="text/event-stream",
    )
    response["Cache-Control"] = "no-cache"
    response["X-Accel-Buffering"] = "no"
    return response
