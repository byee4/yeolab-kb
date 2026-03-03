import json
import re
from collections import defaultdict
from django.shortcuts import render, get_object_or_404
from django.db.models import Q, Count, Sum
from django.db import connection
from django.core.paginator import Paginator
from django.core.cache import cache
from django.http import JsonResponse, HttpResponse, StreamingHttpResponse
from django.views.decorators.http import require_GET, require_POST
from django.views.decorators.csrf import csrf_protect, csrf_exempt
from django.contrib.auth.decorators import login_required
from .models import (
    Publication, Author, PublicationAuthor, DatasetAccession,
    PublicationDataset, DatasetFile, Grant, PublicationGrant,
    SraExperiment, SraRun, ComputationalMethod, PublicationMethod,
    AnalysisPipeline, PipelineStep,
)

SEARCH_PREVIEW_LIMIT = 25
SEARCH_FACETS_CACHE_KEY = "publications_search_facets_v1"
SEARCH_FACETS_CACHE_TTL = 60 * 10
CODE_EXAMPLE_PIPELINES_CACHE_KEY = "code_example_pipelines_v1"
CODE_EXAMPLE_PIPELINES_CACHE_TTL = 60 * 5


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
    """Search publications, datasets, and analyses."""
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

    paginator = Paginator(results, SEARCH_PREVIEW_LIMIT)
    page = paginator.get_page(page_num)

    cached_facets = cache.get(SEARCH_FACETS_CACHE_KEY)
    if cached_facets is None:
        years = list(
            Publication.objects.values_list("pub_year", flat=True)
            .distinct()
            .order_by("-pub_year")
        )
        journals = list(
            Publication.objects.values("journal_name")
            .annotate(count=Count("pmid"))
            .order_by("-count")[:30]
        )
        cached_facets = {
            "years": [y for y in years if y],
            "journals": journals,
        }
        cache.set(SEARCH_FACETS_CACHE_KEY, cached_facets, SEARCH_FACETS_CACHE_TTL)
    years = cached_facets["years"]
    journals = cached_facets["journals"]

    dataset_results = []
    dataset_has_more = False
    analysis_results = []
    analysis_has_more = False

    if len(query) >= 2:
        dataset_qs = DatasetAccession.objects.filter(
            Q(accession__icontains=query)
            | Q(title__icontains=query)
            | Q(organism__icontains=query)
            | Q(platform__icontains=query)
            | Q(summary__icontains=query)
            | Q(overall_design__icontains=query)
            | Q(experiment_types__icontains=query)
        ).order_by("-accession_id")
        dataset_preview = list(dataset_qs[: SEARCH_PREVIEW_LIMIT + 1])
        dataset_has_more = len(dataset_preview) > SEARCH_PREVIEW_LIMIT
        dataset_results = dataset_preview[:SEARCH_PREVIEW_LIMIT]

        # Build merged analysis items, preferring curated code_examples entries
        ce_pipelines = _get_cached_code_example_pipelines()
        q_lower = query.lower()
        ce_matches = [
            p for p in ce_pipelines
            if q_lower in (p.get("pipeline_title") or "").lower()
            or q_lower in (p.get("accession") or "").lower()
            or q_lower in (p.get("acc_title") or "").lower()
            or q_lower in (p.get("pub_title") or "").lower()
            or q_lower in (p.get("assay_type") or "").lower()
        ]
        ce_accessions = {p.get("accession") for p in ce_matches if p.get("accession")}

        db_qs = (
            AnalysisPipeline.objects.select_related("pmid", "accession")
            .annotate(step_count=Count("pipelinestep"))
            .filter(
                Q(pipeline_title__icontains=query)
                | Q(assay_type__icontains=query)
                | Q(source__icontains=query)
                | Q(accession__accession__icontains=query)
                | Q(accession__title__icontains=query)
                | Q(pmid__pmid__icontains=query)
                | Q(pmid__title__icontains=query)
            )
            .order_by("-pmid__pub_year", "pipeline_title")
        )

        db_items = []
        for p in db_qs:
            db_acc = p.accession.accession if p.accession else ""
            if db_acc and db_acc in ce_accessions:
                continue
            db_items.append({
                "id": p.pipeline_id,
                "is_db": True,
                "pipeline_title": p.pipeline_title or "Analysis Pipeline",
                "accession": db_acc,
                "source": p.source or "",
                "assay_type": p.assay_type or "",
                "step_count": p.step_count,
                "pub_title": p.pmid.title if p.pmid else "",
                "pub_year": p.pmid.pub_year if p.pmid else None,
            })

        ce_items = [{
            "id": p["id"],
            "is_db": False,
            "pipeline_title": p["pipeline_title"],
            "accession": p["accession"],
            "source": p["source"],
            "assay_type": p["assay_type"],
            "step_count": p["step_count"],
            "pub_title": p["pub_title"],
            "pub_year": p["pub_year"],
        } for p in ce_matches]

        merged_analysis = db_items + ce_items
        merged_analysis.sort(key=lambda item: (-(item.get("pub_year") or 0), item.get("pipeline_title") or ""))
        analysis_has_more = len(merged_analysis) > SEARCH_PREVIEW_LIMIT
        analysis_results = merged_analysis[:SEARCH_PREVIEW_LIMIT]

    ctx = {
        "query": query,
        "year": year,
        "journal": journal,
        "author_q": author_q,
        "page": page,
        "total": paginator.count,
        "years": years,
        "journals": journals,
        "dataset_results": dataset_results,
        "dataset_has_more": dataset_has_more,
        "analysis_results": analysis_results,
        "analysis_has_more": analysis_has_more,
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

    # Get analysis pipelines for this publication
    pipelines = (
        AnalysisPipeline.objects.filter(pmid=pmid)
        .annotate(step_count=Count("pipelinestep"))
        .select_related("accession")
        .order_by("pipeline_title")
    )

    ctx = {
        "pub": pub,
        "authors": authors,
        "datasets": datasets,
        "potential_datasets": potential_datasets,
        "grants": grants,
        "files_by_acc": files_by_acc,
        "pipelines": pipelines,
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
    from .code_examples import get_steps_for_dataset

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

    analysis_accession = (dataset.accession or "").strip()
    analysis_steps = get_steps_for_dataset(analysis_accession) or []

    return render(request, "publications/dataset_detail.html", {
        "dataset": dataset,
        "analysis_accession": analysis_accession,
        "analysis_steps": analysis_steps,
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
# Computational Methods browsing
# ============================================================

def method_list(request):
    """Browse computational methods with filters."""
    page_num = request.GET.get("page", 1)
    q = request.GET.get("q", "").strip()
    category = request.GET.get("category", "")

    methods = ComputationalMethod.objects.annotate(
        pub_count=Count("publicationmethod")
    )

    if q:
        methods = methods.filter(
            Q(canonical_name__icontains=q) | Q(category__icontains=q)
        )
    if category:
        methods = methods.filter(category=category)

    methods = methods.order_by("-pub_count", "canonical_name")

    paginator = Paginator(methods, 30)
    page = paginator.get_page(page_num)

    # Get category counts for sidebar filter
    categories = (
        ComputationalMethod.objects.values("category")
        .annotate(count=Count("method_id"))
        .order_by("-count")
    )

    return render(request, "publications/method_list.html", {
        "page": page, "q": q, "category": category,
        "categories": categories, "total": paginator.count,
    })


def method_detail(request, method_id):
    """Detail page for a single computational method."""
    method = get_object_or_404(ComputationalMethod, method_id=method_id)

    # Get all publication links with source info
    links = PublicationMethod.objects.filter(
        method=method
    ).select_related("pmid").order_by("-pmid__pub_year", "pmid__pmid")

    # Group by publication for cleaner display
    pub_map = {}
    for link in links:
        pmid = link.pmid_id
        if pmid not in pub_map:
            pub_map[pmid] = {
                "publication": link.pmid,
                "sources": [],
                "version": None,
            }
        pub_map[pmid]["sources"].append(link.source_type)
        if link.version:
            pub_map[pmid]["version"] = link.version

    publications = sorted(pub_map.values(),
                          key=lambda x: x["publication"].pub_year or 0, reverse=True)

    # Source breakdown
    source_counts = defaultdict(int)
    for link in links:
        source_counts[link.source_type] += 1

    return render(request, "publications/method_detail.html", {
        "method": method,
        "publications": publications,
        "source_counts": dict(source_counts),
        "total_pubs": len(pub_map),
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
    if mode not in ("full", "pubmed", "geo", "encode", "methods", "analysis", "pipelines"):
        return JsonResponse({"error": "Invalid mode"}, status=400)

    try:
        if mode == "methods":
            started = services.start_methods_update()
        elif mode in ("analysis", "pipelines"):
            started = services.start_pipeline_update()
            mode = "analysis"
        else:
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
    upload_id = (request.GET.get("upload_id", "") or "").strip()
    return JsonResponse(services.get_update_status(upload_id=upload_id or None))


@login_required
@require_POST
@csrf_protect
def admin_upload_encode_json(request):
    """Upload ENCODE Experiment search JSON and import experiments/processing steps."""
    from . import services

    upload = request.FILES.get("json_file")
    if not upload:
        return JsonResponse({"ok": False, "message": "Missing file field 'json_file'."}, status=400)

    filename = (upload.name or "").lower()
    if not filename.endswith(".json"):
        return JsonResponse({"ok": False, "message": "Please upload a .json file."}, status=400)

    try:
        payload = json.loads(upload.read().decode("utf-8"))
    except Exception as exc:
        return JsonResponse({"ok": False, "message": f"Invalid JSON file: {exc}"}, status=400)

    grant_label = request.POST.get("grant_label", "").strip() or "uploaded_json"
    batch_size = request.POST.get("batch_size", "").strip() or "50"
    override_existing = str(request.POST.get("override_existing", "")).strip().lower() in {
        "1", "true", "yes", "on",
    }
    try:
        started = services.start_encode_json_upload_import(
            payload=payload,
            grant_label=grant_label,
            batch_size=int(batch_size),
            override_existing=override_existing,
        )
        if not started.get("ok"):
            return JsonResponse(
                {
                    "ok": False,
                    "message": started.get("message", "Another update is already running."),
                    "upload_id": started.get("upload_id"),
                    "resume_from_batch": started.get("resume_from_batch", 0),
                    "resume_from_experiment": started.get("resume_from_experiment", 0),
                },
                status=409,
            )
        return JsonResponse({
            "ok": True,
            "message": "ENCODE JSON uploaded. Batch import started.",
            "upload_id": started.get("upload_id"),
            "resume_from_batch": started.get("resume_from_batch", 0),
            "resume_from_experiment": started.get("resume_from_experiment", 0),
            "total_experiments": started.get("total_experiments", 0),
        })
    except ValueError as exc:
        return JsonResponse({"ok": False, "message": str(exc)}, status=400)
    except Exception as exc:
        return JsonResponse({"ok": False, "message": f"Upload import failed: {exc}"}, status=500)


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
def admin_code_editor(request):
    """Dataset browser + JSON editor page."""
    import os
    from publications.code_examples import list_datasets_with_paths
    from publications.github_sync import get_pat_status

    pat_status = get_pat_status()
    datasets = list_datasets_with_paths()

    return render(request, "publications/code_editor.html", {
        "datasets_json": json.dumps(datasets),
        "dataset_count": len(datasets),
        "pat_configured": pat_status["configured"],
        "pat_valid": pat_status["valid"],
        "github_repo": pat_status["repo"],
        "github_branch": pat_status["branch"],
    })


@login_required
@require_GET
def admin_code_editor_datasets(request):
    """JSON API: list all dataset accessions with paths (for AJAX search)."""
    from publications.code_examples import list_datasets_with_paths
    return JsonResponse({"ok": True, "datasets": list_datasets_with_paths()})


@login_required
@require_GET
def admin_code_editor_dataset_content(request, accession):
    """JSON API: return one dataset's JSON content + path + raw_text."""
    from publications.code_examples import get_dataset_content, get_dataset_rel_path, get_dataset_raw_text
    content = get_dataset_content(accession)
    if content is None:
        return JsonResponse({"ok": False, "error": f"Dataset {accession} not found"}, status=404)
    # Ensure legacy files expose default lock policy in the editor payload.
    try:
        parsed = json.loads(content)
        if isinstance(parsed, dict) and "locked" not in parsed:
            parsed["locked"] = True
            content = json.dumps(parsed, indent=2)
    except Exception:
        pass
    # Get raw_text from the registry (separate from the editable JSON content)
    raw_text = get_dataset_raw_text(accession)
    # Also get raw_text_source
    from publications.code_examples import get_registry
    registry = get_registry()
    entry = registry.get(accession, {})
    raw_text_source = entry.get("raw_text_source", "")
    return JsonResponse({
        "ok": True,
        "accession": accession,
        "content": content,
        "path": get_dataset_rel_path(accession) or "",
        "raw_text": raw_text or "",
        "raw_text_source": raw_text_source,
    })


@login_required
@require_GET
def admin_code_editor_lookup_date(request, accession):
    """JSON API: look up publication date for an accession (for new dataset creation)."""
    from publications.code_examples import lookup_pub_date, month_abbr
    year, month = lookup_pub_date(accession)
    if year is None:
        return JsonResponse({"ok": False, "error": f"No publication found for {accession}"})
    return JsonResponse({
        "ok": True,
        "accession": accession,
        "year": year,
        "month": month,
        "month_abbr": month_abbr(month) if month else "Unknown",
    })


@login_required
@require_POST
@csrf_protect
def admin_code_editor_fetch(request):
    """Fetch one dataset from GitHub."""
    accession = request.POST.get("accession", "").strip()
    rel_path = request.POST.get("path", "").strip()
    if not accession:
        return JsonResponse({"ok": False, "error": "No accession provided"}, status=400)

    try:
        from publications.code_examples import get_dataset_rel_path
        from publications.github_sync import fetch_dataset
        if not rel_path:
            rel_path = get_dataset_rel_path(accession) or ""
        content, sha = fetch_dataset(accession, rel_path=rel_path or None)
        return JsonResponse({
            "ok": True,
            "accession": accession,
            "content": content,
            "sha": sha,
            "path": rel_path,
        })
    except Exception as e:
        return JsonResponse({"ok": False, "error": str(e)}, status=500)


@login_required
@require_POST
@csrf_protect
def admin_code_editor_save(request):
    """Save one dataset file locally."""
    accession = request.POST.get("accession", "").strip()
    content = request.POST.get("content", "")
    year = request.POST.get("year", "").strip()
    month = request.POST.get("month", "").strip()
    if not accession:
        return JsonResponse({"ok": False, "error": "No accession provided"}, status=400)
    if not content:
        return JsonResponse({"ok": False, "error": "No content provided"}, status=400)

    try:
        from publications.code_examples import save_dataset_content
        kwargs = {}
        if year:
            kwargs["year"] = int(year)
        if month:
            kwargs["month"] = month
        path = save_dataset_content(accession, content, **kwargs)
        return JsonResponse({"ok": True, "accession": accession, "path": path})
    except ValueError as e:
        return JsonResponse({"ok": False, "error": str(e)}, status=400)


@login_required
@require_POST
@csrf_protect
def admin_code_editor_push(request):
    """Save one dataset locally and push to GitHub."""
    accession = request.POST.get("accession", "").strip()
    content = request.POST.get("content", "")
    if not accession:
        return JsonResponse({"ok": False, "error": "No accession provided"}, status=400)
    if not content:
        return JsonResponse({"ok": False, "error": "No content provided"}, status=400)

    # Save locally first
    try:
        from publications.code_examples import save_dataset_content
        path = save_dataset_content(accession, content)
    except ValueError as e:
        return JsonResponse({"ok": False, "error": f"Validation failed: {e}"}, status=400)

    # Push to GitHub (using the rel_path from the registry)
    try:
        from publications.github_sync import push_dataset
        from publications.code_examples import get_dataset_rel_path
        rel_path = get_dataset_rel_path(accession)
        result = push_dataset(accession, content, rel_path=rel_path)
        return JsonResponse({
            "ok": True,
            "accession": accession,
            "path": path,
            "commit_sha": result.get("commit_sha", ""),
            "html_url": result.get("html_url", ""),
        })
    except RuntimeError as e:
        return JsonResponse({
            "ok": False,
            "error": f"Saved locally but GitHub push failed: {e}",
            "path": path,
        }, status=500)


@login_required
@require_POST
@csrf_protect
def admin_code_editor_delete(request):
    """Delete one dataset file locally (and optionally from GitHub)."""
    accession = request.POST.get("accession", "").strip()
    if not accession:
        return JsonResponse({"ok": False, "error": "No accession provided"}, status=400)

    from publications.code_examples import delete_dataset
    deleted = delete_dataset(accession)
    if not deleted:
        return JsonResponse({"ok": False, "error": f"Dataset {accession} not found locally"}, status=404)

    # Optionally delete from GitHub too
    delete_remote = request.POST.get("delete_remote") == "1"
    remote_result = None
    if delete_remote:
        try:
            from publications.github_sync import delete_remote_dataset
            from publications.code_examples import get_dataset_rel_path
            rel_path = get_dataset_rel_path(accession)
            remote_result = delete_remote_dataset(accession, rel_path=rel_path)
        except Exception as e:
            return JsonResponse({
                "ok": True,
                "accession": accession,
                "deleted_local": True,
                "deleted_remote": False,
                "remote_error": str(e),
            })

    return JsonResponse({
        "ok": True,
        "accession": accession,
        "deleted_local": True,
        "deleted_remote": bool(remote_result),
    })



@login_required
@require_POST
@csrf_protect
def admin_sync_code_examples(request):
    """Sync code examples from GitHub and optionally backfill."""
    import io
    from django.core.management import call_command

    backfill = request.POST.get("backfill") == "1"
    force = request.POST.get("force") == "1"

    out = io.StringIO()
    err = io.StringIO()

    try:
        kwargs = {"stdout": out, "stderr": err}
        if backfill:
            kwargs["backfill"] = True
        if force:
            kwargs["force"] = True
        call_command("sync_code_examples", **kwargs)

        output = out.getvalue()
        errors = err.getvalue()

        return JsonResponse({
            "ok": True,
            "output": output,
            "errors": errors if errors else None,
        })
    except Exception as e:
        return JsonResponse({
            "ok": False,
            "message": str(e),
            "output": out.getvalue(),
            "errors": err.getvalue(),
        }, status=500)


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


@require_GET
def healthz(request):
    """Liveness/readiness endpoint with DB connectivity check."""
    try:
        with connection.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return JsonResponse({"status": "ok", "db": "ok"})
    except Exception as exc:
        return JsonResponse(
            {"status": "error", "db": "down", "error": str(exc)},
            status=503,
        )


def custom_404(request, exception=None):
    """Global custom 404 page."""
    return render(request, "publications/404.html", status=404)


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


# ============================================================
# Analysis Pipelines
# ============================================================

def _build_code_example_pipelines():
    """
    Build pipeline-like data structures from the code_examples JSON registry.
    Returns a list of dicts that mirror AnalysisPipeline + steps.
    """
    from .code_examples import get_registry, get_dataset_rel_path

    registry = dict(get_registry())  # copy so we don't mutate the original

    if not registry:
        return []

    # Lookup dataset info from DB in one query
    accessions = list(registry.keys())
    accession_info = {}
    try:
        with connection.cursor() as cur:
            placeholders = ",".join(["%s"] * len(accessions))
            cur.execute(f"""
                SELECT da.accession, da.accession_id, da.title, da.accession_type,
                       p.pmid, p.title, p.pub_year, p.pub_month, p.journal_name
                FROM dataset_accessions da
                LEFT JOIN publication_datasets pd ON da.accession_id = pd.accession_id
                LEFT JOIN publications p ON pd.pmid = p.pmid
                WHERE da.accession IN ({placeholders})
                ORDER BY p.pub_year ASC
            """, accessions)
            for row in cur.fetchall():
                acc = row[0]
                if acc not in accession_info:
                    accession_info[acc] = {
                        "accession": acc,
                        "accession_id": row[1],
                        "acc_title": row[2],
                        "accession_type": row[3],
                        "pmid": row[4],
                        "pub_title": row[5],
                        "pub_year": row[6],
                        "pub_month": row[7],
                        "journal_name": row[8],
                    }

            # Lookup dominant library_strategy per accession from SRA
            cur.execute(f"""
                SELECT da.accession, se.library_strategy, COUNT(*) as cnt
                FROM dataset_accessions da
                JOIN sra_experiments se ON da.accession_id = se.parent_accession_id
                WHERE da.accession IN ({placeholders})
                  AND se.library_strategy IS NOT NULL
                GROUP BY da.accession, se.library_strategy
                ORDER BY da.accession, cnt DESC
            """, accessions)
            strategy_map = {}
            for row in cur.fetchall():
                acc = row[0]
                if acc not in strategy_map:  # first row per accession is the dominant one
                    strategy_map[acc] = row[1]

            for acc, info in accession_info.items():
                info["library_strategy"] = strategy_map.get(acc, "")
    except Exception:
        pass

    # Lookup method IDs for tool names
    method_ids = {}
    try:
        with connection.cursor() as cur:
            cur.execute("SELECT method_id, canonical_name FROM computational_methods")
            for row in cur.fetchall():
                method_ids[row[1].lower()] = {"method_id": row[0], "canonical_name": row[1]}
    except Exception:
        pass

    pipelines = []
    for acc, data in sorted(registry.items()):
        steps = data.get("steps", [])
        if not steps:
            continue

        info = accession_info.get(acc, {})
        rel_path = get_dataset_rel_path(acc) or ""

        pipeline = {
            "id": acc,  # Use accession as pipeline ID
            "pipeline_title": f"{acc} Processing Pipeline",
            "accession": acc,
            "accession_id": info.get("accession_id"),
            "acc_title": info.get("acc_title", ""),
            "pmid_id": info.get("pmid", ""),
            "pub_title": info.get("pub_title", ""),
            "pub_year": info.get("pub_year"),
            "journal_name": info.get("journal_name", ""),
            "source": "code_examples",
            "assay_type": info.get("library_strategy") or info.get("accession_type", ""),
            "step_count": len(steps),
            "rel_path": rel_path,
            "steps": [],
            "tools_used": [],
        }

        seen_tools = set()
        for step in steps:
            tool_name = step.get("tool_name", "")
            tool_lower = tool_name.lower() if tool_name else ""
            method_info = method_ids.get(tool_lower, {})

            step_data = {
                "step_order": step.get("step_order", 0),
                "description": step.get("description", tool_name),
                "tool_name": tool_name,
                "tool_version": step.get("tool_version", ""),
                "code_example": step.get("code_example", ""),
                "code_language": step.get("code_language", ""),
                "github_url": step.get("github_url", ""),
                "method_id": method_info.get("method_id"),
                "method_name": method_info.get("canonical_name"),
            }
            pipeline["steps"].append(step_data)

            if method_info and tool_lower not in seen_tools:
                seen_tools.add(tool_lower)
                pipeline["tools_used"].append(method_info)

        pipelines.append(pipeline)

    return pipelines


def _get_cached_code_example_pipelines():
    """Cache code_examples-derived pipeline index used by search views."""
    cached = cache.get(CODE_EXAMPLE_PIPELINES_CACHE_KEY)
    if cached is not None:
        return cached
    pipelines = _build_code_example_pipelines()
    cache.set(CODE_EXAMPLE_PIPELINES_CACHE_KEY, pipelines, CODE_EXAMPLE_PIPELINES_CACHE_TTL)
    return pipelines


def analysis_list(request):
    """Browse analysis pipelines — merges DB pipelines with code_examples registry."""
    page_num = request.GET.get("page", 1)
    q = request.GET.get("q", "").strip()
    assay = request.GET.get("assay", "")
    source = request.GET.get("source", "")

    # Try DB-backed pipelines first
    db_pipelines = list(
        AnalysisPipeline.objects.annotate(
            step_count=Count("pipelinestep")
        ).select_related("pmid", "accession").order_by("-pmid__pub_year", "pipeline_title")
    )

    # Build code_examples-backed pipelines
    ce_pipelines = _build_code_example_pipelines()
    ce_by_accession = {
        p.get("accession"): p for p in ce_pipelines if p.get("accession")
    }

    # Merge with override semantics:
    # If a dataset has code_examples JSON, prefer it over DB pipeline rows.
    all_items = []

    for p in db_pipelines:
        db_acc = p.accession.accession if p.accession else ""
        if db_acc and db_acc in ce_by_accession:
            continue
        all_items.append({
            "id": p.pipeline_id,
            "is_db": True,
            "pipeline_title": p.pipeline_title or "Analysis Pipeline",
            "accession": db_acc,
            "accession_id": p.accession.accession_id if p.accession else None,
            "pmid_id": p.pmid_id,
            "pub_title": p.pmid.title if p.pmid else "",
            "pub_year": p.pmid.pub_year if p.pmid else None,
            "source": p.source,
            "assay_type": p.assay_type or "",
            "step_count": p.step_count,
        })

    for p in ce_pipelines:
        all_items.append({
            "id": p["id"],
            "is_db": False,
            "pipeline_title": p["pipeline_title"],
            "accession": p["accession"],
            "accession_id": p["accession_id"],
            "pmid_id": p["pmid_id"],
            "pub_title": p["pub_title"],
            "pub_year": p["pub_year"],
            "source": p["source"],
            "assay_type": p["assay_type"],
            "step_count": p["step_count"],
        })

    # Apply filters
    if q:
        q_lower = q.lower()
        all_items = [
            item for item in all_items
            if q_lower in (item.get("pipeline_title") or "").lower()
            or q_lower in (item.get("pub_title") or "").lower()
            or q_lower in (item.get("accession") or "").lower()
        ]
    if assay:
        all_items = [item for item in all_items if item.get("assay_type") == assay]
    if source:
        all_items = [item for item in all_items if item.get("source") == source]

    # Sort by year descending
    all_items.sort(key=lambda x: (-(x.get("pub_year") or 0), x.get("pipeline_title") or ""))

    # Compute filter options from unfiltered merged list
    all_unfiltered = []
    for p in db_pipelines:
        db_acc = p.accession.accession if p.accession else ""
        if db_acc and db_acc in ce_by_accession:
            continue
        all_unfiltered.append({"assay_type": p.assay_type or "", "source": p.source})
    for p in ce_pipelines:
        all_unfiltered.append({"assay_type": p["assay_type"], "source": p["source"]})

    assay_counts = defaultdict(int)
    source_counts_dict = defaultdict(int)
    for item in all_unfiltered:
        if item["assay_type"]:
            assay_counts[item["assay_type"]] += 1
        source_counts_dict[item["source"]] += 1

    assay_types = sorted(
        [{"assay_type": k, "count": v} for k, v in assay_counts.items()],
        key=lambda x: -x["count"]
    )
    source_counts = sorted(
        [{"source": k, "count": v} for k, v in source_counts_dict.items()],
        key=lambda x: -x["count"]
    )

    paginator = Paginator(all_items, 20)
    page = paginator.get_page(page_num)

    return render(request, "publications/pipeline_list.html", {
        "page": page, "q": q, "assay": assay, "source": source,
        "assay_types": assay_types, "source_counts": source_counts,
        "total": paginator.count,
    })


def analysis_detail(request, pipeline_id):
    """Detail page for a single analysis pipeline with ordered steps."""
    # Try DB first
    try:
        pipeline = (
            AnalysisPipeline.objects.select_related("pmid", "accession")
            .get(pipeline_id=pipeline_id)
        )
        # Override semantics: if a curated code_examples JSON exists for this
        # accession, always use it as the canonical processing-steps view.
        if pipeline.accession:
            from .code_examples import get_steps_for_dataset
            from django.shortcuts import redirect
            accession = pipeline.accession.accession
            if get_steps_for_dataset(accession) is not None:
                return redirect("publications:analysis_detail_by_accession", accession=accession)

        steps = PipelineStep.objects.filter(
            pipeline=pipeline
        ).select_related("method").order_by("step_order")

        tools_used = steps.exclude(method__isnull=True).values(
            "method__canonical_name", "method__method_id"
        ).distinct()

        return render(request, "publications/pipeline_detail.html", {
            "pipeline": pipeline,
            "steps": steps,
            "tools_used": tools_used,
        })
    except (AnalysisPipeline.DoesNotExist, ValueError):
        pass

    # Fall through: pipeline_id might be an accession from code_examples
    # (handled by the new accession-based URL)
    from django.http import Http404
    raise Http404("Pipeline not found")


def analysis_detail_by_accession(request, accession):
    """Detail page for a code_examples-backed pipeline, keyed by accession."""
    from .code_examples import get_steps_for_dataset, get_dataset_raw_text

    steps = get_steps_for_dataset(accession)
    if steps is None:
        return render(
            request,
            "publications/404.html",
            {
                "missing_accession": accession,
                "missing_context": "analysis",
            },
            status=404,
        )

    # Lookup DB info for this accession
    pub_info = {}
    acc_info = {}
    method_ids = {}
    try:
        with connection.cursor() as cur:
            cur.execute("""
                SELECT da.accession_id, da.title, da.accession_type
                FROM dataset_accessions da WHERE da.accession = %s
            """, [accession])
            row = cur.fetchone()
            accession_id = None
            if row:
                accession_id = row[0]
                acc_info = {"accession_id": row[0], "title": row[1], "type": row[2]}

            # Get library_strategy from SRA experiments
            if accession_id:
                cur.execute("""
                    SELECT library_strategy, COUNT(*) as cnt
                    FROM sra_experiments WHERE parent_accession_id = %s
                    AND library_strategy IS NOT NULL
                    GROUP BY library_strategy ORDER BY cnt DESC LIMIT 1
                """, [accession_id])
                strat_row = cur.fetchone()
                if strat_row:
                    acc_info["library_strategy"] = strat_row[0]

            cur.execute("""
                SELECT p.pmid, p.title, p.pub_year, p.pub_month, p.journal_name
                FROM publications p
                JOIN publication_datasets pd ON p.pmid = pd.pmid
                JOIN dataset_accessions da ON pd.accession_id = da.accession_id
                WHERE da.accession = %s
                ORDER BY p.pub_year ASC LIMIT 1
            """, [accession])
            row = cur.fetchone()
            if row:
                pub_info = {
                    "pmid": row[0], "title": row[1], "pub_year": row[2],
                    "pub_month": row[3], "journal_name": row[4],
                }

            cur.execute("SELECT method_id, canonical_name FROM computational_methods")
            for row in cur.fetchall():
                method_ids[row[1].lower()] = {"method_id": row[0], "canonical_name": row[1]}
    except Exception:
        pass

    # Build step data
    step_list = []
    tools_used = []
    seen_tools = set()
    for step in steps:
        tool_name = step.get("tool_name", "")
        tool_lower = tool_name.lower() if tool_name else ""
        method_info = method_ids.get(tool_lower, {})

        step_list.append({
            "step_order": step.get("step_order", 0),
            "description": step.get("description", tool_name),
            "tool_name": tool_name,
            "tool_version": step.get("tool_version", ""),
            "code_example": step.get("code_example", ""),
            "code_language": step.get("code_language", ""),
            "github_url": step.get("github_url", ""),
            "method": method_info if method_info else None,
            "step_id": f"{accession}-{step.get('step_order', 0)}",
        })

        if method_info and tool_lower not in seen_tools:
            seen_tools.add(tool_lower)
            tools_used.append({
                "method__canonical_name": method_info["canonical_name"],
                "method__method_id": method_info["method_id"],
            })

    # Build pipeline-like context object
    pipeline = {
        "pipeline_title": f"{accession} Processing Pipeline",
        "assay_type": acc_info.get("library_strategy") or acc_info.get("type", ""),
        "source": "code_examples",
        "pmid_id": pub_info.get("pmid", ""),
        "pmid": pub_info if pub_info else None,
        "accession": acc_info if acc_info else None,
        "raw_text": get_dataset_raw_text(accession),
    }

    return render(request, "publications/pipeline_detail_ce.html", {
        "pipeline": pipeline,
        "steps": step_list,
        "tools_used": tools_used,
        "accession": accession,
    })
