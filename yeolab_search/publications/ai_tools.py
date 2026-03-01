"""
Pre-defined tool functions for the Claude AI chat feature.
Each tool queries the Yeo Lab publications database and returns
JSON-serializable results. No raw SQL — uses Django ORM only.
"""
import json
from django.db.models import Q, Count, Sum
from django.db import connection

from .models import (
    Publication, Author, PublicationAuthor, DatasetAccession,
    PublicationDataset, DatasetFile, Grant, PublicationGrant,
    SraExperiment, SraRun,
)


# ============================================================
# Tool definitions (Anthropic API format)
# ============================================================

TOOL_DEFINITIONS = [
    {
        "name": "search_publications",
        "description": (
            "Search Yeo Lab publications by keyword. Searches titles, abstracts, "
            "and journal names. Returns up to 10 matching publications with basic "
            "metadata. Use this when the user asks about papers on a topic, method, "
            "or gene."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search keywords (e.g. 'eCLIP', 'TDP-43', 'splicing', 'RNA-seq')",
                },
                "year": {
                    "type": "integer",
                    "description": "Optional: filter by publication year",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results to return (default 10, max 25)",
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_publication",
        "description": (
            "Get full details for a single publication by its PubMed ID (PMID). "
            "Returns title, abstract, all authors, grants, linked datasets, "
            "keywords, MeSH terms, and external links. Use this after finding a "
            "PMID via search to get comprehensive information."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "pmid": {
                    "type": "string",
                    "description": "The PubMed ID (e.g. '32728249')",
                },
            },
            "required": ["pmid"],
        },
    },
    {
        "name": "search_authors",
        "description": (
            "Search for authors by name. Returns matching authors with their "
            "publication counts. Use this when the user asks about a specific "
            "researcher or collaborator."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Author name to search (e.g. 'Van Nostrand', 'Yeo')",
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_author",
        "description": (
            "Get full details for an author by their database ID. Returns all "
            "their publications, co-authors, first/last author counts, and ORCID. "
            "Use after search_authors to get detailed info."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "author_id": {
                    "type": "integer",
                    "description": "Author ID from the database (from search_authors results)",
                },
            },
            "required": ["author_id"],
        },
    },
    {
        "name": "search_datasets",
        "description": (
            "Search GEO/SRA datasets by accession, title, or organism. Returns "
            "matching datasets with basic metadata. Use when the user asks about "
            "available data, specific accessions, or data for an organism."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search term (e.g. 'GSE120023', 'eCLIP', 'human', 'ENCODE')",
                },
                "accession_type": {
                    "type": "string",
                    "description": "Optional: filter by type (GSE, SRX, ENCSR, etc.)",
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_dataset",
        "description": (
            "Get full details for a dataset by its accession string (e.g. 'GSE120023'). "
            "Returns metadata, linked publications, SRA experiment summary, and "
            "file counts. Use after search_datasets for comprehensive info."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "accession": {
                    "type": "string",
                    "description": "Dataset accession string (e.g. 'GSE120023')",
                },
            },
            "required": ["accession"],
        },
    },
    {
        "name": "get_database_stats",
        "description": (
            "Get aggregate statistics about the entire Yeo Lab publications database. "
            "Returns total counts, publications by year, top journals, top organisms, "
            "and common library strategies. Use when the user asks about the lab's "
            "overall output, trends, or scope."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "search_grants",
        "description": (
            "Search grants by grant number or funding agency. Returns matching "
            "grants with the number of linked publications. Use when the user "
            "asks about funding sources or specific grants."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Grant number or agency name (e.g. 'R01', 'NIH', 'HG004659')",
                },
            },
            "required": ["query"],
        },
    },
]


# ============================================================
# Tool execution functions
# ============================================================

def _fts_search(query, limit=500):
    """Use FTS5 for fast full-text search. Returns list of PMIDs or None."""
    safe_q = query.replace('"', '""')
    fts_query = f'"{safe_q}"'
    with connection.cursor() as cur:
        try:
            cur.execute(
                "SELECT pmid FROM publications_fts WHERE publications_fts MATCH ? LIMIT ?",
                [fts_query, limit],
            )
            return [row[0] for row in cur.fetchall()]
        except Exception:
            return None


def search_publications(query, year=None, limit=10):
    """Search publications by keyword with optional year filter."""
    limit = min(max(1, limit), 25)
    results = Publication.objects.all()

    pmids = _fts_search(query, limit=200)
    if pmids is not None:
        results = results.filter(pmid__in=pmids)
    else:
        results = results.filter(
            Q(title__icontains=query)
            | Q(abstract__icontains=query)
            | Q(journal_name__icontains=query)
        )

    if year:
        results = results.filter(pub_year=year)

    results = results.order_by("-pub_year", "-pub_date")[:limit]

    output = []
    for pub in results:
        authors = (
            PublicationAuthor.objects.filter(pmid=pub.pmid)
            .select_related("author")
            .order_by("author_position")[:5]
        )
        author_names = [pa.author.display_name for pa in authors]
        output.append({
            "pmid": pub.pmid,
            "title": pub.title,
            "year": pub.pub_year,
            "journal": pub.journal_name,
            "authors": author_names,
            "doi": pub.doi,
            "is_open_access": bool(pub.is_open_access),
        })
    return {"count": len(output), "results": output}


def get_publication(pmid):
    """Get full details for a publication by PMID."""
    try:
        pub = Publication.objects.get(pmid=pmid)
    except Publication.DoesNotExist:
        return {"error": f"Publication with PMID {pmid} not found"}

    # Authors
    pub_authors = (
        PublicationAuthor.objects.filter(pmid=pmid)
        .select_related("author")
        .order_by("author_position")
    )
    authors = []
    for pa in pub_authors:
        authors.append({
            "name": pa.author.display_name,
            "position": pa.author_position,
            "is_first": bool(pa.is_first_author),
            "is_last": bool(pa.is_last_author),
            "orcid": pa.author.orcid,
            "author_id": pa.author.author_id,
        })

    # Grants
    pub_grants = (
        PublicationGrant.objects.filter(pmid=pmid)
        .select_related("grant")
    )
    grants = [
        {
            "grant_number": pg.grant.grant_number,
            "agency": pg.grant.agency,
            "country": pg.grant.country,
        }
        for pg in pub_grants
    ]

    # Datasets
    pub_datasets = (
        PublicationDataset.objects.filter(pmid=pmid)
        .select_related("accession")
    )
    datasets = []
    for pd_obj in pub_datasets:
        ds = pd_obj.accession
        datasets.append({
            "accession": ds.accession,
            "type": ds.accession_type,
            "title": ds.title,
            "organism": ds.organism,
            "source": pd_obj.source,
        })

    return {
        "pmid": pub.pmid,
        "title": pub.title,
        "abstract": pub.abstract,
        "journal": pub.journal_name,
        "year": pub.pub_year,
        "volume": pub.volume,
        "issue": pub.issue,
        "pages": pub.pages,
        "doi": pub.doi,
        "pmc_id": pub.pmc_id,
        "pubmed_url": pub.pubmed_url,
        "is_open_access": bool(pub.is_open_access),
        "keywords": pub.keywords_list,
        "mesh_terms": pub.mesh_terms_list,
        "pub_types": pub.pub_types_list,
        "authors": authors,
        "grants": grants,
        "datasets": datasets,
    }


def search_authors(query):
    """Search authors by name."""
    authors = (
        Author.objects.filter(
            Q(last_name__icontains=query) | Q(fore_name__icontains=query)
        )
        .annotate(pub_count=Count("publicationauthor"))
        .order_by("-pub_count")[:20]
    )
    return {
        "count": len(authors),
        "results": [
            {
                "author_id": a.author_id,
                "name": a.display_name,
                "orcid": a.orcid,
                "publication_count": a.pub_count,
            }
            for a in authors
        ],
    }


def get_author(author_id):
    """Get full details for an author."""
    try:
        author = Author.objects.get(author_id=author_id)
    except Author.DoesNotExist:
        return {"error": f"Author with ID {author_id} not found"}

    # Publications
    pub_links = (
        PublicationAuthor.objects.filter(author=author)
        .select_related("pmid")
        .order_by("-pmid__pub_year")
    )
    publications = []
    first_count = 0
    last_count = 0
    for pa in pub_links:
        publications.append({
            "pmid": pa.pmid_id,
            "title": pa.pmid.title,
            "year": pa.pmid.pub_year,
            "journal": pa.pmid.journal_name,
            "is_first": bool(pa.is_first_author),
            "is_last": bool(pa.is_last_author),
        })
        if pa.is_first_author:
            first_count += 1
        if pa.is_last_author:
            last_count += 1

    # Top co-authors
    coauthor_ids = (
        PublicationAuthor.objects.filter(
            pmid__in=[p["pmid"] for p in publications]
        )
        .exclude(author=author)
        .values("author__author_id", "author__fore_name", "author__last_name")
        .annotate(shared=Count("pmid", distinct=True))
        .order_by("-shared")[:10]
    )
    coauthors = [
        {
            "author_id": ca["author__author_id"],
            "name": f'{ca["author__fore_name"] or ""} {ca["author__last_name"] or ""}'.strip(),
            "shared_publications": ca["shared"],
        }
        for ca in coauthor_ids
    ]

    return {
        "author_id": author.author_id,
        "name": author.display_name,
        "orcid": author.orcid,
        "total_publications": len(publications),
        "first_author_count": first_count,
        "last_author_count": last_count,
        "publications": publications,
        "top_coauthors": coauthors,
    }


def search_datasets(query, accession_type=None):
    """Search datasets by accession, title, or organism."""
    results = DatasetAccession.objects.filter(
        Q(accession__icontains=query)
        | Q(title__icontains=query)
        | Q(organism__icontains=query)
        | Q(summary__icontains=query)
    )
    if accession_type:
        results = results.filter(accession_type=accession_type)

    results = results.order_by("-accession_id")[:15]

    output = []
    for ds in results:
        pub_count = PublicationDataset.objects.filter(accession=ds).count()
        output.append({
            "accession_id": ds.accession_id,
            "accession": ds.accession,
            "type": ds.accession_type,
            "database": ds.database,
            "title": ds.title,
            "organism": ds.organism,
            "num_samples": ds.num_samples,
            "linked_publications": pub_count,
        })
    return {"count": len(output), "results": output}


def get_dataset(accession):
    """Get full details for a dataset by accession string."""
    try:
        ds = DatasetAccession.objects.get(accession=accession)
    except DatasetAccession.DoesNotExist:
        return {"error": f"Dataset with accession '{accession}' not found"}

    # Linked publications
    pub_links = (
        PublicationDataset.objects.filter(accession=ds)
        .select_related("pmid")
    )
    publications = [
        {
            "pmid": pl.pmid_id,
            "title": pl.pmid.title,
            "year": pl.pmid.pub_year,
            "journal": pl.pmid.journal_name,
            "source": pl.source,
        }
        for pl in pub_links
    ]

    # SRA experiments summary
    sra_exps = SraExperiment.objects.filter(parent_accession=ds)
    sra_summary = (
        sra_exps.values("library_strategy")
        .annotate(count=Count("experiment_id"))
        .order_by("-count")
    )
    total_runs = SraRun.objects.filter(experiment__parent_accession=ds).count()
    total_size = (
        SraRun.objects.filter(experiment__parent_accession=ds)
        .aggregate(total=Sum("size_mb"))["total"]
    ) or 0

    # Files
    file_count = DatasetFile.objects.filter(accession=ds).count()

    # Parse experiment types
    exp_types = []
    if ds.experiment_types:
        try:
            exp_types = json.loads(ds.experiment_types)
        except (json.JSONDecodeError, TypeError):
            exp_types = [t.strip() for t in ds.experiment_types.split(";") if t.strip()]

    return {
        "accession": ds.accession,
        "accession_id": ds.accession_id,
        "type": ds.accession_type,
        "database": ds.database,
        "title": ds.title,
        "organism": ds.organism,
        "platform": ds.platform,
        "summary": ds.summary,
        "overall_design": ds.overall_design,
        "num_samples": ds.num_samples,
        "experiment_types": exp_types,
        "submission_date": ds.submission_date,
        "status": ds.status,
        "contact_name": ds.contact_name,
        "contact_institute": ds.contact_institute,
        "publications": publications,
        "sra_library_strategies": [
            {"strategy": s["library_strategy"], "count": s["count"]}
            for s in sra_summary
        ],
        "total_sra_experiments": sra_exps.count(),
        "total_sra_runs": total_runs,
        "total_size_mb": round(total_size, 1),
        "file_count": file_count,
    }


def get_database_stats():
    """Get aggregate statistics for the entire database."""
    # Counts
    counts = {
        "publications": Publication.objects.count(),
        "authors": Author.objects.count(),
        "datasets": DatasetAccession.objects.count(),
        "files": DatasetFile.objects.count(),
        "grants": Grant.objects.count(),
        "sra_experiments": SraExperiment.objects.count(),
        "sra_runs": SraRun.objects.count(),
    }

    # Publications by year
    by_year = list(
        Publication.objects.values("pub_year")
        .annotate(count=Count("pmid"))
        .order_by("pub_year")
    )
    by_year = [
        {"year": y["pub_year"], "count": y["count"]}
        for y in by_year if y["pub_year"]
    ]

    # Top journals
    top_journals = list(
        Publication.objects.values("journal_name")
        .annotate(count=Count("pmid"))
        .order_by("-count")[:15]
    )

    # Top organisms (from datasets)
    top_organisms = list(
        DatasetAccession.objects.exclude(organism__isnull=True)
        .exclude(organism="")
        .values("organism")
        .annotate(count=Count("accession_id"))
        .order_by("-count")[:10]
    )

    # Library strategies
    library_strategies = list(
        SraExperiment.objects.exclude(library_strategy__isnull=True)
        .values("library_strategy")
        .annotate(count=Count("experiment_id"))
        .order_by("-count")[:10]
    )

    # Year range
    years = Publication.objects.exclude(pub_year__isnull=True)
    min_year = years.order_by("pub_year").values_list("pub_year", flat=True).first()
    max_year = years.order_by("-pub_year").values_list("pub_year", flat=True).first()

    return {
        "counts": counts,
        "year_range": {"min": min_year, "max": max_year},
        "publications_by_year": by_year,
        "top_journals": [
            {"journal": j["journal_name"], "count": j["count"]}
            for j in top_journals
        ],
        "top_organisms": [
            {"organism": o["organism"], "count": o["count"]}
            for o in top_organisms
        ],
        "library_strategies": [
            {"strategy": ls["library_strategy"], "count": ls["count"]}
            for ls in library_strategies
        ],
    }


def search_grants(query):
    """Search grants by number or agency."""
    grants = (
        Grant.objects.filter(
            Q(grant_number__icontains=query) | Q(agency__icontains=query)
        )
        .annotate(pub_count=Count("publicationgrant"))
        .order_by("-pub_count")[:20]
    )
    return {
        "count": len(grants),
        "results": [
            {
                "grant_id": g.grant_id,
                "grant_number": g.grant_number,
                "agency": g.agency,
                "country": g.country,
                "linked_publications": g.pub_count,
            }
            for g in grants
        ],
    }


# ============================================================
# Tool dispatcher
# ============================================================

_TOOL_FUNCTIONS = {
    "search_publications": search_publications,
    "get_publication": get_publication,
    "search_authors": search_authors,
    "get_author": get_author,
    "search_datasets": search_datasets,
    "get_dataset": get_dataset,
    "get_database_stats": get_database_stats,
    "search_grants": search_grants,
}


def execute_tool(name, tool_input):
    """Execute a named tool with the given input dict. Returns result dict."""
    func = _TOOL_FUNCTIONS.get(name)
    if func is None:
        return {"error": f"Unknown tool: {name}"}
    try:
        return func(**tool_input)
    except Exception as e:
        return {"error": f"Tool '{name}' failed: {str(e)}"}
