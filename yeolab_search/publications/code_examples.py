"""
Dataset-keyed code example registry for analysis pipelines.

Each dataset accession has its own JSON file organized by publication date:
  ``code_examples/2020/Oct/GSE137810.json``
  ``code_examples/2018/Mar/ENCSR519QAA.json``

Each file contains a ``steps`` list with ordered processing steps, tool info,
and code snippets.  The registry loads all files at startup and provides
lookup functions keyed by (accession, step_order) or (accession, tool_name).

Admin editing is done via the JSON editor at ``/admin/code-editor/``.
GitHub sync uses ``publications/github_sync.py``.
"""

import glob
import json
import os
import threading
import time

# ---------------------------------------------------------------------------
# Month abbreviation helper
# ---------------------------------------------------------------------------

_MONTH_ABBR = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}

_MONTH_FROM_ABBR = {v: k for k, v in _MONTH_ABBR.items()}


def month_abbr(month_int: int) -> str:
    """Convert month integer (1–12) to three-letter abbreviation."""
    return _MONTH_ABBR.get(month_int, "Unknown")


# ---------------------------------------------------------------------------
# Directory discovery
# ---------------------------------------------------------------------------

_DIR_PATHS = [
    # 0. Deployment clone target (synced from byee4/yeolab-publications-db)
    "/app/yeolab-publications-db/code_examples",
    # 1. Docker / deployed: alongside the Django project
    os.path.join(os.path.dirname(__file__), "..", "..", "code_examples"),
    # 2. Development: repo root
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "code_examples"),
]

# Env var override
_env_dir = os.environ.get("CODE_EXAMPLES_DIR")
if _env_dir:
    _DIR_PATHS.insert(0, _env_dir)

# In-memory stores
_REGISTRY: dict = {}       # accession → data dict (with "steps" list)
_PATHS: dict = {}           # accession → relative path from base dir (e.g. "2020/Oct")
_REGISTRY_LOCK = threading.Lock()
_LAST_REFRESH_TS = 0.0
_REFRESH_INTERVAL_SEC = float(os.environ.get("CODE_EXAMPLES_REFRESH_INTERVAL_SEC", "10"))


def _find_dir() -> str | None:
    """Find the first existing code_examples directory."""
    for d in _DIR_PATHS:
        resolved = os.path.normpath(d)
        if os.path.isdir(resolved):
            return resolved
    return None


def _load_registry() -> tuple[dict, dict]:
    """
    Scan code_examples/ recursively for *.json files.
    Returns (registry_dict, paths_dict).

    Supports both flat files (code_examples/GSE120023.json) and
    nested files (code_examples/2020/Oct/GSE120023.json).
    """
    registry = {}
    paths = {}
    base_dir = _find_dir()
    if not base_dir:
        return registry, paths

    pattern = os.path.join(base_dir, "**", "*.json")
    for filepath in sorted(glob.glob(pattern, recursive=True)):
        filename = os.path.basename(filepath)
        accession = os.path.splitext(filename)[0]

        # Skip index or meta files
        if accession.startswith("_") or accession.startswith("."):
            continue

        try:
            with open(filepath, "r") as fh:
                data = json.load(fh)
            if isinstance(data, dict) and "steps" in data:
                # Default lock behavior: if missing, treat as locked.
                if "locked" not in data:
                    data["locked"] = True
                registry[accession] = data
                # Compute relative path from base_dir to parent of the file
                parent = os.path.dirname(filepath)
                rel = os.path.relpath(parent, base_dir)
                if rel == ".":
                    rel = ""  # flat file at root level
                paths[accession] = rel
        except (json.JSONDecodeError, OSError):
            continue

    return registry, paths


def reload_registry():
    """Force-reload all dataset files from disk."""
    global _REGISTRY, _PATHS, _LAST_REFRESH_TS
    with _REGISTRY_LOCK:
        _REGISTRY, _PATHS = _load_registry()
        _LAST_REFRESH_TS = time.monotonic()


def _ensure_fresh_registry(force: bool = False):
    """
    Refresh in-memory registry from disk.

    Important for multi-worker deployments: each worker keeps its own process
    memory, so relying on a one-time import cache can make pages show stale
    steps after edits/sync/extract in another worker.
    """
    global _REGISTRY, _PATHS, _LAST_REFRESH_TS
    now = time.monotonic()
    if not force and (now - _LAST_REFRESH_TS) < _REFRESH_INTERVAL_SEC:
        return
    with _REGISTRY_LOCK:
        now2 = time.monotonic()
        if not force and (now2 - _LAST_REFRESH_TS) < _REFRESH_INTERVAL_SEC:
            return
        _REGISTRY, _PATHS = _load_registry()
        _LAST_REFRESH_TS = time.monotonic()


# Load at import time
_REGISTRY, _PATHS = _load_registry()


def get_registry() -> dict:
    """Return the current in-memory registry dict."""
    _ensure_fresh_registry()
    return _REGISTRY


def get_paths_map() -> dict:
    """Return accession -> relative path map for the loaded registry."""
    _ensure_fresh_registry()
    return _PATHS


def get_dir_path() -> str | None:
    """Return the resolved code_examples directory path, or None."""
    return _find_dir()


# ---------------------------------------------------------------------------
# Per-dataset file I/O
# ---------------------------------------------------------------------------

def list_datasets() -> list[str]:
    """Return all dataset accessions in the registry, sorted."""
    _ensure_fresh_registry()
    return sorted((_REGISTRY or {}).keys())


def list_datasets_with_paths() -> list[dict]:
    """
    Return dataset list with path info for the admin UI.
    Each item: {"accession": "GSE120023", "path": "2020/Oct"}
    """
    _ensure_fresh_registry()
    result = []
    for acc in sorted((_REGISTRY or {}).keys()):
        result.append({
            "accession": acc,
            "path": _PATHS.get(acc, ""),
        })
    return result


def get_dataset_rel_path(accession: str) -> str | None:
    """Return the relative path (e.g. '2020/Oct') for an accession, or None."""
    _ensure_fresh_registry()
    return _PATHS.get(accession)


def get_dataset_content(accession: str) -> str | None:
    """Return the raw JSON string for one dataset file, or None."""
    _ensure_fresh_registry()
    base_dir = _find_dir()
    if not base_dir:
        return None

    rel_path = _PATHS.get(accession, "")
    if rel_path:
        filepath = os.path.join(base_dir, rel_path, f"{accession}.json")
    else:
        filepath = os.path.join(base_dir, f"{accession}.json")

    if not os.path.isfile(filepath):
        return None
    with open(filepath, "r") as fh:
        return fh.read()


def save_dataset_content(
    accession: str,
    content: str,
    year: int | None = None,
    month: int | str | None = None,
) -> str:
    """
    Validate and save content for a single dataset file.
    If year/month provided, saves to code_examples/{year}/{Mon}/.
    If not provided, uses existing path or falls back to root.
    Returns the save path. Raises ValueError on invalid input.
    """
    # Validate accession (alphanumeric + dashes/underscores only)
    if not accession or not all(c.isalnum() or c in "-_" for c in accession):
        raise ValueError(f"Invalid accession: '{accession}'")

    # Validate JSON
    try:
        data = json.loads(content)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {e}")

    if not isinstance(data, dict):
        raise ValueError("Expected a JSON object at top level")
    if "steps" not in data:
        raise ValueError("Missing 'steps' key")
    if not isinstance(data["steps"], list):
        raise ValueError("'steps' must be a list")
    # Default lock behavior: locked unless explicitly set false.
    if "locked" not in data:
        data["locked"] = True
    elif not isinstance(data["locked"], bool):
        raise ValueError("'locked' must be a boolean (true/false)")

    # Validate each step
    for i, step in enumerate(data["steps"]):
        if not isinstance(step, dict):
            raise ValueError(f"Step {i} must be an object")
        if "step_order" not in step:
            raise ValueError(f"Step {i} missing 'step_order'")

    # Preserve raw_text from existing file if not in the new data.
    # raw_text is write-once: set during extract analysis and never changed.
    if "raw_text" not in data and accession in _REGISTRY:
        existing = _REGISTRY[accession]
        if existing.get("raw_text"):
            data["raw_text"] = existing["raw_text"]
        if existing.get("raw_text_source"):
            data["raw_text_source"] = existing["raw_text_source"]

    # Determine save directory
    base_dir = _find_dir()
    if not base_dir:
        base_dir = os.path.normpath(_DIR_PATHS[0])
        os.makedirs(base_dir, exist_ok=True)

    # Determine subdirectory
    if year and month:
        if isinstance(month, int):
            month_str = month_abbr(month)
        else:
            month_str = str(month)
        rel_path = os.path.join(str(year), month_str)
    elif accession in _PATHS and _PATHS[accession]:
        rel_path = _PATHS[accession]
    else:
        rel_path = ""

    if rel_path:
        save_dir = os.path.join(base_dir, rel_path)
        os.makedirs(save_dir, exist_ok=True)
    else:
        save_dir = base_dir

    save_path = os.path.join(save_dir, f"{accession}.json")

    with open(save_path, "w") as fh:
        json.dump(data, fh, indent=2)

    # Update in-memory registry
    _REGISTRY[accession] = data
    _PATHS[accession] = rel_path

    return save_path


def update_dataset_raw_text(
    accession: str,
    raw_text: str,
    source: str = "",
) -> bool:
    """
    Save raw source text into an existing dataset JSON file.

    The raw_text is write-once: if the file already contains a ``raw_text``
    field, it will **not** be overwritten — ensuring that the original
    extracted text is preserved even if the file's steps are later edited
    manually or if extract-analysis is re-run.

    If the file does not yet exist, this function does nothing (returns False).
    Call ``save_dataset_content`` or the backfill command first.

    Returns True if the file was updated, False otherwise.
    """
    base_dir = _find_dir()
    if not base_dir:
        return False

    # Find existing file
    rel_path = _PATHS.get(accession, "")
    if rel_path:
        filepath = os.path.join(base_dir, rel_path, f"{accession}.json")
    else:
        filepath = os.path.join(base_dir, f"{accession}.json")

    if not os.path.isfile(filepath):
        return False

    try:
        with open(filepath, "r") as fh:
            data = json.load(fh)
    except (json.JSONDecodeError, OSError):
        return False

    # Write-once: never overwrite an existing raw_text
    if data.get("raw_text"):
        return False

    data["raw_text"] = raw_text
    if source:
        data["raw_text_source"] = source

    with open(filepath, "w") as fh:
        json.dump(data, fh, indent=2)

    # Update in-memory registry
    if accession in _REGISTRY:
        _REGISTRY[accession]["raw_text"] = raw_text
        if source:
            _REGISTRY[accession]["raw_text_source"] = source

    return True


def is_dataset_locked(accession: str) -> bool:
    """
    Return whether a dataset JSON is locked against automatic overwrite.
    Missing files or missing keys default to True (locked).
    """
    _ensure_fresh_registry()
    entry = (_REGISTRY or {}).get(accession)
    if not entry:
        return True
    return bool(entry.get("locked", True))


def get_dataset_raw_text(accession: str) -> str | None:
    """Return the raw_text field from a dataset's JSON file, or None."""
    _ensure_fresh_registry()
    registry = _REGISTRY or {}
    entry = registry.get(accession)
    if not entry:
        return None
    return entry.get("raw_text") or None


def delete_dataset(accession: str) -> bool:
    """Delete a dataset file and remove from registry. Returns True if deleted."""
    base_dir = _find_dir()
    if not base_dir:
        return False

    rel_path = _PATHS.get(accession, "")
    if rel_path:
        filepath = os.path.join(base_dir, rel_path, f"{accession}.json")
    else:
        filepath = os.path.join(base_dir, f"{accession}.json")

    if not os.path.isfile(filepath):
        return False

    os.remove(filepath)
    _REGISTRY.pop(accession, None)
    _PATHS.pop(accession, None)

    # Clean up empty parent directories
    if rel_path:
        parent = os.path.dirname(filepath)
        try:
            while parent != base_dir:
                if not os.listdir(parent):
                    os.rmdir(parent)
                    parent = os.path.dirname(parent)
                else:
                    break
        except OSError:
            pass

    return True


# ---------------------------------------------------------------------------
# Database lookup for publication dates
# ---------------------------------------------------------------------------

def lookup_pub_date(accession: str) -> tuple[int | None, int | None]:
    """
    Query the DB for the earliest publication year/month linked to this accession.
    Returns (year, month) or (None, None) if not found.
    """
    try:
        from django.db import connection
        with connection.cursor() as cur:
            cur.execute("""
                SELECT p.pub_year, p.pub_month
                FROM publications p
                JOIN publication_datasets pd ON p.pmid = pd.pmid
                JOIN dataset_accessions da ON pd.accession_id = da.accession_id
                WHERE da.accession = %s AND p.pub_year IS NOT NULL
                ORDER BY p.pub_year ASC, p.pub_month ASC
                LIMIT 1
            """, [accession])
            row = cur.fetchone()
            if row:
                return row[0], row[1]
    except Exception:
        pass
    return None, None


# ---------------------------------------------------------------------------
# Lookup functions (unchanged API from previous version)
# ---------------------------------------------------------------------------

def get_steps_for_dataset(accession: str) -> list[dict] | None:
    """
    Return the list of steps for a dataset accession, or None if not found.
    """
    _ensure_fresh_registry()
    registry = _REGISTRY or {}
    entry = registry.get(accession)
    if not entry:
        return None
    return entry.get("steps", [])


def get_code_example(
    accession: str,
    step_order: int,
) -> tuple[str | None, str | None, str | None]:
    """
    Look up code example for a specific dataset + step.
    Returns (code_example, code_language, github_url) or (None, None, None).
    """
    steps = get_steps_for_dataset(accession)
    if not steps:
        return None, None, None

    for step in steps:
        if step.get("step_order") == step_order:
            return (
                step.get("code_example") or None,
                step.get("code_language") or None,
                step.get("github_url") or None,
            )

    return None, None, None


def get_code_example_by_tool(
    accession: str,
    tool_name: str,
) -> tuple[str | None, str | None, str | None]:
    """
    Look up code example by dataset + tool name (case-insensitive match).
    Returns (code_example, code_language, github_url) or (None, None, None).
    """
    steps = get_steps_for_dataset(accession)
    if not steps:
        return None, None, None

    tool_lower = tool_name.lower()
    for step in steps:
        if (step.get("tool_name") or "").lower() == tool_lower:
            return (
                step.get("code_example") or None,
                step.get("code_language") or None,
                step.get("github_url") or None,
            )

    return None, None, None


def get_github_url(accession: str, tool_name: str) -> str | None:
    """Return the GitHub URL for a tool within a dataset, or None."""
    _, _, url = get_code_example_by_tool(accession, tool_name)
    return url


# ---------------------------------------------------------------------------
# Pipeline generation from DB metadata
# ---------------------------------------------------------------------------
#
# NOTE (2026-03-03):
# These template defaults are intentionally retained for manual/admin/offline
# tooling, but they are no longer used by user-facing dataset/analysis views.
# Runtime views now render only curated code_examples JSON steps to avoid
# fallback-generated output that can appear to clobber curated pipelines.

# Standard pipeline templates keyed by SRA library_strategy
_PIPELINE_TEMPLATES = {
    "RNA-Seq": [
        {"description": "Quality control of raw reads", "tool_name": "FastQC", "code_language": "bash"},
        {"description": "Adapter trimming and quality filtering", "tool_name": "cutadapt", "code_language": "bash"},
        {"description": "Genome alignment", "tool_name": "STAR", "code_language": "bash"},
        {"description": "Alignment sorting and indexing", "tool_name": "samtools", "code_language": "bash"},
        {"description": "Gene expression quantification", "tool_name": "featureCounts", "code_language": "bash"},
        {"description": "Differential expression analysis", "tool_name": "DESeq2", "code_language": "r"},
    ],
    "RIP-Seq": [
        {"description": "Quality control of raw reads", "tool_name": "FastQC", "code_language": "bash"},
        {"description": "Adapter trimming and quality filtering", "tool_name": "cutadapt", "code_language": "bash"},
        {"description": "Genome alignment", "tool_name": "STAR", "code_language": "bash"},
        {"description": "PCR duplicate removal and sorting", "tool_name": "samtools", "code_language": "bash"},
        {"description": "Peak calling for RBP binding sites", "tool_name": "CLIPper", "code_language": "bash"},
        {"description": "Motif analysis of binding regions", "tool_name": "HOMER", "code_language": "bash"},
    ],
    "CLIP-Seq": [
        {"description": "Quality control of raw reads", "tool_name": "FastQC", "code_language": "bash"},
        {"description": "Adapter trimming", "tool_name": "cutadapt", "code_language": "bash"},
        {"description": "Repetitive element filtering", "tool_name": "Bowtie2", "code_language": "bash"},
        {"description": "Genome alignment", "tool_name": "STAR", "code_language": "bash"},
        {"description": "PCR duplicate removal", "tool_name": "samtools", "code_language": "bash"},
        {"description": "Peak calling for CLIP binding sites", "tool_name": "CLIPper", "code_language": "bash"},
        {"description": "Irreproducible discovery rate analysis", "tool_name": "IDR", "code_language": "bash"},
    ],
    "OTHER": [  # Often eCLIP/CLIP variants in the Yeo Lab
        {"description": "Quality control of raw reads", "tool_name": "FastQC", "code_language": "bash"},
        {"description": "Adapter trimming", "tool_name": "cutadapt", "code_language": "bash"},
        {"description": "Repetitive element filtering", "tool_name": "Bowtie2", "code_language": "bash"},
        {"description": "Genome alignment", "tool_name": "STAR", "code_language": "bash"},
        {"description": "PCR duplicate removal", "tool_name": "samtools", "code_language": "bash"},
        {"description": "Peak calling", "tool_name": "CLIPper", "code_language": "bash"},
    ],
    "ChIP-Seq": [
        {"description": "Quality control of raw reads", "tool_name": "FastQC", "code_language": "bash"},
        {"description": "Adapter trimming", "tool_name": "cutadapt", "code_language": "bash"},
        {"description": "Genome alignment", "tool_name": "Bowtie2", "code_language": "bash"},
        {"description": "PCR duplicate removal", "tool_name": "samtools", "code_language": "bash"},
        {"description": "Peak calling", "tool_name": "MACS2", "code_language": "bash"},
    ],
    "ATAC-seq": [
        {"description": "Quality control of raw reads", "tool_name": "FastQC", "code_language": "bash"},
        {"description": "Adapter trimming", "tool_name": "cutadapt", "code_language": "bash"},
        {"description": "Genome alignment", "tool_name": "Bowtie2", "code_language": "bash"},
        {"description": "PCR duplicate removal and filtering", "tool_name": "samtools", "code_language": "bash"},
        {"description": "Peak calling on accessible regions", "tool_name": "MACS2", "code_language": "bash"},
    ],
    "miRNA-Seq": [
        {"description": "Quality control of raw reads", "tool_name": "FastQC", "code_language": "bash"},
        {"description": "Adapter trimming", "tool_name": "cutadapt", "code_language": "bash"},
        {"description": "Small RNA alignment and quantification", "tool_name": "Bowtie2", "code_language": "bash"},
        {"description": "miRNA expression quantification", "tool_name": "featureCounts", "code_language": "bash"},
    ],
    "ncRNA-Seq": [
        {"description": "Quality control of raw reads", "tool_name": "FastQC", "code_language": "bash"},
        {"description": "Adapter trimming", "tool_name": "cutadapt", "code_language": "bash"},
        {"description": "Genome alignment", "tool_name": "STAR", "code_language": "bash"},
        {"description": "Non-coding RNA quantification", "tool_name": "featureCounts", "code_language": "bash"},
    ],
    "Bisulfite-Seq": [
        {"description": "Quality control of raw reads", "tool_name": "FastQC", "code_language": "bash"},
        {"description": "Adapter trimming", "tool_name": "cutadapt", "code_language": "bash"},
        {"description": "Bisulfite alignment", "tool_name": "Bismark", "code_language": "bash"},
        {"description": "Methylation extraction and calling", "tool_name": "Bismark", "code_language": "bash"},
    ],
    "AMPLICON": [
        {"description": "Quality control of raw reads", "tool_name": "FastQC", "code_language": "bash"},
        {"description": "Adapter trimming", "tool_name": "cutadapt", "code_language": "bash"},
        {"description": "Genome alignment", "tool_name": "BWA", "code_language": "bash"},
        {"description": "Variant calling", "tool_name": "samtools", "code_language": "bash"},
    ],
}

# Default template when library_strategy is unknown
_DEFAULT_TEMPLATE = [
    {"description": "Quality control of raw reads", "tool_name": "FastQC", "code_language": "bash"},
    {"description": "Adapter trimming and quality filtering", "tool_name": "cutadapt", "code_language": "bash"},
    {"description": "Genome alignment", "tool_name": "STAR", "code_language": "bash"},
    {"description": "Post-alignment processing", "tool_name": "samtools", "code_language": "bash"},
]

# Skip these method categories when building tool-hint steps
_SKIP_CATEGORIES = {"Assay", "Sequencing Platform", "Language"}

# Category ordering for additional method-based steps
_CATEGORY_ORDER = {
    "QC": 10, "Read Processing": 20, "Alignment": 30, "Quantification": 40,
    "Peak Calling": 50, "Peak Calling / Motif": 55, "CLIP Analysis": 60,
    "Differential Expression": 70, "Splicing Analysis": 75, "Single-Cell": 80,
    "RBP Binding": 85, "RNA Editing": 86, "Motif Analysis": 87,
    "Variant Calling": 88, "Long-Read Analysis": 89, "Machine Learning": 90,
    "Pathway Analysis": 91, "Structure Prediction": 92, "Visualization": 93,
    "Statistics": 94, "Genomic Utilities": 95, "Workflow": 96,
    "Reference": 97, "Sequence Search": 98,
}


def generate_pipeline_from_metadata(accession: str) -> dict | None:
    """
    Deprecated for user-facing runtime rendering.

    Generate a pipeline JSON structure from DB metadata for a given accession.

    Uses SRA library_strategy to select a standard pipeline template, then
    enriches with any linked computational_methods from the publication.

    Returns a dict with "steps" list, or None if the accession is not in the DB.

    This function is kept for non-runtime workflows (manual/admin/backfill),
    but dataset/analysis web views should not call it.
    """
    try:
        from django.db import connection
    except ImportError:
        return None

    try:
        with connection.cursor() as cur:
            # 1. Get accession info
            cur.execute("""
                SELECT da.accession_id, da.title, da.organism, da.summary, da.overall_design
                FROM dataset_accessions da WHERE da.accession = %s
            """, [accession])
            acc_row = cur.fetchone()
            if not acc_row:
                return None

            accession_id = acc_row[0]
            organism = acc_row[2] or ""

            # 2. Get dominant library_strategy from SRA experiments
            cur.execute("""
                SELECT library_strategy, COUNT(*) as cnt
                FROM sra_experiments
                WHERE parent_accession_id = %s AND library_strategy IS NOT NULL
                GROUP BY library_strategy
                ORDER BY cnt DESC
                LIMIT 1
            """, [accession_id])
            strategy_row = cur.fetchone()
            library_strategy = strategy_row[0] if strategy_row else None

            # 3. Get linked PMIDs
            cur.execute("""
                SELECT DISTINCT pd.pmid FROM publication_datasets pd
                WHERE pd.accession_id = %s
            """, [accession_id])
            pmids = [row[0] for row in cur.fetchall()]

            # 4. Get computational methods linked to these publications
            linked_methods = []
            if pmids:
                placeholders = ",".join(["%s"] * len(pmids))
                cur.execute(f"""
                    SELECT DISTINCT cm.canonical_name, cm.category, cm.url,
                           cm.description, pm.version
                    FROM publication_methods pm
                    JOIN computational_methods cm ON pm.method_id = cm.method_id
                    WHERE pm.pmid IN ({placeholders})
                      AND cm.category NOT IN ('Assay', 'Sequencing Platform', 'Language')
                    ORDER BY cm.category, cm.canonical_name
                """, pmids)
                linked_methods = cur.fetchall()

    except Exception:
        return None

    # 5. Select base template from library_strategy
    template = _PIPELINE_TEMPLATES.get(library_strategy, _DEFAULT_TEMPLATE)

    # Build initial steps from template
    steps = []
    template_tools = set()
    for i, tmpl_step in enumerate(template):
        step = {
            "step_order": i + 1,
            "description": tmpl_step["description"],
            "tool_name": tmpl_step["tool_name"],
            "tool_version": "",
            "code_example": "",
            "code_language": tmpl_step.get("code_language", "bash"),
            "github_url": "",
        }
        steps.append(step)
        template_tools.add(tmpl_step["tool_name"].lower())

    # 6. Enrich with linked methods not already in template
    extra_steps = []
    for name, category, url, desc, version in linked_methods:
        if name.lower() in template_tools:
            # Update existing template step with url/version
            for step in steps:
                if step["tool_name"].lower() == name.lower():
                    if url and not step["github_url"]:
                        step["github_url"] = url
                    if version and not step["tool_version"]:
                        step["tool_version"] = version
                    break
            continue

        sort_key = _CATEGORY_ORDER.get(category, 99)
        extra_steps.append((sort_key, {
            "step_order": 0,
            "description": desc or f"{name} ({category})",
            "tool_name": name,
            "tool_version": version or "",
            "code_example": "",
            "code_language": "bash",
            "github_url": url or "",
        }))

    # Sort extra steps by category and append
    extra_steps.sort(key=lambda x: (x[0], x[1]["tool_name"]))
    for _, step in extra_steps:
        step["step_order"] = len(steps) + 1
        steps.append(step)
        template_tools.add(step["tool_name"].lower())

    # 7. Add organism-specific genome reference hint
    genome = ""
    if "homo sapiens" in organism.lower():
        genome = "hg38"
    elif "mus musculus" in organism.lower():
        genome = "mm10"
    elif "drosophila" in organism.lower():
        genome = "dm6"
    elif "c. elegans" in organism.lower() or "caenorhabditis" in organism.lower():
        genome = "ce11"

    if genome:
        for step in steps:
            if "alignment" in step["description"].lower() or "genome" in step["description"].lower():
                step["description"] = f"{step['description']} ({genome})"
                break

    return {
        "library_strategy": library_strategy,
        "organism": organism,
        "steps": steps,
    }
