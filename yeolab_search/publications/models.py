"""
Unmanaged Django models mapping to the existing yeolab_publications.db schema.
All models use `managed = False` so Django never tries to create/alter these tables.
"""
from django.db import models
from django.conf import settings


def _junction_pk_column():
    """
    Junction tables use SQLite implicit `rowid` in SQLite DBs, but explicit `id`
    in PostgreSQL schema. Choose the correct PK column for the active backend.
    """
    engine = settings.DATABASES.get("default", {}).get("ENGINE", "")
    return "id" if "postgresql" in engine else "rowid"


class Publication(models.Model):
    pmid = models.TextField(primary_key=True)
    pmc_id = models.TextField(blank=True, null=True)
    doi = models.TextField(blank=True, null=True)
    pii = models.TextField(blank=True, null=True)
    title = models.TextField()
    abstract = models.TextField(blank=True, null=True)
    journal_name = models.TextField(blank=True, null=True)
    journal_iso = models.TextField(blank=True, null=True)
    pub_date = models.TextField(blank=True, null=True)
    pub_year = models.IntegerField(blank=True, null=True)
    pub_month = models.IntegerField(blank=True, null=True)
    pub_day = models.IntegerField(blank=True, null=True)
    volume = models.TextField(blank=True, null=True)
    issue = models.TextField(blank=True, null=True)
    pages = models.TextField(blank=True, null=True)
    pub_types = models.TextField(blank=True, null=True)
    mesh_terms = models.TextField(blank=True, null=True)
    keywords = models.TextField(blank=True, null=True)
    language = models.TextField(blank=True, null=True)
    is_open_access = models.IntegerField(default=0)
    citation_count = models.IntegerField(blank=True, null=True)
    abstract_word_count = models.IntegerField(blank=True, null=True)
    created_at = models.TextField(blank=True, null=True)
    updated_at = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = "publications"

    def __str__(self):
        return f"{self.pmid}: {self.title[:80]}"

    @property
    def pubmed_url(self):
        return f"https://pubmed.ncbi.nlm.nih.gov/{self.pmid}/"

    @property
    def doi_url(self):
        return f"https://doi.org/{self.doi}" if self.doi else None

    @property
    def pub_types_list(self):
        if not self.pub_types:
            return []
        return [t.strip() for t in self.pub_types.split(";") if t.strip()]

    @property
    def keywords_list(self):
        if not self.keywords:
            return []
        return [k.strip() for k in self.keywords.split(";") if k.strip()]

    @property
    def mesh_terms_list(self):
        if not self.mesh_terms:
            return []
        return [m.strip() for m in self.mesh_terms.split(";") if m.strip()]


class Author(models.Model):
    author_id = models.AutoField(primary_key=True)
    last_name = models.TextField(blank=True, null=True)
    fore_name = models.TextField(blank=True, null=True)
    initials = models.TextField(blank=True, null=True)
    orcid = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = "authors"

    def __str__(self):
        return f"{self.fore_name or ''} {self.last_name or ''}".strip()

    @property
    def display_name(self):
        parts = []
        if self.fore_name:
            parts.append(self.fore_name)
        if self.last_name:
            parts.append(self.last_name)
        return " ".join(parts) if parts else f"Author #{self.author_id}"


class PublicationAuthor(models.Model):
    id = models.IntegerField(primary_key=True, db_column=_junction_pk_column())
    pmid = models.ForeignKey(
        Publication, on_delete=models.DO_NOTHING, db_column="pmid"
    )
    author = models.ForeignKey(
        Author, on_delete=models.DO_NOTHING, db_column="author_id"
    )
    author_position = models.IntegerField()
    is_first_author = models.IntegerField(default=0)
    is_last_author = models.IntegerField(default=0)
    affiliation = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = "publication_authors"

    def __str__(self):
        return f"{self.pmid_id} — {self.author}"


class DatasetAccession(models.Model):
    accession_id = models.AutoField(primary_key=True)
    accession = models.TextField(unique=True)
    accession_type = models.TextField()
    database = models.TextField()
    title = models.TextField(blank=True, null=True)
    organism = models.TextField(blank=True, null=True)
    platform = models.TextField(blank=True, null=True)
    summary = models.TextField(blank=True, null=True)
    overall_design = models.TextField(blank=True, null=True)
    num_samples = models.IntegerField(blank=True, null=True)
    submission_date = models.TextField(blank=True, null=True)
    status = models.TextField(blank=True, null=True)
    supplementary_files = models.TextField(blank=True, null=True)
    last_update_date = models.TextField(blank=True, null=True)
    contact_name = models.TextField(blank=True, null=True)
    contact_institute = models.TextField(blank=True, null=True)
    experiment_types = models.TextField(blank=True, null=True)
    relations = models.TextField(blank=True, null=True)
    sample_ids = models.TextField(blank=True, null=True)
    citation_pmids = models.TextField(blank=True, null=True)
    created_at = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = "dataset_accessions"

    def __str__(self):
        return self.accession

    @property
    def geo_url(self):
        if self.accession_type == "GSE":
            return f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={self.accession}"
        return None

    @property
    def sra_url(self):
        if self.accession_type in ("SRX", "SRP", "SRR"):
            return f"https://www.ncbi.nlm.nih.gov/sra/{self.accession}"
        return None


class PublicationDataset(models.Model):
    id = models.IntegerField(primary_key=True, db_column=_junction_pk_column())
    pmid = models.ForeignKey(
        Publication, on_delete=models.DO_NOTHING, db_column="pmid"
    )
    accession = models.ForeignKey(
        DatasetAccession, on_delete=models.DO_NOTHING, db_column="accession_id"
    )
    source = models.TextField(default="abstract")

    class Meta:
        managed = False
        db_table = "publication_datasets"


class DatasetFile(models.Model):
    file_id = models.AutoField(primary_key=True)
    accession = models.ForeignKey(
        DatasetAccession, on_delete=models.DO_NOTHING, db_column="accession_id"
    )
    file_name = models.TextField()
    file_type = models.TextField(blank=True, null=True)
    file_size_bytes = models.IntegerField(blank=True, null=True)
    file_url = models.TextField(blank=True, null=True)
    md5_checksum = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = "dataset_files"

    def __str__(self):
        return self.file_name

    @property
    def file_size_display(self):
        if not self.file_size_bytes:
            return ""
        size = self.file_size_bytes
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if abs(size) < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} PB"


class Grant(models.Model):
    grant_id = models.AutoField(primary_key=True)
    grant_number = models.TextField(blank=True, null=True)
    agency = models.TextField(blank=True, null=True)
    country = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = "grants"

    def __str__(self):
        return f"{self.grant_number} ({self.agency})"


class PublicationGrant(models.Model):
    id = models.IntegerField(primary_key=True, db_column=_junction_pk_column())
    pmid = models.ForeignKey(
        Publication, on_delete=models.DO_NOTHING, db_column="pmid"
    )
    grant = models.ForeignKey(
        Grant, on_delete=models.DO_NOTHING, db_column="grant_id"
    )

    class Meta:
        managed = False
        db_table = "publication_grants"


class SraExperiment(models.Model):
    experiment_id = models.AutoField(primary_key=True)
    srx_accession = models.TextField(unique=True)
    parent_accession = models.ForeignKey(
        DatasetAccession,
        on_delete=models.DO_NOTHING,
        db_column="parent_accession_id",
        blank=True,
        null=True,
    )
    source_gse = models.TextField(blank=True, null=True)
    title = models.TextField(blank=True, null=True)
    alias = models.TextField(blank=True, null=True)
    sample_accession = models.TextField(blank=True, null=True)
    sample_name = models.TextField(blank=True, null=True)
    sample_alias = models.TextField(blank=True, null=True)
    study_accession = models.TextField(blank=True, null=True)
    bioproject = models.TextField(blank=True, null=True)
    biosample = models.TextField(blank=True, null=True)
    library_name = models.TextField(blank=True, null=True)
    library_strategy = models.TextField(blank=True, null=True)
    library_source = models.TextField(blank=True, null=True)
    library_selection = models.TextField(blank=True, null=True)
    library_layout = models.TextField(blank=True, null=True)
    platform = models.TextField(blank=True, null=True)
    instrument_model = models.TextField(blank=True, null=True)
    organism = models.TextField(blank=True, null=True)
    sample_attributes = models.TextField(blank=True, null=True)
    original_file_names = models.TextField(blank=True, null=True)
    created_at = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = "sra_experiments"

    def __str__(self):
        return self.srx_accession


class SraRun(models.Model):
    run_id = models.AutoField(primary_key=True)
    srr_accession = models.TextField(unique=True)
    experiment = models.ForeignKey(
        SraExperiment,
        on_delete=models.DO_NOTHING,
        db_column="experiment_id",
        blank=True,
        null=True,
    )
    srx_accession = models.TextField(blank=True, null=True)
    alias = models.TextField(blank=True, null=True)
    total_spots = models.IntegerField(blank=True, null=True)
    total_bases = models.IntegerField(blank=True, null=True)
    size_mb = models.FloatField(blank=True, null=True)
    published_date = models.TextField(blank=True, null=True)
    sra_url = models.TextField(blank=True, null=True)
    cloud_urls = models.TextField(blank=True, null=True)
    file_names = models.TextField(blank=True, null=True)
    created_at = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = "sra_runs"

    def __str__(self):
        return self.srr_accession


class ComputationalMethod(models.Model):
    method_id = models.AutoField(primary_key=True)
    canonical_name = models.TextField(unique=True)
    category = models.TextField()
    url = models.TextField(blank=True, null=True)
    description = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = "computational_methods"

    def __str__(self):
        return f"{self.canonical_name} [{self.category}]"


class PublicationMethod(models.Model):
    id = models.AutoField(primary_key=True)
    pmid = models.ForeignKey(
        Publication, on_delete=models.DO_NOTHING, db_column="pmid"
    )
    method = models.ForeignKey(
        ComputationalMethod, on_delete=models.DO_NOTHING, db_column="method_id"
    )
    version = models.TextField(blank=True, null=True)
    source_type = models.TextField()
    matched_text = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = "publication_methods"

    def __str__(self):
        return f"{self.pmid_id} — {self.method}"


class AnalysisPipeline(models.Model):
    pipeline_id = models.AutoField(primary_key=True)
    pmid = models.ForeignKey(
        Publication, on_delete=models.DO_NOTHING, db_column="pmid"
    )
    accession = models.ForeignKey(
        DatasetAccession, on_delete=models.DO_NOTHING,
        db_column="accession_id", blank=True, null=True,
    )
    assay_type = models.TextField(blank=True, null=True)
    pipeline_title = models.TextField(blank=True, null=True)
    source = models.TextField()
    raw_text = models.TextField(blank=True, null=True)
    created_at = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = "analysis_pipelines"

    def __str__(self):
        title = self.pipeline_title or self.assay_type or "Pipeline"
        return f"{self.pmid_id}: {title}"


class PipelineStep(models.Model):
    step_id = models.AutoField(primary_key=True)
    pipeline = models.ForeignKey(
        AnalysisPipeline, on_delete=models.DO_NOTHING,
        db_column="pipeline_id",
    )
    step_order = models.IntegerField()
    description = models.TextField()
    method = models.ForeignKey(
        ComputationalMethod, on_delete=models.DO_NOTHING,
        db_column="method_id", blank=True, null=True,
    )
    tool_name = models.TextField(blank=True, null=True)
    tool_version = models.TextField(blank=True, null=True)
    parameters = models.TextField(blank=True, null=True)
    code_example = models.TextField(blank=True, null=True)
    code_language = models.TextField(blank=True, null=True)
    github_url = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = "pipeline_steps"
        ordering = ["step_order"]

    def __str__(self):
        return f"Step {self.step_order}: {self.description[:60]}"
