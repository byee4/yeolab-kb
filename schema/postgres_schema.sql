-- PostgreSQL schema for Yeo Lab Publications Database
-- Mirrors the SQLite schema in yeolab_publications.db with proper PG types.

BEGIN;

-- ============================================================
-- Core tables
-- ============================================================

CREATE TABLE IF NOT EXISTS publications (
    pmid            TEXT PRIMARY KEY,
    pmc_id          TEXT,
    doi             TEXT,
    pii             TEXT,
    title           TEXT NOT NULL,
    abstract        TEXT,
    journal_name    TEXT,
    journal_iso     TEXT,
    pub_date        TEXT,
    pub_year        INTEGER,
    pub_month       INTEGER,
    pub_day         INTEGER,
    volume          TEXT,
    issue           TEXT,
    pages           TEXT,
    pub_types       TEXT,
    mesh_terms      TEXT,
    keywords        TEXT,
    language        TEXT,
    is_open_access  INTEGER DEFAULT 0,
    citation_count  INTEGER,
    abstract_word_count INTEGER,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Full-text search vector (PostgreSQL only)
    search_vector   tsvector
);

CREATE INDEX IF NOT EXISTS idx_publications_year ON publications (pub_year);
CREATE INDEX IF NOT EXISTS idx_publications_journal ON publications (journal_name);
CREATE INDEX IF NOT EXISTS idx_publications_doi ON publications (doi);
CREATE INDEX IF NOT EXISTS idx_publications_search ON publications USING gin(search_vector);

-- Trigger to auto-populate search_vector on INSERT or UPDATE
CREATE OR REPLACE FUNCTION publications_search_trigger() RETURNS trigger AS $$
BEGIN
    NEW.search_vector :=
        to_tsvector('english', COALESCE(NEW.title, '') || ' ' ||
                               COALESCE(NEW.abstract, '') || ' ' ||
                               COALESCE(NEW.journal_name, ''));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trig_publications_search ON publications;
CREATE TRIGGER trig_publications_search
    BEFORE INSERT OR UPDATE ON publications
    FOR EACH ROW EXECUTE FUNCTION publications_search_trigger();


CREATE TABLE IF NOT EXISTS authors (
    author_id   SERIAL PRIMARY KEY,
    last_name   TEXT,
    fore_name   TEXT,
    initials    TEXT,
    orcid       TEXT
);

CREATE INDEX IF NOT EXISTS idx_authors_name ON authors (last_name, fore_name);
CREATE UNIQUE INDEX IF NOT EXISTS idx_authors_unique ON authors (last_name, fore_name, initials)
    WHERE last_name IS NOT NULL;


CREATE TABLE IF NOT EXISTS publication_authors (
    id              SERIAL PRIMARY KEY,
    pmid            TEXT NOT NULL REFERENCES publications(pmid) ON DELETE CASCADE,
    author_id       INTEGER NOT NULL REFERENCES authors(author_id) ON DELETE CASCADE,
    author_position INTEGER NOT NULL,
    is_first_author INTEGER DEFAULT 0,
    is_last_author  INTEGER DEFAULT 0,
    affiliation     TEXT,
    UNIQUE (pmid, author_id, author_position)
);

CREATE INDEX IF NOT EXISTS idx_pub_authors_pmid ON publication_authors (pmid);
CREATE INDEX IF NOT EXISTS idx_pub_authors_author ON publication_authors (author_id);


CREATE TABLE IF NOT EXISTS affiliations (
    affiliation_id  SERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE
);


CREATE TABLE IF NOT EXISTS publication_affiliations (
    id              SERIAL PRIMARY KEY,
    pmid            TEXT NOT NULL REFERENCES publications(pmid) ON DELETE CASCADE,
    affiliation_id  INTEGER NOT NULL REFERENCES affiliations(affiliation_id) ON DELETE CASCADE,
    UNIQUE (pmid, affiliation_id)
);


CREATE TABLE IF NOT EXISTS dataset_accessions (
    accession_id        SERIAL PRIMARY KEY,
    accession           TEXT NOT NULL UNIQUE,
    accession_type      TEXT NOT NULL,
    database            TEXT NOT NULL,
    title               TEXT,
    organism            TEXT,
    platform            TEXT,
    summary             TEXT,
    overall_design      TEXT,
    num_samples         INTEGER,
    submission_date     TEXT,
    status              TEXT,
    supplementary_files TEXT,
    last_update_date    TEXT,
    contact_name        TEXT,
    contact_institute   TEXT,
    experiment_types    TEXT,
    relations           TEXT,
    sample_ids          TEXT,
    citation_pmids      TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dataset_accession ON dataset_accessions (accession);
CREATE INDEX IF NOT EXISTS idx_dataset_type ON dataset_accessions (accession_type);


CREATE TABLE IF NOT EXISTS publication_datasets (
    id              SERIAL PRIMARY KEY,
    pmid            TEXT NOT NULL REFERENCES publications(pmid) ON DELETE CASCADE,
    accession_id    INTEGER NOT NULL REFERENCES dataset_accessions(accession_id) ON DELETE CASCADE,
    source          TEXT DEFAULT 'abstract',
    UNIQUE (pmid, accession_id, source)
);

CREATE INDEX IF NOT EXISTS idx_pub_datasets_pmid ON publication_datasets (pmid);
CREATE INDEX IF NOT EXISTS idx_pub_datasets_accession ON publication_datasets (accession_id);


CREATE TABLE IF NOT EXISTS dataset_files (
    file_id         SERIAL PRIMARY KEY,
    accession_id    INTEGER NOT NULL REFERENCES dataset_accessions(accession_id) ON DELETE CASCADE,
    file_name       TEXT NOT NULL,
    file_type       TEXT,
    file_size_bytes BIGINT,
    file_url        TEXT,
    md5_checksum    TEXT,
    UNIQUE (accession_id, file_name)
);

CREATE INDEX IF NOT EXISTS idx_dataset_files_accession ON dataset_files (accession_id);


CREATE TABLE IF NOT EXISTS grants (
    grant_id        SERIAL PRIMARY KEY,
    grant_number    TEXT,
    agency          TEXT,
    country         TEXT,
    UNIQUE (grant_number, agency)
);


CREATE TABLE IF NOT EXISTS publication_grants (
    id          SERIAL PRIMARY KEY,
    pmid        TEXT NOT NULL REFERENCES publications(pmid) ON DELETE CASCADE,
    grant_id    INTEGER NOT NULL REFERENCES grants(grant_id) ON DELETE CASCADE,
    UNIQUE (pmid, grant_id)
);

CREATE INDEX IF NOT EXISTS idx_pub_grants_pmid ON publication_grants (pmid);
CREATE INDEX IF NOT EXISTS idx_pub_grants_grant ON publication_grants (grant_id);


CREATE TABLE IF NOT EXISTS sra_experiments (
    experiment_id       SERIAL PRIMARY KEY,
    srx_accession       TEXT NOT NULL UNIQUE,
    parent_accession_id INTEGER REFERENCES dataset_accessions(accession_id) ON DELETE SET NULL,
    source_gse          TEXT,
    title               TEXT,
    alias               TEXT,
    sample_accession    TEXT,
    sample_name         TEXT,
    sample_alias        TEXT,
    study_accession     TEXT,
    bioproject          TEXT,
    biosample           TEXT,
    library_name        TEXT,
    library_strategy    TEXT,
    library_source      TEXT,
    library_selection   TEXT,
    library_layout      TEXT,
    platform            TEXT,
    instrument_model    TEXT,
    organism            TEXT,
    sample_attributes   TEXT,
    original_file_names TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sra_exp_parent ON sra_experiments (parent_accession_id);


CREATE TABLE IF NOT EXISTS sra_runs (
    run_id          SERIAL PRIMARY KEY,
    srr_accession   TEXT NOT NULL UNIQUE,
    experiment_id   INTEGER REFERENCES sra_experiments(experiment_id) ON DELETE SET NULL,
    srx_accession   TEXT,
    alias           TEXT,
    total_spots     BIGINT,
    total_bases     BIGINT,
    size_mb         DOUBLE PRECISION,
    published_date  TEXT,
    sra_url         TEXT,
    cloud_urls      TEXT,
    file_names      TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sra_runs_experiment ON sra_runs (experiment_id);


CREATE TABLE IF NOT EXISTS publication_summaries (
    pmid        TEXT PRIMARY KEY REFERENCES publications(pmid) ON DELETE CASCADE,
    summary     TEXT,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS update_log (
    id          SERIAL PRIMARY KEY,
    update_type TEXT,
    status      TEXT,
    message     TEXT,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMIT;
