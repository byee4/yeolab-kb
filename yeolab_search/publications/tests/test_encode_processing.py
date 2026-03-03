from unittest.mock import patch

from django.test import SimpleTestCase

from publications import services


class EncodeProcessingExtractionTests(SimpleTestCase):
    def test_encode_fetch_experiment_and_files_queries_live_endpoints(self):
        with (
            patch("publications.services._encode_api_get", return_value={"accession": "ENCSR423ERM"}) as api_get,
            patch("publications.services._encode_search", return_value=[{"accession": "ENCFF000AAA"}]) as search,
        ):
            detail, files = services._encode_fetch_experiment_and_files("ENCSR423ERM")

        self.assertEqual(detail.get("accession"), "ENCSR423ERM")
        self.assertEqual(len(files), 1)
        api_get.assert_called_once()
        search.assert_called_once()

    def test_encode_fetch_experiment_and_files_returns_empty_for_blank_accession(self):
        detail, files = services._encode_fetch_experiment_and_files("")
        self.assertEqual(detail, {})
        self.assertEqual(files, [])

    def test_extract_encode_processing_steps_uses_detail_and_files(self):
        exp = {
            "accession": "ENCSR773ABC",
            "assay_title": "eCLIP",
            "description": "Reads were processed with the ENCODE eCLIP pipeline.",
            "biosample_summary": "K562",
        }
        detail = {
            "notes": ["Peak calling and IDR filtering were applied."],
        }
        files = [
            {
                "mapped_by": "STAR",
                "output_type": "alignments",
                "assembly": "hg38",
                "genome_annotation": "GENCODE v29",
                "file_format": "bam",
            }
        ]

        steps, raw_text = services._extract_encode_processing_steps(exp, detail=detail, files=files)

        self.assertGreaterEqual(len(steps), 3)
        self.assertIn("STAR", raw_text)
        self.assertIn("alignments", raw_text)
        self.assertIn("hg38", raw_text)

    def test_extract_encode_processing_steps_has_fallback(self):
        exp = {"accession": "ENCSR000AAA", "assay_title": "", "description": ""}
        steps, raw_text = services._extract_encode_processing_steps(exp, detail={}, files=[])

        self.assertEqual(len(steps), 1)
        self.assertIn("ENCSR000AAA", raw_text)

    def test_resolve_encode_pipeline_pmid_prefers_existing_reference(self):
        class FakeCursor:
            def __init__(self):
                self._rows = []

            def execute(self, sql, params):
                q = " ".join(sql.split()).lower()
                if "from publications where pmid" in q:
                    self._rows = [("12345",)] if params[0] == "12345" else []
                elif "from publication_datasets" in q:
                    self._rows = [("77777",)]
                else:
                    self._rows = []

            def fetchone(self):
                return self._rows[0] if self._rows else None

        pmid = services._resolve_encode_pipeline_pmid(FakeCursor(), accession_id=1, pmids=["12345", "99999"])
        self.assertEqual(pmid, "12345")

    def test_resolve_encode_pipeline_pmid_falls_back_to_linked_publication(self):
        class FakeCursor:
            def __init__(self):
                self._rows = []

            def execute(self, sql, params):
                q = " ".join(sql.split()).lower()
                if "from publications where pmid" in q:
                    self._rows = []
                elif "from publication_datasets" in q:
                    self._rows = [("88888",)]
                else:
                    self._rows = []

            def fetchone(self):
                return self._rows[0] if self._rows else None

        pmid = services._resolve_encode_pipeline_pmid(FakeCursor(), accession_id=2, pmids=["nope"])
        self.assertEqual(pmid, "88888")

    @patch("publications.services._log")
    def test_encode_search_experiments_for_grant_falls_back_after_error(self, _log_mock):
        def fake_encode_search(search_type, params):
            self.assertEqual(search_type, "Experiment")
            if "award.name" in params:
                raise RuntimeError("403 forbidden")
            if "award.project_num" in params:
                return [{"accession": "ENCSR001AAA"}]
            return []

        with patch("publications.services._encode_search", side_effect=fake_encode_search):
            results = services._encode_search_experiments_for_grant("U41HG009889")

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["accession"], "ENCSR001AAA")
