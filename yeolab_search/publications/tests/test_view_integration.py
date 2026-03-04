from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from django.test import RequestFactory, SimpleTestCase
from django.core.files.uploadedfile import SimpleUploadedFile

from publications import views


class FakeQuerySet:
    """Small chainable queryset stub for view-level integration tests."""

    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self


class FakeSliceableQuerySet:
    """Sliceable queryset stub used by dataset preview queries."""

    def __init__(self, records):
        self._records = records

    def order_by(self, *args, **kwargs):
        return self

    def __getitem__(self, item):
        return self._records[item]


class FakePaginator:
    def __init__(self, _object_list, _per_page):
        self.count = 3

    def get_page(self, _page_num):
        return SimpleNamespace(
            has_other_pages=lambda: False,
            has_previous=lambda: False,
            has_next=lambda: False,
            number=1,
            paginator=SimpleNamespace(num_pages=1),
        )


class PublicationViewsIntegrationTests(SimpleTestCase):
    def setUp(self):
        self.rf = RequestFactory()

    @patch("publications.views.render")
    def test_home_builds_expected_stats_context(self, render_mock):
        pub_values = MagicMock()
        pub_values.annotate.return_value.order_by.return_value = [
            {"pub_year": 2024, "count": 5},
            {"pub_year": None, "count": 1},
        ]
        journal_values = MagicMock()
        journal_values.annotate.return_value.order_by.return_value = [
            {"journal_name": "Nature", "count": 10},
        ]

        with (
            patch.object(views.Publication, "objects") as pub_objects,
            patch.object(views.Author, "objects") as author_objects,
            patch.object(views.DatasetAccession, "objects") as ds_objects,
            patch.object(views.DatasetFile, "objects") as df_objects,
            patch.object(views.Grant, "objects") as grant_objects,
            patch.object(views.SraExperiment, "objects") as srx_objects,
            patch.object(views.SraRun, "objects") as srr_objects,
        ):
            pub_objects.count.return_value = 100
            pub_objects.values.side_effect = lambda field: pub_values if field == "pub_year" else journal_values
            author_objects.count.return_value = 20
            ds_objects.count.return_value = 30
            df_objects.count.return_value = 40
            grant_objects.count.return_value = 50
            srx_objects.count.return_value = 60
            srr_objects.count.return_value = 70

            views.home(self.rf.get("/"))

        self.assertTrue(render_mock.called)
        ctx = render_mock.call_args.args[2]
        self.assertEqual(ctx["stats"]["publications"], 100)
        self.assertEqual(ctx["stats"]["datasets"], 30)
        self.assertIn('"year": 2024', ctx["stats"]["year_data"])
        self.assertEqual(ctx["stats"]["top_journals"][0]["journal_name"], "Nature")

    @patch("publications.views.render")
    @patch("publications.views.Paginator", new=FakePaginator)
    def test_search_short_query_skips_dataset_and_analysis(self, render_mock):
        with (
            patch.object(views.Publication, "objects") as pub_objects,
            patch("publications.views.cache.get", return_value={"years": [2024], "journals": []}),
            patch("publications.views._fts_search", return_value=[]),
            patch.object(views.DatasetAccession, "objects") as ds_objects,
            patch.object(views.AnalysisPipeline, "objects") as ap_objects,
        ):
            pub_objects.all.return_value = FakeQuerySet()
            views.search(self.rf.get("/search/", {"q": "a"}))

        ctx = render_mock.call_args.args[2]
        self.assertEqual(ctx["dataset_results"], [])
        self.assertEqual(ctx["analysis_results"], [])
        self.assertFalse(ctx["dataset_has_more"])
        self.assertFalse(ctx["analysis_has_more"])
        ds_objects.filter.assert_not_called()
        ap_objects.select_related.assert_not_called()

    @patch("publications.views.render")
    @patch("publications.views.Paginator", new=FakePaginator)
    def test_search_query_builds_dataset_and_analysis_previews(self, render_mock):
        dataset_records = [
            SimpleNamespace(
                accession_id=i,
                accession=f"GSE{i}",
                accession_type="GSE",
                database="GEO",
                title=f"Dataset {i}",
                organism="Homo sapiens",
                platform="GPL",
            )
            for i in range(30)
        ]
        db_pipeline = SimpleNamespace(
            pipeline_id=1,
            accession=SimpleNamespace(accession="GSE777"),
            pipeline_title="RNA Pipeline",
            source="db",
            assay_type="RNA-Seq",
            step_count=4,
            pmid=SimpleNamespace(title="Paper", pub_year=2025),
        )
        ce_pipeline = {
            "id": "GSE778",
            "pipeline_title": "GSE778 Processing Pipeline",
            "accession": "GSE778",
            "source": "code_examples",
            "assay_type": "RNA-Seq",
            "step_count": 3,
            "pub_title": "Paper CE",
            "pub_year": 2026,
            "acc_title": "Dataset CE",
        }

        with (
            patch.object(views.Publication, "objects") as pub_objects,
            patch("publications.views.cache.get", return_value={"years": [2025], "journals": []}),
            patch("publications.views._fts_search", return_value=["1"]),
            patch.object(views.DatasetAccession, "objects") as ds_objects,
            patch.object(views.AnalysisPipeline, "objects") as ap_objects,
            patch("publications.views._get_cached_code_example_pipelines", return_value=[ce_pipeline]),
        ):
            pub_objects.all.return_value = FakeQuerySet()
            ds_objects.filter.return_value = FakeSliceableQuerySet(dataset_records)
            ap_objects.select_related.return_value.annotate.return_value.filter.return_value.order_by.return_value = [
                db_pipeline
            ]

            views.search(self.rf.get("/search/", {"q": "GSE"}))

        ctx = render_mock.call_args.args[2]
        self.assertEqual(len(ctx["dataset_results"]), views.SEARCH_PREVIEW_LIMIT)
        self.assertTrue(ctx["dataset_has_more"])
        self.assertEqual(len(ctx["analysis_results"]), 2)
        self.assertFalse(ctx["analysis_has_more"])
        self.assertEqual(ctx["analysis_results"][0]["accession"], "GSE778")

    def test_get_cached_code_example_pipelines_uses_cache(self):
        with (
            patch("publications.views.cache.get", return_value=[{"id": "X"}]) as cache_get,
            patch("publications.views._build_code_example_pipelines") as build_mock,
        ):
            result = views._get_cached_code_example_pipelines()

        self.assertEqual(result, [{"id": "X"}])
        cache_get.assert_called_once()
        build_mock.assert_not_called()

    def test_healthz_returns_503_when_db_fails(self):
        fake_connection = SimpleNamespace(cursor=MagicMock(side_effect=RuntimeError("db down")))
        with patch("publications.views.connection", fake_connection):
            response = views.healthz(self.rf.get("/healthz/"))
        self.assertEqual(response.status_code, 503)

    def test_analysis_detail_by_accession_missing_returns_custom_404(self):
        request = self.rf.get("/analysis/dataset/ENCSR519QAA/")
        with patch("publications.code_examples.get_steps_for_dataset", return_value=None):
            response = views.analysis_detail_by_accession(request, "ENCSR519QAA")
        self.assertEqual(response.status_code, 404)
        self.assertIn(b"No analysis content is currently available", response.content)

    @patch("publications.services.start_encode_json_upload_import")
    def test_admin_upload_encode_json_imports_payload(self, import_mock):
        import_mock.return_value = {
            "ok": True,
            "upload_id": "abc123_1",
            "resume_from_batch": 0,
            "total_experiments": 1,
        }
        upload = SimpleUploadedFile(
            "encode.json",
            b'{"@graph":[{"accession":"ENCSR000AAA"}]}',
            content_type="application/json",
        )

        request = self.rf.post(
            "/admin/upload-encode-json/",
            {"grant_label": "U41HG009889", "override_existing": "1", "json_file": upload},
        )
        request.user = SimpleNamespace(is_authenticated=True)
        request._dont_enforce_csrf_checks = True

        response = views.admin_upload_encode_json(request)
        self.assertEqual(response.status_code, 200)
        import_mock.assert_called_once()
        kwargs = import_mock.call_args.kwargs
        self.assertTrue(kwargs["override_existing"])

    @patch("publications.services.request_stop_encode_json_upload")
    def test_admin_stop_encode_json_upload(self, stop_mock):
        stop_mock.return_value = {"ok": True, "message": "Stop requested."}
        request = self.rf.post(
            "/admin/upload-encode-json/stop/",
            {"upload_id": "abc123_1"},
        )
        request.user = SimpleNamespace(is_authenticated=True)
        request._dont_enforce_csrf_checks = True

        response = views.admin_stop_encode_json_upload(request)
        self.assertEqual(response.status_code, 200)
        stop_mock.assert_called_once_with("abc123_1")
