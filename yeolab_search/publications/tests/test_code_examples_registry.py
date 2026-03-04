from types import SimpleNamespace
from unittest.mock import patch

from django.test import RequestFactory, SimpleTestCase

from publications import code_examples, views


class CodeExamplesRegistryTests(SimpleTestCase):
    def setUp(self):
        self._orig_registry = code_examples._REGISTRY
        self._orig_paths = code_examples._PATHS
        self._orig_last_refresh = code_examples._LAST_REFRESH_TS
        self._orig_interval = code_examples._REFRESH_INTERVAL_SEC
        self.rf = RequestFactory()

    def tearDown(self):
        code_examples._REGISTRY = self._orig_registry
        code_examples._PATHS = self._orig_paths
        code_examples._LAST_REFRESH_TS = self._orig_last_refresh
        code_examples._REFRESH_INTERVAL_SEC = self._orig_interval

    def test_ensure_fresh_registry_skips_reload_within_interval(self):
        code_examples._LAST_REFRESH_TS = 100.0
        code_examples._REFRESH_INTERVAL_SEC = 10.0

        with (
            patch("publications.code_examples.time.monotonic", return_value=105.0),
            patch("publications.code_examples._load_registry") as load_mock,
        ):
            code_examples._ensure_fresh_registry()

        load_mock.assert_not_called()

    def test_ensure_fresh_registry_reloads_after_interval(self):
        code_examples._LAST_REFRESH_TS = 100.0
        code_examples._REFRESH_INTERVAL_SEC = 10.0

        with (
            patch("publications.code_examples.time.monotonic", side_effect=[120.0, 120.0, 121.0]),
            patch(
                "publications.code_examples._load_registry",
                return_value=({"GSE1": {"steps": []}}, {"GSE1": "2025/Mar"}),
            ) as load_mock,
        ):
            code_examples._ensure_fresh_registry()

        load_mock.assert_called_once()
        self.assertIn("GSE1", code_examples._REGISTRY)
        self.assertEqual(code_examples._PATHS.get("GSE1"), "2025/Mar")
        self.assertEqual(code_examples._LAST_REFRESH_TS, 121.0)

    @patch("publications.views.render")
    def test_analysis_list_uses_cached_code_example_pipelines(self, render_mock):
        fake_model = SimpleNamespace(
            objects=SimpleNamespace(
                annotate=lambda *args, **kwargs: SimpleNamespace(
                    select_related=lambda *a, **k: SimpleNamespace(order_by=lambda *oa, **ok: [])
                )
            )
        )
        with (
            patch("publications.views.AnalysisPipeline", fake_model),
            patch("publications.views._get_cached_code_example_pipelines", return_value=[]),
            patch("publications.views._build_code_example_pipelines") as build_mock,
        ):
            views.analysis_list(self.rf.get("/analysis/"))

        build_mock.assert_not_called()
