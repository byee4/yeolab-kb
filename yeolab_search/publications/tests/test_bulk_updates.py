from unittest.mock import patch

from django.test import SimpleTestCase

from publications import services


class BulkUpdateStatusTests(SimpleTestCase):
    def setUp(self):
        services._set_status(
            running=False,
            progress="",
            log=[],
            started_at=None,
            finished_at=None,
            error=None,
            stats={},
        )

    def tearDown(self):
        services._set_status(
            running=False,
            progress="",
            log=[],
            started_at=None,
            finished_at=None,
            error=None,
            stats={},
        )

    def test_get_update_status_returns_defensive_copies(self):
        services._set_status(log=["entry"], stats={"publications": 1})

        snapshot = services.get_update_status()
        snapshot["log"].append("mutated")
        snapshot["stats"]["publications"] = 999

        fresh = services.get_update_status()
        self.assertEqual(fresh["log"], ["entry"])
        self.assertEqual(fresh["stats"], {"publications": 1})

    @patch("publications.services._run_methods_update")
    @patch("publications.services.threading.Thread")
    def test_start_methods_update_starts_worker(self, thread_cls, _run_methods_update):
        thread = thread_cls.return_value

        started = services.start_methods_update()

        self.assertTrue(started)
        thread_cls.assert_called_once()
        thread.start.assert_called_once()
        status = services.get_update_status()
        self.assertTrue(status["running"])
        self.assertEqual(status["progress"], "Starting methods extraction...")

    @patch("publications.services._run_pipeline_update")
    @patch("publications.services.threading.Thread")
    def test_start_pipeline_update_handles_thread_start_failure(self, thread_cls, _run_pipeline_update):
        thread = thread_cls.return_value
        thread.start.side_effect = RuntimeError("thread boom")

        started = services.start_pipeline_update()

        self.assertFalse(started)
        status = services.get_update_status()
        self.assertFalse(status["running"])
        self.assertIsNotNone(status["finished_at"])
        self.assertIn("Failed to start background worker", status["error"] or "")

    @patch("publications.services._check_deps")
    @patch("publications.services._run_update")
    @patch("publications.services.threading.Thread")
    def test_start_full_update_handles_thread_start_failure(self, thread_cls, _run_update, _check_deps):
        thread = thread_cls.return_value
        thread.start.side_effect = RuntimeError("cannot start")

        started = services.start_full_update(mode="full")

        self.assertFalse(started)
        status = services.get_update_status()
        self.assertFalse(status["running"])
        self.assertIn("Failed to start background worker", status["error"] or "")

    @patch("publications.services._run_methods_update")
    @patch("publications.services.threading.Thread")
    def test_start_methods_update_returns_false_when_already_running(self, thread_cls, _run_methods_update):
        services._set_status(running=True)

        started = services.start_methods_update()

        self.assertFalse(started)
        thread_cls.assert_not_called()
