import unittest
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from opportunity_lab.life_os_reporter import publish, fetch_ideas


class Accepted:
    status = 200
    def __enter__(self):
        return self
    def __exit__(self, *_):
        pass

    def read(self, _size):
        return json.dumps({"schema": "kalshi_research_ideas_v1", "ideas": []}).encode()


class ReporterTests(unittest.TestCase):
    def test_demo_report_uses_intake_only_token(self):
        packet = {"worker": {"environment": "demo", "production_execution_enabled": False}}
        with patch("opportunity_lab.life_os_reporter.urlopen", return_value=Accepted()) as call:
            self.assertTrue(publish(packet, url="https://life.example/ingest/kalshi", token="intake-token"))
        request = call.call_args.args[0]
        self.assertEqual(request.get_header("Authorization"), "Bearer intake-token")
        self.assertEqual(request.get_method(), "POST")

    def test_rejects_production_or_insecure_destination(self):
        packet = {"worker": {"environment": "production", "production_execution_enabled": True}}
        with self.assertRaises(ValueError):
            publish(packet, url="https://life.example/ingest/kalshi", token="token")
        packet["worker"] = {"environment": "demo", "production_execution_enabled": False}
        with self.assertRaises(ValueError):
            publish(packet, url="http://life.example/ingest/kalshi", token="token")

    def test_idea_feed_is_authenticated_and_atomically_saved(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "ideas.json"
            with patch("opportunity_lab.life_os_reporter.urlopen", return_value=Accepted()) as call:
                self.assertEqual(fetch_ideas(target, url="https://life.example/ingest/kalshi",
                                             token="intake-token"), 0)
            request = call.call_args.args[0]
            self.assertEqual(request.get_method(), "GET")
            self.assertEqual(request.get_header("Authorization"), "Bearer intake-token")
            self.assertEqual(request.full_url, "https://life.example/research/kalshi/ideas")
            self.assertEqual(json.loads(target.read_text())["ideas"], [])


if __name__ == "__main__":
    unittest.main()
