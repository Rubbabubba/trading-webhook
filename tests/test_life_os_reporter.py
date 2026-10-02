import unittest
from unittest.mock import patch

from opportunity_lab.life_os_reporter import publish


class Accepted:
    status = 200
    def __enter__(self):
        return self
    def __exit__(self, *_):
        pass


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


if __name__ == "__main__":
    unittest.main()
