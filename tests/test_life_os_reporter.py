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
    def test_reports_actual_host_revision_without_mutating_monitor_packet(self):
        packet={'worker':{'environment':'demo','production_execution_enabled':False}}
        with patch.dict('os.environ',{'RENDER_GIT_COMMIT':'a'*40,'RENDER_SERVICE_ID':'srv-dalhjqv40ujc73e2km1g'}), patch('opportunity_lab.life_os_reporter.urlopen',return_value=Accepted()) as call:
            publish(packet,url='https://life.example/ingest/kalshi',token='intake-token')
        sent=json.loads(call.call_args.args[0].data)
        self.assertEqual(sent['workforce_runtime']['revision'],'a'*40)
        self.assertNotIn('workforce_runtime',packet)

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


class WalletCopyTelemetryRegressionTests(unittest.TestCase):
    def test_read_states_and_unknown_inventory(self):
        from datetime import datetime, timezone, timedelta
        from opportunity_lab.life_os_reporter import wallet_copy_status
        now = datetime.now(timezone.utc)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'status.json'
            self.assertEqual(wallet_copy_status(path, now=now)['state'], 'missing')
            for payload in ('{', json.dumps({'environment':'live','at':now.isoformat()})):
                path.write_text(payload)
                self.assertEqual(wallet_copy_status(path, now=now)['state'], 'invalid')
            path.write_text(json.dumps({'environment':'demo','at':now.isoformat(), 'wallet_id':'secret', 'error':'Bearer secret', 'signals':{'buy:observed':3}}))
            result = wallet_copy_status(path, now=now)
            self.assertEqual(result['state'], 'observed')
            self.assertIsNone(result['open_positions'])
            self.assertNotIn('secret', json.dumps(result))
            self.assertEqual(result['signal_counts_lifetime'], {'buy:observed':3})
            self.assertEqual(wallet_copy_status(path, now=now+timedelta(seconds=181))['state'], 'stale')

    def test_optional_failure_isolated(self):
        packet = {'worker':{'environment':'demo','production_execution_enabled':False}}
        with patch('opportunity_lab.life_os_reporter.wallet_copy_status', side_effect=RuntimeError), patch('opportunity_lab.life_os_reporter.urlopen', return_value=Accepted()) as call:
            self.assertTrue(publish(packet, url='https://life.example/ingest/kalshi', token='intake-token'))
        self.assertEqual(json.loads(call.call_args.args[0].data)['wallet_copy']['state'], 'invalid')
        self.assertNotIn('wallet_copy', packet)


class WalletCopyHealthRepairRegressionTests(unittest.TestCase):
    def test_source_error_count_regression(self):
        from datetime import datetime, timezone
        from opportunity_lab.life_os_reporter import wallet_copy_status
        now = datetime.now(timezone.utc)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'status.json'
            for extra, expected in (({}, None), ({'source_errors': []}, 0),
                                    ({'source_errors': ['private detail', 'other']}, 2),
                                    ({'source_errors': 'private detail'}, None),
                                    ({'source_errors': {}}, None),
                                    ({'source_errors': None}, None)):
                with self.subTest(extra=extra):
                    path.write_text(json.dumps(dict(environment='demo', at=now.isoformat(), **extra)))
                    result = wallet_copy_status(path, now=now)
                    self.assertEqual(result['source_error_count'], expected)
                    self.assertIs(result['current_error_present'], False)
                    self.assertNotIn('private detail', json.dumps(result))

    def test_redacted_current_error_presence_regression(self):
        from datetime import datetime, timezone
        from opportunity_lab.life_os_reporter import wallet_copy_status
        now = datetime.now(timezone.utc)
        packet = {'worker': {'environment': 'demo', 'production_execution_enabled': False}}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'status.json'
            for field in ('error', 'execution_error'):
                with self.subTest(field=field):
                    path.write_text(json.dumps({'environment': 'demo', 'at': now.isoformat(),
                                                field: 'private error text / not_a_code'}))
                    result = wallet_copy_status(path, now=now)
                    self.assertIs(result['current_error_present'], True)
                    self.assertIsNone(result['current_error']['code'])
                    with patch('opportunity_lab.life_os_reporter.wallet_copy_status', return_value=result), patch('opportunity_lab.life_os_reporter.urlopen', return_value=Accepted()) as call:
                        self.assertTrue(publish(packet, url='https://life.example/ingest/kalshi', token='intake-token'))
                    exported = call.call_args.args[0].data.decode()
                    self.assertNotIn('private error text', exported)
                    self.assertIs(json.loads(exported)['wallet_copy']['current_error_present'], True)
            path.write_text(json.dumps({'environment': 'demo', 'at': now.isoformat(),
                                        'error': '', 'execution_error': ''}))
            self.assertIs(wallet_copy_status(path, now=now)['current_error_present'], False)


class WalletCopyTimestampRegressionTests(unittest.TestCase):
    def test_future_timestamp_matches_intake_tolerance(self):
        from datetime import datetime, timezone, timedelta
        from opportunity_lab.life_os_reporter import wallet_copy_status
        now = datetime.now(timezone.utc)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'status.json'
            for seconds, expected in ((4, 'observed'), (6, 'invalid'), (30, 'invalid')):
                with self.subTest(seconds=seconds):
                    path.write_text(json.dumps({'environment': 'demo',
                        'at': (now + timedelta(seconds=seconds)).isoformat()}))
                    self.assertEqual(wallet_copy_status(path, now=now)['state'], expected)


class WalletCopyOptionalTimestampRepairTests(unittest.TestCase):
    def test_malformed_optional_timestamp_retains_coverage(self):
        from datetime import datetime, timezone, timedelta
        from opportunity_lab.life_os_reporter import wallet_copy_status
        now = datetime.now(timezone.utc)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'status.json'
            for stamp in (None, 'bad', '2026-01-01T00:00:00',
                          (now + timedelta(seconds=30)).isoformat(), {}, 1e300):
                with self.subTest(stamp=stamp):
                    path.write_text(json.dumps(dict(environment='demo', at=now.isoformat(),
                        ranked_wallets=4, last_submission_failure=dict(
                            error='private detail', observed_at=stamp))))
                    result = wallet_copy_status(path, now=now)
                    self.assertEqual(result['state'], 'observed')
                    self.assertEqual(result['ranked_wallets'], 4)
                    self.assertNotIn('latest_submission_error', result)
                    self.assertNotIn('private detail', json.dumps(result))
