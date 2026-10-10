import json
import sqlite3
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from opportunity_lab.kalshi_scan_trade_capture import init, poll, status, ingest, quote_summary


class ScanTradeCaptureTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:', isolation_level=None)
        init(self.db, 100)
        self.frame = {'ticker': 'M', 'received_at': 399,
            'orderbook_fp': {'yes_dollars': [['0.4', '5']], 'no_dollars': [['0.5', '6']]}}
        self.trade = {'trade_id': 'T', 'ticker': 'M', 'created_time':
            datetime.fromtimestamp(399, timezone.utc).isoformat(),
            'count_fp': '1', 'yes_price_dollars': '0.4', 'no_price_dollars': '0.6',
            'is_block_trade': False}

    def test_independent_of_frozen_depth_cap_and_never_claims_fill(self):
        # No V1 depth tables are present: the new collector must not depend on them.
        calls = []
        def get(**kw):
            calls.append(kw)
            return {'trades': [self.trade], 'cursor': ''}, 400, 401
        markets = SimpleNamespace(get_targeted_trades=get)
        poll(self.db, markets, self.frame, 'E', 400)
        result = status(self.db)
        self.assertEqual(result['public_trades'], 1)
        self.assertEqual(result['trade_events'], 1)
        self.assertFalse(result['promotion_ready'])
        self.assertEqual(result['owned_fills'], 0)
        poll(self.db, markets, self.frame, 'E', 430)
        self.assertEqual(len(calls), 1)
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute('DELETE FROM scan_trade_v4_trades')

    def test_stale_quote_and_future_trade_rejected(self):
        with self.assertRaises(ValueError): quote_summary(self.frame, 500)
        with self.assertRaises(ValueError): quote_summary({**self.frame, 'received_at': 401}, 400)
        future = {**self.trade, 'created_time': datetime.fromtimestamp(401, timezone.utc).isoformat()}
        with self.assertRaises(ValueError):
            ingest(self.db, {'trades': [future], 'cursor': ''}, ticker='M', event_id='E', now=400, received=402)

    def test_failures_partial_pages_and_restart_do_not_bypass_bounds(self):
        calls = []
        def get(**kw):
            calls.append(kw)
            return {'trades': [], 'cursor': 'more'}, 400, 401
        markets = SimpleNamespace(get_targeted_trades=get)
        poll(self.db, markets, self.frame, 'E', 400)
        self.assertEqual(len(calls), 2)
        self.assertFalse(status(self.db)['latest_poll']['ticker_window_complete'])
        init(self.db, 420)
        poll(self.db, markets, self.frame, 'E', 430)
        self.assertEqual(len(calls), 2)
        self.db.execute("INSERT INTO scan_trade_v4_polls SELECT n,400,'M','E','{}','{}' FROM "
            "(WITH RECURSIVE c(n) AS (SELECT 2 UNION ALL SELECT n+1 FROM c WHERE n<4096) SELECT n FROM c)")
        poll(self.db, markets, {**self.frame, 'received_at': 500}, 'E', 500)
        self.assertTrue(status(self.db)['capacity_reached'])
        self.assertEqual(status(self.db)['state'], 'capacity_insufficient_coverage')
        self.assertEqual(len(calls), 2)

    def test_transport_failure_is_durable_and_rate_limited(self):
        calls = []
        def fail(**kw):
            calls.append(kw)
            raise RuntimeError('untrusted transport detail')
        markets = SimpleNamespace(get_targeted_trades=fail)
        poll(self.db, markets, self.frame, 'E', 400)
        self.assertEqual(status(self.db)['latest_poll']['state'], 'blocked')
        self.assertNotIn('untrusted', json.dumps(status(self.db)))
        init(self.db, 410)
        poll(self.db, markets, self.frame, 'E', 410)
        self.assertEqual(len(calls), 1)

    def test_protocol_is_frozen(self):
        self.db.execute("UPDATE scan_trade_v4_protocol SET detail='{}'")
        with self.assertRaises(ValueError): init(self.db, 500)
