import json
import sqlite3
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from opportunity_lab.kalshi_depth_replenishment import init as init_depth
from opportunity_lab.kalshi_depth_cohort_trades import PROTOCOL, init, candidate, poll, status


class DepthCohortTradeTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        init_depth(self.db, now=100)
        init(self.db, 100)
        self.db.execute("INSERT INTO depth_latest VALUES('M-1',399,'{}')")
        self.db.execute("INSERT INTO depth_latest VALUES('M-2',398,'{}')")

    @staticmethod
    def trade(identity, ticker='M-1'):
        return {'trade_id':identity,'ticker':ticker,
                'created_time':datetime.fromtimestamp(399,timezone.utc).isoformat(),
                'count_fp':'2','yes_price_dollars':'0.45','no_price_dollars':'0.55',
                'is_block_trade':False}

    def test_rotates_fresh_depth_without_signal(self):
        calls=[]
        def get_targeted_trades(*,params):
            calls.append(params)
            return {'trades':[self.trade(params['ticker'],params['ticker'])],'cursor':''},params['max_ts'],params['max_ts']+1
        markets=SimpleNamespace(get_targeted_trades=get_targeted_trades)
        self.assertEqual(candidate(self.db,400),'M-1')
        poll(self.db,markets,400)
        self.assertEqual(status(self.db)['public_trades'],1)
        self.assertTrue(status(self.db)['latest_poll']['ticker_window_complete'])
        self.assertFalse(status(self.db)['promotion_ready'])
        poll(self.db,markets,430)
        self.assertEqual([call['ticker'] for call in calls],['M-1'])
        self.db.execute("UPDATE depth_latest SET observed_at=460")
        poll(self.db,markets,520)
        self.assertEqual([call['ticker'] for call in calls],['M-1','M-2'])

    def test_signal_preempts_depth_probe_and_cap_is_bounded(self):
        calls=[]
        markets=SimpleNamespace(get_targeted_trades=lambda **kw:calls.append(kw))
        poll(self.db,markets,400,signal_candidate='M-1')
        self.assertEqual(calls,[])
        self.assertEqual(status(self.db)['polls'],0)
        poll(self.db,markets,400)
        self.assertEqual(len(calls),1)
        self.assertFalse(status(self.db)['continuous_coverage'])

    def test_partial_window_and_frozen_registration(self):
        calls=[]
        def get_targeted_trades(*,params):
            calls.append(params)
            return {'trades':[],'cursor':'next'},400,401
        poll(self.db,SimpleNamespace(get_targeted_trades=get_targeted_trades),400)
        self.assertEqual(len(calls),2)
        self.assertFalse(status(self.db)['latest_poll']['ticker_window_complete'])
        registration=Path(__file__).resolve().parents[1]/'configs/kalshi_depth_cohort_trades_v3_20261008/registration.json'
        self.assertEqual(json.loads(registration.read_text()),PROTOCOL)
        self.db.execute("UPDATE depth_trade_protocol SET detail='{}'")
        with self.assertRaisesRegex(ValueError,'protocol_changed'): init(self.db,500)


if __name__ == '__main__':
    unittest.main()
