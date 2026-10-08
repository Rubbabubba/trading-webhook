from datetime import datetime,timezone
import json
import sqlite3
import unittest
from types import SimpleNamespace
from pathlib import Path

from opportunity_lab.kalshi_depth_replenishment import init as init_depth
from opportunity_lab.kalshi_targeted_trade_liquidity import PROTOCOL, init, candidate, ingest, poll, status
from opportunity_lab.kalshi_demo_market_data import DemoMarkets


class TargetedTradeTests(unittest.TestCase):
    def setUp(self):
        self.db=sqlite3.connect(':memory:')
        self.addCleanup(self.db.close)
        init_depth(self.db,now=100)
        init(self.db,100)
        self.db.execute('CREATE TABLE v12_shadow_signals(id INTEGER PRIMARY KEY,ticker TEXT,observed_at REAL,outcome TEXT,price_cents INTEGER,detail TEXT,event_id TEXT)')
        self.db.execute('INSERT INTO depth_latest VALUES(?,?,?)',('M-1',390,json.dumps({'event_id':'E-1'})))
        self.db.execute('INSERT INTO v12_shadow_signals VALUES(?,?,?,?,?,?,?)',(1,'M-1',380,'yes',40,'{}','E-1'))
    def trade(self,identity,at=350,**changes):
        return {'trade_id':identity,'ticker':'M-1',
                'created_time':datetime.fromtimestamp(at,timezone.utc).isoformat(),
                'count_fp':'2','yes_price_dollars':'.40','no_price_dollars':'.60',
                'is_block_trade':False,**changes}
    def test_two_page_ticker_window_records_only_public_evidence(self):
        calls=[]
        def get_targeted_trades(*,params):
            calls.append(params)
            return ({'trades':[self.trade('a' if len(calls)==1 else 'b')],
                     'cursor':'next' if len(calls)==1 else ''},400,401)
        poll(self.db,SimpleNamespace(get_targeted_trades=get_targeted_trades),400)
        self.assertEqual(len(calls),2)
        self.assertEqual(calls[0]['ticker'],'M-1')
        self.assertEqual(calls[1]['cursor'],'next')
        result=status(self.db)
        self.assertEqual(result['public_trades'],2)
        self.assertEqual(result['onbook_trades'],2)
        self.assertTrue(result['latest_poll']['ticker_window_complete'])
        self.assertFalse(result['continuous_coverage'])
        self.assertEqual(result['actual_owned_fills'],0)
        self.assertFalse(result['promotion_ready'])
    def test_stale_depth_or_signal_never_queries_market(self):
        calls=[]
        poll(self.db,SimpleNamespace(get_targeted_trades=lambda **kw:calls.append(kw)),2000)
        self.assertEqual(calls,[])
        self.assertEqual(status(self.db)['latest_poll']['state'],'no_fresh_candidate')
    def test_partial_page_and_wrong_ticker_fail_closed(self):
        calls=[]
        def get_targeted_trades(*,params):
            calls.append(params)
            return {'trades':[self.trade(str(len(calls)))],'cursor':'more'},400,401
        poll(self.db,SimpleNamespace(get_targeted_trades=get_targeted_trades),400)
        self.assertEqual(len(calls),2)
        self.assertFalse(status(self.db)['latest_poll']['ticker_window_complete'])
        with self.assertRaisesRegex(ValueError,'ticker_mismatch'):
            ingest(self.db,{'trades':[self.trade('bad',ticker='OTHER')],'cursor':''},
                   ticker='M-1',attempted_at=400,received_at=401)
    def test_frozen_protocol_and_bounded_demo_query(self):
        registration=Path(__file__).resolve().parents[1]/'configs/kalshi_targeted_trade_liquidity_v2_20261008/registration.json'
        self.assertEqual(json.loads(registration.read_text()),PROTOCOL)
        self.assertEqual(candidate(self.db,400),'M-1')
        client=DemoMarkets(); calls=[]
        client._read=lambda path,params:calls.append((path,params))
        client.get_targeted_trades(params={'limit':100,'min_ts':100,'max_ts':400,'ticker':'M-1'})
        self.assertEqual(calls[0][0],'/markets/trades')
        self.assertIn('demo-api',client.BASE)
        for change in ({'ticker':'../../live'},{'cursor':''},{'limit':1000},{'min_ts':401}):
            params={'limit':100,'min_ts':100,'max_ts':400,'ticker':'M-1',**change}
            with self.assertRaises(ValueError):client.get_targeted_trades(params=params)
        self.db.execute("UPDATE targeted_trade_protocol SET detail='{}'")
        with self.assertRaisesRegex(ValueError,'protocol_changed'):init(self.db,500)


if __name__=='__main__': unittest.main()
