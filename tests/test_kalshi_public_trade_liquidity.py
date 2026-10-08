from datetime import datetime,timezone
import json,sqlite3,unittest
from types import SimpleNamespace
from opportunity_lab.kalshi_public_trade_liquidity import init,ingest,poll,status
from opportunity_lab.kalshi_depth_replenishment import init as init_depth

class PublicTradeProbeTests(unittest.TestCase):
    def setUp(self):
        self.db=sqlite3.connect(':memory:');self.addCleanup(self.db.close)
        init_depth(self.db,now=100);init(self.db,100)
        self.db.execute("INSERT INTO depth_latest VALUES('M',150,?)",(json.dumps({'event_id':'E'}),))
    def trade(self,identity='t1',at=150,**change):
        return {'trade_id':identity,'ticker':'M','created_time':datetime.fromtimestamp(at,timezone.utc).isoformat(),
                'count_fp':'2.00','yes_price_dollars':'.40','no_price_dollars':'.60',
                'is_block_trade':False,'taker_outcome_side':'yes','taker_book_side':'ask',**change}
    def test_executed_public_trades_are_not_owned_fills_or_cancellation_proof(self):
        receipt=ingest(self.db,{'trades':[self.trade()],'cursor':''},attempted_at=200,received_at=201)
        self.assertTrue(receipt['page_complete'])
        result=status(self.db)
        self.assertEqual(result['public_trades'],1);self.assertEqual(result['onbook_trades'],1)
        self.assertEqual(result['matched_depth_events'],1)
        self.assertEqual(result['actual_owned_fills'],0)
        self.assertFalse(result['fill_assumed']);self.assertFalse(result['promotion_ready'])
        self.assertEqual(result['trade_cancel_attribution'],'unresolved')
    def test_partial_pages_and_unknown_block_flag_cannot_certify_absence_or_onbook(self):
        trade=self.trade();trade.pop('is_block_trade')
        r=ingest(self.db,{'trades':[trade],'cursor':'more'},attempted_at=200,received_at=201)
        self.assertFalse(r['page_complete']);self.assertFalse(r['continuous_coverage'])
        self.assertEqual(status(self.db)['onbook_trades'],0)
    def test_pre_registration_discard_future_and_malformed_are_rejected(self):
        r=ingest(self.db,{'trades':[self.trade(at=90)],'cursor':''},attempted_at=200,received_at=201)
        self.assertEqual(r['discarded_pre_registration_or_window'],1)
        for trade in (self.trade(at=202),self.trade(count_fp='NaN'),self.trade(created_time='2026-01-01')):
            with self.assertRaises((ValueError,ArithmeticError)):
                ingest(self.db,{'trades':[trade],'cursor':''},attempted_at=200,received_at=201)
        self.assertEqual(status(self.db)['public_trades'],0)
    def test_dedup_conflicts_append_only_and_frozen_protocol(self):
        payload={'trades':[self.trade()],'cursor':''}
        ingest(self.db,payload,attempted_at=200,received_at=201)
        ingest(self.db,payload,attempted_at=250,received_at=251)
        self.assertEqual(status(self.db)['public_trades'],1)
        with self.assertRaisesRegex(ValueError,'conflicting_trade_id'):
            ingest(self.db,{'trades':[self.trade(count_fp='3')],'cursor':''},attempted_at=250,received_at=251)
        with self.assertRaises(sqlite3.IntegrityError): self.db.execute('DELETE FROM liquidity_public_trades')
        self.db.execute("UPDATE liquidity_trade_protocol SET detail='{}'")
        with self.assertRaisesRegex(ValueError,'protocol_changed'):init(self.db,300)
    def test_http_failure_is_visible_and_does_not_spin_or_infer_zero_liquidity(self):
        calls=[]
        def get_trades(**kw):calls.append(kw);raise ValueError('demo_market_http_401')
        markets=SimpleNamespace(get_trades=get_trades)
        poll(self.db,markets,200);poll(self.db,markets,201)
        result=status(self.db)
        self.assertEqual(len(calls),1)
        self.assertEqual(result['latest_poll']['state'],'blocked')
        self.assertEqual(result['latest_poll']['error_code'],'demo_market_http_401')
        self.assertFalse(result['continuous_coverage'])
    def test_probe_endpoint_is_fixed_and_bounded(self):
        from opportunity_lab.kalshi_demo_market_data import DemoMarkets
        client=DemoMarkets();calls=[]
        client._read=lambda path,params:calls.append((path,params))
        client.get_trades(params={'limit':100,'min_ts':100,'max_ts':200})
        self.assertEqual(calls[0][0],'/markets/trades')
        self.assertIn('demo-api',client.BASE)
        for params in ({'limit':1000,'min_ts':100,'max_ts':200},
                       {'limit':100,'min_ts':300,'max_ts':200},
                       {'limit':100,'min_ts':True,'max_ts':200}):
            with self.assertRaises(ValueError):client.get_trades(params=params)
