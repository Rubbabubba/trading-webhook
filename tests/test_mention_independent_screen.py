import unittest
from research.mention_no_taker_20261016.collect import choose
from research.mention_no_taker_20261016.screen import entry,fee
from research.mention_source_audit_20261016.audit import necessary_cross,source_predicate
from decimal import Decimal
class MentionScreenTests(unittest.TestCase):
 def market(self,ticker="S-26JUL10-A",event="S-26JUL10"):
  return dict(ticker=ticker,event_ticker=event,market_type="binary",open_time="2026-07-01T00:00:00Z",close_time="2026-07-10T00:00:00Z",expected_expiration_time=None,occurrence_datetime=None)
 def test_outcome_and_final_volume_cannot_change_selection(self):
  a=self.market();b=self.market("S-26JUL10-B");c=self.market("T-26JUL11-X","T-26JUL11")
  before=choose([a,b,c]);a.update(result="no",volume_fp="999999",yes_bid_dollars=".9");b.update(result="yes",volume_fp="0")
  self.assertEqual(before,choose([c,b,a]));self.assertEqual(len(before),2)
 def test_exact_completed_minute_only(self):
  m=self.market();t=1783555200
  from research.mention_no_taker_20261016.collect import ts
  t=int(ts(m["close_time"])-86400)
  c=dict(end_period_ts=t,yes_bid=dict(close=".5"),yes_ask=dict(close=".52"))
  self.assertEqual(entry(m,[c])["status"],"conditional_candidate")
  self.assertEqual(entry(m,[dict(c,end_period_ts=t+60)])["status"],"missing_or_ambiguous_exact_completed_minute")
  self.assertEqual(entry(m,[c,c])["status"],"missing_or_ambiguous_exact_completed_minute")
 def test_ask_not_midpoint_and_fee_rounding(self):
  from research.mention_no_taker_20261016.collect import ts
  m=self.market();c=dict(end_period_ts=int(ts(m["close_time"])-86400),yes_bid=dict(close=".40"),yes_ask=dict(close=".44"))
  x=entry(m,[c]);self.assertEqual(x["no_ask_cents"],60);self.assertEqual(x["fee_cents"],2);self.assertEqual(fee(Decimal(".5")),2)
 def test_no_bid_trade_direction_counterexample(self):
  self.assertTrue(source_predicate(35,36,"yes"));self.assertFalse(necessary_cross(35,36,"yes"))
  self.assertFalse(source_predicate(35,34,"yes"));self.assertTrue(necessary_cross(35,34,"yes"))
  self.assertFalse(necessary_cross(35,34,"no"))
 def test_before_open_rejected(self):
  m=self.market();m["open_time"]=m["close_time"];self.assertEqual(entry(m,[])["status"],"not_open_at_decision")
if __name__=="__main__":unittest.main()
