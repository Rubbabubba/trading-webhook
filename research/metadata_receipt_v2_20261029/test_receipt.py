import copy,json,unittest
from receipt import build,audit,BASE,digest

def fixture(**updates):
    m=dict(ticker='TEST-X',status='active',market_type='binary',rules_primary='Named resolution source',
           open_time='1970-01-01T00:00:00Z',close_time='1970-01-01T00:03:00Z')
    m.update(updates)
    raw=json.dumps({'market':m},indent=2).encode()
    return build(raw,ticker='TEST-X',url=BASE+'TEST-X',requested_at=90,received_at=91,http_status=200,http_age_seconds=0)

class Receipt(unittest.TestCase):
    def test_valid_status_never_grants_authentication_fees_queue_or_orders(self):
        r=audit(fixture(),ticker='TEST-X',decision_at=100)
        self.assertEqual(r['state'],'byte_integrity_and_causal_status_only')
        for k in ('execution_enabled','promotion_ready','source_authenticity_verified','fees_verified','queue_verified'):self.assertIs(r[k],False)
    def test_original_bytes_including_whitespace_retained(self):
        import base64
        r=fixture();self.assertIn(b'\n',base64.b64decode(r['raw_body_base64']))
    def test_old_missing_status_not_repaired(self):
        r=fixture();import base64
        m=json.loads(base64.b64decode(r['raw_body_base64']));del m['market']['status']
        r=build(json.dumps(m).encode(),ticker='TEST-X',url=BASE+'TEST-X',requested_at=90,received_at=91,http_status=200,http_age_seconds=0)
        self.assertEqual(audit(r,ticker='TEST-X',decision_at=100)['reason'],'status_absent')
    def test_inactive_stays_unavailable(self):self.assertEqual(audit(fixture(status='closed'),ticker='TEST-X',decision_at=100)['reason'],'status_not_active')
    def test_future_or_stale_decision(self):
        for t in (90,122,float('nan'),True):self.assertEqual(audit(fixture(),ticker='TEST-X',decision_at=t)['state'],'blocked')
    def test_raw_tamper(self):
        r=fixture();r['raw_body_base64']='e30=';r['receipt_sha256']=digest({k:v for k,v in r.items() if k!='receipt_sha256'})
        self.assertEqual(audit(r,ticker='TEST-X',decision_at=100)['reason'],'raw_body_hash')
    def test_receipt_tamper(self):
        r=fixture();r['received_at']=90;self.assertEqual(audit(r,ticker='TEST-X',decision_at=100)['reason'],'receipt_hash')
    def test_duplicate_or_nonfinite_json_rejected(self):
        for raw in (b'{"market":{},"market":{}}',b'{"market":{"ticker":"TEST-X","x":NaN}}',b'{"market":{"ticker":"TEST-X","x":1e999}}'):
            with self.assertRaises(ValueError):build(raw,ticker='TEST-X',url=BASE+'TEST-X',requested_at=90,received_at=91,http_status=200,http_age_seconds=0)
    def test_no_alias_or_query_or_live_endpoint(self):
        for url in ('https://api.elections.kalshi.com/trade-api/v2/markets/TEST-X',BASE+'TEST-X?x=1'):
            with self.assertRaises(ValueError):build(b'{}',ticker='TEST-X',url=url,requested_at=90,received_at=91,http_status=200,http_age_seconds=0)
    def test_bad_clock_transport_or_size(self):
        for change in ({'received_at':89},{'http_age_seconds':3},{'received_at':True},{'http_status':302},{'raw':b'x'*65537}):
            args=dict(raw=b'{"market":{"ticker":"TEST-X"}}',ticker='TEST-X',url=BASE+'TEST-X',requested_at=90,received_at=91,http_status=200,http_age_seconds=0);args.update(change)
            with self.assertRaises(ValueError):build(**args)
    def test_lifecycle_and_timezone(self):
        for m in ({'close_time':'1970-01-01T00:01:00Z'},{'open_time':'1970-01-01T00:00:00'}):self.assertEqual(audit(fixture(**m),ticker='TEST-X',decision_at=100)['state'],'blocked')
    def test_absent_rules_or_nonbinary(self):
        for m in ({'rules_primary':None},{'market_type':'scalar'}):self.assertEqual(audit(fixture(**m),ticker='TEST-X',decision_at=100)['state'],'blocked')
    def test_fee_field_presence_is_not_fee_verification(self):self.assertIs(audit(fixture(fee_multiplier=0),ticker='TEST-X',decision_at=100)['fees_verified'],False)
    def test_malformed_structures_fail_closed(self):
        for r in ({},[],None):self.assertEqual(audit(r,ticker='TEST-X',decision_at=100)['state'],'blocked')

if __name__=='__main__':unittest.main()
