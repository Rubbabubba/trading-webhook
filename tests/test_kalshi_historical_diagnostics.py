import hashlib,json,sqlite3,tempfile,unittest,zlib
import gc
from pathlib import Path
from opportunity_lab.kalshi_historical_diagnostics import collect,digest,encoded,fault_receipt

class HistoryCollector(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        with sqlite3.connect(self.root/'stream-source-v5.sqlite3') as db:
            db.executescript('CREATE TABLE protocol(id,name);INSERT INTO protocol VALUES(1,"synthetic_protocol");CREATE TABLE connections(started,ended,reason);CREATE TABLE messages(id,received,chain_sha);INSERT INTO messages VALUES(1,2,"synthetic_chain");INSERT INTO connections VALUES(1,2,"source_or_storage_fault")')
        with sqlite3.connect(self.root/'stream-shadow-comparison-v4.sqlite3') as db:
            db.executescript('CREATE TABLE outcome_attempts(id,signal_id,horizon,started_at);CREATE TABLE outcome_receipts(attempt_id,received_at,raw_sha,raw_z,result);CREATE TABLE high_edge_v15_signals(id,at);CREATE TABLE high_edge_v15_marks(signal_id,horizon,at);INSERT INTO high_edge_v15_signals VALUES(1,100);INSERT INTO high_edge_v15_marks VALUES(1,5,106)')
            raw=b'{"synthetic":"receipt"}'
            for i,h,at in ((1,5,105),(2,300,400)):
                db.execute('INSERT INTO outcome_attempts VALUES(?,?,?,?)',(i,1,h,at))
                db.execute('INSERT INTO outcome_receipts VALUES(?,?,?,?,?)',(i,at+.1,hashlib.sha256(raw).hexdigest(),zlib.compress(raw),json.dumps({'result':'rejected_missing_book' if i==2 else 'valid_quote'})))
        with sqlite3.connect(self.root/'journal.sqlite3') as db:
            db.executescript('CREATE TABLE intents(id,payload,state,filled);CREATE TABLE economic_intents(id,detail);CREATE TABLE broker_evidence(client_id,detail)')
            db.execute('INSERT INTO intents VALUES(?,?,?,?)',('v12-fill-1',json.dumps({'client_order_id':'v12-fill-1'}),'terminal',0))
            db.execute('INSERT INTO economic_intents VALUES(?,?)',('v12-fill-1',json.dumps({'order_mode':'post_only_gtc'})))
            db.execute('INSERT INTO broker_evidence VALUES(?,?)',('v12-fill-1',json.dumps({'order':{'status':'canceled'},'fills':[]})))
        with sqlite3.connect(self.root/'worker.sqlite3') as db:
            db.executescript('CREATE TABLE intent_meta(client_id,event_id,ticker,created_at);CREATE TABLE queue_records(client_id);INSERT INTO intent_meta VALUES("v12-fill-1","event","FAMILY-TICKER",1)')
    def tearDown(self):gc.collect();self.temp.cleanup()
    def test_full_read_only_receipt_retains_missing_marks_and_queue_gaps(self):
        before={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in self.root.iterdir()}
        value=collect(self.root)
        self.assertTrue(all(value[k]['state']=='observed' for k in ('source','outcomes','trial')))
        self.assertEqual(value['trial']['facts']['orders_with_queue_records'],0)
        self.assertEqual(value['outcomes']['facts']['hashed_responses'],2)
        self.assertEqual(value['outcomes']['facts']['response_results']['rejected_missing_book'],1)
        self.assertEqual(value['receipt_sha256'],digest({k:v for k,v in value.items() if k!='receipt_sha256'}))
        self.assertEqual(before,{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in self.root.iterdir()})
    def test_corrupt_raw_hash_is_unavailable_not_fabricated(self):
        with sqlite3.connect(self.root/'stream-shadow-comparison-v4.sqlite3') as db:db.execute('UPDATE outcome_receipts SET raw_sha="wrong" WHERE attempt_id=1')
        value=collect(self.root);self.assertEqual(value['outcomes']['state'],'unavailable');self.assertEqual(value['outcomes']['error_code'],'outcome_hash_mismatch')
    def test_missing_broker_record_is_explicit(self):
        with sqlite3.connect(self.root/'journal.sqlite3') as db:db.execute('DELETE FROM broker_evidence')
        self.assertEqual(collect(self.root)['trial']['facts']['missing_broker_records'],1)
    def test_unknown_files_never_created(self):
        empty=self.root/'empty';empty.mkdir();value=collect(empty)
        self.assertTrue(all(value[k]['state']=='unavailable' for k in ('source','outcomes','trial')));self.assertEqual(list(empty.iterdir()),[])
    def test_oversized_decompression_is_rejected(self):
        with sqlite3.connect(self.root/'stream-shadow-comparison-v4.sqlite3') as db:db.execute('UPDATE outcome_receipts SET raw_z=? WHERE attempt_id=1',(zlib.compress(b'x'*300000),))
        self.assertEqual(collect(self.root)['outcomes']['error_code'],'outcome_decompression_capacity')
    def test_new_fault_receipt_redacts_arbitrary_exception_message(self):
        result=fault_receipt(ValueError('secret-like-body-never-export'),123)
        self.assertNotIn('secret-like',encoded(result));self.assertEqual(result['code'],'detail_not_allowlisted');self.assertIn('not a reconstructed',result['scope'])
