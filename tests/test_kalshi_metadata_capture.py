import base64
import json
from pathlib import Path
import queue
import sqlite3
import tempfile
import time
from types import SimpleNamespace as NS
import unittest
from unittest.mock import patch
from opportunity_lab.kalshi_metadata_capture import MetadataCapture,envelope,BASE,SPEC
from opportunity_lab.kalshi_demo_market_data import DemoMarkets


class MetadataCaptureTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name);self.now=100.
        self.a=MetadataCapture(self.root,clock=lambda:self.now);self.a._open()
        self.raw=b'{ "market": {"ticker":"TEST-X","status":"active"} }'
        self.facts=dict(url=BASE+'TEST-X',method='GET',status=200,requested_at=100.,received_at=101.,age=None)

    def tearDown(self):
        if self.a.db:self.a.db.close()
        if self.a.lease:self.a.lease.close()
        self.tmp.cleanup()

    def store(self,**kw):
        facts={**self.facts,**kw};self.now=max(self.now,facts['received_at'])
        self.a._store(envelope(self.raw,**facts))

    def reopen(self):
        if self.a.db:self.a.db.close()
        self.a.lease.close();self.a=MetadataCapture(self.root,clock=lambda:self.now);self.a._open()

    def test_unknown_age_and_exact_entity_retained_without_pass(self):
        self.store();r=json.loads(self.a.db.execute('SELECT raw FROM receipts').fetchone()[0])
        self.assertEqual(base64.b64decode(r['entity_base64']),self.raw)
        self.assertIsNone(r['http_age_seconds']);self.assertEqual(r['freshness'],'http_age_unknown')
        self.assertFalse(self.a.status()['source_authenticity_verified']);self.assertFalse(r['fees_verified'])

    def test_stale_age_and_latency_are_retained_as_stale(self):
        self.store(age='4',received_at=105.)
        self.assertEqual(self.a.status()['latest_freshness'],'transport_not_fresh')

    def test_invalid_methods_hosts_query_and_identity_reject(self):
        for kw in ({'method':'POST'},{'url':BASE+'TEST-X?x=1'},{'url':BASE.replace('demo-api','api')+'TEST-X'},
                   {'status':500},{'received_at':99.},{'age':'NaN'},{'age':'-1'}):
            with self.assertRaises(ValueError):envelope(self.raw,**{**self.facts,**kw})
        with self.assertRaises(ValueError):envelope(b'{"market":{"ticker":"OTHER"}}',**self.facts)

    def test_body_capacity_and_duplicate_json_reject(self):
        for raw in (b'x'*65537,b'{"market":{},"market":{}}',b'{"market":{"ticker":"TEST-X","x":1e999}}'):
            with self.assertRaises(ValueError):envelope(raw,**self.facts)

    def test_registration_prevents_cached_history_and_future_receipt(self):
        with self.assertRaisesRegex(ValueError,'pre_registration'):self.a._store(envelope(self.raw,**{**self.facts,'requested_at':99.}))
        with self.assertRaisesRegex(ValueError,'future_receipt'):self.a._store(envelope(self.raw,**self.facts))
        self.assertEqual(self.a.status()['receipts'],0)

    def test_restart_keeps_start_and_deduplicates(self):
        self.store();self.reopen();self.store()
        self.assertEqual(self.a.status()['receipts'],1);self.assertEqual(self.a.started_at,100.)
        self.assertEqual(self.a.status()['age_unknown'],1)

    def test_duration_stop_is_durable_without_rollover(self):
        self.now=100.+SPEC['duration_seconds'];self.a._store(envelope(self.raw,**self.facts))
        self.assertEqual(self.a.status()['stop_reason'],'duration_cap');self.reopen()
        self.assertEqual(self.a.status()['stop_reason'],'duration_cap');self.assertIsNone(self.a.db)

    def test_physical_floor_stop_does_not_resume(self):
        self.a.disk_usage=lambda _:NS(free=1);self.store()
        self.assertEqual(self.a.status()['stop_reason'],'physical_free_space_floor');self.reopen()
        self.assertEqual(self.a.status()['stop_reason'],'physical_free_space_floor')

    def test_receipt_and_payload_caps_stop_without_extra_write(self):
        self.a.view['receipts']=SPEC['maximum_receipts'];self.store()
        self.assertEqual(self.a.status()['stop_reason'],'receipt_cap')
        self.assertEqual(self.a.db.execute('SELECT count(*) FROM receipts').fetchone()[0],0)

    def test_payload_cap(self):
        self.a.view['payload_bytes']=SPEC['maximum_payload_bytes'];self.store()
        self.assertEqual(self.a.status()['stop_reason'],'payload_cap')

    def test_writer_conflict_does_not_modify_registration_or_archive(self):
        before=self.a.path.read_bytes();reg=self.a.registration.read_bytes()
        other=MetadataCapture(self.root,clock=lambda:100.)
        try:
            with self.assertRaisesRegex(RuntimeError,'already_owned'):other._open()
        finally:
            if other.lease:other.lease.close()
        self.assertEqual(before,self.a.path.read_bytes());self.assertEqual(reg,self.a.registration.read_bytes())

    def test_sqlite_full_has_durable_external_stop(self):
        real=self.a.db
        class Full:
            def execute(self,*a):
                if a[0].startswith('SELECT'):return real.execute(*a)
                raise sqlite3.OperationalError('database or disk is full')
            def __enter__(self):return self
            def __exit__(self,*a):return False
        self.a.db=Full()
        with self.assertRaises(sqlite3.DatabaseError):self.store()
        self.a.db=real;self.assertEqual(self.a.status()['stop_reason'],'sqlite_storage_fault')

    def test_admission_never_waits_for_disk_and_queue_is_bounded(self):
        for _ in range(SPEC['queue_slots']+1):self.a.submit(self.raw,**self.facts)
        self.assertEqual(self.a.q.qsize(),8);self.assertEqual(self.a.status()['queue_dropped'],1)
        self.a.submit(b'x'*65537,**self.facts);self.assertEqual(self.a.status()['rejected'],1)

    def test_existing_reader_only_one_open_and_same_data_with_unknown_age(self):
        a=self.a;raw=self.raw
        class Response:
            headers={};status=200
            def __enter__(self):return self
            def __exit__(self,*a):return False
            def read(self):return raw
            def geturl(self):return BASE+'TEST-X'
        client=DemoMarkets();client.metadata_capture=a
        with patch.object(client.opener,'open',return_value=Response()) as opened,patch('time.sleep'):
            data,_,_=client.get('TEST-X')
        self.assertEqual(data['market']['ticker'],'TEST-X');self.assertEqual(opened.call_count,1)
        _,facts=a.q.get_nowait();self.assertIsNone(facts['age'])

    def test_observer_failure_does_not_change_existing_reader(self):
        class Broken:
            def submit(self,*a,**kw):raise RuntimeError('synthetic')
        class Response:
            headers={};status=200
            def __enter__(self):return self
            def __exit__(self,*a):return False
            def read(self):return b'{"market":{"ticker":"TEST-X"}}'
            def geturl(self):return BASE+'TEST-X'
        client=DemoMarkets();client.metadata_capture=Broken()
        with patch.object(client.opener,'open',return_value=Response()),patch('time.sleep'):
            self.assertEqual(client.get('TEST-X')[0]['market']['ticker'],'TEST-X')

    def test_actual_background_writer_and_status_queue_are_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            service=MetadataCapture(directory).start()
            try:
                deadline=time.monotonic()+3
                while service.status()['state']=='starting' and time.monotonic()<deadline:time.sleep(.01)
                self.assertEqual(service.status()['state'],'collecting')
                now=time.time();facts={**self.facts,'requested_at':now,'received_at':now}
                service.submit(self.raw,**facts)
                while service.status()['receipts']==0 and time.monotonic()<deadline:time.sleep(.01)
                self.assertEqual(service.status()['receipts'],1)
                self.assertEqual(service.status()['pending'],0)
                self.assertFalse(service.status()['execution_enabled'])
            finally:service.close()
            self.assertFalse(service.thread.is_alive())


if __name__=='__main__':unittest.main()
