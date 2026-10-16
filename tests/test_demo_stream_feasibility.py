import gzip,json,sqlite3
import pytest
from research.demo_stream_feasibility_20261016 import probe

def test_targets_are_existing_bounded_demo_sampling_window(tmp_path):
 p=tmp_path/"worker.sqlite3";c=sqlite3.connect(p);c.execute("create table settings(name,detail)");c.execute("insert into settings values(?,?)",("sampling_window",json.dumps({"tickers":["A-1","B-2.5"]})));c.commit();c.close()
 assert probe.target_list(p)==["A-1","B-2.5"]
 c=sqlite3.connect(p);c.execute("update settings set detail=?",(json.dumps({"tickers":["A"]*9}),));c.commit();c.close()
 with pytest.raises(ValueError,match="invalid_targets"):probe.target_list(p)

def test_public_only_and_owner_field_redaction():
 p={"type":"orderbook_delta","msg":{"client_order_id":"private","market_ticker":"A","delta_fp":"1"}}
 r=probe.public_record(p,1000);assert r["owner_fields_redacted"] and "client_order_id" not in r["payload"]["msg"]
 assert p["msg"]["client_order_id"]=="private"
 with pytest.raises(ValueError,match="unexpected_message_type"):probe.public_record({"type":"fill"},1000)

def test_capacity_stops_before_overflow_and_preserves_exact_prefix(tmp_path,monkeypatch):
 p=tmp_path/"raw.gz";s=probe.Sink(p);r=probe.public_record({"type":"trade","msg":{}},1000)
 s.add(r);before=s.bytes;monkeypatch.setattr(probe,"MAX_BYTES",before)
 with pytest.raises(ValueError,match="capture_capacity"):s.add(r)
 s.close();assert len(gzip.decompress(p.read_bytes()).splitlines())==1
 with pytest.raises(FileExistsError):probe.Sink(p)

def test_nan_clock_or_payload_is_not_serialized(tmp_path):
 s=probe.Sink(tmp_path/"bad.gz")
 with pytest.raises(ValueError):s.add(probe.public_record({"type":"trade"},float("nan")))
 s.close();assert s.messages==0
