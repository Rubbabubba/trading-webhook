import copy
import pytest
from opportunity_lab import kalshi_demo_v5_maker_worker as worker

class State:
    def __init__(self):self.data={};self.actions=[]
    def load(self,key,default=None):return copy.deepcopy(self.data.get(key,default))
    def save(self,key,value):self.data[key]=copy.deepcopy(value)
    def record(self,ticker,detail):self.actions.append((ticker,copy.deepcopy(detail)))

def setup(count=18):
    state=State();cohort=[{"ticker":f"T{i:02d}","event_ticker":f"E{i:02d}"} for i in range(count)]
    window=worker.sampling_window(state,cohort,now=1000)
    return state,cohort,window

def fail(state,ticker,at=1020):
    worker.record_sampling_book_health(state,ticker,usable=False,now=at,reason="missing_book")

def test_only_wholly_repeated_unusable_window_advances_and_retains_universe():
    state,cohort,window=setup();original=copy.deepcopy(cohort)
    for m in window:fail(state,m["ticker"])
    assert worker.sampling_window(state,cohort,now=1040)==window
    for m in window[:-1]:fail(state,m["ticker"],1041)
    assert worker.sampling_window(state,cohort,now=1042)==window
    fail(state,window[-1]["ticker"],1043)
    nxt=worker.sampling_window(state,cohort,now=1044)
    assert {m["ticker"] for m in window}.isdisjoint({m["ticker"] for m in nxt})
    assert cohort==original
    receipts=[d for _,d in state.actions if d["action"]=="sampling_window_unusable_advanced"]
    assert len(receipts)==1 and receipts[0]["cohort_members_removed"]==0
    # Normal rotation eventually revisits every original unusable event.
    again=worker.sampling_window(state,cohort,now=3000)
    later=worker.sampling_window(state,cohort,now=4000)
    assert {m["ticker"] for m in window}<={m["ticker"] for m in again+later}

def test_success_resets_member_and_keeps_observation_window_stable():
    state,cohort,window=setup()
    for m in window:
        fail(state,m["ticker"]);fail(state,m["ticker"],1021)
        if m==window[0]:worker.record_sampling_book_health(state,m["ticker"],usable=True,now=1022)
    assert worker.sampling_window(state,cohort,now=1030)==window
    fail(state,window[0]["ticker"],1031)
    assert worker.sampling_window(state,cohort,now=1032)==window
    fail(state,window[0]["ticker"],1033)
    assert worker.sampling_window(state,cohort,now=1034)!=window

def test_old_window_or_outside_member_cannot_abandon_new_window():
    state,cohort,window=setup()
    for m in window:fail(state,m["ticker"]);fail(state,m["ticker"],1021)
    nxt=worker.sampling_window(state,cohort,now=1022)
    for m in window:fail(state,m["ticker"],1023)
    assert worker.sampling_window(state,cohort,now=1024)==nxt
    assert state.data["sampling_window_offset"]==16

def test_single_window_cohort_does_not_fast_rotate_back_to_itself():
    state,cohort,window=setup(4)
    for m in window:fail(state,m["ticker"]);fail(state,m["ticker"],1021)
    assert worker.sampling_window(state,cohort,now=1022)==window
    assert "abandoned_at" not in state.data["sampling_window_book_health"]

def test_real_scan_skip_records_unusable_without_order_or_observation(monkeypatch):
    state,cohort,window=setup()
    class Markets:
        def quote(self,payload):raise ValueError("missing_book")
    result=worker.scan_market_candidate(state,object(),Markets(),window[0])
    assert result==(None,None)
    assert state.data["sampling_window_book_health"]["failures"][window[0]["ticker"]]["consecutive"]==1
    assert state.actions[-1][1]=={"action":"scan_skip","reason":"missing_book"}

def test_unexpected_failure_propagates_without_counting_lifecycle_failure():
    state,cohort,window=setup()
    class Markets:
        def quote(self,payload):raise ValueError("transport_clock_invalid")
    with pytest.raises(ValueError,match="transport_clock_invalid"):
        worker.scan_market_candidate(state,object(),Markets(),window[0])
    assert "sampling_window_book_health" not in state.data


def test_http_success_with_invalid_empty_book_does_not_reset_consecutive_failures(monkeypatch):
    state,cohort,window=setup()
    class Markets:
        def quote(self,payload):return {"observed_at":1000}
    monkeypatch.setattr(worker,"one_contract_frame",lambda q,**kw:dict(received_at=1000))
    def invalid(*a,**kw):raise ValueError("missing_book")
    monkeypatch.setattr(worker,"observe_frame",invalid)
    for _ in range(2):
        assert worker.scan_market_candidate(state,object(),Markets(),window[0])==(None,None)
    assert state.data["sampling_window_book_health"]["failures"][window[0]["ticker"]]["consecutive"]==2
