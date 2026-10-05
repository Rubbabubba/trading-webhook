from datetime import datetime, timedelta, timezone
import hashlib
import json
import sqlite3

from opportunity_lab.kalshi_experiment_registry import register, status


def _idea():
    frozen = {"capability_id": "bea_gdp_release_quote_v1", "version": 1, "spec": {}}
    digest = hashlib.sha256(json.dumps(frozen, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return {"id": "12345678-1234-1234-1234-123456789abc",
            "capability_id": "bea_gdp_release_quote_v1", "version": 1,
            "spec_hash": digest, "spec": {}}


def test_registered_bea_experiment_only_counts_future_release():
    db = sqlite3.connect(":memory:")
    at = datetime(2026, 10, 4, tzinfo=timezone.utc)
    idea = _idea()
    assert register(db, [idea], now=at) == 1
    assert register(db, [idea], now=at + timedelta(days=1)) == 0
    release = {"first_publication_observed_at": (at - timedelta(days=1)).isoformat(),
               "first_publication_source_url": "https://www.bea.gov/news/2026/old",
               "shadow_screen": {"release_events_observed": 1}}
    first = status(db, {"candidates": []}, release)["experiments"][0]
    assert first["state"] == "awaiting_future_release"
    assert first["independent_events"] == 0
    release["first_publication_observed_at"] = (at + timedelta(days=25)).isoformat()
    later = status(db, {"candidates": []}, release)["experiments"][0]
    assert later["state"] == "shadow"
    assert later["independent_events"] == 1
    assert later["orders_enabled"] is False


def test_registry_rejects_unregistered_grammar_and_hash():
    db = sqlite3.connect(":memory:")
    idea = _idea()
    assert register(db, [{**idea, "spec_hash": "0" * 64}]) == 0
    assert register(db, [{**idea, "spec": {"execute": "live"}}]) == 0
    assert status(db, {"candidates": []}, {})["experiments"] == []


def test_nested_spread_counts_only_future_independent_depth_and_never_fills():
    db=sqlite3.connect(':memory:');at=datetime(2026,10,5,tzinfo=timezone.utc)
    idea=_idea(); frozen={'capability_id':'nested_threshold_spread_quote_v1','version':1,'spec':{}}
    idea.update(frozen);idea['spec_hash']=hashlib.sha256(json.dumps(frozen,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    assert register(db,[idea],now=at)==1
    db.execute('CREATE TABLE structural_signals(signal_id TEXT,event_id TEXT,bucket INTEGER,observed_at TEXT,detail TEXT)')
    good={'fill_assumed':False,'execution_enabled':False,'fully_executable_snapshot':True}
    for index,(event,when,value) in enumerate([
        ('old',at-timedelta(seconds=1),good),('new',at+timedelta(seconds=1),good),
        ('new',at+timedelta(seconds=2),good),('invalid',at+timedelta(seconds=3),{**good,'fill_assumed':True})]):
        db.execute('INSERT INTO structural_signals VALUES(?,?,?,?,?)',(str(index),event,index,when.isoformat(),json.dumps(value)))
    row=status(db,{'candidates':[]},{})['experiments'][0]
    assert row['independent_events']==1
    assert row['state']=='shadow'
    assert row['orders_enabled'] is False
    assert 'not_filled_profit' in row['evidence_ref']
