"""Freeze a separate queue hypothesis before unused source prices are read."""
import json,hashlib
from pathlib import Path
from datetime import datetime,timezone
ROOT=Path(__file__).parent
catalog=json.loads((ROOT.parent/'receipt_depth_20261015/source-catalog.json').read_text())
spec={
 'id':'external_ask_markdown_settlement_v1_20261016',
 'registered_at':datetime.now(timezone.utc).isoformat(),
 'hypothesis':'A transient ask markdown of at least3c with an unchanged or stronger bid may offer a settlement-value entry; this is distinct from directional momentum and is not an inversion of its losses.',
 'source_grounding':'Existing Transient Ask Markdown Fill Test engineering queue; independent unvalidated hypothesis, not published profitability.',
 'files':catalog['files'][:6],
 'warmup_files':4,'decision_files':[5,6],
 'selection':'Chronological next two uninspected files; first four used only for snapshot/sequence/history initialization. No entries from inspected window. No outcome-driven extension.',
 'signal':{'grid_seconds':5,'lookback_seconds':10,'lookback_tolerance_seconds':2,'minimum_ask_drop_cents':3,'minimum_bid_change_cents':0,'maximum_spread_cents':3,'entry_cents':[10,90],'quote_age_seconds':1,'sides':['yes','no'],'ticker_attempt_cap':1,'event_attempt_cap':1,'eligible_event_cap':100},
 'entry':{'delay_seconds':1,'maximum_lateness_seconds':1,'minimum_displayed_contracts':1,'price_guard':'Delayed ask must not exceed decision ask and must remain10–90c','require_snapshot_initialized_unbroken_sequence':True},
 'outcome':{'type':'hold_to_official_settlement','fee_reserve_cents':5,'slippage_cents':1,'additional_stress_cents':[0,2,5],'unverified_or_unresolved':'Retain unknown; full-principal-loss stress; do not drop'},
 'metadata':'Official binary contract/event/rules and open/earliest close, occurrence, expected expiration required. Exclude entries outside trading windows; one earliest chronological attempt per official event even if entry fails. Modern metadata does not prove contemporaneous rule availability.',
 'gate':'Exploratory one-date diagnostic cannot qualify. Require100complete signals,40independent events,5families and positive stressed event-cluster95%lower bound on unused multi-date holdout, fee/source verification and authentic operational evidence before separately authorized Demo trial.',
 'execution_enabled':False,'fill_assumed':False,'actual_orders':0,'prior_failed_results_preserved':True,'no_optimization_on_inspected_data':True}
raw=json.dumps(spec,indent=2,allow_nan=False)
with (ROOT/'registration.json').open('x') as f:f.write(raw)
print(json.dumps({'registered_at':spec['registered_at'],'sha256':hashlib.sha256((ROOT/'registration.json').read_bytes()).hexdigest(),'new_files':2}))
