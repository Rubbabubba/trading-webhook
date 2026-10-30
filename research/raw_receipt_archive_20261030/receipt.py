"""Offline metadata receipt integrity only. No requests, orders or authority."""
import base64
import hashlib
import json
import math
import re
from datetime import datetime

SCHEMA='kalshi_decoded_status_receipt_v2_20261029'
BASE='https://demo-api.kalshi.co/trade-api/v2/markets/'

def canonical(x):return json.dumps(x,sort_keys=True,separators=(',',':'),allow_nan=False)
def digest(x):return hashlib.sha256(canonical(x).encode()).hexdigest()
def unique(pairs):
    result={}
    for k,v in pairs:
        if k in result:raise ValueError('duplicate_json_key')
        result[k]=v
    return result
def decode(raw):
    def bad_constant(x):raise ValueError('nonfinite_json')
    def finite_float(x):
        v=float(x)
        if not math.isfinite(v):raise ValueError('nonfinite_json')
        return v
    return json.loads(raw.decode('utf-8'),object_pairs_hook=unique,parse_constant=bad_constant,parse_float=finite_float)
def number(x):return type(x) in (int,float) and math.isfinite(x)

def build(raw,*,ticker,url,requested_at,received_at,http_status,http_age_seconds):
    """Wrap caller-provided HTTP bytes; this does not authenticate their origin."""
    if not isinstance(raw,bytes) or not 0<len(raw)<=65536:raise ValueError('bounded_raw_bytes_required')
    if not isinstance(ticker,str) or not re.fullmatch('[A-Z0-9_.-]{1,160}',ticker):raise ValueError('ticker_required')
    if url!=BASE+ticker:raise ValueError('exact_demo_market_endpoint_required')
    if http_status!=200 or type(http_status) is not int:raise ValueError('successful_http_required')
    if not all(number(x) for x in (requested_at,received_at,http_age_seconds)):raise ValueError('finite_clocks_required')
    if not 0<=received_at-requested_at<=2 or not 0<=http_age_seconds<=2:raise ValueError('fresh_transport_required')
    value=decode(raw)
    if not isinstance(value,dict) or not isinstance(value.get('market'),dict):raise ValueError('market_object_required')
    market=value['market']
    if market.get('ticker')!=ticker:raise ValueError('market_identity_mismatch')
    # Preserve the original body, including absent/unknown status and fee fields.
    result=dict(schema=SCHEMA,ticker=ticker,url=url,requested_at=requested_at,
                received_at=received_at,http_status=http_status,http_age_seconds=http_age_seconds,
                raw_body_base64=base64.b64encode(raw).decode(),raw_sha256=hashlib.sha256(raw).hexdigest())
    result['receipt_sha256']=digest(result)
    return result

def audit(receipt,*,ticker,decision_at):
    out=dict(schema=SCHEMA,execution_enabled=False,promotion_ready=False,
             source_authenticity_verified=False,fees_verified=False,queue_verified=False)
    def blocked(reason):return {**out,'state':'blocked','reason':reason}
    try:
        if not isinstance(receipt,dict) or len(canonical(receipt).encode())>90000:return blocked('invalid_receipt')
        if receipt.get('schema')!=SCHEMA or receipt.get('ticker')!=ticker:return blocked('receipt_identity')
        if digest({k:v for k,v in receipt.items() if k!='receipt_sha256'})!=receipt.get('receipt_sha256'):return blocked('receipt_hash')
        raw=base64.b64decode(receipt['raw_body_base64'],validate=True)
        if hashlib.sha256(raw).hexdigest()!=receipt['raw_sha256']:return blocked('raw_body_hash')
        rebuilt=build(raw,**{k:receipt[k] for k in ('ticker','url','requested_at','received_at','http_status','http_age_seconds')})
        if rebuilt!=receipt:return blocked('receipt_shape')
        if not number(decision_at) or not 0<=decision_at-receipt['received_at']<=30:return blocked('decision_receipt_age')
        market=decode(raw)['market']
        if 'status' not in market:return blocked('status_absent')
        if market['status']!='active':return blocked('status_not_active')
        if market.get('market_type')!='binary':return blocked('market_not_binary')
        if not any(market.get(k) for k in ('rules_primary','rules_secondary')):return blocked('rules_absent')
        def stamp(x):
            v=datetime.fromisoformat(x.replace('Z','+00:00'))
            if v.tzinfo is None:raise ValueError('timezone_required')
            return v.timestamp()
        opened=stamp(market['open_time'])
        closed=min(stamp(market[k]) for k in ('close_time','expected_expiration_time') if market.get(k))
        if not opened<=decision_at<closed:return blocked('outside_observed_lifecycle')
        return {**out,'state':'byte_integrity_and_causal_status_only','observed_status':market['status'],
                'raw_sha256':receipt['raw_sha256'],'receipt_sha256':receipt['receipt_sha256'],
                'reason':'caller_bytes_and_hashes_do_not_authenticate_venue_or_fees_or_queue'}
    except (ValueError,TypeError,KeyError,OverflowError,AttributeError):return blocked('invalid_receipt')
