"""Bounded public trade reads, immutable cache, explicit completeness."""
import hashlib, json, math, time, urllib.request, urllib.error
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from research.v15_delayed_ioc_20261016.trade_contract import validate

BASE='https://external-api.kalshi.com/trade-api/v2/historical/trades'


class Tape:
    def __init__(self,cache,reader=None):
        self.cache=Path(cache);self.cache.mkdir(parents=True,exist_ok=True)
        self.reader=reader;self.requests=0;self.next_at=0

    def page(self,params):
        url=BASE+'?'+urlencode(params)
        key=hashlib.sha256(url.encode()).hexdigest()
        path=self.cache/(key+'.json')
        if path.exists():
            record=json.loads(path.read_text())
            if record['url']!=url:raise ValueError('tape_cache_request_changed')
            body=self.cache/(key+'.body')
            if record.get('raw_sha256') and hashlib.sha256(body.read_bytes()).hexdigest()!=record['raw_sha256']:
                raise ValueError('tape_cache_hash_changed')
            return record
        if self.requests>=300:raise ValueError('tape_request_budget')
        self.requests+=1
        record=dict(url=url,captured_at=datetime.now(timezone.utc).isoformat())
        try:
            if self.reader:
                payload=self.reader(params);raw=json.dumps(payload,allow_nan=False).encode()
            else:
                time.sleep(max(0,self.next_at-time.monotonic()))
                with urllib.request.urlopen(url,timeout=20) as response:raw=response.read(4000001)
                self.next_at=time.monotonic()+.5
            body=self.cache/(key+'.body')
            with body.open('xb') as f:f.write(raw)
            record.update(raw_sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw))
            if len(raw)>4000000:raise ValueError('tape_response_capacity')
            record.update(status=200,payload=json.loads(raw))
        except (ValueError,OSError,urllib.error.URLError) as error:
            record.update(status='blocked',error=type(error).__name__,http_status=getattr(error,'code',None))
        with path.open('x') as f:json.dump(record,f,indent=2,allow_nan=False)
        return record

    def fetch(self,attempt):
        minimum=math.floor(attempt['activation_at'])-1
        maximum=math.ceil(attempt['cancel_ack_at']+1)
        cursor=None;seen_cursors=set();rows={};pages=[];complete=False;error=None
        try:
            for unused in range(3):
                params=dict(ticker=attempt['ticker'],min_ts=minimum,max_ts=maximum,
                            limit=1000,is_block_trade='false')
                if cursor:params['cursor']=cursor
                record=self.page(params);pages.append({k:v for k,v in record.items() if k!='payload'})
                if record.get('status')!=200:raise ValueError('tape_page_failed')
                payload=record['payload'];trades=payload.get('trades');cursor=payload.get('cursor')
                if (not isinstance(trades,list) or len(trades)>1000
                        or not isinstance(cursor,str) or len(cursor)>2000):
                    raise ValueError('tape_page_contract')
                for raw in trades:
                    row=validate(raw,minimum,maximum)
                    if row['ticker']!=attempt['ticker']:raise ValueError('tape_ticker_mismatch')
                    old=rows.get(row['trade_id'])
                    if old is not None and old!=row:raise ValueError('conflicting_trade_identity')
                    rows[row['trade_id']]=row
                if not cursor:complete=True;break
                if cursor in seen_cursors:raise ValueError('pagination_cursor_repeated')
                seen_cursors.add(cursor)
        except (ValueError,KeyError,TypeError,ArithmeticError,OSError) as e:
            error=type(e).__name__+': '+str(e)
        if not complete and error is None:error='page_capacity_incomplete'
        return dict(complete=complete,error=error,pages=pages,trades=list(rows.values()),
                    minimum=minimum,maximum=maximum,owned_fills=0)
