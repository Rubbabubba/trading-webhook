"""Unauthenticated demo-only market reads with bounded freshness checks."""
from datetime import datetime,timezone
import json
import math
import time
from urllib.parse import urlencode,quote
from urllib.request import Request,build_opener
from urllib.error import HTTPError
from .kalshi_account_monitor import NoRedirect


class DemoMarkets:
    BASE='https://demo-api.kalshi.co/trade-api/v2'
    def __init__(self):self.next_at=0;self.opener=build_opener(NoRedirect())
    def get(self,ticker=None,*,book=False,params=None):
        if book and ticker is None:raise ValueError('ticker_required')
        path='/markets'+('/'+quote(ticker,safe='') if ticker else '')+('/orderbook' if book else '')
        time.sleep(max(0,self.next_at-time.monotonic()));start=datetime.now(timezone.utc).timestamp()
        try:
            with self.opener.open(Request(self.BASE+path+('?' + urlencode(params) if params else '')),timeout=8) as response:
                data=json.load(response);age=float(response.headers.get('Age','0'))
            end=datetime.now(timezone.utc).timestamp()
            if not math.isfinite(age) or not 0<=age<=2 or not 0<=end-start<=2:
                raise ValueError('stale_demo_market_data')
            return data,start,end
        except HTTPError as exc:raise ValueError('demo_market_http_'+str(exc.code)) from None
        finally:self.next_at=time.monotonic()+2
    def quote(self,payload):
        market,_,_=self.get(payload['ticker']);m=market['market']
        if m.get('ticker')!=payload['ticker'] or m.get('status')!='active' or m.get('market_type')!='binary':
            raise ValueError('demo_market_not_active_binary')
        data,start,end=self.get(payload['ticker'],book=True,params={'depth':20})
        return dict(data,environment='demo',ticker=payload['ticker'],started_at=start,observed_at=end,market=m)
