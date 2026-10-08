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
    def _read(self,path,params=None):
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
    def get(self,ticker=None,*,book=False,params=None):
        if book and ticker is None:raise ValueError('ticker_required')
        path='/markets'+('/'+quote(ticker,safe='') if ticker else '')+('/orderbook' if book else '')
        return self._read(path,params)
    def get_trades(self,*,params):
        if (set(params)!={'limit','min_ts','max_ts'} or params['limit']!=100
                or any(type(params[k]) is not int for k in params)
                or not 0<=params['min_ts']<=params['max_ts']):
            raise ValueError('bounded_trade_query_required')
        return self._read('/markets/trades',params)
    def get_targeted_trades(self,*,params):
        required={'limit','min_ts','max_ts','ticker'}
        if (set(params) not in (required,required|{'cursor'}) or params['limit']!=100
                or any(type(params[k]) is not int for k in ('limit','min_ts','max_ts'))
                or not 0<=params['min_ts']<=params['max_ts']
                or not isinstance(params['ticker'],str)
                or not 1<=len(params['ticker'])<=160
                or not all(c.isascii() and (c.isupper() or c.isdigit() or c in '-_') for c in params['ticker'])
                or ('cursor' in params and (not isinstance(params['cursor'],str)
                    or not 1<=len(params['cursor'])<=2000))):
            raise ValueError('bounded_targeted_trade_query_required')
        return self._read('/markets/trades',params)
    def get_event(self,event_ticker):
        return self._read('/events/'+quote(event_ticker,safe=''))
    def get_series(self,series_ticker):
        return self._read('/series/'+quote(series_ticker,safe=''))
    def quote(self,payload):
        market,_,_=self.get(payload['ticker']);m=market['market']
        if m.get('ticker')!=payload['ticker'] or m.get('status')!='active' or m.get('market_type')!='binary':
            raise ValueError('demo_market_not_active_binary')
        data,start,end=self.get(payload['ticker'],book=True,params={'depth':20})
        return dict(data,environment='demo',ticker=payload['ticker'],started_at=start,observed_at=end,market=m)
