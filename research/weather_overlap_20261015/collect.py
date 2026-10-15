"""Bounded public metadata/forecast/candle GET collector; no trading interface."""
import hashlib
import json
import sys
import urllib.request
import urllib.error
from datetime import date,datetime,timedelta,timezone
from pathlib import Path
from urllib.parse import urlencode

ROOT=Path(__file__).parent


class Collector:
    def __init__(self):
        self.requests=0
        self.cache=ROOT/'cache'
        self.cache.mkdir(exist_ok=True)

    def get(self,key,url):
        path=self.cache/(key+'.json')
        if path.exists():
            saved=json.loads(path.read_text())
            if saved['url']!=url: raise ValueError('cache_identity_mismatch')
            return saved
        if len(list(self.cache.glob('*.json')))>=300: raise ValueError('request_cap')
        self.requests+=1
        receipt={'url':url,'captured_at':datetime.now(timezone.utc).isoformat(),'authenticated':False,'method':'GET'}
        try:
            with urllib.request.urlopen(urllib.request.Request(url,headers={'User-Agent':'bounded-weather-research/1'}),timeout=15) as r:
                b=r.read(3000001)
                if len(b)>3000000: raise ValueError('response_size_cap')
                receipt.update(status=r.status,payload=json.loads(b),body_sha256=hashlib.sha256(b).hexdigest())
        except urllib.error.HTTPError as e:
            receipt.update(status=e.code,error=e.read(4096).decode('utf-8',errors='replace'))
        except Exception as e:
            receipt.update(status=None,error=type(e).__name__+': '+str(e))
        with path.open('x',encoding='utf-8') as f: json.dump(receipt,f,indent=2)
        if receipt['status'] in (401,403,429) or receipt['status'] is None:
            raise RuntimeError('access_or_transport_blocked:'+key)
        return receipt


def collect(days):
    c=Collector()
    for i in range(days):
        day=date(2026,6,1)+timedelta(days=i)
        prior=day-timedelta(days=1)
        event='KXHIGHNY-'+day.strftime('%y%b%d').upper()
        query=urlencode({'event_ticker':event,'limit':1000})
        event_result=c.get(event,'https://external-api.kalshi.com/trade-api/v2/historical/markets?'+query)
        for model in ('ecmwf_ifs','gfs_seamless'):
            q=urlencode({'latitude':40.7794,'longitude':-73.9692,'models':model,'run':str(prior)+'T00:00','hourly':'temperature_2m','temperature_unit':'fahrenheit','timezone':'America/New_York','forecast_days':3})
            forecast=c.get(str(day)+'-'+model+'-hourly-v2','https://single-runs-api.open-meteo.com/v1/forecast?'+q)
            if forecast['status']!=200: raise RuntimeError('forecast_request_rejected:'+model)
        if event_result.get('status')==200:
            markets=event_result['payload'].get('markets',[])
            if event_result['payload'].get('cursor') or len(markets)>12: raise ValueError('unexpected_incomplete_event')
            decision=int(datetime.combine(prior,datetime.min.time(),tzinfo=timezone.utc).timestamp())+20*3600
            for m in markets:
                ticker=m['ticker']
                if m.get('event_ticker')!=event: raise ValueError('event_identity')
                q=urlencode({'start_ts':decision-60,'end_ts':decision,'period_interval':1})
                c.get(ticker+'-decision','https://external-api.kalshi.com/trade-api/v2/historical/markets/'+ticker+'/candlesticks?'+q)
        print(json.dumps({'day':str(day),'new_requests':c.requests,'event_status':event_result['status']}),flush=True)


if __name__=='__main__':
    days=int(sys.argv[1]) if len(sys.argv)>1 else 30
    assert 1<=days<=30
    collect(days)
