"""Bounded immutable public raw METAR / official contract metadata capture."""
import hashlib,json,time,urllib.request,urllib.error
from pathlib import Path
from datetime import date,datetime,timedelta,timezone
from urllib.parse import urlencode

ROOT=Path(__file__).parent


class Collector:
    def __init__(self):
        self.cache=ROOT/'cache';self.cache.mkdir(exist_ok=True)
        self.requests=0;self.next_at=0
    def get(self,url):
        key=hashlib.sha256(url.encode()).hexdigest();p=self.cache/(key+'.json');body=self.cache/(key+'.body')
        if p.exists():
            r=json.loads(p.read_text())
            if r['url']!=url:raise ValueError('request_identity_changed')
            if r.get('body_sha256') and hashlib.sha256(body.read_bytes()).hexdigest()!=r['body_sha256']:
                raise ValueError('cached_body_changed')
            return r
        if self.requests>=220:raise ValueError('request_capacity')
        self.requests+=1;time.sleep(max(0,self.next_at-time.monotonic()))
        r=dict(url=url,captured_at=datetime.now(timezone.utc).isoformat(),authenticated=False,method='GET')
        try:
            with urllib.request.urlopen(urllib.request.Request(url,headers={'User-Agent':'bounded-weather-source-research/1'}),timeout=20) as response:
                raw=response.read(4000001);r['status']=response.status
            with body.open('xb') as f:f.write(raw)
            r.update(body_path=str(body),body_sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw))
            if len(raw)>4000000:raise ValueError('response_capacity')
        except urllib.error.HTTPError as e:
            r.update(status=e.code,error='HTTPError')
        except (OSError,ValueError) as e:
            r.update(status=None,error=type(e).__name__)
        finally:self.next_at=time.monotonic()+.5
        with p.open('x') as f:json.dump(r,f,indent=2)
        return r


def run():
    spec=json.loads((ROOT/'registration.json').read_text());c=Collector();observations={};events={};blocked=[]
    for station in spec['stations']:
        params=dict(station=station['iem'],network=station['network'],data='metar',
            year1=2026,month1=7,day1=1,year2=2026,month2=9,day2=2,tz='Etc/UTC',
            format='onlycomma',latlon='no',elev='no',missing='M',trace='T',direct='no',report_type=[3,4])
        url='https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?'+urlencode(params,doseq=True)
        r=c.get(url);observations[station['station']]=r
        print(json.dumps(dict(station=station['station'],observation_status=r.get('status'),new_requests=c.requests)),flush=True)
        if r.get('status') in (401,403,429):blocked.append(dict(provider='IEM',status=r['status']));break
    for station in spec['stations']:
        for i in range(62):
            day=date(2026,7,1)+timedelta(days=i)
            event=station['series']+'-'+day.strftime('%y%b%d').upper()
            url='https://external-api.kalshi.com/trade-api/v2/historical/markets?'+urlencode(dict(event_ticker=event,limit=1000))
            r=c.get(url);events[event]=r
            print(json.dumps(dict(event=event,status=r.get('status'),new_requests=c.requests)),flush=True)
            if r.get('status') in (401,403,429):
                blocked.append(dict(provider='Kalshi',status=r['status']));break
        if any(b['provider']=='Kalshi' for b in blocked):break
    receipt=dict(schema='weather_bound_collection_v1',registration_sha256=hashlib.sha256((ROOT/'registration.json').read_bytes()).hexdigest(),
        captured_at=datetime.now(timezone.utc).isoformat(),observations=observations,events=events,
        new_requests=c.requests,access_blocks=blocked,actual_orders=0,actual_fills=0)
    with (ROOT/'collection.json').open('x') as f:json.dump(receipt,f,indent=2)
    print(json.dumps(dict(done=True,observation_sources=len(observations),events=len(events),new_requests=c.requests,blocks=blocked)))


if __name__=='__main__':run()
