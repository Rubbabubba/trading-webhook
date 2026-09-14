"""Self-contained public discovery for the credential-free hosted paper worker."""
from collections import defaultdict
import json
from pathlib import Path
import time
from urllib.parse import urlencode
from urllib.request import Request,urlopen
from .kalshi_shadow_live import epoch


class Discovery:
    def __init__(self,path,stop):
        self.path=Path(path);self.stop=stop;self.cursor='';self.catalog={};self.cache={}
        self.cycle=0;self.last=0;self.next_request=0
        if self.path.exists():self.cache=json.loads(self.path.read_text())
    def get(self,path,**params):
        time.sleep(max(0,self.next_request-time.monotonic()))
        try:
            url='https://external-api.kalshi.com/trade-api/v2'+path+('?' + urlencode(params) if params else '')
            with urlopen(Request(url,headers={'Accept':'application/json'}),timeout=8) as response:return json.load(response)
        finally:self.next_request=time.monotonic()+2
    def update(self):
        if time.time()-self.last<60:return
        if not self.catalog or self.cycle%60==0:
            self.catalog={s['ticker']:s for s in self.get('/series')['series']}
        data=self.get('/events',status='open',with_nested_markets='true',limit=200,cursor=self.cursor)
        self.cursor=data.get('cursor','');events=data.get('events',[])
        if self.cycle==0:
            # Seed known high-activity series so a first page dominated by
            # combos does not leave the independent worker without a universe.
            for ticker in ('KXFEDDECISION','KXMLBGAME'):
                if ticker in self.catalog:
                    seed=self.get('/markets',series_ticker=ticker,status='open',limit=1000,mve_filter='exclude')
                    events.append(dict(series_ticker=ticker,markets=seed.get('markets',[])))
        groups=defaultdict(list)
        for s in self.catalog.values():groups[s.get('category','Unknown')].append(s)
        categories=sorted(groups)
        if categories:
            category=categories[self.cycle%len(categories)]
            series=sorted(groups[category],key=lambda s:(s.get('frequency') not in ('hourly','daily','weekly'),s['ticker']))
            chosen=series[(self.cycle//len(categories))%len(series)]['ticker']
            extra=self.get('/markets',series_ticker=chosen,status='open',limit=1000,mve_filter='exclude')
            events.append(dict(series_ticker=chosen,markets=extra.get('markets',[])))
        for event in events:
            series=self.catalog.get(event.get('series_ticker'),{})
            if series.get('fee_type') not in ('quadratic','quadratic_with_maker_fees'):continue
            for market in event.get('markets',[]):
                try:
                    if (market.get('status')=='active' and market.get('market_type')=='binary'
                            and market.get('rules_primary') and market.get('event_ticker')
                            and time.time()+1800<epoch(market['close_time'])<=self.stop):
                        self.cache[market['ticker']]=dict(market=market,series=series)
                except (ValueError,TypeError,KeyError):continue
        self.cache={k:v for k,v in self.cache.items() if epoch(v['market']['close_time'])>time.time()+1200}
        self.path.parent.mkdir(parents=True,exist_ok=True)
        tmp=self.path.with_suffix('.tmp');tmp.write_text(json.dumps(self.cache));tmp.replace(self.path)
        self.cycle+=1;self.last=time.time()
