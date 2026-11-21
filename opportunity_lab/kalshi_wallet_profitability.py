"""Forward-only executable-price wallet research. Never submits broker orders."""
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import statistics
import time

from .wallet_qualification import POLICY,POLICY_HASH,score,validate_report

DIRECTORY = 'wallet-profitability-v2'


def fee(price, multiplier):
    p=Decimal(price)/100; m=Decimal(str(multiplier))
    if not m.is_finite() or not 0<=m<=10 or not 0<=p<=1:raise ValueError('invalid_research_fee')
    return int((7*m*p*(1-p)).to_integral_value(rounding=ROUND_CEILING))


class Research:
    def __init__(self,root,*,read=None,clock=time.time):
        from .kalshi_wallet_monitor import public_get
        self.read=read or public_get;self.clock=clock
        self.root=Path(root)/DIRECTORY;self.root.mkdir(parents=True,exist_ok=True)
        self.db=sqlite3.connect(self.root/'research.sqlite3',isolation_level=None)
        self.db.executescript('''CREATE TABLE IF NOT EXISTS protocol(id INTEGER PRIMARY KEY,hash TEXT,started REAL);
          CREATE TABLE IF NOT EXISTS observations(id TEXT PRIMARY KEY,wallet TEXT,at REAL,state TEXT,detail TEXT);
          CREATE TABLE IF NOT EXISTS trades(id TEXT PRIMARY KEY,wallet TEXT,event TEXT,day TEXT,phase TEXT,detail TEXT,checked REAL DEFAULT 0);
          CREATE TABLE IF NOT EXISTS cohort(id INTEGER PRIMARY KEY,at REAL,wallets TEXT);
          CREATE TABLE IF NOT EXISTS decisions(at REAL PRIMARY KEY,detail TEXT);''')
        self.db.execute('INSERT OR IGNORE INTO protocol VALUES(1,?,?)',(POLICY_HASH,clock()))
        saved=self.db.execute('SELECT hash,started FROM protocol WHERE id=1').fetchone()
        if saved[0]!=POLICY_HASH:raise ValueError('research_protocol_changed')
        self.started=saved[1]
        from .kalshi_wallet_pool import ensure
        ensure(self.db)
        self.discovery_errors=[]
        self.collection={}
        self.stage_metrics = {}
        self.db.executescript('CREATE INDEX IF NOT EXISTS wallet_observation_lookup ON observations(wallet,at);CREATE INDEX IF NOT EXISTS wallet_trade_lookup ON trades(wallet,event);')

    def candidates(self):
        from .kalshi_wallet_pool import admit,discover
        scores=self.score_all()
        admit(self.db,self.clock(),scores)  # Import retained v2 cohort before enlarging discovery.
        result=discover(self.db,self.read,self.clock());self.discovery_errors=result.get('errors',[])
        wallets=admit(self.db,self.clock(),self.score_all())
        if not self.db.execute('SELECT 1 FROM wallet_pool WHERE last_seen>=?',(self.clock()-86400,)).fetchone():raise ValueError('wallet_discovery_unavailable')
        self.db.execute('INSERT INTO cohort VALUES(1,?,?) ON CONFLICT(id) DO UPDATE SET at=excluded.at,wallets=excluded.wallets',(self.clock(),json.dumps(wallets)))
        return wallets

    def observe(self,trade):
        from .kalshi_wallet_monitor import trade_key
        key=trade_key(trade)
        if self.db.execute('SELECT 1 FROM observations WHERE id=?',(key,)).fetchone():return
        now=self.clock()
        source_seen = trade.get('observed_at', now)
        self.record_latency('source_to_poll', source_seen - trade['timestamp'])
        self.record_latency('poll_to_journal', now - source_seen)
        state='observed' if self.started<=trade['timestamp']<=now and now-trade['timestamp']<=90 else 'late'
        if state == 'observed' and trade['side'] == 'BUY':
            from .kalshi_wallet_sports import supported_game
            if supported_game(trade.get('event_slug'), research_nhl=True) is None:
                state = 'unsupported_sports_event'
        self.db.execute('INSERT INTO observations VALUES(?,?,?,?,?)',(key,trade['proxy_wallet'],now,state,json.dumps(trade)))
        return state

    def record_latency(self, stage, seconds):
        self.stage_metrics.setdefault(stage, []).append(max(0, seconds))

    def timed_read(self, stage, read, *args, **kwargs):
        started = time.monotonic()
        try:
            return read(*args, **kwargs)
        finally:
            self.record_latency(stage, time.monotonic() - started)

    def quote(self,mapping):
        from .kalshi_wallet_monitor import _read_quote,read_fee
        from .kalshi_live_execution import LiveClient
        q=self.timed_read('quote', _read_quote, mapping, LiveClient.BASE_URL, self.read, environment='production')
        if not 0<=self.clock()-q['observed_at']<=5:raise ValueError('research_quote_stale')
        basis=self.timed_read('fee_metadata', read_fee, q, LiveClient.BASE_URL, self.read)
        if not 0<=self.clock()-q['observed_at']<=5:raise ValueError('research_quote_stale')
        return q,basis

    def nhl_demo_evidence(self, trade, production_mapping):
        """Retain a contemporaneous, executable Demo counterpart for scoring."""
        from .kalshi_wallet_copy_worker import executable
        from .kalshi_wallet_monitor import DEMO, read_fee, read_quote
        from .kalshi_wallet_sports import discover

        mapping = discover(trade, DEMO, self.read, research_nhl=True)
        comparable_fields = ('ticker', 'event_ticker', 'side', 'kalshi_rules')
        if any(mapping[field] != production_mapping[field] for field in comparable_fields):
            raise ValueError('nhl_demo_contract_differs')
        quote = read_quote(mapping, DEMO, self.read)
        price = executable(trade, mapping, quote)
        basis = read_fee(quote, DEMO, self.read)
        if not 0 <= self.clock() - quote['observed_at'] <= POLICY['maximum_quote_age_seconds']:
            raise ValueError('nhl_demo_quote_stale')
        return {'ticker': mapping['ticker'], 'side': mapping['side'],
                'price_cents': price, 'quote_at': quote['observed_at'], 'fee_basis': basis}

    def process(self,key,trade):
        from .kalshi_wallet_copy_worker import executable,source_identity
        from .kalshi_wallet_sports import discover
        from .kalshi_live_execution import LiveClient
        if self.clock()-trade['timestamp']>90:raise ValueError('research_signal_expired')
        if trade['side']=='BUY':
            from .wallet_pool_policy import POLICY as POOL
            owners={w for w, in self.db.execute("SELECT DISTINCT wallet FROM trades WHERE json_extract(detail,'$.net_cents') IS NULL")}
            if len(owners)>=POOL['maximum_shadow_owners'] and trade['proxy_wallet'] not in owners:return 'shadow_owner_limit'
            # At most one observation per wallet/event; repeated source fills cannot inflate evidence.
            nhl = str(trade.get('event_slug') or '').startswith('nhl-')
            mapping = self.timed_read('contract_match', discover, trade, LiveClient.BASE_URL, self.read,
                                      **({'research_nhl': True} if nhl else {}))
            if self.db.execute('SELECT 1 FROM trades WHERE wallet=? AND event=?',(trade['proxy_wallet'],mapping['event_ticker'])).fetchone():return 'duplicate_event'
            demo_evidence = None
            demo_hold = None
            if nhl:
                from .kalshi_wallet_copy_worker import error_code
                try:
                    demo_evidence = self.timed_read('nhl_demo_execution', self.nhl_demo_evidence, trade, mapping)
                except Exception as error:
                    demo_hold = error_code(error)
            q,basis=self.quote(mapping);price=executable(trade,mapping,q)
            if self.clock()-trade['timestamp']>90:raise ValueError('research_signal_expired')
            # Persist the admission decision when observed. Historical
            # exploratory entries do not move into either scored cohort.
            exploratory = nhl and demo_evidence is None
            count = self.db.execute("SELECT count(*) FROM trades WHERE wallet=? AND phase!='coverage'",
                                    (trade['proxy_wallet'],)).fetchone()[0]
            detail=dict(id=key,wallet=trade['proxy_wallet'],event=mapping['event_ticker'],
                day=datetime.fromtimestamp(self.clock(),timezone.utc).date().isoformat(),
                phase='coverage' if exploratory else 'training' if count<20 else 'holdout',trade=trade,mapping=mapping,
                entered_at=self.clock(),entry_cents=price,entry_fee_cents=fee(price,basis['fee_multiplier']),
                entry_quote=q,entry_fee_basis=basis,net_cents=None,exit_kind=None,
                modeled=True,actual_fill=False)
            if nhl:
                detail['nhl_demo_evidence'] = demo_evidence
                detail['nhl_demo_hold'] = demo_hold
            self.db.execute('INSERT INTO trades(id,wallet,event,day,phase,detail) VALUES(?,?,?,?,?,?)',
                (key,detail['wallet'],detail['event'],detail['day'],detail['phase'],json.dumps(detail)))
            return 'modeled_coverage_entry' if exploratory else 'modeled_entry'
        for ident,raw in self.db.execute("SELECT id,detail FROM trades WHERE wallet=? AND json_extract(detail,'$.net_cents') IS NULL",(trade['proxy_wallet'],)).fetchall():
            d=json.loads(raw)
            if d['net_cents'] is not None or source_identity(d['trade'])!=source_identity(trade) or d['trade']['timestamp']>trade['timestamp']:continue
            q,basis=self.quote(d['mapping']);price=executable(trade,d['mapping'],q)
            if self.clock()-trade['timestamp']>90:raise ValueError('research_signal_expired')
            f=fee(price,basis['fee_multiplier'])
            d.update(net_cents=price-d['entry_cents']-d['entry_fee_cents']-f,
                exit_kind='wallet_sell',exit_cents=price,exit_fee_cents=f,exit_quote=q,exit_fee_basis=basis,
                exited_at=self.clock(),exit_signal=trade)
            self.db.execute('UPDATE trades SET detail=? WHERE id=?',(json.dumps(d),ident))
            return 'modeled_exit'
        return 'no_shadow_owner'

    def coverage_gap(self,trade):
        if trade.get('side')!='SELL':return
        from .kalshi_wallet_copy_worker import source_identity
        for ident,raw in self.db.execute("SELECT id,detail FROM trades WHERE wallet=? AND json_extract(detail,'$.net_cents') IS NULL",(trade['proxy_wallet'],)).fetchall():
            detail=json.loads(raw)
            if 'condition_id' in trade and (source_identity(detail['trade'])!=source_identity(trade) or detail['trade']['timestamp']>trade['timestamp']):continue
            detail['coverage_gap']=True
            self.db.execute('UPDATE trades SET detail=? WHERE id=?',(json.dumps(detail),ident))

    def settle(self):
        from .kalshi_live_execution import LiveClient
        for ident,raw in self.db.execute("SELECT id,detail FROM trades WHERE json_extract(detail,'$.settled_hold_net_cents') IS NULL ORDER BY checked,id LIMIT 4").fetchall():
            d=json.loads(raw)
            self.db.execute('UPDATE trades SET checked=? WHERE id=?',(self.clock(),ident))
            if d.get('settled_hold_net_cents') is not None:continue
            row=self.read(LiveClient.BASE_URL+'/markets/'+d['mapping']['ticker'])[0]['market']
            if row.get('ticker')!=d['mapping']['ticker'] or row.get('event_ticker')!=d['event'] or any(row.get(k)!=v for k,v in d['mapping'].get('kalshi_rules',{}).items()):raise ValueError('research_settlement_identity')
            if row.get('status')!='finalized' or row.get('result') not in ('yes','no'):continue
            payout=100 if row['result']==d['mapping']['side'] else 0
            d['settled_hold_net_cents']=payout-d['entry_cents']-d['entry_fee_cents']
            if d['net_cents'] is None:d.update(net_cents=d['settled_hold_net_cents'],exit_kind='settlement',exited_at=self.clock())
            d['settlement']=row
            self.db.execute('UPDATE trades SET detail=? WHERE id=?',(json.dumps(d),ident))

    def score_all(self):
        from .wallet_pool_policy import adjusted
        rows=[dict(zip(('wallet','event','day','phase','net_cents','exit_kind','settled_hold_net_cents','coverage_gap'),row)) for row in self.db.execute("SELECT wallet,event,day,phase,json_extract(detail,'$.net_cents'),json_extract(detail,'$.exit_kind'),json_extract(detail,'$.settled_hold_net_cents'),json_extract(detail,'$.coverage_gap') FROM trades WHERE phase!='coverage' ORDER BY rowid")]
        registry=[w for w, in self.db.execute('SELECT wallet FROM wallet_pool ORDER BY first_seen,wallet')]
        prior=self.db.execute('SELECT wallets FROM cohort WHERE id=1').fetchone()
        wallets=registry or (json.loads(prior[0]) if prior else [])
        grouped={}
        for r in rows:grouped.setdefault(r['wallet'],[]).append(r)
        cutoff=datetime.fromtimestamp(self.clock(),timezone.utc).date().isoformat();scores={}
        for wallet in wallets:
            all_owned=grouped.get(wallet,[]);owned=[r for r in all_owned if r['day']<cutoff];v=score(owned)
            held=[r['settled_hold_net_cents'] for r in owned if r.get('settled_hold_net_cents') is not None]
            row=dict(wallet=wallet,**v,coverage_gaps=sum(bool(r.get('coverage_gap')) for r in owned),assessment_before_utc_day=cutoff,current_open=sum(r['net_cents'] is None for r in all_owned),open=sum(r['net_cents'] is None for r in owned),hold_resolved=len(held),hold_net_cents=sum(held))
            if registry:row=adjusted(row)
            scores[wallet]=row
        return scores

    def report(self,errors=None):
        from .kalshi_wallet_pool import summary
        all_scores=self.score_all();registry=bool(self.db.execute('SELECT 1 FROM wallet_pool').fetchone())
        for wallet,row in all_scores.items():
            self.db.execute('INSERT INTO wallet_pool_scores VALUES(?,?,?) ON CONFLICT(wallet) DO UPDATE SET at=excluded.at,detail=excluded.detail',(wallet,self.clock(),json.dumps(row)))
        current={w for w, in self.db.execute("SELECT wallet FROM wallet_pool WHERE state!='archived' AND last_seen>=?",(self.clock()-86400,))} if registry else set(all_scores)
        field='pool_event_lower_cents' if registry else 'event_mean_lower_cents'
        ranked=sorted((r for w,r in all_scores.items() if w in current),key=lambda r:r['holdout'].get(field) or -1e9,reverse=True)
        selected=[r['wallet'] for r in ranked if r['qualified']][:4]
        # All scores remain durable; only eight are transported. Selected wallets always appear.
        rest=[r for r in ranked if r['wallet'] not in selected]
        offset=(int(self.clock())//30*4)%len(rest) if rest else 0
        featured=[all_scores[w] for w in selected]+(rest[offset:]+rest[:offset])[:8-len(selected)]
        total,resolved=self.db.execute("SELECT count(*),sum(CASE WHEN json_extract(detail,'$.net_cents') IS NOT NULL THEN 1 ELSE 0 END) FROM trades").fetchone()
        result=dict(schema='wallet_profitability_v1',at=datetime.fromtimestamp(self.clock(),timezone.utc).isoformat(),
            policy_hash=POLICY_HASH,policy=POLICY,mode='prospective_shadow',actual_fills=False,
            state='error' if errors else 'observed',errors=errors or [],scores=featured,
            selected_wallets=selected if not errors else [],observations=self.db.execute('SELECT count(*) FROM observations').fetchone()[0],
            modeled_entries=total,resolved=resolved or 0,
            observation_state_counts_lifetime=dict(self.db.execute('SELECT state,count(*) FROM observations GROUP BY state ORDER BY count(*) DESC,state LIMIT 32').fetchall()))
        if self.collection:result['collection']=self.collection
        coverage, coverage_resolved = self.db.execute(
            "SELECT count(*),count(json_extract(detail,'$.net_cents')) FROM trades WHERE phase='coverage'").fetchone()
        nhl_scored = self.db.execute(
            "SELECT count(*) FROM trades WHERE phase!='coverage' AND json_type(detail,'$.nhl_demo_evidence')='object'").fetchone()[0]
        if nhl_scored:
            result['verified_nhl_scored_entries'] = nhl_scored
        if coverage:
            result['exploratory_nhl'] = {'entries': coverage, 'resolved': coverage_resolved,
                                         'counts_toward_qualification': False}
        # Surface the closest wallet even when the other rows rotate.
        if rest:
            closest = max(rest, key=lambda r: (min(r['training']['resolved'],20)+min(r['holdout']['resolved'],20),
                                               r['training']['events']+r['holdout']['events'], -r['open']))
            result['nearest_qualification'] = {k: closest[k] for k in ('wallet','training','holdout','open','reasons')}

        if registry:
            from .wallet_pool_audit import snapshot,bound_report
            result.update(discovery=summary(self.db,self.clock()),discovery_errors=self.discovery_errors,
                pool_audit=snapshot(self.db,all_scores,self.clock()))
            bound_report(result,self.clock())
        encoded=json.dumps(result,sort_keys=True)
        if len(encoded.encode())>16000:raise ValueError('research_report_too_large')
        self.db.execute('INSERT OR IGNORE INTO decisions VALUES(?,?)',(self.clock(),encoded))
        tmp=self.root/'status.tmp';tmp.write_text(encoded,encoding='utf-8');tmp.replace(self.root/'status.json')
        return result

    def tick(self):
        from .kalshi_wallet_monitor import read_trades
        from .kalshi_wallet_copy_worker import error_code
        errors=[];started=time.monotonic()
        self.stage_metrics = {}
        processed_states = Counter()
        collected_states = Counter()
        try:wallets=self.candidates()
        except Exception as e:return self.report([error_code(e)])
        owners={w for w, in self.db.execute("SELECT DISTINCT wallet FROM trades WHERE json_extract(detail,'$.net_cents') IS NULL")}
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=8) as pool:
            poll_order = sorted(owners) + [wallet for wallet in wallets if wallet not in owners]
            tasks = {pool.submit(read_trades, w, max(self.started, self.clock()-90), self.read): w
                     for w in poll_order}
            for task in as_completed(tasks):
                wallet = tasks[task]
                try:
                    last=self.db.execute('SELECT last_polled FROM wallet_pool WHERE wallet=?',(wallet,)).fetchone()
                    if last and last[0] and self.clock()-last[0]>90:self.coverage_gap({'side':'SELL','proxy_wallet':wallet})
                    for trade in task.result():
                        state = self.observe(trade)
                        if state:
                            collected_states[state] += 1
                    self.db.execute('UPDATE wallet_pool SET last_polled=? WHERE wallet=?',(self.clock(),wallet))
                except Exception as e:errors.append(error_code(e))
        stale=self.db.execute("SELECT detail FROM observations WHERE state='observed' AND json_extract(detail,'$.timestamp')<?",(self.clock()-90,)).fetchall()
        poll_elapsed=time.monotonic()-started
        for raw, in stale:self.coverage_gap(json.loads(raw))
        self.db.execute("UPDATE observations SET state='late' WHERE state='observed' AND json_extract(detail,'$.timestamp')<?",(self.clock()-90,))
        groups={}
        for wallet in sorted(set(wallets)|owners):
            pending=self.db.execute("SELECT id,detail FROM observations WHERE state='observed' AND wallet=? ORDER BY json_extract(detail,'$.timestamp'),json_extract(detail,'$.side'),rowid LIMIT 2",(wallet,)).fetchall()
            if pending:groups[wallet]=pending
        names=list(groups);offset=(int(self.clock())//30*12)%len(names) if names else 0
        names=names[offset:]+names[:offset];fair=[]
        for _ in range(2):
            for wallet in names:
                if groups[wallet]:fair.append(groups[wallet].pop(0))
        deadline=self.clock()+20;processed=0
        for key,raw in fair[:24]:
            if self.clock()>deadline:break
            trade=json.loads(raw)
            self.record_latency('poll_to_processing', self.clock() - trade.get('observed_at', self.clock()))
            try:
                state='wallet_outside_active_research' if trade['side']=='BUY' and trade['proxy_wallet'] not in wallets else self.process(key,trade)
            except Exception as e:
                state=error_code(e)
                if trade['side']=='SELL':self.coverage_gap(trade)
            self.db.execute('UPDATE observations SET state=? WHERE id=?',(state,key))
            processed_states[state] += 1
            processed+=1
        try:self.settle()
        except Exception as e:errors.append(error_code(e))
        self.collection=dict(at=self.clock(),tick_work_seconds=round(time.monotonic()-started,3),
            discovery_and_poll_seconds=round(poll_elapsed,3),processed=processed,
            polled_wallets=len(set(wallets)|owners),pending_observations=self.db.execute(
                "SELECT count(*) FROM observations WHERE state='observed'").fetchone()[0],
            target_cycle_seconds=30,minimum_pause_seconds=5,maximum_signal_seconds=90,
            collected_states=dict(collected_states), processed_states=dict(processed_states.most_common(12)),
            latency_seconds={stage: {'samples': len(values), 'p50': round(statistics.median(values), 3),
                                     'max': round(max(values), 3)}
                             for stage, values in self.stage_metrics.items()})
        return self.report(errors)


def run(root):
    from .kalshi_process_lock import acquire
    directory=Path(root)/DIRECTORY;directory.mkdir(parents=True,exist_ok=True)
    with acquire(directory/'worker.lock'):
        worker=Research(root)
        try:
            while True:
                began=time.monotonic()
                try:worker.tick()
                except Exception as error:worker.report([type(error).__name__])
                # Work belongs inside the 30-second cadence, rather than adding
                # another 30 seconds after slow mapping/poll work. Never spin or
                # accumulate catch-up requests when a cycle overruns.
                time.sleep(max(5,30-(time.monotonic()-began)))
        finally:worker.db.close()


def read_report(path,now=None):
    """Bounded report read used by both telemetry and live selection."""
    now=now or datetime.now(timezone.utc)
    raw=Path(path).read_bytes()
    if len(raw)>16000:raise ValueError('research_report_too_large')
    return validate_report(json.loads(raw),now=now)


