"""Forward-only executable-price wallet research. Never submits broker orders."""
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING
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

    def candidates(self):
        from .kalshi_wallet_copy_worker import ranked_wallets
        saved=self.db.execute('SELECT at,wallets FROM cohort WHERE id=1').fetchone()
        if not saved or self.clock()-saved[0]>=3600:
            wallets=ranked_wallets(self.read)
            self.db.execute('INSERT INTO cohort VALUES(1,?,?) ON CONFLICT(id) DO UPDATE SET at=excluded.at,wallets=excluded.wallets',(self.clock(),json.dumps(wallets)))
        else:wallets=json.loads(saved[1])
        return wallets

    def observe(self,trade):
        from .kalshi_wallet_monitor import trade_key
        key=trade_key(trade)
        if self.db.execute('SELECT 1 FROM observations WHERE id=?',(key,)).fetchone():return
        now=self.clock()
        state='observed' if self.started<=trade['timestamp']<=now and now-trade['timestamp']<=90 else 'late'
        self.db.execute('INSERT INTO observations VALUES(?,?,?,?,?)',(key,trade['proxy_wallet'],now,state,json.dumps(trade)))

    def quote(self,mapping):
        from .kalshi_wallet_monitor import _read_quote,read_fee
        from .kalshi_live_execution import LiveClient
        q=_read_quote(mapping,LiveClient.BASE_URL,self.read,environment='production')
        if not 0<=self.clock()-q['observed_at']<=5:raise ValueError('research_quote_stale')
        basis=read_fee(q,LiveClient.BASE_URL,self.read)
        if not 0<=self.clock()-q['observed_at']<=5:raise ValueError('research_quote_stale')
        return q,basis

    def process(self,key,trade):
        from .kalshi_wallet_copy_worker import executable,source_identity
        from .kalshi_wallet_sports import discover
        from .kalshi_live_execution import LiveClient
        if self.clock()-trade['timestamp']>90:raise ValueError('research_signal_expired')
        if trade['side']=='BUY':
            # At most one observation per wallet/event; repeated source fills cannot inflate evidence.
            mapping=discover(trade,LiveClient.BASE_URL,self.read)
            if self.db.execute('SELECT 1 FROM trades WHERE wallet=? AND event=?',(trade['proxy_wallet'],mapping['event_ticker'])).fetchone():return 'duplicate_event'
            q,basis=self.quote(mapping);price=executable(trade,mapping,q)
            if self.clock()-trade['timestamp']>90:raise ValueError('research_signal_expired')
            count=self.db.execute('SELECT count(*) FROM trades WHERE wallet=?',(trade['proxy_wallet'],)).fetchone()[0]
            detail=dict(id=key,wallet=trade['proxy_wallet'],event=mapping['event_ticker'],
                day=datetime.fromtimestamp(self.clock(),timezone.utc).date().isoformat(),
                phase='training' if count<20 else 'holdout',trade=trade,mapping=mapping,
                entered_at=self.clock(),entry_cents=price,entry_fee_cents=fee(price,basis['fee_multiplier']),
                entry_quote=q,entry_fee_basis=basis,net_cents=None,exit_kind=None,
                modeled=True,actual_fill=False)
            self.db.execute('INSERT INTO trades(id,wallet,event,day,phase,detail) VALUES(?,?,?,?,?,?)',
                (key,detail['wallet'],detail['event'],detail['day'],detail['phase'],json.dumps(detail)))
            return 'modeled_entry'
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

    def report(self,errors=None):
        rows=[dict(zip(('wallet','event','day','phase','net_cents','exit_kind','settled_hold_net_cents'),row)) for row in self.db.execute("SELECT wallet,event,day,phase,json_extract(detail,'$.net_cents'),json_extract(detail,'$.exit_kind'),json_extract(detail,'$.settled_hold_net_cents') FROM trades ORDER BY rowid")]
        candidates=self.db.execute('SELECT wallets FROM cohort WHERE id=1').fetchone()
        wallets=json.loads(candidates[0]) if candidates else []
        scores=[]
        for wallet in wallets:
            all_owned=[r for r in rows if r['wallet']==wallet]
            cutoff=datetime.fromtimestamp(self.clock(),timezone.utc).date().isoformat()
            owned=[r for r in all_owned if r['day']<cutoff];v=score(owned)
            held=[r['settled_hold_net_cents'] for r in owned if r.get('settled_hold_net_cents') is not None]
            scores.append(dict(wallet=wallet,**v,assessment_before_utc_day=cutoff,current_open=sum(r['net_cents'] is None for r in all_owned),open=sum(r['net_cents'] is None for r in owned),
                hold_resolved=len(held),hold_net_cents=sum(held)))
        selected=[r['wallet'] for r in sorted(scores,key=lambda r:r['holdout']['event_mean_lower_cents'] or -1e9,reverse=True) if r['qualified']][:4]
        result=dict(schema='wallet_profitability_v1',at=datetime.fromtimestamp(self.clock(),timezone.utc).isoformat(),
            policy_hash=POLICY_HASH,policy=POLICY,mode='prospective_shadow',actual_fills=False,
            state='error' if errors else 'observed',errors=errors or [],scores=scores,
            selected_wallets=selected if not errors else [],observations=self.db.execute('SELECT count(*) FROM observations').fetchone()[0],
            modeled_entries=len(rows),resolved=sum(r['net_cents'] is not None for r in rows))
        encoded=json.dumps(result,sort_keys=True)
        self.db.execute('INSERT OR IGNORE INTO decisions VALUES(?,?)',(self.clock(),encoded))
        tmp=self.root/'status.tmp';tmp.write_text(encoded,encoding='utf-8');tmp.replace(self.root/'status.json')
        return result

    def tick(self):
        from .kalshi_wallet_monitor import read_trades
        from .kalshi_wallet_copy_worker import error_code
        errors=[]
        try:wallets=self.candidates()
        except Exception as e:return self.report([error_code(e)])
        owners={w for w, in self.db.execute("SELECT DISTINCT wallet FROM trades WHERE json_extract(detail,'$.net_cents') IS NULL")}
        for wallet in sorted(set(wallets)|owners):
            try:
                for t in read_trades(wallet,max(self.started,self.clock()-90),self.read):self.observe(t)
            except Exception as e:errors.append(error_code(e))
        self.db.execute("UPDATE observations SET state='late' WHERE state='observed' AND at<?",(self.clock()-90,))
        for key,raw in self.db.execute("SELECT id,detail FROM observations WHERE state='observed' ORDER BY json_extract(detail,'$.timestamp'),json_extract(detail,'$.side'),rowid LIMIT 8").fetchall():
            try:state=self.process(key,json.loads(raw))
            except Exception as e:state=error_code(e)
            self.db.execute('UPDATE observations SET state=? WHERE id=?',(state,key))
        try:self.settle()
        except Exception as e:errors.append(error_code(e))
        return self.report(errors)


def run(root):
    from .kalshi_process_lock import acquire
    directory=Path(root)/DIRECTORY;directory.mkdir(parents=True,exist_ok=True)
    with acquire(directory/'worker.lock'):
        worker=Research(root)
        try:
            while True:
                try:worker.tick()
                except Exception as error:worker.report([type(error).__name__])
                time.sleep(30)
        finally:worker.db.close()


def read_report(path,now=None):
    """Bounded report read used by both telemetry and live selection."""
    now=now or datetime.now(timezone.utc)
    raw=Path(path).read_bytes()
    if len(raw)>16000:raise ValueError('research_report_too_large')
    return validate_report(json.loads(raw),now=now)


