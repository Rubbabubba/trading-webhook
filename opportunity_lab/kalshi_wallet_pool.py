"""Bounded leaderboard discovery and persistent, coverage-aware research admission."""
import json
from .wallet_pool_policy import POLICY,HASH
from .kalshi_wallet_monitor import WALLET
from .kalshi_wallet_copy_worker import LEADERBOARD


def ensure(db):
    db.executescript('''CREATE TABLE IF NOT EXISTS wallet_pool(
      wallet TEXT PRIMARY KEY,first_seen REAL,last_seen REAL,rank INTEGER,sources TEXT,
      last_polled REAL DEFAULT 0,active_since REAL DEFAULT 0,cooldown_until REAL DEFAULT 0,state TEXT DEFAULT 'queued');
      CREATE TABLE IF NOT EXISTS wallet_pool_discovery(id INTEGER PRIMARY KEY,at REAL,detail TEXT);
      CREATE TABLE IF NOT EXISTS wallet_pool_scores(wallet TEXT PRIMARY KEY,at REAL,detail TEXT);''')


def discover(db,read,now):
    ensure(db)
    old=db.execute('SELECT at,detail FROM wallet_pool_discovery WHERE id=1').fetchone()
    if old and now-old[0]<POLICY['refresh_seconds']:return json.loads(old[1])
    from .wallet_pool_audit import begin,changed
    before=begin(db,now)
    found={};errors=[]
    for category in POLICY['categories']:
        for period in POLICY['periods']:
            try:
                rows=read(LEADERBOARD,{'category':category,'time_period':period,'order_by':'PNL','limit':POLICY['leaderboard_size']})[0]['data']
                if not isinstance(rows,list) or not 1<=len(rows)<=25:raise ValueError('invalid_pool_leaderboard')
                wallets=[r['user_id'].lower() for r in rows]
                if len(set(wallets))!=len(wallets) or any(not WALLET.fullmatch(w) for w in wallets) or [str(r['rank']) for r in rows]!=[str(i) for i in range(1,len(rows)+1)]:
                    raise ValueError('invalid_pool_leaderboard')
                for rank,w in enumerate(wallets,1):
                    item=found.setdefault(w,{'rank':rank,'sources':[]});item['rank']=min(item['rank'],rank);item['sources'].append(category+':'+period)
            except Exception as e:errors.append(category+':'+period+':'+type(e).__name__)
    # Failed refreshes do not erase the last good registry or its evidence.
    protected=[w for w,raw in db.execute("SELECT s.wallet,s.detail FROM wallet_pool_scores s JOIN wallet_pool p ON p.wallet=s.wallet WHERE p.last_seen>=?",(now-86400,)) if json.loads(raw).get('qualified')][:4]
    retained=[w for w, in db.execute("SELECT wallet FROM wallet_pool WHERE state!='archived' ORDER BY rank,wallet")] if errors else []
    wanted=list(dict.fromkeys(protected+retained+[w for w,v in sorted(found.items(),key=lambda item:(item[1]['rank'],item[0]))]))[:100]
    for (w,) in db.execute('SELECT wallet FROM wallet_pool').fetchall():
        if w not in wanted:db.execute("UPDATE wallet_pool SET state='archived' WHERE wallet=?",(w,))
    for w,v in sorted(found.items(),key=lambda item:(item[1]['rank'],item[0])):
        existing=db.execute('SELECT 1 FROM wallet_pool WHERE wallet=?',(w,)).fetchone()
        if w not in wanted:continue
        db.execute("INSERT INTO wallet_pool(wallet,first_seen,last_seen,rank,sources) VALUES(?,?,?,?,?) ON CONFLICT(wallet) DO UPDATE SET last_seen=excluded.last_seen,rank=excluded.rank,sources=excluded.sources,state=CASE WHEN wallet_pool.state='archived' THEN 'queued' ELSE wallet_pool.state END",
            (w,now,now,v['rank'],json.dumps(v['sources'])))
    detail={'policy':POLICY,'policy_hash':HASH,'at':now,'successful_lists':4-len(errors),'errors':errors}
    # Retry a failed list next cycle; successful discovery remains bounded hourly.
    if not errors:db.execute('INSERT INTO wallet_pool_discovery VALUES(1,?,?) ON CONFLICT(id) DO UPDATE SET at=excluded.at,detail=excluded.detail',(now,json.dumps(detail)))
    changed(db,before,now,'discovery_refresh')
    return detail


def admit(db,now,scores):
    ensure(db)
    from .wallet_pool_audit import begin,changed
    before=begin(db,now)
    # Bootstrap retained v2 candidates without discarding any observations/trades.
    prior=db.execute('SELECT wallets FROM cohort WHERE id=1').fetchone()
    if prior:
        for w in json.loads(prior[0]):
            first=db.execute('SELECT min(at) FROM observations WHERE wallet=?',(w,)).fetchone()[0] or now
            if db.execute('SELECT count(*) FROM wallet_pool').fetchone()[0]<100:
                db.execute("INSERT OR IGNORE INTO wallet_pool(wallet,first_seen,last_seen,rank,sources,active_since,state) VALUES(?,?,?,999,'[]',?,'active')",(w,first,now,first))
    metrics={w:(n,entries) for w,n,entries in db.execute('SELECT p.wallet,(SELECT count(*) FROM observations o WHERE o.wallet=p.wallet),(SELECT count(*) FROM trades t WHERE t.wallet=p.wallet) FROM wallet_pool p')}
    rows=[dict(zip(('wallet','first','seen','rank','sources','polled','since','until','state'),row)) for row in db.execute('SELECT * FROM wallet_pool')]
    for r in rows:
        w=r['wallet'];s=scores.get(w) or {};n,entries=metrics[w]
        training=s.get('training') or {}
        if s.get('coverage_gaps',0):
            r['state']='failed_coverage';r['until']=1e30
        elif training.get('resolved',0)>=20 and any(x.startswith('training:') or x=='pool:training_lower_bound' for x in s.get('reasons',[])):
            r['state']='failed_training';r['until']=1e30
        elif r['state']=='active' and not entries and now-r['since']>=POLICY['incompatible_grace_seconds'] and (
                n>=POLICY['incompatible_minimum_observations'] or now-r['since']>=POLICY['inactivity_seconds']):
            r['state']='cooldown';r['until']=now+POLICY['cooldown_seconds']
        db.execute('UPDATE wallet_pool SET state=?,cooldown_until=? WHERE wallet=?',(r['state'],r['until'],w))
    eligible=[r for r in rows if r['state']!='archived' and r['until']<=now and now-r['seen']<=POLICY['maximum_discovery_age_seconds']]
    qualified=sorted((r for r in eligible if (scores.get(r['wallet']) or {}).get('qualified')),key=lambda r:-(scores[r['wallet']]['holdout']['pool_event_lower_cents']))[:4]
    selected=[r['wallet'] for r in qualified]
    owners={w for w, in db.execute("SELECT DISTINCT wallet FROM trades WHERE json_extract(detail,'$.net_cents') IS NULL")}
    selected += [r['wallet'] for r in eligible if r['wallet'] in owners and r['wallet'] not in selected][:12-len(selected)]
    established=sorted((r for r in eligible if r['wallet'] not in selected and metrics[r['wallet']][1]>0),key=lambda r:(-metrics[r['wallet']][1],r['polled'],r['rank'],r['wallet']))
    selected += [r['wallet'] for r in established[:max(0,8-len(selected))]]
    # Four slots deliberately sample previously untested or least-recently polled candidates.
    exploratory=sorted((r for r in eligible if r['wallet'] not in selected and not metrics[r['wallet']][1]),key=lambda r:(0 if r['state']=='active' else 1,r['polled'],r['rank'],r['wallet']))
    selected += [r['wallet'] for r in exploratory[:12-len(selected)]]
    # If fewer fresh explorers exist, keep collecting supported wallets in the spare slots.
    selected += [r['wallet'] for r in established if r['wallet'] not in selected][:12-len(selected)]
    for r in rows:
        w=r['wallet']
        if w in selected:
            db.execute("UPDATE wallet_pool SET state='active',active_since=? WHERE wallet=?",(r['since'] if r['state']=='active' else now,w))
        elif r['state']=='active':db.execute("UPDATE wallet_pool SET state='queued',active_since=0 WHERE wallet=?",(w,))
    changed(db,before,now,'coverage_activity_and_frozen_score_admission')
    return selected


def summary(db,now):
    ensure(db)
    states=dict(db.execute("SELECT state,count(*) FROM wallet_pool WHERE state!='archived' GROUP BY state"))
    return {'policy':POLICY,'policy_hash':HASH,'total_wallets':sum(states.values()),'active_wallets':states.get('active',0),
        'tested_wallets':db.execute('SELECT count(DISTINCT wallet) FROM observations').fetchone()[0],
        'scored_wallets':db.execute('SELECT count(*) FROM wallet_pool_scores').fetchone()[0],
        'qualified_wallets':sum(json.loads(raw)['qualified'] for raw, in db.execute("SELECT s.detail FROM wallet_pool_scores s JOIN wallet_pool p ON p.wallet=s.wallet WHERE p.state!='archived'")),
        'archived_wallets':db.execute("SELECT count(*) FROM wallet_pool WHERE state='archived'").fetchone()[0],
        'states':states,'at':now,'featured_scores_only':True}
