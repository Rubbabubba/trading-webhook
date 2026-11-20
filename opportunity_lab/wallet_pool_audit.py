"""Bounded operational evidence, separate from authoritative qualification."""
import csv,io,json
from datetime import datetime,timezone

STATES=['queued','active','cooldown','failed_training','failed_coverage','archived']
COLUMNS=['wallet','state','observations','modeled_entries','last_poll_age_seconds','active_age_seconds',
    'cooldown_remaining_seconds','training_resolved','training_events','training_days','training_net_cents',
    'training_stressed_net_cents','training_pool_lower_cents','holdout_resolved','holdout_events',
    'holdout_days','holdout_net_cents','holdout_stressed_net_cents','holdout_pool_lower_cents',
    'pending_prior_entries','coverage_gaps','qualified']


def begin(db,now):
    db.executescript('''CREATE TABLE IF NOT EXISTS wallet_pool_audit_start(id INTEGER PRIMARY KEY,at REAL);
      CREATE TABLE IF NOT EXISTS wallet_pool_changes(
      at REAL,wallet TEXT,previous_state TEXT,state TEXT,reason TEXT,
      UNIQUE(at,wallet,previous_state,state));''')
    before=dict(db.execute('SELECT wallet,state FROM wallet_pool'))
    if not db.execute('SELECT 1 FROM wallet_pool_audit_start WHERE id=1').fetchone():
        db.execute('INSERT INTO wallet_pool_audit_start VALUES(1,?)',(now,))
        db.executemany('INSERT INTO wallet_pool_changes VALUES(?,?,NULL,?,?)',
            [(now,w,state,'initial_observation_only') for w,state in before.items()])
    return before


def changed(db,before,now,reason):
    after=dict(db.execute('SELECT wallet,state FROM wallet_pool'))
    db.executemany('INSERT OR IGNORE INTO wallet_pool_changes VALUES(?,?,?,?,?)',
        [(now,w,before.get(w),state,reason) for w,state in after.items() if before.get(w)!=state])


def snapshot(db,scores,now):
    begin(db,now)
    current=list(db.execute("SELECT p.wallet,p.state,p.last_polled,p.active_since,p.cooldown_until,"
        "(SELECT count(*) FROM observations o WHERE o.wallet=p.wallet),"
        "(SELECT count(*) FROM trades t WHERE t.wallet=p.wallet) "
        "FROM wallet_pool p WHERE p.state!='archived' ORDER BY p.wallet"))
    rows=[]
    for w,state,polled,since,until,observations,entries in current:
        s=scores[w];row=[w,STATES.index(state),observations,entries,
            max(0,int(now-polled)) if polled else None,
            max(0,int(now-since)) if since else None,
            max(0,int(until-now)) if state=='cooldown' else None]
        for phase in ('training','holdout'):
            p=s[phase];row.extend(p.get(k) for k in ('resolved','events','days','net_cents','stressed_net_cents','pool_event_lower_cents'))
        row.extend((s['open'],s.get('coverage_gaps',0),int(s['qualified'])))
        rows.append(row)
    stream=io.StringIO();csv.writer(stream,lineterminator='\n').writerows(rows)
    changes=[{'at':at,'wallet':w,'from':old,'to':new,'reason':reason}
        for at,w,old,new,reason in db.execute('SELECT * FROM wallet_pool_changes ORDER BY at DESC,rowid DESC LIMIT 6')]
    return {'schema':'wallet_pool_audit_v1','at':now,'started_at':db.execute('SELECT at FROM wallet_pool_audit_start WHERE id=1').fetchone()[0],
        'history_started_at':datetime.fromtimestamp(db.execute('SELECT at FROM wallet_pool_audit_start WHERE id=1').fetchone()[0],timezone.utc).isoformat(),
        'history_scope':'Observed state changes since started_at; initial_observation_only rows are baselines, not reconstructed rotations.',
        'columns':COLUMNS,'state_codes':STATES,
        'total_rows':len(rows),'shown_rows':len(rows),'complete':True,'rows_csv':stream.getvalue(),
        'retained_score_rows':db.execute('SELECT count(*) FROM wallet_pool_scores').fetchone()[0],
        'retained_decision_packets':db.execute('SELECT count(*) FROM decisions').fetchone()[0],
        'observed_state_changes':db.execute("SELECT count(*) FROM wallet_pool_changes WHERE reason!='initial_observation_only'").fetchone()[0],
        'recent_state_changes':changes,'use':'Operational audit only; live selection still uses full fresh scorecards and independent owner authority.'}


def bound_report(result,now):
    """Keep selected full scorecards. Optional audit paging never changes selection."""
    selected=set(result['selected_wallets']);audit=result.get('pool_audit')
    full=audit['rows_csv'].splitlines() if audit else []
    while len(json.dumps(result,allow_nan=False).encode())>16000:
        removable=[i for i,r in enumerate(result['scores']) if r['wallet'] not in selected]
        if removable:result['scores'].pop(removable[-1]);continue
        if not audit or audit['shown_rows']<=1:raise ValueError('research_report_too_large')
        size=max(1,audit['shown_rows']//2);start=(int(now)//30*size)%len(full)
        page=(full[start:]+full[:start])[:size]
        audit.update(rows_csv='\n'.join(page)+'\n',shown_rows=size,complete=False,page_start=start)
    return result
