"""Automatic review evidence for the authoritative live-paper continuation."""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import sqlite3
import time
from pathlib import Path
from review_sports_rolling_v2 import snapshot_ledger


def review_snapshot(path):
    # Live diagnostics are statement-consistent, not an accounting cutoff snapshot.
    # Never hold one read transaction across the whole review: the rollback-journal
    # writer has a five-second commit timeout. Final/daily accounting uses its own cutoff.
    source = sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro', uri=True, timeout=3)
    target = sqlite3.connect(':memory:')
    deadline = time.monotonic()+60
    source.set_progress_handler(lambda: int(time.monotonic()>deadline), 10000)
    try:
        for table in ('games', 'accounts', 'probes', 'samples', 'actions'):
            ddl = source.execute('select sql from sqlite_master where type="table" and name=?', (table,)).fetchone()[0]
            target.execute(ddl)
            columns = [r[1] for r in source.execute('pragma table_info('+table+')')]
            if table == 'samples' and 'at' in columns:
                # Read only indexed recent rows. Extract diagnostic fields in SQLite
                # rather than copying full snapshots and historical observations.
                for slug, in source.execute('select slug from games').fetchall():
                    rows = source.execute("""select id,slug,at,snapshot,
                        json_object('model',json_object('state',json_extract(observation,'$.model.state'),
                          'blockers',json_extract(observation,'$.model.blockers')),
                          'markets',json_extract(observation,'$.markets'),
                          'admission_ok',json_extract(observation,'$.admission_ok'))
                        from samples where slug=? order by id desc limit 900""",(slug,))
                    target.executemany('insert into samples values(?,?,?,?,?)', rows)
                continue
            if table == 'actions' and 'account' in columns:
                # Closed episode evidence only; per-observation rejection diagnostics
                # are already available in the independent generated game reports.
                maximum=source.execute('select max(id) from samples').fetchone()[0] or 0
                for start in range(0,maximum+1,500):
                    rows=source.execute("""select a.sample_id,a.horizon,a.detail,s.slug,s.at
                        from samples s join actions a on a.sample_id=s.id
                        where s.id>=? and s.id<? and
                        (a.detail like '%"action": "sell"%' or a.detail like '%"action": "settle"%')""",(start,start+500)).fetchall()
                    for sid,h,detail,slug,at in rows:
                        target.execute('insert or ignore into samples values(?,?,?,?,?)',(sid,slug,at,'','{}'))
                        target.execute('insert into actions values(?,?,?,NULL)',(sid,h,detail))
                    time.sleep(.001)
                continue
            projection = ','.join('NULL' if table=='actions' and c=='account' else c for c in columns)
            cursor = source.execute('select '+projection+' from '+table)
            placeholders = ','.join('?' for _ in cursor.description)
            while rows := cursor.fetchmany(256):
                if time.monotonic()>deadline: raise TimeoutError('review_snapshot_deadline')
                target.executemany('insert into '+table+' values('+placeholders+')', rows)
        target.execute('create index review_samples on samples(slug,id)')
        target.execute('create index review_actions on actions(sample_id)')
        return target
    except Exception:
        target.close()
        raise
    finally:
        source.close()


def assess_live(db, now):
    games = {}
    sports = defaultdict(lambda: {'completed': 0, 'traded_games': set(), 'episodes': set(), 'net_by_horizon': Counter()})
    alerts = []
    probes = [json.loads(r) for r, in db.execute('select state from probes')]
    for slug, raw, _, _, state in db.execute('select * from games'):
        c = json.loads(raw)
        accounts = {str(h): json.loads(a) for h, a in db.execute('select horizon,state from accounts where slug=?', (slug,))}
        sport = sports[c['league']]
        sport['completed'] += state == 'completed'
        entries = max((a['entries'] for a in accounts.values()), default=0)
        if entries: sport['traded_games'].add(slug)
        for h, a in accounts.items(): sport['net_by_horizon'][h] += a['realized_cents']
        blockers = Counter()
        coverage = Counter()
        # Recent diagnostic window, separate from cumulative P&L.
        for raw_obs, in db.execute('select observation from samples where slug=? order by id desc limit 900', (slug,)):
            o = json.loads(raw_obs)
            if not o.get('model'): continue
            if o['model'].get('state') != 'in': continue
            coverage['in_play'] += 1
            blockers.update(o['model']['blockers'])
            coverage['valid_model'] += not bool(o['model']['blockers'])
            coverage['valid_market'] += any(m['valid'] for m in o['markets'].values())
            coverage['admitted'] += bool(o.get('admission_ok'))
        reasons = Counter()
        for h, raw_action in db.execute('select a.horizon,a.detail from actions a join samples s on s.id=a.sample_id where s.slug=?', (slug,)):
            for action in json.loads(raw_action):
                if action.get('reason'): reasons[action['reason']] += 1
                # Horizon variants share an entry episode; partial sales are not full exits.
                if action['action'] in ('sell', 'settle'):
                    p = action['position']
                    sport['episodes'].add((slug, p.get('ticker'), p.get('side'), p.get('opened_at')))
        probe_entries = sum(bool(p.get('entry_at')) for p in probes if p['game'] == slug)
        elapsed = (now-datetime.fromisoformat(c['kickoff'])).total_seconds()
        if elapsed >= 900 and state in ('running', 'completed') and (entries == 0 or probe_entries == 0):
            checkpoint = str(int(now.timestamp())//1800) if state == 'running' else 'completed'
            alerts.append({'id': 'activity:'+slug+':'+checkpoint, 'kind': 'zero_trade_review', 'game': slug,
                           'strategy_entries': entries, 'probe_entries': probe_entries,
                           'diagnosis': 'coverage_or_execution' if not coverage['admitted'] or blockers else 'inspect_cost_adjusted_edge_and_cancellations'})
        games[slug] = {'name': c['game'], 'state': state, 'accounts': accounts,
                       'recent_coverage': coverage, 'blockers': blockers, 'action_reasons': reasons, 'probe_entries': probe_entries}
    for league, sport in sports.items():
        n = sport['completed']
        if n >= 3: alerts.append({'id': f'checkpoint:{league}:{n//3}', 'kind': 'three_game_checkpoint', 'league': league})
        losing = [h for h, net in sport['net_by_horizon'].items() if net < 0]
        if len(sport['traded_games']) >= 5 and len(sport['episodes']) >= 20 and losing:
            alerts.append({'id': f'loss:{league}:{len(sport["episodes"])//5}', 'kind': 'loss_strategy_review', 'league': league, 'losing_horizons': losing})
        sport['traded_games'] = sorted(sport['traded_games'])
        sport['closed_unique_episodes'] = len(sport.pop('episodes'))
    return {'at': now.isoformat(), 'games': games, 'sports': dict(sports), 'alerts': alerts,
            'policy': 'Review triggers are investigation thresholds, not proof of edge. Preserve costs and caps; preregister changes on future games. Strategy P&L and execution probes remain separate.',
            'action_reason_scope': 'This bounded snapshot retains fully closed trade actions only. Consult generated per-game reports for entry rejections, cancellations and partial exits; empty action_reasons here does not mean none occurred.',
            'snapshot_scope': 'Live diagnostics use separate short reads; account and episode totals can differ by activity during collection. Do not use this report as a final or daily accounting snapshot.'}


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--output', default='sports_paper/live_v33_20260912')
    args = p.parse_args()
    output = Path(args.output)
    db = review_snapshot(output/'paper.sqlite3')
    try: report = assess_live(db, datetime.now(timezone.utc))
    finally: db.close()
    dest = output/'automatic_review'
    dest.mkdir(exist_ok=True)
    (dest/'latest.json').write_text(json.dumps(report, indent=2))
    journal = dest/'decisions.json'
    if not journal.exists(): journal.write_text(json.dumps({'resolved_alerts': [], 'changes': []}, indent=2))
    resolved = set(json.loads(journal.read_text())['resolved_alerts'])
    pending = [a for a in report['alerts'] if a['id'] not in resolved]
    (dest/'pending.json').write_text(json.dumps(pending, indent=2))
    print(json.dumps({'report': str(dest/'latest.json'), 'pending_reviews': len(pending)}))


if __name__ == '__main__': main()
