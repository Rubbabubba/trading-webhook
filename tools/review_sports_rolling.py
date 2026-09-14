"""Read-only rolling checkpoints for prospective sports paper experiments."""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3


def assess(db, decisions):
    reviewed = set(decisions.get('reviewed_checkpoints', []))
    sports = {}
    for slug, config_raw, _, _, state in db.execute('SELECT * FROM games'):
        config = json.loads(config_raw)
        league = config['league']
        sport = sports.setdefault(league, {'games': [], 'completed_games': [], 'unresolved_games': [],
            'accounts': {}, 'in_play_samples': 0, 'valid_signal_samples': 0, 'blockers': Counter(),
            'closed_entry_keys': set(), 'traded_games': set(), 'closed_trade_games': set(), 'no_trade_games': []})
        sport['games'].append(slug)
        accounts = [(h, json.loads(raw)) for h, raw in db.execute('SELECT horizon,state FROM accounts WHERE slug=?', (slug,))]
        terminal = state in ('completed', 'deadline_reached')
        if terminal:
            sport['completed_games'].append(slug)
        if any(a['position'] for _, a in accounts):
            sport['unresolved_games'].append(slug)
        if terminal and not any(a['entries'] for _, a in accounts):
            sport['no_trade_games'].append(slug)
        for horizon, account in accounts:
            variant = sport['accounts'].setdefault(str(horizon), {'entries': 0, 'exits': 0,
                'realized_cents': 0, 'fees_cents': 0, 'slippage_cents': 0, 'worst_game_drawdown_cents': 0})
            for key in ('entries', 'exits', 'realized_cents', 'fees_cents', 'slippage_cents'):
                variant[key] += account[key]
            variant['worst_game_drawdown_cents'] = max(variant['worst_game_drawdown_cents'], account['drawdown_cents'])
            if account['entries']:
                sport['traded_games'].add(slug)
        for raw, in db.execute('SELECT observation FROM samples WHERE slug=?', (slug,)):
            model = json.loads(raw)['model']
            if model.get('state') != 'in' or model.get('completed'):
                continue
            sport['in_play_samples'] += 1
            sport['valid_signal_samples'] += int(not model['blockers'] and bool(model['probabilities']))
            sport['blockers'].update(model['blockers'])
        for raw, in db.execute('SELECT a.detail FROM actions a JOIN samples s ON a.sample_id=s.id WHERE s.slug=?', (slug,)):
            for action in json.loads(raw):
                if action['action'] in ('sell', 'settle'):
                    position = action['position']
                    # Same entry simulated with two holding limits is one
                    # entry episode for checkpoint counts, not two samples.
                    sport['closed_entry_keys'].add((slug, position['ticker'], position['opened_at']))
                    sport['closed_trade_games'].add(slug)
    checkpoints = []
    for league, sport in sports.items():
        completed = sorted(sport['completed_games'])
        prefix = league + ':strategy:'
        reviewed_games = {slug for key in reviewed if key.startswith(prefix)
                          for slug in key[len(prefix):].split(',')}
        remaining = [slug for slug in completed if slug not in reviewed_games]
        for start in range(0, len(remaining) - 2, 3):
            # Identity includes the cohort, not just its count. Journal entries
            # preserve which games actually informed a candidate change.
            cohort = remaining[start:start + 3]
            key = league + ':strategy:' + ','.join(cohort)
            if key not in reviewed:
                checkpoints.append({'id': key, 'league': league, 'kind': 'strategy_review',
                                    'completed_games': cohort, 'action': 'Review evidence and consider one separately versioned paper challenger; not automatic parameter tuning.'})
        for slug in sport['no_trade_games']:
            key = league + ':no_trade:' + slug
            if key not in reviewed:
                checkpoints.append({'id': key, 'league': league, 'kind': 'no_trade_diagnostic',
                                    'completed_games': [slug], 'action': 'Explain feed, mapping, liquidity, costs and model gates; do not force a trade.'})
        sport['completed_games'] = completed
        sport['closed_entry_episodes'] = len(sport.pop('closed_entry_keys'))
        sport['traded_games'] = sorted(sport['traded_games'])
        sport['closed_trade_games'] = sorted(sport['closed_trade_games'])
        sport['blockers'] = dict(sport['blockers'].most_common())
        n = sport['in_play_samples']
        sport['valid_signal_fraction'] = sport['valid_signal_samples'] / n if n else None
        sport['candidate_evaluation_screen_met'] = len(sport['closed_trade_games']) >= 5 and sport['closed_entry_episodes'] >= 20
    return {'at': datetime.now(timezone.utc).isoformat(), 'sports': sports, 'pending_checkpoints': checkpoints,
        'note': 'Thresholds are workflow triggers, not statistical proof. Quotes and trades within a game are correlated. Evaluate challengers on future paired games; never combine holding-account P&L into one portfolio.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--ledger', default='sports_paper/research_20260910/paper.sqlite3')
    parser.add_argument('--output', default='sports_paper/rolling_review_20260910')
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    journal = output / 'decisions.json'
    decisions = json.loads(journal.read_text()) if journal.exists() else {'reviewed_checkpoints': [], 'changes': []}
    if not journal.exists():
        journal.write_text(json.dumps(decisions, indent=2))
    db = sqlite3.connect(Path(args.ledger).resolve().as_uri() + '?mode=ro', uri=True)
    try:
        db.execute('BEGIN')  # Consistent read snapshot while the worker writes.
        report = assess(db, decisions)
    finally:
        db.close()
    (output / 'latest.json').write_text(json.dumps(report, indent=2))
    lines = ['# Rolling sports review', '', 'Updated ' + report['at'] + '.', '',
        'Review each completed game. Open a strategy checkpoint after every three completed games within a sport. Diagnose zero-trade games immediately. Operational faults do not wait for a strategy sample.', '',
        '| Sport | Finished games | Games with entries | Closed entry episodes | Valid in-play signal samples |',
        '|---|---:|---:|---:|---:|']
    for league, sport in sorted(report['sports'].items()):
        lines.append(f"| {league.upper()} | {len(sport['completed_games'])} | {len(sport['traded_games'])} | {sport['closed_entry_episodes']} | {sport['valid_signal_samples']} / {sport['in_play_samples']} |")
    lines += ['', '## Pending checkpoints', '']
    lines += [f"- {p['league'].upper()}: {p['kind']} ({len(p['completed_games'])} finished games)." for p in report['pending_checkpoints']] or ['None yet. Games are still pending or already reviewed.']
    lines += ['', report['note'], '', 'Detailed costs, exclusions and separate holding-account returns are in latest.json. decisions.json records reviewed checkpoints and candidate changes; the checker never marks its own output as a completed review.']
    (output / 'latest.md').write_text('\n'.join(lines), encoding='utf-8')
    print(json.dumps({'sports': len(report['sports']), 'pending_checkpoints': len(report['pending_checkpoints']), 'report': str(output / 'latest.md')}))


if __name__ == '__main__':
    main()
