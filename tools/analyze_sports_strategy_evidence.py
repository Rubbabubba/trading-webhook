"""Produce a reproducible cost attribution from the frozen sports v3.5 report."""
import argparse
from collections import defaultdict
import json
from pathlib import Path


def summarize(report):
    totals = defaultdict(lambda: defaultdict(lambda: {
        'games': 0, 'traded_games': set(), 'entries': 0, 'net_cents': 0,
        'fees_cents': 0, 'slippage_cents': 0,
    }))
    for game in report['games']:
        for horizon, account in game['accounts'].items():
            row = totals[game['league']][horizon]
            row['games'] += 1
            row['entries'] += account['entries']
            row['net_cents'] += account['realized_cents']
            row['fees_cents'] += account['fees_cents']
            row['slippage_cents'] += account['slippage_cents']
            if account['entries']:
                row['traded_games'].add(game['slug'])
    result = {}
    for league, horizons in sorted(totals.items()):
        result[league] = {}
        for horizon, row in sorted(horizons.items()):
            costs = row['fees_cents'] + row['slippage_cents']
            result[league][horizon] = {
                **{k: v for k, v in row.items() if k != 'traded_games'},
                'traded_games': len(row['traded_games']),
                'modeled_transaction_cost_cents': costs,
                'gross_before_modeled_cost_cents': row['net_cents'] + costs,
            }
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report', default='sports_paper/live_v35_20260915/report.json')
    parser.add_argument('--output', default='sports_paper/strategy_review_20260928/cost_attribution.json')
    args = parser.parse_args()
    result = {
        'source': args.report,
        'scope': 'Frozen cumulative sports_live_3.5 evidence ending 2026-09-18; holding horizons are separate counterfactual sleeves and must not be added together.',
        'by_league_and_horizon': summarize(json.loads(Path(args.report).read_text())),
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2), encoding='utf-8')
    print(json.dumps({'output': str(output), 'leagues': len(result['by_league_and_horizon'])}))


if __name__ == '__main__':
    main()
