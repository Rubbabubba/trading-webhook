"""Build an exact-match, shadow-only challenger manifest from current Demo listings."""
import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from opportunity_lab.kalshi_demo_market_data import DemoMarkets
from opportunity_lab.sports_capture_expansion import event_date
from opportunity_lab.sports_dynamic_mapping import schedule_events
from opportunity_lab.sports_research_mapping import match


SOURCES = {
    'ncaaf': {'series': 'KXNCAAFGAME', 'sport': 'football/college-football'},
    'nfl': {'series': 'KXNFLGAME', 'sport': 'football/nfl'},
    'mlb': {'series': 'KXMLBGAME', 'sport': 'baseball/mlb'},
    'epl': {'series': 'KXEPLGAME', 'sport': 'soccer/eng.1'},
    'mls': {'series': 'KXMLSGAME', 'sport': 'soccer/usa.1'},
    'atp': {'series': 'KXATPMATCH', 'sport': 'tennis/atp'},
    'wta': {'series': 'KXWTAMATCH', 'sport': 'tennis/wta'},
}


def public_json(url):
    # ESPN's public JSON endpoint rejects some custom User-Agent strings.
    with urlopen(Request(url, headers={'Accept': 'application/json'}), timeout=15) as response:
        return json.load(response)


def demo_markets(client, series, first_date, last_date):
    rows, cursor = [], None
    while True:
        params = {'series_ticker': series, 'status': 'open', 'limit': 1000,
                  'mve_filter': 'exclude'}
        if cursor:
            params['cursor'] = cursor
        page, _, _ = client.get(params=params)
        rows.extend(m for m in page.get('markets', [])
                    if first_date <= (event_date(m.get('event_ticker', '')) or '') <= last_date
                    and 'COPY' not in m.get('event_ticker', '').upper())
        cursor = page.get('cursor') or None
        if not cursor:
            return rows


def build(now, days, client= None, fetch=public_json):
    client = client or DemoMarkets()
    first_date = now.date().isoformat()
    last_date = (now + timedelta(days=days)).date().isoformat()
    manifest = {
        'strategy_id': 'sports_persistent_passive_v1_shadow',
        'execution_enabled': False,
        'generated_at': now.isoformat(),
        'first_date': first_date,
        'last_date': last_date,
        'games': [],
        'coverage': {},
    }
    for league, source in SOURCES.items():
        markets = demo_markets(client, source['series'], first_date, last_date)
        # Query only dates actually represented in the Demo catalog. Several
        # ESPN league endpoints reject long date ranges even though they accept
        # the same dates individually.
        days_needed = sorted({event_date(m['event_ticker']) for m in markets})
        event_index = {}
        for day in days_needed:
            query = {'dates': day.replace('-', ''), 'limit': 1000}
            if league == 'ncaaf':
                query['groups'] = 80
            url = ('https://site.api.espn.com/apis/site/v2/sports/' + source['sport']
                   + '/scoreboard?' + urlencode(query))
            for event in schedule_events(fetch(url), league):
                event_index[str(event['id'])] = event
        events = list(event_index.values())
        mapped, unmatched = [], []
        for event in events:
            try:
                config = match(event, markets, league, source)
            except (KeyError, TypeError, ValueError):
                config = None
            if config:
                config['execution_enabled'] = False
                config['challenger'] = manifest['strategy_id']
                mapped.append(config)
            else:
                unmatched.append({'event_id': str(event.get('id')), 'game': event.get('name'),
                                  'date': event.get('date'), 'reason': 'no_unique_exact_demo_match'})
        manifest['games'].extend(mapped)
        manifest['coverage'][league] = {
            'catalog_events': len({m['event_ticker'] for m in markets}),
            'schedule_events': len(events),
            'mapped_events': len(mapped),
            'unmatched_schedule_events': unmatched,
        }
    manifest['games'].sort(key=lambda row: (row['kickoff'], row['league'], row['event_id']))
    manifest['mapped_events'] = len(manifest['games'])
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--days', type=int, default=14)
    parser.add_argument('--output', default='configs/sports_persistent_passive_v1_20260928/events.json')
    args = parser.parse_args()
    result = build(datetime.now(timezone.utc), args.days)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2), encoding='utf-8')
    print(json.dumps({'output': str(output), 'mapped_events': result['mapped_events'],
                      'coverage': {k: {x: v[x] for x in ('catalog_events', 'schedule_events', 'mapped_events')}
                                   for k, v in result['coverage'].items()}}))


if __name__ == '__main__':
    main()
