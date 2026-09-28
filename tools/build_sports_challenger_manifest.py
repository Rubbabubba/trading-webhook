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


def demo_markets(client, series, first_date, last_date=None):
    rows, cursor = [], None
    while True:
        params = {'series_ticker': series, 'status': 'open', 'limit': 1000,
                  'mve_filter': 'exclude'}
        if cursor:
            params['cursor'] = cursor
        page, _, _ = client.get(params=params)
        for market in page.get('markets', []):
            day = event_date(market.get('event_ticker', ''))
            if (day and day >= first_date and (last_date is None or day <= last_date)
                    and 'COPY' not in market.get('event_ticker', '').upper()):
                rows.append(market)
        cursor = page.get('cursor') or None
        if not cursor:
            return rows


def build(now, days=None, client=None, fetch=public_json):
    client = client or DemoMarkets()
    first_date = now.date().isoformat()
    last_date = ((now + timedelta(days=days)).date().isoformat()
                 if days is not None else None)
    manifest = {
        'strategy_id': 'sports_persistent_passive_v1_shadow',
        'execution_enabled': False,
        'generated_at': now.isoformat(),
        'first_date': first_date,
        'last_date': last_date,
        'horizon_policy': ('bounded_days' if days is not None else
                           'all_currently_open_upcoming_events_in_registered_series'),
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
        mapped_catalog_events = {row['market_event'] for row in mapped}
        catalog_events = sorted({m['event_ticker'] for m in markets})
        unmatched_catalog = [{
            'event_ticker': ticker,
            'date': event_date(ticker),
            'reason': 'no_unique_exact_schedule_match',
        } for ticker in catalog_events if ticker not in mapped_catalog_events]
        manifest['coverage'][league] = {
            'catalog_events': len(catalog_events),
            'schedule_events': len(events),
            'mapped_events': len(mapped),
            'unmatched_schedule_events': unmatched,
            'unmatched_catalog_events': unmatched_catalog,
            'catalog_accounted_for': len(mapped_catalog_events) + len(unmatched_catalog),
        }
    manifest['games'].sort(key=lambda row: (row['kickoff'], row['league'], row['event_id']))
    manifest['mapped_events'] = len(manifest['games'])
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--days', type=int, default=None,
                        help='Optional bounded horizon; default covers every open upcoming event')
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
