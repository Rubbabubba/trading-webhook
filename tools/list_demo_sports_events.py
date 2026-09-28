"""List currently open Demo sports events without credentials or order access."""
import argparse
from collections import defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path
import re

from opportunity_lab.kalshi_demo_market_data import DemoMarkets


SERIES = {
    'ncaaf': 'KXNCAAFGAME',
    'nfl': 'KXNFLGAME',
    'mlb': 'KXMLBGAME',
    'epl': 'KXEPLGAME',
    'mls': 'KXMLSGAME',
    'atp': 'KXATPMATCH',
    'wta': 'KXWTAMATCH',
}


def catalog_state(event_ticker, observed_at):
    match = re.search(r'-(\d{2})', event_ticker)
    expected = observed_at.strftime('%y')
    if not match or match.group(1) != expected or 'COPY' in event_ticker.upper():
        return 'rejected_stale_or_copied_ticker'
    return 'awaiting_schedule_mapping_and_pregame_anchor'


def discover(client, series):
    events = defaultdict(list)
    cursor = None
    while True:
        params = {'status': 'open', 'limit': 1000, 'series_ticker': series,
                  'mve_filter': 'exclude'}
        if cursor:
            params['cursor'] = cursor
        page, _, _ = client.get(params=params)
        for market in page.get('markets', []):
            events[market.get('event_ticker') or market['ticker']].append({
                key: market.get(key) for key in (
                    'ticker', 'title', 'yes_sub_title', 'status', 'close_time',
                    'yes_bid_dollars', 'yes_ask_dollars', 'volume_24h_fp')
            })
        cursor = page.get('cursor') or None
        if not cursor:
            return events


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', default='sports_paper/strategy_review_20260928/demo_open_events.json')
    args = parser.parse_args()
    client = DemoMarkets()
    observed_at = datetime.now(timezone.utc)
    result = {'observed_at': observed_at.isoformat(), 'environment': 'demo',
              'credential_use': False, 'families': {}}
    for family, series in SERIES.items():
        events = discover(client, series)
        rows = [{'event_ticker': event, 'catalog_state': catalog_state(event, observed_at),
                 'markets': markets} for event, markets in sorted(events.items())]
        result['families'][family] = {
            'series_ticker': series,
            'event_count': len(events),
            'market_count': sum(map(len, events.values())),
            'current_candidate_count': sum(row['catalog_state'].startswith('awaiting_') for row in rows),
            'rejected_catalog_count': sum(row['catalog_state'].startswith('rejected_') for row in rows),
            'events': rows,
        }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2), encoding='utf-8')
    print(json.dumps({'output': str(output), 'counts': {
        family: row['event_count'] for family, row in result['families'].items()}}))


if __name__ == '__main__':
    main()
