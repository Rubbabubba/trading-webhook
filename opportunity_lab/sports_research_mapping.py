"""Exact participant-name and local-date joins; ambiguous matches are rejected."""
from collections import defaultdict
from datetime import timedelta
import re
import unicodedata
from zoneinfo import ZoneInfo

from .college_football_paper import timestamp
from .sports_capture_expansion import event_date, in_scope


def normalized(value):
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode().lower()
    value = value.replace('&', ' and ')
    value = re.sub(r'\bst\.?\b', 'state', value)
    value = re.sub(r'\b(fc|afc|cf|sc)\b', '', value)
    return re.sub('[^a-z0-9]', '', value)


# Explicit display-name equivalences only; never substring or surname matching.
ALIASES = {'mls': {'Inter Miami CF': ['Miami'], 'LAFC': ['Los Angeles F'],
    'LA Galaxy': ['Los Angeles G'], 'Red Bull New York': ['New York RB'],
    'St. Louis CITY SC': ['Saint Louis'], 'Real Salt Lake': ['Salt Lake'],
    'New York City FC': ['New York City'], 'CF Montréal': ['Montreal']},
    'epl': {'Tottenham Hotspur': ['Tottenham']},
    'ncaaf': {'Miami': ['Miami (FL)'], 'Miami (OH)': ['Miami (Ohio)'],
              'Massachusetts': ['UMass'], 'UConn': ['Connecticut'],
              'NC State': ['North Carolina St.'], 'App State': ['Appalachian St.'],
              'UT Martin': ['Tennessee-Martin'], 'UL Monroe': ['Louisiana-Monroe'],
              'Central Connecticut': ['Central Connecticut St.'],
              'Southern': ['Southern University'], 'Grambling': ['Grambling St.']}}


def names(team, league):
    obj = team.get('athlete', team.get('team', {}))
    values = [obj.get(k, '') for k in ('displayName', 'fullName', 'shortDisplayName', 'location')]
    for value in list(values):
        values.extend(ALIASES.get(league, {}).get(value, []))
    return {normalized(v) for v in values if v}


def schedule_events(payload, league):
    if league not in ('atp', 'wta'):
        return payload.get('events', [])
    result = []
    group_name = 'mens-singles' if league == 'atp' else 'womens-singles'
    for tournament in payload.get('events', []):
        if tournament.get('name') != 'US Open':
            continue
        for group in tournament.get('groupings', []):
            if group.get('grouping', {}).get('slug') != group_name:
                continue
            for competition in group.get('competitions', []):
                people = competition.get('competitors', [])
                if len(people) != 2:
                    continue
                result.append({'id': str(competition['id']), 'date': competition['date'],
                    'name': ' vs '.join(c.get('athlete', {}).get('displayName', '?') for c in people),
                    'competitions': [competition], 'tournament_id': tournament['id']})
    return result


def match(event, markets, league, source):
    competition = event['competitions'][0]
    teams = {c.get('homeAway'): c for c in competition.get('competitors', [])}
    if set(teams) != {'home', 'away'}:
        return None
    day = timestamp(event['date']).astimezone(ZoneInfo('America/New_York')).date().isoformat()
    groups = defaultdict(list)
    for market in markets:
        if event_date(market['event_ticker']) == day:
            groups[market['event_ticker']].append(market)
    matches = []
    for key, rows in groups.items():
        mapped = {}
        for side, team in teams.items():
            candidates = [m for m in rows if normalized(m.get('yes_sub_title', '')) in names(team, league)]
            if len(candidates) == 1:
                mapped[side] = candidates[0]
        if set(mapped) != {'home', 'away'} or mapped['home']['ticker'] == mapped['away']['ticker']:
            continue
        if league in ('epl', 'mls'):
            draws = [m for m in rows if normalized(m.get('yes_sub_title', '')) in {'tie', 'draw'}]
            if len(draws) != 1 or len(rows) != 3:
                continue
            mapped['draw'] = draws[0]
            if any('90 minutes plus stoppage time' not in m.get('rules_primary', '') for m in mapped.values()):
                continue
        elif len(rows) != 2:
            continue
        if any(m.get('market_type') != 'binary' for m in mapped.values()):
            continue
        matches.append({'league': league, 'event_id': str(event['id']), 'game': event['name'],
            'home_team_id': str(teams['home']['id']), 'away_team_id': str(teams['away']['id']),
            'markets': {s: m['ticker'] for s, m in mapped.items()}, 'market_event': key,
            'kickoff': timestamp(event['date']).isoformat(),
            'stop_at': (timestamp(event['date']) + timedelta(hours=12)).isoformat(),
            'sport': source['sport'], 'series': source['series'], 'archive': 'expansion',
            'mapping_evidence': {'date_eastern': day, 'participants': {s: sorted(names(t, league)) for s, t in teams.items()},
                'markets': {s: {k: m.get(k) for k in ('ticker', 'yes_sub_title', 'rules_primary', 'rules_secondary')} for s, m in mapped.items()}}})
    return matches[0] if len(matches) == 1 else None


def discover(cache, sources, window):
    configs, missing = [], []
    for league, source in sources.items():
        record = cache.get((league, 'scoreboard'), {})
        payload = record.get('data', {})
        # Collector discovery may paginate; caller combines all latest pages.
        markets = [m for m in cache.get((league, 'market_list'), {}).get('data', []) if in_scope(m, window, source['series'])]
        for event in schedule_events(payload, league):
            day = timestamp(event['date']).astimezone(ZoneInfo('America/New_York')).date().isoformat()
            if not window['first_date'] <= day <= window['last_date']:
                continue
            config = match(event, markets, league, source)
            if config:
                configs.append(config)
            else:
                missing.append({'league': league, 'id': event['id'], 'game': event['name'], 'reason': 'No unique exact participant/date/rule match'})
    return configs, missing
