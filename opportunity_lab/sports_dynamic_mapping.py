"""Current multi-tournament schedule parsing for the passive sports challenger."""


def schedule_events(payload, league):
    if league not in ('atp', 'wta'):
        return payload.get('events', [])
    wanted = 'mens-singles' if league == 'atp' else 'womens-singles'
    result = []
    for tournament in payload.get('events', []):
        for group in tournament.get('groupings', []):
            if group.get('grouping', {}).get('slug') != wanted:
                continue
            for competition in group.get('competitions', []):
                people = competition.get('competitors', [])
                if len(people) != 2 or not competition.get('date'):
                    continue
                result.append({
                    'id': str(competition['id']),
                    'date': competition['date'],
                    'name': ' vs '.join(
                        c.get('athlete', {}).get('displayName', '?') for c in people
                    ),
                    'competitions': [competition],
                    'tournament_id': str(tournament.get('id', '')),
                    'tournament_name': tournament.get('name'),
                })
    return result
