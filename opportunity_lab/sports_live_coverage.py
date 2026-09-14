"""Bounded game/family allocation with dedicated held-contract service."""
from collections import defaultdict
import math


def slug(c):
    return c['game']['league']+'_'+c['game']['event_id']


def core_markets(cache, eligible, held=()):
    groups = defaultdict(lambda: defaultdict(list))
    for ticker, c in cache.items():
        if slug(c) not in eligible or c.get('mapping_changed') or c['market'].get('status') != 'active':
            continue
        # Full-game main series only; period/alternate catalog remains in original lab.
        prefix = {'ncaaf':'KXNCAAF', 'nfl':'KXNFL', 'mlb':'KXMLB', 'epl':'KXEPL',
                  'mls':'KXMLS', 'atp':'KXATP', 'wta':'KXWTA'}.get(c['league'], '')
        family = c['family']
        if c['series'] not in {prefix+'GAME', prefix+'MATCH', prefix+'SPREAD', prefix+'TOTAL'}:
            continue
        groups[slug(c)][family].append(ticker)
    selected = set(held)
    missing = {}
    for game in sorted(eligible):
        missing[game] = []
        for family in ('winner','spread','total'):
            candidates = groups[game].get(family, [])
            if not candidates:
                missing[game].append(family)
                continue
            def liquidity(t):
                try:
                    n = float(cache[t]['market'].get('volume_fp', cache[t]['market'].get('volume', 0)))
                    return n if math.isfinite(n) else 0
                except (ValueError, TypeError):
                    return 0
            # All winner outcomes; most-traded listed line as provisional main line.
            selected.update(candidates if family == 'winner' else sorted(candidates, key=lambda t:(-liquidity(t), t))[:1])
    return selected, missing


def polling_batch(tickers, held, last, now, capacity=8):
    overdue = sorted(tickers, key=lambda t:(last.get(t, 0), t))
    critical = [t for t in overdue if t in held and now-last.get(t, 0) >= 1]
    other = [t for t in overdue if t not in held and now-last.get(t, 0) >= 2]
    # Reserve at least one request for broader coverage even under saturation.
    selected = critical[:max(0,capacity-1)] + other[:1]
    selected += [t for t in critical+other if t not in selected][:capacity-len(selected)]
    return selected


def admission(quote_age, service_gap, pending_contracts, max_pending=20):
    return quote_age <= 5 and service_gap <= 5 and pending_contracts < max_pending
