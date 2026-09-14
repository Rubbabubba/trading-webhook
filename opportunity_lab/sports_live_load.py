"""Bounded complete-book handoff; coalescing is explicit, never dropping input deltas."""
from collections import OrderedDict
import threading


class LatestBooks:
    def __init__(self, capacity=1024):
        self.capacity = capacity
        self.lock = threading.Lock()
        self.pending = OrderedDict()
        self.replaced = self.high_water = 0

    def put(self, item):
        with self.lock:
            if item['kind'] == 'stream_state':
                # A new session may never expose cached books from an earlier session.
                self.pending.clear()
                self.pending['@state'] = item
            else:
                key = item['ticker']
                if key in self.pending:
                    self.replaced += 1
                elif len(self.pending) >= self.capacity:
                    raise RuntimeError('mailbox_capacity_exceeded')
                self.pending[key] = item
            self.high_water = max(self.high_water, len(self.pending))

    def drain(self):
        with self.lock:
            items = list(self.pending.values())
            self.pending.clear()
            return items

    def stats(self):
        with self.lock:
            return dict(pending=len(self.pending), high_water=self.high_water,
                        coalesced_complete_views=self.replaced, capacity=self.capacity)


def fresh_stream(book, now):
    return bool(book and book.get('transport') == 'websocket' and book.get('valid')
                and 0 <= now-book['at'] <= 5)


class RateBudget:
    def __init__(self):
        self.until = 0
        self.next_request = 0
        self.consecutive = 0
        self.incidents = 0
        self.last_incident = None

    def limited(self, now):
        self.consecutive += 1
        self.incidents += 1
        self.last_incident = now
        self.until = now + min(120, 15 * 2 ** min(self.consecutive, 3))

    def success(self):
        self.consecutive = 0

    def ready(self, now):
        return now >= max(self.until, self.next_request)

    def reserve(self, now, interval=1):
        self.next_request = now + interval
