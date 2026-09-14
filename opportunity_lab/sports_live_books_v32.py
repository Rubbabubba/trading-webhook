"""Read-only Kalshi book reconstruction and authenticated stream transport."""
import asyncio
import base64
from decimal import Decimal
import json
import os
from pathlib import Path
import time

WS_URL = 'wss://external-api-ws.kalshi.com/trade-api/ws/v2'


class Book:
    def __init__(self):
        self.levels = {'yes': {}, 'no': {}}
        self.valid = False
        self.at = 0.0
        self.revision = None

    def snapshot(self, yes, no, at, revision):
        levels = {}
        for side, rows in [('yes', yes), ('no', no)]:
            levels[side] = {}
            for price, size in rows:
                p, n = Decimal(str(price)), Decimal(str(size))
                if not p.is_finite() or not n.is_finite() or not 0 <= p <= 1 or n < 0:
                    raise ValueError('invalid_book_level')
                if n:
                    levels[side][p] = n
        self.levels, self.at, self.revision, self.valid = levels, at, revision, True
        self._check_cross()

    def _check_cross(self):
        if all(self.levels.values()) and max(self.levels['yes']) + max(self.levels['no']) > 1:
            self.valid = False
            raise ValueError('crossed_book')

    def delta(self, side, price, size, at, revision):
        if not self.valid or side not in self.levels:
            raise ValueError('snapshot_required')
        p, change = Decimal(str(price)), Decimal(str(size))
        if not p.is_finite() or not change.is_finite() or not 0 <= p <= 1:
            raise ValueError('invalid_book_delta')
        n = self.levels[side].get(p, Decimal(0)) + change
        if n < 0:
            self.valid = False
            raise ValueError('negative_book_size')
        if n:
            self.levels[side][p] = n
        else:
            self.levels[side].pop(p, None)
        self.at, self.revision = at, revision
        self._check_cross()

    def quote(self, side='yes'):
        other = 'no' if side == 'yes' else 'yes'
        if not self.valid or not self.levels[side] or not self.levels[other]:
            return None
        bids = [[float(p), float(n)] for p, n in sorted(self.levels[side].items(), reverse=True)]
        asks = [[float(1-p), float(n)] for p, n in sorted(self.levels[other].items(), reverse=True)]
        return dict(bid=bids[0][0], bid_size=bids[0][1], ask=asks[0][0], ask_size=asks[0][1],
                    bids=bids, asks=asks)


class StreamBooks:
    """A sequence discontinuity invalidates the session, never patches missing deltas."""
    def __init__(self):
        self.books, self.sequence = {}, {}

    def apply(self, message, at):
        kind = message.get('type')
        if kind not in ('orderbook_snapshot', 'orderbook_delta'):
            return None
        sid, seq = message['sid'], int(message['seq'])
        # One subscription per session. Track its sequence, not per-market counters.
        if sid in self.sequence and seq != self.sequence[sid] + 1:
            for book in self.books.values():
                book.valid = False
            raise ValueError('stream_sequence_gap')
        self.sequence[sid] = seq
        msg = message['msg']; ticker = msg['market_ticker']
        book = self.books.setdefault(ticker, Book())
        if kind == 'orderbook_snapshot':
            book.snapshot(msg.get('yes_dollars_fp') or [], msg.get('no_dollars_fp') or [], at, (sid, seq))
        else:
            book.delta(msg['side'], msg['price_dollars'], msg['delta_fp'], at, (sid, seq))
        return ticker


def credentials():
    key_id = os.getenv('KALSHI_API_KEY_ID')
    key_path = os.getenv('KALSHI_PRIVATE_KEY_PATH')
    return (key_id, Path(key_path)) if key_id and key_path and Path(key_path).is_file() else None


def auth_headers(key_id, path, now_ms=None):
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
    key = serialization.load_pem_private_key(Path(path).read_bytes(), password=None)
    stamp = str(now_ms if now_ms is not None else int(time.time()*1000))
    signature = key.sign((stamp+'GET/trade-api/ws/v2').encode(),
                         padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
                         hashes.SHA256())
    return {'KALSHI-ACCESS-KEY': key_id, 'KALSHI-ACCESS-TIMESTAMP': stamp,
            'KALSHI-ACCESS-SIGNATURE': base64.b64encode(signature).decode()}


async def stream_loop(targets, publish, stop):
    """Only subscribes to market data. No account/order APIs or header logging."""
    import websockets
    backoff = 1
    while not stop.is_set():
        creds = credentials()
        tickers = sorted(targets())
        if not creds or not tickers:
            publish({'kind': 'stream_state', 'state': 'credentials_missing' if not creds else 'no_targets'})
            await asyncio.sleep(5)
            continue
        try:
            async with websockets.connect(WS_URL, additional_headers=auth_headers(*creds),
                                          ping_interval=10, ping_timeout=10, open_timeout=10,
                                          max_queue=2048) as ws:
                await ws.send(json.dumps({'id': 1, 'cmd': 'subscribe', 'params':
                    {'channels': ['orderbook_delta'], 'market_tickers': tickers}}))
                books = StreamBooks(); started = time.monotonic(); backoff = 1
                last_snapshot=started;command_id=1
                publish({'kind': 'stream_state', 'state': 'connected'})
                while not stop.is_set():
                    # Periodic fresh sessions recover snapshots even for quiet markets.
                    if time.monotonic()-started > 60 or sorted(targets()) != tickers:
                        break
                    if books.sequence and time.monotonic()-last_snapshot >= 5:
                        command_id+=1
                        await ws.send(json.dumps({'id':command_id,'cmd':'update_subscription','params':
                            {'sid':next(iter(books.sequence)),'action':'get_snapshot','market_tickers':tickers}}))
                        last_snapshot=time.monotonic()
                    try:
                        message = json.loads(await asyncio.wait_for(ws.recv(), timeout=2))
                    except asyncio.TimeoutError:
                        continue
                    if message.get('type') == 'error':
                        raise ValueError('stream_server_error')
                    at = time.time(); ticker = books.apply(message, at)
                    if ticker:
                        publish({'kind': 'stream_book', 'ticker': ticker, 'at': at,
                                 'message': message, 'quote': books.books[ticker].quote(),
                                 'revision': books.books[ticker].revision})
        except Exception as exc:
            # Exception type only: client exceptions can contain authenticated headers.
            publish({'kind': 'stream_state', 'state': 'disconnected', 'error': type(exc).__name__})
        finally:
            publish({'kind': 'stream_state', 'state': 'resnapshot_required'})
        await asyncio.sleep(backoff)
        backoff = min(30, backoff*2)
