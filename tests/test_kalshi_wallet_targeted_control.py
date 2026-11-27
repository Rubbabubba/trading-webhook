from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from unittest.mock import patch

from opportunity_lab import kalshi_wallet_targeted_control as capture


def test_targeted_roster_uses_correct_series_horizon_parent_and_price_across_pages():
    now = 1800000000
    def market(parent, hours, ask):
        return dict(event_ticker='KXNBAGAME-'+parent, ticker='KXNBAGAME-'+parent+'-A',
                    market_type='binary', yes_ask_dollars=ask, no_ask_dollars='0.90',
                    expected_expiration_time=datetime.fromtimestamp(now+hours*3600+600,timezone.utc).isoformat())
    pages = [{'cursor':'next','markets':[market('OWN',2,'0.52'),market('LATE',6,'0.52')]},
             {'markets':[market('USED',2,'0.52'),market('WRONGPRICE',2,'0.41'),market('GOOD',2,'0.53')]}]
    def read(url,params):
        assert url == capture.DEMO+'/markets' and params['series_ticker']=='KXNBAGAME'
        assert params.get('cursor') == (None if len(pages)==2 else 'next')
        return (pages.pop(0),)
    wallet = dict(series='KXNBAGAME',parent='NBA:OWN',ask_cents='52',received_at=now,expected_settlement=now+8000)
    rows=capture.shortlist(wallet,{'NBA:USED'},read,lambda:now)
    assert [r[:3] for r in rows] == [(Decimal(1),'KXNBAGAME-GOOD-A','yes')]


def test_low_space_creates_no_trial_and_sends_no_requests(tmp_path):
    with patch.object(capture.shutil,'disk_usage') as usage:
        usage.return_value.free=capture.trial.FREE_FLOOR+capture.trial.MAX_BYTES-1
        capture.run(tmp_path)
    assert list(tmp_path.iterdir())==[]


def test_registration_bytes_and_new_archive_are_distinct():
    p=Path(capture.__file__).resolve().parents[1]/'configs'/capture.REGISTER/'registration.json'
    assert sha256(p.read_bytes().replace(b'\r\n',b'\n')).hexdigest()==capture.HASH
    assert capture.DIRECTORY!=capture.trial.DIRECTORY
