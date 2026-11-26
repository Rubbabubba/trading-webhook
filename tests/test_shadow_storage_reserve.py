from types import SimpleNamespace
from unittest.mock import patch
import sqlite3

import pytest

from opportunity_lab import kalshi_external_sleeves_worker as sleeves
from opportunity_lab import sports_persistent_passive_worker as sports
from opportunity_lab.kalshi_shadow_storage import MIN_FREE, ShadowDiskReserveReached


@pytest.mark.parametrize('worker',[sports, sleeves])
@pytest.mark.parametrize('free',[0, MIN_FREE])
def test_reserve_blocks_before_database_open(worker, free, tmp_path):
    with patch('shutil.disk_usage', return_value=SimpleNamespace(free=free)), patch.object(worker,'open_db') as opened:
        with pytest.raises(ShadowDiskReserveReached):
            worker.run(tmp_path, cycles=1)
        opened.assert_not_called()
        assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize('worker',[sports, sleeves])
def test_lost_headroom_closes_connection_before_collection(worker, tmp_path):
    db = sqlite3.connect(':memory:')
    with patch('shutil.disk_usage',side_effect=[SimpleNamespace(free=2*MIN_FREE),SimpleNamespace(free=0)]), \
            patch.object(worker,'open_db',return_value=db), patch.object(worker,'DemoMarkets'):
        with pytest.raises(ShadowDiskReserveReached):
            worker.run(tmp_path,cycles=1)
    with pytest.raises(sqlite3.ProgrammingError,match='closed'):
        db.execute('SELECT 1')
