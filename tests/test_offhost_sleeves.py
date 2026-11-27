from unittest.mock import patch
import pytest

from opportunity_lab import kalshi_external_sleeves_worker as sleeves


def test_archived_sleeves_cannot_reinitialize(tmp_path):
    (tmp_path/'research_sleeves.offhost.json').write_text('{}')
    with patch.object(sleeves,'open_db',side_effect=AssertionError), pytest.raises(RuntimeError,match='archived_offhost'):
        sleeves.run(tmp_path,cycles=1)
    assert not (tmp_path/'research_sleeves.sqlite3').exists()
