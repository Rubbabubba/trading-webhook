import pytest
from opportunity_lab.kalshi_order_journal import Journal


def reserve(j, id="one"):
    return j.reserve(id, "TEST", 2, 40, 4, cash_cents=50000)


def test_crash_restart_never_resends(tmp_path):
    path = tmp_path / "journal.sqlite3"
    j = Journal(path)
    assert reserve(j)
    assert j.mark_submission_started("one")["side"] == "bid"
    j.close()
    j = Journal(path)
    assert not reserve(j)
    with pytest.raises(ValueError, match="unreconciled"):
        j.mark_submission_started("one")
    with pytest.raises(ValueError, match="unreconciled"):
        reserve(j, "two")
    j.reconcile("one", broker_id="broker", filled=1, remaining=0, terminal=True)
    assert reserve(j, "two")
    assert j.db.execute("SELECT reserve FROM intents WHERE id='one'").fetchone()[0] == 84
    j.close()


def test_identity_and_fill_regression(tmp_path):
    j = Journal(tmp_path / "journal.sqlite3")
    reserve(j)
    j.mark_submission_started("one")
    j.reconcile("one", broker_id="broker", filled=1, remaining=1, terminal=False)
    with pytest.raises(ValueError, match="inconsistent"):
        j.reconcile("one", broker_id="broker", filled=0, remaining=2, terminal=False)
    with pytest.raises(ValueError, match="identity"):
        j.reconcile("one", broker_id="different", filled=1, remaining=1, terminal=False)
    j.close()


def test_kill_switch_survives_restart(tmp_path):
    path = tmp_path / "journal.sqlite3"
    j = Journal(path)
    reserve(j)
    j.stop()
    j.close()
    j = Journal(path)
    with pytest.raises(ValueError, match="stopped"):
        reserve(j, "two")
    with pytest.raises(ValueError, match="stopped"):
        j.mark_submission_started("one")
    j.close()


def test_budget_and_id_reuse(tmp_path):
    j = Journal(tmp_path / "journal.sqlite3")
    reserve(j)
    with pytest.raises(ValueError, match="different_intent"):
        j.reserve("one", "OTHER", 2, 40, 4, cash_cents=50000)
    with pytest.raises(ValueError, match="budget"):
        j.reserve("large", "TEST", 20, 40, 4, cash_cents=50000)
    with pytest.raises(ValueError, match="budget"):
        j.reserve("cash", "TEST", 2, 40, 4, cash_cents=100)
    j.close()
