from datetime import datetime, timezone

from opportunity_lab.kalshi_binary_journal import BinaryJournal
from opportunity_lab.kalshi_demo_v5_maker_worker import MakerState, current_position, evidence


def snapshot(cash=50000):
    now = datetime.now(timezone.utc).timestamp()
    return {"environment": "demo", "started_at": now, "observed_at": now,
            "balance": {"balance": cash}, "positions": [], "resting_orders": []}


def test_empty_maker_evidence_is_demo_only_and_flat(tmp_path):
    state = MakerState(tmp_path / "state.sqlite3")
    journal = BinaryJournal(tmp_path / "journal.sqlite3", order_limit_cents=110,
                            capital_limit_cents=160, daily_loss_cents=100)
    try:
        result = evidence(state, journal)
        assert result["environment"] == "demo" and result["post_only"] is True
        assert result["post_only_attempts"] == result["maker_fills"] == 0
        assert result["ending_position_contracts"] == 0
    finally:
        journal.close(); state.close()


def test_current_position_requires_matching_maker_metadata(tmp_path):
    state = MakerState(tmp_path / "state.sqlite3")
    journal = BinaryJournal(tmp_path / "journal.sqlite3", order_limit_cents=110,
                            capital_limit_cents=160, daily_loss_cents=100)
    try:
        journal.reserve("m1", "TEST", 1, 40, 5, outcome="yes", action="buy",
                        account_snapshot=snapshot(), order_mode="post_only_gtc")
        state.db.execute("INSERT INTO intent_meta VALUES(?,?,?,?,?,?)",
                         ("m1", "maker_entry", "EVENT", "TEST", "yes", 100.0))
        position, accounting = current_position(journal, state)
        assert position is None and accounting["positions"] == {}
    finally:
        journal.close(); state.close()
