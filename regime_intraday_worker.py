"""Dedicated scheduler for the regime-intraday paper system.

This process calls only regime-intraday endpoints. It cannot invoke legacy
swing scanning, swing exits, or a live-order endpoint.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, time as clock_time, timezone
from zoneinfo import ZoneInfo


WORKER_VERSION = "regime-intraday-worker-v1"


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _log(message: str) -> None:
    stamp = datetime.now(timezone.utc).isoformat()
    print(f"{stamp} [regime-intraday-worker] {message}", flush=True)


def _post(url: str, payload: dict, timeout: int) -> tuple[int, dict]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/json", "User-Agent": WORKER_VERSION},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read().decode("utf-8", errors="replace")
        return response.status, json.loads(body or "{}")


def main() -> None:
    base_url = (os.getenv("MAIN_SERVICE_URL") or os.getenv("BASE_URL") or "").strip().rstrip("/")
    if not base_url:
        raise RuntimeError("MAIN_SERVICE_URL is required")
    secret = (os.getenv("WORKER_SECRET") or "").strip()
    if not secret:
        raise RuntimeError("WORKER_SECRET is required")

    interval = max(30, _env_int("REGIME_INTRADAY_SCAN_INTERVAL_SEC", 60))
    timeout = max(10, _env_int("REGIME_INTRADAY_WORKER_TIMEOUT_SEC", 60))
    scan_url = (os.getenv("REGIME_INTRADAY_SCAN_URL") or f"{base_url}/worker/regime_intraday_scan").strip()
    reconcile_url = (os.getenv("REGIME_INTRADAY_RECONCILE_URL") or f"{base_url}/worker/regime_intraday_paper_reconcile").strip()
    replay_url = (os.getenv("REGIME_INTRADAY_REPLAY_URL") or f"{base_url}/worker/regime_intraday_after_hours_replay").strip()
    payload = {"worker_secret": secret}
    replay_date = None

    _log(f"boot version={WORKER_VERSION} interval_sec={interval} timeout_sec={timeout}")
    while True:
        started = time.monotonic()
        cycle_failed = False
        ny_now = datetime.now(ZoneInfo("America/New_York"))
        if ny_now.weekday() < 5 and ny_now.time() >= clock_time(16, 5) and replay_date != ny_now.date():
            try:
                validation_days = max(60, min(252, _env_int("REGIME_INTRADAY_VALIDATION_DAYS", 180)))
                status, response = _post(replay_url, {**payload, "calendar_days": validation_days}, max(timeout, 900))
                ranking = list(response.get("ranking") or [])
                _log(f"after_hours_replay_ok http={status} leader={ranking[0] if ranking else 'none'} variants={len(response.get('variants') or {})} live_submission={response.get('live_submission', False)}")
                for name in ranking:
                    variant = dict(dict(response.get("variants") or {}).get(name) or {})
                    net = dict(variant.get("cost_adjusted") or {})
                    _log(
                        f"after_hours_variant name={name} trades={variant.get('trade_count')} raw_avg_r={variant.get('average_r')} "
                        f"net_avg_r={net.get('net_average_r')} net_dollars={net.get('net_total_dollars')} "
                        f"max_drawdown_r={variant.get('max_drawdown_r')} goal100_rate={net.get('daily_goal_100_rate')} goal200_rate={net.get('daily_goal_200_rate')}"
                    )
                walk = dict(response.get("mean_reversion_walk_forward") or {})
                test = dict(walk.get("test") or {})
                test_net = dict(test.get("cost_adjusted") or {})
                _log(
                    f"after_hours_walk_forward ready={walk.get('ready')} parameters={json.dumps(walk.get('selected_parameters') or {}, separators=(',', ':'))} "
                    f"train_sessions={walk.get('train_sessions')} test_sessions={walk.get('test_sessions')} test_trades={test.get('trade_count')} "
                    f"test_net_avg_r={test_net.get('net_average_r')} test_net_dollars={test_net.get('net_total_dollars')} "
                    f"test_drawdown_r={test.get('max_drawdown_r')} out_of_sample_positive={walk.get('out_of_sample_positive')}"
                )
                lab = dict(response.get("validation_lab") or {})
                gate = dict(lab.get("gate") or {})
                instruments = dict(lab.get("instrument_comparison") or {})
                instrument_net = {name: dict(report.get("cost_adjusted") or {}).get("net_average_r") for name, report in instruments.items()}
                candidates = dict(lab.get("candidate_sleeves") or {})
                candidate_net = {name: dict(report.get("ordinary_cost") or {}).get("net_average_r") for name, report in candidates.items()}
                feasibility = dict(lab.get("daily_goal_feasibility") or {})
                goal_risk = {str(row.get("daily_goal_dollars")): row.get("required_risk_per_trade_dollars") for row in list(feasibility.get("goals") or [])}
                dia_test = dict(dict(lab.get("dia_fixed_holdout") or {}).get("test") or {})
                _log(
                    f"after_hours_validation paper_pass={gate.get('paper_validation_pass')} promotion_locked={gate.get('promotion_locked')} "
                    f"blockers={json.dumps(gate.get('blockers') or [], separators=(',', ':'))} "
                    f"instrument_net_avg_r={json.dumps(instrument_net, separators=(',', ':'))} "
                    f"candidate_net_avg_r={json.dumps(candidate_net, separators=(',', ':'))} "
                    f"goal_required_risk={json.dumps(goal_risk, separators=(',', ':'))} "
                    f"dia_test_trades={dia_test.get('trade_count')} dia_test_net_012={dict(dia_test.get('cost_012') or {}).get('net_average_r')} dia_test_net_030={dict(dia_test.get('cost_030') or {}).get('net_average_r')} "
                    f"cost_050_net_avg_r={dict((lab.get('cost_stress') or [{}])[-1]).get('net_average_r')} "
                    f"monte_loss_probability={dict(lab.get('monte_carlo') or {}).get('probability_negative_total')}"
                )
                replay_date = ny_now.date()
            except Exception as error:
                cycle_failed = True
                _log(f"after_hours_replay_error kind={type(error).__name__} detail={str(error)[:300]}")
        for action, url in (("scan", scan_url), ("reconcile", reconcile_url)):
            try:
                status, response = _post(url, payload, timeout)
                attention = [row for row in response.get("refreshed", []) if row.get("status") in {"reconcile_error", "close_requires_attention", "entry_requires_attention"}]
                if attention:
                    cycle_failed = True
                    _log(f"{action}_attention count={len(attention)} statuses={[row.get('status') for row in attention]}")
                _log(f"{action}_ok http={status} status={response.get('status', 'ok')} live_submission={response.get('live_submission', False)}")
            except urllib.error.HTTPError as error:
                cycle_failed = True
                detail = error.read().decode("utf-8", errors="replace")[:300]
                _log(f"{action}_http_error http={error.code} detail={detail}")
            except Exception as error:
                cycle_failed = True
                _log(f"{action}_error kind={type(error).__name__} detail={str(error)[:300]}")
        next_interval = min(interval, max(10, _env_int("REGIME_INTRADAY_FAILURE_RETRY_SEC", 30))) if cycle_failed else interval
        time.sleep(max(0.0, next_interval - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
