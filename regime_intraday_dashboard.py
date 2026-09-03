"""Side-effect-free HTML renderer for the regime-intraday operator console."""

from __future__ import annotations

import html
from regime_intraday_ledger import performance_views
from intraday_monitoring import candidate_views
from datetime import datetime, timezone
from typing import Any


def _esc(value: Any) -> str:
    return html.escape(str(value if value is not None else ""))


def _rows(items: list[tuple[str, Any]]) -> str:
    return "".join(f"<tr><th>{_esc(key)}</th><td>{_esc(value)}</td></tr>" for key, value in items)


def render_intraday_dashboard(*, scan: dict, ledger: dict, readiness: dict, scanner: dict) -> str:
    config = dict(scan.get("config") or {})
    regime = dict(scan.get("regime") or {})
    summary = dict(ledger.get("summary") or {})
    performance = performance_views(ledger)
    pending = dict(ledger.get("pending_candidates") or {})
    entry_blocker = ", ".join(readiness.get("paper_blockers") or [])
    views = candidate_views(ledger, now=datetime.now(timezone.utc), blocker=entry_blocker)
    pending = views["active"]
    history_rows = "".join(f"<tr><td>{_esc(identity)}</td><td>{_esc(row.get('display_status'))}</td><td>{_esc(row.get('expires_at'))}</td></tr>" for identity, row in views["history"].items()) or "<tr><td colspan='3'>No candidate history.</td></tr>"
    freshness_rows = "".join(f"<tr><td>{_esc(symbol)}</td><td>{_esc(row.get('bars'))}</td><td>{_esc(row.get('last_ts'))}</td><td>{_esc(row.get('bar_age_sec'))}</td><td>{_esc(row.get('freshness') or 'not assessed')}</td></tr>" for symbol, row in dict(scan.get("features") or {}).items())
    orders = dict(ledger.get("orders") or {})
    notifications = dict(readiness.get("notifications") or {})
    signal_rows = "".join(
        f"<tr><td>{_esc(row.get('signal_id'))}</td><td>{_esc(row.get('symbol'))}</td><td>{_esc(row.get('strategy'))}</td><td>{_esc(row.get('underlying_side'))}</td><td>{_esc(row.get('entry_price'))}</td><td>{_esc(row.get('stop_price'))}</td><td>{_esc(row.get('target_price'))}</td></tr>"
        for row in list(scan.get("signals") or [])
    ) or "<tr><td colspan='7' class='muted'>No actionable signal in the latest scan.</td></tr>"
    pending_rows = "".join(
        f"<tr><td>{_esc(signal_id)}</td><td>{_esc('blocked: ' + entry_blocker if row.get('status') == 'awaiting_paper_approval' and entry_blocker else row.get('status'))}</td><td>{_esc(row.get('expires_at'))}</td><td>${_esc(dict(row.get('plan') or {}).get('max_loss_dollars'))}</td></tr>"
        for signal_id, row in pending.items()
    ) or "<tr><td colspan='4' class='muted'>No candidates awaiting approval.</td></tr>"
    order_rows = "".join(
        f"<tr><td>{_esc(signal_id)}</td><td>{_esc(row.get('status'))}</td><td>{_esc(row.get('order_id'))}</td><td>{_esc(dict(row.get('exit_decision') or {}).get('reason'))}</td><td>{_esc(dict(row.get('close_order') or {}).get('status'))}</td></tr>"
        for signal_id, row in orders.items()
    ) or "<tr><td colspan='5' class='muted'>No paper orders recorded.</td></tr>"
    paper_ready = bool(readiness.get("paper_ready"))
    live_ready = bool(readiness.get("live_ready"))
    generated = datetime.now(timezone.utc).isoformat()
    return f"""<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><meta http-equiv='refresh' content='30'><title>Regime Intraday Console</title><style>
body{{margin:0;padding:20px;background:#090f17;color:#eef6ff;font-family:Inter,system-ui,sans-serif}}a{{color:#77c7ff}}.top{{display:flex;justify-content:space-between;gap:18px;align-items:flex-start}}.pill{{display:inline-block;padding:7px 11px;border-radius:999px;background:#43202b;color:#ffb7c7;font-weight:800}}.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:12px;margin:14px 0}}.card{{background:#111c29;border:1px solid #29425b;border-radius:10px;padding:14px}}.good{{color:#63e6a6}}.bad{{color:#ff7597}}.warn{{color:#ffd166}}.metric{{font-size:27px;font-weight:800}}.muted{{color:#9fb1c4;font-size:12px}}table{{width:100%;border-collapse:collapse;font-size:12px}}th,td{{text-align:left;padding:7px;border-bottom:1px solid #26394c;vertical-align:top}}th{{color:#9dd7ff}}code{{color:#d9ecff}}</style></head><body>
<div class='top'><div><div class='pill'>PAPER ONLY — LIVE INTRADAY CLOSED</div><h1>Regime-Intraday Console</h1><div class='muted'>Generated {generated}. Auto-refresh 30 seconds. SPY and the optional DIA paper sleeve have separate signals; QQQ supplies regime context. Verify enabled sleeves in the latest scan. Exit requests are not fills: confirm remaining positions in Alpaca paper.</div></div><div><a href='/health'>System health</a></div></div>
<div class='grid'><section class='card'><div class='muted'>Current regime</div><div class='metric'>{_esc(regime.get('name') or 'not scanned')}</div><table>{_rows([('direction',regime.get('direction')),('trade_allowed',regime.get('trade_allowed')),('reason',regime.get('reason')),('latest_scan',scan.get('ts_utc')),('signal_count',scan.get('signal_count'))])}</table></section>
<section class='card'><div class='muted'>Paper readiness</div><div class='metric {'good' if paper_ready else 'bad'}'>{'READY' if paper_ready else 'BLOCKED'}</div><table>{_rows([('blockers',', '.join(readiness.get('paper_blockers') or []) or 'none'),('email_configured',notifications.get('email_configured')),('pending_candidates',len(pending)),('paper_orders',len(orders))])}</table></section>
<section class='card'><div class='muted'>Guarded live readiness</div><div class='metric {'good' if live_ready else 'bad'}'>{'READY' if live_ready else 'CLOSED'}</div><table>{_rows([('blockers',', '.join(readiness.get('live_blockers') or []) or 'none'),('live_submission',False),('shadow_closed',readiness.get('shadow_closed_count')),('paper_order_count',readiness.get('paper_order_count'))])}</table></section>
<section class='card'><div class='muted'>Candidate configuration</div><table>{_rows([('regime_inputs',', '.join(config.get('symbols') or [])),('trade_symbols',', '.join(config.get('trade_symbols') or [])),('momentum_enabled',config.get('momentum_enabled')),('mean_reversion_enabled',config.get('mean_reversion_enabled')),('max_loss_per_spread','$100'),('paper_attempt_limit','unlimited for supervised troubleshooting'),('one_active_order_or_position',True)])}</table></section></div>
<section class='card'><h2>Latest actionable signals</h2><table><tr><th>Signal ID</th><th>Symbol</th><th>Strategy</th><th>Side</th><th>Entry</th><th>Stop</th><th>Target</th></tr>{signal_rows}</table></section>
<section class='card'><h2>Approval queue</h2><table><tr><th>Signal ID</th><th>Status</th><th>Expires</th><th>Max loss</th></tr>{pending_rows}</table></section>
<section class='card'><h2>Candidate history — expired, blocked, or submitted</h2><table><tr><th>Signal ID</th><th>Status</th><th>Expires</th></tr>{history_rows}</table></section>
<section class='card'><h2>Market data freshness at scan time</h2><p>Live entries require completed bars with timestamps no more than 180 seconds old. This does not certify uninterrupted bar coverage.</p><table><tr><th>Symbol</th><th>Bars</th><th>Latest bar</th><th>Age seconds</th><th>Freshness</th></tr>{freshness_rows}</table></section>
<section class='card'><h2>Paper order lifecycle</h2><table><tr><th>Signal ID</th><th>Status</th><th>Entry order</th><th>Exit reason</th><th>Close status</th></tr>{order_rows}</table></section>
<div class='grid'><section class='card'><h2>Underlying shadow simulation — NOT broker profit</h2><p>Sampled bars only; gaps and costs are not modeled. Legacy records are preserved and excluded from the new-method total.</p><table>{_rows(list(performance['shadow'].items()))}</table></section><section class='card'><h2>Alpaca paper execution — recorded orders</h2><p>Gross P/L uses available broker fills only, before fees. Missing fills are excluded. Verify account inventory in Alpaca.</p><table>{_rows(list(performance['broker_paper'].items()))}</table></section><section class='card'><h2>Worker state</h2><table>{_rows([('last_event',scanner.get('last_event')),('last_status',scanner.get('last_status')),('last_success_utc',scanner.get('last_success_utc')),('consecutive_failures',scanner.get('consecutive_failures'))])}</table></section></div>
</body></html>"""
