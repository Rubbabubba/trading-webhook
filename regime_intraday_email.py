"""One-shot email notifications for actionable intraday paper candidates."""

from __future__ import annotations

import json
from typing import Any
from urllib.request import Request, urlopen


def _send_message(*, api_key: str, to_email: str, from_email: str, message: dict[str, str], idempotency_key: str, timeout: int) -> dict[str, Any]:
    if not api_key or not to_email:
        return {"sent": False, "reason": "email_not_configured"}
    payload = {"from": from_email or "Trading System <onboarding@resend.dev>", "to": [to_email], **message}
    request = Request(
        "https://api.resend.com/emails",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json", "User-Agent": "trading-webhook/regime-intraday-alerts", "Idempotency-Key": idempotency_key[:256]},
    )
    with urlopen(request, timeout=max(2, int(timeout))) as response:
        result = json.loads(response.read().decode("utf-8"))
    return {"sent": True, "provider": "resend", "message_id": result.get("id")}


def build_signal_email(signal: dict[str, Any], plan: dict[str, Any]) -> dict[str, str]:
    legs = list(plan.get("legs") or [])
    leg_text = "\n".join(f"- {str(leg.get('side') or '').upper()} {leg.get('symbol')}" for leg in legs)
    signal_id = str(signal.get("signal_id") or "")
    subject = f"PAPER SIGNAL: {signal.get('symbol')} {signal.get('strategy')}"
    text = (
        "A valid paper-trading candidate passed the signal and contract-selection gates.\n\n"
        f"Signal ID: {signal_id}\n"
        f"Underlying: {signal.get('symbol')}\n"
        f"Direction: {signal.get('underlying_side')}\n"
        f"Strategy: {signal.get('strategy')}\n"
        f"Entry / stop / target: {signal.get('entry_price')} / {signal.get('stop_price')} / {signal.get('target_price')}\n"
        f"Expiration: {plan.get('expiration')}\n"
        f"Limit debit: ${float(plan.get('limit_debit') or 0):.2f}\n"
        f"Maximum loss: ${float(plan.get('max_loss_dollars') or 0):.2f}\n"
        f"Maximum profit: ${float(plan.get('max_profit_dollars') or 0):.2f}\n"
        f"Selection source: {dict(plan.get('quote_basis') or {}).get('selection_source')}\n"
        f"Approval expires: {signal.get('approval_expires_at')}\n"
        f"Legs:\n{leg_text}\n\n"
        "This is a candidate notification, not an order or fill confirmation. Paper automation may submit it if enabled and risk checks pass. Check the intraday dashboard and Alpaca paper orders for execution status."
    )
    return {"subject": subject, "text": text}


def send_signal_email(*, api_key: str, to_email: str, from_email: str, signal: dict[str, Any], plan: dict[str, Any], timeout: int = 10) -> dict[str, Any]:
    return _send_message(api_key=api_key, to_email=to_email, from_email=from_email, message=build_signal_email(signal, plan), idempotency_key=f"regime-signal-{str(signal.get('signal_id') or '')}", timeout=timeout)


def build_exit_email(signal_id: str, record: dict[str, Any]) -> dict[str, str]:
    plan = dict(record.get("plan") or {})
    valuation = dict(record.get("valuation") or {})
    decision = dict(record.get("exit_decision") or {})
    subject = f"ACTION NEEDED: {plan.get('underlying')} paper spread exit — {decision.get('reason')}"
    text = (
        "A filled paper spread has reached an exit condition.\n\n"
        f"Signal ID: {signal_id}\n"
        f"Underlying: {plan.get('underlying')}\n"
        f"Exit reason: {decision.get('reason')}\n"
        f"Entry debit: ${float(plan.get('limit_debit') or 0):.2f}\n"
        f"Conservative liquidation credit: ${float(valuation.get('liquidation_credit') or 0):.2f}\n"
        f"Estimated unrealized P/L: ${float(valuation.get('unrealized_dollars') or 0):.2f}\n\n"
        "Paper automation may attempt a closing order if enabled. This email does not confirm a submission or fill. Verify the closing order and remaining positions in Alpaca paper; an unfilled or rejected exit needs attention."
    )
    return {"subject": subject, "text": text}


def send_exit_email(*, api_key: str, to_email: str, from_email: str, signal_id: str, record: dict[str, Any], timeout: int = 10) -> dict[str, Any]:
    reason = str(dict(record.get("exit_decision") or {}).get("reason") or "exit")
    return _send_message(api_key=api_key, to_email=to_email, from_email=from_email, message=build_exit_email(signal_id, record), idempotency_key=f"regime-exit-{signal_id}-{reason}", timeout=timeout)
