"""One-shot email notifications for actionable intraday paper candidates."""

from __future__ import annotations

import json
from typing import Any
from urllib.request import Request, urlopen


def build_signal_email(signal: dict[str, Any], plan: dict[str, Any]) -> dict[str, str]:
    legs = list(plan.get("legs") or [])
    leg_text = "\n".join(f"- {str(leg.get('side') or '').upper()} {leg.get('symbol')}" for leg in legs)
    signal_id = str(signal.get("signal_id") or "")
    subject = f"ACTION NEEDED: {signal.get('symbol')} {signal.get('strategy')} paper signal"
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
        f"Legs:\n{leg_text}\n\n"
        "No order has been sent. Return to the Codex trading task to review and explicitly authorize the paper order."
    )
    return {"subject": subject, "text": text}


def send_signal_email(*, api_key: str, to_email: str, from_email: str, signal: dict[str, Any], plan: dict[str, Any], timeout: int = 10) -> dict[str, Any]:
    if not api_key or not to_email:
        return {"sent": False, "reason": "email_not_configured"}
    message = build_signal_email(signal, plan)
    payload = {"from": from_email or "Trading System <onboarding@resend.dev>", "to": [to_email], **message}
    request = Request(
        "https://api.resend.com/emails",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "User-Agent": "trading-webhook/regime-intraday-alerts",
            "Idempotency-Key": f"regime-signal-{str(signal.get('signal_id') or '')}"[:256],
        },
    )
    with urlopen(request, timeout=max(2, int(timeout))) as response:
        result = json.loads(response.read().decode("utf-8"))
    return {"sent": True, "provider": "resend", "message_id": result.get("id")}
