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


def build_entry_lifecycle_email(signal_id: str, record: dict[str, Any], stage: str) -> dict[str, str]:
    plan = dict(record.get("plan") or {})
    broker = dict(record.get("broker") or {})
    underlying = plan.get("underlying") or dict(record.get("signal") or {}).get("symbol")
    legs = list(plan.get("legs") or [])
    leg_text = "\n".join(f"- {str(leg.get('side') or '').upper()} {leg.get('symbol')}" for leg in legs)
    if stage == "filled":
        fill = abs(float(broker.get("filled_avg_price") or 0))
        subject = f"PAPER ENTRY FILLED: {underlying} spread"
        lead = "Alpaca reports that the paper entry spread filled."
        detail = f"Filled quantity: {broker.get('filled_qty')}\nAverage fill debit: ${fill:.2f}\nFilled at: {broker.get('filled_at')}"
    else:
        subject = f"PAPER ORDER SUBMITTED: {underlying} spread"
        lead = "Alpaca accepted a paper entry order. This does not confirm a fill."
        detail = f"Broker status: {broker.get('status') or record.get('status')}\nLimit debit: ${float(plan.get('limit_debit') or 0):.2f}"
    text = (
        f"{lead}\n\n"
        f"Signal ID: {signal_id}\n"
        f"Order: {record.get('order_id')}\n"
        f"Underlying: {underlying}\n"
        f"{detail}\n"
        f"Maximum loss at entry: ${float(plan.get('max_loss_dollars') or 0):.2f}\n"
        f"Legs:\n{leg_text}\n\n"
        "Paper account only. Verify the order and position directly in Alpaca."
    )
    return {"subject": subject, "text": text}


def send_entry_lifecycle_email(*, api_key: str, to_email: str, from_email: str, signal_id: str, record: dict[str, Any], stage: str, timeout: int = 10) -> dict[str, Any]:
    if stage not in {"submitted", "filled"}:
        raise ValueError("entry lifecycle email stage must be submitted or filled")
    return _send_message(api_key=api_key, to_email=to_email, from_email=from_email,
                         message=build_entry_lifecycle_email(signal_id, record, stage),
                         idempotency_key=f"regime-entry-{signal_id}-{stage}", timeout=timeout)


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
        f"Entry debit: ${float(record.get('entry_fill_debit') or plan.get('limit_debit') or 0):.2f}\n"
        f"Conservative liquidation credit: ${float(valuation.get('liquidation_credit') or 0):.2f}\n"
        f"Estimated unrealized P/L: ${float(valuation.get('unrealized_dollars') or 0):.2f}\n\n"
        "Paper automation may attempt a closing order if enabled. This email does not confirm a submission or fill. Verify the closing order and remaining positions in Alpaca paper; an unfilled or rejected exit needs attention."
    )
    return {"subject": subject, "text": text}


def send_exit_email(*, api_key: str, to_email: str, from_email: str, signal_id: str, record: dict[str, Any], timeout: int = 10) -> dict[str, Any]:
    reason = str(dict(record.get("exit_decision") or {}).get("reason") or "exit")
    return _send_message(api_key=api_key, to_email=to_email, from_email=from_email, message=build_exit_email(signal_id, record), idempotency_key=f"regime-exit-{signal_id}-{reason}", timeout=timeout)


def send_order_outcome_email(*, api_key: str, to_email: str, from_email: str, record: dict, timeout: int = 10) -> dict:
    broker = dict(record.get("broker") or {})
    status = record.get("status")
    message = {"subject": f"PAPER ORDER: {dict(record.get('plan') or {}).get('underlying')} — {status}",
               "text": f"Order: {record.get('order_id')}\nStatus: {status}\nBroker filled quantity: {broker.get('filled_qty')}\nAverage fill price: {broker.get('filled_avg_price')}\nVerify remaining positions and orders in Alpaca paper. No automatic entry repricing or resubmission is performed."}
    return _send_message(api_key=api_key, to_email=to_email, from_email=from_email, message=message,
                         idempotency_key=f"paper-outcome-{record.get('order_id')}-{status}", timeout=timeout)
