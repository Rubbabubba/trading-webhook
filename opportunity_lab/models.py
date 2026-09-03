"""Shared records used by every Opportunity Lab strategy."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class Opportunity:
    strategy: str
    opportunity_id: str
    observed_at: datetime
    instruments: tuple[str, ...]
    gross_edge_bps: float
    estimated_cost_bps: float
    executable_notional: float
    expires_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def net_edge_bps(self) -> float:
        return self.gross_edge_bps - self.estimated_cost_bps

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["observed_at"] = self.observed_at.astimezone(timezone.utc).isoformat()
        payload["expires_at"] = self.expires_at.astimezone(timezone.utc).isoformat() if self.expires_at else None
        payload["net_edge_bps"] = self.net_edge_bps
        return payload
