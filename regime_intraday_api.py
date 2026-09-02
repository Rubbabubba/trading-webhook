"""FastAPI surface for the isolated regime-intraday system."""

from __future__ import annotations

from collections.abc import Callable

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from regime_intraday_dashboard import render_intraday_dashboard


def build_regime_intraday_router(
    *,
    get_scan: Callable[[], dict],
    refresh_scan: Callable[[], dict],
    get_ledger_payload: Callable[[], dict],
    get_readiness_payload: Callable[[], dict],
    get_dashboard_payload: Callable[[], dict],
    html_response: Callable[[str], HTMLResponse],
) -> APIRouter:
    router = APIRouter()

    @router.get("/diagnostics/regime_intraday")
    def diagnostics_regime_intraday(request: Request, refresh: bool = False):
        del request
        if refresh or not get_scan():
            return refresh_scan()
        return dict(get_scan())

    @router.get("/dashboard/intraday", response_class=HTMLResponse)
    def dashboard_regime_intraday(request: Request):
        del request
        payload = get_dashboard_payload()
        return html_response(render_intraday_dashboard(**payload))

    @router.get("/diagnostics/regime_intraday_ledger")
    def diagnostics_regime_intraday_ledger(request: Request):
        del request
        return get_ledger_payload()

    @router.get("/diagnostics/regime_intraday_readiness")
    def diagnostics_regime_intraday_readiness(request: Request):
        del request
        return get_readiness_payload()

    return router
