"""FastAPI surface for the isolated regime-intraday system."""

from __future__ import annotations

from collections.abc import Callable

from fastapi import APIRouter, Body, Request
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
    replay: Callable[[dict], dict],
    scan_worker: Callable[[dict], dict],
    paper_roundtrip: Callable[[dict], dict],
    paper_reconcile: Callable[[dict], dict],
    paper_close: Callable[[dict], dict],
    after_hours_replay: Callable[[dict], dict],
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

    @router.post("/diagnostics/regime_intraday_replay")
    def diagnostics_regime_intraday_replay(request: Request, body: dict = Body(default={})):
        del request
        return replay(body)

    @router.post("/worker/regime_intraday_scan")
    def worker_regime_intraday_scan(body: dict = Body(default={})):
        return scan_worker(body)

    @router.post("/worker/regime_intraday_paper_roundtrip")
    def worker_regime_intraday_paper_roundtrip(body: dict = Body(default={})):
        return paper_roundtrip(body)

    @router.post("/worker/regime_intraday_paper_reconcile")
    def worker_regime_intraday_paper_reconcile(body: dict = Body(default={})):
        return paper_reconcile(body)

    @router.post("/worker/regime_intraday_paper_close")
    def worker_regime_intraday_paper_close(body: dict = Body(default={})):
        return paper_close(body)

    @router.post("/worker/regime_intraday_after_hours_replay")
    def worker_regime_intraday_after_hours_replay(body: dict = Body(default={})):
        return after_hours_replay(body)

    return router
