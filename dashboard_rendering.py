# =============================================================================
# Dashboard Rendering Helpers
# =============================================================================
# Small rendering primitives shared by dashboard routes. Keep this module
# broker-free and side-effect-free.

from fastapi.responses import HTMLResponse


DASHBOARD_RENDERING_MODULE_VERSION = "patch-429-dashboard-internal-link-scrub-render-helper-extraction-phase-1"


def dashboard_no_store_headers() -> dict:
    return {"Cache-Control": "no-store, max-age=0", "Pragma": "no-cache"}


def dashboard_html_response(html_doc: str) -> HTMLResponse:
    return HTMLResponse(content=html_doc, headers=dashboard_no_store_headers())