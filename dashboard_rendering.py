# =============================================================================
# Dashboard Rendering Helpers
# =============================================================================
# Small rendering primitives shared by dashboard routes. Keep this module
# broker-free and side-effect-free.

import html as _html
from datetime import datetime, timezone

from fastapi.responses import HTMLResponse


DASHBOARD_RENDERING_MODULE_VERSION = "patch-431-dashboard-full-route-time-budget-rendering-status-version-sync"


def dashboard_no_store_headers() -> dict:
    return {"Cache-Control": "no-store, max-age=0", "Pragma": "no-cache"}


def dashboard_html_response(html_doc: str) -> HTMLResponse:
    return HTMLResponse(content=html_doc, headers=dashboard_no_store_headers())


def dashboard_rendering_status_snapshot(
    *,
    patch_version: str,
    fast_default: bool,
    full_heavy_enabled: bool,
    full_route_budget_ms: int,
    research_heavy_enabled: bool,
) -> dict:
    return {
        "ok": True,
        "patch_version": patch_version,
        "module": "dashboard_rendering",
        "module_version": DASHBOARD_RENDERING_MODULE_VERSION,
        "broker_free": True,
        "side_effect_free": True,
        "fast_default": bool(fast_default),
        "full_heavy_enabled": bool(full_heavy_enabled),
        "full_route_budget_ms": int(full_route_budget_ms or 0),
        "research_heavy_enabled": bool(research_heavy_enabled),
        "routes": {
            "fast": "/dashboard",
            "fast_alias": "/dashboard/fast",
            "live": "/dashboard/live",
            "full": "/dashboard/full",
            "research": "/dashboard/research",
        },
        "guarded_routes": {
            "full": not bool(full_heavy_enabled),
            "research": not bool(research_heavy_enabled),
        },
        "generated_utc": datetime.now(timezone.utc).isoformat(),
    }


def dashboard_heavy_route_guard_html(
    *,
    patch_version: str,
    route_title: str,
    route_path: str,
    heavy_enabled: bool,
    heavy_requested: bool,
    route_budget_ms: int | None = None,
) -> str:
    patch = _html.escape(str(patch_version or "unknown"))
    title = _html.escape(str(route_title or "Dashboard Route Guard"))
    path = _html.escape(str(route_path or "/dashboard"))
    heavy_state = "enabled" if heavy_enabled else "disabled"
    requested = "true" if heavy_requested else "false"
    budget = "not configured" if route_budget_ms is None else f"{int(route_budget_ms or 0)} ms"
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
:root{{color-scheme:dark}}
body{{font-family:Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#080912;color:#eef2ff;margin:0;padding:24px}}
.wrap{{max-width:980px;margin:0 auto}}
.card{{background:#111321;border:1px solid rgba(139,92,246,.22);border-radius:16px;padding:20px;margin-top:18px}}
h1,h2{{margin:0 0 10px}}
p{{color:#b6bdd4;line-height:1.5}}
a{{color:#c4b5fd;text-decoration:none}}
.links{{display:flex;flex-wrap:wrap;gap:10px;margin-top:14px}}
.links a{{border:1px solid rgba(139,92,246,.28);border-radius:12px;padding:9px 12px;background:rgba(139,92,246,.10)}}
table{{width:100%;border-collapse:collapse;margin-top:10px}}
td{{border-bottom:1px solid rgba(255,255,255,.08);padding:8px 6px}}
.badge{{display:inline-block;border-radius:999px;padding:5px 10px;background:rgba(245,158,11,.14);color:#fde68a;font-weight:700;font-size:12px}}
</style>
</head>
<body>
<div class="wrap">
<h1>{title}</h1>
<p>This route is intentionally guarded so heavy dashboard rendering does not cause Render timeouts during normal operator use.</p>
<div class="card">
<h2>Route Status</h2>
<span class="badge">heavy rendering {heavy_state}</span>
<table>
<tr><td>patch_version</td><td>{patch}</td></tr>
<tr><td>route</td><td>{path}</td></tr>
<tr><td>heavy_requested</td><td>{requested}</td></tr>
<tr><td>route_budget</td><td>{budget}</td></tr>
<tr><td>recommended_action</td><td>Use fast/light dashboards first. Open the heavy route only when intentionally investigating.</td></tr>
</table>
<div class="links">
<a href="/dashboard">Fast dashboard</a>
<a href="/dashboard/live">Live dashboard</a>
<a href="/diagnostics/dashboard_rendering_status">Rendering status</a>
<a href="{path}?heavy=1">Open heavy route once</a>
</div>
</div>
</div>
</body>
</html>"""


def dashboard_research_guard_html(
    *,
    patch_version: str,
    research_heavy_enabled: bool,
    heavy_requested: bool,
) -> str:
    return dashboard_heavy_route_guard_html(
        patch_version=patch_version,
        route_title="Dashboard Research Guard",
        route_path="/dashboard/research",
        heavy_enabled=research_heavy_enabled,
        heavy_requested=heavy_requested,
        route_budget_ms=None,
    )