# =============================================================================
# Dashboard Rendering Helpers
# =============================================================================
# Small rendering primitives shared by dashboard routes. Keep this module
# broker-free and side-effect-free.

import html as _html
from datetime import datetime, timezone

from fastapi.responses import HTMLResponse


DASHBOARD_RENDERING_MODULE_VERSION = "patch-430-dashboard-research-route-heavy-load-guard-dashboard-rendering-module-status"


def dashboard_no_store_headers() -> dict:
    return {"Cache-Control": "no-store, max-age=0", "Pragma": "no-cache"}


def dashboard_html_response(html_doc: str) -> HTMLResponse:
    return HTMLResponse(content=html_doc, headers=dashboard_no_store_headers())


def dashboard_rendering_status_snapshot(
    *,
    patch_version: str,
    fast_default: bool,
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
        "research_heavy_enabled": bool(research_heavy_enabled),
        "routes": {
            "fast": "/dashboard",
            "fast_alias": "/dashboard/fast",
            "live": "/dashboard/live",
            "full": "/dashboard/full",
            "research": "/dashboard/research",
        },
        "generated_utc": datetime.now(timezone.utc).isoformat(),
    }


def dashboard_research_guard_html(
    *,
    patch_version: str,
    research_heavy_enabled: bool,
    heavy_requested: bool,
) -> str:
    patch = _html.escape(str(patch_version or "unknown"))
    heavy_state = "enabled" if research_heavy_enabled else "disabled"
    requested = "true" if heavy_requested else "false"
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Dashboard Research Guard</title>
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
<h1>Dashboard Research Guard</h1>
<p>This route is intentionally guarded so heavy research panels do not cause Render timeouts during normal operator use.</p>
<div class="card">
<h2>Research Route Status</h2>
<span class="badge">heavy research {heavy_state}</span>
<table>
<tr><td>patch_version</td><td>{patch}</td></tr>
<tr><td>heavy_requested</td><td>{requested}</td></tr>
<tr><td>recommended_action</td><td>Use light diagnostics first. Enable heavy research only when intentionally investigating.</td></tr>
</table>
<div class="links">
<a href="/dashboard">Fast dashboard</a>
<a href="/dashboard/full">Full swing</a>
<a href="/dashboard/live">Live dashboard</a>
<a href="/diagnostics/dashboard_rendering_status">Rendering status</a>
<a href="/diagnostics/intraday_runtime_isolation_status">Intraday isolation</a>
</div>
</div>
</div>
</body>
</html>"""