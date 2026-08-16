# =============================================================================
# Dashboard Rendering Helpers
# =============================================================================
# Small rendering primitives shared by dashboard routes. Keep this module
# broker-free and side-effect-free.

import html as _html
from datetime import datetime, timezone

from fastapi.responses import HTMLResponse


DASHBOARD_RENDERING_MODULE_VERSION = "patch-434-dashboard-fast-route-renderer-extraction-prep"


DASHBOARD_ROUTE_CONFIG = {
    "fast": {"path": "/dashboard", "heavy": False, "guarded": False},
    "fast_alias": {"path": "/dashboard/fast", "heavy": False, "guarded": False},
    "live": {"path": "/dashboard/live", "heavy": False, "guarded": False},
    "full": {"path": "/dashboard/full", "heavy": True, "guarded": True},
    "research": {"path": "/dashboard/research", "heavy": True, "guarded": True},
}


def dashboard_no_store_headers() -> dict:
    return {"Cache-Control": "no-store, max-age=0", "Pragma": "no-cache"}


def dashboard_html_response(html_doc: str) -> HTMLResponse:
    return HTMLResponse(content=html_doc, headers=dashboard_no_store_headers())

def dashboard_escape(value) -> str:
    return _html.escape(str(value if value is not None else ""))


def dashboard_safe_dict(value) -> dict:
    return value if isinstance(value, dict) else {}


def dashboard_safe_list(value) -> list:
    return value if isinstance(value, list) else []


def dashboard_pick(row: dict, *keys, default=""):
    row = dashboard_safe_dict(row)
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return default


def dashboard_card_html(title: str, rows: list[tuple[str, object]]) -> str:
    body = "\n".join(
        f"<tr><th>{dashboard_escape(key)}</th><td>{dashboard_escape(value)}</td></tr>"
        for key, value in rows
    )
    return f"<section class='card'><h2>{dashboard_escape(title)}</h2><table>{body}</table></section>"


def render_fast_dashboard_html(
    *,
    generated_utc: str,
    render_ms: float,
    scanner_rows: list[tuple[str, object]],
    position_summary_rows: list[tuple[str, object]],
    position_rows_html: str,
) -> str:
    return f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Fast Operator Dashboard</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
body {{ margin:0; padding:18px; background:#0d0b18; color:#f5f2ff; font-family:system-ui,-apple-system,Segoe UI,sans-serif; }}
a {{ color:#b9a7ff; }}
.header {{ display:flex; justify-content:space-between; align-items:flex-start; gap:16px; margin-bottom:16px; }}
.grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(260px,1fr)); gap:12px; }}
.card {{ border:1px solid #35255e; border-radius:8px; padding:14px; background:#151223; }}
h1 {{ margin:0 0 6px; font-size:26px; }}
h2 {{ margin:0 0 10px; font-size:16px; }}
.small {{ color:#c9c0ea; font-size:12px; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th,td {{ text-align:left; padding:7px 4px; border-bottom:1px solid #28213f; }}
th {{ color:#c4b5fd; font-weight:700; }}
.actions a {{ display:inline-block; margin-left:8px; padding:7px 10px; border:1px solid #7c5cff; border-radius:6px; text-decoration:none; }}
</style>
</head>
<body>
<div class="header">
  <div>
    <h1>Fast Operator Dashboard</h1>
    <div class="small">Generated {dashboard_escape(generated_utc)}. Render {dashboard_escape(render_ms)} ms. Snapshot-only fast path.</div>
  </div>
  <div class="actions">
    <a href="/dashboard/fast">Refresh</a>
    <a href="/dashboard/live">Live Broker View</a>
    <a href="/dashboard/full">Full Swing</a>
    <a href="/dashboard/research">Research</a>
  </div>
</div>

<div class="grid">
{dashboard_card_html("Scanner", scanner_rows)}
{dashboard_card_html("Positions", position_summary_rows)}
</div>

<section class="card" style="margin-top:12px">
<h2>Positions</h2>
<table>
<tr><th>Symbol</th><th>Qty</th><th>Entry</th><th>Last</th><th>U P&L</th><th>Signal</th></tr>
{position_rows_html}
</table>
</section>
</body>
</html>"""

def dashboard_heavy_requested_from_params(params) -> bool:
    return str((params.get("heavy") if hasattr(params, "get") else "") or (params.get("full") if hasattr(params, "get") else "") or "").strip().lower() in {"1", "true", "yes", "y"}


def dashboard_route_heavy_allowed(*, heavy_enabled: bool, heavy_requested: bool) -> bool:
    return bool(heavy_enabled) and bool(heavy_requested)


def dashboard_route_status(
    *,
    route_key: str,
    heavy_enabled: bool,
    heavy_requested: bool,
    route_budget_ms: int | None = None,
) -> dict:
    route = dict(DASHBOARD_ROUTE_CONFIG.get(route_key) or {})
    heavy_allowed = dashboard_route_heavy_allowed(
        heavy_enabled=heavy_enabled,
        heavy_requested=heavy_requested,
    )
    return {
        "route_key": route_key,
        "path": route.get("path") or "",
        "heavy_route": bool(route.get("heavy")),
        "guarded": bool(route.get("guarded")),
        "heavy_enabled": bool(heavy_enabled),
        "heavy_requested": bool(heavy_requested),
        "heavy_allowed": bool(heavy_allowed),
        "route_budget_ms": None if route_budget_ms is None else int(route_budget_ms or 0),
        "access": "allowed" if heavy_allowed else ("env_and_query_required" if heavy_enabled else "disabled_by_env"),
    }


def dashboard_rendering_status_snapshot(
    *,
    patch_version: str,
    fast_default: bool,
    full_heavy_enabled: bool,
    full_route_budget_ms: int,
    research_heavy_enabled: bool,
) -> dict:
    full_status = dashboard_route_status(
        route_key="full",
        heavy_enabled=bool(full_heavy_enabled),
        heavy_requested=False,
        route_budget_ms=int(full_route_budget_ms or 0),
    )
    research_status = dashboard_route_status(
        route_key="research",
        heavy_enabled=bool(research_heavy_enabled),
        heavy_requested=False,
        route_budget_ms=None,
    )
    return {
        "ok": True,
        "patch_version": patch_version,
        "module": "dashboard_rendering",
        "module_version": DASHBOARD_RENDERING_MODULE_VERSION,
        "broker_free": True,
        "side_effect_free": True,
        "fast_default": bool(fast_default),
        "route_config": DASHBOARD_ROUTE_CONFIG,
        "route_status": {
            "full": full_status,
            "research": research_status,
        },
        "heavy_route_access": {
            "full": full_status.get("access"),
            "research": research_status.get("access"),
        },
        "routes": {key: str(val.get("path") or "") for key, val in DASHBOARD_ROUTE_CONFIG.items()},
        "guarded_routes": {
            "full": True,
            "research": True,
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
    status = dashboard_route_status(
        route_key="full" if "full" in str(route_path or "") else "research",
        heavy_enabled=bool(heavy_enabled),
        heavy_requested=bool(heavy_requested),
        route_budget_ms=route_budget_ms,
    )
    budget = "not configured" if route_budget_ms is None else f"{int(route_budget_ms or 0)} ms"
    unlock_note = "Heavy rendering requires the env flag and an explicit query parameter. There is no dashboard link to trigger it accidentally."
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
<span class="badge">heavy access {_html.escape(str(status.get("access")))}</span>
<table>
<tr><td>patch_version</td><td>{patch}</td></tr>
<tr><td>route</td><td>{path}</td></tr>
<tr><td>heavy_enabled</td><td>{_html.escape(str(status.get("heavy_enabled")).lower())}</td></tr>
<tr><td>heavy_requested</td><td>{_html.escape(str(status.get("heavy_requested")).lower())}</td></tr>
<tr><td>heavy_allowed</td><td>{_html.escape(str(status.get("heavy_allowed")).lower())}</td></tr>
<tr><td>route_budget</td><td>{_html.escape(budget)}</td></tr>
<tr><td>heavy_access</td><td>{_html.escape(unlock_note)}</td></tr>
<tr><td>recommended_action</td><td>Use fast/light dashboards first. Heavy rendering should be opened manually only during intentional debugging.</td></tr>
</table>
<div class="links">
<a href="/dashboard">Fast dashboard</a>
<a href="/dashboard/live">Live dashboard</a>
<a href="/diagnostics/dashboard_rendering_status">Rendering status</a>
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