"""Local console for calling and capturing trading-webhook API responses.

Run with: uvicorn webhook_console:app --reload --port 8090
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel, Field


ROOT = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("WEBHOOK_CONSOLE_DB", ROOT / "webhook_console.sqlite3"))
DEFAULT_BASE_URL = os.getenv(
    "WEBHOOK_CONSOLE_BASE_URL", "https://trading-webhook-q4d5.onrender.com"
).rstrip("/")
MAX_RESPONSE_BYTES = int(os.getenv("WEBHOOK_CONSOLE_MAX_RESPONSE_BYTES", "2000000"))
TIMEOUT_LIMIT_SECONDS = 120.0
SECRET_KEYS = re.compile(r"token|secret|password|authorization|api[-_]?key|signature", re.I)
URL_PATTERN = re.compile(r"https?://[^\s<>'\"]+")

app = FastAPI(title="Trading Webhook Console", version="1.0.0")


class RequestSpec(BaseModel):
    method: str = "GET"
    url: str
    headers: dict[str, str] = Field(default_factory=dict)
    query: dict[str, Any] = Field(default_factory=dict)
    body: Any | None = None
    timeout_seconds: float = Field(default=30.0, ge=1.0, le=TIMEOUT_LIMIT_SECONDS)
    label: str = ""


class BatchSpec(BaseModel):
    requests: list[RequestSpec] = Field(min_length=1, max_length=100)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def db() -> Any:
    connection = sqlite3.connect(DB_PATH, timeout=15)
    connection.row_factory = sqlite3.Row
    try:
        yield connection
        connection.commit()
    finally:
        connection.close()


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with db() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS captures (
                id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                label TEXT NOT NULL,
                method TEXT NOT NULL,
                url TEXT NOT NULL,
                request_headers TEXT NOT NULL,
                request_body TEXT,
                status_code INTEGER,
                elapsed_ms INTEGER NOT NULL,
                response_headers TEXT NOT NULL,
                response_body TEXT,
                content_type TEXT NOT NULL,
                byte_count INTEGER NOT NULL,
                truncated INTEGER NOT NULL DEFAULT 0,
                error TEXT
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_captures_created ON captures(created_at DESC)"
        )


def redact(value: Any, key: str = "") -> Any:
    if SECRET_KEYS.search(key):
        return "[REDACTED]"
    if isinstance(value, dict):
        return {str(k): redact(v, str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [redact(item) for item in value]
    return value


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def parse_body(raw: bytes, content_type: str) -> Any:
    text = raw.decode("utf-8", errors="replace")
    if "json" in content_type.lower():
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
    return text


def execute(spec: RequestSpec) -> dict[str, Any]:
    method = spec.method.upper().strip()
    if method not in {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}:
        raise ValueError(f"Unsupported HTTP method: {method}")

    parsed = urllib.parse.urlsplit(spec.url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("URL must be an absolute http:// or https:// address")
    query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    query.extend((key, str(value)) for key, value in spec.query.items())
    url = urllib.parse.urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path or "/", urllib.parse.urlencode(query), "")
    )

    headers = {str(k): str(v) for k, v in spec.headers.items()}
    body_bytes: bytes | None = None
    if spec.body is not None:
        if isinstance(spec.body, str):
            body_bytes = spec.body.encode()
        else:
            body_bytes = json_text(spec.body).encode()
            headers.setdefault("Content-Type", "application/json")
    request = urllib.request.Request(url, data=body_bytes, headers=headers, method=method)
    started = time.perf_counter()
    status_code: int | None = None
    response_headers: dict[str, str] = {}
    response_body: Any = ""
    content_type = ""
    byte_count = 0
    truncated = False
    error = None
    try:
        with urllib.request.urlopen(request, timeout=spec.timeout_seconds) as result:
            status_code = result.status
            response_headers = dict(result.headers.items())
            content_type = result.headers.get("Content-Type", "")
            raw = result.read(MAX_RESPONSE_BYTES + 1)
    except urllib.error.HTTPError as exc:
        status_code = exc.code
        response_headers = dict(exc.headers.items()) if exc.headers else {}
        content_type = response_headers.get("Content-Type", "")
        raw = exc.read(MAX_RESPONSE_BYTES + 1)
        error = f"HTTP {exc.code}: {exc.reason}"
    except Exception as exc:
        raw = b""
        error = f"{type(exc).__name__}: {exc}"
    elapsed_ms = round((time.perf_counter() - started) * 1000)
    byte_count = len(raw)
    if len(raw) > MAX_RESPONSE_BYTES:
        raw = raw[:MAX_RESPONSE_BYTES]
        truncated = True
    response_body = parse_body(raw, content_type)
    capture_id = str(uuid.uuid4())
    record = {
        "id": capture_id,
        "created_at": utc_now(),
        "label": spec.label.strip(),
        "method": method,
        "url": url,
        "request_headers": redact(headers),
        "request_body": redact(spec.body),
        "status_code": status_code,
        "elapsed_ms": elapsed_ms,
        "response_headers": redact(response_headers),
        "response_body": response_body,
        "content_type": content_type,
        "byte_count": byte_count,
        "truncated": truncated,
        "error": error,
    }
    with db() as connection:
        connection.execute(
            """INSERT INTO captures VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                capture_id, record["created_at"], record["label"], method, url,
                json_text(record["request_headers"]), json_text(record["request_body"]),
                status_code, elapsed_ms, json_text(record["response_headers"]),
                json_text(response_body), content_type, byte_count, int(truncated), error,
            ),
        )
    return record


def row_to_record(row: sqlite3.Row, include_body: bool = True) -> dict[str, Any]:
    item = dict(row)
    for key in ("request_headers", "request_body", "response_headers", "response_body"):
        if key in item and item[key] is not None:
            try:
                item[key] = json.loads(item[key])
            except json.JSONDecodeError:
                pass
    item["truncated"] = bool(item["truncated"])
    if not include_body:
        item.pop("request_body", None)
        item.pop("response_body", None)
        item.pop("request_headers", None)
        item.pop("response_headers", None)
    return item


def discover_endpoints() -> list[dict[str, str]]:
    paths: dict[str, dict[str, str]] = {}
    doc = ROOT / "SYSTEM_ENDPOINTS.md"
    if doc.exists():
        for match in URL_PATTERN.findall(doc.read_text(encoding="utf-8", errors="replace")):
            parsed = urllib.parse.urlsplit(match.rstrip(".,)`"))
            path = parsed.path or "/"
            paths[path] = {"method": "GET", "path": path, "source": "SYSTEM_ENDPOINTS.md"}
    source = ROOT / "app.py"
    if source.exists():
        route_pattern = re.compile(r'@app\.(get|post|put|patch|delete)\(["\']([^"\']+)["\']')
        for method, path in route_pattern.findall(source.read_text(encoding="utf-8", errors="replace")):
            paths[path] = {"method": method.upper(), "path": path, "source": "app.py"}
    return sorted(paths.values(), key=lambda item: (item["path"], item["method"]))


@app.on_event("startup")
def startup() -> None:
    init_db()


@app.get("/", response_class=HTMLResponse)
def home() -> str:
    return UI_HTML.replace("__DEFAULT_BASE_URL__", json.dumps(DEFAULT_BASE_URL))


@app.get("/api/endpoints")
def endpoints() -> dict[str, Any]:
    items = discover_endpoints()
    return {"base_url": DEFAULT_BASE_URL, "count": len(items), "endpoints": items}


@app.post("/api/call")
def call(spec: RequestSpec) -> dict[str, Any]:
    try:
        return execute(spec)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.post("/api/batch")
def batch(spec: BatchSpec) -> dict[str, Any]:
    results = []
    for request in spec.requests:
        try:
            results.append(execute(request))
        except ValueError as exc:
            results.append({"url": request.url, "error": str(exc)})
    return {"count": len(results), "results": results}


@app.get("/api/captures")
def captures(limit: int = Query(50, ge=1, le=500), q: str = "") -> dict[str, Any]:
    sql = "SELECT * FROM captures"
    params: list[Any] = []
    if q:
        sql += " WHERE url LIKE ? OR label LIKE ? OR response_body LIKE ?"
        needle = f"%{q}%"
        params.extend([needle, needle, needle])
    sql += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    with db() as connection:
        rows = connection.execute(sql, params).fetchall()
    return {"captures": [row_to_record(row, include_body=False) for row in rows]}


@app.get("/api/captures/{capture_id}")
def capture(capture_id: str) -> dict[str, Any]:
    with db() as connection:
        row = connection.execute("SELECT * FROM captures WHERE id = ?", (capture_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Capture not found")
    return row_to_record(row)


@app.get("/api/export")
def export(q: str = "") -> Response:
    sql = "SELECT * FROM captures"
    params: list[Any] = []
    if q:
        sql += " WHERE url LIKE ? OR label LIKE ? OR response_body LIKE ?"
        needle = f"%{q}%"
        params.extend([needle, needle, needle])
    sql += " ORDER BY created_at DESC"
    with db() as connection:
        records = [row_to_record(row) for row in connection.execute(sql, params).fetchall()]
    filename = f"webhook-captures-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
    return Response(
        json.dumps(records, indent=2, ensure_ascii=False),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


UI_HTML = r'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Webhook Console</title><style>
:root{--bg:#0a0e13;--panel:#111820;--line:#283543;--text:#e7eef6;--muted:#8fa1b3;--blue:#4ba3ff;--green:#30d18c;--red:#ff6174;--amber:#ffbf5b}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.45 ui-monospace,SFMono-Regular,Consolas,monospace}header{padding:22px 28px;border-bottom:1px solid var(--line);display:flex;align-items:center;justify-content:space-between}h1{font-size:20px;margin:0}.dot{display:inline-block;width:9px;height:9px;border-radius:50%;background:var(--green);margin-right:10px;box-shadow:0 0 12px var(--green)}main{display:grid;grid-template-columns:minmax(430px,1fr) minmax(400px,1.15fr);height:calc(100vh - 70px)}section{padding:22px 28px;overflow:auto}.composer{border-right:1px solid var(--line)}label{display:block;color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.08em;margin:15px 0 7px}input,select,textarea{width:100%;color:var(--text);background:#0d131a;border:1px solid var(--line);border-radius:6px;padding:10px;font:inherit;outline:none}input:focus,textarea:focus,select:focus{border-color:var(--blue)}textarea{resize:vertical;min-height:90px}.row{display:grid;grid-template-columns:105px 1fr;gap:9px}.actions{display:flex;gap:9px;margin-top:16px}button,.button{border:0;border-radius:6px;background:var(--blue);color:#04101d;padding:10px 15px;font:700 13px inherit;cursor:pointer;text-decoration:none}.secondary{background:#202d3a;color:var(--text)}.status{display:flex;gap:16px;color:var(--muted);margin:18px 0 10px}.ok{color:var(--green)}.bad{color:var(--red)}pre{white-space:pre-wrap;word-break:break-word;background:#0d131a;border:1px solid var(--line);border-radius:6px;padding:14px;min-height:170px}.toolbar{display:flex;gap:8px;align-items:center;margin-bottom:14px}.toolbar input{flex:1}.capture{border:1px solid var(--line);border-radius:7px;padding:12px;margin-bottom:9px;cursor:pointer}.capture:hover{border-color:#466078}.capture-top{display:flex;justify-content:space-between;gap:12px}.method{color:var(--blue);font-weight:700}.meta{color:var(--muted);font-size:12px;margin-top:5px}.pill{border-radius:20px;padding:2px 8px;background:#17232e}.endpoint-list{max-height:230px;overflow:auto;border:1px solid var(--line);border-radius:6px}.endpoint{padding:7px 10px;border-bottom:1px solid #1d2934;cursor:pointer}.endpoint:hover{background:#17212b}@media(max-width:900px){main{display:block;height:auto}.composer{border-right:0;border-bottom:1px solid var(--line)}}
</style></head><body><header><h1><span class="dot"></span>Trading Webhook Console</h1><span id="endpointCount" class="pill">loading routes…</span></header><main><section class="composer">
<div class="row"><div><label>Method</label><select id="method"><option>GET</option><option>POST</option><option>PUT</option><option>PATCH</option><option>DELETE</option></select></div><div><label>Request URL</label><input id="url"></div></div>
<label>Find endpoint</label><input id="endpointSearch" placeholder="Filter documented routes…"><div id="endpoints" class="endpoint-list"></div>
<label>Label (optional)</label><input id="label" placeholder="Morning health snapshot"><label>Headers (JSON)</label><textarea id="headers">{}</textarea><label>Body (JSON or text)</label><textarea id="body" placeholder='{"symbol":"SPY"}'></textarea>
<div class="actions"><button id="send">Send & capture</button><button id="copyCurl" class="secondary">Copy cURL</button></div><div id="status" class="status"></div><pre id="response">Ready.</pre>
</section><section><div class="toolbar"><input id="historySearch" placeholder="Search URL, label, or response…"><button id="refresh" class="secondary">Refresh</button><a class="button secondary" href="/api/export">Export JSON</a></div><div id="history"></div></section></main>
<script>
const $=id=>document.getElementById(id), base=__DEFAULT_BASE_URL__; let routes=[];$('url').value=base+'/health';
const pretty=v=>JSON.stringify(v,null,2);function parseMaybe(s){if(!s.trim())return null;try{return JSON.parse(s)}catch{return s}}
async function loadRoutes(){const d=await fetch('/api/endpoints').then(r=>r.json());routes=d.endpoints;$('endpointCount').textContent=d.count+' routes';renderRoutes()}
function renderRoutes(){const q=$('endpointSearch').value.toLowerCase();$('endpoints').innerHTML=routes.filter(x=>x.path.toLowerCase().includes(q)).slice(0,100).map(x=>`<div class="endpoint" data-path="${x.path}" data-method="${x.method}"><span class="method">${x.method}</span> ${x.path}</div>`).join('')}
$('endpointSearch').oninput=renderRoutes;$('endpoints').onclick=e=>{const x=e.target.closest('.endpoint');if(!x)return;$('method').value=x.dataset.method;$('url').value=base+x.dataset.path};
function spec(){let headers=parseMaybe($('headers').value);if(typeof headers!=='object'||Array.isArray(headers))throw Error('Headers must be a JSON object');return{method:$('method').value,url:$('url').value,headers,body:parseMaybe($('body').value),label:$('label').value}}
$('send').onclick=async()=>{try{$('send').disabled=true;$('status').textContent='Sending…';const r=await fetch('/api/call',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(spec())});const d=await r.json();$('status').innerHTML=`<span class="${d.status_code>=200&&d.status_code<400?'ok':'bad'}">${d.status_code??'ERROR'}</span><span>${d.elapsed_ms??0} ms</span><span>${d.byte_count??0} bytes</span>`;$('response').textContent=pretty(d.response_body??d);loadHistory()}catch(e){$('status').innerHTML=`<span class="bad">${e.message}</span>`}finally{$('send').disabled=false}}
$('copyCurl').onclick=async()=>{try{const s=spec();let c=`curl -X ${s.method} '${s.url}'`;for(const[k,v]of Object.entries(s.headers))c+=` -H '${k}: ${v}'`;if(s.body!==null)c+=` --data '${typeof s.body==='string'?s.body:JSON.stringify(s.body)}'`;await navigator.clipboard.writeText(c);$('status').textContent='cURL copied'}catch(e){$('status').textContent=e.message}}
async function loadHistory(){const q=encodeURIComponent($('historySearch').value);const d=await fetch('/api/captures?limit=100&q='+q).then(r=>r.json());$('history').innerHTML=d.captures.map(x=>`<div class="capture" data-id="${x.id}"><div class="capture-top"><span><span class="method">${x.method}</span> ${esc(x.url)}</span><span class="${x.status_code>=200&&x.status_code<400?'ok':'bad'}">${x.status_code??'ERR'}</span></div><div class="meta">${new Date(x.created_at).toLocaleString()} · ${x.elapsed_ms} ms · ${x.byte_count} bytes ${x.label?'· '+esc(x.label):''}</div></div>`).join('')||'<p class="meta">No captures yet.</p>'}
function esc(s){return String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}$('history').onclick=async e=>{const x=e.target.closest('.capture');if(!x)return;const d=await fetch('/api/captures/'+x.dataset.id).then(r=>r.json());$('response').textContent=pretty(d);$('status').innerHTML=`<span>${d.created_at}</span><span>${d.elapsed_ms} ms</span>`};$('refresh').onclick=loadHistory;$('historySearch').oninput=()=>{clearTimeout(window.st);window.st=setTimeout(loadHistory,250)};loadRoutes();loadHistory();
</script></body></html>'''
