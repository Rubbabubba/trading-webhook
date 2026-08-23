# Trading Webhook Console

The console is a separate local FastAPI app for discovering, calling, and saving responses from the trading webhook service. It does not modify or proxy the live trading app.

## Start it

```powershell
uvicorn webhook_console:app --reload --port 8090
```

Open `http://127.0.0.1:8090`. The console defaults to the production Render URL and discovers routes from both `SYSTEM_ENDPOINTS.md` and FastAPI decorators in `app.py`.

To point it elsewhere:

```powershell
$env:WEBHOOK_CONSOLE_BASE_URL = "http://127.0.0.1:8000"
uvicorn webhook_console:app --reload --port 8090
```

## What it captures

Every call records its timestamp, label, method, final URL, status, elapsed time, request/response headers, request/response body, content type, byte count, truncation state, and transport error. Captures are stored in `webhook_console.sqlite3` and can be searched or exported from the UI.

Header and body fields whose keys resemble tokens, secrets, passwords, signatures, authorization, or API keys are redacted before storage. Responses are capped at 2 MB by default. Override the database or cap with `WEBHOOK_CONSOLE_DB` and `WEBHOOK_CONSOLE_MAX_RESPONSE_BYTES`.

## Console API

- `GET /api/endpoints` — discovered endpoint catalog
- `POST /api/call` — send one request and capture it
- `POST /api/batch` — send up to 100 requests sequentially
- `GET /api/captures` — search capture summaries
- `GET /api/captures/{id}` — retrieve a full capture
- `GET /api/export` — download all matching captures as JSON

The batch API deliberately requires explicit request specifications so mutating trading endpoints are never called merely by discovering them.
