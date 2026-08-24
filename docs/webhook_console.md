# Trading Webhook Console

The console is a separate local FastAPI app for discovering, calling, and saving responses from the trading webhook service. It does not modify or proxy the live trading app.

## Start it

```powershell
.\run_webhook_console.ps1
```

Open `http://127.0.0.1:8090`. The console defaults to the production Render URL and discovers routes from both `SYSTEM_ENDPOINTS.md` and FastAPI decorators in `app.py`.

To point it elsewhere:

```powershell
$env:WEBHOOK_CONSOLE_BASE_URL = "http://127.0.0.1:8000"
.\run_webhook_console.ps1
```

The launcher uses the project-local `.venv`, so `uvicorn` does not need to be installed globally or added to PATH. Use `-Port 8091` to select another port or `-NoReload` to disable development reloads.

## What it captures

Every call records its timestamp, label, method, final URL, status, elapsed time, request/response headers, request/response body, content type, byte count, truncation state, and transport error. Captures are stored in `webhook_console.sqlite3` and can be searched or exported from the UI.

Use **Clear history** to start a fresh capture set. With an empty search it deletes all history; with a search active it deletes only matching captures. The console always asks for confirmation.

The **Query parameters** field accepts values such as `limit=25` or `limit=25&symbol=SPY`, without a leading `?`. The sequence runner accepts one full URL or endpoint path per line, runs them top-to-bottom, and stores every response in the same capture history.

The **Timeout** field applies to an individual request and to each request in a sequence. It defaults to 90 seconds and accepts up to 600 seconds. Saved sequences retain their chosen timeout. Override these console-wide defaults with `WEBHOOK_CONSOLE_DEFAULT_TIMEOUT_SECONDS` and `WEBHOOK_CONSOLE_MAX_TIMEOUT_SECONDS` before starting the launcher.

Sequences can be saved by name and are persisted in SQLite. Choosing a saved sequence immediately loads its endpoints into the runner; it does not execute until **Run sequence** is clicked. **Save current** creates a preset or updates one with the same name, and **Delete saved** removes the selected preset. A starter **Market Open Check** sequence is created automatically.

Header and body fields whose keys resemble tokens, secrets, passwords, signatures, authorization, or API keys are redacted before storage. Responses are capped at 2 MB by default. Override the database or cap with `WEBHOOK_CONSOLE_DB` and `WEBHOOK_CONSOLE_MAX_RESPONSE_BYTES`.

## Console API

- `GET /api/endpoints` — discovered endpoint catalog
- `POST /api/call` — send one request and capture it
- `POST /api/batch` — send up to 100 requests sequentially
- `GET /api/sequences` — list saved sequences
- `POST /api/sequences` — create or update a named sequence
- `DELETE /api/sequences/{id}` — delete a saved sequence
- `GET /api/captures` — search capture summaries
- `GET /api/captures/{id}` — retrieve a full capture
- `DELETE /api/captures` — clear all captures, or matching captures with `?q=...`
- `DELETE /api/captures/{id}` — delete one capture
- `GET /api/export` — download all matching captures as JSON

The batch API deliberately requires explicit request specifications so mutating trading endpoints are never called merely by discovering them.
