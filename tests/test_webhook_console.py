import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import webhook_console as console


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({"ok": True, "path": self.path}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass


def test_redacts_nested_secrets():
    value = {"Authorization": "Bearer abc", "nested": {"api_key": "xyz", "safe": 4}}
    assert console.redact(value) == {
        "Authorization": "[REDACTED]",
        "nested": {"api_key": "[REDACTED]", "safe": 4},
    }


def test_execute_captures_json_response(tmp_path, monkeypatch):
    monkeypatch.setattr(console, "DB_PATH", tmp_path / "captures.sqlite3")
    console.init_db()
    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        result = console.execute(
            console.RequestSpec(
                url=f"http://127.0.0.1:{server.server_port}/health",
                headers={"Authorization": "Bearer abc"},
            )
        )
    finally:
        server.shutdown()
        thread.join()
    assert result["status_code"] == 200
    assert result["response_body"] == {"ok": True, "path": "/health"}
    assert result["request_headers"]["Authorization"] == "[REDACTED]"
    with console.db() as connection:
        row = connection.execute("SELECT * FROM captures").fetchone()
    assert console.row_to_record(row)["response_body"]["ok"] is True


def test_discovers_routes():
    endpoints = console.discover_endpoints()
    assert any(item["path"] == "/webhook" and item["method"] == "POST" for item in endpoints)
    assert any(item["path"] == "/health" for item in endpoints)

