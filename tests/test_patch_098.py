from pathlib import Path


def test_dashboard_decorator_bound_to_dashboard_function():
    app_py = Path(__file__).resolve().parents[1] / "app.py"
    text = app_py.read_text()
    assert '@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):' in text
    assert '@app.get("/dashboard", response_class=HTMLResponse)

@app.get("/diagnostics/strategy_performance")' not in text
