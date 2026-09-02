import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


def test_regime_intraday_modules_do_not_import_legacy_swing_or_app():
    for path in ROOT.glob("regime_intraday*.py"):
        imports = _imports(path)
        assert "app" not in imports, path.name
        assert not any(name == "legacy_swing" or name.startswith("legacy_swing.") for name in imports), path.name


def test_legacy_swing_is_confined_to_package():
    assert not list(ROOT.glob("swing_*.py"))
    assert (ROOT / "legacy_swing" / "__init__.py").exists()


def test_legacy_swing_workers_do_not_dispatch_regime_intraday():
    scanner = (ROOT / "legacy_swing" / "scanner.py").read_text(encoding="utf-8")
    worker = (ROOT / "legacy_swing" / "worker.py").read_text(encoding="utf-8")
    assert "regime_intraday" not in scanner
    assert "regime_intraday" not in worker


def test_regime_worker_cannot_call_swing_routes():
    worker = (ROOT / "regime_intraday_worker.py").read_text(encoding="utf-8")
    assert "/worker/scan_entries" not in worker
    assert "/worker/exit" not in worker
    assert "/worker/regime_intraday_scan" in worker


def test_legacy_swing_entries_are_hard_retired_in_application_source():
    source = (ROOT / "app.py").read_text(encoding="utf-8")
    assert "LEGACY_SWING_NEW_ENTRIES_RETIRED = True" in source
    assert "if LEGACY_SWING_NEW_ENTRIES_RETIRED:\n    NEW_ENTRIES_ENABLED = False" in source
