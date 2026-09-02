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
