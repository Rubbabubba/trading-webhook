import py_compile
from pathlib import Path


def test_app_source_compiles_for_startup_import():
    py_compile.compile(str(Path(__file__).resolve().parents[1] / "intraday_app.py"), doraise=True)
