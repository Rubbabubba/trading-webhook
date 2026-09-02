"""Compatibility entry point for the isolated legacy swing scanner.

Render may continue using ``python scanner.py`` until its service command is
retired. New regime-intraday scheduling uses ``regime_intraday_worker.py``.
"""

from legacy_swing.scanner import main


if __name__ == "__main__":
    main()
