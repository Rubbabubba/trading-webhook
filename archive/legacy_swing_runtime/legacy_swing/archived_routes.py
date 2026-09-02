"""Registry for preserved swing research handlers removed from the HTTP surface."""

from __future__ import annotations


ARCHIVED_SWING_RESEARCH_ROUTES: list[dict] = []


def archived_swing_research_route(path: str):
    """Preserve a handler as callable legacy code without registering a FastAPI route."""
    def decorate(func):
        ARCHIVED_SWING_RESEARCH_ROUTES.append({"path": path, "method": "GET", "handler": func.__name__})
        func.__archived_swing_route__ = path
        return func
    return decorate
