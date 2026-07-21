"""loom_sdk — Python client library for loom.

Sync (`Client`) and async (`AsyncClient`) transport shells over a shared
sans-IO request/response core, plus the `loom_sdk.errors` exception
hierarchy. This top-level package deliberately does not import pydantic or
`loom_sdk.pydantic` — the pydantic-based ontology layer is an opt-in extra.
"""

from __future__ import annotations

from .aclient import AsyncClient
from .client import Client
from .errors import (
    AuthError,
    ConformanceError,
    ForbiddenError,
    LoomError,
    NotFoundError,
    RequestError,
    ServerError,
    Violation,
)

__all__ = [
    "AsyncClient",
    "AuthError",
    "Client",
    "ConformanceError",
    "ForbiddenError",
    "LoomError",
    "NotFoundError",
    "RequestError",
    "ServerError",
    "Violation",
]
