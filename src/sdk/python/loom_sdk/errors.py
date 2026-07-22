"""Exception hierarchy and HTTP-response-to-exception mapping for loom_sdk.

All request-building/response-parsing logic in loom_sdk lives in pure
functions; this module owns the response side of that contract via
`raise_for_response`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass


@dataclass
class Violation:
    """One conformance-check failure from a 422 response.

    `expected`/`found`/`rule` are `None` when the wire response omits them
    (they are never sent as JSON `null`).
    """

    column: str
    reason: str
    expected: str | None = None
    found: str | None = None
    rule: str | None = None


class LoomError(Exception):
    """Base class for all loom_sdk errors; carries the HTTP status and message."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


class AuthError(LoomError):
    """401 — missing, invalid, or expired credentials/token."""


class ForbiddenError(LoomError):
    """403 — ACL or admin-role denial."""


class NotFoundError(LoomError):
    """404 — the requested resource does not exist."""


class ConformanceError(LoomError):
    """422 — one or more columns/rows failed conformance checks."""

    def __init__(self, status: int, message: str, violations: list[Violation]) -> None:
        super().__init__(status, message)
        self.violations = violations


class RequestError(LoomError):
    """Other 4xx (400, 409, ...) — malformed request or conflict."""


class ServerError(LoomError):
    """5xx — internal server error."""


class OntologyDriftError(LoomError):
    """Raised by `ontology.apply` when a server-side ontology type's shape
    doesn't match its `LoomModel` declaration.

    Unlike the other `LoomError` subclasses, this isn't derived from an HTTP
    error response — the triggering `GET /ontology/types/{name}` returned a
    normal 200, but its `identity`/`properties`/`table` differ from what the
    class declares. `status` is fixed at 200 for that reason; `message`
    names the first field found to differ (identity, then properties, then
    table — the order `apply`'s comparison checks them in).
    """

    def __init__(self, message: str) -> None:
        super().__init__(200, message)


def _decode(body: bytes) -> str:
    return body.decode("utf-8", errors="replace")


def _parse_violations(body: bytes) -> list[Violation]:
    try:
        data = json.loads(body)
    except ValueError:
        return []
    if not isinstance(data, dict):
        return []
    raw_violations = data.get("violations", [])
    if not isinstance(raw_violations, list):
        return []
    violations = []
    for raw in raw_violations:
        if not isinstance(raw, dict):
            continue
        violations.append(
            Violation(
                column=raw.get("column", ""),
                reason=raw.get("reason", ""),
                expected=raw.get("expected"),
                found=raw.get("found"),
                rule=raw.get("rule"),
            )
        )
    return violations


def _parse_message(status: int, body: bytes, content_type: str) -> str:
    if not body:
        return "forbidden" if status == 403 else ""
    if "json" in content_type:
        try:
            data = json.loads(body)
        except ValueError:
            return _decode(body)
        if isinstance(data, dict) and "error" in data:
            return str(data["error"])
        return _decode(body)
    return _decode(body)


def raise_for_response(status: int, body: bytes, content_type: str) -> None:
    """Raise the loom_sdk exception matching an HTTP response, if any.

    Does nothing for status codes below 400. 422 bodies are parsed as
    `{"violations": [...]}` into `ConformanceError.violations`; other bodies
    are treated as a plain-text message (JSON `{"error": ...}` bodies unwrap
    to their `error` field), except a 403 with an empty body, whose message
    is `"forbidden"`.
    """
    if status < 400:
        return
    if status == 422:
        violations = _parse_violations(body)
        message = f"{len(violations)} conformance violation(s)" if violations else "conformance violation"
        raise ConformanceError(status, message, violations)
    message = _parse_message(status, body, content_type)
    if status == 401:
        raise AuthError(status, message)
    if status == 403:
        raise ForbiddenError(status, message)
    if status == 404:
        raise NotFoundError(status, message)
    if status < 500:
        raise RequestError(status, message)
    raise ServerError(status, message)
