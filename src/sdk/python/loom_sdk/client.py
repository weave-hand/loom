"""Synchronous loom client — a thin httpx transport shell over `_core`."""

from __future__ import annotations

from types import TracebackType
from typing import Self

import httpx

from ._core import PreparedRequest, build_headers, login_request, parse_login, resolve_base
from .errors import raise_for_response


class Client:
    """Synchronous loom client.

    Routes requests to `ingest_url` or `query_url` depending on the service
    a `PreparedRequest` targets; the single `url` form sets both.
    """

    def __init__(
        self,
        url: str | None = None,
        *,
        ingest_url: str | None = None,
        query_url: str | None = None,
        token: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        self._ingest_url = ingest_url if ingest_url is not None else url
        self._query_url = query_url if query_url is not None else url
        self._token = token
        self._http = httpx.Client(timeout=timeout)

    def login(self, username: str, password: str) -> Self:
        """Authenticate and store the returned bearer token on this client."""
        response = self._send(login_request(username, password))
        self._token = parse_login(response.content)
        return self

    def close(self) -> None:
        """Close the underlying HTTP connection pool."""
        self._http.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def _send(self, prep: PreparedRequest) -> httpx.Response:
        """Send a `PreparedRequest`, applying base-URL routing + bearer auth.

        Raises the matching `loom_sdk.errors` exception for any >= 400
        response.
        """
        base = resolve_base(prep.service, self._ingest_url, self._query_url)
        response = self._http.request(
            prep.method,
            f"{base}{prep.path}",
            params=prep.params,
            headers=build_headers(prep.headers, self._token),
            content=prep.content,
        )
        if response.status_code >= 400:
            raise_for_response(response.status_code, response.content, response.headers.get("content-type", ""))
        return response
