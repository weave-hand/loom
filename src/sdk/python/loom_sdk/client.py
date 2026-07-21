"""Synchronous loom client — a thin httpx transport shell over `_core`."""

from __future__ import annotations

from types import TracebackType
from typing import TYPE_CHECKING, Self

import httpx
import pyarrow as pa

from ._arrow import to_ipc
from ._core import (
    PreparedRequest,
    build_headers,
    land_dataset_request,
    land_model_request,
    login_request,
    parse_land_ack,
    parse_login,
    parse_model_ack,
    resolve_base,
)
from .errors import raise_for_response

if TYPE_CHECKING:
    from .models import LandAck, ModelLandAck


class _DatasetsNamespace:
    """`client.datasets` — the dataset side of the ingest write surface."""

    def __init__(self, client: Client) -> None:
        self._client = client

    def land(
        self,
        schema: str,
        table: str,
        data: pa.Table | pa.RecordBatch | list[dict],
        *,
        mode: str | None = None,
        buckets: int | None = None,
        model_gate: list[dict] | None = None,
        run_id: str | None = None,
    ) -> LandAck:
        """Land `data` into `{schema}.{table}`, returning the commit ack."""
        ipc = to_ipc(data)
        prep = land_dataset_request(
            schema,
            table,
            ipc,
            mode=mode,
            buckets=buckets,
            model_gate=model_gate,
            run_id=run_id,
        )
        response = self._client._send(prep)
        return parse_land_ack(response.content)


class _ModelsNamespace:
    """`client.models` — the ontology-type side of the ingest write surface."""

    def __init__(self, client: Client) -> None:
        self._client = client

    def land(
        self,
        type_name: str,
        data: pa.Table | pa.RecordBatch | list[dict],
        *,
        identity: str | None = None,
        mode: str | None = None,
        buckets: int | None = None,
        merge_engine: str | None = None,
    ) -> ModelLandAck:
        """Land `data` into ontology type `type_name`, returning the commit ack."""
        ipc = to_ipc(data)
        prep = land_model_request(
            type_name,
            ipc,
            identity=identity,
            mode=mode,
            buckets=buckets,
            merge_engine=merge_engine,
        )
        response = self._client._send(prep)
        return parse_model_ack(response.content)


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
        self.datasets = _DatasetsNamespace(self)
        self.models = _ModelsNamespace(self)

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
