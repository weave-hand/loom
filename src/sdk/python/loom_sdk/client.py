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
    define_link_request,
    define_model_request,
    get_dataset_request,
    land_dataset_request,
    land_model_request,
    list_datasets_request,
    login_request,
    ontology_type_request,
    ontology_types_request,
    parse_dataset_detail,
    parse_dataset_list,
    parse_define_model_ack,
    parse_land_ack,
    parse_login,
    parse_model_ack,
    parse_ontology_types,
    parse_preview,
    parse_type_detail,
    preview_dataset_request,
    resolve_base,
)
from .errors import raise_for_response

if TYPE_CHECKING:
    from .models import DatasetDetail, DatasetEntry, LandAck, ModelLandAck, Preview, TypeDetail


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

    def list(self) -> list[DatasetEntry]:
        """List all datasets visible to the caller (`GET /datasets`)."""
        response = self._client._send(list_datasets_request())
        return parse_dataset_list(response.content)

    def get(
        self,
        schema: str,
        table: str,
        *,
        as_of: str | None = None,
        as_of_snapshot: int | None = None,
    ) -> DatasetDetail:
        """Fetch dataset metadata (`GET /datasets/{schema}/{table}`).

        `as_of` and `as_of_snapshot` are mutually exclusive; passing both
        raises `ValueError` before any request is sent.
        """
        prep = get_dataset_request(schema, table, as_of=as_of, as_of_snapshot=as_of_snapshot)
        response = self._client._send(prep)
        return parse_dataset_detail(response.content)

    def preview(self, schema: str, table: str, *, limit: int | None = None) -> Preview:
        """Fetch a display-string preview (`GET /datasets/{schema}/{table}/preview`)."""
        prep = preview_dataset_request(schema, table, limit=limit)
        response = self._client._send(prep)
        return parse_preview(response.content)


class _OntologyNamespace:
    """`client.ontology` — the ontology-type read surface."""

    def __init__(self, client: Client) -> None:
        self._client = client

    def types(self) -> list[str]:
        """List all ontology type names (`GET /ontology/types`)."""
        response = self._client._send(ontology_types_request())
        return parse_ontology_types(response.content)

    def type(self, name: str) -> TypeDetail:
        """Fetch one ontology type's detail (`GET /ontology/types/{name}`)."""
        response = self._client._send(ontology_type_request(name))
        return parse_type_detail(response.content)


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


class _AdminNamespace:
    """`client.admin` — the admin ontology-write surface (admin role required)."""

    def __init__(self, client: Client) -> None:
        self._client = client

    def define_model(self, payload: dict) -> str:
        """Register an ontology type (`POST /admin/models`), returning its name.

        `payload` is passed through verbatim — dict-level API; build it with
        `loom_sdk._core.model_payload` for a wire-conformant body.
        """
        response = self._client._send(define_model_request(payload))
        return parse_define_model_ack(response.content)

    def define_link(self, payload: dict) -> None:
        """Register a link between ontology types (`POST /admin/links`).

        `payload` is passed through verbatim — dict-level API; build it with
        `loom_sdk._core.fk_link_payload` for a wire-conformant FK-backed body.
        """
        self._client._send(define_link_request(payload))


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
        self.ontology = _OntologyNamespace(self)
        self.admin = _AdminNamespace(self)

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
