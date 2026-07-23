"""Asynchronous loom client — mirrors `client.py` 1:1 over `httpx.AsyncClient`."""

from __future__ import annotations

from collections.abc import Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Self

import httpx
import pyarrow as pa

from ._arrow import to_ipc
from ._core import (
    PreparedRequest,
    assign_role_request,
    build_headers,
    create_role_request,
    define_link_request,
    define_model_request,
    delete_role_request,
    get_dataset_request,
    grant_list_request,
    grant_request,
    land_dataset_request,
    land_model_request,
    list_datasets_request,
    list_roles_request,
    login_request,
    ontology_type_request,
    ontology_types_request,
    parse_create_role,
    parse_dataset_detail,
    parse_dataset_list,
    parse_define_model_ack,
    parse_grants,
    parse_land_ack,
    parse_login,
    parse_model_ack,
    parse_ontology_types,
    parse_preview,
    parse_role_list,
    parse_type_detail,
    preview_dataset_request,
    resolve_base,
    revoke_request,
    unassign_role_request,
    user_roles_request,
)
from .errors import NotFoundError, raise_for_response
from .models import ApplyReport

if TYPE_CHECKING:
    from .models import DatasetDetail, DatasetEntry, GrantEntry, LandAck, ModelLandAck, Preview, TypeDetail
    from .pydantic import LoomModel


class _AsyncDatasetsNamespace:
    """`client.datasets` — the dataset side of the ingest write surface."""

    def __init__(self, client: AsyncClient) -> None:
        self._client = client

    async def land(
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
        response = await self._client._send(prep)
        return parse_land_ack(response.content)

    async def list(self) -> list[DatasetEntry]:
        """List all datasets visible to the caller (`GET /datasets`)."""
        response = await self._client._send(list_datasets_request())
        return parse_dataset_list(response.content)

    async def get(
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
        response = await self._client._send(prep)
        return parse_dataset_detail(response.content)

    async def preview(self, schema: str, table: str, *, limit: int | None = None) -> Preview:
        """Fetch a display-string preview (`GET /datasets/{schema}/{table}/preview`)."""
        prep = preview_dataset_request(schema, table, limit=limit)
        response = await self._client._send(prep)
        return parse_preview(response.content)


class _AsyncOntologyNamespace:
    """`client.ontology` — the ontology-type read surface."""

    def __init__(self, client: AsyncClient) -> None:
        self._client = client

    async def types(self) -> list[str]:
        """List all ontology type names (`GET /ontology/types`)."""
        response = await self._client._send(ontology_types_request())
        return parse_ontology_types(response.content)

    async def type(self, name: str) -> TypeDetail:
        """Fetch one ontology type's detail (`GET /ontology/types/{name}`)."""
        response = await self._client._send(ontology_type_request(name))
        return parse_type_detail(response.content)

    async def apply(self, *models: type[LoomModel]) -> ApplyReport:
        """Apply each `LoomModel`'s declared shape to the ontology, in order.

        Requires the `pydantic` extra (imported lazily here, so the core
        SDK stays pydantic-free). Per model: if `GET /ontology/types/{name}`
        finds an existing type, its shape must match exactly (identity,
        properties, table) or `OntologyDriftError` is raised and nothing is
        written; otherwise the type is bootstrapped — a zero-row dataset
        land (gated by `X-Loom-Model`, so date/timestamp columns survive)
        followed by `define_model`. Once every model has been processed,
        each model's link specs absent from their declaring type's current
        `links` are registered via `define_link`.
        """
        from .pydantic import _apply

        created: list[str] = []
        unchanged: list[str] = []
        current_links: dict[str, list[str]] = {}

        for cls in models:
            try:
                response = await self._client._send(_apply.type_check_request(cls))
            except NotFoundError:
                pass
            else:
                detail = parse_type_detail(response.content)
                _apply.diff_type(cls, detail)
                unchanged.append(cls.__name__)
                current_links[cls.__name__] = [link.name for link in detail.links]
                continue

            try:
                await self._client._send(_apply.dataset_check_request(cls))
            except NotFoundError:
                await self._client._send(_apply.land_bootstrap_request(cls))
            await self._client._send(_apply.define_model_request_for(cls))
            created.append(cls.__name__)
            current_links[cls.__name__] = []

        links_created: list[str] = []
        for cls in models:
            for spec in _apply.links_to_create(cls, current_links.get(cls.__name__, [])):
                await self._client._send(_apply.define_link_request_for(cls, spec))
                links_created.append(_apply.link_name(cls, spec))

        return ApplyReport(created=created, unchanged=unchanged, links_created=links_created)


class _AsyncModelsNamespace:
    """`client.models` — the ontology-type side of the ingest write surface."""

    def __init__(self, client: AsyncClient) -> None:
        self._client = client

    async def land(
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
        response = await self._client._send(prep)
        return parse_model_ack(response.content)

    async def land_instances(self, instances: Sequence[LoomModel]) -> ModelLandAck:
        """Land pydantic `LoomModel` instances, returning the commit ack.

        Requires the `pydantic` extra (imported lazily here). Builds a
        `pa.Table` from `instances` via their shared class's Arrow schema,
        flattening link fields to their FK column name (`Order.customer`
        lands under `customer_id`, not `customer`). Every instance must be
        the exact same `LoomModel` subclass — a mix (or an empty sequence)
        raises `ValueError` before any request is built.
        """
        from .pydantic import _apply

        prep = _apply.land_instances_request(instances)
        response = await self._client._send(prep)
        return parse_model_ack(response.content)


class _AsyncRolesNamespace:
    """`client.admin.roles` — the ACL roles + grants + user-role lifecycle."""

    def __init__(self, client: AsyncClient) -> None:
        self._client = client

    async def create(self, role: str) -> str:
        """Declare a role (`POST /admin/roles`), returning its id."""
        response = await self._client._send(create_role_request(role))
        return parse_create_role(response.content)

    async def list(self) -> list[str]:
        """List all declared role ids (`GET /admin/roles`)."""
        response = await self._client._send(list_roles_request())
        return parse_role_list(response.content)

    async def delete(self, role: str) -> None:
        """Delete a role and everything hanging off it (`DELETE /admin/roles/{role}`; idempotent)."""
        await self._client._send(delete_role_request(role))

    async def assign(self, role: str, username: str) -> None:
        """Assign a role to a user (`PUT /admin/users/{u}/roles/{r}`; idempotent)."""
        await self._client._send(assign_role_request(role, username))

    async def assigned(self, username: str) -> list[str]:
        """List the roles assigned to a user (`GET /admin/users/{u}/roles`)."""
        response = await self._client._send(user_roles_request(username))
        return parse_role_list(response.content)

    async def unassign(self, role: str, username: str) -> None:
        """Unassign a role from a user (`DELETE /admin/users/{u}/roles/{r}`; idempotent)."""
        await self._client._send(unassign_role_request(role, username))

    async def grant(self, role: str, action: str, *, type: str | None = None, table: tuple[str, str] | None = None) -> None:
        """Grant a role Read/Write on a type or table (`POST /admin/roles/{r}/grants`).

        Exactly one of `type` (an ontology type name) or `table` (a
        `(schema, name)` tuple) must be set — else `ValueError` before any
        request is sent. `action` is `"read"`/`"write"` (the server 400s
        anything else).
        """
        await self._client._send(grant_request(role, action, type, table))

    async def grants(self, role: str) -> list[GrantEntry]:
        """List a role's coarse grants (`GET /admin/roles/{r}/grants`)."""
        response = await self._client._send(grant_list_request(role))
        return parse_grants(response.content)

    async def revoke(self, role: str, action: str, *, type: str | None = None, table: tuple[str, str] | None = None) -> None:
        """Revoke a coarse grant (`DELETE /admin/roles/{r}/grants`; idempotent).

        Same exactly-one-of-`type`/`table` rule as `grant`.
        """
        await self._client._send(revoke_request(role, action, type, table))


class _AsyncAdminNamespace:
    """`client.admin` — the admin ontology-write surface (admin role required)."""

    def __init__(self, client: AsyncClient) -> None:
        self._client = client
        self.roles = _AsyncRolesNamespace(client)

    async def define_model(self, payload: dict) -> str:
        """Register an ontology type (`POST /admin/models`), returning its name.

        `payload` is passed through verbatim — dict-level API; build it with
        `loom_sdk._core.model_payload` for a wire-conformant body.
        """
        response = await self._client._send(define_model_request(payload))
        return parse_define_model_ack(response.content)

    async def define_link(self, payload: dict) -> None:
        """Register a link between ontology types (`POST /admin/links`).

        `payload` is passed through verbatim — dict-level API; build it with
        `loom_sdk._core.fk_link_payload` for a wire-conformant FK-backed body.
        """
        await self._client._send(define_link_request(payload))


class AsyncClient:
    """Asynchronous loom client.

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
        self._http = httpx.AsyncClient(timeout=timeout)
        self.datasets = _AsyncDatasetsNamespace(self)
        self.models = _AsyncModelsNamespace(self)
        self.ontology = _AsyncOntologyNamespace(self)
        self.admin = _AsyncAdminNamespace(self)

    async def login(self, username: str, password: str) -> Self:
        """Authenticate and store the returned bearer token on this client."""
        response = await self._send(login_request(username, password))
        self._token = parse_login(response.content)
        return self

    async def aclose(self) -> None:
        """Close the underlying HTTP connection pool."""
        await self._http.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def _send(self, prep: PreparedRequest) -> httpx.Response:
        """Send a `PreparedRequest`, applying base-URL routing + bearer auth.

        Raises the matching `loom_sdk.errors` exception for any >= 400
        response.
        """
        base = resolve_base(prep.service, self._ingest_url, self._query_url)
        response = await self._http.request(
            prep.method,
            f"{base}{prep.path}",
            params=prep.params,
            headers=build_headers(prep.headers, self._token),
            content=prep.content,
        )
        if response.status_code >= 400:
            raise_for_response(response.status_code, response.content, response.headers.get("content-type", ""))
        return response
