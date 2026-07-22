"""Tests for the verification-read surface: `_core` request builders/parsers
and the `client.ontology` / `client.datasets` (list/get/preview) namespaces.
"""

from __future__ import annotations

import asyncio
import json
import unittest

import httpx

from loom_sdk._core import (
    get_dataset_request,
    list_datasets_request,
    ontology_type_request,
    ontology_types_request,
    parse_dataset_detail,
    parse_dataset_list,
    parse_ontology_types,
    parse_preview,
    parse_type_detail,
    preview_dataset_request,
)
from loom_sdk.aclient import AsyncClient
from loom_sdk.client import Client
from loom_sdk.errors import NotFoundError
from loom_sdk.models import (
    ColumnView,
    DatasetDetail,
    DatasetEntry,
    LinkView,
    Preview,
    PropertyView,
    TableRef,
    TypeDetail,
)

TYPE_DETAIL_BODY = json.dumps(
    {
        "name": "Customer",
        "table": {"schema": "raw", "name": "customers"},
        "identity": "id",
        "properties": [
            {"name": "id", "ty": "long", "required": True},
            {
                "name": "email",
                "ty": "string",
                "required": False,
                "description": "primary contact email",
            },
        ],
        "links": [
            {
                "name": "orders",
                "from": "Customer",
                "to": "Order",
                "cardinality": "MANY",
                "description": "orders placed by the customer",
            }
        ],
        "links_to": [
            {"name": "customer", "from": "Order", "to": "Customer", "cardinality": "One"},
        ],
        "description": "A customer record",
    }
).encode("utf-8")


DATASET_LIST_BODY = json.dumps(
    {
        "datasets": [
            {
                "schema": "raw",
                "name": "customers",
                "project": "default",
                "updated": "2026-07-20T12:00:00Z",
                "kind": "table",
            },
            {
                "schema": "curated",
                "name": "customer_view",
                "project": "default",
                "updated": "2026-07-20T13:00:00Z",
                "kind": "view",
                "base": "raw.customers",
            },
        ]
    }
).encode("utf-8")


DATASET_DETAIL_BODY = json.dumps(
    {
        "table": {"schema": "raw", "name": "customers"},
        "snapshot_id": 42,
        "snapshot_time": "2026-07-20T12:00:00Z",
        "columns": [
            {"name": "id", "ty": "long", "nullable": False},
            {"name": "email", "ty": "string", "nullable": True},
        ],
        "kind": "table",
    }
).encode("utf-8")


PREVIEW_BODY = json.dumps(
    {
        "columns": ["id", "email"],
        "rows": [["1", "ada@example.com"], ["2", ""]],
        "sampled": True,
    }
).encode("utf-8")


class OntologyTypesRequestTest(unittest.TestCase):
    def test_shape(self) -> None:
        prep = ontology_types_request()
        self.assertEqual(prep.method, "GET")
        self.assertEqual(prep.service, "query")
        self.assertEqual(prep.path, "/ontology/types")
        self.assertEqual(prep.params, {})
        self.assertIsNone(prep.content)

    def test_parse(self) -> None:
        types = parse_ontology_types(json.dumps({"types": ["Customer", "Order"]}).encode("utf-8"))
        self.assertEqual(types, ["Customer", "Order"])


class OntologyTypeRequestTest(unittest.TestCase):
    def test_shape(self) -> None:
        prep = ontology_type_request("Customer")
        self.assertEqual(prep.method, "GET")
        self.assertEqual(prep.service, "query")
        self.assertEqual(prep.path, "/ontology/types/Customer")

    def test_parse_full_shape(self) -> None:
        detail = parse_type_detail(TYPE_DETAIL_BODY)
        self.assertEqual(
            detail,
            TypeDetail(
                name="Customer",
                table=TableRef(schema="raw", name="customers"),
                identity="id",
                properties=[
                    PropertyView(name="id", ty="long", required=True, description=None),
                    PropertyView(
                        name="email",
                        ty="string",
                        required=False,
                        description="primary contact email",
                    ),
                ],
                links=[
                    LinkView(
                        name="orders",
                        from_type="Customer",
                        to_type="Order",
                        cardinality="many",
                        description="orders placed by the customer",
                    )
                ],
                links_to=[
                    LinkView(
                        name="customer",
                        from_type="Order",
                        to_type="Customer",
                        cardinality="one",
                        description=None,
                    )
                ],
                description="A customer record",
            ),
        )

    def test_parse_absent_identity_and_description(self) -> None:
        body = json.dumps(
            {
                "name": "Widget",
                "table": {"schema": "raw", "name": "widgets"},
                "identity": None,
                "properties": [],
                "links": [],
                "links_to": [],
            }
        ).encode("utf-8")
        detail = parse_type_detail(body)
        self.assertIsNone(detail.identity)
        self.assertIsNone(detail.description)
        self.assertEqual(detail.properties, [])


class ListDatasetsRequestTest(unittest.TestCase):
    def test_shape(self) -> None:
        prep = list_datasets_request()
        self.assertEqual(prep.method, "GET")
        self.assertEqual(prep.service, "query")
        self.assertEqual(prep.path, "/datasets")
        self.assertEqual(prep.params, {})

    def test_parse(self) -> None:
        entries = parse_dataset_list(DATASET_LIST_BODY)
        self.assertEqual(
            entries,
            [
                DatasetEntry(
                    schema="raw",
                    name="customers",
                    project="default",
                    updated="2026-07-20T12:00:00Z",
                    kind="table",
                    base=None,
                ),
                DatasetEntry(
                    schema="curated",
                    name="customer_view",
                    project="default",
                    updated="2026-07-20T13:00:00Z",
                    kind="view",
                    base="raw.customers",
                ),
            ],
        )


class GetDatasetRequestTest(unittest.TestCase):
    def test_shape_minimal(self) -> None:
        prep = get_dataset_request("raw", "customers")
        self.assertEqual(prep.method, "GET")
        self.assertEqual(prep.service, "query")
        self.assertEqual(prep.path, "/datasets/raw/customers")
        self.assertEqual(prep.params, {})

    def test_as_of(self) -> None:
        prep = get_dataset_request("raw", "customers", as_of="2026-07-20T00:00:00Z")
        self.assertEqual(prep.params, {"as_of": "2026-07-20T00:00:00Z"})

    def test_as_of_snapshot(self) -> None:
        prep = get_dataset_request("raw", "customers", as_of_snapshot=42)
        self.assertEqual(prep.params, {"as_of_snapshot": "42"})

    def test_both_raises_value_error(self) -> None:
        with self.assertRaises(ValueError):
            get_dataset_request("raw", "customers", as_of="2026-07-20T00:00:00Z", as_of_snapshot=42)

    def test_parse(self) -> None:
        detail = parse_dataset_detail(DATASET_DETAIL_BODY)
        self.assertEqual(
            detail,
            DatasetDetail(
                table=TableRef(schema="raw", name="customers"),
                snapshot_id=42,
                snapshot_time="2026-07-20T12:00:00Z",
                columns=[
                    ColumnView(name="id", ty="long", nullable=False),
                    ColumnView(name="email", ty="string", nullable=True),
                ],
                kind="table",
                base=None,
            ),
        )


class PreviewDatasetRequestTest(unittest.TestCase):
    def test_shape_minimal(self) -> None:
        prep = preview_dataset_request("raw", "customers")
        self.assertEqual(prep.method, "GET")
        self.assertEqual(prep.service, "query")
        self.assertEqual(prep.path, "/datasets/raw/customers/preview")
        self.assertEqual(prep.params, {})

    def test_limit(self) -> None:
        prep = preview_dataset_request("raw", "customers", limit=50)
        self.assertEqual(prep.params, {"limit": "50"})

    def test_parse(self) -> None:
        preview = parse_preview(PREVIEW_BODY)
        self.assertEqual(
            preview,
            Preview(
                columns=["id", "email"],
                rows=[["1", "ada@example.com"], ["2", ""]],
                sampled=True,
            ),
        )


class OntologyNamespaceTest(unittest.TestCase):
    def test_types_delegates_through_mock_transport(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.url.path, "/ontology/types")
            return httpx.Response(200, json={"types": ["Customer"]})

        client = Client(url="http://loom.invalid")
        client._http = httpx.Client(transport=httpx.MockTransport(handler))

        self.assertEqual(client.ontology.types(), ["Customer"])
        client.close()

    def test_type_delegates_through_mock_transport(self) -> None:
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, content=TYPE_DETAIL_BODY, headers={"content-type": "application/json"})

        client = Client(url="http://loom.invalid")
        client._http = httpx.Client(transport=httpx.MockTransport(handler))

        detail = client.ontology.type("Customer")
        self.assertEqual(detail.name, "Customer")
        self.assertEqual(len(seen), 1)
        self.assertEqual(seen[0].url.path, "/ontology/types/Customer")
        client.close()


class DatasetsReadNamespaceTest(unittest.TestCase):
    def test_list_delegates_through_mock_transport(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.url.path, "/datasets")
            return httpx.Response(200, content=DATASET_LIST_BODY, headers={"content-type": "application/json"})

        client = Client(url="http://loom.invalid")
        client._http = httpx.Client(transport=httpx.MockTransport(handler))

        entries = client.datasets.list()
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0].schema, "raw")
        client.close()

    def test_get_delegates_through_mock_transport(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.url.path, "/datasets/raw/customers")
            return httpx.Response(200, content=DATASET_DETAIL_BODY, headers={"content-type": "application/json"})

        client = Client(url="http://loom.invalid")
        client._http = httpx.Client(transport=httpx.MockTransport(handler))

        detail = client.datasets.get("raw", "customers")
        self.assertEqual(detail.snapshot_id, 42)
        client.close()

    def test_get_both_as_of_raises_before_any_request(self) -> None:
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, json={})

        client = Client(url="http://loom.invalid")
        client._http = httpx.Client(transport=httpx.MockTransport(handler))

        with self.assertRaises(ValueError):
            client.datasets.get("raw", "customers", as_of="2026-07-20T00:00:00Z", as_of_snapshot=42)
        self.assertEqual(len(seen), 0)
        client.close()

    def test_get_404_raises_not_found_error(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404, text="dataset not found")

        client = Client(url="http://loom.invalid")
        client._http = httpx.Client(transport=httpx.MockTransport(handler))

        with self.assertRaises(NotFoundError):
            client.datasets.get("raw", "missing")
        client.close()

    def test_preview_delegates_through_mock_transport(self) -> None:
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, content=PREVIEW_BODY, headers={"content-type": "application/json"})

        client = Client(url="http://loom.invalid")
        client._http = httpx.Client(transport=httpx.MockTransport(handler))

        preview = client.datasets.preview("raw", "customers", limit=50)
        self.assertEqual(preview.sampled, True)
        self.assertEqual(len(seen), 1)
        self.assertEqual(seen[0].url.path, "/datasets/raw/customers/preview")
        self.assertEqual(seen[0].url.params["limit"], "50")
        client.close()


class AsyncOntologyNamespaceTest(unittest.TestCase):
    def test_type_delegates_through_mock_transport(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=TYPE_DETAIL_BODY, headers={"content-type": "application/json"})

        async def run() -> None:
            client = AsyncClient(url="http://loom.invalid")
            client._http = httpx.AsyncClient(transport=httpx.MockTransport(handler))

            detail = await client.ontology.type("Customer")
            self.assertEqual(detail.name, "Customer")
            await client.aclose()

        asyncio.run(run())


class AsyncDatasetsReadNamespaceTest(unittest.TestCase):
    def test_preview_delegates_through_mock_transport(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=PREVIEW_BODY, headers={"content-type": "application/json"})

        async def run() -> None:
            client = AsyncClient(url="http://loom.invalid")
            client._http = httpx.AsyncClient(transport=httpx.MockTransport(handler))

            preview = await client.datasets.preview("raw", "customers")
            self.assertEqual(preview.columns, ["id", "email"])
            await client.aclose()

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
