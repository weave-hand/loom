"""Tests for the admin write surface: `_core` builders/parsers for
`define_model`/`define_link`, and the `client.admin` namespace.
"""

from __future__ import annotations

import asyncio
import json
import unittest

import httpx

from loom_sdk._core import (
    define_link_request,
    define_model_request,
    fk_link_payload,
    model_payload,
    parse_define_model_ack,
)
from loom_sdk.aclient import AsyncClient
from loom_sdk.client import Client
from loom_sdk.errors import RequestError


class ModelPayloadTest(unittest.TestCase):
    def test_full_shape(self) -> None:
        payload = model_payload(
            "Customer",
            "raw",
            "customers",
            "id",
            [("id", "long", True), ("email", "string", False)],
            description="A customer record",
        )
        self.assertEqual(
            payload,
            {
                "name": "Customer",
                "table": {"schema": "raw", "name": "customers"},
                "identity": "id",
                "properties": [
                    {"name": "id", "ty": "long", "required": True},
                    {"name": "email", "ty": "string", "required": False},
                ],
                "description": "A customer record",
            },
        )

    def test_no_description_omits_key(self) -> None:
        payload = model_payload("Widget", "raw", "widgets", None, [])
        self.assertEqual(
            payload,
            {
                "name": "Widget",
                "table": {"schema": "raw", "name": "widgets"},
                "identity": None,
                "properties": [],
            },
        )
        self.assertNotIn("description", payload)

    def test_identity_none_kept_as_null(self) -> None:
        payload = model_payload("Widget", "raw", "widgets", None, [("sku", "string", True)])
        self.assertIsNone(payload["identity"])


class FkLinkPayloadTest(unittest.TestCase):
    def test_default_cardinality_one(self) -> None:
        payload = fk_link_payload("orders", "Order", "Customer", "customer_id", "id")
        self.assertEqual(
            payload,
            {
                "name": "orders",
                "from": "Order",
                "to": "Customer",
                "cardinality": "One",
                "backing": {"ForeignKey": {"from_column": "customer_id", "to_column": "id"}},
            },
        )

    def test_cardinality_many_and_description(self) -> None:
        payload = fk_link_payload(
            "customers",
            "Customer",
            "Order",
            "id",
            "customer_id",
            cardinality="Many",
            description="all orders for a customer",
        )
        self.assertEqual(payload["cardinality"], "Many")
        self.assertEqual(payload["description"], "all orders for a customer")
        self.assertEqual(
            payload["backing"],
            {"ForeignKey": {"from_column": "id", "to_column": "customer_id"}},
        )

    def test_invalid_cardinality_raises_value_error(self) -> None:
        with self.assertRaises(ValueError):
            fk_link_payload("orders", "Order", "Customer", "customer_id", "id", cardinality="MANY")

    def test_no_description_omits_key(self) -> None:
        payload = fk_link_payload("orders", "Order", "Customer", "customer_id", "id")
        self.assertNotIn("description", payload)


class DefineModelRequestTest(unittest.TestCase):
    def test_shape(self) -> None:
        payload = model_payload("Widget", "raw", "widgets", None, [])
        prep = define_model_request(payload)
        self.assertEqual(prep.method, "POST")
        self.assertEqual(prep.service, "query")
        self.assertEqual(prep.path, "/admin/models")
        self.assertEqual(prep.params, {})
        self.assertEqual(prep.headers["Content-Type"], "application/json")
        self.assertEqual(json.loads(prep.content), payload)

    def test_parse_ack(self) -> None:
        name = parse_define_model_ack(json.dumps({"name": "Widget"}).encode("utf-8"))
        self.assertEqual(name, "Widget")


class DefineLinkRequestTest(unittest.TestCase):
    def test_shape(self) -> None:
        payload = fk_link_payload("orders", "Order", "Customer", "customer_id", "id")
        prep = define_link_request(payload)
        self.assertEqual(prep.method, "POST")
        self.assertEqual(prep.service, "query")
        self.assertEqual(prep.path, "/admin/links")
        self.assertEqual(prep.params, {})
        self.assertEqual(prep.headers["Content-Type"], "application/json")
        self.assertEqual(json.loads(prep.content), payload)


class AdminNamespaceTest(unittest.TestCase):
    def test_define_model_delegates_through_mock_transport(self) -> None:
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(201, json={"name": "Widget"})

        client = Client(url="http://loom.invalid")
        client._http = httpx.Client(transport=httpx.MockTransport(handler))

        payload = model_payload("Widget", "raw", "widgets", None, [])
        name = client.admin.define_model(payload)
        self.assertEqual(name, "Widget")
        self.assertEqual(len(seen), 1)
        self.assertEqual(seen[0].url.path, "/admin/models")
        self.assertEqual(json.loads(seen[0].content), payload)
        client.close()

    def test_define_link_delegates_through_mock_transport(self) -> None:
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(201, text="defined")

        client = Client(url="http://loom.invalid")
        client._http = httpx.Client(transport=httpx.MockTransport(handler))

        payload = fk_link_payload("orders", "Order", "Customer", "customer_id", "id")
        result = client.admin.define_link(payload)
        self.assertIsNone(result)
        self.assertEqual(len(seen), 1)
        self.assertEqual(seen[0].url.path, "/admin/links")
        client.close()

    def test_define_model_400_surfaces_inner_error_message(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(400, json={"error": "bad aggregate expression"})

        client = Client(url="http://loom.invalid")
        client._http = httpx.Client(transport=httpx.MockTransport(handler))

        with self.assertRaises(RequestError) as ctx:
            client.admin.define_model(model_payload("Widget", "raw", "widgets", None, []))
        self.assertEqual(ctx.exception.status, 400)
        self.assertEqual(ctx.exception.message, "bad aggregate expression")
        client.close()

    def test_define_model_409_raises_request_error(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(409, text="model already exists")

        client = Client(url="http://loom.invalid")
        client._http = httpx.Client(transport=httpx.MockTransport(handler))

        with self.assertRaises(RequestError) as ctx:
            client.admin.define_model(model_payload("Widget", "raw", "widgets", None, []))
        self.assertEqual(ctx.exception.status, 409)
        client.close()


class AsyncAdminNamespaceTest(unittest.TestCase):
    def test_define_model_delegates_through_mock_transport(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(201, json={"name": "Widget"})

        async def run() -> None:
            client = AsyncClient(url="http://loom.invalid")
            client._http = httpx.AsyncClient(transport=httpx.MockTransport(handler))

            name = await client.admin.define_model(model_payload("Widget", "raw", "widgets", None, []))
            self.assertEqual(name, "Widget")
            await client.aclose()

        asyncio.run(run())

    def test_define_link_delegates_through_mock_transport(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(201, text="defined")

        async def run() -> None:
            client = AsyncClient(url="http://loom.invalid")
            client._http = httpx.AsyncClient(transport=httpx.MockTransport(handler))

            payload = fk_link_payload("orders", "Order", "Customer", "customer_id", "id")
            result = await client.admin.define_link(payload)
            self.assertIsNone(result)
            await client.aclose()

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
