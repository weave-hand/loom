"""Tests for the pydantic extra's `ontology.apply` and `models.land_instances`.

MockTransport scripts assert the exact request SEQUENCE `apply` drives —
that sequencing (not just the end state) is the contract this task
implements. `_Script` is a small ordered-request-assertion handler shared
by every test in this file.
"""

from __future__ import annotations

import asyncio
import json
import unittest
from collections.abc import Callable

import httpx
import pyarrow as pa

from loom_sdk import OntologyDriftError
from loom_sdk.aclient import AsyncClient
from loom_sdk.client import Client
from loom_sdk.pydantic import Identity, Link, LoomModel, model_gate


class Customer(LoomModel, table=("crm", "customers")):
    customer_id: Identity[int]
    name: str


class Order(LoomModel, table=("crm", "orders")):
    order_id: Identity[int]
    customer: Link[Customer]
    total: float


def _customer_type_detail_json(*, links: list[dict] | None = None) -> dict:
    return {
        "name": "Customer",
        "table": {"schema": "crm", "name": "customers"},
        "identity": "customer_id",
        "properties": [
            {"name": "customer_id", "ty": "long", "required": True},
            {"name": "name", "ty": "string", "required": True},
        ],
        "links": links or [],
        "links_to": [],
    }


def _order_type_detail_json(*, links: list[dict] | None = None) -> dict:
    return {
        "name": "Order",
        "table": {"schema": "crm", "name": "orders"},
        "identity": "order_id",
        "properties": [
            {"name": "order_id", "ty": "long", "required": True},
            {"name": "customer_id", "ty": "long", "required": True},
            {"name": "total", "ty": "double", "required": True},
        ],
        "links": links or [],
        "links_to": [],
    }


def _land_ok(dataset: str) -> Callable[[httpx.Request], httpx.Response]:
    return lambda request: httpx.Response(200, json={"snapshot_id": 1, "dataset": dataset})


class _Script:
    """A MockTransport handler that asserts requests arrive in a fixed order.

    `steps` is a list of `(method, path, responder)`; each incoming request
    must match the next step's method+path (else `AssertionError`, failing
    the test loudly rather than silently mis-scripting a response).
    """

    def __init__(self, steps: list[tuple[str, str, Callable[[httpx.Request], httpx.Response]]]) -> None:
        self._steps = list(steps)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if not self._steps:
            raise AssertionError(f"unexpected extra request: {request.method} {request.url.path}")
        method, path, responder = self._steps.pop(0)
        if (request.method, request.url.path) != (method, path):
            raise AssertionError(
                f"expected {method} {path}, got {request.method} {request.url.path} "
                f"(after {len(self.requests)} prior request(s))"
            )
        self.requests.append(request)
        return responder(request)

    def assert_done(self) -> None:
        assert not self._steps, f"missing {len(self._steps)} expected request(s): {self._steps}"


def _fresh_apply_steps() -> list[tuple[str, str, Callable[[httpx.Request], httpx.Response]]]:
    return [
        ("GET", "/ontology/types/Customer", lambda r: httpx.Response(404, text="Customer")),
        ("GET", "/datasets/crm/customers", lambda r: httpx.Response(404, text="dataset not found")),
        ("POST", "/datasets/crm/customers", _land_ok("crm.customers")),
        ("POST", "/admin/models", lambda r: httpx.Response(201, json={"name": "Customer"})),
        ("GET", "/ontology/types/Order", lambda r: httpx.Response(404, text="Order")),
        ("GET", "/datasets/crm/orders", lambda r: httpx.Response(404, text="dataset not found")),
        ("POST", "/datasets/crm/orders", _land_ok("crm.orders")),
        ("POST", "/admin/models", lambda r: httpx.Response(201, json={"name": "Order"})),
        ("POST", "/admin/links", lambda r: httpx.Response(201, text="defined")),
    ]


class ApplyFreshTest(unittest.TestCase):
    def test_bootstrap_sequence_and_report(self) -> None:
        script = _Script(_fresh_apply_steps())
        client = Client(url="http://loom.invalid")
        client._http = httpx.Client(transport=httpx.MockTransport(script))

        report = client.ontology.apply(Customer, Order)

        script.assert_done()
        self.assertEqual(report.created, ["Customer", "Order"])
        self.assertEqual(report.unchanged, [])
        self.assertEqual(report.links_created, ["Order_customer"])
        client.close()

    def test_bootstrap_land_carries_x_loom_model_gate(self) -> None:
        script = _Script(_fresh_apply_steps())
        client = Client(url="http://loom.invalid")
        client._http = httpx.Client(transport=httpx.MockTransport(script))

        client.ontology.apply(Customer, Order)

        land_customer = script.requests[2]
        self.assertEqual(land_customer.method, "POST")
        self.assertEqual(land_customer.url.path, "/datasets/crm/customers")
        gate = json.loads(land_customer.headers["x-loom-model"])
        self.assertEqual(gate, {"columns": model_gate(Customer)})
        client.close()

    def test_define_link_payload_shape_and_cardinality(self) -> None:
        script = _Script(_fresh_apply_steps())
        client = Client(url="http://loom.invalid")
        client._http = httpx.Client(transport=httpx.MockTransport(script))

        client.ontology.apply(Customer, Order)

        link_request = script.requests[8]
        payload = json.loads(link_request.content)
        self.assertEqual(
            payload,
            {
                "name": "Order_customer",
                "from": "Order",
                "to": "Customer",
                "cardinality": "One",
                "backing": {"ForeignKey": {"from_column": "customer_id", "to_column": "customer_id"}},
            },
        )
        client.close()


class ApplyIdempotentTest(unittest.TestCase):
    def test_matching_types_produce_no_writes(self) -> None:
        steps = [
            ("GET", "/ontology/types/Customer", lambda r: httpx.Response(200, json=_customer_type_detail_json())),
            (
                "GET",
                "/ontology/types/Order",
                lambda r: httpx.Response(
                    200,
                    json=_order_type_detail_json(
                        links=[{"name": "Order_customer", "from": "Order", "to": "Customer", "cardinality": "one"}]
                    ),
                ),
            ),
        ]
        script = _Script(steps)
        client = Client(url="http://loom.invalid")
        client._http = httpx.Client(transport=httpx.MockTransport(script))

        report = client.ontology.apply(Customer, Order)

        script.assert_done()
        self.assertEqual(report.created, [])
        self.assertEqual(report.unchanged, ["Customer", "Order"])
        self.assertEqual(report.links_created, [])
        client.close()


class ApplyDriftTest(unittest.TestCase):
    def test_property_type_drift_raises_and_writes_nothing(self) -> None:
        drifted = _customer_type_detail_json()
        drifted["properties"][1]["ty"] = "text"  # declared as "string"
        script = _Script([("GET", "/ontology/types/Customer", lambda r: httpx.Response(200, json=drifted))])
        client = Client(url="http://loom.invalid")
        client._http = httpx.Client(transport=httpx.MockTransport(script))

        with self.assertRaises(OntologyDriftError):
            client.ontology.apply(Customer, Order)

        self.assertEqual(len(script.requests), 1)
        client.close()

    def test_identity_drift_raises_before_property_comparison(self) -> None:
        drifted = _customer_type_detail_json()
        drifted["identity"] = "name"
        script = _Script([("GET", "/ontology/types/Customer", lambda r: httpx.Response(200, json=drifted))])
        client = Client(url="http://loom.invalid")
        client._http = httpx.Client(transport=httpx.MockTransport(script))

        with self.assertRaises(OntologyDriftError) as ctx:
            client.ontology.apply(Customer)
        self.assertIn("identity", str(ctx.exception))
        client.close()


class LandInstancesTest(unittest.TestCase):
    def test_lands_decodable_arrow_with_fk_column(self) -> None:
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, json={"snapshot_id": 7, "type": "Order"})

        client = Client(url="http://loom.invalid")
        client._http = httpx.Client(transport=httpx.MockTransport(handler))

        orders = [
            Order(order_id=1, customer=10, total=9.5),
            Order(order_id=2, customer=11, total=3.25),
        ]
        ack = client.models.land_instances(orders)

        self.assertEqual(ack.type_name, "Order")
        self.assertEqual(ack.snapshot_id, 7)
        self.assertEqual(len(seen), 1)
        self.assertEqual(seen[0].url.path, "/models/Order")

        table = pa.ipc.open_stream(seen[0].content).read_all()
        self.assertIn("customer_id", table.column_names)
        self.assertNotIn("customer", table.column_names)
        self.assertEqual(table.column("customer_id").to_pylist(), [10, 11])
        self.assertEqual(table.column("order_id").to_pylist(), [1, 2])
        self.assertEqual(table.num_rows, 2)
        client.close()

    def test_mixed_class_instances_raises_value_error_before_any_request(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise AssertionError("no request should be sent for a mixed-class batch")

        client = Client(url="http://loom.invalid")
        client._http = httpx.Client(transport=httpx.MockTransport(handler))

        customer = Customer(customer_id=1, name="Ada")
        order = Order(order_id=1, customer=1, total=1.0)
        with self.assertRaises(ValueError):
            client.models.land_instances([customer, order])
        client.close()

    def test_empty_instances_raises_value_error(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise AssertionError("no request should be sent for an empty batch")

        client = Client(url="http://loom.invalid")
        client._http = httpx.Client(transport=httpx.MockTransport(handler))

        with self.assertRaises(ValueError):
            client.models.land_instances([])
        client.close()


class AsyncApplyTest(unittest.TestCase):
    def test_fresh_apply_sequence(self) -> None:
        script = _Script(_fresh_apply_steps())

        async def handler(request: httpx.Request) -> httpx.Response:
            return script(request)

        async def run() -> None:
            client = AsyncClient(url="http://loom.invalid")
            client._http = httpx.AsyncClient(transport=httpx.MockTransport(handler))

            report = await client.ontology.apply(Customer, Order)

            script.assert_done()
            self.assertEqual(report.created, ["Customer", "Order"])
            self.assertEqual(report.unchanged, [])
            self.assertEqual(report.links_created, ["Order_customer"])
            await client.aclose()

        asyncio.run(run())

    def test_land_instances(self) -> None:
        seen: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, json={"snapshot_id": 3, "type": "Order"})

        async def run() -> None:
            client = AsyncClient(url="http://loom.invalid")
            client._http = httpx.AsyncClient(transport=httpx.MockTransport(handler))

            ack = await client.models.land_instances([Order(order_id=1, customer=5, total=1.0)])
            self.assertEqual(ack.type_name, "Order")
            self.assertEqual(seen[0].url.path, "/models/Order")
            await client.aclose()

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
