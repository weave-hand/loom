"""Tests for the ingest write surface: `_core` request builders/ack parsers
and the `client.datasets` / `client.models` namespaces.
"""

from __future__ import annotations

import asyncio
import json
import unittest

import httpx
import pyarrow as pa

from loom_sdk._arrow import empty_ipc, to_ipc
from loom_sdk._core import land_dataset_request, land_model_request, parse_land_ack, parse_model_ack
from loom_sdk.aclient import AsyncClient
from loom_sdk.client import Client
from loom_sdk.errors import ConformanceError
from loom_sdk.models import LandAck, ModelLandAck


class LandDatasetRequestTest(unittest.TestCase):
    def test_shape_minimal(self) -> None:
        ipc = to_ipc([{"id": 1}])
        prep = land_dataset_request("raw", "trades", ipc)

        self.assertEqual(prep.method, "POST")
        self.assertEqual(prep.service, "ingest")
        self.assertEqual(prep.path, "/datasets/raw/trades")
        self.assertEqual(prep.params, {})
        self.assertEqual(prep.headers["Content-Type"], "application/vnd.apache.arrow.stream")
        self.assertNotIn("X-Loom-Model", prep.headers)
        self.assertNotIn("X-Loom-Run-Id", prep.headers)
        self.assertEqual(prep.content, ipc)

    def test_params_only_when_set(self) -> None:
        ipc = empty_ipc(pa.schema([("id", pa.int64())]))
        prep = land_dataset_request("raw", "trades", ipc, mode="stream", buckets=4)

        self.assertEqual(prep.params, {"mode": "stream", "buckets": "4"})

    def test_model_gate_header(self) -> None:
        ipc = empty_ipc(pa.schema([("id", pa.int64())]))
        columns = [{"name": "id", "ty": "long", "required": True}]
        prep = land_dataset_request("raw", "trades", ipc, model_gate=columns)

        self.assertEqual(json.loads(prep.headers["X-Loom-Model"]), {"columns": columns})

    def test_run_id_header(self) -> None:
        ipc = empty_ipc(pa.schema([("id", pa.int64())]))
        prep = land_dataset_request("raw", "trades", ipc, run_id="00000000-0000-0000-0000-000000000000")

        self.assertEqual(prep.headers["X-Loom-Run-Id"], "00000000-0000-0000-0000-000000000000")


class LandModelRequestTest(unittest.TestCase):
    def test_shape_minimal(self) -> None:
        ipc = to_ipc([{"id": 1}])
        prep = land_model_request("Customer", ipc)

        self.assertEqual(prep.method, "POST")
        self.assertEqual(prep.service, "ingest")
        self.assertEqual(prep.path, "/models/Customer")
        self.assertEqual(prep.params, {})
        self.assertEqual(prep.headers["Content-Type"], "application/vnd.apache.arrow.stream")
        self.assertEqual(prep.content, ipc)

    def test_params_only_when_set(self) -> None:
        ipc = to_ipc([{"id": 1}])
        prep = land_model_request(
            "Customer",
            ipc,
            identity="id",
            mode="cdc",
            buckets=2,
            merge_engine="last_row",
        )

        self.assertEqual(
            prep.params,
            {"identity": "id", "mode": "cdc", "buckets": "2", "merge_engine": "last_row"},
        )


class ParseAckTest(unittest.TestCase):
    def test_parse_land_ack(self) -> None:
        ack = parse_land_ack(json.dumps({"snapshot_id": 42, "dataset": "raw.trades"}).encode("utf-8"))
        self.assertEqual(ack, LandAck(snapshot_id=42, dataset="raw.trades"))

    def test_parse_model_ack(self) -> None:
        ack = parse_model_ack(json.dumps({"snapshot_id": 7, "type": "Customer"}).encode("utf-8"))
        self.assertEqual(ack, ModelLandAck(snapshot_id=7, type_name="Customer"))


class DatasetsNamespaceTest(unittest.TestCase):
    def test_land_returns_ack_and_sends_arrow_bytes(self) -> None:
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, json={"snapshot_id": 1, "dataset": "raw.trades"})

        client = Client(url="http://loom.invalid")
        client._http = httpx.Client(transport=httpx.MockTransport(handler))

        data = [{"id": 1}, {"id": 2}]
        ack = client.datasets.land("raw", "trades", data)

        self.assertEqual(ack, LandAck(snapshot_id=1, dataset="raw.trades"))
        self.assertEqual(len(seen), 1)
        request = seen[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.url.path, "/datasets/raw/trades")
        self.assertEqual(
            request.headers["content-type"],
            "application/vnd.apache.arrow.stream",
        )
        with pa.ipc.open_stream(request.content) as reader:
            table = reader.read_all()
        self.assertEqual(table, pa.Table.from_pylist(data))
        client.close()

    def test_land_raises_conformance_error_on_422(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                422,
                json={"violations": [{"column": "age", "reason": "missing_required"}]},
            )

        client = Client(url="http://loom.invalid")
        client._http = httpx.Client(transport=httpx.MockTransport(handler))

        with self.assertRaises(ConformanceError) as ctx:
            client.datasets.land("raw", "trades", [{"id": 1}])
        self.assertEqual(len(ctx.exception.violations), 1)
        self.assertEqual(ctx.exception.violations[0].column, "age")
        client.close()


class ModelsNamespaceTest(unittest.TestCase):
    def test_land_returns_ack_and_sends_arrow_bytes(self) -> None:
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, json={"snapshot_id": 3, "type": "Customer"})

        client = Client(url="http://loom.invalid")
        client._http = httpx.Client(transport=httpx.MockTransport(handler))

        data = [{"id": 1, "name": "ada"}]
        ack = client.models.land("Customer", data)

        self.assertEqual(ack, ModelLandAck(snapshot_id=3, type_name="Customer"))
        self.assertEqual(len(seen), 1)
        request = seen[0]
        self.assertEqual(request.url.path, "/models/Customer")
        with pa.ipc.open_stream(request.content) as reader:
            table = reader.read_all()
        self.assertEqual(table, pa.Table.from_pylist(data))
        client.close()


class AsyncDatasetsNamespaceTest(unittest.TestCase):
    def test_land_returns_ack(self) -> None:
        seen: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, json={"snapshot_id": 5, "dataset": "raw.trades"})

        async def run() -> None:
            client = AsyncClient(url="http://loom.invalid")
            client._http = httpx.AsyncClient(transport=httpx.MockTransport(handler))

            ack = await client.datasets.land("raw", "trades", [{"id": 1}])
            self.assertEqual(ack, LandAck(snapshot_id=5, dataset="raw.trades"))
            await client.aclose()

        asyncio.run(run())
        self.assertEqual(len(seen), 1)


class AsyncModelsNamespaceTest(unittest.TestCase):
    def test_land_returns_ack(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"snapshot_id": 9, "type": "Customer"})

        async def run() -> None:
            client = AsyncClient(url="http://loom.invalid")
            client._http = httpx.AsyncClient(transport=httpx.MockTransport(handler))

            ack = await client.models.land("Customer", [{"id": 1}])
            self.assertEqual(ack, ModelLandAck(snapshot_id=9, type_name="Customer"))
            await client.aclose()

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
