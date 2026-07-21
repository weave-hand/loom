"""Tests for loom_sdk's errors, sans-IO request core, and client shells."""

from __future__ import annotations

import asyncio
import json
import unittest

import httpx

from loom_sdk._core import PreparedRequest, login_request, parse_login, resolve_base
from loom_sdk.aclient import AsyncClient
from loom_sdk.client import Client
from loom_sdk.errors import (
    AuthError,
    ConformanceError,
    ForbiddenError,
    NotFoundError,
    RequestError,
    ServerError,
    Violation,
    raise_for_response,
)


class ResolveBaseTest(unittest.TestCase):
    def test_ingest_picks_ingest_url(self) -> None:
        self.assertEqual(resolve_base("ingest", "http://ingest", "http://query"), "http://ingest")

    def test_query_picks_query_url(self) -> None:
        self.assertEqual(resolve_base("query", "http://ingest", "http://query"), "http://query")

    def test_single_url_form_sets_both(self) -> None:
        # The single-`url` constructor form resolves to the same base for
        # both services once passed through as both ingest_url and query_url.
        self.assertEqual(resolve_base("ingest", "http://x", "http://x"), "http://x")
        self.assertEqual(resolve_base("query", "http://x", "http://x"), "http://x")

    def test_missing_needed_url_raises(self) -> None:
        with self.assertRaises(ValueError):
            resolve_base("ingest", None, "http://query")
        with self.assertRaises(ValueError):
            resolve_base("query", "http://ingest", None)

    def test_unknown_service_raises(self) -> None:
        with self.assertRaises(ValueError):
            resolve_base("bogus", "http://ingest", "http://query")


class LoginRequestTest(unittest.TestCase):
    def test_shape(self) -> None:
        prep = login_request("ada", "hunter2")
        self.assertEqual(prep.method, "POST")
        self.assertEqual(prep.service, "query")
        self.assertEqual(prep.path, "/auth/login")
        self.assertIsNotNone(prep.content)
        assert prep.content is not None
        self.assertEqual(json.loads(prep.content), {"username": "ada", "password": "hunter2"})

    def test_parse_login(self) -> None:
        self.assertEqual(parse_login(b'{"token":"t1"}'), "t1")


class RaiseForResponseTest(unittest.TestCase):
    def test_2xx_does_not_raise(self) -> None:
        raise_for_response(200, b"", "application/json")

    def test_401_auth_error(self) -> None:
        with self.assertRaises(AuthError) as ctx:
            raise_for_response(401, b"unauthorized", "text/plain")
        self.assertEqual(ctx.exception.status, 401)
        self.assertEqual(ctx.exception.message, "unauthorized")

    def test_403_empty_body_becomes_forbidden(self) -> None:
        with self.assertRaises(ForbiddenError) as ctx:
            raise_for_response(403, b"", "text/plain")
        self.assertEqual(ctx.exception.status, 403)
        self.assertEqual(ctx.exception.message, "forbidden")

    def test_404_not_found(self) -> None:
        with self.assertRaises(NotFoundError) as ctx:
            raise_for_response(404, b"dataset not found", "text/plain")
        self.assertEqual(ctx.exception.status, 404)
        self.assertEqual(ctx.exception.message, "dataset not found")

    def test_422_conformance_error_violations(self) -> None:
        body = json.dumps(
            {
                "violations": [
                    {
                        "column": "age",
                        "expected": "long",
                        "found": "string",
                        "reason": "type_mismatch",
                    }
                ]
            }
        ).encode("utf-8")
        with self.assertRaises(ConformanceError) as ctx:
            raise_for_response(422, body, "application/json")
        self.assertEqual(ctx.exception.status, 422)
        self.assertEqual(
            ctx.exception.violations,
            [Violation(column="age", reason="type_mismatch", expected="long", found="string", rule=None)],
        )

    def test_400_request_error(self) -> None:
        with self.assertRaises(RequestError) as ctx:
            raise_for_response(400, b"bad request", "text/plain")
        self.assertEqual(ctx.exception.status, 400)
        self.assertEqual(ctx.exception.message, "bad request")

    def test_409_is_also_request_error(self) -> None:
        with self.assertRaises(RequestError) as ctx:
            raise_for_response(409, b"conflict", "text/plain")
        self.assertEqual(ctx.exception.status, 409)

    def test_500_server_error(self) -> None:
        with self.assertRaises(ServerError) as ctx:
            raise_for_response(500, b"internal error", "text/plain")
        self.assertEqual(ctx.exception.status, 500)
        self.assertEqual(ctx.exception.message, "internal error")


class ClientTest(unittest.TestCase):
    def test_login_sends_request_and_stores_token(self) -> None:
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, json={"token": "t1"})

        client = Client(url="http://loom.invalid")
        client._http = httpx.Client(transport=httpx.MockTransport(handler))

        result = client.login("ada", "hunter2")

        self.assertIs(result, client)
        self.assertEqual(client._token, "t1")
        self.assertEqual(len(seen), 1)
        self.assertEqual(seen[0].method, "POST")
        self.assertEqual(seen[0].url.path, "/auth/login")
        self.assertEqual(json.loads(seen[0].content), {"username": "ada", "password": "hunter2"})
        client.close()

    def test_send_carries_bearer_token(self) -> None:
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, json={})

        client = Client(url="http://loom.invalid", token="t1")
        client._http = httpx.Client(transport=httpx.MockTransport(handler))

        client._send(
            PreparedRequest(
                method="GET",
                service="query",
                path="/ontology/types",
                params={},
                headers={},
                content=None,
            )
        )

        self.assertEqual(seen[0].headers["authorization"], "Bearer t1")
        client.close()

    def test_send_raises_for_error_status(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404, text="dataset not found")

        client = Client(url="http://loom.invalid")
        client._http = httpx.Client(transport=httpx.MockTransport(handler))

        with self.assertRaises(NotFoundError):
            client._send(
                PreparedRequest(
                    method="GET",
                    service="query",
                    path="/datasets/s/t",
                    params={},
                    headers={},
                    content=None,
                )
            )
        client.close()

    def test_context_manager_closes(self) -> None:
        with Client(url="http://loom.invalid") as client:
            self.assertIsInstance(client, Client)


class AsyncClientTest(unittest.TestCase):
    def test_login_sends_request_and_bearer_carried_forward(self) -> None:
        seen: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, json={"token": "t1"})

        async def run() -> None:
            client = AsyncClient(url="http://loom.invalid")
            client._http = httpx.AsyncClient(transport=httpx.MockTransport(handler))

            result = await client.login("ada", "hunter2")
            self.assertIs(result, client)
            self.assertEqual(client._token, "t1")
            self.assertEqual(seen[0].method, "POST")
            self.assertEqual(seen[0].url.path, "/auth/login")

            await client._send(
                PreparedRequest(
                    method="GET",
                    service="query",
                    path="/ontology/types",
                    params={},
                    headers={},
                    content=None,
                )
            )
            self.assertEqual(seen[-1].headers["authorization"], "Bearer t1")
            await client.aclose()

        asyncio.run(run())

    def test_async_context_manager(self) -> None:
        async def run() -> None:
            async with AsyncClient(url="http://loom.invalid") as client:
                self.assertIsInstance(client, AsyncClient)

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
