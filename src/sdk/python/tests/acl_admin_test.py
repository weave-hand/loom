"""Tests for the ACL admin surface: `_core` builders/parsers for the
roles + grants + user-role lifecycle, and the `client.admin.roles` namespace.
"""

from __future__ import annotations

import asyncio
import json
import unittest

import httpx

from loom_sdk._core import (
    assign_role_request,
    create_role_request,
    delete_role_request,
    grant_list_request,
    grant_payload,
    grant_request,
    list_roles_request,
    parse_create_role,
    parse_grants,
    parse_role_list,
    revoke_request,
    unassign_role_request,
    user_roles_request,
)
from loom_sdk.aclient import AsyncClient
from loom_sdk.client import Client
from loom_sdk.models import GrantEntry, TableRef


class GrantPayloadTest(unittest.TestCase):
    def test_type_target(self) -> None:
        self.assertEqual(
            grant_payload("read", "Customer", None),
            {"action": "read", "type": "Customer"},
        )

    def test_table_target(self) -> None:
        self.assertEqual(
            grant_payload("write", None, ("raw", "customers")),
            {"action": "write", "table": {"schema": "raw", "name": "customers"}},
        )

    def test_neither_target_raises(self) -> None:
        with self.assertRaises(ValueError):
            grant_payload("read", None, None)

    def test_both_targets_raise(self) -> None:
        with self.assertRaises(ValueError):
            grant_payload("read", "Customer", ("raw", "customers"))


class ParseRoleListTest(unittest.TestCase):
    def test_parses_roles_key(self) -> None:
        self.assertEqual(
            parse_role_list(json.dumps({"roles": ["admin", "analyst"]}).encode()),
            ["admin", "analyst"],
        )

    def test_empty(self) -> None:
        self.assertEqual(parse_role_list(json.dumps({"roles": []}).encode()), [])


class ParseCreateRoleTest(unittest.TestCase):
    def test_echoes_role(self) -> None:
        self.assertEqual(parse_create_role(json.dumps({"role": "analyst"}).encode()), "analyst")


class ParseGrantsTest(unittest.TestCase):
    def test_type_target(self) -> None:
        body = json.dumps(
            {"grants": [{"action": "read", "target": {"Type": "Customer"}, "effect": "allow"}]}
        ).encode()
        self.assertEqual(
            parse_grants(body),
            [GrantEntry(action="read", effect="allow", type="Customer", table=None)],
        )

    def test_table_target(self) -> None:
        body = json.dumps(
            {
                "grants": [
                    {
                        "action": "write",
                        "target": {"Table": {"schema": "raw", "name": "customers"}},
                        "effect": "allow",
                    }
                ]
            }
        ).encode()
        self.assertEqual(
            parse_grants(body),
            [
                GrantEntry(
                    action="write",
                    effect="allow",
                    type=None,
                    table=TableRef(schema="raw", name="customers"),
                )
            ],
        )

    def test_empty(self) -> None:
        self.assertEqual(parse_grants(json.dumps({"grants": []}).encode()), [])

    def test_unknown_target_variant_raises(self) -> None:
        body = json.dumps(
            {"grants": [{"action": "read", "target": {"Nonsense": "x"}, "effect": "allow"}]}
        ).encode()
        with self.assertRaises(ValueError):
            parse_grants(body)


class RequestBuilderTest(unittest.TestCase):
    def test_create_role_request(self) -> None:
        prep = create_role_request("analyst")
        self.assertEqual((prep.method, prep.service, prep.path), ("POST", "query", "/admin/roles"))
        self.assertEqual(prep.headers["Content-Type"], "application/json")
        self.assertEqual(json.loads(prep.content), {"role": "analyst"})

    def test_list_roles_request(self) -> None:
        prep = list_roles_request()
        self.assertEqual((prep.method, prep.service, prep.path), ("GET", "query", "/admin/roles"))
        self.assertIsNone(prep.content)

    def test_delete_role_request(self) -> None:
        prep = delete_role_request("analyst")
        self.assertEqual((prep.method, prep.service, prep.path), ("DELETE", "query", "/admin/roles/analyst"))
        self.assertIsNone(prep.content)

    def test_assign_role_request(self) -> None:
        prep = assign_role_request("analyst", "ada")
        self.assertEqual(
            (prep.method, prep.service, prep.path),
            ("PUT", "query", "/admin/users/ada/roles/analyst"),
        )
        self.assertIsNone(prep.content)

    def test_user_roles_request(self) -> None:
        prep = user_roles_request("ada")
        self.assertEqual(
            (prep.method, prep.service, prep.path),
            ("GET", "query", "/admin/users/ada/roles"),
        )
        self.assertIsNone(prep.content)

    def test_unassign_role_request(self) -> None:
        prep = unassign_role_request("analyst", "ada")
        self.assertEqual(
            (prep.method, prep.service, prep.path),
            ("DELETE", "query", "/admin/users/ada/roles/analyst"),
        )

    def test_grant_request_type_target(self) -> None:
        prep = grant_request("analyst", "read", "Customer", None)
        self.assertEqual(
            (prep.method, prep.service, prep.path),
            ("POST", "query", "/admin/roles/analyst/grants"),
        )
        self.assertEqual(prep.headers["Content-Type"], "application/json")
        self.assertEqual(json.loads(prep.content), {"action": "read", "type": "Customer"})

    def test_revoke_request_table_target(self) -> None:
        prep = revoke_request("analyst", "write", None, ("raw", "customers"))
        self.assertEqual(
            (prep.method, prep.service, prep.path),
            ("DELETE", "query", "/admin/roles/analyst/grants"),
        )
        self.assertEqual(
            json.loads(prep.content),
            {"action": "write", "table": {"schema": "raw", "name": "customers"}},
        )

    def test_grant_list_request(self) -> None:
        prep = grant_list_request("analyst")
        self.assertEqual(
            (prep.method, prep.service, prep.path),
            ("GET", "query", "/admin/roles/analyst/grants"),
        )
        self.assertIsNone(prep.content)

    def test_grant_request_bad_target_raises_before_build(self) -> None:
        with self.assertRaises(ValueError):
            grant_request("analyst", "read", None, None)


def _mock_client(handler) -> Client:
    client = Client(url="http://loom.invalid")
    client._http = httpx.Client(transport=httpx.MockTransport(handler))
    return client


class RolesNamespaceTest(unittest.TestCase):
    def test_create_returns_role(self) -> None:
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(201, json={"role": "analyst"})

        client = _mock_client(handler)
        self.assertEqual(client.admin.roles.create("analyst"), "analyst")
        self.assertEqual(seen[0].url.path, "/admin/roles")
        self.assertEqual(json.loads(seen[0].content), {"role": "analyst"})
        client.close()

    def test_list_and_assigned_share_parser(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"roles": ["admin", "analyst"]})

        client = _mock_client(handler)
        self.assertEqual(client.admin.roles.list(), ["admin", "analyst"])
        self.assertEqual(client.admin.roles.assigned("ada"), ["admin", "analyst"])
        client.close()

    def test_assign_unassign_delete_return_none(self) -> None:
        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(f"{request.method} {request.url.path}")
            return httpx.Response(200, json={})

        client = _mock_client(handler)
        self.assertIsNone(client.admin.roles.assign("analyst", "ada"))
        self.assertIsNone(client.admin.roles.unassign("analyst", "ada"))
        self.assertIsNone(client.admin.roles.delete("analyst"))
        self.assertEqual(
            seen,
            [
                "PUT /admin/users/ada/roles/analyst",
                "DELETE /admin/users/ada/roles/analyst",
                "DELETE /admin/roles/analyst",
            ],
        )
        client.close()

    def test_grant_and_revoke(self) -> None:
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            # grant → 201 "granted"; revoke → 200 "revoked" (server fidelity)
            status = 201 if request.method == "POST" else 200
            return httpx.Response(status, text="granted" if status == 201 else "revoked")

        client = _mock_client(handler)
        self.assertIsNone(client.admin.roles.grant("analyst", "read", type="Customer"))
        self.assertIsNone(client.admin.roles.revoke("analyst", "write", table=("raw", "customers")))
        self.assertEqual(json.loads(seen[0].content), {"action": "read", "type": "Customer"})
        self.assertEqual(
            json.loads(seen[1].content), {"action": "write", "table": {"schema": "raw", "name": "customers"}}
        )
        client.close()

    def test_grants_parses_entries(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={"grants": [{"action": "read", "target": {"Type": "Customer"}, "effect": "allow"}]},
            )

        client = _mock_client(handler)
        self.assertEqual(
            client.admin.roles.grants("analyst"),
            [GrantEntry(action="read", effect="allow", type="Customer", table=None)],
        )
        client.close()

    def test_grant_bad_target_raises_before_send(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover — never reached
            raise AssertionError("should not send")

        client = _mock_client(handler)
        with self.assertRaises(ValueError):
            client.admin.roles.grant("analyst", "read")
        client.close()


class AsyncRolesNamespaceTest(unittest.TestCase):
    def test_create_and_grants_roundtrip(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST" and request.url.path == "/admin/roles":
                return httpx.Response(201, json={"role": "analyst"})
            if request.method == "GET" and request.url.path == "/admin/roles/analyst/grants":
                return httpx.Response(
                    200,
                    json={"grants": [{"action": "read", "target": {"Table": {"schema": "raw", "name": "c"}}, "effect": "allow"}]},
                )
            return httpx.Response(200, json={})

        async def run() -> None:
            client = AsyncClient(url="http://loom.invalid")
            client._http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            self.assertEqual(await client.admin.roles.create("analyst"), "analyst")
            self.assertIsNone(await client.admin.roles.grant("analyst", "read", type="Customer"))
            self.assertEqual(
                await client.admin.roles.grants("analyst"),
                [GrantEntry(action="read", effect="allow", type=None, table=TableRef("raw", "c"))],
            )
            await client.aclose()

        asyncio.run(run())

    def test_grant_bad_target_raises_before_send(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover
            raise AssertionError("should not send")

        async def run() -> None:
            client = AsyncClient(url="http://loom.invalid")
            client._http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            with self.assertRaises(ValueError):
                await client.admin.roles.grant("analyst", "read")
            await client.aclose()

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
