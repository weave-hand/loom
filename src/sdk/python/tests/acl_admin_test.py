"""Tests for the ACL admin surface: `_core` builders/parsers for the
roles + grants + user-role lifecycle, and the `client.admin.roles` namespace.
"""

from __future__ import annotations

import json
import unittest

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


if __name__ == "__main__":
    unittest.main()
