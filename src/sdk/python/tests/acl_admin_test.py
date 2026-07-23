"""Tests for the ACL admin surface: `_core` builders/parsers for the
roles + grants + user-role lifecycle, and the `client.admin.roles` namespace.
"""

from __future__ import annotations

import json
import unittest

from loom_sdk._core import (
    grant_payload,
    parse_create_role,
    parse_grants,
    parse_role_list,
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


if __name__ == "__main__":
    unittest.main()
