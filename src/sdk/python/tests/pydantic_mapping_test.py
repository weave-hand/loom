"""Tests for the pydantic declaration layer: `LoomModel`/`Identity`/`Link`
class machinery and `loom_sdk.pydantic._mapping`'s pure type-mapping
functions.
"""

from __future__ import annotations

import datetime
import unittest

import pyarrow as pa

from loom_sdk.pydantic import Identity, Link, LoomModel, arrow_schema, model_gate
from loom_sdk.pydantic._mapping import loom_type


class Customer(LoomModel, table=("crm", "customers")):
    customer_id: Identity[int]
    name: str
    tier: str | None = None
    created_at: datetime.datetime


class Order(LoomModel, table=("crm", "orders")):
    order_id: Identity[int]
    customer: Link[Customer]
    total: float


class LoomTypeTest(unittest.TestCase):
    def test_basic_mappings(self) -> None:
        self.assertEqual(loom_type(int), ("long", True))
        self.assertEqual(loom_type(str), ("string", True))
        self.assertEqual(loom_type(float), ("double", True))
        self.assertEqual(loom_type(bool), ("boolean", True))
        self.assertEqual(loom_type(datetime.datetime), ("timestamp", True))
        self.assertEqual(loom_type(datetime.date), ("date", True))

    def test_optional_is_not_required(self) -> None:
        self.assertEqual(loom_type(str | None), ("string", False))
        self.assertEqual(loom_type(int | None), ("long", False))

    def test_unmappable_annotation_raises_type_error(self) -> None:
        with self.assertRaises(TypeError):
            loom_type(bytes)


class LoomModelClassMachineryTest(unittest.TestCase):
    def test_customer_derived_attributes(self) -> None:
        self.assertEqual(Customer.__loom_table__, ("crm", "customers"))
        self.assertEqual(Customer.__loom_identity__, "customer_id")
        self.assertEqual(
            Customer.__loom_properties__,
            [
                ("customer_id", "long", True),
                ("name", "string", True),
                ("tier", "string", False),
                ("created_at", "timestamp", True),
            ],
        )
        self.assertEqual(Customer.__loom_links__, [])

    def test_order_fk_link_derived_attributes(self) -> None:
        self.assertEqual(Order.__loom_table__, ("crm", "orders"))
        self.assertEqual(Order.__loom_identity__, "order_id")
        self.assertEqual(
            Order.__loom_properties__,
            [
                ("order_id", "long", True),
                ("customer_id", "long", True),
                ("total", "double", True),
            ],
        )
        self.assertEqual(len(Order.__loom_links__), 1)
        link = Order.__loom_links__[0]
        self.assertEqual(link.field, "customer")
        self.assertIs(link.target, Customer)
        self.assertEqual(link.fk_column, "customer_id")

    def test_instances_construct_normally(self) -> None:
        customer = Customer(
            customer_id=1,
            name="Ada",
            created_at=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
        )
        self.assertEqual(customer.customer_id, 1)
        self.assertIsNone(customer.tier)

        order = Order(order_id=1, customer=1, total=42.5)
        self.assertEqual(order.customer, 1)


class ArrowSchemaTest(unittest.TestCase):
    def test_customer_schema_incl_timestamp(self) -> None:
        schema = arrow_schema(Customer)
        self.assertEqual(
            schema,
            pa.schema(
                [
                    pa.field("customer_id", pa.int64(), nullable=False),
                    pa.field("name", pa.string(), nullable=False),
                    pa.field("tier", pa.string(), nullable=True),
                    pa.field("created_at", pa.timestamp("us"), nullable=False),
                ]
            ),
        )

    def test_order_schema(self) -> None:
        schema = arrow_schema(Order)
        self.assertEqual(
            schema,
            pa.schema(
                [
                    pa.field("order_id", pa.int64(), nullable=False),
                    pa.field("customer_id", pa.int64(), nullable=False),
                    pa.field("total", pa.float64(), nullable=False),
                ]
            ),
        )


class ModelGateTest(unittest.TestCase):
    def test_customer_columns(self) -> None:
        self.assertEqual(
            model_gate(Customer),
            [
                {"name": "customer_id", "ty": "long", "required": True},
                {"name": "name", "ty": "string", "required": True},
                {"name": "tier", "ty": "string", "required": False},
                {"name": "created_at", "ty": "timestamp", "required": True},
            ],
        )

    def test_order_columns(self) -> None:
        self.assertEqual(
            model_gate(Order),
            [
                {"name": "order_id", "ty": "long", "required": True},
                {"name": "customer_id", "ty": "long", "required": True},
                {"name": "total", "ty": "double", "required": True},
            ],
        )


class DefinitionTimeErrorsTest(unittest.TestCase):
    def test_missing_table_raises(self) -> None:
        with self.assertRaises(TypeError):

            class NoTable(LoomModel):
                id: Identity[int]

    def test_zero_identity_fields_raises(self) -> None:
        with self.assertRaises(TypeError):

            class NoIdentity(LoomModel, table=("s", "t")):
                name: str

    def test_two_identity_fields_raises(self) -> None:
        with self.assertRaises(TypeError):

            class TwoIdentities(LoomModel, table=("s", "t")):
                a: Identity[int]
                b: Identity[int]

    def test_unmappable_annotation_raises(self) -> None:
        with self.assertRaises(TypeError):

            class BadField(LoomModel, table=("s", "t")):
                id: Identity[int]
                blob: bytes

    def test_link_to_class_lacking_identity_raises(self) -> None:
        with self.assertRaises(TypeError):

            class BadLink(LoomModel, table=("s", "t")):
                id: Identity[int]
                base: Link[LoomModel]

    def test_duplicate_fk_column_without_override_raises(self) -> None:
        with self.assertRaises(TypeError):

            class TwoLinksSameTarget(LoomModel, table=("s", "t")):
                id: Identity[int]
                buyer: Link[Customer]
                seller: Link[Customer]

    def test_duplicate_fk_column_resolved_by_explicit_column_override(self) -> None:
        class TwoLinksDisambiguated(LoomModel, table=("s", "t")):
            id: Identity[int]
            buyer: Link[Customer, "buyer_id"]
            seller: Link[Customer]

        self.assertEqual(
            [(link.field, link.fk_column) for link in TwoLinksDisambiguated.__loom_links__],
            [("buyer", "buyer_id"), ("seller", "customer_id")],
        )


if __name__ == "__main__":
    unittest.main()
