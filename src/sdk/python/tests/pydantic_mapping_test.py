"""Tests for the pydantic declaration layer: `LoomModel`/`Identity`/`Link`
class machinery and `loom_sdk.pydantic._mapping`'s pure type-mapping
functions.
"""

from __future__ import annotations

import datetime
import typing
import unittest

import pyarrow as pa
import pydantic

from loom_sdk.pydantic import Identity, Link, LoomModel, arrow_schema, model_gate
from loom_sdk.pydantic import _PendingLink  # noqa: F401  (private marker under test)
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


class PendingLinkGetitemTest(unittest.TestCase):
    def test_string_target_two_arg_returns_pending_marker(self) -> None:
        annotated = Link["Node", "parent_id"]
        self.assertIs(typing.get_origin(annotated), typing.Annotated)
        inner, marker = typing.get_args(annotated)
        self.assertIs(inner, typing.Any)
        self.assertEqual(marker, _PendingLink(name="Node", column="parent_id"))

    def test_string_target_one_arg_returns_pending_marker_with_no_column(self) -> None:
        annotated = Link["Node"]
        _inner, marker = typing.get_args(annotated)
        self.assertEqual(marker, _PendingLink(name="Node", column=None))

    def test_real_class_target_keeps_eager_link_marker(self) -> None:
        annotated = Link[Customer]
        inner, marker = typing.get_args(annotated)
        self.assertIs(inner, int)  # Customer.customer_id is Identity[int]
        self.assertIsInstance(marker, Link)
        self.assertIs(marker.target, Customer)


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

    def test_optional_identity_raises_deliberate_error(self) -> None:
        # Finding 1: `Identity[int] | None` must raise a deliberate, clearly
        # worded error — the identity is always required — rather than fail
        # incidentally (e.g. an "unmappable annotation" or a miscounted
        # "found 0 Identity fields" error).
        with self.assertRaisesRegex(TypeError, "identity property cannot be optional"):

            class BadOptionalIdentity(LoomModel, table=("s", "t")):
                id: Identity[int] | None = None

    def test_forward_referenced_link_target_raises_ordering_error(self) -> None:
        # Finding 2: a link to a not-yet-defined sibling class must raise a
        # TypeError stating the actual constraint (declare-before-link
        # ordering), not the misleading "unmappable annotation:
        # ForwardRef(...)" that leaks out of `loom_type` today.
        with self.assertRaisesRegex(TypeError, "fully-defined LoomModel classes declared before"):

            class OrderForward(LoomModel, table=("s", "o")):
                order_id: Identity[int]
                customer: Link[CustomerDeclaredLater]

            class CustomerDeclaredLater(LoomModel, table=("s", "c")):
                customer_id: Identity[int]

    def test_self_referential_link_raises_ordering_error(self) -> None:
        # Finding 2: a self-referential link must raise a TypeError stating
        # the actual constraint (self-reference unsupported in v1), not the
        # misleading "has no loom identity" that leaks out today (the target
        # resolves to the still-under-construction class itself).
        with self.assertRaisesRegex(TypeError, "self-referential"):

            class Node(LoomModel, table=("s", "n")):
                node_id: Identity[int]
                parent: Link[Node] | None = None

    def test_fk_column_collision_with_plain_property_raises_naming_both_fields(self) -> None:
        # Finding 3: an FK column silently colliding with a plain property
        # of the same name must raise, naming both fields and suggesting the
        # `Link[Target, "other_col"]` override.
        with self.assertRaises(TypeError) as ctx:

            class OrderWithCollidingColumn(LoomModel, table=("s", "o")):
                order_id: Identity[int]
                customer_id: int
                customer: Link[Customer]

        message = str(ctx.exception)
        self.assertIn("customer_id", message)
        self.assertIn("customer", message)
        self.assertIn("other_column", message)


class OptionalLinkTest(unittest.TestCase):
    def test_optional_link_maps_to_nullable_fk_and_still_emits_link_spec(self) -> None:
        # Finding 1: `Link[Customer] | None` (a nullable FK) must not raise
        # "unmappable annotation" — it should map to the FK property with
        # required=False, and the link spec must still be emitted.
        class OrderMaybeCustomer(LoomModel, table=("s", "t")):
            id: Identity[int]
            customer: Link[Customer] | None = None

        self.assertEqual(
            OrderMaybeCustomer.__loom_properties__,
            [("id", "long", True), ("customer_id", "long", False)],
        )
        self.assertEqual(len(OrderMaybeCustomer.__loom_links__), 1)
        link = OrderMaybeCustomer.__loom_links__[0]
        self.assertEqual(link.field, "customer")
        self.assertIs(link.target, Customer)
        self.assertEqual(link.fk_column, "customer_id")


class SelfReferentialLinkTest(unittest.TestCase):
    def test_optional_self_link_builds_derived_attributes(self) -> None:
        class Node(LoomModel, table=("graph", "nodes")):
            node_id: Identity[int]
            label: str
            parent: Link["Node", "parent_id"] | None = None

        self.assertEqual(Node.__loom_identity__, "node_id")
        self.assertEqual(
            Node.__loom_properties__,
            [("node_id", "long", True), ("label", "string", True), ("parent_id", "long", False)],
        )
        self.assertEqual(len(Node.__loom_links__), 1)
        link = Node.__loom_links__[0]
        self.assertEqual(link.field, "parent")
        self.assertIs(link.target, Node)
        self.assertEqual(link.fk_column, "parent_id")

    def test_arrow_schema_and_gate_include_nullable_fk(self) -> None:
        class Node(LoomModel, table=("graph", "nodes")):
            node_id: Identity[int]
            parent: Link["Node", "parent_id"] | None = None

        schema = arrow_schema(Node)
        self.assertEqual(schema.field("parent_id").type, pa.int64())
        self.assertTrue(schema.field("parent_id").nullable)
        self.assertEqual(
            model_gate(Node)[-1], {"name": "parent_id", "ty": "long", "required": False}
        )

    def test_post_rebuild_validation_rejects_non_int_fk(self) -> None:
        class Node(LoomModel, table=("graph", "nodes")):
            node_id: Identity[int]
            parent: Link["Node", "parent_id"] | None = None

        # Annotation repair restored full pydantic typing of the FK value.
        Node(node_id=1, parent=2)          # valid
        Node(node_id=1)                    # default None
        with self.assertRaises(pydantic.ValidationError):
            Node(node_id=1, parent="not-an-int")

    def test_non_optional_self_link_builds(self) -> None:
        class Tree(LoomModel, table=("graph", "trees")):
            tree_id: Identity[int]
            root: Link["Tree", "root_id"]

        self.assertEqual(
            Tree.__loom_properties__,
            [("tree_id", "long", True), ("root_id", "long", True)],
        )
        self.assertEqual(Tree.__loom_links__[0].fk_column, "root_id")
        with self.assertRaises(pydantic.ValidationError):
            Tree(tree_id=1, root="nope")

    def test_one_arg_self_link_raises_collision_naming_column_hint(self) -> None:
        with self.assertRaises(TypeError) as ctx:

            class BadNode(LoomModel, table=("graph", "nodes")):
                node_id: Identity[int]
                parent: Link["BadNode"] | None = None

        message = str(ctx.exception)
        self.assertIn("node_id", message)
        self.assertIn("other_column", message)

    def test_string_target_naming_other_class_raises_forward_ref_error(self) -> None:
        with self.assertRaisesRegex(TypeError, "forward reference"):

            class Widget(LoomModel, table=("s", "w")):
                widget_id: Identity[int]
                other: Link["SomethingElse", "other_id"] | None = None

    def test_whole_string_annotation_still_raises(self) -> None:
        with self.assertRaisesRegex(TypeError, "not supported in v1"):

            class Stringy(LoomModel, table=("s", "n")):
                node_id: Identity[int]
                parent: "Link[Stringy] | None" = None


if __name__ == "__main__":
    unittest.main()
