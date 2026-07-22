"""Tests for loom_sdk._arrow: Arrow IPC stream encoding helpers."""

from __future__ import annotations

import datetime
import unittest

import pyarrow as pa

from loom_sdk._arrow import empty_ipc, to_ipc


class ToIpcTest(unittest.TestCase):
    def test_round_trips_list_of_dicts(self) -> None:
        data = [{"id": 1, "name": "ada"}, {"id": 2, "name": "grace"}]
        encoded = to_ipc(data)

        with pa.ipc.open_stream(encoded) as reader:
            table = reader.read_all()

        self.assertEqual(table, pa.Table.from_pylist(data))

    def test_round_trips_with_explicit_schema(self) -> None:
        schema = pa.schema([("id", pa.int64()), ("ts", pa.timestamp("us"))])
        data = [{"id": 1, "ts": datetime.datetime(2026, 1, 1, 12, 0, 0)}]
        encoded = to_ipc(data, schema=schema)

        with pa.ipc.open_stream(encoded) as reader:
            table = reader.read_all()

        self.assertEqual(table.schema, schema)
        self.assertEqual(table, pa.Table.from_pylist(data, schema=schema))

    def test_round_trips_table(self) -> None:
        table_in = pa.table({"id": [1, 2, 3]})
        encoded = to_ipc(table_in)

        with pa.ipc.open_stream(encoded) as reader:
            table_out = reader.read_all()

        self.assertEqual(table_out, table_in)

    def test_round_trips_record_batch(self) -> None:
        batch = pa.record_batch({"id": [1, 2, 3]})
        encoded = to_ipc(batch)

        with pa.ipc.open_stream(encoded) as reader:
            table_out = reader.read_all()

        self.assertEqual(table_out, pa.Table.from_batches([batch]))


class EmptyIpcTest(unittest.TestCase):
    def test_zero_row_stream_preserves_schema(self) -> None:
        schema = pa.schema(
            [
                ("id", pa.int64()),
                ("created", pa.timestamp("us")),
                ("dob", pa.date32()),
            ]
        )
        encoded = empty_ipc(schema)

        with pa.ipc.open_stream(encoded) as reader:
            table = reader.read_all()

        self.assertEqual(table.num_rows, 0)
        self.assertEqual(table.schema, schema)


if __name__ == "__main__":
    unittest.main()
