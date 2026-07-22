"""Acceptance test for road-python-build-infra.

The three SDK deps import and exercise their native code on the hermetic
CPython 3.13 toolchain, proving the muntjac-generated //third-party/python
wheels agree with the interpreter.
"""

import sys
import unittest


class ImportsTest(unittest.TestCase):
    def test_interpreter_is_pinned_cpython_313(self) -> None:
        self.assertEqual((sys.version_info.major, sys.version_info.minor), (3, 13))

    def test_httpx(self) -> None:
        import httpx

        request = httpx.Request("GET", "http://loom.invalid/objects/Customer")
        self.assertEqual(request.url.host, "loom.invalid")

    def test_pyarrow_native(self) -> None:
        import pyarrow as pa

        table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        self.assertEqual(table.num_rows, 3)
        self.assertEqual(table.column("id").type, pa.int64())

    def test_pydantic_core_native(self) -> None:
        import pydantic

        class Row(pydantic.BaseModel):
            id: int
            name: str

        row = Row.model_validate({"id": 7, "name": "ada"})
        self.assertEqual(row.id, 7)
        with self.assertRaises(pydantic.ValidationError):
            Row.model_validate({"id": "not-an-int-7x", "name": "ada"})


if __name__ == "__main__":
    unittest.main()
