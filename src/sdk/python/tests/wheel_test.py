"""Guards for the buck2-built loom-sdk wheel (//src/sdk/python:wheel).

Runs as a python_test. Three env vars are wired by the BUCK target:
  WHEEL_PATH             $(location :wheel)         — the built .whl
  PYPROJECT_PATH         $(location :pyproject)     — the committed pyproject.toml
  EXPECTED_WHEEL_VERSION the BUCK `version` attr    — the single source of truth

NOTE: buck2's python_test main (prelude/python/tools/__test_main__.py) discovers
tests via unittest.TestLoader, which only finds unittest.TestCase subclasses —
bare pytest-style `def test_...()` module functions are silently never collected
("NO TESTS RAN" yet reported Pass). So, unlike a plain pytest file, these are
wrapped in a TestCase, matching every other tests/*_test.py in this package.
"""

import os
import tomllib
import unittest
import zipfile


def _env(name: str) -> str:
    value = os.environ.get(name)
    assert value, f"{name} not set — check the :wheel-test BUCK env wiring"
    return value


class WheelTest(unittest.TestCase):
    def test_pyproject_version_matches_buck_wheel_version(self) -> None:
        with open(_env("PYPROJECT_PATH"), "rb") as fh:
            pyproject = tomllib.load(fh)
        pyproject_version = pyproject["project"]["version"]
        assert pyproject_version == _env("EXPECTED_WHEEL_VERSION"), (
            "pyproject.toml [project] version has drifted from the BUCK python_wheel "
            f"version: {pyproject_version!r} != {_env('EXPECTED_WHEEL_VERSION')!r}"
        )

    def test_wheel_ships_both_modules_and_the_pydantic_extra(self) -> None:
        with zipfile.ZipFile(_env("WHEEL_PATH")) as whl:
            names = whl.namelist()
            metadata_name = next(n for n in names if n.endswith(".dist-info/METADATA"))
            metadata = whl.read(metadata_name).decode("utf-8")

        assert any(n.startswith("loom_sdk/") and n.endswith(".py") for n in names), (
            f"loom_sdk/ package missing from wheel: {names}"
        )
        assert any(n.startswith("loom_sdk/pydantic/") for n in names), (
            f"loom_sdk/pydantic/ subpackage missing from wheel: {names}"
        )
        assert "Name: loom-sdk" in metadata, metadata
        assert f"Version: {_env('EXPECTED_WHEEL_VERSION')}" in metadata, metadata
        assert "Provides-Extra: pydantic" in metadata, metadata
        assert "Requires-Dist: httpx>=0.28" in metadata, metadata
        assert "Requires-Dist: pyarrow>=25" in metadata, metadata
        assert 'Requires-Dist: pydantic>=2.11; extra == "pydantic"' in metadata, metadata


if __name__ == "__main__":
    unittest.main()
