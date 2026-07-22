"""End-to-end smoke test: boots the *real* `loom` composite (embedded Postgres,
engine + ingest + query-api + worker in one process) as a subprocess, then drives
it entirely over HTTP from `loom_sdk.AsyncClient` — no MockTransport anywhere in
this file. This is the SDK's one contact point with a real server; if a wire
assumption baked into the mocked unit tests is wrong, this is where it shows up.

Deliberately a single test method (`unittest` gives no cheap fixture reuse across
methods, and the composite boot — embedded initdb + migrations — is the expensive
part): boot -> create-admin -> login -> apply -> land -> re-apply(idempotent) ->
preview -> ontology read -> one deliberate conformance violation -> teardown.

Env (`STANDALONE_BIN`, `POSTGRES_BIN_DIR`, `POSTGRES_LD_LIBRARY_PATH`,
`LOOM_PG_FIXTURE_SLOT_DIR`) comes from `src/sdk/python/BUCK`'s `:e2e` target,
which copies the `loom_fixture_test` fixture wiring (`src/control-plane/postgres/
defs.bzl`) plus a `configured_alias`-pinned `STANDALONE_BIN` (see that BUCK file's
comments for why the alias is needed).
"""

from __future__ import annotations

import asyncio
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

import httpx

from loom_sdk import AsyncClient, ConformanceError
from loom_sdk.pydantic import Identity, Link, LoomModel


class Customer(LoomModel, table=("crm", "customers")):
    customer_id: Identity[int]
    name: str


class Order(LoomModel, table=("crm", "orders")):
    order_id: Identity[int]
    customer: Link[Customer]
    total: float


ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "e2e-smoke-password"  # ephemeral, per-run embedded-pg fixture only
ACL_ROLE = "e2e-smoke"

READY_TIMEOUT_S = 90.0
POLL_INTERVAL_S = 0.5


def _free_port() -> int:
    """Bind an ephemeral TCP port on 127.0.0.1, then release it.

    Bind-and-release, not a held listener: the composite binds the real
    listener a moment later. Accepted race (loopback, single test process).
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]
    finally:
        sock.close()


def _dump_log(label: str, path: Path) -> str:
    """Read back a subprocess log file for a failure message. Never raises."""
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        return f"--- {label} (unreadable: {exc}) ---"
    return f"--- {label} ---\n{text}\n--- end {label} ---"


class CompositeSmokeTest(unittest.TestCase):
    def test_smoke_against_real_composite(self) -> None:
        standalone_bin = os.environ["STANDALONE_BIN"]

        with tempfile.TemporaryDirectory() as data_dir:
            data_path = Path(data_dir)
            (data_path / "warehouse").mkdir(parents=True, exist_ok=True)

            query_port = _free_port()
            ingest_port = _free_port()
            engine_socket = data_path / "engine.sock"

            # Embedded-PG + composite env. Mirrors `embedded_config` in
            # `src/services/standalone/tests/composite_e2e.rs`, plus the two bind
            # addrs and the engine UDS path that only the real `loom` binary's
            # `main.rs` (not the in-process `standalone::run` the Rust test calls
            # directly) requires from the env.
            env = dict(os.environ)
            env.update(
                {
                    "LOOM_BIND_ADDR": "127.0.0.1:0",  # unused by the composite
                    "LOOM_DB_HOST": str(data_path / "pgrun"),
                    "LOOM_DB_PORT": "5432",
                    "LOOM_DB_USER": "postgres",
                    "LOOM_DB_PASSWORD": "postgres",
                    "LOOM_DB_NAME": "loom",
                    "LOOM_DATA_PATH": str(data_path),
                    "LOOM_WAREHOUSE_URI": f"file://{data_path / 'warehouse'}",
                    "LOOM_PG_MODE": "embedded",
                    "LOOM_PG_BIN_DIR": os.environ["POSTGRES_BIN_DIR"],
                    "LOOM_PG_LD_LIBRARY_PATH": os.environ["POSTGRES_LD_LIBRARY_PATH"],
                    "LOOM_QUERY_API_BIND_ADDR": f"127.0.0.1:{query_port}",
                    "LOOM_INGEST_BIND_ADDR": f"127.0.0.1:{ingest_port}",
                    "LOOM_ENGINE_SOCKET": str(engine_socket),
                }
            )

            log_path = data_path / "composite.log"
            log_file = log_path.open("w", encoding="utf-8")
            proc = subprocess.Popen(  # fixed argv, no shell, hermetic buck2-built binary
                [standalone_bin],
                env=env,
                stdout=log_file,
                stderr=subprocess.STDOUT,
            )

            def _stop_composite() -> None:
                if proc.poll() is None:
                    proc.send_signal(signal.SIGTERM)
                    try:
                        proc.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait(timeout=15)
                log_file.close()

            self.addCleanup(_stop_composite)

            query_url = f"http://127.0.0.1:{query_port}"
            ingest_url = f"http://127.0.0.1:{ingest_port}"

            self._wait_for_ready(proc, query_url, log_path)
            self._create_admin(standalone_bin, env, log_path)

            asyncio.run(self._drive(ingest_url, query_url, log_path))

    def _wait_for_ready(self, proc: subprocess.Popen, query_url: str, log_path: Path) -> None:
        """Poll `POST /auth/login` with a bogus (never-registered) username until
        it returns 401 — proof the query-api router is serving, with no admin
        token required (none exists yet). An unknown username 401s immediately,
        with no lockout bookkeeping (`find_password_credential` short-circuits
        before `record_failed_login`), so this is side-effect-free to poll.
        """
        deadline = time.monotonic() + READY_TIMEOUT_S
        last_error: object = None
        with httpx.Client(base_url=query_url, timeout=5.0) as probe:
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    self.fail(
                        f"composite exited early (code {proc.returncode}) before becoming ready\n"
                        f"{_dump_log('composite log', log_path)}"
                    )
                try:
                    resp = probe.post(
                        "/auth/login",
                        json={"username": "__readiness_probe__", "password": "x"},
                    )
                except httpx.TransportError as exc:
                    last_error = exc
                    time.sleep(POLL_INTERVAL_S)
                    continue
                if resp.status_code == 401:
                    return
                last_error = f"status {resp.status_code}: {resp.text!r}"
                time.sleep(POLL_INTERVAL_S)
        self.fail(
            f"composite did not become ready within {READY_TIMEOUT_S}s "
            f"(last probe result: {last_error})\n{_dump_log('composite log', log_path)}"
        )

    def _create_admin(self, standalone_bin: str, env: dict, log_path: Path) -> None:
        """`loom create-admin --username admin`, password piped on stdin."""
        result = subprocess.run(  # fixed argv, no shell, hermetic buck2-built binary
            [standalone_bin, "create-admin", "--username", ADMIN_USERNAME],
            input=f"{ADMIN_PASSWORD}\n",
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        if result.returncode != 0:
            self.fail(
                f"create-admin failed (exit {result.returncode})\n"
                f"stdout: {result.stdout}\nstderr: {result.stderr}\n"
                f"{_dump_log('composite log', log_path)}"
            )

    async def _drive(self, ingest_url: str, query_url: str, log_path: Path) -> None:
        try:
            async with AsyncClient(ingest_url=ingest_url, query_url=query_url) as client:
                await client.login(ADMIN_USERNAME, ADMIN_PASSWORD)

                # Fresh apply: bootstraps both types (zero-row land -> define_model)
                # plus the Order -> Customer FK link. Neither step needs an ACL
                # grant yet: dataset-land is auth-only and /admin/* is gated by the
                # `admin` role alone (which create-admin already assigned) — see
                # `_grant_acl`'s docstring for why the *next* phase does need one.
                report = await client.ontology.apply(Customer, Order)
                self.assertEqual(report.created, ["Customer", "Order"])
                self.assertEqual(report.unchanged, [])
                self.assertEqual(report.links_created, ["Order_customer"])

                await self._grant_acl(client, query_url)

                await client.models.land_instances(
                    [
                        Customer(customer_id=1, name="Ada"),
                        Customer(customer_id=2, name="Grace"),
                    ]
                )
                await client.models.land_instances(
                    [
                        Order(order_id=1, customer=1, total=9.5),
                        Order(order_id=2, customer=2, total=3.25),
                    ]
                )

                # Re-apply is idempotent: both types already match, nothing written.
                report2 = await client.ontology.apply(Customer, Order)
                self.assertEqual(report2.created, [])
                self.assertEqual(report2.unchanged, ["Customer", "Order"])
                self.assertEqual(report2.links_created, [])

                preview = await client.datasets.preview("crm", "customers")
                self.assertEqual(len(preview.rows), 2)

                order_type = await client.ontology.type("Order")
                self.assertIn("Order_customer", [link.name for link in order_type.links])

                # Deliberate conformance violation: `order_id` only, missing the two
                # required columns (`customer_id`, `total`).
                with self.assertRaises(ConformanceError) as ctx:
                    await client.models.land("Order", [{"order_id": 1}])
                reasons = {v.reason for v in ctx.exception.violations}
                self.assertIn("missing_required", reasons)
        except Exception:
            print(_dump_log("composite log", log_path), file=sys.stderr)
            raise

    async def _grant_acl(self, client: AsyncClient, query_url: str) -> None:
        """Grant the admin subject Read+Write on Customer/Order.

        loom's ACL is deny-by-default even for the `admin` role: holding
        `ADMIN_ROLE` only gates `/admin/*` (`service_runtime::require_admin`) —
        it carries no implicit ACL grants, so a governed write (`POST
        /models/{type}`) or read (dataset preview) still 403s/404s without an
        explicit grant. The pydantic SDK layer has no ACL admin surface (out of
        scope for v1 — `client.admin` only wraps `define_model`/`define_link`),
        so this one setup step drives the raw `/admin/roles*` HTTP endpoints
        directly instead of going through the SDK. Both `Customer` and `Order`
        already exist at this point (the preceding `apply()` created them) —
        `POST /admin/roles/{role}/grants` 400s on an unknown type.
        """
        headers = {"Authorization": f"Bearer {client._token}"}  # test-only, no public accessor
        async with httpx.AsyncClient(base_url=query_url, headers=headers) as raw:
            resp = await raw.post("/admin/roles", json={"role": ACL_ROLE})
            assert resp.status_code == 201, f"create role: {resp.status_code} {resp.text}"
            resp = await raw.put(f"/admin/users/{ADMIN_USERNAME}/roles/{ACL_ROLE}")
            assert resp.status_code == 200, f"assign role: {resp.status_code} {resp.text}"
            for action, type_name in (
                ("write", "Customer"),
                ("write", "Order"),
                ("read", "Customer"),
            ):
                resp = await raw.post(
                    f"/admin/roles/{ACL_ROLE}/grants",
                    json={"action": action, "type": type_name},
                )
                assert resp.status_code == 201, f"grant {action} {type_name}: {resp.status_code} {resp.text}"


if __name__ == "__main__":
    unittest.main()
