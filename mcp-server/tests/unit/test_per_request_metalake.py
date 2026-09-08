# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Tests for per-call metalake selection.

Any tool call may name the metalake it operates on with a `metalake` argument,
falling back to the server's `--metalake` default. The argument is advertised
and consumed by MetalakeArgumentMiddleware, so no tool declares it, and it is
carried in a context variable scoped to one call - never across calls, never
shared between server replicas.
"""

import asyncio
import sys
import unittest
from unittest import mock
from unittest.mock import MagicMock, patch

from fastmcp import Client

from mcp_server.client.factory import RESTClientFactory
from mcp_server.client.plain.plain_rest_client_operation import (
    PlainRESTClientOperation,
)
from mcp_server.core import context as context_module
from mcp_server.core.context import (
    METALAKE_ARGUMENT,
    GravitinoContext,
    ServiceIdentityFallbackDisabled,
    get_request_metalake,
    reset_request_metalake,
    set_request_metalake,
)
from mcp_server.core.middleware import (
    TOOLS_WITHOUT_METALAKE,
    _schema_with_metalake,
)
from mcp_server.core.setting import Setting
from mcp_server.main import _parse_args, do_main
from mcp_server.server import GravitinoMCPServer
from tests.unit.tools import MockOperation

# Tests intentionally exercise context internals (_default_client,
# _clients_by_auth, _catalog_operation) to assert per-call isolation;
# protected access is expected.
# pylint: disable=protected-access


class TestSchemaInjection(unittest.TestCase):
    """_schema_with_metalake() advertises the argument without breaking tools."""

    def test_adds_optional_metalake_property(self):
        schema = _schema_with_metalake(
            {"type": "object", "properties": {"name": {"type": "string"}}}
        )
        self.assertIn(METALAKE_ARGUMENT, schema["properties"])
        self.assertEqual(
            schema["properties"][METALAKE_ARGUMENT]["type"], "string"
        )

    def test_does_not_make_metalake_required(self):
        """Omitting it is what every single-metalake deployment does."""
        schema = _schema_with_metalake(
            {
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "required": ["name"],
            }
        )
        self.assertEqual(schema["required"], ["name"])

    def test_does_not_mutate_the_original_schema(self):
        original = {
            "type": "object",
            "properties": {"name": {"type": "string"}},
        }
        _schema_with_metalake(original)
        self.assertNotIn(METALAKE_ARGUMENT, original["properties"])

    def test_never_shadows_a_parameter_the_tool_declares(self):
        original = {
            "type": "object",
            "properties": {METALAKE_ARGUMENT: {"type": "integer"}},
        }
        schema = _schema_with_metalake(original)
        self.assertEqual(
            schema["properties"][METALAKE_ARGUMENT]["type"], "integer"
        )


class TestMiddlewareOverTheProtocol(unittest.TestCase):
    """End-to-end through a real FastMCP client, not a stubbed context."""

    def setUp(self):
        RESTClientFactory.set_rest_client(MockOperation)
        self.mcp = GravitinoMCPServer(Setting("mock_metalake")).mcp

    def tearDown(self):
        RESTClientFactory.set_rest_client(PlainRESTClientOperation)

    def test_every_metalake_scoped_tool_advertises_the_argument(self):
        async def _run():
            async with Client(self.mcp) as client:
                return await client.list_tools()

        tools = asyncio.run(_run())
        self.assertTrue(tools)
        missing = [
            t.name
            for t in tools
            if t.name not in TOOLS_WITHOUT_METALAKE
            and METALAKE_ARGUMENT not in (t.inputSchema.get("properties") or {})
        ]
        self.assertEqual(missing, [])

    def test_tool_call_accepts_and_consumes_the_argument(self):
        """The tool function must not receive an argument it cannot accept."""

        async def _run():
            async with Client(self.mcp) as client:
                return await client.call_tool(
                    "get_list_of_catalogs", {METALAKE_ARGUMENT: "other_ml"}
                )

        # Would raise if the argument reached the tool function.
        result = asyncio.run(_run())
        self.assertIsNotNone(result)

    def test_metalake_does_not_leak_into_the_next_call(self):
        """The middleware must reset the context variable after every call.

        Without the reset this per-call plumbing would silently become session
        state - exactly what this design exists to avoid.
        """
        seen = []

        async def _run():
            async with Client(self.mcp) as client:
                await client.call_tool(
                    "get_list_of_catalogs", {METALAKE_ARGUMENT: "first_ml"}
                )
                seen.append(get_request_metalake())
                await client.call_tool("get_list_of_catalogs")
                seen.append(get_request_metalake())

        asyncio.run(_run())
        self.assertEqual(seen, ["", ""])


class _EchoCatalogOperation:
    """A catalog listing that reports which metalake its client was built for,
    and parks until every in-flight call has arrived."""

    barrier = None

    def __init__(self, metalake_name):
        self._metalake_name = metalake_name

    async def get_list_of_catalogs(self) -> str:
        if _EchoCatalogOperation.barrier is not None:
            await _EchoCatalogOperation.barrier()
        return self._metalake_name


class _EchoMetalakeClient(MockOperation):
    """MockOperation that remembers the metalake it was constructed with."""

    def __init__(self, metalake, uri, authorization="", *, auth=None):
        super().__init__(metalake, uri, authorization, auth=auth)
        self._metalake = metalake

    def as_catalog_operation(self):
        return _EchoCatalogOperation(self._metalake)


class TestConcurrentToolCallsOverTheProtocol(unittest.TestCase):
    """The isolation guarantee, proven on the real path.

    The context-level concurrency test drives set_request_metalake() directly.
    This one goes through the middleware and the MCP protocol, which is what
    actually sets and resets the context variable per call - line coverage of
    the middleware does not prove two overlapping calls stay separate.
    """

    def setUp(self):
        RESTClientFactory.set_rest_client(_EchoMetalakeClient)
        self.mcp = GravitinoMCPServer(Setting("ml_default")).mcp

    def tearDown(self):
        _EchoCatalogOperation.barrier = None
        RESTClientFactory.set_rest_client(PlainRESTClientOperation)

    def test_overlapping_calls_each_use_their_own_metalake(self):
        arrived = asyncio.Event()
        counter = {"n": 0}

        async def _barrier():
            # Neither call may finish until both are inside the tool, so the
            # two requests are genuinely in flight at the same time.
            counter["n"] += 1
            if counter["n"] == 2:
                arrived.set()
            await arrived.wait()

        _EchoCatalogOperation.barrier = _barrier

        async def _run():
            async with Client(self.mcp) as client:
                return await asyncio.gather(
                    client.call_tool(
                        "get_list_of_catalogs", {METALAKE_ARGUMENT: "ml_a"}
                    ),
                    client.call_tool(
                        "get_list_of_catalogs", {METALAKE_ARGUMENT: "ml_b"}
                    ),
                )

        first, second = asyncio.run(asyncio.wait_for(_run(), timeout=10))

        self.assertEqual(
            [first.content[0].text, second.content[0].text], ["ml_a", "ml_b"]
        )

    def test_overlapping_calls_do_not_poison_the_default(self):
        """A call that names no metalake must still get the default even while
        another call naming one is in flight."""
        arrived = asyncio.Event()
        counter = {"n": 0}

        async def _barrier():
            counter["n"] += 1
            if counter["n"] == 2:
                arrived.set()
            await arrived.wait()

        _EchoCatalogOperation.barrier = _barrier

        async def _run():
            async with Client(self.mcp) as client:
                return await asyncio.gather(
                    client.call_tool(
                        "get_list_of_catalogs", {METALAKE_ARGUMENT: "ml_named"}
                    ),
                    client.call_tool("get_list_of_catalogs"),
                )

        named, defaulted = asyncio.run(asyncio.wait_for(_run(), timeout=10))

        self.assertEqual(named.content[0].text, "ml_named")
        self.assertEqual(defaulted.content[0].text, "ml_default")


class TestDiscoveryWithoutADefaultMetalake(unittest.TestCase):
    """A server with no --metalake must still be usable from a cold start.

    list_metalakes is the tool an agent reaches for when it does not know a
    metalake yet, so it must not be gated behind having one - otherwise it is
    unusable on exactly the deployment that needs it.
    """

    def setUp(self):
        RESTClientFactory.set_rest_client(MockOperation)
        self.mcp = GravitinoMCPServer(Setting(metalake="")).mcp

    def tearDown(self):
        RESTClientFactory.set_rest_client(PlainRESTClientOperation)

    def test_list_metalakes_works_with_no_metalake_configured(self):
        async def _run():
            async with Client(self.mcp) as client:
                return await client.call_tool("list_metalakes")

        result = asyncio.run(_run())
        self.assertEqual(result.content[0].text, "mock_metalakes")

    def test_other_tools_report_how_to_recover(self):
        """The agent should be told to call list_metalakes, not just fail."""

        async def _run():
            async with Client(self.mcp) as client:
                return await client.call_tool("get_list_of_catalogs")

        with self.assertRaises(Exception) as raised:
            asyncio.run(_run())
        self.assertIn("list_metalakes", str(raised.exception))

    def test_naming_a_metalake_per_call_works_with_no_default(self):
        async def _run():
            async with Client(self.mcp) as client:
                return await client.call_tool(
                    "get_list_of_catalogs", {METALAKE_ARGUMENT: "ml_a"}
                )

        self.assertIsNotNone(asyncio.run(_run()))


class TestToolsThatNeverResolveAMetalake(unittest.TestCase):
    """Tools that ignore the metalake must not advertise the argument."""

    def setUp(self):
        RESTClientFactory.set_rest_client(MockOperation)
        self.mcp = GravitinoMCPServer(Setting("mock_metalake")).mcp

    def tearDown(self):
        RESTClientFactory.set_rest_client(PlainRESTClientOperation)

    def test_discovery_and_pure_computation_tools_skip_the_argument(self):
        """Offering a knob that does nothing invites the model to misuse it -
        on list_metalakes it would otherwise be the only parameter, reading
        like a filter for the listing."""

        async def _run():
            async with Client(self.mcp) as client:
                return {
                    t.name: t.inputSchema for t in await client.list_tools()
                }

        schemas = asyncio.run(_run())
        for name in ("list_metalakes", "metadata_type_to_fullname_formats"):
            self.assertNotIn(
                METALAKE_ARGUMENT,
                schemas[name].get("properties") or {},
                f"{name} should not advertise the metalake argument",
            )


class TestRecoveryHintMatchesTheDeployment(unittest.TestCase):
    """--include-tool-tags is an allowlist and can hide list_metalakes."""

    def setUp(self):
        RESTClientFactory.set_rest_client(MockOperation)

    def tearDown(self):
        RESTClientFactory.set_rest_client(PlainRESTClientOperation)

    def _call_with_tags(self, tags):
        mcp = GravitinoMCPServer(Setting("", tags=tags)).mcp

        async def _run():
            async with Client(mcp) as client:
                await client.call_tool("get_list_of_catalogs")

        with self.assertRaises(Exception) as raised:
            asyncio.run(_run())
        return str(raised.exception)

    def test_hint_points_at_discovery_when_it_is_exposed(self):
        self.assertIn("list_metalakes", self._call_with_tags(set()))

    def test_hint_omits_discovery_when_a_tag_filter_hides_it(self):
        """Naming a tool the agent cannot call leaves it with no way forward."""
        message = self._call_with_tags({"catalog"})
        self.assertNotIn("list_metalakes", message)
        self.assertIn(f"'{METALAKE_ARGUMENT}' argument", message)


class TestMetalakeResolution(unittest.TestCase):
    """GravitinoContext resolves the metalake per call."""

    def setUp(self):
        RESTClientFactory.set_rest_client(PlainRESTClientOperation)

    def _make_context(self, metalake: str = "ml_default") -> GravitinoContext:
        return GravitinoContext(
            Setting(
                metalake=metalake,
                gravitino_uri="http://localhost:8090",
                transport="http",
            )
        )

    def test_call_argument_overrides_startup_default(self):
        ctx = self._make_context()
        token = set_request_metalake("ml_other")
        try:
            client = ctx.rest_client()
        finally:
            reset_request_metalake(token)

        self.assertEqual(client._catalog_operation.metalake_name, "ml_other")

    def test_falls_back_to_startup_default(self):
        ctx = self._make_context()
        client = ctx.rest_client()

        self.assertIs(client, ctx._default_client)
        self.assertEqual(client._catalog_operation.metalake_name, "ml_default")

    def test_whitespace_only_argument_is_treated_as_absent(self):
        ctx = self._make_context()
        token = set_request_metalake("   ")
        try:
            client = ctx.rest_client()
        finally:
            reset_request_metalake(token)

        self.assertEqual(client._catalog_operation.metalake_name, "ml_default")

    def test_missing_metalake_raises_with_a_recoverable_message(self):
        """The message is read by an agent, so it must name the way out."""
        ctx = self._make_context(metalake="")

        with self.assertRaises(ValueError) as raised:
            ctx.rest_client()

        message = str(raised.exception)
        self.assertIn("list_metalakes", message)
        self.assertIn(f"'{METALAKE_ARGUMENT}' argument", message)

    def test_missing_metalake_takes_priority_over_fallback_disabled(self):
        ctx = GravitinoContext(
            Setting(
                metalake="",
                gravitino_uri="http://localhost:8090",
                transport="http",
                token="static-token",
                no_service_identity_fallback=True,
            )
        )

        with patch(
            "fastmcp.server.dependencies.get_http_request",
            side_effect=LookupError,
        ):
            with self.assertRaises(ValueError) as raised:
                ctx.rest_client()

        self.assertNotIsInstance(
            raised.exception, ServiceIdentityFallbackDisabled
        )

    def test_discovery_works_with_no_metalake_anywhere(self):
        """list_metalakes is what an agent calls before it knows a metalake,
        so it must not require one - otherwise it is unusable on exactly the
        server that needs it."""
        ctx = self._make_context(metalake="")

        client = ctx.rest_client(require_metalake=False)

        self.assertIsNotNone(client.as_metalake_operation())

    def test_two_concurrent_calls_get_different_metalake_clients(self):
        """Two calls naming different metalakes, in flight at the same time,
        must each get their own client. Both tasks publish their metalake and
        then park until the other has too, so the calls genuinely overlap."""
        ctx = self._make_context()

        async def _call(metalake, ready, go):
            token = set_request_metalake(metalake)
            try:
                ready.set()
                await go.wait()
                return ctx.rest_client()
            finally:
                reset_request_metalake(token)

        async def _drive():
            ready_a, ready_b, go = (
                asyncio.Event(),
                asyncio.Event(),
                asyncio.Event(),
            )
            task_a = asyncio.ensure_future(_call("ml_a", ready_a, go))
            task_b = asyncio.ensure_future(_call("ml_b", ready_b, go))
            await ready_a.wait()
            await ready_b.wait()
            go.set()
            return await asyncio.gather(task_a, task_b)

        client_a, client_b = asyncio.run(_drive())

        self.assertEqual(client_a._catalog_operation.metalake_name, "ml_a")
        self.assertEqual(client_b._catalog_operation.metalake_name, "ml_b")
        self.assertIsNot(client_a, client_b)

    def test_same_identity_and_metalake_reuses_cached_client(self):
        ctx = self._make_context()
        token = set_request_metalake("ml_a")
        try:
            first = ctx.rest_client()
            second = ctx.rest_client()
        finally:
            reset_request_metalake(token)

        self.assertIs(first, second)

    def test_one_bound_covers_principal_and_service_clients_together(self):
        """_MAX_CACHED_CLIENTS bounds the total number of open connection
        pools, not each cache separately."""
        ctx = self._make_context()
        cap = context_module._MAX_CACHED_CLIENTS

        def _mock_request(authorization):
            request = MagicMock()
            request.headers.get.side_effect = (
                lambda key, default="": authorization
            )
            return request

        for i in range(cap):
            with patch(
                "fastmcp.server.dependencies.get_http_request",
                return_value=_mock_request(f"Bearer t{i}"),
            ):
                ctx.rest_client()

        for i in range(10):
            token = set_request_metalake(f"ml_{i}")
            try:
                ctx.rest_client()
            finally:
                reset_request_metalake(token)

        self.assertLessEqual(len(ctx._clients_by_auth), cap)


class TestSettingMetalake(unittest.TestCase):
    def test_whitespace_only_metalake_is_stripped_to_empty(self):
        """A shell-quoting mistake like --metalake "  " must not read as a
        configured default."""
        self.assertEqual(Setting(metalake="  ").metalake, "")

    def test_stdio_without_metalake_is_allowed(self):
        """stdio can now name a metalake per call like any other transport, so
        --metalake is no longer required there."""
        GravitinoContext(Setting(metalake="", transport="stdio"))


class TestMetalakeArgParsing(unittest.TestCase):
    def test_metalake_is_optional_and_defaults_to_empty(self):
        with mock.patch.object(sys, "argv", ["mcp_server"]):
            args = _parse_args()
        self.assertEqual(args.metalake, "")

    def test_server_starts_without_a_default_metalake(self):
        with mock.patch("mcp_server.main._init_logging"), mock.patch(
            "mcp_server.main.GravitinoMCPServer"
        ) as server, mock.patch.object(sys, "argv", ["mcp_server"]):
            do_main()
        server.assert_called_once()


if __name__ == "__main__":
    unittest.main()
