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

"""Tests for OAuth2 client-credentials fetch, cache, refresh, and 401 retry."""

# pylint: disable=protected-access

import asyncio
import base64
import json
import sys
import threading
import time
import unittest
from unittest import mock

import httpx
from httpx_auth import OAuth2

from mcp_server.client.factory import RESTClientFactory
from mcp_server.client.plain.plain_rest_client_operation import (
    PlainRESTClientOperation,
)
from mcp_server.core.context import (
    GravitinoContext,
    ServiceIdentityFallbackDisabled,
    service_fallback_authorization,
)
from mcp_server.core.oauth import RefreshableBearerAuth
from mcp_server.core.setting import Setting
from mcp_server.main import _parse_args, do_main


def _jwt_with_exp(exp) -> str:
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').rstrip(b"=").decode()
    payload = (
        base64.urlsafe_b64encode(json.dumps({"exp": exp}).encode())
        .rstrip(b"=")
        .decode()
    )
    return f"{header}.{payload}.sig"


class _OAuthHttpTestCase(unittest.TestCase):
    """Drive ``httpx.AsyncClient(auth=...)`` against a shared MockTransport.

    httpx-auth caches tokens in a process-global map, so every test clears it.
    Tests inject a sync ``httpx.Client`` on the same transport so IdP calls
    never leave the process. Production token POSTs use ``AsyncClient``.
    """

    def setUp(self):
        OAuth2.token_cache.clear()

    def tearDown(self):
        OAuth2.token_cache.clear()

    def _auth(self, handler, **kwargs) -> RefreshableBearerAuth:
        transport = httpx.MockTransport(handler)
        self.addCleanup(OAuth2.token_cache.clear)
        token_client = httpx.Client(transport=transport)
        self.addCleanup(token_client.close)
        auth = RefreshableBearerAuth(
            token_endpoint="https://idp.example/token",
            client_id="mcp",
            client_secret="s3cret",
            client=token_client,
            **kwargs,
        )
        auth._test_transport = transport
        return auth

    def _get(self, auth: RefreshableBearerAuth, path: str = "/api"):
        async def _run():
            async with httpx.AsyncClient(
                auth=auth,
                transport=auth._test_transport,
                base_url="https://gravitino.example",
            ) as client:
                return await client.get(path)

        return asyncio.run(_run())

    def _get_twice(self, auth: RefreshableBearerAuth):
        async def _run():
            async with httpx.AsyncClient(
                auth=auth,
                transport=auth._test_transport,
                base_url="https://gravitino.example",
            ) as client:
                first = await client.get("/api")
                second = await client.get("/api")
                return first, second

        return asyncio.run(_run())


class TestRefreshableBearerAuth(_OAuthHttpTestCase):
    def test_fetches_and_reuses_while_fresh(self):
        calls = {"token": 0, "api": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                calls["token"] += 1
                self.assertEqual(str(request.url), "https://idp.example/token")
                body = request.content.decode()
                self.assertIn("grant_type=client_credentials", body)
                self.assertIn("client_id=mcp", body)
                self.assertIn("client_secret=s3cret", body)
                self.assertIsNone(request.headers.get("authorization"))
                return httpx.Response(
                    200, json={"access_token": "tok-1", "expires_in": 3600}
                )
            calls["api"] += 1
            self.assertEqual(
                request.headers.get("authorization"), "Bearer tok-1"
            )
            return httpx.Response(200, json={"ok": True})

        first, second = self._get_twice(self._auth(handler))
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(calls["token"], 1)
        self.assertEqual(calls["api"], 2)

    def test_parallel_cold_cache_uses_single_token_post(self):
        """Concurrent async callers must not stampede the IdP on cache miss."""
        calls = {"token": 0, "api": 0}
        counter_lock = threading.Lock()

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                with counter_lock:
                    calls["token"] += 1
                time.sleep(0.05)
                return httpx.Response(
                    200, json={"access_token": "tok-1", "expires_in": 3600}
                )
            with counter_lock:
                calls["api"] += 1
            return httpx.Response(200, json={"ok": True})

        auth = self._auth(handler)

        async def run_parallel() -> None:
            async with httpx.AsyncClient(
                auth=auth,
                transport=auth._test_transport,
                base_url="https://gravitino.example",
            ) as client:
                await asyncio.gather(*[client.get("/api") for _ in range(10)])

        asyncio.run(run_parallel())
        self.assertEqual(calls["token"], 1)
        self.assertEqual(calls["api"], 10)

    def test_fresh_token_401_skips_retry(self):
        """A brand-new token that is rejected is not retried or refetched."""
        calls = {"token": 0, "api": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                calls["token"] += 1
                return httpx.Response(
                    200, json={"access_token": "tok-1", "expires_in": 3600}
                )
            calls["api"] += 1
            return httpx.Response(401)

        response = self._get(self._auth(handler))
        self.assertEqual(response.status_code, 401)
        self.assertEqual(calls["token"], 1)
        self.assertEqual(calls["api"], 1)

    def test_parallel_401_does_not_stampede_token_refresh(self):
        """Concurrent 401s must not each invalidate and refetch the token."""
        calls = {"token": 0, "api": 0, "phase": "prime"}
        counter_lock = threading.Lock()

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                with counter_lock:
                    calls["token"] += 1
                time.sleep(0.02)
                return httpx.Response(
                    200, json={"access_token": "tok-1", "expires_in": 3600}
                )
            with counter_lock:
                calls["api"] += 1
                if calls["phase"] == "prime":
                    return httpx.Response(200, json={"ok": True})
            return httpx.Response(401)

        auth = self._auth(handler)

        async def prime_and_parallel() -> None:
            async with httpx.AsyncClient(
                auth=auth,
                transport=auth._test_transport,
                base_url="https://gravitino.example",
            ) as client:
                await client.get("/api")
                calls["phase"] = "parallel"
                token_before = calls["token"]
                await asyncio.gather(*[client.get("/api") for _ in range(10)])
                self.assertLessEqual(
                    calls["token"] - token_before,
                    1,
                    "parallel 401 retry must not stampede the IdP",
                )

        asyncio.run(prime_and_parallel())
        self.assertGreaterEqual(calls["api"], 11)

    def test_config_401_does_not_refetch_on_every_call(self):
        """Persistent Gravitino 401 must not POST to the IdP every call."""
        calls = {"token": 0, "api": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                calls["token"] += 1
                return httpx.Response(
                    200, json={"access_token": "tok-1", "expires_in": 3600}
                )
            calls["api"] += 1
            return httpx.Response(401)

        auth = self._auth(handler)

        async def run_twice() -> None:
            async with httpx.AsyncClient(
                auth=auth,
                transport=auth._test_transport,
                base_url="https://gravitino.example",
            ) as client:
                first = await client.get("/api")
                second = await client.get("/api")
                self.assertEqual(first.status_code, 401)
                self.assertEqual(second.status_code, 401)

        asyncio.run(run_twice())
        self.assertEqual(calls["token"], 1)
        self.assertEqual(calls["api"], 2)

    def test_sends_scope_when_configured(self):
        seen = {}

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                seen["body"] = request.content.decode()
                return httpx.Response(
                    200, json={"access_token": "tok", "expires_in": 3600}
                )
            return httpx.Response(200, json={"ok": True})

        self._get(self._auth(handler, scope="gravitino"))
        self.assertIn("scope=gravitino", seen["body"])

    def test_refetches_when_expired(self):
        calls = {"token": 0}
        seen = []

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                calls["token"] += 1
                return httpx.Response(
                    200,
                    json={
                        "access_token": f"tok-{calls['token']}",
                        "expires_in": 1,
                    },
                )
            seen.append(request.headers.get("authorization"))
            return httpx.Response(200, json={"ok": True})

        # Skew > expires_in makes the cache stale immediately after fetch.
        self._get_twice(self._auth(handler, refresh_skew_seconds=60))
        self.assertEqual(seen, ["Bearer tok-1", "Bearer tok-2"])
        self.assertEqual(calls["token"], 2)

    def test_accepts_string_expires_in(self):
        calls = {"token": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                calls["token"] += 1
                return httpx.Response(
                    200,
                    json={"access_token": "tok-1", "expires_in": "3600"},
                )
            return httpx.Response(200, json={"ok": True})

        self._get_twice(self._auth(handler))
        self.assertEqual(calls["token"], 1)

    def test_expires_in_zero_falls_back_to_jwt_exp(self):
        """expires_in: 0 is falsy; cache from JWT exp instead of refetching."""
        token = _jwt_with_exp(int(time.time()) + 3600)
        calls = {"token": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                calls["token"] += 1
                return httpx.Response(
                    200, json={"access_token": token, "expires_in": 0}
                )
            return httpx.Response(200, json={"ok": True})

        self._get_twice(self._auth(handler))
        self.assertEqual(calls["token"], 1)

    def test_expires_in_zero_without_jwt_exp_raises(self):
        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                return httpx.Response(
                    200, json={"access_token": "opaque-ref", "expires_in": 0}
                )
            return httpx.Response(200)

        with self.assertRaises(ValueError) as raised:
            self._get(self._auth(handler))
        self.assertIn("expires_in", str(raised.exception))

    def test_uses_jwt_exp_when_expires_in_missing(self):
        token = _jwt_with_exp(int(time.time()) + 3600)
        calls = {"token": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                calls["token"] += 1
                return httpx.Response(200, json={"access_token": token})
            self.assertEqual(
                request.headers.get("authorization"), f"Bearer {token}"
            )
            return httpx.Response(200, json={"ok": True})

        self._get_twice(self._auth(handler))
        self.assertEqual(calls["token"], 1)

    def test_jwt_exp_float_accepted(self):
        token = _jwt_with_exp(float(int(time.time()) + 3600))
        calls = {"token": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                calls["token"] += 1
                return httpx.Response(200, json={"access_token": token})
            return httpx.Response(200, json={"ok": True})

        self._get_twice(self._auth(handler))
        self.assertEqual(calls["token"], 1)

    def test_jwt_exp_string_accepted(self):
        token = _jwt_with_exp(str(int(time.time()) + 3600))
        calls = {"token": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                calls["token"] += 1
                return httpx.Response(200, json={"access_token": token})
            return httpx.Response(200, json={"ok": True})

        self._get_twice(self._auth(handler))
        self.assertEqual(calls["token"], 1)

    def test_opaque_token_without_expires_in_raises(self):
        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                return httpx.Response(200, json={"access_token": "opaque-ref"})
            return httpx.Response(200)

        with self.assertRaises(ValueError) as raised:
            self._get(self._auth(handler))
        self.assertIn("expires_in", str(raised.exception))
        self.assertIn("JWT", str(raised.exception))

    def test_jwt_without_exp_and_expires_in_raises(self):
        header = (
            base64.urlsafe_b64encode(b'{"alg":"none"}').rstrip(b"=").decode()
        )
        payload = (
            base64.urlsafe_b64encode(b'{"sub":"mcp"}').rstrip(b"=").decode()
        )
        token = f"{header}.{payload}.sig"

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                return httpx.Response(200, json={"access_token": token})
            return httpx.Response(200)

        with self.assertRaises(ValueError) as raised:
            self._get(self._auth(handler))
        self.assertIn("expires_in", str(raised.exception))

    def test_http_error_is_raised(self):
        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                return httpx.Response(401, json={"error": "invalid_client"})
            return httpx.Response(200)

        with self.assertRaises(httpx.HTTPStatusError):
            self._get(self._auth(handler))

    def test_retries_once_after_401(self):
        calls = {"token": 0, "api": 0, "primed": False}
        seen = []

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                calls["token"] += 1
                return httpx.Response(
                    200,
                    json={
                        "access_token": f"t{calls['token']}",
                        "expires_in": 3600,
                    },
                )
            calls["api"] += 1
            if not calls["primed"]:
                return httpx.Response(200, json={"ok": True})
            seen.append(request.headers.get("authorization"))
            if len(seen) == 1:
                return httpx.Response(401)
            return httpx.Response(200, json={"ok": True})

        auth = self._auth(handler)

        async def _run():
            async with httpx.AsyncClient(
                auth=auth,
                transport=auth._test_transport,
                base_url="https://gravitino.example",
            ) as client:
                await client.get("/api")
                calls["primed"] = True
                return await client.get("/api")

        response = asyncio.run(_run())
        self.assertEqual(response.status_code, 200)
        self.assertEqual(seen, ["Bearer t1", "Bearer t2"])
        self.assertEqual(calls["token"], 2)

    def test_401_retry_replays_streaming_request_body(self):
        seen_bodies = []
        primed = {"done": False}

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST" and request.url.path == "/token":
                return httpx.Response(
                    200, json={"access_token": "t1", "expires_in": 3600}
                )
            seen_bodies.append(request.content)
            if not primed["done"]:
                return httpx.Response(200, json={"ok": True})
            if len(seen_bodies) == 2:
                return httpx.Response(401)
            return httpx.Response(200, json={"ok": True})

        auth = self._auth(handler)

        async def _run():
            async def body():
                yield b'{"name":"fileset"}'

            async with httpx.AsyncClient(
                auth=auth,
                transport=auth._test_transport,
                base_url="https://gravitino.example",
            ) as client:
                await client.get("/api")
                primed["done"] = True
                return await client.post("/api", content=body())

        response = asyncio.run(_run())
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            seen_bodies,
            [b"", b'{"name":"fileset"}', b'{"name":"fileset"}'],
        )

    def test_async_auth_flow_posts_token_with_async_client(self):
        calls = {"token": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                calls["token"] += 1
                return httpx.Response(
                    200, json={"access_token": "tok-async", "expires_in": 3600}
                )
            self.assertEqual(
                request.headers.get("authorization"), "Bearer tok-async"
            )
            return httpx.Response(200, json={"ok": True})

        transport = httpx.MockTransport(handler)
        auth = RefreshableBearerAuth(
            token_endpoint="https://idp.example/token",
            client_id="mcp",
            client_secret="s3cret",
        )
        real_async_client = httpx.AsyncClient

        def async_client_factory(*args, **kwargs):
            kwargs["transport"] = transport
            return real_async_client(*args, **kwargs)

        with mock.patch(
            "mcp_server.core.oauth.httpx.AsyncClient",
            side_effect=async_client_factory,
        ) as async_cls, mock.patch(
            "mcp_server.core.oauth.httpx.Client"
        ) as sync_cls:
            sync_cls.side_effect = AssertionError(
                "async_auth_flow must not use httpx.Client"
            )

            async def _run():
                async with httpx.AsyncClient(
                    auth=auth,
                    transport=transport,
                    base_url="https://gravitino.example",
                ) as client:
                    return await client.get("/api")

            response = asyncio.run(_run())

        self.assertEqual(response.status_code, 200)
        self.assertEqual(calls["token"], 1)
        async_cls.assert_called()


class TestSettingOAuth(unittest.TestCase):
    def test_partial_oauth_is_rejected(self):
        setting = Setting(
            metalake="ml",
            oauth_client_id="mcp",
            oauth_client_secret="s",
        )
        with self.assertRaises(ValueError):
            setting.validate_oauth()

    def test_oauth_scope_without_credentials_is_rejected(self):
        setting = Setting(metalake="ml", oauth_scope="gravitino")
        with self.assertRaises(ValueError) as raised:
            setting.validate_oauth()
        self.assertIn("scope", str(raised.exception).lower())

    def test_complete_oauth_is_accepted(self):
        setting = Setting(
            metalake="ml",
            oauth_token_endpoint="https://idp/token",
            oauth_client_id="mcp",
            oauth_client_secret="s",
        )
        setting.validate_oauth()
        self.assertTrue(setting.has_oauth_client())

    def test_secret_masked_in_str(self):
        setting = Setting(
            metalake="ml",
            oauth_token_endpoint="https://idp/token",
            oauth_client_id="mcp",
            oauth_client_secret="super-oauth-secret",
        )
        self.assertNotIn("super-oauth-secret", str(setting))
        self.assertNotIn("super-oauth-secret", repr(setting))


class TestOAuthArgParsing(unittest.TestCase):
    def test_env_vars_used_when_flags_omitted(self):
        env = {
            "GRAVITINO_OAUTH_TOKEN_ENDPOINT": "https://idp/token",
            "GRAVITINO_OAUTH_CLIENT_ID": "env-id",
            "GRAVITINO_OAUTH_CLIENT_SECRET": "env-secret",
            "GRAVITINO_OAUTH_SCOPE": "env-scope",
        }
        with mock.patch.dict("os.environ", env, clear=True), mock.patch.object(
            sys, "argv", ["prog", "--metalake", "ml"]
        ):
            args = _parse_args()
        self.assertEqual(args.oauth_token_endpoint, "https://idp/token")
        self.assertEqual(args.oauth_client_id, "env-id")
        self.assertEqual(args.oauth_client_secret, "env-secret")
        self.assertEqual(args.oauth_scope, "env-scope")

    def test_cli_overrides_env(self):
        env = {
            "GRAVITINO_OAUTH_CLIENT_ID": "env-id",
            "GRAVITINO_OAUTH_CLIENT_SECRET": "env-secret",
            "GRAVITINO_OAUTH_TOKEN_ENDPOINT": "https://env/token",
        }
        with mock.patch.dict("os.environ", env), mock.patch.object(
            sys,
            "argv",
            [
                "prog",
                "--metalake",
                "ml",
                "--oauth-client-id",
                "cli-id",
                "--oauth-client-secret",
                "cli-secret",
                "--oauth-token-endpoint",
                "https://cli/token",
            ],
        ):
            args = _parse_args()
        self.assertEqual(args.oauth_client_id, "cli-id")
        self.assertEqual(args.oauth_client_secret, "cli-secret")
        self.assertEqual(args.oauth_token_endpoint, "https://cli/token")

    def test_no_service_identity_fallback_from_env(self):
        env = {"GRAVITINO_NO_SERVICE_IDENTITY_FALLBACK": "true"}
        with mock.patch.dict("os.environ", env, clear=True), mock.patch.object(
            sys, "argv", ["prog", "--metalake", "ml"]
        ):
            args = _parse_args()
        self.assertTrue(args.no_service_identity_fallback)


class TestMainOAuthValidation(unittest.TestCase):
    def test_partial_oauth_inits_logging_before_exit(self):
        with mock.patch(
            "mcp_server.main._init_logging"
        ) as init_log, mock.patch(
            "mcp_server.main.GravitinoMCPServer"
        ), mock.patch.object(
            sys,
            "argv",
            ["mcp_server", "--metalake", "ml", "--oauth-client-id", "x"],
        ):
            with self.assertRaises(SystemExit) as raised:
                do_main()
            self.assertEqual(raised.exception.code, 1)
        init_log.assert_called_once()


class TestNoServiceIdentityFallback(unittest.TestCase):
    def setUp(self):
        RESTClientFactory.set_rest_client(PlainRESTClientOperation)

    def test_http_without_auth_raises_when_flag_set(self):
        setting = Setting(
            metalake="ml",
            gravitino_uri="http://localhost:8090",
            transport="http",
            oauth_token_endpoint="https://idp/token",
            oauth_client_id="mcp",
            oauth_client_secret="s",
            no_service_identity_fallback=True,
        )
        ctx = GravitinoContext(setting)
        with mock.patch(
            "mcp_server.core.context._in_http_request", return_value=True
        ), mock.patch(
            "mcp_server.core.context._get_request_authorization",
            return_value="",
        ):
            with self.assertRaises(ServiceIdentityFallbackDisabled):
                ctx.rest_client()

    def test_http_without_auth_allows_fallback_when_flag_false(self):
        setting = Setting(
            metalake="ml",
            gravitino_uri="http://localhost:8090",
            transport="http",
            oauth_token_endpoint="https://idp/token",
            oauth_client_id="mcp",
            oauth_client_secret="s",
        )
        ctx = GravitinoContext(setting)
        with mock.patch(
            "mcp_server.core.context._in_http_request", return_value=True
        ), mock.patch(
            "mcp_server.core.context._get_request_authorization",
            return_value="",
        ):
            client = ctx.rest_client()
        self.assertIs(client, ctx._default_client)

    def test_http_flag_noop_without_service_identity(self):
        setting = Setting(
            metalake="ml",
            gravitino_uri="http://localhost:8090",
            transport="http",
            no_service_identity_fallback=True,
        )
        ctx = GravitinoContext(setting)
        with mock.patch(
            "mcp_server.core.context._in_http_request", return_value=True
        ), mock.patch(
            "mcp_server.core.context._get_request_authorization",
            return_value="",
        ):
            client = ctx.rest_client()
        self.assertIs(client, ctx._default_client)

    def test_stdio_uses_default_client_even_when_flag_set(self):
        setting = Setting(
            metalake="ml",
            gravitino_uri="http://localhost:8090",
            transport="stdio",
            oauth_token_endpoint="https://idp/token",
            oauth_client_id="mcp",
            oauth_client_secret="s",
            no_service_identity_fallback=True,
        )
        ctx = GravitinoContext(setting)
        with mock.patch(
            "mcp_server.core.context._in_http_request", return_value=False
        ), mock.patch(
            "mcp_server.core.context._get_request_authorization",
            return_value="",
        ):
            client = ctx.rest_client()
        self.assertIs(client, ctx._default_client)


class TestServiceFallbackAuthorization(unittest.TestCase):
    def test_token_wins_over_oauth(self):
        setting = Setting(
            metalake="ml",
            token="static-token",
            oauth_token_endpoint="https://idp/token",
            oauth_client_id="mcp-service",
            oauth_client_secret="s",
        )
        self.assertEqual(
            service_fallback_authorization(setting), "Bearer static-token"
        )

    def test_oauth_client_id_used_when_no_token(self):
        setting = Setting(
            metalake="ml",
            oauth_token_endpoint="https://idp/token",
            oauth_client_id="mcp-service",
            oauth_client_secret="s",
        )
        self.assertEqual(
            service_fallback_authorization(setting), "OAuth mcp-service"
        )


class TestGravitinoContextOAuth(unittest.TestCase):
    def setUp(self):
        RESTClientFactory.set_rest_client(PlainRESTClientOperation)

    def test_oauth_uses_httpx_auth_hook(self):
        setting = Setting(
            metalake="ml",
            gravitino_uri="http://localhost:8090",
            oauth_token_endpoint="https://idp/token",
            oauth_client_id="mcp",
            oauth_client_secret="s",
        )
        ctx = GravitinoContext(setting)
        client = ctx.rest_client()
        try:
            rest = client._catalog_operation.rest_client
            self.assertIsInstance(rest, httpx.AsyncClient)
            self.assertIsInstance(rest.auth, RefreshableBearerAuth)
            self.assertIsNone(rest.headers.get("Authorization"))
        finally:
            asyncio.run(client.close())

    def test_static_token_overrides_oauth(self):
        setting = Setting(
            metalake="ml",
            gravitino_uri="http://localhost:8090",
            token="frozen",
            oauth_token_endpoint="https://idp/token",
            oauth_client_id="mcp",
            oauth_client_secret="s",
        )
        ctx = GravitinoContext(setting)
        client = ctx.rest_client()
        try:
            rest = client._catalog_operation.rest_client
            self.assertEqual(rest.headers.get("Authorization"), "Bearer frozen")
            self.assertIsNone(rest.auth)
        finally:
            asyncio.run(client.close())
