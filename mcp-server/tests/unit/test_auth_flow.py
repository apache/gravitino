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

import asyncio
import sys
import unittest
from unittest import mock

from mcp_server.client.plain.plain_rest_client_operation import (
    PlainRESTClientOperation,
)
from mcp_server.core.context import GravitinoContext
from mcp_server.core.setting import Setting
from mcp_server.main import _parse_args


def _shared_rest_client(operation: PlainRESTClientOperation):
    # pylint: disable=protected-access
    return operation._catalog_operation.rest_client


def _headers_of(operation: PlainRESTClientOperation):
    return _shared_rest_client(operation).headers


def _close(operation: PlainRESTClientOperation):
    asyncio.run(_shared_rest_client(operation).aclose())


class TestTokenInjection(unittest.TestCase):
    """Verify Bearer token is correctly injected into the httpx client."""

    def test_token_sets_authorization_header(self):
        """When a token is provided, the httpx client carries Authorization: Bearer."""
        client = PlainRESTClientOperation(
            "my_metalake", "http://localhost:8090", token="my-secret-token"
        )
        try:
            self.assertEqual(
                _headers_of(client).get("Authorization"),
                "Bearer my-secret-token",
            )
        finally:
            _close(client)

    def test_empty_token_no_authorization_header(self):
        """When token is empty, no Authorization header is added."""
        client = PlainRESTClientOperation(
            "my_metalake", "http://localhost:8090", token=""
        )
        try:
            self.assertIsNone(_headers_of(client).get("Authorization"))
        finally:
            _close(client)

    def test_no_token_argument_no_authorization_header(self):
        """When token argument is omitted entirely, no Authorization header is added."""
        client = PlainRESTClientOperation(
            "my_metalake", "http://localhost:8090"
        )
        try:
            self.assertIsNone(_headers_of(client).get("Authorization"))
        finally:
            _close(client)


class TestSettingTokenMasking(unittest.TestCase):
    """Verify that the token is not exposed in Setting string representations."""

    def test_token_masked_in_str(self):
        """Token value must not appear in Setting.__str__."""
        setting = Setting(metalake="ml", token="super-secret-token-value")
        self.assertNotIn("super-secret-token-value", str(setting))
        self.assertIn("***", str(setting))

    def test_token_not_in_repr(self):
        """Token value must not appear in Setting.__repr__ either."""
        setting = Setting(metalake="ml", token="super-secret-token-value")
        self.assertNotIn("super-secret-token-value", repr(setting))

    def test_empty_token_shows_empty_in_str(self):
        """When no token is set, __str__ shows empty placeholder."""
        setting = Setting(metalake="ml", token="")
        self.assertNotIn("***", str(setting))


class TestTokenArgParsing(unittest.TestCase):
    """Verify --token CLI argument and GRAVITINO_TOKEN env var precedence."""

    def test_env_var_used_when_token_omitted(self):
        """GRAVITINO_TOKEN is used when --token is not passed."""
        with mock.patch.dict(
            "os.environ", {"GRAVITINO_TOKEN": "env-token"}
        ), mock.patch.object(sys, "argv", ["prog", "--metalake", "ml"]):
            args = _parse_args()
        self.assertEqual(args.token, "env-token")

    def test_cli_token_overrides_env_var(self):
        """--token takes precedence over GRAVITINO_TOKEN when both are set."""
        with mock.patch.dict(
            "os.environ", {"GRAVITINO_TOKEN": "env-token"}
        ), mock.patch.object(
            sys, "argv", ["prog", "--metalake", "ml", "--token", "cli-token"]
        ):
            args = _parse_args()
        self.assertEqual(args.token, "cli-token")

    def test_no_token_anywhere_defaults_to_empty(self):
        """Without --token and GRAVITINO_TOKEN, token defaults to empty string."""
        with mock.patch.dict("os.environ", {}, clear=True), mock.patch.object(
            sys, "argv", ["prog", "--metalake", "ml"]
        ):
            args = _parse_args()
        self.assertEqual(args.token, "")


class TestGravitinoContextTokenPropagation(unittest.TestCase):
    """Verify GravitinoContext passes token from Setting to the REST client."""

    def test_context_propagates_token(self):
        """Token from Setting reaches the httpx client Authorization header."""
        setting = Setting(
            metalake="ml",
            gravitino_uri="http://localhost:8090",
            token="ctx-token-xyz",
        )
        ctx = GravitinoContext(setting)
        rest_client = ctx.rest_client()
        try:
            self.assertEqual(
                _headers_of(rest_client).get("Authorization"),
                "Bearer ctx-token-xyz",
            )
        finally:
            _close(rest_client)

    def test_context_anonymous_when_no_token(self):
        """Empty token in Setting → no Authorization header in REST calls."""
        setting = Setting(
            metalake="ml",
            gravitino_uri="http://localhost:8090",
            token="",
        )
        ctx = GravitinoContext(setting)
        rest_client = ctx.rest_client()
        try:
            self.assertIsNone(_headers_of(rest_client).get("Authorization"))
        finally:
            _close(rest_client)
