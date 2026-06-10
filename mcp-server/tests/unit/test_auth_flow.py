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

import unittest

from mcp_server.client.plain.plain_rest_client_operation import (
    PlainRESTClientOperation,
)
from mcp_server.core.context import GravitinoContext
from mcp_server.core.setting import Setting


class TestAuthorizationInjection(unittest.TestCase):
    """Verify the Authorization header is forwarded verbatim to the httpx client."""

    def test_bearer_authorization_header(self):
        """A Bearer authorization value is forwarded unchanged."""
        client = PlainRESTClientOperation(
            "my_metalake",
            "http://localhost:8090",
            authorization="Bearer my-secret-token",
        )
        headers = dict(client._catalog_operation.rest_client.headers)
        self.assertEqual(headers.get("authorization"), "Bearer my-secret-token")

    def test_basic_authorization_header(self):
        """A Basic authorization value (simple auth) is forwarded unchanged."""
        client = PlainRESTClientOperation(
            "my_metalake",
            "http://localhost:8090",
            authorization="Basic YWxpY2U6ZHVtbXk=",
        )
        headers = dict(client._catalog_operation.rest_client.headers)
        self.assertEqual(headers.get("authorization"), "Basic YWxpY2U6ZHVtbXk=")

    def test_empty_authorization_no_header(self):
        """When authorization is empty, no Authorization header is added."""
        client = PlainRESTClientOperation(
            "my_metalake", "http://localhost:8090", authorization=""
        )
        headers = dict(client._catalog_operation.rest_client.headers)
        self.assertNotIn("authorization", headers)

    def test_no_authorization_argument_no_header(self):
        """When authorization argument is omitted, no Authorization header is added."""
        client = PlainRESTClientOperation(
            "my_metalake", "http://localhost:8090"
        )
        headers = dict(client._catalog_operation.rest_client.headers)
        self.assertNotIn("authorization", headers)


class TestSettingTokenMasking(unittest.TestCase):
    """Verify that the token is not exposed in Setting string representation."""

    def test_token_masked_in_str(self):
        """Token value must not appear in Setting.__str__."""
        setting = Setting(metalake="ml", token="super-secret-token-value")
        self.assertNotIn("super-secret-token-value", str(setting))
        self.assertIn("***", str(setting))

    def test_empty_token_shows_empty_in_str(self):
        """When no token is set, __str__ shows empty placeholder."""
        setting = Setting(metalake="ml", token="")
        self.assertNotIn("***", str(setting))


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
        headers = dict(rest_client._catalog_operation.rest_client.headers)
        self.assertEqual(headers.get("authorization"), "Bearer ctx-token-xyz")

    def test_context_anonymous_when_no_token(self):
        """Empty token in Setting → no Authorization header in REST calls."""
        setting = Setting(
            metalake="ml",
            gravitino_uri="http://localhost:8090",
            token="",
        )
        ctx = GravitinoContext(setting)
        rest_client = ctx.rest_client()
        headers = dict(rest_client._catalog_operation.rest_client.headers)
        self.assertNotIn("authorization", headers)
