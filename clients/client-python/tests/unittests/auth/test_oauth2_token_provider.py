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
from unittest.mock import patch

from gravitino.auth.auth_constants import AuthConstants
from gravitino.auth.default_oauth2_token_provider import DefaultOAuth2TokenProvider
from gravitino.exceptions.base import (
    BadRequestException,
    IllegalArgumentException,
    UnauthorizedException,
)
from tests.unittests.auth import mock_base

OAUTH_PORT = 1082


class TestOAuth2TokenProvider(unittest.TestCase):
    def test_provider_init_exception(self):
        with self.assertRaises(IllegalArgumentException):
            _ = DefaultOAuth2TokenProvider(uri="test")

        with self.assertRaises(IllegalArgumentException):
            _ = DefaultOAuth2TokenProvider(uri="test", credential="xx")

        with self.assertRaises(IllegalArgumentException):
            _ = DefaultOAuth2TokenProvider(uri="test", credential="xx", scope="test")

    @patch(
        "gravitino.utils.http_client.HTTPClient._make_request",
        return_value=mock_base.mock_authentication_invalid_client_error(),
    )
    def test_authentication_invalid_client_error(self, *mock_methods):
        with self.assertRaises(UnauthorizedException):
            _ = DefaultOAuth2TokenProvider(
                uri=f"http://127.0.0.1:{OAUTH_PORT}",
                credential="yy:xx",
                path="oauth/token",
                scope="test",
            )

    @patch(
        "gravitino.utils.http_client.HTTPClient._make_request",
        return_value=mock_base.mock_authentication_invalid_grant_error(),
    )
    def test_authentication_invalid_grant_error(self, *mock_methods):
        with self.assertRaises(BadRequestException):
            _ = DefaultOAuth2TokenProvider(
                uri=f"http://127.0.0.1:{OAUTH_PORT}",
                credential="yy:xx",
                path="oauth/token",
                scope="test",
            )

    @patch(
        "gravitino.utils.http_client.HTTPClient.post_form",
        return_value=mock_base.mock_authentication_with_error_authentication_type(),
    )
    def test_authentication_with_error_authentication_type(self, *mock_methods):
        with self.assertRaises(IllegalArgumentException):
            _ = DefaultOAuth2TokenProvider(
                uri=f"http://127.0.0.1:{OAUTH_PORT}",
                credential="yy:xx",
                path="oauth/token",
                scope="test",
            )

    @patch(
        "gravitino.utils.http_client.HTTPClient.post_form",
        return_value=mock_base.mock_authentication_with_non_jwt(),
    )
    def test_authentication_with_non_jwt(self, *mock_methods):
        token_provider = DefaultOAuth2TokenProvider(
            uri=f"http://127.0.0.1:{OAUTH_PORT}",
            credential="yy:xx",
            path="oauth/token",
            scope="test",
        )

        self.assertTrue(token_provider.has_token_data())
        self.assertIsNone(token_provider.get_token_data())

    @patch(
        "gravitino.utils.http_client.HTTPClient.post_form",
        side_effect=mock_base.mock_authentication_with_jwt(),
    )
    def test_authentication_with_jwt(self, *mock_methods):
        old_access_token, new_access_token = mock_base.mock_old_new_jwt()

        token_provider = DefaultOAuth2TokenProvider(
            uri=f"http://127.0.0.1:{OAUTH_PORT}",
            credential="yy:xx",
            path="oauth/token",
            scope="test",
        )

        self.assertNotEqual(old_access_token, new_access_token)
        self.assertEqual(
            token_provider.get_token_data().decode("utf-8"),
            AuthConstants.AUTHORIZATION_BEARER_HEADER + new_access_token,
        )
