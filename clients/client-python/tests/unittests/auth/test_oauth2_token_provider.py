"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import unittest
from unittest.mock import patch

from gravitino.auth.auth_constants import AuthConstants
from gravitino.auth.default_oauth_to_token_provider import DefaultOAuth2TokenProvider
from tests.unittests.auth import mock_base

OAUTH_PORT = 1082


class TestOAuth2TokenProvider(unittest.TestCase):

    def test_provider_init_exception(self):

        with self.assertRaises(AssertionError):
            _ = DefaultOAuth2TokenProvider(uri="test")

        with self.assertRaises(AssertionError):
            _ = DefaultOAuth2TokenProvider(uri="test", credential="xx")

        with self.assertRaises(AssertionError):
            _ = DefaultOAuth2TokenProvider(uri="test", credential="xx", scope="test")

    # TODO
    # Error Test

    @patch(
        "gravitino.utils.http_client.HTTPClient.post_form",
        return_value=mock_base.mock_authentication_with_error_authentication_type(),
    )
    def test_authentication_with_error_authentication_type(self, *mock_methods):

        with self.assertRaises(AssertionError):
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
