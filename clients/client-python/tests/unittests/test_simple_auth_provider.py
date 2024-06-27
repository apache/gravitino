"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import base64
import os
import unittest

from gravitino.auth.auth_constants import AuthConstants
from gravitino.auth.auth_data_provider import AuthDataProvider
from gravitino.auth.simple_auth_provider import SimpleAuthProvider


class TestSimpleAuthProvider(unittest.TestCase):

    def test_auth_provider(self):
        os.environ["user.name"] = "test_auth1"
        provider: AuthDataProvider = SimpleAuthProvider()
        self.assertTrue(provider.has_token_data())
        user: str = os.environ["user.name"]
        token = provider.get_token_data().decode("utf-8")
        self.assertTrue(token.startswith(AuthConstants.AUTHORIZATION_BASIC_HEADER))
        token_string = base64.b64decode(
            token[len(AuthConstants.AUTHORIZATION_BASIC_HEADER) :]
        ).decode("utf-8")
        self.assertEqual(f"{user}:dummy", token_string)

        os.environ["GRAVITINO_USER"] = "test_auth2"
        provider: AuthDataProvider = SimpleAuthProvider()
        self.assertTrue(provider.has_token_data())
        user: str = os.environ["GRAVITINO_USER"]
        token = provider.get_token_data().decode("utf-8")
        self.assertTrue(token.startswith(AuthConstants.AUTHORIZATION_BASIC_HEADER))
        token_string = base64.b64decode(
            token[len(AuthConstants.AUTHORIZATION_BASIC_HEADER) :]
        ).decode("utf-8")
        self.assertEqual(f"{user}:dummy", token_string)
