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

import base64
import unittest

from gravitino.auth.auth_constants import AuthConstants
from gravitino.auth.auth_data_provider import AuthDataProvider
from gravitino.auth.basic_auth_provider import BasicAuthProvider
from gravitino.exceptions.base import IllegalArgumentException


class TestBasicAuthProvider(unittest.TestCase):
    def test_auth_provider(self):
        provider: AuthDataProvider = BasicAuthProvider(
            "admin", "YourSecureGravitinoPassword"
        )
        self.assertTrue(provider.has_token_data())
        token = provider.get_token_data().decode("utf-8")
        self.assertTrue(token.startswith(AuthConstants.AUTHORIZATION_BASIC_HEADER))
        token_string = base64.b64decode(
            token[len(AuthConstants.AUTHORIZATION_BASIC_HEADER) :]
        ).decode("utf-8")
        self.assertEqual("admin:YourSecureGravitinoPassword", token_string)
        provider.close()

    def test_blank_username(self):
        with self.assertRaises(IllegalArgumentException):
            BasicAuthProvider(" ", "password")

    def test_blank_password(self):
        with self.assertRaises(IllegalArgumentException):
            BasicAuthProvider("admin", " ")
