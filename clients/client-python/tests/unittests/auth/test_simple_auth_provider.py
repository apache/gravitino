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

        original_gravitino_user = (
            os.environ["GRAVITINO_USER"] if "GRAVITINO_USER" in os.environ else ""
        )
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
        os.environ["GRAVITINO_USER"] = original_gravitino_user
