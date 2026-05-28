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

from gravitino.auth.auth_constants import AuthConstants
from gravitino.auth.auth_data_provider import AuthDataProvider
from gravitino.exceptions.base import IllegalArgumentException


class BasicAuthProvider(AuthDataProvider):
    """Provides HTTP Basic credentials for Gravitino built-in IdP authentication."""

    _token: bytes

    def __init__(self, username: str, password: str):
        if username is None or not username.strip():
            raise IllegalArgumentException("username can't be blank")
        if password is None or not password.strip():
            raise IllegalArgumentException("password can't be blank")

        self._token = self._build_basic_auth_token(username, password)

    def has_token_data(self) -> bool:
        return True

    def get_token_data(self) -> bytes:
        return self._token

    def close(self):
        pass

    @staticmethod
    def _build_basic_auth_token(username: str, password: str) -> bytes:
        credentials = f"{username}:{password}"
        encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode(
            "utf-8"
        )
        authorization_header = (
            AuthConstants.AUTHORIZATION_BASIC_HEADER + encoded_credentials
        )
        return authorization_header.encode("utf-8")
