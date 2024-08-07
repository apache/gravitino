"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

import base64
import os

from gravitino.auth.auth_constants import AuthConstants
from gravitino.auth.auth_data_provider import AuthDataProvider


class SimpleAuthProvider(AuthDataProvider):
    """SimpleAuthProvider will use the environment variable `GRAVITINO_USER` or
    the user of the system to generate a basic token for every request.

    """

    _token: bytes

    def __init__(self):
        gravitino_user = os.environ.get("GRAVITINO_USER")
        if gravitino_user is None or len(gravitino_user) == 0:
            gravitino_user = os.environ.get("user.name")

        if gravitino_user is None or len(gravitino_user) == 0:
            gravitino_user = "anonymous"

        user_information = f"{gravitino_user}:dummy"
        self._token = (
            AuthConstants.AUTHORIZATION_BASIC_HEADER
            + base64.b64encode(user_information.encode("utf-8")).decode("utf-8")
        ).encode("utf-8")

    def has_token_data(self) -> bool:
        return True

    def get_token_data(self) -> bytes:
        return self._token

    def close(self):
        pass
