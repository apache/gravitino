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


from enum import Enum
from typing import Union


class OAuth2ClientAuthenticationMethod(Enum):
    """OAuth2 client authentication method for the client-credentials flow.

    Config values use the RFC 6749 style names ``client_secret_post`` and
    ``client_secret_basic``. Matching via ``from_config_value`` is case-insensitive.
    """

    CLIENT_SECRET_POST = "client_secret_post"
    CLIENT_SECRET_BASIC = "client_secret_basic"

    @classmethod
    def from_config_value(
        cls, value: Union[str, "OAuth2ClientAuthenticationMethod"]
    ) -> "OAuth2ClientAuthenticationMethod":
        """Parse an RFC-style OAuth2 client authentication method config value.

        Accepted values are ``client_secret_post`` and ``client_secret_basic``
        (case-insensitive).
        """
        if isinstance(value, cls):
            return value
        if value is None or not str(value).strip():
            raise ValueError("OAuth2 client authentication method must not be blank")
        normalized = str(value).strip().lower()
        for method in cls:
            if method.value == normalized:
                return method
        raise ValueError(f"Unsupported OAuth2 client authentication method: {value}")
