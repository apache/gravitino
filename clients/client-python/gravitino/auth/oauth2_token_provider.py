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

from abc import abstractmethod
from typing import Optional

from gravitino.utils.http_client import HTTPClient
from gravitino.auth.auth_data_provider import AuthDataProvider
from gravitino.auth.auth_constants import AuthConstants


class OAuth2TokenProvider(AuthDataProvider):
    """OAuth2TokenProvider will request the access token from the authorization server and then provide
    the access token for every request.
    """

    # The HTTP client used to request the access token from the authorization server.
    _client: HTTPClient

    def __init__(self, uri: str):
        self._client = HTTPClient(uri)

    def has_token_data(self) -> bool:
        """Judge whether AuthDataProvider can provide token data.

        Returns:
            true if the AuthDataProvider can provide token data otherwise false.
        """
        return True

    def get_token_data(self) -> Optional[bytes]:
        """Acquire the data of token for authentication. The client will set the token data as HTTP header
        Authorization directly. So the return value should ensure token data contain the token header
        (eg: Bearer, Basic) if necessary.

        Returns:
            the token data is used for authentication.
        """
        access_token = self._get_access_token()

        if access_token is None:
            return None

        return (AuthConstants.AUTHORIZATION_BEARER_HEADER + access_token).encode(
            "utf-8"
        )

    def close(self):
        """Closes the OAuth2TokenProvider and releases any underlying resources."""
        if self._client is not None:
            self._client.close()

    @abstractmethod
    def _get_access_token(self) -> Optional[str]:
        """Get the access token from the authorization server."""

    @abstractmethod
    def validate(self):
        """Validate the OAuth2TokenProvider"""
