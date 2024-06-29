"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from abc import abstractmethod
from typing import Optional

from gravitino.utils.http_client import HTTPClient
from gravitino.auth.auth_data_provider import AuthDataProvider
from gravitino.auth.auth_constants import AuthConstants


class OAuth2TokenProvider(AuthDataProvider):

    _client: HTTPClient

    def __init__(self, uri: str):
        self._client = HTTPClient(uri)

    def has_token_data(self) -> bool:
        return True

    def get_token_data(self) -> Optional[bytes]:
        access_token = self._get_access_token()

        if access_token is None:
            return None

        return (AuthConstants.AUTHORIZATION_BEARER_HEADER + access_token).encode(
            "utf-8"
        )

    def close(self):
        if self._client is not None:
            self._client.close()

    @abstractmethod
    def _get_access_token(self) -> Optional[str]:
        """Get the access token from the authorization server."""

    @abstractmethod
    def validate(self):
        """Validate the OAuth2TokenProvider"""
