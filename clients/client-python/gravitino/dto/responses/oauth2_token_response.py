"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from typing import Optional
from dataclasses import dataclass, field
from dataclasses_json import config

from gravitino.dto.responses.base_response import BaseResponse
from gravitino.auth.auth_constants import AuthConstants


@dataclass
class OAuth2TokenResponse(BaseResponse):

    _access_token: str = field(metadata=config(field_name="access_token"))
    _issue_token_type: Optional[str] = field(
        metadata=config(field_name="issued_token_type")
    )
    _token_type: str = field(metadata=config(field_name="token_type"))
    _expires_in: int = field(metadata=config(field_name="expires_in"))
    _scope: str = field(metadata=config(field_name="scope"))
    _refresh_token: Optional[str] = field(metadata=config(field_name="refresh_token"))

    def validate(self):
        """Validates the response.

        Raise:
            IllegalArgumentException If the response is invalid, this exception is thrown.
        """
        super().validate()

        assert self._access_token is not None, "Invalid access token: None"
        assert (
            AuthConstants.AUTHORIZATION_BEARER_HEADER.strip().lower()
            == self._token_type.lower()
        ), f'Unsupported token type: {self._token_type} (must be "bearer")'

    def access_token(self) -> str:
        return self._access_token
