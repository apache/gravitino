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

from typing import Optional
from dataclasses import dataclass, field
from dataclasses_json import config

from gravitino.dto.responses.base_response import BaseResponse
from gravitino.auth.auth_constants import AuthConstants
from gravitino.exceptions.base import IllegalArgumentException


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

        if self._access_token is None:
            raise IllegalArgumentException("Invalid access token: None")

        if (
            AuthConstants.AUTHORIZATION_BEARER_HEADER.strip().lower()
            != self._token_type.lower()
        ):
            raise IllegalArgumentException(
                f'Unsupported token type: {self._token_type} (must be "bearer")'
            )

    def access_token(self) -> str:
        return self._access_token
