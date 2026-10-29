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

import time
import json
import base64
import math
from dataclasses import dataclass, field
from threading import Lock
from typing import Optional
from gravitino.auth.oauth2_token_provider import OAuth2TokenProvider
from gravitino.dto.responses.oauth2_token_response import OAuth2TokenResponse
from gravitino.dto.requests.oauth2_client_credential_request import (
    OAuth2ClientCredentialRequest,
)
from gravitino.exceptions.base import (
    GravitinoRuntimeException,
    IllegalArgumentException,
    RESTException,
    UnknownError,
)
from gravitino.exceptions.handlers.oauth_error_handler import OAUTH_ERROR_HANDLER

CLIENT_CREDENTIALS = "client_credentials"
CREDENTIAL_SPLITTER = ":"
TOKEN_SPLITTER = "."
JWT_EXPIRE = "exp"


@dataclass(frozen=True)
class _TokenState:
    token: str = field(repr=False)
    expires_at: float
    renew_at: float
    jwt_expires_at: Optional[float]
    margin: float

    def needs_renewal(self) -> bool:
        return time.monotonic() >= self.renew_at or (
            self.jwt_expires_at is not None
            and time.time() >= self.jwt_expires_at - self.margin
        )

    def is_expired(self) -> bool:
        return time.monotonic() >= self.expires_at or (
            self.jwt_expires_at is not None and time.time() >= self.jwt_expires_at
        )


class DefaultOAuth2TokenProvider(OAuth2TokenProvider):
    """This class is the default implement of OAuth2TokenProvider."""

    _credential: Optional[str]
    _scope: Optional[str]
    _path: Optional[str]
    _token: _TokenState

    def __init__(
        self,
        uri: str = None,
        credential: str = None,
        scope: str = None,
        path: str = None,
    ):
        super().__init__(uri)

        self._credential = credential
        self._scope = scope
        self._path = path
        self._lock = Lock()

        self.validate()

        self._token = self._fetch_token()

    def validate(self):
        if not self._credential or not self._credential.strip():
            raise IllegalArgumentException("OAuth2TokenProvider must set credential")

        if not self._scope or not self._scope.strip():
            raise IllegalArgumentException("OAuth2TokenProvider must set scope")

        if not self._path or not self._path.strip():
            raise IllegalArgumentException("OAuth2TokenProvider must set path")

    def _get_access_token(self) -> str:
        with self._lock:
            if self._token.needs_renewal():
                candidate = self._fetch_token()
                self._check_expiry(candidate)
                self._token = candidate
            self._check_expiry(self._token)
            return self._token.token

    def _parse_credential(self):
        if self._credential is None:
            raise ValueError("Invalid credential: None")

        credential_info = self._credential.split(CREDENTIAL_SPLITTER, maxsplit=1)
        client_id = None
        client_secret = None

        if len(credential_info) == 2:
            client_id, client_secret = credential_info
        elif len(credential_info) == 1:
            client_secret = credential_info[0]
        else:
            raise GravitinoRuntimeException("Invalid OAuth2 credential")

        return client_id, client_secret

    def _fetch_token(self) -> _TokenState:
        client_id, client_secret = self._parse_credential()

        client_credential_request = OAuth2ClientCredentialRequest(
            grant_type=CLIENT_CREDENTIALS,
            client_id=client_id,
            client_secret=client_secret,
            scope=self._scope,
        )

        failure: Optional[Exception] = None
        try:
            resp = self._client.post_form(
                self._path,
                data=client_credential_request,
                error_handler=OAUTH_ERROR_HANDLER,
            )
            received_at = time.monotonic()
            received_wall = time.time()
            payload = json.loads(resp.body)
            if not isinstance(payload, dict):
                raise GravitinoRuntimeException("Invalid OAuth2 token response")
            oauth2_resp = OAuth2TokenResponse.from_dict(payload, infer_missing=True)
            oauth2_resp.validate()
        except (
            IllegalArgumentException,
            GravitinoRuntimeException,
            RESTException,
            UnknownError,
        ) as error:
            failure = type(error)("OAuth2 token acquisition failed")
        # This boundary must sanitize even unexpected parsing/transport failures.
        except Exception:  # pylint: disable=broad-exception-caught
            failure = GravitinoRuntimeException("OAuth2 token acquisition failed")

        # Raise outside the handler so the original secret-bearing context is not retained.
        if failure is not None:
            raise failure
        return self._create_token(oauth2_resp, received_at, received_wall)

    @staticmethod
    def _create_token(
        response: OAuth2TokenResponse, received_at: float, received_wall: float
    ) -> _TokenState:
        deadlines = []
        expires_in = response.expires_in()
        if expires_in is not None:
            if expires_in <= 0:
                raise GravitinoRuntimeException("OAuth2 token has expired")
            relative_deadline = DefaultOAuth2TokenProvider._finite_number(expires_in)
            if relative_deadline is not None:
                relative_deadline += received_at
                if math.isfinite(relative_deadline):
                    deadlines.append(relative_deadline)

        jwt_expiry = DefaultOAuth2TokenProvider._jwt_expiry(response.access_token())
        if jwt_expiry is not None:
            if jwt_expiry <= received_wall:
                raise GravitinoRuntimeException("OAuth2 token has expired")
            jwt_deadline = received_at + (jwt_expiry - received_wall)
            if math.isfinite(jwt_deadline):
                deadlines.append(jwt_deadline)

        if not deadlines:
            raise GravitinoRuntimeException("OAuth2 token has no usable expiry")
        expires_at = min(deadlines)
        # Cap the lead time so short-lived tokens retain a useful reuse interval.
        margin = min(30.0, (expires_at - received_at) * 0.1)
        candidate = _TokenState(
            response.access_token(), expires_at, expires_at - margin, jwt_expiry, margin
        )
        DefaultOAuth2TokenProvider._check_expiry(candidate)
        return candidate

    @staticmethod
    def _jwt_expiry(token: str) -> Optional[float]:
        parts = token.split(TOKEN_SPLITTER)
        if len(parts) != 3:
            return None
        try:
            payload = json.loads(
                base64.b64decode(
                    parts[1] + "=" * (-len(parts[1]) % 4),
                    altchars=b"-_",
                    validate=True,
                )
            )
            if not isinstance(payload, dict):
                return None
            return DefaultOAuth2TokenProvider._finite_number(payload.get(JWT_EXPIRE))
        except (ValueError, TypeError, OverflowError):
            return None

    @staticmethod
    def _finite_number(value: object) -> Optional[float]:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        try:
            number = float(value)
        except OverflowError:
            return None
        return number if math.isfinite(number) else None

    @staticmethod
    def _check_expiry(token: _TokenState) -> None:
        if token.is_expired():
            raise GravitinoRuntimeException("OAuth2 token has expired")
