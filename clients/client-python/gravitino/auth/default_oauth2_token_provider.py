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

import time
import json
import base64
from typing import Optional
from gravitino.auth.oauth2_token_provider import OAuth2TokenProvider
from gravitino.dto.responses.oauth2_token_response import OAuth2TokenResponse
from gravitino.dto.requests.oauth2_client_credential_request import (
    OAuth2ClientCredentialRequest,
)
from gravitino.exceptions.base import GravitinoRuntimeException

CLIENT_CREDENTIALS = "client_credentials"
CREDENTIAL_SPLITTER = ":"
TOKEN_SPLITTER = "."
JWT_EXPIRE = "exp"


class DefaultOAuth2TokenProvider(OAuth2TokenProvider):
    """This class is the default implement of OAuth2TokenProvider."""

    _credential: Optional[str]
    _scope: Optional[str]
    _path: Optional[str]
    _token: Optional[str]

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

        self.validate()

        self._token = self._fetch_token()

    def validate(self):
        assert (
            self._credential and self._credential.strip()
        ), "OAuth2TokenProvider must set credential"
        assert self._scope and self._scope.strip(), "OAuth2TokenProvider must set scope"
        assert self._path and self._path.strip(), "OAuth2TokenProvider must set path"

    def _get_access_token(self) -> Optional[str]:

        expires = self._expires_at_millis()

        if expires is None:
            return None

        if expires > time.time() * 1000:
            return self._token

        self._token = self._fetch_token()
        return self._token

    def _parse_credential(self):
        assert self._credential is not None, "Invalid credential: None"

        credential_info = self._credential.split(CREDENTIAL_SPLITTER, maxsplit=1)
        client_id = None
        client_secret = None

        if len(credential_info) == 2:
            client_id, client_secret = credential_info
        elif len(credential_info) == 1:
            client_secret = credential_info[0]
        else:
            raise GravitinoRuntimeException(f"Invalid credential: {self._credential}")

        return client_id, client_secret

    def _fetch_token(self) -> str:

        client_id, client_secret = self._parse_credential()

        client_credential_request = OAuth2ClientCredentialRequest(
            grant_type=CLIENT_CREDENTIALS,
            client_id=client_id,
            client_secret=client_secret,
            scope=self._scope,
        )

        resp = self._client.post_form(
            self._path, data=client_credential_request.to_dict()
        )
        oauth2_resp = OAuth2TokenResponse.from_json(resp.body, infer_missing=True)
        oauth2_resp.validate()

        return oauth2_resp.access_token()

    def _expires_at_millis(self) -> int:
        if self._token is None:
            return None

        parts = self._token.split(TOKEN_SPLITTER)

        if len(parts) != 3:
            return None

        jwt = json.loads(
            base64.b64decode(parts[1] + "=" * (-len(parts[1]) % 4)).decode("utf-8")
        )

        if JWT_EXPIRE not in jwt or not isinstance(jwt[JWT_EXPIRE], int):
            return None

        return jwt[JWT_EXPIRE] * 1000
