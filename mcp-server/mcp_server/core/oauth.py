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

"""httpx ``auth=`` hook for MCP → Gravitino OAuth2 client-credentials.

Uses ``httpx-auth`` for fetch and cache on the existing ``httpx.AsyncClient``.
Credentials go in the form body (``client_secret_post``), matching the
Java/Python Gravitino clients. httpx-auth defaults to HTTP Basic. This class
retries once after Gravitino HTTP 401.
"""

import asyncio
import base64
import json
import logging
import threading
import time
from collections.abc import AsyncGenerator, Generator
from typing import Optional, Union

import httpx
from httpx_auth import AuthenticationFailed, OAuth2, OAuth2ClientCredentials

_LOG = logging.getLogger(__name__)

# Refresh this many seconds before recorded expiry. httpx-auth default is 30.
DEFAULT_REFRESH_SKEW_SECONDS = 60

_TokenTuple = Union[tuple[str, str], tuple[str, str, Union[int, str]]]


class OAuthTokenEndpointError(httpx.HTTPStatusError):
    """The service identity's OAuth token endpoint returned an HTTP error."""


class OAuthTokenResponseError(ValueError):
    """The identity provider returned an unusable token response."""


class RefreshableBearerAuth(OAuth2ClientCredentials):
    """httpx-auth client-credentials with form POST and one 401 retry."""

    requires_request_body = True
    requires_response_body = True

    def __init__(
        self,
        *,
        token_endpoint: str,
        client_id: str,
        client_secret: str,
        scope: str = "",
        refresh_skew_seconds: int = DEFAULT_REFRESH_SKEW_SECONDS,
        client: Optional[httpx.Client] = None,
    ):
        """Build an ``auth=`` hook for the service hop.

        Args:
            token_endpoint: Identity-provider token URL.
            client_id: OAuth2 client id.
            client_secret: OAuth2 client secret.
            scope: Optional OAuth2 scope.
            refresh_skew_seconds: httpx-auth ``early_expiry``.
            client: Optional sync httpx client used only for token POSTs
                (tests inject ``MockTransport`` here).
        """
        kwargs = {"early_expiry": float(refresh_skew_seconds)}
        if scope:
            kwargs["scope"] = scope
        if client is not None:
            kwargs["client"] = client
        super().__init__(token_endpoint, client_id, client_secret, **kwargs)
        self._token_lock = asyncio.Lock()
        self._sync_lock = threading.Lock()
        self._rejected_tokens: set[str] = set()
        self._retried_tokens: set[str] = set()

    def invalidate(self) -> None:
        """Drop the cached token so the next call fetches a new one."""
        cache = OAuth2.token_cache
        # TokenMemoryCache.clear() wipes every client; only drop ours.
        with cache._forbid_concurrent_cache_access:  # pylint: disable=protected-access
            cache.tokens.pop(self.state, None)

    def request_new_token(self) -> _TokenTuple:
        """POST ``client_credentials`` with id/secret in the form body."""
        data = self._token_form_data()
        client = self.client or httpx.Client()
        self._configure_client(client)
        try:
            response = client.post(self.token_url, data=data)
            return self._token_tuple(response)
        finally:
            if self.client is None:
                client.close()

    async def request_new_token_async(self) -> _TokenTuple:
        """POST ``client_credentials`` without blocking the event loop."""
        if self.client is not None:
            return await asyncio.to_thread(self.request_new_token)
        data = self._token_form_data()
        async with httpx.AsyncClient() as client:
            client.timeout = self.timeout
            response = await client.post(self.token_url, data=data)
            return self._token_tuple(response)

    def auth_flow(
        self, request: httpx.Request
    ) -> Generator[httpx.Request, httpx.Response, None]:
        """Attach a cached or freshly fetched Bearer; retry once on HTTP 401."""
        if self.requires_request_body:
            request.read()
        token, fetched = self._apply_token(request)
        response = yield request
        if response.status_code != 401:
            return
        with self._sync_lock:
            if not self._begin_401_retry(token, fetched):
                return
        self._invalidate_if_still_cached(token)
        retry_token, _ = self._apply_token(request)
        response = yield request
        if response.status_code == 401:
            self._rejected_tokens = {retry_token}

    async def async_auth_flow(
        self, request: httpx.Request
    ) -> AsyncGenerator[httpx.Request, httpx.Response]:
        """Attach a Bearer without a blocking IdP POST on the event loop."""
        if self.requires_request_body:
            await request.aread()
        token, fetched = await self._apply_token_async(request)
        response = yield request
        if response.status_code != 401:
            return
        async with self._token_lock:
            if not self._begin_401_retry(token, fetched):
                return
        self._invalidate_if_still_cached(token)
        retry_token, _ = await self._apply_token_async(request)
        response = yield request
        if response.status_code == 401:
            self._rejected_tokens = {retry_token}

    def _configure_client(self, client: httpx.Client) -> None:
        """Do not send HTTP Basic; id and secret go in the form body."""
        client.timeout = self.timeout

    def _token_form_data(self) -> dict:
        data = dict(self.data)
        data["client_id"] = self.client_id
        data["client_secret"] = self.client_secret
        return data

    def _token_tuple(self, response: httpx.Response) -> _TokenTuple:
        if not response.is_success:
            message = (
                f"OAuth token endpoint returned HTTP {response.status_code}"
            )
            _LOG.error("%s", message)
            raise OAuthTokenEndpointError(
                message, request=response.request, response=response
            )
        try:
            body = response.json()
        except ValueError as exc:
            raise OAuthTokenResponseError(
                "Invalid JSON in OAuth token response"
            ) from exc
        if not isinstance(body, dict):
            raise OAuthTokenResponseError(
                "OAuth token response must be a JSON object"
            )
        token = body.get(self.token_field_name)
        if not token or not isinstance(token, str):
            raise OAuthTokenResponseError(
                "OAuth token response missing access_token"
            )
        expires_in = body.get("expires_in")
        _LOG.info("Fetched OAuth access token")
        if expires_in:
            try:
                return self.state, token, int(expires_in)
            except (TypeError, ValueError, OverflowError) as exc:
                raise OAuthTokenResponseError(
                    "Invalid expires_in in OAuth token response"
                ) from exc
        jwt_expires_in = self._jwt_expires_in(token)
        if jwt_expires_in is None:
            raise OAuthTokenResponseError(
                "OAuth token response omitted expires_in and "
                "access_token is not a JWT with exp"
            )
        return self.state, token, jwt_expires_in

    @staticmethod
    def _jwt_expires_in(token: str) -> Optional[int]:
        """Return seconds until JWT ``exp``, or None when not cacheable."""
        parts = token.split(".")
        if len(parts) != 3:
            return None
        try:
            padded = parts[1] + "=" * (-len(parts[1]) % 4)
            payload = json.loads(base64.urlsafe_b64decode(padded))
            exp = payload.get("exp") if isinstance(payload, dict) else None
            if not RefreshableBearerAuth._is_numeric_date(exp):
                return None
            expires_at = int(float(exp))
            return max(expires_at - int(time.time()), 1)
        except (ValueError, OverflowError, TypeError):
            return None

    @staticmethod
    def _is_numeric_date(value) -> bool:
        """Return True when value is a JWT NumericDate (RFC 7519)."""
        if isinstance(value, bool):
            return False
        if isinstance(value, (int, float)):
            return True
        if isinstance(value, str):
            try:
                float(value)
                return True
            except ValueError:
                return False
        return False

    def _cached_bearer(self) -> Optional[str]:
        try:
            return OAuth2.token_cache.get_token(
                self.state, early_expiry=self.early_expiry
            )
        except AuthenticationFailed:
            return None

    def _store_and_get(self, fetched: _TokenTuple) -> str:
        return OAuth2.token_cache.get_token(
            self.state,
            early_expiry=self.early_expiry,
            on_missing_token=lambda: fetched,
        )

    def _invalidate_if_still_cached(self, token: str) -> None:
        """Drop the cache entry only when it still holds the rejected token."""
        if self._cached_bearer() == token:
            self.invalidate()

    def _begin_401_retry(self, token: str, fetched: bool) -> bool:
        """Reserve a single 401 retry for this token; skip hopeless cases.

        Sets hold only the current token. Historical JWTs are not retained.
        """
        if fetched:
            self._rejected_tokens = {token}
            return False
        if token in self._rejected_tokens or token in self._retried_tokens:
            return False
        self._retried_tokens = {token}
        return True

    def _apply_token(self, request: httpx.Request) -> tuple[str, bool]:
        fetched = False
        token = self._cached_bearer()
        if token is None:
            token = OAuth2.token_cache.get_token(
                self.state,
                early_expiry=self.early_expiry,
                on_missing_token=self.request_new_token,
                on_expired_token=self.refresh_token,
            )
            fetched = True
        self._update_user_request(request, token)
        return token, fetched

    async def _apply_token_async(
        self, request: httpx.Request
    ) -> tuple[str, bool]:
        fetched = False
        token = self._cached_bearer()
        if token is None:
            async with self._token_lock:
                token = self._cached_bearer()
                if token is None:
                    token = self._store_and_get(
                        await self.request_new_token_async()
                    )
                    fetched = True
        self._update_user_request(request, token)
        return token, fetched
