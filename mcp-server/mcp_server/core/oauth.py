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

import logging
from collections.abc import Generator
from typing import Optional, Union

import httpx
from httpx_auth import OAuth2, OAuth2ClientCredentials

_LOG = logging.getLogger(__name__)

# Refresh this many seconds before recorded expiry. httpx-auth default is 30.
DEFAULT_REFRESH_SKEW_SECONDS = 60


class RefreshableBearerAuth(OAuth2ClientCredentials):
    """httpx-auth client-credentials with form POST and one 401 retry."""

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

    def invalidate(self) -> None:
        """Drop the cached token so the next call fetches a new one."""
        cache = OAuth2.token_cache
        # TokenMemoryCache.clear() wipes every client; only drop ours.
        with cache._forbid_concurrent_cache_access:  # pylint: disable=protected-access
            cache.tokens.pop(self.state, None)

    def _configure_client(self, client: httpx.Client) -> None:
        """Do not send HTTP Basic; id and secret go in the form body."""
        client.timeout = self.timeout

    def request_new_token(
        self,
    ) -> Union[tuple[str, str], tuple[str, str, Union[int, str]]]:
        """POST ``client_credentials`` with id/secret in the form body."""
        data = dict(self.data)
        data["client_id"] = self.client_id
        data["client_secret"] = self.client_secret
        client = self.client or httpx.Client()
        self._configure_client(client)
        try:
            response = client.post(self.token_url, data=data)
            if response.status_code >= 400:
                _LOG.error(
                    "OAuth token request failed: HTTP %s", response.status_code
                )
            response.raise_for_status()
            body = response.json()
        finally:
            if self.client is None:
                client.close()
        token = body.get(self.token_field_name)
        if not token or not isinstance(token, str):
            raise ValueError("OAuth token response missing access_token")
        expires_in = body.get("expires_in")
        _LOG.info("Fetched OAuth access token")
        if expires_in in (None, ""):
            return self.state, token
        return self.state, token, expires_in

    def auth_flow(
        self, request: httpx.Request
    ) -> Generator[httpx.Request, httpx.Response, None]:
        """Attach a cached or freshly fetched Bearer; retry once on HTTP 401."""
        self._apply_token(request)
        response = yield request
        if response.status_code != 401:
            return
        self.invalidate()
        self._apply_token(request)
        yield request

    def _apply_token(self, request: httpx.Request) -> None:
        token = OAuth2.token_cache.get_token(
            self.state,
            early_expiry=self.early_expiry,
            on_missing_token=self.request_new_token,
            on_expired_token=self.refresh_token,
        )
        self._update_user_request(request, token)
