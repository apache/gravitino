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

from mcp_server.client.factory import RESTClientFactory
from mcp_server.core.setting import Setting


def _extract_bearer_token(authorization: str) -> str:
    """Parse a Bearer token from a raw Authorization header value."""
    parts = authorization.split()
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1]
    return ""


def _get_request_token() -> str:
    """Extract the Bearer token from the current HTTP request, if any.

    Returns an empty string in stdio mode or when the header is absent.
    """
    try:
        from fastmcp.server.dependencies import get_http_request

        authorization = get_http_request().headers.get("authorization", "")
        return _extract_bearer_token(authorization)
    except Exception:  # noqa: BLE001 – stdio mode or missing request context
        return ""


class GravitinoContext:
    def __init__(self, setting: Setting):
        self._setting = setting
        # Fallback client for stdio mode or when no per-request token is present.
        self._default_client = RESTClientFactory.create_rest_client(
            setting.metalake, setting.gravitino_uri, setting.token
        )

    def rest_client(self):
        """Return a REST client carrying the correct identity for this request.

        In HTTP transport mode the Bearer token from the incoming MCP request's
        Authorization header takes priority over the static startup token.
        This ensures concurrent sessions with different principals are fully
        isolated — one principal's token never leaks into another's Gravitino calls.

        Falls back to the shared default client (static startup token) when:
        - running in stdio mode (no HTTP request context), or
        - the incoming request carries no Authorization header.
        """
        request_token = _get_request_token()
        if request_token:
            return RESTClientFactory.create_rest_client(
                self._setting.metalake,
                self._setting.gravitino_uri,
                request_token,
            )
        return self._default_client
