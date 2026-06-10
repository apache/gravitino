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


def _get_request_authorization() -> str:
    """Return the raw ``Authorization`` header of the current HTTP request.

    The header is forwarded to Gravitino verbatim so the auth scheme chosen by
    the agent (``Basic`` for simple auth, ``Bearer`` for OAuth2, ``Negotiate``
    for Kerberos) is preserved. Returns an empty string in stdio mode or when
    the header is absent.
    """
    try:
        from fastmcp.server.dependencies import get_http_request

        return get_http_request().headers.get("authorization", "")
    except Exception:  # noqa: BLE001 – stdio mode or missing request context
        return ""


class GravitinoContext:
    def __init__(self, setting: Setting):
        self._setting = setting
        # Static startup identity: the --token CLI value is treated as a Bearer
        # token (OAuth2). Used in stdio mode or as the fallback when an HTTP
        # request carries no Authorization header.
        default_authorization = (
            f"Bearer {setting.token}" if setting.token else ""
        )
        self._default_client = RESTClientFactory.create_rest_client(
            setting.metalake, setting.gravitino_uri, default_authorization
        )

    def rest_client(self):
        """Return a REST client carrying the correct identity for this request.

        In HTTP transport mode the incoming request's ``Authorization`` header is
        forwarded verbatim to Gravitino, taking priority over the static startup
        token. This keeps concurrent sessions with different principals fully
        isolated — one principal's identity never leaks into another's calls.

        Falls back to the shared default client (static startup token) when:
        - running in stdio mode (no HTTP request context), or
        - the incoming request carries no Authorization header.
        """
        authorization = _get_request_authorization()
        if authorization:
            return RESTClientFactory.create_rest_client(
                self._setting.metalake,
                self._setting.gravitino_uri,
                authorization,
            )
        return self._default_client
