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

import asyncio
import logging
from collections import OrderedDict

from mcp_server.client.factory import RESTClientFactory
from mcp_server.core.setting import Setting

_LOG = logging.getLogger(__name__)

# Upper bound on the number of per-principal REST clients kept alive at once.
# Each client owns an httpx connection pool; caching by Authorization header lets
# repeated calls from the same principal reuse a pool instead of opening a new one
# per tool call, while the LRU bound keeps memory/sockets in check as principals
# (e.g. rotating tokens) come and go.
_MAX_CACHED_CLIENTS = 128


def _get_request_authorization() -> str:
    """Return the raw ``Authorization`` header of the current HTTP request.

    The header is forwarded to Gravitino verbatim so the auth scheme chosen by
    the agent (``Basic`` for simple or Basic auth, ``Bearer`` for OAuth2,
    ``Negotiate`` for Kerberos) is preserved. Returns an empty string in stdio
    mode or when the header is absent.
    """
    try:
        # Imported lazily: only available within an HTTP request context.
        # pylint: disable=import-outside-toplevel
        from fastmcp.server.dependencies import get_http_request

        return get_http_request().headers.get("authorization", "")
    except (LookupError, RuntimeError):
        # No active HTTP request: stdio mode (get_http_request raises
        # RuntimeError) or missing request context (LookupError).
        return ""


def startup_authorization(setting: Setting) -> str:
    """The static --token rendered as an ``Authorization`` header value.

    The CLI token is treated as an OAuth2 Bearer token. Empty string when no
    token is configured (anonymous). This is the identity used in stdio mode and
    the fallback for HTTP requests that carry no ``Authorization`` header.
    """
    return f"Bearer {setting.token}" if setting.token else ""


class GravitinoContext:
    def __init__(self, setting: Setting):
        self._setting = setting
        self._default_client = RESTClientFactory.create_rest_client(
            setting.metalake,
            setting.gravitino_uri,
            startup_authorization(setting),
        )
        # LRU cache of per-principal clients keyed by the raw Authorization header.
        # Safe without locking: rest_client() runs on the single asyncio event
        # loop and never awaits between lookup and insert.
        self._clients_by_auth: "OrderedDict[str, object]" = OrderedDict()
        # Strong references to in-flight background close tasks; the event loop
        # only keeps weak references, so without this they could be GC'd before
        # running. Entries are discarded when each task completes.
        self._pending_closes: "set[asyncio.Task]" = set()

    def rest_client(self):
        """Return a REST client carrying the correct identity for this request.

        In HTTP transport mode the incoming request's ``Authorization`` header is
        forwarded verbatim to Gravitino, taking priority over the static startup
        token. This keeps concurrent sessions with different principals fully
        isolated — one principal's identity never leaks into another's calls.

        Falls back to the shared default client (static startup token) when:
        - running in stdio mode (no HTTP request context), or
        - the incoming request carries no Authorization header.

        Per-principal clients are cached (and their connection pools reused) so a
        new pool is not opened on every tool call.
        """
        authorization = _get_request_authorization()
        if not authorization:
            return self._default_client

        cached = self._clients_by_auth.get(authorization)
        if cached is not None:
            self._clients_by_auth.move_to_end(authorization)
            return cached

        client = RESTClientFactory.create_rest_client(
            self._setting.metalake,
            self._setting.gravitino_uri,
            authorization,
        )
        self._clients_by_auth[authorization] = client
        if len(self._clients_by_auth) > _MAX_CACHED_CLIENTS:
            _, evicted = self._clients_by_auth.popitem(last=False)
            self._schedule_close(evicted)
        return client

    def _schedule_close(self, client) -> None:
        """Best-effort close of an evicted client's connection pool.

        Closing is async; schedule it on the running event loop if there is one
        (the normal HTTP-serving case). With no running loop (stdio mode/tests)
        there is nothing to schedule and the client is left for GC.
        """
        close = getattr(client, "close", None)
        if close is None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        task = loop.create_task(close())
        # Hold a strong reference until the task finishes (see _pending_closes).
        self._pending_closes.add(task)
        task.add_done_callback(self._on_close_done)

    def _on_close_done(self, task: "asyncio.Task") -> None:
        """Drop the finished close task and log any failure."""
        self._pending_closes.discard(task)
        exc = task.exception()
        if exc is not None:
            _LOG.warning("Failed to close evicted REST client: %s", exc)
