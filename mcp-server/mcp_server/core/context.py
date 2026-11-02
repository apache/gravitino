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
import re
from collections import OrderedDict

from mcp_server.client.factory import RESTClientFactory
from mcp_server.core.oauth import RefreshableBearerAuth
from mcp_server.core.setting import Setting

_LOG = logging.getLogger(__name__)

# Upper bound on the number of per-principal REST clients kept alive at once.
# Each client owns an httpx connection pool; caching by Authorization header lets
# repeated calls from the same principal reuse a pool instead of opening a new one
# per tool call, while the LRU bound keeps memory/sockets in check as principals
# (e.g. rotating tokens) come and go.
_MAX_CACHED_CLIENTS = 128

# An RFC 9110 auth-scheme uses the HTTP token syntax. Here it must be followed by
# one or more spaces plus credentials. Requiring credentials preserves the legacy
# behavior for a bare token whose value happens to be a scheme name (for example,
# Bearer).
_AUTHORIZATION_CREDENTIAL = re.compile(
    r"^(?P<scheme>[!#$%&'*+\-.^_`|~0-9A-Za-z]+) +(?P<credential>\S.*)$"
)

# Gravitino currently matches its built-in schemes case-sensitively. HTTP scheme
# names are case-insensitive, so normalize them before forwarding. Custom scheme
# names remain unchanged for custom Gravitino authenticators.
_CANONICAL_AUTH_SCHEMES = {
    "basic": "Basic",
    "bearer": "Bearer",
    "negotiate": "Negotiate",
}


class ServiceIdentityFallbackDisabled(RuntimeError):
    """HTTP omitted Authorization while service-identity fallback is disabled."""


def _in_http_request() -> bool:
    """Return True when ``rest_client()`` runs inside an active HTTP request."""
    try:
        # pylint: disable=import-outside-toplevel
        from fastmcp.server.dependencies import get_http_request

        get_http_request()
        return True
    except (LookupError, RuntimeError):
        return False


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

    A value containing a valid HTTP authentication scheme and credentials is
    forwarded as an Authorization credential. Built-in Gravitino scheme names
    are normalized to the capitalization its authenticators expect, while a
    custom scheme name is preserved. A bare token is treated as OAuth2 and
    prefixed with ``Bearer``. Empty string when no token is configured
    (anonymous). This is the identity used in stdio mode and the fallback for
    HTTP requests that carry no ``Authorization`` header.
    """
    token = setting.token.strip()
    if not token:
        return ""
    match = _AUTHORIZATION_CREDENTIAL.fullmatch(token)
    if match:
        scheme = match.group("scheme")
        credential = match.group("credential")
        canonical_scheme = _CANONICAL_AUTH_SCHEMES.get(scheme.lower(), scheme)
        return f"{canonical_scheme} {credential}"
    return f"Bearer {token}"


def service_fallback_authorization(setting: Setting) -> str:
    """Audit / fallback identity when no hop-1 Authorization header is present.

    Prefers the static ``--token``. When only OAuth client-credentials is
    configured, returns ``OAuth <client_id>`` so audit logs can attribute
    stdio / no-header calls to the service client.
    """
    static = startup_authorization(setting)
    if static:
        return static
    if setting.has_oauth_client():
        return f"OAuth {setting.oauth_client_id.strip()}"
    return ""


def _service_auth(setting: Setting):
    """httpx ``auth=`` hook for service OAuth, or None for static/anonymous."""
    setting.validate_oauth()
    static = startup_authorization(setting)
    if static:
        if setting.has_oauth_client():
            _LOG.warning(
                "Ignoring OAuth client credentials because --token is set"
            )
        return None
    if not setting.has_oauth_client():
        return None
    return RefreshableBearerAuth(
        token_endpoint=setting.oauth_token_endpoint.strip(),
        client_id=setting.oauth_client_id.strip(),
        client_secret=setting.oauth_client_secret.strip(),
        scope=setting.oauth_scope.strip(),
    )


class GravitinoContext:
    def __init__(self, setting: Setting):
        self._setting = setting
<<<<<<< HEAD
        self._default_client = RESTClientFactory.create_rest_client(
            setting.metalake,
            setting.gravitino_uri,
            startup_authorization(setting),
            auth=_service_auth(setting),
=======
        # Built once and shared by every service-identity client. Separate
        # instances share the token cache key but each owns its own refresh
        # lock and 401 retry state, so per-metalake instances would bypass
        # refresh coalescing and hit the IdP once per metalake.
        self._service_auth = _service_auth(setting)
        # Eagerly built only when a startup default is configured, so the
        # common single-metalake deployment pays no extra cost. Left unset
        # (None) when metalake resolution must come from a per-request header
        # on every call (HTTP transport with no --metalake default).
        self._default_client = (
            RESTClientFactory.create_rest_client(
                setting.metalake,
                setting.gravitino_uri,
                startup_authorization(setting),
                auth=self._service_auth,
                read_timeout=setting.gravitino_read_timeout,
            )
            if setting.metalake
            else None
>>>>>>> 696efe1c2 ([#14498] fix(mcp): configure Gravitino response read timeout (#14506))
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

        Falls back to the shared default client (static token or OAuth
        client-credentials) when:
        - running in stdio mode (no HTTP request context), or
        - the incoming request carries no Authorization header.

        Per-principal clients are cached (and their connection pools reused) so a
        new pool is not opened on every tool call.
        """
        authorization = _get_request_authorization()
        if not authorization:
            if (
                self._setting.no_service_identity_fallback
                and _in_http_request()
                and self._setting.has_service_identity()
            ):
                raise ServiceIdentityFallbackDisabled(
                    "HTTP request omitted Authorization and "
                    "--no-service-identity-fallback is set"
                )
<<<<<<< HEAD
=======
            return self._borrow(self._service_client(metalake))

        key = (authorization, metalake)
        cached = self._clients_by_auth.get(key)
        if cached is not None:
            self._clients_by_auth.move_to_end(key)
            return self._borrow(cached)

        client = RESTClientFactory.create_rest_client(
            metalake,
            self._setting.gravitino_uri,
            authorization,
            read_timeout=self._setting.gravitino_read_timeout,
        )
        self._cache_put(key, client)
        return self._borrow(client)

    def _resolve_metalake(self) -> str:
        """Resolve the metalake for the current call, tool argument first.

        Raises ``ValueError`` (an invalid/missing request parameter, mapped
        by FastMCP's error middleware to a client-facing "Invalid params"
        error rather than an internal-error code) when the call names none and
        no startup default (``--metalake``) is configured. The message is
        written for the agent that will read it: it names the recovery path so
        a model can correct itself instead of just reporting the failure.
        """
        requested = _REQUEST_METALAKE.get()
        if isinstance(requested, InvalidMetalakeArgument):
            raise ValueError(requested.reason)
        if requested is not MISSING_METALAKE and not isinstance(
            requested, (str, type(None))
        ):
            # An explicitly supplied non-string must not be silently treated as
            # an omitted argument: `false`, `0` and `[]` would otherwise route
            # a call - including a mutation - to the default metalake.
            raise ValueError(
                f"The '{METALAKE_ARGUMENT}' argument must be a string naming a "
                f"metalake, but got {type(requested).__name__}."
            )

        metalake = get_request_metalake() or self._setting.metalake
        if not metalake:
            # Only point at the discovery tool when this deployment actually
            # exposes it; a tag filter can hide it, and naming a tool the
            # agent cannot call leaves it with no way forward.
            recovery = (
                "Call 'list_metalakes' to see the metalakes you can access, "
                f"then retry this call with the '{METALAKE_ARGUMENT}' "
                "argument set to one of them."
                if self._setting.exposes_metalake_discovery()
                else f"Retry this call with the '{METALAKE_ARGUMENT}' argument "
                "set to the metalake to use, or ask the user which one to use."
            )
            raise ValueError(f"No metalake specified. {recovery}")
        return metalake

    def _service_client(self, metalake: str):
        """Return the service-identity client (static token / OAuth) for ``metalake``."""
        if (
            metalake == self._setting.metalake
            and self._default_client is not None
        ):
>>>>>>> 696efe1c2 ([#14498] fix(mcp): configure Gravitino response read timeout (#14506))
            return self._default_client

        cached = self._clients_by_auth.get(authorization)
        if cached is not None:
            self._clients_by_auth.move_to_end(authorization)
            return cached

        client = RESTClientFactory.create_rest_client(
            self._setting.metalake,
            self._setting.gravitino_uri,
<<<<<<< HEAD
            authorization,
=======
            startup_authorization(self._setting),
            auth=self._service_auth,
            read_timeout=self._setting.gravitino_read_timeout,
>>>>>>> 696efe1c2 ([#14498] fix(mcp): configure Gravitino response read timeout (#14506))
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
