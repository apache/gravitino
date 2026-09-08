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
from contextvars import ContextVar

from mcp_server.client.factory import RESTClientFactory
from mcp_server.core.oauth import RefreshableBearerAuth
from mcp_server.core.setting import Setting

_LOG = logging.getLogger(__name__)

# Upper bound on the number of cached REST clients kept alive at once, across
# every (principal, metalake) combination. Each client owns an httpx connection
# pool; caching lets repeated calls from the same principal against the same
# metalake reuse a pool instead of opening a new one per tool call, while the
# LRU bound keeps memory/sockets in check as principals (e.g. rotating tokens)
# and metalakes come and go.
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

# Name of the optional argument that every tool accepts to name the metalake
# it should operate on. The argument is not declared on any tool function:
# MetalakeArgumentMiddleware advertises it in each tool's input schema, strips
# it from the incoming arguments, and publishes it on _REQUEST_METALAKE below.
METALAKE_ARGUMENT = "metalake"

# The metalake named by the tool call currently being served, or "" when the
# call named none. Scoped to a single tool invocation (the middleware resets it
# in a finally block), so this is request plumbing, not session state: nothing
# is remembered between calls and no state is shared between server replicas.
_REQUEST_METALAKE: ContextVar[str] = ContextVar("request_metalake", default="")


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


def set_request_metalake(metalake: str):
    """Publish the metalake named by the current tool call.

    Returns the token the caller must pass to :func:`reset_request_metalake`
    once the call finishes, so nothing leaks into the next one.
    """
    return _REQUEST_METALAKE.set((metalake or "").strip())


def reset_request_metalake(token) -> None:
    """Undo :func:`set_request_metalake` at the end of a tool call."""
    _REQUEST_METALAKE.reset(token)


def get_request_metalake() -> str:
    """The metalake named by the current tool call, or "" when it named none."""
    return _REQUEST_METALAKE.get()


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
        # Enforced here (not only in do_main()) so any path that constructs a
        # GravitinoContext directly - not just the CLI entrypoint - fails fast
        # on an invalid Setting.
        setting.validate_oauth()
        self._setting = setting
        # Eagerly built only when a startup default is configured, so the
        # common single-metalake deployment pays no extra cost. Left unset
        # (None) when metalake resolution must come from a per-request header
        # on every call (HTTP transport with no --metalake default).
        self._default_client = (
            RESTClientFactory.create_rest_client(
                setting.metalake,
                setting.gravitino_uri,
                startup_authorization(setting),
                auth=_service_auth(setting),
            )
            if setting.metalake
            else None
        )
        # One LRU cache for every cached client, keyed by (Authorization
        # header, metalake), so _MAX_CACHED_CLIENTS bounds the total number of
        # open connection pools rather than being applied per cache. An empty
        # Authorization means the service identity (static token / OAuth),
        # which is only ever produced by the no-Authorization branch of
        # rest_client(), so it can never collide with a real principal's key.
        # Safe without locking: rest_client() runs on the single asyncio event
        # loop and never awaits between lookup and insert.
        self._clients_by_auth: "OrderedDict[tuple[str, str], object]" = (
            OrderedDict()
        )
        # Strong references to in-flight background close tasks; the event loop
        # only keeps weak references, so without this they could be GC'd before
        # running. Entries are discarded when each task completes.
        self._pending_closes: "set[asyncio.Task]" = set()

    def rest_client(self, *, require_metalake: bool = True):
        """Return a REST client carrying the correct identity and metalake.

        The metalake is resolved per call: the ``metalake`` argument of the
        current tool call takes priority, falling back to the configured
        startup default (``--metalake``). Raises ``ValueError`` when neither is
        available.

        ``require_metalake=False`` skips metalake resolution entirely, for the
        metalake-listing tool: it is what an agent calls when it does not know
        a metalake yet, so it must work on a server with no default configured.
        The returned client can only be used for operations that are not
        metalake-scoped.

        Identity resolution is unchanged: in HTTP transport mode the incoming
        request's ``Authorization`` header is forwarded verbatim to Gravitino,
        taking priority over the static startup token. This keeps concurrent
        sessions with different principals and/or metalakes fully isolated —
        one caller's identity or metalake never leaks into another's calls.

        Falls back to a service-identity client (static token or OAuth
        client-credentials) when:
        - running in stdio mode (no HTTP request context), or
        - the incoming request carries no Authorization header.

        Clients are cached per (identity, metalake) combination (and their
        connection pools reused) so a new pool is not opened on every call.
        """
        metalake = self._resolve_metalake() if require_metalake else ""
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
            return self._service_client(metalake)

        key = (authorization, metalake)
        cached = self._clients_by_auth.get(key)
        if cached is not None:
            self._clients_by_auth.move_to_end(key)
            return cached

        client = RESTClientFactory.create_rest_client(
            metalake,
            self._setting.gravitino_uri,
            authorization,
        )
        self._cache_put(key, client)
        return client

    def _resolve_metalake(self) -> str:
        """Resolve the metalake for the current call, tool argument first.

        Raises ``ValueError`` (an invalid/missing request parameter, mapped
        by FastMCP's error middleware to a client-facing "Invalid params"
        error rather than an internal-error code) when the call names none and
        no startup default (``--metalake``) is configured. The message is
        written for the agent that will read it: it names the recovery path so
        a model can correct itself instead of just reporting the failure.
        """
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
            return self._default_client

        key = ("", metalake)
        cached = self._clients_by_auth.get(key)
        if cached is not None:
            self._clients_by_auth.move_to_end(key)
            return cached

        client = RESTClientFactory.create_rest_client(
            metalake,
            self._setting.gravitino_uri,
            startup_authorization(self._setting),
            auth=_service_auth(self._setting),
        )
        self._cache_put(key, client)
        return client

    def _cache_put(self, key: "tuple[str, str]", client) -> None:
        """Cache a client, evicting (and closing) the oldest past the cap."""
        self._clients_by_auth[key] = client
        if len(self._clients_by_auth) > _MAX_CACHED_CLIENTS:
            _, evicted = self._clients_by_auth.popitem(last=False)
            self._schedule_close(evicted)

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
