<!--
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
-->

# Design: Trusted Proxy Users for Apache Gravitino

---

## Background

Gravitino authenticates each REST request with one of the authenticators in `gravitino.authenticators` (`simple`, `basic`, `oauth`, `kerberos`), and authorization, ownership and audit use the authenticated principal. This breaks when a service sits between the end user and Gravitino and the end user holds no credential that Gravitino can verify:

| Deployment | Who authenticates the end user | What Gravitino sees today |
|------------|--------------------------------|---------------------------|
| Trino serving BI, JDBC and ODBC clients | Trino | With `authType=simple` and `gravitino.client.session.forwardUser=true`, an unverified session user name. With `oauth2`, the user's token if the session has one, otherwise the connector's identity. With `basic` or `kerberos`, only the connector's identity, because a catalog that sets `forwardUser=true` fails to load ([#13472](https://github.com/apache/gravitino/issues/13472)). |
| Standalone Lance REST server | Lance REST | One configured service identity, so the service account owns every table Lance REST creates and audit never records the caller ([#13090](https://github.com/apache/gravitino/issues/13090)). |

Knox in front of the Gravitino UI has the same problem ([#8876](https://github.com/apache/gravitino/issues/8876)); see Non-Goal 7.

Hadoop (`hadoop.proxyuser.*`), HiveServer2 and Kyuubi let an authenticated service act for a user that server-side rules allow. Gravitino has no such mechanism, and `SimpleAuthenticator` accepts any user name the client sends.

---

## Goals

1. **Trusted proxy**: A request carrying `X-Gravitino-Proxy-User: alice` runs as `alice` for authorization, ownership and audit if and only if proxying is enabled, its proxy headers pass the checks in [Headers](#headers), and one proxy rule matches the authenticated identity and allows `alice`, every declared group and the TCP peer address. The request must be authenticated by Kerberos, by OAuth 2 with `JwksTokenValidator` or `StaticSignKeyValidator`, by Basic, or by a custom authenticator that supports proxying. A request that fails authentication is rejected as today; among authenticated requests, a malformed proxy header gets 400 and any other failed condition gets 403.
2. **Real user in audit**: For every proxied request, post events and audit logs in the `SimpleAuditLogV2` and JSON formats record the authenticated identity as `realUser`, the authenticator that accepted it as `realUserAuthenticator` and, for OAuth, its issuer as `realUserIssuer`.
3. **Per-user metadata authorization behind Trino**: With `gravitino.client.session.forwardUser=true`, the Trino connector's Gravitino calls run as the session user under `authType=basic` and `kerberos`, and under `oauth2` for sessions without a user token when `gravitino.client.session.forwardUser.useProxy=true`. Reads and writes of table data are authorized only as Non-Goal 1 describes.
4. **No silent fallback**: A client configured to proxy a user fails at `build()` when the server does not apply the proxy header, and rejects any later successful response that the server did not run as the proxied user.
5. **Per-caller identity behind Lance REST**: With `gravitino.lance-rest.gravitino-proxy-caller=true`, objects that a caller creates through the standalone Lance REST server are owned and audited as that caller.
6. **Off by default**: Without the new settings, requests without the new headers behave as today, apart from the [fileset fix](#fileset-fix), and requests with a proxy header are rejected.

---

## Non-Goals

1. **A Trino `SystemAccessControl` plugin or an `/authorize` decision API**: Running the connector's own calls as the end user gives per-user decisions on Gravitino metadata. For data access the connector only loads the table, which `SELECT_TABLE`, `MODIFY_TABLE` or ownership allows, so a user with only `SELECT_TABLE` can write through Trino wherever Trino access control and storage permissions allow it, as today. Checking `MODIFY_TABLE` on the connector's write paths, as the Spark connector does, and a decision API can be added later; see [Solution Investigations](#solution-investigations).
2. **Server-side group resolution**: Gravitino does not look up a proxied user's groups. The proxy declares them, limited to its rule's `groups`. A group mapping SPI belongs with LDAP support ([#6006](https://github.com/apache/gravitino/issues/6006)).
3. **Credentials for processes that run user code**: User code in such a process, for example a Spark engine that Kyuubi launches, could read a proxy's credential and act for every user the proxy's rule allows. Such processes need a per-user credential, which is out of scope.
4. **Proxying over Iceberg REST routes that cannot carry the proxied user**:
   - From Trino to Gravitino's Iceberg REST service: when an Iceberg REST endpoint is available, the connector routes Iceberg catalogs whose backend is not `rest` through it, and Trino's native Iceberg REST client sends none of the Gravitino client's headers. Wherever the connector uses the proxy header, it therefore refuses to register such a catalog ([Trino connector](#trino-connector)), so no proxied session runs as the identity configured for that route.
   - From Gravitino to a remote catalog (`rest` backend), through both the Gravitino API and the Iceberg REST service: with `rest-backend-auth-type=user-token`, the default, Gravitino forwards the caller's own `Authorization` header through `UserPrincipalForwardingAuthManager`; proxied principals carry none, so their requests fail. With `service-account` ([#14595](https://github.com/apache/gravitino/pull/14595)), the remote catalog sees only the service account; Gravitino authorizes the request as the proxied user wherever authorization is enabled, and the Iceberg REST service refuses to serve such a catalog when it is not.
5. **Python client**: No deployment in [Background](#background) uses the Python client. A Python `with_proxy_user` needs no server change and can follow separately.
6. **Replacing token exchange**: Sites whose IdP supports RFC 8693 token exchange can keep using it ([#13092](https://github.com/apache/gravitino/issues/13092)); Gravitino sees an exchanged token as an ordinary OAuth token.
7. **Knox and the MCP server**: Knox passes the end user in a `doAs` query parameter. Using this mechanism needs either a Knox dispatch that sends `X-Gravitino-Proxy-User` instead, or Gravitino accepting `doAs` under the same proxy rules; [#8877](https://github.com/apache/gravitino/pull/8877) proposes accepting `doAs` from any Kerberos caller. That choice is left to a follow-up. The MCP server forwards the `Authorization` header of each HTTP request to Gravitino, by default uses its own configured identity when there is none, and does not authenticate its callers, so a proxy mode would first need caller authentication in the MCP server.

---

## Solution Investigations

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| Decision API `POST /api/metalakes/{metalake}/authorize` plus a Trino `SystemAccessControl` plugin (the sketch in #11307) | Trino enforces before planning and covers every Trino check hook | Answers allow or deny only. The connector's metadata calls still run as the service account, so owners and audit stay wrong. Each engine needs its own plugin. Does nothing for Lance REST. | Rejected |
| RFC 8693 token exchange at the IdP | Standard; Gravitino sees an ordinary OAuth token and needs no proxy rules | Requires an IdP that supports the exchange grant; Kerberos-only sites have none | Compatible, not the base mechanism |
| Replay the end user's credential (Trino `oauth2` forwarding; [#12984](https://github.com/apache/gravitino/pull/12984) for Lance REST) | No new trust configuration | Trino's SPI does not pass passwords to connectors ([#13473](https://github.com/apache/gravitino/pull/13473)). A Kerberos ticket cannot be replayed to another service. The relay holds the user's credential. | Rejected |
| Trust `simple` user names from a trusted network | Works today | Nothing is authenticated; any client can claim any name | Rejected |
| Trusted proxy header checked against server rules | Same model as Hadoop proxy users, which Trino, Kyuubi and HiveServer2 operators already run. Works with any strong authenticator. One server change serves every such deployment. | New trust configuration to manage | **Chosen** |

---

## Proposal

### Overview

```
 BI client ──► Trino ──(Kerberos as trino/coord1@EXAMPLE.COM + X-Gravitino-Proxy-User: alice)──► Gravitino
 caller ────► Lance REST ──(OAuth as lance-rest + X-Gravitino-Proxy-User: bob)─────────────────► Gravitino
```

### Headers

| Header | Direction | Value | Notes |
|--------|-----------|-------|-------|
| `X-Gravitino-Proxy-User` | Request | One user name | The effective user of the request |
| `X-Gravitino-Proxy-Groups` | Request | Comma-separated group names | Valid only with `X-Gravitino-Proxy-User` |
| `X-Gravitino-Effective-User` | Response | One user name | Set on every response to a request whose proxy header the server applied |

Names are UTF-8 and percent-encoded as in RFC 3986. The server encodes `X-Gravitino-Effective-User` the same way, and the client decodes it strictly before comparing. The server splits `X-Gravitino-Proxy-Groups` on raw commas before decoding, so a comma inside a group name is sent as `%2C`.

The server rejects with 400 a repeated proxy header, a name with an invalid percent escape or invalid UTF-8 (`+` is a literal `+`), and any decoded name that:

- is empty or has a leading or trailing space;
- contains a space separator (category Zs) other than U+0020, a character of category Cc, Cf, Zl, Zp, Co, Cs or Cn, or a `Default_Ignorable_Code_Point` character such as a zero-width space or a variation selector;
- changes under NFKC normalization, which covers non-NFC forms and compatibility characters such as full-width letters and the long s `ſ`.

Every name the server compares is therefore in one form and has no invisible characters. Names may contain `/` and `@`, as e-mail addresses and Kerberos principals do.

### Configuration

Key names follow the existing `gravitino.authenticator.oauth.*` style. `<id>` is a rule name chosen by the administrator.

| Key | Default | Description |
|-----|---------|-------------|
| `gravitino.authenticator.proxy.enable` | `false` | Whether the server accepts proxy headers. When `false`, a proxy header gets 403, or 400 when malformed. |
| `gravitino.authenticator.proxy.rules` | empty | Comma-separated rule names |
| `gravitino.authenticator.proxy.rule.<id>.authenticator` | none, required | The authenticator that must have accepted the request: `kerberos`, `oauth`, `basic`, or a custom authenticator's class name as written in `gravitino.authenticators` |
| `gravitino.authenticator.proxy.rule.<id>.issuer` | none, required for `oauth` | The `iss` claim the proxy's token must carry |
| `gravitino.authenticator.proxy.rule.<id>.principals` | none, required | Comma-separated proxy identities the rule applies to (see below) |
| `gravitino.authenticator.proxy.rule.<id>.users` | none, required | Comma-separated users the rule allows, or `*` |
| `gravitino.authenticator.proxy.rule.<id>.excludeUsers` | empty | Comma-separated users the rule never allows, even when `users` names them |
| `gravitino.authenticator.proxy.rule.<id>.groups` | empty | Comma-separated groups the proxy may declare, or `*`. When empty, `X-Gravitino-Proxy-Groups` is rejected. |
| `gravitino.authenticator.proxy.rule.<id>.hosts` | `*` | Comma-separated IP addresses or CIDR ranges the TCP peer address must be in, or `*` |

A rule matches when the authenticator that accepted the request is the rule's `authenticator` and the proxy identity is in `principals`. The proxy identity is the name the authenticator verified, before principal mapping:

| Authenticator | Proxy identity | Accepted forms in `principals` | Example |
|---------------|----------------|--------------------------------|---------|
| `kerberos` | The full principal, with instance and realm | `primary@REALM`, `primary/instance@REALM`, or `primary/*@REALM`, where `*` matches one non-empty instance; primary and realm are literal | `trino/*@EXAMPLE.COM` |
| `oauth` | The token's `sub`, only when its `iss` equals the rule's `issuer` | Literal values without `*` | `lance-rest` |
| `basic` | The user name | Literal values without `*` | `etl-service` |
| A custom authenticator, by class name | The proxy identity the authenticator sets on its `UserPrincipal` | Literal values without `*` | `svc-gateway` |

The OAuth proxy identity is always `sub`, whatever `gravitino.authenticator.oauth.principalFields` selects, because users can choose claims such as `preferred_username` in many IdPs.

Rule semantics:

- One rule must allow everything in a request: the authenticator, the identity, the user, every declared group and the peer address. Rules do not combine.
- `*` in `users` matches no user in `gravitino.authorization.serviceAdmins`, so a service admin can be proxied only when named in `users`. `anonymous` cannot be proxied.
- `hosts` is compared with `HttpServletRequest.getRemoteAddr()`, never with `X-Forwarded-For`. Behind a load balancer the peer is the balancer, so `hosts` can list the balancer's addresses to keep out requests that bypass it, but cannot restrict the clients behind it.
- The standalone Iceberg REST and Lance REST servers read the same keys, and `gravitino.authorization.serviceAdmins`, from their own configuration files.
- Rules are read at startup, so a change takes effect on a server, standalone ones included, only after that server restarts.
- The server fails to start when a listed rule lacks a required key, when `authenticator` is a built-in authenticator's class name instead of its short name, when a `principals` entry does not have an accepted form, when a `users`, `excludeUsers` or `groups` entry fails the decoded-name checks in [Headers](#headers), or when a `hosts` entry is not `*`, an IP address or a CIDR range. A rule whose `authenticator` is any other name not registered on a server loads there and matches no request.

The names in `excludeUsers` and `gravitino.authorization.serviceAdmins`, and `anonymous`, match a proxied name by `String.equalsIgnoreCase` against both the full name and its part before the first `/` or `@`. So `excludeUsers=hdfs` also excludes `HDFS`, `hdfs@EXAMPLE.COM` and `hdfs/x@EXAMPLE.COM`, and `hive` also excludes `hıve` with a dotless `ı`. Exclusions ignore case because Hadoop group mapping often does. Entries in `users` and `groups` match only the exact name.

### Changes to `Authenticator` and `AuthenticationFilter`

`Authenticator` gains a method that marks the authenticators whose principals may act as a proxy. `KerberosAuthenticator`, `OAuth2TokenAuthenticator` and the idp-basic `BasicAuthenticator` return `true`; `SimpleAuthenticator` and existing custom authenticators keep the default.

```java
/**
 * Returns whether a principal accepted by this authenticator may act as a trusted proxy.
 *
 * @return true if requests accepted by this authenticator may carry a proxy header
 */
default boolean supportsProxy() {
  return false;
}
```

An authenticator that supports proxying sets the proxy identity on the `UserPrincipal` it returns. Principal mapping happens inside `KerberosAuthenticator` and the OAuth validators (`JwksTokenValidator`, `StaticSignKeyValidator`), so they set it, and the validators also set the token's issuer. `OAuth2TokenAuthenticator` keeps both only when the configured validator's class is exactly one of those two, and drops them for any other `tokenValidatorClass`, including a subclass. A principal without a proxy identity cannot proxy.

`AuthenticatorFactory` names each authenticator after its entry in `gravitino.authenticators`, with built-in class names normalized to `kerberos`, `oauth`, `basic` and `simple`, so an authenticator cannot claim another's name. `ServerAuthenticator` keeps an ordered name-to-instance map. For a duplicate name the factory keeps the first entry and logs a warning, which matches today's first-match behavior. The `BasicAuthenticator` of `plugins/idp-basic` is named `basic`, including the one that `IdpRESTFeature.registerBasicAuthenticator` adds. `resolvePrincipal` stores the name of the accepting authenticator as a request attribute and clears the real-user fields of the principal, so only `applyProxyUser` sets them.

`authenticate` gains a step between principal resolution and active roles:

```java
private Principal authenticate(HttpServletRequest request, HttpServletResponse response) {
  return applyActiveRoles(request, applyProxyUser(request, response, resolvePrincipal(request)));
}
```

`applyProxyUser` does the following, in order:

1. Neither proxy header is present: return the principal unchanged.
2. A proxy header fails the checks in [Headers](#headers), or `X-Gravitino-Proxy-Groups` comes without `X-Gravitino-Proxy-User`: throw `IllegalProxyHeaderException` (400).
3. Proxying is disabled, the accepting authenticator does not support proxying, or the principal has no proxy identity: throw `ForbiddenException` (403).
4. The user is `anonymous`, or no single rule allows the authenticator, the identity, the user, the peer address and every declared group: throw `ForbiddenException` (403).
5. Otherwise set `X-Gravitino-Effective-User` on the response and return a `UserPrincipal` for the proxied user with the declared groups, no access token, the proxy identity as real user, the accepting authenticator's name and, for `oauth`, the proxy identity issuer. Also store it in the `AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME` request attribute, which today only `runAsPrincipal` sets, so that `HttpAuditFilter` records the proxied user and real user when `applyActiveRoles` then rejects the request with 400. `runAsPrincipal` later replaces it with the principal that active roles narrowed.

`IllegalProxyHeaderException` extends `IllegalArgumentException`. `AuthenticationFilter.doFilter` catches it next to `IllegalActiveRolesException`, and `sendAuthErrorResponse` in `AuthenticationFilter` and `LanceAuthenticationFilter` maps it to 400. `IcebergAuthenticationFilter` already maps every `IllegalArgumentException` to 400. All three map `ForbiddenException` to 403, and the Iceberg and Lance filters, which override only `sendAuthErrorResponse`, inherit the new step. Active roles are applied after it, so `X-Gravitino-Active-Roles` narrows the proxied user's roles.

### Changes to `UserPrincipal`

`UserPrincipal` gets five optional fields:

| Accessor | Value |
|----------|-------|
| `getProxyIdentity()` | The name the authenticator verified, before principal mapping: the full Kerberos principal, the OAuth `sub`, the Basic user name, or what a custom authenticator sets. |
| `getProxyIdentityIssuer()` | The `iss` of the caller's OAuth token, set by the OAuth validators. Only `oauth` rules read it. |
| `getRealUser()` | The proxy identity of the service that sent `X-Gravitino-Proxy-User` for this user, for example `trino/coord1@EXAMPLE.COM`. Audit and events record this form. |
| `getRealUserAuthenticator()` | The name of the authenticator that accepted the real user, for example `kerberos`. |
| `getRealUserIssuer()` | The proxy identity issuer of the real user; set only when the authenticator is `oauth`. |

Two new public constructors take either the proxy identity and its issuer, used by the authenticators and validators above, or the real user with its authenticator and issuer, used by `applyProxyUser`. `withActiveRoles` and every other rebuild for the same user copy all five fields. Only `OAuth2TokenAuthenticator` and `resolvePrincipal` drop any, as described above.

A proxied principal carries no access token, so no component can forward the proxy's credential as the user's. `UserPrincipalForwardingAuthManager`, the one server-side reader of the access token, fails with its existing `UserPrincipal has no authorization header value` error under `rest-backend-auth-type=user-token` (Non-Goal 4).

### Audit and events

Owners, the `creator` field of audit info, and privilege checks read the current principal's name, so they see the proxied user with no change. Two additions record the real user:

- `AuditLog` gets `default @Nullable` methods `realUser()`, `realUserAuthenticator()` and `realUserIssuer()` that return `null`. `SimpleAuditLogV2` and the `JsonAuditLog` of `JsonAuditFormatter` write each one only when it is set. `SimpleAuditLogV2` writes them as `realUser=`, `realUserAuthenticator=` and `realUserIssuer=` fields between `remoteAddress` and `customInfo`, because `customInfo` holds raw query parameters and is not escaped, so a field after it could be forged. The v1 `SimpleFormatter` writes none of them.
- The `Event` constructor captures the three values from the current principal, as it captures `remoteAddress` from `RequestContext`, and `BaseEvent` exposes them. `HttpAuditFilter` builds `HttpRequestEvent` and `HttpRequestFailureEvent` outside `Subject.doAs`, so it passes them the values from the authenticated principal request attribute.

### Client changes

The Java client builder gets one method, which combines with any authentication method:

```java
/**
 * Makes every request act for the given user, which the server accepts only from a trusted proxy.
 *
 * @param user the user to act for
 * @param groups the groups to declare for the user, or an empty list
 * @return this builder
 */
public Builder<T> withProxyUser(String user, List<String> groups)
```

The builder keeps the user and groups in their own fields and adds the headers at `build()`, so a later `withHeaders`, which replaces the whole header map, cannot drop them. It percent-encodes names as in [Headers](#headers) and sends no groups header for an empty list.

A client built with `withProxyUser` checks that the proxy takes effect:

- `build()` of `GravitinoClient` and `GravitinoAdminClient` calls `GET /api/authn/me`, which returns the effective user in `principal`, and fails unless it equals the proxied user. A server without the endpoint fails the build with an error that names the endpoint.
- Every successful response must carry an `X-Gravitino-Effective-User` equal to the proxied user.

A server that ignores the proxy header therefore fails `build()`. The per-response check fires only after a request has run, so every server behind one address must have this feature before clients proxy through it, and must keep it while they do: before a server is downgraded, the clients stop proxying or the server leaves the address.

Because only `withProxyUser` clients make these checks, `withHeaders` rejects both proxy headers, in any letter case, with `IllegalArgumentException`; GVFS's `fs.gravitino.client.request.header.*` keys go through `withHeaders` and are rejected too. A service that acts for many users builds one client per user, as the Trino connector already does for `forwardUser`.

### Client configuration

| Component | Key | Default | Description |
|-----------|-----|---------|-------------|
| Trino connector | `gravitino.client.session.forwardUser.useProxy` | `false` | Under `oauth2`, use the proxy header for sessions without a user token |
| Trino connector | `gravitino.client.session.forwardGroups` | `false` | Send the session's groups in `X-Gravitino-Proxy-Groups` on calls that use the proxy header; ignored otherwise |
| Lance REST | `gravitino.lance-rest.gravitino-proxy-caller` | `false` | Call Gravitino as the service, with the caller in proxy headers |
| Lance REST | `gravitino.lance-rest.gravitino-proxy-client-cache-size` | `1000` | Maximum number of per-caller clients in `gravitino-proxy-caller` mode |

### Trino connector

With `gravitino.client.session.forwardUser=true`:

| `authType` | Current behavior | Behavior with this design |
|------------|------------------|---------------------------|
| `simple` | Sends the session user name, unverified | Unchanged |
| `basic`, `kerberos` | `GravitinoConnector.buildSessionCache` rejects the setting, so the catalog fails to load | The connector's own credentials plus `X-Gravitino-Proxy-User: <session user>` |
| `oauth2`, session has a user token | Forwards the user's token | Unchanged |
| `oauth2`, no user token | `resolveSessionMetadata` returns the shared metadata, so calls run as the connector | Proxy header when `forwardUser.useProxy=true`; unchanged otherwise |

- `buildSessionCache` accepts `basic` and `kerberos`, `resolveSessionMetadata` honors `forwardUser.useProxy`, and `GravitinoAuthProvider.buildForSession` builds each per-user client that uses the proxy header with `withProxyUser`.
- Under `basic` and `kerberos`, and under `oauth2` with `forwardUser.useProxy`, `GravitinoMetadata` resolves the per-user client from the `ConnectorSession` of each call, not once in `getMetadata`, because Trino creates one `ConnectorMetadata` per catalog and transaction, and the session of a `SECURITY DEFINER` view shares the transaction of the query that reads it. Under `simple`, and under `oauth2` without `forwardUser.useProxy`, `GravitinoMetadata` keeps one client per transaction, as today.
- With `forwardGroups=true` the connector declares `ConnectorSession.getIdentity().getGroups()`, which is empty unless Trino has a group provider. A session with a group that the rule does not list fails every query with 403. Without `forwardGroups`, proxied sessions carry no groups, so no group's privileges apply to them, `DENY` included.
- Where `GravitinoMetadata` resolves the client per call, each call leases it as in [Lance REST proxy-caller mode](#lance-rest-proxy-caller-mode), and idle expiry retires a client as eviction does, so neither eviction nor idle expiry closes a client during a call. Where one client serves the transaction, eviction or idle expiry can already close it while the transaction holds it; that existing bug is fixed separately ([#14549](https://github.com/apache/gravitino/issues/14549)).
- Under `kerberos`, each per-user client gets its own `KerberosTokenProvider` built from the connector's keytab, so evicting one client logs out only that client. The cost is one Kerberos login per cache miss.
- Under `oauth2` with `forwardUser.useProxy`, the proxied per-user clients share the connector's OAuth token provider through a `CustomTokenProvider` that delegates `getTokenData()` and whose `close()` does nothing, as in [Lance REST proxy-caller mode](#lance-rest-proxy-caller-mode), because `HTTPClient.close()` also closes its token provider.
- `sessionCacheKey` becomes a structured key of the auth type, the session user, the sorted set of declared groups, whether the proxy header is used and, for `oauth2` sessions with a user token, a SHA-256 hash of the token. No separator character can make two sessions share a client, and a refreshed user token gets a new client.

With `forwardUser=true` under `basic` and `kerberos`, and under `oauth2` with `forwardUser.useProxy`, the connector fails to register an Iceberg catalog that it would route through Gravitino's Iceberg REST service (Non-Goal 4). In these modes such a catalog is usable only with the routing off: `gravitino.iceberg.rest-routing-enabled=false` and neither `gravitino.iceberg.rest-uri` nor `gravitino.iceberg.rest-uri.<metalake>` set.

### Lance REST proxy-caller mode

With `gravitino.lance-rest.gravitino-proxy-caller=true`, Lance REST calls Gravitino with its own OAuth client credentials (`gravitino.lance-rest.gravitino-auth-type=oauth2`):

- For each caller it builds a client with `withProxyUser(caller, groups)` and passes the caller's `X-Gravitino-Active-Roles` through `withHeaders`, so the checks in [Client changes](#client-changes) apply to every call.
- It caches at most `gravitino-proxy-client-cache-size` clients, under a structured key of the caller, the sorted set of groups and the sorted, deduplicated set of trimmed role names. Spellings of one role list share a client, no separator character makes two callers collide, and a caller cannot grow the cache without bound.
- All clients share the one OAuth token provider that Lance REST closes at shutdown. Each client gets it through a `CustomTokenProvider` that delegates `getTokenData()` and whose `close()` does nothing, because `HTTPClient.close()` also closes its token provider.
- Each request leases its client until it returns. Eviction retires a client, which cannot be leased again, so a request that finds its client retired takes a new one from the cache. Leasing and retiring a client are atomic with respect to each other. A retired client is closed at once if no request holds a lease on it, and otherwise when its last lease is released. Without leases, a client evicted between a request's catalog load and its table call would already be closed.
- Lance REST fails to start in this mode unless the outbound auth type is `oauth2` and its own `gravitino.authenticators`, whose default is `simple`, does not include `simple`, so it never forwards a name from `simple`. It also fails to start when the cache size is not positive.

### Catalog impersonation

Catalogs, schemas and filesets with `authentication.impersonation-enable` act on HMS and HDFS as the current user. Today the Kerberos call sites and the fileset proxy append a realm to a name without `@` and pass it to `UserGroupInformation.createProxyUser`. HMS and the NameNode then map that principal to a short name with their own `hadoop.security.auth_to_local` rules, which Gravitino cannot see and which may map, for example, `hdfs-prod@EXAMPLE.COM` to `hdfs`. A proxied user must not reach storage through such a mapping, so for a principal that has a real user, every impersonation call site first does the following:

1. **Reject service names.** Throw `ForbiddenException` (403) when the name contains `/` or `@`, or equals, by `String.equalsIgnoreCase`, the login user name of the call site or its part before the first `/` or `@`. Under Kerberos the login user name is the login principal, for a fileset the one in the merged catalog, schema and fileset properties; under simple authentication it is the name of the process user from `UserGroupInformation.getCurrentUser()`. No `auth_to_local` rule is applied, because the JVM's rules may differ from those of HMS and the NameNode.
2. **Pass the bare name.** Call `createProxyUser` with the name and no realm, and use the same bare name as the owner of any HMS delegation token that the call site requests. `KerberosName.getShortName` returns a one-component name without a realm unchanged, so no `auth_to_local` rule applies to it anywhere.

| Call site | Covers | Change |
|-----------|--------|--------|
| `KerberosCatalogUtils` | Iceberg catalogs, the Iceberg REST server, `ClosableHiveCatalog` | `resolveProxyPrincipal` returns the bare name; the impersonation branch of `createRealUser` runs the check; `ClosableHiveCatalog` requests an HMS token only when impersonation is enabled ([#14408](https://github.com/apache/gravitino/pull/14408)), and for a principal with a real user uses the bare owner |
| `HmsKerberosClient.loginProxyUser` | Hive | The check runs before the existing shortcut `currentUser.equals(realLoginUgi.getUserName()) || hiveClient == null`, which returns the login UGI. For a principal with a real user, only the `hiveClient == null` branch stays; it builds the client that `HiveClientFactory` uses for token requests. The HMS token owner is the bare name. |
| `HiveClientFactory`, simple-authentication branch | Hive | The check runs before the shortcut that reuses the process UGI when the names match |
| `FilesetCatalogOperations` with `ImpersonationHDFSFileSystemProxy` | Filesets | The check runs in the request thread, where the [fileset fix](#fileset-fix) reads the user. For a principal with a real user, `FilesetCatalogOperations` tells `ImpersonationHDFSFileSystemProxy` not to append a realm, so `createProxyUser` gets the bare name. |

Each call site throws the `ForbiddenException` before its own error handling. All code between the call site and the REST layer rethrows that exception unchanged, so the client gets 403. Several layers wrap exceptions today, among them `HiveClientFactory.createHiveClient`, `HiveClientPool.newClient`, `HiveCatalogOperations`, `HiveViewCatalogOperations` and `IcebergCatalogOperations`. The Hive client pool key (`CachedClientPool.extractKey`) and `FilesetCatalogOperations.FileSystemCacheKey` gain the real user, so proxied and non-proxied requests for the same name never share a client or a file system. The Iceberg Hive client pool needs no change: under Kerberos it is keyed by the UGI user name, which is the bare name only for a principal that has a real user.

**Short names.** Step 1 compares only with the login user name of the call site and its first component. HMS and the NameNode may give a principal a short name that differs from its first component, for example `gravitino` for `svc-gravitino@CORP.COM`, and HMS issues a delegation token without a proxy-user check when the owner's short name equals the caller's. A proxied name equal to the login principal's short name, or to the short name of another principal, such as one that a schema sets with impersonation off, therefore passes step 1 and reaches storage as that principal.

So when any catalog, schema or fileset enables impersonation, the `excludeUsers` of every rule lists the users that the NameNode and HMS run as, usually `hdfs` and `hive`, and the short name that HMS and HDFS give each principal that a catalog, schema or fileset sets, whether or not it enables impersonation. Administrators add a name before a catalog, schema or fileset is created with that principal or altered to set it.

With those names excluded, wherever the merged catalog, schema and fileset properties enable impersonation, a proxied user reaches HMS and HDFS only under their own name, never as a superuser through site rules and never under the short name of a principal that a catalog, schema or fileset sets. A proxied name that contains `/` or `@` gets 403 there but can still use Gravitino metadata. Requests whose principal has no real user keep today's behavior, apart from the fileset fix.

#### Fileset fix

Two fileset bugs also affect requests that are not proxied, so they are fixed first, in their own issues and pull requests:

| Bug | Fix |
|-----|-----|
| **Wrong user on pool threads** ([#14415](https://github.com/apache/gravitino/issues/14415)). `FilesetCatalogOperations` creates file systems on its own thread pool, and a pool thread keeps the `Subject` of the request that started it, so a file system created there can act for an earlier user. | [#14416](https://github.com/apache/gravitino/pull/14416) reads the current user in the request thread and passes it to `ImpersonationHDFSFileSystemProxy`, which builds the proxy user from it on every call. |
| **Cache key too coarse** ([#14405](https://github.com/apache/gravitino/issues/14405)). `FileSystemCacheKey` holds the scheme, the authority and the user. For an `hdfs` path, the authority adds at most the principal without its realm and the `authentication.impersonation-enable` value; a location without a scheme carries neither. A fileset can therefore reuse a file system built under other authentication settings, such as another authentication type, principal, keytab or krb5.conf. | [#14407](https://github.com/apache/gravitino/pull/14407) adds the authentication settings of the merged properties to the key. |

### User process

#### Trino with BI clients

1. Trino authenticates its users (password, LDAP, Kerberos or OAuth 2), and no user can choose another session user, for example by making another user the owner of a `SECURITY DEFINER` view.
2. The Gravitino administrator adds a rule for the Trino coordinator principal. Here `gravitino` stands for the short names from [Catalog impersonation](#catalog-impersonation), `lance-rest` is another service account that owns data, and `analysts` and `engineering` are groups that Trino's group provider is authoritative for.

   ```
   gravitino.authenticator.proxy.enable = true
   gravitino.authenticator.proxy.rules = trino
   gravitino.authenticator.proxy.rule.trino.authenticator = kerberos
   gravitino.authenticator.proxy.rule.trino.principals = trino/*@EXAMPLE.COM
   gravitino.authenticator.proxy.rule.trino.users = *
   gravitino.authenticator.proxy.rule.trino.excludeUsers = lance-rest,hdfs,hive,gravitino
   gravitino.authenticator.proxy.rule.trino.groups = analysts,engineering
   ```

3. The Trino administrator configures the Gravitino catalog with `gravitino.client.authType=kerberos`, the connector's keytab and `gravitino.client.session.forwardUser=true`, and optionally `gravitino.client.session.forwardGroups=true`.
4. Alice runs a query from a BI tool. Each Gravitino call the connector makes for alice's session is authorized as `alice`, and the audit line records `user=alice`, `realUser=trino/coord1@EXAMPLE.COM` and `realUserAuthenticator=kerberos`.

#### Standalone Lance REST

1. Lance REST's `gravitino.authenticators`, whose default is `simple`, does not include `simple`. Under `oauth`, callers cannot choose the claims that its `principalFields` and `groupsFields` select.
2. The Gravitino administrator enables proxying and adds a rule with `authenticator=oauth`, `issuer` set to the IdP's issuer, `principals` set to the `sub` of the Lance REST service's OAuth client, `users=*` and `groups` listing only groups that the source of Lance REST's caller groups is authoritative for. The rule's `excludeUsers` lists the users that the NameNode and HMS run as, the short names from [Catalog impersonation](#catalog-impersonation) and the other service accounts that own data, such as the Trino connector's user.
3. The Lance REST administrator sets `gravitino.lance-rest.gravitino-auth-type=oauth2` with the service's client credentials and `gravitino.lance-rest.gravitino-proxy-caller=true`. Tables a caller creates through Lance REST are owned by that caller.

### Error handling

No failed check falls back to the service identity, to `anonymous`, or to another authenticator.

| Situation | Response |
|-----------|----------|
| The caller fails authentication | 401, or 419 from the Iceberg REST server for an expired OAuth token |
| A proxy header fails the checks in [Headers](#headers); groups are declared without a user | 400 |
| Proxying is disabled; the accepting authenticator does not support proxying or the principal has no proxy identity; the user is `anonymous`; no single rule allows the request | 403 |
| A proxied name fails step 1 of [Catalog impersonation](#catalog-impersonation) in an operation whose merged catalog, schema and fileset properties enable impersonation | 403 for that operation |
| A proxy rule fails a startup check in [Configuration](#configuration) | The server does not start |
| Lance REST in `gravitino-proxy-caller` mode fails a startup check of [Lance REST proxy-caller mode](#lance-rest-proxy-caller-mode) | Lance REST does not start |
| The Trino connector uses the proxy header and would route an Iceberg catalog through the Iceberg REST service | The catalog fails to register |
| A `withProxyUser` client gets a successful response with a missing or different effective user, or the server has no `GET /api/authn/me`; `withHeaders` is given a proxy header | The client throws |

### Security considerations

- A proxied identity is only as strong as the proxy's authentication of its own callers. Rules are for services that authenticate every caller and do not let a caller choose another user. This is a deployment requirement for Trino and for Lance REST, which refuses only `simple` at startup. Declared groups are only as trustworthy as the proxy's group source, so `groups` lists only groups that source is authoritative for and complete for: a group's `DENY` privileges apply to a proxied user only when the proxy declares that group.
- Any client can send a proxy header, but it takes effect only when a rule matches; otherwise the request fails. `hosts` uses the TCP peer address, which a client cannot set.
- Wherever impersonation is enabled, a rule grants storage access as the proxied user, not only metadata access. [Catalog impersonation](#catalog-impersonation) keeps proxied users away from superusers that site `auth_to_local` rules produce, and the `excludeUsers` entries it requires keep them away from the users that the NameNode and HMS run as and from the principals that catalogs, schemas and filesets set.
- Through Trino, Gravitino authorizes the connector's metadata calls only (Non-Goal 1). Catalogs where some users may read but not write need Trino system access control that denies those writes, or storage that checks each Trino user's own permissions, for example HDFS with Trino's `hive.hdfs.impersonation.enabled`, set with the `trino.bypass.` prefix.
- Service accounts that own data, such as the Lance REST service user, belong in `excludeUsers` of every rule except their own.
- Where the merged catalog, schema and fileset properties enable impersonation, let `<P>` be the short name of their principal. `hadoop.proxyuser.<P>.users` must not name a member of a superuser group, and `hadoop.proxyuser.<P>.groups` must not list a group that has one, because the NameNode and HMS allow impersonation when the user or any of their groups is listed.
- OAuth proxy identities are bound to the issuer and to `sub`, so a user who can choose a display name or `preferred_username` cannot make a token that matches a rule.
- `Authorization` headers never appear in logs or error responses. A 403 tells the client only that this caller may not act for this user; the proxy identity, the target user and the peer address go to the server log, and a value that failed the header checks is not logged.

### Backward compatibility

- With the feature disabled, requests without the new headers behave as today, apart from the [fileset fix](#fileset-fix). The storage schema does not change.
- Requests that carry a proxy header, which current servers ignore, get 403 or 400 unless a rule allows them. `withHeaders`, including GVFS's `fs.gravitino.client.request.header.*`, rejects both proxy headers.
- `Authenticator.supportsProxy()` defaults to `false`, so existing custom authenticators keep working and cannot proxy. `ServerAuthenticator.authenticators()` still returns the ordered instances; code that adds an authenticator, such as `IdpRESTFeature`, registers it with a name.
- The new `AuditLog` methods have default implementations, so custom `AuditLogWriter` and formatter implementations compile unchanged, and log lines for requests that are not proxied stay as they are.
- Trino catalogs with `forwardUser=true` and `basic` or `kerberos`, which current releases refuse to load, load and use the proxy header, except that Iceberg catalogs the connector would route through the Iceberg REST service fail to register ([Trino connector](#trino-connector)). `oauth2` sessions without a user token keep the connector's identity unless `forwardUser.useProxy` is set; setting it also makes Iceberg catalogs the connector would route through the Iceberg REST service fail to register.
- A connector or Lance REST server that uses the proxy header against a server without this feature fails when it builds its client, because `GET /api/authn/me` returns the proxy's own name or does not exist. Behind one address, every server must have this feature before clients proxy through that address, and keep it while they do ([Client changes](#client-changes)).

### Testing

Unit tests:

| Area | Cases |
|------|-------|
| Proxy rules | Each startup failure in [Configuration](#configuration); duplicate authenticator names keep the first with a warning; matching on the authenticator and the unmapped identity, including the Kerberos instance wildcard, another realm (no match), an OAuth `iss` other than `issuer` (no match), and a `principalFields` claim other than `sub` (matches on `sub`); `users`, `excludeUsers` (also for a name in `users`), service admins, `anonymous`, `groups`, and `hosts` with `*`, addresses and CIDR ranges; two rules that each allow part of a request do not combine; the idp-basic `BasicAuthenticator` matches `basic` rules; a custom authenticator matches by class name; a rule naming an unregistered authenticator loads and matches nothing |
| Header checks | One case per 400 condition, including `admin%20`, `admin%C2%A0`, `admin%E1%9A%80` (an Ogham space mark, which NFKC leaves unchanged), `admin%E2%80%8B`, `admin%E3%85%A4`, `admin%EF%B8%8F`, a non-NFC form of an excluded name, `%EF%BD%88dfs`, `hdf%C5%BF` and the group `%EF%BD%81nalysts`; `admin+` is the literal `admin+`; a malformed header gets 400 with proxying disabled; header checks run before rule checks; `%2C` stays inside one group; a trailing comma in the groups header gets 400; non-ASCII names round-trip |
| Exclusions | Under `users=*` and `excludeUsers=hdfs,hive`, `hdfs%40EXAMPLE.COM`, `HDFS`, `h%C4%B1ve` and `h%C4%B0ve` get 403 and `alice%40example.com` is allowed |
| `AuthenticationFilter` | Each 403 condition in [Error handling](#error-handling); `hosts` ignores `X-Forwarded-For`; active roles narrow the proxied user's roles and keep `realUser`; `X-Gravitino-Effective-User` only on proxied requests; requests without proxy headers are unaffected; the Iceberg and Lance filters return their own error bodies; a custom `tokenValidatorClass`, including a subclass of `JwksTokenValidator`, that sets a proxy identity gets 403 under an `oauth` rule; a real user set by an authenticator is cleared |
| Audit and events | `SimpleAuditLogV2` and `JsonAuditLog` write `realUser` and `realUserAuthenticator` only for proxied requests, and `realUserIssuer` only for proxies accepted by `oauth`; a `customInfo` value containing a tab cannot change the fields after `remoteAddress`; events carry them, including `HttpRequestEvent` and `HttpRequestFailureEvent`; a proxied request with a malformed `X-Gravitino-Active-Roles` gets 400, and its `HttpRequestFailureEvent` carries the proxied user and `realUser` |
| Java client | `withProxyUser` survives a later `withHeaders`; `withHeaders` rejects both proxy headers in any letter case; an empty group list sends no groups header; a non-ASCII user matches the decoded response header; `build()` fails when `GET /api/authn/me` returns another user or does not exist; a successful response with a missing or different `X-Gravitino-Effective-User` fails |
| Trino | `buildSessionCache` accepts `basic` and `kerberos` with `forwardUser`; under `kerberos`, a table read in the same query as another user's `SECURITY DEFINER` view is loaded as the session user and the view's tables as the view owner; under `simple`, and under `oauth2` without `forwardUser.useProxy`, one client serves the transaction; an `oauth2` session without a user token proxies only with `forwardUser.useProxy`; the user `a:b` with no groups and the user `a` with the group `b` never share a client, nor do two user tokens of one user; evicting one per-user client leaves the others' Kerberos logins working, and under `oauth2` with `forwardUser.useProxy` leaves the connector's token provider working; a per-user client evicted or expired during a call stays usable until the call returns and is closed afterwards, and a call whose client is retired before it leases it gets a new client; under `kerberos` with `forwardUser`, an Iceberg catalog that the connector would route through the Iceberg REST service fails to register, and with the routing off it registers |
| Lance REST | Each startup failure; the caller's name, groups and active roles reach Gravitino; role lists that differ only in order, spacing or duplicates share a client; the cache never exceeds its size; evicting a client leaves the shared token provider working; a client evicted between a request's catalog load and its table call stays usable until the request returns and is closed afterwards; a client evicted while no request holds it is closed at once; a request whose client is retired before it leases it gets a new client from the cache |
| Catalog impersonation | For each call site in the table, `hdfs-prod` reaches `createProxyUser`, and the owner of any HMS token, as the bare `hdfs-prod`; 403 for a name containing `@` or `/`, for the login user name or its first component in any letter case (`gravitino` for `gravitino/gw1@EXAMPLE.COM`, or the process user under simple authentication), for the full login principal, and, for a fileset, for the first component of the principal in its merged properties; loading a Hive catalog under a proxied principal succeeds; with impersonation off, a proxied request to an Iceberg catalog with a Hive backend requests no HMS token; proxied and non-proxied requests for one name get separate Hive clients and file systems; non-proxied requests work as today |
| Fileset fix | The tests added by [#14416](https://github.com/apache/gravitino/pull/14416) and [#14407](https://github.com/apache/gravitino/pull/14407) |

Integration tests, following the MiniKdc setup of `MultiAuthOperationsIT` and `KerberosOperationsIT`:

- A Kerberos principal with a rule creates a table as alice; alice owns it and audit records the real user.
- A principal without a rule sends a proxy header and gets 403.
- The Iceberg REST server accepts a proxy header and returns Iceberg-format error bodies on rejection.
- A table created through Lance REST in `gravitino-proxy-caller` mode is owned by the caller, and audit records the service's `sub` as `realUser`, `oauth` as `realUserAuthenticator` and the IdP's issuer as `realUserIssuer`.

### Limitations and open questions

- In the Trino connector's proxy modes, Iceberg catalogs whose backend is not `rest` work only with Iceberg REST routing off. Proxied requests to Iceberg catalogs with a `rest` backend fail under `rest-backend-auth-type=user-token`, and under `service-account` the remote catalog sees only the service account (Non-Goal 4).
- Proxied names that contain `/` or `@`, such as e-mail addresses and Kerberos principals, get 403 wherever impersonation is enabled.
- Names that change under NFKC, for example ones written in full-width letters or half-width katakana, get 400.
- `excludeUsers` is static. A principal that a catalog, schema or fileset sets after the last restart is not excluded until an administrator adds its short name and restarts the servers, so a deployment that lets other users set principals cannot list them in advance.
- `KerberosTokenProvider` derives the realm of Gravitino's HTTP principal from the client principal, so Kerberos proxies must be in the Gravitino server's realm.
- The standalone Iceberg REST server has no authorization, so there a proxied identity does not change access decisions. It is recorded in audit and, with impersonation, presented to HMS and HDFS.
- Open: whether `hosts` should use the client address that `ClientAddressResolver` derives from `X-Forwarded-For` for the proxies in `trustedProxies` ([#14024](https://github.com/apache/gravitino/pull/14024)), as audit does. A rule could then restrict the clients behind a load balancer, at the cost of trusting every address in `trustedProxies`.
- Open: whether standalone servers should keep their own copy of the proxy rules, as proposed, or fetch them from the Gravitino server.

---

## Task Breakdown

- [ ] Publish this design and collect feedback on #11307
- [ ] Fix the two [fileset bugs](#fileset-fix) before the catalog impersonation task: [#14416](https://github.com/apache/gravitino/pull/14416) and [#14407](https://github.com/apache/gravitino/pull/14407)
- [x] Request no HMS token in `ClosableHiveCatalog` when impersonation is disabled ([#14408](https://github.com/apache/gravitino/pull/14408))
- [ ] Add the proxy fields, constructors and copy rules of [Changes to `UserPrincipal`](#changes-to-userprincipal) in `core/`, and `IllegalProxyHeaderException` next to `IllegalActiveRolesException` in `common/`
- [ ] Add `realUser`, `realUserAuthenticator` and `realUserIssuer` to `Event`, `BaseEvent`, `FailureEvent`, `HttpRequestEvent`, `HttpRequestFailureEvent`, `AuditLog`, `SimpleAuditLogV2` and `JsonAuditFormatter` in `core/`, and to `HttpAuditFilter` in `server-common/` (parallel with the next task)
- [ ] Make the changes of [Catalog impersonation](#catalog-impersonation) in `iceberg/iceberg-common/`, `catalogs/hive-metastore-common/`, `catalogs/catalog-hive/`, `catalogs/catalog-lakehouse-iceberg/` and `catalogs/catalog-fileset/`
- [ ] Add proxy rule configuration, parsing, startup checks and matching, `Authenticator.supportsProxy()`, authenticator names in `AuthenticatorFactory` and `ServerAuthenticator`, the `resolvePrincipal` changes, `applyProxyUser` with its header checks and `X-Gravitino-Effective-User`, and the 400 mapping in `AuthenticationFilter`, in `server-common/`; map `IllegalProxyHeaderException` to 400 in `LanceAuthenticationFilter` in `lance/lance-rest-server/`; register the instance that `IdpRESTFeature.registerBasicAuthenticator` adds under `basic` in `plugins/idp-basic/`
- [ ] Return `true` from `supportsProxy()` in `KerberosAuthenticator`, `OAuth2TokenAuthenticator` and `BasicAuthenticator`; set the proxy identity in `KerberosAuthenticator` and `BasicAuthenticator`, and the proxy identity and issuer in `JwksTokenValidator` and `StaticSignKeyValidator`; and make `OAuth2TokenAuthenticator` keep both only when the validator's class is exactly one of those two, in `server-common/` and, for `BasicAuthenticator`, `plugins/idp-basic/`
- [ ] Add `withProxyUser`, the `withHeaders` rejection, the `GET /api/authn/me` check in `build()` of `GravitinoClient` and `GravitinoAdminClient`, and the per-response check in `clients/client-java/`
- [ ] Make the changes of [Trino connector](#trino-connector) in `trino-connector/`
- [ ] Follow-up: check `MODIFY_TABLE` on the Trino connector's write paths, as the Spark connector does, with a test that a user with only `SELECT_TABLE` can read and gets a denial before any storage write
- [ ] Add [Lance REST proxy-caller mode](#lance-rest-proxy-caller-mode) in `lance/`, coordinated with #13090
- [ ] Document proxy users, including the `excludeUsers` guidance of [Catalog impersonation](#catalog-impersonation), the `hadoop.proxyuser` guidance of [Security considerations](#security-considerations), the server upgrade requirement of [Client changes](#client-changes), the audit fields of [Audit and events](#audit-and-events), and the Trino write requirement and group completeness of [Security considerations](#security-considerations), in `docs/security/how-to-authenticate.md`, `docs/gravitino-server-config.md`, `docs/trino-connector/authentication.md`, `docs/iceberg-rest-service.md` and `docs/lance-rest-service.md`
