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

# Design of Verified Basic Credential Cache in Gravitino

This document describes the optional in-process cache for successfully verified Basic credentials
in the built-in IdP (`plugins:idp-basic`). It complements
[Design of Local Authentication Support in Gravitino](gravitino-local-authentication.md), which
covers the overall local authentication model.

---

## 1. Background

### 1.1 Current Behavior

Built-in IdP authenticates every HTTP request that carries Basic credentials:

```text
Authorization: Basic <base64(username:password)>
```

Password verification uses salted SHA3-512 with 100,000 iterations (`$sha3-512$i=100000$...`).
Each full verification costs roughly **40 ms of CPU** on typical server hardware. There is no
session or token layer after authentication; the client resends the same credentials on every
request, which is correct Basic semantics.

### 1.2 Why This Becomes a Problem

Password hashing cost is intentionally slow for **login-time** verification (OWASP guidance:
tune to stay under about one second per check; common practice is roughly 50–500 ms). Basic
authentication, however, attaches that cost to **every API request** when clients poll or submit
jobs at high QPS.

Measured on an idle server with the `basic` authenticator and built-in IdP:

| Request type | Average latency |
|---|---|
| No credentials (401) | ~1 ms |
| Wrong or correct password (401/200) | ~40 ms |

Under about 100 RPS from a single Basic client, server CPU can reach hundreds of percent on an
8-core host and slow unrelated users. The bottleneck is not header parsing; it is repeated
password derivation on the success path.

### 1.3 Industry Context

HTTP Basic does not define a cache duration. It requires each request to be authenticated, but
does not require each request to repeat the full key-derivation function (KDF). Common practice:

| System | Approach |
|---|---|
| Apache `mod_authn_socache` | Cache successful backend auth results; default TTL 300 s |
| Authelia 4.39+ | Optional in-memory Basic cache (`scheme_basic_cache_lifespan`); default off |
| Traefik | Persistent cache was declined; only concurrent-request deduplication (`singleflight`) |
| High-QPS APIs | Prefer OAuth Bearer / session tokens instead of uncached Basic |

Gravitino adds an **opt-in, in-process** cache aligned with Apache and Authelia: skip repeat KDF
for recently verified credentials while still authenticating every request.

---

## 2. Goals

1. **Reduce CPU on repeat Basic requests**: allow high-QPS Basic clients (UI polling, job
   submission) to avoid paying full SHA3-512 derivation on every request when the same credential
   was verified recently.

2. **Preserve security on the miss path**: failed logins, unknown users, and disabled accounts must
   still run the full verification path; failed attempts are never cached.

3. **Fail closed on credential changes**: password changes, user disable, and user removal must
   invalidate cached entries promptly on the local node and be detected on the next request even
   when another node still holds a stale entry.

4. **Stay opt-in and backward compatible**: default configuration keeps today's behavior (cache
   disabled); enabling the cache does not change REST APIs or stored metadata.

5. **Align with existing authenticator configuration style**: use the
   `gravitino.authenticator.basic.*` prefix, consistent with OAuth and Kerberos settings.

---

## 3. Non-Goals

1. **Replacing Basic with sessions or JWT for local IdP**: this design does not add a login
   endpoint or post-auth token. Operators who need long-lived, high-QPS access should prefer OAuth
   or another token-based flow.

2. **Caching failed authentications**: negative results are not stored, to avoid speeding up
   brute-force attempts against known usernames.

3. **Cross-node shared cache**: the cache is per Gravitino process (Caffeine in heap). A shared
   memcache or Hazelcast layer is out of scope; short TTL plus per-request metadata reload bounds
   multi-node lag.

4. **Lowering KDF cost parameters**: iteration count and algorithm remain unchanged. Performance
   is improved by skipping redundant work, not by weakening stored passwords.

5. **Caching authorization decisions**: only password verification is cached. Group resolution and
   JCasbin policy evaluation are unchanged.

---

## 4. Solution Investigations

| Approach | Pros | Cons | Decision |
|---|---|---|---|
| **A. Do nothing; document low-QPS limit** | No code change | Does not fix real deployments hitting CPU limits at modest QPS | **Rejected** |
| **B. Reduce SHA3-512 iterations** | Faster every request | Weakens offline attack resistance; fights password-storage guidance | **Rejected** |
| **C. Issue session cookie after first Basic login** | Standard web pattern | New API, token lifecycle, CSRF/session storage; large scope for built-in IdP | **Deferred** |
| **D. In-process cache of verified credentials (chosen)** | Small change in `idp-basic`; opt-in; matches Apache/Authelia patterns | Short revocation window across nodes; memory per cached credential | **Chosen** |
| **E. `singleflight` only (no TTL cache)** | Helps concurrent duplicates only | Does not help steady polling at 10–100 RPS | **Rejected as sole fix**; may be added later |

**Why D over C:** Built-in IdP targets POC and isolated deployments. A verified-credential cache
delivers most of the CPU win with no new user-visible protocol. Session support can be considered
separately if product requirements grow.

---

## 5. Proposal

### 5.1 Component Overview

```
HTTP Request (Authorization: Basic ...)
        │
        ▼
 BasicAuthenticator
        │
        ▼
 IdpUserGroupManager.authenticate()
        │
        ├──► IdpUserMetaService.getIdpUser(username)     [always: DB read]
        │
        ├──► VerifiedBasicCredentialCache.isVerified()   [if enabled]
        │         hit + hash fingerprint match → return user (skip KDF)
        │
        └──► PasswordHasher.verify()                     [miss or fingerprint mismatch]
                  success → rememberSuccess()
                  failure → 401 (not cached)
```

New class: `VerifiedBasicCredentialCache` in `plugins:idp-basic`.

### 5.2 Cache Entry Design

| Field | Role |
|---|---|
| **Key** | `SHA-256(username + '\0' + password)` as lowercase hex. Binds the credential pair without storing plaintext in the map key string. |
| **Value** | `username` + `passwordHash` fingerprint (full stored PHC string from `idp_user_meta.password_hash`) |
| **Secondary index** | `username → credentialKey` for O(1) invalidation on admin writes |

**Why key includes password:** A username-only key would allow a wrong password to hit cache while
the stored hash is unchanged. Including the password (via digest) ensures only the exact credential
pair that was verified can hit.

**Why value includes `passwordHash`:** On every request the server reloads the user from storage.
If the cached fingerprint does not equal the current `password_hash`, the entry is treated as a
miss and full KDF runs. This covers password rotation and races before explicit invalidation.

**What is not stored:** Plaintext passwords are not kept as cache values. The key is a one-way
digest of the credential pair.

### 5.3 Cache Policy

| Policy | Setting |
|---|---|
| Store successes only | Yes |
| Store failures | No |
| Eviction | `expireAfterWrite(credentialCacheExpirationSecs)` + `maximumSize(credentialCacheMaxSize)` |
| Default enabled | `false` |
| Default TTL | `60` seconds |
| Default max entries | `10000` |

On Caffeine removal (expiry or size eviction), the username index entry is cleared via a removal
listener.

### 5.4 Authentication Flow

1. `BasicAuthenticator` decodes `Authorization: Basic` and calls
   `IdpUserGroupManager.authenticate(username, password)`.
2. Load `IdpUser` from `idp_user_meta` (always).
3. If the user is disabled or has no `password_hash`, invalidate any cache entry for that username
   and return **401**.
4. If `credentialCacheEnabled` is `true` and `isVerified(username, password, passwordHash)` is
   `true`, return the user **without** calling `PasswordHasher.verify()`.
5. Otherwise run `PasswordHasher.verify()`. On success, call `rememberSuccess()` when the cache is
   enabled; on failure return **401** without writing the cache.
6. Continue with existing group resolution and authorization unchanged.

### 5.5 Invalidation

Invalidate by username on these write paths in `IdpUserGroupManager`:

| Operation | Invalidates cache |
|---|---|
| `changePassword` | Yes |
| `updateEnabled` | Yes |
| `removeUser` | Yes |
| `addUser` | Yes (clears any stale entry for that username) |
| User disabled at authenticate time | Yes |

Multi-node behavior: invalidation is local. Other nodes rely on (a) per-request `password_hash`
comparison on cache hit, and (b) TTL expiry. Worst-case stale success window is bounded by
`credentialCacheExpirationSecs` on remote nodes.

### 5.6 Module and Wiring

| Item | Location |
|---|---|
| Cache implementation | `plugins:idp-basic/.../VerifiedBasicCredentialCache.java` |
| Config keys | `plugins:idp-basic/.../IdpBasicConfigs.java` |
| Integration | `IdpUserGroupManager.authenticate()` and admin mutators |
| Lifecycle | `Closeable`; `invalidateAll()` on manager close |

No new database tables or REST endpoints.

---

## 6. Configuration

| Key | Description | Default |
|---|---|---|
| `gravitino.authenticator.basic.credentialCacheEnabled` | Cache successfully verified Basic credentials between requests | `false` |
| `gravitino.authenticator.basic.credentialCacheExpirationSecs` | TTL in seconds when enabled | `60` |
| `gravitino.authenticator.basic.credentialCacheMaxSize` | Maximum cached credential entries when enabled | `10000` |

Example for a high-QPS Basic deployment:

```properties
gravitino.authenticators=basic
gravitino.authenticator.basic.credentialCacheEnabled=true
gravitino.authenticator.basic.credentialCacheExpirationSecs=60
gravitino.authenticator.basic.credentialCacheMaxSize=10000
```

Operators should enable the cache only after accepting:

- a bounded revocation delay on peer nodes (≤ TTL), and
- additional heap proportional to active cached credentials.

---

## 7. User Process

There is no new operator or end-user workflow. Behavior is controlled entirely by server
configuration.

1. Deploy Gravitino with built-in IdP and Basic authentication (see
   [local authentication design](gravitino-local-authentication.md)).
2. If Basic clients generate sustained load and CPU is dominated by password verification, set
   `gravitino.authenticator.basic.credentialCacheEnabled=true`.
3. Tune `credentialCacheExpirationSecs` (for example 60–300 seconds) based on security vs. load
   requirements. Shorter TTL reduces cross-node stale window; longer TTL reduces KDF frequency.
4. After a password change or user disable, the next request on any node fails authentication even
   if a remote cache entry has not yet expired, because each hit reloads `password_hash` from
   storage.

---

## 8. Comparison with Common Industry Patterns

| Aspect | Apache `mod_authn_socache` | Authelia Basic cache | Gravitino |
|---|---|---|---|
| Default | Off (must configure) | Off (`lifespan=0`) | Off |
| Key | `context + username` | `username` | `SHA-256(user + password)` |
| Value | Backend auth result | HMAC credential fingerprint | Stored `password_hash` |
| Hit skips DB | Often yes | Yes (skips KDF) | No (always reloads user row) |
| Failed login cached | No | No | No |
| Typical TTL | 300 s | Configurable | 60 s default |

Gravitino is more conservative than Apache on cache hits because it always reads the user row and
compares the stored hash fingerprint before skipping KDF.

---

## 9. Work Plan and Checklist

### 9.1 Implementation Tasks

| Phase | Work Item | Module / Files | Notes |
|---|---|---|---|
| 1 | Config entries | `IdpBasicConfigs` | `credentialCacheEnabled`, `credentialCacheExpirationSecs`, `credentialCacheMaxSize` |
| 2 | Cache implementation | `VerifiedBasicCredentialCache` | Caffeine, key digest, username index, removal listener |
| 3 | Auth integration | `IdpUserGroupManager.authenticate()` | `isVerified` / `rememberSuccess` on success path only |
| 4 | Write-path invalidation | `IdpUserGroupManager` mutators | `changePassword`, `updateEnabled`, `removeUser`, `addUser` |
| 5 | Unit tests | `TestVerifiedBasicCredentialCache`, `TestIdpUserGroupManagerCredentialCache` | Hit/miss, TTL, invalidation, disabled default |
| 6 | Documentation | `docs/security/local-users-and-groups.md`, local auth design §7.2 | Config table and security notes |

### 9.2 Review Checklist

| Area | Checklist |
|---|---|
| Security | Failed logins never cached; cache disabled by default; no plaintext passwords in cache values |
| Correctness | Cache key binds username+password; value fingerprint mismatches after password change |
| Invalidation | All user-mutating admin paths call `invalidateUser` |
| Compatibility | Disabled cache preserves pre-change behavior for every request |
| Configuration | Keys use `gravitino.authenticator.basic.*` prefix |
| Operations | Docs state HTTPS requirement, TTL trade-off, and multi-node stale window |

---

## 10. Security Considerations

- **Transport**: Basic credentials must be sent over HTTPS; caching does not change wire exposure.
- **Memory**: Cache keys are digests; values hold username and password-hash strings, not plaintext
  passwords. Entries are bounded by `credentialCacheMaxSize` and TTL.
- **Brute force**: Failed attempts are not accelerated by the cache. Attackers still pay full KDF
  cost on every wrong password.
- **Revocation**: Local invalidation is immediate. Cross-node worst case is bounded by TTL; per-hit
  hash comparison fails closed when the database row changes first.
- **Scope**: Recommended for deployments that already accept Basic authentication risks and need
  throughput; not a substitute for OAuth or enterprise IdP in untrusted networks.

---

## 11. Summary

Built-in IdP Basic authentication correctly verifies every request but pays ~40 ms of SHA3-512
derivation each time. An optional `VerifiedBasicCredentialCache` remembers **successful**
verifications for a short TTL, skips redundant KDF on cache hits, and remains safe by reloading
the user from storage and comparing the stored hash fingerprint. The feature is **disabled by
default**, requires no API or schema changes, and follows patterns used by Apache and Authelia while
keeping a stricter per-request metadata check than typical socache deployments.
