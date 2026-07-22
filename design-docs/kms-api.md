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

# KMS API

## Status

The provider-neutral KMS foundation is implemented. This document defines the general KMS model,
configuration, routing, lifecycle, provider boundary, and error contract.

The current implementation reads key properties. The API is not limited to inspection.
Cryptographic and administrative operations can be added when their cross-provider semantics are
defined.

## Goals

- Represent a key without embedding credentials or key material.
- Support multiple named KMS instances, including multiple instances of the same provider API.
- Route requests to long-lived, server-private provider clients.
- Define provider-neutral operations and results where provider semantics agree.
- Preserve provider-specific key identifiers and validation.
- Provide a reusable foundation for future KMS capabilities.

## Current scope

The implementation includes provider configuration, discovery, routing, lifecycle, error handling, and
key-property reads. It does not currently provide:

- Creating, rotating, enabling, disabling, or deleting keys.
- Wrapping, unwrapping, encrypting, or decrypting data.
- Listing keys or configuring a key allowlist or alias map.
- Vending provider credentials to engines or users.
- Defining which Gravitino users may use which keys.

Those concerns may use this foundation later without changing the key-reference or named-source model.

## Core model

### KMS API

`KmsApi` identifies the provider API and therefore the syntax and semantics of a key ID. The stable
configuration values are:

- `aws-kms`
- `google-cloud-kms`
- `azure-key-vault`
- `openbao-transit`
- `vault-transit`

There is no custom, unknown, or implicit default value. OpenBao Transit and Vault Transit are separate
APIs because their implementations and product lifecycles may diverge, even though the current wire
operation is compatible.

### Key reference

`KmsReference` is the portable routing envelope:

```java
public final class KmsReference {
  private final KmsApi api;
  private final String source;
  private final String keyId;
}
```

- `api` selects the provider contract.
- `source` names one configured KMS client instance.
- `keyId` is the opaque, provider-native identifier interpreted by that instance.

The source is not a key alias. The current implementation intentionally has no configured key list or
alias map. A higher-level policy may constrain which references are accepted without changing KMS
connection configuration.

Key IDs contain no key material and are not secrets by this contract. They may appear in validation
errors and diagnostics; deployments should still follow their normal identifier-redaction policy.

Examples:

```java
new KmsReference(KmsApi.AWS_KMS, "aws-production", "alias/customer-data");

new KmsReference(
    KmsApi.GOOGLE_CLOUD_KMS,
    "gcp-production",
    "projects/p/locations/us/keyRings/r/cryptoKeys/customer-data");

new KmsReference(
    KmsApi.AZURE_KEY_VAULT,
    "azure-production",
    "https://team-kms.vault.azure.net/keys/customer-data/version");

new KmsReference(KmsApi.OPENBAO_TRANSIT, "bao-production", "customer-data");
```

### Key properties

`KmsClient` owns provider resources and currently exposes `getKeyProperties`:

```java
public interface KmsClient extends AutoCloseable {
  KmsKeyProperties getKeyProperties(KmsReference reference);
}
```

The operation returns `KmsKeyProperties`:

```java
public interface KmsKeyProperties {
  KmsReference reference();

  boolean present();

  boolean enabled();

  boolean supportsWrapping();

  boolean supportsUnwrapping();
}
```

The result preserves the exact requested reference. `enabled()` means currently usable according to
the metadata exposed by that provider. It does not prove that the caller has cryptographic permission,
that an external HSM is reachable, or that a trial cryptographic operation succeeds.

The KMS API reports facts. Callers define which combination of properties their operation requires.

## Configuration

Gravitino configuration defines named client instances. The same `KmsApi` may be used by more than one
source:

```properties
gravitino.kms.sources=aws-us,aws-eu,bao-production

gravitino.kms.source.aws-us.api=aws-kms
gravitino.kms.source.aws-us.endpoint.region=us-west-2
gravitino.kms.source.aws-us.credential.method=default

gravitino.kms.source.aws-eu.api=aws-kms
gravitino.kms.source.aws-eu.endpoint.region=eu-west-1
gravitino.kms.source.aws-eu.credential.method=default

gravitino.kms.source.bao-production.api=openbao-transit
gravitino.kms.source.bao-production.endpoint.address=https://openbao.example.com
gravitino.kms.source.bao-production.endpoint.transitMount=transit
gravitino.kms.source.bao-production.credential.method=token_file
gravitino.kms.source.bao-production.credential.path=/run/secrets/openbao-token
```

Each source must be listed exactly once. Unlisted sources, duplicate names, unsupported APIs, unknown
provider properties, missing required properties, and malformed endpoints fail initialization.

Provider configuration is currently:

| API              | Endpoint properties                                                                             | Credential properties                                                      |
| ---------------- | ----------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| AWS KMS          | Required `endpoint.region`; optional `endpoint.serviceAddress` for an HTTP(S) endpoint override | `credential.method=default`                                                |
| Google Cloud KMS | Required `endpoint.projectId` and `endpoint.location`                                           | `credential.method=default`                                                |
| Azure Key Vault  | Required `endpoint.vaultUrl`                                                                    | `credential.method=default`; optional `credential.managedIdentityClientId` |
| OpenBao Transit  | Required `endpoint.address`; optional `endpoint.transitMount`, default `transit`                | `credential.method=token_file`; required absolute `credential.path`        |
| Vault Transit    | Required `endpoint.address`; optional `endpoint.transitMount`, default `transit`                | `credential.method=token_file`; required absolute `credential.path`        |

Cloud providers use their official SDK credential chains. Credential secrets are not accepted as KMS
source properties. Transit reads a token from the configured file on first use, caches it, and reloads
the file once when the backend returns HTTP 401 or 403 before reporting authentication failure.

The explicit `credential.method` discriminator makes the configuration extensible without accepting
partially specified or ambiguous credential shapes. Only the methods in the table are implemented.

## Discovery, routing, and lifecycle

`KmsClientFactory` creates a client for one API and configured source. Factories are registered through
`META-INF/services/org.apache.gravitino.encryption.kms.KmsClientFactory` and discovered through the
application context classloader.

`KmsClientRegistry` is the server-side router and lifecycle owner:

1. Parse every configured source.
2. Index at most one discovered factory for each API and reject duplicate implementations.
3. Require a matching factory and create one client for each configured source during full-server
   initialization.
4. Reuse that client, including its SDK or pooled HTTP transport, for every request to the source.
5. Reject an unknown source or an API/source mismatch before calling a provider.
6. Close all clients in reverse creation order during shutdown.

One factory per API does not mean one source per API. A factory can create any number of named client
instances.

The registry is created only by `GravitinoEnv.initializeFullComponents`; base-only environments do not
create it.

The distribution ships four lean shaded provider bundles under
`distribution/package/kms-providers`: AWS KMS, Google Cloud KMS, Azure Key Vault, and Transit. The
Transit bundle registers separate OpenBao and Vault factories. The launcher adds
`${GRAVITINO_HOME}/kms-providers` to the application classpath, so the registry discovers all five
factories through the application context classloader while creating clients only for configured
named sources.

## Boundary with credential vending

Gravitino credential vending and the KMS API are adjacent but distinct:

- `CredentialProvider` produces serializable credentials for clients or engines to access external
  data systems. It is catalog-scoped, user/context-aware, and caches expiring `Credential` values.
- `KmsClient` uses server-private authentication to invoke a configured KMS and returns non-secret key
  metadata or operation results. It is server-scoped and routes by `KmsReference`.

KMS provider credentials must never be returned as `Credential`, a credential DTO, catalog properties,
or key properties. `KmsClient` and `KmsClientFactory` therefore do not extend `CredentialProvider` or
reuse `CatalogCredentialManager`.

The KMS implementation does follow the repository's `ServiceLoader`, context-classloader,
`AutoCloseable`, shaded-bundle, and contract-test conventions. That mechanical similarity does not
justify merging two APIs with opposite secret-handling boundaries. If a future KMS authentication flow
needs another abstraction, it should compose with a server-private secret resolver or authentication
provider rather than the credential-vending API.

## Provider normalization

| Provider         | Read operation and accepted key ID                                                            | Normalized result                                                                                                                                                                         |
| ---------------- | --------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| AWS KMS          | SDK `DescribeKey`; key ID, key ARN, alias name, or alias ARN                                  | Present unless AWS returns not found; enabled only for `enabled=true` and `KeyState.ENABLED`; wrap and unwrap require `ENCRYPT_DECRYPT` usage                                             |
| Google Cloud KMS | SDK `GetCryptoKey`; full CryptoKey name within the configured project and location            | Present unless GCP returns not found; enabled when an enabled primary version exists; wrap and unwrap require `ENCRYPT_DECRYPT` purpose                                                   |
| Azure Key Vault  | SDK `KeyClient.getKey`; full key URL for the configured vault, optionally including a version | Present unless Azure returns not found; enabled requires the enabled flag and current `notBefore`/`expiresOn` bounds; capabilities require the exact `wrapKey` and `unwrapKey` operations |
| OpenBao Transit  | `GET /v1/{mount}/keys/{name}`; one Transit key-name segment                                   | Present for a valid, non-soft-deleted record; enabled while present; capabilities use `supports_encryption` and `supports_decryption`                                                     |
| Vault Transit    | `GET /v1/{mount}/keys/{name}`; one Transit key-name segment                                   | Present for a valid record; enabled while present; capabilities use `supports_encryption` and `supports_decryption`                                                                       |

AWS, GCP, and Azure use their official Java SDK request and response types. Transit uses a small Apache
HttpClient 5 adapter with typed request and response objects because adding Spring Vault would conflict
with the repository's Java baseline and introduce a substantially larger Spring dependency surface.

## Error contract

- `KmsConfigurationException` extends `IllegalArgumentException` and reports invalid startup
  configuration.
- Other invalid arguments include malformed key IDs, unknown sources, and API/source mismatches.
- Only a definitive provider not-found result returns `present() == false`.
- `KmsAuthenticationException` extends `ConnectionFailedException` and reports credentials rejected by
  the backend.
- Timeouts, throttling, provider failures, malformed successful responses, token-file read failures,
  and other inability to query a backend throw `ConnectionFailedException` with the cause when one is
  available.
- Provider unavailability is never represented as a key property or normalized to a missing key.

For Transit, HTTP 404 on the configured key metadata route is treated as missing. A wrong server or
mount may also return 404, so deployment configuration and real-backend integration tests are required
to establish that the route is correct.

## Test evidence

Implemented automated coverage includes:

- API model and shared `KmsClient`/`KmsClientFactory` contract tests;
- registry configuration, dispatch, concurrency, partial-startup cleanup, close, and environment
  lifecycle tests;
- provider identifier validation, SDK/HTTP normalization, error mapping, configuration, and
  `ServiceLoader` discovery tests;
- packaged-runtime verification that launches a fresh JVM with only the assembled distribution
  classpath and discovers all five provider factories from `kms-providers`;
- deterministic AWS SDK, GCP SDK, Azure in-memory transport, and Transit HTTP tests;
- Docker integration tests against pinned OpenBao and Vault images;
- an opt-in LocalStack KMS test requiring a licensed LocalStack Pro fixture; and
- opt-in, read-only AWS, GCP, and Azure live-provider tests.

The unit suites and Dockerized OpenBao/Vault tests have been run for this implementation. LocalStack
and live-cloud tests are available but require external fixtures and were not part of the local
verification.

## Extending the API

The shared foundation is `KmsApi`, `KmsReference`, named-source configuration, factory discovery,
routing, lifecycle, and the error model. Key-property reads are the operations currently implemented.

Future work may add cryptographic or administrative capability interfaces and provider-specific result
extensions. The exact Java shape should be decided when the operation semantics are defined rather
than by adding placeholder methods now. New capabilities must continue to:

- route through the configured source;
- preserve provider-native identifiers;
- keep provider credentials and key material private;
- expose normalized cross-provider contracts only where semantics genuinely agree; and
- permit provider-specific extensions without weakening the common contract.

## Remaining implementation work

- Additional KMS operations as their portable semantics are defined.
- Authorization if KMS operations become directly user-facing.
- A server-private authentication abstraction if provider SDK credential chains and token files no
  longer cover the required authentication methods.

## Applications

### Iceberg table encryption

Iceberg table encryption can use `KmsReference` and `getKeyProperties` to validate a configured key.
The encryption policy, key allowlisting, enforcement, audit events, and metadata integrity remain
application concerns outside the KMS API.
