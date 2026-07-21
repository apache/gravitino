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

# Azure Key Vault KMS testing

The Azure KMS client has deterministic unit tests backed by an in-memory Azure SDK HTTP transport
and an optional read-only integration test against a real Azure Key Vault. The live test is never
enabled implicitly.

## Local tests

Run the module's unit tests without discovering live integration tests:

```shell
./gradlew :bundles:azure:test -PskipITs
```

The unit suite covers key URL validation, latest and pinned SDK requests, 404 handling, provider
and transport failures, time-based enabled state, capability mapping, factory configuration, and
ServiceLoader discovery. It needs neither Azure credentials nor network access.

The module routes `-PskipITs` by excluding `**/integration/test/**`. Supplying that property always
excludes the live test, even when its opt-in environment variable is set.

## Why the live test requires Azure

There is no supported local Key Vault emulator. Azurite emulates Azure Storage Blob, Queue, and
Table services; it does not emulate Azure Key Vault. Do not point this test at Azurite or treat an
HTTP stub as provider integration coverage. See the [Azurite service support documentation][azurite].

The live test is
`org.apache.gravitino.encryption.kms.azure.integration.test.TestAzureKeyVaultKmsLiveIT`. It only
calls the Key Vault Get Key API. A successful run makes three read requests:

1. Read one explicitly versioned key.
2. Read the latest version of the same key.
3. Verify that a reserved key name is absent.

It does not wrap or unwrap data, retrieve private key material, or create, rotate, enable, disable,
update, delete, recover, or purge any Azure resource.

## Live fixture contract

The test reads these variables:

| Variable | Required | Meaning |
| --- | --- | --- |
| `GRAVITINO_AZURE_KMS_IT` | Yes to run | Must be exactly `true`, ignoring case. |
| `GRAVITINO_AZURE_KMS_VAULT_URL` | Yes | Vault HTTPS origin, such as `https://example.vault.azure.net`. |
| `GRAVITINO_AZURE_KMS_KEY_ID` | Yes | Full, versioned key URL ending in `/keys/<name>/<version>`. |
| `GRAVITINO_AZURE_KMS_MANAGED_IDENTITY_CLIENT_ID` | No | Client ID of a user-assigned managed identity. Do not set it for a system-assigned identity or other credential flows. |

The configured key version and its latest version must both be present, enabled, currently valid,
and allow the `wrapKey` and `unwrapKey` operations. The reserved name
`gravitino-kms-it-absent-8bb16e12832d4dd9a6e33dc621647c31` must not exist in the fixture vault.

### Fixture ownership and provisioning

Use a dedicated non-production vault. A cloud/KMS fixture owner should create and own the vault,
key, RBAC assignments, rotation policy, and eventual cleanup. The CI identity must not own or
provision them. Keep the versioned key ID stable; if the fixture owner rotates the key, both the
pinned version and new latest version must remain enabled with the expected operations.

An owner with provisioning permission can create the software-protected RSA fixture and obtain its
URLs with Azure CLI:

```shell
export AZURE_KMS_VAULT_NAME='<dedicated-test-vault>'
export AZURE_KMS_FIXTURE_KEY_NAME='gravitino-kms-it'

az keyvault key create \
  --vault-name "$AZURE_KMS_VAULT_NAME" \
  --name "$AZURE_KMS_FIXTURE_KEY_NAME" \
  --kty RSA \
  --disabled false \
  --ops wrapKey unwrapKey \
  --tags owner='<team>' purpose='gravitino-kms-it'

export GRAVITINO_AZURE_KMS_VAULT_URL="$(
  az keyvault show \
    --name "$AZURE_KMS_VAULT_NAME" \
    --query properties.vaultUri \
    --output tsv
)"
export GRAVITINO_AZURE_KMS_KEY_ID="$(
  az keyvault key show \
    --vault-name "$AZURE_KMS_VAULT_NAME" \
    --name "$AZURE_KMS_FIXTURE_KEY_NAME" \
    --query key.kid \
    --output tsv
)"
```

`az keyvault key create` creates a new version when the name already exists. Provisioning is an
owner action, not part of the test. The accepted `--ops` values and create behavior are documented
in the [Azure CLI key reference][key-cli].

### Least-privilege authorization

For an RBAC-enabled vault, assign the built-in **Key Vault Reader** role to the test principal at
the dedicated fixture-vault scope. It provides metadata reads without secret contents or private
key material and is the least-privilege built-in role needed for the present-key and absent-key
reads. Do not grant Key Vault Administrator, Key Vault Crypto Officer, or cryptographic-operation
roles to the test principal.

```shell
export AZURE_KMS_TEST_PRINCIPAL_OBJECT_ID='<service-principal-or-managed-identity-object-id>'
export AZURE_KMS_VAULT_SCOPE="$(
  az keyvault show \
    --name "$AZURE_KMS_VAULT_NAME" \
    --query id \
    --output tsv
)"

az role assignment create \
  --assignee-object-id "$AZURE_KMS_TEST_PRINCIPAL_OBJECT_ID" \
  --role 'Key Vault Reader' \
  --scope "$AZURE_KMS_VAULT_SCOPE"
```

Use the principal's object ID, not its application/client ID, for
`AZURE_KMS_TEST_PRINCIPAL_OBJECT_ID`. Azure documents the role's data actions in the
[built-in role reference][key-vault-reader]. Allow time for a new assignment to propagate before
running the test.

For a legacy access-policy vault, grant only `get` for keys:

```shell
az keyvault set-policy \
  --name "$AZURE_KMS_VAULT_NAME" \
  --object-id "$AZURE_KMS_TEST_PRINCIPAL_OBJECT_ID" \
  --key-permissions get
```

Azure recommends RBAC for new deployments; access policies are retained here only for existing
fixtures. See the [access-policy guidance][access-policy].

## Authentication with `DefaultAzureCredential`

The factory deliberately accepts no credential secrets. It builds Azure
`DefaultAzureCredential`, so configure exactly one appropriate runtime flow and assign that
identity the read role described above. The principal examples below are alternatives, not a
single combined configuration.

Local Azure CLI login:

```shell
az login
az account set --subscription '<subscription-id-or-name>'
```

The signed-in user needs the Key Vault Reader assignment. `DefaultAzureCredential` can reuse the
Azure CLI login.

Service principal with a client secret, commonly used by a protected CI secret store:

```shell
export AZURE_TENANT_ID='<tenant-id>'
export AZURE_CLIENT_ID='<application-client-id>'
export AZURE_CLIENT_SECRET='<client-secret>'
```

Service principal with a certificate:

```shell
export AZURE_TENANT_ID='<tenant-id>'
export AZURE_CLIENT_ID='<application-client-id>'
export AZURE_CLIENT_CERTIFICATE_PATH='<absolute-path-to-pem-or-pfx>'
# Set AZURE_CLIENT_CERTIFICATE_PASSWORD only when the certificate requires it.
```

Federated workload identity, preferred for CI systems that can issue OIDC tokens:

```shell
export AZURE_TENANT_ID='<tenant-id>'
export AZURE_CLIENT_ID='<application-client-id>'
export AZURE_FEDERATED_TOKEN_FILE='<absolute-path-to-federated-token-file>'
```

For an Azure-hosted runner with a system-assigned managed identity, do not set credential
variables. For a user-assigned identity, attach it to the runner and set:

```shell
export GRAVITINO_AZURE_KMS_MANAGED_IDENTITY_CLIENT_ID='<managed-identity-client-id>'
```

Sovereign clouds may also require `AZURE_AUTHORITY_HOST`, and the vault URL must use that cloud's
Key Vault DNS suffix. See the official [Azure Identity Java documentation][azure-identity] and
[credential-chain guidance][credential-chain]. In CI, expose only the intended flow; stale partial
environment credentials or cached developer-tool sessions can make a credential chain select an
unexpected identity.

## Running the live test

After configuring one credential flow and the two fixture variables, opt in explicitly:

```shell
export GRAVITINO_AZURE_KMS_IT=true

./gradlew :bundles:azure:test \
  --tests 'org.apache.gravitino.encryption.kms.azure.integration.test.TestAzureKeyVaultKmsLiveIT' \
  --rerun-tasks
```

The repository's integration-only routing is equivalent for this module:

```shell
./gradlew :bundles:azure:test -PskipTests --rerun-tasks
```

The `--rerun-tasks` option prevents Gradle from reusing a previous test result after environment
variables or Azure RBAC assignments change.

### Skip versus fail behavior

- With `-PskipITs`, Gradle excludes the live-test package entirely.
- Without `-PskipITs`, when `GRAVITINO_AZURE_KMS_IT` is absent or not `true`, the class logs one
  warning naming any missing fixture variables and aborts through a JUnit assumption.
- Once `GRAVITINO_AZURE_KMS_IT=true`, missing fixture variables fail setup. Malformed URLs, missing
  credentials, authentication failures, authorization failures, disabled or expired fixtures,
  unexpected capabilities, and connectivity or provider errors also fail the run. They are never
  converted into skips.

This split makes ordinary builds safe while ensuring an opted-in CI job cannot silently pass with
a broken cloud configuration.

## Cost and cleanup

The live test creates no resources and performs three Key Vault read operations per successful
run. Azure may charge for Key Vault operations, and price varies by region, agreement, and key
type; consult the current [Key Vault pricing page][pricing]. A standard software RSA key avoids the
additional characteristics of HSM-backed fixtures.

Because the fixture is persistent, the test performs no automatic cleanup. The fixture owner
should periodically review its tags, RBAC assignment, version lifecycle, and usage. When the test
environment is retired, the owner—not CI—should remove the role assignment and delete the key or
dedicated resource group according to organizational retention rules. Key Vault soft-delete and
purge-protection policies can keep deleted resources recoverable and reserve names, so cleanup may
not be immediate.

## Azure SDK version alignment

This module pins `azure-security-keyvault-keys` to `4.8.6`. That release resolves with the existing
`azure-identity` `1.13.1` and `azure-storage-file-datalake` `12.20.0` dependencies on
`azure-core` `1.50.0` and `azure-core-http-netty` `1.15.2`. Keeping one Azure Core/HTTP stack avoids
class and method incompatibilities inside the shared Azure bundle. Do not upgrade the Key Vault
Keys client alone; evaluate and upgrade the Azure identity, storage, core, and HTTP dependencies as
one family, then rebuild the shaded bundle and rerun both deterministic and live tests.

[access-policy]: https://learn.microsoft.com/en-us/azure/key-vault/general/assign-access-policy
[azure-identity]: https://learn.microsoft.com/en-us/java/api/overview/azure/identity-readme?view=azure-java-stable
[azurite]: https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite
[credential-chain]: https://learn.microsoft.com/en-us/azure/developer/java/sdk/authentication/credential-chains
[key-cli]: https://learn.microsoft.com/en-us/cli/azure/keyvault/key?view=azure-cli-latest
[key-vault-reader]: https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles/security#key-vault-reader
[pricing]: https://azure.microsoft.com/en-us/pricing/details/key-vault/
