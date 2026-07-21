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

# Google Cloud KMS testing

The Google Cloud KMS client reads key metadata through the provider's `GetCryptoKey` API. It does
not create keys or key versions, encrypt or decrypt data, rotate keys, or change IAM policies. Unit
tests mock the Google SDK. The live integration test is a separate, explicit opt-in because Google
Cloud does not provide an official Cloud KMS emulator.

## Live fixtures

Provision these four fixtures in a dedicated test project and pass their complete CryptoKey resource
names. Short key names and CryptoKeyVersion names are not accepted.

| Environment variable | Required fixture state |
| --- | --- |
| `GRAVITINO_GCP_KMS_KEY_ID` | An `ENCRYPT_DECRYPT` CryptoKey with an `ENABLED` primary version |
| `GRAVITINO_GCP_KMS_DISABLED_KEY_ID` | An `ENCRYPT_DECRYPT` CryptoKey whose primary version is not `ENABLED` |
| `GRAVITINO_GCP_KMS_NON_ENCRYPTION_KEY_ID` | A non-encryption CryptoKey, such as `ASYMMETRIC_SIGN`, with an `ENABLED` primary version |
| `GRAVITINO_GCP_KMS_MISSING_KEY_ID` | A valid but deliberately nonexistent CryptoKey name in a readable project and key ring |

Every value must use this form:

```text
projects/PROJECT_ID/locations/LOCATION/keyRings/KEY_RING/cryptoKeys/CRYPTO_KEY
```

The fixture owner should be the infrastructure or security team, not the test process. Keep the
fixtures in a labeled, budget-monitored test project and record an owner and review date. Cloud KMS
keys and versions have an ongoing cost, and API calls consume quota and may have request costs; use
the current Google Cloud pricing page when budgeting. The test performs no cleanup because it makes
no changes.

## Authentication and permission

The client accepts no credential properties. It uses Google Application Default Credentials (ADC)
only. The test identity needs `cloudkms.cryptoKeys.get` for every fixture. `roles/cloudkms.viewer`
provides read-only access, or use a custom role containing only that permission. Grant it at a test
project or key-ring scope that also covers the deliberately missing name; do not grant encrypt,
decrypt, create, update, destroy, or IAM-administration permissions.

For local development, authenticate ADC with:

```shell
gcloud auth application-default login
```

For CI, prefer Workload Identity Federation (WIF) and a short-lived identity. Configure the CI
provider to produce an ADC-compatible external-account credential file and expose its path through
`GOOGLE_APPLICATION_CREDENTIALS`, or use the platform's attached workload identity. Do not store or
pass a service-account JSON key as a KMS client property.

## Commands and outcomes

Run all local checks while excluding live integration tests:

```shell
./gradlew :bundles:gcp:check -PskipITs
```

Exercise the safe default skip path without credentials or fixtures:

```shell
./gradlew :bundles:gcp:test \
  --tests org.apache.gravitino.kms.gcp.integration.test.TestGoogleCloudKmsClientIT
```

That command emits one `WARN` naming missing fixture variables, aborts the test class as skipped, and
does not construct the Google SDK client.

Run the live test only after provisioning all fixtures and ADC:

```shell
GRAVITINO_GCP_KMS_IT=true \
GRAVITINO_GCP_KMS_KEY_ID=projects/PROJECT_ID/locations/LOCATION/keyRings/KEY_RING/cryptoKeys/enabled-encryption \
GRAVITINO_GCP_KMS_DISABLED_KEY_ID=projects/PROJECT_ID/locations/LOCATION/keyRings/KEY_RING/cryptoKeys/disabled-encryption \
GRAVITINO_GCP_KMS_NON_ENCRYPTION_KEY_ID=projects/PROJECT_ID/locations/LOCATION/keyRings/KEY_RING/cryptoKeys/enabled-signing \
GRAVITINO_GCP_KMS_MISSING_KEY_ID=projects/PROJECT_ID/locations/LOCATION/keyRings/KEY_RING/cryptoKeys/intentionally-absent \
./gradlew :bundles:gcp:test \
  --tests org.apache.gravitino.kms.gcp.integration.test.TestGoogleCloudKmsClientIT
```

The skip/fail boundary is intentional:

- `-PskipITs` excludes the test class through Gradle before JUnit runs.
- Without `GRAVITINO_GCP_KMS_IT=true`, JUnit logs one warning and reports the class skipped before
  ADC initialization.
- Once `GRAVITINO_GCP_KMS_IT=true`, missing or malformed fixture variables, duplicate fixtures,
  invalid ADC, insufficient permission, unavailable service, missing expected fixtures, or unexpected
  key state fail the test. An opted-in CI job never turns broken configuration into a skip.

## Dependency updates

The Cloud KMS SDK is pinned as `google-cloud-kms` 2.97.0 in `gradle/libs.versions.toml`. That SDK
currently causes Gradle to select `google-auth-library-oauth2-http` 1.49.0 over the GCP bundle's
direct 1.28.0 declaration. The GCP bundle does not relocate `com.google.auth` or `com.google.api.gax`,
so treat KMS SDK and authentication-library updates as one compatibility change. Before updating,
run:

```shell
./gradlew :bundles:gcp:dependencyInsight \
  --configuration runtimeClasspath \
  --dependency google-auth-library-oauth2-http
./gradlew :bundles:gcp:check -PskipITs
```
