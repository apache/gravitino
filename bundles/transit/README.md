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

# Transit KMS provider testing

The Transit bundle contains separate OpenBao and HashiCorp Vault implementations of the common KMS
key-inspection API. The clients only read key metadata; they never encrypt, decrypt, wrap, or unwrap
data.

## Unit tests

Unit tests use an in-process HTTP server and need no Docker daemon or external credentials.

```shell
./gradlew :bundles:transit:check -PskipITs
```

## Docker integration tests

The integration tests start disposable development-mode servers, enable Transit, and create an
encryption key and a signing-only key. They verify the full factory-to-HTTP metadata path, including
a missing key. The containers and all key state are removed after the test.

```shell
./gradlew :bundles:transit:test \
  -PskipTests \
  -PskipDockerTests=false
```

The default immutable image tags are:

| Provider | Image | Optional override |
| --- | --- | --- |
| OpenBao | `openbao/openbao:2.6.0` | `GRAVITINO_OPENBAO_DOCKER_IMAGE` |
| Vault | `hashicorp/vault:2.0.3` | `GRAVITINO_VAULT_DOCKER_IMAGE` |

Pin updates should be made in the test and this README together, then verified on the CI runner
architecture. The tests use insecure development-mode root tokens only inside disposable
containers; production credentials are never accepted or required.

## CI configuration

The CI runner needs:

- a supported Docker daemon reachable by Testcontainers;
- permission to pull the two pinned images; and
- `-PskipDockerTests=false` on the dedicated Docker-test job.

No CI secrets are required. Ordinary unit-test jobs use `-PskipITs`, so the integration package is
excluded even if Docker is available.

OpenBao is the primary open-source Transit reference fixture and is published under the Mozilla
Public License 2.0. Gravitino does not bundle the OpenBao server or its source; the test pulls its
official image at runtime.

Vault is an interoperability target. Its image may be pulled as an external test fixture, but no
Vault binary, image, or source is bundled, redistributed, or shipped with Gravitino. Any CI use of
the image must comply with the image's current HashiCorp terms.
