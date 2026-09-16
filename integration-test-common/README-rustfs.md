<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership. The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied. See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# RustFS credential-vending test fixture

The fileset connection and Iceberg credential-vending integration tests use
RustFS instead of MinIO. The authoritative version and multi-platform digest
are in [`src/test/resources/docker-compose-rustfs.yml`](src/test/resources/docker-compose-rustfs.yml).
`RustFSContainer` reads this packaged resource directly; changing that image
entry changes the image used by the tests. Keep exactly one unquoted image
entry with a release tag and SHA-256 digest.

The initial pin is RustFS **1.0.0-rc.6**, a release candidate. Its published
image index includes `linux/amd64` and `linux/arm64`. The fixture uses the
image's default startup command, waits for a successful signed S3 request,
and creates buckets with the AWS SDK. It needs no MinIO client or additional
setup container. Testcontainers publishes the S3/STS port for host JVMs,
forwards container logs, and removes the container and its temporary data
when the owning test or shared container suite closes.

## Run the compatibility checks

Use JDK 17 and a running Docker daemon. The image-pin unit tests and focused
signed STS policy checks do not start a Gravitino server:

```shell
./gradlew :integration-test-common:test \
  --tests org.apache.gravitino.integration.test.container.TestRustFSContainer \
  :iceberg:iceberg-rest-server:test \
  --tests org.apache.gravitino.iceberg.integration.test.RustFSS3TokenIT \
  -PskipDockerTests=false
```

Build the catalog runtime before running the existing embedded client suites
from a fresh checkout:

```shell
./gradlew :catalogs:catalog-lakehouse-iceberg:jar
./gradlew :catalogs:catalog-fileset:test \
  --tests org.apache.gravitino.catalog.fileset.integration.test.FilesetS3TokenConnectionIT \
  :iceberg:iceberg-rest-server:test \
  --tests org.apache.gravitino.iceberg.integration.test.IcebergRESTRustFSTokenAuthorizationIT \
  -PskipDockerTests=false
```

The backend integration workflow also runs the affected suites in its existing
Linux amd64 embedded/deploy matrix when the image resource changes. Deploy
tests require the normal assembled Gravitino distribution.

The focused tests obtain temporary credentials through `S3TokenGenerator`,
using its actual inline policy and signed `AssumeRole` request. They check
read/write/delete permissions, another bucket, adjacent object paths,
`GetBucketLocation`, both listing APIs with allowed and denied prefixes,
read-only sessions, and a completed two-part multipart upload. Denials must
be S3 `403 AccessDenied` responses. The RustFS Spark tests also require that
error in the cause chain for select-only and narrowed-active-role writes.

## Compatibility boundaries

- RustFS derives a session's permissions from its caller and inline policy.
  The fixture's role ARN satisfies the SDK's request format; these tests do
  not establish AWS named-role trust or external-ID enforcement.
- Fileset policies deliberately allow the bare location prefix for Hadoop
  directory probing. This permits listing sibling key names sharing that
  prefix, while the object-resource policy still denies access to their
  contents. The focused checks preserve this distinction.
- Gravitino's generated write policy grants `PutObject` and `DeleteObject`;
  it does not grant `AbortMultipartUpload`. The multipart test uses root
  credentials to clean up an unfinished upload after a failure. Passing
  these checks does not establish every S3 multipart edge case.
- Image scanning cannot establish Rust dependency coverage when an image
  lacks usable Rust package metadata. See the linked image-maintenance
  follow-up for update detection, scan coverage, and maintainer triage.

Related to [#13154](https://github.com/apache/gravitino/issues/13154).
