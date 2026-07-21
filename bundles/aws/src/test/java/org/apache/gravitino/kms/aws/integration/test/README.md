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

# AWS KMS integration tests

The AWS bundle has two KMS integration-test modes. `AwsKmsLocalStackIT` creates disposable KMS
fixtures in a LocalStack container. `AwsKmsLiveIT` performs one read-only `DescribeKey` request
against an explicitly selected AWS key. Neither test sends key material to Gravitino, and the live
test never calls an encrypt, decrypt, wrap, unwrap, create, update, disable, or delete API.

## Unit and contract tests

Run the tests that require no external backend:

```shell
./gradlew :bundles:aws:test -PskipITs
```

`-PskipITs` excludes `**/integration/test/**` in this module. Docker-tagged tests are also excluded
unless `-PskipDockerTests=false` is supplied.

## LocalStack KMS fixture

Prerequisites:

- A working Docker daemon.
- An explicit, immutable `localstack/localstack-pro:YYYY.MM.patch` image tag. The current pinned
  test image as of July 2026 is `localstack/localstack-pro:2026.06.2`.
- A LocalStack CI Auth Token in `LOCALSTACK_AUTH_TOKEN`. LocalStack requires CI tokens for CI
  environments; a developer token must not be used there.
- Network access to pull the image and activate its license with LocalStack.

Run only the LocalStack KMS test:

```shell
GRAVITINO_AWS_KMS_LOCALSTACK_IMAGE=localstack/localstack-pro:2026.06.2 \
LOCALSTACK_AUTH_TOKEN='<ci-auth-token>' \
./gradlew :bundles:aws:test \
  -PskipTests -PskipDockerTests=false \
  --tests org.apache.gravitino.kms.aws.integration.test.AwsKmsLocalStackIT
```

The test supplies disposable AWS credentials internally. Do not set real AWS credentials for this
fixture. It creates symmetric encryption, RSA encryption, signing-only, disabled, aliased, and
missing-key cases. It verifies key ID, key ARN, alias name, and alias ARN lookups through the
production factory and client.

The Pro image, service availability, and Auth Token are governed by LocalStack's licensing and plan
terms, not the Apache License. Never commit, print, or place the token in a command checked into CI;
store it as a masked CI secret. See the official [Auth Token documentation][localstack-auth] and
[Docker image documentation][localstack-images].

## Live AWS read-only fixture

The principal resolved by the AWS SDK default credential provider chain needs only
`kms:DescribeKey` for the selected key, permitted by its IAM policy and the key policy. No
cryptographic or mutating permission is needed.

Required fixture variables:

- `GRAVITINO_AWS_KMS_LIVE_REGION`: Region containing the key.
- `GRAVITINO_AWS_KMS_LIVE_KEY_ID`: key ID, key ARN, alias name, or alias ARN.

Configure credentials through the normal AWS SDK default chain, such as `AWS_PROFILE`, IAM Identity
Center, `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN`, web identity, or an attached
container/instance role. Then run:

```shell
AWS_PROFILE='<read-only-profile>' \
GRAVITINO_AWS_KMS_LIVE_REGION='us-west-2' \
GRAVITINO_AWS_KMS_LIVE_KEY_ID='alias/gravitino-read-only-test' \
./gradlew :bundles:aws:test \
  -PskipTests \
  --tests org.apache.gravitino.kms.aws.integration.test.AwsKmsLiveIT
```

AWS may charge for the `DescribeKey` request according to the account's service pricing.

## Skip and failure behavior

- With `-PskipITs`, neither integration test is selected.
- Without `-PskipDockerTests=false`, the LocalStack test's `gravitino-docker-test` tag is excluded.
- If a selected LocalStack test lacks `GRAVITINO_AWS_KMS_LOCALSTACK_IMAGE` or
  `LOCALSTACK_AUTH_TOKEN`, it logs a warning naming every missing variable and is reported as
  aborted/skipped before pulling an image.
- If the live test lacks either live fixture variable, it logs the missing names and is reported as
  aborted/skipped before creating a client.
- Once all required fixture variables are present, invalid image tags, Docker failures, license or
  token failures, missing credentials, denied access, missing keys, and contract mismatches fail the
  test. They are not converted to skips.

## Cleanup

The LocalStack test closes both AWS clients, restores any JVM credential system properties it
temporarily replaced, and stops/removes its container in `@AfterAll`. All keys and aliases exist only
inside that disposable container. If the JVM is forcibly terminated before cleanup, remove the
leftover container with `docker ps` and `docker rm -f <container-id>`.

The live test creates no resources and requires no cleanup.

[localstack-auth]: https://docs.localstack.cloud/aws/getting-started/auth-token/
[localstack-images]: https://docs.localstack.cloud/aws/customization/other-installations/docker-images/
