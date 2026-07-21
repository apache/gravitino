/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.kms.aws.integration.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.encryption.kms.KmsApi;
import org.apache.gravitino.encryption.kms.KmsClient;
import org.apache.gravitino.encryption.kms.KmsKeyProperties;
import org.apache.gravitino.encryption.kms.KmsReference;
import org.apache.gravitino.kms.aws.AwsKmsClientFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.model.CreateAliasRequest;
import software.amazon.awssdk.services.kms.model.CreateKeyRequest;
import software.amazon.awssdk.services.kms.model.DisableKeyRequest;
import software.amazon.awssdk.services.kms.model.KeyMetadata;
import software.amazon.awssdk.services.kms.model.KeySpec;
import software.amazon.awssdk.services.kms.model.KeyUsageType;

/** Integration tests for AWS KMS inspection against an isolated LocalStack KMS fixture. */
@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AwsKmsLocalStackIT {

  private static final Logger LOG = LoggerFactory.getLogger(AwsKmsLocalStackIT.class);

  private static final String ENV_LOCALSTACK_IMAGE = "GRAVITINO_AWS_KMS_LOCALSTACK_IMAGE";
  private static final String ENV_LOCALSTACK_AUTH_TOKEN = "LOCALSTACK_AUTH_TOKEN";
  private static final String AWS_ACCESS_KEY_ID_PROPERTY = "aws.accessKeyId";
  private static final String AWS_SECRET_ACCESS_KEY_PROPERTY = "aws.secretAccessKey";
  private static final String REGION = "us-east-1";
  private static final String SOURCE = "localstack-kms";

  private LocalStackContainer localStack;
  private software.amazon.awssdk.services.kms.KmsClient fixtureClient;
  private KmsClient client;
  private String previousAccessKeyId;
  private String previousSecretAccessKey;
  private boolean installedSystemCredentials;
  private KeyMetadata symmetricKey;
  private KeyMetadata asymmetricKey;
  private KeyMetadata signingKey;
  private KeyMetadata disabledKey;
  private String aliasName;
  private String aliasArn;

  @BeforeAll
  void startLocalStackAndCreateFixtures() {
    String image = System.getenv(ENV_LOCALSTACK_IMAGE);
    String authToken = System.getenv(ENV_LOCALSTACK_AUTH_TOKEN);
    abortWhenFixtureIsNotConfigured(image, authToken);

    DockerImageName imageName = pinnedLocalStackImage(image);
    localStack =
        new LocalStackContainer(imageName)
            .withServices(LocalStackContainer.Service.KMS)
            .withEnv(ENV_LOCALSTACK_AUTH_TOKEN, authToken)
            .withEnv("ACTIVATE_PRO", "1")
            .withEnv("AWS_DEFAULT_REGION", REGION);
    localStack.start();

    fixtureClient =
        software.amazon.awssdk.services.kms.KmsClient.builder()
            .endpointOverride(localStack.getEndpointOverride(LocalStackContainer.Service.KMS))
            .region(Region.of(REGION))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        localStack.getAccessKey(), localStack.getSecretKey())))
            .build();
    createFixtures();
    installDefaultChainCredentials();

    Map<String, String> properties = new HashMap<>();
    properties.put(AwsKmsClientFactory.REGION, REGION);
    properties.put(AwsKmsClientFactory.CREDENTIAL_METHOD, "default");
    properties.put(
        AwsKmsClientFactory.SERVICE_ADDRESS,
        localStack.getEndpointOverride(LocalStackContainer.Service.KMS).toString());
    client = new AwsKmsClientFactory().create(SOURCE, properties);
  }

  @AfterAll
  void cleanUp() {
    try {
      if (client != null) {
        client.close();
      }
    } finally {
      try {
        if (fixtureClient != null) {
          fixtureClient.close();
        }
      } finally {
        restoreDefaultChainCredentials();
        if (localStack != null) {
          localStack.stop();
        }
      }
    }
  }

  @Test
  void testInspectsKeyByEveryAwsSupportedReferenceForm() {
    List<String> keyIdentifiers =
        Arrays.asList(symmetricKey.keyId(), symmetricKey.arn(), aliasName, aliasArn);

    for (String keyIdentifier : keyIdentifiers) {
      KmsReference reference = new KmsReference(KmsApi.AWS_KMS, SOURCE, keyIdentifier);
      KmsKeyProperties properties = client.getKeyProperties(reference);

      Assertions.assertEquals(reference, properties.reference());
      Assertions.assertTrue(properties.present());
      Assertions.assertTrue(properties.enabled());
      Assertions.assertTrue(properties.supportsWrapping());
      Assertions.assertTrue(properties.supportsUnwrapping());
    }
  }

  @Test
  void testNormalizesKeySpecUsageAndState() {
    KmsKeyProperties asymmetric = properties(asymmetricKey.keyId());
    Assertions.assertTrue(asymmetric.present());
    Assertions.assertTrue(asymmetric.enabled());
    Assertions.assertTrue(asymmetric.supportsWrapping());
    Assertions.assertTrue(asymmetric.supportsUnwrapping());

    KmsKeyProperties signing = properties(signingKey.keyId());
    Assertions.assertTrue(signing.present());
    Assertions.assertTrue(signing.enabled());
    Assertions.assertFalse(signing.supportsWrapping());
    Assertions.assertFalse(signing.supportsUnwrapping());

    KmsKeyProperties disabled = properties(disabledKey.keyId());
    Assertions.assertTrue(disabled.present());
    Assertions.assertFalse(disabled.enabled());
    Assertions.assertTrue(disabled.supportsWrapping());
    Assertions.assertTrue(disabled.supportsUnwrapping());
  }

  @Test
  void testReportsMissingKeyWithoutFailingTheRequest() {
    KmsKeyProperties missing = properties(UUID.randomUUID().toString());

    Assertions.assertFalse(missing.present());
    Assertions.assertFalse(missing.enabled());
    Assertions.assertFalse(missing.supportsWrapping());
    Assertions.assertFalse(missing.supportsUnwrapping());
  }

  private static void abortWhenFixtureIsNotConfigured(String image, String authToken) {
    List<String> missing = new ArrayList<>();
    if (StringUtils.isBlank(image)) {
      missing.add(ENV_LOCALSTACK_IMAGE);
    }
    if (StringUtils.isBlank(authToken)) {
      missing.add(ENV_LOCALSTACK_AUTH_TOKEN);
    }
    if (!missing.isEmpty()) {
      String message =
          "Skipping AWS KMS LocalStack IT; missing required environment variables: "
              + String.join(", ", missing);
      LOG.warn(message);
      Assumptions.assumeTrue(false, message);
    }
  }

  private static DockerImageName pinnedLocalStackImage(String image) {
    DockerImageName imageName = DockerImageName.parse(image);
    boolean officialProImage = "localstack/localstack-pro".equals(imageName.getUnversionedPart());
    boolean pinnedCalendarVersion = imageName.getVersionPart().matches("\\d{4}\\.\\d{2}\\.\\d+");
    if (!officialProImage || !pinnedCalendarVersion) {
      throw new IllegalArgumentException(
          String.format(
              "%s must use a pinned localstack/localstack-pro:YYYY.MM.patch image, not '%s'",
              ENV_LOCALSTACK_IMAGE, image));
    }
    return imageName.asCompatibleSubstituteFor("localstack/localstack");
  }

  private void createFixtures() {
    symmetricKey = createKey(KeySpec.SYMMETRIC_DEFAULT, KeyUsageType.ENCRYPT_DECRYPT);
    asymmetricKey = createKey(KeySpec.RSA_2048, KeyUsageType.ENCRYPT_DECRYPT);
    signingKey = createKey(KeySpec.RSA_2048, KeyUsageType.SIGN_VERIFY);
    disabledKey = createKey(KeySpec.SYMMETRIC_DEFAULT, KeyUsageType.ENCRYPT_DECRYPT);
    fixtureClient.disableKey(DisableKeyRequest.builder().keyId(disabledKey.keyId()).build());

    aliasName = "alias/gravitino-kms-it-" + UUID.randomUUID();
    fixtureClient.createAlias(
        CreateAliasRequest.builder()
            .aliasName(aliasName)
            .targetKeyId(symmetricKey.keyId())
            .build());
    aliasArn = symmetricKey.arn().substring(0, symmetricKey.arn().lastIndexOf(':') + 1) + aliasName;
  }

  private KeyMetadata createKey(KeySpec keySpec, KeyUsageType keyUsage) {
    return fixtureClient
        .createKey(
            CreateKeyRequest.builder()
                .description("Gravitino AWS KMS LocalStack integration fixture")
                .keySpec(keySpec)
                .keyUsage(keyUsage)
                .build())
        .keyMetadata();
  }

  private void installDefaultChainCredentials() {
    previousAccessKeyId = System.getProperty(AWS_ACCESS_KEY_ID_PROPERTY);
    previousSecretAccessKey = System.getProperty(AWS_SECRET_ACCESS_KEY_PROPERTY);
    System.setProperty(AWS_ACCESS_KEY_ID_PROPERTY, localStack.getAccessKey());
    System.setProperty(AWS_SECRET_ACCESS_KEY_PROPERTY, localStack.getSecretKey());
    installedSystemCredentials = true;
  }

  private void restoreDefaultChainCredentials() {
    if (!installedSystemCredentials) {
      return;
    }
    restoreSystemProperty(AWS_ACCESS_KEY_ID_PROPERTY, previousAccessKeyId);
    restoreSystemProperty(AWS_SECRET_ACCESS_KEY_PROPERTY, previousSecretAccessKey);
  }

  private static void restoreSystemProperty(String property, String previousValue) {
    if (previousValue == null) {
      System.clearProperty(property);
    } else {
      System.setProperty(property, previousValue);
    }
  }

  private KmsKeyProperties properties(String keyId) {
    return client.getKeyProperties(new KmsReference(KmsApi.AWS_KMS, SOURCE, keyId));
  }
}
