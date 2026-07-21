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
package org.apache.gravitino.kms.gcp.integration.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.gravitino.encryption.kms.KmsApi;
import org.apache.gravitino.encryption.kms.KmsClient;
import org.apache.gravitino.encryption.kms.KmsKeyProperties;
import org.apache.gravitino.encryption.kms.KmsReference;
import org.apache.gravitino.kms.gcp.GoogleCloudKmsClientFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Read-only live tests for Google Cloud KMS metadata inspection through ADC. */
class TestGoogleCloudKmsClientIT {

  private static final Logger LOG = LoggerFactory.getLogger(TestGoogleCloudKmsClientIT.class);

  private static final String SOURCE = "gcp-kms-live-it";
  private static final String ENV_OPT_IN = "GRAVITINO_GCP_KMS_IT";
  private static final String ENV_ENABLED_KEY = "GRAVITINO_GCP_KMS_KEY_ID";
  private static final String ENV_MISSING_KEY = "GRAVITINO_GCP_KMS_MISSING_KEY_ID";
  private static final String ENV_DISABLED_KEY = "GRAVITINO_GCP_KMS_DISABLED_KEY_ID";
  private static final String ENV_NON_ENCRYPTION_KEY = "GRAVITINO_GCP_KMS_NON_ENCRYPTION_KEY_ID";
  private static final List<String> FIXTURE_VARIABLES =
      Arrays.asList(ENV_ENABLED_KEY, ENV_MISSING_KEY, ENV_DISABLED_KEY, ENV_NON_ENCRYPTION_KEY);
  private static final Pattern CRYPTO_KEY_NAME =
      Pattern.compile("projects/[^/]+/locations/[^/]+/keyRings/[^/]+/cryptoKeys/[^/]+");

  private static KmsClient client;
  private static String enabledKey;
  private static String missingKey;
  private static String disabledKey;
  private static String nonEncryptionKey;

  @BeforeAll
  static void createClientWhenExplicitlyEnabled() {
    List<String> missingVariables =
        FIXTURE_VARIABLES.stream()
            .filter(variable -> isBlank(System.getenv(variable)))
            .collect(Collectors.toList());
    String optIn = System.getenv(ENV_OPT_IN);
    boolean enabled = optIn != null && "true".equalsIgnoreCase(optIn.trim());

    if (!enabled) {
      LOG.warn(
          "Skipping Google Cloud KMS live integration tests because {} is not true. "
              + "Missing fixture variables: {}",
          ENV_OPT_IN,
          missingVariables.isEmpty() ? "(none)" : String.join(", ", missingVariables));
      assumeTrue(false, "Google Cloud KMS live integration tests require explicit opt-in");
    }

    assertTrue(
        missingVariables.isEmpty(),
        () ->
            "Google Cloud KMS live integration tests are enabled but required fixture variables "
                + "are missing: "
                + String.join(", ", missingVariables));

    enabledKey = System.getenv(ENV_ENABLED_KEY);
    missingKey = System.getenv(ENV_MISSING_KEY);
    disabledKey = System.getenv(ENV_DISABLED_KEY);
    nonEncryptionKey = System.getenv(ENV_NON_ENCRYPTION_KEY);

    Stream.of(enabledKey, missingKey, disabledKey, nonEncryptionKey)
        .forEach(
            keyId ->
                assertTrue(
                    CRYPTO_KEY_NAME.matcher(keyId).matches(),
                    () ->
                        "Fixture is not a full Google Cloud KMS CryptoKey resource name: "
                            + keyId));
    assertEquals(
        FIXTURE_VARIABLES.size(),
        Stream.of(enabledKey, missingKey, disabledKey, nonEncryptionKey).distinct().count(),
        "Google Cloud KMS live integration fixtures must use distinct CryptoKeys");

    String[] keySegments = enabledKey.split("/");
    client =
        new GoogleCloudKmsClientFactory()
            .create(
                SOURCE,
                Map.of(
                    GoogleCloudKmsClientFactory.PROJECT_ID,
                    keySegments[1],
                    GoogleCloudKmsClientFactory.LOCATION,
                    keySegments[3],
                    GoogleCloudKmsClientFactory.CREDENTIAL_METHOD,
                    "default"));
  }

  @AfterAll
  static void closeClient() {
    if (client != null) {
      client.close();
    }
  }

  @Test
  void readsEnabledEncryptionKey() {
    KmsKeyProperties properties = client.getKeyProperties(reference(enabledKey));

    assertEquals(reference(enabledKey), properties.reference());
    assertTrue(properties.present());
    assertTrue(properties.enabled());
    assertTrue(properties.supportsWrapping());
    assertTrue(properties.supportsUnwrapping());
  }

  @Test
  void reportsMissingKey() {
    KmsKeyProperties properties = client.getKeyProperties(reference(missingKey));

    assertEquals(reference(missingKey), properties.reference());
    assertFalse(properties.present());
    assertFalse(properties.enabled());
    assertFalse(properties.supportsWrapping());
    assertFalse(properties.supportsUnwrapping());
  }

  @Test
  void readsDisabledEncryptionKey() {
    KmsKeyProperties properties = client.getKeyProperties(reference(disabledKey));

    assertEquals(reference(disabledKey), properties.reference());
    assertTrue(properties.present());
    assertFalse(properties.enabled());
    assertTrue(properties.supportsWrapping());
    assertTrue(properties.supportsUnwrapping());
  }

  @Test
  void readsEnabledNonEncryptionKey() {
    KmsKeyProperties properties = client.getKeyProperties(reference(nonEncryptionKey));

    assertEquals(reference(nonEncryptionKey), properties.reference());
    assertTrue(properties.present());
    assertTrue(properties.enabled());
    assertFalse(properties.supportsWrapping());
    assertFalse(properties.supportsUnwrapping());
  }

  private static KmsReference reference(String keyId) {
    return new KmsReference(KmsApi.GOOGLE_CLOUD_KMS, SOURCE, keyId);
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }
}
