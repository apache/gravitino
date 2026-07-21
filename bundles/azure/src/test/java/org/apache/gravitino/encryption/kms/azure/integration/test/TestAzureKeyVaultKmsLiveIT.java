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
package org.apache.gravitino.encryption.kms.azure.integration.test;

import com.google.common.collect.ImmutableList;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.encryption.kms.KmsApi;
import org.apache.gravitino.encryption.kms.KmsClient;
import org.apache.gravitino.encryption.kms.KmsKeyProperties;
import org.apache.gravitino.encryption.kms.KmsReference;
import org.apache.gravitino.encryption.kms.azure.AzureKeyVaultKmsClientFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Read-only integration tests against an explicitly configured live Azure Key Vault fixture. */
class TestAzureKeyVaultKmsLiveIT {

  private static final Logger LOG = LoggerFactory.getLogger(TestAzureKeyVaultKmsLiveIT.class);
  private static final String SOURCE = "azure-live-it";
  private static final String VAULT_URL = "endpoint.vaultUrl";
  private static final String CREDENTIAL_METHOD = "credential.method";
  private static final String MANAGED_IDENTITY_CLIENT_ID = "credential.managedIdentityClientId";
  private static final String ENV_OPT_IN = "GRAVITINO_AZURE_KMS_IT";
  private static final String ENV_VAULT_URL = "GRAVITINO_AZURE_KMS_VAULT_URL";
  private static final String ENV_KEY_ID = "GRAVITINO_AZURE_KMS_KEY_ID";
  private static final String ENV_MANAGED_IDENTITY_CLIENT_ID =
      "GRAVITINO_AZURE_KMS_MANAGED_IDENTITY_CLIENT_ID";
  private static final String ABSENT_KEY_NAME =
      "gravitino-kms-it-absent-8bb16e12832d4dd9a6e33dc621647c31";
  private static final List<String> REQUIRED_FIXTURE_VARIABLES =
      ImmutableList.of(ENV_VAULT_URL, ENV_KEY_ID);

  @Nullable private static KmsClient client;
  private static String pinnedKeyId = "";
  private static String latestKeyId = "";
  private static String absentKeyId = "";

  @BeforeAll
  static void setUpFixture() {
    Map<String, String> environment = System.getenv();
    List<String> missingVariables = missingFixtureVariables(environment);
    if (!"true".equalsIgnoreCase(StringUtils.trimToEmpty(environment.get(ENV_OPT_IN)))) {
      LOG.warn(
          "Skipping Azure Key Vault KMS live integration test because {} is not true. Missing fixture variables: {}.",
          ENV_OPT_IN,
          missingVariables.isEmpty() ? "none" : String.join(", ", missingVariables));
      Assumptions.assumeTrue(false, ENV_OPT_IN + " must be true to run the live test");
    }

    Assertions.assertTrue(
        missingVariables.isEmpty(),
        "Azure Key Vault KMS live integration test opted in but required fixture variables are missing: "
            + String.join(", ", missingVariables));

    String vaultUrl = environment.get(ENV_VAULT_URL).trim();
    pinnedKeyId = environment.get(ENV_KEY_ID).trim();
    String keyName = versionedKeyName(pinnedKeyId);
    String normalizedVaultUrl = StringUtils.removeEnd(vaultUrl, "/");
    latestKeyId = normalizedVaultUrl + "/keys/" + keyName;
    absentKeyId = normalizedVaultUrl + "/keys/" + ABSENT_KEY_NAME;

    Map<String, String> properties = new HashMap<>();
    properties.put(VAULT_URL, vaultUrl);
    properties.put(CREDENTIAL_METHOD, "default");
    String managedIdentityClientId = environment.get(ENV_MANAGED_IDENTITY_CLIENT_ID);
    if (StringUtils.isNotBlank(managedIdentityClientId)) {
      properties.put(MANAGED_IDENTITY_CLIENT_ID, managedIdentityClientId.trim());
    }
    client = new AzureKeyVaultKmsClientFactory().create(SOURCE, properties);
  }

  @AfterAll
  static void closeClient() {
    if (client != null) {
      client.close();
    }
  }

  @Test
  void testReadsPinnedEnabledWrappingKey() {
    assertUsable(inspect(pinnedKeyId), pinnedKeyId);
  }

  @Test
  void testReadsLatestEnabledWrappingKey() {
    assertUsable(inspect(latestKeyId), latestKeyId);
  }

  @Test
  void testReportsKnownAbsentKey() {
    KmsKeyProperties properties = inspect(absentKeyId);

    Assertions.assertEquals(reference(absentKeyId), properties.reference());
    Assertions.assertFalse(properties.present());
    Assertions.assertFalse(properties.enabled());
    Assertions.assertFalse(properties.supportsWrapping());
    Assertions.assertFalse(properties.supportsUnwrapping());
  }

  private static List<String> missingFixtureVariables(Map<String, String> environment) {
    List<String> missingVariables = new ArrayList<>();
    for (String variable : REQUIRED_FIXTURE_VARIABLES) {
      if (StringUtils.isBlank(environment.get(variable))) {
        missingVariables.add(variable);
      }
    }
    return missingVariables;
  }

  private static String versionedKeyName(String keyId) {
    URI uri;
    try {
      uri = new URI(keyId);
    } catch (URISyntaxException e) {
      throw new AssertionError(ENV_KEY_ID + " must be a valid URI", e);
    }

    String[] pathSegments = uri.getRawPath().split("/", -1);
    Assertions.assertTrue(
        pathSegments.length == 4
            && pathSegments[0].isEmpty()
            && "keys".equals(pathSegments[1])
            && StringUtils.isNoneBlank(pathSegments[2], pathSegments[3]),
        ENV_KEY_ID + " must be a full, versioned key URL ending in /keys/<name>/<version>");
    return pathSegments[2];
  }

  private static KmsKeyProperties inspect(String keyId) {
    Assertions.assertNotNull(client);
    return client.getKeyProperties(reference(keyId));
  }

  private static KmsReference reference(String keyId) {
    return new KmsReference(KmsApi.AZURE_KEY_VAULT, SOURCE, keyId);
  }

  private static void assertUsable(KmsKeyProperties properties, String keyId) {
    Assertions.assertEquals(reference(keyId), properties.reference());
    Assertions.assertTrue(properties.present());
    Assertions.assertTrue(properties.enabled());
    Assertions.assertTrue(properties.supportsWrapping());
    Assertions.assertTrue(properties.supportsUnwrapping());
  }
}
