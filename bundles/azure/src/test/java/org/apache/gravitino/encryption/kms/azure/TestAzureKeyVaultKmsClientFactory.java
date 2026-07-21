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
package org.apache.gravitino.encryption.kms.azure;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.gravitino.encryption.kms.KmsApi;
import org.apache.gravitino.encryption.kms.KmsClient;
import org.apache.gravitino.encryption.kms.KmsClientFactory;
import org.apache.gravitino.encryption.kms.TestKmsClientFactoryContract;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestAzureKeyVaultKmsClientFactory extends TestKmsClientFactoryContract {

  private static final String VAULT_URL = "https://example.vault.azure.net";

  private final AzureKeyVaultKmsClientFactory factory = new AzureKeyVaultKmsClientFactory();

  @Override
  protected KmsClientFactory factory() {
    return factory;
  }

  @Override
  protected KmsApi expectedApi() {
    return KmsApi.AZURE_KEY_VAULT;
  }

  @Test
  void testCreatesClientWithDefaultCredentialChain() {
    KmsClient client =
        factory.create(
            "primary",
            ImmutableMap.of(
                AzureKeyVaultKmsClientFactory.VAULT_URL,
                VAULT_URL,
                AzureKeyVaultKmsClientFactory.CREDENTIAL_METHOD,
                "default"));

    Assertions.assertInstanceOf(AzureKeyVaultKmsClient.class, client);
  }

  @Test
  void testCreatesClientForUserAssignedManagedIdentity() {
    KmsClient client =
        factory.create(
            "primary",
            ImmutableMap.of(
                AzureKeyVaultKmsClientFactory.VAULT_URL,
                VAULT_URL + "/",
                AzureKeyVaultKmsClientFactory.CREDENTIAL_METHOD,
                "default",
                AzureKeyVaultKmsClientFactory.MANAGED_IDENTITY_CLIENT_ID,
                "client-id"));

    Assertions.assertInstanceOf(AzureKeyVaultKmsClient.class, client);
  }

  @Test
  void testLoadsFactoryThroughServiceLoader() {
    int matchingFactories = 0;
    for (KmsClientFactory loadedFactory : ServiceLoader.load(KmsClientFactory.class)) {
      if (loadedFactory instanceof AzureKeyVaultKmsClientFactory) {
        matchingFactories++;
      }
    }

    Assertions.assertEquals(1, matchingFactories);
  }

  @Test
  void testRejectsMissingAndUnsupportedProperties() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> factory.create("primary", null));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> factory.create("primary", ImmutableMap.of()));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            factory.create(
                "primary", ImmutableMap.of(AzureKeyVaultKmsClientFactory.VAULT_URL, " ")));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            factory.create(
                "primary",
                ImmutableMap.of(
                    AzureKeyVaultKmsClientFactory.VAULT_URL,
                    VAULT_URL,
                    "clientSecret",
                    "must-not-be-accepted")));

    Map<String, String> properties = new HashMap<>();
    properties.put(AzureKeyVaultKmsClientFactory.VAULT_URL, VAULT_URL);
    properties.put(AzureKeyVaultKmsClientFactory.CREDENTIAL_METHOD, "default");
    properties.put(null, "value");
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> factory.create("primary", properties));
  }

  @Test
  void testRejectsInvalidSourcesAndManagedIdentity() {
    Map<String, String> properties =
        ImmutableMap.of(
            AzureKeyVaultKmsClientFactory.VAULT_URL,
            VAULT_URL,
            AzureKeyVaultKmsClientFactory.CREDENTIAL_METHOD,
            "default");

    Assertions.assertThrows(IllegalArgumentException.class, () -> factory.create(null, properties));
    Assertions.assertThrows(IllegalArgumentException.class, () -> factory.create(" ", properties));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            factory.create(
                "primary",
                ImmutableMap.of(
                    AzureKeyVaultKmsClientFactory.VAULT_URL,
                    VAULT_URL,
                    AzureKeyVaultKmsClientFactory.CREDENTIAL_METHOD,
                    "default",
                    AzureKeyVaultKmsClientFactory.MANAGED_IDENTITY_CLIENT_ID,
                    " ")));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "example.vault.azure.net",
        "http://example.vault.azure.net",
        "https://",
        "https://user@example.vault.azure.net",
        "https://example.vault.azure.net:443",
        "https://example.vault.azure.net/keys",
        "https://example.vault.azure.net?tenant=example",
        "https://example.vault.azure.net#fragment"
      })
  void testRejectsInvalidServiceAddresses(String serviceAddress) {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            factory.create(
                "primary",
                ImmutableMap.of(AzureKeyVaultKmsClientFactory.VAULT_URL, serviceAddress)));
  }
}
