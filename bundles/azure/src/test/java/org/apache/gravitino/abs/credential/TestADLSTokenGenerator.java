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
package org.apache.gravitino.abs.credential;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.azure.storage.file.datalake.models.UserDelegationKey;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.gravitino.credential.ADLSTokenCredential;
import org.apache.gravitino.credential.PathBasedCredentialContext;
import org.apache.gravitino.storage.AzureProperties;
import org.junit.jupiter.api.Test;

class TestADLSTokenGenerator {

  @Test
  void generatesTokenForParentDirectoryOfMultiplePaths() throws Exception {
    try (ADLSTokenGenerator generator = new TestableADLSTokenGenerator()) {
      generator.initialize(createProperties());
      PathBasedCredentialContext context =
          new PathBasedCredentialContext(
              "test-user",
              Collections.emptySet(),
              new HashSet<>(
                  Arrays.asList(
                      "abfss://container@account.dfs.core.windows.net/warehouse/table",
                      "abfss://container@account.dfs.core.windows.net/warehouse/table/data",
                      "abfss://container@account.dfs.core.windows.net/warehouse/table/metadata")));

      ADLSTokenCredential credential = generator.generate(context);

      assertEquals("account", credential.accountName());
      assertTrue(credential.sasToken().contains("sr=d"));
      assertTrue(credential.sasToken().contains("sdd=2"));
    }
  }

  @Test
  void treatsEquivalentLocationsAsOneRoot() throws Exception {
    try (ADLSTokenGenerator generator = new TestableADLSTokenGenerator()) {
      generator.initialize(createProperties());
      PathBasedCredentialContext context =
          new PathBasedCredentialContext(
              "test-user",
              Collections.emptySet(),
              new HashSet<>(
                  Arrays.asList(
                      "abfss://container@account.dfs.core.windows.net/warehouse/table",
                      "abfs://container@Account.dfs.core.windows.net/warehouse/table/",
                      "abfss://container@account.dfs.core.windows.net/warehouse/table/data")));

      ADLSTokenCredential credential = generator.generate(context);

      assertTrue(credential.sasToken().contains("sr=d"));
      assertTrue(credential.sasToken().contains("sdd=2"));
    }
  }

  @Test
  void rejectsPathsWithoutProvidedCommonRoot() throws Exception {
    String first = "abfss://container@account.dfs.core.windows.net/warehouse/table/data";
    String second = "abfss://container@account.dfs.core.windows.net/warehouse/table/metadata";
    try (ADLSTokenGenerator generator = new TestableADLSTokenGenerator()) {
      generator.initialize(createProperties());
      PathBasedCredentialContext context =
          new PathBasedCredentialContext(
              "test-user", Collections.emptySet(), new HashSet<>(Arrays.asList(first, second)));

      IllegalArgumentException exception =
          assertThrows(IllegalArgumentException.class, () -> generator.generate(context));

      assertTrue(exception.getMessage().contains("exactly one root location"));
      assertTrue(exception.getMessage().contains(first));
      assertTrue(exception.getMessage().contains(second));
    }
  }

  @Test
  void rejectsSiblingPathsWithCommonStringPrefix() throws Exception {
    String first = "abfss://container@account.dfs.core.windows.net/warehouse/table";
    String second = "abfss://container@account.dfs.core.windows.net/warehouse/table2";
    try (ADLSTokenGenerator generator = new TestableADLSTokenGenerator()) {
      generator.initialize(createProperties());
      PathBasedCredentialContext context =
          new PathBasedCredentialContext(
              "test-user", Collections.emptySet(), new HashSet<>(Arrays.asList(first, second)));

      IllegalArgumentException exception =
          assertThrows(IllegalArgumentException.class, () -> generator.generate(context));

      assertTrue(exception.getMessage().contains("exactly one root location"));
    }
  }

  @Test
  void rejectsPathsFromDifferentContainers() throws Exception {
    String first = "abfss://first@account.dfs.core.windows.net/warehouse/table";
    String second = "abfss://second@account.dfs.core.windows.net/warehouse/table/data";
    try (ADLSTokenGenerator generator = new TestableADLSTokenGenerator()) {
      generator.initialize(createProperties());
      PathBasedCredentialContext context =
          new PathBasedCredentialContext(
              "test-user", Collections.emptySet(), new HashSet<>(Arrays.asList(first, second)));

      IllegalArgumentException exception =
          assertThrows(IllegalArgumentException.class, () -> generator.generate(context));

      assertTrue(exception.getMessage().contains(first));
      assertTrue(exception.getMessage().contains(second));
    }
  }

  @Test
  void rejectsPathsFromDifferentStorageAccounts() throws Exception {
    String first = "abfss://container@account.dfs.core.windows.net/warehouse/table";
    String second = "abfss://container@other.dfs.core.windows.net/warehouse/table/data";
    try (ADLSTokenGenerator generator = new TestableADLSTokenGenerator()) {
      generator.initialize(createProperties());
      PathBasedCredentialContext context =
          new PathBasedCredentialContext(
              "test-user", Collections.emptySet(), new HashSet<>(Arrays.asList(first, second)));

      IllegalArgumentException exception =
          assertThrows(IllegalArgumentException.class, () -> generator.generate(context));

      assertTrue(exception.getMessage().contains(first));
      assertTrue(exception.getMessage().contains(second));
    }
  }

  @Test
  void rejectsSinglePathFromUnconfiguredStorageAccount() throws Exception {
    String path = "abfss://container@other.dfs.core.windows.net/warehouse/table";
    try (ADLSTokenGenerator generator = new TestableADLSTokenGenerator()) {
      generator.initialize(createProperties());
      PathBasedCredentialContext context =
          new PathBasedCredentialContext(
              "test-user", Collections.emptySet(), Collections.singleton(path));

      IllegalArgumentException exception =
          assertThrows(IllegalArgumentException.class, () -> generator.generate(context));

      assertTrue(exception.getMessage().contains(path));
    }
  }

  @Test
  void rejectsBlobHostLocationsBeforeVendingSas() throws Exception {
    String path = "wasbs://container@account.blob.core.windows.net/warehouse/table";
    try (ADLSTokenGenerator generator = new TestableADLSTokenGenerator()) {
      generator.initialize(createProperties());
      PathBasedCredentialContext context =
          new PathBasedCredentialContext(
              "test-user", Collections.emptySet(), Collections.singleton(path));

      IllegalArgumentException exception =
          assertThrows(IllegalArgumentException.class, () -> generator.generate(context));

      assertTrue(exception.getMessage().contains("abfs or abfss"));
      assertTrue(exception.getMessage().contains(path));
    }
  }

  @Test
  void providerPreservesUnsupportedLocationError() throws Exception {
    String path = "wasbs://container@account.blob.core.windows.net/warehouse/table";
    try (ADLSTokenProvider provider = new ADLSTokenProvider()) {
      provider.initialize(createProperties());
      PathBasedCredentialContext context =
          new PathBasedCredentialContext(
              "test-user", Collections.emptySet(), Collections.singleton(path));

      IllegalArgumentException exception =
          assertThrows(IllegalArgumentException.class, () -> provider.getCredential(context));

      assertTrue(exception.getMessage().contains("abfs or abfss"));
      assertTrue(exception.getMessage().contains(path));
    }
  }

  private static Map<String, String> createProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME, "account");
    properties.put(AzureProperties.GRAVITINO_AZURE_TENANT_ID, "tenant");
    properties.put(AzureProperties.GRAVITINO_AZURE_CLIENT_ID, "client");
    properties.put(AzureProperties.GRAVITINO_AZURE_CLIENT_SECRET, "secret");
    return properties;
  }

  private static class TestableADLSTokenGenerator extends ADLSTokenGenerator {
    @Override
    UserDelegationKey getUserDelegationKey(OffsetDateTime start, OffsetDateTime expiry) {
      return new UserDelegationKey()
          .setSignedObjectId("object")
          .setSignedTenantId("tenant")
          .setSignedStart(start)
          .setSignedExpiry(expiry)
          .setSignedService("b")
          .setSignedVersion("2020-02-10")
          .setValue(Base64.getEncoder().encodeToString(new byte[32]));
    }
  }
}
