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
package org.apache.gravitino.connector;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRegisteredPropertyKeys {

  @Test
  void testCloudAndConnectorKeysAreDefined() {
    Assertions.assertTrue(RegisteredPropertyKeys.isRegistered("s3-access-key-id"));
    Assertions.assertTrue(RegisteredPropertyKeys.isRegistered("aws-access-key-id"));
    Assertions.assertTrue(RegisteredPropertyKeys.isRegistered("dlf-access-key-id"));
    Assertions.assertTrue(RegisteredPropertyKeys.isRegistered("credential-providers"));
    Assertions.assertTrue(RegisteredPropertyKeys.isRegistered("location-unknown"));
    Assertions.assertTrue(RegisteredPropertyKeys.isRegistered("location-warehouse"));
    Assertions.assertFalse(RegisteredPropertyKeys.isRegistered("typo-access-key"));
    Assertions.assertFalse(RegisteredPropertyKeys.isRegistered(null));
  }

  @Test
  void testHiddenMatchesOfficialSecretsNotIdentifiers() {
    Assertions.assertFalse(RegisteredPropertyKeys.isHidden("s3-access-key-id"));
    Assertions.assertFalse(RegisteredPropertyKeys.isHidden("aws-access-key-id"));
    Assertions.assertTrue(RegisteredPropertyKeys.isHidden("s3-secret-access-key"));
    Assertions.assertTrue(RegisteredPropertyKeys.isHidden("aws-secret-access-key"));
    Assertions.assertTrue(RegisteredPropertyKeys.isHidden("jdbc-password"));
    Assertions.assertTrue(RegisteredPropertyKeys.isHidden("location-unknown"));
    Assertions.assertTrue(RegisteredPropertyKeys.isHidden("presto_view"));
  }

  @Test
  void testReservedMatchesConnectorDefinitions() {
    Assertions.assertTrue(RegisteredPropertyKeys.isReserved("in-use"));
    Assertions.assertTrue(RegisteredPropertyKeys.isReserved("PartitionName"));
    Assertions.assertTrue(RegisteredPropertyKeys.isReserved("presto_view"));
    Assertions.assertFalse(RegisteredPropertyKeys.isReserved("s3-access-key-id"));
  }

  @Test
  void testSharedCloudOrCredentialKeys() {
    Assertions.assertTrue(RegisteredPropertyKeys.isSharedCloudOrCredentialKey("s3-access-key-id"));
    Assertions.assertTrue(
        RegisteredPropertyKeys.isSharedCloudOrCredentialKey("credential-providers"));
    Assertions.assertTrue(
        RegisteredPropertyKeys.isSharedCloudOrCredentialKey("adls-token-expire-in-secs"));
    Assertions.assertFalse(
        RegisteredPropertyKeys.isSharedCloudOrCredentialKey("aws-access-key-id"));
  }

  @Test
  void testConnectorSpecificPropertyMustBeRegistered() {
    BasePropertiesMetadata missingOfficial =
        new BasePropertiesMetadata() {
          @Override
          protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
            return ImmutableMap.of(
                "connector-only-unregistered-key",
                PropertyEntry.stringOptionalPropertyEntry(
                    "connector-only-unregistered-key", "test", false, null, false));
          }
        };

    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, missingOfficial::propertyEntries);
    Assertions.assertTrue(exception.getMessage().contains("connector-only-unregistered-key"));
    Assertions.assertTrue(
        exception.getMessage().contains("base properties")
            || exception.getMessage().contains("RegisteredPropertyKeys"));
  }

  @Test
  void testSharedCloudKeysInSpecificEntriesDoNotRequireConnectorRegistration() {
    BasePropertiesMetadata cloudOnly =
        new BasePropertiesMetadata() {
          @Override
          protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
            return ImmutableMap.of(
                "s3-access-key-id",
                PropertyEntry.stringOptionalPropertyEntry(
                    "s3-access-key-id", "ak", false, null, false));
          }
        };
    Assertions.assertDoesNotThrow(cloudOnly::propertyEntries);
    Assertions.assertTrue(cloudOnly.containsProperty("s3-access-key-id"));
  }
}
