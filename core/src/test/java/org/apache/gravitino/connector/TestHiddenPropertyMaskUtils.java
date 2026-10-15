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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.secret.SecretPropertyUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestHiddenPropertyMaskUtils {

  @AfterEach
  void resetAdditionalMatcher() {
    SecretPropertyUtils.configureSensitiveKeyKeywords(new Config(false) {});
  }

  @Test
  void testMaskHiddenPropertiesByName() {
    Map<String, String> properties =
        ImmutableMap.of("visible", "v", "jdbc-password", "secret", "jdbc-user", "admin");
    Map<String, String> masked =
        HiddenPropertyMaskUtils.maskHiddenProperties(
            properties, ImmutableSet.of("jdbc-password", "jdbc-user"));

    Assertions.assertEquals("v", masked.get("visible"));
    Assertions.assertEquals(HiddenPropertyMaskUtils.MASKED_VALUE, masked.get("jdbc-password"));
    Assertions.assertEquals(HiddenPropertyMaskUtils.MASKED_VALUE, masked.get("jdbc-user"));
  }

  @Test
  void testMaskHiddenPropertiesSkipsNullEntries() {
    Map<String, String> properties = new java.util.HashMap<>();
    properties.put("visible", "v");
    properties.put("hidden", "secret");
    properties.put("nullValue", null);

    Map<String, String> masked =
        HiddenPropertyMaskUtils.maskHiddenProperties(properties, ImmutableSet.of("hidden"));

    Assertions.assertEquals(2, masked.size());
    Assertions.assertEquals("v", masked.get("visible"));
    Assertions.assertEquals(HiddenPropertyMaskUtils.MASKED_VALUE, masked.get("hidden"));
    Assertions.assertFalse(masked.containsKey("nullValue"));
  }

  @Test
  void testValidateNoMaskedPlaceholdersRejectsMaskedValue() {
    Map<String, String> properties =
        ImmutableMap.of("jdbc-password", HiddenPropertyMaskUtils.MASKED_VALUE, "comment", "new");
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> HiddenPropertyMaskUtils.validateNoMaskedPlaceholders(properties));
    Assertions.assertTrue(exception.getMessage().contains("jdbc-password"));
    Assertions.assertTrue(exception.getMessage().contains(HiddenPropertyMaskUtils.MASKED_VALUE));
  }

  @Test
  void testValidateNoMaskedPlaceholdersAllowsNormalValues() {
    HiddenPropertyMaskUtils.validateNoMaskedPlaceholders(
        ImmutableMap.of("jdbc-password", "secret", "comment", "new"));
  }

  @Test
  void testMaskHiddenPropertiesReturnsMutableEmptyMap() {
    Map<String, String> masked =
        HiddenPropertyMaskUtils.maskHiddenProperties(Map.of(), ImmutableSet.of());
    masked.put("in-use", "true");
    Assertions.assertEquals("true", masked.get("in-use"));
  }

  @Test
  void testMaskHiddenPropertiesOmitsReservedHiddenKeys() {
    PropertiesMetadata metadata =
        new PropertiesMetadata() {
          @Override
          public Map<String, PropertyEntry<?>> propertyEntries() {
            return ImmutableMap.of(
                "numFiles",
                PropertyEntry.stringReservedPropertyEntry("numFiles", "number of files", false),
                "jdbc-password",
                PropertyEntry.stringOptionalPropertyEntry(
                    "jdbc-password", "password", false, null, true),
                "gravitino.identifier",
                PropertyEntry.stringReservedPropertyEntry(
                    "gravitino.identifier", "system id", true));
          }
        };

    Map<String, String> properties =
        ImmutableMap.of(
            "numFiles",
            "10",
            "jdbc-password",
            "secret",
            "gravitino.identifier",
            "uid-1",
            "visible",
            "v");
    Map<String, String> masked = HiddenPropertyMaskUtils.maskHiddenProperties(properties, metadata);

    // reserved + not hidden → keep real value
    Assertions.assertEquals("10", masked.get("numFiles"));
    // hidden credential → ******
    Assertions.assertEquals(HiddenPropertyMaskUtils.MASKED_VALUE, masked.get("jdbc-password"));
    // reserved + hidden → omitted (not editable; mask would be useless)
    Assertions.assertFalse(masked.containsKey("gravitino.identifier"));
    Assertions.assertEquals("v", masked.get("visible"));
  }

  @Test
  void testMaskHiddenPropertiesMasksSensitiveNamedUndeclaredKeys() {
    PropertiesMetadata metadata =
        new PropertiesMetadata() {
          @Override
          public Map<String, PropertyEntry<?>> propertyEntries() {
            return ImmutableMap.of(
                "aws-region",
                PropertyEntry.stringOptionalPropertyEntry(
                    "aws-region", "region", false, null, false),
                "azure-storage-account-name",
                PropertyEntry.stringOptionalPropertyEntry(
                    "azure-storage-account-name", "account", false, null, false),
                "credential-providers",
                PropertyEntry.stringOptionalPropertyEntry(
                    "credential-providers", "providers", false, null, false));
          }
        };

    Map<String, String> properties =
        ImmutableMap.of(
            "aws-region",
            "us-east-2",
            "warehouse",
            "s3://bucket/prefix",
            "s3-access-key-id",
            "AKIA...",
            "s3-secret-access-key",
            "super-secret",
            "custom-token",
            "tok",
            "azure-storage-account-key",
            "abs-key",
            "azure-storage-account-name",
            "abs-account",
            "credential-providers",
            "s3-token");
    Map<String, String> masked = HiddenPropertyMaskUtils.maskHiddenProperties(properties, metadata);

    Assertions.assertEquals("us-east-2", masked.get("aws-region"));
    Assertions.assertEquals("s3://bucket/prefix", masked.get("warehouse"));
    Assertions.assertEquals("abs-account", masked.get("azure-storage-account-name"));
    Assertions.assertEquals("s3-token", masked.get("credential-providers"));
    Assertions.assertEquals(HiddenPropertyMaskUtils.MASKED_VALUE, masked.get("s3-access-key-id"));
    Assertions.assertEquals(
        HiddenPropertyMaskUtils.MASKED_VALUE, masked.get("s3-secret-access-key"));
    Assertions.assertEquals(HiddenPropertyMaskUtils.MASKED_VALUE, masked.get("custom-token"));
    Assertions.assertEquals(
        HiddenPropertyMaskUtils.MASKED_VALUE, masked.get("azure-storage-account-key"));
  }

  @Test
  void testMaskHiddenPropertiesMasksAdditionalSensitiveKey() {
    Config config = new Config(false) {};
    config.set(Configs.SENSITIVE_KEY_KEYWORDS, List.of("passwrod"));
    SecretPropertyUtils.configureSensitiveKeyKeywords(config);
    PropertiesMetadata metadata =
        new PropertiesMetadata() {
          @Override
          public Map<String, PropertyEntry<?>> propertyEntries() {
            return ImmutableMap.of(
                "jdbc-user",
                PropertyEntry.stringOptionalPropertyEntry("jdbc-user", "user", false, null, false));
          }
        };

    Map<String, String> properties =
        ImmutableMap.of("jdbc-user", "root", "jdbc-passwrod", "typo-secret");
    Map<String, String> masked = HiddenPropertyMaskUtils.maskHiddenProperties(properties, metadata);

    Assertions.assertEquals("root", masked.get("jdbc-user"));
    Assertions.assertEquals(HiddenPropertyMaskUtils.MASKED_VALUE, masked.get("jdbc-passwrod"));
  }

  @Test
  void testMaskSensitiveNamedPropertiesMasksKeywordKeysAndNestedJson() throws Exception {
    Map<String, String> properties = new java.util.LinkedHashMap<>();
    properties.put("gravitino_uri", "http://localhost:8090");
    properties.put("password", "plain-password");
    properties.put("oauth_token", "tok-123");
    properties.put("spark.hadoop.fs.s3a.secret.key", "s3-secret");
    properties.put(
        "updater_options",
        "{\"gravitino_uri\":\"http://localhost:8090\",\"password\":\"nested-pw\","
            + "\"oauth_credential\":\"cred\"}");
    properties.put("spark_conf", "{\"spark.master\":\"local[*]\",\"api_token\":\"nested-tok\"}");
    properties.put("not_json", "password=looks-like-but-not-a-key");

    Map<String, String> masked = HiddenPropertyMaskUtils.maskSensitiveNamedProperties(properties);

    Assertions.assertEquals("http://localhost:8090", masked.get("gravitino_uri"));
    Assertions.assertEquals(HiddenPropertyMaskUtils.MASKED_VALUE, masked.get("password"));
    Assertions.assertEquals(HiddenPropertyMaskUtils.MASKED_VALUE, masked.get("oauth_token"));
    Assertions.assertEquals(
        HiddenPropertyMaskUtils.MASKED_VALUE, masked.get("spark.hadoop.fs.s3a.secret.key"));
    Assertions.assertEquals("password=looks-like-but-not-a-key", masked.get("not_json"));

    Map<String, String> updaterOptions =
        JsonUtils.anyFieldMapper()
            .readValue(masked.get("updater_options"), new TypeReference<Map<String, String>>() {});
    Assertions.assertEquals("http://localhost:8090", updaterOptions.get("gravitino_uri"));
    Assertions.assertEquals(HiddenPropertyMaskUtils.MASKED_VALUE, updaterOptions.get("password"));
    Assertions.assertEquals(
        HiddenPropertyMaskUtils.MASKED_VALUE, updaterOptions.get("oauth_credential"));

    Map<String, String> sparkConf =
        JsonUtils.anyFieldMapper()
            .readValue(masked.get("spark_conf"), new TypeReference<Map<String, String>>() {});
    Assertions.assertEquals("local[*]", sparkConf.get("spark.master"));
    Assertions.assertEquals(HiddenPropertyMaskUtils.MASKED_VALUE, sparkConf.get("api_token"));
  }

  @Test
  void testMaskSensitiveNamedPropertiesReturnsEmptyForNull() {
    Map<String, String> masked = HiddenPropertyMaskUtils.maskSensitiveNamedProperties(null);
    Assertions.assertTrue(masked.isEmpty());
    masked.put("k", "v");
    Assertions.assertEquals("v", masked.get("k"));
  }
}
