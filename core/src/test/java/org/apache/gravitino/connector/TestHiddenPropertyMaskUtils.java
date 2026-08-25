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
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestHiddenPropertyMaskUtils {

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
    // hidden credential (not reserved) → ******
    Assertions.assertEquals(HiddenPropertyMaskUtils.MASKED_VALUE, masked.get("jdbc-password"));
    // reserved + hidden → omitted from API response
    Assertions.assertFalse(masked.containsKey("gravitino.identifier"));
    Assertions.assertEquals("v", masked.get("visible"));
  }
}
