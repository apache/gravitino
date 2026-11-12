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
package org.apache.gravitino.catalog;

import static org.apache.gravitino.TestBasePropertiesMetadata.TEST_REQUIRED_KEY;
import static org.apache.gravitino.catalog.PropertiesMetadataHelpers.validatePropertyForAlter;
import static org.apache.gravitino.catalog.PropertiesMetadataHelpers.validatePropertyForCreate;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.TestBasePropertiesMetadata;
import org.apache.gravitino.connector.HiddenPropertyMaskUtils;
import org.junit.jupiter.api.Test;

public class TestPropertiesMetadataHelpers {

  private static final TestBasePropertiesMetadata METADATA = new TestBasePropertiesMetadata();

  @Test
  void testCreateRejectsMaskedPlaceholder() {
    Map<String, String> props =
        ImmutableMap.of(TEST_REQUIRED_KEY, "value", "custom", HiddenPropertyMaskUtils.MASKED_VALUE);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> validatePropertyForCreate(METADATA, props));
    assertTrue(exception.getMessage().contains("custom"));
    assertTrue(exception.getMessage().contains(HiddenPropertyMaskUtils.MASKED_VALUE));
  }

  @Test
  void testCreateAllowsNormalValues() {
    Map<String, String> props = ImmutableMap.of(TEST_REQUIRED_KEY, "value", "custom", "secret");
    assertDoesNotThrow(() -> validatePropertyForCreate(METADATA, props));
  }

  @Test
  void testAlterRejectsMaskedPlaceholder() {
    Map<String, String> upserts = ImmutableMap.of("custom", HiddenPropertyMaskUtils.MASKED_VALUE);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validatePropertyForAlter(METADATA, upserts, Collections.emptyMap()));
    assertTrue(exception.getMessage().contains("custom"));
  }

  @Test
  void testAlterAllowsNormalUpserts() {
    Map<String, String> upserts = ImmutableMap.of("custom", "new-value");
    assertDoesNotThrow(() -> validatePropertyForAlter(METADATA, upserts, Collections.emptyMap()));
  }
}
