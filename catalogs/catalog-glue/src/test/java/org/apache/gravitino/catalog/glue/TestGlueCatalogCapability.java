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
package org.apache.gravitino.catalog.glue;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestGlueCatalogCapability {

  private GlueCatalogCapability capability;

  @BeforeEach
  void setUp() {
    capability = new GlueCatalogCapability();
  }

  @Test
  void testColumnNotNullIsUnsupported() {
    CapabilityResult result = capability.columnNotNull();
    assertFalse(result.supported(), "Glue does not enforce NOT NULL constraints");
    assertNotNull(result.unsupportedMessage());
    assertFalse(result.unsupportedMessage().isEmpty());
  }

  @Test
  void testColumnDefaultValueIsUnsupported() {
    CapabilityResult result = capability.columnDefaultValue();
    assertFalse(result.supported(), "Glue does not support DEFAULT values on columns");
    assertNotNull(result.unsupportedMessage());
    assertFalse(result.unsupportedMessage().isEmpty());
  }

  @Test
  void testCaseSensitiveOnSchemaIsUnsupported() {
    CapabilityResult result = capability.caseSensitiveOnName(Capability.Scope.SCHEMA);
    assertFalse(result.supported(), "Glue folds schema names to lowercase");
    assertNotNull(result.unsupportedMessage());
  }

  @Test
  void testCaseSensitiveOnTableIsUnsupported() {
    CapabilityResult result = capability.caseSensitiveOnName(Capability.Scope.TABLE);
    assertFalse(result.supported(), "Glue folds table names to lowercase");
    assertNotNull(result.unsupportedMessage());
  }

  @Test
  void testCaseSensitiveOnColumnIsSupported() {
    // Column name case folding is not documented in Glue Column API — treated as supported.
    CapabilityResult result = capability.caseSensitiveOnName(Capability.Scope.COLUMN);
    assertTrue(result.supported());
  }
}
