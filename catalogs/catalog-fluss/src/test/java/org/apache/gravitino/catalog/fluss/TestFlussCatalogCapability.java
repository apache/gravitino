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

package org.apache.gravitino.catalog.fluss;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.fluss.metadata.TableDescriptor;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link FlussCatalogCapability}. */
class TestFlussCatalogCapability {

  private final FlussCatalogCapability capability = new FlussCatalogCapability();

  @Test
  void testDatabaseAndTableNamesUseFlussTablePathRules() {
    assertNameSupported(Capability.Scope.SCHEMA, "db_01");
    assertNameSupported(Capability.Scope.TABLE, "Table-01");

    assertNameUnsupported(Capability.Scope.SCHEMA, null);
    assertNameUnsupported(Capability.Scope.SCHEMA, "");
    assertNameUnsupported(Capability.Scope.TABLE, ".");
    assertNameUnsupported(Capability.Scope.TABLE, "..");
    assertNameUnsupported(Capability.Scope.TABLE, "table.name");
    assertNameUnsupported(Capability.Scope.TABLE, "table/name");
    assertNameUnsupported(Capability.Scope.TABLE, "table$name");
    assertNameUnsupported(Capability.Scope.TABLE, "__system");
    assertNameUnsupported(Capability.Scope.TABLE, "a".repeat(201));
  }

  @Test
  void testColumnNameRulesFollowFlussSchemaAndSystemColumns() {
    assertNameSupported(Capability.Scope.COLUMN, "column");
    assertNameSupported(Capability.Scope.COLUMN, "column.with.characters");
    assertNameSupported(Capability.Scope.COLUMN, "__user_column");
    assertNameSupported(Capability.Scope.COLUMN, "a".repeat(201));

    assertNameUnsupported(Capability.Scope.COLUMN, null);
    assertNameUnsupported(Capability.Scope.COLUMN, "");
    assertNameUnsupported(Capability.Scope.COLUMN, "   ");
    assertNameUnsupported(Capability.Scope.COLUMN, TableDescriptor.OFFSET_COLUMN_NAME);
    assertNameUnsupported(Capability.Scope.COLUMN, TableDescriptor.TIMESTAMP_COLUMN_NAME);
    assertNameUnsupported(Capability.Scope.COLUMN, TableDescriptor.BUCKET_COLUMN_NAME);
    assertNameUnsupported(Capability.Scope.COLUMN, TableDescriptor.CHANGE_TYPE_COLUMN);
    assertNameUnsupported(Capability.Scope.COLUMN, TableDescriptor.LOG_OFFSET_COLUMN);
    assertNameUnsupported(Capability.Scope.COLUMN, TableDescriptor.COMMIT_TIMESTAMP_COLUMN);
  }

  @Test
  void testColumnNullabilityAndDefaults() {
    assertTrue(capability.columnNotNull().supported());
    assertFalse(capability.columnDefaultValue().supported());
  }

  private void assertNameSupported(Capability.Scope scope, String name) {
    CapabilityResult result = capability.specificationOnName(scope, name);
    assertTrue(result.supported(), result.unsupportedMessage());
  }

  private void assertNameUnsupported(Capability.Scope scope, String name) {
    assertFalse(capability.specificationOnName(scope, name).supported());
  }
}
