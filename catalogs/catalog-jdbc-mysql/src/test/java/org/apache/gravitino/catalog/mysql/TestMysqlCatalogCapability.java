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
package org.apache.gravitino.catalog.mysql;

import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMysqlCatalogCapability {

  private final MysqlCatalogCapability capability = new MysqlCatalogCapability();

  @Test
  void testValidNames() {
    // testing valid names for all scopes
    for (Capability.Scope scope : Capability.Scope.values()) {
      // normal alphanumeric names
      CapabilityResult result = capability.specificationOnName(scope, "test_table");
      Assertions.assertTrue(result.supported());

      result = capability.specificationOnName(scope, "TestTable123");
      Assertions.assertTrue(result.supported());

      result = capability.specificationOnName(scope, "table_with_underscores");
      Assertions.assertTrue(result.supported());

      // names with allowed special characters
      result = capability.specificationOnName(scope, "table-with-hyphens");
      Assertions.assertTrue(result.supported());

      result = capability.specificationOnName(scope, "table$with$dollar");
      Assertions.assertTrue(result.supported());

      result = capability.specificationOnName(scope, "table/with/slash");
      Assertions.assertTrue(result.supported());

      result = capability.specificationOnName(scope, "table=with=equals");
      Assertions.assertTrue(result.supported());

      // names with unicode letters
      result = capability.specificationOnName(scope, "测试表");
      Assertions.assertTrue(result.supported());

      result = capability.specificationOnName(scope, "tableção");
      Assertions.assertTrue(result.supported());

      // maximum length(64 chars)
      String maxLengthName = "a".repeat(64);
      result = capability.specificationOnName(scope, maxLengthName);
      Assertions.assertTrue(result.supported());
    }
  }

  @Test
  void testInvalidNames() {
    // Test invalid names for all scopes
    for (Capability.Scope scope : Capability.Scope.values()) {
      // empty name
      CapabilityResult result = capability.specificationOnName(scope, "");
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("illegal"));

      // name exceeding maximum length (65 characters)
      String tooLongName = "a".repeat(65);
      result = capability.specificationOnName(scope, tooLongName);
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("illegal"));

      // names with invalid characters
      result = capability.specificationOnName(scope, "table with space");
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("illegal"));

      result = capability.specificationOnName(scope, "table@with@at");
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("illegal"));

      result = capability.specificationOnName(scope, "table#with#hash");
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("illegal"));

      result = capability.specificationOnName(scope, "table%with%percent");
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("illegal"));

      result = capability.specificationOnName(scope, "table.with.dot");
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("illegal"));

      result = capability.specificationOnName(scope, "table(with)parentheses");
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("illegal"));

      result = capability.specificationOnName(scope, "table[with]brackets");
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("illegal"));

      result = capability.specificationOnName(scope, "table{with}braces");
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("illegal"));
    }
  }

  @Test
  void testReservedSchemaNames() {
    // Test reserved schema names are rejected
    CapabilityResult result = capability.specificationOnName(Capability.Scope.SCHEMA, "mysql");
    Assertions.assertFalse(result.supported());
    Assertions.assertTrue(result.unsupportedMessage().contains("reserved"));

    result = capability.specificationOnName(Capability.Scope.SCHEMA, "information_schema");
    Assertions.assertFalse(result.supported());
    Assertions.assertTrue(result.unsupportedMessage().contains("reserved"));

    result = capability.specificationOnName(Capability.Scope.SCHEMA, "performance_schema");
    Assertions.assertFalse(result.supported());
    Assertions.assertTrue(result.unsupportedMessage().contains("reserved"));

    result = capability.specificationOnName(Capability.Scope.SCHEMA, "sys");
    Assertions.assertFalse(result.supported());
    Assertions.assertTrue(result.unsupportedMessage().contains("reserved"));

    // case insensitive reserved names
    result = capability.specificationOnName(Capability.Scope.SCHEMA, "MYSQL");
    Assertions.assertFalse(result.supported());
    Assertions.assertTrue(result.unsupportedMessage().contains("reserved"));

    result = capability.specificationOnName(Capability.Scope.SCHEMA, "Information_Schema");
    Assertions.assertFalse(result.supported());
    Assertions.assertTrue(result.unsupportedMessage().contains("reserved"));
  }

  @Test
  void testReservedNamesNotAppliedToOtherScopes() {
    // Reserved names should not apply to column scope
    CapabilityResult result = capability.specificationOnName(Capability.Scope.COLUMN, "mysql");
    Assertions.assertTrue(result.supported());

    result = capability.specificationOnName(Capability.Scope.COLUMN, "information_schema");
    Assertions.assertTrue(result.supported());

    result = capability.specificationOnName(Capability.Scope.COLUMN, "sys");
    Assertions.assertTrue(result.supported());

    result = capability.specificationOnName(Capability.Scope.COLUMN, "performance_schema");
    Assertions.assertTrue(result.supported());

    // Reserved names should not apply to other scopes like FILESET, TOPIC, etc.
    result = capability.specificationOnName(Capability.Scope.FILESET, "mysql");
    Assertions.assertTrue(result.supported());

    result = capability.specificationOnName(Capability.Scope.TOPIC, "information_schema");
    Assertions.assertTrue(result.supported());
  }

  @Test
  void testBoundaryConditions() {
    // minimum length (1 character)
    CapabilityResult result = capability.specificationOnName(Capability.Scope.TABLE, "a");
    Assertions.assertTrue(result.supported());

    // exactly 64 characters (maximum allowed)
    String exactMaxName = "a".repeat(64);
    result = capability.specificationOnName(Capability.Scope.TABLE, exactMaxName);
    Assertions.assertTrue(result.supported());

    // exactly 65 characters (exceeds maximum)
    String exceedsMaxName = "a".repeat(65);
    result = capability.specificationOnName(Capability.Scope.TABLE, exceedsMaxName);
    Assertions.assertFalse(result.supported());
    Assertions.assertTrue(result.unsupportedMessage().contains("illegal"));
  }

  @Test
  void testMixedValidAndInvalidCharacters() {
    // names that start valid but contain invalid characters
    CapabilityResult result =
        capability.specificationOnName(Capability.Scope.TABLE, "valid_start invalid_space");
    Assertions.assertFalse(result.supported());

    result = capability.specificationOnName(Capability.Scope.TABLE, "valid_start@invalid");
    Assertions.assertFalse(result.supported());

    // names with mix of valid special characters
    result =
        capability.specificationOnName(Capability.Scope.TABLE, "test_table-with$mixed/chars=ok");
    Assertions.assertTrue(result.supported());
  }
}
