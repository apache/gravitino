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
package org.apache.gravitino.catalog.postgresql;

import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPostgreSqlCatalogCapability {

  private final PostgreSqlCatalogCapability capability = new PostgreSqlCatalogCapability();

  @Test
  void testValidNames() {
    // testing valid names for all scopes
    for (Capability.Scope scope : Capability.Scope.values()) {
      // normal alphanumeric names should work
      CapabilityResult result = capability.specificationOnName(scope, "test_table");
      Assertions.assertTrue(result.supported());

      result = capability.specificationOnName(scope, "TestTable123");
      Assertions.assertTrue(result.supported());

      result = capability.specificationOnName(scope, "table_with_underscores");
      Assertions.assertTrue(result.supported());

      // names starting with underscore are allowed
      result = capability.specificationOnName(scope, "_test_table");
      Assertions.assertTrue(result.supported());

      // names with unicode letters should be supported
      result = capability.specificationOnName(scope, "测试表");
      Assertions.assertTrue(result.supported());

      result = capability.specificationOnName(scope, "tableção");
      Assertions.assertTrue(result.supported());

      // maximum length (63 characters for PostgreSQL)
      String maxLengthName = "a".repeat(63);
      result = capability.specificationOnName(scope, maxLengthName);
      Assertions.assertTrue(result.supported());
    }
  }

  @Test
  void testInvalidNames() {
    // Test invalid names for all scopes
    for (Capability.Scope scope : Capability.Scope.values()) {
      // empty name should be rejected
      CapabilityResult result = capability.specificationOnName(scope, "");
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("illegal"));

      // name exceeding maximum length (64 characters)
      String tooLongName = "a".repeat(64);
      result = capability.specificationOnName(scope, tooLongName);
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("illegal"));

      // names starting with digits should be rejected
      result = capability.specificationOnName(scope, "123table");
      Assertions.assertFalse(result.supported());
      Assertions.assertTrue(result.unsupportedMessage().contains("illegal"));

      // names with invalid characters should be rejected
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
    }
  }

  @Test
  void testValidSpecialCharacters() {
    for (Capability.Scope scope : Capability.Scope.values()) {
      // hyphens should be allowed (as per the regex pattern)
      CapabilityResult result = capability.specificationOnName(scope, "table-with-hyphen");
      Assertions.assertTrue(result.supported());

      // dollar signs should be allowed
      result = capability.specificationOnName(scope, "table$with$dollar");
      Assertions.assertTrue(result.supported());

      // slashes should be allowed
      result = capability.specificationOnName(scope, "table/with/slash");
      Assertions.assertTrue(result.supported());

      // equals signs should be allowed
      result = capability.specificationOnName(scope, "table=with=equals");
      Assertions.assertTrue(result.supported());
    }
  }

  @Test
  void testReservedSchemaNames() {
    CapabilityResult result = capability.specificationOnName(Capability.Scope.SCHEMA, "pg_catalog");
    Assertions.assertFalse(result.supported());
    Assertions.assertTrue(result.unsupportedMessage().contains("reserved"));

    result = capability.specificationOnName(Capability.Scope.SCHEMA, "information_schema");
    Assertions.assertFalse(result.supported());
    Assertions.assertTrue(result.unsupportedMessage().contains("reserved"));

    // case insensitive reserved names should be rejected
    result = capability.specificationOnName(Capability.Scope.SCHEMA, "PG_CATALOG");
    Assertions.assertFalse(result.supported());
    Assertions.assertTrue(result.unsupportedMessage().contains("reserved"));

    result = capability.specificationOnName(Capability.Scope.SCHEMA, "Information_Schema");
    Assertions.assertFalse(result.supported());
    Assertions.assertTrue(result.unsupportedMessage().contains("reserved"));
  }

  @Test
  void testReservedNamesNotAppliedToOtherScopes() {
    // Reserved names should not apply to column scope
    CapabilityResult result = capability.specificationOnName(Capability.Scope.COLUMN, "pg_catalog");
    Assertions.assertTrue(result.supported());

    result = capability.specificationOnName(Capability.Scope.COLUMN, "information_schema");
    Assertions.assertTrue(result.supported());

    // Reserved names should not apply to other scopes like FILESET, TOPIC, etc.
    result = capability.specificationOnName(Capability.Scope.FILESET, "pg_catalog");
    Assertions.assertTrue(result.supported());

    result = capability.specificationOnName(Capability.Scope.TOPIC, "information_schema");
    Assertions.assertTrue(result.supported());
  }

  @Test
  void testBoundaryConditions() {
    // minimum length (1 character) should work
    CapabilityResult result = capability.specificationOnName(Capability.Scope.TABLE, "a");
    Assertions.assertTrue(result.supported());

    // exactly 63 characters (maximum allowed for PostgreSQL)
    String exactMaxName = "a".repeat(63);
    result = capability.specificationOnName(Capability.Scope.TABLE, exactMaxName);
    Assertions.assertTrue(result.supported());

    // exactly 64 characters (exceeds maximum)
    String exceedsMaxName = "a".repeat(64);
    result = capability.specificationOnName(Capability.Scope.TABLE, exceedsMaxName);
    Assertions.assertFalse(result.supported());
    Assertions.assertTrue(result.unsupportedMessage().contains("illegal"));
  }

  @Test
  void testMixedValidAndInvalidCharacters() {
    // names that start valid but contain invalid characters should be rejected
    CapabilityResult result =
        capability.specificationOnName(Capability.Scope.TABLE, "valid_start invalid_space");
    Assertions.assertFalse(result.supported());

    result = capability.specificationOnName(Capability.Scope.TABLE, "valid_start@invalid");
    Assertions.assertFalse(result.supported());

    // names with only valid characters should be accepted
    result = capability.specificationOnName(Capability.Scope.TABLE, "valid_table_name_123");
    Assertions.assertTrue(result.supported());
  }

  @Test
  void testUnicodeSupport() {
    // Test various Unicode letters as starting characters
    CapabilityResult result = capability.specificationOnName(Capability.Scope.TABLE, "αβγ_table");
    Assertions.assertTrue(result.supported());

    result = capability.specificationOnName(Capability.Scope.TABLE, "测试_table");
    Assertions.assertTrue(result.supported());

    result = capability.specificationOnName(Capability.Scope.TABLE, "أحمد_table");
    Assertions.assertTrue(result.supported());

    result = capability.specificationOnName(Capability.Scope.TABLE, "ñoño_table");
    Assertions.assertTrue(result.supported());

    // Test Unicode letters in the end
    result = capability.specificationOnName(Capability.Scope.TABLE, "_table_测试");
    Assertions.assertTrue(result.supported());

    result = capability.specificationOnName(Capability.Scope.TABLE, "table_αβγ");
    Assertions.assertTrue(result.supported());
  }

  @Test
  void testValidStartingCharacterCombinations() {
    // Test all valid starting character types
    CapabilityResult result =
        capability.specificationOnName(Capability.Scope.TABLE, "_underscore_start");
    Assertions.assertTrue(result.supported());

    result = capability.specificationOnName(Capability.Scope.TABLE, "a_letter_start");
    Assertions.assertTrue(result.supported());

    result = capability.specificationOnName(Capability.Scope.TABLE, "Z_letter_start");
    Assertions.assertTrue(result.supported());

    result = capability.specificationOnName(Capability.Scope.TABLE, "/slash_start");
    Assertions.assertTrue(result.supported());

    // invalid starting characters
    result = capability.specificationOnName(Capability.Scope.TABLE, "1_number_start");
    Assertions.assertFalse(result.supported());

    result = capability.specificationOnName(Capability.Scope.TABLE, "-hyphen_start");
    Assertions.assertFalse(result.supported());

    result = capability.specificationOnName(Capability.Scope.TABLE, "$dollar_start");
    Assertions.assertFalse(result.supported());

    result = capability.specificationOnName(Capability.Scope.TABLE, "=equals_start");
    Assertions.assertFalse(result.supported());
  }
}
