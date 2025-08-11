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
package org.apache.gravitino.catalog.postgresql.integration.test;

import org.apache.gravitino.catalog.postgresql.PostgreSqlCatalogCapability;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class PostgreSqlCatalogCapabilityIT {

  private static PostgreSqlCatalogCapability capability;

  @BeforeAll
  public static void setup() {
    capability = new PostgreSqlCatalogCapability();
  }

  @Test
  void testPostgreSqlNameValidationIntegration() {
    // valid PostgreSQL schema names should be accepted
    String[] validSchemaNames = {
      "_user_schema",
      "test123",
      "schema_with_underscores",
      "user_schema",
      "测试模式", // Unicode support
      "a".repeat(63) // Maximum length for PostgreSQL
    };

    for (String name : validSchemaNames) {
      CapabilityResult result = capability.specificationOnName(Capability.Scope.SCHEMA, name);
      Assertions.assertTrue(
          result.supported(),
          "Schema name '"
              + name
              + "' should be valid but was rejected: "
              + result.unsupportedMessage());
    }

    // valid PostgreSQL table names should be accepted
    String[] validTableNames = {
      "_user_table",
      "test123",
      "table_with_underscores",
      "user_table",
      "测试表", // Unicode support
      "b".repeat(63) // Maximum length for PostgreSQL
    };

    for (String name : validTableNames) {
      CapabilityResult result = capability.specificationOnName(Capability.Scope.TABLE, name);
      Assertions.assertTrue(
          result.supported(),
          "Table name '"
              + name
              + "' should be valid but was rejected: "
              + result.unsupportedMessage());
    }
  }

  @Test
  void testPostgreSqlReservedWordsIntegration() {
    String[] reservedSchemaNames = {"pg_catalog", "information_schema"};

    for (String name : reservedSchemaNames) {
      CapabilityResult result = capability.specificationOnName(Capability.Scope.SCHEMA, name);
      Assertions.assertFalse(
          result.supported(),
          "Reserved schema name '" + name + "' should be rejected but was accepted");
      Assertions.assertTrue(
          result.unsupportedMessage().contains("reserved"),
          "Error message should mention 'reserved' for name: " + name);
    }
  }

  @Test
  void testPostgreSqlInvalidNamesIntegration() {
    // Test that invalid PostgreSQL names are properly rejected
    String[] invalidNames = {
      "", // empty name
      "a".repeat(64), // exceeds maximum length
      "123table", // starts with digit
      "table with space", // contains space
      "table@with@at", // contains @ symbol
      "table#with#hash", // contains # symbol
      "table%with%percent", // contains % symbol
      "table.with.dot" // contains dot
    };

    for (String name : invalidNames) {
      for (Capability.Scope scope :
          new Capability.Scope[] {Capability.Scope.SCHEMA, Capability.Scope.TABLE}) {
        CapabilityResult result = capability.specificationOnName(scope, name);
        Assertions.assertFalse(
            result.supported(),
            "Invalid " + scope + " name '" + name + "' should be rejected but was accepted");
        Assertions.assertTrue(
            result.unsupportedMessage().contains("illegal"),
            "Error message should mention 'illegal' for name: " + name);
      }
    }
  }

  @Test
  void testPostgreSqlValidSpecialCharactersIntegration() {
    String[] validNamesWithSpecialChars = {
      "table-with-hyphen", // hyphens should be allowed
      "table$with$dollar", // dollar signs should be allowed
      "table/with/slash", // slashes should be allowed
      "table=with=equals" // equals signs should be allowed
    };

    for (String name : validNamesWithSpecialChars) {
      CapabilityResult schemaResult = capability.specificationOnName(Capability.Scope.SCHEMA, name);
      Assertions.assertTrue(
          schemaResult.supported(),
          "Schema name with special characters '"
              + name
              + "' should be valid but was rejected: "
              + schemaResult.unsupportedMessage());

      CapabilityResult tableResult = capability.specificationOnName(Capability.Scope.TABLE, name);
      Assertions.assertTrue(
          tableResult.supported(),
          "Table name with special characters '"
              + name
              + "' should be valid but was rejected: "
              + tableResult.unsupportedMessage());
    }
  }

  @Test
  void testPostgreSqlCaseInsensitiveReservedWords() {
    // Test that reserved words are rejected regardless of case
    String[] caseVariants = {
      "PG_CATALOG", "Pg_Catalog", "pg_CATALOG", "INFORMATION_SCHEMA", "Information_Schema"
    };

    for (String name : caseVariants) {
      CapabilityResult result = capability.specificationOnName(Capability.Scope.SCHEMA, name);
      Assertions.assertFalse(
          result.supported(),
          "Case variant reserved schema name '" + name + "' should be rejected but was accepted");
      Assertions.assertTrue(
          result.unsupportedMessage().contains("reserved"),
          "Error message should mention 'reserved' for case variant: " + name);
    }
  }

  @Test
  void testPostgreSqlReservedWordsNotAppliedToColumns() {
    // Reserved schema/table names should be allowed as column names
    String[] reservedNames = {"pg_catalog", "information_schema"};

    for (String name : reservedNames) {
      CapabilityResult result = capability.specificationOnName(Capability.Scope.COLUMN, name);
      Assertions.assertTrue(
          result.supported(),
          "Reserved name '"
              + name
              + "' should be allowed as column name but was rejected: "
              + result.unsupportedMessage());
    }
  }

  @Test
  void testPostgreSqlUnicodeNamesIntegration() {
    // Test that various Unicode characters work properly in names
    String[] unicodeNames = {
      "测试表_integration", // Chinese characters
      "αβγ_table", // Greek letters
      "café_table", // Latin with accents
      "أحمد_table", // Arabic characters
      "тест_table" // Cyrillic characters
    };

    for (String name : unicodeNames) {
      CapabilityResult result = capability.specificationOnName(Capability.Scope.TABLE, name);
      Assertions.assertTrue(
          result.supported(),
          "Unicode table name '"
              + name
              + "' should be valid but was rejected: "
              + result.unsupportedMessage());

      result = capability.specificationOnName(Capability.Scope.SCHEMA, name);
      Assertions.assertTrue(
          result.supported(),
          "Unicode schema name '"
              + name
              + "' should be valid but was rejected: "
              + result.unsupportedMessage());
    }
  }

  @Test
  void testPostgreSqlBoundaryConditions() {
    // Test boundary conditions for name length

    // Single character names should work
    CapabilityResult result = capability.specificationOnName(Capability.Scope.TABLE, "a");
    Assertions.assertTrue(result.supported(), "Single character name should be valid");

    result = capability.specificationOnName(Capability.Scope.TABLE, "_");
    Assertions.assertTrue(result.supported(), "Single underscore name should be valid");

    // Exactly 63 characters (PostgreSQL maximum)
    String maxLengthName = "a".repeat(63);
    result = capability.specificationOnName(Capability.Scope.TABLE, maxLengthName);
    Assertions.assertTrue(
        result.supported(), "63-character name should be valid (PostgreSQL maximum)");

    // Exactly 64 characters (exceeds PostgreSQL maximum)
    String tooLongName = "a".repeat(64);
    result = capability.specificationOnName(Capability.Scope.TABLE, tooLongName);
    Assertions.assertFalse(
        result.supported(), "64-character name should be rejected (exceeds PostgreSQL maximum)");
  }
}
