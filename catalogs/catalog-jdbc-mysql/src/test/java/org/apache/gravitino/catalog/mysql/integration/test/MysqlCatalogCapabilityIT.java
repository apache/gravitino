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
package org.apache.gravitino.catalog.mysql.integration.test;

import org.apache.gravitino.catalog.mysql.MysqlCatalogCapability;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class MysqlCatalogCapabilityIT {

  private static MysqlCatalogCapability capability;

  @BeforeAll
  public static void setup() {
    capability = new MysqlCatalogCapability();
  }

  @Test
  void testMysqlNameValidationIntegration() {
    // valid mysql schema names should be accepted
    String[] validSchemaNames = {
      "user_schema",
      "test123",
      "schema_with_underscores",
      "schema-with-hyphens",
      "schema$with$dollar",
      "schema/with/slash",
      "schema=with=equals",
      "测试模式", // Unicode support
      "a".repeat(64) // Maximum length
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

    // valid mysql table names should be accepted
    String[] validTableNames = {
      "user_table",
      "test123",
      "table_with_underscores",
      "table-with-hyphens",
      "table$with$dollar",
      "table/with/slash",
      "table=with=equals",
      "测试表", // Unicode support
      "b".repeat(64) // Maximum length
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
  void testMysqlReservedWordsIntegration() {
    // Test that MySQL reserved schema names are properly rejected
    String[] reservedSchemaNames = {"mysql", "information_schema", "performance_schema", "sys"};

    for (String name : reservedSchemaNames) {
      CapabilityResult result = capability.specificationOnName(Capability.Scope.SCHEMA, name);
      Assertions.assertFalse(
          result.supported(),
          "Reserved schema name '" + name + "' should be rejected but was accepted");
      Assertions.assertTrue(
          result.unsupportedMessage().contains("reserved"),
          "Error message should mention 'reserved' for name: " + name);
    }

    // case insensitivity for schemas
    String[] mixedCaseReserved = {"MYSQL", "Information_Schema", "Performance_Schema", "SYS"};

    for (String name : mixedCaseReserved) {
      CapabilityResult schemaResult = capability.specificationOnName(Capability.Scope.SCHEMA, name);
      Assertions.assertFalse(
          schemaResult.supported(),
          "Reserved schema name '" + name + "' (mixed case) should be rejected but was accepted");
    }
  }

  @Test
  void testMysqlInvalidNamesIntegration() {
    // Test for checking that invalid MySQL names are properly rejected
    String[] invalidNames = {
      "", // empty
      "name with spaces",
      "name@with@at",
      "name#with#hash",
      "name%with%percent",
      "name.with.dots",
      "name(with)parentheses",
      "name[with]brackets",
      "name{with}braces",
      "a".repeat(65) // exceeds maximim length
    };

    for (String name : invalidNames) {
      CapabilityResult schemaResult = capability.specificationOnName(Capability.Scope.SCHEMA, name);
      Assertions.assertFalse(
          schemaResult.supported(),
          "Invalid schema name '" + name + "' should be rejected but was accepted");
      Assertions.assertTrue(
          schemaResult.unsupportedMessage().contains("illegal"),
          "Error message should mention 'illegal' for schema name: " + name);

      CapabilityResult tableResult = capability.specificationOnName(Capability.Scope.TABLE, name);
      Assertions.assertFalse(
          tableResult.supported(),
          "Invalid table name '" + name + "' should be rejected but was accepted");
      Assertions.assertTrue(
          tableResult.unsupportedMessage().contains("illegal"),
          "Error message should mention 'illegal' for table name: " + name);
    }
  }

  @Test
  void testMysqlColumnNameValidation() {
    String[] validColumnNames = {
      "user_column",
      "column123",
      "column_with_underscores",
      "mysql", // Should be valid for columns even though it's reserved for schemas/tables
      "information_schema", // Should be valid for columns
      "测试列" // testing unicode support here
    };

    for (String name : validColumnNames) {
      CapabilityResult result = capability.specificationOnName(Capability.Scope.COLUMN, name);
      Assertions.assertTrue(
          result.supported(),
          "Column name '"
              + name
              + "' should be valid but was rejected: "
              + result.unsupportedMessage());
    }

    // invalid column names should be rejected
    String[] invalidColumnNames = {
      "column with spaces",
      "column@invalid",
      "column#invalid",
      "a".repeat(65) // Exceeds maximum length
    };

    for (String name : invalidColumnNames) {
      CapabilityResult result = capability.specificationOnName(Capability.Scope.COLUMN, name);
      Assertions.assertFalse(
          result.supported(),
          "Invalid column name '" + name + "' should be rejected but was accepted");
    }
  }

  @Test
  void testMysqlBoundaryConditions() {
    // boundary conditions for name length
    String exactMaxName = "a".repeat(64);
    CapabilityResult result = capability.specificationOnName(Capability.Scope.SCHEMA, exactMaxName);
    Assertions.assertTrue(result.supported(), "64-character name should be valid");

    String tooLongName = "a".repeat(65);
    result = capability.specificationOnName(Capability.Scope.SCHEMA, tooLongName);
    Assertions.assertFalse(result.supported(), "65-character name should be invalid");

    // minimum length
    result = capability.specificationOnName(Capability.Scope.SCHEMA, "a");
    Assertions.assertTrue(result.supported(), "Single character name should be valid");
  }
}
