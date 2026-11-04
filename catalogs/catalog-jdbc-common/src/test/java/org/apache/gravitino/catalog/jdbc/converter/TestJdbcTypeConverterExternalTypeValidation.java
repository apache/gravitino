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
package org.apache.gravitino.catalog.jdbc.converter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests {@link JdbcTypeConverter#validateExternalTypeString(String)}. The accepted values are the
 * external types the JDBC catalogs actually produce today, including the ClickHouse, OceanBase and
 * Hologres shapes, so that those contrib catalogs can reuse this helper without widening it.
 */
public class TestJdbcTypeConverterExternalTypeValidation {

  @ParameterizedTest
  @ValueSource(
      strings = {
        // MySQL, recovered from information_schema by MysqlTableOperations.
        "enum('a','b','c')",
        "set('x','y','z')",
        "bit(8)",
        "binary(16)",
        "varbinary(100)",
        "blob",
        // Doris.
        "json",
        "variant",
        "ipv4",
        "ipv6",
        "largeint",
        "bitmap",
        "hll",
        "bigint unsigned",
        // Doris falls back to the raw string when the type parameters cannot be parsed.
        "varchar(abc)",
        "char(xyz)",
        "decimal(a,b)",
        // PostgreSQL.
        "numeric",
        "bit",
        // Used by the MySQL and PostgreSQL converter tests.
        "user-defined",
        // A quoted literal may contain anything, including an escaped quote.
        "enum('it''s','ok')",
        "enum('a;b')",
        "enum('a--b')",
        // ClickHouse, so the contrib follow-up can reuse this helper unchanged.
        "Enum8('active'=1,'inactive'=2)",
        "Enum16('x'=1,'y'=2)",
        "Decimal(50,10)",
        "Int128",
        "Date32",
        "IPv4",
        "Map(String, Int32)",
        "Tuple(Int32, String)",
        // Nested generic types keep their commas inside angle brackets.
        "struct<a:int,b:int>",
        "array<int>"
      })
  public void testAcceptsLegitimateExternalTypes(String catalogString) {
    Assertions.assertEquals(
        catalogString, JdbcTypeConverter.validateExternalTypeString(catalogString));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        // A top level comma appends another action to the generated ALTER TABLE statement.
        "int, DROP COLUMN secret",
        "decimal(10,2), DROP COLUMN secret",
        // Statement separators and comment markers.
        "json; DROP TABLE foo",
        "int -- comment",
        "int /* comment */",
        "int#comment",
        "int\nDROP COLUMN secret",
        // Quoting that does not close.
        "a'b",
        "enum('a','b",
        // Brackets that do not balance.
        "foo(",
        "foo)",
        "a<b",
        "struct<a:int",
        // Blank input, which ExternalType.of accepts today.
        "   "
      })
  public void testRejectsUnsafeExternalTypes(String catalogString) {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> JdbcTypeConverter.validateExternalTypeString(catalogString));
  }

  @ParameterizedTest
  @NullSource
  @EmptySource
  public void testRejectsNullAndEmptyExternalTypes(String catalogString) {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> JdbcTypeConverter.validateExternalTypeString(catalogString));
  }

  @Test
  public void testRejectsHyphensThatBecomeAdjacentAfterStrippingLiterals() {
    // Conservative by design: the '--' check runs after quoted literals are removed, so hyphens
    // that only become adjacent because a literal between them was stripped are rejected too.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> JdbcTypeConverter.validateExternalTypeString("a-'x'-b"));
  }

  @Test
  public void testErrorMessageNamesTheOffendingType() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> JdbcTypeConverter.validateExternalTypeString("int, DROP COLUMN secret"));
    Assertions.assertTrue(
        exception.getMessage().contains("int, DROP COLUMN secret"), exception.getMessage());
  }
}
