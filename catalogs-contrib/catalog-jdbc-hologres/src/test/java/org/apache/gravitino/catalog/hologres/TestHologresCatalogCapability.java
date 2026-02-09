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
package org.apache.gravitino.catalog.hologres;

import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link HologresCatalogCapability}. */
public class TestHologresCatalogCapability {

  private final HologresCatalogCapability capability = new HologresCatalogCapability();

  // --- Valid names ---

  @Test
  public void testValidNameStartingWithLetter() {
    CapabilityResult result = capability.specificationOnName(Capability.Scope.TABLE, "my_table");
    Assertions.assertEquals(CapabilityResult.SUPPORTED, result);
  }

  @Test
  public void testValidNameStartingWithUnderscore() {
    CapabilityResult result = capability.specificationOnName(Capability.Scope.TABLE, "_my_table");
    Assertions.assertEquals(CapabilityResult.SUPPORTED, result);
  }

  @Test
  public void testValidNameWithDigits() {
    CapabilityResult result = capability.specificationOnName(Capability.Scope.TABLE, "table123");
    Assertions.assertEquals(CapabilityResult.SUPPORTED, result);
  }

  @Test
  public void testValidNameWithHyphen() {
    CapabilityResult result = capability.specificationOnName(Capability.Scope.TABLE, "my-table");
    Assertions.assertEquals(CapabilityResult.SUPPORTED, result);
  }

  @Test
  public void testValidNameWithDollar() {
    CapabilityResult result = capability.specificationOnName(Capability.Scope.TABLE, "my$table");
    Assertions.assertEquals(CapabilityResult.SUPPORTED, result);
  }

  @Test
  public void testValidNameWithSlash() {
    CapabilityResult result = capability.specificationOnName(Capability.Scope.TABLE, "/my_table");
    Assertions.assertEquals(CapabilityResult.SUPPORTED, result);
  }

  @Test
  public void testValidNameWithUnicode() {
    CapabilityResult result = capability.specificationOnName(Capability.Scope.TABLE, "表名test");
    Assertions.assertEquals(CapabilityResult.SUPPORTED, result);
  }

  @Test
  public void testValidNameSingleChar() {
    CapabilityResult result = capability.specificationOnName(Capability.Scope.TABLE, "a");
    Assertions.assertEquals(CapabilityResult.SUPPORTED, result);
  }

  // --- Invalid names ---

  @Test
  public void testInvalidNameStartingWithDigit() {
    CapabilityResult result = capability.specificationOnName(Capability.Scope.TABLE, "1table");
    Assertions.assertNotEquals(CapabilityResult.SUPPORTED, result);
  }

  @Test
  public void testInvalidNameEmpty() {
    CapabilityResult result = capability.specificationOnName(Capability.Scope.TABLE, "");
    Assertions.assertNotEquals(CapabilityResult.SUPPORTED, result);
  }

  @Test
  public void testInvalidNameTooLong() {
    // 64 characters (max allowed is 63)
    String longName = "a" + "b".repeat(63);
    Assertions.assertEquals(64, longName.length());
    CapabilityResult result = capability.specificationOnName(Capability.Scope.TABLE, longName);
    Assertions.assertNotEquals(CapabilityResult.SUPPORTED, result);
  }

  @Test
  public void testValidNameMaxLength() {
    // 63 characters (max allowed)
    String maxName = "a" + "b".repeat(62);
    Assertions.assertEquals(63, maxName.length());
    CapabilityResult result = capability.specificationOnName(Capability.Scope.TABLE, maxName);
    Assertions.assertEquals(CapabilityResult.SUPPORTED, result);
  }

  // --- Reserved words for SCHEMA scope ---

  @Test
  public void testReservedWordPgCatalog() {
    CapabilityResult result = capability.specificationOnName(Capability.Scope.SCHEMA, "pg_catalog");
    Assertions.assertNotEquals(CapabilityResult.SUPPORTED, result);
  }

  @Test
  public void testReservedWordInformationSchema() {
    CapabilityResult result =
        capability.specificationOnName(Capability.Scope.SCHEMA, "information_schema");
    Assertions.assertNotEquals(CapabilityResult.SUPPORTED, result);
  }

  @Test
  public void testReservedWordHologres() {
    CapabilityResult result = capability.specificationOnName(Capability.Scope.SCHEMA, "hologres");
    Assertions.assertNotEquals(CapabilityResult.SUPPORTED, result);
  }

  @Test
  public void testReservedWordNotAppliedToTableScope() {
    // "hologres" as a table name should be valid (reserved words only apply to SCHEMA scope)
    CapabilityResult result = capability.specificationOnName(Capability.Scope.TABLE, "hologres");
    Assertions.assertEquals(CapabilityResult.SUPPORTED, result);
  }

  @Test
  public void testNonReservedSchemaName() {
    CapabilityResult result = capability.specificationOnName(Capability.Scope.SCHEMA, "my_schema");
    Assertions.assertEquals(CapabilityResult.SUPPORTED, result);
  }
}
