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

package org.apache.gravitino.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import org.apache.gravitino.auth.ActiveRoles.Mode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

public class TestActiveRolesParser {

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"", "   ", "\t"})
  public void testAbsentOrBlankIsAll(String value) {
    ActiveRoles roles = ActiveRolesParser.parse(value);
    assertEquals(Mode.ALL, roles.mode());
    assertTrue(roles.isAll());
    assertTrue(roles.roleNames().isEmpty());
  }

  @Test
  public void testAllKeyword() {
    assertEquals(Mode.ALL, ActiveRolesParser.parse("ALL").mode());
  }

  @Test
  public void testNoneKeyword() {
    ActiveRoles roles = ActiveRolesParser.parse("NONE");
    assertEquals(Mode.NONE, roles.mode());
    assertTrue(roles.isNone());
    assertTrue(roles.roleNames().isEmpty());
  }

  @Test
  public void testSingleRole() {
    ActiveRoles roles = ActiveRolesParser.parse("analyst");
    assertEquals(Mode.NAMED, roles.mode());
    assertIterableEquals(Collections.singletonList("analyst"), roles.roleNames());
  }

  @Test
  public void testCommaSeparatedList() {
    ActiveRoles roles = ActiveRolesParser.parse("analyst,reader");
    assertEquals(Mode.NAMED, roles.mode());
    assertIterableEquals(Arrays.asList("analyst", "reader"), roles.roleNames());
  }

  @Test
  public void testWhitespaceIsTrimmed() {
    ActiveRoles roles = ActiveRolesParser.parse("  analyst ,  reader  ");
    assertIterableEquals(Arrays.asList("analyst", "reader"), roles.roleNames());
  }

  @Test
  public void testDuplicatesCollapseAndPreserveOrder() {
    ActiveRoles roles = ActiveRolesParser.parse("reader, analyst, reader");
    assertIterableEquals(Arrays.asList("reader", "analyst"), roles.roleNames());
  }

  @Test
  public void testRoleNamesAreCaseSensitive() {
    ActiveRoles roles = ActiveRolesParser.parse("Analyst,analyst");
    assertIterableEquals(Arrays.asList("Analyst", "analyst"), roles.roleNames());
  }

  @Test
  public void testKeywordsAreCaseSensitiveSoLowercaseIsARoleName() {
    ActiveRoles roles = ActiveRolesParser.parse("all");
    assertEquals(Mode.NAMED, roles.mode());
    assertIterableEquals(Collections.singletonList("all"), roles.roleNames());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "analyst,", // trailing comma
        ",analyst", // leading comma
        "analyst,,reader", // empty middle entry
        ",", // only a comma
        "ALL,analyst", // reserved keyword with a role name
        "analyst,ALL", // reserved keyword with a role name
        "NONE,reader", // reserved keyword with a role name
        "ALL,NONE", // two different reserved keywords
        "ALL,ALL", // repeated reserved keyword
        "NONE,NONE" // repeated reserved keyword
      })
  public void testMalformedValuesThrow(String value) {
    assertThrows(IllegalActiveRolesException.class, () -> ActiveRolesParser.parse(value));
  }

  @Test
  public void testMalformedExceptionIsIllegalArgument() {
    // The server maps this to 400 Bad Request, so it must remain an IllegalArgumentException.
    assertThrows(IllegalArgumentException.class, () -> ActiveRolesParser.parse("analyst,"));
  }
}
