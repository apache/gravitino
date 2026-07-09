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

public class TestActiveRoleParser {

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"", "   ", "\t"})
  public void testAbsentOrBlankIsAll(String value) {
    ActiveRoles roles = ActiveRoleParser.parse(value);
    assertEquals(Mode.ALL, roles.mode());
    assertTrue(roles.isAll());
    assertTrue(roles.roleNames().isEmpty());
  }

  @Test
  public void testAllKeyword() {
    assertEquals(Mode.ALL, ActiveRoleParser.parse("ALL").mode());
  }

  @Test
  public void testNoneKeyword() {
    ActiveRoles roles = ActiveRoleParser.parse("NONE");
    assertEquals(Mode.NONE, roles.mode());
    assertTrue(roles.isNone());
    assertTrue(roles.roleNames().isEmpty());
  }

  @Test
  public void testSingleRole() {
    ActiveRoles roles = ActiveRoleParser.parse("analyst");
    assertEquals(Mode.NAMED, roles.mode());
    assertIterableEquals(Collections.singletonList("analyst"), roles.roleNames());
  }

  @Test
  public void testCommaSeparatedList() {
    ActiveRoles roles = ActiveRoleParser.parse("analyst,reader");
    assertEquals(Mode.NAMED, roles.mode());
    assertIterableEquals(Arrays.asList("analyst", "reader"), roles.roleNames());
  }

  @Test
  public void testWhitespaceIsTrimmed() {
    ActiveRoles roles = ActiveRoleParser.parse("  analyst ,  reader  ");
    assertIterableEquals(Arrays.asList("analyst", "reader"), roles.roleNames());
  }

  @Test
  public void testDuplicatesCollapseAndPreserveOrder() {
    ActiveRoles roles = ActiveRoleParser.parse("reader, analyst, reader");
    assertIterableEquals(Arrays.asList("reader", "analyst"), roles.roleNames());
  }

  @Test
  public void testRoleNamesAreCaseSensitive() {
    ActiveRoles roles = ActiveRoleParser.parse("Analyst,analyst");
    assertIterableEquals(Arrays.asList("Analyst", "analyst"), roles.roleNames());
  }

  @Test
  public void testKeywordsAreCaseSensitiveSoLowercaseIsARoleName() {
    ActiveRoles roles = ActiveRoleParser.parse("all");
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
        "ALL,NONE" // two reserved keywords
      })
  public void testMalformedValuesThrow(String value) {
    assertThrows(IllegalActiveRolesException.class, () -> ActiveRoleParser.parse(value));
  }
}
