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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import org.apache.gravitino.auth.ActiveRoles.Mode;
import org.junit.jupiter.api.Test;

public class TestActiveRoles {

  @Test
  public void testAll() {
    ActiveRoles roles = ActiveRoles.all();
    assertEquals(Mode.ALL, roles.mode());
    assertTrue(roles.isAll());
    assertFalse(roles.isNone());
    assertTrue(roles.roleNames().isEmpty());
    assertSame(ActiveRoles.all(), roles);
  }

  @Test
  public void testNone() {
    ActiveRoles roles = ActiveRoles.none();
    assertEquals(Mode.NONE, roles.mode());
    assertTrue(roles.isNone());
    assertFalse(roles.isAll());
    assertTrue(roles.roleNames().isEmpty());
    assertSame(ActiveRoles.none(), roles);
  }

  @Test
  public void testNamedPreservesOrderAndDeduplicates() {
    ActiveRoles roles = ActiveRoles.of(Arrays.asList("reader", "analyst", "reader"));
    assertEquals(Mode.NAMED, roles.mode());
    assertFalse(roles.isAll());
    assertFalse(roles.isNone());
    assertIterableEquals(Arrays.asList("reader", "analyst"), roles.roleNames());
  }

  @Test
  public void testOfNullThrows() {
    assertThrows(IllegalArgumentException.class, () -> ActiveRoles.of(null));
  }

  @Test
  public void testOfEmptyThrows() {
    assertThrows(IllegalArgumentException.class, () -> ActiveRoles.of(Collections.emptyList()));
  }

  @Test
  public void testOfBlankNameThrows() {
    assertThrows(
        IllegalArgumentException.class, () -> ActiveRoles.of(Arrays.asList("analyst", "  ")));
    assertThrows(
        IllegalArgumentException.class,
        () -> ActiveRoles.of(Collections.singletonList((String) null)));
  }

  @Test
  public void testRoleNamesIsImmutable() {
    ActiveRoles roles = ActiveRoles.of(Collections.singletonList("analyst"));
    assertThrows(UnsupportedOperationException.class, () -> roles.roleNames().add("reader"));
  }

  @Test
  public void testEqualsAndHashCode() {
    ActiveRoles a = ActiveRoles.of(Arrays.asList("analyst", "reader"));
    ActiveRoles b = ActiveRoles.of(Arrays.asList("analyst", "reader"));
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());

    assertNotEquals(a, ActiveRoles.of(Collections.singletonList("analyst")));
    assertNotEquals(ActiveRoles.all(), ActiveRoles.none());
    assertNotEquals(ActiveRoles.all(), ActiveRoles.of(Collections.singletonList("analyst")));
  }
}
