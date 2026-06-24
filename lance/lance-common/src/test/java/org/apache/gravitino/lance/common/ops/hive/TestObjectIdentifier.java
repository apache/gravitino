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
package org.apache.gravitino.lance.common.ops.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/** Unit tests for the delimiter-based {@link ObjectIdentifier}. */
class TestObjectIdentifier {

  @Test
  void testEmptyIdIsRoot() {
    ObjectIdentifier id = ObjectIdentifier.of("", Pattern.quote("$"));
    assertEquals(0, id.levels());
  }

  @Test
  void testSingleLevelDefaultDelimiter() {
    ObjectIdentifier id = ObjectIdentifier.of("db", Pattern.quote("$"));
    assertEquals(1, id.levels());
    assertEquals("db", id.levelAtListPos(0));
  }

  @Test
  void testTwoLevelsDefaultDelimiter() {
    ObjectIdentifier id = ObjectIdentifier.of("db$table", Pattern.quote("$"));
    assertEquals(2, id.levels());
    assertEquals("db", id.levelAtListPos(0));
    assertEquals("table", id.levelAtListPos(1));
  }

  @Test
  void testCustomDelimiter() {
    ObjectIdentifier id = ObjectIdentifier.of("db.table", Pattern.quote("."));
    assertEquals(2, id.levels());
    assertEquals("db", id.levelAtListPos(0));
    assertEquals("table", id.levelAtListPos(1));
  }

  @Test
  void testListStyleId() {
    ObjectIdentifier id = ObjectIdentifier.of("db$table", Pattern.quote("$"));
    assertEquals(java.util.Arrays.asList("db", "table"), id.listStyleId());
  }

  @Test
  void testNullIdThrows() {
    assertThrows(
        IllegalArgumentException.class, () -> ObjectIdentifier.of(null, Pattern.quote("$")));
  }

  @Test
  void testBlankDelimiterThrows() {
    assertThrows(IllegalArgumentException.class, () -> ObjectIdentifier.of("db", " "));
  }
}
