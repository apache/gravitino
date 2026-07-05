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
package org.apache.gravitino;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class TestExternalIdIdentifier {

  @Test
  public void testCreateExternalIdIdentifier() {
    ExternalIdIdentifier id = ExternalIdIdentifier.of("a", "b", "c");
    assertEquals(Namespace.of("a", "b"), id.namespace());
    assertEquals("c", id.externalId());

    ExternalIdIdentifier id1 = ExternalIdIdentifier.of(Namespace.of("a", "b"), "c");
    assertEquals(Namespace.of("a", "b"), id1.namespace());
    assertEquals("c", id1.externalId());

    ExternalIdIdentifier id2 = ExternalIdIdentifier.parse("a.b.c");
    assertEquals(Namespace.of("a", "b"), id2.namespace());
    assertEquals("c", id2.externalId());

    ExternalIdIdentifier id3 = ExternalIdIdentifier.parse("a");
    assertEquals(Namespace.empty(), id3.namespace());
    assertEquals("a", id3.externalId());
  }

  @Test
  public void testCreateWithInvalidArgs() {
    assertThrows(IllegalArgumentException.class, ExternalIdIdentifier::of);
    assertThrows(IllegalArgumentException.class, () -> ExternalIdIdentifier.of("a", null));
    assertThrows(IllegalArgumentException.class, () -> ExternalIdIdentifier.of("a", ""));

    Namespace empty = Namespace.empty();
    assertThrows(IllegalArgumentException.class, () -> ExternalIdIdentifier.of(null, "a"));
    assertThrows(IllegalArgumentException.class, () -> ExternalIdIdentifier.of(empty, null));
    assertThrows(IllegalArgumentException.class, () -> ExternalIdIdentifier.of(empty, ""));

    assertThrows(IllegalArgumentException.class, () -> ExternalIdIdentifier.parse(null));
    assertThrows(IllegalArgumentException.class, () -> ExternalIdIdentifier.parse(""));
    assertThrows(IllegalArgumentException.class, () -> ExternalIdIdentifier.parse("a."));
    assertThrows(IllegalArgumentException.class, () -> ExternalIdIdentifier.parse("a.."));
    assertThrows(IllegalArgumentException.class, () -> ExternalIdIdentifier.parse(".a"));
    assertThrows(IllegalArgumentException.class, () -> ExternalIdIdentifier.parse("..a"));
    assertThrows(IllegalArgumentException.class, () -> ExternalIdIdentifier.parse("a..b"));
    assertThrows(IllegalArgumentException.class, () -> ExternalIdIdentifier.parse("a.b."));
    assertThrows(IllegalArgumentException.class, () -> ExternalIdIdentifier.parse(".a.b"));
  }

  @Test
  public void testEqualsAndHashExternalIdIdentifier() {
    ExternalIdIdentifier id1 = ExternalIdIdentifier.parse("a.b.c");
    ExternalIdIdentifier id2 = ExternalIdIdentifier.parse("a.b.c");

    assertTrue(id1.equals(id2));
    assertTrue(id2.equals(id1));
    assertEquals(id1.hashCode(), id2.hashCode());
  }

  @Test
  public void testNotEqualsAndHashExternalIdIdentifier() {
    ExternalIdIdentifier id1 = ExternalIdIdentifier.parse("a.b.c");
    ExternalIdIdentifier id2 = ExternalIdIdentifier.parse("a.b.z");

    assertFalse(id1.equals(null));
    assertFalse(id1.equals(id2));
    assertFalse(id2.equals(id1));
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(NameIdentifier.parse("a.b.c"), id1);
  }

  @Test
  public void testHasNamespace() {
    ExternalIdIdentifier id = ExternalIdIdentifier.parse("a.b.c");
    assertTrue(id.hasNamespace());
  }

  @Test
  public void testToString() {
    ExternalIdIdentifier id1 = ExternalIdIdentifier.parse("a");
    ExternalIdIdentifier id2 = ExternalIdIdentifier.parse("a.b.c");

    assertEquals("a", id1.toString());
    assertEquals("a.b.c", id2.toString());
  }
}
