/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class TestNameIdentifier {

  @Test
  public void testCreateNameIdentifier() {
    NameIdentifier id = NameIdentifier.of("a", "b", "c");
    assertEquals(Namespace.of("a", "b"), id.namespace());
    assertEquals("c", id.name());

    NameIdentifier id1 = NameIdentifier.of(Namespace.of("a", "b"), "c");
    assertEquals(Namespace.of("a", "b"), id1.namespace());
    assertEquals("c", id1.name());

    NameIdentifier id2 = NameIdentifier.parse("a.b.c");
    assertEquals(Namespace.of("a", "b"), id2.namespace());
    assertEquals("c", id2.name());

    NameIdentifier id3 = NameIdentifier.parse("a");
    assertEquals(Namespace.empty(), id3.namespace());
    assertEquals("a", id3.name());
  }

  @Test
  public void testCreateWithInvalidArgs() {
    assertThrows(IllegalArgumentException.class, NameIdentifier::of);
    assertThrows(IllegalArgumentException.class, () -> NameIdentifier.of("a", null));
    assertThrows(IllegalArgumentException.class, () -> NameIdentifier.of("a", ""));

    Namespace empty = Namespace.empty();
    assertThrows(IllegalArgumentException.class, () -> NameIdentifier.of(null, "a"));
    assertThrows(IllegalArgumentException.class, () -> NameIdentifier.of(empty, null));
    assertThrows(IllegalArgumentException.class, () -> NameIdentifier.of(empty, ""));

    assertThrows(IllegalArgumentException.class, () -> NameIdentifier.parse(null));
    assertThrows(IllegalArgumentException.class, () -> NameIdentifier.parse(""));
    assertThrows(IllegalArgumentException.class, () -> NameIdentifier.parse("a."));
    assertThrows(IllegalArgumentException.class, () -> NameIdentifier.parse("a.."));
    assertThrows(IllegalArgumentException.class, () -> NameIdentifier.parse(".a"));
    assertThrows(IllegalArgumentException.class, () -> NameIdentifier.parse("..a"));
    assertThrows(IllegalArgumentException.class, () -> NameIdentifier.parse("a..b"));
    assertThrows(IllegalArgumentException.class, () -> NameIdentifier.parse("a.b."));
    assertThrows(IllegalArgumentException.class, () -> NameIdentifier.parse(".a.b"));
  }

  @Test
  public void testEqualsAndHashNameIdentifier() {
    NameIdentifier id1 = NameIdentifier.parse("a.b.c");
    NameIdentifier id2 = NameIdentifier.parse("a.b.c");

    assertTrue(id1.equals(id2));
    assertTrue(id2.equals(id1));
    assertEquals(id1.hashCode(), id2.hashCode());
  }

  @Test
  public void testNotEqualsAndHashNameIdentifier() {
    NameIdentifier id1 = NameIdentifier.parse("a.b.c");
    NameIdentifier id2 = NameIdentifier.parse("a.b.z");

    assertFalse(id1.equals(null));
    assertFalse(id1.equals(id2));
    assertFalse(id2.equals(id1));
    assertNotEquals(id1.hashCode(), id2.hashCode());
  }

  @Test
  public void testHasNamespace() {
    NameIdentifier id = NameIdentifier.parse("a.b.c");
    assertTrue(id.hasNamespace());
  }

  @Test
  public void testToString() {
    NameIdentifier id1 = NameIdentifier.parse("a");
    NameIdentifier id2 = NameIdentifier.parse("a.b.c");

    assertEquals("a", id1.toString());
    assertEquals("a.b.c", id2.toString());
  }
}
