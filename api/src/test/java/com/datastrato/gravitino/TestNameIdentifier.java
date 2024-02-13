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

import com.datastrato.gravitino.exceptions.IllegalNameIdentifierException;
import com.datastrato.gravitino.exceptions.IllegalNamespaceException;
import org.junit.jupiter.api.Test;

public class TestNameIdentifier {

  @Test
  void testCreateNameIdentifier() {
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
  void testCreateWithInvalidArgs() {
    assertThrows(IllegalArgumentException.class, NameIdentifier::of);
    assertThrows(IllegalArgumentException.class, () -> NameIdentifier.of("a", null));
    assertThrows(IllegalArgumentException.class, () -> NameIdentifier.of("a", ""));

    assertThrows(IllegalArgumentException.class, () -> NameIdentifier.of(null, "a"));
    assertThrows(IllegalArgumentException.class, () -> NameIdentifier.of(Namespace.empty(), null));
    assertThrows(IllegalArgumentException.class, () -> NameIdentifier.of(Namespace.empty(), ""));

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
  void testEqualsAndHashNameIdentifier() {
    NameIdentifier id1 = NameIdentifier.parse("a.b.c");
    NameIdentifier id2 = NameIdentifier.parse("a.b.c");

    assertTrue(id1.equals(id2));
    assertTrue(id2.equals(id1));
    assertEquals(id1.hashCode(), id2.hashCode());
  }

  @Test
  void testNotEqualsAndHashNameIdentifier() {
    NameIdentifier id1 = NameIdentifier.parse("a.b.c");
    NameIdentifier id2 = NameIdentifier.parse("a.b.z");

    assertFalse(id1.equals(null));
    assertFalse(id1.equals(id2));
    assertFalse(id2.equals(id1));
    assertNotEquals(id1.hashCode(), id2.hashCode());
  }

  @Test
  void testHasNamespace() {
    NameIdentifier id = NameIdentifier.parse("a.b.c");
    assertTrue(id.hasNamespace());
  }

  @Test
  void testToString() {
    NameIdentifier id1 = NameIdentifier.parse("a");
    NameIdentifier id2 = NameIdentifier.parse("a.b.c");

    assertEquals(id1.toString(), "a");
    assertEquals(id2.toString(), "a.b.c");
  }

  @Test
  void testCheckNameIdentifier() {
    // Test metalake
    assertThrows(IllegalNameIdentifierException.class, () -> NameIdentifier.checkMetalake(null));
    Throwable excep =
        assertThrows(
            IllegalNamespaceException.class,
            () -> NameIdentifier.checkMetalake(NameIdentifier.of("a", "b", "c")));
    assertTrue(excep.getMessage().contains("Metalake namespace must be non-null and empty"));

    // test catalog
    assertThrows(IllegalNameIdentifierException.class, () -> NameIdentifier.checkCatalog(null));
    Throwable excep1 =
        assertThrows(
            IllegalNamespaceException.class,
            () -> NameIdentifier.checkCatalog(NameIdentifier.of("a", "b", "c")));
    assertTrue(excep1.getMessage().contains("Catalog namespace must be non-null and have 1 level"));

    // test schema
    assertThrows(IllegalNameIdentifierException.class, () -> NameIdentifier.checkSchema(null));
    Throwable excep2 =
        assertThrows(
            IllegalNamespaceException.class,
            () -> NameIdentifier.checkSchema(NameIdentifier.of("a", "b", "c", "d")));
    assertTrue(excep2.getMessage().contains("Schema namespace must be non-null and have 2 levels"));

    // test table
    assertThrows(IllegalNameIdentifierException.class, () -> NameIdentifier.checkTable(null));
    Throwable excep3 =
        assertThrows(
            IllegalNamespaceException.class,
            () -> NameIdentifier.checkTable(NameIdentifier.of("a", "b", "c")));
    assertTrue(excep3.getMessage().contains("Table namespace must be non-null and have 3 levels"));
  }
}
