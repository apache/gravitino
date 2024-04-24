/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.exceptions.IllegalNamespaceException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestNamespace {

  @Test
  public void testEmptyNamespace() {
    Namespace ns = Namespace.empty();

    Assertions.assertEquals(0, ns.length());
    Assertions.assertArrayEquals(new String[0], ns.levels());
    Assertions.assertEquals(true, ns.isEmpty());
  }

  @Test
  public void testCreateNamespace() {
    Namespace ns = Namespace.of("a", "b", "c");

    Assertions.assertEquals(3, ns.length());
    Assertions.assertArrayEquals(new String[] {"a", "b", "c"}, ns.levels());
    Assertions.assertEquals("a", ns.level(0));
    Assertions.assertThrows(IllegalArgumentException.class, () -> ns.level(3));
    Assertions.assertEquals(false, ns.isEmpty());

    // Test namespace with null or empty levels
    Assertions.assertThrows(IllegalArgumentException.class, () -> Namespace.of("a", null, "c"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> Namespace.of("a", "", "c"));
  }

  @Test
  public void testCheckNamespace() {
    Namespace a = Namespace.of("a");
    Namespace ab = Namespace.of("a", "b");
    Namespace abcd = Namespace.of("a", "b", "c", "d");

    // Test metalake
    Assertions.assertThrows(IllegalNamespaceException.class, () -> Namespace.checkMetalake(null));
    Throwable excep =
        Assertions.assertThrows(IllegalNamespaceException.class, () -> Namespace.checkMetalake(ab));
    Assertions.assertTrue(
        excep.getMessage().contains("Metalake namespace must be non-null and empty"));

    // Test catalog
    Assertions.assertThrows(IllegalNamespaceException.class, () -> Namespace.checkCatalog(null));
    Throwable excep1 =
        Assertions.assertThrows(IllegalNamespaceException.class, () -> Namespace.checkCatalog(ab));
    Assertions.assertTrue(
        excep1.getMessage().contains("Catalog namespace must be non-null and have 1 level"));

    // Test schema
    Assertions.assertThrows(IllegalNamespaceException.class, () -> Namespace.checkSchema(null));
    Throwable excep2 =
        Assertions.assertThrows(IllegalNamespaceException.class, () -> Namespace.checkSchema(a));
    Assertions.assertTrue(
        excep2.getMessage().contains("Schema namespace must be non-null and have 2 levels"));

    // Test table
    Assertions.assertThrows(IllegalNamespaceException.class, () -> Namespace.checkTable(null));
    Throwable excep3 =
        Assertions.assertThrows(IllegalNamespaceException.class, () -> Namespace.checkTable(abcd));
    Assertions.assertTrue(
        excep3.getMessage().contains("Table namespace must be non-null and have 3 levels"));
  }
}
