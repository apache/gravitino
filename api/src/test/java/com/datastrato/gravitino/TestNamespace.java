/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

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
}
