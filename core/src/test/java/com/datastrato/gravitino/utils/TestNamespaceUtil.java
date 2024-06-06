/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.utils;

import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.IllegalNamespaceException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestNamespaceUtil {

  @Test
  public void testCheckNamespace() {
    Namespace a = Namespace.of("a");
    Namespace ab = Namespace.of("a", "b");
    Namespace abcd = Namespace.of("a", "b", "c", "d");

    // Test metalake
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> NamespaceUtil.checkMetalake(null));
    Throwable excep =
        Assertions.assertThrows(
            IllegalNamespaceException.class, () -> NamespaceUtil.checkMetalake(ab));
    Assertions.assertTrue(
        excep.getMessage().contains("Metalake namespace must be non-null and empty"));

    // Test catalog
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> NamespaceUtil.checkCatalog(null));
    Throwable excep1 =
        Assertions.assertThrows(
            IllegalNamespaceException.class, () -> NamespaceUtil.checkCatalog(ab));
    Assertions.assertTrue(
        excep1.getMessage().contains("Catalog namespace must be non-null and have 1 level"));

    // Test schema
    Assertions.assertThrows(IllegalNamespaceException.class, () -> NamespaceUtil.checkSchema(null));
    Throwable excep2 =
        Assertions.assertThrows(
            IllegalNamespaceException.class, () -> NamespaceUtil.checkSchema(a));
    Assertions.assertTrue(
        excep2.getMessage().contains("Schema namespace must be non-null and have 2 levels"));

    // Test table
    Assertions.assertThrows(IllegalNamespaceException.class, () -> NamespaceUtil.checkTable(null));
    Throwable excep3 =
        Assertions.assertThrows(
            IllegalNamespaceException.class, () -> NamespaceUtil.checkTable(abcd));
    Assertions.assertTrue(
        excep3.getMessage().contains("Table namespace must be non-null and have 3 levels"));
  }
}
