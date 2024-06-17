/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.utils;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.exceptions.IllegalNameIdentifierException;
import com.datastrato.gravitino.exceptions.IllegalNamespaceException;
import org.junit.jupiter.api.Test;

public class TestNameIdentifierUtil {

  @Test
  public void testCheckNameIdentifier() {
    NameIdentifier abc = NameIdentifier.of("a", "b", "c");
    NameIdentifier abcd = NameIdentifier.of("a", "b", "c", "d");

    // Test metalake
    assertThrows(
        IllegalNameIdentifierException.class, () -> NameIdentifierUtil.checkMetalake(null));
    Throwable excep =
        assertThrows(IllegalNamespaceException.class, () -> NameIdentifierUtil.checkMetalake(abc));
    assertTrue(excep.getMessage().contains("Metalake namespace must be non-null and empty"));

    // test catalog
    assertThrows(IllegalNameIdentifierException.class, () -> NameIdentifierUtil.checkCatalog(null));
    Throwable excep1 =
        assertThrows(IllegalNamespaceException.class, () -> NameIdentifierUtil.checkCatalog(abc));
    assertTrue(excep1.getMessage().contains("Catalog namespace must be non-null and have 1 level"));

    // test schema
    assertThrows(IllegalNameIdentifierException.class, () -> NameIdentifierUtil.checkSchema(null));
    Throwable excep2 =
        assertThrows(IllegalNamespaceException.class, () -> NameIdentifierUtil.checkSchema(abcd));
    assertTrue(excep2.getMessage().contains("Schema namespace must be non-null and have 2 levels"));

    // test table
    assertThrows(IllegalNameIdentifierException.class, () -> NameIdentifierUtil.checkTable(null));
    Throwable excep3 =
        assertThrows(IllegalNamespaceException.class, () -> NameIdentifierUtil.checkTable(abc));
    assertTrue(excep3.getMessage().contains("Table namespace must be non-null and have 3 levels"));
  }
}
