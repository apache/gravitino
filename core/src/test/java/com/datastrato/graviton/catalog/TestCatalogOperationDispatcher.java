/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastrato.graviton.NameIdentifier;
import org.junit.jupiter.api.Test;

public class TestCatalogOperationDispatcher {
  @Test
  public void testGetCatalogIdentifier() {
    CatalogOperationDispatcher dispatcher = new CatalogOperationDispatcher(null);

    NameIdentifier id1 = NameIdentifier.of("a");
    assertThrows(IllegalArgumentException.class, () -> dispatcher.getCatalogIdentifier(id1));

    NameIdentifier id2 = NameIdentifier.of("a", "b");
    assertEquals(dispatcher.getCatalogIdentifier(id2), NameIdentifier.of("a", "b"));

    NameIdentifier id3 = NameIdentifier.of("a", "b", "c");
    assertEquals(dispatcher.getCatalogIdentifier(id3), NameIdentifier.of("a", "b"));

    NameIdentifier id4 = NameIdentifier.of("a", "b", "c", "d");
    assertEquals(dispatcher.getCatalogIdentifier(id4), NameIdentifier.of("a", "b"));

    NameIdentifier id5 = NameIdentifier.of("a", "b", "c", "d", "e");
    assertEquals(dispatcher.getCatalogIdentifier(id5), NameIdentifier.of("a", "b"));
  }
}
