package com.datastrato.graviton.catalog;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.exceptions.IllegalNameIdentifierException;
import com.datastrato.graviton.exceptions.IllegalNamespaceException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCatalogOperationDispatcher {
  @Test
  public void testGetCatalogIdentifier() {
    CatalogOperationDispatcher dispatcher = new CatalogOperationDispatcher(null);

    NameIdentifier id1 = NameIdentifier.of("a");
    assertThrows(IllegalNamespaceException.class, () -> dispatcher.getCatalogIdentifier(id1));

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
