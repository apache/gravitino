/*·Copyright·2023·Datastrato.·This·software·is·licensed·under·the·Apache·License·version·2.·*/
package com.datastrato.graviton;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestNameIdentifier {

  @Test
  public void testCreateNameIdentifier() {
    NameIdentifier id = NameIdentifier.of("a", "b", "c");
    Assertions.assertEquals(Namespace.of("a", "b"), id.namespace());
    Assertions.assertEquals("c", id.name());

    NameIdentifier id1 = NameIdentifier.of(Namespace.of("a", "b"), "c");
    Assertions.assertEquals(Namespace.of("a", "b"), id1.namespace());
    Assertions.assertEquals("c", id1.name());

    NameIdentifier id2 = NameIdentifier.parse("a.b.c");
    Assertions.assertEquals(Namespace.of("a", "b"), id2.namespace());
    Assertions.assertEquals("c", id2.name());

    NameIdentifier id3 = NameIdentifier.parse("a");
    Assertions.assertEquals(Namespace.empty(), id3.namespace());
    Assertions.assertEquals("a", id3.name());
  }

  @Test
  public void testCreateWithInvalidArgs() {
    Assertions.assertThrows(IllegalArgumentException.class, NameIdentifier::of);
    Assertions.assertThrows(IllegalArgumentException.class, () -> NameIdentifier.of("a", null));
    Assertions.assertThrows(IllegalArgumentException.class, () -> NameIdentifier.of("a", ""));

    Assertions.assertThrows(IllegalArgumentException.class, () -> NameIdentifier.of(null, "a"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> NameIdentifier.of(Namespace.empty(), null));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> NameIdentifier.of(Namespace.empty(), ""));

    Assertions.assertThrows(IllegalArgumentException.class, () -> NameIdentifier.parse(null));
    Assertions.assertThrows(IllegalArgumentException.class, () -> NameIdentifier.parse(""));
    Assertions.assertThrows(IllegalArgumentException.class, () -> NameIdentifier.parse("a."));
    Assertions.assertThrows(IllegalArgumentException.class, () -> NameIdentifier.parse("a.."));
    Assertions.assertThrows(IllegalArgumentException.class, () -> NameIdentifier.parse(".a"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> NameIdentifier.parse("..a"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> NameIdentifier.parse("a..b"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> NameIdentifier.parse("a.b."));
    Assertions.assertThrows(IllegalArgumentException.class, () -> NameIdentifier.parse(".a.b"));
  }
}
