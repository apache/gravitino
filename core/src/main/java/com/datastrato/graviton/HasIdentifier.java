/*·Copyright·2023·Datastrato.·This·software·is·licensed·under·the·Apache·License·version·2.·*/
package com.datastrato.graviton;

public interface HasIdentifier {

  /** Return the name of the entity. */
  String name();

  /** Returns the namespace of the entity. */
  default Namespace namespace() {
    return Namespace.empty();
  }

  /** Returns the name identifier of the entity. */
  default NameIdentifier nameIdentifier() {
    return NameIdentifier.of(namespace(), name());
  }

  // TODO. Returns a binary compact unique identifier of the entity. @Jerry
}
