package com.datastrato.graviton;

public interface HasIdentifier {

  /**
   * Return the name of the entity.
   *
   * @return A String with the name of the entity.
   */
  String name();

  /**
   * Returns the name identifier of the entity.
   *
   * @return NameIdentifier of the entity.
   */
  default NameIdentifier nameIdentifier(Namespace namespace) {
    return NameIdentifier.of(namespace, name());
  }

  // TODO. Returns a binary compact unique identifier of the entity. @Jerry
}
