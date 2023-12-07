/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

/** This interface represents entities that have identifiers. */
public interface HasIdentifier {

  /**
   * Get the name of the entity.
   *
   * @return The name of the entity.
   */
  String name();

  /**
   * Get the unique id of the entity.
   *
   * @return The unique id of the entity.
   */
  Long id();

  /**
   * Get the namespace of the entity.
   *
   * @return The namespace of the entity.
   */
  default Namespace namespace() {
    return Namespace.empty();
  }

  /**
   * Get the name identifier of the entity.
   *
   * @return The name identifier of the entity.
   */
  default NameIdentifier nameIdentifier() {
    return NameIdentifier.of(namespace(), name());
  }
}
