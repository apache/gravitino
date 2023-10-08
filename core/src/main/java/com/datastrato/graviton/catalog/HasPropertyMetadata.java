/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog;

/** This interface represents entities that have property metadata. */
public interface HasPropertyMetadata {

  /**
   * Returns the table property metadata.
   *
   * @return The table property metadata.
   * @throws UnsupportedOperationException if the entity does not support table properties.
   */
  PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException;

  /**
   * Returns the catalog property metadata.
   *
   * @return The catalog property metadata.
   * @throws UnsupportedOperationException if the entity does not support catalog properties.
   */
  PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException;
}
