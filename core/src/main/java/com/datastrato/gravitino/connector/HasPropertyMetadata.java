/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.connector;

import com.datastrato.gravitino.annotation.Evolving;

/** This interface represents entities that have property metadata. */
@Evolving
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

  /**
   * Returns the schema property metadata.
   *
   * @return The schema property metadata.
   * @throws UnsupportedOperationException if the entity does not support schema properties.
   */
  PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException;

  /**
   * Returns the fileset property metadata.
   *
   * @return The fileset property metadata.
   * @throws UnsupportedOperationException if the entity does not support fileset properties.
   */
  PropertiesMetadata filesetPropertiesMetadata() throws UnsupportedOperationException;

  /**
   * Returns the topic property metadata.
   *
   * @return The topic property metadata.
   * @throws UnsupportedOperationException if the entity does not support topic properties.
   */
  PropertiesMetadata topicPropertiesMetadata() throws UnsupportedOperationException;
}
