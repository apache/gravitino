/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog;

/** This interface represents entities that have property metadata. */
public interface HasPropertyMetadata {

  /** Returns the table property metadata. */
  PropertiesMetadata tablePropertiesMetadata();

  /** Returns the catalog property metadata. */
  PropertiesMetadata catalogPropertiesMetadata();
}
