/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog;

public interface HasPropertyMetadata {
  PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException;
}
