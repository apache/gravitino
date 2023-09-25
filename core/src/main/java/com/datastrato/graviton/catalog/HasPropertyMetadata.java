/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog;

import com.datastrato.graviton.PropertyMetadata;

public interface HasPropertyMetadata {
  PropertyMetadata tableProperty() throws UnsupportedOperationException;
}
