/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.hive;

import com.datastrato.gravitino.shaded.org.apache.commons.collections4.bidimap.TreeBidiMap;
import com.datastrato.gravitino.trino.connector.catalog.PropertyConverter;

public class HiveSchemaPropertyConverter extends PropertyConverter {

  @Override
  public TreeBidiMap<String, String> trinoPropertyKeyToGravitino() {
    return new TreeBidiMap<>();
  }
}
