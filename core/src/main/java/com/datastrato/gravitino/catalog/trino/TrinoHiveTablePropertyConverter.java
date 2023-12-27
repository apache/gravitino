/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.trino;

import com.datastrato.gravitino.catalog.PropertyConverter;
import com.datastrato.gravitino.catalog.PropertyEntry;
import org.apache.commons.collections4.bidimap.TreeBidiMap;

public class TrinoHiveTablePropertyConverter extends PropertyConverter {

  @Override
  public TreeBidiMap<PropertyEntry<?>, PropertyEntry<?>> gravitinoToEngineProperty() {
    // Get Hive properties set from HiveTablePropertiesMetadata???
    return new TreeBidiMap<>();
  }
}
