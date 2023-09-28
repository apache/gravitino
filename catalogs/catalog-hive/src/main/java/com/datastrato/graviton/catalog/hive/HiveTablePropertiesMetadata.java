/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.hive;

import com.datastrato.graviton.catalog.AbstractPropertiesMetadata;
import com.datastrato.graviton.catalog.PropertyEntry;
import com.google.common.collect.Maps;
import java.util.Map;

public class HiveTablePropertiesMetadata extends AbstractPropertiesMetadata {
  @Override
  protected Map<String, PropertyEntry<?>> propertyMetas() {
    // TODO(Minghuang): support Hive table property specs
    return Maps.newHashMap();
  }
}
