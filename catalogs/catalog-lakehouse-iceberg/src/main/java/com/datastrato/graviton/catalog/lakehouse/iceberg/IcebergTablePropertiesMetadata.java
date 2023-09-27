/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg;

import com.datastrato.graviton.catalog.PropertyEntry;
import com.datastrato.graviton.catalog.TablePropertiesMetadata;
import com.google.common.collect.Maps;
import java.util.Map;

public class IcebergTablePropertiesMetadata extends TablePropertiesMetadata {
  @Override
  protected Map<String, PropertyEntry<?>> tablePropertyEntries() {
    // TODO: support Iceberg table property specs
    return Maps.newHashMap();
  }
}
