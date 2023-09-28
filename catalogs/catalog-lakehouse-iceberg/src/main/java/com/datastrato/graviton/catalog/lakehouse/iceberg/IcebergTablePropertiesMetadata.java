/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg;

import com.datastrato.graviton.catalog.AbstractPropertiesMetadata;
import com.datastrato.graviton.catalog.PropertyEntry;
import com.google.common.collect.Maps;
import java.util.Map;

public class IcebergTablePropertiesMetadata extends AbstractPropertiesMetadata {
  @Override
  protected Map<String, PropertyEntry<?>> properties() {
    // TODO: support Iceberg table property specs
    return Maps.newHashMap();
  }
}
