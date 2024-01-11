/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.iceberg;

import com.datastrato.catalog.common.property.PropertyConverter;
import com.datastrato.gravitino.catalog.BasePropertiesMetadata;
import com.datastrato.gravitino.catalog.PropertyEntry;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata;
import com.datastrato.gravitino.shaded.org.apache.commons.collections4.bidimap.TreeBidiMap;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class IcebergTablePropertyConverter extends PropertyConverter {

  private final BasePropertiesMetadata icebergTablePropertiesMetadata =
      new IcebergTablePropertiesMetadata();

  // TODO (yuqi) add more properties
  private static final TreeBidiMap<String, String> TRINO_KEY_TO_GRAVITINO_KEY =
      new TreeBidiMap<>(new ImmutableMap.Builder<String, String>().build());

  @Override
  public TreeBidiMap<String, String> engineToGravitino() {
    // Note: As the properties for Iceberg table loaded from Gravitino are always empty currently,
    // no matter what the mapping is, the properties will be empty.
    return TRINO_KEY_TO_GRAVITINO_KEY;
  }

  @Override
  public Map<String, PropertyEntry<?>> gravitinoPropertyMeta() {
    return icebergTablePropertiesMetadata.propertyEntries();
  }
}
