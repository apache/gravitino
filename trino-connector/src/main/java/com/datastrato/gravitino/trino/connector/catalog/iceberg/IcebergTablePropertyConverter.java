/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.iceberg;

import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata.LOCATION;
import static com.datastrato.gravitino.trino.connector.catalog.iceberg.IcebergPropertyMeta.ICEBERG_LOCATION_PROPERTY;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata;
import com.datastrato.gravitino.catalog.property.PropertyConverter;
import com.datastrato.gravitino.connector.BasePropertiesMetadata;
import com.datastrato.gravitino.connector.PropertyEntry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.collections4.bidimap.TreeBidiMap;

public class IcebergTablePropertyConverter extends PropertyConverter {

  private final BasePropertiesMetadata icebergTablePropertiesMetadata =
      new IcebergTablePropertiesMetadata();

  // TODO (yuqi) add more properties
  @VisibleForTesting
  static final TreeBidiMap<String, String> TRINO_KEY_TO_GRAVITINO_KEY =
      new TreeBidiMap<>(
          new ImmutableMap.Builder<String, String>()
              .put(ICEBERG_LOCATION_PROPERTY, LOCATION)
              .build());

  @Override
  public TreeBidiMap<String, String> engineToGravitinoMapping() {
    // Note: As the properties for Iceberg table loaded from Gravitino are always empty currently,
    // no matter what the mapping is, the properties will be empty.
    return TRINO_KEY_TO_GRAVITINO_KEY;
  }

  @Override
  public Map<String, PropertyEntry<?>> gravitinoPropertyMeta() {
    return icebergTablePropertiesMetadata.propertyEntries();
  }
}
