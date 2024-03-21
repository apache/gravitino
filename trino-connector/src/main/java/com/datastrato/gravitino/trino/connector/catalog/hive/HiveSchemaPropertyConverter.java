/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.hive;

import static com.datastrato.gravitino.trino.connector.catalog.hive.HivePropertyMeta.HIVE_SCHEMA_LOCATION;

import com.datastrato.gravitino.catalog.hive.HiveSchemaPropertiesMetadata;
import com.datastrato.gravitino.catalog.property.PropertyConverter;
import com.datastrato.gravitino.connector.BasePropertiesMetadata;
import com.datastrato.gravitino.connector.PropertyEntry;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.collections4.bidimap.TreeBidiMap;

public class HiveSchemaPropertyConverter extends PropertyConverter {
  private final BasePropertiesMetadata hiveSchemaPropertiesMetadata =
      new HiveSchemaPropertiesMetadata();

  // Trino property key does not allow upper case character and '-', so we need to map it to
  // Gravitino
  private static final TreeBidiMap<String, String> TRINO_KEY_TO_GRAVITINO_KEY =
      new TreeBidiMap<>(
          new ImmutableMap.Builder<String, String>()
              .put(HIVE_SCHEMA_LOCATION, HiveSchemaPropertiesMetadata.LOCATION)
              .build());

  @Override
  public TreeBidiMap<String, String> engineToGravitinoMapping() {
    return TRINO_KEY_TO_GRAVITINO_KEY;
  }

  @Override
  public Map<String, PropertyEntry<?>> gravitinoPropertyMeta() {
    return hiveSchemaPropertiesMetadata.propertyEntries();
  }
}
