/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.iceberg;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata;
import com.datastrato.gravitino.shaded.com.google.common.collect.ImmutableMap;
import com.datastrato.gravitino.shaded.com.google.common.collect.ImmutableSet;
import com.datastrato.gravitino.spark.connector.PropertiesConverter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

/** Transform Iceberg catalog properties between Spark and Gravitino. */
public class IcebergPropertiesConverter implements PropertiesConverter {

  private static final Set<String> RESERVED_PROPERTIES =
      ImmutableSet.of(
          IcebergTablePropertiesMetadata.LOCATION, IcebergTablePropertiesMetadata.PROVIDER);

  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, String> properties) {
    return rebuildCreateProperties(properties);
  }

  @Override
  public Map<String, String> toSparkTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }

  private Map<String, String> rebuildCreateProperties(Map<String, String> createProperties) {
    ImmutableMap.Builder<String, String> tableProperties = ImmutableMap.builder();
    createProperties.entrySet().stream()
        .filter(entry -> !RESERVED_PROPERTIES.contains(entry.getKey()))
        .forEach(tableProperties::put);

    String provider = createProperties.get(IcebergTablePropertiesMetadata.PROVIDER);
    if (IcebergPropertiesConstants.SPARK_ICEBERG_PARQUET_FORMAT.equalsIgnoreCase(provider)) {
      tableProperties.put(
          IcebergPropertiesConstants.SPARK_ICEBERG_DEFAULT_FILE_FORMAT,
          IcebergPropertiesConstants.SPARK_ICEBERG_PARQUET_FORMAT);
    } else if (IcebergPropertiesConstants.SPARK_ICEBERG_AVRO_FORMAT.equalsIgnoreCase(provider)) {
      tableProperties.put(
          IcebergPropertiesConstants.SPARK_ICEBERG_DEFAULT_FILE_FORMAT,
          IcebergPropertiesConstants.SPARK_ICEBERG_AVRO_FORMAT);
    } else if (IcebergPropertiesConstants.SPARK_ICEBERG_ORC_FORMAT.equalsIgnoreCase(provider)) {
      tableProperties.put(
          IcebergPropertiesConstants.SPARK_ICEBERG_DEFAULT_FILE_FORMAT,
          IcebergPropertiesConstants.SPARK_ICEBERG_ORC_FORMAT);
    } else if (provider != null
        && !IcebergPropertiesConstants.SPARK_ICEBERG_DEFAULT_PROVIDER.equalsIgnoreCase(provider)) {
      throw new IllegalArgumentException("Unsupported format in USING: " + provider);
    }

    String localtion = createProperties.get(IcebergTablePropertiesMetadata.LOCATION);
    if (StringUtils.isNotBlank(localtion)) {
      tableProperties.put(IcebergTablePropertiesMetadata.LOCATION, localtion);
    }

    return tableProperties.build();
  }
}
