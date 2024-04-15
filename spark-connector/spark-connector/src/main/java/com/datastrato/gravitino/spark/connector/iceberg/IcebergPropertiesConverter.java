/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.iceberg;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata;
import com.datastrato.gravitino.spark.connector.PropertiesConverter;
import java.util.HashMap;
import java.util.Map;

/** Transform Iceberg catalog properties between Spark and Gravitino. */
public class IcebergPropertiesConverter implements PropertiesConverter {
  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }

  @Override
  public Map<String, String> toSparkTableProperties(
      Map<String, String> gravitinoProperties, Map<String, String> sparkProperties) {
    Map<String, String> sparkTableProperties = new HashMap<>();
    if (gravitinoProperties != null) {
      gravitinoProperties.remove(IcebergPropertiesConstants.GRAVITINO_ID_KEY);
      sparkTableProperties.putAll(gravitinoProperties);
    }
    if (sparkProperties != null) {
      if (sparkProperties.containsKey(IcebergPropertiesConstants.GRAVITINO_ICEBERG_FILE_FORMAT)) {
        sparkTableProperties.put(
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_FILE_FORMAT,
            sparkProperties.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_FILE_FORMAT));
      }

      if (sparkProperties.containsKey(IcebergTablePropertiesMetadata.PROVIDER)) {
        sparkTableProperties.put(
            IcebergTablePropertiesMetadata.PROVIDER,
            sparkProperties.get(IcebergTablePropertiesMetadata.PROVIDER));
      }

      if (sparkProperties.containsKey(IcebergTablePropertiesMetadata.CURRENT_SNAPSHOT_ID)) {
        sparkTableProperties.put(
            IcebergTablePropertiesMetadata.CURRENT_SNAPSHOT_ID,
            sparkProperties.get(IcebergTablePropertiesMetadata.CURRENT_SNAPSHOT_ID));
      }

      if (sparkProperties.containsKey(IcebergTablePropertiesMetadata.LOCATION)) {
        sparkTableProperties.put(
            IcebergTablePropertiesMetadata.LOCATION,
            sparkProperties.get(IcebergTablePropertiesMetadata.LOCATION));
      }

      if (sparkProperties.containsKey(
          IcebergPropertiesConstants.GRAVITINO_ICEBERG_FORMAT_VERSION)) {
        sparkTableProperties.put(
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_FORMAT_VERSION,
            sparkProperties.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_FORMAT_VERSION));
      }

      if (sparkProperties.containsKey(IcebergTablePropertiesMetadata.SORT_ORDER)) {
        sparkTableProperties.put(
            IcebergTablePropertiesMetadata.SORT_ORDER,
            sparkProperties.get(IcebergTablePropertiesMetadata.SORT_ORDER));
      }

      if (sparkProperties.containsKey(IcebergTablePropertiesMetadata.IDENTIFIER_FIELDS)) {
        sparkTableProperties.put(
            IcebergTablePropertiesMetadata.IDENTIFIER_FIELDS,
            sparkProperties.get(IcebergTablePropertiesMetadata.IDENTIFIER_FIELDS));
      }
    }
    return sparkTableProperties;
  }
}
