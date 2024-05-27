/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.spark.connector.integration.test.iceberg;

import com.datastrato.gravitino.spark.connector.iceberg.IcebergPropertiesConstants;
import com.google.common.collect.Maps;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;

/** This class use Iceberg HiveCatalog for backend catalog. */
@Tag("gravitino-docker-it")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class SparkIcebergCatalogHiveBackendIT extends SparkIcebergCatalogIT {

  @Override
  protected Map<String, String> getCatalogConfigs() {
    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_HIVE);
    catalogProperties.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE, warehouse);
    catalogProperties.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI, hiveMetastoreUri);
    return catalogProperties;
  }
}
