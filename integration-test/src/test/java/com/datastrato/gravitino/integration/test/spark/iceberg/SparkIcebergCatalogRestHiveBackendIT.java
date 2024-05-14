/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.spark.iceberg;

import com.datastrato.gravitino.auxiliary.AuxiliaryServiceManager;
import com.datastrato.gravitino.spark.connector.iceberg.IcebergPropertiesConstants;
import com.google.common.collect.Maps;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;

/** This class use Iceberg RESTCatalog for test, and the real backend catalog is HiveCatalog. */
@Tag("gravitino-docker-it")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SparkIcebergCatalogRestHiveBackendIT extends SparkIcebergCatalogIT {

  private static final String icebergRestServiceName = "iceberg-rest";

  @Override
  protected Map<String, String> getCatalogConfigs() {
    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_REST);
    catalogProperties.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI, icebergRestServiceUri);
    catalogProperties.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE, warehouse);

    catalogProperties.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + icebergRestServiceName
            + "."
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_HIVE);
    catalogProperties.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + icebergRestServiceName
            + "."
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI,
        hiveMetastoreUri);
    catalogProperties.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + icebergRestServiceName
            + "."
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE,
        warehouse);

    return catalogProperties;
  }
}
