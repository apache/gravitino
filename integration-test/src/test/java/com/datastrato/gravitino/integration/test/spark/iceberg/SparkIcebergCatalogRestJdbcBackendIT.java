package com.datastrato.gravitino.integration.test.spark.iceberg;

import com.datastrato.gravitino.auxiliary.AuxiliaryServiceManager;
import com.datastrato.gravitino.spark.connector.iceberg.IcebergPropertiesConstants;
import com.google.common.collect.Maps;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;

@Tag("gravitino-docker-it")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SparkIcebergCatalogRestJdbcBackendIT extends SparkIcebergCatalogIT {

  @Override
  protected Map<String, String> getCatalogConfigs() {
    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND_REST);
    catalogProperties.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI, icebergRestServiceUri);
    catalogProperties.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE, warehouse);

    catalogProperties.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + icebergRestServiceName
            + "."
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND_JDBC);
    catalogProperties.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + icebergRestServiceName
            + "."
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI,
        jdbcUrl);
    catalogProperties.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + icebergRestServiceName
            + "."
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_JDBC_USER,
        jdbcUsername);
    catalogProperties.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + icebergRestServiceName
            + "."
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_JDBC_PASSWORD,
        jdbcPassword);
    catalogProperties.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + icebergRestServiceName
            + "."
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_JDBC_DRIVER,
        "com.mysql.cj.jdbc.Driver");
    catalogProperties.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + icebergRestServiceName
            + "."
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_JDBC_INITIALIZE,
        "true");
    catalogProperties.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + icebergRestServiceName
            + "."
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE,
        warehouse.replace("warehouse", "jdbc_warehouse"));

    return catalogProperties;
  }
}
