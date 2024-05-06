/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.iceberg;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergPropertiesConverter {
  private final IcebergPropertiesConverter icebergPropertiesConverter =
      IcebergPropertiesConverter.getInstance();

  @Test
  void testCatalogPropertiesWithHiveBackend() {
    Map<String, String> properties =
        icebergPropertiesConverter.transformSparkCatalogProperties(
            ImmutableMap.of(
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND_HIVE,
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI,
                "hive-uri",
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE,
                "hive-warehouse",
                "key1",
                "value1"));
    Assertions.assertEquals(
        ImmutableMap.of(
            IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE,
            IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_HIVE,
            IcebergPropertiesConstants.ICEBERG_CATALOG_URI,
            "hive-uri",
            IcebergPropertiesConstants.ICEBERG_CATALOG_WAREHOUSE,
            "hive-warehouse"),
        properties);
  }

  @Test
  void testCatalogPropertiesWithJdbcBackend() {
    Map<String, String> properties =
        icebergPropertiesConverter.transformSparkCatalogProperties(
            ImmutableMap.of(
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
                IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_JDBC,
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI,
                "jdbc-uri",
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE,
                "jdbc-warehouse",
                IcebergPropertiesConstants.GRAVITINO_JDBC_USER,
                "user",
                IcebergPropertiesConstants.GRAVITINO_JDBC_PASSWORD,
                "passwd",
                "key1",
                "value1"));
    Assertions.assertEquals(
        ImmutableMap.of(
            IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE,
            IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_JDBC,
            IcebergPropertiesConstants.ICEBERG_CATALOG_URI,
            "jdbc-uri",
            IcebergPropertiesConstants.ICEBERG_CATALOG_WAREHOUSE,
            "jdbc-warehouse",
            IcebergPropertiesConstants.ICEBERG_JDBC_USER,
            "user",
            IcebergPropertiesConstants.ICEBERG_JDBC_PASSWORD,
            "passwd"),
        properties);
  }
}
