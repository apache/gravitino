/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.iceberg;

import com.datastrato.gravitino.trino.connector.catalog.PropertyConverter;
import java.util.Map;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestIcebergCatalogPropertyConverter {

  @Test
  public void testHiveBackendProperty() {
    PropertyConverter propertyConverter = new IcebergCatalogPropertyConverter();
    Map<String, String> gravitinoIcebergConfig =
        ImmutableMap.<String, String>builder()
            .put("uri", "1111")
            .put("catalog-backend", "hive")
            .build();
    Map<String, String> hiveBackendConfig =
        propertyConverter.toTrinoProperties(gravitinoIcebergConfig);

    Assert.assertEquals(hiveBackendConfig.get("iceberg.catalog.type"), "hive_metastore");
    Assert.assertEquals(hiveBackendConfig.get("hive.metastore.uri"), "1111");
  }

  @Test
  public void testJDBCBackendProperty() {
    PropertyConverter propertyConverter = new IcebergCatalogPropertyConverter();
    Map<String, String> gravitinoIcebergConfig =
        ImmutableMap.<String, String>builder()
            .put("uri", "jdbc:mysql://127.0.0.1:3306/metastore_db?createDatabaseIfNotExist=true")
            .put("catalog-backend", "jdbc")
            .put("jdbc-user", "zhangsan")
            .put("jdbc-password", "lisi")
            .put("other-key", "other")
            .build();
    Map<String, String> hiveBackendConfig =
        propertyConverter.toTrinoProperties(gravitinoIcebergConfig);

    // Test all properties are converted
    Assert.assertEquals(
        hiveBackendConfig.get("iceberg.jdbc-catalog.connection-url"),
        "jdbc:mysql://127.0.0.1:3306/metastore_db?createDatabaseIfNotExist=true");
    Assert.assertEquals(hiveBackendConfig.get("iceberg.jdbc-catalog.connection-user"), "zhangsan");
    Assert.assertEquals(hiveBackendConfig.get("iceberg.jdbc-catalog.connection-password"), "lisi");
    Assert.assertNull(hiveBackendConfig.get("other-key"));
    Assert.assertEquals(hiveBackendConfig.get("iceberg.catalog.type"), "jdbc");
    Assert.assertEquals(
        hiveBackendConfig.get("iceberg.jdbc-catalog.driver-class"), "com.mysql.cj.jdbc.Driver");
  }
}
