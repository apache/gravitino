/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.iceberg;

import com.datastrato.gravitino.catalog.PropertyEntry;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata;
import com.datastrato.gravitino.trino.connector.catalog.PropertyConverter;
import io.trino.spi.TrinoException;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

public class TestIcebergCatalogPropertyConverter {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestIcebergCatalogPropertyConverter.class);

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

    Map<String, String> wrongMap = Maps.newHashMap(gravitinoIcebergConfig);
    wrongMap.remove("uri");

    Assertions.assertThatThrownBy(() -> propertyConverter.toTrinoProperties(wrongMap))
        .isInstanceOf(TrinoException.class)
        .hasMessageContaining("Missing required property for Hive backend: [uri]");
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
            .put("jdbc-driver", "com.mysql.cj.jdbc.Driver")
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

    Map<String, String> wrongMap = Maps.newHashMap(gravitinoIcebergConfig);
    wrongMap.remove("jdbc-driver");

    Assertions.assertThatThrownBy(() -> propertyConverter.toTrinoProperties(wrongMap))
        .isInstanceOf(TrinoException.class)
        .hasMessageContaining("Missing required property for JDBC backend: [jdbc-driver]");
  }

  // To test whether we load jar `bundled-catalog` successfully.
  @Test
  public void testPropertyMetadata() {
    for (Map.Entry<String, PropertyEntry<?>> entryEntry :
        new IcebergTablePropertiesMetadata().propertyEntries().entrySet()) {
      System.out.println(entryEntry.getKey() + " " + entryEntry.getValue());
    }
  }
}
