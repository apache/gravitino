/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.iceberg;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata;
import com.datastrato.gravitino.catalog.property.PropertyConverter;
import com.google.common.collect.Sets;
import io.trino.spi.TrinoException;
import java.util.Map;
import java.util.Set;
import org.assertj.core.api.Assertions;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

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
        propertyConverter.gravitinoToEngineProperties(gravitinoIcebergConfig);

    Assert.assertEquals(hiveBackendConfig.get("iceberg.catalog.type"), "hive_metastore");
    Assert.assertEquals(hiveBackendConfig.get("hive.metastore.uri"), "1111");

    Map<String, String> wrongMap = Maps.newHashMap(gravitinoIcebergConfig);
    wrongMap.remove("uri");

    Assertions.assertThatThrownBy(() -> propertyConverter.gravitinoToEngineProperties(wrongMap))
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
        propertyConverter.gravitinoToEngineProperties(gravitinoIcebergConfig);

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

    Assertions.assertThatThrownBy(() -> propertyConverter.gravitinoToEngineProperties(wrongMap))
        .isInstanceOf(TrinoException.class)
        .hasMessageContaining("Missing required property for JDBC backend: [jdbc-driver]");
  }

  // To test whether we load jar `bundled-catalog` successfully.
  @Test
  public void testPropertyMetadata() {
    Set<String> gravitinoHiveKeys =
        Sets.newHashSet(IcebergTablePropertyConverter.TRINO_KEY_TO_GRAVITINO_KEY.values());
    Set<String> actualGravitinoKeys =
        Sets.newHashSet(new IcebergTablePropertiesMetadata().propertyEntries().keySet());

    Assert.assertTrue(actualGravitinoKeys.containsAll(gravitinoHiveKeys));
  }
}
