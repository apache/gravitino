/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.iceberg;

import com.datastrato.catalog.common.property.PropertyConverter;
import com.datastrato.gravitino.catalog.PropertyEntry;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata;
import io.trino.spi.TrinoException;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

public class TestIcebergCatalogPropertyConverter {

  @Test
  public void testHiveBackendProperty() {
    PropertyConverter propertyConverterDeprecated = new IcebergCatalogPropertyConverter();
    Map<String, String> gravitinoIcebergConfig =
        ImmutableMap.<String, String>builder()
            .put("uri", "1111")
            .put("catalog-backend", "hive")
            .build();
    Map<String, String> hiveBackendConfig =
        propertyConverterDeprecated.fromGravitinoProperties(gravitinoIcebergConfig);

    Assert.assertEquals(hiveBackendConfig.get("iceberg.catalog.type"), "hive_metastore");
    Assert.assertEquals(hiveBackendConfig.get("hive.metastore.uri"), "1111");

    Map<String, String> wrongMap = Maps.newHashMap(gravitinoIcebergConfig);
    wrongMap.remove("uri");

    Assertions.assertThatThrownBy(
            () -> propertyConverterDeprecated.fromGravitinoProperties(wrongMap))
        .isInstanceOf(TrinoException.class)
        .hasMessageContaining("Missing required property for Hive backend: [uri]");
  }

  @Test
  public void testJDBCBackendProperty() {
    PropertyConverter propertyConverterDeprecated = new IcebergCatalogPropertyConverter();
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
        propertyConverterDeprecated.fromGravitinoProperties(gravitinoIcebergConfig);

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

    Assertions.assertThatThrownBy(
            () -> propertyConverterDeprecated.fromGravitinoProperties(wrongMap))
        .isInstanceOf(TrinoException.class)
        .hasMessageContaining("Missing required property for JDBC backend: [jdbc-driver]");
  }

  @Test
  // To test whether we can load property metadata from IcebergTablePropertiesMetadata successfully.
  public void testPropertyMetadata() {
    for (Map.Entry<String, PropertyEntry<?>> entryEntry :
        IcebergTablePropertiesMetadata.PROPERTIES_METADATA.entrySet()) {
      System.out.println(entryEntry.getKey() + " " + entryEntry.getValue());
    }
  }
}
