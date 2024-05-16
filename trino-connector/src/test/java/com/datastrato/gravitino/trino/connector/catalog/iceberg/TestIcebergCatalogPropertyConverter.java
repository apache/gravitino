/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.iceberg;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata;
import com.datastrato.gravitino.catalog.property.PropertyConverter;
import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoCatalog;
import com.datastrato.gravitino.trino.connector.metadata.TestGravitinoCatalog;
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

  @Test
  @SuppressWarnings("unchecked")
  public void testBuildConnectorPropertiesWithHiveBackend() throws Exception {
    String name = "test_catalog";
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put("uri", "thrift://localhost:9083")
            .put("catalog-backend", "hive")
            .put("warehouse", "hdfs://tmp/warehouse")
            .put("unknown-key", "1")
            .put("trino.bypass.unknown-key", "1")
            .put("trino.bypass.iceberg.table-statistics-enabled", "true")
            .build();
    Catalog mockCatalog =
        TestGravitinoCatalog.mockCatalog(
            name, "lakehouse-iceberg", "test catalog", Catalog.Type.RELATIONAL, properties);
    IcebergConnectorAdapter adapter = new IcebergConnectorAdapter();

    Map<String, Object> stringObjectMap =
        adapter.buildInternalConnectorConfig(new GravitinoCatalog("test", mockCatalog));

    // test connector attributes
    Assert.assertEquals(stringObjectMap.get("connectorName"), "iceberg");

    Map<String, Object> propertiesMap = (Map<String, Object>) stringObjectMap.get("properties");

    // test converted properties
    Assert.assertEquals(propertiesMap.get("hive.metastore.uri"), "thrift://localhost:9083");
    Assert.assertEquals(propertiesMap.get("iceberg.catalog.type"), "hive_metastore");

    // test trino passing properties
    Assert.assertEquals(propertiesMap.get("iceberg.table-statistics-enabled"), "true");

    // test unknown properties
    Assert.assertNull(propertiesMap.get("hive.unknown-key"));
    Assert.assertNull(propertiesMap.get("trino.bypass.unknown-key"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBuildConnectorPropertiesWithMySqlBackEnd() throws Exception {
    String name = "test_catalog";
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put("uri", "jdbc:mysql://%s:3306/metastore_db?createDatabaseIfNotExist=true")
            .put("catalog-backend", "jdbc")
            .put("warehouse", "://tmp/warehouse")
            .put("jdbc-user", "root")
            .put("jdbc-password", "ds123")
            .put("jdbc-driver", "com.mysql.cj.jdbc.Driver")
            .put("unknown-key", "1")
            .put("trino.bypass.unknown-key", "1")
            .put("trino.bypass.iceberg.table-statistics-enabled", "true")
            .build();
    Catalog mockCatalog =
        TestGravitinoCatalog.mockCatalog(
            name, "lakehouse-iceberg", "test catalog", Catalog.Type.RELATIONAL, properties);
    IcebergConnectorAdapter adapter = new IcebergConnectorAdapter();

    Map<String, Object> stringObjectMap =
        adapter.buildInternalConnectorConfig(new GravitinoCatalog("test", mockCatalog));

    // test connector attributes
    Assert.assertEquals(stringObjectMap.get("connectorName"), "iceberg");

    Map<String, Object> propertiesMap = (Map<String, Object>) stringObjectMap.get("properties");

    // test converted properties
    Assert.assertEquals(
        propertiesMap.get("iceberg.jdbc-catalog.connection-url"),
        "jdbc:mysql://%s:3306/metastore_db?createDatabaseIfNotExist=true");
    Assert.assertEquals(propertiesMap.get("iceberg.jdbc-catalog.connection-user"), "root");
    Assert.assertEquals(propertiesMap.get("iceberg.jdbc-catalog.connection-password"), "ds123");
    Assert.assertEquals(
        propertiesMap.get("iceberg.jdbc-catalog.driver-class"), "com.mysql.cj.jdbc.Driver");
    Assert.assertEquals(propertiesMap.get("iceberg.catalog.type"), "jdbc");

    // test trino passing properties
    Assert.assertEquals(propertiesMap.get("iceberg.table-statistics-enabled"), "true");

    // test unknown properties
    Assert.assertNull(propertiesMap.get("hive.unknown-key"));
    Assert.assertNull(propertiesMap.get("trino.bypass.unknown-key"));
  }
}
