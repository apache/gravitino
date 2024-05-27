/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.iceberg;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTablePropertiesMetadata;
import com.datastrato.gravitino.catalog.property.PropertyConverter;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoCatalog;
import com.datastrato.gravitino.trino.connector.metadata.TestGravitinoCatalog;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.trino.spi.TrinoException;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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

    Assertions.assertEquals(hiveBackendConfig.get("iceberg.catalog.type"), "hive_metastore");
    Assertions.assertEquals(hiveBackendConfig.get("hive.metastore.uri"), "1111");

    Map<String, String> wrongMap = Maps.newHashMap(gravitinoIcebergConfig);
    wrongMap.remove("uri");

    Assertions.assertThrows(
        TrinoException.class,
        () -> propertyConverter.gravitinoToEngineProperties(wrongMap),
        "Missing required property for Hive backend: [uri]");
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
    Assertions.assertEquals(
        hiveBackendConfig.get("iceberg.jdbc-catalog.connection-url"),
        "jdbc:mysql://127.0.0.1:3306/metastore_db?createDatabaseIfNotExist=true");
    Assertions.assertEquals(
        hiveBackendConfig.get("iceberg.jdbc-catalog.connection-user"), "zhangsan");
    Assertions.assertEquals(
        hiveBackendConfig.get("iceberg.jdbc-catalog.connection-password"), "lisi");
    Assertions.assertNull(hiveBackendConfig.get("other-key"));
    Assertions.assertEquals(hiveBackendConfig.get("iceberg.catalog.type"), "jdbc");
    Assertions.assertEquals(
        hiveBackendConfig.get("iceberg.jdbc-catalog.driver-class"), "com.mysql.cj.jdbc.Driver");

    Map<String, String> wrongMap = Maps.newHashMap(gravitinoIcebergConfig);
    wrongMap.remove("jdbc-driver");

    Assertions.assertThrows(
        TrinoException.class,
        () -> propertyConverter.gravitinoToEngineProperties(wrongMap),
        "Missing required property for JDBC backend: [jdbc-driver]");
  }

  // To test whether we load jar `bundled-catalog` successfully.
  @Test
  public void testPropertyMetadata() {
    Set<String> gravitinoHiveKeys =
        Sets.newHashSet(IcebergTablePropertyConverter.TRINO_KEY_TO_GRAVITINO_KEY.values());
    Set<String> actualGravitinoKeys =
        Sets.newHashSet(new IcebergTablePropertiesMetadata().propertyEntries().keySet());

    Assertions.assertTrue(actualGravitinoKeys.containsAll(gravitinoHiveKeys));
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

    Map<String, String> config =
        adapter.buildInternalConnectorConfig(new GravitinoCatalog("test", mockCatalog));

    // test converted properties
    Assertions.assertEquals(config.get("hive.metastore.uri"), "thrift://localhost:9083");
    Assertions.assertEquals(config.get("iceberg.catalog.type"), "hive_metastore");

    // test trino passing properties
    Assertions.assertEquals(config.get("iceberg.table-statistics-enabled"), "true");

    // test unknown properties
    Assertions.assertNull(config.get("hive.unknown-key"));
    Assertions.assertNull(config.get("trino.bypass.unknown-key"));
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

    Map<String, String> config =
        adapter.buildInternalConnectorConfig(new GravitinoCatalog("test", mockCatalog));

    // test converted properties
    Assertions.assertEquals(
        config.get("iceberg.jdbc-catalog.connection-url"),
        "jdbc:mysql://%s:3306/metastore_db?createDatabaseIfNotExist=true");
    Assertions.assertEquals(config.get("iceberg.jdbc-catalog.connection-user"), "root");
    Assertions.assertEquals(config.get("iceberg.jdbc-catalog.connection-password"), "ds123");
    Assertions.assertEquals(
        config.get("iceberg.jdbc-catalog.driver-class"), "com.mysql.cj.jdbc.Driver");
    Assertions.assertEquals(config.get("iceberg.catalog.type"), "jdbc");

    // test trino passing properties
    Assertions.assertEquals(config.get("iceberg.table-statistics-enabled"), "true");

    // test unknown properties
    Assertions.assertNull(config.get("hive.unknown-key"));
    Assertions.assertNull(config.get("trino.bypass.unknown-key"));
  }
}
