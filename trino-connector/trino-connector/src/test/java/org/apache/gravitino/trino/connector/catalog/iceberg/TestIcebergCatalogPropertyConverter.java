/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.trino.connector.catalog.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.trino.spi.TrinoException;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.property.PropertyConverter;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;
import org.apache.gravitino.trino.connector.metadata.TestGravitinoCatalog;
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
            .put("trino.bypass.iceberg.unknown-key", "1")
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
    Assertions.assertNull(config.get("unknown-key"));
    Assertions.assertEquals(config.get("iceberg.unknown-key"), "1");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBuildConnectorPropertiesWithMySqlBackEnd() throws Exception {
    String name = "test_catalog";
    // trino.bypass properties will be skipped when the catalog properties is defined by Gravitino
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put("trino.bypass.iceberg.jdbc-catalog.connection-url", "skip_value")
            .put("uri", "jdbc:mysql://%s:3306/metastore_db?createDatabaseIfNotExist=true")
            .put("catalog-backend", "jdbc")
            .put("warehouse", "://tmp/warehouse")
            .put("jdbc-user", "root")
            .put("jdbc-password", "ds123")
            .put("jdbc-driver", "com.mysql.cj.jdbc.Driver")
            .put("unknown-key", "1")
            .put("trino.bypass.iceberg.unknown-key", "1")
            .put("trino.bypass.iceberg.table-statistics-enabled", "true")
            .put("trino.bypass.iceberg.jdbc-catalog.connection-user", "skip_value")
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
    Assertions.assertNull(config.get("unknown-key"));
    Assertions.assertEquals(config.get("iceberg.unknown-key"), "1");
  }
}
