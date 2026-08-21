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
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.trino.connector.GravitinoConfig;
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
  public void testRestBackendProperty() {
    PropertyConverter propertyConverter = new IcebergCatalogPropertyConverter();
    Map<String, String> gravitinoIcebergConfig =
        ImmutableMap.<String, String>builder()
            .put("uri", "http://localhost:9001/iceberg")
            .put("catalog-backend", "rest")
            .put("warehouse", "gt_iceberg_rest")
            .build();
    Map<String, String> restBackendConfig =
        propertyConverter.gravitinoToEngineProperties(gravitinoIcebergConfig);

    Assertions.assertEquals(restBackendConfig.get("iceberg.catalog.type"), "rest");
    Assertions.assertEquals(
        restBackendConfig.get("iceberg.rest-catalog.uri"), "http://localhost:9001/iceberg");
    Assertions.assertEquals(
        restBackendConfig.get("iceberg.rest-catalog.warehouse"), "gt_iceberg_rest");

    Map<String, String> wrongMap = Maps.newHashMap(gravitinoIcebergConfig);
    wrongMap.remove("uri");

    Assertions.assertThrows(
        TrinoException.class,
        () -> propertyConverter.gravitinoToEngineProperties(wrongMap),
        "Missing required property for Rest backend: [uri]");
  }

  @Test
  public void testJDBCBackendProperty() {
    PropertyConverter propertyConverter = new IcebergCatalogPropertyConverter();
    Map<String, String> gravitinoIcebergConfig =
        ImmutableMap.<String, String>builder()
            .put("uri", "jdbc:mysql://127.0.0.1:3306/metastore_db?createDatabaseIfNotExist=true")
            .put("catalog-backend", "jdbc")
            .put("jdbc-user", "jack")
            .put("jdbc-password", "alice")
            .put("jdbc-driver", "com.mysql.cj.jdbc.Driver")
            .put("other-key", "other")
            .build();
    Map<String, String> hiveBackendConfig =
        propertyConverter.gravitinoToEngineProperties(gravitinoIcebergConfig);

    // Test all properties are converted
    Assertions.assertEquals(
        hiveBackendConfig.get("iceberg.jdbc-catalog.connection-url"),
        "jdbc:mysql://127.0.0.1:3306/metastore_db?createDatabaseIfNotExist=true");
    Assertions.assertEquals(hiveBackendConfig.get("iceberg.jdbc-catalog.connection-user"), "jack");
    Assertions.assertEquals(
        hiveBackendConfig.get("iceberg.jdbc-catalog.connection-password"), "alice");
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
    IcebergConnectorAdapter adapter = new IcebergConnectorAdapter(icebergRestDisabledConfig());

    Map<String, String> config =
        adapter.buildInternalConnectorConfig(
            new GravitinoCatalog("test", mockCatalog), new Credential[0]);

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
    IcebergConnectorAdapter adapter = new IcebergConnectorAdapter(icebergRestDisabledConfig());

    Map<String, String> config =
        adapter.buildInternalConnectorConfig(
            new GravitinoCatalog("test", mockCatalog), new Credential[0]);

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

  @Test
  public void testBuildConnectorPropertiesWithJdbcCredential() throws Exception {
    String name = "test_catalog";
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put("uri", "jdbc:mysql://localhost:3306/metastore_db?createDatabaseIfNotExist=true")
            .put("catalog-backend", "jdbc")
            .put("warehouse", "/tmp/warehouse")
            .put("jdbc-driver", "com.mysql.cj.jdbc.Driver")
            .build();
    Catalog mockCatalog =
        TestGravitinoCatalog.mockCatalog(
            name, "lakehouse-iceberg", "test catalog", Catalog.Type.RELATIONAL, properties);
    IcebergConnectorAdapter adapter = new IcebergConnectorAdapter(icebergRestDisabledConfig());

    Map<String, String> config =
        adapter.buildInternalConnectorConfig(
            new GravitinoCatalog("test", mockCatalog),
            new Credential[] {new JdbcCredential("root", "ds123")});

    Assertions.assertEquals(config.get("iceberg.catalog.type"), "jdbc");
    Assertions.assertEquals(config.get("iceberg.jdbc-catalog.connection-user"), "root");
    Assertions.assertEquals(config.get("iceberg.jdbc-catalog.connection-password"), "ds123");
  }

  @Test
  public void testBuildConnectorPropertiesRoutesJdbcBackendThroughIcebergRest() throws Exception {
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put("uri", "jdbc:postgresql://localhost:5432/iceberg")
            .put("catalog-backend", "jdbc")
            .put("jdbc-driver", "org.postgresql.Driver")
            .put("jdbc-user", "iceberg")
            .put("jdbc-password", "secret")
            .put("warehouse", "s3://bucket/warehouse/")
            .put("s3-region", "us-east-1")
            .put("credential-providers", "s3-token")
            .build();

    Map<String, String> config =
        buildConnectorConfig("catalog1", properties, icebergRestEnabledConfig(ImmutableMap.of()));

    Assertions.assertEquals("rest", config.get("iceberg.catalog.type"));
    Assertions.assertEquals(
        "http://localhost:9001/iceberg", config.get("iceberg.rest-catalog.uri"));
    Assertions.assertEquals("catalog1", config.get("iceberg.rest-catalog.prefix"));
    // The initial GET /v1/config selects the catalog by `warehouse`, not by `prefix`.
    Assertions.assertEquals("catalog1", config.get("iceberg.rest-catalog.warehouse"));
    Assertions.assertEquals("true", config.get("iceberg.rest-catalog.vended-credentials-enabled"));

    // The JDBC backend is how Gravitino stores the metadata; it must not reach Trino.
    Assertions.assertNull(config.get("iceberg.jdbc-catalog.connection-url"));
    Assertions.assertNull(config.get("iceberg.jdbc-catalog.connection-user"));
    Assertions.assertNull(config.get("iceberg.jdbc-catalog.connection-password"));
  }

  @Test
  public void testBuildConnectorPropertiesRoutesHiveBackendThroughIcebergRest() throws Exception {
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put("uri", "thrift://localhost:9083")
            .put("catalog-backend", "hive")
            .put("warehouse", "s3://bucket/warehouse/")
            .build();

    Map<String, String> config =
        buildConnectorConfig("catalog1", properties, icebergRestEnabledConfig(ImmutableMap.of()));

    Assertions.assertEquals("rest", config.get("iceberg.catalog.type"));
    Assertions.assertEquals("catalog1", config.get("iceberg.rest-catalog.prefix"));
    Assertions.assertNull(config.get("hive.metastore.uri"));
  }

  @Test
  public void testBuildConnectorPropertiesKeepsRestBackendEndpoint() throws Exception {
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put("uri", "http://other-irc:9001/iceberg")
            .put("catalog-backend", "rest")
            .put("warehouse", "gt_iceberg_rest")
            .build();

    Map<String, String> config =
        buildConnectorConfig("catalog1", properties, icebergRestEnabledConfig(ImmutableMap.of()));

    Assertions.assertEquals("rest", config.get("iceberg.catalog.type"));
    Assertions.assertEquals(
        "http://other-irc:9001/iceberg", config.get("iceberg.rest-catalog.uri"));
    Assertions.assertEquals("gt_iceberg_rest", config.get("iceberg.rest-catalog.warehouse"));
    Assertions.assertNull(config.get("iceberg.rest-catalog.prefix"));
  }

  @Test
  public void testBuildConnectorPropertiesWithIcebergRestDisabled() throws Exception {
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put("uri", "jdbc:postgresql://localhost:5432/iceberg")
            .put("catalog-backend", "jdbc")
            .put("jdbc-driver", "org.postgresql.Driver")
            .put("warehouse", "s3://bucket/warehouse/")
            .build();

    Map<String, String> config =
        buildConnectorConfig("catalog1", properties, icebergRestDisabledConfig());

    Assertions.assertEquals("jdbc", config.get("iceberg.catalog.type"));
    Assertions.assertEquals(
        "jdbc:postgresql://localhost:5432/iceberg",
        config.get("iceberg.jdbc-catalog.connection-url"));
    Assertions.assertNull(config.get("iceberg.rest-catalog.uri"));
  }

  @Test
  public void testBuildConnectorPropertiesMissingIcebergRestUri() {
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put("uri", "jdbc:postgresql://localhost:5432/iceberg")
            .put("catalog-backend", "jdbc")
            .put("jdbc-driver", "org.postgresql.Driver")
            .build();
    GravitinoConfig gravitinoConfig =
        new GravitinoConfig(ImmutableMap.of("gravitino.metalake", "test"));

    Assertions.assertThrows(
        TrinoException.class, () -> buildConnectorConfig("catalog1", properties, gravitinoConfig));
  }

  @Test
  public void testBuildConnectorPropertiesWithIcebergRestAuthentication() throws Exception {
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put("catalog-backend", "jdbc")
            .put("uri", "jdbc:postgresql://localhost:5432/iceberg")
            .put("jdbc-driver", "org.postgresql.Driver")
            .put("warehouse", "s3://bucket/warehouse/")
            .build();
    GravitinoConfig gravitinoConfig =
        icebergRestEnabledConfig(
            ImmutableMap.of(
                "gravitino.iceberg.rest-catalog.security", "OAUTH2",
                "gravitino.iceberg.rest-catalog.oauth2.credential", "client_id:client_secret",
                "gravitino.iceberg.rest-catalog.uri", "http://ignored:9001/iceberg"));

    Map<String, String> config = buildConnectorConfig("catalog1", properties, gravitinoConfig);

    Assertions.assertEquals("OAUTH2", config.get("iceberg.rest-catalog.security"));
    Assertions.assertEquals(
        "client_id:client_secret", config.get("iceberg.rest-catalog.oauth2.credential"));
    // The endpoint the connector routes to cannot be overridden by the pass-through prefix.
    Assertions.assertEquals(
        "http://localhost:9001/iceberg", config.get("iceberg.rest-catalog.uri"));
  }

  @Test
  public void testBuildConnectorPropertiesForwardsSessionUser() throws Exception {
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put("catalog-backend", "jdbc")
            .put("uri", "jdbc:postgresql://localhost:5432/iceberg")
            .put("jdbc-driver", "org.postgresql.Driver")
            .build();

    Map<String, String> config =
        buildConnectorConfig(
            "catalog1",
            properties,
            icebergRestEnabledConfig(
                ImmutableMap.of("gravitino.client.session.forwardUser", "true")));
    Assertions.assertEquals("USER", config.get("iceberg.rest-catalog.session"));

    Map<String, String> explicitConfig =
        buildConnectorConfig(
            "catalog1",
            properties,
            icebergRestEnabledConfig(
                ImmutableMap.of(
                    "gravitino.client.session.forwardUser", "true",
                    "gravitino.iceberg.rest-catalog.session", "NONE")));
    Assertions.assertEquals("NONE", explicitConfig.get("iceberg.rest-catalog.session"));

    Map<String, String> defaultConfig =
        buildConnectorConfig("catalog1", properties, icebergRestEnabledConfig(ImmutableMap.of()));
    Assertions.assertNull(defaultConfig.get("iceberg.rest-catalog.session"));
  }

  @Test
  public void testBuildConnectorPropertiesStorageDetection() throws Exception {
    Map<String, String> s3Config =
        buildConnectorConfig(
            "catalog1",
            ImmutableMap.of(
                "catalog-backend", "jdbc",
                "warehouse", "s3://bucket/warehouse/",
                "s3-region", "us-east-1",
                "s3-endpoint", "http://minio:9000"),
            icebergRestEnabledConfig(ImmutableMap.of()));
    Assertions.assertEquals("true", s3Config.get("fs.native-s3.enabled"));
    Assertions.assertEquals("us-east-1", s3Config.get("s3.region"));
    Assertions.assertEquals("http://minio:9000", s3Config.get("s3.endpoint"));
    Assertions.assertEquals("true", s3Config.get("fs.hadoop.enabled"));

    Map<String, String> gcsConfig =
        buildConnectorConfig(
            "catalog1",
            ImmutableMap.of("catalog-backend", "jdbc", "warehouse", "gs://bucket/warehouse/"),
            icebergRestEnabledConfig(ImmutableMap.of()));
    Assertions.assertEquals("true", gcsConfig.get("fs.native-gcs.enabled"));
    Assertions.assertNull(gcsConfig.get("fs.native-s3.enabled"));

    Map<String, String> azureConfig =
        buildConnectorConfig(
            "catalog1",
            ImmutableMap.of(
                "catalog-backend", "jdbc", "warehouse", "abfss://container@account/warehouse/"),
            icebergRestEnabledConfig(ImmutableMap.of()));
    Assertions.assertEquals("true", azureConfig.get("fs.native-azure.enabled"));

    Map<String, String> hdfsConfig =
        buildConnectorConfig(
            "catalog1",
            ImmutableMap.of("catalog-backend", "jdbc", "warehouse", "hdfs://namenode:9000/wh"),
            icebergRestEnabledConfig(ImmutableMap.of()));
    Assertions.assertEquals("true", hdfsConfig.get("fs.hadoop.enabled"));
    Assertions.assertNull(hdfsConfig.get("fs.native-s3.enabled"));
    Assertions.assertNull(hdfsConfig.get("fs.native-gcs.enabled"));
    Assertions.assertNull(hdfsConfig.get("fs.native-azure.enabled"));
  }

  @Test
  public void testBuildConnectorPropertiesIcebergRestKeepsCatalogBypass() throws Exception {
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put("catalog-backend", "jdbc")
            .put("warehouse", "s3://bucket/warehouse/")
            .put("trino.bypass.iceberg.table-statistics-enabled", "true")
            .put("trino.bypass.fs.native-s3.enabled", "false")
            .build();

    Map<String, String> config =
        buildConnectorConfig("catalog1", properties, icebergRestEnabledConfig(ImmutableMap.of()));

    Assertions.assertEquals("true", config.get("iceberg.table-statistics-enabled"));
    // A catalog can override a derived default, so a Trino release renaming it is not a blocker.
    Assertions.assertEquals("false", config.get("fs.native-s3.enabled"));
  }

  @SuppressWarnings("unchecked")
  private static Map<String, String> buildConnectorConfig(
      String catalogName, Map<String, String> properties, GravitinoConfig gravitinoConfig)
      throws Exception {
    Catalog mockCatalog =
        TestGravitinoCatalog.mockCatalog(
            catalogName, "lakehouse-iceberg", "test catalog", Catalog.Type.RELATIONAL, properties);
    return new IcebergConnectorAdapter(gravitinoConfig)
        .buildInternalConnectorConfig(new GravitinoCatalog("test", mockCatalog), new Credential[0]);
  }

  private static GravitinoConfig icebergRestDisabledConfig() {
    return new GravitinoConfig(
        ImmutableMap.of("gravitino.metalake", "test", "gravitino.iceberg.rest-enabled", "false"));
  }

  private static GravitinoConfig icebergRestEnabledConfig(Map<String, String> extraConfig) {
    return new GravitinoConfig(
        ImmutableMap.<String, String>builder()
            .put("gravitino.metalake", "test")
            .put("gravitino.iceberg.rest-uri", "http://localhost:9001/iceberg")
            .putAll(extraConfig)
            .build());
  }
}
