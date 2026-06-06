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

package org.apache.gravitino.iceberg.common.utils;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.jdbc.JdbcCatalogWithMetadataLocationSupport;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestIcebergCatalogUtil {

  @TempDir private Path warehouse;

  @Test
  void testLoadCatalog() {
    Catalog catalog;

    catalog =
        IcebergCatalogUtil.loadCatalogBackend(
            IcebergConfig.CATALOG_BACKEND.getDefaultValue().toLowerCase());
    Assertions.assertTrue(catalog instanceof InMemoryCatalog);

    catalog =
        IcebergCatalogUtil.loadCatalogBackend(
            IcebergConfig.CATALOG_BACKEND.getDefaultValue().toUpperCase());
    Assertions.assertTrue(catalog instanceof InMemoryCatalog);

    catalog = IcebergCatalogUtil.loadCatalogBackend("hive");
    Assertions.assertTrue(catalog instanceof HiveCatalog);

    catalog = IcebergCatalogUtil.loadCatalogBackend("HIVE");
    Assertions.assertTrue(catalog instanceof HiveCatalog);

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> {
          IcebergCatalogUtil.loadCatalogBackend("jdbc");
        });

    Map<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.URI, "jdbc:sqlite::memory:");
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, "test");
    properties.put(IcebergConstants.GRAVITINO_JDBC_DRIVER, "org.sqlite.JDBC");
    properties.put(IcebergConstants.ICEBERG_JDBC_USER, "test");
    properties.put(IcebergConstants.ICEBERG_JDBC_PASSWORD, "test");
    properties.put(IcebergConstants.ICEBERG_JDBC_INITIALIZE, "true");
    catalog =
        IcebergCatalogUtil.loadCatalogBackend(
            IcebergCatalogBackend.JDBC, new IcebergConfig(properties));
    Assertions.assertTrue(catalog instanceof JdbcCatalog);

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> {
          IcebergCatalogUtil.loadCatalogBackend("other");
        });
  }

  @Test
  void testJdbcCatalogDefaultSchemaVersionIsV1() {
    Map<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.URI, "jdbc:sqlite::memory:");
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, "test");
    properties.put(IcebergConstants.GRAVITINO_JDBC_DRIVER, "org.sqlite.JDBC");
    properties.put(IcebergConstants.ICEBERG_JDBC_USER, "test");
    properties.put(IcebergConstants.ICEBERG_JDBC_PASSWORD, "test");
    // jdbc.schema-version is intentionally not set; default should be V1
    Catalog catalog =
        IcebergCatalogUtil.loadCatalogBackend(
            IcebergCatalogBackend.JDBC, new IcebergConfig(properties));
    Assertions.assertInstanceOf(JdbcCatalogWithMetadataLocationSupport.class, catalog);
    Assertions.assertTrue(
        ((JdbcCatalogWithMetadataLocationSupport) catalog).supportsViewsWithSchemaVersion(),
        "JDBC catalog should default to V1 schema and support view operations");
  }

  @Test
  void testJdbcCatalogExplicitSchemaVersionNotOverridden() {
    Map<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.URI, "jdbc:sqlite::memory:");
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, "test");
    properties.put(IcebergConstants.GRAVITINO_JDBC_DRIVER, "org.sqlite.JDBC");
    properties.put(IcebergConstants.ICEBERG_JDBC_USER, "test");
    properties.put(IcebergConstants.ICEBERG_JDBC_PASSWORD, "test");
    // Explicitly set V0; loadJdbcCatalog must not override it with V1
    properties.put(IcebergConstants.ICEBERG_JDBC_SCHEMA_VERSION, "V0");
    Catalog catalog =
        IcebergCatalogUtil.loadCatalogBackend(
            IcebergCatalogBackend.JDBC, new IcebergConfig(properties));
    Assertions.assertInstanceOf(JdbcCatalogWithMetadataLocationSupport.class, catalog);
    Assertions.assertFalse(
        ((JdbcCatalogWithMetadataLocationSupport) catalog).supportsViewsWithSchemaVersion(),
        "Explicitly configured V0 schema should not be overridden to V1");
  }

  @Test
  void testJdbcCatalogDefaultStrictModeRejectsImplicitNamespace() {
    Map<String, String> properties = new HashMap<>();
    properties.put(
        CatalogProperties.URI, "jdbc:sqlite:file:strict_default?mode=memory&cache=shared");
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse.toString());
    properties.put(IcebergConstants.GRAVITINO_JDBC_DRIVER, "org.sqlite.JDBC");
    properties.put(IcebergConstants.ICEBERG_JDBC_USER, "test");
    properties.put(IcebergConstants.ICEBERG_JDBC_PASSWORD, "test");
    // jdbc.strict-mode is intentionally not set; default should be true
    Catalog catalog =
        IcebergCatalogUtil.loadCatalogBackend(
            IcebergCatalogBackend.JDBC, new IcebergConfig(properties));

    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("absent_ns"), "t");
    // Strict mode must reject creating a table in a namespace that does not exist.
    Assertions.assertThrows(
        NoSuchNamespaceException.class, () -> catalog.createTable(identifier, schema));
  }

  @Test
  void testJdbcCatalogExplicitStrictModeDisabledAllowsImplicitNamespace() {
    Map<String, String> properties = new HashMap<>();
    properties.put(
        CatalogProperties.URI, "jdbc:sqlite:file:strict_disabled?mode=memory&cache=shared");
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse.toString());
    properties.put(IcebergConstants.GRAVITINO_JDBC_DRIVER, "org.sqlite.JDBC");
    properties.put(IcebergConstants.ICEBERG_JDBC_USER, "test");
    properties.put(IcebergConstants.ICEBERG_JDBC_PASSWORD, "test");
    // Explicitly disable strict mode; loadJdbcCatalog must not override it back to true.
    properties.put(IcebergConstants.ICEBERG_JDBC_STRICT_MODE, "false");
    Catalog catalog =
        IcebergCatalogUtil.loadCatalogBackend(
            IcebergCatalogBackend.JDBC, new IcebergConfig(properties));

    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("implicit_ns"), "t");
    // With strict mode disabled the namespace is created implicitly, so this must succeed.
    Assertions.assertNotNull(catalog.createTable(identifier, schema));
  }

  @Test
  void testValidLoadCustomCatalog() {
    Catalog catalog;
    Map<String, String> config = new HashMap<>();

    config.put(
        "catalog-backend-impl", "org.apache.gravitino.iceberg.common.utils.CustomCatalogForTest");
    catalog =
        IcebergCatalogUtil.loadCatalogBackend(
            IcebergCatalogBackend.valueOf("CUSTOM"), new IcebergConfig(config));
    Assertions.assertTrue(catalog instanceof CustomCatalogForTest);
  }

  @Test
  void testInvalidLoadCustomCatalog() {
    Assertions.assertThrowsExactly(
        NullPointerException.class,
        () ->
            IcebergCatalogUtil.loadCatalogBackend(
                IcebergCatalogBackend.valueOf("CUSTOM"), new IcebergConfig(new HashMap<>())));

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () ->
            IcebergCatalogUtil.loadCatalogBackend(
                IcebergCatalogBackend.valueOf("CUSTOM"),
                new IcebergConfig(
                    new HashMap<String, String>() {
                      {
                        put("catalog-backend-impl", "org.apache.");
                      }
                    })));
  }

  @Test
  void testApplyDefaultResolvingFileIOWhenMissing() {
    Map<String, String> properties = new HashMap<>();
    properties.put(IcebergConstants.WAREHOUSE, "s3://bucket/warehouse");

    IcebergCatalogUtil.applyDefaultResolvingFileIO(properties);

    Assertions.assertEquals(
        org.apache.iceberg.io.ResolvingFileIO.class.getName(),
        properties.get(IcebergConstants.IO_IMPL));
  }

  @Test
  void testApplyDefaultResolvingFileIODoesNotOverrideExplicitIOImpl() {
    Map<String, String> properties = new HashMap<>();
    properties.put(IcebergConstants.WAREHOUSE, "s3://bucket/warehouse");
    properties.put(IcebergConstants.IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");

    IcebergCatalogUtil.applyDefaultResolvingFileIO(properties);

    Assertions.assertEquals(
        "org.apache.iceberg.aws.s3.S3FileIO", properties.get(IcebergConstants.IO_IMPL));
  }
}
