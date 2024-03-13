/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.iceberg.integration.test;

import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogPropertiesMetadata.GRAVITINO_JDBC_PASSWORD;
import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogPropertiesMetadata.GRAVITINO_JDBC_USER;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergConfig;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.ITUtils;
import com.datastrato.gravitino.integration.test.util.JdbcDriverDownloader;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.utils.RandomNameUtils;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;

@Tag("gravitino-docker-it")
public class TestMultipleJDBCLoad extends AbstractIT {
  private static String TEST_DB_NAME = RandomNameUtils.genRandomName("ct_db");

  private static MySQLContainer mySQLContainer;
  private static PostgreSQLContainer postgreSQLContainer;

  private static final String DOWNLOAD_MYSQL_JDBC_DRIVER_URL =
      "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar";
  private static final String MYSQL_DEFAULT_IMAGE_NAME = "mysql:8.0";
  public static final String DEFAULT_POSTGRES_IMAGE = "postgres:13";
  public static final String DOWNLOAD_PG_JDBC_DRIVER_URL =
      "https://jdbc.postgresql.org/download/postgresql-42.7.0.jar";

  @BeforeAll
  public static void startup() throws IOException {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");

    // Deploy mode
    if (!ITUtils.EMBEDDED_TEST_MODE.equals(testMode)) {
      Path icebergLibsPath = Paths.get(gravitinoHome, "/catalogs/lakehouse-iceberg/libs");
      JdbcDriverDownloader.downloadJdbcDriver(
          DOWNLOAD_MYSQL_JDBC_DRIVER_URL, icebergLibsPath.toString());
      JdbcDriverDownloader.downloadJdbcDriver(
          DOWNLOAD_PG_JDBC_DRIVER_URL, icebergLibsPath.toString());
    } else {
      // embedded mode
      Path icebergLibsPath =
          Paths.get(gravitinoHome, "/catalogs/catalog-lakehouse-iceberg/build/libs");
      JdbcDriverDownloader.downloadJdbcDriver(
          DOWNLOAD_MYSQL_JDBC_DRIVER_URL, icebergLibsPath.toString());

      JdbcDriverDownloader.downloadJdbcDriver(
          DOWNLOAD_PG_JDBC_DRIVER_URL, icebergLibsPath.toString());
    }

    mySQLContainer =
        new MySQLContainer<>(MYSQL_DEFAULT_IMAGE_NAME)
            .withDatabaseName(TEST_DB_NAME)
            .withUsername("root")
            .withPassword("root");
    mySQLContainer.start();
    postgreSQLContainer =
        new PostgreSQLContainer<>(DEFAULT_POSTGRES_IMAGE)
            .withDatabaseName(TEST_DB_NAME)
            .withUsername("root")
            .withPassword("root");
    postgreSQLContainer.start();
  }

  @Test
  public void testCreateMultipleJdbcInIceberg() throws URISyntaxException {
    String metalakeName = RandomNameUtils.genRandomName("it_metalake");
    String postgreSqlCatalogName = RandomNameUtils.genRandomName("it_iceberg_postgresql");
    GravitinoMetaLake metalake =
        client.createMetalake(NameIdentifier.of(metalakeName), "comment", Collections.emptyMap());

    Map<String, String> icebergPgConf = Maps.newHashMap();
    String jdbcUrl = postgreSQLContainer.getJdbcUrl();
    icebergPgConf.put(IcebergConfig.CATALOG_URI.getKey(), jdbcUrl);
    icebergPgConf.put(IcebergConfig.CATALOG_BACKEND.getKey(), "jdbc");
    icebergPgConf.put(IcebergConfig.CATALOG_WAREHOUSE.getKey(), "file:///tmp/iceberg-jdbc");
    icebergPgConf.put(IcebergConfig.JDBC_DRIVER.getKey(), postgreSQLContainer.getDriverClassName());
    icebergPgConf.put(GRAVITINO_JDBC_USER, postgreSQLContainer.getUsername());
    icebergPgConf.put(GRAVITINO_JDBC_PASSWORD, postgreSQLContainer.getPassword());

    Catalog postgreSqlCatalog =
        metalake.createCatalog(
            NameIdentifier.of(metalakeName, postgreSqlCatalogName),
            Catalog.Type.RELATIONAL,
            "lakehouse-iceberg",
            "comment",
            icebergPgConf);

    Map<String, String> icebergMysqlConf = Maps.newHashMap();

    icebergMysqlConf.put(IcebergConfig.CATALOG_URI.getKey(), mySQLContainer.getJdbcUrl());
    icebergMysqlConf.put(IcebergConfig.CATALOG_BACKEND.getKey(), "jdbc");
    icebergMysqlConf.put(IcebergConfig.CATALOG_WAREHOUSE.getKey(), "file:///tmp/iceberg-jdbc");
    icebergMysqlConf.put(IcebergConfig.JDBC_DRIVER.getKey(), mySQLContainer.getDriverClassName());
    icebergMysqlConf.put(GRAVITINO_JDBC_USER, mySQLContainer.getUsername());
    icebergMysqlConf.put(GRAVITINO_JDBC_PASSWORD, mySQLContainer.getPassword());
    String mysqlCatalogName = RandomNameUtils.genRandomName("it_iceberg_mysql");
    Catalog mysqlCatalog =
        metalake.createCatalog(
            NameIdentifier.of(metalakeName, mysqlCatalogName),
            Catalog.Type.RELATIONAL,
            "lakehouse-iceberg",
            "comment",
            icebergMysqlConf);

    NameIdentifier[] nameIdentifiers =
        mysqlCatalog.asSchemas().listSchemas(Namespace.of(metalakeName, mysqlCatalogName));
    Assertions.assertEquals(0, nameIdentifiers.length);
    nameIdentifiers =
        postgreSqlCatalog
            .asSchemas()
            .listSchemas(Namespace.of(metalakeName, postgreSqlCatalogName));
    Assertions.assertEquals(0, nameIdentifiers.length);

    String schemaName = RandomNameUtils.genRandomName("it_schema");
    mysqlCatalog
        .asSchemas()
        .createSchema(
            NameIdentifier.of(metalakeName, mysqlCatalogName, schemaName),
            null,
            Collections.emptyMap());

    postgreSqlCatalog
        .asSchemas()
        .createSchema(
            NameIdentifier.of(metalakeName, postgreSqlCatalogName, schemaName),
            null,
            Collections.emptyMap());

    String tableName = RandomNameUtils.genRandomName("it_table");

    Column col1 = Column.of("col_1", Types.IntegerType.get(), "col_1_comment");
    String comment = "test";
    mysqlCatalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(metalakeName, mysqlCatalogName, schemaName, tableName),
            new Column[] {col1},
            comment,
            Collections.emptyMap());

    postgreSqlCatalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(metalakeName, postgreSqlCatalogName, schemaName, tableName),
            new Column[] {col1},
            comment,
            Collections.emptyMap());

    Assertions.assertTrue(
        mysqlCatalog
            .asTableCatalog()
            .tableExists(NameIdentifier.of(metalakeName, mysqlCatalogName, schemaName, tableName)));
    Assertions.assertTrue(
        postgreSqlCatalog
            .asTableCatalog()
            .tableExists(
                NameIdentifier.of(metalakeName, postgreSqlCatalogName, schemaName, tableName)));
  }
}
