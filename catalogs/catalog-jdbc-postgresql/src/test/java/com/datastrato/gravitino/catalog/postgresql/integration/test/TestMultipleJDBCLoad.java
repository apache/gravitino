/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.postgresql.integration.test;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.ITUtils;
import com.datastrato.gravitino.integration.test.util.JdbcDriverDownloader;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.utils.RandomNameUtils;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
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

  private static final String DOWNLOAD_JDBC_DRIVER_URL =
      "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar";
  private static final String MYSQL_DEFAULT_IMAGE_NAME = "mysql:8.0";

  @BeforeAll
  public static void startup() throws IOException {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");

    // Deploy mode
    if (!ITUtils.EMBEDDED_TEST_MODE.equals(testMode)) {
      Path icebergLibsPath = Paths.get(gravitinoHome, "/catalogs/lakehouse-iceberg/libs");
      Path pgDirPath = Paths.get(gravitinoHome, "/catalogs/jdbc-postgresql/libs");
      JdbcDriverDownloader.downloadJdbcDriver(
          CatalogPostgreSqlIT.DOWNLOAD_JDBC_DRIVER_URL,
          pgDirPath.toString(),
          icebergLibsPath.toString());

      JdbcDriverDownloader.downloadJdbcDriver(
          DOWNLOAD_JDBC_DRIVER_URL, pgDirPath.toString(), icebergLibsPath.toString());
    } else {
      // embedded mode
      Path icebergLibsPath =
          Paths.get(gravitinoHome, "/catalogs/catalog-lakehouse-iceberg/build/libs");
      Path pgDirPath = Paths.get(gravitinoHome, "/catalogs/catalog-jdbc-postgresql/build/libs");
      JdbcDriverDownloader.downloadJdbcDriver(
          CatalogPostgreSqlIT.DOWNLOAD_JDBC_DRIVER_URL,
          icebergLibsPath.toString(),
          pgDirPath.toString());

      JdbcDriverDownloader.downloadJdbcDriver(
          DOWNLOAD_JDBC_DRIVER_URL, pgDirPath.toString(), icebergLibsPath.toString());
    }

    mySQLContainer =
        new MySQLContainer<>(MYSQL_DEFAULT_IMAGE_NAME)
            .withDatabaseName(TEST_DB_NAME)
            .withUsername("root")
            .withPassword("root");
    mySQLContainer.start();
    postgreSQLContainer =
        new PostgreSQLContainer<>(CatalogPostgreSqlIT.DEFAULT_POSTGRES_IMAGE)
            .withDatabaseName(TEST_DB_NAME)
            .withUsername("root")
            .withPassword("root");
    postgreSQLContainer.start();
  }

  @Test
  public void testCreateMultipleJdbc() throws URISyntaxException {
    String metalakeName = RandomNameUtils.genRandomName("it_metalake");
    String postgreSqlCatalogName = RandomNameUtils.genRandomName("it_postgresql");
    GravitinoMetaLake metalake =
        client.createMetalake(NameIdentifier.of(metalakeName), "comment", Collections.emptyMap());

    Map<String, String> pgConf = Maps.newHashMap();
    String jdbcUrl = postgreSQLContainer.getJdbcUrl();
    String database = new URI(jdbcUrl.substring(jdbcUrl.lastIndexOf("/") + 1)).getPath();
    pgConf.put(JdbcConfig.JDBC_URL.getKey(), jdbcUrl);
    pgConf.put(JdbcConfig.JDBC_DATABASE.getKey(), database);
    pgConf.put(JdbcConfig.JDBC_DRIVER.getKey(), postgreSQLContainer.getDriverClassName());
    pgConf.put(JdbcConfig.USERNAME.getKey(), postgreSQLContainer.getUsername());
    pgConf.put(JdbcConfig.PASSWORD.getKey(), postgreSQLContainer.getPassword());

    Catalog postgreSqlCatalog =
        metalake.createCatalog(
            NameIdentifier.of(metalakeName, postgreSqlCatalogName),
            Catalog.Type.RELATIONAL,
            "jdbc-postgresql",
            "comment",
            pgConf);

    Map<String, String> mysqlConf = Maps.newHashMap();

    mysqlConf.put(
        JdbcConfig.JDBC_URL.getKey(),
        StringUtils.substring(
            mySQLContainer.getJdbcUrl(), 0, mySQLContainer.getJdbcUrl().lastIndexOf("/")));
    mysqlConf.put(JdbcConfig.JDBC_DRIVER.getKey(), mySQLContainer.getDriverClassName());
    mysqlConf.put(JdbcConfig.USERNAME.getKey(), mySQLContainer.getUsername());
    mysqlConf.put(JdbcConfig.PASSWORD.getKey(), mySQLContainer.getPassword());
    String mysqlCatalogName = RandomNameUtils.genRandomName("it_mysql");
    Catalog mysqlCatalog =
        metalake.createCatalog(
            NameIdentifier.of(metalakeName, mysqlCatalogName),
            Catalog.Type.RELATIONAL,
            "jdbc-mysql",
            "comment",
            mysqlConf);

    NameIdentifier[] nameIdentifiers =
        mysqlCatalog.asSchemas().listSchemas(Namespace.of(metalakeName, mysqlCatalogName));
    Assertions.assertNotEquals(0, nameIdentifiers.length);
    nameIdentifiers =
        postgreSqlCatalog
            .asSchemas()
            .listSchemas(Namespace.of(metalakeName, postgreSqlCatalogName));
    Assertions.assertEquals(1, nameIdentifiers.length);
    Assertions.assertEquals("public", nameIdentifiers[0].name());

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
