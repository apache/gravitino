/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.postgresql.integration.test;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.datastrato.gravitino.client.GravitinoMetalake;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.MySQLContainer;
import com.datastrato.gravitino.integration.test.container.PostgreSQLContainer;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.ITUtils;
import com.datastrato.gravitino.integration.test.util.JdbcDriverDownloader;
import com.datastrato.gravitino.integration.test.util.TestDatabaseName;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.utils.RandomNameUtils;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-it")
public class TestMultipleJDBCLoad extends AbstractIT {
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private static final TestDatabaseName TEST_DB_NAME =
      TestDatabaseName.PG_TEST_PG_CATALOG_MULTIPLE_JDBC_LOAD;

  private static MySQLContainer mySQLContainer;
  private static PostgreSQLContainer postgreSQLContainer;

  private static final String DOWNLOAD_JDBC_DRIVER_URL =
      "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar";

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

    containerSuite.startMySQLContainer(TEST_DB_NAME);
    mySQLContainer = containerSuite.getMySQLContainer();
    containerSuite.startPostgreSQLContainer(TEST_DB_NAME);
    postgreSQLContainer = containerSuite.getPostgreSQLContainer();
  }

  @Test
  public void testCreateMultipleJdbc() throws URISyntaxException, SQLException {
    String metalakeName = RandomNameUtils.genRandomName("it_metalake");
    String postgreSqlCatalogName = RandomNameUtils.genRandomName("it_postgresql");
    GravitinoMetalake metalake =
        client.createMetalake(metalakeName, "comment", Collections.emptyMap());

    Map<String, String> pgConf = Maps.newHashMap();
    pgConf.put(JdbcConfig.JDBC_URL.getKey(), postgreSQLContainer.getJdbcUrl(TEST_DB_NAME));
    pgConf.put(JdbcConfig.JDBC_DATABASE.getKey(), TEST_DB_NAME.toString());
    pgConf.put(
        JdbcConfig.JDBC_DRIVER.getKey(), postgreSQLContainer.getDriverClassName(TEST_DB_NAME));
    pgConf.put(JdbcConfig.USERNAME.getKey(), postgreSQLContainer.getUsername());
    pgConf.put(JdbcConfig.PASSWORD.getKey(), postgreSQLContainer.getPassword());

    Catalog postgreSqlCatalog =
        metalake.createCatalog(
            postgreSqlCatalogName, Catalog.Type.RELATIONAL, "jdbc-postgresql", "comment", pgConf);

    Map<String, String> mysqlConf = Maps.newHashMap();

    mysqlConf.put(JdbcConfig.JDBC_URL.getKey(), mySQLContainer.getJdbcUrl());
    mysqlConf.put(JdbcConfig.JDBC_DRIVER.getKey(), mySQLContainer.getDriverClassName(TEST_DB_NAME));
    mysqlConf.put(JdbcConfig.USERNAME.getKey(), mySQLContainer.getUsername());
    mysqlConf.put(JdbcConfig.PASSWORD.getKey(), mySQLContainer.getPassword());
    String mysqlCatalogName = RandomNameUtils.genRandomName("it_mysql");
    Catalog mysqlCatalog =
        metalake.createCatalog(
            mysqlCatalogName, Catalog.Type.RELATIONAL, "jdbc-mysql", "comment", mysqlConf);

    NameIdentifier[] nameIdentifiers = mysqlCatalog.asSchemas().listSchemas();
    Assertions.assertNotEquals(0, nameIdentifiers.length);
    nameIdentifiers = postgreSqlCatalog.asSchemas().listSchemas();
    Assertions.assertEquals(1, nameIdentifiers.length);
    Assertions.assertEquals("public", nameIdentifiers[0].name());

    String schemaName = RandomNameUtils.genRandomName("it_schema");
    mysqlCatalog.asSchemas().createSchema(schemaName, null, Collections.emptyMap());

    postgreSqlCatalog.asSchemas().createSchema(schemaName, null, Collections.emptyMap());

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
