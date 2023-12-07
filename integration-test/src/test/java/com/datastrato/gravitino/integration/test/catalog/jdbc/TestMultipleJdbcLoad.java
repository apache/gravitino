/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.catalog.jdbc;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.integration.test.catalog.jdbc.mysql.CatalogMysqlIT;
import com.datastrato.gravitino.integration.test.catalog.jdbc.postgresql.CatalogPostgreSqlIT;
import com.datastrato.gravitino.integration.test.catalog.jdbc.utils.JdbcDriverDownloader;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.integration.test.util.ITUtils;
import com.datastrato.gravitino.rel.types.Types;
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
public class TestMultipleJdbcLoad extends AbstractIT {

  @BeforeAll
  public static void startup() throws IOException {
    if (!ITUtils.EMBEDDED_TEST_MODE.equals(testMode)) {
      String gravitinoHome = System.getenv("GRAVITINO_HOME");
      Path tmpPath = Paths.get(gravitinoHome, "/catalogs/jdbc-postgresql/libs");
      JdbcDriverDownloader.downloadJdbcDriver(
          CatalogPostgreSqlIT.DOWNLOAD_JDBC_DRIVER_URL, tmpPath.toString());
      tmpPath = Paths.get(gravitinoHome, "/catalogs/jdbc-mysql/libs");
      JdbcDriverDownloader.downloadJdbcDriver(
          CatalogMysqlIT.DOWNLOAD_JDBC_DRIVER_URL, tmpPath.toString());
    }
  }

  @Test
  public void testCreateMultipleJdbc() throws URISyntaxException {
    String TEST_DB_NAME = GravitinoITUtils.genRandomName("ct_db");
    MySQLContainer mySQLContainer =
        new MySQLContainer<>(CatalogMysqlIT.mysqlImageName)
            .withDatabaseName(TEST_DB_NAME)
            .withUsername("root")
            .withPassword("root");
    mySQLContainer.start();
    PostgreSQLContainer postgreSQLContainer =
        new PostgreSQLContainer<>(CatalogPostgreSqlIT.POSTGRES_IMAGE)
            .withDatabaseName(TEST_DB_NAME)
            .withUsername("root")
            .withPassword("root");
    postgreSQLContainer.start();

    String metalakeName = GravitinoITUtils.genRandomName("it_metalake");
    String postgreSqlCatalogName = GravitinoITUtils.genRandomName("it_postgresql");
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
    pgConf.put(JdbcConfig.JDBC_DRIVER.getKey(), mySQLContainer.getDriverClassName());
    mysqlConf.put(JdbcConfig.USERNAME.getKey(), mySQLContainer.getUsername());
    mysqlConf.put(JdbcConfig.PASSWORD.getKey(), mySQLContainer.getPassword());
    String mysqlCatalogName = GravitinoITUtils.genRandomName("it_mysql");
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
    Assertions.assertNotEquals(0, nameIdentifiers.length);

    String schemaName = GravitinoITUtils.genRandomName("it_schema");
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

    String tableName = GravitinoITUtils.genRandomName("it_table");

    ColumnDTO col1 =
        new ColumnDTO.Builder()
            .withName("col_1")
            .withDataType(Types.IntegerType.get())
            .withComment("col_1_comment")
            .build();
    String comment = "test";
    mysqlCatalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(metalakeName, mysqlCatalogName, schemaName, tableName),
            new ColumnDTO[] {col1},
            comment,
            Collections.emptyMap());

    postgreSqlCatalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(metalakeName, postgreSqlCatalogName, schemaName, tableName),
            new ColumnDTO[] {col1},
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
