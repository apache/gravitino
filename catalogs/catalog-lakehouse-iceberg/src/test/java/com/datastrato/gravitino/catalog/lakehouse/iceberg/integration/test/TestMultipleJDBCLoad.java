/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.iceberg.integration.test;

import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogPropertiesMetadata.GRAVITINO_JDBC_PASSWORD;
import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogPropertiesMetadata.GRAVITINO_JDBC_USER;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergConfig;
import com.datastrato.gravitino.client.GravitinoMetalake;
import com.datastrato.gravitino.integration.test.container.MySQLContainer;
import com.datastrato.gravitino.integration.test.container.PostgreSQLContainer;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.TestDatabaseName;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.utils.RandomNameUtils;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-it")
public class TestMultipleJDBCLoad extends AbstractIT {
  private static final TestDatabaseName TEST_DB_NAME =
      TestDatabaseName.PG_TEST_ICEBERG_CATALOG_MULTIPLE_JDBC_LOAD;

  private static MySQLContainer mySQLContainer;
  private static PostgreSQLContainer postgreSQLContainer;

  public static final String DEFAULT_POSTGRES_IMAGE = "postgres:13";

  @BeforeAll
  public static void startup() throws IOException {
    containerSuite.startMySQLContainer(TEST_DB_NAME);
    mySQLContainer = containerSuite.getMySQLContainer();
    containerSuite.startPostgreSQLContainer(TEST_DB_NAME);
    postgreSQLContainer = containerSuite.getPostgreSQLContainer();
  }

  @Test
  public void testCreateMultipleJdbcInIceberg() throws URISyntaxException, SQLException {
    String metalakeName = RandomNameUtils.genRandomName("it_metalake");
    String postgreSqlCatalogName = RandomNameUtils.genRandomName("it_iceberg_postgresql");
    GravitinoMetalake metalake =
        client.createMetalake(metalakeName, "comment", Collections.emptyMap());

    Map<String, String> icebergPgConf = Maps.newHashMap();
    String jdbcUrl = postgreSQLContainer.getJdbcUrl(TEST_DB_NAME);
    icebergPgConf.put(IcebergConfig.CATALOG_URI.getKey(), jdbcUrl);
    icebergPgConf.put(IcebergConfig.CATALOG_BACKEND.getKey(), "jdbc");
    icebergPgConf.put(IcebergConfig.CATALOG_WAREHOUSE.getKey(), "file:///tmp/iceberg-jdbc");
    icebergPgConf.put(
        IcebergConfig.JDBC_DRIVER.getKey(), postgreSQLContainer.getDriverClassName(TEST_DB_NAME));
    icebergPgConf.put(GRAVITINO_JDBC_USER, postgreSQLContainer.getUsername());
    icebergPgConf.put(GRAVITINO_JDBC_PASSWORD, postgreSQLContainer.getPassword());

    Catalog postgreSqlCatalog =
        metalake.createCatalog(
            postgreSqlCatalogName,
            Catalog.Type.RELATIONAL,
            "lakehouse-iceberg",
            "comment",
            icebergPgConf);

    Map<String, String> icebergMysqlConf = Maps.newHashMap();

    icebergMysqlConf.put(
        IcebergConfig.CATALOG_URI.getKey(), mySQLContainer.getJdbcUrl(TEST_DB_NAME));
    icebergMysqlConf.put(IcebergConfig.CATALOG_BACKEND.getKey(), "jdbc");
    icebergMysqlConf.put(IcebergConfig.CATALOG_WAREHOUSE.getKey(), "file:///tmp/iceberg-jdbc");
    icebergMysqlConf.put(
        IcebergConfig.JDBC_DRIVER.getKey(), mySQLContainer.getDriverClassName(TEST_DB_NAME));
    icebergMysqlConf.put(GRAVITINO_JDBC_USER, mySQLContainer.getUsername());
    icebergMysqlConf.put(GRAVITINO_JDBC_PASSWORD, mySQLContainer.getPassword());
    String mysqlCatalogName = RandomNameUtils.genRandomName("it_iceberg_mysql");
    Catalog mysqlCatalog =
        metalake.createCatalog(
            mysqlCatalogName,
            Catalog.Type.RELATIONAL,
            "lakehouse-iceberg",
            "comment",
            icebergMysqlConf);

    String[] nameIdentifiers = mysqlCatalog.asSchemas().listSchemas();
    Assertions.assertEquals(0, nameIdentifiers.length);
    nameIdentifiers = postgreSqlCatalog.asSchemas().listSchemas();
    Assertions.assertEquals(0, nameIdentifiers.length);

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
