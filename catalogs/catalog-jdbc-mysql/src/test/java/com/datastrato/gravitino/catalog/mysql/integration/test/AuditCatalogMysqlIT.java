/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.mysql.integration.test;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.CatalogChange;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.auth.AuthenticatorType;
import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.datastrato.gravitino.catalog.mysql.integration.test.service.MysqlService;
import com.datastrato.gravitino.client.GravitinoMetalake;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.MySQLContainer;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.integration.test.util.ITUtils;
import com.datastrato.gravitino.integration.test.util.JdbcDriverDownloader;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.types.Types;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-it")
public class AuditCatalogMysqlIT extends AbstractIT {
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  public static final String metalakeName = GravitinoITUtils.genRandomName("audit_mysql_metalake");
  private static final String expectUser = System.getProperty("user.name");
  public static final String DOWNLOAD_JDBC_DRIVER_URL =
      "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar";
  protected static String TEST_DB_NAME;
  private static final String provider = "jdbc-mysql";

  private static MysqlService mysqlService;
  private static MySQLContainer MYSQL_CONTAINER;
  private static GravitinoMetalake metalake;

  @BeforeAll
  public static void startIntegrationTest() throws Exception {
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.AUTHENTICATOR.getKey(), AuthenticatorType.SIMPLE.name().toLowerCase());
    registerCustomConfigs(configs);
    AbstractIT.startIntegrationTest();

    if (!ITUtils.EMBEDDED_TEST_MODE.equals(testMode)) {
      String gravitinoHome = System.getenv("GRAVITINO_HOME");
      Path tmpPath = Paths.get(gravitinoHome, "/catalogs/jdbc-mysql/libs");
      JdbcDriverDownloader.downloadJdbcDriver(DOWNLOAD_JDBC_DRIVER_URL, tmpPath.toString());
    }

    // FIXME: startMySQLContainer should create db name by test class name
    containerSuite.startMySQLContainer(AuditCatalogMysqlIT.class);
    MYSQL_CONTAINER = containerSuite.getMySQLContainer();
    TEST_DB_NAME = MYSQL_CONTAINER.getDatabaseNameByClass(AuditCatalogMysqlIT.class);

    mysqlService = new MysqlService(containerSuite.getMySQLContainer(), TEST_DB_NAME);
    createMetalake();
  }

  @AfterAll
  public static void stopIntegrationTest() throws IOException, InterruptedException {
    AbstractIT.stopIntegrationTest();
    client.dropMetalake(NameIdentifier.of(metalakeName));
    mysqlService.close();
  }

  @Test
  public void testAuditCatalog() throws Exception {
    String catalogName = GravitinoITUtils.genRandomName("audit_mysql_catalog");
    Catalog catalog = createCatalog(catalogName);

    Assertions.assertEquals(expectUser, catalog.auditInfo().creator());
    Assertions.assertEquals(catalog.auditInfo().creator(), catalog.auditInfo().lastModifier());
    Assertions.assertEquals(
        catalog.auditInfo().createTime(), catalog.auditInfo().lastModifiedTime());
    catalog =
        metalake.alterCatalog(
            NameIdentifier.of(metalakeName, catalogName),
            CatalogChange.setProperty("key1", "value1"));
    Assertions.assertEquals(expectUser, catalog.auditInfo().creator());
    Assertions.assertEquals(expectUser, catalog.auditInfo().lastModifier());
  }

  @Test
  public void testAuditSchema() throws Exception {
    String catalogName = GravitinoITUtils.genRandomName("audit_mysql_schema_catalog");
    String schemaName = GravitinoITUtils.genRandomName("audit_mysql_schema");
    Catalog catalog = createCatalog(catalogName);
    NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName, schemaName);
    Map<String, String> prop = Maps.newHashMap();
    Schema schema = catalog.asSchemas().createSchema(ident, null, prop);
    Assertions.assertEquals(expectUser, schema.auditInfo().creator());
    Assertions.assertNull(schema.auditInfo().lastModifier());
  }

  @Test
  public void testAuditTable() throws Exception {
    String catalogName = GravitinoITUtils.genRandomName("audit_mysql_table_catalog");
    String schemaName = GravitinoITUtils.genRandomName("audit_mysql_table_schma");
    String tableName = GravitinoITUtils.genRandomName("audit_mysql_table");
    Catalog catalog = createCatalog(catalogName);
    Map<String, String> properties = Maps.newHashMap();

    Column col1 = Column.of("col_1", Types.IntegerType.get(), "col_1_comment");

    catalog
        .asSchemas()
        .createSchema(NameIdentifier.of(metalakeName, catalogName, schemaName), null, properties);
    Table table =
        catalog
            .asTableCatalog()
            .createTable(
                NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
                new Column[] {col1},
                "comment",
                properties);
    Assertions.assertEquals(expectUser, table.auditInfo().creator());
    Assertions.assertNull(table.auditInfo().lastModifier());
    table =
        catalog
            .asTableCatalog()
            .alterTable(
                NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
                TableChange.addColumn(new String[] {"col_4"}, Types.StringType.get()));
    Assertions.assertEquals(expectUser, table.auditInfo().creator());
    Assertions.assertEquals(expectUser, table.auditInfo().lastModifier());
  }

  private static Catalog createCatalog(String catalogName) throws SQLException {
    Map<String, String> catalogProperties = Maps.newHashMap();

    catalogProperties.put(
        JdbcConfig.JDBC_URL.getKey(),
        StringUtils.substring(
            MYSQL_CONTAINER.getJdbcUrl(TEST_DB_NAME),
            0,
            MYSQL_CONTAINER.getJdbcUrl(TEST_DB_NAME).lastIndexOf("/")));
    catalogProperties.put(JdbcConfig.JDBC_DRIVER.getKey(), MYSQL_CONTAINER.getDriverClassName());
    catalogProperties.put(JdbcConfig.USERNAME.getKey(), MYSQL_CONTAINER.getUsername());
    catalogProperties.put(JdbcConfig.PASSWORD.getKey(), MYSQL_CONTAINER.getPassword());

    return metalake.createCatalog(
        NameIdentifier.of(metalakeName, catalogName),
        Catalog.Type.RELATIONAL,
        provider,
        "comment",
        catalogProperties);
  }

  private static void createMetalake() {
    GravitinoMetalake[] gravitinoMetalakes = client.listMetalakes();
    Assertions.assertEquals(0, gravitinoMetalakes.length);

    GravitinoMetalake createdMetalake =
        client.createMetalake(NameIdentifier.of(metalakeName), "comment", Collections.emptyMap());
    GravitinoMetalake loadMetalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Assertions.assertEquals(createdMetalake, loadMetalake);
    metalake = loadMetalake;
  }
}
