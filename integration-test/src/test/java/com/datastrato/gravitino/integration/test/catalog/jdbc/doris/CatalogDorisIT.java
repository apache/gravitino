/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.catalog.jdbc.doris;

import static com.datastrato.gravitino.catalog.mysql.MysqlTablePropertiesMetadata.GRAVITINO_ENGINE_KEY;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.integration.test.catalog.jdbc.utils.JdbcDriverDownloader;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.DorisContainer;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.integration.test.util.ITUtils;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.rel.types.Types;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@Tag("gravitino-docker-it")
@TestInstance(Lifecycle.PER_CLASS)
public class CatalogDorisIT extends AbstractIT {
  private static final String provider = "jdbc-doris";
  private static final String DOWNLOAD_JDBC_DRIVER_URL =
      "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar";

  private static final String DRIVER_CLASS_NAME = "com.mysql.jdbc.Driver";

  public String metalakeName = GravitinoITUtils.genRandomName("doris_it_metalake");
  public String catalogName = GravitinoITUtils.genRandomName("doris_it_catalog");
  public String schemaName = GravitinoITUtils.genRandomName("doris_it_schema");
  public String tableName = GravitinoITUtils.genRandomName("doris_it_table");

  public String alertTableName = "alert_table_name";
  public String table_comment = "table_comment";

  public String schema_comment = null;
  public String MYSQL_COL_NAME1 = "mysql_col_name1";
  public String MYSQL_COL_NAME2 = "mysql_col_name2";
  public String MYSQL_COL_NAME3 = "mysql_col_name3";
  public String MYSQL_COL_NAME4 = "mysql_col_name4";
  public String MYSQL_COL_NAME5 = "mysql_col_name5";

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private GravitinoMetaLake metalake;

  protected Catalog catalog;

  protected final String TEST_DB_NAME = RandomUtils.nextInt(10000) + "_test_db";

  @BeforeAll
  public void startup() throws IOException {

    if (!ITUtils.EMBEDDED_TEST_MODE.equals(testMode)) {
      String gravitinoHome = System.getenv("GRAVITINO_HOME");
      Path tmpPath = Paths.get(gravitinoHome, "/catalogs/jdbc-mysql/libs");
      JdbcDriverDownloader.downloadJdbcDriver(DOWNLOAD_JDBC_DRIVER_URL, tmpPath.toString());
    }

    containerSuite.startDorisContainer();

    createMetalake();
    createCatalog();
    createSchema();
  }

  @AfterAll
  public void stop() {
    clearTableAndSchema();
    //    client.dropMetalake(NameIdentifier.of(metalakeName));
  }

  @AfterEach
  private void resetSchema() {
    clearTableAndSchema();
    createSchema();
  }

  private void clearTableAndSchema() {
    //    NameIdentifier[] nameIdentifiers =
    //        catalog.asTableCatalog().listTables(Namespace.of(metalakeName, catalogName,
    // schemaName));
    //    for (NameIdentifier nameIdentifier : nameIdentifiers) {
    //      catalog.asTableCatalog().dropTable(nameIdentifier);
    //    }
    //    catalog.asSchemas().dropSchema(NameIdentifier.of(metalakeName, catalogName, schemaName),
    // false);
  }

  private void createMetalake() {
    GravitinoMetaLake[] gravitinoMetaLakes = client.listMetalakes();
    Assertions.assertEquals(0, gravitinoMetaLakes.length);

    GravitinoMetaLake createdMetalake =
        client.createMetalake(NameIdentifier.of(metalakeName), "comment", Collections.emptyMap());
    GravitinoMetaLake loadMetalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Assertions.assertEquals(createdMetalake, loadMetalake);

    metalake = loadMetalake;
  }

  private void createCatalog() {
    Map<String, String> catalogProperties = Maps.newHashMap();

    DorisContainer dorisContainer = containerSuite.getDorisContainer();

    String jdbcUrl =
        String.format(
            "jdbc:mysql://%s:%d/",
            dorisContainer.getContainerIpAddress(), DorisContainer.FE_MYSQL_PORT);

    catalogProperties.put(JdbcConfig.JDBC_URL.getKey(), jdbcUrl);
    catalogProperties.put(JdbcConfig.JDBC_DRIVER.getKey(), DRIVER_CLASS_NAME);
    catalogProperties.put(JdbcConfig.USERNAME.getKey(), DorisContainer.USER_NAME);

    Catalog createdCatalog =
        metalake.createCatalog(
            NameIdentifier.of(metalakeName, catalogName),
            Catalog.Type.RELATIONAL,
            provider,
            "comment",
            catalogProperties);
    Catalog loadCatalog = metalake.loadCatalog(NameIdentifier.of(metalakeName, catalogName));
    Assertions.assertEquals(createdCatalog, loadCatalog);

    catalog = loadCatalog;
  }

  private void createSchema() {
    NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName, schemaName);
    Map<String, String> prop = Maps.newHashMap();

    Schema createdSchema = catalog.asSchemas().createSchema(ident, schema_comment, prop);
    Schema loadSchema = catalog.asSchemas().loadSchema(ident);
    Assertions.assertEquals(createdSchema.name(), loadSchema.name());
    prop.forEach((key, value) -> Assertions.assertEquals(loadSchema.properties().get(key), value));
  }

  private ColumnDTO[] createColumns() {
    ColumnDTO col1 =
        new ColumnDTO.Builder()
            .withName(MYSQL_COL_NAME1)
            .withDataType(Types.IntegerType.get())
            .withComment("col_1_comment")
            .build();
    ColumnDTO col2 =
        new ColumnDTO.Builder()
            .withName(MYSQL_COL_NAME2)
            .withDataType(Types.DateType.get())
            .withComment("col_2_comment")
            .build();
    ColumnDTO col3 =
        new ColumnDTO.Builder()
            .withName(MYSQL_COL_NAME3)
            .withDataType(Types.StringType.get())
            .withComment("col_3_comment")
            .build();
    return new ColumnDTO[] {col1, col2, col3};
  }

  private Map<String, String> createProperties() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(GRAVITINO_ENGINE_KEY, "InnoDB");
    return properties;
  }

  @Test
  void testOperationDorisSchema() {
    SupportsSchemas schemas = catalog.asSchemas();
    Namespace namespace = Namespace.of(metalakeName, catalogName);
    // list schema check.
    NameIdentifier[] nameIdentifiers = schemas.listSchemas(namespace);
    Set<String> schemaNames =
        Arrays.stream(nameIdentifiers).map(NameIdentifier::name).collect(Collectors.toSet());
    Assertions.assertTrue(schemaNames.contains(schemaName));

    //    NameIdentifier[] mysqlNamespaces = mysqlService.listSchemas(namespace);
    //    schemaNames =
    //
    // Arrays.stream(mysqlNamespaces).map(NameIdentifier::name).collect(Collectors.toSet());
    //    Assertions.assertTrue(schemaNames.contains(schemaName));
    //
    //    // create schema check.
    //    String testSchemaName = GravitinoITUtils.genRandomName("test_schema_1");
    //    NameIdentifier schemaIdent = NameIdentifier.of(metalakeName, catalogName, testSchemaName);
    //    schemas.createSchema(schemaIdent, schema_comment, Collections.emptyMap());
    //    nameIdentifiers = schemas.listSchemas(Namespace.of(metalakeName, catalogName));
    //    Map<String, NameIdentifier> schemaMap =
    //            Arrays.stream(nameIdentifiers).collect(Collectors.toMap(NameIdentifier::name, v ->
    // v));
    //    Assertions.assertTrue(schemaMap.containsKey(testSchemaName));
    //
    //    mysqlNamespaces = mysqlService.listSchemas(namespace);
    //    schemaNames =
    //
    // Arrays.stream(mysqlNamespaces).map(NameIdentifier::name).collect(Collectors.toSet());
    //    Assertions.assertTrue(schemaNames.contains(testSchemaName));
    //
    //    Map<String, String> emptyMap = Collections.emptyMap();
    //    Assertions.assertThrows(
    //            SchemaAlreadyExistsException.class,
    //            () -> {
    //              schemas.createSchema(schemaIdent, schema_comment, emptyMap);
    //            });
    //
    //    // drop schema check.
    //    schemas.dropSchema(schemaIdent, false);
    //    Assertions.assertThrows(NoSuchSchemaException.class, () ->
    // schemas.loadSchema(schemaIdent));
    //    Assertions.assertThrows(
    //            NoSuchSchemaException.class, () -> mysqlService.loadSchema(schemaIdent));
    //
    //    nameIdentifiers = schemas.listSchemas(Namespace.of(metalakeName, catalogName));
    //    schemaMap =
    //            Arrays.stream(nameIdentifiers).collect(Collectors.toMap(NameIdentifier::name, v ->
    // v));
    //    Assertions.assertFalse(schemaMap.containsKey(testSchemaName));
    //    Assertions.assertFalse(
    //            schemas.dropSchema(NameIdentifier.of(metalakeName, catalogName, "no-exits"),
    // false));
    //    TableCatalog tableCatalog = catalog.asTableCatalog();
    //
    //    // create failed check.
    //    NameIdentifier table =
    //            NameIdentifier.of(metalakeName, catalogName, testSchemaName, "test_table");
    //    Assertions.assertThrows(
    //            NoSuchSchemaException.class,
    //            () ->
    //                    tableCatalog.createTable(
    //                            table,
    //                            createColumns(),
    //                            table_comment,
    //                            createProperties(),
    //                            null,
    //                            Distributions.NONE,
    //                            null));
    //    // drop schema failed check.
    //    Assertions.assertFalse(schemas.dropSchema(schemaIdent, true));
    //    Assertions.assertFalse(schemas.dropSchema(schemaIdent, false));
    //    Assertions.assertFalse(tableCatalog.dropTable(table));
    //    mysqlNamespaces = mysqlService.listSchemas(Namespace.empty());
    //    schemaNames =
    //
    // Arrays.stream(mysqlNamespaces).map(NameIdentifier::name).collect(Collectors.toSet());
    //    Assertions.assertTrue(schemaNames.contains(schemaName));
  }
}
