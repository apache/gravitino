/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.catalog.clickhouse.integration.test;

import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.ClusterConstants.CLUSTER_NAME;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.ClusterConstants.ON_CLUSTER;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.DistributedTableConstants.REMOTE_DATABASE;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.DistributedTableConstants.REMOTE_TABLE;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.DistributedTableConstants.SHARDING_KEY;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseTablePropertiesMetadata.ENGINE;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseTablePropertiesMetadata.GRAVITINO_ENGINE_KEY;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseUtils.getSortOrders;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import com.google.common.collect.Maps;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.catalog.clickhouse.integration.test.service.ClickHouseService;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.container.ClickHouseContainer;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@Tag("gravitino-docker-test")
@TestInstance(Lifecycle.PER_CLASS)
public class CatalogClickHouseClusterIT extends BaseIT {
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private static final String provider = "jdbc-clickhouse";

  private final String metalakeName = GravitinoITUtils.genRandomName("ck_cluster_metalake");
  private final String catalogName = GravitinoITUtils.genRandomName("ck_cluster_catalog");
  private final String schemaName = GravitinoITUtils.genRandomName("ck_cluster_schema");
  private final String localTableName = GravitinoITUtils.genRandomName("ck_cluster_local");
  private final String distributedTableName = GravitinoITUtils.genRandomName("ck_cluster_dist");
  private final String nonClusterTableName = GravitinoITUtils.genRandomName("ck_non_cluster_local");
  private final String sqlClusterLocalTableName =
      GravitinoITUtils.genRandomName("ck_sql_cluster_local");
  private final String sqlClusterDistributedTableName =
      GravitinoITUtils.genRandomName("ck_sql_cluster_dist");
  private final String sqlNonClusterLocalTableName =
      GravitinoITUtils.genRandomName("ck_sql_non_cluster_local");
  private final String sqlNonClusterDistributedTableName =
      GravitinoITUtils.genRandomName("ck_sql_non_cluster_dist");
  private final String apiClusterSchemaName =
      GravitinoITUtils.genRandomName("ck_api_cluster_schema");
  private final String sqlClusterSchemaName =
      GravitinoITUtils.genRandomName("ck_sql_cluster_schema");
  private final String apiNonClusterSchemaName =
      GravitinoITUtils.genRandomName("ck_api_non_cluster_schema");
  private final String sqlNonClusterSchemaName =
      GravitinoITUtils.genRandomName("ck_sql_non_cluster_schema");
  private final String tableComment = "cluster_table_comment";

  private GravitinoMetalake metalake;
  private Catalog catalog;
  private ClickHouseService clickHouseService;
  private ClickHouseContainer clickHouseClusterContainer;
  private final TestDatabaseName TEST_DB_NAME = TestDatabaseName.CLICKHOUSE_CLUSTER_CLICKHOUSE_IT;

  @BeforeAll
  public void startup() {
    String remoteServersConfig =
        Paths.get("src", "test", "resources", "remote_servers.xml").toAbsolutePath().toString();
    containerSuite.startClickHouseClusterContainer(TEST_DB_NAME, remoteServersConfig);
    clickHouseClusterContainer = containerSuite.getClickHouseClusterContainer();

    clickHouseService = new ClickHouseService(clickHouseClusterContainer, TEST_DB_NAME);
    createMetalake();
    createCatalog();
    createSchema();
  }

  @AfterAll
  public void stop() {
    clearTableAndSchema();
    metalake.disableCatalog(catalogName);
    metalake.dropCatalog(catalogName, true);
    client.disableMetalake(metalakeName);
    client.dropMetalake(metalakeName, true);
    clickHouseService.close();
  }

  private void clearTableAndSchema() {
    NameIdentifier[] nameIdentifiers =
        catalog.asTableCatalog().listTables(Namespace.of(schemaName));
    for (NameIdentifier nameIdentifier : nameIdentifiers) {
      catalog.asTableCatalog().dropTable(nameIdentifier);
    }
    catalog.asSchemas().dropSchema(schemaName, false);
  }

  private void createMetalake() {
    client.createMetalake(metalakeName, "cluster metalake", Collections.emptyMap());
    metalake = client.loadMetalake(metalakeName);
  }

  private void createCatalog() {
    Map<String, String> catalogProperties = Maps.newHashMap();

    catalogProperties.put(
        JdbcConfig.JDBC_URL.getKey(),
        StringUtils.substring(
            clickHouseClusterContainer.getJdbcUrl(TEST_DB_NAME),
            0,
            clickHouseClusterContainer.getJdbcUrl(TEST_DB_NAME).lastIndexOf("/")));
    try {
      catalogProperties.put(
          JdbcConfig.JDBC_DRIVER.getKey(),
          clickHouseClusterContainer.getDriverClassName(TEST_DB_NAME));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    catalogProperties.put(JdbcConfig.USERNAME.getKey(), clickHouseClusterContainer.getUsername());
    catalogProperties.put(JdbcConfig.PASSWORD.getKey(), clickHouseClusterContainer.getPassword());

    catalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.RELATIONAL, provider, "cluster catalog", catalogProperties);
  }

  private void createSchema() {
    Schema createdSchema =
        catalog.asSchemas().createSchema(schemaName, null, Collections.emptyMap());
    Schema loadSchema = catalog.asSchemas().loadSchema(schemaName);
    Assertions.assertEquals(createdSchema.name(), loadSchema.name());
  }

  private Column[] createColumns() {
    // TODO( check hash field)
    Column col1 =
        Column.of(
            "col_1",
            Types.IntegerType.get(),
            "col_1_comment",
            false,
            false,
            Literals.integerLiteral(0));
    Column col2 = Column.of("col_2", Types.DateType.get(), "col_2_comment");
    Column col3 =
        Column.of(
            "col_3", Types.StringType.get(), "col_3_comment", false, false, DEFAULT_VALUE_NOT_SET);
    return new Column[] {col1, col2, col3};
  }

  private Map<String, String> clusterMergeTreeProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put(GRAVITINO_ENGINE_KEY, ENGINE.MERGETREE.getValue());
    properties.put(CLUSTER_NAME, ClickHouseContainer.DEFAULT_CLUSTER_NAME);
    properties.put(ON_CLUSTER, String.valueOf(true));
    return properties;
  }

  private Map<String, String> distributedProperties(String remoteTable) {
    Map<String, String> properties = new HashMap<>();
    properties.put(GRAVITINO_ENGINE_KEY, ENGINE.DISTRIBUTED.getValue());
    properties.put(CLUSTER_NAME, ClickHouseContainer.DEFAULT_CLUSTER_NAME);
    properties.put(ON_CLUSTER, String.valueOf(true));
    properties.put(REMOTE_DATABASE, schemaName);
    properties.put(REMOTE_TABLE, remoteTable);
    properties.put(SHARDING_KEY, "cityHash64(col_1)");
    return properties;
  }

  private Map<String, String> clusterSchemaProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put(CLUSTER_NAME, ClickHouseContainer.DEFAULT_CLUSTER_NAME);
    properties.put(ON_CLUSTER, String.valueOf(true));
    return properties;
  }

  @Test
  public void testCreateDistributedTableOnCluster() throws Exception {
    Column[] columns = createColumns();
    NameIdentifier localTableIdent = NameIdentifier.of(schemaName, localTableName);
    NameIdentifier distributedTableIdent = NameIdentifier.of(schemaName, distributedTableName);
    Distribution distribution = Distributions.NONE;
    Transform[] partitioning = Transforms.EMPTY_TRANSFORM;
    TableCatalog tableCatalog = catalog.asTableCatalog();

    Table localTable =
        tableCatalog.createTable(
            localTableIdent,
            columns,
            tableComment,
            clusterMergeTreeProperties(),
            partitioning,
            distribution,
            getSortOrders("col_3"));

    Assertions.assertTrue(localTable != null && localTable.name().equals(localTableName));
    Table loadedLocalTable = tableCatalog.loadTable(localTableIdent);
    Assertions.assertEquals(
        ENGINE.MERGETREE.getValue(), loadedLocalTable.properties().get(GRAVITINO_ENGINE_KEY));
    Assertions.assertEquals("false", loadedLocalTable.properties().get(ON_CLUSTER));
    Assertions.assertFalse(loadedLocalTable.properties().containsKey(CLUSTER_NAME));

    Table distributedTable =
        tableCatalog.createTable(
            distributedTableIdent,
            columns,
            tableComment,
            distributedProperties(localTableName),
            partitioning,
            distribution,
            null);
    Assertions.assertTrue(
        distributedTable != null && distributedTable.name().equals(distributedTableName));
    Table loadedDistributedTable = tableCatalog.loadTable(distributedTableIdent);
    Assertions.assertEquals(
        ENGINE.DISTRIBUTED.getValue(),
        loadedDistributedTable.properties().get(GRAVITINO_ENGINE_KEY));
    Assertions.assertEquals("false", loadedDistributedTable.properties().get(ON_CLUSTER));
    Assertions.assertEquals(
        ClickHouseContainer.DEFAULT_CLUSTER_NAME,
        loadedDistributedTable.properties().get(CLUSTER_NAME));
    Assertions.assertEquals(schemaName, loadedDistributedTable.properties().get(REMOTE_DATABASE));
    Assertions.assertEquals(localTableName, loadedDistributedTable.properties().get(REMOTE_TABLE));

    NameIdentifier nonClusterTableIdent = NameIdentifier.of(schemaName, nonClusterTableName);
    Table nonClusterTable =
        tableCatalog.createTable(
            nonClusterTableIdent,
            columns,
            tableComment,
            Collections.singletonMap(GRAVITINO_ENGINE_KEY, ENGINE.MERGETREE.getValue()),
            partitioning,
            distribution,
            getSortOrders("col_3"));
    Assertions.assertTrue(
        nonClusterTable != null && nonClusterTable.name().equals(nonClusterTableName));
    Table loadedNonClusterTable = tableCatalog.loadTable(nonClusterTableIdent);
    Assertions.assertEquals(
        ENGINE.MERGETREE.getValue(), loadedNonClusterTable.properties().get(GRAVITINO_ENGINE_KEY));
    Assertions.assertEquals("false", loadedNonClusterTable.properties().get(ON_CLUSTER));
    Assertions.assertFalse(loadedNonClusterTable.properties().containsKey(CLUSTER_NAME));

    try (Connection connection =
            DriverManager.getConnection(
                clickHouseClusterContainer.getJdbcUrl(TEST_DB_NAME),
                clickHouseClusterContainer.getUsername(),
                clickHouseClusterContainer.getPassword());
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "INSERT INTO `%s`.`%s` VALUES (1, toDate('2020-01-01'), 'inserted')",
              schemaName, localTableName));
      try (ResultSet resultSet =
          statement.executeQuery(
              String.format("SELECT count() FROM `%s`.`%s`", schemaName, distributedTableName))) {
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(1L, resultSet.getLong(1));
      }
    }
  }

  @Test
  public void testLoadSqlCreatedLocalAndDistributedTableProperties() {
    clickHouseService.executeQuery(
        String.format(
            "CREATE TABLE `%s`.`%s` ON CLUSTER `%s` ("
                + "col_1 Int32, col_2 Date, col_3 String) "
                + "ENGINE = MergeTree ORDER BY col_1",
            schemaName, sqlClusterLocalTableName, ClickHouseContainer.DEFAULT_CLUSTER_NAME));

    clickHouseService.executeQuery(
        String.format(
            "CREATE TABLE `%s`.`%s` ON CLUSTER `%s` AS `%s`.`%s` "
                + "ENGINE = Distributed('%s', '%s', '%s', cityHash64(col_1))",
            schemaName,
            sqlClusterDistributedTableName,
            ClickHouseContainer.DEFAULT_CLUSTER_NAME,
            schemaName,
            sqlClusterLocalTableName,
            ClickHouseContainer.DEFAULT_CLUSTER_NAME,
            schemaName,
            sqlClusterLocalTableName));

    clickHouseService.executeQuery(
        String.format(
            "CREATE TABLE `%s`.`%s` (col_1 Int32, col_2 Date, col_3 String) "
                + "ENGINE = MergeTree ORDER BY col_1",
            schemaName, sqlNonClusterLocalTableName));

    clickHouseService.executeQuery(
        String.format(
            "CREATE TABLE `%s`.`%s` AS `%s`.`%s` "
                + "ENGINE = Distributed('%s', '%s', '%s', cityHash64(col_1))",
            schemaName,
            sqlNonClusterDistributedTableName,
            schemaName,
            sqlNonClusterLocalTableName,
            ClickHouseContainer.DEFAULT_CLUSTER_NAME,
            schemaName,
            sqlNonClusterLocalTableName));

    TableCatalog tableCatalog = catalog.asTableCatalog();
    Table sqlLocalTable =
        tableCatalog.loadTable(NameIdentifier.of(schemaName, sqlClusterLocalTableName));
    Assertions.assertEquals(
        ENGINE.MERGETREE.getValue(), sqlLocalTable.properties().get(GRAVITINO_ENGINE_KEY));
    Assertions.assertEquals("false", sqlLocalTable.properties().get(ON_CLUSTER));

    Table sqlDistributedTable =
        tableCatalog.loadTable(NameIdentifier.of(schemaName, sqlClusterDistributedTableName));
    Assertions.assertEquals(
        ENGINE.DISTRIBUTED.getValue(), sqlDistributedTable.properties().get(GRAVITINO_ENGINE_KEY));
    Assertions.assertEquals("false", sqlDistributedTable.properties().get(ON_CLUSTER));
    Assertions.assertEquals(
        ClickHouseContainer.DEFAULT_CLUSTER_NAME,
        sqlDistributedTable.properties().get(CLUSTER_NAME));
    Assertions.assertEquals(schemaName, sqlDistributedTable.properties().get(REMOTE_DATABASE));
    Assertions.assertEquals(
        sqlClusterLocalTableName, sqlDistributedTable.properties().get(REMOTE_TABLE));

    Table sqlNonClusterDistributedTable =
        tableCatalog.loadTable(NameIdentifier.of(schemaName, sqlNonClusterDistributedTableName));
    Assertions.assertEquals(
        ENGINE.DISTRIBUTED.getValue(),
        sqlNonClusterDistributedTable.properties().get(GRAVITINO_ENGINE_KEY));
    Assertions.assertEquals("false", sqlNonClusterDistributedTable.properties().get(ON_CLUSTER));
    Assertions.assertEquals(
        ClickHouseContainer.DEFAULT_CLUSTER_NAME,
        sqlNonClusterDistributedTable.properties().get(CLUSTER_NAME));
    Assertions.assertEquals(
        schemaName, sqlNonClusterDistributedTable.properties().get(REMOTE_DATABASE));
    Assertions.assertEquals(
        sqlNonClusterLocalTableName, sqlNonClusterDistributedTable.properties().get(REMOTE_TABLE));
  }

  @Test
  public void testCreateClusterSchemaByApiAndLoadSqlCreatedClusterDatabase() {
    final String apiClusterComment = "cluster schema from gravitino api";
    final String sqlClusterComment = "cluster schema from sql";
    Schema apiClusterSchema =
        catalog
            .asSchemas()
            .createSchema(apiClusterSchemaName, apiClusterComment, clusterSchemaProperties());
    Schema loadedApiClusterSchema = catalog.asSchemas().loadSchema(apiClusterSchemaName);
    Assertions.assertEquals(apiClusterSchema.name(), loadedApiClusterSchema.name());
    Assertions.assertEquals(apiClusterComment, loadedApiClusterSchema.comment());
    Assertions.assertEquals("true", loadedApiClusterSchema.properties().get(ON_CLUSTER));
    Assertions.assertTrue(
        StringUtils.isNotBlank(loadedApiClusterSchema.properties().get(CLUSTER_NAME)));

    clickHouseService.executeQuery(
        String.format(
            "CREATE DATABASE `%s` ON CLUSTER `%s` COMMENT 'cluster schema from sql'",
            sqlClusterSchemaName, ClickHouseContainer.DEFAULT_CLUSTER_NAME));

    Schema loadedSqlClusterSchema = catalog.asSchemas().loadSchema(sqlClusterSchemaName);
    Assertions.assertEquals(sqlClusterSchemaName, loadedSqlClusterSchema.name());
    Assertions.assertEquals(sqlClusterComment, loadedSqlClusterSchema.comment());
    Assertions.assertEquals("true", loadedSqlClusterSchema.properties().get(ON_CLUSTER));
    Assertions.assertEquals(
        ClickHouseContainer.DEFAULT_CLUSTER_NAME,
        loadedSqlClusterSchema.properties().get(CLUSTER_NAME));
    Assertions.assertEquals(
        sqlClusterSchemaName,
        clickHouseService.loadSchema(NameIdentifier.of(sqlClusterSchemaName)).name());

    catalog.asSchemas().dropSchema(apiClusterSchemaName, false);
    catalog.asSchemas().dropSchema(sqlClusterSchemaName, false);
  }

  @Test
  public void testLoadNonClusterDatabaseProperties() {
    final String apiNonClusterComment = "non cluster schema from gravitino api";
    final String sqlNonClusterComment = "non cluster schema from sql";

    Schema apiNonClusterSchema =
        catalog
            .asSchemas()
            .createSchema(apiNonClusterSchemaName, apiNonClusterComment, Collections.emptyMap());
    Schema loadedApiNonClusterSchema = catalog.asSchemas().loadSchema(apiNonClusterSchemaName);
    Assertions.assertEquals(apiNonClusterSchema.name(), loadedApiNonClusterSchema.name());
    Assertions.assertEquals(apiNonClusterComment, loadedApiNonClusterSchema.comment());
    Assertions.assertEquals("false", loadedApiNonClusterSchema.properties().get(ON_CLUSTER));
    Assertions.assertFalse(loadedApiNonClusterSchema.properties().containsKey(CLUSTER_NAME));

    clickHouseService.executeQuery(
        String.format(
            "CREATE DATABASE `%s` COMMENT '%s'", sqlNonClusterSchemaName, sqlNonClusterComment));
    Schema loadedSqlNonClusterSchema = catalog.asSchemas().loadSchema(sqlNonClusterSchemaName);
    Assertions.assertEquals(sqlNonClusterSchemaName, loadedSqlNonClusterSchema.name());
    Assertions.assertEquals(sqlNonClusterComment, loadedSqlNonClusterSchema.comment());
    Assertions.assertEquals("false", loadedSqlNonClusterSchema.properties().get(ON_CLUSTER));
    Assertions.assertFalse(loadedSqlNonClusterSchema.properties().containsKey(CLUSTER_NAME));

    catalog.asSchemas().dropSchema(apiNonClusterSchemaName, false);
    catalog.asSchemas().dropSchema(sqlNonClusterSchemaName, false);
  }
}
