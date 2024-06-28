/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.doris.integration.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Schema;
import com.datastrato.gravitino.SupportsSchemas;
import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.datastrato.gravitino.client.GravitinoMetalake;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.DorisContainer;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.integration.test.util.ITUtils;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableCatalog;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.transforms.Transforms;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.rel.indexes.Indexes;
import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.utils.RandomNameUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@Tag("gravitino-docker-it")
@TestInstance(Lifecycle.PER_CLASS)
public class CatalogDorisIT extends AbstractIT {

  private static final String provider = "jdbc-doris";

  private static final String DRIVER_CLASS_NAME = "com.mysql.jdbc.Driver";

  public String metalakeName = GravitinoITUtils.genRandomName("doris_it_metalake");
  public String catalogName = GravitinoITUtils.genRandomName("doris_it_catalog");
  public String schemaName = GravitinoITUtils.genRandomName("doris_it_schema");
  public String tableName = GravitinoITUtils.genRandomName("doris_it_table");

  public String table_comment = "table_comment";

  // Doris doesn't support schema comment
  public String schema_comment = null;
  public String DORIS_COL_NAME1 = "doris_col_name1";
  public String DORIS_COL_NAME2 = "doris_col_name2";
  public String DORIS_COL_NAME3 = "doris_col_name3";

  // Because the creation of Schema Change is an asynchronous process, we need to wait for a while
  // For more information, you can refer to the comment in
  // DorisTableOperations.generateAlterTableSql().
  private static final long MAX_WAIT_IN_SECONDS = 30;

  private static final long WAIT_INTERVAL_IN_SECONDS = 1;

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private GravitinoMetalake metalake;

  protected Catalog catalog;

  @BeforeAll
  public void startup() throws IOException {
    containerSuite.startDorisContainer();

    createMetalake();
    createCatalog();
    createSchema();
  }

  @AfterAll
  public void stop() {
    clearTableAndSchema();
    metalake.dropCatalog(catalogName);
    AbstractIT.client.dropMetalake(metalakeName);
  }

  @AfterEach
  public void resetSchema() {
    clearTableAndSchema();
    createSchema();
  }

  private void clearTableAndSchema() {
    catalog.asSchemas().dropSchema(schemaName, true);
  }

  private void createMetalake() {
    GravitinoMetalake[] gravitinoMetaLakes = AbstractIT.client.listMetalakes();
    Assertions.assertEquals(0, gravitinoMetaLakes.length);

    GravitinoMetalake createdMetalake =
        AbstractIT.client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    GravitinoMetalake loadMetalake = AbstractIT.client.loadMetalake(metalakeName);
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
    catalogProperties.put(JdbcConfig.PASSWORD.getKey(), DorisContainer.PASSWORD);

    Catalog createdCatalog =
        metalake.createCatalog(
            catalogName,
            Catalog.Type.RELATIONAL,
            provider,
            "doris catalog comment",
            catalogProperties);
    Catalog loadCatalog = metalake.loadCatalog(catalogName);
    Assertions.assertEquals(createdCatalog, loadCatalog);

    catalog = loadCatalog;
  }

  private void createSchema() {
    NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName, schemaName);
    String propKey = "key";
    String propValue = "value";
    Map<String, String> prop = Maps.newHashMap();
    prop.put(propKey, propValue);

    Schema createdSchema = catalog.asSchemas().createSchema(ident.name(), schema_comment, prop);
    Schema loadSchema = catalog.asSchemas().loadSchema(ident.name());
    Assertions.assertEquals(createdSchema.name(), loadSchema.name());

    Assertions.assertEquals(createdSchema.properties().get(propKey), propValue);
  }

  private Column[] createColumns() {
    Column col1 = Column.of(DORIS_COL_NAME1, Types.IntegerType.get(), "col_1_comment");
    Column col2 = Column.of(DORIS_COL_NAME2, Types.VarCharType.of(10), "col_2_comment");
    Column col3 = Column.of(DORIS_COL_NAME3, Types.VarCharType.of(10), "col_3_comment");

    return new Column[] {col1, col2, col3};
  }

  private Map<String, String> createTableProperties() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("replication_allocation", "tag.location.default: 1");
    return properties;
  }

  private Distribution createDistribution() {
    return Distributions.hash(2, NamedReference.field(DORIS_COL_NAME1));
  }

  @Test
  void testDorisSchemaBasicOperation() {
    SupportsSchemas schemas = catalog.asSchemas();

    // test list schemas
    String[] schemaNames = schemas.listSchemas();
    Assertions.assertTrue(Arrays.asList(schemaNames).contains(schemaName));

    // test create schema already exists
    String testSchemaName = GravitinoITUtils.genRandomName("create_schema_test");
    NameIdentifier schemaIdent = NameIdentifier.of(metalakeName, catalogName, testSchemaName);
    schemas.createSchema(schemaIdent.name(), schema_comment, Collections.emptyMap());

    List<String> schemaNameList = Arrays.asList(schemas.listSchemas());
    Assertions.assertTrue(schemaNameList.contains(testSchemaName));

    Assertions.assertThrows(
        SchemaAlreadyExistsException.class,
        () -> {
          schemas.createSchema(schemaIdent.name(), schema_comment, Collections.emptyMap());
        });

    // test drop schema
    Assertions.assertTrue(schemas.dropSchema(schemaIdent.name(), false));

    // check schema is deleted
    // 1. check by load schema
    Assertions.assertThrows(
        NoSuchSchemaException.class, () -> schemas.loadSchema(schemaIdent.name()));

    // 2. check by list schema
    schemaNameList = Arrays.asList(schemas.listSchemas());
    Assertions.assertFalse(schemaNameList.contains(testSchemaName));

    // test drop schema not exists
    NameIdentifier notExistsSchemaIdent = NameIdentifier.of(metalakeName, catalogName, "no-exits");
    Assertions.assertFalse(schemas.dropSchema(notExistsSchemaIdent.name(), false));
  }

  @Test
  void testDropDorisSchema() {
    String schemaName = GravitinoITUtils.genRandomName("doris_it_schema_dropped").toLowerCase();

    catalog.asSchemas().createSchema(schemaName, "test_comment", ImmutableMap.of("key", "value"));

    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(schemaName, tableName),
            createColumns(),
            "Created by gravitino client",
            createTableProperties(),
            Transforms.EMPTY_TRANSFORM,
            createDistribution(),
            null);

    // Try to drop a database, and cascade equals to false, it should not be allowed.
    Throwable excep =
        Assertions.assertThrows(
            RuntimeException.class, () -> catalog.asSchemas().dropSchema(schemaName, false));
    Assertions.assertTrue(excep.getMessage().contains("the value of cascade should be true."));

    // Check the database still exists
    catalog.asSchemas().loadSchema(schemaName);

    // Try to drop a database, and cascade equals to true, it should be allowed.
    Assertions.assertTrue(catalog.asSchemas().dropSchema(schemaName, true));

    // Check database has been dropped
    SupportsSchemas schemas = catalog.asSchemas();
    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () -> {
          schemas.loadSchema(schemaName);
        });
  }

  @Test
  void testSchemaWithIllegalName() {
    SupportsSchemas schemas = catalog.asSchemas();
    String databaseName = RandomNameUtils.genRandomName("it_db");
    Map<String, String> properties = new HashMap<>();
    String comment = "comment";

    // should throw an exception with string that might contain SQL injection
    String sqlInjection = databaseName + "`; DROP TABLE important_table; -- ";
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          schemas.createSchema(sqlInjection, comment, properties);
        });
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          schemas.dropSchema(sqlInjection, false);
        });

    String sqlInjection1 = databaseName + "`; SLEEP(10); -- ";
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          schemas.createSchema(sqlInjection1, comment, properties);
        });
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          schemas.dropSchema(sqlInjection1, false);
        });

    String sqlInjection2 =
        databaseName + "`; UPDATE Users SET password = 'newpassword' WHERE username = 'admin'; -- ";
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          schemas.createSchema(sqlInjection2, comment, properties);
        });
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          schemas.dropSchema(sqlInjection2, false);
        });

    // should throw an exception with input that has more than 64 characters
    String invalidInput = StringUtils.repeat("a", 65);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          schemas.createSchema(invalidInput, comment, properties);
        });
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          schemas.dropSchema(invalidInput, false);
        });
  }

  @Test
  void testDorisTableBasicOperation() {
    // create a table
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
    Column[] columns = createColumns();

    Distribution distribution = createDistribution();

    Index[] indexes =
        new Index[] {
          Indexes.of(Index.IndexType.PRIMARY_KEY, "k1_index", new String[][] {{DORIS_COL_NAME1}})
        };

    Map<String, String> properties = createTableProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    Table createdTable =
        tableCatalog.createTable(
            tableIdentifier,
            columns,
            table_comment,
            properties,
            Transforms.EMPTY_TRANSFORM,
            distribution,
            null,
            indexes);

    ITUtils.assertionsTableInfo(
        tableName, table_comment, Arrays.asList(columns), properties, indexes, createdTable);

    // load table
    Table loadTable = tableCatalog.loadTable(tableIdentifier);
    ITUtils.assertionsTableInfo(
        tableName, table_comment, Arrays.asList(columns), properties, indexes, loadTable);

    // rename table
    String newTableName = GravitinoITUtils.genRandomName("new_table_name");
    tableCatalog.alterTable(tableIdentifier, TableChange.rename(newTableName));
    NameIdentifier newTableIdentifier = NameIdentifier.of(schemaName, newTableName);
    Table renamedTable = tableCatalog.loadTable(newTableIdentifier);
    ITUtils.assertionsTableInfo(
        newTableName, table_comment, Arrays.asList(columns), properties, indexes, renamedTable);
  }

  @Test
  void testDorisIllegalTableName() {
    Map<String, String> properties = createTableProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    String table_name = "t123";

    String t1_name = table_name + "`; DROP TABLE important_table; -- ";
    Column t1_col = Column.of(t1_name, Types.LongType.get(), "id", false, false, null);
    Column[] columns = {t1_col};
    Index[] t1_indexes = {Indexes.unique("u1_key", new String[][] {{t1_name}})};
    NameIdentifier tableIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, t1_name);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          tableCatalog.createTable(
              tableIdentifier,
              columns,
              table_comment,
              properties,
              Transforms.EMPTY_TRANSFORM,
              Distributions.NONE,
              new SortOrder[0],
              t1_indexes);
        });
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          catalog.asTableCatalog().dropTable(tableIdentifier);
        });

    String t2_name = table_name + "`; SLEEP(10); -- ";
    Column t2_col = Column.of(t2_name, Types.LongType.get(), "id", false, false, null);
    Index[] t2_indexes = {Indexes.unique("u2_key", new String[][] {{t2_name}})};
    Column[] columns2 = new Column[] {t2_col};
    NameIdentifier tableIdentifier2 =
        NameIdentifier.of(metalakeName, catalogName, schemaName, t2_name);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          tableCatalog.createTable(
              tableIdentifier2,
              columns2,
              table_comment,
              properties,
              Transforms.EMPTY_TRANSFORM,
              Distributions.NONE,
              new SortOrder[0],
              t2_indexes);
        });
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          catalog.asTableCatalog().dropTable(tableIdentifier2);
        });

    String t3_name =
        table_name + "`; UPDATE Users SET password = 'newpassword' WHERE username = 'admin'; -- ";
    Column t3_col = Column.of(t3_name, Types.LongType.get(), "id", false, false, null);
    Index[] t3_indexes = {Indexes.unique("u3_key", new String[][] {{t3_name}})};
    Column[] columns3 = new Column[] {t3_col};
    NameIdentifier tableIdentifier3 =
        NameIdentifier.of(metalakeName, catalogName, schemaName, t3_name);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          tableCatalog.createTable(
              tableIdentifier3,
              columns3,
              table_comment,
              properties,
              Transforms.EMPTY_TRANSFORM,
              Distributions.NONE,
              new SortOrder[0],
              t3_indexes);
        });
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          catalog.asTableCatalog().dropTable(tableIdentifier3);
        });

    String invalidInput = StringUtils.repeat("a", 65);
    Column t4_col = Column.of(invalidInput, Types.LongType.get(), "id", false, false, null);
    Index[] t4_indexes = {Indexes.unique("u4_key", new String[][] {{invalidInput}})};
    Column[] columns4 = new Column[] {t4_col};
    NameIdentifier tableIdentifier4 =
        NameIdentifier.of(metalakeName, catalogName, schemaName, invalidInput);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          tableCatalog.createTable(
              tableIdentifier4,
              columns4,
              table_comment,
              properties,
              Transforms.EMPTY_TRANSFORM,
              Distributions.NONE,
              new SortOrder[0],
              t4_indexes);
        });
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          catalog.asTableCatalog().dropTable(tableIdentifier4);
        });
  }

  @Test
  void testAlterDorisTable() {
    // create a table
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
    Column[] columns = createColumns();

    Distribution distribution = createDistribution();

    Index[] indexes =
        new Index[] {
          Indexes.of(Index.IndexType.PRIMARY_KEY, "k1_index", new String[][] {{DORIS_COL_NAME1}})
        };

    Map<String, String> properties = createTableProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    Table createdTable =
        tableCatalog.createTable(
            tableIdentifier,
            columns,
            table_comment,
            properties,
            Transforms.EMPTY_TRANSFORM,
            distribution,
            null,
            indexes);

    ITUtils.assertionsTableInfo(
        tableName, table_comment, Arrays.asList(columns), properties, indexes, createdTable);

    // Alter column type
    tableCatalog.alterTable(
        tableIdentifier,
        TableChange.updateColumnType(new String[] {DORIS_COL_NAME3}, Types.VarCharType.of(255)));

    Awaitility.await()
        .atMost(MAX_WAIT_IN_SECONDS, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL_IN_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                ITUtils.assertColumn(
                    Column.of(DORIS_COL_NAME3, Types.VarCharType.of(255), "col_3_comment"),
                    tableCatalog.loadTable(tableIdentifier).columns()[2]));

    // update column comment
    // Alter column type
    tableCatalog.alterTable(
        tableIdentifier,
        TableChange.updateColumnComment(new String[] {DORIS_COL_NAME3}, "new_comment"));

    Awaitility.await()
        .atMost(MAX_WAIT_IN_SECONDS, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL_IN_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                ITUtils.assertColumn(
                    Column.of(DORIS_COL_NAME3, Types.VarCharType.of(255), "new_comment"),
                    tableCatalog.loadTable(tableIdentifier).columns()[2]));

    // add new column
    tableCatalog.alterTable(
        tableIdentifier,
        TableChange.addColumn(
            new String[] {"col_4"}, Types.VarCharType.of(255), "col_4_comment", true));
    Awaitility.await()
        .atMost(MAX_WAIT_IN_SECONDS, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL_IN_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                Assertions.assertEquals(
                    4, tableCatalog.loadTable(tableIdentifier).columns().length));

    ITUtils.assertColumn(
        Column.of("col_4", Types.VarCharType.of(255), "col_4_comment"),
        tableCatalog.loadTable(tableIdentifier).columns()[3]);

    // change column position
    // TODO: change column position is unstable, add it later

    // drop column
    tableCatalog.alterTable(
        tableIdentifier, TableChange.deleteColumn(new String[] {"col_4"}, true));

    Awaitility.await()
        .atMost(MAX_WAIT_IN_SECONDS, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL_IN_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                Assertions.assertEquals(
                    3, tableCatalog.loadTable(tableIdentifier).columns().length));
  }

  @Test
  void testDorisIndex() {
    String tableName = GravitinoITUtils.genRandomName("test_add_index");

    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
    Column[] columns = createColumns();

    Distribution distribution = createDistribution();

    Map<String, String> properties = createTableProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    Table createdTable =
        tableCatalog.createTable(
            tableIdentifier,
            columns,
            table_comment,
            properties,
            Transforms.EMPTY_TRANSFORM,
            distribution,
            null);
    Assertions.assertEquals(createdTable.name(), tableName);

    // add index test.
    tableCatalog.alterTable(
        NameIdentifier.of(schemaName, tableName),
        TableChange.addIndex(
            Index.IndexType.PRIMARY_KEY, "k1_index", new String[][] {{DORIS_COL_NAME1}}));

    Awaitility.await()
        .atMost(MAX_WAIT_IN_SECONDS, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL_IN_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertEquals(
                    1,
                    tableCatalog
                        .loadTable(NameIdentifier.of(schemaName, tableName))
                        .index()
                        .length));

    // delete index and add new column and index.
    tableCatalog.alterTable(
        NameIdentifier.of(schemaName, tableName),
        TableChange.deleteIndex("k1_index", true),
        TableChange.addIndex(
            Index.IndexType.PRIMARY_KEY, "k2_index", new String[][] {{DORIS_COL_NAME2}}));

    Awaitility.await()
        .atMost(MAX_WAIT_IN_SECONDS, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL_IN_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertEquals(
                    1,
                    tableCatalog
                        .loadTable(NameIdentifier.of(schemaName, tableName))
                        .index()
                        .length));
  }
}
