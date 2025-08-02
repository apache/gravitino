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
package org.apache.gravitino.catalog.starrocks.integration.test;

import static org.apache.gravitino.integration.test.util.ITUtils.assertPartition;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.StarRocksContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.partitions.ListPartition;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.partitions.RangePartition;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.utils.RandomNameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@Tag("gravitino-docker-test")
public class CatalogStarRocksIT extends BaseIT {

  private static final String provider = "jdbc-starrocks";

  private static final String DRIVER_CLASS_NAME = "com.mysql.jdbc.Driver";

  public String metalakeName = GravitinoITUtils.genRandomName("starrocks_it_metalake");
  public String catalogName = GravitinoITUtils.genRandomName("starrocks_it_catalog");
  public String schemaName = GravitinoITUtils.genRandomName("starrocks_it_schema");
  public String tableName = GravitinoITUtils.genRandomName("starrocks_it_table");

  public String table_comment = "table_comment_by_gravitino_it";

  // StarRocks doesn't support schema comment
  public String schema_comment = null;
  public String STARROCKS_COL_NAME1 = "starrocks_col_name1";
  public String STARROCKS_COL_NAME2 = "starrocks_col_name2";
  public String STARROCKS_COL_NAME3 = "starrocks_col_name3";
  public String STARROCKS_COL_NAME4 = "starrocks_col_name4";

  // Because the creation of Schema Change is an asynchronous process, we need to wait for a while
  // For more information, you can refer to the comment in
  // StarRocksTableOperations.generateAlterTableSql().
  private static final long MAX_WAIT_IN_SECONDS = 30;

  private static final long WAIT_INTERVAL_IN_SECONDS = 1;

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private GravitinoMetalake metalake;
  private String jdbcUrl;

  protected Catalog catalog;

  @BeforeAll
  public void startup() throws IOException {
    containerSuite.startStarRocksContainer();

    createMetalake();
    createCatalog();
    createSchema();
  }

  @AfterAll
  public void stop() {
    clearTableAndSchema();
    metalake.dropCatalog(catalogName, true);
    client.dropMetalake(metalakeName, true);
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
    GravitinoMetalake[] gravitinoMetaLakes = client.listMetalakes();
    assertEquals(0, gravitinoMetaLakes.length);

    client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    GravitinoMetalake loadMetalake = client.loadMetalake(metalakeName);
    assertEquals(metalakeName, loadMetalake.name());

    metalake = loadMetalake;
  }

  private void createCatalog() {
    Map<String, String> catalogProperties = Maps.newHashMap();

    StarRocksContainer starRocksContainer = containerSuite.getStarRocksContainer();

    jdbcUrl =
        String.format(
            "jdbc:mysql://%s:%d/",
            starRocksContainer.getContainerIpAddress(), StarRocksContainer.FE_MYSQL_PORT);

    catalogProperties.put(JdbcConfig.JDBC_URL.getKey(), jdbcUrl);
    catalogProperties.put(JdbcConfig.JDBC_DRIVER.getKey(), DRIVER_CLASS_NAME);
    catalogProperties.put(JdbcConfig.USERNAME.getKey(), StarRocksContainer.USER_NAME);
    catalogProperties.put(JdbcConfig.PASSWORD.getKey(), "");

    Catalog createdCatalog =
        metalake.createCatalog(
            catalogName,
            Catalog.Type.RELATIONAL,
            provider,
            "starrocks catalog comment",
            catalogProperties);
    Catalog loadCatalog = metalake.loadCatalog(catalogName);
    assertEquals(createdCatalog, loadCatalog);

    catalog = loadCatalog;
  }

  private void createSchema() {
    NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName, schemaName);
    Map<String, String> prop = Maps.newHashMap();

    Schema createdSchema = catalog.asSchemas().createSchema(ident.name(), schema_comment, prop);
    Schema loadSchema = catalog.asSchemas().loadSchema(ident.name());
    assertEquals(createdSchema.name(), loadSchema.name());
  }

  private Column[] createColumns() {
    Column col1 =
        Column.of(
            STARROCKS_COL_NAME1, Types.IntegerType.get(), "col_1_comment", false, false, null);
    Column col2 = Column.of(STARROCKS_COL_NAME2, Types.VarCharType.of(10), "col_2_comment");
    Column col3 = Column.of(STARROCKS_COL_NAME3, Types.VarCharType.of(10), "col_3_comment");
    Column col4 =
        Column.of(STARROCKS_COL_NAME4, Types.DateType.get(), "col_4_comment", false, false, null);
    return new Column[] {col1, col2, col3, col4};
  }

  private Map<String, String> createTableProperties() {
    return ImmutableMap.of();
  }

  private Distribution createDistribution() {
    return Distributions.hash(2, NamedReference.field(STARROCKS_COL_NAME1));
  }

  @Test
  void testStarRocksSchemaBasicOperation() {
    SupportsSchemas schemas = catalog.asSchemas();

    // test list schemas
    String[] schemaNames = schemas.listSchemas();
    assertTrue(Arrays.asList(schemaNames).contains(schemaName));

    // test create schema already exists
    String testSchemaName = GravitinoITUtils.genRandomName("create_schema_test");
    NameIdentifier schemaIdent = NameIdentifier.of(metalakeName, catalogName, testSchemaName);
    schemas.createSchema(schemaIdent.name(), schema_comment, Collections.emptyMap());

    List<String> schemaNameList = Arrays.asList(schemas.listSchemas());
    assertTrue(schemaNameList.contains(testSchemaName));

    assertThrows(
        SchemaAlreadyExistsException.class,
        () -> schemas.createSchema(schemaIdent.name(), schema_comment, Collections.emptyMap()));

    // test drop schema
    assertTrue(schemas.dropSchema(schemaIdent.name(), false));

    // check schema is deleted
    // 1. check by load schema
    assertThrows(NoSuchSchemaException.class, () -> schemas.loadSchema(schemaIdent.name()));

    // 2. check by list schema
    schemaNameList = Arrays.asList(schemas.listSchemas());
    assertFalse(schemaNameList.contains(testSchemaName));

    // test drop schema not exists
    NameIdentifier notExistsSchemaIdent = NameIdentifier.of(metalakeName, catalogName, "no-exits");
    assertFalse(schemas.dropSchema(notExistsSchemaIdent.name(), false));
  }

  @Test
  void testDropStarRocksSchema() {
    String schemaName = GravitinoITUtils.genRandomName("starrocks_it_schema_dropped").toLowerCase();

    catalog.asSchemas().createSchema(schemaName, "", ImmutableMap.of());

    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(schemaName, tableName),
            createColumns(),
            "Created by Gravitino client",
            createTableProperties(),
            Transforms.EMPTY_TRANSFORM,
            createDistribution(),
            null);

    // Try to drop a database, and cascade equals to false, it should not be allowed.
    Throwable excep =
        assertThrows(
            RuntimeException.class, () -> catalog.asSchemas().dropSchema(schemaName, false));
    assertTrue(excep.getMessage().contains("the value of cascade should be true."));

    // Check the database still exists
    catalog.asSchemas().loadSchema(schemaName);

    // Try to drop a database, and cascade equals to true, it should be allowed.
    assertTrue(catalog.asSchemas().dropSchema(schemaName, true));

    // Check database has been dropped
    SupportsSchemas schemas = catalog.asSchemas();
    assertThrows(NoSuchSchemaException.class, () -> schemas.loadSchema(schemaName));
  }

  @Test
  void testSchemaWithIllegalName() {
    SupportsSchemas schemas = catalog.asSchemas();
    String databaseName = RandomNameUtils.genRandomName("it_db");
    Map<String, String> properties = new HashMap<>();
    String comment = "comment";

    // should throw an exception with string that might contain SQL injection
    String sqlInjection = databaseName + "`; DROP TABLE important_table; -- ";
    assertThrows(
        IllegalArgumentException.class,
        () -> schemas.createSchema(sqlInjection, comment, properties));
    assertThrows(IllegalArgumentException.class, () -> schemas.dropSchema(sqlInjection, false));

    String sqlInjection1 = databaseName + "`; SLEEP(10); -- ";
    assertThrows(
        IllegalArgumentException.class,
        () -> schemas.createSchema(sqlInjection1, comment, properties));
    assertThrows(IllegalArgumentException.class, () -> schemas.dropSchema(sqlInjection1, false));

    String sqlInjection2 =
        databaseName + "`; UPDATE Users SET password = 'newpassword' WHERE username = 'admin'; -- ";
    assertThrows(
        IllegalArgumentException.class,
        () -> schemas.createSchema(sqlInjection2, comment, properties));
    assertThrows(IllegalArgumentException.class, () -> schemas.dropSchema(sqlInjection2, false));

    // should throw an exception with input that has more than 64 characters
    String invalidInput = StringUtils.repeat("a", 65);
    assertThrows(
        IllegalArgumentException.class,
        () -> schemas.createSchema(invalidInput, comment, properties));
    assertThrows(IllegalArgumentException.class, () -> schemas.dropSchema(invalidInput, false));
  }

  @Test
  void testStarRocksTableBasicOperation() {
    // create a table
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
    Column[] columns = createColumns();

    Distribution distribution = createDistribution();

    Map<String, String> properties = createTableProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    tableCatalog.createTable(
        tableIdentifier,
        columns,
        table_comment,
        properties,
        Transforms.EMPTY_TRANSFORM,
        distribution,
        null,
        null);

    // load table
    Table loadTable = tableCatalog.loadTable(tableIdentifier);
    ITUtils.assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(columns),
        properties,
        null,
        Transforms.EMPTY_TRANSFORM,
        loadTable);

    // rename table
    String newTableName = GravitinoITUtils.genRandomName("new_table_name");
    tableCatalog.alterTable(tableIdentifier, TableChange.rename(newTableName));
    NameIdentifier newTableIdentifier = NameIdentifier.of(schemaName, newTableName);
    Table renamedTable = tableCatalog.loadTable(newTableIdentifier);
    ITUtils.assertionsTableInfo(
        newTableName,
        table_comment,
        Arrays.asList(columns),
        properties,
        null,
        Transforms.EMPTY_TRANSFORM,
        renamedTable);
  }

  @Test
  void testStarRocksIllegalTableName() {
    Map<String, String> properties = createTableProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    String table_name = "t123";

    String t1_name = table_name + "`; DROP TABLE important_table; -- ";
    Column t1_col = Column.of(t1_name, Types.LongType.get(), "id", false, false, null);
    Column[] columns = {t1_col};
    NameIdentifier tableIdentifier =
        NameIdentifier.of(metalakeName, catalogName, schemaName, t1_name);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            tableCatalog.createTable(
                tableIdentifier,
                columns,
                table_comment,
                properties,
                Transforms.EMPTY_TRANSFORM,
                Distributions.NONE,
                new SortOrder[0],
                null));
    assertThrows(
        IllegalArgumentException.class, () -> catalog.asTableCatalog().dropTable(tableIdentifier));

    String t2_name = table_name + "`; SLEEP(10); -- ";
    Column t2_col = Column.of(t2_name, Types.LongType.get(), "id", false, false, null);
    Column[] columns2 = new Column[] {t2_col};
    NameIdentifier tableIdentifier2 =
        NameIdentifier.of(metalakeName, catalogName, schemaName, t2_name);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            tableCatalog.createTable(
                tableIdentifier2,
                columns2,
                table_comment,
                properties,
                Transforms.EMPTY_TRANSFORM,
                Distributions.NONE,
                new SortOrder[0],
                null));
    assertThrows(
        IllegalArgumentException.class, () -> catalog.asTableCatalog().dropTable(tableIdentifier2));

    String t3_name =
        table_name + "`; UPDATE Users SET password = 'newpassword' WHERE username = 'admin'; -- ";
    Column t3_col = Column.of(t3_name, Types.LongType.get(), "id", false, false, null);
    Column[] columns3 = new Column[] {t3_col};
    NameIdentifier tableIdentifier3 =
        NameIdentifier.of(metalakeName, catalogName, schemaName, t3_name);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            tableCatalog.createTable(
                tableIdentifier3,
                columns3,
                table_comment,
                properties,
                Transforms.EMPTY_TRANSFORM,
                Distributions.NONE,
                new SortOrder[0],
                null));
    assertThrows(
        IllegalArgumentException.class, () -> catalog.asTableCatalog().dropTable(tableIdentifier3));

    String invalidInput = StringUtils.repeat("a", 65);
    Column t4_col = Column.of(invalidInput, Types.LongType.get(), "id", false, false, null);
    Column[] columns4 = new Column[] {t4_col};
    NameIdentifier tableIdentifier4 =
        NameIdentifier.of(metalakeName, catalogName, schemaName, invalidInput);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            tableCatalog.createTable(
                tableIdentifier4,
                columns4,
                table_comment,
                properties,
                Transforms.EMPTY_TRANSFORM,
                Distributions.NONE,
                new SortOrder[0],
                null));
    assertThrows(
        IllegalArgumentException.class, () -> catalog.asTableCatalog().dropTable(tableIdentifier4));
  }

  @Test
  void testTestConnection() {
    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put(JdbcConfig.JDBC_URL.getKey(), jdbcUrl);
    catalogProperties.put(JdbcConfig.JDBC_DRIVER.getKey(), DRIVER_CLASS_NAME);
    catalogProperties.put(JdbcConfig.USERNAME.getKey(), StarRocksContainer.USER_NAME);
    catalogProperties.put(JdbcConfig.PASSWORD.getKey(), "wrong_password");

    Exception exception =
        assertThrows(
            ConnectionFailedException.class,
            () ->
                metalake.testConnection(
                    GravitinoITUtils.genRandomName("starrocks_it_catalog"),
                    Catalog.Type.RELATIONAL,
                    provider,
                    "starrocks catalog comment",
                    catalogProperties));
    Assertions.assertTrue(exception.getMessage().contains("Access denied for user"));
  }

  @Test
  void testAlterStarRocksTable() {
    // create a table
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
    Column[] columns = createColumns();

    Distribution distribution = createDistribution();

    Map<String, String> properties = createTableProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    tableCatalog.createTable(
        tableIdentifier,
        columns,
        table_comment,
        properties,
        Transforms.EMPTY_TRANSFORM,
        distribution,
        null,
        null);
    Table loadedTable = tableCatalog.loadTable(tableIdentifier);

    ITUtils.assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(columns),
        properties,
        null,
        Transforms.EMPTY_TRANSFORM,
        loadedTable);

    // Alter column type
    tableCatalog.alterTable(
        tableIdentifier,
        TableChange.updateColumnType(
            new String[] {STARROCKS_COL_NAME3}, Types.VarCharType.of(255)));

    Awaitility.await()
        .atMost(MAX_WAIT_IN_SECONDS, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL_IN_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                ITUtils.assertColumn(
                    Column.of(STARROCKS_COL_NAME3, Types.VarCharType.of(255), "col_3_comment"),
                    tableCatalog.loadTable(tableIdentifier).columns()[2]));

    // add new column
    tableCatalog.alterTable(
        tableIdentifier,
        TableChange.addColumn(
            new String[] {"col_5"}, Types.VarCharType.of(255), "col_5_comment", true));
    Awaitility.await()
        .atMost(MAX_WAIT_IN_SECONDS, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL_IN_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(
            () -> assertEquals(5, tableCatalog.loadTable(tableIdentifier).columns().length));

    ITUtils.assertColumn(
        Column.of("col_5", Types.VarCharType.of(255), "col_5_comment"),
        tableCatalog.loadTable(tableIdentifier).columns()[4]);

    // change column position
    // TODO: change column position is unstable, add it later

    // drop column
    tableCatalog.alterTable(
        tableIdentifier, TableChange.deleteColumn(new String[] {"col_5"}, true));

    Awaitility.await()
        .atMost(MAX_WAIT_IN_SECONDS, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL_IN_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(
            () -> assertEquals(4, tableCatalog.loadTable(tableIdentifier).columns().length));
  }

  @Test
  void testStarRocksTablePartitionOperation() {
    // create a partitioned table
    String tableName = GravitinoITUtils.genRandomName("test_partitioned_table");
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
    Column[] columns = createColumns();
    Distribution distribution = createDistribution();
    Map<String, String> properties = createTableProperties();
    Transform[] partitioning = {Transforms.list(new String[][] {{STARROCKS_COL_NAME1}})};
    TableCatalog tableCatalog = catalog.asTableCatalog();
    tableCatalog.createTable(
        tableIdentifier,
        columns,
        table_comment,
        properties,
        partitioning,
        distribution,
        null,
        null);

    // load table
    Table loadTable = tableCatalog.loadTable(tableIdentifier);
    ITUtils.assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(columns),
        properties,
        null,
        partitioning,
        loadTable);

    // get table partition operations
    SupportsPartitions tablePartitionOperations = loadTable.supportPartitions();

    // assert partition info when there is no partitions actually
    String[] emptyPartitionNames = tablePartitionOperations.listPartitionNames();
    assertEquals(0, emptyPartitionNames.length);
    Partition[] emptyPartitions = tablePartitionOperations.listPartitions();
    assertEquals(0, emptyPartitions.length);

    // get non-existing partition
    assertThrows(NoSuchPartitionException.class, () -> tablePartitionOperations.getPartition("p1"));

    // add partition with incorrect type
    Partition incorrectType =
        Partitions.range("p1", Literals.NULL, Literals.NULL, Collections.emptyMap());
    assertThrows(
        IllegalArgumentException.class, () -> tablePartitionOperations.addPartition(incorrectType));

    // add partition with incorrect value
    Partition incorrectValue =
        Partitions.list(
            "p1", new Literal[][] {{Literals.NULL, Literals.NULL}}, Collections.emptyMap());
    assertThrows(
        IllegalArgumentException.class,
        () -> tablePartitionOperations.addPartition(incorrectValue));

    // add partition
    Literal[][] p1Values = {{Literals.integerLiteral(1)}};
    Literal[][] p2Values = {{Literals.integerLiteral(2)}};
    Literal[][] p3Values = {{Literals.integerLiteral(3)}};

    ListPartition p1 = Partitions.list("p1", p1Values, Collections.emptyMap());
    ListPartition p2 = Partitions.list("p2", p2Values, Collections.emptyMap());
    ListPartition p3 = Partitions.list("p3", p3Values, Collections.emptyMap());
    ListPartition p1Added = (ListPartition) tablePartitionOperations.addPartition(p1);
    assertPartition(p1, p1Added);
    ListPartition p2Added = (ListPartition) tablePartitionOperations.addPartition(p2);
    assertPartition(p2, p2Added);
    ListPartition p3Added = (ListPartition) tablePartitionOperations.addPartition(p3);
    assertPartition(p3, p3Added);

    // check partitions
    Set<String> partitionNames =
        Arrays.stream(tablePartitionOperations.listPartitionNames()).collect(Collectors.toSet());
    assertEquals(3, partitionNames.size());
    assertTrue(partitionNames.contains("p1"));
    assertTrue(partitionNames.contains("p2"));
    assertTrue(partitionNames.contains("p3"));

    Map<String, ListPartition> partitions =
        Arrays.stream(tablePartitionOperations.listPartitions())
            .collect(Collectors.toMap(p -> p.name(), p -> (ListPartition) p));
    assertEquals(3, partitions.size());
    assertPartition(p1, partitions.get("p1"));
    assertPartition(p2, partitions.get("p2"));
    assertPartition(p3, partitions.get("p3"));

    assertPartition(p1, tablePartitionOperations.getPartition("p1"));
    assertPartition(p2, tablePartitionOperations.getPartition("p2"));
    assertPartition(p3, tablePartitionOperations.getPartition("p3"));

    // drop partition
    assertTrue(tablePartitionOperations.dropPartition("p3"));
    partitionNames =
        Arrays.stream(tablePartitionOperations.listPartitionNames()).collect(Collectors.toSet());
    assertEquals(2, partitionNames.size());
    assertFalse(partitionNames.contains("p3"));
    assertThrows(NoSuchPartitionException.class, () -> tablePartitionOperations.getPartition("p3"));

    // drop non-existing partition
    assertFalse(tablePartitionOperations.dropPartition("p3"));
  }

  @Test
  void testCreatePartitionedTable() {
    // create a range-partitioned table with assignments
    String tableName = GravitinoITUtils.genRandomName("test_create_range_partitioned_table");
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
    Column[] columns = createColumns();
    Distribution distribution = createDistribution();

    Map<String, String> properties = createTableProperties();
    Literal todayLiteral = Literals.of("2024-07-24", Types.DateType.get());
    Literal tomorrowLiteral = Literals.of("2024-07-25", Types.DateType.get());
    RangePartition p1 = Partitions.range("p1", todayLiteral, Literals.NULL, Collections.emptyMap());
    RangePartition p2 =
        Partitions.range("p2", tomorrowLiteral, todayLiteral, Collections.emptyMap());
    RangePartition p3 =
        Partitions.range("p3", Literals.NULL, Literals.NULL, Collections.emptyMap());
    Transform[] partitioning = {
      Transforms.range(new String[] {STARROCKS_COL_NAME4}, new RangePartition[] {p1, p2, p3})
    };
    TableCatalog tableCatalog = catalog.asTableCatalog();
    tableCatalog.createTable(
        tableIdentifier,
        columns,
        table_comment,
        properties,
        partitioning,
        distribution,
        null,
        null);
    Table loadedTable = tableCatalog.loadTable(tableIdentifier);
    ITUtils.assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(columns),
        properties,
        null,
        new Transform[] {Transforms.range(new String[] {STARROCKS_COL_NAME4})},
        loadedTable);

    // assert partition info
    SupportsPartitions tablePartitionOperations = loadedTable.supportPartitions();
    Map<String, RangePartition> loadedRangePartitions =
        Arrays.stream(tablePartitionOperations.listPartitions())
            .collect(Collectors.toMap(Partition::name, p -> (RangePartition) p));
    assertTrue(loadedRangePartitions.size() == 3);
    assertTrue(loadedRangePartitions.containsKey("p1"));
    assertPartition(
        Partitions.range(
            "p1",
            todayLiteral,
            Literals.of("0000-01-01", Types.DateType.get()),
            Collections.emptyMap()),
        loadedRangePartitions.get("p1"));
    assertTrue(loadedRangePartitions.containsKey("p2"));
    assertPartition(p2, loadedRangePartitions.get("p2"));
    assertTrue(loadedRangePartitions.containsKey("p3"));
    assertPartition(
        Partitions.range(
            "p3",
            Literals.of("MAXVALUE", Types.DateType.get()),
            tomorrowLiteral,
            Collections.emptyMap()),
        loadedRangePartitions.get("p3"));

    // create a list-partitioned table with assignments
    tableName = GravitinoITUtils.genRandomName("test_create_list_partitioned_table");
    tableIdentifier = NameIdentifier.of(schemaName, tableName);
    Literal<Integer> integerLiteral1 = Literals.integerLiteral(1);
    Literal<Integer> integerLiteral2 = Literals.integerLiteral(2);
    ListPartition p4 =
        Partitions.list(
            "p4",
            new Literal[][] {{integerLiteral1, todayLiteral}, {integerLiteral1, tomorrowLiteral}},
            Collections.emptyMap());
    ListPartition p5 =
        Partitions.list(
            "p5",
            new Literal[][] {{integerLiteral2, todayLiteral}, {integerLiteral2, tomorrowLiteral}},
            Collections.emptyMap());
    partitioning =
        new Transform[] {
          Transforms.list(
              new String[][] {{STARROCKS_COL_NAME1}, {STARROCKS_COL_NAME4}},
              new ListPartition[] {p4, p5})
        };
    tableCatalog.createTable(
        tableIdentifier,
        columns,
        table_comment,
        properties,
        partitioning,
        distribution,
        null,
        null);
    loadedTable = tableCatalog.loadTable(tableIdentifier);
    ITUtils.assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(columns),
        properties,
        null,
        new Transform[] {
          Transforms.list(new String[][] {{STARROCKS_COL_NAME1}, {STARROCKS_COL_NAME4}})
        },
        loadedTable);

    // assert partition info
    tablePartitionOperations = loadedTable.supportPartitions();
    Map<String, ListPartition> loadedListPartitions =
        Arrays.stream(tablePartitionOperations.listPartitions())
            .collect(Collectors.toMap(Partition::name, p -> (ListPartition) p));
    assertTrue(loadedListPartitions.size() == 2);
    assertTrue(loadedListPartitions.containsKey("p4"));
    assertPartition(p4, loadedListPartitions.get("p4"));
    assertTrue(loadedListPartitions.containsKey("p5"));
    assertPartition(p5, loadedListPartitions.get("p5"));
  }

  @Test
  void testTableWithTimeStampColumn() {
    // create a table
    String tableName = GravitinoITUtils.genRandomName("test_table_with_timestamp_column");
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
    Column[] columns = createColumns();
    columns =
        ArrayUtils.add(columns, Column.of("timestamp_col", Types.TimestampType.withoutTimeZone()));
    Distribution distribution = createDistribution();

    Map<String, String> properties = createTableProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    tableCatalog.createTable(
        tableIdentifier,
        columns,
        table_comment,
        properties,
        Transforms.EMPTY_TRANSFORM,
        distribution,
        null,
        null);

    // load table
    Table loadTable = tableCatalog.loadTable(tableIdentifier);
    Column timestampColumn =
        Arrays.stream(loadTable.columns())
            .filter(c -> "timestamp_col".equals(c.name()))
            .findFirst()
            .get();

    Assertions.assertEquals(Types.TimestampType.withoutTimeZone(), timestampColumn.dataType());
  }

  @Test
  void testNonPartitionedTable() {
    // create a non-partitioned table
    String tableName = GravitinoITUtils.genRandomName("test_non_partitioned_table");
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
    Column[] columns = createColumns();
    Distribution distribution = createDistribution();
    Map<String, String> properties = createTableProperties();
    Transform[] partitioning = Transforms.EMPTY_TRANSFORM;
    TableCatalog tableCatalog = catalog.asTableCatalog();
    tableCatalog.createTable(
        tableIdentifier,
        columns,
        table_comment,
        properties,
        partitioning,
        distribution,
        null,
        null);

    // load table
    Table loadTable = tableCatalog.loadTable(tableIdentifier);
    ITUtils.assertionsTableInfo(
        tableName,
        table_comment,
        Arrays.asList(columns),
        properties,
        null,
        partitioning,
        loadTable);

    // get table partition operations
    SupportsPartitions tablePartitionOperations = loadTable.supportPartitions();

    assertThrows(
        UnsupportedOperationException.class, () -> tablePartitionOperations.listPartitionNames());

    assertThrows(
        UnsupportedOperationException.class, () -> tablePartitionOperations.listPartitions());

    assertThrows(
        UnsupportedOperationException.class,
        () ->
            tablePartitionOperations.addPartition(
                Partitions.range("p1", Literals.NULL, Literals.NULL, Collections.emptyMap())));

    assertThrows(
        UnsupportedOperationException.class, () -> tablePartitionOperations.getPartition("p1"));

    assertThrows(
        UnsupportedOperationException.class, () -> tablePartitionOperations.dropPartition("p1"));
  }

  @Test
  void testAllDistribution() {
    Distribution[] distributions =
        new Distribution[] {
          Distributions.even(1, Expression.EMPTY_EXPRESSION),
          Distributions.hash(1, NamedReference.field(STARROCKS_COL_NAME1)),
          Distributions.even(10, Expression.EMPTY_EXPRESSION),
          Distributions.hash(0, NamedReference.field(STARROCKS_COL_NAME1)),
          Distributions.hash(11, NamedReference.field(STARROCKS_COL_NAME1)),
          Distributions.hash(
              12,
              NamedReference.field(STARROCKS_COL_NAME1),
              NamedReference.field(STARROCKS_COL_NAME2))
        };

    for (Distribution distribution : distributions) {
      String tableName = GravitinoITUtils.genRandomName("test_distribution_table");
      NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
      Column[] columns = createColumns();
      Map<String, String> properties = createTableProperties();
      Transform[] partitioning = Transforms.EMPTY_TRANSFORM;
      TableCatalog tableCatalog = catalog.asTableCatalog();
      tableCatalog.createTable(
          tableIdentifier,
          columns,
          table_comment,
          properties,
          partitioning,
          distribution,
          null,
          null);
      // load table
      Table loadTable = tableCatalog.loadTable(tableIdentifier);

      Assertions.assertEquals(distribution.strategy(), loadTable.distribution().strategy());
      Assertions.assertArrayEquals(
          distribution.expressions(), loadTable.distribution().expressions());

      tableCatalog.dropTable(tableIdentifier);
    }
  }

  @Test
  void testAllDistributionWithAuto() {
    Distribution distribution =
        Distributions.auto(Strategy.HASH, NamedReference.field(STARROCKS_COL_NAME1));

    String tableName = GravitinoITUtils.genRandomName("test_distribution_table");
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
    Column[] columns = createColumns();
    Map<String, String> properties = createTableProperties();
    Transform[] partitioning = Transforms.EMPTY_TRANSFORM;
    TableCatalog tableCatalog = catalog.asTableCatalog();
    tableCatalog.createTable(
        tableIdentifier,
        columns,
        table_comment,
        properties,
        partitioning,
        distribution,
        null,
        null);
    // load table
    Table loadTable = tableCatalog.loadTable(tableIdentifier);

    Assertions.assertEquals(distribution.strategy(), loadTable.distribution().strategy());
    Assertions.assertEquals(distribution.number(), loadTable.distribution().number());
    Assertions.assertArrayEquals(
        distribution.expressions(), loadTable.distribution().expressions());
    tableCatalog.dropTable(tableIdentifier);
  }

  @Test
  void testColumnDefaultValue() {
    // create a table
    String tableName = GravitinoITUtils.genRandomName("test_table_with_defaule_value");
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
    Column[] columns = createColumns();
    Column col_default_value_date =
        Column.of(
            "col_default_value_date",
            Types.DateType.get(),
            "col_default_value_date",
            false,
            false,
            Literals.dateLiteral("2024-01-01"));

    Column col_default_value_timestamp =
        Column.of(
            "col_default_value_timestamp",
            Types.TimestampType.withoutTimeZone(),
            "col_default_value_timestamp",
            false,
            false,
            Literals.timestampLiteral("2024-01-01T01:01:01"));

    Column col_default_value_timestamp_2 =
        Column.of(
            "col_default_value_timestamp_2",
            Types.TimestampType.withoutTimeZone(),
            "col_default_value_timestamp_2",
            false,
            false,
            DEFAULT_VALUE_OF_CURRENT_TIMESTAMP);
    columns =
        ArrayUtils.addAll(
            columns,
            col_default_value_date,
            col_default_value_timestamp,
            col_default_value_timestamp_2);
    Distribution distribution = createDistribution();

    Map<String, String> properties = createTableProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    tableCatalog.createTable(
        tableIdentifier,
        columns,
        table_comment,
        properties,
        Transforms.EMPTY_TRANSFORM,
        distribution,
        null,
        null);

    // load table
    Table loadTable = tableCatalog.loadTable(tableIdentifier);

    Column[] colDefaultValues =
        Arrays.stream(loadTable.columns())
            .filter(c -> c.name().startsWith("col_default_value_"))
            .toArray(Column[]::new);

    Assertions.assertEquals(
        Literals.dateLiteral(LocalDate.of(2024, 1, 1)), colDefaultValues[0].defaultValue());
    Assertions.assertEquals(
        Literals.timestampLiteral(LocalDateTime.of(2024, 1, 1, 1, 1, 1)),
        colDefaultValues[1].defaultValue());
    Assertions.assertEquals(DEFAULT_VALUE_OF_CURRENT_TIMESTAMP, colDefaultValues[2].defaultValue());
  }
}
