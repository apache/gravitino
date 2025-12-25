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
package org.apache.gravitino.catalog.hive.integration.test;

import static org.apache.gravitino.catalog.hive.HiveConstants.COMMENT;
import static org.apache.gravitino.catalog.hive.HiveConstants.EXTERNAL;
import static org.apache.gravitino.catalog.hive.HiveConstants.FORMAT;
import static org.apache.gravitino.catalog.hive.HiveConstants.INPUT_FORMAT;
import static org.apache.gravitino.catalog.hive.HiveConstants.LOCATION;
import static org.apache.gravitino.catalog.hive.HiveConstants.METASTORE_URIS;
import static org.apache.gravitino.catalog.hive.HiveConstants.OUTPUT_FORMAT;
import static org.apache.gravitino.catalog.hive.HiveConstants.SERDE_LIB;
import static org.apache.gravitino.catalog.hive.HiveConstants.SERDE_NAME;
import static org.apache.gravitino.catalog.hive.HiveConstants.TABLE_TYPE;
import static org.apache.gravitino.catalog.hive.HiveConstants.TRANSIENT_LAST_DDL_TIME;
import static org.apache.gravitino.catalog.hive.TableType.EXTERNAL_TABLE;
import static org.apache.gravitino.catalog.hive.TableType.MANAGED_TABLE;
import static org.apache.hadoop.hive.serde.serdeConstants.DATE_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.INT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.TINYINT_TYPE_NAME;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.MetalakeChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.catalog.hive.HiveCatalogOperations;
import org.apache.gravitino.catalog.hive.HiveStorageConstants;
import org.apache.gravitino.catalog.hive.HiveTablePropertiesMetadata;
import org.apache.gravitino.catalog.hive.TableType;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.hive.HiveClientPool;
import org.apache.gravitino.hive.HivePartition;
import org.apache.gravitino.hive.HiveSchema;
import org.apache.gravitino.hive.HiveTable;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.sorts.NullOrdering;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.partitions.IdentityPartition;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.types.Types;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.spark.sql.SparkSession;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CatalogHive2IT extends BaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogHive2IT.class);

  public static final String metalakeName =
      GravitinoITUtils.genRandomName("CatalogHiveIT_metalake");
  public String catalogName = GravitinoITUtils.genRandomName("CatalogHiveIT_catalog");
  public String SCHEMA_PREFIX = "CatalogHiveIT_schema";
  public String schemaName = GravitinoITUtils.genRandomName(SCHEMA_PREFIX);
  public String TABLE_PREFIX = "CatalogHiveIT_table";
  public String tableName = GravitinoITUtils.genRandomName(TABLE_PREFIX);
  public static final String ALTER_TABLE_NAME = "alert_table_name";
  public static final String TABLE_COMMENT = "table_comment";
  public static final String HIVE_COL_NAME1 = "hive_col_name1";
  public static final String HIVE_COL_NAME2 = "hive_col_name2";
  public static final String HIVE_COL_NAME3 = "hive_col_name3";
  protected String hiveMetastoreUris;
  protected String hmsCatalog = "";
  protected final String provider = "hive";
  protected final ContainerSuite containerSuite = ContainerSuite.getInstance();
  protected HiveClientPool hiveClientPool;
  protected GravitinoMetalake metalake;
  protected Catalog catalog;
  protected SparkSession sparkSession;
  protected FileSystem fileSystem;
  protected boolean enableSparkTest = true;
  private final String SELECT_ALL_TEMPLATE = "SELECT * FROM %s.%s";

  private static String getInsertWithoutPartitionSql(
      String dbName, String tableName, String values) {
    return String.format("INSERT INTO %s.%s VALUES (%s)", dbName, tableName, values);
  }

  private static String getInsertWithPartitionSql(
      String dbName, String tableName, String partitionExpressions, String values) {
    return String.format(
        "INSERT INTO %s.%s PARTITION (%s) VALUES (%s)",
        dbName, tableName, partitionExpressions, values);
  }

  private static final Map<String, String> typeConstant =
      ImmutableMap.of(
          TINYINT_TYPE_NAME,
          "1",
          INT_TYPE_NAME,
          "2",
          DATE_TYPE_NAME,
          "'2023-01-01'",
          STRING_TYPE_NAME,
          "'gravitino_it_test'");

  protected void startNecessaryContainer() {
    containerSuite.startHiveContainer(
        ImmutableMap.of(HiveContainer.HIVE_RUNTIME_VERSION, HiveContainer.HIVE2));

    hiveMetastoreUris =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);
  }

  protected void initSparkSession() {
    SparkSession.Builder builder =
        SparkSession.builder()
            .master("local[1]")
            .appName("Hive Catalog integration test")
            .config("hive.metastore.uris", hiveMetastoreUris)
            .config(
                "spark.sql.warehouse.dir",
                String.format(
                    "hdfs://%s:%d/user/hive/warehouse",
                    containerSuite.getHiveContainer().getContainerIpAddress(),
                    HiveContainer.HDFS_DEFAULTFS_PORT))
            .config("spark.sql.storeAssignmentPolicy", "LEGACY")
            .config("mapreduce.input.fileinputformat.input.dir.recursive", "true")
            .enableHiveSupport();
    sparkSession = builder.getOrCreate();
  }

  protected void initFileSystem() throws IOException {
    Configuration conf = new Configuration();
    conf.set(
        "fs.defaultFS",
        String.format(
            "hdfs://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT));
    fileSystem = FileSystem.get(conf);
  }

  @BeforeAll
  public void startup() throws Exception {
    startNecessaryContainer();

    HiveConf hiveConf = new HiveConf();
    hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, hiveMetastoreUris);

    Properties hiveClientProperties = new Properties();
    hiveClientProperties.setProperty(HiveConf.ConfVars.METASTOREURIS.varname, hiveMetastoreUris);

    // Check if Hive client can connect to Hive metastore
    hiveClientPool = new HiveClientPool("hive", 1, hiveClientProperties);

    if (enableSparkTest) {
      initSparkSession();
    }
    initFileSystem();

    createMetalake();
    createCatalog();
    createSchema();
  }

  @AfterAll
  public void stop() throws IOException {
    if (client != null) {
      Arrays.stream(catalog.asSchemas().listSchemas())
          .filter(schema -> !schema.equals("default"))
          .forEach(
              (schema -> {
                catalog.asSchemas().dropSchema(schema, true);
              }));
      Arrays.stream(metalake.listCatalogs())
          .forEach(
              catalogName -> {
                metalake.dropCatalog(catalogName, true);
              });
      client.dropMetalake(metalakeName, true);
    }
    if (hiveClientPool != null) {
      hiveClientPool.close();
    }

    if (sparkSession != null) {
      sparkSession.close();
    }

    ContainerSuite.getInstance().close();

    if (fileSystem != null) {
      fileSystem.close();
    }
    try {
      closer.close();
    } catch (Exception e) {
      LOG.error("Failed to close CloseableGroup", e);
    }
    client = null;
  }

  @AfterEach
  public void resetSchema() throws TException, InterruptedException {
    catalog.asSchemas().dropSchema(schemaName, true);
    assertThrows(
        NoSuchSchemaException.class,
        () -> hiveClientPool.run(client -> client.getDatabase(hmsCatalog, schemaName)));
    createSchema();
  }

  private void createMetalake() {
    GravitinoMetalake[] gravitinoMetalakes = client.listMetalakes();
    Assertions.assertEquals(0, gravitinoMetalakes.length);

    client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    GravitinoMetalake loadMetalake = client.loadMetalake(metalakeName);
    Assertions.assertEquals(metalakeName, loadMetalake.name());

    metalake = loadMetalake;
  }

  protected void createCatalog() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(METASTORE_URIS, hiveMetastoreUris);

    metalake.createCatalog(catalogName, Catalog.Type.RELATIONAL, provider, "comment", properties);

    catalog = metalake.loadCatalog(catalogName);
  }

  private void createSchema() throws TException, InterruptedException {
    Map<String, String> schemaProperties = createSchemaProperties();
    String comment = "comment";
    catalog.asSchemas().createSchema(schemaName, comment, schemaProperties);
    Schema loadSchema = catalog.asSchemas().loadSchema(schemaName);
    Assertions.assertEquals(schemaName.toLowerCase(), loadSchema.name());
    Assertions.assertEquals(comment, loadSchema.comment());
    Assertions.assertEquals("val1", loadSchema.properties().get("key1"));
    Assertions.assertEquals("val2", loadSchema.properties().get("key2"));
    Assertions.assertNotNull(loadSchema.properties().get(LOCATION));

    // Directly get database from Hive metastore to verify the schema creation
    HiveSchema hiveSchema = loadHiveSchema(schemaName);
    Assertions.assertEquals(schemaName.toLowerCase(), hiveSchema.name());
    Assertions.assertEquals(comment, hiveSchema.comment());
    Assertions.assertEquals("val1", hiveSchema.properties().get("key1"));
    Assertions.assertEquals("val2", hiveSchema.properties().get("key2"));
  }

  private Column[] createColumns() {
    Column col1 = Column.of(HIVE_COL_NAME1, Types.ByteType.get(), "col_1_comment");
    Column col2 = Column.of(HIVE_COL_NAME2, Types.DateType.get(), "col_2_comment");
    Column col3 = Column.of(HIVE_COL_NAME3, Types.StringType.get(), "col_3_comment");
    return new Column[] {col1, col2, col3};
  }

  private void checkTableReadWrite(HiveTable table) {
    String dbName = table.databaseName();
    String tableName = table.name();
    long count = sparkSession.sql(String.format(SELECT_ALL_TEMPLATE, dbName, tableName)).count();

    List<String> partitionFields = table.partitionFieldNames();
    Map<String, Column> columnByName =
        Arrays.stream(table.columns()).collect(Collectors.toMap(Column::name, c -> c));
    List<Column> dataColumns =
        Arrays.stream(table.columns())
            .filter(c -> !partitionFields.contains(c.name()))
            .collect(Collectors.toList());

    String values =
        dataColumns.stream().map(this::sampleValueForColumn).collect(Collectors.joining(","));
    if (partitionFields.isEmpty()) {
      sparkSession.sql(getInsertWithoutPartitionSql(dbName, tableName, values));
    } else {
      String partitionExpressions =
          partitionFields.stream()
              .map(
                  field -> {
                    Column column = columnByName.get(field);
                    Assertions.assertNotNull(
                        column, "Partition column " + field + " is missing from definition");
                    return field + "=" + sampleValueForColumn(column);
                  })
              .collect(Collectors.joining(","));
      sparkSession.sql(getInsertWithPartitionSql(dbName, tableName, partitionExpressions, values));
    }
    Assertions.assertEquals(
        count + 1, sparkSession.sql(String.format(SELECT_ALL_TEMPLATE, dbName, tableName)).count());

    String tableLocation = table.properties().get(LOCATION);
    Assertions.assertNotNull(tableLocation, "Table location should not be null");
    Path tableDirectory = new Path(tableLocation);
    FileStatus[] fileStatuses;
    try {
      fileStatuses = fileSystem.listStatus(tableDirectory);
    } catch (IOException e) {
      LOG.warn("Failed to list status of table directory", e);
      throw new RuntimeException(e);
    }
    Assertions.assertTrue(fileStatuses.length > 0);
    for (FileStatus fileStatus : fileStatuses) {
      Assertions.assertEquals("anonymous", fileStatus.getOwner());
    }
  }

  protected Map<String, String> createProperties() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    return properties;
  }

  private String sampleValueForColumn(Column column) {
    if (column.dataType().equals(Types.ByteType.get())) {
      return typeConstant.get(TINYINT_TYPE_NAME);
    } else if (column.dataType().equals(Types.IntegerType.get())) {
      return typeConstant.get(INT_TYPE_NAME);
    } else if (column.dataType().equals(Types.DateType.get())) {
      return typeConstant.get(DATE_TYPE_NAME);
    } else if (column.dataType().equals(Types.StringType.get())) {
      return typeConstant.get(STRING_TYPE_NAME);
    }
    throw new IllegalArgumentException(
        "Unsupported column type for sample value: " + column.dataType());
  }

  private HiveTable loadHiveTable(String schema, String table) throws InterruptedException {
    return hiveClientPool.run(client -> client.getTable(hmsCatalog, schema, table));
  }

  private HivePartition loadHivePartition(String schema, String table, String partition)
      throws InterruptedException {
    return hiveClientPool.run(
        client -> {
          HiveTable hiveTable = client.getTable(hmsCatalog, schema, table);
          return client.getPartition(hiveTable, partition);
        });
  }

  private HiveSchema loadHiveSchema(String schema) throws InterruptedException {
    return hiveClientPool.run(client -> client.getDatabase(hmsCatalog, schema));
  }

  private boolean hiveTableExists(String schema, String table) throws InterruptedException {
    return hiveClientPool.run(
        client -> {
          try {
            client.getTable(hmsCatalog, schema, table);
            return true;
          } catch (NoSuchTableException e) {
            return false;
          }
        });
  }

  private void dropHiveTable(String schema, String table, boolean deleteData, boolean ifPurge)
      throws InterruptedException {
    hiveClientPool.run(
        client -> {
          client.dropTable(hmsCatalog, schema, table, deleteData, ifPurge);
          return null;
        });
  }

  private List<String> partitionValues(HivePartition partition) {
    return Arrays.stream(partition.values())
        .map(literal -> literal.value().toString())
        .collect(Collectors.toList());
  }

  private void compareDistributions(Distribution expected, Distribution actual) {
    boolean expectedEmpty = expected == null || Distributions.NONE.equals(expected);
    boolean actualEmpty = actual == null || Distributions.NONE.equals(actual);
    Assertions.assertEquals(expectedEmpty, actualEmpty);
    if (expectedEmpty) {
      return;
    }

    Assertions.assertEquals(expected.number(), actual.number());
    List<String> expectedFields =
        Arrays.stream(expected.expressions())
            .map(expr -> ((NamedReference.FieldReference) expr).fieldName()[0])
            .collect(Collectors.toList());
    List<String> actualFields =
        Arrays.stream(actual.expressions())
            .map(expr -> ((NamedReference.FieldReference) expr).fieldName()[0])
            .collect(Collectors.toList());
    Assertions.assertEquals(expectedFields, actualFields);
  }

  private void compareSortOrders(SortOrder[] expected, SortOrder[] actual) {
    int expectedLength = expected == null ? 0 : expected.length;
    int actualLength = actual == null ? 0 : actual.length;
    Assertions.assertEquals(expectedLength, actualLength);
    for (int i = 0; i < expectedLength; i++) {
      SortOrder expectedOrder = expected[i];
      SortOrder actualOrder = actual[i];
      Assertions.assertEquals(expectedOrder.direction(), actualOrder.direction());
      Assertions.assertEquals(expectedOrder.nullOrdering(), actualOrder.nullOrdering());
      Assertions.assertEquals(
          ((NamedReference.FieldReference) expectedOrder.expression()).fieldName()[0],
          ((NamedReference.FieldReference) actualOrder.expression()).fieldName()[0]);
    }
  }

  private void comparePartitioning(Transform[] expected, List<String> actualPartitionFields) {
    if (expected == null || expected.length == 0) {
      Assertions.assertTrue(actualPartitionFields == null || actualPartitionFields.isEmpty());
      return;
    }

    List<String> expectedPartitionFields =
        Arrays.stream(expected)
            .map(p -> ((Transform.SingleFieldTransform) p).fieldName()[0])
            .collect(Collectors.toList());
    Assertions.assertEquals(expectedPartitionFields, actualPartitionFields);
  }

  @Test
  public void testCreateHiveTableWithDistributionAndSortOrder()
      throws TException, InterruptedException {
    Assumptions.assumeTrue(enableSparkTest);
    Column[] columns = createColumns();

    NameIdentifier nameIdentifier = NameIdentifier.of(schemaName, tableName);

    Distribution distribution =
        Distributions.of(Strategy.EVEN, 10, NamedReference.field(HIVE_COL_NAME1));

    final SortOrder[] sortOrders =
        new SortOrder[] {
          SortOrders.of(NamedReference.field(HIVE_COL_NAME2), SortDirection.DESCENDING)
        };

    Map<String, String> properties = createProperties();
    Table createdTable =
        catalog
            .asTableCatalog()
            .createTable(
                nameIdentifier,
                columns,
                TABLE_COMMENT,
                properties,
                Transforms.EMPTY_TRANSFORM,
                distribution,
                sortOrders);

    // Directly get table from Hive metastore to check if the table is created successfully.
    HiveTable hiveTab = loadHiveTable(schemaName, tableName);
    properties
        .keySet()
        .forEach(
            key -> Assertions.assertEquals(properties.get(key), hiveTab.properties().get(key)));
    assertTableEquals(createdTable, hiveTab);
    checkTableReadWrite(hiveTab);

    // test null partition
    resetSchema();
    Table createdTable1 =
        catalog
            .asTableCatalog()
            .createTable(nameIdentifier, columns, TABLE_COMMENT, properties, (Transform[]) null);

    // Directly get table from Hive metastore to check if the table is created successfully.
    HiveTable hiveTable1 = loadHiveTable(schemaName, tableName);
    properties
        .keySet()
        .forEach(
            key -> Assertions.assertEquals(properties.get(key), hiveTable1.properties().get(key)));
    assertTableEquals(createdTable1, hiveTable1);
    checkTableReadWrite(hiveTable1);

    // Test bad request
    // Bad name in distribution
    final Distribution badDistribution =
        Distributions.of(Strategy.EVEN, 10, NamedReference.field(HIVE_COL_NAME1 + "bad_name"));
    TableCatalog tableCatalog = catalog.asTableCatalog();
    Assertions.assertThrows(
        Exception.class,
        () -> {
          tableCatalog.createTable(
              nameIdentifier,
              columns,
              TABLE_COMMENT,
              properties,
              Transforms.EMPTY_TRANSFORM,
              badDistribution,
              sortOrders);
        });

    final SortOrder[] badSortOrders =
        new SortOrder[] {
          SortOrders.of(
              NamedReference.field(HIVE_COL_NAME2 + "bad_name"),
              SortDirection.DESCENDING,
              NullOrdering.NULLS_FIRST)
        };

    Assertions.assertThrows(
        Exception.class,
        () -> {
          tableCatalog.createTable(
              nameIdentifier,
              columns,
              TABLE_COMMENT,
              properties,
              Transforms.EMPTY_TRANSFORM,
              distribution,
              badSortOrders);
        });
  }

  @Test
  public void testCreateHiveTable() throws TException, InterruptedException {
    Assumptions.assumeTrue(enableSparkTest);
    Column[] columns = createColumns();

    NameIdentifier nameIdentifier = NameIdentifier.of(schemaName, tableName);
    Map<String, String> properties = createProperties();
    Table createdTable =
        catalog
            .asTableCatalog()
            .createTable(
                nameIdentifier, columns, TABLE_COMMENT, properties, Transforms.EMPTY_TRANSFORM);

    // Directly get table from Hive metastore to check if the table is created successfully.
    HiveTable hiveTab = loadHiveTable(schemaName, tableName);
    properties
        .keySet()
        .forEach(
            key -> Assertions.assertEquals(properties.get(key), hiveTab.properties().get(key)));
    assertTableEquals(createdTable, hiveTab);
    checkTableReadWrite(hiveTab);

    // test null comment
    resetSchema();
    createdTable =
        catalog
            .asTableCatalog()
            .createTable(nameIdentifier, columns, null, properties, Transforms.EMPTY_TRANSFORM);
    HiveTable hiveTab2 = loadHiveTable(schemaName, tableName);
    assertTableEquals(createdTable, hiveTab2);
    checkTableReadWrite(hiveTab2);

    // test null partition
    resetSchema();
    Table createdTable1 =
        catalog
            .asTableCatalog()
            .createTable(nameIdentifier, columns, TABLE_COMMENT, properties, (Transform[]) null);

    // Directly get table from Hive metastore to check if the table is created successfully.
    HiveTable hiveTable1 = loadHiveTable(schemaName, tableName);
    properties
        .keySet()
        .forEach(
            key -> Assertions.assertEquals(properties.get(key), hiveTable1.properties().get(key)));
    assertTableEquals(createdTable1, hiveTable1);
    checkTableReadWrite(hiveTable1);

    // test column not null
    Column illegalColumn =
        Column.of("not_null_column", Types.StringType.get(), "not null column", false, false, null);
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                catalog
                    .asTableCatalog()
                    .createTable(
                        nameIdentifier,
                        new Column[] {illegalColumn},
                        TABLE_COMMENT,
                        properties,
                        Transforms.EMPTY_TRANSFORM));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains(
                "The NOT NULL constraint for column is only supported since Hive 3.0, "
                    + "but the current Gravitino Hive catalog only supports Hive 2.x"));

    // test column default value
    Column withDefault =
        Column.of(
            "default_column", Types.StringType.get(), "default column", true, false, Literals.NULL);
    exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                catalog
                    .asTableCatalog()
                    .createTable(
                        nameIdentifier,
                        new Column[] {withDefault},
                        TABLE_COMMENT,
                        properties,
                        Transforms.EMPTY_TRANSFORM));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains(
                "The DEFAULT constraint for column is only supported since Hive 3.0, "
                    + "but the current Gravitino Hive catalog only supports Hive 2.x"),
        "The exception message is: " + exception.getMessage());
  }

  @Test
  public void testHiveTableProperties() throws TException, InterruptedException {
    Assumptions.assumeTrue(enableSparkTest);
    Column[] columns = createColumns();
    NameIdentifier nameIdentifier = NameIdentifier.of(schemaName, tableName);
    // test default properties
    catalog
        .asTableCatalog()
        .createTable(
            nameIdentifier, columns, TABLE_COMMENT, createProperties(), Transforms.EMPTY_TRANSFORM);
    Table loadedTable1 = catalog.asTableCatalog().loadTable(nameIdentifier);
    HiveTablePropertiesMetadata tablePropertiesMetadata = new HiveTablePropertiesMetadata();
    HiveTable actualTable = loadHiveTable(schemaName, tableName);
    assertDefaultTableProperties(loadedTable1, actualTable);
    checkTableReadWrite(actualTable);

    // test set properties
    Map<String, String> properties = createProperties();
    properties.put(FORMAT, "textfile");
    properties.put(SERDE_LIB, HiveStorageConstants.OPENCSV_SERDE_CLASS);
    properties.put(TABLE_TYPE, "external_table");
    String table2 = GravitinoITUtils.genRandomName(TABLE_PREFIX);
    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(schemaName, table2),
            columns,
            TABLE_COMMENT,
            properties,
            Transforms.EMPTY_TRANSFORM);
    Table loadedTable2 = catalog.asTableCatalog().loadTable(NameIdentifier.of(schemaName, table2));
    HiveTable actualTable2 = loadHiveTable(schemaName, table2);

    Assertions.assertEquals(
        HiveStorageConstants.OPENCSV_SERDE_CLASS, actualTable2.properties().get(SERDE_LIB));
    Assertions.assertEquals(
        HiveStorageConstants.TEXT_INPUT_FORMAT_CLASS, actualTable2.properties().get(INPUT_FORMAT));
    Assertions.assertEquals(
        HiveStorageConstants.IGNORE_KEY_OUTPUT_FORMAT_CLASS,
        actualTable2.properties().get(OUTPUT_FORMAT));
    Assertions.assertEquals(
        EXTERNAL_TABLE.name(), actualTable2.properties().get(TABLE_TYPE).toUpperCase());
    Assertions.assertEquals(table2.toLowerCase(), actualTable2.properties().get(SERDE_NAME));
    Assertions.assertEquals(TABLE_COMMENT, actualTable2.comment());
    Assertions.assertEquals(
        ((Boolean) tablePropertiesMetadata.getDefaultValue(EXTERNAL)).toString().toUpperCase(),
        actualTable.properties().get(EXTERNAL));
    Assertions.assertNotNull(loadedTable2.properties().get(TRANSIENT_LAST_DDL_TIME));

    // S3 doesn't support NUM_FILES and TOTAL_SIZE
    checkTableReadWrite(actualTable2);

    // test alter properties exception
    TableCatalog tableCatalog = catalog.asTableCatalog();
    NameIdentifier id = NameIdentifier.of(schemaName, tableName);
    TableChange change = TableChange.setProperty(TRANSIENT_LAST_DDL_TIME, "1234");
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              tableCatalog.alterTable(id, change);
            });
    Assertions.assertTrue(exception.getMessage().contains("cannot be set"));
  }

  @Test
  public void testListTables() {
    // mock iceberg, paimon, and hudi tables
    NameIdentifier icebergTable = NameIdentifier.of(schemaName, "iceberg_table");
    NameIdentifier paimonTable = NameIdentifier.of(schemaName, "paimon_table");
    NameIdentifier hudiTable = NameIdentifier.of(schemaName, "hudi_table");
    NameIdentifier hiveTable = NameIdentifier.of(schemaName, "hive_table");
    catalog
        .asTableCatalog()
        .createTable(icebergTable, createColumns(), null, ImmutableMap.of("table_type", "ICEBERG"));
    catalog
        .asTableCatalog()
        .createTable(paimonTable, createColumns(), null, ImmutableMap.of("table_type", "PAIMON"));
    catalog
        .asTableCatalog()
        .createTable(hudiTable, createColumns(), null, ImmutableMap.of("provider", "hudi"));
    catalog
        .asTableCatalog()
        .createTable(hiveTable, createColumns(), null, ImmutableMap.of("provider", "hive"));
    NameIdentifier[] tables = catalog.asTableCatalog().listTables(Namespace.of(schemaName));
    Assertions.assertEquals(1, tables.length);
  }

  @Test
  public void testHiveSchemaProperties() throws TException, InterruptedException {
    Assumptions.assumeTrue(enableSparkTest);
    // test LOCATION property
    String schemaName = GravitinoITUtils.genRandomName(SCHEMA_PREFIX);

    Map<String, String> schemaProperties = createSchemaProperties();
    String expectedHDFSSchemaLocation = schemaProperties.get(LOCATION);

    catalog.asSchemas().createSchema(schemaName, "comment", schemaProperties);

    HiveSchema actualSchema = loadHiveSchema(schemaName);
    String actualSchemaLocation = actualSchema.properties().get(LOCATION);
    Assertions.assertTrue(actualSchemaLocation.endsWith(expectedHDFSSchemaLocation));

    NameIdentifier tableIdent =
        NameIdentifier.of(schemaName, GravitinoITUtils.genRandomName(TABLE_PREFIX));

    Map<String, String> tableProperties = createProperties();
    String expectedSchemaLocation =
        tableProperties.getOrDefault(LOCATION, expectedHDFSSchemaLocation);

    catalog
        .asTableCatalog()
        .createTable(
            tableIdent,
            createColumns(),
            TABLE_COMMENT,
            tableProperties,
            Transforms.EMPTY_TRANSFORM);
    HiveTable actualTable = loadHiveTable(schemaName, tableIdent.name());
    String actualTableLocation = actualTable.properties().get(LOCATION);
    // use `tableIdent.name().toLowerCase()` because HMS will convert table name to lower

    // actualTableLocation is null for S3
    if (!tableProperties.containsKey(LOCATION) && actualTableLocation != null) {
      String expectedTableLocation = expectedSchemaLocation + "/" + tableIdent.name().toLowerCase();
      Assertions.assertTrue(actualTableLocation.endsWith(expectedTableLocation));
    }
    checkTableReadWrite(actualTable);
  }

  @Test
  public void testCreatePartitionedHiveTable() throws TException, InterruptedException {
    Assumptions.assumeTrue(enableSparkTest);
    // Create table from Gravitino API
    Column[] columns = createColumns();

    NameIdentifier nameIdentifier = NameIdentifier.of(schemaName, tableName);
    Map<String, String> properties = createProperties();
    Table createdTable =
        catalog
            .asTableCatalog()
            .createTable(
                nameIdentifier,
                columns,
                TABLE_COMMENT,
                properties,
                new Transform[] {
                  Transforms.identity(columns[1].name()), Transforms.identity(columns[2].name())
                });

    // Directly get table from Hive metastore to check if the table is created successfully.
    HiveTable hiveTab = loadHiveTable(schemaName, tableName);
    properties
        .keySet()
        .forEach(
            key -> Assertions.assertEquals(properties.get(key), hiveTab.properties().get(key)));
    assertTableEquals(createdTable, hiveTab);
    checkTableReadWrite(hiveTab);

    // test exception
    TableCatalog tableCatalog = catalog.asTableCatalog();
    Transform[] transforms =
        new Transform[] {
          Transforms.identity(columns[0].name()), Transforms.identity(columns[1].name())
        };
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> {
              tableCatalog.createTable(
                  nameIdentifier, columns, TABLE_COMMENT, properties, transforms);
            });
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("The partition field must be placed at the end of the columns in order"));
  }

  @Test
  public void testListPartitionNames() throws TException, InterruptedException {
    Assumptions.assumeTrue(enableSparkTest);
    // test empty partitions
    Column[] columns = createColumns();
    NameIdentifier nameIdentifier = NameIdentifier.of(schemaName, tableName);
    Table nonPartitionedTable =
        catalog
            .asTableCatalog()
            .createTable(
                nameIdentifier,
                columns,
                TABLE_COMMENT,
                ImmutableMap.of(),
                Transforms.EMPTY_TRANSFORM);
    String[] result = nonPartitionedTable.supportPartitions().listPartitionNames();
    Assertions.assertEquals(0, result.length);

    // test partitioned table
    Table createdTable = preparePartitionedTable();

    String[] partitionNames = createdTable.supportPartitions().listPartitionNames();
    Assertions.assertArrayEquals(
        new String[] {"hive_col_name2=2023-01-01/hive_col_name3=gravitino_it_test"},
        partitionNames);
  }

  @Test
  public void testListPartitions() throws TException, InterruptedException {
    Assumptions.assumeTrue(enableSparkTest);
    // test empty partitions
    Column[] columns = createColumns();
    NameIdentifier nameIdentifier = NameIdentifier.of(schemaName, tableName);
    Table nonPartitionedTable =
        catalog
            .asTableCatalog()
            .createTable(
                nameIdentifier,
                columns,
                TABLE_COMMENT,
                ImmutableMap.of(),
                Transforms.EMPTY_TRANSFORM);
    Partition[] result = nonPartitionedTable.supportPartitions().listPartitions();
    Assertions.assertEquals(0, result.length);

    // test partitioned table
    Table createdTable = preparePartitionedTable();
    String insertTemplate =
        "INSERT INTO TABLE %s.%s "
            + "PARTITION (hive_col_name2='2023-01-02', hive_col_name3='gravitino_it_test2') "
            + "VALUES %s, %s";
    sparkSession.sql(String.format(insertTemplate, schemaName, createdTable.name(), "(1)", "(2)"));

    // update partition stats
    String partition1 = "hive_col_name2='2023-01-01', hive_col_name3='gravitino_it_test'";
    String partition2 = "hive_col_name2='2023-01-02', hive_col_name3='gravitino_it_test2'";
    sparkSession.sql(
        String.format(
            "ANALYZE TABLE %s.%s PARTITION (%s) COMPUTE STATISTICS",
            schemaName, createdTable.name(), partition1));
    sparkSession.sql(
        String.format(
            "ANALYZE TABLE %s.%s PARTITION (%s) COMPUTE STATISTICS",
            schemaName, createdTable.name(), partition2));

    Partition[] partitions = createdTable.supportPartitions().listPartitions();
    Assertions.assertEquals(2, partitions.length);
    String partition1Name = "hive_col_name2=2023-01-01/hive_col_name3=gravitino_it_test";
    String partition2Name = "hive_col_name2=2023-01-02/hive_col_name3=gravitino_it_test2";
    Set<String> partitionNames =
        Arrays.stream(partitions).map(Partition::name).collect(Collectors.toSet());
    Assertions.assertTrue(partitionNames.contains(partition1Name));
    Assertions.assertTrue(partitionNames.contains(partition2Name));
    for (Partition partition : partitions) {
      if (partition.name().equals(partition1Name)) {
        Assertions.assertEquals("1", partition.properties().get("spark.sql.statistics.numRows"));
      } else if (partition.name().equals(partition2Name)) {
        Assertions.assertEquals("2", partition.properties().get("spark.sql.statistics.numRows"));
      }
    }
  }

  @Test
  public void testGetPartition() throws TException, InterruptedException {
    Assumptions.assumeTrue(enableSparkTest);
    Table createdTable = preparePartitionedTable();

    String[] partitionNames = createdTable.supportPartitions().listPartitionNames();
    Assertions.assertEquals(1, partitionNames.length);
    IdentityPartition partition =
        (IdentityPartition) createdTable.supportPartitions().getPartition(partitionNames[0]);

    Assertions.assertEquals(
        "hive_col_name2=2023-01-01/hive_col_name3=gravitino_it_test", partition.name());

    // Directly get partition from Hive metastore
    HivePartition hivePartition =
        loadHivePartition(schemaName, createdTable.name(), partition.name());
    List<String> hiveValues = partitionValues(hivePartition);
    Assertions.assertEquals(partition.values()[0].value().toString(), hiveValues.get(0));
    Assertions.assertEquals(partition.values()[1].value().toString(), hiveValues.get(1));
    Assertions.assertNotNull(partition.properties());
    Assertions.assertEquals(partition.properties(), hivePartition.properties());
  }

  @Test
  public void testAddPartition() throws TException, InterruptedException {
    Assumptions.assumeTrue(enableSparkTest);
    Table createdTable = preparePartitionedTable();

    // add partition "hive_col_name2=2023-01-02/hive_col_name3=gravitino_it_test2"
    String[] field1 = new String[] {"hive_col_name2"};
    String[] field2 = new String[] {"hive_col_name3"};
    Literal<?> literal1 = Literals.dateLiteral(LocalDate.parse("2023-01-02"));
    Literal<?> literal2 = Literals.stringLiteral("gravitino_it_test2");

    Partition identity =
        Partitions.identity(new String[][] {field1, field2}, new Literal<?>[] {literal1, literal2});
    IdentityPartition partitionAdded =
        (IdentityPartition) createdTable.supportPartitions().addPartition(identity);

    // Directly get partition from hive metastore to check if the partition is created successfully.
    HivePartition partitionGot =
        loadHivePartition(schemaName, createdTable.name(), partitionAdded.name());
    List<String> partitionValues = partitionValues(partitionGot);
    Assertions.assertEquals(partitionAdded.values()[0].value().toString(), partitionValues.get(0));
    Assertions.assertEquals(partitionAdded.values()[1].value().toString(), partitionValues.get(1));
    Assertions.assertEquals(partitionAdded.properties(), partitionGot.properties());

    // test the new partition can be read and write successfully by dynamic partition
    String selectTemplate =
        "SELECT * FROM %s.%s WHERE hive_col_name2 = '2023-01-02' AND hive_col_name3 = 'gravitino_it_test2'";
    String sql = String.format(selectTemplate, schemaName, createdTable.name());
    long count = sparkSession.sql(sql).count();
    Assertions.assertEquals(0, count);

    String insertTemplate =
        "INSERT INTO TABLE %s.%s PARTITION (hive_col_name2='2023-01-02', hive_col_name3) VALUES (%s, %s)";
    sparkSession.sql(
        String.format(
            insertTemplate, schemaName, createdTable.name(), "1", "'gravitino_it_test2'"));
    count =
        sparkSession.sql(String.format(selectTemplate, schemaName, createdTable.name())).count();
    Assertions.assertEquals(1, count);

    // test the new partition can be read and write successfully by static partition
    String insertTemplate2 =
        "INSERT INTO TABLE %s.%s PARTITION (hive_col_name2='2023-01-02', hive_col_name3='gravitino_it_test2') VALUES (%s)";
    sparkSession.sql(String.format(insertTemplate2, schemaName, createdTable.name(), "2"));
    count =
        sparkSession.sql(String.format(selectTemplate, schemaName, createdTable.name())).count();
    Assertions.assertEquals(2, count);
  }

  @Test
  public void testDropPartition() throws TException, InterruptedException, IOException {
    Assumptions.assumeTrue(enableSparkTest);
    Table createdTable = preparePartitionedTable();

    // add partition "hive_col_name2=2023-01-02/hive_col_name3=gravitino_it_test2"
    String[] field1 = new String[] {"hive_col_name2"};
    String[] field2 = new String[] {"hive_col_name3"};
    Literal<?> literal1 = Literals.dateLiteral(LocalDate.parse("2023-01-02"));
    Literal<?> literal2 = Literals.stringLiteral("gravitino_it_test2");
    Partition identity =
        Partitions.identity(new String[][] {field1, field2}, new Literal<?>[] {literal1, literal2});
    IdentityPartition partitionAdded =
        (IdentityPartition) createdTable.supportPartitions().addPartition(identity);
    // Directly get partition from hive metastore to check if the partition is created successfully.
    HivePartition partitionGot =
        loadHivePartition(schemaName, createdTable.name(), partitionAdded.name());
    List<String> partitionValues = partitionValues(partitionGot);
    Assertions.assertEquals(partitionAdded.values()[0].value().toString(), partitionValues.get(0));
    Assertions.assertEquals(partitionAdded.values()[1].value().toString(), partitionValues.get(1));
    Assertions.assertEquals(partitionAdded.properties(), partitionGot.properties());

    // test drop partition "hive_col_name2=2023-01-02/hive_col_name3=gravitino_it_test2"
    boolean dropRes1 = createdTable.supportPartitions().dropPartition(partitionAdded.name());
    Assertions.assertTrue(dropRes1);
    Assertions.assertThrows(
        NoSuchPartitionException.class,
        () -> loadHivePartition(schemaName, createdTable.name(), partitionAdded.name()));
    HiveTable hiveTable = loadHiveTable(schemaName, createdTable.name());
    String tableLocation = hiveTable.properties().get(LOCATION);
    Assertions.assertNotNull(tableLocation, "Table location should not be null");
    Path partitionDirectory = new Path(tableLocation + identity.name());
    Assertions.assertFalse(
        fileSystem.exists(partitionDirectory), "The partition directory should not exist");

    // add partition "hive_col_name2=2024-01-02/hive_col_name3=gravitino_it_test2"
    String[] field3 = new String[] {"hive_col_name2"};
    String[] field4 = new String[] {"hive_col_name3"};
    Literal<?> literal3 = Literals.dateLiteral(LocalDate.parse("2024-01-02"));
    Literal<?> literal4 = Literals.stringLiteral("gravitino_it_test2");
    Partition identity1 =
        Partitions.identity(new String[][] {field3, field4}, new Literal<?>[] {literal3, literal4});
    IdentityPartition partitionAdded1 =
        (IdentityPartition) createdTable.supportPartitions().addPartition(identity1);

    // Directly get partition from Hive metastore to check if the partition is created successfully.
    HivePartition partitionGot1 =
        loadHivePartition(schemaName, createdTable.name(), partitionAdded1.name());
    List<String> partitionValues1 = partitionValues(partitionGot1);
    Assertions.assertEquals(
        partitionAdded1.values()[0].value().toString(), partitionValues1.get(0));
    Assertions.assertEquals(
        partitionAdded1.values()[1].value().toString(), partitionValues1.get(1));
    Assertions.assertEquals(partitionAdded1.properties(), partitionGot1.properties());

    // add partition "hive_col_name2=2024-01-02/hive_col_name3=gravitino_it_test3"
    String[] field5 = new String[] {"hive_col_name2"};
    String[] field6 = new String[] {"hive_col_name3"};
    Literal<?> literal5 = Literals.dateLiteral(LocalDate.parse("2024-01-02"));
    Literal<?> literal6 = Literals.stringLiteral("gravitino_it_test3");
    Partition identity2 =
        Partitions.identity(new String[][] {field5, field6}, new Literal<?>[] {literal5, literal6});
    IdentityPartition partitionAdded2 =
        (IdentityPartition) createdTable.supportPartitions().addPartition(identity2);
    // Directly get partition from Hive metastore to check if the partition is created successfully.
    HivePartition partitionGot2 =
        loadHivePartition(schemaName, createdTable.name(), partitionAdded2.name());
    List<String> partitionValues2 = partitionValues(partitionGot2);
    Assertions.assertEquals(
        partitionAdded2.values()[0].value().toString(), partitionValues2.get(0));
    Assertions.assertEquals(
        partitionAdded2.values()[1].value().toString(), partitionValues2.get(1));
    Assertions.assertEquals(partitionAdded2.properties(), partitionGot2.properties());

    // test drop partition "hive_col_name2=2024-01-02"
    boolean dropRes2 = createdTable.supportPartitions().dropPartition("hive_col_name2=2024-01-02");
    Assertions.assertTrue(dropRes2);
    Assertions.assertThrows(
        NoSuchPartitionException.class,
        () -> loadHivePartition(schemaName, createdTable.name(), partitionAdded1.name()));
    Path partitionDirectory1 = new Path(tableLocation + identity1.name());
    Assertions.assertFalse(
        fileSystem.exists(partitionDirectory1), "The partition directory should not exist");
    Assertions.assertThrows(
        NoSuchPartitionException.class,
        () -> loadHivePartition(schemaName, createdTable.name(), partitionAdded2.name()));
    Path partitionDirectory2 = new Path(tableLocation + identity2.name());
    Assertions.assertFalse(
        fileSystem.exists(partitionDirectory2), "The partition directory should not exist");

    // test no-exist partition with ifExist=false
    Assertions.assertFalse(createdTable.supportPartitions().dropPartition(partitionAdded.name()));
  }

  @Test
  public void testPurgePartition()
      throws InterruptedException, UnsupportedOperationException, TException {
    Assumptions.assumeTrue(enableSparkTest);
    Table createdTable = preparePartitionedTable();
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> createdTable.supportPartitions().purgePartition("testPartition"));
  }

  private Table preparePartitionedTable() throws TException, InterruptedException {
    Column[] columns = createColumns();

    NameIdentifier nameIdentifier =
        NameIdentifier.of(schemaName, GravitinoITUtils.genRandomName(TABLE_PREFIX));
    Map<String, String> properties = createProperties();
    Table table =
        catalog
            .asTableCatalog()
            .createTable(
                nameIdentifier,
                columns,
                TABLE_COMMENT,
                properties,
                new Transform[] {
                  Transforms.identity(columns[1].name()), Transforms.identity(columns[2].name())
                });
    HiveTable actualTable = loadHiveTable(schemaName, table.name());
    checkTableReadWrite(actualTable);
    return table;
  }

  private void assertTableEquals(Table createdTable, HiveTable hiveTable) {
    Assertions.assertEquals(schemaName.toLowerCase(), hiveTable.databaseName());
    Assertions.assertEquals(createdTable.name(), hiveTable.name());
    Assertions.assertEquals(createdTable.comment(), hiveTable.comment());

    Column[] expectedColumns = createdTable.columns();
    Column[] actualColumns = hiveTable.columns();
    Assertions.assertEquals(expectedColumns.length, actualColumns.length);
    for (int i = 0; i < expectedColumns.length; i++) {
      Column expectedColumn = expectedColumns[i];
      Column actualColumn = actualColumns[i];
      Assertions.assertEquals(expectedColumn.name(), actualColumn.name());
      Assertions.assertEquals(expectedColumn.dataType(), actualColumn.dataType());
      Assertions.assertEquals(expectedColumn.comment(), actualColumn.comment());
    }

    compareDistributions(createdTable.distribution(), hiveTable.distribution());
    compareSortOrders(createdTable.sortOrder(), hiveTable.sortOrder());
    comparePartitioning(createdTable.partitioning(), hiveTable.partitionFieldNames());
  }

  @Test
  void testAlterUnknownTable() {
    NameIdentifier identifier = NameIdentifier.of(schemaName, "unknown");
    TableCatalog tableCatalog = catalog.asTableCatalog();
    TableChange change = TableChange.updateComment("new_comment");
    Assertions.assertThrows(
        NoSuchTableException.class,
        () -> {
          tableCatalog.alterTable(identifier, change);
        });
  }

  @Test
  public void testAlterHiveTable() throws TException, InterruptedException {
    Assumptions.assumeTrue(enableSparkTest);
    Column[] columns = createColumns();
    Table createdTable =
        catalog
            .asTableCatalog()
            .createTable(
                NameIdentifier.of(schemaName, tableName),
                columns,
                TABLE_COMMENT,
                createProperties(),
                new Transform[] {Transforms.identity(columns[2].name())});
    Assertions.assertNull(createdTable.auditInfo().lastModifier());
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, createdTable.auditInfo().creator());
    Table alteredTable =
        catalog
            .asTableCatalog()
            .alterTable(
                NameIdentifier.of(schemaName, tableName),
                TableChange.rename(ALTER_TABLE_NAME),
                TableChange.updateComment(TABLE_COMMENT + "_new"),
                TableChange.removeProperty("key1"),
                TableChange.setProperty("key2", "val2_new"),
                TableChange.addColumn(
                    new String[] {"col_4"},
                    Types.StringType.get(),
                    null,
                    TableChange.ColumnPosition.after(columns[1].name())),
                TableChange.renameColumn(new String[] {HIVE_COL_NAME2}, "col_2_new"),
                TableChange.updateColumnComment(new String[] {HIVE_COL_NAME1}, "comment_new"),
                TableChange.updateColumnType(
                    new String[] {HIVE_COL_NAME1}, Types.IntegerType.get()));
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, alteredTable.auditInfo().creator());
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, alteredTable.auditInfo().lastModifier());

    // Direct get table from Hive metastore to check if the table is altered successfully.
    HiveTable hiveTab = loadHiveTable(schemaName, ALTER_TABLE_NAME);
    Assertions.assertEquals(schemaName.toLowerCase(), hiveTab.databaseName());
    Assertions.assertEquals(ALTER_TABLE_NAME, hiveTab.name());
    Assertions.assertEquals("val2_new", hiveTab.properties().get("key2"));

    Column[] hiveColumns = hiveTab.columns();
    Assertions.assertEquals(HIVE_COL_NAME1, hiveColumns[0].name());
    Assertions.assertEquals(Types.IntegerType.get(), hiveColumns[0].dataType());
    Assertions.assertEquals("comment_new", hiveColumns[0].comment());

    Assertions.assertEquals("col_2_new", hiveColumns[1].name());
    Assertions.assertEquals(Types.DateType.get(), hiveColumns[1].dataType());
    Assertions.assertEquals("col_2_comment", hiveColumns[1].comment());

    Assertions.assertEquals("col_4", hiveColumns[2].name());
    Assertions.assertEquals(Types.StringType.get(), hiveColumns[2].dataType());
    Assertions.assertNull(hiveColumns[2].comment());

    Assertions.assertEquals(1, hiveTab.partitionFieldNames().size());
    Assertions.assertEquals(columns[2].name(), hiveTab.partitionFieldNames().get(0));
    assertDefaultTableProperties(alteredTable, hiveTab);
    checkTableReadWrite(hiveTab);

    // test alter partition column exception
    TableCatalog tableCatalog = catalog.asTableCatalog();
    NameIdentifier id = NameIdentifier.of(schemaName, ALTER_TABLE_NAME);
    TableChange updateType =
        TableChange.updateColumnType(new String[] {HIVE_COL_NAME3}, Types.IntegerType.get());
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> {
              tableCatalog.alterTable(id, updateType);
            });
    Assertions.assertTrue(exception.getMessage().contains("Cannot alter partition column"));

    // test add column with default value exception
    TableChange withDefaultValue =
        TableChange.addColumn(
            new String[] {"col_3"}, Types.ByteType.get(), "comment", Literals.NULL);
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> tableCatalog.alterTable(id, withDefaultValue));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains(
                "The DEFAULT constraint for column is only supported since Hive 3.0, "
                    + "but the current Gravitino Hive catalog only supports Hive 2.x"),
        "The exception message is: " + exception.getMessage());

    // test alter column nullability exception
    TableChange alterColumnNullability =
        TableChange.updateColumnNullability(new String[] {HIVE_COL_NAME1}, false);
    exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> tableCatalog.alterTable(id, alterColumnNullability));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains(
                "The NOT NULL constraint for column is only supported since Hive 3.0,"
                    + " but the current Gravitino Hive catalog only supports Hive 2.x. Illegal column: hive_col_name1"));

    // test update column default value exception
    TableChange updateDefaultValue =
        TableChange.updateColumnDefaultValue(new String[] {HIVE_COL_NAME1}, Literals.NULL);
    exception =
        assertThrows(
            IllegalArgumentException.class, () -> tableCatalog.alterTable(id, updateDefaultValue));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains(
                "The DEFAULT constraint for column is only supported since Hive 3.0, "
                    + "but the current Gravitino Hive catalog only supports Hive 2.x"),
        "The exception message is: " + exception.getMessage());

    // test updateColumnPosition exception
    Column col1 = Column.of("name", Types.StringType.get(), "comment");
    Column col2 = Column.of("address", Types.StringType.get(), "comment");
    Column col3 = Column.of("date_of_birth", Types.DateType.get(), "comment");

    Column[] newColumns = new Column[] {col1, col2, col3};
    NameIdentifier tableIdentifier =
        NameIdentifier.of(schemaName, GravitinoITUtils.genRandomName("CatalogHiveIT_table"));
    catalog
        .asTableCatalog()
        .createTable(
            tableIdentifier,
            newColumns,
            TABLE_COMMENT,
            ImmutableMap.of(),
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            new SortOrder[0]);

    TableChange updatePos =
        TableChange.updateColumnPosition(
            new String[] {"date_of_birth"}, TableChange.ColumnPosition.first());
    exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> tableCatalog.alterTable(tableIdentifier, updatePos));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains(
                "please ensure that the type of the new column position is compatible with the old one"));
  }

  private void assertDefaultTableProperties(Table gravitinoReturnTable, HiveTable actualTable) {
    HiveTablePropertiesMetadata tablePropertiesMetadata = new HiveTablePropertiesMetadata();
    Assertions.assertEquals(
        tablePropertiesMetadata.getDefaultValue(SERDE_LIB),
        actualTable.properties().get(SERDE_LIB));
    Assertions.assertEquals(
        tablePropertiesMetadata.getDefaultValue(INPUT_FORMAT),
        actualTable.properties().get(INPUT_FORMAT));
    Assertions.assertEquals(
        tablePropertiesMetadata.getDefaultValue(OUTPUT_FORMAT),
        actualTable.properties().get(OUTPUT_FORMAT));
    Assertions.assertEquals(
        ((TableType) tablePropertiesMetadata.getDefaultValue(TABLE_TYPE)).name(),
        actualTable.properties().get(TABLE_TYPE));
    Assertions.assertEquals(tableName.toLowerCase(), actualTable.properties().get(SERDE_NAME));
    Assertions.assertEquals(
        ((Boolean) tablePropertiesMetadata.getDefaultValue(EXTERNAL)).toString().toUpperCase(),
        actualTable.properties().get(EXTERNAL));
    Assertions.assertNotNull(actualTable.properties().get(COMMENT));
    Assertions.assertNotNull(actualTable.properties().get(LOCATION));
    Assertions.assertNotNull(gravitinoReturnTable.properties().get(TRANSIENT_LAST_DDL_TIME));
  }

  @Test
  public void testDropHiveTable() {
    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(schemaName, tableName),
            createColumns(),
            TABLE_COMMENT,
            createProperties(),
            Transforms.EMPTY_TRANSFORM);
    catalog.asTableCatalog().dropTable(NameIdentifier.of(schemaName, ALTER_TABLE_NAME));

    // Directly get table from Hive metastore to check if the table is dropped successfully.
    assertThrows(NoSuchTableException.class, () -> loadHiveTable(schemaName, ALTER_TABLE_NAME));
  }

  @Test
  public void testAlterSchema() throws TException, InterruptedException {
    GravitinoMetalake metalake = client.loadMetalake(metalakeName);
    Catalog catalog = metalake.loadCatalog(catalogName);
    Schema schema = catalog.asSchemas().loadSchema(schemaName);
    Assertions.assertNull(schema.auditInfo().lastModifier());
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, schema.auditInfo().creator());
    schema =
        catalog
            .asSchemas()
            .alterSchema(
                schemaName,
                SchemaChange.removeProperty("key1"),
                SchemaChange.setProperty("key2", "val2-alter"));

    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, schema.auditInfo().lastModifier());
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, schema.auditInfo().creator());

    Map<String, String> properties2 = catalog.asSchemas().loadSchema(schemaName).properties();
    Assertions.assertFalse(properties2.containsKey("key1"));
    Assertions.assertEquals("val2-alter", properties2.get("key2"));

    HiveSchema database = loadHiveSchema(schemaName);
    Map<String, String> properties3 = database.properties();
    Assertions.assertFalse(properties3.containsKey("key1"));
    Assertions.assertEquals("val2-alter", properties3.get("key2"));
  }

  @Test
  void testLoadEntityWithSamePrefix() {
    GravitinoMetalake metalake = client.loadMetalake(metalakeName);
    Catalog catalog = metalake.loadCatalog(catalogName);
    Assertions.assertNotNull(catalog);

    for (int i = 1; i < metalakeName.length(); i++) {
      // We can't get the metalake by prefix
      final int length = i;
      final NameIdentifier id = NameIdentifier.of(metalakeName.substring(0, length));
      Assertions.assertThrows(NoSuchMetalakeException.class, () -> client.loadMetalake(id.name()));
    }
    final NameIdentifier idA = NameIdentifier.of(metalakeName + "a");
    Assertions.assertThrows(NoSuchMetalakeException.class, () -> client.loadMetalake(idA.name()));

    for (int i = 1; i < catalogName.length(); i++) {
      // We can't get the catalog by prefix
      final int length = i;
      final NameIdentifier id = NameIdentifier.of(metalakeName, catalogName.substring(0, length));
      Assertions.assertThrows(NoSuchCatalogException.class, () -> metalake.loadCatalog(id.name()));
    }

    // We can't load the catalog.
    final NameIdentifier idB = NameIdentifier.of(metalakeName, catalogName + "a");
    Assertions.assertThrows(NoSuchCatalogException.class, () -> metalake.loadCatalog(idB.name()));

    SupportsSchemas schemas = catalog.asSchemas();

    for (int i = 1; i < schemaName.length(); i++) {
      // We can't get the schema by prefix
      final int length = i;
      final NameIdentifier id = NameIdentifier.of(schemaName.substring(0, length));
      Assertions.assertThrows(NoSuchSchemaException.class, () -> schemas.loadSchema(id.name()));
    }

    NameIdentifier idC = NameIdentifier.of(schemaName + "a");
    Assertions.assertThrows(NoSuchSchemaException.class, () -> schemas.loadSchema(idC.name()));

    TableCatalog tableCatalog = catalog.asTableCatalog();

    for (int i = 1; i < tableName.length(); i++) {
      // We can't get the table by prefix
      final int length = i;
      final NameIdentifier id = NameIdentifier.of(schemaName, tableName.substring(0, length));
      Assertions.assertThrows(NoSuchTableException.class, () -> tableCatalog.loadTable(id));
    }

    NameIdentifier idD = NameIdentifier.of(schemaName, tableName + "a");
    Assertions.assertThrows(NoSuchTableException.class, () -> tableCatalog.loadTable(idD));
  }

  @Test
  void testAlterEntityName() {
    String metalakeName = GravitinoITUtils.genRandomName("CatalogHiveIT_metalake");
    client.createMetalake(metalakeName, "", ImmutableMap.of());
    final GravitinoMetalake metalake = client.loadMetalake(metalakeName);
    String newMetalakeName = GravitinoITUtils.genRandomName("CatalogHiveIT_metalake_new");

    // Test rename metalake
    NameIdentifier id = NameIdentifier.of(metalakeName);
    NameIdentifier newId = NameIdentifier.of(newMetalakeName);
    for (int i = 0; i < 2; i++) {
      Assertions.assertThrows(
          NoSuchMetalakeException.class, () -> client.loadMetalake(newId.name()));
      client.alterMetalake(id.name(), MetalakeChange.rename(newMetalakeName));
      client.loadMetalake(newId.name());
      Assertions.assertThrows(NoSuchMetalakeException.class, () -> client.loadMetalake(id.name()));

      client.alterMetalake(newId.name(), MetalakeChange.rename(metalakeName));
      client.loadMetalake(id.name());
      Assertions.assertThrows(
          NoSuchMetalakeException.class, () -> client.loadMetalake(newId.name()));
    }

    String catalogName = GravitinoITUtils.genRandomName("CatalogHiveIT_catalog");
    metalake.createCatalog(
        catalogName,
        Catalog.Type.RELATIONAL,
        provider,
        "comment",
        ImmutableMap.of(METASTORE_URIS, hiveMetastoreUris));

    Catalog catalog = metalake.loadCatalog(catalogName);
    // Test rename catalog
    String newCatalogName = GravitinoITUtils.genRandomName("CatalogHiveIT_catalog_new");
    NameIdentifier newId2 = NameIdentifier.of(metalakeName, newMetalakeName);
    NameIdentifier oldId = NameIdentifier.of(metalakeName, catalogName);
    for (int i = 0; i < 2; i++) {
      Assertions.assertThrows(
          NoSuchCatalogException.class, () -> metalake.loadCatalog(newId2.name()));
      metalake.alterCatalog(catalogName, CatalogChange.rename(newCatalogName));
      metalake.loadCatalog(newCatalogName);
      Assertions.assertThrows(
          NoSuchCatalogException.class, () -> metalake.loadCatalog(oldId.name()));

      metalake.alterCatalog(newCatalogName, CatalogChange.rename(catalogName));
      catalog = metalake.loadCatalog(oldId.name());
      Assertions.assertThrows(
          NoSuchCatalogException.class, () -> metalake.loadCatalog(newId2.name()));
    }

    // Schema does not have the rename operation.
    final String schemaName = GravitinoITUtils.genRandomName("CatalogHiveIT_schema");
    catalog.asSchemas().createSchema(schemaName, "", createSchemaProperties());

    final Catalog cata = catalog;
    // Now try to rename table
    final String tableName = GravitinoITUtils.genRandomName("CatalogHiveIT_table");
    final String newTableName = GravitinoITUtils.genRandomName("CatalogHiveIT_table_new");
    Column[] columns = createColumns();
    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(schemaName, tableName),
            columns,
            TABLE_COMMENT,
            createProperties(),
            Transforms.EMPTY_TRANSFORM);

    NameIdentifier id3 = NameIdentifier.of(schemaName, newTableName);
    NameIdentifier id4 = NameIdentifier.of(schemaName, tableName);
    TableChange newRename = TableChange.rename(newTableName);
    TableChange oldRename = TableChange.rename(tableName);
    TableCatalog tableCatalog = catalog.asTableCatalog();
    TableCatalog tableCata = cata.asTableCatalog();

    for (int i = 0; i < 2; i++) {
      // The table to be renamed does not exist
      Assertions.assertThrows(NoSuchTableException.class, () -> tableCata.loadTable(id3));
      tableCatalog.alterTable(id4, newRename);
      Table table = tableCatalog.loadTable(id3);
      Assertions.assertNotNull(table);

      // Old Table should not exist anymore.
      Assertions.assertThrows(NoSuchTableException.class, () -> tableCata.loadTable(id4));

      tableCatalog.alterTable(id3, oldRename);
      table = catalog.asTableCatalog().loadTable(id4);
      Assertions.assertNotNull(table);
    }
  }

  @Test
  void testDropAndRename() {
    String metalakeName1 = GravitinoITUtils.genRandomName("CatalogHiveIT_metalake1");
    String metalakeName2 = GravitinoITUtils.genRandomName("CatalogHiveIT_metalake2");

    client.createMetalake(metalakeName1, "comment", Collections.emptyMap());
    client.createMetalake(metalakeName2, "comment", Collections.emptyMap());

    client.dropMetalake(metalakeName1, true);
    client.dropMetalake(metalakeName2, true);

    client.createMetalake(metalakeName1, "comment", Collections.emptyMap());

    client.alterMetalake(metalakeName1, MetalakeChange.rename(metalakeName2));

    client.loadMetalake(metalakeName2);

    Assertions.assertThrows(
        NoSuchMetalakeException.class,
        () -> {
          client.loadMetalake(metalakeName1);
        });
  }

  @Test
  public void testDropHiveManagedTable() throws TException, InterruptedException, IOException {
    Assumptions.assumeTrue(enableSparkTest);
    Column[] columns = createColumns();
    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(schemaName, tableName),
            columns,
            TABLE_COMMENT,
            createProperties(),
            new Transform[] {Transforms.identity(columns[2].name())});

    // Directly get table from Hive metastore to check if the table is created successfully.
    HiveTable hiveTab = loadHiveTable(schemaName, tableName);
    checkTableReadWrite(hiveTab);
    Assertions.assertEquals(MANAGED_TABLE.name(), hiveTab.properties().get(TABLE_TYPE));
    String tableLocation = hiveTab.properties().get(LOCATION);
    Assertions.assertNotNull(tableLocation, "Table location should not be null");
    Path tableDirectory = new Path(tableLocation);
    catalog.asTableCatalog().dropTable(NameIdentifier.of(schemaName, tableName));
    Boolean existed = hiveTableExists(schemaName, tableName);
    Assertions.assertFalse(existed, "The Hive table should not exist");
    Assertions.assertFalse(
        fileSystem.exists(tableDirectory), "The table directory should not exist");
  }

  @Test
  public void testDropHiveExternalTable() throws TException, InterruptedException, IOException {
    Assumptions.assumeTrue(enableSparkTest);
    Column[] columns = createColumns();
    Map<String, String> properties = createProperties();
    properties.put(TABLE_TYPE, EXTERNAL_TABLE.name().toLowerCase(Locale.ROOT));

    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(schemaName, tableName),
            columns,
            TABLE_COMMENT,
            properties,
            new Transform[] {Transforms.identity(columns[2].name())});
    // Directly get table from Hive metastore to check if the table is created successfully.
    HiveTable hiveTab = loadHiveTable(schemaName, tableName);
    checkTableReadWrite(hiveTab);
    Assertions.assertEquals(EXTERNAL_TABLE.name(), hiveTab.properties().get(TABLE_TYPE));
    catalog.asTableCatalog().dropTable(NameIdentifier.of(schemaName, tableName));

    Boolean existed = hiveTableExists(schemaName, tableName);
    Assertions.assertFalse(existed, "The table should be not exist");
    String tableLocation = hiveTab.properties().get(LOCATION);
    Assertions.assertNotNull(tableLocation, "Table location should not be null");
    Path tableDirectory = new Path(tableLocation);
    Assertions.assertTrue(
        fileSystem.listStatus(tableDirectory).length > 0, "The table should not be empty");
  }

  @Test
  public void testPurgeHiveManagedTable() throws TException, InterruptedException, IOException {
    Assumptions.assumeTrue(enableSparkTest);
    Column[] columns = createColumns();
    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(schemaName, tableName),
            columns,
            TABLE_COMMENT,
            createProperties(),
            new Transform[] {Transforms.identity(columns[2].name())});

    // Directly get table from Hive metastore to check if the table is created successfully.
    HiveTable hiveTab = loadHiveTable(schemaName, tableName);
    checkTableReadWrite(hiveTab);
    Assertions.assertEquals(MANAGED_TABLE.name(), hiveTab.properties().get(TABLE_TYPE));
    catalog.asTableCatalog().purgeTable(NameIdentifier.of(schemaName, tableName));
    Boolean existed = hiveTableExists(schemaName, tableName);
    Assertions.assertFalse(existed, "The Hive table should not exist");
    // purging non-exist table should return false
    Assertions.assertFalse(
        catalog.asTableCatalog().purgeTable(NameIdentifier.of(schemaName, tableName)),
        "The table should not be found in the catalog");
    String tableLocation = hiveTab.properties().get(LOCATION);
    Assertions.assertNotNull(tableLocation, "Table location should not be null");
    Path tableDirectory = new Path(tableLocation);
    Assertions.assertFalse(
        fileSystem.exists(tableDirectory), "The table directory should not exist");
    Path trashDirectory = fileSystem.getTrashRoot(tableDirectory);
    Assertions.assertFalse(fileSystem.exists(trashDirectory), "The trash should not exist");
  }

  @Test
  public void testPurgeHiveExternalTable() throws TException, InterruptedException, IOException {
    Assumptions.assumeTrue(enableSparkTest);
    Column[] columns = createColumns();
    Map<String, String> properties = createProperties();
    properties.put(TABLE_TYPE, EXTERNAL_TABLE.name().toLowerCase(Locale.ROOT));

    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(schemaName, tableName),
            columns,
            TABLE_COMMENT,
            properties,
            new Transform[] {Transforms.identity(columns[2].name())});

    // Directly get table from Hive metastore to check if the table is created successfully.
    HiveTable hiveTab = loadHiveTable(schemaName, tableName);
    checkTableReadWrite(hiveTab);
    Assertions.assertEquals(EXTERNAL_TABLE.name(), hiveTab.properties().get(TABLE_TYPE));
    TableCatalog tableCatalog = catalog.asTableCatalog();
    NameIdentifier id = NameIdentifier.of(schemaName, tableName);
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> {
          tableCatalog.purgeTable(id);
        },
        "Can't purge a external Hive table");

    Boolean existed = hiveTableExists(schemaName, tableName);
    Assertions.assertTrue(existed, "The table should be still exist");
    String tableLocation = hiveTab.properties().get(LOCATION);
    Assertions.assertNotNull(tableLocation, "Table location should not be null");
    Path tableDirectory = new Path(tableLocation);
    Assertions.assertTrue(
        fileSystem.listStatus(tableDirectory).length > 0, "The table should not be empty");
  }

  @Test
  public void testRemoveNonExistTable() throws TException, InterruptedException {
    Column[] columns = createColumns();
    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(schemaName, tableName),
            columns,
            TABLE_COMMENT,
            ImmutableMap.of(TABLE_TYPE, EXTERNAL_TABLE.name().toLowerCase(Locale.ROOT)),
            new Transform[] {Transforms.identity(columns[2].name())});

    // Directly drop table from Hive metastore.
    dropHiveTable(schemaName, tableName, true, false);

    // Drop table from catalog, drop non-exist table should return false;
    Assertions.assertFalse(
        catalog.asTableCatalog().dropTable(NameIdentifier.of(schemaName, tableName)),
        "The table should not be found in the catalog");

    Assertions.assertFalse(
        catalog.asTableCatalog().tableExists(NameIdentifier.of(schemaName, tableName)),
        "The table should not be found in the catalog");
  }

  @Test
  public void testPurgeNonExistTable() throws TException, InterruptedException {
    Column[] columns = createColumns();
    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(schemaName, tableName),
            columns,
            TABLE_COMMENT,
            ImmutableMap.of(TABLE_TYPE, EXTERNAL_TABLE.name().toLowerCase(Locale.ROOT)),
            new Transform[] {Transforms.identity(columns[2].name())});

    // Directly drop table from Hive metastore.
    dropHiveTable(schemaName, tableName, true, true);

    // Drop table from catalog, drop non-exist table should return false;
    Assertions.assertFalse(
        catalog.asTableCatalog().purgeTable(NameIdentifier.of(schemaName, tableName)),
        "The table should not be found in the catalog");

    Assertions.assertFalse(
        catalog.asTableCatalog().tableExists(NameIdentifier.of(schemaName, tableName)),
        "The table should not be found in the catalog");
  }

  @Test
  void testCustomCatalogOperations() {
    String catalogName = "custom_catalog";
    Assertions.assertDoesNotThrow(
        () -> createCatalogWithCustomOperation(catalogName, HiveCatalogOperations.class.getName()));
    Assertions.assertThrowsExactly(
        RuntimeException.class,
        () ->
            createCatalogWithCustomOperation(
                catalogName + "_not_exists", "org.apache.gravitino.catalog.not.exists"));
  }

  @Test
  void testAlterCatalogProperties() {
    Map<String, String> properties = Maps.newHashMap();
    String nameOfCatalog = GravitinoITUtils.genRandomName("catalog");
    // Wrong Hive HIVE_METASTORE_URIS
    String wrongHiveMetastoreURI = hiveMetastoreUris + "_wrong";
    properties.put(METASTORE_URIS, wrongHiveMetastoreURI);
    Catalog createdCatalog =
        metalake.createCatalog(
            nameOfCatalog, Catalog.Type.RELATIONAL, provider, "comment", properties);
    Assertions.assertEquals(wrongHiveMetastoreURI, createdCatalog.properties().get(METASTORE_URIS));

    // As it's wrong metastore uri, it should throw exception.
    Exception exception =
        Assertions.assertThrows(
            Exception.class,
            () ->
                createdCatalog
                    .asSchemas()
                    .createSchema("schema", "comment", createSchemaProperties()));
    Assertions.assertTrue(exception.getMessage().contains("Failed to connect to Hive Metastore"));

    Catalog newCatalog =
        metalake.alterCatalog(
            nameOfCatalog, CatalogChange.setProperty(METASTORE_URIS, hiveMetastoreUris));
    Assertions.assertEquals(hiveMetastoreUris, newCatalog.properties().get(METASTORE_URIS));

    // The URI has restored, so it should not throw exception.
    Assertions.assertDoesNotThrow(
        () -> {
          newCatalog.asSchemas().createSchema("schema", "comment", createSchemaProperties());
        });

    newCatalog.asSchemas().dropSchema("schema", true);
    metalake.dropCatalog(nameOfCatalog, true);
  }

  private void createCatalogWithCustomOperation(String catalogName, String customImpl) {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(METASTORE_URIS, hiveMetastoreUris);
    properties.put(BaseCatalog.CATALOG_OPERATION_IMPL, customImpl);

    Catalog catalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.RELATIONAL, provider, "comment", properties);
    catalog.asSchemas().listSchemas();
  }

  protected Map<String, String> createSchemaProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    properties.put(
        "location",
        String.format(
            "hdfs://%s:%d/user/hive/warehouse/%s.db",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT,
            schemaName.toLowerCase()));
    return properties;
  }
}
