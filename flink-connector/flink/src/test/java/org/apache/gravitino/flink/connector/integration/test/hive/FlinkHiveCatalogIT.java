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
package org.apache.gravitino.flink.connector.integration.test.hive;

import static org.apache.gravitino.flink.connector.integration.test.utils.TestUtils.toFlinkPhysicalColumn;
import static org.apache.gravitino.rel.expressions.transforms.Transforms.EMPTY_TRANSFORM;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.DefaultCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions;
import org.apache.flink.table.catalog.hive.util.Constants;
import org.apache.flink.types.Row;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.catalog.hive.HiveStorageConstants;
import org.apache.gravitino.flink.connector.CatalogPropertiesConverter;
import org.apache.gravitino.flink.connector.hive.GravitinoHiveCatalog;
import org.apache.gravitino.flink.connector.hive.GravitinoHiveCatalogFactoryOptions;
import org.apache.gravitino.flink.connector.integration.test.FlinkCommonIT;
import org.apache.gravitino.flink.connector.integration.test.utils.TestUtils;
import org.apache.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.RCFileStorageFormatDescriptor;
import org.apache.hadoop.hive.ql.io.StorageFormatDescriptor;
import org.apache.hadoop.hive.ql.io.StorageFormatFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class FlinkHiveCatalogIT extends FlinkCommonIT {
  private static final String DEFAULT_HIVE_CATALOG = "test_flink_hive_schema_catalog";
  private static final String FLINK_USER_NAME = "gravitino";
  private static final String MYSQL_DATABASE = TestDatabaseName.FLINK_HIVE_CATALOG_IT.name();

  private static org.apache.gravitino.Catalog hiveCatalog;
  private static String hiveConfDir;

  private String mysqlUrl;
  private String mysqlUsername;
  private String mysqlPassword;
  private String mysqlDriver;
  private ContainerSuite containerSuite;

  @Override
  protected boolean supportsPrimaryKey() {
    return false;
  }

  @Override
  protected boolean supportColumnOperation() {
    return false;
  }

  @Override
  protected boolean supportTablePropertiesOperation() {
    return false;
  }

  @BeforeAll
  void hiveStartUp() {
    initHiveConfDir();
    initDefaultHiveCatalog();
  }

  @AfterAll
  static void hiveStop() {
    Preconditions.checkArgument(metalake != null, "metalake should not be null");
    metalake.dropCatalog(DEFAULT_HIVE_CATALOG, true);
  }

  @Override
  protected void initCatalogEnv() throws Exception {
    containerSuite = ContainerSuite.getInstance();
    containerSuite.startMySQLContainer(TestDatabaseName.FLINK_HIVE_CATALOG_IT);
    mysqlUrl =
        containerSuite.getMySQLContainer().getJdbcUrl(TestDatabaseName.FLINK_HIVE_CATALOG_IT);
    mysqlUsername = containerSuite.getMySQLContainer().getUsername();
    mysqlPassword = containerSuite.getMySQLContainer().getPassword();
    mysqlDriver =
        containerSuite
            .getMySQLContainer()
            .getDriverClassName(TestDatabaseName.FLINK_HIVE_CATALOG_IT);
  }

  @Override
  protected void stopCatalogEnv() throws Exception {
    if (containerSuite != null) {
      containerSuite.close();
    }
  }

  protected void initDefaultHiveCatalog() {
    Preconditions.checkArgument(metalake != null, "metalake should not be null");
    hiveCatalog =
        metalake.createCatalog(
            DEFAULT_HIVE_CATALOG,
            org.apache.gravitino.Catalog.Type.RELATIONAL,
            getProvider(),
            null,
            ImmutableMap.of("metastore.uris", hiveMetastoreUri));
  }

  private void initHiveConfDir() {
    if (hiveConfDir != null) {
      return;
    }
    try {
      java.nio.file.Path dir = java.nio.file.Files.createTempDirectory("flink-hive-conf");
      java.nio.file.Path hiveSite = dir.resolve("hive-site.xml");
      String hiveSiteXml =
          "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
              + "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n"
              + "<configuration>\n"
              + "  <property>\n"
              + "    <name>hive.metastore.sasl.enabled</name>\n"
              + "    <value>false</value>\n"
              + "  </property>\n"
              + "  <property>\n"
              + "    <name>hive.metastore.uris</name>\n"
              + "    <value>"
              + hiveMetastoreUri
              + "</value>\n"
              + "  </property>\n"
              + "  <property>\n"
              + "    <name>hadoop.security.authentication</name>\n"
              + "    <value>simple</value>\n"
              + "  </property>\n"
              + "  <property>\n"
              + "    <name>hive.metastore.warehouse.dir</name>\n"
              + "    <value>"
              + warehouse
              + "</value>\n"
              + "  </property>\n"
              + "</configuration>\n";
      java.nio.file.Files.write(hiveSite, hiveSiteXml.getBytes(StandardCharsets.UTF_8));
      hiveConfDir = dir.toAbsolutePath().toString();
    } catch (IOException e) {
      throw new RuntimeException("Failed to prepare hive conf dir for ITs", e);
    }
  }

  @Test
  public void testCreateGravitinoHiveCatalog() {
    tableEnv.useCatalog(DEFAULT_CATALOG);
    int numCatalogs = tableEnv.listCatalogs().length;

    // Create a new catalog.

    String catalogName = "gravitino_hive";

    Configuration configuration = new Configuration();
    configuration.set(
        CommonCatalogOptions.CATALOG_TYPE, GravitinoHiveCatalogFactoryOptions.IDENTIFIER);
    configuration.set(HiveCatalogFactoryOptions.HIVE_CONF_DIR, "src/test/resources/flink-tests");
    configuration.set(GravitinoHiveCatalogFactoryOptions.HIVE_METASTORE_URIS, hiveMetastoreUri);
    CatalogDescriptor catalogDescriptor = CatalogDescriptor.of(catalogName, configuration);
    tableEnv.createCatalog(catalogName, catalogDescriptor);
    Assertions.assertTrue(metalake.catalogExists(catalogName));

    // Check the catalog properties.
    org.apache.gravitino.Catalog gravitinoCatalog = metalake.loadCatalog(catalogName);
    Assertions.assertEquals(FLINK_USER_NAME, gravitinoCatalog.auditInfo().creator());

    Map<String, String> properties = gravitinoCatalog.properties();
    Assertions.assertEquals(hiveMetastoreUri, properties.get(HiveConstants.METASTORE_URIS));
    Map<String, String> flinkProperties =
        gravitinoCatalog.properties().entrySet().stream()
            .filter(e -> e.getKey().startsWith(CatalogPropertiesConverter.FLINK_PROPERTY_PREFIX))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    Assertions.assertEquals(2, flinkProperties.size());
    Assertions.assertEquals(
        "src/test/resources/flink-tests",
        flinkProperties.get(flinkByPass(HiveCatalogFactoryOptions.HIVE_CONF_DIR.key())));
    Assertions.assertEquals(
        GravitinoHiveCatalogFactoryOptions.IDENTIFIER,
        flinkProperties.get(flinkByPass(CommonCatalogOptions.CATALOG_TYPE.key())));

    // Get the created catalog.
    Optional<org.apache.flink.table.catalog.Catalog> catalog = tableEnv.getCatalog(catalogName);
    Assertions.assertTrue(catalog.isPresent());
    Assertions.assertInstanceOf(GravitinoHiveCatalog.class, catalog.get());

    // List catalogs.
    String[] catalogs = tableEnv.listCatalogs();
    Assertions.assertEquals(numCatalogs + 1, catalogs.length, "Should create a new catalog");
    Assertions.assertTrue(
        Arrays.asList(catalogs).contains(catalogName), "Should create the correct catalog.");

    Assertions.assertEquals(
        DEFAULT_CATALOG,
        tableEnv.getCurrentCatalog(),
        "Current catalog should be default_catalog in flink");

    // Change the current catalog to the new created catalog.
    tableEnv.useCatalog(catalogName);
    Assertions.assertEquals(
        catalogName,
        tableEnv.getCurrentCatalog(),
        "Current catalog should be the one that is created just now.");

    // Drop the catalog. Only support drop catalog by SQL.
    tableEnv.useCatalog(DEFAULT_CATALOG);
    tableEnv.executeSql("drop catalog " + catalogName);
    Assertions.assertFalse(metalake.catalogExists(catalogName));

    Optional<Catalog> droppedCatalog = tableEnv.getCatalog(catalogName);
    Assertions.assertFalse(droppedCatalog.isPresent(), "Catalog should be dropped");
  }

  @Test
  public void testCreateGravitinoHiveCatalogUsingSQL() {
    tableEnv.useCatalog(DEFAULT_CATALOG);
    int numCatalogs = tableEnv.listCatalogs().length;

    // Create a new catalog.
    String catalogName = "gravitino_hive_sql";
    tableEnv.executeSql(
        String.format(
            "create catalog %s with ("
                + "'type'='gravitino-hive', "
                + "'hive-conf-dir'='src/test/resources/flink-tests',"
                + "'hive.metastore.uris'='%s',"
                + "'unknown.key'='unknown.value'"
                + ")",
            catalogName, hiveMetastoreUri));
    Assertions.assertTrue(metalake.catalogExists(catalogName));

    // Check the properties of the created catalog.
    org.apache.gravitino.Catalog gravitinoCatalog = metalake.loadCatalog(catalogName);
    Map<String, String> properties = gravitinoCatalog.properties();
    Assertions.assertEquals(hiveMetastoreUri, properties.get(HiveConstants.METASTORE_URIS));
    Map<String, String> flinkProperties =
        properties.entrySet().stream()
            .filter(e -> e.getKey().startsWith(CatalogPropertiesConverter.FLINK_PROPERTY_PREFIX))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    Assertions.assertEquals(3, flinkProperties.size());
    Assertions.assertEquals(
        "src/test/resources/flink-tests",
        flinkProperties.get(flinkByPass(HiveCatalogFactoryOptions.HIVE_CONF_DIR.key())));
    Assertions.assertEquals(
        GravitinoHiveCatalogFactoryOptions.IDENTIFIER,
        flinkProperties.get(flinkByPass(CommonCatalogOptions.CATALOG_TYPE.key())));
    Assertions.assertEquals(
        "unknown.value",
        flinkProperties.get(flinkByPass("unknown.key")),
        "The unknown.key will not cause failure and will be saved in Gravitino.");

    // Get the created catalog.
    Optional<org.apache.flink.table.catalog.Catalog> catalog = tableEnv.getCatalog(catalogName);
    Assertions.assertTrue(catalog.isPresent());
    Assertions.assertInstanceOf(GravitinoHiveCatalog.class, catalog.get());

    // List catalogs.
    String[] catalogs = tableEnv.listCatalogs();
    Assertions.assertEquals(numCatalogs + 1, catalogs.length, "Should create a new catalog");
    Assertions.assertTrue(
        Arrays.asList(catalogs).contains(catalogName), "Should create the correct catalog.");

    // Use SQL to list catalogs.
    TableResult result = tableEnv.executeSql("show catalogs");
    Assertions.assertEquals(
        numCatalogs + 1, Lists.newArrayList(result.collect()).size(), "Should have 2 catalogs");

    Assertions.assertEquals(
        DEFAULT_CATALOG,
        tableEnv.getCurrentCatalog(),
        "Current catalog should be default_catalog in flink");

    // Change the current catalog to the new created catalog.
    tableEnv.useCatalog(catalogName);
    Assertions.assertEquals(
        catalogName,
        tableEnv.getCurrentCatalog(),
        "Current catalog should be the one that is created just now.");

    // Drop the catalog. Only support using SQL to drop catalog.
    tableEnv.useCatalog(DEFAULT_CATALOG);
    tableEnv.executeSql("drop catalog " + catalogName);
    Assertions.assertFalse(metalake.catalogExists(catalogName));

    Optional<Catalog> droppedCatalog = tableEnv.getCatalog(catalogName);
    Assertions.assertFalse(droppedCatalog.isPresent(), "Catalog should be dropped");
  }

  @Test
  public void testCreateGravitinoHiveCatalogRequireOptions() {
    tableEnv.useCatalog(DEFAULT_CATALOG);

    // Failed to create the catalog for missing the required options.
    String catalogName = "gravitino_hive_sql2";
    Assertions.assertThrows(
        ValidationException.class,
        () -> {
          tableEnv.executeSql(
              String.format(
                  "create catalog %s with ("
                      + "'type'='gravitino-hive', "
                      + "'hive-conf-dir'='src/test/resources/flink-tests'"
                      + ")",
                  catalogName));
        },
        "The hive.metastore.uris is required.");

    Assertions.assertFalse(metalake.catalogExists(catalogName));
  }

  @Test
  public void testStreamExecutionEnvironmentLoadsGravitinoCatalogStoreConfiguration() {
    String catalogName = "gravitino_hive_stream_env";
    if (metalake.catalogExists(catalogName)) {
      metalake.dropCatalog(catalogName, true);
    }

    metalake.createCatalog(
        catalogName,
        org.apache.gravitino.Catalog.Type.RELATIONAL,
        getProvider(),
        null,
        ImmutableMap.of(HiveConstants.METASTORE_URIS, hiveMetastoreUri));

    try {
      Configuration configuration = new Configuration();
      configuration.setString(
          "table.catalog-store.kind", GravitinoCatalogStoreFactoryOptions.GRAVITINO);
      configuration.setString(
          "table.catalog-store.gravitino.gravitino.metalake", GRAVITINO_METALAKE);
      configuration.setString("table.catalog-store.gravitino.gravitino.uri", gravitinoUri);

      StreamExecutionEnvironment env =
          StreamExecutionEnvironment.getExecutionEnvironment(configuration);
      EnvironmentSettings settings =
          EnvironmentSettings.newInstance().withConfiguration(configuration).build();
      StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env, settings);

      Assertions.assertTrue(
          Arrays.asList(streamTableEnv.listCatalogs()).contains(catalogName),
          "StreamTableEnvironment should load Gravitino catalog store configs");
    } finally {
      metalake.dropCatalog(catalogName, true);
    }
  }

  @Test
  public void testGetCatalogFromGravitino() {
    // list catalogs.
    int numCatalogs = tableEnv.listCatalogs().length;

    // create a new catalog.
    String catalogName = "hive_catalog_in_gravitino";
    org.apache.gravitino.Catalog gravitinoCatalog =
        metalake.createCatalog(
            catalogName,
            org.apache.gravitino.Catalog.Type.RELATIONAL,
            "hive",
            null,
            ImmutableMap.of(
                "flink.bypass.hive-conf-dir",
                "src/test/resources/flink-tests",
                "flink.bypass.hive.test",
                "hive.config",
                "metastore.uris",
                hiveMetastoreUri));
    Assertions.assertNotNull(gravitinoCatalog);
    Assertions.assertEquals(catalogName, gravitinoCatalog.name());
    Assertions.assertTrue(metalake.catalogExists(catalogName));
    Assertions.assertEquals(
        numCatalogs + 1, tableEnv.listCatalogs().length, "Should create a new catalog");

    // get the catalog from Gravitino.
    Optional<Catalog> flinkHiveCatalog = tableEnv.getCatalog(catalogName);
    Assertions.assertTrue(flinkHiveCatalog.isPresent());
    Assertions.assertInstanceOf(GravitinoHiveCatalog.class, flinkHiveCatalog.get());
    GravitinoHiveCatalog gravitinoHiveCatalog = (GravitinoHiveCatalog) flinkHiveCatalog.get();
    HiveConf hiveConf = gravitinoHiveCatalog.getHiveConf();
    Assertions.assertTrue(hiveConf.size() > 0, "Should have hive conf");
    Assertions.assertEquals("hive.config", hiveConf.get("hive.test"));
    Assertions.assertEquals(
        hiveMetastoreUri, hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname));

    // drop the catalog.
    tableEnv.useCatalog(DEFAULT_CATALOG);
    tableEnv.executeSql("drop catalog " + catalogName);
    Assertions.assertFalse(metalake.catalogExists(catalogName));
    Assertions.assertEquals(
        numCatalogs, tableEnv.listCatalogs().length, "The created catalog should be dropped.");
  }

  @Test
  public void testRawHivePartitionTable() {
    String databaseName = "test_create_hive_partition_table_db";
    String tableName = "test_create_hive_partition_table";
    String comment = "test comment";
    String key = "test key";
    String value = "test value";

    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          TableResult result =
              sql(
                  "CREATE TABLE %s "
                      + "(string_type STRING COMMENT 'string_type', "
                      + " double_type DOUBLE COMMENT 'double_type')"
                      + " COMMENT '%s' "
                      + " PARTITIONED BY (string_type, double_type)"
                      + " WITH ("
                      + "'%s' = '%s',"
                      + "'connector'='hive')",
                  tableName, comment, key, value);
          TestUtils.assertTableResult(result, ResultKind.SUCCESS);

          Table table =
              catalog.asTableCatalog().loadTable(NameIdentifier.of(databaseName, tableName));
          Assertions.assertNotNull(table);
          Assertions.assertEquals(comment, table.comment());
          Assertions.assertEquals(value, table.properties().get(key));
          Column[] columns =
              new Column[] {
                Column.of("string_type", Types.StringType.get(), "string_type", true, false, null),
                Column.of("double_type", Types.DoubleType.get(), "double_type")
              };
          assertColumns(columns, table.columns());
          Transform[] partitions =
              new Transform[] {
                Transforms.identity("string_type"), Transforms.identity("double_type")
              };
          Assertions.assertArrayEquals(partitions, table.partitioning());

          // load flink catalog
          try {
            Catalog flinkCatalog = tableEnv.getCatalog(currentCatalog().name()).get();
            CatalogBaseTable flinkTable =
                flinkCatalog.getTable(ObjectPath.fromString(databaseName + "." + tableName));
            DefaultCatalogTable catalogTable = (DefaultCatalogTable) flinkTable;
            Assertions.assertTrue(catalogTable.isPartitioned());
            Assertions.assertArrayEquals(
                new String[] {"string_type", "double_type"},
                catalogTable.getPartitionKeys().toArray());
          } catch (Exception e) {
            Assertions.fail("Table should be exist", e);
          }

          // write and read.
          TestUtils.assertTableResult(
              sql("INSERT INTO %s VALUES ('A', 1.0), ('B', 2.0)", tableName),
              ResultKind.SUCCESS_WITH_CONTENT,
              Row.of(-1L));
          TestUtils.assertTableResult(
              sql("SELECT * FROM %s ORDER BY double_type", tableName),
              ResultKind.SUCCESS_WITH_CONTENT,
              Row.of("A", 1.0),
              Row.of("B", 2.0));
          try {
            Assertions.assertTrue(
                hdfs.exists(
                    new Path(
                        table.properties().get("location") + "/string_type=A/double_type=1.0")));
            Assertions.assertTrue(
                hdfs.exists(
                    new Path(
                        table.properties().get("location") + "/string_type=B/double_type=2.0")));
          } catch (IOException e) {
            Assertions.fail("The partition directory should be exist.", e);
          }
        },
        true);
  }

  @Test
  public void testCreateRawHiveTable() {
    String databaseName = "test_create_hive_table_db";
    String tableName = "test_create_hive_table";
    String comment = "test comment";
    String key = "test key";
    String value = "test value";

    // 1. The NOT NULL constraint for column is only supported since Hive 3.0,
    // but the current Gravitino Hive catalog only supports Hive 2.x.
    // 2. Hive doesn't support Time and Timestamp with timezone type.
    // 3. Flink SQL only support to create Interval Month and Second(3).
    doWithSchema(
        metalake.loadCatalog(DEFAULT_HIVE_CATALOG),
        databaseName,
        catalog -> {
          TableResult result =
              sql(
                  "CREATE TABLE %s "
                      + "(string_type STRING COMMENT 'string_type', "
                      + " double_type DOUBLE COMMENT 'double_type',"
                      + " int_type INT COMMENT 'int_type',"
                      + " varchar_type VARCHAR COMMENT 'varchar_type',"
                      + " char_type CHAR COMMENT 'char_type',"
                      + " boolean_type BOOLEAN COMMENT 'boolean_type',"
                      + " byte_type TINYINT COMMENT 'byte_type',"
                      + " binary_type VARBINARY(10) COMMENT 'binary_type',"
                      + " decimal_type DECIMAL(10, 2) COMMENT 'decimal_type',"
                      + " bigint_type BIGINT COMMENT 'bigint_type',"
                      + " float_type FLOAT COMMENT 'float_type',"
                      + " date_type DATE COMMENT 'date_type',"
                      + " timestamp_type TIMESTAMP COMMENT 'timestamp_type',"
                      + " smallint_type SMALLINT COMMENT 'smallint_type',"
                      + " array_type ARRAY<INT> COMMENT 'array_type',"
                      + " map_type MAP<INT, STRING> COMMENT 'map_type',"
                      + " struct_type ROW<k1 INT, k2 String>)"
                      + " COMMENT '%s' WITH ("
                      + "'%s' = '%s',"
                      + "'connector'='hive')",
                  tableName, comment, key, value);
          TestUtils.assertTableResult(result, ResultKind.SUCCESS);

          Table table =
              catalog.asTableCatalog().loadTable(NameIdentifier.of(databaseName, tableName));
          Assertions.assertNotNull(table);
          Assertions.assertEquals(comment, table.comment());
          Assertions.assertEquals(value, table.properties().get(key));
          Column[] columns =
              new Column[] {
                Column.of("string_type", Types.StringType.get(), "string_type", true, false, null),
                Column.of("double_type", Types.DoubleType.get(), "double_type"),
                Column.of("int_type", Types.IntegerType.get(), "int_type"),
                Column.of("varchar_type", Types.StringType.get(), "varchar_type"),
                Column.of("char_type", Types.FixedCharType.of(1), "char_type"),
                Column.of("boolean_type", Types.BooleanType.get(), "boolean_type"),
                Column.of("byte_type", Types.ByteType.get(), "byte_type"),
                Column.of("binary_type", Types.BinaryType.get(), "binary_type"),
                Column.of("decimal_type", Types.DecimalType.of(10, 2), "decimal_type"),
                Column.of("bigint_type", Types.LongType.get(), "bigint_type"),
                Column.of("float_type", Types.FloatType.get(), "float_type"),
                Column.of("date_type", Types.DateType.get(), "date_type"),
                Column.of(
                    "timestamp_type", Types.TimestampType.withoutTimeZone(), "timestamp_type"),
                Column.of("smallint_type", Types.ShortType.get(), "smallint_type"),
                Column.of(
                    "array_type", Types.ListType.of(Types.IntegerType.get(), true), "array_type"),
                Column.of(
                    "map_type",
                    Types.MapType.of(Types.IntegerType.get(), Types.StringType.get(), true),
                    "map_type"),
                Column.of(
                    "struct_type",
                    Types.StructType.of(
                        Types.StructType.Field.nullableField("k1", Types.IntegerType.get()),
                        Types.StructType.Field.nullableField("k2", Types.StringType.get())),
                    null)
              };
          assertColumns(columns, table.columns());
          Assertions.assertArrayEquals(EMPTY_TRANSFORM, table.partitioning());
        },
        true);
  }

  @Test
  public void testRowFormatSerdeOverridesDefaultFormat() {
    String databaseName = "test_hive_row_format_precedence_db";
    String tableName = "test_row_format_overrides_default";
    String customSerde = HiveStorageConstants.OPENCSV_SERDE_CLASS;

    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          TestUtils.assertTableResult(
              sql(
                  "CREATE TABLE %s (id STRING) WITH ('%s'='%s', 'connector'='hive')",
                  tableName, Constants.SERDE_LIB_CLASS_NAME, customSerde),
              ResultKind.SUCCESS);
          TestUtils.assertTableResult(
              sql("INSERT INTO %s VALUES ('1')", tableName),
              ResultKind.SUCCESS_WITH_CONTENT,
              Row.of(-1L));
          Table tableWithRowFormat =
              catalog.asTableCatalog().loadTable(NameIdentifier.of(databaseName, tableName));
          Assertions.assertEquals(
              HiveStorageConstants.TEXT_INPUT_FORMAT_CLASS,
              tableWithRowFormat.properties().get(HiveConstants.INPUT_FORMAT));
          Assertions.assertEquals(
              "org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat",
              tableWithRowFormat.properties().get(HiveConstants.OUTPUT_FORMAT));
          Assertions.assertEquals(
              customSerde, tableWithRowFormat.properties().get(HiveConstants.SERDE_LIB));
          assertHiveCatalogRead(databaseName, tableName, Row.of("1"));
        },
        true);
  }

  @Test
  public void testStoredAsOverridesRowFormatSerde() {
    String databaseName = "test_hive_stored_as_precedence_db";
    String tableName = "test_stored_as_overrides_row_format";
    String customSerde = HiveStorageConstants.OPENCSV_SERDE_CLASS;

    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          TestUtils.assertTableResult(
              sql(
                  "CREATE TABLE %s (id INT) WITH ('%s'='%s', '%s'='%s', 'connector'='hive')",
                  tableName,
                  Constants.SERDE_LIB_CLASS_NAME,
                  customSerde,
                  Constants.STORED_AS_FILE_FORMAT,
                  "ORC"),
              ResultKind.SUCCESS);
          TestUtils.assertTableResult(
              sql("INSERT INTO %s VALUES (1)", tableName),
              ResultKind.SUCCESS_WITH_CONTENT,
              Row.of(-1L));
          Table tableWithStoredAs =
              catalog.asTableCatalog().loadTable(NameIdentifier.of(databaseName, tableName));
          Assertions.assertEquals(
              HiveStorageConstants.ORC_SERDE_CLASS,
              tableWithStoredAs.properties().get(HiveConstants.SERDE_LIB));
          Assertions.assertEquals(
              HiveStorageConstants.ORC_INPUT_FORMAT_CLASS,
              tableWithStoredAs.properties().get(HiveConstants.INPUT_FORMAT));
          Assertions.assertEquals(
              HiveStorageConstants.ORC_OUTPUT_FORMAT_CLASS,
              tableWithStoredAs.properties().get(HiveConstants.OUTPUT_FORMAT));
          assertHiveCatalogRead(databaseName, tableName, Row.of(1));
        },
        true);
  }

  @Test
  public void testDefaultFormatAndSerdeApplied() {
    String databaseName = "test_hive_default_format_db";
    String tableName = "test_default_format_and_serde";

    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          Optional<Catalog> flinkCatalog = tableEnv.getCatalog(catalog.name());
          Assertions.assertTrue(flinkCatalog.isPresent());
          HiveConf hiveConf = ((GravitinoHiveCatalog) flinkCatalog.get()).getHiveConf();
          TestUtils.assertTableResult(
              sql("CREATE TABLE %s (id INT) WITH ('connector'='hive')", tableName),
              ResultKind.SUCCESS);
          TestUtils.assertTableResult(
              sql("INSERT INTO %s VALUES (1)", tableName),
              ResultKind.SUCCESS_WITH_CONTENT,
              Row.of(-1L));
          Table tableWithDefaults =
              catalog.asTableCatalog().loadTable(NameIdentifier.of(databaseName, tableName));
          StorageFormatFactory storageFormatFactory = new StorageFormatFactory();
          String defaultFormat = hiveConf.getVar(HiveConf.ConfVars.HIVEDEFAULTFILEFORMAT);
          StorageFormatDescriptor descriptor = storageFormatFactory.get(defaultFormat);
          Assertions.assertNotNull(descriptor);
          Assertions.assertEquals(
              descriptor.getInputFormat(),
              tableWithDefaults.properties().get(HiveConstants.INPUT_FORMAT));
          Assertions.assertEquals(
              descriptor.getOutputFormat(),
              tableWithDefaults.properties().get(HiveConstants.OUTPUT_FORMAT));
          String expectedSerde = descriptor.getSerde();
          if (expectedSerde == null && descriptor instanceof RCFileStorageFormatDescriptor) {
            expectedSerde = hiveConf.getVar(HiveConf.ConfVars.HIVEDEFAULTRCFILESERDE);
          }
          if (expectedSerde == null) {
            expectedSerde = hiveConf.getVar(HiveConf.ConfVars.HIVEDEFAULTSERDE);
          }
          Assertions.assertEquals(
              expectedSerde, tableWithDefaults.properties().get(HiveConstants.SERDE_LIB));
          assertHiveCatalogRead(databaseName, tableName, Row.of(1));
        },
        true);
  }

  private void assertHiveCatalogRead(String databaseName, String tableName, Row expectedRow) {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment hiveEnv = TableEnvironment.create(settings);
    try {
      TestUtils.assertTableResult(
          hiveEnv.executeSql(
              String.format(
                  "CREATE CATALOG hive_it WITH ('type'='hive', 'hive-conf-dir'='%s')",
                  hiveConfDir)),
          ResultKind.SUCCESS);
      TestUtils.assertTableResult(hiveEnv.executeSql("USE CATALOG hive_it"), ResultKind.SUCCESS);
      TestUtils.assertTableResult(hiveEnv.executeSql("USE " + databaseName), ResultKind.SUCCESS);
      TableResult result = hiveEnv.executeSql("SELECT * FROM " + tableName);
      List<Row> rows = Lists.newArrayList(result.collect());
      Assertions.assertEquals(1, rows.size());
      Assertions.assertEquals(expectedRow, rows.get(0));
    } finally {
      ((TableEnvironmentImpl) hiveEnv).getCatalogManager().close();
    }
  }

  @Test
  public void testGetHiveTable() {
    Column[] columns =
        new Column[] {
          Column.of("string_type", Types.StringType.get(), "string_type", true, false, null),
          Column.of("double_type", Types.DoubleType.get(), "double_type"),
          Column.of("int_type", Types.IntegerType.get(), "int_type"),
          Column.of("varchar_type", Types.StringType.get(), "varchar_type"),
          Column.of("char_type", Types.FixedCharType.of(1), "char_type"),
          Column.of("boolean_type", Types.BooleanType.get(), "boolean_type"),
          Column.of("byte_type", Types.ByteType.get(), "byte_type"),
          Column.of("binary_type", Types.BinaryType.get(), "binary_type"),
          Column.of("decimal_type", Types.DecimalType.of(10, 2), "decimal_type"),
          Column.of("bigint_type", Types.LongType.get(), "bigint_type"),
          Column.of("float_type", Types.FloatType.get(), "float_type"),
          Column.of("date_type", Types.DateType.get(), "date_type"),
          Column.of("timestamp_type", Types.TimestampType.withoutTimeZone(), "timestamp_type"),
          Column.of("smallint_type", Types.ShortType.get(), "smallint_type"),
          Column.of("fixed_char_type", Types.FixedCharType.of(10), "fixed_char_type"),
          Column.of("array_type", Types.ListType.of(Types.IntegerType.get(), true), "array_type"),
          Column.of(
              "map_type",
              Types.MapType.of(Types.IntegerType.get(), Types.StringType.get(), true),
              "map_type"),
          Column.of(
              "struct_type",
              Types.StructType.of(
                  Types.StructType.Field.nullableField("k1", Types.IntegerType.get()),
                  Types.StructType.Field.nullableField("k2", Types.StringType.get())),
              null)
        };

    String databaseName = "test_get_hive_table_db";
    doWithSchema(
        metalake.loadCatalog(DEFAULT_HIVE_CATALOG),
        databaseName,
        catalog -> {
          String tableName = "test_desc_table";
          String comment = "comment1";
          catalog
              .asTableCatalog()
              .createTable(
                  NameIdentifier.of(databaseName, "test_desc_table"),
                  columns,
                  comment,
                  ImmutableMap.of("k1", "v1"));

          Optional<org.apache.flink.table.catalog.Catalog> flinkCatalog =
              tableEnv.getCatalog(DEFAULT_HIVE_CATALOG);
          Assertions.assertTrue(flinkCatalog.isPresent());
          try {
            CatalogBaseTable table =
                flinkCatalog.get().getTable(new ObjectPath(databaseName, tableName));
            Assertions.assertNotNull(table);
            Assertions.assertEquals(CatalogBaseTable.TableKind.TABLE, table.getTableKind());
            Assertions.assertEquals(comment, table.getComment());

            org.apache.flink.table.catalog.Column[] expected =
                new org.apache.flink.table.catalog.Column[] {
                  org.apache.flink.table.catalog.Column.physical("string_type", DataTypes.STRING())
                      .withComment("string_type"),
                  org.apache.flink.table.catalog.Column.physical("double_type", DataTypes.DOUBLE())
                      .withComment("double_type"),
                  org.apache.flink.table.catalog.Column.physical("int_type", DataTypes.INT())
                      .withComment("int_type"),
                  org.apache.flink.table.catalog.Column.physical(
                          "varchar_type", DataTypes.VARCHAR(Integer.MAX_VALUE))
                      .withComment("varchar_type"),
                  org.apache.flink.table.catalog.Column.physical("char_type", DataTypes.CHAR(1))
                      .withComment("char_type"),
                  org.apache.flink.table.catalog.Column.physical(
                          "boolean_type", DataTypes.BOOLEAN())
                      .withComment("boolean_type"),
                  org.apache.flink.table.catalog.Column.physical("byte_type", DataTypes.TINYINT())
                      .withComment("byte_type"),
                  org.apache.flink.table.catalog.Column.physical("binary_type", DataTypes.BYTES())
                      .withComment("binary_type"),
                  org.apache.flink.table.catalog.Column.physical(
                          "decimal_type", DataTypes.DECIMAL(10, 2))
                      .withComment("decimal_type"),
                  org.apache.flink.table.catalog.Column.physical("bigint_type", DataTypes.BIGINT())
                      .withComment("bigint_type"),
                  org.apache.flink.table.catalog.Column.physical("float_type", DataTypes.FLOAT())
                      .withComment("float_type"),
                  org.apache.flink.table.catalog.Column.physical("date_type", DataTypes.DATE())
                      .withComment("date_type"),
                  org.apache.flink.table.catalog.Column.physical(
                          "timestamp_type", DataTypes.TIMESTAMP())
                      .withComment("timestamp_type"),
                  org.apache.flink.table.catalog.Column.physical(
                          "smallint_type", DataTypes.SMALLINT())
                      .withComment("smallint_type"),
                  org.apache.flink.table.catalog.Column.physical(
                          "fixed_char_type", DataTypes.CHAR(10))
                      .withComment("fixed_char_type"),
                  org.apache.flink.table.catalog.Column.physical(
                          "array_type", DataTypes.ARRAY(DataTypes.INT()))
                      .withComment("array_type"),
                  org.apache.flink.table.catalog.Column.physical(
                          "map_type", DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()))
                      .withComment("map_type"),
                  org.apache.flink.table.catalog.Column.physical(
                      "struct_type",
                      DataTypes.ROW(
                          DataTypes.FIELD("k1", DataTypes.INT()),
                          DataTypes.FIELD("k2", DataTypes.STRING())))
                };
            org.apache.flink.table.catalog.Column[] actual =
                toFlinkPhysicalColumn(table.getUnresolvedSchema().getColumns());
            Assertions.assertArrayEquals(expected, actual);

            CatalogTable catalogTable = (CatalogTable) table;
            Assertions.assertFalse(catalogTable.isPartitioned());
          } catch (TableNotExistException e) {
            Assertions.fail(e);
          }
        },
        true);
  }

  @Test
  public void testGravitinoCreateRawHiveTableReadableByNativeHiveCatalog() {
    String databaseName = "test_native_read_raw_hive_db";
    String tableName = "raw_hive_table";
    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          TestUtils.assertTableResult(
              sql("CREATE TABLE %s (id INT, name STRING) WITH ('connector'='hive')", tableName),
              ResultKind.SUCCESS);
          TestUtils.assertTableResult(
              sql("ALTER TABLE %s ADD age INT", tableName), ResultKind.SUCCESS);
          TestUtils.assertTableResult(
              sql("INSERT INTO %s VALUES (1, 'a', 10)", tableName),
              ResultKind.SUCCESS_WITH_CONTENT,
              Row.of(-1L));
          withNativeHiveEnv(
              databaseName, env -> assertSingleRow(env, tableName, Row.of(1, "a", 10)));
        },
        true);
  }

  @Test
  public void testGravitinoCreateGenericJdbcTableReadableByNativeHiveCatalog() {
    String databaseName = "test_native_read_jdbc_db";
    String tableName = "generic_jdbc_table";
    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          createJdbcTable(tableName);
          try {
            TestUtils.assertTableResult(
                sql(
                    "CREATE TABLE %s (id INT, name STRING) WITH ("
                        + "'connector'='jdbc',"
                        + "'url'='%s',"
                        + "'table-name'='%s',"
                        + "'username'='%s',"
                        + "'password'='%s',"
                        + "'driver'='%s')",
                    tableName, mysqlUrl, tableName, mysqlUsername, mysqlPassword, mysqlDriver),
                ResultKind.SUCCESS);
            TestUtils.assertTableResult(
                sql("ALTER TABLE %s ADD age INT", tableName), ResultKind.SUCCESS);
            alterJdbcTableAddColumn(tableName);
            TestUtils.assertTableResult(
                sql("INSERT INTO %s VALUES (1, 'a', 10)", tableName),
                ResultKind.SUCCESS_WITH_CONTENT,
                Row.of(-1L));
            withNativeHiveEnv(
                databaseName, env -> assertSingleRow(env, tableName, Row.of(1, "a", 10)));
          } finally {
            dropJdbcTable(tableName);
          }
        },
        true);
  }

  @Test
  public void testGravitinoCreatePaimonTableReadableByNativePaimonCatalog() {
    String databaseName = "test_native_read_paimon_db";
    String tableName = "paimon_table";
    String tablePath = paimonTablePath(databaseName, tableName);
    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          TestUtils.assertTableResult(
              sql(
                  "CREATE TABLE %s (id INT, name STRING) WITH ("
                      + "'connector'='paimon',"
                      + "'path'='%s')",
                  tableName, tablePath),
              ResultKind.SUCCESS);
          withNativePaimonEnv(
              databaseName,
              env -> {
                TestUtils.assertTableResult(
                    env.executeSql(
                        String.format("CREATE TABLE %s (id INT, name STRING)", tableName)),
                    ResultKind.SUCCESS);
                TestUtils.assertTableResult(
                    env.executeSql(String.format("ALTER TABLE %s ADD age INT", tableName)),
                    ResultKind.SUCCESS);
              });
          TestUtils.assertTableResult(
              sql("ALTER TABLE %s ADD age INT", tableName), ResultKind.SUCCESS);
          TestUtils.assertTableResult(
              sql("INSERT INTO %s VALUES (1, 'a', 10)", tableName),
              ResultKind.SUCCESS_WITH_CONTENT,
              Row.of(-1L));
          withNativePaimonEnv(
              databaseName, env -> assertSingleRow(env, tableName, Row.of(1, "a", 10)));
        },
        true);
  }

  @Test
  public void testNativeCreateRawHiveTableReadableByGravitino() {
    String databaseName = "test_gravitino_read_raw_hive_db";
    String tableName = "raw_hive_table";
    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          withNativeHiveEnv(
              databaseName,
              env -> {
                TestUtils.assertTableResult(
                    env.executeSql(
                        String.format(
                            "CREATE TABLE %s (id INT, name STRING) WITH ('connector'='hive')",
                            tableName)),
                    ResultKind.SUCCESS);
                TestUtils.assertTableResult(
                    env.executeSql(String.format("ALTER TABLE %s ADD age INT", tableName)),
                    ResultKind.SUCCESS);
                TestUtils.assertTableResult(
                    env.executeSql(String.format("INSERT INTO %s VALUES (1, 'a', 10)", tableName)),
                    ResultKind.SUCCESS_WITH_CONTENT,
                    Row.of(-1L));
              });
          assertSingleRow(tableEnv, tableName, Row.of(1, "a", 10));
        },
        true);
  }

  @Test
  public void testNativeCreateGenericJdbcTableReadableByGravitino() {
    String databaseName = "test_gravitino_read_jdbc_db";
    String tableName = "generic_jdbc_table";
    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          createJdbcTable(tableName);
          try {
            withNativeHiveEnv(
                databaseName,
                env -> {
                  TestUtils.assertTableResult(
                      env.executeSql(
                          String.format(
                              "CREATE TABLE %s (id INT, name STRING) WITH ("
                                  + "'connector'='jdbc',"
                                  + "'url'='%s',"
                                  + "'table-name'='%s',"
                                  + "'username'='%s',"
                                  + "'password'='%s',"
                                  + "'driver'='%s')",
                              tableName,
                              mysqlUrl,
                              tableName,
                              mysqlUsername,
                              mysqlPassword,
                              mysqlDriver)),
                      ResultKind.SUCCESS);
                  TestUtils.assertTableResult(
                      env.executeSql(String.format("ALTER TABLE %s ADD age INT", tableName)),
                      ResultKind.SUCCESS);
                  alterJdbcTableAddColumn(tableName);
                  TestUtils.assertTableResult(
                      env.executeSql(
                          String.format("INSERT INTO %s VALUES (1, 'a', 10)", tableName)),
                      ResultKind.SUCCESS_WITH_CONTENT,
                      Row.of(-1L));
                });
            assertSingleRow(tableEnv, tableName, Row.of(1, "a", 10));
          } finally {
            dropJdbcTable(tableName);
          }
        },
        true);
  }

  @Test
  public void testNativeCreatePaimonTableReadableByGravitino() {
    String databaseName = "test_gravitino_read_paimon_db";
    String tableName = "paimon_table";
    String tablePath = paimonTablePath(databaseName, tableName);
    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          withNativePaimonEnv(
              databaseName,
              env -> {
                TestUtils.assertTableResult(
                    env.executeSql(
                        String.format("CREATE TABLE %s (id INT, name STRING)", tableName)),
                    ResultKind.SUCCESS);
                TestUtils.assertTableResult(
                    env.executeSql(String.format("ALTER TABLE %s ADD age INT", tableName)),
                    ResultKind.SUCCESS);
                TestUtils.assertTableResult(
                    env.executeSql(String.format("INSERT INTO %s VALUES (1, 'a', 10)", tableName)),
                    ResultKind.SUCCESS_WITH_CONTENT,
                    Row.of(-1L));
              });
          TestUtils.assertTableResult(
              sql(
                  "CREATE TABLE %s (id INT, name STRING, age INT) WITH ("
                      + "'connector'='paimon',"
                      + "'path'='%s')",
                  tableName, tablePath),
              ResultKind.SUCCESS);
          assertSingleRow(tableEnv, tableName, Row.of(1, "a", 10));
        },
        true);
  }

  @Test
  @Override
  public void testCreateSimpleTable() {
    String databaseName = "test_create_no_partition_table_db";
    String tableName = "test_create_no_partition_table";
    String comment = "test comment";

    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          TableResult result =
              sql(
                  "CREATE TABLE %s "
                      + "(string_type STRING COMMENT 'string_type', "
                      + " double_type DOUBLE COMMENT 'double_type')"
                      + " COMMENT '%s' WITH ('connector'='hive')",
                  tableName, comment);
          TestUtils.assertTableResult(result, ResultKind.SUCCESS);

          Optional<Catalog> flinkCatalog = tableEnv.getCatalog(currentCatalog().name());
          Assertions.assertTrue(flinkCatalog.isPresent());
          try {
            CatalogBaseTable table =
                flinkCatalog.get().getTable(new ObjectPath(databaseName, tableName));
            Assertions.assertNotNull(table);
            Assertions.assertEquals(comment, table.getComment());

            CatalogTable catalogTable = (CatalogTable) table;
            Assertions.assertFalse(catalogTable.isPartitioned());
          } catch (TableNotExistException e) {
            Assertions.fail(e);
          }

          assertFlinkTableColumns(databaseName, tableName, "string_type", "double_type");

          TestUtils.assertTableResult(
              sql("INSERT INTO %s VALUES ('A', 1.0), ('B', 2.0)", tableName),
              ResultKind.SUCCESS_WITH_CONTENT,
              Row.of(-1L));
          TableResult selectResult = sql("SELECT * FROM %s ORDER BY string_type", tableName);
          Assertions.assertEquals(ResultKind.SUCCESS_WITH_CONTENT, selectResult.getResultKind());
          List<Row> actualRows = Lists.newArrayList(selectResult.collect());
          Assertions.assertEquals(2, actualRows.size());
          Assertions.assertTrue(hasRow(actualRows, "A", 1.0));
          Assertions.assertTrue(hasRow(actualRows, "B", 2.0));
        },
        true,
        supportDropCascade());
  }

  @Test
  public void testAlterRawHiveTableAddColumn() {
    String databaseName = "test_alter_raw_hive_table_db";
    String tableName = "test_alter_raw_hive_table";
    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          TestUtils.assertTableResult(
              sql("CREATE TABLE %s (id INT, name STRING) WITH ('connector'='hive')", tableName),
              ResultKind.SUCCESS);
          TestUtils.assertTableResult(
              sql("ALTER TABLE %s ADD age INT", tableName), ResultKind.SUCCESS);
          Column[] columns =
              catalog
                  .asTableCatalog()
                  .loadTable(NameIdentifier.of(databaseName, tableName))
                  .columns();
          Assertions.assertEquals(3, columns.length);
        },
        true);
  }

  @Test
  public void testAlterRawHiveTableRenameColumn() {
    String databaseName = "test_alter_raw_hive_table_rename_db";
    String tableName = "test_alter_raw_hive_table_rename";
    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          TestUtils.assertTableResult(
              sql("CREATE TABLE %s (id INT, name STRING) WITH ('connector'='hive')", tableName),
              ResultKind.SUCCESS);
          TestUtils.assertTableResult(
              sql("ALTER TABLE %s RENAME id TO id_new", tableName), ResultKind.SUCCESS);
          Column[] columns =
              catalog
                  .asTableCatalog()
                  .loadTable(NameIdentifier.of(databaseName, tableName))
                  .columns();
          Column[] expected =
              new Column[] {
                Column.of("id_new", Types.IntegerType.get(), null),
                Column.of("name", Types.StringType.get(), null)
              };
          assertColumns(expected, columns);
        },
        true);
  }

  @Test
  public void testAlterRawHiveTableDropColumn() {
    String databaseName = "test_alter_raw_hive_table_drop_db";
    String tableName = "test_alter_raw_hive_table_drop";
    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          TestUtils.assertTableResult(
              sql("CREATE TABLE %s (id INT, name STRING) WITH ('connector'='hive')", tableName),
              ResultKind.SUCCESS);
          TestUtils.assertTableResult(
              sql("ALTER TABLE %s DROP name", tableName), ResultKind.SUCCESS);
          Column[] columns =
              catalog
                  .asTableCatalog()
                  .loadTable(NameIdentifier.of(databaseName, tableName))
                  .columns();
          Column[] expected = new Column[] {Column.of("id", Types.IntegerType.get(), null)};
          assertColumns(expected, columns);
        },
        true);
  }

  @Test
  public void testAlterGenericTableAddColumn() {
    String databaseName = "test_alter_generic_table_db";
    String tableName = "test_alter_generic_table";
    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          TestUtils.assertTableResult(
              sql("CREATE TABLE %s (id INT, name STRING)", tableName), ResultKind.SUCCESS);
          TestUtils.assertTableResult(
              sql("ALTER TABLE %s ADD age INT", tableName), ResultKind.SUCCESS);
          assertFlinkTableColumns(databaseName, tableName, "id", "name", "age");
        },
        true);
  }

  @Test
  public void testAlterGenericTableRenameColumn() {
    String databaseName = "test_alter_generic_table_rename_db";
    String tableName = "test_alter_generic_table_rename";
    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          TestUtils.assertTableResult(
              sql("CREATE TABLE %s (id INT, name STRING)", tableName), ResultKind.SUCCESS);
          TestUtils.assertTableResult(
              sql("ALTER TABLE %s RENAME id TO id_new", tableName), ResultKind.SUCCESS);
          assertFlinkTableColumns(databaseName, tableName, "id_new", "name");
        },
        true);
  }

  @Test
  public void testAlterGenericTableDropColumn() {
    String databaseName = "test_alter_generic_table_drop_db";
    String tableName = "test_alter_generic_table_drop";
    doWithSchema(
        currentCatalog(),
        databaseName,
        catalog -> {
          TestUtils.assertTableResult(
              sql("CREATE TABLE %s (id INT, name STRING)", tableName), ResultKind.SUCCESS);
          TestUtils.assertTableResult(
              sql("ALTER TABLE %s DROP name", tableName), ResultKind.SUCCESS);
          assertFlinkTableColumns(databaseName, tableName, "id");
        },
        true);
  }

  private void assertFlinkTableColumns(
      String databaseName, String tableName, String... expectedColumnNames) {
    Optional<Catalog> flinkCatalog = tableEnv.getCatalog(currentCatalog().name());
    Assertions.assertTrue(flinkCatalog.isPresent());
    ObjectPath tablePath = new ObjectPath(databaseName, tableName);
    try {
      CatalogTable table = (CatalogTable) flinkCatalog.get().getTable(tablePath);
      List<String> actual =
          table.getUnresolvedSchema().getColumns().stream()
              .map(column -> column.getName())
              .collect(Collectors.toList());
      Assertions.assertEquals(Arrays.asList(expectedColumnNames), actual);
    } catch (TableNotExistException e) {
      Assertions.fail(e);
    }
  }

  private boolean hasRow(List<Row> rows, String name, double value) {
    return rows.stream()
        .anyMatch(
            row ->
                row.toString().contains("[" + name + ",")
                    && row.toString().contains(String.valueOf(value)));
  }

  private void withNativeHiveEnv(
      String databaseName, java.util.function.Consumer<TableEnvironment> action) {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment nativeEnv = TableEnvironment.create(settings);
    try {
      TestUtils.assertTableResult(
          nativeEnv.executeSql(
              String.format(
                  "CREATE CATALOG native_hive WITH ('type'='hive', 'hive-conf-dir'='%s')",
                  hiveConfDir)),
          ResultKind.SUCCESS);
      TestUtils.assertTableResult(
          nativeEnv.executeSql("USE CATALOG native_hive"), ResultKind.SUCCESS);
      if (databaseName != null) {
        TestUtils.assertTableResult(
            nativeEnv.executeSql(String.format("CREATE DATABASE IF NOT EXISTS %s", databaseName)),
            ResultKind.SUCCESS);
        TestUtils.assertTableResult(
            nativeEnv.executeSql("USE " + databaseName), ResultKind.SUCCESS);
      }
      action.accept(nativeEnv);
    } finally {
      ((TableEnvironmentImpl) nativeEnv).getCatalogManager().close();
    }
  }

  private void withNativePaimonEnv(
      String databaseName, java.util.function.Consumer<TableEnvironment> action) {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment nativeEnv = TableEnvironment.create(settings);
    try {
      TestUtils.assertTableResult(
          nativeEnv.executeSql(
              String.format(
                  "CREATE CATALOG native_paimon WITH ('type'='paimon', 'warehouse'='%s')",
                  warehouse)),
          ResultKind.SUCCESS);
      TestUtils.assertTableResult(
          nativeEnv.executeSql("USE CATALOG native_paimon"), ResultKind.SUCCESS);
      if (databaseName != null) {
        TestUtils.assertTableResult(
            nativeEnv.executeSql(String.format("CREATE DATABASE IF NOT EXISTS %s", databaseName)),
            ResultKind.SUCCESS);
        TestUtils.assertTableResult(
            nativeEnv.executeSql("USE " + databaseName), ResultKind.SUCCESS);
      }
      action.accept(nativeEnv);
    } finally {
      ((TableEnvironmentImpl) nativeEnv).getCatalogManager().close();
    }
  }

  private void assertSingleRow(TableEnvironment env, String tableName, Row expectedRow) {
    TableResult result = env.executeSql("SELECT * FROM " + tableName);
    List<Row> rows = Lists.newArrayList(result.collect());
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(expectedRow, rows.get(0));
  }

  private void createJdbcTable(String tableName) {
    try (Connection connection =
            DriverManager.getConnection(mysqlUrl, mysqlUsername, mysqlPassword);
        Statement statement = connection.createStatement()) {
      statement.execute(String.format("CREATE DATABASE IF NOT EXISTS %s", MYSQL_DATABASE));
      statement.execute(String.format("DROP TABLE IF EXISTS %s.%s", MYSQL_DATABASE, tableName));
      statement.execute(
          String.format(
              "CREATE TABLE %s.%s (id INT, name VARCHAR(32))", MYSQL_DATABASE, tableName));
    } catch (SQLException e) {
      throw new RuntimeException("Failed to create JDBC table " + tableName, e);
    }
  }

  private void alterJdbcTableAddColumn(String tableName) {
    try (Connection connection =
            DriverManager.getConnection(mysqlUrl, mysqlUsername, mysqlPassword);
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format("ALTER TABLE %s.%s ADD COLUMN age INT", MYSQL_DATABASE, tableName));
    } catch (SQLException e) {
      throw new RuntimeException("Failed to alter JDBC table " + tableName, e);
    }
  }

  private String paimonTablePath(String databaseName, String tableName) {
    return String.format("%s/%s.db/%s", warehouse, databaseName, tableName);
  }

  private void dropJdbcTable(String tableName) {
    try (Connection connection =
            DriverManager.getConnection(mysqlUrl, mysqlUsername, mysqlPassword);
        Statement statement = connection.createStatement()) {
      statement.execute(String.format("DROP TABLE IF EXISTS %s.%s", MYSQL_DATABASE, tableName));
    } catch (SQLException e) {
      throw new RuntimeException("Failed to drop JDBC table " + tableName, e);
    }
  }

  @Override
  protected Map<String, String> getCreateSchemaProps(String schemaName) {
    return ImmutableMap.of("location", warehouse + "/" + schemaName);
  }

  @Override
  protected org.apache.gravitino.Catalog currentCatalog() {
    return hiveCatalog;
  }

  @Override
  protected String getProvider() {
    return "hive";
  }

  @Override
  protected boolean supportDropCascade() {
    return true;
  }

  @Override
  protected void initFlinkEnv() {
    try {
      UserGroupInformation proxyUser =
          UserGroupInformation.createProxyUser(
              FLINK_USER_NAME, UserGroupInformation.getCurrentUser());
      proxyUser.doAs(
          (PrivilegedAction<Void>)
              () -> {
                super.initFlinkEnv();
                return null;
              });
    } catch (IOException e) {
      throw new RuntimeException("Failed to obtain UGI for Flink user", e);
    }
  }
}
