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
package org.apache.gravitino.catalog.lakehouse.hudi.integration.test;

import static org.apache.gravitino.Catalog.PROPERTY_IN_USE;
import static org.apache.gravitino.catalog.lakehouse.hudi.HudiCatalogPropertiesMetadata.CATALOG_BACKEND;
import static org.apache.gravitino.catalog.lakehouse.hudi.HudiCatalogPropertiesMetadata.URI;
import static org.apache.gravitino.catalog.lakehouse.hudi.HudiSchemaPropertiesMetadata.LOCATION;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.utils.RandomNameUtils;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class HudiCatalogHMSIT extends BaseIT {
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  private static String hmsURI;
  private static SparkSession sparkSession;
  private static GravitinoMetalake metalake;
  private static Catalog catalog;
  private static final String METALAKE_NAME = RandomNameUtils.genRandomName("hudi_metalake");
  private static final String CATALOG_NAME = RandomNameUtils.genRandomName("hudi_catalog");
  private static final String DB_NAME = RandomNameUtils.genRandomName("hudi_schema");
  private static final String DB_LOCATION = "/user/hive/warehouse-catalog-hudi/" + DB_NAME;
  private static final String DATA_TABLE_NAME = RandomNameUtils.genRandomName("hudi_data_table");
  private static final String COW_TABLE = RandomNameUtils.genRandomName("hudi_cow_table");
  private static final String MOR_TABLE = RandomNameUtils.genRandomName("hudi_mor_table");

  @BeforeAll
  public void prepare() {
    containerSuite.startHiveContainer();
    hmsURI =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);

    createHudiTables();

    metalake =
        client.createMetalake(METALAKE_NAME, "metalake for hudi catalog IT", ImmutableMap.of());
    catalog =
        metalake.createCatalog(
            CATALOG_NAME,
            Catalog.Type.RELATIONAL,
            "lakehouse-hudi",
            "hudi catalog for hms",
            ImmutableMap.of(CATALOG_BACKEND, "hms", URI, hmsURI));
  }

  @Test
  public void testCatalog() {
    String catalogName = RandomNameUtils.genRandomName("hudi_catalog");
    String comment = "hudi catalog for hms";
    // test create exception
    Exception exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                metalake.createCatalog(
                    catalogName,
                    Catalog.Type.RELATIONAL,
                    "lakehouse-hudi",
                    comment,
                    ImmutableMap.of()));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("Properties are required and must be set: [catalog-backend, uri]"),
        "Unexpected exception message: " + exception.getMessage());

    // test testConnection exception
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                metalake.testConnection(
                    catalogName,
                    Catalog.Type.RELATIONAL,
                    "lakehouse-hudi",
                    comment,
                    ImmutableMap.of()));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("Properties are required and must be set: [catalog-backend, uri]"),
        "Unexpected exception message: " + exception.getMessage());

    // test testConnection
    ImmutableMap<String, String> properties = ImmutableMap.of(CATALOG_BACKEND, "hms", URI, hmsURI);
    Assertions.assertDoesNotThrow(
        () ->
            metalake.testConnection(
                catalogName, Catalog.Type.RELATIONAL, "lakehouse-hudi", comment, properties));

    // test create and load
    metalake.createCatalog(
        catalogName, Catalog.Type.RELATIONAL, "lakehouse-hudi", comment, properties);
    Catalog catalog = metalake.loadCatalog(catalogName);
    Assertions.assertEquals(catalogName, catalog.name());
    Assertions.assertEquals(Catalog.Type.RELATIONAL, catalog.type());
    Assertions.assertEquals("lakehouse-hudi", catalog.provider());
    Assertions.assertEquals(comment, catalog.comment());
    Map<String, String> expectedProperties = new HashMap<>(properties);
    expectedProperties.put(PROPERTY_IN_USE, "true");
    Assertions.assertEquals(expectedProperties, catalog.properties());

    // test list
    String[] catalogs = metalake.listCatalogs();
    Assertions.assertTrue(Arrays.asList(catalogs).contains(catalogName));
  }

  @Test
  public void testSchema() {
    SupportsSchemas schemaOps = catalog.asSchemas();
    String schemaName = RandomNameUtils.genRandomName("hudi_schema");
    // test create
    Exception exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> schemaOps.createSchema(schemaName, null, ImmutableMap.of()));
    Assertions.assertTrue(
        exception.getMessage().contains("Not implemented yet"),
        "Unexpected exception message: " + exception.getMessage());

    // test alter
    exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> schemaOps.alterSchema(schemaName, SchemaChange.removeProperty("test")));
    Assertions.assertTrue(
        exception.getMessage().contains("Not implemented yet"),
        "Unexpected exception message: " + exception.getMessage());

    // test list
    String[] schemas = schemaOps.listSchemas();
    Assertions.assertTrue(Arrays.asList(schemas).contains(DB_NAME));

    // test load
    Schema schema = schemaOps.loadSchema(DB_NAME);
    Assertions.assertEquals(DB_NAME, schema.name());
    Assertions.assertEquals("", schema.comment());
    Assertions.assertTrue(schema.properties().get(LOCATION).endsWith(DB_NAME));
  }

  @Test
  public void testTable() {
    TableCatalog tableOps = catalog.asTableCatalog();
    String tableName = RandomNameUtils.genRandomName("hudi_table");
    NameIdentifier tableIdent = NameIdentifier.of(DB_NAME, tableName);

    // test create
    Exception exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () ->
                tableOps.createTable(
                    tableIdent,
                    new Column[] {Column.of("col1", Types.StringType.get())},
                    null,
                    null));
    Assertions.assertTrue(
        exception.getMessage().contains("Not implemented yet"),
        "Unexpected exception message: " + exception.getMessage());

    // test alter
    exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> tableOps.alterTable(tableIdent, TableChange.updateComment("new comment")));
    Assertions.assertTrue(
        exception.getMessage().contains("Not implemented yet"),
        "Unexpected exception message: " + exception.getMessage());

    // test list
    NameIdentifier[] tables = tableOps.listTables(Namespace.of(DB_NAME));
    List<String> tableNames =
        Arrays.stream(tables).map(NameIdentifier::name).collect(Collectors.toList());

    Assertions.assertTrue(tableNames.contains(DATA_TABLE_NAME));

    Assertions.assertTrue(tableNames.contains(COW_TABLE));
    Assertions.assertFalse(tableNames.contains(COW_TABLE + "_rt"));
    Assertions.assertFalse(tableNames.contains(COW_TABLE + "_ro"));

    Assertions.assertTrue(tableNames.contains(MOR_TABLE));
    Assertions.assertTrue(tableNames.contains(MOR_TABLE + "_rt"));
    Assertions.assertTrue(tableNames.contains(MOR_TABLE + "_ro"));

    // test load
    Table table = tableOps.loadTable(NameIdentifier.of(DB_NAME, COW_TABLE));
    Assertions.assertEquals(COW_TABLE, table.name());
    assertTable(table);

    table = tableOps.loadTable(NameIdentifier.of(DB_NAME, MOR_TABLE));
    Assertions.assertEquals(MOR_TABLE, table.name());
    assertTable(table);

    table = tableOps.loadTable(NameIdentifier.of(DB_NAME, MOR_TABLE + "_rt"));
    Assertions.assertEquals(MOR_TABLE + "_rt", table.name());
    assertTable(table);

    table = tableOps.loadTable(NameIdentifier.of(DB_NAME, MOR_TABLE + "_ro"));
    Assertions.assertEquals(MOR_TABLE + "_ro", table.name());
    assertTable(table);
  }

  private void assertTable(Table table) {
    Assertions.assertNull(table.comment());
    assertColumns(table);
    assertProperties(table);
    assertPartitioning(table.partitioning());
    Assertions.assertEquals(Distributions.NONE, table.distribution());
    Assertions.assertEquals(SortOrders.NONE, table.sortOrder());
    Assertions.assertEquals(Indexes.EMPTY_INDEXES, table.index());
  }

  private void assertPartitioning(Transform[] partitioning) {
    Assertions.assertEquals(1, partitioning.length);
    Assertions.assertEquals(Transforms.identity("city"), partitioning[0]);
  }

  private void assertProperties(Table table) {
    Map<String, String> properties = table.properties();
    Assertions.assertTrue(properties.containsKey("last_commit_time_sync"));
    Assertions.assertTrue(properties.containsKey("last_commit_completion_time_sync"));
    Assertions.assertTrue(properties.containsKey("transient_lastDdlTime"));
    Assertions.assertTrue(properties.containsKey("spark.sql.sources.schema.numParts"));
    Assertions.assertTrue(properties.containsKey("spark.sql.sources.schema.part.0"));
    Assertions.assertTrue(properties.containsKey("spark.sql.sources.schema.partCol.0"));
    Assertions.assertTrue(properties.containsKey("spark.sql.sources.schema.numPartCols"));
    Assertions.assertTrue(properties.containsKey("spark.sql.sources.provider"));
    Assertions.assertTrue(properties.containsKey("spark.sql.create.version"));

    if (table.name().endsWith("_rt") || table.name().endsWith("_ro")) {
      Assertions.assertEquals("TRUE", properties.get("EXTERNAL"));
    } else {
      Assertions.assertTrue(properties.containsKey("type"));
      Assertions.assertTrue(properties.containsKey("provider"));
    }
  }

  private void assertColumns(Table table) {
    Column[] columns = table.columns();
    Assertions.assertEquals(11, columns.length);
    if (table.name().endsWith("_rt") || table.name().endsWith("_ro")) {
      assertColumn(
          ColumnDTO.builder()
              .withName("_hoodie_commit_time")
              .withDataType(Types.StringType.get())
              .withComment("")
              .build(),
          columns[0]);
      assertColumn(
          ColumnDTO.builder()
              .withName("_hoodie_commit_seqno")
              .withDataType(Types.StringType.get())
              .withComment("")
              .build(),
          columns[1]);
      assertColumn(
          ColumnDTO.builder()
              .withName("_hoodie_record_key")
              .withDataType(Types.StringType.get())
              .withComment("")
              .build(),
          columns[2]);
      assertColumn(
          ColumnDTO.builder()
              .withName("_hoodie_partition_path")
              .withDataType(Types.StringType.get())
              .withComment("")
              .build(),
          columns[3]);
      assertColumn(
          ColumnDTO.builder()
              .withName("_hoodie_file_name")
              .withDataType(Types.StringType.get())
              .withComment("")
              .build(),
          columns[4]);
      assertColumn(
          ColumnDTO.builder()
              .withName("ts")
              .withDataType(Types.LongType.get())
              .withComment("")
              .build(),
          columns[5]);
      assertColumn(
          ColumnDTO.builder()
              .withName("uuid")
              .withDataType(Types.StringType.get())
              .withComment("")
              .build(),
          columns[6]);
      assertColumn(
          ColumnDTO.builder()
              .withName("rider")
              .withDataType(Types.StringType.get())
              .withComment("")
              .build(),
          columns[7]);
      assertColumn(
          ColumnDTO.builder()
              .withName("driver")
              .withDataType(Types.StringType.get())
              .withComment("")
              .build(),
          columns[8]);
      assertColumn(
          ColumnDTO.builder()
              .withName("fare")
              .withDataType(Types.DoubleType.get())
              .withComment("")
              .build(),
          columns[9]);
      assertColumn(
          ColumnDTO.builder()
              .withName("city")
              .withDataType(Types.StringType.get())
              .withComment("")
              .build(),
          columns[10]);
    } else {
      assertColumn(
          ColumnDTO.builder()
              .withName("_hoodie_commit_time")
              .withDataType(Types.StringType.get())
              .build(),
          columns[0]);
      assertColumn(
          ColumnDTO.builder()
              .withName("_hoodie_commit_seqno")
              .withDataType(Types.StringType.get())
              .build(),
          columns[1]);
      assertColumn(
          ColumnDTO.builder()
              .withName("_hoodie_record_key")
              .withDataType(Types.StringType.get())
              .build(),
          columns[2]);
      assertColumn(
          ColumnDTO.builder()
              .withName("_hoodie_partition_path")
              .withDataType(Types.StringType.get())
              .build(),
          columns[3]);
      assertColumn(
          ColumnDTO.builder()
              .withName("_hoodie_file_name")
              .withDataType(Types.StringType.get())
              .build(),
          columns[4]);
      assertColumn(
          ColumnDTO.builder().withName("ts").withDataType(Types.LongType.get()).build(),
          columns[5]);
      assertColumn(
          ColumnDTO.builder().withName("uuid").withDataType(Types.StringType.get()).build(),
          columns[6]);
      assertColumn(
          ColumnDTO.builder().withName("rider").withDataType(Types.StringType.get()).build(),
          columns[7]);
      assertColumn(
          ColumnDTO.builder().withName("driver").withDataType(Types.StringType.get()).build(),
          columns[8]);
      assertColumn(
          ColumnDTO.builder().withName("fare").withDataType(Types.DoubleType.get()).build(),
          columns[9]);
      assertColumn(
          ColumnDTO.builder().withName("city").withDataType(Types.StringType.get()).build(),
          columns[10]);
    }
  }

  private void assertColumn(ColumnDTO columnDTO, Column column) {
    Assertions.assertEquals(columnDTO.name(), column.name());
    Assertions.assertEquals(columnDTO.dataType(), column.dataType());
    Assertions.assertEquals(columnDTO.comment(), column.comment());
    Assertions.assertEquals(columnDTO.nullable(), column.nullable());
    Assertions.assertEquals(columnDTO.autoIncrement(), column.autoIncrement());
    Assertions.assertEquals(columnDTO.defaultValue(), column.defaultValue());
  }

  private static void createHudiTables() {
    sparkSession =
        SparkSession.builder()
            .master("local[1]")
            .appName("Hudi Catalog integration test")
            .config("hive.metastore.uris", hmsURI)
            .config(
                "spark.sql.warehouse.dir",
                String.format(
                    "hdfs://%s:%d/user/hive/warehouse-catalog-hudi",
                    containerSuite.getHiveContainer().getContainerIpAddress(),
                    HiveContainer.HDFS_DEFAULTFS_PORT))
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
            .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
            .config("dfs.replication", "1")
            .enableHiveSupport()
            .getOrCreate();

    sparkSession.sql(
        String.format("CREATE DATABASE IF NOT EXISTS %s LOCATION '%s'", DB_NAME, DB_LOCATION));

    sparkSession.sql(
        String.format(
            "CREATE TABLE %s.%s (\n"
                + "    ts BIGINT,\n"
                + "    uuid STRING,\n"
                + "    rider STRING,\n"
                + "    driver STRING,\n"
                + "    fare DOUBLE,\n"
                + "    city STRING\n"
                + ") USING HUDI TBLPROPERTIES (type = 'cow') \n"
                + "PARTITIONED BY (city)",
            DB_NAME, COW_TABLE));
    sparkSession.sql(
        String.format(
            "INSERT INTO %s.%s\n"
                + "VALUES\n"
                + "(1695115999911,'c8abbe79-8d89-47ea-b4ce-4d224bae5bfa','rider-J','driver-T',17.85,'chennai')",
            DB_NAME, COW_TABLE));

    sparkSession.sql(
        String.format(
            "CREATE TABLE %s.%s (\n"
                + "    ts BIGINT,\n"
                + "    uuid STRING,\n"
                + "    rider STRING,\n"
                + "    driver STRING,\n"
                + "    fare DOUBLE,\n"
                + "    city STRING\n"
                + ") USING HUDI TBLPROPERTIES (type = 'mor') \n"
                + "PARTITIONED BY (city)",
            DB_NAME, MOR_TABLE));
    sparkSession.sql(
        String.format(
            "INSERT INTO %s.%s\n"
                + "VALUES\n"
                + "(1695115999911,'c8abbe79-8d89-47ea-b4ce-4d224bae5bfa','rider-J','driver-T',17.85,'chennai')",
            DB_NAME, MOR_TABLE));

    sparkSession.sql(
        String.format(
            "CREATE TABLE %s.%s (\n"
                + "    ts BIGINT,\n"
                + "    uuid STRING,\n"
                + "    rider STRING,\n"
                + "    driver STRING,\n"
                + "    fare DOUBLE,\n"
                + "    city STRING\n"
                + ") USING HUDI\n"
                + "PARTITIONED BY (city)",
            DB_NAME, DATA_TABLE_NAME));

    sparkSession.sql(
        String.format(
            "INSERT INTO %s.%s\n"
                + "VALUES\n"
                + "(1695159649087,'334e26e9-8355-45cc-97c6-c31daf0df330','rider-A','driver-K',19.10,'san_francisco'),\n"
                + "(1695115999911,'c8abbe79-8d89-47ea-b4ce-4d224bae5bfa','rider-J','driver-T',17.85,'chennai')",
            DB_NAME, DATA_TABLE_NAME));
  }
}
