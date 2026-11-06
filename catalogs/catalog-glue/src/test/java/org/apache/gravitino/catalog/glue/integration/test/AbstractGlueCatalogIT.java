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
package org.apache.gravitino.catalog.glue.integration.test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.catalog.glue.GlueCatalogOperations;
import org.apache.gravitino.catalog.glue.GlueClientProvider;
import org.apache.gravitino.catalog.glue.GlueConstants;
import org.apache.gravitino.catalog.hive.HiveStorageConstants;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.TableInput;

/**
 * Abstract base class for Glue catalog integration tests.
 *
 * <p>Subclasses provide backend-specific configuration via {@link #catalogConfig()}. All test
 * scenarios are defined here and shared between backend implementations.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractGlueCatalogIT {

  protected GlueCatalogOperations ops;
  private String currentSchema;
  private GlueClient rawGlueClient;

  private static final Namespace SCHEMA_NS = Namespace.of("ml", "cat");

  protected abstract Map<String, String> catalogConfig();

  @BeforeAll
  void initOps() {
    ops = new GlueCatalogOperations();
    ops.initialize(catalogConfig(), null, null);
  }

  @AfterAll
  void closeOps() throws Exception {
    if (rawGlueClient != null) {
      rawGlueClient.close();
    }
    if (ops != null) {
      ops.close();
    }
  }

  @AfterEach
  void cleanupSchema() {
    if (currentSchema != null) {
      try {
        ops.dropSchema(schemaIdent(currentSchema), true);
      } catch (Exception ignored) {
      }
      currentSchema = null;
    }
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private String newSchema() {
    String name = "glue_it_" + System.nanoTime();
    currentSchema = name;
    return name;
  }

  private NameIdentifier schemaIdent(String name) {
    return NameIdentifier.of("ml", "cat", name);
  }

  private Namespace tableNs(String schema) {
    return Namespace.of("ml", "cat", schema);
  }

  private NameIdentifier tableIdent(String schema, String table) {
    return NameIdentifier.of("ml", "cat", schema, table);
  }

  private Column[] hiveColumns() {
    return new Column[] {
      Column.of("id", Types.LongType.get(), "primary key", false, false, null),
      Column.of("name", Types.StringType.get(), null),
    };
  }

  private Map<String, String> hiveTableProps() {
    Map<String, String> props = new HashMap<>();
    props.put(GlueConstants.INPUT_FORMAT_CLASS, HiveStorageConstants.TEXT_INPUT_FORMAT_CLASS);
    props.put(GlueConstants.OUTPUT_FORMAT, HiveStorageConstants.IGNORE_KEY_OUTPUT_FORMAT_CLASS);
    props.put(GlueConstants.SERDE_LIB, HiveStorageConstants.LAZY_SIMPLE_SERDE_CLASS);
    return props;
  }

  private Table createHiveTable(String schema, String table) {
    return ops.createTable(
        tableIdent(schema, table),
        hiveColumns(),
        null,
        hiveTableProps(),
        new Transform[0],
        Distributions.NONE,
        new SortOrder[0],
        new Index[0]);
  }

  private Column[] partitionedColumns() {
    return new Column[] {
      Column.of("id", Types.LongType.get(), null, false, false, null),
      Column.of("dt", Types.DateType.get(), null),
    };
  }

  private SupportsPartitions createPartitionedTable(String schema, String table) {
    Map<String, String> props = hiveTableProps();
    props.put(GlueConstants.LOCATION, "s3://gravitino-test-bucket/" + schema + "/" + table);
    ops.createTable(
        tableIdent(schema, table),
        partitionedColumns(),
        null,
        props,
        new Transform[] {Transforms.identity("dt")},
        Distributions.NONE,
        new SortOrder[0],
        new Index[0]);
    return ops.loadTable(tableIdent(schema, table)).supportPartitions();
  }

  private GlueClient glueClient() {
    if (rawGlueClient == null) {
      rawGlueClient = GlueClientProvider.buildClient(catalogConfig());
    }
    return rawGlueClient;
  }

  /**
   * Creates the Glue entry of an Iceberg table directly through the Glue API, as another engine
   * would. Its metadata file does not exist, so the table can be listed and dropped but not loaded.
   */
  private void createExternalIcebergTable(String schema, String name) {
    glueClient()
        .createTable(
            CreateTableRequest.builder()
                .databaseName(schema)
                .tableInput(
                    TableInput.builder()
                        .name(name)
                        .tableType(GlueConstants.EXTERNAL_TABLE_TYPE)
                        .parameters(
                            Map.of(
                                GlueConstants.TABLE_TYPE_PARAM,
                                GlueConstants.ICEBERG_TABLE_TYPE_VALUE,
                                GlueConstants.METADATA_LOCATION,
                                "s3://bucket/path/metadata/v1.metadata.json"))
                        .build())
                .build());
  }

  /** Creates a Presto/Athena style view directly through the Glue API. */
  private void createGlueView(String schema, String name) {
    glueClient()
        .createTable(
            CreateTableRequest.builder()
                .databaseName(schema)
                .tableInput(
                    TableInput.builder()
                        .name(name)
                        .tableType(GlueConstants.VIRTUAL_VIEW_TABLE_TYPE)
                        .viewOriginalText("/* Presto View: dGVzdA== */")
                        .viewExpandedText("/* Presto View */")
                        .parameters(Map.of("presto_view", "true"))
                        .storageDescriptor(
                            StorageDescriptor.builder()
                                .columns(
                                    software.amazon.awssdk.services.glue.model.Column.builder()
                                        .name("id")
                                        .type("int")
                                        .build())
                                .build())
                        .build())
                .build());
  }

  private Partition identityPartition(String dateValue) {
    return Partitions.identity(
        "dt=" + dateValue,
        new String[][] {{"dt"}},
        new Literal[] {Literals.stringLiteral(dateValue)},
        Collections.emptyMap());
  }

  @Test
  void testExistingCatalogConnection() {
    assertDoesNotThrow(() -> ops.testConnection(NameIdentifier.of("ml", "cat")));
  }

  // -------------------------------------------------------------------------
  // Schema tests
  // -------------------------------------------------------------------------

  @Test
  void testSchemaProperties() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), "test comment", Map.of("k1", "v1"));

    Schema loaded = ops.loadSchema(schemaIdent(schema));
    assertEquals(schema, loaded.name());
    assertEquals("test comment", loaded.comment());
    assertTrue(loaded.properties().containsKey("k1"));
    assertEquals("v1", loaded.properties().get("k1"));
  }

  @Test
  void testListSchemas() {
    String schema1 = newSchema();
    String schema2 = "glue_it_" + System.nanoTime() + "_b";
    ops.createSchema(schemaIdent(schema1), null, Collections.emptyMap());
    ops.createSchema(schemaIdent(schema2), null, Collections.emptyMap());
    try {
      List<String> names =
          Arrays.stream(ops.listSchemas(SCHEMA_NS))
              .map(NameIdentifier::name)
              .collect(Collectors.toList());
      assertTrue(names.contains(schema1));
      assertTrue(names.contains(schema2));
    } finally {
      try {
        ops.dropSchema(schemaIdent(schema2), false);
      } catch (Exception ignored) {
      }
    }
  }

  @Test
  void testAlterSchema() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Collections.emptyMap());

    ops.alterSchema(schemaIdent(schema), SchemaChange.setProperty("newKey", "newVal"));
    Schema loaded = ops.loadSchema(schemaIdent(schema));
    assertEquals("newVal", loaded.properties().get("newKey"));

    ops.alterSchema(schemaIdent(schema), SchemaChange.removeProperty("newKey"));
    Schema reloaded = ops.loadSchema(schemaIdent(schema));
    assertFalse(reloaded.properties().containsKey("newKey"));
  }

  @Test
  void testDropSchemaEmpty() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Collections.emptyMap());
    boolean dropped = ops.dropSchema(schemaIdent(schema), false);
    assertTrue(dropped);
    currentSchema = null;
  }

  @Test
  void testDropMissingSchemaWithoutCascadeReturnsFalse() {
    assertFalse(ops.dropSchema(schemaIdent("glue_it_missing_" + System.nanoTime()), false));
  }

  @Test
  void testFormatFilterHidesExcludedTables() throws Exception {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Collections.emptyMap());
    createHiveTable(schema, "hive_tbl");

    Map<String, String> filteredConfig = new HashMap<>(catalogConfig());
    filteredConfig.put(GlueConstants.TABLE_FORMAT_FILTER, "iceberg");
    GlueCatalogOperations filteredOps = new GlueCatalogOperations();
    try {
      filteredOps.initialize(filteredConfig, null, null);
      NameIdentifier ident = tableIdent(schema, "hive_tbl");

      assertEquals(0, filteredOps.listTables(tableNs(schema)).length);
      assertThrows(NoSuchTableException.class, () -> filteredOps.loadTable(ident));
      assertThrows(
          NoSuchTableException.class,
          () -> filteredOps.alterTable(ident, TableChange.setProperty("k", "v")));
      assertFalse(filteredOps.dropTable(ident));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              filteredOps.createTable(
                  tableIdent(schema, "hidden_tbl"),
                  hiveColumns(),
                  null,
                  hiveTableProps(),
                  new Transform[0],
                  Distributions.NONE,
                  new SortOrder[0],
                  new Index[0]));
    } finally {
      filteredOps.close();
    }

    // The table is untouched and still visible through the unfiltered catalog.
    Table table = ops.loadTable(tableIdent(schema, "hive_tbl"));
    assertFalse(table.properties().containsKey("k"));
    assertThrows(NoSuchTableException.class, () -> ops.loadTable(tableIdent(schema, "hidden_tbl")));
  }

  @Test
  void testDropSchemaNonEmpty() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Collections.emptyMap());
    createHiveTable(schema, "tbl");

    try {
      assertThrows(NonEmptySchemaException.class, () -> ops.dropSchema(schemaIdent(schema), false));

      assertTrue(ops.dropTable(tableIdent(schema, "tbl")));
      assertTrue(ops.dropSchema(schemaIdent(schema), false));
      currentSchema = null;
    } finally {
      if (currentSchema != null) {
        try {
          ops.dropTable(tableIdent(schema, "tbl"));
        } catch (Exception ignored) {
        }
        try {
          ops.dropSchema(schemaIdent(schema), false);
        } catch (Exception ignored) {
        }
        currentSchema = null;
      }
    }
  }

  // -------------------------------------------------------------------------
  // Hive table tests
  // -------------------------------------------------------------------------

  @Test
  void testCreateHiveTable() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Collections.emptyMap());
    createHiveTable(schema, "hive_tbl");

    Table loaded = ops.loadTable(tableIdent(schema, "hive_tbl"));
    assertEquals("hive_tbl", loaded.name());
    assertEquals(2, loaded.columns().length);
    assertEquals("id", loaded.columns()[0].name());
    assertEquals(Types.LongType.get(), loaded.columns()[0].dataType());
    assertNotNull(loaded.properties().get(GlueConstants.INPUT_FORMAT_CLASS));
    assertNotNull(loaded.properties().get(GlueConstants.OUTPUT_FORMAT));
    assertNotNull(loaded.properties().get(GlueConstants.SERDE_LIB));
  }

  @Test
  void testListTables() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Collections.emptyMap());
    createHiveTable(schema, "tbl1");
    createHiveTable(schema, "tbl2");

    List<String> names =
        Arrays.stream(ops.listTables(tableNs(schema)))
            .map(NameIdentifier::name)
            .collect(Collectors.toList());
    assertTrue(names.contains("tbl1"));
    assertTrue(names.contains("tbl2"));
  }

  @Test
  void testAlterTable() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Collections.emptyMap());
    createHiveTable(schema, "altertbl");

    ops.alterTable(
        tableIdent(schema, "altertbl"),
        TableChange.addColumn(new String[] {"email"}, Types.StringType.get()));
    Table altered = ops.loadTable(tableIdent(schema, "altertbl"));
    assertEquals(3, altered.columns().length);
    assertEquals("email", altered.columns()[2].name());
  }

  @Test
  void testDropTable() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Collections.emptyMap());
    createHiveTable(schema, "droptbl");

    assertTrue(ops.dropTable(tableIdent(schema, "droptbl")));
    assertFalse(ops.dropTable(tableIdent(schema, "droptbl")));
  }

  // -------------------------------------------------------------------------
  // View tests: Glue keeps views alongside tables, the table APIs must skip them
  // -------------------------------------------------------------------------

  @Test
  void testListTablesExcludesViews() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Collections.emptyMap());
    createHiveTable(schema, "base_tbl");
    createGlueView(schema, "a_view");

    List<String> names =
        Arrays.stream(ops.listTables(tableNs(schema)))
            .map(NameIdentifier::name)
            .collect(Collectors.toList());
    assertTrue(names.contains("base_tbl"));
    assertFalse(names.contains("a_view"));
  }

  @Test
  void testLoadTableRejectsView() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Collections.emptyMap());
    createGlueView(schema, "load_view");

    assertThrows(NoSuchTableException.class, () -> ops.loadTable(tableIdent(schema, "load_view")));
  }

  @Test
  void testDropTableRejectsView() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Collections.emptyMap());
    createGlueView(schema, "drop_view");

    assertThrows(NoSuchTableException.class, () -> ops.dropTable(tableIdent(schema, "drop_view")));
    assertEquals(
        GlueConstants.VIRTUAL_VIEW_TABLE_TYPE,
        glueClient()
            .getTable(GetTableRequest.builder().databaseName(schema).name("drop_view").build())
            .table()
            .tableType(),
        "the view must survive the rejected drop");
  }

  // -------------------------------------------------------------------------
  // Iceberg table tests
  // -------------------------------------------------------------------------

  @Test
  void testRegisterIcebergTableWithBlankMetadataLocationIsRefused() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Collections.emptyMap());

    Map<String, String> props = new HashMap<>();
    props.put(GlueConstants.TABLE_FORMAT, "ICEBERG");
    props.put(GlueConstants.METADATA_LOCATION, "   ");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.createTable(
                tableIdent(schema, "iceberg_tbl"),
                new Column[0],
                null,
                props,
                new Transform[0],
                Distributions.NONE,
                new SortOrder[0],
                new Index[0]));
    assertThrows(
        NoSuchTableException.class, () -> ops.loadTable(tableIdent(schema, "iceberg_tbl")));
  }

  // -------------------------------------------------------------------------
  // Partition tests
  // -------------------------------------------------------------------------

  @Test
  void testAlterNativeTableKeepsFieldsGravitinoDoesNotModel() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Collections.emptyMap());
    glueClient()
        .createTable(
            CreateTableRequest.builder()
                .databaseName(schema)
                .tableInput(
                    TableInput.builder()
                        .name("native_tbl")
                        .owner("crawler")
                        .tableType("EXTERNAL_TABLE")
                        .parameters(Map.of("classification", "csv"))
                        .storageDescriptor(
                            StorageDescriptor.builder()
                                .columns(
                                    software.amazon.awssdk.services.glue.model.Column.builder()
                                        .name("s")
                                        .type("struct<Outer:int>")
                                        .parameters(Map.of("stat", "1"))
                                        .build())
                                .location("s3://gravitino-test-bucket/" + schema + "/native_tbl")
                                .parameters(Map.of("recordCount", "10"))
                                .build())
                        .build())
                .build());

    ops.alterTable(tableIdent(schema, "native_tbl"), TableChange.updateComment("altered"));

    software.amazon.awssdk.services.glue.model.Table stored =
        glueClient()
            .getTable(GetTableRequest.builder().databaseName(schema).name("native_tbl").build())
            .table();
    assertEquals("altered", stored.description());
    assertEquals("crawler", stored.owner());
    assertEquals("csv", stored.parameters().get("classification"));
    assertEquals("10", stored.storageDescriptor().parameters().get("recordCount"));
    assertEquals("struct<Outer:int>", stored.storageDescriptor().columns().get(0).type());
    assertEquals("1", stored.storageDescriptor().columns().get(0).parameters().get("stat"));
  }

  @Test
  void testAlterSchemaKeepsNativeLocationUri() {
    String schema = newSchema();
    String location = "s3://gravitino-test-bucket/" + schema;
    glueClient()
        .createDatabase(
            CreateDatabaseRequest.builder()
                .databaseInput(DatabaseInput.builder().name(schema).locationUri(location).build())
                .build());

    ops.alterSchema(schemaIdent(schema), SchemaChange.setProperty("k", "v"));

    assertEquals(
        location,
        glueClient()
            .getDatabase(GetDatabaseRequest.builder().name(schema).build())
            .database()
            .locationUri());
    assertEquals(
        location, ops.loadSchema(schemaIdent(schema)).properties().get(GlueConstants.LOCATION));
  }

  @Test
  void testAddPartitionIsEscapedAndReadable() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Collections.emptyMap());
    SupportsPartitions sp = createPartitionedTable(schema, "ptbl");

    Partition added = sp.addPartition(identityPartition("2025/12/31"));

    assertEquals("dt=2025%2F12%2F31", added.name());
    assertTrue(Arrays.asList(sp.listPartitionNames()).contains("dt=2025%2F12%2F31"));
    assertEquals("dt=2025%2F12%2F31", sp.getPartition("dt=2025%2F12%2F31").name());

    software.amazon.awssdk.services.glue.model.Partition stored =
        glueClient()
            .getPartition(
                GetPartitionRequest.builder()
                    .databaseName(schema)
                    .tableName("ptbl")
                    .partitionValues("2025/12/31")
                    .build())
            .partition();
    assertEquals(
        "s3://gravitino-test-bucket/" + schema + "/ptbl/dt=2025%2F12%2F31",
        stored.storageDescriptor().location());
    assertEquals(
        HiveStorageConstants.LAZY_SIMPLE_SERDE_CLASS,
        stored.storageDescriptor().serdeInfo().serializationLibrary());

    assertTrue(sp.dropPartition("dt=2025%2F12%2F31"));
  }

  @Test
  void testAddPartition() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Collections.emptyMap());
    SupportsPartitions sp = createPartitionedTable(schema, "ptbl");

    sp.addPartition(identityPartition("2024-01-01"));

    List<String> names = Arrays.asList(sp.listPartitionNames());
    assertTrue(names.contains("dt=2024-01-01"));
  }

  @Test
  void testAddPartitionRejectsEmptyValue() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Collections.emptyMap());
    SupportsPartitions sp = createPartitionedTable(schema, "ptbl");

    assertThrows(IllegalArgumentException.class, () -> sp.addPartition(identityPartition("")));
    assertEquals(0, sp.listPartitionNames().length);
  }

  @Test
  void testListPartitions() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Collections.emptyMap());
    SupportsPartitions sp = createPartitionedTable(schema, "ptbl");

    sp.addPartition(identityPartition("2024-02-01"));

    Partition[] parts = sp.listPartitions();
    assertEquals(1, parts.length);
    assertEquals("dt=2024-02-01", parts[0].name());
  }

  @Test
  void testGetPartition() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Collections.emptyMap());
    SupportsPartitions sp = createPartitionedTable(schema, "ptbl");

    sp.addPartition(identityPartition("2024-03-01"));

    Partition p = sp.getPartition("dt=2024-03-01");
    assertNotNull(p);
    assertEquals("dt=2024-03-01", p.name());
  }

  @Test
  void testDropPartition() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Collections.emptyMap());
    SupportsPartitions sp = createPartitionedTable(schema, "ptbl");

    sp.addPartition(identityPartition("2024-04-01"));

    assertTrue(sp.dropPartition("dt=2024-04-01"));
    assertFalse(sp.dropPartition("dt=2024-04-01"));
  }

  // -------------------------------------------------------------------------
  // Hive table feature tests
  // -------------------------------------------------------------------------

  @Test
  void testHashDistribution() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Collections.emptyMap());

    ops.createTable(
        tableIdent(schema, "bucketed"),
        hiveColumns(),
        null,
        hiveTableProps(),
        new Transform[0],
        Distributions.hash(4, NamedReference.field("id")),
        new SortOrder[0],
        new Index[0]);

    Table loaded = ops.loadTable(tableIdent(schema, "bucketed"));
    assertEquals(4, loaded.distribution().number());
    assertEquals(1, loaded.distribution().expressions().length);
    assertEquals(NamedReference.field("id"), loaded.distribution().expressions()[0]);
  }

  @Test
  void testSortOrders() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Collections.emptyMap());

    ops.createTable(
        tableIdent(schema, "sorted"),
        hiveColumns(),
        null,
        hiveTableProps(),
        new Transform[0],
        Distributions.NONE,
        new SortOrder[] {
          SortOrders.ascending(NamedReference.field("id")),
          SortOrders.descending(NamedReference.field("name"))
        },
        new Index[0]);

    Table loaded = ops.loadTable(tableIdent(schema, "sorted"));
    assertEquals(2, loaded.sortOrder().length);
    assertEquals(SortDirection.ASCENDING, loaded.sortOrder()[0].direction());
    assertEquals(NamedReference.field("id"), loaded.sortOrder()[0].expression());
    assertEquals(SortDirection.DESCENDING, loaded.sortOrder()[1].direction());
    assertEquals(NamedReference.field("name"), loaded.sortOrder()[1].expression());
  }

  @Test
  void testTableComment() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Collections.emptyMap());

    ops.createTable(
        tableIdent(schema, "commented"),
        hiveColumns(),
        "my table comment",
        hiveTableProps(),
        new Transform[0],
        Distributions.NONE,
        new SortOrder[0],
        new Index[0]);

    Table loaded = ops.loadTable(tableIdent(schema, "commented"));
    assertEquals("my table comment", loaded.comment());
  }

  @Test
  void testUnsupportedTransformRejected() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Collections.emptyMap());

    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.createTable(
                tableIdent(schema, "bad"),
                hiveColumns(),
                null,
                hiveTableProps(),
                new Transform[] {Transforms.year("dt")},
                Distributions.NONE,
                new SortOrder[0],
                new Index[0]));
  }

  @Test
  void testDecimalAndVarcharColumns() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Collections.emptyMap());

    Column[] cols =
        new Column[] {
          Column.of("price", Types.DecimalType.of(10, 2), null),
          Column.of("label", Types.VarCharType.of(255), null),
        };
    ops.createTable(
        tableIdent(schema, "typed"),
        cols,
        null,
        hiveTableProps(),
        new Transform[0],
        Distributions.NONE,
        new SortOrder[0],
        new Index[0]);

    Table loaded = ops.loadTable(tableIdent(schema, "typed"));
    assertEquals(Types.DecimalType.of(10, 2), loaded.columns()[0].dataType());
    assertEquals(Types.VarCharType.of(255), loaded.columns()[1].dataType());
  }

  @Test
  void testComplexColumnTypes() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Collections.emptyMap());

    Column[] cols =
        new Column[] {
          Column.of("tags", Types.ExternalType.of("array<string>"), null),
          Column.of("info", Types.ExternalType.of("struct<name:string,age:int>"), null),
        };
    ops.createTable(
        tableIdent(schema, "complex"),
        cols,
        null,
        hiveTableProps(),
        new Transform[0],
        Distributions.NONE,
        new SortOrder[0],
        new Index[0]);

    Table loaded = ops.loadTable(tableIdent(schema, "complex"));
    // GlueTypeConverter.toGravitino converts known complex types to native types.
    assertEquals(Types.ListType.class, loaded.columns()[0].dataType().getClass());
    assertEquals(Types.StructType.class, loaded.columns()[1].dataType().getClass());
  }

  @Test
  void testSerDeParameters() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Collections.emptyMap());

    Map<String, String> props = hiveTableProps();
    props.put("serde.parameter.serialization.format", "1");
    ops.createTable(
        tableIdent(schema, "serde_tbl"),
        hiveColumns(),
        null,
        props,
        new Transform[0],
        Distributions.NONE,
        new SortOrder[0],
        new Index[0]);

    Table loaded = ops.loadTable(tableIdent(schema, "serde_tbl"));
    assertEquals("1", loaded.properties().get("serde.parameter.serialization.format"));
  }

  // -------------------------------------------------------------------------
  // Iceberg table tests (additional)
  // -------------------------------------------------------------------------

  @Test
  void testDropIcebergTable() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Collections.emptyMap());

    createExternalIcebergTable(schema, "iceberg_drop");

    assertTrue(ops.dropTable(tableIdent(schema, "iceberg_drop")));
    assertFalse(ops.dropTable(tableIdent(schema, "iceberg_drop")));
  }

  @Test
  void testListTablesIncludesIceberg() {
    String schema = newSchema();
    ops.createSchema(schemaIdent(schema), null, Collections.emptyMap());

    createHiveTable(schema, "hive_mixed");

    createExternalIcebergTable(schema, "iceberg_mixed");

    List<String> names =
        Arrays.stream(ops.listTables(tableNs(schema)))
            .map(NameIdentifier::name)
            .collect(Collectors.toList());
    assertTrue(names.contains("hive_mixed"));
    assertTrue(names.contains("iceberg_mixed"));
  }
}
