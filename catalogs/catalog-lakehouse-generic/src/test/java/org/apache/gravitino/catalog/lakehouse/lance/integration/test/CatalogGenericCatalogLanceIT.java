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
package org.apache.gravitino.catalog.lakehouse.lance.integration.test;

import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_CREATION_MODE;
import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_TABLE_FORMAT;
import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_TABLE_REGISTER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.lancedb.lance.Dataset;
import com.lancedb.lance.Fragment;
import com.lancedb.lance.FragmentMetadata;
import com.lancedb.lance.Transaction;
import com.lancedb.lance.WriteParams;
import com.lancedb.lance.ipc.LanceScanner;
import com.lancedb.lance.ipc.ScanOptions;
import com.lancedb.lance.operation.Append;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.sorts.NullOrdering;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Index.IndexType;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.partitions.RangePartition;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatalogGenericCatalogLanceIT extends BaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogGenericCatalogLanceIT.class);
  public static final String metalakeName =
      GravitinoITUtils.genRandomName("CatalogGenericLakeLanceIT_metalake");

  public String catalogName = GravitinoITUtils.genRandomName("CatalogGenericLakeLanceI_catalog");
  public String SCHEMA_PREFIX = "CatalogGenericLakeLance_schema";
  public String schemaName = GravitinoITUtils.genRandomName(SCHEMA_PREFIX);
  public String TABLE_PREFIX = "CatalogGenericLakeLance_table";
  public String tableName = GravitinoITUtils.genRandomName(TABLE_PREFIX);
  public static final String TABLE_COMMENT = "table_comment";
  public static final String LANCE_COL_NAME1 = "lance_col_name1";
  public static final String LANCE_COL_NAME2 = "lance_col_name2";
  public static final String LANCE_COL_NAME3 = "lance_col_name3";
  protected final String provider = "lakehouse-generic";
  protected GravitinoMetalake metalake;
  protected Catalog catalog;
  protected String tempDirectory;

  @BeforeAll
  public void startup() throws Exception {
    createMetalake();
    createCatalog();
    createSchema();

    // Create a temp directory for test use
    Path tempDir = Files.createTempDirectory("myTempDir");
    tempDirectory = tempDir.toString();
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
    try {
      closer.close();
    } catch (Exception e) {
      LOG.error("Failed to close CloseableGroup", e);
    }

    client = null;

    FileUtils.deleteDirectory(new File(tempDirectory));
  }

  @AfterEach
  public void resetSchema() throws InterruptedException {
    catalog.asSchemas().dropSchema(schemaName, true);
    createSchema();
  }

  @Test
  public void testCreateLanceTable() {
    // Create a table from Gravitino API
    Column[] columns = createColumns();
    NameIdentifier nameIdentifier = NameIdentifier.of(schemaName, tableName);

    Map<String, String> properties = createProperties();
    String tableLocation = tempDirectory + "/" + tableName;
    properties.put("format", "lance");
    properties.put("location", tableLocation);

    Table createdTable =
        catalog
            .asTableCatalog()
            .createTable(
                nameIdentifier,
                columns,
                TABLE_COMMENT,
                properties,
                Transforms.EMPTY_TRANSFORM,
                null,
                null);

    Assertions.assertEquals(createdTable.name(), tableName);
    Map<String, String> createdTableProperties = createdTable.properties();
    Assertions.assertEquals("lance", createdTableProperties.get("format"));

    Assertions.assertEquals(TABLE_COMMENT, createdTable.comment());
    Assertions.assertEquals(3, createdTable.columns().length);
    columnEquals(columns, createdTable.columns());
    String expectedTableLocation = tempDirectory + "/" + tableName + "/";
    Assertions.assertEquals(expectedTableLocation, createdTableProperties.get("location"));
    Assertions.assertTrue(new File(expectedTableLocation).exists());

    // Drop table
    catalog.asTableCatalog().dropTable(nameIdentifier);
    catalog.asSchemas().dropSchema(schemaName, true);

    Map<String, String> schemaProperties = createSchemaProperties();
    String schemaLocation = tempDirectory + "/schema_location";
    schemaProperties.put("location", schemaLocation);
    catalog.asSchemas().createSchema(schemaName, "comment", schemaProperties);
    properties = createProperties();
    properties.put("format", "lance");

    Distribution distribution =
        Distributions.of(Strategy.EVEN, 10, NamedReference.field(LANCE_COL_NAME1));
    SortOrder[] sortOrders =
        new SortOrder[] {
          SortOrders.of(
              NamedReference.field(LANCE_COL_NAME2),
              SortDirection.ASCENDING,
              NullOrdering.NULLS_FIRST)
        };
    Index[] indexes =
        new Index[] {
          Indexes.of(IndexType.UNIQUE_KEY, "unique_index", new String[][] {{LANCE_COL_NAME3}})
        };

    RangePartition p1 =
        Partitions.range(
            "p1", Literals.stringLiteral("20220101"), Literals.NULL, Collections.emptyMap());
    RangePartition p2 =
        Partitions.range(
            "p2", Literals.stringLiteral("20220301"), Literals.NULL, Collections.emptyMap());
    Transform[] partitioning = {
      Transforms.range(new String[] {LANCE_COL_NAME3}, new RangePartition[] {p1, p2})
    };

    createdTable =
        catalog
            .asTableCatalog()
            .createTable(
                nameIdentifier,
                columns,
                TABLE_COMMENT,
                properties,
                partitioning,
                distribution,
                sortOrders,
                indexes);
    Assertions.assertEquals(createdTable.name(), tableName);
    createdTableProperties = createdTable.properties();
    Assertions.assertEquals("lance", createdTableProperties.get("format"));

    Assertions.assertEquals(TABLE_COMMENT, createdTable.comment());
    Assertions.assertEquals(3, createdTable.columns().length);
    columnEquals(columns, createdTable.columns());
    expectedTableLocation = schemaLocation + "/" + tableName + "/";
    Assertions.assertEquals(expectedTableLocation, createdTableProperties.get("location"));
    Assertions.assertTrue(new File(expectedTableLocation).exists());

    Table loadTable = catalog.asTableCatalog().loadTable(nameIdentifier);
    Assertions.assertEquals(distribution, loadTable.distribution());
    Assertions.assertArrayEquals(sortOrders, loadTable.sortOrder());
    Assertions.assertArrayEquals(indexes, loadTable.index());
    Assertions.assertArrayEquals(partitioning, loadTable.partitioning());

    // Now try to load table
    Table loadedTable = catalog.asTableCatalog().loadTable(nameIdentifier);
    Assertions.assertEquals(createdTable.name(), loadedTable.name());
    Map<String, String> loadedTableProperties = loadedTable.properties();
    Assertions.assertEquals("lance", loadedTableProperties.get("format"));
    Assertions.assertEquals(expectedTableLocation, loadedTableProperties.get("location"));
    Assertions.assertEquals(TABLE_COMMENT, loadedTable.comment());

    // Now test list tables
    List<NameIdentifier> tableIdentifiers =
        Arrays.asList(catalog.asTableCatalog().listTables(nameIdentifier.namespace()));
    Assertions.assertEquals(1, tableIdentifiers.size());
    Assertions.assertEquals(nameIdentifier, tableIdentifiers.get(0));

    // Now try to simulate the location of lance table does not exist.
    Map<String, String> newProperties = createProperties();
    newProperties.put("format", "lance");
    // Use a wrong location to let the table creation fail
    newProperties.put("location", "hdfs://localhost:9000/wrong_location");

    String nameNew = GravitinoITUtils.genRandomName(TABLE_PREFIX);
    NameIdentifier newNameIdentifier = NameIdentifier.of(schemaName, nameNew);
    Exception e =
        Assertions.assertThrows(
            Exception.class,
            () -> {
              catalog
                  .asTableCatalog()
                  .createTable(
                      newNameIdentifier,
                      columns,
                      TABLE_COMMENT,
                      newProperties,
                      Transforms.EMPTY_TRANSFORM,
                      null,
                      null);
            });

    Assertions.assertTrue(e.getMessage().contains("Invalid user input"));

    Assertions.assertThrows(
        RuntimeException.class, () -> catalog.asTableCatalog().loadTable(newNameIdentifier));
  }

  @Test
  void testLanceTableFormat() {
    String tableName = GravitinoITUtils.genRandomName(TABLE_PREFIX);
    Column[] columns = createColumns();
    NameIdentifier nameIdentifier = NameIdentifier.of(schemaName, tableName);

    Map<String, String> properties = createProperties();
    String tableLocation = tempDirectory + "/" + tableName;
    properties.put("format", "lance");
    properties.put("location", tableLocation);

    catalog
        .asTableCatalog()
        .createTable(
            nameIdentifier,
            columns,
            TABLE_COMMENT,
            properties,
            Transforms.EMPTY_TRANSFORM,
            null,
            null);

    // Now try to read the lance directory and check it.
    try (Dataset dataset = Dataset.open(tableLocation)) {
      org.apache.arrow.vector.types.pojo.Schema lanceSchema = dataset.getSchema();
      List<Field> fields = lanceSchema.getFields();
      for (Field field : fields) {
        if (field.getName().equals(LANCE_COL_NAME1)) {
          Assertions.assertEquals(new ArrowType.Int(32, true), field.getType());
        } else if (field.getName().equals(LANCE_COL_NAME2)) {
          Assertions.assertEquals(new ArrowType.Int(64, true), field.getType());
        } else if (field.getName().equals(LANCE_COL_NAME3)) {
          Assertions.assertEquals(new ArrowType.Utf8(), field.getType());
        } else {
          Assertions.fail("Unexpected column name in lance table: " + field.getName());
        }
      }

      // Now try to write some data to the dataset
      Transaction trans =
          dataset
              .newTransactionBuilder()
              .operation(
                  Append.builder()
                      .fragments(
                          createFragmentMetadata(
                              tableLocation,
                              Arrays.asList(
                                  new LanceDataValue(1, 100L, "first"),
                                  new LanceDataValue(2, 200L, "second"),
                                  new LanceDataValue(3, 300L, "third")),
                              lanceSchema))
                      .build())
              .writeParams(ImmutableMap.of())
              .build();

      Dataset newDataset = dataset.commitTransaction(trans);
      try (LanceScanner scanner =
          newDataset.newScan(
              new ScanOptions.Builder()
                  .columns(Arrays.asList(LANCE_COL_NAME1, LANCE_COL_NAME2, LANCE_COL_NAME3))
                  .batchSize(1000)
                  .build())) {

        List<LanceDataValue> dataValues = Lists.newArrayList();
        try (ArrowReader reader = scanner.scanBatches()) {
          while (reader.loadNextBatch()) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            List<FieldVector> fieldVectors = root.getFieldVectors();

            IntVector col1Vector = (IntVector) fieldVectors.get(0);
            BigIntVector col2Vector = (BigIntVector) fieldVectors.get(1);
            VarCharVector col3Vector = (VarCharVector) fieldVectors.get(2);

            for (int i = 0; i < root.getRowCount(); i++) {
              int col1 = col1Vector.get(i);
              long col2 = col2Vector.get(i);
              String col3 = new String(col3Vector.get(i), StandardCharsets.UTF_8);
              dataValues.add(new LanceDataValue(col1, col2, col3));
            }
          }
        }

        Assertions.assertEquals(3, dataValues.size());
        Assertions.assertEquals(1, dataValues.get(0).col1);
        Assertions.assertEquals(100L, dataValues.get(0).col2);
        Assertions.assertEquals("first", dataValues.get(0).col3);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  static class LanceDataValue {
    public Integer col1;
    public Long col2;
    public String col3;

    public LanceDataValue(Integer col1, Long col2, String col3) {
      this.col1 = col1;
      this.col2 = col2;
      this.col3 = col3;
    }
  }

  private List<FragmentMetadata> createFragmentMetadata(
      String tableLocation,
      List<LanceDataValue> updates,
      org.apache.arrow.vector.types.pojo.Schema schema)
      throws JsonProcessingException {
    List<FragmentMetadata> fragmentMetas;
    int count = 0;
    RootAllocator rootAllocator = new RootAllocator();
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, rootAllocator)) {
      for (FieldVector vector : root.getFieldVectors()) {
        vector.setInitialCapacity(count);
      }
      root.allocateNew();

      IntVector col1Vector = (IntVector) root.getVector(LANCE_COL_NAME1);
      BigIntVector col2Vector = (BigIntVector) root.getVector(LANCE_COL_NAME2);
      VarCharVector col3Vector = (VarCharVector) root.getVector(LANCE_COL_NAME3);

      int index = 0;
      for (LanceDataValue data : updates) {
        col1Vector.setSafe(index, data.col1);
        col2Vector.setSafe(index, data.col2);
        col3Vector.setSafe(index, data.col3.getBytes(StandardCharsets.UTF_8));
        index++;
      }
      root.setRowCount(index);

      fragmentMetas =
          Fragment.create(tableLocation, rootAllocator, root, new WriteParams.Builder().build());
      return fragmentMetas;
    }
  }

  protected Map<String, String> createSchemaProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    return properties;
  }

  private void columnEquals(Column[] expect, Column[] actual) {
    Assertions.assertEquals(expect.length, actual.length);

    for (int i = 0; i < expect.length; i++) {
      Column expectCol = expect[i];
      Column actualCol = actual[i];

      Assertions.assertEquals(expectCol.name(), actualCol.name());
      Assertions.assertEquals(expectCol.dataType(), actualCol.dataType());
      Assertions.assertEquals(expectCol.comment(), actualCol.comment());
    }
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
    metalake.createCatalog(catalogName, Catalog.Type.RELATIONAL, provider, "comment", properties);

    catalog = metalake.loadCatalog(catalogName);
  }

  private void createSchema() throws InterruptedException {
    Map<String, String> schemaProperties = createSchemaProperties();
    String comment = "comment";
    catalog.asSchemas().createSchema(schemaName, comment, schemaProperties);
    Schema loadSchema = catalog.asSchemas().loadSchema(schemaName);
    Assertions.assertEquals(schemaName, loadSchema.name());
    Assertions.assertEquals(comment, loadSchema.comment());
    Assertions.assertEquals("val1", loadSchema.properties().get("key1"));
    Assertions.assertEquals("val2", loadSchema.properties().get("key2"));
  }

  private Column[] createColumns() {
    Column col1 = Column.of(LANCE_COL_NAME1, Types.IntegerType.get(), "col_1_comment");
    Column col2 = Column.of(LANCE_COL_NAME2, Types.LongType.get(), "col_2_comment");
    Column col3 = Column.of(LANCE_COL_NAME3, Types.StringType.get(), "col_3_comment");
    return new Column[] {col1, col2, col3};
  }

  protected Map<String, String> createProperties() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    return properties;
  }

  @Test
  public void testCreateTableWithExistOkMode() {
    // Create initial table
    Column[] columns =
        new Column[] {
          Column.of(LANCE_COL_NAME1, Types.IntegerType.get(), "col_1_comment"),
          Column.of(LANCE_COL_NAME2, Types.StringType.get(), "col_2_comment"),
          Column.of(LANCE_COL_NAME3, Types.LongType.get(), "col_3_comment")
        };

    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
    String location = String.format("%s/%s/%s", tempDirectory, schemaName, tableName);

    Map<String, String> properties = createProperties();
    properties.put(Table.PROPERTY_LOCATION, location);
    properties.put(Table.PROPERTY_TABLE_FORMAT, LANCE_TABLE_FORMAT);
    properties.put(Table.PROPERTY_EXTERNAL, "true");

    Table createdTable =
        catalog
            .asTableCatalog()
            .createTable(
                tableIdentifier,
                columns,
                TABLE_COMMENT,
                properties,
                Transforms.EMPTY_TRANSFORM,
                Distributions.NONE,
                new SortOrder[0]);
    Assertions.assertNotNull(createdTable);

    // Try to create the same table again with EXIST_OK mode
    Map<String, String> existOkProperties = createProperties();
    existOkProperties.put(Table.PROPERTY_LOCATION, location);
    existOkProperties.put(Table.PROPERTY_TABLE_FORMAT, LANCE_TABLE_FORMAT);
    existOkProperties.put(LANCE_CREATION_MODE, "EXIST_OK");
    existOkProperties.put(Table.PROPERTY_EXTERNAL, "true");

    Table existingTable =
        catalog
            .asTableCatalog()
            .createTable(
                tableIdentifier,
                columns,
                TABLE_COMMENT,
                existOkProperties,
                Transforms.EMPTY_TRANSFORM,
                Distributions.NONE,
                new SortOrder[0]);

    // Should return the existing table without error
    Assertions.assertNotNull(existingTable);
    Assertions.assertEquals(createdTable.name(), existingTable.name());

    // Verify the table exists on disk
    File tableDir = new File(location);
    Assertions.assertTrue(tableDir.exists());
  }

  @Test
  public void testCreateTableWithOverwriteMode() {
    // Create initial table
    Column[] columns =
        new Column[] {
          Column.of(LANCE_COL_NAME1, Types.IntegerType.get(), "col_1_comment"),
          Column.of(LANCE_COL_NAME2, Types.StringType.get(), "col_2_comment")
        };

    NameIdentifier tableIdentifier =
        NameIdentifier.of(schemaName, GravitinoITUtils.genRandomName(TABLE_PREFIX));
    String location = String.format("%s/%s/%s", tempDirectory, schemaName, tableIdentifier.name());

    Map<String, String> properties = createProperties();
    properties.put(Table.PROPERTY_LOCATION, location);
    properties.put(Table.PROPERTY_TABLE_FORMAT, LANCE_TABLE_FORMAT);
    properties.put(Table.PROPERTY_EXTERNAL, "true");

    Table createdTable =
        catalog
            .asTableCatalog()
            .createTable(
                tableIdentifier,
                columns,
                TABLE_COMMENT,
                properties,
                Transforms.EMPTY_TRANSFORM,
                Distributions.NONE,
                new SortOrder[0]);
    Assertions.assertNotNull(createdTable);

    // Create the table again with OVERWRITE mode and different columns
    Column[] newColumns =
        new Column[] {
          Column.of(LANCE_COL_NAME1, Types.IntegerType.get(), "col_1_comment"),
          Column.of(LANCE_COL_NAME2, Types.StringType.get(), "col_2_comment"),
          Column.of(LANCE_COL_NAME3, Types.LongType.get(), "col_3_comment")
        };

    Map<String, String> overwriteProperties = createProperties();
    overwriteProperties.put(Table.PROPERTY_LOCATION, location);
    overwriteProperties.put(Table.PROPERTY_TABLE_FORMAT, LANCE_TABLE_FORMAT);
    overwriteProperties.put(LANCE_CREATION_MODE, "OVERWRITE");
    overwriteProperties.put(Table.PROPERTY_EXTERNAL, "true");

    Table overwrittenTable =
        catalog
            .asTableCatalog()
            .createTable(
                tableIdentifier,
                newColumns,
                TABLE_COMMENT,
                overwriteProperties,
                Transforms.EMPTY_TRANSFORM,
                Distributions.NONE,
                new SortOrder[0]);

    // Should create a new table
    Assertions.assertNotNull(overwrittenTable);
    Assertions.assertEquals(3, overwrittenTable.columns().length);

    // Verify the table exists on disk
    File tableDir = new File(location);
    Assertions.assertTrue(tableDir.exists());
  }

  @Test
  public void testCreateTableWithCreateModeFailsWhenExists() {
    // Create initial table
    Column[] columns =
        new Column[] {
          Column.of(LANCE_COL_NAME1, Types.IntegerType.get(), "col_1_comment"),
          Column.of(LANCE_COL_NAME2, Types.StringType.get(), "col_2_comment")
        };

    NameIdentifier tableIdentifier =
        NameIdentifier.of(schemaName, GravitinoITUtils.genRandomName(TABLE_PREFIX));
    String location = String.format("%s/%s/%s", tempDirectory, schemaName, tableIdentifier.name());

    Map<String, String> properties = createProperties();
    properties.put(Table.PROPERTY_LOCATION, location);
    properties.put(Table.PROPERTY_TABLE_FORMAT, LANCE_TABLE_FORMAT);
    properties.put(Table.PROPERTY_EXTERNAL, "true");

    Table createdTable =
        catalog
            .asTableCatalog()
            .createTable(
                tableIdentifier,
                columns,
                TABLE_COMMENT,
                properties,
                Transforms.EMPTY_TRANSFORM,
                Distributions.NONE,
                new SortOrder[0]);
    Assertions.assertNotNull(createdTable);

    // Try to create the same table again with CREATE mode (default) - should fail
    Map<String, String> createProperties = createProperties();
    createProperties.put(Table.PROPERTY_LOCATION, location);
    createProperties.put(Table.PROPERTY_TABLE_FORMAT, LANCE_TABLE_FORMAT);
    createProperties.put(LANCE_CREATION_MODE, "CREATE");
    createProperties.put(Table.PROPERTY_EXTERNAL, "true");

    Assertions.assertThrows(
        Exception.class,
        () ->
            catalog
                .asTableCatalog()
                .createTable(
                    tableIdentifier,
                    columns,
                    TABLE_COMMENT,
                    createProperties,
                    Transforms.EMPTY_TRANSFORM,
                    Distributions.NONE,
                    new SortOrder[0]));
  }

  @Test
  public void testRegisterTableWithExistOkMode() throws IOException {
    // First, create a physical Lance dataset
    Column[] columns =
        new Column[] {
          Column.of(LANCE_COL_NAME1, Types.IntegerType.get(), "col_1_comment"),
          Column.of(LANCE_COL_NAME2, Types.StringType.get(), "col_2_comment")
        };

    NameIdentifier tableIdentifier =
        NameIdentifier.of(schemaName, GravitinoITUtils.genRandomName(TABLE_PREFIX));
    String location = String.format("%s/%s/%s", tempDirectory, schemaName, tableIdentifier.name());

    // Create a physical Lance dataset using Lance SDK directly
    org.apache.arrow.vector.types.pojo.Schema arrowSchema =
        new org.apache.arrow.vector.types.pojo.Schema(
            Arrays.asList(
                Field.nullable("lance_col_name1", new ArrowType.Int(32, true)),
                Field.nullable("lance_col_name2", new ArrowType.Utf8())));

    try (RootAllocator allocator = new RootAllocator();
        Dataset dataset =
            Dataset.create(allocator, location, arrowSchema, new WriteParams.Builder().build())) {
      // Dataset created successfully
    }

    // Register the table in Gravitino
    Map<String, String> properties = createProperties();
    properties.put(Table.PROPERTY_LOCATION, location);
    properties.put(Table.PROPERTY_TABLE_FORMAT, LANCE_TABLE_FORMAT);
    properties.put(LANCE_TABLE_REGISTER, "true");
    properties.put(Table.PROPERTY_EXTERNAL, "true");

    Table registeredTable =
        catalog
            .asTableCatalog()
            .createTable(
                tableIdentifier,
                columns,
                TABLE_COMMENT,
                properties,
                Transforms.EMPTY_TRANSFORM,
                Distributions.NONE,
                new SortOrder[0]);

    Assertions.assertNotNull(registeredTable);
    Assertions.assertEquals(tableIdentifier.name(), registeredTable.name());

    Map<String, String> existOkProperties = createProperties();
    existOkProperties.put(Table.PROPERTY_LOCATION, location);
    existOkProperties.put(Table.PROPERTY_TABLE_FORMAT, LANCE_TABLE_FORMAT);
    existOkProperties.put(LANCE_TABLE_REGISTER, "true");
    existOkProperties.put(Table.PROPERTY_EXTERNAL, "true");
    existOkProperties.put(LANCE_CREATION_MODE, "EXIST_OK");

    // Throw an exception for registering the table with EXIST_OK mode, since register operation
    // doesn't support EXIST_OK mode currently.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog
                .asTableCatalog()
                .createTable(
                    tableIdentifier,
                    columns,
                    TABLE_COMMENT,
                    existOkProperties,
                    Transforms.EMPTY_TRANSFORM,
                    Distributions.NONE,
                    new SortOrder[0]));
  }

  @Test
  public void testRegisterTableWithOverwriteMode() throws IOException {
    // First, create a physical Lance dataset
    Column[] columns =
        new Column[] {
          Column.of(LANCE_COL_NAME1, Types.IntegerType.get(), "col_1_comment"),
          Column.of(LANCE_COL_NAME2, Types.StringType.get(), "col_2_comment")
        };

    NameIdentifier tableIdentifier =
        NameIdentifier.of(schemaName, GravitinoITUtils.genRandomName(TABLE_PREFIX));
    String location = String.format("%s/%s/%s", tempDirectory, schemaName, tableIdentifier.name());

    // Create a physical Lance dataset
    org.apache.arrow.vector.types.pojo.Schema arrowSchema =
        new org.apache.arrow.vector.types.pojo.Schema(
            Arrays.asList(
                Field.nullable("lance_col_name1", new ArrowType.Int(32, true)),
                Field.nullable("lance_col_name2", new ArrowType.Utf8())));

    try (RootAllocator allocator = new RootAllocator();
        Dataset dataset =
            Dataset.create(allocator, location, arrowSchema, new WriteParams.Builder().build())) {
      // Dataset created
    }

    // Register the table in Gravitino
    Map<String, String> properties = createProperties();
    properties.put(Table.PROPERTY_LOCATION, location);
    properties.put(Table.PROPERTY_TABLE_FORMAT, LANCE_TABLE_FORMAT);
    properties.put(Table.PROPERTY_EXTERNAL, "true");
    properties.put(LANCE_TABLE_REGISTER, "true");

    Table registeredTable =
        catalog
            .asTableCatalog()
            .createTable(
                tableIdentifier,
                columns,
                TABLE_COMMENT,
                properties,
                Transforms.EMPTY_TRANSFORM,
                Distributions.NONE,
                new SortOrder[0]);

    Assertions.assertNotNull(registeredTable);

    // Register again with OVERWRITE mode - should replace metadata
    Column[] newColumns =
        new Column[] {
          Column.of(LANCE_COL_NAME1, Types.IntegerType.get(), "col_1_comment"),
          Column.of(LANCE_COL_NAME2, Types.StringType.get(), "col_2_comment"),
          Column.of(LANCE_COL_NAME3, Types.LongType.get(), "col_3_comment")
        };

    Map<String, String> overwriteProperties = createProperties();
    overwriteProperties.put(Table.PROPERTY_LOCATION, location);
    overwriteProperties.put(Table.PROPERTY_TABLE_FORMAT, LANCE_TABLE_FORMAT);
    overwriteProperties.put(Table.PROPERTY_EXTERNAL, "true");
    overwriteProperties.put(LANCE_TABLE_REGISTER, "true");
    overwriteProperties.put(LANCE_CREATION_MODE, "OVERWRITE");

    Table overwrittenTable =
        catalog
            .asTableCatalog()
            .createTable(
                tableIdentifier,
                newColumns,
                "Updated comment",
                overwriteProperties,
                Transforms.EMPTY_TRANSFORM,
                Distributions.NONE,
                new SortOrder[0]);

    Assertions.assertNotNull(overwrittenTable);
    Assertions.assertEquals(3, overwrittenTable.columns().length);
    Assertions.assertEquals("Updated comment", overwrittenTable.comment());

    // Verify physical dataset still exists
    File tableDir = new File(location);
    Assertions.assertTrue(tableDir.exists());
  }

  @Test
  public void testRegisterTableWithCreateModeFailsWhenExists() throws IOException {
    // First, create a physical Lance dataset
    Column[] columns =
        new Column[] {
          Column.of(LANCE_COL_NAME1, Types.IntegerType.get(), "col_1_comment"),
          Column.of(LANCE_COL_NAME2, Types.StringType.get(), "col_2_comment")
        };

    NameIdentifier tableIdentifier =
        NameIdentifier.of(schemaName, GravitinoITUtils.genRandomName(TABLE_PREFIX));
    String location = String.format("%s/%s/%s", tempDirectory, schemaName, tableIdentifier.name());

    // Create a physical Lance dataset
    org.apache.arrow.vector.types.pojo.Schema arrowSchema =
        new org.apache.arrow.vector.types.pojo.Schema(
            Arrays.asList(
                Field.nullable("lance_col_name1", new ArrowType.Int(32, true)),
                Field.nullable("lance_col_name2", new ArrowType.Utf8())));

    try (RootAllocator allocator = new RootAllocator();
        Dataset dataset =
            Dataset.create(allocator, location, arrowSchema, new WriteParams.Builder().build())) {
      // Dataset created
    }

    // Register the table in Gravitino
    Map<String, String> properties = createProperties();
    properties.put(Table.PROPERTY_LOCATION, location);
    properties.put(Table.PROPERTY_TABLE_FORMAT, LANCE_TABLE_FORMAT);
    properties.put(Table.PROPERTY_EXTERNAL, "true");
    properties.put(LANCE_TABLE_REGISTER, "true");

    Table registeredTable =
        catalog
            .asTableCatalog()
            .createTable(
                tableIdentifier,
                columns,
                TABLE_COMMENT,
                properties,
                Transforms.EMPTY_TRANSFORM,
                Distributions.NONE,
                new SortOrder[0]);

    Assertions.assertNotNull(registeredTable);

    // Try to register again with CREATE mode - should fail
    Map<String, String> createProperties = createProperties();
    createProperties.put(Table.PROPERTY_LOCATION, location);
    createProperties.put(Table.PROPERTY_TABLE_FORMAT, LANCE_TABLE_FORMAT);
    createProperties.put(Table.PROPERTY_EXTERNAL, "true");
    createProperties.put(LANCE_TABLE_REGISTER, "true");
    createProperties.put(LANCE_CREATION_MODE, "CREATE");

    Assertions.assertThrows(
        Exception.class,
        () ->
            catalog
                .asTableCatalog()
                .createTable(
                    tableIdentifier,
                    columns,
                    TABLE_COMMENT,
                    createProperties,
                    Transforms.EMPTY_TRANSFORM,
                    Distributions.NONE,
                    new SortOrder[0]));
  }

  @Test
  void testRegisterWithNonExistLocation() {
    // Now try to register a table with non-existing location with CREATE mode - should succeed
    String newTableName = GravitinoITUtils.genRandomName(TABLE_PREFIX);
    NameIdentifier newTableIdentifier = NameIdentifier.of(schemaName, newTableName);
    Map<String, String> newCreateProperties = createProperties();
    String newLocation = String.format("%s/%s/%s/", tempDirectory, schemaName, newTableName);
    boolean dirExists = new File(newLocation).exists();
    Assertions.assertFalse(dirExists);

    newCreateProperties.put(Table.PROPERTY_LOCATION, newLocation);
    newCreateProperties.put(Table.PROPERTY_TABLE_FORMAT, LANCE_TABLE_FORMAT);
    newCreateProperties.put(Table.PROPERTY_EXTERNAL, "true");
    newCreateProperties.put(LANCE_TABLE_REGISTER, "true");

    Table nonExistingTable =
        catalog
            .asTableCatalog()
            .createTable(
                newTableIdentifier,
                new Column[0],
                "Updated comment",
                newCreateProperties,
                Transforms.EMPTY_TRANSFORM,
                Distributions.NONE,
                new SortOrder[0]);

    Assertions.assertNotNull(nonExistingTable);
    Assertions.assertEquals(
        newLocation, nonExistingTable.properties().get(Table.PROPERTY_LOCATION));
    Assertions.assertEquals(dirExists, new File(newLocation).exists());

    // Now try to drop table, there should no problem here
    boolean dropSuccess = catalog.asTableCatalog().dropTable(newTableIdentifier);
    Assertions.assertTrue(dropSuccess);
  }

  @Test
  void testDropCatalogWithManagedAndExternalEntities() {
    // Create a new catalog for this test to avoid interfering with other tests
    String testCatalogName = GravitinoITUtils.genRandomName("drop_catalog_test");
    Map<String, String> catalogProperties = Maps.newHashMap();
    metalake.createCatalog(
        testCatalogName,
        Catalog.Type.RELATIONAL,
        provider,
        "Test catalog for drop",
        catalogProperties);

    Catalog testCatalog = metalake.loadCatalog(testCatalogName);

    // Create a schema
    String testSchemaName = GravitinoITUtils.genRandomName(SCHEMA_PREFIX);
    Map<String, String> schemaProperties = createSchemaProperties();
    testCatalog.asSchemas().createSchema(testSchemaName, "Test schema", schemaProperties);

    Column[] columns = createColumns();

    // Create a managed (non-external) Lance table
    String managedTableName = GravitinoITUtils.genRandomName(TABLE_PREFIX + "_managed");
    NameIdentifier managedTableIdentifier = NameIdentifier.of(testSchemaName, managedTableName);
    String managedTableLocation =
        String.format("%s/%s/%s", tempDirectory, testSchemaName, managedTableName);

    Map<String, String> managedTableProperties = createProperties();
    managedTableProperties.put(Table.PROPERTY_TABLE_FORMAT, "lance");
    managedTableProperties.put(Table.PROPERTY_LOCATION, managedTableLocation);

    Table managedTable =
        testCatalog
            .asTableCatalog()
            .createTable(
                managedTableIdentifier,
                columns,
                "Managed table",
                managedTableProperties,
                Transforms.EMPTY_TRANSFORM,
                null,
                null);

    Assertions.assertNotNull(managedTable);
    File managedTableDir = new File(managedTableLocation);
    Assertions.assertTrue(
        managedTableDir.exists(), "Managed table directory should exist after creation");

    // Create an external Lance table
    String externalTableName = GravitinoITUtils.genRandomName(TABLE_PREFIX + "_external");
    NameIdentifier externalTableIdentifier = NameIdentifier.of(testSchemaName, externalTableName);
    String externalTableLocation =
        String.format("%s/%s/%s", tempDirectory, testSchemaName, externalTableName);

    Map<String, String> externalTableProperties = createProperties();
    externalTableProperties.put(Table.PROPERTY_TABLE_FORMAT, "lance");
    externalTableProperties.put(Table.PROPERTY_LOCATION, externalTableLocation);
    externalTableProperties.put(Table.PROPERTY_EXTERNAL, "true");

    Table externalTable =
        testCatalog
            .asTableCatalog()
            .createTable(
                externalTableIdentifier,
                columns,
                "External table",
                externalTableProperties,
                Transforms.EMPTY_TRANSFORM,
                null,
                null);

    Assertions.assertNotNull(externalTable);
    File externalTableDir = new File(externalTableLocation);
    Assertions.assertTrue(
        externalTableDir.exists(), "External table directory should exist after creation");

    // Verify both tables exist in catalog
    Table loadedManagedTable = testCatalog.asTableCatalog().loadTable(managedTableIdentifier);
    Assertions.assertNotNull(loadedManagedTable);

    Table loadedExternalTable = testCatalog.asTableCatalog().loadTable(externalTableIdentifier);
    Assertions.assertNotNull(loadedExternalTable);

    // Drop the catalog with force=true
    boolean catalogDropped = metalake.dropCatalog(testCatalogName, true);
    Assertions.assertTrue(catalogDropped, "Catalog should be dropped successfully");

    // Verify the catalog is dropped
    Assertions.assertThrows(
        NoSuchCatalogException.class,
        () -> metalake.loadCatalog(testCatalogName),
        "Catalog should not exist after drop");

    // Verify the managed table's physical directory is removed
    Assertions.assertFalse(
        managedTableDir.exists(),
        "Managed table directory should be removed after dropping catalog");

    // Verify the external table's physical directory is preserved
    Assertions.assertTrue(
        externalTableDir.exists(),
        "External table directory should be preserved after dropping catalog");

    // Clean up external table directory manually
    try {
      FileUtils.deleteDirectory(externalTableDir);
    } catch (IOException e) {
      LOG.warn("Failed to delete external table directory: {}", externalTableLocation, e);
    }
  }
}
