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
package org.apache.gravitino.catalog.lakehouse.integration.test;

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
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatalogGenericLakehouseLanceIT extends BaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogGenericLakehouseLanceIT.class);
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
  protected final String provider = "generic-lakehouse";
  protected final ContainerSuite containerSuite = ContainerSuite.getInstance();
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
    File file = new File(tempDirectory);
    file.deleteOnExit();
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
  }

  @AfterEach
  public void resetSchema() throws InterruptedException {
    catalog.asSchemas().dropSchema(schemaName, true);
    createSchema();
  }

  @Test
  public void testCreateLanceTable() throws InterruptedException {
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

    createdTable =
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
    createdTableProperties = createdTable.properties();
    Assertions.assertEquals("lance", createdTableProperties.get("format"));

    Assertions.assertEquals(TABLE_COMMENT, createdTable.comment());
    Assertions.assertEquals(3, createdTable.columns().length);
    columnEquals(columns, createdTable.columns());
    expectedTableLocation = schemaLocation + "/" + tableName + "/";
    Assertions.assertEquals(expectedTableLocation, createdTableProperties.get("location"));
    Assertions.assertTrue(new File(expectedTableLocation).exists());

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
}
