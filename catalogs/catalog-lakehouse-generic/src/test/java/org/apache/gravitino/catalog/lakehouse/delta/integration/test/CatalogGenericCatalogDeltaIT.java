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
package org.apache.gravitino.catalog.lakehouse.delta.integration.test;

import com.google.common.collect.Maps;
import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.catalog.lakehouse.delta.DeltaConstants;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for Delta table support in Gravitino generic lakehouse catalog.
 *
 * <p>These tests verify:
 *
 * <ul>
 *   <li>Creating a physical Delta table using Delta Kernel
 *   <li>Registering the Delta table in Gravitino catalog
 *   <li>Loading table metadata from Gravitino
 *   <li>Reading actual Delta table using location from Gravitino metadata
 *   <li>Verifying table still exists after dropping from Gravitino (metadata-only drop)
 * </ul>
 */
public class CatalogGenericCatalogDeltaIT extends BaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogGenericCatalogDeltaIT.class);
  public static final String METALAKE_NAME =
      GravitinoITUtils.genRandomName("CatalogGenericDeltaIT_metalake");

  public String catalogName = GravitinoITUtils.genRandomName("CatalogGenericDeltaIT_catalog");
  public String SCHEMA_PREFIX = "CatalogGenericDelta_schema";
  public String schemaName = GravitinoITUtils.genRandomName(SCHEMA_PREFIX);
  public String TABLE_PREFIX = "CatalogGenericDelta_table";
  public String tableName = GravitinoITUtils.genRandomName(TABLE_PREFIX);
  public static final String TABLE_COMMENT = "Delta table comment";
  public static final String COL_NAME1 = "id";
  public static final String COL_NAME2 = "name";
  protected final String provider = "lakehouse-generic";
  protected GravitinoMetalake metalake;
  protected Catalog catalog;
  protected String tempDirectory;
  protected Engine deltaEngine;

  @BeforeAll
  public void startup() throws Exception {
    createMetalake();
    createCatalog();
    createSchema();

    Path tempDir = Files.createTempDirectory("deltaTempDir");
    tempDirectory = tempDir.toString();

    deltaEngine = DefaultEngine.create(new Configuration());
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
      client.dropMetalake(METALAKE_NAME, true);
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
  public void testCreateDeltaTableAndRegisterToGravitino() throws Exception {
    String tableLocation = tempDirectory + "/" + tableName;

    // Step 1: Create a physical Delta table using Delta Kernel
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER, true).add("name", StringType.STRING, true);

    TransactionBuilder txnBuilder =
        io.delta.kernel.Table.forPath(deltaEngine, tableLocation)
            .createTransactionBuilder(deltaEngine, "test", Operation.CREATE_TABLE);

    txnBuilder
        .withSchema(deltaEngine, schema)
        .withPartitionColumns(deltaEngine, Collections.emptyList())
        .build(deltaEngine)
        .commit(deltaEngine, emptyRowIterable());

    LOG.info("Created Delta table at: {}", tableLocation);

    // Step 2: Register the Delta table in Gravitino catalog
    Column[] gravitinoColumns =
        new Column[] {
          Column.of(COL_NAME1, Types.IntegerType.get(), "id column"),
          Column.of(COL_NAME2, Types.StringType.get(), "name column")
        };

    NameIdentifier nameIdentifier = NameIdentifier.of(schemaName, tableName);
    Map<String, String> properties = createTableProperties();
    properties.put(Table.PROPERTY_TABLE_FORMAT, DeltaConstants.DELTA_TABLE_FORMAT);
    properties.put(Table.PROPERTY_LOCATION, tableLocation);
    properties.put(Table.PROPERTY_EXTERNAL, "true");

    Table gravitinoTable =
        catalog
            .asTableCatalog()
            .createTable(
                nameIdentifier,
                gravitinoColumns,
                TABLE_COMMENT,
                properties,
                Transforms.EMPTY_TRANSFORM,
                null,
                null);

    Assertions.assertEquals(tableName, gravitinoTable.name());
    Assertions.assertEquals(TABLE_COMMENT, gravitinoTable.comment());
    LOG.info("Registered Delta table in Gravitino catalog");

    // Step 3: Load table metadata from Gravitino
    Table loadedTable = catalog.asTableCatalog().loadTable(nameIdentifier);
    Assertions.assertEquals(tableName, loadedTable.name());
    Assertions.assertEquals(2, loadedTable.columns().length);

    // Note: Gravitino may normalize the location by adding trailing slash
    String locationFromMetadata = loadedTable.properties().get(Table.PROPERTY_LOCATION);
    Assertions.assertTrue(
        locationFromMetadata.equals(tableLocation)
            || locationFromMetadata.equals(tableLocation + "/"),
        "Location should match with or without trailing slash");

    // Step 4: Use the location from Gravitino metadata to read actual Delta table
    Assertions.assertNotNull(locationFromMetadata);

    // Read Delta table using Delta Kernel
    io.delta.kernel.Table deltaTable =
        io.delta.kernel.Table.forPath(deltaEngine, locationFromMetadata);
    Snapshot snapshot = deltaTable.getLatestSnapshot(deltaEngine);
    Assertions.assertNotNull(snapshot);

    StructType deltaSchema = snapshot.getSchema(deltaEngine);
    Assertions.assertEquals(2, deltaSchema.fields().size());
    Assertions.assertEquals(COL_NAME1, deltaSchema.fields().get(0).getName());
    Assertions.assertEquals(COL_NAME2, deltaSchema.fields().get(1).getName());

    // Step 5: Drop table from Gravitino catalog (metadata only)
    boolean dropped = catalog.asTableCatalog().dropTable(nameIdentifier);
    Assertions.assertTrue(dropped);

    // Step 6: Verify Delta table still exists at location and can be accessed
    io.delta.kernel.Table deltaTableAfterDrop =
        io.delta.kernel.Table.forPath(deltaEngine, locationFromMetadata);
    Snapshot snapshotAfterDrop = deltaTableAfterDrop.getLatestSnapshot(deltaEngine);
    Assertions.assertNotNull(snapshotAfterDrop);
    Assertions.assertEquals(2, snapshotAfterDrop.getSchema(deltaEngine).fields().size());
  }

  @Test
  public void testCreateDeltaTableWithoutExternalFails() {
    Column[] columns = createColumns();
    NameIdentifier nameIdentifier = NameIdentifier.of(schemaName, tableName);

    Map<String, String> properties = createTableProperties();
    String tableLocation = tempDirectory + "/" + tableName;
    properties.put(Table.PROPERTY_TABLE_FORMAT, DeltaConstants.DELTA_TABLE_FORMAT);
    properties.put(Table.PROPERTY_LOCATION, tableLocation);

    Exception exception =
        Assertions.assertThrows(
            Exception.class,
            () ->
                catalog
                    .asTableCatalog()
                    .createTable(
                        nameIdentifier,
                        columns,
                        TABLE_COMMENT,
                        properties,
                        Transforms.EMPTY_TRANSFORM,
                        null,
                        null));

    Assertions.assertTrue(exception.getMessage().contains("external Delta tables"));
  }

  @Test
  public void testCreateDeltaTableWithoutLocationFails() {
    Column[] columns = createColumns();
    NameIdentifier nameIdentifier = NameIdentifier.of(schemaName, tableName);

    Map<String, String> properties = createTableProperties();
    properties.put(Table.PROPERTY_TABLE_FORMAT, DeltaConstants.DELTA_TABLE_FORMAT);
    properties.put(Table.PROPERTY_EXTERNAL, "true");

    Exception exception =
        Assertions.assertThrows(
            Exception.class,
            () ->
                catalog
                    .asTableCatalog()
                    .createTable(
                        nameIdentifier,
                        columns,
                        TABLE_COMMENT,
                        properties,
                        Transforms.EMPTY_TRANSFORM,
                        null,
                        null));

    Assertions.assertTrue(exception.getMessage().contains("location"));
  }

  @Test
  public void testAlterDeltaTableFails() throws Exception {
    String tableLocation = tempDirectory + "/" + tableName + "_alter";

    // Create physical Delta table
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER, true).add("name", StringType.STRING, true);

    TransactionBuilder txnBuilder =
        io.delta.kernel.Table.forPath(deltaEngine, tableLocation)
            .createTransactionBuilder(deltaEngine, "test", Operation.CREATE_TABLE);

    txnBuilder
        .withSchema(deltaEngine, schema)
        .withPartitionColumns(deltaEngine, Collections.emptyList())
        .build(deltaEngine)
        .commit(deltaEngine, emptyRowIterable());

    // Register in Gravitino
    Column[] columns = createColumns();
    NameIdentifier nameIdentifier = NameIdentifier.of(schemaName, tableName);

    Map<String, String> properties = createTableProperties();
    properties.put(Table.PROPERTY_TABLE_FORMAT, DeltaConstants.DELTA_TABLE_FORMAT);
    properties.put(Table.PROPERTY_LOCATION, tableLocation);
    properties.put(Table.PROPERTY_EXTERNAL, "true");

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

    TableChange addColumn = TableChange.addColumn(new String[] {"new_col"}, Types.StringType.get());

    Exception exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> catalog.asTableCatalog().alterTable(nameIdentifier, addColumn));

    Assertions.assertTrue(exception.getMessage().contains("ALTER TABLE"));
    Assertions.assertTrue(exception.getMessage().contains("not supported"));
  }

  @Test
  public void testPurgeDeltaTableFails() throws Exception {
    String tableLocation = tempDirectory + "/" + tableName + "_purge";

    // Create physical Delta table
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER, true).add("name", StringType.STRING, true);

    TransactionBuilder txnBuilder =
        io.delta.kernel.Table.forPath(deltaEngine, tableLocation)
            .createTransactionBuilder(deltaEngine, "test", Operation.CREATE_TABLE);

    txnBuilder
        .withSchema(deltaEngine, schema)
        .withPartitionColumns(deltaEngine, Collections.emptyList())
        .build(deltaEngine)
        .commit(deltaEngine, emptyRowIterable());

    // Register in Gravitino
    Column[] columns = createColumns();
    NameIdentifier nameIdentifier = NameIdentifier.of(schemaName, tableName);

    Map<String, String> properties = createTableProperties();
    properties.put(Table.PROPERTY_TABLE_FORMAT, DeltaConstants.DELTA_TABLE_FORMAT);
    properties.put(Table.PROPERTY_LOCATION, tableLocation);
    properties.put(Table.PROPERTY_EXTERNAL, "true");

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

    Exception exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> catalog.asTableCatalog().purgeTable(nameIdentifier));

    Assertions.assertTrue(exception.getMessage().contains("Purge"));
    Assertions.assertTrue(exception.getMessage().contains("not supported"));
  }

  @Test
  public void testCreateDeltaTableWithPartitionsThrowsException() {
    Column[] columns = createColumns();
    NameIdentifier nameIdentifier = NameIdentifier.of(schemaName, tableName);

    Map<String, String> properties = createTableProperties();
    String tableLocation = tempDirectory + "/" + tableName;
    properties.put(Table.PROPERTY_TABLE_FORMAT, DeltaConstants.DELTA_TABLE_FORMAT);
    properties.put(Table.PROPERTY_LOCATION, tableLocation);
    properties.put(Table.PROPERTY_EXTERNAL, "true");

    Transform[] partitions = new Transform[] {Transforms.identity("created_at")};

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                catalog
                    .asTableCatalog()
                    .createTable(
                        nameIdentifier,
                        columns,
                        TABLE_COMMENT,
                        properties,
                        partitions,
                        null,
                        null,
                        null));

    Assertions.assertTrue(exception.getMessage().contains("partitioning"));
    Assertions.assertTrue(exception.getMessage().contains("doesn't support"));
  }

  @Test
  public void testCreateDeltaTableWithDistributionThrowsException() {
    Column[] columns = createColumns();
    NameIdentifier nameIdentifier = NameIdentifier.of(schemaName, tableName);

    Map<String, String> properties = createTableProperties();
    String tableLocation = tempDirectory + "/" + tableName;
    properties.put(Table.PROPERTY_TABLE_FORMAT, DeltaConstants.DELTA_TABLE_FORMAT);
    properties.put(Table.PROPERTY_LOCATION, tableLocation);
    properties.put(Table.PROPERTY_EXTERNAL, "true");

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                catalog
                    .asTableCatalog()
                    .createTable(
                        nameIdentifier,
                        columns,
                        TABLE_COMMENT,
                        properties,
                        null,
                        Distributions.hash(5, NamedReference.field(COL_NAME1)),
                        null,
                        null));

    Assertions.assertTrue(exception.getMessage().contains("distribution"));
    Assertions.assertTrue(exception.getMessage().contains("doesn't support"));
  }

  @Test
  public void testCreateDeltaTableWithSortOrdersThrowsException() {
    Column[] columns = createColumns();
    NameIdentifier nameIdentifier = NameIdentifier.of(schemaName, tableName);

    Map<String, String> properties = createTableProperties();
    String tableLocation = tempDirectory + "/" + tableName;
    properties.put(Table.PROPERTY_TABLE_FORMAT, DeltaConstants.DELTA_TABLE_FORMAT);
    properties.put(Table.PROPERTY_LOCATION, tableLocation);
    properties.put(Table.PROPERTY_EXTERNAL, "true");

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                catalog
                    .asTableCatalog()
                    .createTable(
                        nameIdentifier,
                        columns,
                        TABLE_COMMENT,
                        properties,
                        null,
                        null,
                        new SortOrder[] {SortOrders.ascending(NamedReference.field(COL_NAME1))},
                        null));

    Assertions.assertTrue(exception.getMessage().contains("sort orders"));
    Assertions.assertTrue(exception.getMessage().contains("doesn't support"));
  }

  @Test
  public void testCreateDeltaTableWithIndexesThrowsException() {
    Column[] columns = createColumns();
    NameIdentifier nameIdentifier = NameIdentifier.of(schemaName, tableName);

    Map<String, String> properties = createTableProperties();
    String tableLocation = tempDirectory + "/" + tableName;
    properties.put(Table.PROPERTY_TABLE_FORMAT, DeltaConstants.DELTA_TABLE_FORMAT);
    properties.put(Table.PROPERTY_LOCATION, tableLocation);
    properties.put(Table.PROPERTY_EXTERNAL, "true");

    Index[] indexes = new Index[] {Indexes.primary("pk_id", new String[][] {{COL_NAME1}})};

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                catalog
                    .asTableCatalog()
                    .createTable(
                        nameIdentifier,
                        columns,
                        TABLE_COMMENT,
                        properties,
                        null,
                        null,
                        null,
                        indexes));

    Assertions.assertTrue(exception.getMessage().contains("indexes"));
    Assertions.assertTrue(exception.getMessage().contains("doesn't support"));
  }

  protected Map<String, String> createSchemaProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    return properties;
  }

  private void createMetalake() {
    GravitinoMetalake[] gravitinoMetalakes = client.listMetalakes();
    Assertions.assertEquals(0, gravitinoMetalakes.length);

    client.createMetalake(METALAKE_NAME, "comment", Collections.emptyMap());
    GravitinoMetalake loadMetalake = client.loadMetalake(METALAKE_NAME);
    Assertions.assertEquals(METALAKE_NAME, loadMetalake.name());

    metalake = loadMetalake;
  }

  protected void createCatalog() {
    Map<String, String> properties = Maps.newHashMap();
    metalake.createCatalog(catalogName, Catalog.Type.RELATIONAL, provider, "comment", properties);

    catalog = metalake.loadCatalog(catalogName);
  }

  private void createSchema() throws InterruptedException {
    Map<String, String> schemaProperties = createSchemaProperties();
    String comment = "schema comment";
    catalog.asSchemas().createSchema(schemaName, comment, schemaProperties);
    Schema loadSchema = catalog.asSchemas().loadSchema(schemaName);
    Assertions.assertEquals(schemaName, loadSchema.name());
    Assertions.assertEquals(comment, loadSchema.comment());
    Assertions.assertEquals("val1", loadSchema.properties().get("key1"));
    Assertions.assertEquals("val2", loadSchema.properties().get("key2"));
  }

  private Column[] createColumns() {
    Column col1 = Column.of(COL_NAME1, Types.IntegerType.get(), "id column");
    Column col2 = Column.of(COL_NAME2, Types.StringType.get(), "name column");
    Column col3 = Column.of("created_at", Types.DateType.get(), "created_at column");
    return new Column[] {col1, col2, col3};
  }

  protected Map<String, String> createTableProperties() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    return properties;
  }

  /**
   * Helper method to create an empty {@code CloseableIterable<Row>} for Delta Kernel transaction
   * commits.
   */
  private static CloseableIterable<Row> emptyRowIterable() {
    return new CloseableIterable<Row>() {

      @Override
      public CloseableIterator<Row> iterator() {
        return new CloseableIterator<Row>() {
          @Override
          public void close() throws IOException {
            // No resources to close
          }

          @Override
          public boolean hasNext() {
            return false;
          }

          @Override
          public Row next() {
            throw new java.util.NoSuchElementException("Empty iterator");
          }
        };
      }

      @Override
      public void close() throws IOException {
        // No resources to close
      }
    };
  }
}
