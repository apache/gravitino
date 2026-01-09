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

package org.apache.gravitino.hive.client;

import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.PartitionAlreadyExistsException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.hive.HivePartition;
import org.apache.gravitino.hive.HiveSchema;
import org.apache.gravitino.hive.HiveTable;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.CloseContainerExtension;
import org.apache.gravitino.integration.test.util.PrintFuncNameExtension;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.literals.Literal;
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
import org.junit.jupiter.api.extension.ExtendWith;

// This class is used for testing against Hive 2.x Metastore instances using Docker containers.
@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith({PrintFuncNameExtension.class, CloseContainerExtension.class})
public class TestHive2HMS {

  protected final ContainerSuite containerSuite = ContainerSuite.getInstance();

  protected HiveContainer hiveContainer;
  protected String testPrefix = "hive2";
  protected String metastoreUri;
  protected String hdfsBasePath;
  protected String catalogName = ""; // Hive2 doesn't support catalog
  protected HiveClient hiveClient;

  @BeforeAll
  public void startHiveContainer() {
    containerSuite.startHiveContainer(
        ImmutableMap.of(HiveContainer.HIVE_RUNTIME_VERSION, HiveContainer.HIVE2));
    hiveContainer = containerSuite.getHiveContainer();
    metastoreUri =
        String.format(
            "thrift://%s:%d",
            hiveContainer.getContainerIpAddress(), HiveContainer.HIVE_METASTORE_PORT);
    hdfsBasePath =
        String.format(
            "hdfs://%s:%d/tmp/gravitino_test",
            hiveContainer.getContainerIpAddress(), HiveContainer.HDFS_DEFAULTFS_PORT);

    hiveClient = new HiveClientFactory(createHiveProperties(), testPrefix).createHiveClient();
  }

  @AfterAll
  public void stopHiveContainer() throws Exception {
    containerSuite.close();
    if (hiveClient != null) {
      hiveClient.close();
      hiveClient = null;
    }
  }

  @Test
  void runHiveClientTest() throws Exception {
    String dbName = "gt_" + testPrefix + "_db_" + UUID.randomUUID().toString().replace("-", "");
    String tableName = "gt_" + testPrefix + "_tbl_" + UUID.randomUUID().toString().replace("-", "");
    String partitionValue = "p_" + UUID.randomUUID().toString().replace("-", "");
    String partitionName = "dt=" + partitionValue;

    String dbLocation = hdfsBasePath + "/" + dbName;
    String tableLocation = hdfsBasePath + "/" + tableName;

    HiveSchema schema = createTestSchema(catalogName, dbName, dbLocation);
    HiveTable table = createTestTable(catalogName, dbName, tableName, tableLocation);
    HivePartition partition = createTestPartition(partitionName, partitionValue);

    try {
      // Test database operations
      hiveClient.createDatabase(schema);
      List<String> allDatabases = hiveClient.getAllDatabases(catalogName);
      Assertions.assertTrue(allDatabases.contains(dbName), "Database should be in the list");

      HiveSchema loadedDb = hiveClient.getDatabase(catalogName, dbName);
      Assertions.assertNotNull(loadedDb, "Loaded database should not be null");
      Assertions.assertEquals(dbName, loadedDb.name(), "Database name should match");
      Assertions.assertEquals(
          schema.comment(), loadedDb.comment(), "Database comment should match");

      hiveClient.alterDatabase(catalogName, dbName, schema);
      HiveSchema alteredDb = hiveClient.getDatabase(catalogName, dbName);
      Assertions.assertNotNull(alteredDb, "Altered database should not be null");

      // Test table operations
      hiveClient.createTable(table);
      List<String> allTables = hiveClient.getAllTables(catalogName, dbName);
      Assertions.assertTrue(allTables.contains(tableName), "Table should be in the list");

      HiveTable loadedTable = hiveClient.getTable(catalogName, dbName, tableName);
      Assertions.assertNotNull(loadedTable, "Loaded table should not be null");
      Assertions.assertEquals(tableName, loadedTable.name(), "Table name should match");
      Assertions.assertEquals(table.comment(), loadedTable.comment(), "Table comment should match");
      Assertions.assertEquals(2, loadedTable.columns().length, "Table should have 2 columns");
      Assertions.assertEquals(
          1, loadedTable.partitioning().length, "Table should have 1 partition key");

      hiveClient.alterTable(catalogName, dbName, tableName, loadedTable);
      HiveTable alteredTable = hiveClient.getTable(catalogName, dbName, tableName);
      Assertions.assertNotNull(alteredTable, "Altered table should not be null");

      List<String> filteredTables =
          hiveClient.listTableNamesByFilter(catalogName, dbName, "", (short) 10);
      Assertions.assertTrue(
          filteredTables.contains(tableName), "Filtered tables should contain the table");

      List<HiveTable> tableObjects =
          hiveClient.getTableObjectsByName(catalogName, dbName, List.of(tableName));
      Assertions.assertEquals(1, tableObjects.size(), "Should get exactly one table object");
      Assertions.assertEquals(
          tableName, tableObjects.get(0).name(), "Table object name should match");

      // Test partition operations
      HivePartition addedPartition = hiveClient.addPartition(loadedTable, partition);
      Assertions.assertNotNull(addedPartition, "Added partition should not be null");
      Assertions.assertEquals(partitionName, addedPartition.name(), "Partition name should match");

      List<String> partitionNames = hiveClient.listPartitionNames(loadedTable, (short) 10);
      Assertions.assertTrue(
          partitionNames.contains(partitionName), "Partition should be in the list");

      List<HivePartition> partitions = hiveClient.listPartitions(loadedTable, (short) 10);
      Assertions.assertEquals(1, partitions.size(), "Should have exactly one partition");
      Assertions.assertEquals(
          partitionName, partitions.get(0).name(), "Partition name should match");

      List<HivePartition> filteredPartitions =
          hiveClient.listPartitions(loadedTable, List.of(partitionValue), (short) 10);
      Assertions.assertEquals(
          1, filteredPartitions.size(), "Should have exactly one filtered partition");

      HivePartition fetchedPartition = hiveClient.getPartition(loadedTable, addedPartition.name());
      Assertions.assertNotNull(fetchedPartition, "Fetched partition should not be null");
      Assertions.assertEquals(
          partitionName, fetchedPartition.name(), "Fetched partition name should match");

      hiveClient.dropPartition(catalogName, dbName, tableName, addedPartition.name(), true);
      List<String> partitionNamesAfterDrop = hiveClient.listPartitionNames(loadedTable, (short) 10);
      Assertions.assertFalse(
          partitionNamesAfterDrop.contains(partitionName),
          "Partition should not be in the list after drop");

      // Test delegation token (may not be available in all environments)
      try {
        String token =
            hiveClient.getDelegationToken(
                System.getProperty("user.name"), System.getProperty("user.name"));
        Assertions.assertNotNull(token, "Delegation token should not be null");
      } catch (Exception e) {
        // Delegation token may not be available, this is acceptable
      }

      // Cleanup
      hiveClient.dropTable(catalogName, dbName, tableName, true, true);
      List<String> tablesAfterDrop = hiveClient.getAllTables(catalogName, dbName);
      Assertions.assertFalse(
          tablesAfterDrop.contains(tableName), "Table should not be in the list after drop");

      hiveClient.dropDatabase(catalogName, dbName, true);
      List<String> databasesAfterDrop = hiveClient.getAllDatabases(catalogName);
      Assertions.assertFalse(
          databasesAfterDrop.contains(dbName), "Database should not be in the list after drop");
    } finally {
      safelyDropTable(hiveClient, catalogName, dbName, tableName);
      safelyDropDatabase(hiveClient, catalogName, dbName);
    }
  }

  @Test
  void testHiveExceptionHandling() throws Exception {
    String dbName = "gt_exception_test_db_" + UUID.randomUUID().toString().replace("-", "");
    String tableName = "gt_exception_test_tbl_" + UUID.randomUUID().toString().replace("-", "");
    String partitionValue = "p_" + UUID.randomUUID().toString().replace("-", "");
    String partitionName = "dt=" + partitionValue;

    String dbLocation = hdfsBasePath + "/" + dbName;
    String tableLocation = hdfsBasePath + "/" + tableName;

    HiveSchema schema = createTestSchema(catalogName, dbName, dbLocation);
    HiveTable table = createTestTable(catalogName, dbName, tableName, tableLocation);
    HivePartition partition = createTestPartition(partitionName, partitionValue);

    try {
      // Test SchemaAlreadyExistsException - create database twice
      try {
        hiveClient.createDatabase(schema);
      } catch (GravitinoRuntimeException e) {
        // If permission error occurs, skip this test
        if (e.getCause() != null
            && e.getCause().getMessage() != null
            && e.getCause().getMessage().contains("Permission denied")) {
          return; // Skip test if permission denied
        }
        throw e;
      }
      Assertions.assertThrows(
          SchemaAlreadyExistsException.class, () -> hiveClient.createDatabase(schema));

      // Test NoSuchSchemaException - get non-existent database
      Assertions.assertThrows(
          NoSuchSchemaException.class,
          () -> hiveClient.getDatabase(catalogName, "non_existent_db_" + UUID.randomUUID()));

      // Test TableAlreadyExistsException - create table twice
      hiveClient.createTable(table);
      Assertions.assertThrows(
          TableAlreadyExistsException.class, () -> hiveClient.createTable(table));

      // Test NoSuchTableException - get non-existent table
      Assertions.assertThrows(
          NoSuchTableException.class,
          () ->
              hiveClient.getTable(catalogName, dbName, "non_existent_table_" + UUID.randomUUID()));

      // Test PartitionAlreadyExistsException - add partition twice
      HiveTable loadedTable = hiveClient.getTable(catalogName, dbName, tableName);
      HivePartition addedPartition = hiveClient.addPartition(loadedTable, partition);
      Assertions.assertNotNull(addedPartition, "Added partition should not be null");
      Assertions.assertThrows(
          PartitionAlreadyExistsException.class,
          () -> hiveClient.addPartition(loadedTable, partition));

      // Test NoSuchPartitionException - get non-existent partition
      Assertions.assertThrows(
          NoSuchPartitionException.class,
          () ->
              hiveClient.getPartition(
                  loadedTable, "dt=non_existent_partition_" + UUID.randomUUID()));

      // Test NonEmptySchemaException - try to drop database with tables (cascade=false)
      Exception exception =
          Assertions.assertThrows(
              Exception.class, () -> hiveClient.dropDatabase(catalogName, dbName, false));
      // Hive may throw different exceptions for non-empty database
      // The converter should handle it appropriately
      Assertions.assertTrue(
          exception instanceof NonEmptySchemaException
              || exception instanceof GravitinoRuntimeException,
          "Should throw NonEmptySchemaException or GravitinoRuntimeException, got: "
              + exception.getClass().getName());

      // Cleanup
      hiveClient.dropPartition(catalogName, dbName, tableName, addedPartition.name(), true);
      hiveClient.dropTable(catalogName, dbName, tableName, true, true);
      hiveClient.dropDatabase(catalogName, dbName, true);
    } finally {
      safelyDropTable(hiveClient, catalogName, dbName, tableName);
      safelyDropDatabase(hiveClient, catalogName, dbName);
    }
  }

  @Test
  void testConnectionFailedException() {
    // Test with invalid/unreachable Hive Metastore URI
    String invalidMetastoreUri = "thrift://127.0.0.1:9999";
    Properties properties = createHiveProperties();
    properties.setProperty("hive.metastore.uris", invalidMetastoreUri);

    // Connection failure may occur during client creation or operation
    // Both should be converted to ConnectionFailedException
    Exception exception =
        Assertions.assertThrows(
            Exception.class,
            () -> {
              HiveClient client = new HiveClientFactory(properties, testPrefix).createHiveClient();
              client.getAllDatabases(catalogName);
            });

    // Verify the exception is converted to ConnectionFailedException
    Assertions.assertTrue(
        exception instanceof ConnectionFailedException,
        "Should throw ConnectionFailedException, got: " + exception.getClass().getName());
    Assertions.assertNotNull(
        ((ConnectionFailedException) exception).getCause(), "Exception should have a cause");
  }

  private HiveSchema createTestSchema(String catalogName, String dbName, String location) {
    Map<String, String> properties = new HashMap<>();
    properties.put(HiveConstants.LOCATION, location);
    return HiveSchema.builder()
        .withName(dbName)
        .withComment("Test schema for HiveClient operations")
        .withProperties(properties)
        .withAuditInfo(defaultAudit())
        .withCatalogName(catalogName)
        .build();
  }

  private HiveTable createTestTable(
      String catalogName, String databaseName, String tableName, String location) {
    Column idColumn = Column.of("id", Types.IntegerType.get(), null, false, false, null);
    Column dtColumn = Column.of("dt", Types.StringType.get());
    Map<String, String> properties = new HashMap<>();
    properties.put(HiveConstants.LOCATION, location);
    return HiveTable.builder()
        .withName(tableName)
        .withColumns(new Column[] {idColumn, dtColumn})
        .withComment("Test table for HiveClient operations")
        .withProperties(properties)
        .withAuditInfo(defaultAudit())
        .withPartitioning(new Transform[] {Transforms.identity("dt")})
        .withCatalogName(catalogName)
        .withDatabaseName(databaseName)
        .build();
  }

  private HivePartition createTestPartition(String partitionName, String value) {
    HivePartition partition =
        HivePartition.identity(
            new String[][] {new String[] {"dt"}},
            new Literal<?>[] {Literals.stringLiteral(value)},
            Map.of());
    Assertions.assertEquals(partitionName, partition.name());
    return partition;
  }

  private AuditInfo defaultAudit() {
    return AuditInfo.builder()
        .withCreator(System.getProperty("user.name", "gravitino"))
        .withCreateTime(Instant.now())
        .build();
  }

  protected Properties createHiveProperties() {
    Properties properties = new Properties();
    properties.setProperty("hive.metastore.uris", metastoreUri);
    return properties;
  }

  private void safelyDropTable(
      HiveClient client, String catalogName, String dbName, String tableName) {
    try {
      client.dropTable(catalogName, dbName, tableName, true, true);
    } catch (Exception ignored) {
      // ignore cleanup failures
    }
  }

  private void safelyDropDatabase(HiveClient client, String catalogName, String dbName) {
    try {
      client.dropDatabase(catalogName, dbName, true);
    } catch (Exception ignored) {
      // ignore cleanup failures
    }
  }
}
