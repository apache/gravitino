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

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.gravitino.Schema;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.SchemaDTO;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.TableDTO;
import org.apache.gravitino.dto.rel.partitioning.IdentityPartitioningDTO;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestHiveClient {

  private static final String HIVE2_HMS_URL = "thrift://172.17.0.2:9083";
  private static final String HIVE2_HDFS_URL = "hdfs://172.17.0.2:9000";
  private static final String HIVE3_HMS_URL = "thrift://172.17.0.3:9083";
  private static final String HIVE3_HDFS_URL = "hdfs://172.17.0.3:9000";

  @Test
  void testHive2Client() throws Exception {
    runHiveClientTest("HIVE2", "", "hive2", HIVE2_HMS_URL, HIVE2_HDFS_URL + "/tmp/gravitino_test");
  }

  @Test
  void testHive3DefaultCatalog() throws Exception {
    // Hive3 default catalog is "hive", not empty string
    runHiveClientTest(
        "HIVE3", "hive", "hive3_default", HIVE3_HMS_URL, HIVE3_HDFS_URL + "/tmp/gravitino_test");
  }

  @Test
  void testHive3SampleCatalog() throws Exception {
    runHiveClientTest(
        "HIVE3",
        "sample_catalog",
        "hive3_sample",
        HIVE3_HMS_URL,
        HIVE3_HDFS_URL + "/tmp/gravitino_test");
  }

  private void runHiveClientTest(
      String version,
      String catalogName,
      String testPrefix,
      String metastoreUri,
      String hdfsBasePath)
      throws Exception {
    Properties properties = new Properties();
    properties.setProperty("hive.metastore.uris", metastoreUri);
    IsolatedClientLoader loader = IsolatedClientLoader.forVersion(version, Map.of());
    HiveClient client = loader.createClient(properties);

    String dbName = "gt_" + testPrefix + "_db_" + UUID.randomUUID().toString().replace("-", "");
    String tableName = "gt_" + testPrefix + "_tbl_" + UUID.randomUUID().toString().replace("-", "");
    String partitionValue = "p_" + UUID.randomUUID().toString().replace("-", "");
    String partitionName = "dt=" + partitionValue;

    String dbLocation = hdfsBasePath + "/" + dbName;
    String tableLocation = hdfsBasePath + "/" + tableName;

    Schema schema = createTestSchema(dbName, dbLocation);
    Table table = createTestTable(tableName, tableLocation);
    Partition partition = createTestPartition(partitionName, partitionValue);

    try {
      // Test database operations
      client.createDatabase(catalogName, schema);
      List<String> allDatabases = client.getAllDatabases(catalogName);
      Assertions.assertTrue(allDatabases.contains(dbName), "Database should be in the list");

      Schema loadedDb = client.getDatabase(catalogName, dbName);
      Assertions.assertNotNull(loadedDb, "Loaded database should not be null");
      Assertions.assertEquals(dbName, loadedDb.name(), "Database name should match");
      Assertions.assertEquals(
          schema.comment(), loadedDb.comment(), "Database comment should match");

      client.alterDatabase(catalogName, dbName, schema);
      Schema alteredDb = client.getDatabase(catalogName, dbName);
      Assertions.assertNotNull(alteredDb, "Altered database should not be null");

      // Test table operations
      client.createTable(catalogName, dbName, table);
      List<String> allTables = client.getAllTables(catalogName, dbName);
      Assertions.assertTrue(allTables.contains(tableName), "Table should be in the list");

      Table loadedTable = client.getTable(catalogName, dbName, tableName);
      Assertions.assertNotNull(loadedTable, "Loaded table should not be null");
      Assertions.assertEquals(tableName, loadedTable.name(), "Table name should match");
      Assertions.assertEquals(table.comment(), loadedTable.comment(), "Table comment should match");
      Assertions.assertEquals(2, loadedTable.columns().length, "Table should have 2 columns");
      Assertions.assertEquals(
          1, loadedTable.partitioning().length, "Table should have 1 partition key");

      client.alterTable(catalogName, dbName, tableName, loadedTable);
      Table alteredTable = client.getTable(catalogName, dbName, tableName);
      Assertions.assertNotNull(alteredTable, "Altered table should not be null");

      List<String> filteredTables =
          client.listTableNamesByFilter(catalogName, dbName, tableName, (short) 10);
      Assertions.assertTrue(
          filteredTables.contains(tableName), "Filtered tables should contain the table");

      List<Table> tableObjects =
          client.getTableObjectsByName(catalogName, dbName, List.of(tableName));
      Assertions.assertEquals(1, tableObjects.size(), "Should get exactly one table object");
      Assertions.assertEquals(
          tableName, tableObjects.get(0).name(), "Table object name should match");

      // Test partition operations
      Partition addedPartition = client.addPartition(catalogName, dbName, loadedTable, partition);
      Assertions.assertNotNull(addedPartition, "Added partition should not be null");
      Assertions.assertEquals(partitionName, addedPartition.name(), "Partition name should match");

      List<String> partitionNames =
          client.listPartitionNames(catalogName, dbName, tableName, (short) 10);
      Assertions.assertTrue(
          partitionNames.contains(partitionName), "Partition should be in the list");

      List<Partition> partitions =
          client.listPartitions(catalogName, dbName, loadedTable, (short) 10);
      Assertions.assertEquals(1, partitions.size(), "Should have exactly one partition");
      Assertions.assertEquals(
          partitionName, partitions.get(0).name(), "Partition name should match");

      List<Partition> filteredPartitions =
          client.listPartitions(
              catalogName, dbName, loadedTable, List.of(partitionValue), (short) 10);
      Assertions.assertEquals(
          1, filteredPartitions.size(), "Should have exactly one filtered partition");

      Partition fetchedPartition =
          client.getPartition(catalogName, dbName, loadedTable, addedPartition.name());
      Assertions.assertNotNull(fetchedPartition, "Fetched partition should not be null");
      Assertions.assertEquals(
          partitionName, fetchedPartition.name(), "Fetched partition name should match");

      client.dropPartition(catalogName, dbName, tableName, addedPartition.name(), true);
      List<String> partitionNamesAfterDrop =
          client.listPartitionNames(catalogName, dbName, tableName, (short) 10);
      Assertions.assertFalse(
          partitionNamesAfterDrop.contains(partitionName),
          "Partition should not be in the list after drop");

      // Test delegation token (may not be available in all environments)
      try {
        String token =
            client.getDelegationToken(
                System.getProperty("user.name"), System.getProperty("user.name"));
        Assertions.assertNotNull(token, "Delegation token should not be null");
      } catch (Exception e) {
        // Delegation token may not be available, this is acceptable
      }

      // Cleanup
      client.dropTable(catalogName, dbName, tableName, true, true);
      List<String> tablesAfterDrop = client.getAllTables(catalogName, dbName);
      Assertions.assertFalse(
          tablesAfterDrop.contains(tableName), "Table should not be in the list after drop");

      client.dropDatabase(catalogName, dbName, true);
      List<String> databasesAfterDrop = client.getAllDatabases(catalogName);
      Assertions.assertFalse(
          databasesAfterDrop.contains(dbName), "Database should not be in the list after drop");
    } finally {
      safelyDropTable(client, catalogName, dbName, tableName);
      safelyDropDatabase(client, catalogName, dbName);
    }
  }

  private Schema createTestSchema(String dbName, String location) {
    Map<String, String> properties = new HashMap<>();
    properties.put(HiveConstants.LOCATION, location);
    return SchemaDTO.builder()
        .withName(dbName)
        .withComment("Test schema for HiveClient operations")
        .withProperties(properties)
        .withAudit(defaultAudit())
        .build();
  }

  private Table createTestTable(String tableName, String location) {
    ColumnDTO idColumn =
        ColumnDTO.builder()
            .withName("id")
            .withDataType(Types.IntegerType.get())
            .withNullable(false)
            .build();
    ColumnDTO dtColumn =
        ColumnDTO.builder().withName("dt").withDataType(Types.StringType.get()).build();
    Map<String, String> properties = new HashMap<>();
    properties.put(HiveConstants.LOCATION, location);
    return TableDTO.builder()
        .withName(tableName)
        .withColumns(new ColumnDTO[] {idColumn, dtColumn})
        .withComment("Test table for HiveClient operations")
        .withProperties(properties)
        .withAudit(defaultAudit())
        .withPartitioning(new IdentityPartitioningDTO[] {IdentityPartitioningDTO.of("dt")})
        .build();
  }

  private Partition createTestPartition(String partitionName, String value) {
    return Partitions.identity(
        partitionName,
        new String[][] {new String[] {"dt"}},
        new org.apache.gravitino.rel.expressions.literals.Literal<?>[] {
          Literals.stringLiteral(value)
        },
        Map.of());
  }

  private AuditDTO defaultAudit() {
    return AuditDTO.builder()
        .withCreator(System.getProperty("user.name", "gravitino"))
        .withCreateTime(Instant.now())
        .build();
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
