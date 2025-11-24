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

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Comparator;
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
import org.junit.jupiter.api.Test;

public class TestHiveClient {

  @Test
  void testHive2Client() throws Exception {
    String catalogName = "sample_catalog";
    String version = "HIVE3";
    Properties properties = new Properties();
    // properties.setProperty("hive.metastore.uris", "thrift://localhost:9083");
    properties.setProperty("hive.metastore.uris", "thrift://172.17.0.3:9083");
    IsolatedClientLoader loader = IsolatedClientLoader.forVersion(version, Map.of());
    HiveClient client = loader.createClient(properties);

    String dbName = "gt_hive2_db_" + UUID.randomUUID().toString().replace("-", "");
    String tableName = "gt_hive2_tbl_" + UUID.randomUUID().toString().replace("-", "");
    String partitionValue = "p_" + UUID.randomUUID().toString().replace("-", "");
    String partitionName = "dt=" + partitionValue;

    Path tempRoot = Files.createTempDirectory("hiveclient-hive2-");
    Path dbLocation = tempRoot.resolve(dbName);
    Path tableLocation = tempRoot.resolve(tableName);

    Schema schema = createTestSchema(dbName, dbLocation);
    Table table = createTestTable(tableName, tableLocation);
    Partition partition = createTestPartition(partitionName, partitionValue);

    client.createDatabase(catalogName, schema);
    System.out.println("Created database: " + dbName);

    try {
      List<String> allDatabases = client.getAllDatabases(catalogName);
      System.out.println("All databases: " + allDatabases);

      Schema loadedDb = client.getDatabase(catalogName, dbName);
      System.out.println("Loaded database: " + loadedDb.name());

      client.alterDatabase(catalogName, dbName, schema);
      System.out.println("Altered database: " + dbName);

      client.createTable(catalogName, dbName, table);
      System.out.println("Created table: " + tableName);

      Table loadedTable = client.getTable(catalogName, dbName, tableName);
      System.out.println("Loaded table: " + loadedTable.name());

      client.alterTable(catalogName, dbName, tableName, loadedTable);
      System.out.println("Altered table: " + tableName);

      List<String> allTables = client.getAllTables(catalogName, dbName);
      System.out.println("All tables: " + allTables);

      List<String> filteredTables =
          client.listTableNamesByFilter(catalogName, dbName, tableName, (short) 10);
      System.out.println("Filtered tables: " + filteredTables);

      List<Table> tableObjects = client.getTableObjectsByName(dbName, List.of(tableName));
      System.out.println("Table objects: " + tableObjects.size());

      Partition addedPartition = client.addPartition(catalogName, dbName, loadedTable, partition);
      System.out.println("Added partition: " + addedPartition.name());

      List<String> partitionNames =
          client.listPartitionNames(catalogName, dbName, tableName, (short) 10);
      System.out.println("Partition names: " + partitionNames);

      List<Partition> partitions =
          client.listPartitions(catalogName, dbName, loadedTable, (short) 10);
      System.out.println("Partitions count: " + partitions.size());

      List<Partition> filteredPartitions =
          client.listPartitions(
              catalogName, dbName, loadedTable, List.of(partitionValue), (short) 10);
      System.out.println("Filtered partitions count: " + filteredPartitions.size());

      Partition fetchedPartition =
          client.getPartition(catalogName, dbName, loadedTable, addedPartition.name());
      System.out.println("Fetched partition: " + fetchedPartition.name());

      client.dropPartition(catalogName, dbName, tableName, addedPartition.name(), true);
      System.out.println("Dropped partition: " + addedPartition.name());

      try {
        String token =
            client.getDelegationToken(
                System.getProperty("user.name"), System.getProperty("user.name"));
        System.out.println("Delegation token: " + token);
      } catch (Exception e) {
        System.out.println("Delegation token not available: " + e.getMessage());
      }

      client.dropTable(catalogName, dbName, tableName, true, true);
      System.out.println("Dropped table: " + tableName);

      client.dropDatabase(catalogName, dbName, true);
      System.out.println("Dropped database: " + dbName);
    } finally {
      safelyDropTable(client, catalogName, dbName, tableName);
      safelyDropDatabase(client, catalogName, dbName);
      deleteRecursively(tempRoot);
    }
  }

  /*
  void testHive3Client() throws Exception {
    String catalogName = "";
    String version = "HIVE3";
    Properties properties = new Properties();
    properties.setProperty("hive.metastore.uris", "thrift://172.17.0.3:9083");
    IsolatedClientLoader loader = IsolatedClientLoader.forVersion(version, Map.of());
    HiveClient client = loader.createClient(properties);
    List<String> allDatabases = client.getAllDatabases(catalogName);
    System.out.println("Databases: " + allDatabases);
    Schema db = client.getDatabase(catalogName, "default");
    System.out.println("Database s1: " + db.name());
  }
    */

  private Schema createTestSchema(String dbName, Path location) throws Exception {
    Files.createDirectories(location);
    Map<String, String> properties = new HashMap<>();
    properties.put(HiveConstants.LOCATION, location.toUri().toString());
    return SchemaDTO.builder()
        .withName(dbName)
        .withComment("Test schema for HiveClient operations")
        .withProperties(properties)
        .withAudit(defaultAudit())
        .build();
  }

  private Table createTestTable(String tableName, Path location) throws Exception {
    Files.createDirectories(location);
    ColumnDTO idColumn =
        ColumnDTO.builder()
            .withName("id")
            .withDataType(Types.IntegerType.get())
            .withNullable(false)
            .build();
    ColumnDTO dtColumn =
        ColumnDTO.builder().withName("dt").withDataType(Types.StringType.get()).build();
    Map<String, String> properties = new HashMap<>();
    properties.put(HiveConstants.LOCATION, location.toUri().toString());
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

  private void deleteRecursively(Path root) {
    try {
      if (root == null || !Files.exists(root)) {
        return;
      }
      Files.walk(root)
          .sorted(Comparator.reverseOrder())
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (Exception ignored) {
                }
              });
    } catch (Exception ignored) {
      // ignore cleanup failures
    }
  }
}
