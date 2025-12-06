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
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

// This class is use for manual testing against real Hive Metastore instances.
// @Disabled
public class TestHiveClient {

  private static final String HIVE2_HMS_URL = "thrift://172.17.0.4:9083";
  private static final String HIVE2_HDFS_URL = "hdfs://172.17.0.4:9000";
  private static final String HIVE3_HMS_URL = "thrift://172.17.0.3:9083";
  private static final String HIVE3_HDFS_URL = "hdfs://172.17.0.3:9000";

  private static final String KERBEROS_HIVE2_HMS_URL = "thrift://172.17.0.2:9083";
  private static final String KERBEROS_HIVE2_HDFS_URL = "hdfs://172.17.0.2:9000";

  @Test
  void testHive2Client() throws Exception {
    runHiveClientTest("", "hive2", HIVE2_HMS_URL, HIVE2_HDFS_URL + "/tmp/gravitino_test");
  }

  @Test
  void testHive3DefaultCatalog() throws Exception {
    // Hive3 default catalog is "hive", not empty string
    runHiveClientTest(
        "hive", "hive3_default", HIVE3_HMS_URL, HIVE3_HDFS_URL + "/tmp/gravitino_test");
  }

  @Test
  void testHive3SampleCatalog() throws Exception {
    runHiveClientTest(
        "sample_catalog", "hive3_sample", HIVE3_HMS_URL, HIVE3_HDFS_URL + "/tmp/gravitino_test");
  }

  private void runHiveClientTest(
      String catalogName, String testPrefix, String metastoreUri, String hdfsBasePath) {
    Properties properties = new Properties();
    properties.setProperty("hive.metastore.uris", metastoreUri);
    HiveClient client = HiveClientFactory.createHiveClient(properties);

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
      client.createDatabase(schema);
      List<String> allDatabases = client.getAllDatabases(catalogName);
      Assertions.assertTrue(allDatabases.contains(dbName), "Database should be in the list");

      HiveSchema loadedDb = client.getDatabase(catalogName, dbName);
      Assertions.assertNotNull(loadedDb, "Loaded database should not be null");
      Assertions.assertEquals(dbName, loadedDb.name(), "Database name should match");
      Assertions.assertEquals(
          schema.comment(), loadedDb.comment(), "Database comment should match");

      client.alterDatabase(catalogName, dbName, schema);
      HiveSchema alteredDb = client.getDatabase(catalogName, dbName);
      Assertions.assertNotNull(alteredDb, "Altered database should not be null");

      // Test table operations
      client.createTable(table);
      List<String> allTables = client.getAllTables(catalogName, dbName);
      Assertions.assertTrue(allTables.contains(tableName), "Table should be in the list");

      HiveTable loadedTable = client.getTable(catalogName, dbName, tableName);
      Assertions.assertNotNull(loadedTable, "Loaded table should not be null");
      Assertions.assertEquals(tableName, loadedTable.name(), "Table name should match");
      Assertions.assertEquals(table.comment(), loadedTable.comment(), "Table comment should match");
      Assertions.assertEquals(2, loadedTable.columns().length, "Table should have 2 columns");
      Assertions.assertEquals(
          1, loadedTable.partitioning().length, "Table should have 1 partition key");

      client.alterTable(catalogName, dbName, tableName, loadedTable);
      HiveTable alteredTable = client.getTable(catalogName, dbName, tableName);
      Assertions.assertNotNull(alteredTable, "Altered table should not be null");

      List<String> filteredTables =
          client.listTableNamesByFilter(catalogName, dbName, "", (short) 10);
      Assertions.assertTrue(
          filteredTables.contains(tableName), "Filtered tables should contain the table");

      List<HiveTable> tableObjects =
          client.getTableObjectsByName(catalogName, dbName, List.of(tableName));
      Assertions.assertEquals(1, tableObjects.size(), "Should get exactly one table object");
      Assertions.assertEquals(
          tableName, tableObjects.get(0).name(), "Table object name should match");

      // Test partition operations
      HivePartition addedPartition = client.addPartition(loadedTable, partition);
      Assertions.assertNotNull(addedPartition, "Added partition should not be null");
      Assertions.assertEquals(partitionName, addedPartition.name(), "Partition name should match");

      List<String> partitionNames = client.listPartitionNames(loadedTable, (short) 10);
      Assertions.assertTrue(
          partitionNames.contains(partitionName), "Partition should be in the list");

      List<HivePartition> partitions = client.listPartitions(loadedTable, (short) 10);
      Assertions.assertEquals(1, partitions.size(), "Should have exactly one partition");
      Assertions.assertEquals(
          partitionName, partitions.get(0).name(), "Partition name should match");

      List<HivePartition> filteredPartitions =
          client.listPartitions(loadedTable, List.of(partitionValue), (short) 10);
      Assertions.assertEquals(
          1, filteredPartitions.size(), "Should have exactly one filtered partition");

      HivePartition fetchedPartition = client.getPartition(loadedTable, addedPartition.name());
      Assertions.assertNotNull(fetchedPartition, "Fetched partition should not be null");
      Assertions.assertEquals(
          partitionName, fetchedPartition.name(), "Fetched partition name should match");

      client.dropPartition(catalogName, dbName, tableName, addedPartition.name(), true);
      List<String> partitionNamesAfterDrop = client.listPartitionNames(loadedTable, (short) 10);
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

  @Test
  void testHiveExceptionHandling() throws Exception {
    testHiveExceptionHandlingForVersion("", HIVE2_HMS_URL, HIVE2_HDFS_URL);
  }

  @Test
  void testHive3ExceptionHandling() throws Exception {
    testHiveExceptionHandlingForVersion("hive", HIVE3_HMS_URL, HIVE3_HDFS_URL);
  }

  private void testHiveExceptionHandlingForVersion(
      String catalogName, String metastoreUri, String hdfsBasePath) throws Exception {
    Properties properties = new Properties();
    properties.setProperty("hive.metastore.uris", metastoreUri);
    HiveClient client = HiveClientFactory.createHiveClient(properties);

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
        client.createDatabase(schema);
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
          SchemaAlreadyExistsException.class, () -> client.createDatabase(schema));

      // Test NoSuchSchemaException - get non-existent database
      Assertions.assertThrows(
          NoSuchSchemaException.class,
          () -> client.getDatabase(catalogName, "non_existent_db_" + UUID.randomUUID()));

      // Test TableAlreadyExistsException - create table twice
      client.createTable(table);
      Assertions.assertThrows(TableAlreadyExistsException.class, () -> client.createTable(table));

      // Test NoSuchTableException - get non-existent table
      Assertions.assertThrows(
          NoSuchTableException.class,
          () -> client.getTable(catalogName, dbName, "non_existent_table_" + UUID.randomUUID()));

      // Test PartitionAlreadyExistsException - add partition twice
      HiveTable loadedTable = client.getTable(catalogName, dbName, tableName);
      HivePartition addedPartition = client.addPartition(loadedTable, partition);
      Assertions.assertNotNull(addedPartition, "Added partition should not be null");
      Assertions.assertThrows(
          PartitionAlreadyExistsException.class, () -> client.addPartition(loadedTable, partition));

      // Test NoSuchPartitionException - get non-existent partition
      Assertions.assertThrows(
          NoSuchPartitionException.class,
          () -> client.getPartition(loadedTable, "dt=non_existent_partition_" + UUID.randomUUID()));

      // Test NonEmptySchemaException - try to drop database with tables (cascade=false)
      Exception exception =
          Assertions.assertThrows(
              Exception.class, () -> client.dropDatabase(catalogName, dbName, false));
      // Hive may throw different exceptions for non-empty database
      // The converter should handle it appropriately
      Assertions.assertTrue(
          exception instanceof NonEmptySchemaException
              || exception instanceof GravitinoRuntimeException,
          "Should throw NonEmptySchemaException or GravitinoRuntimeException, got: "
              + exception.getClass().getName());

      // Cleanup
      client.dropPartition(catalogName, dbName, tableName, addedPartition.name(), true);
      client.dropTable(catalogName, dbName, tableName, true, true);
      client.dropDatabase(catalogName, dbName, true);
    } finally {
      safelyDropTable(client, catalogName, dbName, tableName);
      safelyDropDatabase(client, catalogName, dbName);
    }
  }

  private void testConnectionFailedExceptionForVersion(String catalogName) {
    // Test with invalid/unreachable Hive Metastore URI
    String invalidMetastoreUri = "thrift://127.0.0.1:9999";
    Properties properties = new Properties();
    properties.setProperty("hive.metastore.uris", invalidMetastoreUri);

    // Connection failure may occur during client creation or operation
    // Both should be converted to ConnectionFailedException
    Exception exception =
        Assertions.assertThrows(
            Exception.class,
            () -> {
              HiveClient client = HiveClientFactory.createHiveClient(properties);
              client.getAllDatabases(catalogName);
            });

    // Verify the exception is converted to ConnectionFailedException
    Assertions.assertTrue(
        exception instanceof ConnectionFailedException,
        "Should throw ConnectionFailedException, got: " + exception.getClass().getName());
    Assertions.assertNotNull(
        ((ConnectionFailedException) exception).getCause(), "Exception should have a cause");
  }

  @Test
  void testConnectionFailedException() throws Exception {
    // Test with HIVE2
    testConnectionFailedExceptionForVersion("");

    // Test with HIVE3
    testConnectionFailedExceptionForVersion("hive");
  }

  @Test
  void testKerberosConnection() {
    // This method can be implemented to test Kerberos authentication with Hive Metastore
    // when a Kerberos-enabled environment is available.
    Properties properties = new Properties();
    properties.setProperty("hive.metastore.uris", KERBEROS_HIVE2_HMS_URL);
    properties.setProperty("authentication.kerberos.principal", "cli@HADOOPKRB");
    properties.setProperty("authentication.impersonation-enable", "true");
    properties.setProperty(
        "authentication.kerberos.keytab-uri", "/tmp/test4310082059861441407/client.keytab");
    properties.setProperty("hive.metastore.kerberos.principal", "hive/6b1955fcb754@HADOOPKRB");
    properties.setProperty("hive.metastore.sasl.enabled", "true");
    properties.setProperty("hadoop.security.authentication", "kerberos");

    System.setProperty("java.security.krb5.conf", "/tmp/test4310082059861441407/krb5.conf");

    String catalogName = "hive";
    String dbName = "test_kerberos_db";
    String dbLocation = KERBEROS_HIVE2_HDFS_URL + "/tmp/gravitino_kerberos_test/" + dbName;

    HiveClient client = HiveClientFactory.createHiveClient(properties);
    HiveSchema schema = createTestSchema(catalogName, dbName, dbLocation);
    client.createDatabase(schema);
    List<String> allDatabases = client.getAllDatabases(catalogName);
    Assertions.assertTrue(allDatabases.contains(dbName), "Database should be in the list");
    client.dropDatabase(catalogName, dbName, true);
  }
}
