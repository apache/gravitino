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

import java.util.List;
import org.apache.gravitino.Schema;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;

/**
 * Java version of HiveClientImpl from Spark Hive module. Provides full database, table, and
 * partition operations.
 */
public class HiveClientImpl implements HiveClient {

  Shim shim;

  public HiveClientImpl(HiveVersion hiveVersion, IMetaStoreClient hiveClient) {
    switch (hiveVersion) {
      case HIVE2:
        shim = new HiveShimV2(hiveClient);
        break;
      case HIVE3:
        shim = new HiveShimV3(hiveClient);
        break;
      default:
        throw new IllegalArgumentException("Unsupported Hive version: " + hiveVersion);
    }
  }

  @Override
  public List<String> getAllDatabases(String catalogName) {
    return shim.getAllDatabases(catalogName);
  }

  @Override
  public void createDatabase(String catalogName, Schema database) {
    shim.createDatabase(catalogName, database);
  }

  @Override
  public Schema getDatabase(String catalogName, String dbName) {
    return shim.getDatabase(catalogName, dbName);
  }

  @Override
  public void alterDatabase(String catalogName, String dbName, Schema database) {
    shim.alterDatabase(catalogName, dbName, database);
  }

  @Override
  public void dropDatabase(String catalogName, String dbName, boolean cascade) {
    shim.dropDatabase(catalogName, dbName, cascade);
  }

  @Override
  public List<String> getAllTables(String catalogName, String dbName) {
    return shim.getAllTables(catalogName, dbName);
  }

  @Override
  public List<String> listTableNamesByFilter(
      String catalogName, String dbName, String filter, short maxTables) {
    return shim.getAllDatabTables(catalogName, dbName, filter, maxTables);
  }

  @Override
  public Table getTable(String catalogName, String dbName, String tableName) {
    return shim.getTable(catalogName, dbName, tableName);
  }

  @Override
  public void alterTable(
      String catalogName, String dbName, String tableName, Table alteredHiveTable) {
    shim.alterTable(catalogName, dbName, tableName, alteredHiveTable);
  }

  @Override
  public void dropTable(
      String catalogName, String dbName, String tableName, boolean deleteData, boolean ifPurge) {
    shim.dropTable(catalogName, dbName, tableName, deleteData, ifPurge);
  }

  @Override
  public void createTable(String catalogName, String dbName, Table hiveTable) {
    shim.createTable(catalogName, dbName, hiveTable);
  }

  @Override
  public List<String> listPartitionNames(
      String catalogName, String dbName, String tableName, short pageSize) {
    return shim.listPartitionNames(catalogName, dbName, tableName, pageSize);
  }

  @Override
  public List<Partition> listPartitions(
      String catalogName, String dbName, Table table, short pageSize) {
    return shim.listPartitions(catalogName, dbName, table, pageSize);
  }

  @Override
  public List<Partition> listPartitions(
      String catalogName,
      String dbName,
      Table table,
      List<String> filterPartitionValueList,
      short pageSize) {
    return shim.listPartitions(catalogName, dbName, table, filterPartitionValueList, pageSize);
  }

  @Override
  public Partition getPartition(
      String catalogName, String dbName, Table table, String partitionName) {
    return shim.getPartition(catalogName, dbName, table, partitionName);
  }

  @Override
  public Partition addPartition(
      String catalogName, String dbName, Table table, Partition partition) {
    return shim.addPartition(catalogName, dbName, table, partition);
  }

  @Override
  public void dropPartition(
      String catalogName, String dbName, String tableName, String partitionName, boolean b) {
    shim.dropPartition(catalogName, dbName, tableName, partitionName, b);
  }

  @Override
  public String getDelegationToken(String finalPrincipalName, String userName) {
    return shim.getDelegationToken(finalPrincipalName, userName);
  }

  @Override
  public List<Table> getTableObjectsByName(
      String catalogName, String dbName, List<String> allTables) {
    return shim.getTableObjectsByName(catalogName, dbName, allTables);
  }
}
