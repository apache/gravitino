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
import java.util.Properties;
import org.apache.gravitino.hive.HivePartition;
import org.apache.gravitino.hive.HiveSchema;
import org.apache.gravitino.hive.HiveTable;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Java version of HiveClientImpl from Spark Hive module. Provides full database, table, and
 * partition operations.
 */
public class HiveClientImpl implements HiveClient {

  private final HiveShim shim;

  public HiveClientImpl(HiveClientClassLoader.HiveVersion hiveVersion, Properties properties) {
    switch (hiveVersion) {
      case HIVE2:
        {
          shim = new HiveShimV2(properties);
          break;
        }
      case HIVE3:
        {
          shim = new HiveShimV3(properties);
          break;
        }
      default:
        throw new IllegalArgumentException("Unsupported Hive version: " + hiveVersion);
    }
  }

  @Override
  public List<String> getAllDatabases(String catalogName) {
    return shim.getAllDatabases(catalogName);
  }

  @Override
  public void createDatabase(HiveSchema database) {
    shim.createDatabase(database);
  }

  @Override
  public HiveSchema getDatabase(String catalogName, String databaseName) {
    return shim.getDatabase(catalogName, databaseName);
  }

  @Override
  public void alterDatabase(String catalogName, String databaseName, HiveSchema database) {
    shim.alterDatabase(catalogName, databaseName, database);
  }

  @Override
  public void dropDatabase(String catalogName, String databaseName, boolean cascade) {
    shim.dropDatabase(catalogName, databaseName, cascade);
  }

  @Override
  public List<String> getAllTables(String catalogName, String databaseName) {
    return shim.getAllTables(catalogName, databaseName);
  }

  @Override
  public List<String> listTableNamesByFilter(
      String catalogName, String databaseName, String filter, short pageSize) {
    return shim.listTableNamesByFilter(catalogName, databaseName, filter, pageSize);
  }

  @Override
  public HiveTable getTable(String catalogName, String databaseName, String tableName) {
    return shim.getTable(catalogName, databaseName, tableName);
  }

  @Override
  public void alterTable(
      String catalogName, String databaseName, String tableName, HiveTable alteredHiveTable) {
    shim.alterTable(catalogName, databaseName, tableName, alteredHiveTable);
  }

  @Override
  public void dropTable(
      String catalogName,
      String databaseName,
      String tableName,
      boolean deleteData,
      boolean ifPurge) {
    shim.dropTable(catalogName, databaseName, tableName, deleteData, ifPurge);
  }

  @Override
  public void createTable(HiveTable hiveTable) {
    shim.createTable(hiveTable);
  }

  @Override
  public List<String> listPartitionNames(HiveTable table, short pageSize) {
    return shim.listPartitionNames(table, pageSize);
  }

  @Override
  public List<HivePartition> listPartitions(HiveTable table, short pageSize) {
    return shim.listPartitions(table, pageSize);
  }

  @Override
  public List<HivePartition> listPartitions(
      HiveTable table, List<String> filterPartitionValueList, short pageSize) {
    return shim.listPartitions(table, filterPartitionValueList, pageSize);
  }

  @Override
  public HivePartition getPartition(HiveTable table, String partitionName) {
    return shim.getPartition(table, partitionName);
  }

  @Override
  public HivePartition addPartition(HiveTable table, HivePartition partition) {
    return shim.addPartition(table, partition);
  }

  @Override
  public void dropPartition(
      String catalogName,
      String databaseName,
      String tableName,
      String partitionName,
      boolean deleteData) {
    shim.dropPartition(catalogName, databaseName, tableName, partitionName, deleteData);
  }

  @Override
  public String getDelegationToken(String finalPrincipalName, String userName) {
    return shim.getDelegationToken(finalPrincipalName, userName);
  }

  @Override
  public List<HiveTable> getTableObjectsByName(
      String catalogName, String databaseName, List<String> allTables) {
    return shim.getTableObjectsByName(catalogName, databaseName, allTables);
  }

  @Override
  public List<String> getCatalogs() {
    return shim.getCatalogs();
  }

  @Override
  public void createCatalog(String catalogName, String location, String description) {
    shim.createCatalog(catalogName, location, description);
  }

  @Override
  public void close() {
    try {
      shim.close();
    } catch (Exception e) {
      throw new RuntimeException("Failed to close HiveClient", e);
    }
  }

  @Override
  public UserGroupInformation getUser() {
    try {
      return UserGroupInformation.getCurrentUser();
    } catch (Exception e) {
      throw new RuntimeException("Failed to get current user", e);
    }
  }
}
