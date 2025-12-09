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

  HiveShim hiveShim;

  public HiveClientImpl(HiveClientClassLoader.HiveVersion hiveVersion, Properties properties) {
    // todo create hive shim based on hive version
  }

  @Override
  public List<String> getAllDatabases(String catalogName) {
    return hiveShim.getAllDatabases(catalogName);
  }

  @Override
  public void createDatabase(HiveSchema database) {
    hiveShim.createDatabase(database);
  }

  @Override
  public HiveSchema getDatabase(String catalogName, String databaseName) {
    return hiveShim.getDatabase(catalogName, databaseName);
  }

  @Override
  public void alterDatabase(String catalogName, String databaseName, HiveSchema database) {
    hiveShim.alterDatabase(catalogName, databaseName, database);
  }

  @Override
  public void dropDatabase(String catalogName, String databaseName, boolean cascade) {
    hiveShim.dropDatabase(catalogName, databaseName, cascade);
  }

  @Override
  public List<String> getAllTables(String catalogName, String databaseName) {
    return hiveShim.getAllTables(catalogName, databaseName);
  }

  @Override
  public List<String> listTableNamesByFilter(
      String catalogName, String databaseName, String filter, short pageSize) {
    return hiveShim.listTableNamesByFilter(catalogName, databaseName, filter, pageSize);
  }

  @Override
  public HiveTable getTable(String catalogName, String databaseName, String tableName) {
    return hiveShim.getTable(catalogName, databaseName, tableName);
  }

  @Override
  public void alterTable(
      String catalogName, String databaseName, String tableName, HiveTable alteredHiveTable) {
    hiveShim.alterTable(catalogName, databaseName, tableName, alteredHiveTable);
  }

  @Override
  public void dropTable(
      String catalogName,
      String databaseName,
      String tableName,
      boolean deleteData,
      boolean ifPurge) {
    hiveShim.dropTable(catalogName, databaseName, tableName, deleteData, ifPurge);
  }

  @Override
  public void createTable(HiveTable hiveTable) {
    hiveShim.createTable(hiveTable);
  }

  @Override
  public List<String> listPartitionNames(HiveTable table, short pageSize) {
    return hiveShim.listPartitionNames(table, pageSize);
  }

  @Override
  public List<HivePartition> listPartitions(HiveTable table, short pageSize) {
    return hiveShim.listPartitions(table, pageSize);
  }

  @Override
  public List<HivePartition> listPartitions(
      HiveTable table, List<String> filterPartitionValueList, short pageSize) {
    return hiveShim.listPartitions(table, filterPartitionValueList, pageSize);
  }

  @Override
  public HivePartition getPartition(HiveTable table, String partitionName) {
    return hiveShim.getPartition(table, partitionName);
  }

  @Override
  public HivePartition addPartition(HiveTable table, HivePartition partition) {
    return hiveShim.addPartition(table, partition);
  }

  @Override
  public void dropPartition(
      String catalogName, String databaseName, String tableName, String partitionName, boolean b) {
    hiveShim.dropPartition(catalogName, databaseName, tableName, partitionName, b);
  }

  @Override
  public String getDelegationToken(String finalPrincipalName, String userName) {
    return hiveShim.getDelegationToken(finalPrincipalName, userName);
  }

  @Override
  public List<HiveTable> getTableObjectsByName(
      String catalogName, String databaseName, List<String> allTables) {
    return hiveShim.getTableObjectsByName(catalogName, databaseName, allTables);
  }

  @Override
  public List<String> getCatalogs() {
    return hiveShim.getCatalogs();
  }

  @Override
  public void close() {
    try {
      hiveShim.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
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
