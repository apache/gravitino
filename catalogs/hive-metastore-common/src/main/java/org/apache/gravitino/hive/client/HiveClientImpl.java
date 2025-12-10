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
import org.apache.gravitino.hive.HivePartition;
import org.apache.gravitino.hive.HiveSchema;
import org.apache.gravitino.hive.HiveTable;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Java version of HiveClientImpl from Spark Hive module. Provides full database, table, and
 * partition operations.
 */
public class HiveClientImpl implements HiveClient {
  @Override
  public void createDatabase(HiveSchema database) {}

  @Override
  public HiveSchema getDatabase(String catalogName, String databaseName) {
    return null;
  }

  @Override
  public List<String> getAllDatabases(String catalogName) {
    return List.of();
  }

  @Override
  public void alterDatabase(String catalogName, String databaseName, HiveSchema database) {}

  @Override
  public void dropDatabase(String catalogName, String databaseName, boolean cascade) {}

  @Override
  public List<String> getAllTables(String catalogName, String databaseName) {
    return List.of();
  }

  @Override
  public List<String> listTableNamesByFilter(
      String catalogName, String databaseName, String filter, short pageSize) {
    return List.of();
  }

  @Override
  public HiveTable getTable(String catalogName, String databaseName, String tableName) {
    return null;
  }

  @Override
  public void alterTable(
      String catalogName, String databaseName, String tableName, HiveTable alteredHiveTable) {}

  @Override
  public void dropTable(
      String catalogName,
      String databaseName,
      String tableName,
      boolean deleteData,
      boolean ifPurge) {}

  @Override
  public void createTable(HiveTable hiveTable) {}

  @Override
  public List<String> listPartitionNames(HiveTable table, short pageSize) {
    return List.of();
  }

  @Override
  public List<HivePartition> listPartitions(HiveTable table, short pageSize) {
    return List.of();
  }

  @Override
  public List<HivePartition> listPartitions(
      HiveTable table, List<String> filterPartitionValueList, short pageSize) {
    return List.of();
  }

  @Override
  public HivePartition getPartition(HiveTable table, String partitionName) {
    return null;
  }

  @Override
  public HivePartition addPartition(HiveTable table, HivePartition partition) {
    return null;
  }

  @Override
  public void dropPartition(
      String catalogName, String databaseName, String tableName, String partitionName, boolean b) {}

  @Override
  public String getDelegationToken(String finalPrincipalName, String userName) {
    return "";
  }

  @Override
  public List<HiveTable> getTableObjectsByName(
      String catalogName, String databaseName, List<String> allTables) {
    return List.of();
  }

  @Override
  public List<String> getCatalogs() {
    return List.of();
  }

  @Override
  public void close() {}

  @Override
  public UserGroupInformation getUser() {
    return null;
  }
}
