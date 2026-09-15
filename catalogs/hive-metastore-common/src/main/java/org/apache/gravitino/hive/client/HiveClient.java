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
 * An externally visible interface to the Hive client. This interface is shared across both the
 * internal and external classloaders for a given version of Hive and thus must expose only shared
 * classes.
 */
public interface HiveClient extends AutoCloseable {

  void createDatabase(HiveSchema database);

  HiveSchema getDatabase(String catalogName, String databaseName);

  List<String> getAllDatabases(String catalogName);

  void alterDatabase(String catalogName, String databaseName, HiveSchema database);

  void dropDatabase(String catalogName, String databaseName, boolean cascade);

  List<String> getAllTables(String catalogName, String databaseName);

  List<String> listTablesByType(
      String catalogName, String databaseName, String tablePattern, String tableType);

  List<String> listTableNamesByFilter(
      String catalogName, String databaseName, String filter, short pageSize);

  HiveTable getTable(String catalogName, String databaseName, String tableName);

  default void alterTable(
      String catalogName, String databaseName, String tableName, HiveTable alteredHiveTable) {
    alterTable(catalogName, databaseName, tableName, alteredHiveTable, false);
  }

  /**
   * Alters a table in the Hive metastore.
   *
   * <p>When {@code skipStatsUpdate} is {@code true}, the metastore is instructed (via the {@code
   * DO_NOT_UPDATE_STATS} environment context) not to recompute table statistics for this alter.
   * This avoids the metastore accessing the table's storage location (for example an {@code
   * getFileInfo} call against the NameNode), which is unnecessary for property-only or comment-only
   * changes and can otherwise make a lightweight alter hang when the underlying filesystem is slow
   * or unavailable.
   *
   * @param catalogName The Hive catalog name.
   * @param databaseName The database name.
   * @param tableName The table name.
   * @param alteredHiveTable The altered table definition.
   * @param skipStatsUpdate Whether to skip metastore statistics recomputation for this alter.
   */
  void alterTable(
      String catalogName,
      String databaseName,
      String tableName,
      HiveTable alteredHiveTable,
      boolean skipStatsUpdate);

  void dropTable(
      String catalogName,
      String databaseName,
      String tableName,
      boolean deleteData,
      boolean ifPurge);

  void createTable(HiveTable hiveTable);

  List<String> listPartitionNames(HiveTable table, short pageSize);

  List<HivePartition> listPartitions(HiveTable table, short pageSize);

  List<HivePartition> listPartitions(
      HiveTable table, List<String> filterPartitionValueList, short pageSize);

  HivePartition getPartition(HiveTable table, String partitionName);

  HivePartition addPartition(HiveTable table, HivePartition partition);

  void dropPartition(
      String catalogName,
      String databaseName,
      String tableName,
      String partitionName,
      boolean deleteData);

  String getDelegationToken(String finalPrincipalName, String userName);

  List<HiveTable> getTableObjectsByName(
      String catalogName, String databaseName, List<String> allTables);

  List<String> getCatalogs();

  default void createCatalog(String catalogName, String location) {
    createCatalog(catalogName, location, "");
  }

  void createCatalog(String catalogName, String location, String description);

  void close();

  UserGroupInformation getUser();
}
