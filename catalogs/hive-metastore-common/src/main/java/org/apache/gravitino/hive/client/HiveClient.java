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

/**
 * An externally visible interface to the Hive client. This interface is shared across both the
 * internal and external classloaders for a given version of Hive and thus must expose only shared
 * classes.
 */
public interface HiveClient {

  void createDatabase(String catalogName, Schema schema);

  Schema getDatabase(String catalogName, String name);

  List<String> getAllDatabases(String catalogName);

  void alterDatabase(String catalogName, String dbName, Schema database);

  void dropDatabase(String catalogName, String dbName, boolean cascade);

  List<String> getAllTables(String catalogName, String dbName);

  List<String> listTableNamesByFilter(
      String catalogName, String dbName, String filter, short maxTables);

  Table getTable(String catalogName, String dbName, String tableName);

  void alterTable(String catalogName, String dbName, String tableName, Table alteredHiveTable);

  void dropTable(
      String catalogName, String dbName, String tableName, boolean deleteData, boolean ifPurge);

  void createTable(String catalogName, String dbName, Table hiveTable);

  List<String> listPartitionNames(
      String catalogName, String dbName, String tableName, short pageSize);

  List<Partition> listPartitions(
      String catalogName, String dbName, Table table, short pageSize);

  List<Partition> listPartitions(
      String catalogName,
      String dbName,
      Table table,
      List<String> filterPartitionValueList,
      short pageSize);

  Partition getPartition(String catalogName, String dbName, String tableName, String partitionName);

  Partition addPartition(String catalogName, String dbName, String tableName, Partition partition);

  void dropPartition(
      String catalogName, String dbName, String tableName, String partitionName, boolean b);

  String getDelegationToken(String finalPrincipalName, String userName);

  List<Table> getTableObjectsByName(String name, List<String> allTables);

  public enum HiveVersion {
    HIVE2,
    HIVE3,
  }
}
