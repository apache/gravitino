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
 * Java translation of Scala's `Shim` sealed abstract class.
 *
 * <p>This class declares the compatibility layer between Spark and different Hive versions.
 * Concrete subclasses (e.g. Shim_v0_12, Shim_v0_13, Shim_v2_3, Shim_v3_0 ...) must implement these
 * methods according to the behavior of the corresponding Hive release.
 */
public abstract class Shim {
  protected abstract List<String> getAllDatabase();

  public void createDatabase(String catalogName, Schema schema) {}

  public Schema getDatabase(String catalogName, String dbName) {
    return null;
  }

  public abstract void alterDatabase(String catalogName, String dbName, Schema database);

  public abstract void dropDatabase(String catalogName, String dbName, boolean cascade);

  public abstract List<String> getAllTables(String catalogName, String dbName);

  public abstract List<String> getAllDatabTables(
      String catalogName, String dbName, String filter, short maxTables);

  public abstract Table getTable(String catalogName, String dbName, String tableName);

  public abstract void alterTable(
      String catalogName, String dbName, String tableName, Table alteredHiveTable);

  public abstract void dropTable(
      String catalogName, String dbName, String tableName, boolean deleteData, boolean ifPurge);

  public abstract void createTable(String catalogName, String dbName, Table hiveTable);

  public abstract List<String> listPartitionNames(
      String catalogName, String dbName, String tableName, short pageSize);

  public abstract List<Partition> listPartitions(
      String catalogName, String dbName, String tableName, short pageSize);

  public abstract List<Partition> listPartitions(
      String catalogName,
      String dbName,
      String tableName,
      List<String> filterPartitionValueList,
      short pageSize);

  public abstract Partition getPartition(
      String catalogName, String dbName, String tableName, String partitionName);

  public abstract Partition addPartition(
      String catalogName, String dbName, String tableName, Partition partition);

  public abstract void dropPartition(
      String catalogName, String dbName, String tableName, String partitionName, boolean b);

  public abstract String getDelegationToken(String finalPrincipalName, String userName);

  public abstract List<Table> getTableObjectsByName(String name, List<String> allTables);
}
