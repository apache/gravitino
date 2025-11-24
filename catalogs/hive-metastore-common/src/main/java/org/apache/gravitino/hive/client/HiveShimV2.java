/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gravitino.hive.client;

import static org.apache.gravitino.hive.client.HiveClient.HiveVersion.HIVE2;

import java.util.List;
import org.apache.gravitino.Schema;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.hive.converter.HiveDatabaseConverter;
import org.apache.gravitino.hive.converter.HiveTableConverter;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.thrift.TException;

class HiveShimV2 extends Shim {

  HiveShimV2(IMetaStoreClient client) {
    super(client, HIVE2);
  }

  HiveShimV2(IMetaStoreClient client,  HiveClient.HiveVersion version) {
    super(client, HIVE2);
  }

  public void createDatabase(String catalogName, Schema database) {
    try {
      Database db = HiveDatabaseConverter.toHiveDb(database);
      client.createDatabase(db);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create database", e);
    }
  }

  @Override
  public Schema getDatabase(String catalogName, String dbName) {
    try {
      Database db = client.getDatabase(dbName);
      if (db == null) {
        throw new NoSuchSchemaException("Database %s does not exist", dbName);
      }
      return HiveDatabaseConverter.fromHiveDB(db);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get database", e);
    }
  }

  @Override
  public void alterDatabase(String catalogName, String dbName, Schema database) {
    try {
      Database db = HiveDatabaseConverter.toHiveDb(database);
      client.alterDatabase(dbName, db);
    } catch (Exception e) {
      throw new RuntimeException("Failed to alter database", e);
    }
  }

  @Override
  public void dropDatabase(String catalogName, String dbName, boolean cascade) {
    try {
       client.dropDatabase(dbName, cascade, false);
    } catch (Exception e) {
      throw new RuntimeException("Failed to drop database", e);
    }
  }

  @Override
  public List<String> getAllTables(String catalogName, String dbName) {
    try {
      return client.getAllTables(dbName);
    } catch (TException e) {
      throw new RuntimeException("Failed to get all tables", e);
    }
  }

  @Override
  public List<String> getAllDatabTables(
      String catalogName, String dbName, String filter, short maxTables) {
    try {
      return client.getTables(dbName, filter);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get all datab tables", e);
    }
  }

  @Override
  public Table getTable(String catalogName, String dbName, String tableName) {
    try {
      var tb = client.getTable(dbName, tableName);
      return HiveTableConverter.fromHiveTable(tb);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get table", e);
    }
  }

  @Override
  public void alterTable(
      String catalogName, String dbName, String tableName, Table alteredHiveTable) {
    try {
      var tb = HiveTableConverter.toHiveTable(dbName, alteredHiveTable);
      client.alter_table(dbName, tableName, tb);
    } catch (Exception e) {
      throw new RuntimeException("Failed to alter table", e);
    }
  }

  @Override
  public void dropTable(
      String catalogName, String dbName, String tableName, boolean deleteData, boolean ifPurge) {
     try {
       client.dropTable(dbName, tableName, deleteData, ifPurge);
     } catch (Exception e) {
       throw new RuntimeException("Failed to drop table", e);
     }
  }

  @Override
  public void createTable(String catalogName, String dbName, Table hiveTable) {
    try {
      var tb = HiveTableConverter.toHiveTable(dbName, hiveTable);
      client.createTable(tb);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create table", e);
    }
  }

  @Override
  public List<String> listPartitionNames(
      String catalogName, String dbName, String tableName, short pageSize) {
    try {
      return client.listPartitionNames(dbName, tableName, pageSize);
    } catch (Exception e) {
      throw new RuntimeException("Failed to list partition names", e);
    }
  }

  @Override
  public List<Partition> listPartitions(
      String catalogName, String dbName, Table table, short pageSize) {
    try {
      var partitions = client.listPartitions(dbName, table.name(), pageSize);
      return partitions.stream()
          .map( p -> HiveTableConverter.fromHivePartition(table, p))
          .toList();
    } catch (Exception e) {
      throw new RuntimeException("Failed to list partitions", e);
    }
  }

  @Override
  public List<Partition> listPartitions(
      String catalogName,
      String dbName,
      Table table,
      List<String> filterPartitionValueList,
      short pageSize) {
    try {
      var partitions = client.listPartitions(dbName, table.name(), filterPartitionValueList, pageSize);
      return partitions.stream()
          .map( p -> HiveTableConverter.fromHivePartition(table, p))
          .toList();
    } catch (Exception e) {
      throw new RuntimeException("Failed to list partitions", e);
    }
  }

  @Override
  public Partition getPartition(
      String catalogName, String dbName, Table table, String partitionName) {
    try {
      var partitionValues = HiveTableConverter.parsePartitionValues(partitionName);
      var partition = client.getPartition(dbName, tableName, partitionValues);
      var table = getTable(catalogName, dbName, tableName);
      return HiveTableConverter.fromHivePartition(table, partition);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get partition", e);
  }

  @Override
  public Partition addPartition(
      String catalogName, String dbName, String tableName, Partition partition) {
    return null;
  }

  @Override
  public void dropPartition(
      String catalogName, String dbName, String tableName, String partitionName, boolean b) {}

  @Override
  public String getDelegationToken(String finalPrincipalName, String userName) {
    return "";
  }

  @Override
  public List<Table> getTableObjectsByName(String name, List<String> allTables) {
    return List.of();
  }
}
