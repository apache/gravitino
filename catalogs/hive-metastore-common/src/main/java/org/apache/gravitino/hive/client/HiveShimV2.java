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
import org.apache.gravitino.hive.HiveSchema;
import org.apache.gravitino.hive.HiveTable;
import org.apache.gravitino.hive.converter.HiveDatabaseConverter;
import org.apache.gravitino.hive.converter.HiveTableConverter;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;

class HiveShimV2 extends Shim {

  HiveShimV2(IMetaStoreClient client) {
    super(client, HIVE2);
  }

  HiveShimV2(IMetaStoreClient client, HiveClient.HiveVersion version) {
    super(client, version);
  }

  @Override
  public void createDatabase(HiveSchema database) {
    try {
      Database db = HiveDatabaseConverter.toHiveDb(database);
      client.createDatabase(db);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e);
    }
  }

  @Override
  public HiveSchema getDatabase(String catalogName, String databaseName) {
    try {
      Database db = client.getDatabase(databaseName);
      return HiveDatabaseConverter.fromHiveDB(db);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e);
    }
  }

  @Override
  public void alterDatabase(String catalogName, String databaseName, HiveSchema database) {
    try {
      Database db = HiveDatabaseConverter.toHiveDb(database);
      client.alterDatabase(databaseName, db);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e);
    }
  }

  @Override
  public void dropDatabase(String catalogName, String databaseName, boolean cascade) {
    try {
      client.dropDatabase(databaseName, false, false, cascade);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e);
    }
  }

  @Override
  public List<String> getAllTables(String catalogName, String databaseName) {
    try {
      return client.getAllTables(databaseName);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e);
    }
  }

  @Override
  public List<String> listTableNamesByFilter(
      String catalogName, String databaseName, String filter, short pageSize) {
    try {
      return client.listTableNamesByFilter(databaseName, filter, pageSize);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e);
    }
  }

  @Override
  public HiveTable getTable(String catalogName, String databaseName, String tableName) {
    try {
      var tb = client.getTable(databaseName, tableName);
      return HiveTableConverter.fromHiveTable(tb);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e);
    }
  }

  @Override
  public void alterTable(
      String catalogName, String databaseName, String tableName, HiveTable alteredHiveTable) {
    try {
      var tb = HiveTableConverter.toHiveTable(alteredHiveTable);
      client.alter_table(databaseName, tableName, tb);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e);
    }
  }

  @Override
  public void dropTable(
      String catalogName,
      String databaseName,
      String tableName,
      boolean deleteData,
      boolean ifPurge) {
    try {
      client.dropTable(databaseName, tableName, deleteData, ifPurge);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e);
    }
  }

  @Override
  public void createTable(HiveTable hiveTable) {
    try {
      var tb = HiveTableConverter.toHiveTable(hiveTable);
      client.createTable(tb);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e);
    }
  }

  @Override
  public List<String> listPartitionNames(HiveTable table, short pageSize) {
    try {
      String databaseName = table.databaseName();
      return client.listPartitionNames(databaseName, table.name(), pageSize);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e);
    }
  }

  @Override
  public List<Partition> listPartitions(HiveTable table, short pageSize) {
    try {
      String databaseName = table.databaseName();
      var partitions = client.listPartitions(databaseName, table.name(), pageSize);
      return partitions.stream().map(p -> HiveTableConverter.fromHivePartition(table, p)).toList();
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e);
    }
  }

  @Override
  public List<Partition> listPartitions(
      HiveTable table, List<String> filterPartitionValueList, short pageSize) {
    try {
      String databaseName = table.databaseName();
      var partitions =
          client.listPartitions(databaseName, table.name(), filterPartitionValueList, pageSize);
      return partitions.stream().map(p -> HiveTableConverter.fromHivePartition(table, p)).toList();
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e);
    }
  }

  @Override
  public Partition getPartition(HiveTable table, String partitionName) {
    try {
      String databaseName = table.databaseName();
      var partitionValues = HiveTableConverter.parsePartitionValues(partitionName);
      var partition = client.getPartition(databaseName, table.name(), partitionValues);
      return HiveTableConverter.fromHivePartition(table, partition);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e);
    }
  }

  @Override
  public Partition addPartition(HiveTable table, Partition partition) {
    try {
      String databaseName = table.databaseName();
      var hivePartition = HiveTableConverter.toHivePartition(databaseName, table, partition);
      var addedPartition = client.add_partition(hivePartition);
      return HiveTableConverter.fromHivePartition(table, addedPartition);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e);
    }
  }

  @Override
  public void dropPartition(
      String catalogName, String databaseName, String tableName, String partitionName, boolean b) {
    try {
      var partitionValues = HiveTableConverter.parsePartitionValues(partitionName);
      client.dropPartition(databaseName, tableName, partitionValues, b);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e);
    }
  }

  @Override
  public String getDelegationToken(String finalPrincipalName, String userName) {
    try {
      return client.getDelegationToken(finalPrincipalName, userName);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e);
    }
  }

  @Override
  public List<HiveTable> getTableObjectsByName(
      String catalogName, String databaseName, List<String> allTables) {
    try {
      // Hive2 doesn't support catalog, so we ignore catalogName and use databaseName
      var tables = client.getTableObjectsByName(databaseName, allTables);
      return tables.stream().map(HiveTableConverter::fromHiveTable).toList();
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e);
    }
  }
}
