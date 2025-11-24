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

import static org.apache.gravitino.hive.client.HiveClient.HiveVersion.HIVE3;

import java.lang.reflect.Method;
import java.util.List;
import org.apache.gravitino.Schema;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.hive.converter.HiveDatabaseConverter;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;

class HiveShimV3 extends HiveShimV2{

  Method createDatabaseMethod;
  Method getDabaseMethod;

  HiveShimV3(IMetaStoreClient client) {
    super(client, HIVE3);
    try {
      createDatabaseMethod = IMetaStoreClient.class.getMethod("createDatabase", Database.class);
      getDabaseMethod = IMetaStoreClient.class.getMethod("getDatabase", String.class, String.class);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Failed to initialize HiveShimV2", e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void createDatabase(String catalogName, Schema database) {
    try {
      Database db = new Database();
      db.setName(database.name());
      db.setDescription(database.comment());
      Util.findMethod(Database.class, "setCatalogName", String.class).invoke(db, catalogName);
      createDatabaseMethod.invoke(client, db);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create database using HiveShimV2", e);
    }
  }

  @Override
  public Schema getDatabase(String catalogName, String dbName) {
    try {
      Database db = (Database) getDabaseMethod.invoke(client, catalogName, dbName);
      if (db == null) {
        throw new NoSuchSchemaException(
            "Database %s does not exist in catalog %s", dbName, catalogName);
      }
      return HiveDatabaseConverter.fromHiveDB(db);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get database using HiveShimV3", e);
    }
  }

  @Override
  public void alterDatabase(String catalogName, String dbName, Schema database) {}

  @Override
  public void dropDatabase(String catalogName, String dbName, boolean cascade) {}

  @Override
  public List<String> getAllTables(String catalogName, String dbName) {
    return List.of();
  }

  @Override
  public List<String> getAllDatabTables(
      String catalogName, String dbName, String filter, short maxTables) {
    return List.of();
  }

  @Override
  public Table getTable(String catalogName, String dbName, String tableName) {
    return null;
  }

  @Override
  public void alterTable(
      String catalogName, String dbName, String tableName, Table alteredHiveTable) {}

  @Override
  public void dropTable(
      String catalogName, String dbName, String tableName, boolean deleteData, boolean ifPurge) {}

  @Override
  public void createTable(String catalogName, String dbName, Table hiveTable) {}

  @Override
  public List<String> listPartitionNames(
      String catalogName, String dbName, String tableName, short pageSize) {
    return List.of();
  }

  @Override
  public List<Partition> listPartitions(
      String catalogName, String dbName, Table table, short pageSize) {
    return List.of();
  }

  @Override
  public List<Partition> listPartitions(
      String catalogName,
      String dbName,
      Table tabel,
      List<String> filterPartitionValueList,
      short pageSize) {
    return List.of();
  }

  @Override
  public Partition getPartition(
      String catalogName, String dbName, String tableName, String partitionName) {
    return null;
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
