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
import org.apache.gravitino.hive.converter.HiveDatabaseConverter;
import org.apache.gravitino.hive.converter.HiveTableConverter;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;

class HiveShimV3 extends HiveShimV2 {

  private final Method createDatabaseMethod;
  private final Method getDatabaseMethod;
  private final Method getAllDatabasesMethod;
  private final Method alterDatabaseMethod;
  private final Method dropDatabaseMethod;
  private final Method getTableMethod;
  private final Method createTableMethod;
  private final Method alterTableMethod;
  private final Method dropTableMethod;
  private final Method getAllTablesMethod;
  private final Method getTablesMethod;
  private final Method listPartitionNamesMethod;
  private final Method listPartitionsMethod;
  private final Method listPartitionsWithFilterMethod;
  private final Method getPartitionMethod;
  private final Method addPartitionMethod;
  private final Method dropPartitionMethod;
  private final Method getTableObjectsByNameMethod;
  private final Method databaseSetCatalogNameMethod;
  private final Method tableSetCatalogNameMethod;
  private final Method partitionSetCatalogNameMethod;

  HiveShimV3(IMetaStoreClient client) {
    super(client, HIVE3);
    try {
      // Hive3 database methods with catalog support
      this.createDatabaseMethod =
          IMetaStoreClient.class.getMethod("createDatabase", Database.class);
      this.getDatabaseMethod =
          IMetaStoreClient.class.getMethod("getDatabase", String.class, String.class);
      this.getAllDatabasesMethod =
          IMetaStoreClient.class.getMethod("getAllDatabases", String.class);
      this.alterDatabaseMethod =
          IMetaStoreClient.class.getMethod(
              "alterDatabase", String.class, String.class, Database.class);
      this.dropDatabaseMethod =
          IMetaStoreClient.class.getMethod(
              "dropDatabase", String.class, String.class, boolean.class, boolean.class);

      // Hive3 table methods with catalog support
      this.getTableMethod =
          IMetaStoreClient.class.getMethod("getTable", String.class, String.class, String.class);
      this.createTableMethod =
          IMetaStoreClient.class.getMethod(
              "createTable", org.apache.hadoop.hive.metastore.api.Table.class);
      this.alterTableMethod =
          IMetaStoreClient.class.getMethod(
              "alter_table",
              String.class,
              String.class,
              String.class,
              org.apache.hadoop.hive.metastore.api.Table.class);
      this.dropTableMethod =
          IMetaStoreClient.class.getMethod(
              "dropTable", String.class, String.class, String.class, boolean.class, boolean.class);
      this.getAllTablesMethod =
          IMetaStoreClient.class.getMethod("getAllTables", String.class, String.class);
      this.getTablesMethod =
          IMetaStoreClient.class.getMethod("getTables", String.class, String.class, String.class);

      // Hive3 partition methods with catalog support (using int for pageSize)
      this.listPartitionNamesMethod =
          IMetaStoreClient.class.getMethod(
              "listPartitionNames", String.class, String.class, String.class, int.class);
      this.listPartitionsMethod =
          IMetaStoreClient.class.getMethod(
              "listPartitions", String.class, String.class, String.class, int.class);
      this.listPartitionsWithFilterMethod =
          IMetaStoreClient.class.getMethod(
              "listPartitions", String.class, String.class, String.class, List.class, int.class);
      this.getPartitionMethod =
          IMetaStoreClient.class.getMethod(
              "getPartition", String.class, String.class, String.class, List.class);
      this.addPartitionMethod =
          IMetaStoreClient.class.getMethod(
              "add_partition", org.apache.hadoop.hive.metastore.api.Partition.class);
      this.dropPartitionMethod =
          IMetaStoreClient.class.getMethod(
              "dropPartition", String.class, String.class, String.class, List.class, boolean.class);
      // Hive3 getTableObjectsByName with catalog parameter
      this.getTableObjectsByNameMethod =
          IMetaStoreClient.class.getMethod(
              "getTableObjectsByName", String.class, String.class, List.class);

      // SetCatalogName methods for Hive3
      this.databaseSetCatalogNameMethod =
          Util.findMethod(Database.class, "setCatalogName", String.class);
      this.tableSetCatalogNameMethod =
          Util.findMethod(
              org.apache.hadoop.hive.metastore.api.Table.class, "setCatName", String.class);
      this.partitionSetCatalogNameMethod =
          Util.findMethod(
              org.apache.hadoop.hive.metastore.api.Partition.class, "setCatName", String.class);

    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e);
    }
  }

  @Override
  public void createDatabase(String catalogName, Schema database) {
    Database db = new Database();
    db.setName(database.name());
    db.setDescription(database.comment());
    // Hive3 requires a valid catalog name, use "hive" as default if empty
    String effectiveCatalogName =
        catalogName == null || catalogName.isEmpty() ? "hive" : catalogName;
    invoke(db, databaseSetCatalogNameMethod, effectiveCatalogName);
    invoke(client, createDatabaseMethod, db);
  }

  @Override
  public List<String> getAllDatabases(String catalogName) {
    return (List<String>) invoke(client, getAllDatabasesMethod, catalogName);
  }

  @Override
  public Schema getDatabase(String catalogName, String dbName) {
    Database db = (Database) invoke(client, getDatabaseMethod, catalogName, dbName);
    return HiveDatabaseConverter.fromHiveDB(db);
  }

  @Override
  public void alterDatabase(String catalogName, String dbName, Schema database) {
    Database db = new Database();
    db.setName(database.name());
    db.setDescription(database.comment());
    invoke(db, databaseSetCatalogNameMethod, catalogName);
    invoke(client, alterDatabaseMethod, catalogName, dbName, db);
  }

  @Override
  public void dropDatabase(String catalogName, String dbName, boolean cascade) {
    invoke(client, dropDatabaseMethod, catalogName, dbName, cascade, false);
  }

  @Override
  public List<String> getAllTables(String catalogName, String dbName) {
    return (List<String>) invoke(client, getAllTablesMethod, catalogName, dbName);
  }

  @Override
  public List<String> getAllDatabTables(
      String catalogName, String dbName, String filter, short maxTables) {
    return (List<String>) invoke(client, getTablesMethod, catalogName, dbName, filter);
  }

  @Override
  public Table getTable(String catalogName, String dbName, String tableName) {
    var tb =
        (org.apache.hadoop.hive.metastore.api.Table)
            invoke(client, getTableMethod, catalogName, dbName, tableName);
    return HiveTableConverter.fromHiveTable(tb);
  }

  @Override
  public void alterTable(
      String catalogName, String dbName, String tableName, Table alteredHiveTable) {
    var tb = HiveTableConverter.toHiveTable(dbName, alteredHiveTable);
    invoke(tb, tableSetCatalogNameMethod, catalogName);
    invoke(client, alterTableMethod, catalogName, dbName, tableName, tb);
  }

  @Override
  public void dropTable(
      String catalogName, String dbName, String tableName, boolean deleteData, boolean ifPurge) {
    invoke(client, dropTableMethod, catalogName, dbName, tableName, deleteData, ifPurge);
  }

  @Override
  public void createTable(String catalogName, String dbName, Table hiveTable) {
    var tb = HiveTableConverter.toHiveTable(dbName, hiveTable);
    invoke(tb, tableSetCatalogNameMethod, catalogName);
    invoke(client, createTableMethod, tb);
  }

  @Override
  public List<String> listPartitionNames(
      String catalogName, String dbName, String tableName, short pageSize) {
    Object pageSizeArg = convertPageSize(listPartitionNamesMethod, 3, pageSize);
    return (List<String>)
        invoke(client, listPartitionNamesMethod, catalogName, dbName, tableName, pageSizeArg);
  }

  @Override
  public List<Partition> listPartitions(
      String catalogName, String dbName, Table table, short pageSize) {
    Object pageSizeArg = convertPageSize(listPartitionsMethod, 3, pageSize);
    var partitions =
        (List<org.apache.hadoop.hive.metastore.api.Partition>)
            invoke(client, listPartitionsMethod, catalogName, dbName, table.name(), pageSizeArg);
    return partitions.stream().map(p -> HiveTableConverter.fromHivePartition(table, p)).toList();
  }

  @Override
  public List<Partition> listPartitions(
      String catalogName,
      String dbName,
      Table table,
      List<String> filterPartitionValueList,
      short pageSize) {
    Object pageSizeArg = convertPageSize(listPartitionsWithFilterMethod, 4, pageSize);
    var partitions =
        (List<org.apache.hadoop.hive.metastore.api.Partition>)
            invoke(
                client,
                listPartitionsWithFilterMethod,
                catalogName,
                dbName,
                table.name(),
                filterPartitionValueList,
                pageSizeArg);
    return partitions.stream().map(p -> HiveTableConverter.fromHivePartition(table, p)).toList();
  }

  @Override
  public Partition getPartition(
      String catalogName, String dbName, Table table, String partitionName) {
    var partitionValues = HiveTableConverter.parsePartitionValues(partitionName);
    var partition =
        (org.apache.hadoop.hive.metastore.api.Partition)
            invoke(client, getPartitionMethod, catalogName, dbName, table.name(), partitionValues);
    return HiveTableConverter.fromHivePartition(table, partition);
  }

  @Override
  public Partition addPartition(
      String catalogName, String dbName, Table table, Partition partition) {
    var hivePartition = HiveTableConverter.toHivePartition(dbName, table, partition);
    invoke(hivePartition, partitionSetCatalogNameMethod, catalogName);
    var addedPartition =
        (org.apache.hadoop.hive.metastore.api.Partition)
            invoke(client, addPartitionMethod, hivePartition);
    return HiveTableConverter.fromHivePartition(table, addedPartition);
  }

  @Override
  public void dropPartition(
      String catalogName, String dbName, String tableName, String partitionName, boolean b) {
    var partitionValues = HiveTableConverter.parsePartitionValues(partitionName);
    invoke(client, dropPartitionMethod, catalogName, dbName, tableName, partitionValues, b);
  }

  @Override
  public List<Table> getTableObjectsByName(
      String catalogName, String dbName, List<String> allTables) {
    var tables =
        (List<org.apache.hadoop.hive.metastore.api.Table>)
            invoke(client, getTableObjectsByNameMethod, catalogName, dbName, allTables);
    return tables.stream().map(HiveTableConverter::fromHiveTable).toList();
  }

  /**
   * Invokes a method on an object and converts any exception to Gravitino exception.
   *
   * @param object The object to invoke the method on
   * @param method The method to invoke
   * @param args The arguments to pass to the method
   * @return The result of the method invocation
   */
  private Object invoke(Object object, Method method, Object... args) {
    try {
      return method.invoke(object, args);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e);
    }
  }

  /**
   * Converts pageSize from short to int if the method parameter expects int type.
   *
   * @param method The method to check parameter types
   * @param paramIndex The index of the pageSize parameter
   * @param pageSize The pageSize value as short
   * @return The pageSize as Object (short or int)
   */
  private Object convertPageSize(Method method, int paramIndex, short pageSize) {
    if (method.getParameterTypes()[paramIndex] == int.class) {
      return (int) pageSize;
    }
    return pageSize;
  }
}
