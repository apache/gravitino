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

import static org.apache.gravitino.hive.client.HiveClientClassLoader.HiveVersion.HIVE3;
import static org.apache.gravitino.hive.client.Util.updateConfigurationFromProperties;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.gravitino.hive.HivePartition;
import org.apache.gravitino.hive.HiveSchema;
import org.apache.gravitino.hive.HiveTable;
import org.apache.gravitino.hive.client.HiveExceptionConverter.ExceptionTarget;
import org.apache.gravitino.hive.converter.HiveDatabaseConverter;
import org.apache.gravitino.hive.converter.HiveTableConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;

class HiveShimV3 extends HiveShimV2 {

  private static final String CATALOG_CLASS = "org.apache.hadoop.hive.metastore.api.Catalog";

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
  private final Method listTableNamesByFilterMethod;
  private final Method listPartitionNamesMethod;
  private final Method listPartitionsMethod;
  private final Method listPartitionsWithFilterMethod;
  private final Method getPartitionMethod;
  private final Method addPartitionMethod;
  private final Method dropPartitionMethod;
  private final Method getTableObjectsByNameMethod;
  private final Method databaseSetCatalogNameMethod;
  private final Method getCatalogsMethod;
  private final Method createCatalogMethod;

  private final Method tableSetCatalogNameMethod;
  private final Method partitionSetCatalogNameMethod;
  private final Method catalogSetDescriptionMethod;

  private final Class<?> catalogClass;
  private final Constructor<?> catalogCreator;

  HiveShimV3(Properties properties) {
    super(HIVE3, properties);
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
              "dropDatabase",
              String.class,
              String.class,
              boolean.class,
              boolean.class,
              boolean.class);

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
      this.listTableNamesByFilterMethod =
          IMetaStoreClient.class.getMethod(
              "listTableNamesByFilter", String.class, String.class, String.class, int.class);

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
      this.getCatalogsMethod = IMetaStoreClient.class.getMethod("getCatalogs");

      this.catalogClass = this.getClass().getClassLoader().loadClass(CATALOG_CLASS);
      this.catalogCreator = this.catalogClass.getDeclaredConstructor(String.class, String.class);
      this.createCatalogMethod = IMetaStoreClient.class.getMethod("createCatalog", catalogClass);
      this.catalogSetDescriptionMethod = catalogClass.getMethod("setDescription", String.class);

      // SetCatalogName methods for Hive3
      this.databaseSetCatalogNameMethod =
          MethodUtils.getAccessibleMethod(Database.class, "setCatalogName", String.class);
      this.tableSetCatalogNameMethod =
          MethodUtils.getAccessibleMethod(
              org.apache.hadoop.hive.metastore.api.Table.class, "setCatName", String.class);
      this.partitionSetCatalogNameMethod =
          MethodUtils.getAccessibleMethod(
              org.apache.hadoop.hive.metastore.api.Partition.class, "setCatName", String.class);

    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e, ExceptionTarget.other("HiveShimV3"));
    }
  }

  @Override
  public IMetaStoreClient createMetaStoreClient(Properties properties) {
    try {
      ClassLoader classLoader = this.getClass().getClassLoader();
      Class<?> clientClass = classLoader.loadClass(RETRYING_META_STORE_CLIENT_CLASS);
      Class<?> confClass = classLoader.loadClass(CONFIGURATION_CLASS);

      Object conf = confClass.getDeclaredConstructor().newInstance();
      updateConfigurationFromProperties(properties, (Configuration) conf);

      Method getProxyMethod = clientClass.getMethod(METHOD_GET_PROXY, confClass, boolean.class);
      return (IMetaStoreClient) getProxyMethod.invoke(null, conf, false);

    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(
          e, ExceptionTarget.other("MetaStoreClient"));
    }
  }

  @Override
  public void createDatabase(HiveSchema database) {
    Database db = HiveDatabaseConverter.toHiveDb(database);
    String catalogName = database.catalogName();
    invoke(ExceptionTarget.other(""), db, databaseSetCatalogNameMethod, catalogName);
    invoke(ExceptionTarget.schema(database.name()), client, createDatabaseMethod, db);
  }

  @Override
  public List<String> getAllDatabases(String catalogName) {
    return (List<String>)
        invoke(ExceptionTarget.catalog(catalogName), client, getAllDatabasesMethod, catalogName);
  }

  @Override
  public HiveSchema getDatabase(String catalogName, String databaseName) {
    Database db =
        (Database)
            invoke(
                ExceptionTarget.schema(databaseName),
                client,
                getDatabaseMethod,
                catalogName,
                databaseName);
    return HiveDatabaseConverter.fromHiveDB(db);
  }

  @Override
  public void alterDatabase(String catalogName, String databaseName, HiveSchema database) {
    Database db = HiveDatabaseConverter.toHiveDb(database);
    invoke(ExceptionTarget.other(""), db, databaseSetCatalogNameMethod, catalogName);
    invoke(
        ExceptionTarget.schema(databaseName),
        client,
        alterDatabaseMethod,
        catalogName,
        databaseName,
        db);
  }

  @Override
  public void dropDatabase(String catalogName, String databaseName, boolean cascade) {
    invoke(
        ExceptionTarget.schema(databaseName),
        client,
        dropDatabaseMethod,
        catalogName,
        databaseName,
        true,
        false,
        cascade);
  }

  @Override
  public List<String> getAllTables(String catalogName, String databaseName) {
    return (List<String>)
        invoke(
            ExceptionTarget.schema(databaseName),
            client,
            getAllTablesMethod,
            catalogName,
            databaseName);
  }

  @Override
  public List<String> listTableNamesByFilter(
      String catalogName, String databaseName, String filter, short pageSize) {
    Object pageSizeArg = convertPageSize(listTableNamesByFilterMethod, 3, pageSize);
    return (List<String>)
        invoke(
            ExceptionTarget.schema(databaseName),
            client,
            listTableNamesByFilterMethod,
            catalogName,
            databaseName,
            filter,
            pageSizeArg);
  }

  @Override
  public HiveTable getTable(String catalogName, String databaseName, String tableName) {
    var tb =
        (org.apache.hadoop.hive.metastore.api.Table)
            invoke(
                ExceptionTarget.table(tableName),
                client,
                getTableMethod,
                catalogName,
                databaseName,
                tableName);
    return HiveTableConverter.fromHiveTable(tb);
  }

  @Override
  public void alterTable(
      String catalogName, String databaseName, String tableName, HiveTable alteredHiveTable) {
    var tb = HiveTableConverter.toHiveTable(alteredHiveTable);
    invoke(ExceptionTarget.other(""), tb, tableSetCatalogNameMethod, catalogName);
    invoke(
        ExceptionTarget.table(tableName),
        client,
        alterTableMethod,
        catalogName,
        databaseName,
        tableName,
        tb);
  }

  @Override
  public void dropTable(
      String catalogName,
      String databaseName,
      String tableName,
      boolean deleteData,
      boolean ifPurge) {
    invoke(
        ExceptionTarget.table(tableName),
        client,
        dropTableMethod,
        catalogName,
        databaseName,
        tableName,
        deleteData,
        ifPurge);
  }

  @Override
  public void createTable(HiveTable hiveTable) {
    String catalogName = hiveTable.catalogName();
    var tb = HiveTableConverter.toHiveTable(hiveTable);
    invoke(ExceptionTarget.other(""), tb, tableSetCatalogNameMethod, catalogName);
    invoke(ExceptionTarget.table(hiveTable.name()), client, createTableMethod, tb);
  }

  @Override
  public List<String> listPartitionNames(HiveTable table, short pageSize) {
    String catalogName = table.catalogName();
    String databaseName = table.databaseName();
    Object pageSizeArg = convertPageSize(listPartitionNamesMethod, 3, pageSize);
    return (List<String>)
        invoke(
            ExceptionTarget.table(table.name()),
            client,
            listPartitionNamesMethod,
            catalogName,
            databaseName,
            table.name(),
            pageSizeArg);
  }

  @Override
  public List<HivePartition> listPartitions(HiveTable table, short pageSize) {
    String catalogName = table.catalogName();
    String databaseName = table.databaseName();
    Object pageSizeArg = convertPageSize(listPartitionsMethod, 3, pageSize);
    var partitions =
        (List<org.apache.hadoop.hive.metastore.api.Partition>)
            invoke(
                ExceptionTarget.table(table.name()),
                client,
                listPartitionsMethod,
                catalogName,
                databaseName,
                table.name(),
                pageSizeArg);
    return partitions.stream().map(p -> HiveTableConverter.fromHivePartition(table, p)).toList();
  }

  @Override
  public List<HivePartition> listPartitions(
      HiveTable table, List<String> filterPartitionValueList, short pageSize) {
    String catalogName = table.catalogName();
    String databaseName = table.databaseName();
    Object pageSizeArg = convertPageSize(listPartitionsWithFilterMethod, 4, pageSize);
    var partitions =
        (List<org.apache.hadoop.hive.metastore.api.Partition>)
            invoke(
                ExceptionTarget.table(table.name()),
                client,
                listPartitionsWithFilterMethod,
                catalogName,
                databaseName,
                table.name(),
                filterPartitionValueList,
                pageSizeArg);
    return partitions.stream().map(p -> HiveTableConverter.fromHivePartition(table, p)).toList();
  }

  @Override
  public HivePartition getPartition(HiveTable table, String partitionName) {
    String catalogName = table.catalogName();
    String databaseName = table.databaseName();
    var partitionValues = HivePartition.extractPartitionValues(partitionName);
    var partition =
        (org.apache.hadoop.hive.metastore.api.Partition)
            invoke(
                ExceptionTarget.partition(partitionName),
                client,
                getPartitionMethod,
                catalogName,
                databaseName,
                table.name(),
                partitionValues);
    return HiveTableConverter.fromHivePartition(table, partition);
  }

  @Override
  public HivePartition addPartition(HiveTable table, HivePartition partition) {
    String catalogName = table.catalogName();
    String databaseName = table.databaseName();
    var hivePartition = HiveTableConverter.toHivePartition(databaseName, table, partition);
    invoke(ExceptionTarget.other(""), hivePartition, partitionSetCatalogNameMethod, catalogName);
    var addedPartition =
        (org.apache.hadoop.hive.metastore.api.Partition)
            invoke(
                ExceptionTarget.partition(partition.name()),
                client,
                addPartitionMethod,
                hivePartition);
    return HiveTableConverter.fromHivePartition(table, addedPartition);
  }

  @Override
  public void dropPartition(
      String catalogName,
      String databaseName,
      String tableName,
      String partitionName,
      boolean deleteData) {
    var partitionValues = HivePartition.extractPartitionValues(partitionName);
    invoke(
        ExceptionTarget.partition(partitionName),
        client,
        dropPartitionMethod,
        catalogName,
        databaseName,
        tableName,
        partitionValues,
        deleteData);
  }

  @Override
  public List<HiveTable> getTableObjectsByName(
      String catalogName, String databaseName, List<String> allTables) {
    var tables =
        (List<Table>)
            invoke(
                ExceptionTarget.schema(databaseName),
                client,
                getTableObjectsByNameMethod,
                catalogName,
                databaseName,
                allTables);
    return tables.stream().map(HiveTableConverter::fromHiveTable).toList();
  }

  @Override
  public List<String> getCatalogs() {
    return (List<String>) invoke(ExceptionTarget.other(""), client, getCatalogsMethod);
  }

  @Override
  public void createCatalog(String catalogName, String location, String description) {
    Object catalog =
        invoke(ExceptionTarget.other(catalogName), catalogCreator, catalogName, location);
    if (StringUtils.isNotBlank(description)) {
      invoke(ExceptionTarget.other(catalogName), catalog, catalogSetDescriptionMethod, description);
    }
    invoke(ExceptionTarget.catalog(catalogName), client, createCatalogMethod, catalog);
  }

  /**
   * Invokes a method on an object and converts any exception to a Gravitino exception.
   *
   * @param target Hive object info used in error messages and exception mapping
   * @param object The object to invoke the method on
   * @param method The method to invoke
   * @param args The arguments to pass to the method
   * @return The result of the method invocation
   */
  private Object invoke(ExceptionTarget target, Object object, Method method, Object... args) {
    try {
      return method.invoke(object, args);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e, target);
    }
  }

  /**
   * Creates an object using a constructor and converts any exception to a Gravitino exception.
   *
   * @param target Hive object info used in error messages and exception mapping
   * @param constructor The constructor to use for creating the object
   * @param args The arguments to pass to the constructor
   * @return The created object
   */
  private Object invoke(ExceptionTarget target, Constructor<?> constructor, Object... args) {
    try {
      return constructor.newInstance(args);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e, target);
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

  @Override
  public void close() throws Exception {
    client.close();
  }
}
