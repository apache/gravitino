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
import org.apache.gravitino.hive.converter.HiveTableConverter;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;

class HiveShimV3 extends HiveShimV2 {

  Method createDatabaseMethod;
  Method getDabaseMethod;
  Method getAllDatabasesMethod;
  Method alterDatabaseMethod;
  Method getTableMethod;
  Method createTableMethod;
  Method alterTableMethod;
  Method dropTableMethod;
  Method getAllTablesMethod;
  Method getTablesMethod;
  Method listPartitionNamesMethod;
  Method listPartitionsMethod;
  Method listPartitionsWithFilterMethod;
  Method getPartitionMethod;
  Method addPartitionMethod;
  Method dropPartitionMethod;
  Method getTableObjectsByNameMethod;

  HiveShimV3(IMetaStoreClient client) {
    super(client, HIVE3);
    try {
      createDatabaseMethod = IMetaStoreClient.class.getMethod("createDatabase", Database.class);
      getDabaseMethod = IMetaStoreClient.class.getMethod("getDatabase", String.class, String.class);
      getAllDatabasesMethod = IMetaStoreClient.class.getMethod("getAllDatabases", String.class);
      alterDatabaseMethod =
          IMetaStoreClient.class.getMethod(
              "alterDatabase", String.class, String.class, Database.class);
      // Hive3 table methods with catalog support
      getTableMethod =
          IMetaStoreClient.class.getMethod("getTable", String.class, String.class, String.class);
      createTableMethod =
          IMetaStoreClient.class.getMethod(
              "createTable", org.apache.hadoop.hive.metastore.api.Table.class);
      alterTableMethod =
          IMetaStoreClient.class.getMethod(
              "alter_table",
              String.class,
              String.class,
              String.class,
              org.apache.hadoop.hive.metastore.api.Table.class);
      dropTableMethod =
          IMetaStoreClient.class.getMethod(
              "dropTable", String.class, String.class, String.class, boolean.class, boolean.class);
      getAllTablesMethod =
          IMetaStoreClient.class.getMethod("getAllTables", String.class, String.class);
      getTablesMethod =
          IMetaStoreClient.class.getMethod("getTables", String.class, String.class, String.class);
      // Try int first, fallback to short if needed
      try {
        listPartitionNamesMethod =
            IMetaStoreClient.class.getMethod(
                "listPartitionNames", String.class, String.class, String.class, int.class);
      } catch (NoSuchMethodException e) {
        listPartitionNamesMethod =
            IMetaStoreClient.class.getMethod(
                "listPartitionNames", String.class, String.class, String.class, short.class);
      }
      try {
        listPartitionsMethod =
            IMetaStoreClient.class.getMethod(
                "listPartitions", String.class, String.class, String.class, int.class);
      } catch (NoSuchMethodException e) {
        listPartitionsMethod =
            IMetaStoreClient.class.getMethod(
                "listPartitions", String.class, String.class, String.class, short.class);
      }
      try {
        listPartitionsWithFilterMethod =
            IMetaStoreClient.class.getMethod(
                "listPartitions", String.class, String.class, String.class, List.class, int.class);
      } catch (NoSuchMethodException e) {
        listPartitionsWithFilterMethod =
            IMetaStoreClient.class.getMethod(
                "listPartitions",
                String.class,
                String.class,
                String.class,
                List.class,
                short.class);
      }
      getPartitionMethod =
          IMetaStoreClient.class.getMethod(
              "getPartition", String.class, String.class, String.class, List.class);
      addPartitionMethod =
          IMetaStoreClient.class.getMethod(
              "add_partition", org.apache.hadoop.hive.metastore.api.Partition.class);
      dropPartitionMethod =
          IMetaStoreClient.class.getMethod(
              "dropPartition", String.class, String.class, String.class, List.class, boolean.class);
      // getTableObjectsByName may have catalog parameter in Hive3
      try {
        getTableObjectsByNameMethod =
            IMetaStoreClient.class.getMethod(
                "getTableObjectsByName", String.class, String.class, List.class);
      } catch (NoSuchMethodException e) {
        // Fallback to Hive2 signature without catalog
        getTableObjectsByNameMethod =
            IMetaStoreClient.class.getMethod("getTableObjectsByName", String.class, List.class);
      }
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Failed to initialize HiveShimV3", e);
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
      // Hive3 requires a valid catalog name, use "hive" as default if empty
      String effectiveCatalogName =
          catalogName == null || catalogName.isEmpty() ? "hive" : catalogName;
      Util.findMethod(Database.class, "setCatalogName", String.class)
          .invoke(db, effectiveCatalogName);
      createDatabaseMethod.invoke(client, db);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create database using HiveShimV3", e);
    }
  }

  @Override
  public List<String> getAllDatabases(String catalogName) {
    try {
      return (List<String>) getAllDatabasesMethod.invoke(client, catalogName);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get all databases", e);
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
  public void alterDatabase(String catalogName, String dbName, Schema database) {
    try {
      Database db = new Database();
      db.setName(database.name());
      db.setDescription(database.comment());
      Util.findMethod(Database.class, "setCatalogName", String.class).invoke(db, catalogName);
      alterDatabaseMethod.invoke(client, catalogName, dbName, db);
    } catch (Exception e) {
      throw new RuntimeException("Failed to alter database using HiveShimV3", e);
    }
  }

  @Override
  public void dropDatabase(String catalogName, String dbName, boolean cascade) {
    try {
      // Try Hive3 signature with catalog first
      try {
        Method dropDatabaseMethod =
            IMetaStoreClient.class.getMethod(
                "dropDatabase", String.class, String.class, boolean.class, boolean.class);
        dropDatabaseMethod.invoke(client, catalogName, dbName, cascade, false);
      } catch (NoSuchMethodException e) {
        // Fallback to Hive2 signature (without catalog)
        Method dropDatabaseMethod =
            IMetaStoreClient.class.getMethod(
                "dropDatabase", String.class, boolean.class, boolean.class);
        dropDatabaseMethod.invoke(client, dbName, cascade, false);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to drop database using HiveShimV3", e);
    }
  }

  @Override
  public List<String> getAllTables(String catalogName, String dbName) {
    try {
      return (List<String>) getAllTablesMethod.invoke(client, catalogName, dbName);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get all tables using HiveShimV3", e);
    }
  }

  @Override
  public List<String> getAllDatabTables(
      String catalogName, String dbName, String filter, short maxTables) {
    try {
      return (List<String>) getTablesMethod.invoke(client, catalogName, dbName, filter);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get all datab tables using HiveShimV3", e);
    }
  }

  @Override
  public Table getTable(String catalogName, String dbName, String tableName) {
    try {
      var tb =
          (org.apache.hadoop.hive.metastore.api.Table)
              getTableMethod.invoke(client, catalogName, dbName, tableName);
      return HiveTableConverter.fromHiveTable(tb);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get table using HiveShimV3", e);
    }
  }

  @Override
  public void alterTable(
      String catalogName, String dbName, String tableName, Table alteredHiveTable) {
    try {
      var tb = HiveTableConverter.toHiveTable(dbName, alteredHiveTable);
      // Try to set catalog name if method exists
      try {
        Util.findMethod(
                org.apache.hadoop.hive.metastore.api.Table.class, "setCatalogName", String.class)
            .invoke(tb, catalogName);
      } catch (NoSuchMethodException e) {
        // If setCatalogName doesn't exist, try setCatName
        try {
          Util.findMethod(
                  org.apache.hadoop.hive.metastore.api.Table.class, "setCatName", String.class)
              .invoke(tb, catalogName);
        } catch (NoSuchMethodException ignored) {
          // Catalog name may not be settable on Table object in this Hive version
        }
      }
      alterTableMethod.invoke(client, catalogName, dbName, tableName, tb);
    } catch (Exception e) {
      throw new RuntimeException("Failed to alter table using HiveShimV3", e);
    }
  }

  @Override
  public void dropTable(
      String catalogName, String dbName, String tableName, boolean deleteData, boolean ifPurge) {
    try {
      dropTableMethod.invoke(client, catalogName, dbName, tableName, deleteData, ifPurge);
    } catch (Exception e) {
      throw new RuntimeException("Failed to drop table using HiveShimV3", e);
    }
  }

  @Override
  public void createTable(String catalogName, String dbName, Table hiveTable) {
    try {
      var tb = HiveTableConverter.toHiveTable(dbName, hiveTable);
      // Try to set catalog name if method exists
      try {
        Util.findMethod(
                org.apache.hadoop.hive.metastore.api.Table.class, "setCatalogName", String.class)
            .invoke(tb, catalogName);
      } catch (NoSuchMethodException e) {
        // If setCatalogName doesn't exist, try setCatName
        try {
          Util.findMethod(
                  org.apache.hadoop.hive.metastore.api.Table.class, "setCatName", String.class)
              .invoke(tb, catalogName);
        } catch (NoSuchMethodException ignored) {
          // Catalog name may not be settable on Table object in this Hive version
        }
      }
      createTableMethod.invoke(client, tb);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create table using HiveShimV3", e);
    }
  }

  @Override
  public List<String> listPartitionNames(
      String catalogName, String dbName, String tableName, short pageSize) {
    try {
      // Convert short to int if method expects int
      Object pageSizeArg = pageSize;
      if (listPartitionNamesMethod.getParameterTypes()[3] == int.class) {
        pageSizeArg = (int) pageSize;
      }
      return (List<String>)
          listPartitionNamesMethod.invoke(client, catalogName, dbName, tableName, pageSizeArg);
    } catch (Exception e) {
      throw new RuntimeException("Failed to list partition names using HiveShimV3", e);
    }
  }

  @Override
  public List<Partition> listPartitions(
      String catalogName, String dbName, Table table, short pageSize) {
    try {
      Object pageSizeArg = pageSize;
      if (listPartitionsMethod.getParameterTypes()[3] == int.class) {
        pageSizeArg = (int) pageSize;
      }
      var partitions =
          (List<org.apache.hadoop.hive.metastore.api.Partition>)
              listPartitionsMethod.invoke(client, catalogName, dbName, table.name(), pageSizeArg);
      return partitions.stream().map(p -> HiveTableConverter.fromHivePartition(table, p)).toList();
    } catch (Exception e) {
      throw new RuntimeException("Failed to list partitions using HiveShimV3", e);
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
      Object pageSizeArg = pageSize;
      if (listPartitionsWithFilterMethod.getParameterTypes()[4] == int.class) {
        pageSizeArg = (int) pageSize;
      }
      var partitions =
          (List<org.apache.hadoop.hive.metastore.api.Partition>)
              listPartitionsWithFilterMethod.invoke(
                  client, catalogName, dbName, table.name(), filterPartitionValueList, pageSizeArg);
      return partitions.stream().map(p -> HiveTableConverter.fromHivePartition(table, p)).toList();
    } catch (Exception e) {
      throw new RuntimeException("Failed to list partitions using HiveShimV3", e);
    }
  }

  @Override
  public Partition getPartition(
      String catalogName, String dbName, Table table, String partitionName) {
    try {
      var partitionValues = HiveTableConverter.parsePartitionValues(partitionName);
      var partition =
          (org.apache.hadoop.hive.metastore.api.Partition)
              getPartitionMethod.invoke(client, catalogName, dbName, table.name(), partitionValues);
      return HiveTableConverter.fromHivePartition(table, partition);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get partition using HiveShimV3", e);
    }
  }

  @Override
  public Partition addPartition(
      String catalogName, String dbName, Table table, Partition partition) {
    try {
      var hivePartition = HiveTableConverter.toHivePartition(dbName, table, partition);
      // Try to set catalog name if method exists
      try {
        Util.findMethod(
                org.apache.hadoop.hive.metastore.api.Partition.class,
                "setCatalogName",
                String.class)
            .invoke(hivePartition, catalogName);
      } catch (NoSuchMethodException e) {
        // If setCatalogName doesn't exist, try setCatName
        try {
          Util.findMethod(
                  org.apache.hadoop.hive.metastore.api.Partition.class, "setCatName", String.class)
              .invoke(hivePartition, catalogName);
        } catch (NoSuchMethodException ignored) {
          // Catalog name may not be settable on Partition object in this Hive version
        }
      }
      var addedPartition =
          (org.apache.hadoop.hive.metastore.api.Partition)
              addPartitionMethod.invoke(client, hivePartition);
      return HiveTableConverter.fromHivePartition(table, addedPartition);
    } catch (Exception e) {
      throw new RuntimeException("Failed to add partition using HiveShimV3", e);
    }
  }

  @Override
  public void dropPartition(
      String catalogName, String dbName, String tableName, String partitionName, boolean b) {
    try {
      var partitionValues = HiveTableConverter.parsePartitionValues(partitionName);
      dropPartitionMethod.invoke(client, catalogName, dbName, tableName, partitionValues, b);
    } catch (Exception e) {
      throw new RuntimeException("Failed to drop partition using HiveShimV3", e);
    }
  }

  @Override
  public List<Table> getTableObjectsByName(
      String catalogName, String dbName, List<String> allTables) {
    try {
      // Hive3 requires catalog parameter
      if (getTableObjectsByNameMethod.getParameterCount() == 3) {
        // Method signature: getTableObjectsByName(String catalog, String dbName, List<String>)
        var tables =
            (List<org.apache.hadoop.hive.metastore.api.Table>)
                getTableObjectsByNameMethod.invoke(client, catalogName, dbName, allTables);
        return tables.stream().map(HiveTableConverter::fromHiveTable).toList();
      } else {
        // Fallback to Hive2 signature (should not happen in Hive3)
        var tables =
            (List<org.apache.hadoop.hive.metastore.api.Table>)
                getTableObjectsByNameMethod.invoke(client, dbName, allTables);
        return tables.stream().map(HiveTableConverter::fromHiveTable).toList();
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to get table objects by name using HiveShimV3", e);
    }
  }
}
