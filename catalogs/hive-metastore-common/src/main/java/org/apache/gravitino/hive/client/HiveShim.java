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

import static org.apache.gravitino.hive.client.Util.updateConfigurationFromProperties;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.gravitino.hive.HivePartition;
import org.apache.gravitino.hive.HiveSchema;
import org.apache.gravitino.hive.HiveTable;
import org.apache.gravitino.hive.client.HiveExceptionConverter.ExceptionTarget;
import org.apache.gravitino.hive.converter.HiveDatabaseConverter;
import org.apache.gravitino.hive.converter.HiveTableConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.thrift.TException;

/**
 * Java translation of Scala's `Shim` sealed abstract class.
 *
 * <p>This class provides the Hive 2.x-compatible behavior of the metastore client, which is also
 * the baseline every later Hive release stays compatible with. Version-specific subclasses (e.g.
 * the Hive 3.x implementation, which adds multi-catalog support and column constraints) live in
 * their own module, compiled directly against their own metastore client jar, and override
 * whichever methods behave differently for that version.
 */
public abstract class HiveShim {

  protected static final String RETRYING_META_STORE_CLIENT_CLASS =
      "org.apache.hadoop.hive.metastore.RetryingMetaStoreClient";
  protected static final String HIVE_CONF_CLASS = "org.apache.hadoop.hive.conf.HiveConf";
  protected static final String CONFIGURATION_CLASS = "org.apache.hadoop.conf.Configuration";
  protected static final String METHOD_GET_PROXY = "getProxy";

  protected final IMetaStoreClient client;
  protected final HiveClientClassLoader.HiveVersion version;

  protected HiveShim(HiveClientClassLoader.HiveVersion version, Properties properties) {
    this.client = createMetaStoreClient(properties);
    this.version = version;
  }

  /**
   * Returns the Hive version this shim was created for, which is the version Gravitino detected for
   * the connected metastore.
   *
   * @return The Hive version of this shim.
   */
  public HiveClientClassLoader.HiveVersion hiveVersion() {
    return version;
  }

  public IMetaStoreClient createMetaStoreClient(Properties properties) {
    try {
      ClassLoader classLoader = this.getClass().getClassLoader();
      Class<?> clientClass = classLoader.loadClass(RETRYING_META_STORE_CLIENT_CLASS);
      Class<?> hiveConfClass = classLoader.loadClass(HIVE_CONF_CLASS);
      Class<?> confClass = classLoader.loadClass(CONFIGURATION_CLASS);

      Object conf = confClass.getDeclaredConstructor().newInstance();
      updateConfigurationFromProperties(properties, (Configuration) conf);

      Constructor<?> hiveConfCtor = hiveConfClass.getConstructor(confClass, Class.class);
      Object hiveConfInstance = hiveConfCtor.newInstance(conf, hiveConfClass);

      Method getProxyMethod = clientClass.getMethod(METHOD_GET_PROXY, hiveConfClass, boolean.class);
      return (IMetaStoreClient) getProxyMethod.invoke(null, hiveConfInstance, false);

    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(
          e, ExceptionTarget.other("MetaStoreClient"));
    }
  }

  public List<String> getAllDatabases(String catalogName) {
    try {
      return client.getAllDatabases();
    } catch (TException e) {
      throw HiveExceptionConverter.toGravitinoException(
          e, HiveExceptionConverter.ExceptionTarget.catalog(catalogName));
    }
  }

  public void createDatabase(HiveSchema database) {
    try {
      Database db = HiveDatabaseConverter.toHiveDb(database);
      client.createDatabase(db);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e, ExceptionTarget.schema(database.name()));
    }
  }

  public HiveSchema getDatabase(String catalogName, String databaseName) {
    try {
      Database db = client.getDatabase(databaseName);
      return HiveDatabaseConverter.fromHiveDB(db);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e, ExceptionTarget.schema(databaseName));
    }
  }

  public void alterDatabase(String catalogName, String databaseName, HiveSchema database) {
    try {
      Database db = HiveDatabaseConverter.toHiveDb(database);
      client.alterDatabase(databaseName, db);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e, ExceptionTarget.schema(databaseName));
    }
  }

  public void dropDatabase(String catalogName, String databaseName, boolean cascade) {
    try {
      client.dropDatabase(databaseName, false, false, cascade);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e, ExceptionTarget.schema(databaseName));
    }
  }

  public List<String> getAllTables(String catalogName, String databaseName) {
    try {
      return client.getAllTables(databaseName);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e, ExceptionTarget.schema(databaseName));
    }
  }

  public List<String> listTablesByType(
      String catalogName, String databaseName, String tablePattern, String tableType) {
    try {
      return client.getTables(databaseName, tablePattern, TableType.valueOf(tableType));
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e, ExceptionTarget.schema(databaseName));
    }
  }

  public List<String> listTableNamesByFilter(
      String catalogName, String databaseName, String filter, short pageSize) {
    try {
      return client.listTableNamesByFilter(databaseName, filter, pageSize);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e, ExceptionTarget.schema(databaseName));
    }
  }

  public HiveTable getTable(String catalogName, String databaseName, String tableName) {
    try {
      var tb = client.getTable(databaseName, tableName);
      return HiveTableConverter.fromHiveTable(tb);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e, ExceptionTarget.table(tableName));
    }
  }

  public void alterTable(
      String catalogName,
      String databaseName,
      String tableName,
      HiveTable alteredHiveTable,
      boolean skipStatsUpdate) {
    try {
      var tb = HiveTableConverter.toHiveTable(alteredHiveTable);
      if (skipStatsUpdate) {
        // Instruct the metastore not to recompute statistics for this alter, so it does not access
        // the table's storage location. Hive 2.x has no catalog-aware alter, so the database name
        // is used directly.
        client.alter_table_with_environmentContext(
            databaseName, tableName, tb, doNotUpdateStatsContext());
      } else {
        client.alter_table(databaseName, tableName, tb);
      }
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e, ExceptionTarget.table(tableName));
    }
  }

  public void dropTable(
      String catalogName,
      String databaseName,
      String tableName,
      boolean deleteData,
      boolean ifPurge) {
    try {
      client.dropTable(databaseName, tableName, deleteData, ifPurge);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e, ExceptionTarget.table(tableName));
    }
  }

  public void createTable(HiveTable hiveTable) {
    try {
      var tb = HiveTableConverter.toHiveTable(hiveTable);
      client.createTable(tb);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e, ExceptionTarget.table(hiveTable.name()));
    }
  }

  public List<String> listPartitionNames(HiveTable table, short pageSize) {
    try {
      String databaseName = table.databaseName();
      return client.listPartitionNames(databaseName, table.name(), pageSize);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e, ExceptionTarget.table(table.name()));
    }
  }

  public List<HivePartition> listPartitions(HiveTable table, short pageSize) {
    try {
      String databaseName = table.databaseName();
      var partitions = client.listPartitions(databaseName, table.name(), pageSize);
      return partitions.stream().map(p -> HiveTableConverter.fromHivePartition(table, p)).toList();
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e, ExceptionTarget.table(table.name()));
    }
  }

  public List<HivePartition> listPartitions(
      HiveTable table, List<String> filterPartitionValueList, short pageSize) {
    try {
      String databaseName = table.databaseName();
      var partitions =
          client.listPartitions(databaseName, table.name(), filterPartitionValueList, pageSize);
      return partitions.stream().map(p -> HiveTableConverter.fromHivePartition(table, p)).toList();
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e, ExceptionTarget.table(table.name()));
    }
  }

  public HivePartition getPartition(HiveTable table, String partitionName) {
    try {
      String databaseName = table.databaseName();
      var partitionValues = HivePartition.extractPartitionValues(partitionName);
      var partition = client.getPartition(databaseName, table.name(), partitionValues);
      return HiveTableConverter.fromHivePartition(table, partition);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(
          e, ExceptionTarget.partition(partitionName));
    }
  }

  public HivePartition addPartition(HiveTable table, HivePartition partition) {
    try {
      String databaseName = table.databaseName();
      var hivePartition = HiveTableConverter.toHivePartition(databaseName, table, partition);
      var addedPartition = client.add_partition(hivePartition);
      return HiveTableConverter.fromHivePartition(table, addedPartition);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(
          e, ExceptionTarget.partition(partition.name()));
    }
  }

  public void dropPartition(
      String catalogName,
      String databaseName,
      String tableName,
      String partitionName,
      boolean deleteData) {
    try {
      var partitionValues = HivePartition.extractPartitionValues(partitionName);
      client.dropPartition(databaseName, tableName, partitionValues, deleteData);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(
          e, ExceptionTarget.partition(partitionName));
    }
  }

  public String getDelegationToken(String finalPrincipalName, String userName) {
    try {
      return client.getDelegationToken(finalPrincipalName, userName);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(
          e, ExceptionTarget.other(finalPrincipalName));
    }
  }

  public List<HiveTable> getTableObjectsByName(
      String catalogName, String databaseName, List<String> allTables) {
    try {
      // Hive2 doesn't support catalog, so we ignore catalogName and use databaseName
      var tables = client.getTableObjectsByName(databaseName, allTables);
      return tables.stream().map(HiveTableConverter::fromHiveTable).toList();
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e, ExceptionTarget.schema(databaseName));
    }
  }

  public List<String> getCatalogs() {
    return List.of();
  }

  public void createCatalog(String catalogName, String location, String description) {
    throw new UnsupportedOperationException(
        "Catalog creation is not supported in Hive 2.x. Please upgrade to Hive 3.x.");
  }

  public void close() throws Exception {
    if (client != null) {
      client.close();
    }
  }

  /**
   * Builds an {@link EnvironmentContext} that tells the metastore not to recompute table statistics
   * during an alter, avoiding an access to the table's storage location.
   *
   * @return An environment context with {@code DO_NOT_UPDATE_STATS} set to {@code true}.
   */
  protected EnvironmentContext doNotUpdateStatsContext() {
    return new EnvironmentContext(
        Collections.singletonMap(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE));
  }
}
