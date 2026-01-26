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
import java.util.Properties;
import org.apache.gravitino.hive.HivePartition;
import org.apache.gravitino.hive.HiveSchema;
import org.apache.gravitino.hive.HiveTable;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.thrift.TException;

/**
 * Java translation of Scala's `Shim` sealed abstract class.
 *
 * <p>This class declares the compatibility layer between Spark and different Hive versions.
 * Concrete subclasses (e.g. HiveShimV2, HiveShimV3 ...) must implement these methods according to
 * the behavior of the corresponding Hive release.
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

  public abstract IMetaStoreClient createMetaStoreClient(Properties properties);

  public List<String> getAllDatabases(String catalogName) {
    try {
      return client.getAllDatabases();
    } catch (TException e) {
      throw HiveExceptionConverter.toGravitinoException(
          e, HiveExceptionConverter.ExceptionTarget.catalog(catalogName));
    }
  }

  public abstract void createDatabase(HiveSchema database);

  public abstract HiveSchema getDatabase(String catalogName, String databaseName);

  public abstract void alterDatabase(String catalogName, String databaseName, HiveSchema database);

  public abstract void dropDatabase(String catalogName, String databaseName, boolean cascade);

  public abstract List<String> getAllTables(String catalogName, String databaseName);

  public abstract List<String> listTableNamesByFilter(
      String catalogName, String databaseName, String filter, short pageSize);

  public abstract HiveTable getTable(String catalogName, String databaseName, String tableName);

  public abstract void alterTable(
      String catalogName, String databaseName, String tableName, HiveTable alteredHiveTable);

  public abstract void dropTable(
      String catalogName,
      String databaseName,
      String tableName,
      boolean deleteData,
      boolean ifPurge);

  public abstract void createTable(HiveTable hiveTable);

  public abstract List<String> listPartitionNames(HiveTable table, short pageSize);

  public abstract List<HivePartition> listPartitions(HiveTable table, short pageSize);

  public abstract List<HivePartition> listPartitions(
      HiveTable table, List<String> filterPartitionValueList, short pageSize);

  public abstract HivePartition getPartition(HiveTable table, String partitionName);

  public abstract HivePartition addPartition(HiveTable table, HivePartition partition);

  public abstract void dropPartition(
      String catalogName, String databaseName, String tableName, String partitionName, boolean b);

  public abstract String getDelegationToken(String finalPrincipalName, String userName);

  public abstract List<HiveTable> getTableObjectsByName(
      String catalogName, String databaseName, List<String> allTables);

  public abstract List<String> getCatalogs();

  public abstract void createCatalog(String catalogName, String location, String description);

  public void close() throws Exception {
    if (client != null) {
      client.close();
    }
  }
}
