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
package org.apache.gravitino.lance.common.ops.hive;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.lance.namespace.errors.InvalidInputException;
import org.lance.namespace.errors.ServiceUnavailableException;

/** Helpers that translate between Hive Metastore objects and the Lance table model. */
final class HiveUtil {

  /** Hive Metastore table parameter key that marks the format of the table. */
  static final String TABLE_TYPE_PARAM = "table_type";
  /** Hive Metastore table parameter value identifying a Lance table. */
  static final String TABLE_TYPE_LANCE = "lance";
  /** Hive Metastore table parameter key recording who manages the table data. */
  static final String MANAGED_BY_PARAM = "managed_by";
  /** Hive Metastore table parameter value for storage-managed Lance tables. */
  static final String MANAGED_BY_STORAGE = "storage";

  /** Hadoop input format used for registered Lance tables. */
  static final String LANCE_INPUT_FORMAT = "org.lance.mapred.LanceInputFormat";
  /** Hadoop output format used for registered Lance tables. */
  static final String LANCE_OUTPUT_FORMAT = "org.lance.mapred.LanceOutputFormat";
  /** SerDe used for registered Lance tables. */
  static final String LANCE_SERDE = "org.lance.mapred.LanceSerDe";

  /** External table type for Lance tables registered in the Hive Metastore. */
  static final String EXTERNAL_TABLE = "EXTERNAL_TABLE";

  private HiveUtil() {}

  static Database getDatabaseOrNull(HiveClientPool clientPool, String db) {
    try {
      return clientPool.run(client -> client.getDatabase(db));
    } catch (NoSuchObjectException e) {
      return null;
    } catch (TException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new ServiceUnavailableException(
          e.getMessage(), HiveErrorType.HIVE_META_STORE_ERROR.getType(), "");
    }
  }

  static Optional<Table> getTable(HiveClientPool clientPool, String db, String table) {
    try {
      return Optional.of(clientPool.run(client -> client.getTable(db, table)));
    } catch (NoSuchObjectException e) {
      return Optional.empty();
    } catch (TException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new ServiceUnavailableException(
          e.getMessage(), HiveErrorType.HIVE_META_STORE_ERROR.getType(), "");
    }
  }

  static void setDatabaseProperties(
      Database database,
      Supplier<String> warehouseLocation,
      String dbName,
      Map<String, String> properties) {
    Map<String, String> parameters = Maps.newHashMap();
    properties.forEach(
        (k, v) -> {
          if (k.equals(HiveNamespaceConfig.DATABASE_DESCRIPTION)) {
            database.setDescription(v);
          } else if (k.equals(HiveNamespaceConfig.DATABASE_LOCATION_URI)) {
            database.setLocationUri(v);
          } else if (v != null) {
            parameters.put(k, v);
          }
        });

    if (!database.isSetLocationUri()) {
      String location = databaseLocation(warehouseLocation.get(), dbName);
      database.setLocationUri(location);
      properties.put(HiveNamespaceConfig.DATABASE_LOCATION_URI, location);
    }
    if (!database.isSetOwnerName()) {
      database.setOwnerName(hadoopUser());
    }
    if (!database.isSetOwnerType()) {
      database.setOwnerType(PrincipalType.USER);
    }

    database.setParameters(parameters);
  }

  static String databaseLocation(String warehouse, String dbName) {
    return String.format("%s/%s", makeQualified(warehouse), dbName);
  }

  static String hadoopUser() {
    try {
      return UserGroupInformation.getCurrentUser().getUserName();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static void validateLanceTable(Table table) {
    Map<String, String> params = table.getParameters();
    if (params == null || !TABLE_TYPE_LANCE.equalsIgnoreCase(params.get(TABLE_TYPE_PARAM))) {
      throw new InvalidInputException(
          String.format(
              "Table %s.%s is not a Lance table", table.getDbName(), table.getTableName()),
          HiveErrorType.INVALID_LANCE_TABLE.getType(),
          String.format("%s.%s", table.getDbName(), table.getTableName()));
    }
  }

  static Map<String, String> createLanceTableParams(Map<String, String> properties) {
    Map<String, String> params = new HashMap<>();
    if (properties != null) {
      params.putAll(properties);
    }
    params.put(TABLE_TYPE_PARAM, TABLE_TYPE_LANCE);
    params.put(MANAGED_BY_PARAM, MANAGED_BY_STORAGE);
    return params;
  }

  static String makeQualified(String path) {
    if (path == null) {
      return null;
    }
    return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
  }
}
