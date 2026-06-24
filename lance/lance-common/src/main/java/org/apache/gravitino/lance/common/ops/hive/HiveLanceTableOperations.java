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

import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_LOCATION;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.lance.common.ops.LanceTableOperations;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.lance.namespace.errors.InvalidInputException;
import org.lance.namespace.errors.ServiceUnavailableException;
import org.lance.namespace.errors.TableAlreadyExistsException;
import org.lance.namespace.errors.TableNotFoundException;
import org.lance.namespace.model.CreateTableResponse;
import org.lance.namespace.model.DeclareTableResponse;
import org.lance.namespace.model.DeregisterTableResponse;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.DropTableResponse;
import org.lance.namespace.model.RegisterTableResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Table operations for the Hive Lance backend. Tables are 2-level {@code db.table} identifiers
 * registered as external Lance tables in the Hive Metastore. The Hive backend acts as a pointer
 * registry only: it never writes Lance data, so {@code managedVersioning} is always {@code false}.
 */
public class HiveLanceTableOperations implements LanceTableOperations {

  private static final Logger LOG = LoggerFactory.getLogger(HiveLanceTableOperations.class);

  private static final String MODE_CREATE = "create";
  private static final String MODE_EXIST_OK = "exist_ok";
  private static final String MODE_EXISTOK = "existok";
  private static final String MODE_OVERWRITE = "overwrite";

  private final HiveClientPool clientPool;
  private final String root;

  /**
   * Create table operations bound to the given wrapper.
   *
   * @param wrapper the owning namespace wrapper
   */
  public HiveLanceTableOperations(HiveLanceNamespaceWrapper wrapper) {
    this(wrapper.getClientPool(), wrapper.getRoot());
  }

  @VisibleForTesting
  HiveLanceTableOperations(HiveClientPool clientPool, String root) {
    this.clientPool = clientPool;
    this.root = root;
  }

  @Override
  public DescribeTableResponse describeTable(
      String tableId,
      String delimiter,
      Optional<Long> version,
      boolean checkDeclared,
      boolean loadDetailedMetadata) {
    if (loadDetailedMetadata) {
      throw new InvalidInputException(
          "load_detailed_metadata=true is not supported for the Hive backend");
    }
    if (version.isPresent()) {
      LOG.warn(
          "describeTable: version={} requested for table {} but versioned describe is not "
              + "implemented; returning latest version instead",
          version.get(),
          tableId);
    }

    ObjectIdentifier nsId = ObjectIdentifier.of(tableId, Pattern.quote(delimiter));
    ValidationUtil.checkArgument(
        nsId.levels() == 2, "Expect 2-level table identifier but get %s", tableId);

    String db = nsId.levelAtListPos(0).toLowerCase(Locale.ROOT);
    String table = nsId.levelAtListPos(1).toLowerCase(Locale.ROOT);
    Table hmsTable = requireTable(db, table, tableId);
    HiveUtil.validateLanceTable(hmsTable);

    String location = hmsTable.getSd() != null ? hmsTable.getSd().getLocation() : null;
    if (StringUtils.isBlank(location)) {
      throw new TableNotFoundException(
          String.format("Table does not have a location: %s", tableId),
          HiveErrorType.TABLE_NOT_FOUND.getType(),
          tableId);
    }

    DescribeTableResponse response = new DescribeTableResponse();
    response.setLocation(location);
    response.setProperties(hmsTable.getParameters());
    response.setManagedVersioning(false);
    return response;
  }

  @Override
  public CreateTableResponse createTable(
      String tableId,
      String mode,
      String delimiter,
      String tableLocation,
      Map<String, String> tableProperties,
      byte[] arrowStreamBody) {
    ObjectIdentifier nsId = ObjectIdentifier.of(tableId, Pattern.quote(delimiter));
    ValidationUtil.checkArgument(
        nsId.levels() == 2, "Expect 2-level table identifier but get %s", tableId);

    String db = nsId.levelAtListPos(0).toLowerCase(Locale.ROOT);
    String table = nsId.levelAtListPos(1).toLowerCase(Locale.ROOT);

    String location = tableLocation;
    if (StringUtils.isBlank(location)) {
      location = getDefaultTableLocation(db, table);
    }

    String normalizedMode = mode != null ? mode.toLowerCase(Locale.ROOT) : MODE_CREATE;
    Map<String, String> params = HiveUtil.createLanceTableParams(tableProperties);
    registerLanceTable(db, table, location, params, normalizedMode, tableId);

    CreateTableResponse response = new CreateTableResponse();
    response.setLocation(location);
    response.setProperties(params);
    return response;
  }

  @Override
  public DeclareTableResponse declareTable(
      String tableId, String delimiter, String tableLocation, Map<String, String> tableProperties) {
    ObjectIdentifier nsId = ObjectIdentifier.of(tableId, Pattern.quote(delimiter));
    ValidationUtil.checkArgument(
        nsId.levels() == 2, "Expect 2-level table identifier but get %s", tableId);

    String db = nsId.levelAtListPos(0).toLowerCase(Locale.ROOT);
    String table = nsId.levelAtListPos(1).toLowerCase(Locale.ROOT);

    String location = tableLocation;
    if (StringUtils.isBlank(location)) {
      location = getDefaultTableLocation(db, table);
    }

    Map<String, String> params = HiveUtil.createLanceTableParams(tableProperties);
    registerLanceTable(db, table, location, params, MODE_CREATE, tableId);

    DeclareTableResponse response = new DeclareTableResponse();
    response.setLocation(location);
    response.setProperties(params);
    response.setManagedVersioning(false);
    return response;
  }

  @Override
  public RegisterTableResponse registerTable(
      String tableId, String mode, String delimiter, Map<String, String> tableProperties) {
    ObjectIdentifier nsId = ObjectIdentifier.of(tableId, Pattern.quote(delimiter));
    ValidationUtil.checkArgument(
        nsId.levels() == 2, "Expect 2-level table identifier but get %s", tableId);

    String location = tableProperties != null ? tableProperties.get(LANCE_LOCATION) : null;
    ValidationUtil.checkNotNullOrEmptyString(
        location, String.format("Property '%s' is required to register a table", LANCE_LOCATION));

    String db = nsId.levelAtListPos(0).toLowerCase(Locale.ROOT);
    String table = nsId.levelAtListPos(1).toLowerCase(Locale.ROOT);

    String normalizedMode = mode != null ? mode.toLowerCase(Locale.ROOT) : MODE_CREATE;
    Map<String, String> params = HiveUtil.createLanceTableParams(tableProperties);
    registerLanceTable(db, table, location, params, normalizedMode, tableId);

    RegisterTableResponse response = new RegisterTableResponse();
    response.setLocation(location);
    response.setProperties(params);
    return response;
  }

  @Override
  public DeregisterTableResponse deregisterTable(String tableId, String delimiter) {
    ObjectIdentifier nsId = ObjectIdentifier.of(tableId, Pattern.quote(delimiter));
    ValidationUtil.checkArgument(
        nsId.levels() == 2, "Expect 2-level table identifier but get %s", tableId);

    String db = nsId.levelAtListPos(0).toLowerCase(Locale.ROOT);
    String table = nsId.levelAtListPos(1).toLowerCase(Locale.ROOT);
    String location = doDropTable(db, table, tableId, false);

    DeregisterTableResponse response = new DeregisterTableResponse();
    response.setId(nsId.listStyleId());
    response.setLocation(location);
    return response;
  }

  @Override
  public boolean tableExists(String tableId, String delimiter) {
    ObjectIdentifier nsId = ObjectIdentifier.of(tableId, Pattern.quote(delimiter));
    ValidationUtil.checkArgument(
        nsId.levels() == 2, "Expect 2-level table identifier but get %s", tableId);

    String db = nsId.levelAtListPos(0).toLowerCase(Locale.ROOT);
    String table = nsId.levelAtListPos(1).toLowerCase(Locale.ROOT);
    Optional<Table> hmsTable = HiveUtil.getTable(clientPool, db, table);
    if (!hmsTable.isPresent()) {
      return false;
    }
    Map<String, String> params = hmsTable.get().getParameters();
    return params != null
        && HiveUtil.TABLE_TYPE_LANCE.equalsIgnoreCase(params.get(HiveUtil.TABLE_TYPE_PARAM));
  }

  @Override
  public DropTableResponse dropTable(String tableId, String delimiter) {
    ObjectIdentifier nsId = ObjectIdentifier.of(tableId, Pattern.quote(delimiter));
    ValidationUtil.checkArgument(
        nsId.levels() == 2, "Expect 2-level table identifier but get %s", tableId);

    String db = nsId.levelAtListPos(0).toLowerCase(Locale.ROOT);
    String table = nsId.levelAtListPos(1).toLowerCase(Locale.ROOT);
    String location = doDropTable(db, table, tableId, true);

    DropTableResponse response = new DropTableResponse();
    response.setId(nsId.listStyleId());
    response.setLocation(location);
    return response;
  }

  @Override
  public Object alterTable(String tableId, String delimiter, Object request) {
    throw new InvalidInputException(
        "Column alteration is not supported for the Hive backend; the Hive Metastore stores Lance"
            + " tables as pointers without column metadata");
  }

  private void registerLanceTable(
      String db,
      String tableName,
      String location,
      Map<String, String> params,
      String mode,
      String tableId) {
    try {
      Optional<Table> existing = HiveUtil.getTable(clientPool, db, tableName);
      if (existing.isPresent()) {
        if (MODE_EXIST_OK.equals(mode) || MODE_EXISTOK.equals(mode)) {
          return;
        } else if (MODE_OVERWRITE.equals(mode)) {
          clientPool.run(
              client -> {
                client.dropTable(db, tableName, false, true);
                return null;
              });
        } else {
          throw new TableAlreadyExistsException(
              String.format("Table %s.%s already exists", db, tableName),
              HiveErrorType.TABLE_ALREADY_EXISTS.getType(),
              tableId);
        }
      }

      Table table = new Table();
      table.setDbName(db);
      table.setTableName(tableName);
      table.setTableType(HiveUtil.EXTERNAL_TABLE);
      table.setPartitionKeys(Lists.newArrayList());

      StorageDescriptor sd = new StorageDescriptor();
      sd.setLocation(location);
      sd.setCols(Lists.newArrayList());
      sd.setInputFormat(HiveUtil.LANCE_INPUT_FORMAT);
      sd.setOutputFormat(HiveUtil.LANCE_OUTPUT_FORMAT);
      SerDeInfo serdeInfo = new SerDeInfo();
      serdeInfo.setSerializationLib(HiveUtil.LANCE_SERDE);
      sd.setSerdeInfo(serdeInfo);
      table.setSd(sd);
      table.setParameters(params);

      clientPool.run(
          client -> {
            client.createTable(table);
            return null;
          });
    } catch (TableAlreadyExistsException e) {
      throw e;
    } catch (TException | InterruptedException | RuntimeException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new ServiceUnavailableException(
          "Failed to create table: " + errorMessage(e),
          HiveErrorType.HIVE_META_STORE_ERROR.getType(),
          "");
    }
  }

  private String doDropTable(String db, String tableName, String tableId, boolean deleteData) {
    Table hmsTable = requireTable(db, tableName, tableId);
    HiveUtil.validateLanceTable(hmsTable);
    String location = hmsTable.getSd() != null ? hmsTable.getSd().getLocation() : null;

    try {
      clientPool.run(
          client -> {
            client.dropTable(db, tableName, deleteData, true /* ignoreUnknownTable */);
            return null;
          });
    } catch (TException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new ServiceUnavailableException(
          "Failed to drop table: " + errorMessage(e),
          HiveErrorType.HIVE_META_STORE_ERROR.getType(),
          "");
    }
    return location;
  }

  private Table requireTable(String db, String tableName, String tableId) {
    Optional<Table> hmsTable = HiveUtil.getTable(clientPool, db, tableName);
    if (!hmsTable.isPresent()) {
      throw new TableNotFoundException(
          String.format("Table does not exist: %s", tableId),
          HiveErrorType.TABLE_NOT_FOUND.getType(),
          tableId);
    }
    return hmsTable.get();
  }

  private String getDefaultTableLocation(String db, String tableName) {
    Database database = HiveUtil.getDatabaseOrNull(clientPool, db);
    if (database != null && StringUtils.isNotBlank(database.getLocationUri())) {
      String dbLocation = database.getLocationUri();
      if (!dbLocation.endsWith("/")) {
        dbLocation += "/";
      }
      return dbLocation + tableName;
    }
    return String.format("%s/%s.db/%s", root, db, tableName);
  }

  private static String errorMessage(Exception e) {
    return e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
  }
}
