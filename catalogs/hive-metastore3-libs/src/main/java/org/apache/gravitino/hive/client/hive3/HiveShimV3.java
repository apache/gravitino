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

package org.apache.gravitino.hive.client.hive3;

import static org.apache.gravitino.hive.client.HiveClientClassLoader.HiveVersion.HIVE3;
import static org.apache.gravitino.hive.client.Util.updateConfigurationFromProperties;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.hive.HivePartition;
import org.apache.gravitino.hive.HiveSchema;
import org.apache.gravitino.hive.HiveTable;
import org.apache.gravitino.hive.client.HiveExceptionConverter;
import org.apache.gravitino.hive.client.HiveExceptionConverter.ExceptionTarget;
import org.apache.gravitino.hive.client.HiveShim;
import org.apache.gravitino.hive.converter.HiveColumnDefaultValueConverter;
import org.apache.gravitino.hive.converter.HiveDatabaseConverter;
import org.apache.gravitino.hive.converter.HiveTableConverter;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.utils.RandomNameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hive 3.x metastore shim. Hive 3.x supports multiple metastore catalogs and NOT NULL / DEFAULT
 * column constraints, so every operation below is catalog-aware and column constraints are kept in
 * sync with the metastore.
 */
public class HiveShimV3 extends HiveShim {

  private static final Logger LOG = LoggerFactory.getLogger(HiveShimV3.class);

  // Keeps generated constraint names well within the metastore's 400 character limit
  private static final int MAX_CONSTRAINT_PREFIX = 128;
  // Constraints are recorded for metadata only: enabled and not validated against existing data.
  // rely_cstr is deliberately false: Gravitino never validates existing data against newly added
  // constraints, so telling the query optimizer to rely on them could produce incorrect results.
  private static final boolean CONSTRAINT_ENABLE = true;
  private static final boolean CONSTRAINT_VALIDATE = false;
  private static final boolean CONSTRAINT_RELY = false;

  public HiveShimV3(Properties properties) {
    super(HIVE3, properties);
  }

  @Override
  public IMetaStoreClient createMetaStoreClient(Properties properties) {
    try {
      Configuration conf = new Configuration();
      updateConfigurationFromProperties(properties, conf);
      return RetryingMetaStoreClient.getProxy(conf, false);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(
          e, ExceptionTarget.other("MetaStoreClient"));
    }
  }

  @Override
  public void createDatabase(HiveSchema database) {
    Database db = HiveDatabaseConverter.toHiveDb(database);
    db.setCatalogName(database.catalogName());
    invoke(ExceptionTarget.schema(database.name()), () -> client.createDatabase(db));
  }

  @Override
  public List<String> getAllDatabases(String catalogName) {
    return invoke(ExceptionTarget.catalog(catalogName), () -> client.getAllDatabases(catalogName));
  }

  @Override
  public HiveSchema getDatabase(String catalogName, String databaseName) {
    Database db =
        invoke(
            ExceptionTarget.schema(databaseName),
            () -> client.getDatabase(catalogName, databaseName));
    return HiveDatabaseConverter.fromHiveDB(db);
  }

  @Override
  public void alterDatabase(String catalogName, String databaseName, HiveSchema database) {
    Database db = HiveDatabaseConverter.toHiveDb(database);
    db.setCatalogName(catalogName);
    invoke(
        ExceptionTarget.schema(databaseName),
        () -> client.alterDatabase(catalogName, databaseName, db));
  }

  @Override
  public void dropDatabase(String catalogName, String databaseName, boolean cascade) {
    invoke(
        ExceptionTarget.schema(databaseName),
        () -> client.dropDatabase(catalogName, databaseName, true, false, cascade));
  }

  @Override
  public List<String> getAllTables(String catalogName, String databaseName) {
    return invoke(
        ExceptionTarget.schema(databaseName), () -> client.getAllTables(catalogName, databaseName));
  }

  @Override
  public List<String> listTablesByType(
      String catalogName, String databaseName, String tablePattern, String tableType) {
    TableType hiveTableType = TableType.valueOf(tableType);
    return invoke(
        ExceptionTarget.schema(databaseName),
        () -> client.getTables(catalogName, databaseName, tablePattern, hiveTableType));
  }

  @Override
  public List<String> listTableNamesByFilter(
      String catalogName, String databaseName, String filter, short pageSize) {
    return invoke(
        ExceptionTarget.schema(databaseName),
        () -> client.listTableNamesByFilter(catalogName, databaseName, filter, pageSize));
  }

  @Override
  public HiveTable getTable(String catalogName, String databaseName, String tableName) {
    Table tb =
        invoke(
            ExceptionTarget.table(tableName),
            () -> client.getTable(catalogName, databaseName, tableName));
    ColumnConstraints constraints = loadColumnConstraints(catalogName, databaseName, tableName);
    return HiveTableConverter.fromHiveTable(
        tb, notNullColumns(constraints), defaultValues(constraints));
  }

  @Override
  public void alterTable(
      String catalogName,
      String databaseName,
      String tableName,
      HiveTable alteredHiveTable,
      boolean skipStatsUpdate) {
    Table tb = HiveTableConverter.toHiveTable(alteredHiveTable);
    tb.setCatName(catalogName);
    if (skipStatsUpdate) {
      // Property-only and comment-only alters cannot change columns, so the constraints stay as
      // they are. Instruct the metastore not to recompute statistics for this alter, so it does
      // not access the table's storage location.
      invoke(
          ExceptionTarget.table(tableName),
          () ->
              client.alter_table(
                  catalogName, databaseName, tableName, tb, doNotUpdateStatsContext()));
      return;
    }

    // The Thrift Table passed to alter_table carries no constraint information, so constraints
    // are rewritten around it: the existing ones are dropped first (while their columns and the
    // old table name still exist) and the desired set is added after the alter (once new columns
    // and names are in place). The desired set is built before the first metastore write so that
    // an unconvertible default value leaves the table untouched.
    ColumnConstraints existing = loadColumnConstraints(catalogName, databaseName, tableName);
    ColumnConstraints desired =
        buildColumnConstraints(
            catalogName,
            alteredHiveTable.databaseName(),
            alteredHiveTable.name(),
            alteredHiveTable.columns());
    dropColumnConstraints(catalogName, databaseName, tableName, existing);
    try {
      invoke(
          ExceptionTarget.table(tableName),
          () -> client.alter_table(catalogName, databaseName, tableName, tb));
    } catch (RuntimeException e) {
      // The table is unchanged, so the dropped constraints can be put back as they were.
      restoreColumnConstraints(databaseName, tableName, existing, e);
      throw e;
    }
    try {
      addColumnConstraints(desired);
    } catch (RuntimeException e) {
      String message =
          String.format(
              "Table %s.%s was altered but its column constraints %s were dropped and could not "
                  + "be re-created; the NOT NULL and DEFAULT constraints must be re-applied "
                  + "manually",
              alteredHiveTable.databaseName(), alteredHiveTable.name(), desired.names);
      LOG.error(message, e);
      throw new RuntimeException(message, e);
    }
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
        () -> client.dropTable(catalogName, databaseName, tableName, deleteData, ifPurge));
  }

  @Override
  public void createTable(HiveTable hiveTable) {
    String catalogName = hiveTable.catalogName();
    Table tb = HiveTableConverter.toHiveTable(hiveTable);
    tb.setCatName(catalogName);
    ColumnConstraints constraints =
        buildColumnConstraints(
            catalogName, hiveTable.databaseName(), hiveTable.name(), hiveTable.columns());
    if (constraints.isEmpty()) {
      invoke(ExceptionTarget.table(hiveTable.name()), () -> client.createTable(tb));
      return;
    }
    invoke(
        ExceptionTarget.table(hiveTable.name()),
        () ->
            client.createTableWithConstraints(
                tb,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                constraints.notNulls,
                constraints.defaults,
                Collections.emptyList()));
  }

  @Override
  public List<String> listPartitionNames(HiveTable table, short pageSize) {
    String catalogName = table.catalogName();
    String databaseName = table.databaseName();
    return invoke(
        ExceptionTarget.table(table.name()),
        () -> client.listPartitionNames(catalogName, databaseName, table.name(), (int) pageSize));
  }

  @Override
  public List<HivePartition> listPartitions(HiveTable table, short pageSize) {
    String catalogName = table.catalogName();
    String databaseName = table.databaseName();
    List<Partition> partitions =
        invoke(
            ExceptionTarget.table(table.name()),
            () -> client.listPartitions(catalogName, databaseName, table.name(), (int) pageSize));
    return partitions.stream().map(p -> HiveTableConverter.fromHivePartition(table, p)).toList();
  }

  @Override
  public List<HivePartition> listPartitions(
      HiveTable table, List<String> filterPartitionValueList, short pageSize) {
    String catalogName = table.catalogName();
    String databaseName = table.databaseName();
    List<Partition> partitions =
        invoke(
            ExceptionTarget.table(table.name()),
            () ->
                client.listPartitions(
                    catalogName,
                    databaseName,
                    table.name(),
                    filterPartitionValueList,
                    (int) pageSize));
    return partitions.stream().map(p -> HiveTableConverter.fromHivePartition(table, p)).toList();
  }

  @Override
  public HivePartition getPartition(HiveTable table, String partitionName) {
    String catalogName = table.catalogName();
    String databaseName = table.databaseName();
    List<String> partitionValues = HivePartition.extractPartitionValues(partitionName);
    Partition partition =
        invoke(
            ExceptionTarget.partition(partitionName),
            () -> client.getPartition(catalogName, databaseName, table.name(), partitionValues));
    return HiveTableConverter.fromHivePartition(table, partition);
  }

  @Override
  public HivePartition addPartition(HiveTable table, HivePartition partition) {
    String catalogName = table.catalogName();
    String databaseName = table.databaseName();
    Partition hivePartition = HiveTableConverter.toHivePartition(databaseName, table, partition);
    hivePartition.setCatName(catalogName);
    Partition addedPartition =
        invoke(
            ExceptionTarget.partition(partition.name()), () -> client.add_partition(hivePartition));
    return HiveTableConverter.fromHivePartition(table, addedPartition);
  }

  @Override
  public void dropPartition(
      String catalogName,
      String databaseName,
      String tableName,
      String partitionName,
      boolean deleteData) {
    List<String> partitionValues = HivePartition.extractPartitionValues(partitionName);
    invoke(
        ExceptionTarget.partition(partitionName),
        () ->
            client.dropPartition(
                catalogName, databaseName, tableName, partitionValues, deleteData));
  }

  @Override
  public List<HiveTable> getTableObjectsByName(
      String catalogName, String databaseName, List<String> allTables) {
    List<Table> tables =
        invoke(
            ExceptionTarget.schema(databaseName),
            () -> client.getTableObjectsByName(catalogName, databaseName, allTables));
    return tables.stream()
        .map(
            tb -> {
              ColumnConstraints constraints =
                  loadColumnConstraints(catalogName, databaseName, tb.getTableName());
              return HiveTableConverter.fromHiveTable(
                  tb, notNullColumns(constraints), defaultValues(constraints));
            })
        .toList();
  }

  @Override
  public List<String> getCatalogs() {
    return invoke(ExceptionTarget.other(""), client::getCatalogs);
  }

  @Override
  public void createCatalog(String catalogName, String location, String description) {
    Catalog catalog = new Catalog(catalogName, location);
    if (StringUtils.isNotBlank(description)) {
      catalog.setDescription(description);
    }
    invoke(ExceptionTarget.catalog(catalogName), () -> client.createCatalog(catalog));
  }

  @Override
  public void close() throws Exception {
    client.close();
  }

  /** NOT NULL and DEFAULT constraints of a table, as Hive metastore constraint objects. */
  private static class ColumnConstraints {
    final List<SQLNotNullConstraint> notNulls = new ArrayList<>();
    final List<SQLDefaultConstraint> defaults = new ArrayList<>();
    // constraint names of both kinds
    final Set<String> names = new HashSet<>();

    boolean isEmpty() {
      return notNulls.isEmpty() && defaults.isEmpty();
    }
  }

  private ColumnConstraints loadColumnConstraints(
      String catalogName, String databaseName, String tableName) {
    ExceptionTarget target = ExceptionTarget.table(tableName);
    ColumnConstraints constraints = new ColumnConstraints();
    List<SQLNotNullConstraint> notNulls =
        invoke(
            target,
            () ->
                client.getNotNullConstraints(
                    new NotNullConstraintsRequest(catalogName, databaseName, tableName)));
    for (SQLNotNullConstraint constraint : notNulls) {
      constraints.notNulls.add(constraint);
      constraints.names.add(constraint.getNn_name());
    }
    List<SQLDefaultConstraint> defaults =
        invoke(
            target,
            () ->
                client.getDefaultConstraints(
                    new DefaultConstraintsRequest(catalogName, databaseName, tableName)));
    for (SQLDefaultConstraint constraint : defaults) {
      constraints.defaults.add(constraint);
      constraints.names.add(constraint.getDc_name());
    }
    return constraints;
  }

  private Set<String> notNullColumns(ColumnConstraints constraints) {
    Set<String> columns = new HashSet<>();
    for (SQLNotNullConstraint constraint : constraints.notNulls) {
      columns.add(constraint.getColumn_name());
    }
    return columns;
  }

  private Map<String, String> defaultValues(ColumnConstraints constraints) {
    Map<String, String> values = new HashMap<>();
    for (SQLDefaultConstraint constraint : constraints.defaults) {
      values.put(constraint.getColumn_name(), constraint.getDefault_value());
    }
    return values;
  }

  private void dropColumnConstraints(
      String catalogName, String databaseName, String tableName, ColumnConstraints constraints) {
    for (String constraintName : constraints.names) {
      invoke(
          ExceptionTarget.table(tableName),
          () -> client.dropConstraint(catalogName, databaseName, tableName, constraintName));
    }
  }

  private void addColumnConstraints(ColumnConstraints constraints) {
    if (!constraints.notNulls.isEmpty()) {
      invoke(ExceptionTarget.other(""), () -> client.addNotNullConstraint(constraints.notNulls));
    }
    if (!constraints.defaults.isEmpty()) {
      invoke(ExceptionTarget.other(""), () -> client.addDefaultConstraint(constraints.defaults));
    }
  }

  private void restoreColumnConstraints(
      String databaseName, String tableName, ColumnConstraints constraints, Exception cause) {
    if (constraints.isEmpty()) {
      return;
    }
    try {
      addColumnConstraints(constraints);
    } catch (RuntimeException restoreFailure) {
      cause.addSuppressed(restoreFailure);
      LOG.error(
          "Failed to restore column constraints {} of table {}.{} after a failed alter; "
              + "the NOT NULL and DEFAULT constraints must be re-applied manually",
          constraints.names,
          databaseName,
          tableName,
          restoreFailure);
    }
  }

  private ColumnConstraints buildColumnConstraints(
      String catalogName, String databaseName, String tableName, Column[] columns) {
    ColumnConstraints constraints = new ColumnConstraints();
    for (Column column : columns) {
      if (!column.nullable()) {
        String name = constraintName(tableName, column.name(), "nn");
        constraints.notNulls.add(
            new SQLNotNullConstraint(
                catalogName,
                databaseName,
                tableName,
                column.name(),
                name,
                CONSTRAINT_ENABLE,
                CONSTRAINT_VALIDATE,
                CONSTRAINT_RELY));
        constraints.names.add(name);
      }
      String defaultValue = HiveColumnDefaultValueConverter.fromGravitino(column.defaultValue());
      if (defaultValue != null) {
        String name = constraintName(tableName, column.name(), "dv");
        constraints.defaults.add(
            new SQLDefaultConstraint(
                catalogName,
                databaseName,
                tableName,
                column.name(),
                defaultValue,
                name,
                CONSTRAINT_ENABLE,
                CONSTRAINT_VALIDATE,
                CONSTRAINT_RELY));
        constraints.names.add(name);
      }
    }
    return constraints;
  }

  private static String constraintName(String tableName, String columnName, String suffix) {
    // Constraint names are unique across the whole metastore and the prefix does not include the
    // database name, so a random suffix keeps same-named tables in different databases apart.
    // The prefix is bounded so the name fits the metastore's constraint name column.
    String prefix = tableName + "_" + columnName;
    if (prefix.length() > MAX_CONSTRAINT_PREFIX) {
      prefix = prefix.substring(0, MAX_CONSTRAINT_PREFIX);
    }
    return RandomNameUtils.genRandomName(prefix + "_" + suffix);
  }

  /** A metastore call, deferred so exceptions can be converted uniformly by {@link #invoke}. */
  @FunctionalInterface
  private interface MetastoreCall<T> {
    T call() throws Exception;
  }

  /**
   * A void metastore call, deferred so exceptions can be converted uniformly by {@link #invoke}.
   */
  @FunctionalInterface
  private interface VoidMetastoreCall {
    void call() throws Exception;
  }

  private <T> T invoke(ExceptionTarget target, MetastoreCall<T> call) {
    try {
      return call.call();
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e, target);
    }
  }

  private void invoke(ExceptionTarget target, VoidMetastoreCall call) {
    try {
      call.call();
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(e, target);
    }
  }
}
