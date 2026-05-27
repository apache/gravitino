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

package org.apache.gravitino.catalog.fluss;

import static org.apache.gravitino.StringIdentifier.ID_KEY;
import static org.apache.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.AggFunction;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.DatabaseInfo;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataType;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchColumnException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operations implementation for the Apache Fluss catalog connector.
 *
 * <p>Implements schema CRUD (via {@link SupportsSchemas}) and table CRUD (via {@link TableCatalog})
 * backed by the Apache Fluss Java client API.
 */
public class FlussCatalogOperations implements CatalogOperations, SupportsSchemas, TableCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(FlussCatalogOperations.class);

  private final Function<Configuration, Connection> connectionFactory;
  private Connection connection;
  private FlussAdminOps ops;

  /** Creates a Fluss catalog operations instance. */
  public FlussCatalogOperations() {
    this(ConnectionFactory::createConnection);
  }

  @VisibleForTesting
  FlussCatalogOperations(Function<Configuration, Connection> connectionFactory) {
    this.connectionFactory = connectionFactory;
  }

  @Override
  public void initialize(
      Map<String, String> config, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {
    Configuration flussConfig = toFlussConfiguration(config, info);
    this.connection = connectionFactory.apply(flussConfig);
    this.ops = new FlussAdminOps(connection.getAdmin());
  }

  @Override
  public void testConnection(
      NameIdentifier catalogIdent,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws Exception {
    Preconditions.checkState(ops != null, "Fluss catalog operations has not been initialized");
    ops.doAsAdmin(
        Admin::listDatabases,
        e -> new ConnectionFailedException(e, "Failed to connect to Fluss cluster"));
  }

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    return ops
        .doAsAdmin(
            Admin::listDatabases,
            FlussExceptionConverter.generic("Failed to list Fluss databases under " + namespace))
        .stream()
        .map(database -> NameIdentifier.of(namespace, database))
        .toArray(NameIdentifier[]::new);
  }

  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    DatabaseDescriptor descriptor = FlussSchema.toDatabaseDescriptor(comment, properties);
    ops.doAsAdmin(
        admin -> admin.createDatabase(ident.name(), descriptor, false),
        FlussExceptionConverter.forSchema(ident, "Failed to create Fluss database " + ident));

    return loadSchema(ident);
  }

  @Override
  public boolean schemaExists(NameIdentifier ident) {
    return ops.doAsAdmin(
        admin -> admin.databaseExists(ident.name()),
        FlussExceptionConverter.generic("Failed to check existence of Fluss database " + ident));
  }

  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    DatabaseInfo dbInfo =
        ops.doAsAdmin(
            admin -> admin.getDatabaseInfo(ident.name()),
            FlussExceptionConverter.forSchema(ident, "Failed to load Fluss database " + ident));

    return FlussSchema.fromDatabaseInfo(dbInfo);
  }

  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    Schema schema = loadSchema(ident);
    if (changes == null || changes.length == 0) {
      return schema;
    }
    // TODO: Fluss 0.9.x does not support altering databases yet
    throw new UnsupportedOperationException("Fluss does not currently support schema alterations");
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    if (!schemaExists(ident)) {
      return false;
    }

    ops.doAsAdmin(
        admin -> admin.dropDatabase(ident.name(), false, cascade),
        FlussExceptionConverter.forSchema(ident, "Failed to drop Fluss database " + ident));

    return true;
  }

  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    String database = databaseName(namespace);
    return ops
        .doAsAdmin(
            admin -> admin.listTables(database),
            FlussExceptionConverter.forSchema(
                namespace, "Failed to list Fluss tables under " + namespace))
        .stream()
        .map(table -> NameIdentifier.of(namespace, table))
        .toArray(NameIdentifier[]::new);
  }

  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    TablePath tablePath = toTablePath(ident);
    TableInfo tableInfo = loadTableInfo(ident, tablePath);
    FlussTable table = FlussTable.fromTableInfo(tableInfo);
    table.initOpsContext(ops, tablePath, tableInfo.getPartitionKeys());
    return table;
  }

  @Override
  public Table createTable(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    TableDescriptor descriptor =
        FlussTable.toTableDescriptor(
            columns, comment, properties, partitions, distribution, sortOrders, indexes);
    TablePath tablePath = toTablePath(ident);
    ops.doAsAdmin(
        admin -> admin.createTable(tablePath, descriptor, false),
        FlussExceptionConverter.forTable(ident, "Failed to create Fluss table " + ident));
    return loadTable(ident);
  }

  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    TableChange[] tableChanges = changes == null ? new TableChange[0] : changes;
    if (tableChanges.length == 0) {
      return loadTable(ident);
    }

    TablePath tablePath = toTablePath(ident);
    TableInfo tableInfo = loadTableInfo(ident, tablePath);

    List<org.apache.fluss.metadata.TableChange> flussChanges =
        toFlussTableChanges(tableInfo, tableChanges);

    if (flussChanges.isEmpty()) {
      FlussTable table = FlussTable.fromTableInfo(tableInfo);
      table.initOpsContext(ops, tablePath, tableInfo.getPartitionKeys());
      return table;
    }

    ops.doAsAdmin(
        admin -> admin.alterTable(tablePath, flussChanges, false),
        FlussExceptionConverter.forTable(ident, "Failed to alter Fluss table " + ident));
    return loadTable(ident);
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    TablePath tablePath = toTablePath(ident);
    try {
      ops.doAsAdmin(
          admin -> admin.dropTable(tablePath, false),
          FlussExceptionConverter.forTable(ident, "Failed to drop Fluss table " + ident));
      return true;
    } catch (NoSuchTableException e) {
      LOG.info("Fluss table {} does not exist", ident);
      return false;
    }
  }

  @Override
  public void close() throws IOException {
    if (connection == null) {
      return;
    }

    try {
      connection.close();
    } catch (Exception e) {
      throw new IOException("Failed to close Fluss connection", e);
    } finally {
      ops = null;
      connection = null;
    }
  }

  @VisibleForTesting
  static Configuration toFlussConfiguration(Map<String, String> config, CatalogInfo info) {
    Preconditions.checkArgument(
        config != null && config.containsKey(FlussCatalogPropertiesMetadata.BOOTSTRAP_SERVERS),
        "Missing configuration: %s",
        FlussCatalogPropertiesMetadata.BOOTSTRAP_SERVERS);

    Map<String, String> flussConfig = new LinkedHashMap<>();
    config.entrySet().stream()
        .filter(entry -> entry.getKey().startsWith(CATALOG_BYPASS_PREFIX))
        .forEach(
            entry ->
                flussConfig.put(
                    entry.getKey().substring(CATALOG_BYPASS_PREFIX.length()), entry.getValue()));
    flussConfig.put(
        FlussCatalogPropertiesMetadata.BOOTSTRAP_SERVERS,
        config.get(FlussCatalogPropertiesMetadata.BOOTSTRAP_SERVERS));

    Configuration configuration = Configuration.fromMap(flussConfig);
    if (info != null) {
      String id = config.getOrDefault(ID_KEY, String.valueOf(info.id()));
      configuration.setString(
          ConfigOptions.CLIENT_ID,
          String.format("gravitino-%s-%s.%s", id, info.namespace(), info.name()));
    }
    return configuration;
  }

  @VisibleForTesting
  static List<org.apache.fluss.metadata.TableChange> toFlussTableChanges(
      TableInfo tableInfo, TableChange... changes) {
    FlussColumnChangeVisitor visitor = new FlussColumnChangeVisitor(tableInfo);

    for (TableChange change : changes == null ? new TableChange[0] : changes) {
      applyTableChange(visitor, change);
    }

    return visitor.flussChanges();
  }

  private TableInfo loadTableInfo(NameIdentifier ident, TablePath tablePath)
      throws NoSuchTableException {
    return ops.doAsAdmin(
        admin -> admin.getTableInfo(tablePath),
        FlussExceptionConverter.forTable(ident, "Failed to load Fluss table " + ident));
  }

  private static void applyTableChange(FlussColumnChangeVisitor visitor, TableChange change) {
    if (change instanceof TableChange.SetProperty setProperty) {
      visitor.addFlussChange(
          org.apache.fluss.metadata.TableChange.set(
              setProperty.getProperty(), setProperty.getValue()));
      return;
    }

    if (change instanceof TableChange.RemoveProperty removeProperty) {
      visitor.addFlussChange(
          org.apache.fluss.metadata.TableChange.reset(removeProperty.getProperty()));
      return;
    }

    if (change instanceof TableChange.ColumnChange) {
      applyColumnChange(visitor, (TableChange.ColumnChange) change);
      return;
    }

    if (change instanceof TableChange.UpdateComment) {
      throw new UnsupportedOperationException("Fluss does not support table comment changes");
    }

    if (change instanceof TableChange.RenameTable) {
      throw new UnsupportedOperationException("Fluss does not support renaming tables");
    }

    throw new UnsupportedOperationException("Unsupported Fluss table change: " + change);
  }

  private static void applyColumnChange(
      FlussColumnChangeVisitor visitor, TableChange.ColumnChange columnChange) {
    if (columnChange instanceof TableChange.AddColumn addColumn) {
      visitor.visit(addColumn);
      return;
    }

    if (columnChange instanceof TableChange.DeleteColumn deleteColumn) {
      visitor.visit(deleteColumn);
      return;
    }

    if (columnChange instanceof TableChange.RenameColumn renameColumn) {
      visitor.visit(renameColumn);
      return;
    }

    if (columnChange instanceof TableChange.UpdateColumnType updateColumnType) {
      visitor.visit(updateColumnType);
      return;
    }

    if (columnChange instanceof TableChange.UpdateColumnComment updateColumnComment) {
      visitor.visit(updateColumnComment);
      return;
    }

    if (columnChange instanceof TableChange.UpdateColumnNullability updateColumnNullability) {
      visitor.visit(updateColumnNullability);
      return;
    }

    if (columnChange instanceof TableChange.UpdateColumnPosition updateColumnPosition) {
      visitor.visit(updateColumnPosition);
      return;
    }

    if (columnChange instanceof TableChange.UpdateColumnDefaultValue) {
      throw new UnsupportedOperationException("Fluss does not support column default values");
    }

    if (columnChange instanceof TableChange.UpdateColumnAutoIncrement) {
      throw new UnsupportedOperationException(
          "Fluss does not support altering column auto-increment");
    }

    throw new UnsupportedOperationException("Unsupported Fluss column change: " + columnChange);
  }

  private static class FlussColumnChangeVisitor {
    private final List<org.apache.fluss.metadata.TableChange> flussChanges = new ArrayList<>();
    private final Map<String, org.apache.fluss.metadata.Schema.Column> columnsByName;
    private final List<String> columnOrder;

    private FlussColumnChangeVisitor(TableInfo tableInfo) {
      this.columnsByName =
          tableInfo.getSchema().getColumns().stream()
              .collect(
                  Collectors.toMap(
                      org.apache.fluss.metadata.Schema.Column::getName,
                      c -> c,
                      (left, right) -> right,
                      LinkedHashMap::new));
      this.columnOrder = new ArrayList<>(columnsByName.keySet());
    }

    private List<org.apache.fluss.metadata.TableChange> flussChanges() {
      return flussChanges;
    }

    private void addFlussChange(org.apache.fluss.metadata.TableChange flussChange) {
      flussChanges.add(flussChange);
    }

    private void visit(TableChange.AddColumn addColumn) {
      validateAddColumn(addColumn);
      String columnName = columnName(addColumn);
      Preconditions.checkArgument(
          !columnsByName.containsKey(columnName), "Column already exists: %s", columnName);
      DataType dataType =
          FlussDataTypeConverter.CONVERTER.fromGravitino(
              addColumn.getDataType(), addColumn.isNullable());

      addFlussChange(
          org.apache.fluss.metadata.TableChange.addColumn(
              columnName,
              dataType,
              addColumn.getComment(),
              toFlussColumnPosition(addColumn.getPosition(), true)));
      putColumn(
          new org.apache.fluss.metadata.Schema.Column(columnName, dataType, addColumn.getComment()),
          addColumn.getPosition());
    }

    private void visit(TableChange.DeleteColumn deleteColumn) {
      String columnName = columnName(deleteColumn);
      if (!columnsByName.containsKey(columnName)) {
        if (Boolean.TRUE.equals(deleteColumn.getIfExists())) {
          return;
        }
        throw new NoSuchColumnException("Column does not exist: %s", columnName);
      }

      addFlussChange(org.apache.fluss.metadata.TableChange.dropColumn(columnName));
      removeColumn(columnName);
    }

    private void visit(TableChange.RenameColumn renameColumn) {
      String columnName = columnName(renameColumn);
      org.apache.fluss.metadata.Schema.Column column = requireColumn(columnName);
      String newName = renameColumn.getNewName();
      Preconditions.checkArgument(
          newName != null && !newName.isEmpty(), "New column name cannot be empty");
      Preconditions.checkArgument(
          !columnsByName.containsKey(newName), "Column already exists: %s", newName);

      addFlussChange(org.apache.fluss.metadata.TableChange.renameColumn(columnName, newName));
      replaceColumn(columnName, copyColumn(newName, column));
    }

    private void visit(TableChange.UpdateColumnType updateColumnType) {
      String columnName = columnName(updateColumnType);
      org.apache.fluss.metadata.Schema.Column column = requireColumn(columnName);
      DataType newDataType =
          FlussDataTypeConverter.CONVERTER.fromGravitino(
              updateColumnType.getNewDataType(), column.getDataType().isNullable());
      org.apache.fluss.metadata.Schema.Column updatedColumn =
          copyColumn(column.getName(), newDataType, column.getComment().orElse(null), column);

      appendModifyColumn(updatedColumn, null);
      replaceColumn(columnName, updatedColumn);
    }

    private void visit(TableChange.UpdateColumnComment updateColumnComment) {
      String columnName = columnName(updateColumnComment);
      org.apache.fluss.metadata.Schema.Column column = requireColumn(columnName);
      org.apache.fluss.metadata.Schema.Column updatedColumn =
          copyColumn(
              column.getName(), column.getDataType(), updateColumnComment.getNewComment(), column);

      appendModifyColumn(updatedColumn, null);
      replaceColumn(columnName, updatedColumn);
    }

    private void visit(TableChange.UpdateColumnNullability updateColumnNullability) {
      String columnName = columnName(updateColumnNullability);
      org.apache.fluss.metadata.Schema.Column column = requireColumn(columnName);
      org.apache.fluss.metadata.Schema.Column updatedColumn =
          copyColumn(
              column.getName(),
              column.getDataType().copy(updateColumnNullability.nullable()),
              column.getComment().orElse(null),
              column);

      appendModifyColumn(updatedColumn, null);
      replaceColumn(columnName, updatedColumn);
    }

    private void visit(TableChange.UpdateColumnPosition updateColumnPosition) {
      String columnName = columnName(updateColumnPosition);
      org.apache.fluss.metadata.Schema.Column column = requireColumn(columnName);
      org.apache.fluss.metadata.TableChange.ColumnPosition flussPosition =
          toFlussColumnPosition(updateColumnPosition.getPosition(), false);

      appendModifyColumn(column, flussPosition);
      columnOrder.remove(columnName);
      columnOrder.add(columnPosition(updateColumnPosition.getPosition()), columnName);
    }

    private static void validateAddColumn(TableChange.AddColumn addColumn) {
      if (!addColumn.isNullable()) {
        throw new UnsupportedOperationException("Fluss only supports adding nullable columns");
      }
      if (addColumn.isAutoIncrement()) {
        throw new UnsupportedOperationException(
            "Fluss does not support adding auto-increment columns");
      }
      if (addColumn.getDefaultValue() != Column.DEFAULT_VALUE_NOT_SET) {
        throw new UnsupportedOperationException("Fluss does not support column default values");
      }
    }

    private static String columnName(TableChange.ColumnChange columnChange) {
      return FlussMetadataUtils.requireTopLevelField(columnChange.fieldName());
    }

    private org.apache.fluss.metadata.Schema.Column requireColumn(String name) {
      org.apache.fluss.metadata.Schema.Column column = columnsByName.get(name);
      if (column == null) {
        throw new NoSuchColumnException("Column does not exist: %s", name);
      }
      return column;
    }

    private void putColumn(
        org.apache.fluss.metadata.Schema.Column column, TableChange.ColumnPosition position) {
      columnOrder.add(columnPosition(position), column.getName());
      columnsByName.put(column.getName(), column);
    }

    private void replaceColumn(
        String oldName, org.apache.fluss.metadata.Schema.Column updatedColumn) {
      columnsByName.remove(oldName);
      columnsByName.put(updatedColumn.getName(), updatedColumn);
      columnOrder.set(columnOrder.indexOf(oldName), updatedColumn.getName());
    }

    private void removeColumn(String name) {
      columnsByName.remove(name);
      columnOrder.remove(name);
    }

    private void appendModifyColumn(
        org.apache.fluss.metadata.Schema.Column column,
        @Nullable org.apache.fluss.metadata.TableChange.ColumnPosition position) {
      addFlussChange(
          org.apache.fluss.metadata.TableChange.modifyColumn(
              column.getName(), column.getDataType(), column.getComment().orElse(null), position));
    }

    private int columnPosition(@Nullable TableChange.ColumnPosition position) {
      if (position == null || position instanceof TableChange.Default) {
        return columnOrder.size();
      }
      if (position instanceof TableChange.First) {
        return 0;
      }
      if (position instanceof TableChange.After after) {
        int afterColumnIndex = columnOrder.indexOf(after.getColumn());
        Preconditions.checkArgument(
            afterColumnIndex >= 0, "Column does not exist: %s", after.getColumn());
        return afterColumnIndex + 1;
      }

      throw new UnsupportedOperationException("Unsupported Fluss column position: " + position);
    }

    private static org.apache.fluss.metadata.TableChange.ColumnPosition toFlussColumnPosition(
        @Nullable TableChange.ColumnPosition position, boolean defaultAsLast) {
      if (position == null || position instanceof TableChange.Default) {
        Preconditions.checkArgument(defaultAsLast, "Column position cannot be default");
        return org.apache.fluss.metadata.TableChange.ColumnPosition.last();
      }
      if (position instanceof TableChange.First) {
        return org.apache.fluss.metadata.TableChange.ColumnPosition.first();
      }
      if (position instanceof TableChange.After after) {
        return org.apache.fluss.metadata.TableChange.ColumnPosition.after(after.getColumn());
      }

      throw new UnsupportedOperationException("Unsupported Fluss column position: " + position);
    }

    private static org.apache.fluss.metadata.Schema.Column copyColumn(
        String name, org.apache.fluss.metadata.Schema.Column column) {
      return copyColumn(name, column.getDataType(), column.getComment().orElse(null), column);
    }

    private static org.apache.fluss.metadata.Schema.Column copyColumn(
        String name,
        DataType dataType,
        @Nullable String comment,
        org.apache.fluss.metadata.Schema.Column column) {
      AggFunction aggFunction = column.getAggFunction().orElse(null);
      if (aggFunction == null) {
        return new org.apache.fluss.metadata.Schema.Column(
            name, dataType, comment, column.getColumnId());
      }
      return new org.apache.fluss.metadata.Schema.Column(
          name, dataType, comment, column.getColumnId(), aggFunction);
    }
  }

  private static TablePath toTablePath(NameIdentifier ident) {
    return TablePath.of(databaseName(ident.namespace()), ident.name());
  }

  private static String databaseName(Namespace namespace) {
    String[] levels = namespace.levels();
    Preconditions.checkArgument(
        levels.length >= 2, "Namespace must have at least 2 levels, got: %s", levels.length);
    return levels[levels.length - 1];
  }
}
