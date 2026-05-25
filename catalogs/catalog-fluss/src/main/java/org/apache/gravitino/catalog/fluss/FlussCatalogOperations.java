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
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.DatabaseAlreadyExistException;
import org.apache.fluss.exception.DatabaseNotEmptyException;
import org.apache.fluss.exception.DatabaseNotExistException;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.DatabaseInfo;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
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
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
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

  @VisibleForTesting
  static final Set<String> ALTERABLE_TABLE_PROPERTIES =
      ImmutableSet.of(
          "table.datalake.enabled",
          "table.datalake.freshness",
          "table.log.tiered.local-segments",
          "table.auto-partition.num-retention");

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
    Configuration flussConfig = toFlussConfiguration(properties, null);
    try (Connection testConnection = connectionFactory.apply(flussConfig)) {
      new FlussAdminOps(testConnection.getAdmin())
          .doAsAdmin(
              Admin::listDatabases,
              e -> new ConnectionFailedException(e, "Failed to connect to Fluss cluster"));
    }
  }

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    return ops
        .doAsAdmin(
            Admin::listDatabases,
            e ->
                new GravitinoRuntimeException(
                    e, "Failed to list Fluss databases under %s", namespace))
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
        e -> {
          if (e instanceof DatabaseAlreadyExistException) {
            return new SchemaAlreadyExistsException(e, "Schema %s already exists", ident);
          }
          return new GravitinoRuntimeException(e, "Failed to create Fluss database " + ident);
        });

    return loadSchema(ident);
  }

  @Override
  public boolean schemaExists(NameIdentifier ident) {
    return ops.doAsAdmin(
        admin -> admin.databaseExists(ident.name()),
        e ->
            new GravitinoRuntimeException(
                e, "Failed to check existence of Fluss database " + ident));
  }

  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    DatabaseInfo dbInfo =
        ops.doAsAdmin(
            admin -> admin.getDatabaseInfo(ident.name()),
            e -> {
              if (e instanceof DatabaseNotExistException) {
                return new NoSuchSchemaException(e, "Schema %s does not exist", ident);
              }
              return new GravitinoRuntimeException(e, "Failed to load Fluss database " + ident);
            });

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
        e -> {
          if (e instanceof DatabaseNotEmptyException) {
            throw new NonEmptySchemaException(e, "Schema %s does not exist", ident);
          }
          throw new GravitinoRuntimeException(e, "Failed to drop Fluss database " + ident);
        });

    return true;
  }

  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    String database = databaseName(namespace);
    return ops
        .doAsAdmin(
            admin -> admin.listTables(database),
            e -> {
              if (e instanceof DatabaseNotExistException) {
                return new NoSuchSchemaException(e, "Schema %s does not exist", namespace);
              }
              return new RuntimeException("Failed to list Fluss tables under " + namespace, e);
            })
        .stream()
        .map(table -> NameIdentifier.of(namespace, table))
        .toArray(NameIdentifier[]::new);
  }

  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    TablePath tablePath = toTablePath(ident);
    TableInfo tableInfo =
        ops.doAsAdmin(
            admin -> admin.getTableInfo(tablePath),
            e -> {
              if (e instanceof TableNotExistException) {
                return new NoSuchTableException(e, "Table %s does not exist", ident);
              }
              return new RuntimeException("Failed to load Fluss table " + ident, e);
            });
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
        e -> {
          if (e instanceof DatabaseNotExistException) {
            return new NoSuchSchemaException(e, "Schema %s does not exist", ident.namespace());
          }
          if (e instanceof TableAlreadyExistException) {
            return new TableAlreadyExistsException(e, "Table %s already exists", ident);
          }
          return new RuntimeException("Failed to create Fluss table " + ident, e);
        });
    return loadTable(ident);
  }

  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    List<org.apache.fluss.metadata.TableChange> flussChanges =
        Arrays.stream(changes == null ? new TableChange[0] : changes)
            .map(FlussCatalogOperations::toFlussTableChange)
            .collect(Collectors.toList());
    if (flussChanges.isEmpty()) {
      return loadTable(ident);
    }

    TablePath tablePath = toTablePath(ident);
    ops.doAsAdmin(
        admin -> admin.alterTable(tablePath, flussChanges, false),
        e -> {
          if (e instanceof TableNotExistException) {
            return new NoSuchTableException(e, "Table %s does not exist", ident);
          }
          return new RuntimeException("Failed to alter Fluss table " + ident, e);
        });
    return loadTable(ident);
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    TablePath tablePath = toTablePath(ident);
    try {
      if (!ops.doAsAdmin(admin -> admin.tableExists(tablePath))) {
        return false;
      }
      ops.doAsAdmin(admin -> admin.dropTable(tablePath, false));
      return true;
    } catch (DatabaseNotExistException | TableNotExistException e) {
      return false;
    } catch (RuntimeException e) {
      LOG.warn("Failed to drop Fluss table {}", ident, e);
      throw new RuntimeException("Failed to drop Fluss table " + ident, e);
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

  private static org.apache.fluss.metadata.TableChange toFlussTableChange(TableChange change) {
    if (change instanceof TableChange.SetProperty setProperty) {
      checkAlterableProperty(setProperty.getProperty());
      return org.apache.fluss.metadata.TableChange.set(
          setProperty.getProperty(), setProperty.getValue());
    }

    if (change instanceof TableChange.RemoveProperty removeProperty) {
      checkAlterableProperty(removeProperty.getProperty());
      return org.apache.fluss.metadata.TableChange.reset(removeProperty.getProperty());
    }

    if (change instanceof TableChange.AddColumn addColumn) {
      validateAddColumn(addColumn);
      return org.apache.fluss.metadata.TableChange.addColumn(
          addColumn.getFieldName()[0],
          FlussDataTypeConverter.CONVERTER.fromGravitino(addColumn.getDataType(), true),
          addColumn.getComment(),
          org.apache.fluss.metadata.TableChange.ColumnPosition.last());
    }

    throw new IllegalArgumentException("Unsupported Fluss table change: " + change);
  }

  private static void validateAddColumn(TableChange.AddColumn addColumn) {
    if (addColumn.getFieldName().length != 1) {
      throw new IllegalArgumentException("Fluss only supports adding top-level columns");
    }
    if (!addColumn.isNullable()) {
      throw new IllegalArgumentException("Fluss only supports adding nullable columns");
    }
    if (addColumn.isAutoIncrement()) {
      throw new IllegalArgumentException("Fluss does not support adding auto-increment columns");
    }
    if (addColumn.getDefaultValue() != Column.DEFAULT_VALUE_NOT_SET) {
      throw new IllegalArgumentException("Fluss does not support column default values");
    }
    if (!"DEFAULT".equals(addColumn.getPosition().toString())) {
      throw new IllegalArgumentException("Fluss only supports appending columns");
    }
  }

  private static void checkAlterableProperty(String property) {
    if (!ALTERABLE_TABLE_PROPERTIES.contains(property)) {
      throw new IllegalArgumentException("Unsupported Fluss table option alteration: " + property);
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
