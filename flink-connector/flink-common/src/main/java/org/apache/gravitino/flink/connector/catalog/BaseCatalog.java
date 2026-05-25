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

package org.apache.gravitino.flink.connector.catalog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogView;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.DataType;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.exceptions.ViewAlreadyExistsException;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.gravitino.flink.connector.utils.CatalogCompat;
import org.apache.gravitino.flink.connector.utils.DefaultCatalogCompat;
import org.apache.gravitino.flink.connector.utils.TableUtils;
import org.apache.gravitino.flink.connector.utils.TypeUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Dialects;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewCatalog;
import org.apache.gravitino.rel.ViewChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The BaseCatalog that provides a default implementation for all methods in the {@link
 * org.apache.flink.table.catalog.Catalog} interface.
 */
public abstract class BaseCatalog extends AbstractCatalog {
  private static final Logger LOG = LoggerFactory.getLogger(BaseCatalog.class);

  private final SchemaAndTablePropertiesConverter schemaAndTablePropertiesConverter;
  private final PartitionConverter partitionConverter;
  private final Map<String, String> catalogOptions;

  protected BaseCatalog(
      String catalogName,
      Map<String, String> catalogOptions,
      String defaultDatabase,
      SchemaAndTablePropertiesConverter schemaAndTablePropertiesConverter,
      PartitionConverter partitionConverter) {
    super(catalogName, defaultDatabase);
    this.schemaAndTablePropertiesConverter = schemaAndTablePropertiesConverter;
    this.partitionConverter = partitionConverter;
    this.catalogOptions = catalogOptions;
  }

  protected abstract AbstractCatalog realCatalog();

  @Override
  public void open() throws CatalogException {
    realCatalog().open();
  }

  @Override
  public void close() throws CatalogException {
    realCatalog().close();
  }

  @Override
  public List<String> listDatabases() throws CatalogException {
    return Arrays.asList(catalog().asSchemas().listSchemas());
  }

  @Override
  public CatalogDatabase getDatabase(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    try {
      Schema schema = catalog().asSchemas().loadSchema(databaseName);
      Map<String, String> properties =
          schemaAndTablePropertiesConverter.toFlinkDatabaseProperties(schema.properties());
      return new CatalogDatabaseImpl(properties, schema.comment());
    } catch (NoSuchSchemaException e) {
      throw new DatabaseNotExistException(catalogName(), databaseName);
    }
  }

  @Override
  public boolean databaseExists(String databaseName) throws CatalogException {
    return catalog().asSchemas().schemaExists(databaseName);
  }

  @Override
  public void createDatabase(
      String databaseName, CatalogDatabase catalogDatabase, boolean ignoreIfExists)
      throws DatabaseAlreadyExistException, CatalogException {
    try {
      Map<String, String> properties =
          schemaAndTablePropertiesConverter.toGravitinoSchemaProperties(
              catalogDatabase.getProperties());
      catalog().asSchemas().createSchema(databaseName, catalogDatabase.getComment(), properties);
    } catch (SchemaAlreadyExistsException e) {
      if (!ignoreIfExists) {
        throw new DatabaseAlreadyExistException(catalogName(), databaseName);
      }
    } catch (NoSuchCatalogException e) {
      throw new CatalogException(e);
    }
  }

  @Override
  public void dropDatabase(String databaseName, boolean ignoreIfNotExists, boolean cascade)
      throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
    try {
      boolean dropped = catalog().asSchemas().dropSchema(databaseName, cascade);
      if (!dropped && !ignoreIfNotExists) {
        throw new DatabaseNotExistException(catalogName(), databaseName);
      }
    } catch (NonEmptySchemaException e) {
      throw new DatabaseNotEmptyException(catalogName(), databaseName);
    } catch (NoSuchCatalogException e) {
      throw new CatalogException(e);
    }
  }

  @Override
  public void alterDatabase(
      String databaseName, CatalogDatabase catalogDatabase, boolean ignoreIfNotExists)
      throws DatabaseNotExistException, CatalogException {
    try {
      SchemaChange[] schemaChanges = getSchemaChange(getDatabase(databaseName), catalogDatabase);
      catalog().asSchemas().alterSchema(databaseName, schemaChanges);
    } catch (NoSuchSchemaException e) {
      if (!ignoreIfNotExists) {
        throw new DatabaseNotExistException(catalogName(), databaseName);
      }
    } catch (NoSuchCatalogException e) {
      throw new CatalogException(e);
    }
  }

  @Override
  public List<String> listTables(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    try {
      return Stream.of(catalog().asTableCatalog().listTables(Namespace.of(databaseName)))
          .map(NameIdentifier::name)
          .collect(Collectors.toList());
    } catch (NoSuchSchemaException e) {
      throw new DatabaseNotExistException(catalogName(), databaseName, e);
    } catch (Exception e) {
      throw new CatalogException(e);
    }
  }

  @Override
  public List<String> listViews(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    try {
      ViewCatalog viewCatalog = catalog().asViewCatalog();
      // TODO: Currently returns all VIRTUAL_VIEW entries from the underlying catalog regardless of
      // dialect. Views created by other engines (e.g. Trino, Spark) may appear here but will fail
      // when Flink attempts to load them. Consider filtering to only dialects that Flink can handle
      // (hive, flink), but this requires per-view property inspection which is expensive.
      return Arrays.stream(viewCatalog.listViews(Namespace.of(databaseName)))
          .map(NameIdentifier::name)
          .collect(Collectors.toList());
    } catch (UnsupportedOperationException e) {
      LOG.debug("Catalog {} does not support views; returning empty view list", catalogName(), e);
      return Collections.emptyList();
    } catch (NoSuchSchemaException e) {
      throw new DatabaseNotExistException(catalogName(), databaseName, e);
    } catch (Exception e) {
      throw new CatalogException(e);
    }
  }

  @Override
  public CatalogBaseTable getTable(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    NameIdentifier ident =
        NameIdentifier.of(tablePath.getDatabaseName(), tablePath.getObjectName());
    try {
      Table table = catalog().asTableCatalog().loadTable(ident);
      return toFlinkTable(table, tablePath);
    } catch (NoSuchTableException e) {
      // Fall through to check views.
    } catch (Exception e) {
      LOG.warn("Failed to load table {} from catalog {}", ident, catalogName(), e);
      throw new CatalogException(e);
    }

    return loadViewOrThrow(tablePath);
  }

  @Override
  public boolean tableExists(ObjectPath tablePath) throws CatalogException {
    NameIdentifier ident =
        NameIdentifier.of(tablePath.getDatabaseName(), tablePath.getObjectName());
    try {
      if (catalog().asTableCatalog().tableExists(ident)) {
        return true;
      }
    } catch (Exception e) {
      throw new CatalogException(e);
    }
    try {
      return catalog().asViewCatalog().viewExists(ident);
    } catch (UnsupportedOperationException e) {
      LOG.debug(
          "Catalog {} does not support views; viewExists returns false for {}",
          catalogName(),
          ident,
          e);
      return false;
    } catch (NoSuchSchemaException e) {
      LOG.debug(
          "Schema not found when checking viewExists for {} in catalog {}; returning false",
          ident,
          catalogName(),
          e);
      return false;
    } catch (Exception e) {
      throw new CatalogException(e);
    }
  }

  @Override
  public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    NameIdentifier ident =
        NameIdentifier.of(tablePath.getDatabaseName(), tablePath.getObjectName());

    // Determine kind before dropping to avoid a view being silently missed.
    CatalogBaseTable existing;
    try {
      existing = getTable(tablePath);
    } catch (TableNotExistException e) {
      if (!ignoreIfNotExists) {
        throw e;
      }
      return;
    }

    try {
      if (existing.getTableKind() == CatalogBaseTable.TableKind.VIEW) {
        boolean dropped = catalog().asViewCatalog().dropView(ident);
        if (!dropped && !ignoreIfNotExists) {
          throw new TableNotExistException(catalogName(), tablePath);
        }
      } else {
        boolean dropped = catalog().asTableCatalog().dropTable(ident);
        if (!dropped && !ignoreIfNotExists) {
          throw new TableNotExistException(catalogName(), tablePath);
        }
      }
    } catch (TableNotExistException e) {
      throw e;
    } catch (Exception e) {
      throw new CatalogException(e);
    }
  }

  @Override
  public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
      throws TableNotExistException, TableAlreadyExistException, CatalogException {
    NameIdentifier identifier =
        NameIdentifier.of(Namespace.of(tablePath.getDatabaseName()), newTableName);

    if (catalog().asTableCatalog().tableExists(identifier)) {
      throw new TableAlreadyExistException(
          catalogName(), new ObjectPath(tablePath.getDatabaseName(), newTableName));
    }
    try {
      if (catalog().asViewCatalog().viewExists(identifier)) {
        throw new TableAlreadyExistException(
            catalogName(), new ObjectPath(tablePath.getDatabaseName(), newTableName));
      }
    } catch (UnsupportedOperationException ignored) {
      // catalog does not support views
    }

    CatalogBaseTable existing;
    try {
      existing = getTable(tablePath);
    } catch (TableNotExistException e) {
      if (!ignoreIfNotExists) {
        throw e;
      }
      return;
    }

    NameIdentifier srcIdent =
        NameIdentifier.of(tablePath.getDatabaseName(), tablePath.getObjectName());
    try {
      if (existing.getTableKind() == CatalogBaseTable.TableKind.VIEW) {
        catalog().asViewCatalog().alterView(srcIdent, ViewChange.rename(newTableName));
      } else {
        catalog().asTableCatalog().alterTable(srcIdent, TableChange.rename(newTableName));
      }
    } catch (NoSuchTableException | NoSuchViewException e) {
      if (!ignoreIfNotExists) {
        throw new TableNotExistException(catalogName(), tablePath, e);
      }
    } catch (Exception e) {
      throw new CatalogException(e);
    }
  }

  @Override
  public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
      throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
    Preconditions.checkArgument(
        table instanceof ResolvedCatalogBaseTable, "table should be resolved");

    if (table.getTableKind() == CatalogBaseTable.TableKind.VIEW) {
      createView(tablePath, (ResolvedCatalogView) table, ignoreIfExists);
      return;
    }

    NameIdentifier identifier =
        NameIdentifier.of(tablePath.getDatabaseName(), tablePath.getObjectName());

    ResolvedCatalogBaseTable<?> resolvedTable = (ResolvedCatalogBaseTable<?>) table;
    Column[] columns =
        resolvedTable.getResolvedSchema().getColumns().stream()
            .map(BaseCatalog::toGravitinoColumn)
            .toArray(Column[]::new);
    String comment = table.getComment();
    Map<String, String> flinkOptions = table.getOptions();
    Map<String, String> properties =
        schemaAndTablePropertiesConverter.toGravitinoTableProperties(flinkOptions);
    Transform[] partitions =
        partitionConverter.toGravitinoPartitions(((CatalogTable) table).getPartitionKeys());
    Index[] indices = getGrivatinoIndices(resolvedTable);

    try {
      catalog()
          .asTableCatalog()
          .createTable(
              identifier,
              columns,
              comment,
              properties,
              partitions,
              toGravitinoDistribution(flinkOptions),
              new SortOrder[0],
              indices);
    } catch (NoSuchSchemaException e) {
      throw new DatabaseNotExistException(catalogName(), tablePath.getDatabaseName(), e);
    } catch (TableAlreadyExistsException e) {
      if (!ignoreIfExists) {
        throw new TableAlreadyExistException(catalogName(), tablePath, e);
      }
    } catch (Exception e) {
      throw new CatalogException(e);
    }
  }

  private void createView(ObjectPath tablePath, ResolvedCatalogView view, boolean ignoreIfExists)
      throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
    NameIdentifier identifier =
        NameIdentifier.of(tablePath.getDatabaseName(), tablePath.getObjectName());
    Column[] columns = toGravitinoColumns(view);
    Representation[] representations =
        buildSqlRepresentation(preferredDialect(), view.getExpandedQuery());
    try {
      catalog()
          .asViewCatalog()
          .createView(
              identifier,
              view.getComment(),
              columns,
              representations,
              null,
              null,
              view.getOptions());
    } catch (NoSuchSchemaException e) {
      throw new DatabaseNotExistException(catalogName(), tablePath.getDatabaseName(), e);
    } catch (ViewAlreadyExistsException e) {
      if (!ignoreIfExists) {
        throw new TableAlreadyExistException(catalogName(), tablePath, e);
      }
    } catch (Exception e) {
      throw new CatalogException(e);
    }
  }

  private static Index[] getGrivatinoIndices(ResolvedCatalogBaseTable<?> resolvedTable) {
    Optional<UniqueConstraint> primaryKey = resolvedTable.getResolvedSchema().getPrimaryKey();
    List<String> primaryColumns = primaryKey.map(UniqueConstraint::getColumns).orElse(null);
    if (primaryColumns == null) {
      return new Index[0];
    }
    String[][] primaryField =
        primaryColumns.stream()
            .map(primaryColumn -> new String[] {primaryColumn})
            .toArray(String[][]::new);
    Index primary = Indexes.primary("primary", primaryField, Collections.emptyMap());
    return new Index[] {primary};
  }

  /**
   * The method only is used to change the comments. To alter columns, use the other alterTable API
   * and provide a list of TableChanges.
   *
   * @param tablePath path of the table or view to be modified
   * @param newTable the new table definition
   * @param ignoreIfNotExists flag to specify behavior when the table or view does not exist: if set
   *     to false, throw an exception, if set to true, do nothing.
   * @throws TableNotExistException if the table not exists.
   * @throws CatalogException in case of any runtime exception.
   */
  @Override
  public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    CatalogBaseTable existingTable;

    try {
      existingTable = this.getTable(tablePath);
    } catch (TableNotExistException e) {
      if (!ignoreIfNotExists) {
        throw e;
      }
      return;
    }

    if (existingTable.getTableKind() != newTable.getTableKind()) {
      throw new CatalogException(
          String.format(
              "Table types don't match. Existing table is '%s' and new table is '%s'.",
              existingTable.getTableKind(), newTable.getTableKind()));
    }

    NameIdentifier identifier =
        NameIdentifier.of(tablePath.getDatabaseName(), tablePath.getObjectName());

    if (newTable.getTableKind() == CatalogBaseTable.TableKind.VIEW) {
      try {
        catalog()
            .asViewCatalog()
            .alterView(
                identifier,
                getGravitinoViewChanges(
                    existingTable, (ResolvedCatalogView) newTable, preferredDialect()));
      } catch (NoSuchViewException e) {
        if (!ignoreIfNotExists) {
          throw new TableNotExistException(catalogName(), tablePath, e);
        }
      } catch (Exception e) {
        throw new CatalogException(e);
      }
    } else {
      catalog()
          .asTableCatalog()
          .alterTable(identifier, getGravitinoTableChanges(existingTable, newTable));
    }
  }

  @Override
  public void alterTable(
      ObjectPath tablePath,
      CatalogBaseTable newTable,
      List<org.apache.flink.table.catalog.TableChange> tableChanges,
      boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    CatalogBaseTable existingTable;
    try {
      existingTable = this.getTable(tablePath);
    } catch (TableNotExistException e) {
      if (!ignoreIfNotExists) {
        throw e;
      }
      return;
    }

    if (existingTable.getTableKind() != newTable.getTableKind()) {
      throw new CatalogException(
          String.format(
              "Table types don't match. Existing table is '%s' and new table is '%s'.",
              existingTable.getTableKind(), newTable.getTableKind()));
    }

    NameIdentifier identifier =
        NameIdentifier.of(tablePath.getDatabaseName(), tablePath.getObjectName());

    if (newTable.getTableKind() == CatalogBaseTable.TableKind.VIEW) {
      try {
        catalog()
            .asViewCatalog()
            .alterView(
                identifier,
                getGravitinoViewChanges(
                    tableChanges, (ResolvedCatalogView) newTable, preferredDialect()));
      } catch (NoSuchViewException e) {
        if (!ignoreIfNotExists) {
          throw new TableNotExistException(catalogName(), tablePath, e);
        }
      } catch (Exception e) {
        throw new CatalogException(e);
      }
    } else {
      catalog().asTableCatalog().alterTable(identifier, getGravitinoTableChanges(tableChanges));
    }
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
      throws TableNotExistException, TableNotPartitionedException, CatalogException {
    return realCatalog().listPartitions(tablePath);
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException,
          CatalogException {
    return realCatalog().listPartitions(tablePath, partitionSpec);
  }

  @Override
  public List<CatalogPartitionSpec> listPartitionsByFilter(
      ObjectPath tablePath, List<Expression> filter)
      throws TableNotExistException, TableNotPartitionedException, CatalogException {
    return realCatalog().listPartitionsByFilter(tablePath, filter);
  }

  @Override
  public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws PartitionNotExistException, CatalogException {
    return realCatalog().getPartition(tablePath, partitionSpec);
  }

  @Override
  public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws CatalogException {
    return realCatalog().partitionExists(tablePath, partitionSpec);
  }

  @Override
  public void createPartition(
      ObjectPath objectPath,
      CatalogPartitionSpec catalogPartitionSpec,
      CatalogPartition catalogPartition,
      boolean b)
      throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException,
          PartitionAlreadyExistsException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropPartition(
      ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec, boolean b)
      throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterPartition(
      ObjectPath objectPath,
      CatalogPartitionSpec catalogPartitionSpec,
      CatalogPartition catalogPartition,
      boolean b)
      throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listFunctions(String s) throws DatabaseNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CatalogFunction getFunction(ObjectPath tablePath)
      throws FunctionNotExistException, CatalogException {
    return realCatalog().getFunction(tablePath);
  }

  @Override
  public boolean functionExists(ObjectPath objectPath) throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createFunction(ObjectPath objectPath, CatalogFunction catalogFunction, boolean b)
      throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterFunction(ObjectPath objectPath, CatalogFunction catalogFunction, boolean b)
      throws FunctionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropFunction(ObjectPath objectPath, boolean b)
      throws FunctionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    return realCatalog().getTableStatistics(tablePath);
  }

  @Override
  public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    return realCatalog().getTableColumnStatistics(tablePath);
  }

  @Override
  public CatalogTableStatistics getPartitionStatistics(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws PartitionNotExistException, CatalogException {
    return realCatalog().getPartitionStatistics(tablePath, partitionSpec);
  }

  @Override
  public CatalogColumnStatistics getPartitionColumnStatistics(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws PartitionNotExistException, CatalogException {
    return realCatalog().getPartitionColumnStatistics(tablePath, partitionSpec);
  }

  @Override
  public void alterTableStatistics(
      ObjectPath objectPath, CatalogTableStatistics catalogTableStatistics, boolean b)
      throws TableNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterTableColumnStatistics(
      ObjectPath objectPath, CatalogColumnStatistics catalogColumnStatistics, boolean b)
      throws TableNotExistException, CatalogException, TablePartitionedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterPartitionStatistics(
      ObjectPath objectPath,
      CatalogPartitionSpec catalogPartitionSpec,
      CatalogTableStatistics catalogTableStatistics,
      boolean b)
      throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterPartitionColumnStatistics(
      ObjectPath objectPath,
      CatalogPartitionSpec catalogPartitionSpec,
      CatalogColumnStatistics catalogColumnStatistics,
      boolean b)
      throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  protected CatalogBaseTable toFlinkTable(Table table, ObjectPath tablePath) {
    org.apache.flink.table.api.Schema.Builder builder = buildSchemaFromColumns(table.columns());
    Optional<List<String>> flinkPrimaryKey = getFlinkPrimaryKey(table);
    flinkPrimaryKey.ifPresent(builder::primaryKey);
    Map<String, String> flinkTableProperties =
        new HashMap<>(
            schemaAndTablePropertiesConverter.toFlinkTableProperties(
                catalogOptions, table.properties(), tablePath));
    flinkTableProperties.putAll(fromGravitinoDistribution(table.distribution()));
    List<String> partitionKeys = partitionConverter.toFlinkPartitionKeys(table.partitioning());
    return newCatalogTable(builder.build(), table.comment(), partitionKeys, flinkTableProperties);
  }

  protected CatalogTable newCatalogTable(
      org.apache.flink.table.api.Schema schema,
      String comment,
      List<String> partitionKeys,
      Map<String, String> options) {
    return catalogCompat().createCatalogTable(schema, comment, partitionKeys, options);
  }

  protected CatalogCompat catalogCompat() {
    // Versioned catalog entry classes override this hook when the Flink minor has a different
    // catalog/table API path.
    return DefaultCatalogCompat.INSTANCE;
  }

  private static Optional<List<String>> getFlinkPrimaryKey(Table table) {
    List<Index> primaryKeyList =
        Arrays.stream(table.index())
            .filter(index -> index.type() == Index.IndexType.PRIMARY_KEY)
            .collect(Collectors.toList());
    if (primaryKeyList.isEmpty()) {
      return Optional.empty();
    }
    Preconditions.checkArgument(
        primaryKeyList.size() == 1, "More than one primary key is not supported.");
    List<String> primaryKeyFieldList =
        Arrays.stream(primaryKeyList.get(0).fieldNames())
            .map(
                fieldNames -> {
                  Preconditions.checkArgument(
                      fieldNames.length == 1, "The primary key columns should not be nested.");
                  return fieldNames[0];
                })
            .collect(Collectors.toList());
    Preconditions.checkArgument(
        !primaryKeyFieldList.isEmpty(), "The primary key must contain at least one field.");
    return Optional.of(primaryKeyFieldList);
  }

  private static Column toGravitinoColumn(org.apache.flink.table.catalog.Column column) {
    return Column.of(
        column.getName(),
        TypeUtils.toGravitinoType(column.getDataType().getLogicalType()),
        column.getComment().orElse(null),
        column.getDataType().getLogicalType().isNullable(),
        false,
        null);
  }

  private static void removeProperty(
      org.apache.flink.table.catalog.TableChange.ResetOption change, List<TableChange> changes) {
    changes.add(TableChange.removeProperty(change.getKey()));
  }

  private static void setProperty(
      org.apache.flink.table.catalog.TableChange.SetOption change, List<TableChange> changes) {
    changes.add(TableChange.setProperty(change.getKey(), change.getValue()));
  }

  private static void dropColumn(
      org.apache.flink.table.catalog.TableChange.DropColumn change, List<TableChange> changes) {
    changes.add(TableChange.deleteColumn(new String[] {change.getColumnName()}, true));
  }

  private static void addColumn(
      org.apache.flink.table.catalog.TableChange.AddColumn change, List<TableChange> changes) {
    changes.add(
        TableChange.addColumn(
            new String[] {change.getColumn().getName()},
            TypeUtils.toGravitinoType(change.getColumn().getDataType().getLogicalType()),
            change.getColumn().getComment().orElse(null),
            TableUtils.toGravitinoColumnPosition(change.getPosition())));
  }

  private static void modifyColumn(
      org.apache.flink.table.catalog.TableChange change, List<TableChange> changes) {
    if (change instanceof org.apache.flink.table.catalog.TableChange.ModifyColumnName) {
      org.apache.flink.table.catalog.TableChange.ModifyColumnName modifyColumnName =
          (org.apache.flink.table.catalog.TableChange.ModifyColumnName) change;
      changes.add(
          TableChange.renameColumn(
              new String[] {modifyColumnName.getOldColumnName()},
              modifyColumnName.getNewColumnName()));
    } else if (change
        instanceof org.apache.flink.table.catalog.TableChange.ModifyPhysicalColumnType) {
      org.apache.flink.table.catalog.TableChange.ModifyPhysicalColumnType modifyColumnType =
          (org.apache.flink.table.catalog.TableChange.ModifyPhysicalColumnType) change;
      changes.add(
          TableChange.updateColumnType(
              new String[] {modifyColumnType.getOldColumn().getName()},
              TypeUtils.toGravitinoType(modifyColumnType.getNewType().getLogicalType())));
    } else if (change instanceof org.apache.flink.table.catalog.TableChange.ModifyColumnPosition) {
      org.apache.flink.table.catalog.TableChange.ModifyColumnPosition modifyColumnPosition =
          (org.apache.flink.table.catalog.TableChange.ModifyColumnPosition) change;
      changes.add(
          TableChange.updateColumnPosition(
              new String[] {modifyColumnPosition.getOldColumn().getName()},
              TableUtils.toGravitinoColumnPosition(modifyColumnPosition.getNewPosition())));
    } else if (change instanceof org.apache.flink.table.catalog.TableChange.ModifyColumnComment) {
      org.apache.flink.table.catalog.TableChange.ModifyColumnComment modifyColumnComment =
          (org.apache.flink.table.catalog.TableChange.ModifyColumnComment) change;
      changes.add(
          TableChange.updateColumnComment(
              new String[] {modifyColumnComment.getOldColumn().getName()},
              modifyColumnComment.getNewComment()));
    } else {
      throw new IllegalArgumentException(
          String.format("Not support ModifyColumn : %s", change.getClass()));
    }
  }

  @VisibleForTesting
  static TableChange[] getGravitinoTableChanges(
      CatalogBaseTable existingTable, CatalogBaseTable newTable) {
    Preconditions.checkArgument(
        newTable.getComment() != null, "The new comment should not be null");
    List<TableChange> changes = Lists.newArrayList();
    if (!Objects.equals(newTable.getComment(), existingTable.getComment())) {
      changes.add(TableChange.updateComment(newTable.getComment()));
    }
    return changes.toArray(new TableChange[0]);
  }

  @VisibleForTesting
  static TableChange[] getGravitinoTableChanges(
      List<org.apache.flink.table.catalog.TableChange> tableChanges) {
    List<TableChange> changes = Lists.newArrayList();
    for (org.apache.flink.table.catalog.TableChange change : tableChanges) {
      if (change instanceof org.apache.flink.table.catalog.TableChange.AddColumn) {
        addColumn((org.apache.flink.table.catalog.TableChange.AddColumn) change, changes);
      } else if (change instanceof org.apache.flink.table.catalog.TableChange.DropColumn) {
        dropColumn((org.apache.flink.table.catalog.TableChange.DropColumn) change, changes);
      } else if (change instanceof org.apache.flink.table.catalog.TableChange.ModifyColumn) {
        modifyColumn(change, changes);
      } else if (change instanceof org.apache.flink.table.catalog.TableChange.SetOption) {
        setProperty((org.apache.flink.table.catalog.TableChange.SetOption) change, changes);
      } else if (change instanceof org.apache.flink.table.catalog.TableChange.ResetOption) {
        removeProperty((org.apache.flink.table.catalog.TableChange.ResetOption) change, changes);
      } else {
        throw new UnsupportedOperationException(
            String.format("Not supported change : %s", change.getClass()));
      }
    }
    return changes.toArray(new TableChange[0]);
  }

  @VisibleForTesting
  static SchemaChange[] getSchemaChange(CatalogDatabase current, CatalogDatabase updated) {
    Map<String, String> currentProperties = current.getProperties();
    Map<String, String> updatedProperties = updated.getProperties();

    List<SchemaChange> schemaChanges = Lists.newArrayList();
    MapDifference<String, String> difference =
        Maps.difference(currentProperties, updatedProperties);
    difference
        .entriesOnlyOnLeft()
        .forEach((key, value) -> schemaChanges.add(SchemaChange.removeProperty(key)));
    difference
        .entriesOnlyOnRight()
        .forEach((key, value) -> schemaChanges.add(SchemaChange.setProperty(key, value)));
    difference
        .entriesDiffering()
        .forEach(
            (key, value) -> {
              schemaChanges.add(SchemaChange.setProperty(key, value.rightValue()));
            });
    return schemaChanges.toArray(new SchemaChange[0]);
  }

  /**
   * Loads the entity at {@code tablePath} as a Flink {@link CatalogView} if it is a view in the
   * underlying ViewCatalog. Throws {@link TableNotExistException} if no view is found (or if the
   * catalog does not support views).
   */
  protected CatalogBaseTable loadViewOrThrow(ObjectPath tablePath) throws TableNotExistException {
    NameIdentifier ident =
        NameIdentifier.of(tablePath.getDatabaseName(), tablePath.getObjectName());
    try {
      View view = catalog().asViewCatalog().loadView(ident);
      return toFlinkView(view);
    } catch (NoSuchViewException e) {
      throw new TableNotExistException(catalogName(), tablePath, e);
    } catch (UnsupportedOperationException e) {
      LOG.debug(
          "Catalog {} does not support views; treating {} as not found",
          catalogName(),
          tablePath,
          e);
      throw new TableNotExistException(catalogName(), tablePath, e);
    } catch (Exception e) {
      throw new CatalogException(e);
    }
  }

  /**
   * Converts a Gravitino {@link View} to a Flink {@link CatalogView}.
   *
   * @param view The Gravitino view to convert.
   * @return The corresponding Flink CatalogView.
   */
  protected CatalogView toFlinkView(View view) {
    org.apache.flink.table.api.Schema.Builder builder = buildSchemaFromColumns(view.columns());
    String dialect = preferredDialect();
    String sql =
        view.sqlFor(dialect)
            .map(SQLRepresentation::sql)
            .orElseGet(
                () -> {
                  for (Representation rep : view.representations()) {
                    if (rep instanceof SQLRepresentation) {
                      SQLRepresentation fallback = (SQLRepresentation) rep;
                      LOG.warn(
                          "View {} has no SQL representation for dialect {}; falling back to dialect {}",
                          view.name(),
                          dialect,
                          fallback.dialect());
                      return fallback.sql();
                    }
                  }
                  throw new CatalogException(
                      String.format(
                          "View '%s' in catalog '%s' has no SQL representation for dialect '%s'",
                          view.name(), catalogName(), dialect));
                });

    Map<String, String> properties =
        view.properties() != null
            ? Collections.unmodifiableMap(view.properties())
            : Collections.emptyMap();
    String comment = view.comment() != null ? view.comment() : "";
    return CatalogView.of(builder.build(), comment, sql, sql, properties);
  }

  /**
   * Returns the preferred SQL dialect for this catalog's provider. Used when storing view SQL in
   * Gravitino.
   *
   * @return The dialect string, e.g. {@link Dialects#HIVE} or {@link Dialects#FLINK}.
   */
  protected String preferredDialect() {
    return Dialects.FLINK;
  }

  @VisibleForTesting
  static ViewChange[] getGravitinoViewChanges(
      CatalogBaseTable existingView, ResolvedCatalogView newView, String dialect) {
    return new ViewChange[] {
      ViewChange.replaceView(
          toGravitinoColumns(newView),
          buildSqlRepresentation(dialect, newView.getExpandedQuery()),
          null,
          null,
          newView.getComment())
    };
  }

  @VisibleForTesting
  static ViewChange[] getGravitinoViewChanges(
      List<org.apache.flink.table.catalog.TableChange> tableChanges,
      ResolvedCatalogView newView,
      String dialect) {
    List<ViewChange> changes = Lists.newArrayList();
    boolean needsBodyReplace = false;

    for (org.apache.flink.table.catalog.TableChange change : tableChanges) {
      if (change instanceof org.apache.flink.table.catalog.TableChange.SetOption) {
        org.apache.flink.table.catalog.TableChange.SetOption setOption =
            (org.apache.flink.table.catalog.TableChange.SetOption) change;
        changes.add(ViewChange.setProperty(setOption.getKey(), setOption.getValue()));
      } else if (change instanceof org.apache.flink.table.catalog.TableChange.ResetOption) {
        org.apache.flink.table.catalog.TableChange.ResetOption resetOption =
            (org.apache.flink.table.catalog.TableChange.ResetOption) change;
        changes.add(ViewChange.removeProperty(resetOption.getKey()));
      } else {
        LOG.warn(
            "Unrecognized TableChange type {} for view alter; triggering full ReplaceView",
            change.getClass().getSimpleName());
        needsBodyReplace = true;
      }
    }

    if (needsBodyReplace) {
      changes.add(
          ViewChange.replaceView(
              toGravitinoColumns(newView),
              buildSqlRepresentation(dialect, newView.getExpandedQuery()),
              null,
              null,
              newView.getComment()));
    }

    return changes.toArray(new ViewChange[0]);
  }

  private static Column[] toGravitinoColumns(ResolvedCatalogView view) {
    return view.getResolvedSchema().getColumns().stream()
        .map(BaseCatalog::toGravitinoColumn)
        .toArray(Column[]::new);
  }

  private static org.apache.flink.table.api.Schema.Builder buildSchemaFromColumns(
      Column[] columns) {
    org.apache.flink.table.api.Schema.Builder builder =
        org.apache.flink.table.api.Schema.newBuilder();
    for (Column column : columns) {
      DataType flinkType = TypeUtils.toFlinkType(column.dataType());
      builder
          .column(column.name(), column.nullable() ? flinkType.nullable() : flinkType.notNull())
          .withComment(column.comment());
    }
    return builder;
  }

  private static Representation[] buildSqlRepresentation(String dialect, String sql) {
    return new Representation[] {
      SQLRepresentation.builder().withDialect(dialect).withSql(sql).build()
    };
  }

  protected Catalog catalog() {
    return GravitinoCatalogManager.get().getGravitinoCatalogInfo(getName());
  }

  protected String catalogName() {
    return getName();
  }

  protected Distribution toGravitinoDistribution(Map<String, String> properties) {
    return Distributions.NONE;
  }

  protected Map<String, String> fromGravitinoDistribution(Distribution distribution) {
    return Collections.emptyMap();
  }
}
