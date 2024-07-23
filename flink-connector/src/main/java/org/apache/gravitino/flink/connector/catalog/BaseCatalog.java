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
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
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
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.PropertiesConverter;
import org.apache.gravitino.flink.connector.utils.TypeUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.transforms.Transform;

/**
 * The BaseCatalog that provides a default implementation for all methods in the {@link
 * org.apache.flink.table.catalog.Catalog} interface.
 */
public abstract class BaseCatalog extends AbstractCatalog {
  private final PropertiesConverter propertiesConverter;
  private final PartitionConverter partitionConverter;

  protected BaseCatalog(String catalogName, String defaultDatabase) {
    super(catalogName, defaultDatabase);
    this.propertiesConverter = getPropertiesConverter();
    this.partitionConverter = getPartitionConverter();
  }

  protected abstract AbstractCatalog realCatalog();

  @Override
  public void open() throws CatalogException {}

  @Override
  public void close() throws CatalogException {}

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
          propertiesConverter.toFlinkDatabaseProperties(schema.properties());
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
          propertiesConverter.toGravitinoSchemaProperties(catalogDatabase.getProperties());
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
  public List<String> listViews(String s) throws DatabaseNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CatalogBaseTable getTable(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    try {
      Table table =
          catalog()
              .asTableCatalog()
              .loadTable(NameIdentifier.of(tablePath.getDatabaseName(), tablePath.getObjectName()));
      return toFlinkTable(table);
    } catch (NoSuchTableException e) {
      throw new TableNotExistException(catalogName(), tablePath, e);
    } catch (Exception e) {
      throw new CatalogException(e);
    }
  }

  @Override
  public boolean tableExists(ObjectPath tablePath) throws CatalogException {
    try {
      return catalog()
          .asTableCatalog()
          .tableExists(NameIdentifier.of(tablePath.getDatabaseName(), tablePath.getObjectName()));
    } catch (Exception e) {
      throw new CatalogException(e);
    }
  }

  @Override
  public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    boolean dropped =
        catalog()
            .asTableCatalog()
            .dropTable(NameIdentifier.of(tablePath.getDatabaseName(), tablePath.getObjectName()));
    if (!dropped && !ignoreIfNotExists) {
      throw new TableNotExistException(catalogName(), tablePath);
    }
  }

  @Override
  public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
      throws TableNotExistException, TableAlreadyExistException, CatalogException {
    NameIdentifier identifier =
        NameIdentifier.of(Namespace.of(tablePath.getDatabaseName()), newTableName);

    if (catalog().asTableCatalog().tableExists(identifier)) {
      throw new TableAlreadyExistException(
          catalogName(), ObjectPath.fromString(tablePath.getDatabaseName() + newTableName));
    }

    try {
      catalog()
          .asTableCatalog()
          .alterTable(
              NameIdentifier.of(tablePath.getDatabaseName(), tablePath.getObjectName()),
              TableChange.rename(newTableName));
    } catch (NoSuchTableException e) {
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
    NameIdentifier identifier =
        NameIdentifier.of(tablePath.getDatabaseName(), tablePath.getObjectName());

    ResolvedCatalogBaseTable<?> resolvedTable = (ResolvedCatalogBaseTable<?>) table;
    Column[] columns =
        resolvedTable.getResolvedSchema().getColumns().stream()
            .map(this::toGravitinoColumn)
            .toArray(Column[]::new);
    String comment = table.getComment();
    Map<String, String> properties =
        propertiesConverter.toGravitinoTableProperties(table.getOptions());
    Transform[] partitions =
        partitionConverter.toGravitinoPartitions(((CatalogTable) table).getPartitionKeys());
    try {
      catalog().asTableCatalog().createTable(identifier, columns, comment, properties, partitions);
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

  @Override
  public void alterTable(ObjectPath objectPath, CatalogBaseTable catalogBaseTable, boolean b)
      throws TableNotExistException, CatalogException {
    throw new UnsupportedOperationException();
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

  protected abstract PropertiesConverter getPropertiesConverter();

  protected abstract PartitionConverter getPartitionConverter();

  protected CatalogBaseTable toFlinkTable(Table table) {
    org.apache.flink.table.api.Schema.Builder builder =
        org.apache.flink.table.api.Schema.newBuilder();
    for (Column column : table.columns()) {
      DataType flinkType = TypeUtils.toFlinkType(column.dataType());
      builder
          .column(column.name(), column.nullable() ? flinkType.nullable() : flinkType.notNull())
          .withComment(column.comment());
    }
    Map<String, String> flinkTableProperties =
        propertiesConverter.toFlinkTableProperties(table.properties());
    List<String> partitionKeys = partitionConverter.toFlinkPartitionKeys(table.partitioning());
    return CatalogTable.of(builder.build(), table.comment(), partitionKeys, flinkTableProperties);
  }

  private Column toGravitinoColumn(org.apache.flink.table.catalog.Column column) {
    return Column.of(
        column.getName(),
        TypeUtils.toGravitinoType(column.getDataType().getLogicalType()),
        column.getComment().orElse(null),
        column.getDataType().getLogicalType().isNullable(),
        false,
        null);
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

  private Catalog catalog() {
    return GravitinoCatalogManager.get().getGravitinoCatalogInfo(getName());
  }

  private String catalogName() {
    return getName();
  }
}
