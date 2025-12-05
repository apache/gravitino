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
package org.apache.gravitino.catalog;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.connector.GenericColumn;
import org.apache.gravitino.connector.GenericTable;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.PrincipalUtils;

public abstract class ManagedTableOperations implements TableCatalog {

  private static final Joiner DOT = Joiner.on(".");

  protected abstract EntityStore store();

  protected abstract SupportsSchemas schemas();

  protected abstract IdGenerator idGenerator();

  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    try {
      // The current implementation of JDBC entity store will automatically check the existence of
      // the namespace when listing entities under the namespace. If not, it will throw an
      // NoSuchEntityException. So, we don't need to check the existence of the namespace here
      // again.
      List<TableEntity> tables =
          store().list(namespace, TableEntity.class, Entity.EntityType.TABLE);
      return tables.stream()
          .map(t -> NameIdentifier.of(namespace, t.name()))
          .toArray(NameIdentifier[]::new);

    } catch (NoSuchEntityException e) {
      throw new NoSuchSchemaException(e, "Schema %s does not exist", namespace);
    } catch (IOException e) {
      throw new RuntimeException("Failed to list tables in schema " + namespace, e);
    }
  }

  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    try {
      TableEntity tableEntity = store().get(ident, Entity.EntityType.TABLE, TableEntity.class);
      return toGenericTable(tableEntity);

    } catch (NoSuchEntityException e) {
      throw new NoSuchTableException(e, "Table %s does not exist", ident);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load table " + ident, e);
    }
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
    // createTable in ManagedTableOperations only stores the table metadata in the entity store.
    // It doesn't handle any additional operations like creating physical location, preprocessing
    // the properties, etc. Those operations should be handled in the specific catalog
    // implementation.
    StringIdentifier stringId = StringIdentifier.fromProperties(properties);
    Preconditions.checkArgument(stringId != null, "Property String identifier should not be null");

    AuditInfo auditInfo =
        AuditInfo.builder()
            .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
            .withCreateTime(Instant.now())
            .build();

    TableEntity tableEntity =
        TableEntity.builder()
            .withName(ident.name())
            .withId(stringId.id())
            .withNamespace(ident.namespace())
            .withComment(comment)
            .withColumns(toColumnEntities(columns, auditInfo, idGenerator()))
            .withProperties(properties)
            .withPartitioning(partitions)
            .withDistribution(distribution)
            .withSortOrders(sortOrders)
            .withIndexes(indexes)
            .withAuditInfo(auditInfo)
            .build();

    try {
      store().put(tableEntity, false /* overwrite */);
    } catch (NoSuchEntityException e) {
      // The put operation in the current JDBC entity store will check the existence of the
      // namespace when creating an entity under the namespace. If not, it will throw a
      // NoSuchEntityException. So, we don't need to check the existence of the namespace here
      // again.
      throw new NoSuchSchemaException(e, "Schema %s does not exist", ident.namespace());
    } catch (EntityAlreadyExistsException e) {
      throw new TableAlreadyExistsException(e, "Table %s already exists", ident);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create table " + ident, e);
    }

    return toGenericTable(tableEntity);
  }

  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    // The alterTable in ManagedTableOperations only updates the table metadata in the entity store.
    // It doesn't handle any additional operations like modifying physical data, etc. Those
    // operations should be handled in the specific catalog implementation.
    try {
      TableEntity newTableEntity =
          store()
              .update(
                  ident,
                  TableEntity.class,
                  Entity.EntityType.TABLE,
                  oldEntity -> applyChanges(oldEntity, changes));

      return toGenericTable(newTableEntity);
    } catch (NoSuchEntityException e) {
      throw new NoSuchTableException(e, "Table %s does not exist", ident);
    } catch (EntityAlreadyExistsException e) {
      throw new IllegalArgumentException(
          "Failed to rename table " + ident + " due to table already exists: ", e);
    } catch (IOException e) {
      throw new RuntimeException("Failed to alter table " + ident, e);
    }
  }

  @Override
  public boolean purgeTable(NameIdentifier ident) {
    // For Gravitino managed tables, purgeTable is equivalent to dropTable. It only removes the
    // table metadata from the entity store. Physical data deletion should be handled by the
    // specific catalog implementation if needed.
    return dropTable(ident);
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    try {
      return store().delete(ident, Entity.EntityType.TABLE);
    } catch (NoSuchEntityException e) {
      return false;
    } catch (IOException e) {
      throw new RuntimeException("Failed to drop metadata for table " + ident, e);
    }
  }

  private TableEntity applyChanges(TableEntity oldTableEntity, TableChange... changes) {
    String newName = oldTableEntity.name();
    String newComment = oldTableEntity.comment();
    Map<String, String> newProps = Maps.newHashMap(oldTableEntity.properties());
    List<ColumnEntity> newColumns = Lists.newArrayList(oldTableEntity.columns());
    List<Index> newIndexes = Lists.newArrayList(oldTableEntity.indexes());

    Map<Boolean, List<TableChange>> splitChanges =
        Arrays.stream(changes)
            .collect(
                Collectors.partitioningBy(change -> change instanceof TableChange.ColumnChange));
    List<TableChange.ColumnChange> columnChanges =
        splitChanges.get(true).stream()
            .map(change -> (TableChange.ColumnChange) change)
            .collect(Collectors.toList());
    List<TableChange> tableChanges = splitChanges.get(false);

    for (TableChange change : tableChanges) {
      if (change instanceof TableChange.RenameTable rename) {
        if (rename.getNewSchemaName().isPresent()) {
          throw new IllegalArgumentException(
              "Gravitino managed table doesn't support renaming "
                  + "the table across schemas for now");
        }

        newName = rename.getNewName();

      } else if (change instanceof TableChange.UpdateComment updateComment) {
        newComment = updateComment.getNewComment();

      } else if (change instanceof TableChange.SetProperty setProperty) {
        newProps.put(setProperty.getProperty(), setProperty.getValue());

      } else if (change instanceof TableChange.RemoveProperty removeProperty) {
        newProps.remove(removeProperty.getProperty());

      } else if (change instanceof TableChange.AddIndex addIndex) {
        Index newIndex =
            Indexes.IndexImpl.builder()
                .withName(addIndex.getName())
                .withFieldNames(addIndex.getFieldNames())
                .withIndexType(addIndex.getType())
                .build();
        newIndexes.add(newIndex);

      } else if (change instanceof TableChange.DeleteIndex deleteIndex) {
        boolean removed = newIndexes.removeIf(idx -> idx.name().equals(deleteIndex.getName()));
        if (!removed && !deleteIndex.isIfExists()) {
          throw new IllegalArgumentException(
              String.format(
                  "Index %s does not exist while ifExists is false", deleteIndex.getName()));
        }

      } else {
        throw new IllegalArgumentException("Unsupported table change: " + change);
      }
    }

    newColumns = applyColumnChanges(newColumns, columnChanges);

    return TableEntity.builder()
        .withId(oldTableEntity.id())
        .withName(newName)
        .withNamespace(oldTableEntity.namespace())
        .withComment(newComment)
        .withColumns(newColumns)
        .withProperties(newProps)
        .withPartitioning(oldTableEntity.partitioning())
        .withDistribution(oldTableEntity.distribution())
        .withSortOrders(oldTableEntity.sortOrders())
        .withIndexes(newIndexes.toArray(Index[]::new))
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(oldTableEntity.auditInfo().creator())
                .withCreateTime(oldTableEntity.auditInfo().createTime())
                .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                .withLastModifiedTime(Instant.now())
                .build())
        .build();
  }

  private List<ColumnEntity> applyColumnChanges(
      List<ColumnEntity> oldColumns, List<TableChange.ColumnChange> columnChanges) {
    // sort the column by position first, columns maybe unordered when retrieved from the store.
    List<ColumnEntity> newColumns =
        Lists.newArrayList(oldColumns).stream()
            .sorted(Comparator.comparingInt(ColumnEntity::position))
            .collect(Collectors.toList());

    for (TableChange.ColumnChange change : columnChanges) {
      if (change instanceof TableChange.AddColumn addColumn) {
        String columnName = DOT.join(addColumn.getFieldName());
        boolean exists = newColumns.stream().anyMatch(col -> col.name().equals(columnName));
        if (exists) {
          throw new IllegalArgumentException(String.format("Column %s already exists", columnName));
        }
        // Note. The default behavior of addColumn is to add the column at the end.
        int position = calculateColumnPosition(newColumns, addColumn.getPosition(), true);

        ColumnEntity columnToAdd =
            ColumnEntity.builder()
                .withId(idGenerator().nextId())
                .withName(DOT.join(addColumn.getFieldName()))
                .withDataType(addColumn.getDataType())
                .withPosition(position)
                .withComment(addColumn.getComment())
                .withNullable(addColumn.isNullable())
                .withAutoIncrement(addColumn.isAutoIncrement())
                .withDefaultValue(addColumn.getDefaultValue())
                .withAuditInfo(
                    AuditInfo.builder()
                        .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                        .withCreateTime(Instant.now())
                        .build())
                .build();

        // Add the new column at the specified position
        newColumns.add(position, columnToAdd);

      } else if (change instanceof TableChange.RenameColumn
          || change instanceof TableChange.UpdateColumnDefaultValue
          || change instanceof TableChange.UpdateColumnType
          || change instanceof TableChange.UpdateColumnComment
          || change instanceof TableChange.UpdateColumnPosition
          || change instanceof TableChange.UpdateColumnNullability
          || change instanceof TableChange.UpdateColumnAutoIncrement) {
        int i;
        ColumnEntity oldColumn = null;
        for (i = 0; i < newColumns.size(); i++) {
          ColumnEntity col = newColumns.get(i);
          if (col.name().equals(DOT.join(change.fieldName()))) {
            oldColumn = col;
            break;
          }
        }
        if (oldColumn == null) {
          throw new IllegalArgumentException(
              String.format(
                  "Column %s not found for %s",
                  DOT.join(change.fieldName()), change.getClass().getSimpleName()));
        }

        // Remove the old column temporarily, we will insert it back after updating.
        newColumns.remove(oldColumn);

        Optional<String> newName = Optional.empty();
        if (change instanceof TableChange.RenameColumn rename) {
          boolean columnExists =
              newColumns.stream().anyMatch(col -> col.name().equals(rename.getNewName()));
          if (columnExists) {
            throw new IllegalArgumentException(
                String.format(
                    "Column %s already exists when renaming column %s",
                    rename.getNewName(), DOT.join(change.fieldName())));
          }

          newName = Optional.of(rename.getNewName());
        }

        Optional<Expression> newDefaultValue =
            change instanceof TableChange.UpdateColumnDefaultValue updateDefault
                ? Optional.of(updateDefault.getNewDefaultValue())
                : Optional.empty();
        Optional<Type> newDataType =
            change instanceof TableChange.UpdateColumnType updateType
                ? Optional.of(updateType.getNewDataType())
                : Optional.empty();
        Optional<String> newComment =
            change instanceof TableChange.UpdateColumnComment updateComment
                ? Optional.of(updateComment.getNewComment())
                : Optional.empty();
        Optional<Boolean> newNullable =
            change instanceof TableChange.UpdateColumnNullability updateNullability
                ? Optional.of(updateNullability.nullable())
                : Optional.empty();
        Optional<Boolean> newAutoIncrement =
            change instanceof TableChange.UpdateColumnAutoIncrement updateAutoIncrement
                ? Optional.of(updateAutoIncrement.isAutoIncrement())
                : Optional.empty();

        Optional<Integer> newPosition = Optional.empty();
        if (change instanceof TableChange.UpdateColumnPosition updateColumnPosition) {
          newPosition =
              Optional.of(
                  calculateColumnPosition(newColumns, updateColumnPosition.getPosition(), false));
        }

        // add back the updated column
        ColumnEntity newColumn =
            updateColumnEntity(
                oldColumn,
                newName,
                newDefaultValue,
                newDataType,
                newComment,
                newPosition,
                newNullable,
                newAutoIncrement);
        newColumns.add(newColumn.position(), newColumn);

      } else if (change instanceof TableChange.DeleteColumn deleteColumn) {
        boolean removed =
            newColumns.removeIf(col -> col.name().equals(DOT.join(deleteColumn.fieldName())));
        if (!removed && !deleteColumn.getIfExists()) {
          throw new IllegalArgumentException(
              String.format(
                  "Column %s not found for deletion while ifExists is false",
                  DOT.join(deleteColumn.fieldName())));
        }

      } else {
        throw new IllegalArgumentException("Unsupported column change: " + change);
      }
    }

    // After the column adding, updating, deleting, the positions of the columns may be messed up.
    // We need to reassign the positions to ensure they are continuous and start from 0.
    return updateColumnPositions(newColumns);
  }

  private ColumnEntity updateColumnEntity(
      ColumnEntity oldColumn,
      Optional<String> newName,
      Optional<Expression> newDefaultValue,
      Optional<Type> newDataType,
      Optional<String> newComment,
      Optional<Integer> newPosition,
      Optional<Boolean> newNullable,
      Optional<Boolean> newAutoIncrement) {
    return ColumnEntity.builder()
        .withId(oldColumn.id())
        .withName(newName.orElse(oldColumn.name()))
        .withDataType(newDataType.orElse(oldColumn.dataType()))
        .withPosition(newPosition.orElse(oldColumn.position()))
        .withComment(newComment.orElse(oldColumn.comment()))
        .withNullable(newNullable.orElse(oldColumn.nullable()))
        .withAutoIncrement(newAutoIncrement.orElse(oldColumn.autoIncrement()))
        .withDefaultValue(newDefaultValue.orElse(oldColumn.defaultValue()))
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(oldColumn.auditInfo().creator())
                .withCreateTime(oldColumn.auditInfo().createTime())
                .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                .withLastModifiedTime(Instant.now())
                .build())
        .build();
  }

  private GenericColumn toGenericColumn(ColumnEntity columnEntity) {
    return GenericColumn.builder()
        .withName(columnEntity.name())
        .withComment(columnEntity.comment())
        .withAutoIncrement(columnEntity.autoIncrement())
        .withNullable(columnEntity.nullable())
        .withType(columnEntity.dataType())
        .withDefaultValue(columnEntity.defaultValue())
        .build();
  }

  private GenericTable toGenericTable(TableEntity tableEntity) {
    return GenericTable.builder()
        .withName(tableEntity.name())
        .withComment(tableEntity.comment())
        .withColumns(
            tableEntity.columns().stream().map(this::toGenericColumn).toArray(Column[]::new))
        .withProperties(tableEntity.properties())
        .withAuditInfo(tableEntity.auditInfo())
        .withSortOrders(tableEntity.sortOrders())
        .withPartitioning(tableEntity.partitioning())
        .withDistribution(tableEntity.distribution())
        .withIndexes(tableEntity.indexes())
        .build();
  }

  private List<ColumnEntity> toColumnEntities(
      Column[] columns, AuditInfo audit, IdGenerator idGenerator) {
    return columns == null
        ? Collections.emptyList()
        : IntStream.range(0, columns.length)
            .mapToObj(i -> ColumnEntity.toColumnEntity(columns[i], i, idGenerator.nextId(), audit))
            .collect(Collectors.toList());
  }

  private List<ColumnEntity> updateColumnPositions(List<ColumnEntity> columns) {
    List<ColumnEntity> updatedColumns = Lists.newArrayList();
    for (int i = 0; i < columns.size(); i++) {
      ColumnEntity oldColumn = columns.get(i);
      if (oldColumn.position() != i) {
        ColumnEntity newColumn =
            ColumnEntity.builder()
                .withId(oldColumn.id())
                .withName(oldColumn.name())
                .withDataType(oldColumn.dataType())
                .withPosition(i)
                .withComment(oldColumn.comment())
                .withNullable(oldColumn.nullable())
                .withAutoIncrement(oldColumn.autoIncrement())
                .withDefaultValue(oldColumn.defaultValue())
                .withAuditInfo((AuditInfo) oldColumn.auditInfo())
                .build();
        updatedColumns.add(newColumn);
      } else {
        updatedColumns.add(oldColumn);
      }
    }
    return updatedColumns;
  }

  int calculateColumnPosition(
      List<ColumnEntity> existingColumns, TableChange.ColumnPosition position, boolean forAdd) {
    if (position == TableChange.ColumnPosition.first()) {
      return 0;
    } else if (position instanceof TableChange.After afterColumn) {
      for (int i = 0; i < existingColumns.size(); i++) {
        if (existingColumns.get(i).name().equals(afterColumn.getColumn())) {
          return i + 1;
        }
      }
      throw new IllegalArgumentException(
          String.format("Column %s not found for adding column after it", afterColumn.getColumn()));
    } else if (forAdd && position == TableChange.ColumnPosition.defaultPos()) {
      // Default position of Gravitino managed table is to add the column at the end.
      // For add column operation only. Change column operation with default position is not
      // supported.
      return existingColumns.size();
    } else {
      throw new IllegalArgumentException("Unsupported column position: " + position);
    }
  }
}
