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

import static org.apache.gravitino.Entity.EntityType.TABLE;
import static org.apache.gravitino.catalog.CapabilityHelpers.applyCapabilities;
import static org.apache.gravitino.catalog.PropertiesMetadataHelpers.validatePropertyForCreate;
import static org.apache.gravitino.rel.expressions.transforms.Transforms.EMPTY_TRANSFORM;
import static org.apache.gravitino.utils.NameIdentifierUtil.getCatalogIdentifier;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableOperationDispatcher extends OperationDispatcher implements TableDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(TableOperationDispatcher.class);

  /**
   * Creates a new TableOperationDispatcher instance.
   *
   * @param catalogManager The CatalogManager instance to be used for table operations.
   * @param store The EntityStore instance to be used for table operations.
   * @param idGenerator The IdGenerator instance to be used for table operations.
   */
  public TableOperationDispatcher(
      CatalogManager catalogManager, EntityStore store, IdGenerator idGenerator) {
    super(catalogManager, store, idGenerator);
  }

  /**
   * Lists the tables within a schema.
   *
   * @param namespace The namespace of the schema containing the tables.
   * @return An array of {@link NameIdentifier} objects representing the identifiers of the tables
   *     in the schema.
   * @throws NoSuchSchemaException If the specified schema does not exist.
   */
  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(namespace.levels()),
        LockType.READ,
        () ->
            doWithCatalog(
                getCatalogIdentifier(NameIdentifier.of(namespace.levels())),
                c -> c.doWithTableOps(t -> t.listTables(namespace)),
                NoSuchSchemaException.class));
  }

  /**
   * Loads a table.
   *
   * @param ident The identifier of the table to load.
   * @return The loaded {@link Table} object representing the requested table.
   * @throws NoSuchTableException If the specified table does not exist.
   */
  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    EntityCombinedTable entityCombinedTable =
        TreeLockUtils.doWithTreeLock(ident, LockType.READ, () -> internalLoadTable(ident));

    if (!entityCombinedTable.imported()) {
      // Load the schema to make sure the schema is imported.
      SchemaDispatcher schemaDispatcher = GravitinoEnv.getInstance().schemaDispatcher();
      NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
      schemaDispatcher.loadSchema(schemaIdent);

      // Import the table.
      entityCombinedTable =
          TreeLockUtils.doWithTreeLock(schemaIdent, LockType.WRITE, () -> importTable(ident));
    }

    // Update the column entities in Gravitino store if the columns are different from the ones
    // fetching from the underlying source.
    TableEntity updatedEntity = updateColumnsIfNecessaryWhenLoad(ident, entityCombinedTable);

    return EntityCombinedTable.of(entityCombinedTable.tableFromCatalog(), updatedEntity)
        .withHiddenProperties(
            getHiddenPropertyNames(
                getCatalogIdentifier(ident),
                HasPropertyMetadata::tablePropertiesMetadata,
                entityCombinedTable.tableFromCatalog().properties()))
        .withImported(entityCombinedTable.imported());
  }

  /**
   * Creates a new table in a schema.
   *
   * @param ident The identifier of the table to create.
   * @param columns An array of {@link Column} objects representing the columns of the table.
   * @param comment A description or comment associated with the table.
   * @param properties Additional properties to set for the table.
   * @param partitions An array of {@link Transform} objects representing the partitioning of table
   * @param indexes An array of {@link Index} objects representing the indexes of the table.
   * @return The newly created {@link Table} object.
   * @throws NoSuchSchemaException If the schema in which to create the table does not exist.
   * @throws TableAlreadyExistsException If a table with the same name already exists in the schema.
   */
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

    // Load the schema to make sure the schema exists.
    SchemaDispatcher schemaDispatcher = GravitinoEnv.getInstance().schemaDispatcher();
    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    schemaDispatcher.loadSchema(schemaIdent);

    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(ident.namespace().levels()),
        LockType.WRITE,
        () ->
            internalCreateTable(
                ident,
                columns,
                comment,
                properties,
                partitions,
                distribution,
                sortOrders,
                indexes));
  }

  /**
   * Alters an existing table.
   *
   * @param ident The identifier of the table to alter.
   * @param changes An array of {@link TableChange} objects representing the changes to apply to the
   *     table.
   * @return The altered {@link Table} object after applying the changes.
   * @throws NoSuchTableException If the table to alter does not exist.
   * @throws IllegalArgumentException If an unsupported or invalid change is specified.
   */
  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    validateAlterProperties(ident, HasPropertyMetadata::tablePropertiesMetadata, changes);

    // Check if there exist TableChange.RenameTable in the changes, if so, we need to TreeLock of
    // write on the new table name, or use the read lock on the table instead.
    boolean containsRenameTable =
        Arrays.stream(changes).anyMatch(c -> c instanceof TableChange.RenameTable);
    NameIdentifier nameIdentifierForLock =
        containsRenameTable ? NameIdentifier.of(ident.namespace().levels()) : ident;

    return TreeLockUtils.doWithTreeLock(
        nameIdentifierForLock,
        LockType.WRITE,
        () -> {
          NameIdentifier catalogIdent = getCatalogIdentifier(ident);
          Table alteredTable =
              doWithCatalog(
                  catalogIdent,
                  c ->
                      c.doWithTableOps(
                          t -> t.alterTable(ident, applyCapabilities(c.capabilities(), changes))),
                  NoSuchTableException.class,
                  IllegalArgumentException.class);

          StringIdentifier stringId = getStringIdFromProperties(alteredTable.properties());
          // Case 1: The table is not created by Gravitino and this table is never imported.
          TableEntity te = null;
          if (stringId == null) {
            te = getEntity(ident, TABLE, TableEntity.class);
            if (te == null) {
              return EntityCombinedTable.of(alteredTable)
                  .withHiddenProperties(
                      getHiddenPropertyNames(
                          getCatalogIdentifier(ident),
                          HasPropertyMetadata::tablePropertiesMetadata,
                          alteredTable.properties()));
            }
          }

          long tableId;
          if (stringId != null) {
            tableId = stringId.id();
          } else {
            tableId = te.id();
          }

          TableEntity updatedTableEntity =
              operateOnEntity(
                  ident,
                  id ->
                      store.update(
                          id,
                          TableEntity.class,
                          TABLE,
                          tableEntity -> {
                            String newName =
                                Arrays.stream(changes)
                                    .filter(c -> c instanceof TableChange.RenameTable)
                                    .map(c -> ((TableChange.RenameTable) c).getNewName())
                                    .reduce((c1, c2) -> c2)
                                    .orElse(tableEntity.name());
                            // Update the columns
                            Pair<Boolean, List<ColumnEntity>> columnsUpdateResult =
                                updateColumnsIfNecessary(alteredTable, tableEntity);

                            return TableEntity.builder()
                                .withId(tableEntity.id())
                                .withName(newName)
                                .withNamespace(ident.namespace())
                                .withColumns(columnsUpdateResult.getRight())
                                .withAuditInfo(
                                    AuditInfo.builder()
                                        .withCreator(tableEntity.auditInfo().creator())
                                        .withCreateTime(tableEntity.auditInfo().createTime())
                                        .withLastModifier(
                                            PrincipalUtils.getCurrentPrincipal().getName())
                                        .withLastModifiedTime(Instant.now())
                                        .build())
                                .build();
                          }),
                  "UPDATE",
                  tableId);

          return EntityCombinedTable.of(alteredTable, updatedTableEntity)
              .withHiddenProperties(
                  getHiddenPropertyNames(
                      getCatalogIdentifier(ident),
                      HasPropertyMetadata::tablePropertiesMetadata,
                      alteredTable.properties()));
        });
  }

  /**
   * Drops a table from the catalog.
   *
   * @param ident The identifier of the table to drop.
   * @return {@code true} if the table was successfully dropped, {@code false} if the table does not
   *     exist.
   * @throws RuntimeException If an error occurs while dropping the table.
   */
  @Override
  public boolean dropTable(NameIdentifier ident) {
    NameIdentifier schemaIdentifier = NameIdentifierUtil.getSchemaIdentifier(ident);
    return TreeLockUtils.doWithTreeLock(
        schemaIdentifier,
        LockType.WRITE,
        () -> {
          NameIdentifier catalogIdent = getCatalogIdentifier(ident);
          boolean droppedFromCatalog =
              doWithCatalog(
                  catalogIdent,
                  c -> c.doWithTableOps(t -> t.dropTable(ident)),
                  RuntimeException.class);

          // For unmanaged table, it could happen that the table:
          // 1. Is not found in the catalog (dropped directly from underlying sources)
          // 2. Is found in the catalog but not in the store (not managed by Gravitino)
          // 3. Is found in the catalog and the store (managed by Gravitino)
          // 4. Neither found in the catalog nor in the store.
          // In all situations, we try to delete the schema from the store, but we don't take the
          // return value of the store operation into account. We only take the return value of the
          // catalog into account.
          //
          // For managed table, we should take the return value of the store operation into account.
          boolean droppedFromStore = false;
          try {
            droppedFromStore = store.delete(ident, TABLE);
          } catch (NoSuchEntityException e) {
            LOG.warn("The table to be dropped does not exist in the store: {}", ident, e);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }

          return isManagedEntity(catalogIdent, Capability.Scope.TABLE)
              ? droppedFromStore
              : droppedFromCatalog;
        });
  }

  /**
   * Drop a table from the catalog and completely remove its data. Removes both the metadata and the
   * directory associated with the table completely and skipping trash. If the table is an external
   * table or the catalogs don't support purge table, {@link UnsupportedOperationException} is
   * thrown.
   *
   * <p>If the catalog supports to purge a table, this method should be overridden. The default
   * implementation throws an {@link UnsupportedOperationException}.
   *
   * @param ident A table identifier.
   * @return True if the table is purged, false if the table does not exist.
   * @throws UnsupportedOperationException If the catalog does not support to purge a table.
   * @throws RuntimeException If an error occurs while purging the table.
   */
  @Override
  public boolean purgeTable(NameIdentifier ident) throws UnsupportedOperationException {
    NameIdentifier schemaIdentifier = NameIdentifierUtil.getSchemaIdentifier(ident);
    NameIdentifier catalogIdent = getCatalogIdentifier(ident);
    return TreeLockUtils.doWithTreeLock(
        schemaIdentifier,
        LockType.WRITE,
        () -> {
          boolean droppedFromCatalog =
              doWithCatalog(
                  catalogIdent,
                  c -> c.doWithTableOps(t -> t.purgeTable(ident)),
                  RuntimeException.class,
                  UnsupportedOperationException.class);

          // For unmanaged table, it could happen that the table:
          // 1. Is not found in the catalog (dropped directly from underlying sources)
          // 2. Is found in the catalog but not in the store (not managed by Gravitino)
          // 3. Is found in the catalog and the store (managed by Gravitino)
          // 4. Neither found in the catalog nor in the store.
          // In all situations, we try to delete the schema from the store, but we don't take the
          // return value of the store operation into account. We only take the return value of the
          // catalog into account.
          //
          // For managed table, we should take the return value of the store operation into account.
          boolean droppedFromStore;
          try {
            droppedFromStore = store.delete(ident, TABLE);
          } catch (NoSuchEntityException e) {
            LOG.warn("The table to be purged does not exist in the store: {}", ident, e);
            return false;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }

          return isManagedEntity(catalogIdent, Capability.Scope.TABLE)
              ? droppedFromStore
              : droppedFromCatalog;
        });
  }

  private EntityCombinedTable importTable(NameIdentifier identifier) {
    EntityCombinedTable table = internalLoadTable(identifier);

    if (table.imported()) {
      return table;
    }

    StringIdentifier stringId = null;
    try {
      stringId = table.stringIdentifier();
    } catch (IllegalArgumentException ie) {
      LOG.warn(FormattedErrorMessages.STRING_ID_PARSE_ERROR, ie.getMessage());
    }

    long uid;
    if (stringId != null) {
      // If the entity in the store doesn't match the external system, we use the data
      // of external system to correct it.
      LOG.warn(
          "The Table uid {} existed but still need to be imported, this could be happened "
              + "when Table is renamed by external systems not controlled by Gravitino. In this "
              + "case, we need to overwrite the stored entity to keep the consistency.",
          stringId);
      uid = stringId.id();
    } else {
      // If entity doesn't exist, we import the entity from the external system.
      uid = idGenerator.nextId();
    }

    AuditInfo audit =
        AuditInfo.builder()
            .withCreator(table.auditInfo().creator())
            .withCreateTime(table.auditInfo().createTime())
            .withLastModifier(table.auditInfo().lastModifier())
            .withLastModifiedTime(table.auditInfo().lastModifiedTime())
            .build();
    List<ColumnEntity> columnEntityList =
        toColumnEntities(table.tableFromCatalog().columns(), audit);
    TableEntity tableEntity =
        TableEntity.builder()
            .withId(uid)
            .withName(identifier.name())
            .withNamespace(identifier.namespace())
            .withColumns(columnEntityList)
            .withAuditInfo(audit)
            .build();
    try {
      store.put(tableEntity, true);
    } catch (EntityAlreadyExistsException e) {
      LOG.error("Failed to import table {} with id {} to the store.", identifier, uid, e);
      throw new UnsupportedOperationException(
          "Table managed by multiple catalogs. This may cause unexpected issues such as privilege conflicts. "
              + "To resolve: Remove all catalogs managing this table, then recreate one catalog to ensure single-catalog management.");
    } catch (Exception e) {
      LOG.error(FormattedErrorMessages.STORE_OP_FAILURE, "put", identifier, e);
      throw new RuntimeException("Fail to import the table entity to the store.", e);
    }

    return EntityCombinedTable.of(table.tableFromCatalog(), tableEntity)
        .withHiddenProperties(
            getHiddenPropertyNames(
                getCatalogIdentifier(identifier),
                HasPropertyMetadata::tablePropertiesMetadata,
                table.tableFromCatalog().properties()));
  }

  private EntityCombinedTable internalLoadTable(NameIdentifier ident) {
    NameIdentifier catalogIdentifier = getCatalogIdentifier(ident);
    Table table =
        doWithCatalog(
            catalogIdentifier,
            c -> c.doWithTableOps(t -> t.loadTable(ident)),
            NoSuchTableException.class);

    StringIdentifier stringId = getStringIdFromProperties(table.properties());
    // Case 1: The table is not created by Gravitino or the external system does not support storing
    // string identifier.
    if (stringId == null) {
      TableEntity tableEntity = getEntity(ident, TABLE, TableEntity.class);
      if (tableEntity == null) {
        return EntityCombinedTable.of(table)
            .withHiddenProperties(
                getHiddenPropertyNames(
                    catalogIdentifier,
                    HasPropertyMetadata::tablePropertiesMetadata,
                    table.properties()))
            // Some tables don't have properties or are not created by Gravitino,
            // we can't use stringIdentifier to judge whether schema is ever imported or not.
            // We need to check whether the entity exists.
            .withImported(false);
      }

      return EntityCombinedTable.of(table, tableEntity)
          .withHiddenProperties(
              getHiddenPropertyNames(
                  catalogIdentifier,
                  HasPropertyMetadata::tablePropertiesMetadata,
                  table.properties()))
          // Some tables don't have properties or are not created by Gravitino,
          // we can't use stringIdentifier to judge whether schema is ever imported or not.
          // We need to check whether the entity exists.
          .withImported(true);
    }

    TableEntity tableEntity =
        operateOnEntity(
            ident,
            identifier -> store.get(identifier, TABLE, TableEntity.class),
            "GET",
            stringId.id());

    return EntityCombinedTable.of(table, tableEntity)
        .withHiddenProperties(
            getHiddenPropertyNames(
                catalogIdentifier,
                HasPropertyMetadata::tablePropertiesMetadata,
                table.properties()))
        .withImported(tableEntity != null);
  }

  private Table internalCreateTable(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes) {
    NameIdentifier catalogIdent = getCatalogIdentifier(ident);
    doWithCatalog(
        catalogIdent,
        c ->
            c.doWithPropertiesMeta(
                p -> {
                  validatePropertyForCreate(p.tablePropertiesMetadata(), properties);
                  return null;
                }),
        IllegalArgumentException.class);
    long uid = idGenerator.nextId();
    // Add StringIdentifier to the properties, the specific catalog will handle this
    // StringIdentifier to make sure only when the operation is successful, the related
    // TableEntity will be visible.
    StringIdentifier stringId = StringIdentifier.fromId(uid);
    Map<String, String> updatedProperties =
        StringIdentifier.newPropertiesWithId(stringId, properties);

    // we do not retrieve the table again (to obtain some values generated by underlying catalog)
    // since some catalogs' API is async and the table may not be created immediately
    Table table =
        doWithCatalog(
            catalogIdent,
            c ->
                c.doWithTableOps(
                    t ->
                        t.createTable(
                            ident,
                            columns,
                            comment,
                            updatedProperties,
                            partitions == null ? EMPTY_TRANSFORM : partitions,
                            distribution == null ? Distributions.NONE : distribution,
                            sortOrders == null ? new SortOrder[0] : sortOrders,
                            indexes == null ? Indexes.EMPTY_INDEXES : indexes)),
            NoSuchSchemaException.class,
            TableAlreadyExistsException.class);

    AuditInfo audit =
        AuditInfo.builder()
            .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
            .withCreateTime(Instant.now())
            .build();
    List<ColumnEntity> columnEntityList =
        IntStream.range(0, columns.length)
            .mapToObj(i -> ColumnEntity.toColumnEntity(columns[i], i, idGenerator.nextId(), audit))
            .collect(Collectors.toList());

    TableEntity tableEntity =
        TableEntity.builder()
            .withId(uid)
            .withName(ident.name())
            .withNamespace(ident.namespace())
            .withColumns(columnEntityList)
            .withAuditInfo(audit)
            .build();

    try {
      store.put(tableEntity, true /* overwrite */);
    } catch (Exception e) {
      LOG.error(FormattedErrorMessages.STORE_OP_FAILURE, "put", ident, e);
      return EntityCombinedTable.of(table)
          .withHiddenProperties(
              getHiddenPropertyNames(
                  catalogIdent, HasPropertyMetadata::tablePropertiesMetadata, table.properties()));
    }

    return EntityCombinedTable.of(table, tableEntity)
        .withHiddenProperties(
            getHiddenPropertyNames(
                catalogIdent, HasPropertyMetadata::tablePropertiesMetadata, table.properties()));
  }

  private List<ColumnEntity> toColumnEntities(Column[] columns, AuditInfo audit) {
    return columns == null
        ? Collections.emptyList()
        : IntStream.range(0, columns.length)
            .mapToObj(i -> ColumnEntity.toColumnEntity(columns[i], i, idGenerator.nextId(), audit))
            .collect(Collectors.toList());
  }

  private boolean isSameColumn(Column left, int columnPosition, ColumnEntity right) {
    return Objects.equal(left.name(), right.name())
        && columnPosition == right.position()
        && Objects.equal(left.dataType(), right.dataType())
        && Objects.equal(left.comment(), right.comment())
        && left.nullable() == right.nullable()
        && left.autoIncrement() == right.autoIncrement()
        && Objects.equal(left.defaultValue(), right.defaultValue());
  }

  private Pair<Boolean, List<ColumnEntity>> updateColumnsIfNecessary(
      Table tableFromCatalog, TableEntity tableFromGravitino) {
    if (tableFromCatalog == null || tableFromGravitino == null) {
      LOG.warn(
          "Cannot update columns for table when altering because table or table entity is "
              + "null");
      return Pair.of(false, Collections.emptyList());
    }

    Map<String, Pair<Integer, Column>> columnsFromCatalogTable =
        tableFromCatalog.columns() == null
            ? Collections.emptyMap()
            : IntStream.range(0, tableFromCatalog.columns().length)
                .mapToObj(i -> Pair.of(i, tableFromCatalog.columns()[i]))
                .collect(
                    Collectors.toMap(
                        p -> p.getRight().name(),
                        Function.identity(),
                        (existing, replacement) -> {
                          LOG.warn(
                              "Duplicate column name '{}' found at position {} and {}, using the first occurrence",
                              existing.getRight().name(),
                              existing.getLeft(),
                              replacement.getLeft());
                          return existing;
                        }));
    Map<String, ColumnEntity> columnsFromTableEntity =
        tableFromGravitino.columns() == null
            ? Collections.emptyMap()
            : tableFromGravitino.columns().stream()
                .collect(
                    Collectors.toMap(
                        ColumnEntity::name,
                        Function.identity(),
                        (existing, replacement) -> {
                          LOG.warn(
                              "Duplicate column name '{}' found in table entity, using the existing one",
                              existing.name());
                          return existing;
                        }));

    // Check if columns need to be updated in Gravitino store
    List<ColumnEntity> columnsToInsert = Lists.newArrayList();
    boolean columnsNeedsUpdate = false;
    for (Map.Entry<String, ColumnEntity> entry : columnsFromTableEntity.entrySet()) {
      Pair<Integer, Column> columnPair = columnsFromCatalogTable.get(entry.getKey());
      if (columnPair == null) {
        LOG.debug(
            "Column {} is not found in the table from underlying source, it will be removed"
                + " from the table entity",
            entry.getKey());
        columnsNeedsUpdate = true;

      } else if (!isSameColumn(columnPair.getRight(), columnPair.getLeft(), entry.getValue())) {
        // If the column need to be updated, we create a new ColumnEntity with the same id
        LOG.debug(
            "Column {} is found in the table from underlying source, but it is different "
                + "from the one in the table entity, it will be updated",
            entry.getKey());

        Column column = columnPair.getRight();
        ColumnEntity updatedColumnEntity =
            ColumnEntity.builder()
                .withId(entry.getValue().id())
                .withName(column.name())
                .withPosition(columnPair.getLeft())
                .withDataType(column.dataType())
                .withComment(column.comment())
                .withNullable(column.nullable())
                .withAutoIncrement(column.autoIncrement())
                .withDefaultValue(column.defaultValue())
                .withAuditInfo(
                    AuditInfo.builder()
                        .withCreator(entry.getValue().auditInfo().creator())
                        .withCreateTime(entry.getValue().auditInfo().createTime())
                        .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                        .withLastModifiedTime(Instant.now())
                        .build())
                .build();

        columnsNeedsUpdate = true;
        columnsToInsert.add(updatedColumnEntity);

      } else {
        // If the column is the same, we keep the original ColumnEntity
        columnsToInsert.add(entry.getValue());
      }
    }

    // Check if there are new columns in the table from the underlying source
    for (Map.Entry<String, Pair<Integer, Column>> entry : columnsFromCatalogTable.entrySet()) {
      if (!columnsFromTableEntity.containsKey(entry.getKey())) {
        LOG.debug(
            "Column {} is found in the table from underlying source but not in the table "
                + "entity, it will be added to the table entity",
            entry.getKey());
        ColumnEntity newColumnEntity =
            ColumnEntity.toColumnEntity(
                entry.getValue().getRight(),
                entry.getValue().getLeft(),
                idGenerator.nextId(),
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build());

        columnsNeedsUpdate = true;
        columnsToInsert.add(newColumnEntity);
      }
    }

    return Pair.of(columnsNeedsUpdate, columnsToInsert);
  }

  private TableEntity updateColumnsIfNecessaryWhenLoad(
      NameIdentifier tableIdent, EntityCombinedTable combinedTable) {
    Pair<Boolean, List<ColumnEntity>> columnsUpdateResult =
        updateColumnsIfNecessary(
            combinedTable.tableFromCatalog(), combinedTable.tableFromGravitino());

    // No need to update the columns
    if (!columnsUpdateResult.getLeft()) {
      return combinedTable.tableFromGravitino();
    }

    // Update the columns in the Gravitino store
    return TreeLockUtils.doWithTreeLock(
        tableIdent,
        LockType.WRITE,
        () ->
            operateOnEntity(
                tableIdent,
                id ->
                    store.update(
                        id,
                        TableEntity.class,
                        TABLE,
                        entity ->
                            TableEntity.builder()
                                .withId(entity.id())
                                .withName(entity.name())
                                .withNamespace(entity.namespace())
                                .withColumns(columnsUpdateResult.getRight())
                                .withAuditInfo(
                                    AuditInfo.builder()
                                        .withCreator(entity.auditInfo().creator())
                                        .withCreateTime(entity.auditInfo().createTime())
                                        .withLastModifier(
                                            PrincipalUtils.getCurrentPrincipal().getName())
                                        .withLastModifiedTime(Instant.now())
                                        .build())
                                .build()),
                "UPDATE",
                combinedTable.tableFromGravitino().id()));
  }
}
