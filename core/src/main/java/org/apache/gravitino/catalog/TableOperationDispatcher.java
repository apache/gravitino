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

import static org.apache.gravitino.Entity.EntityType.COLUMN;
import static org.apache.gravitino.Entity.EntityType.TABLE;
import static org.apache.gravitino.catalog.PropertiesMetadataHelpers.validatePropertyForCreate;
import static org.apache.gravitino.rel.expressions.transforms.Transforms.EMPTY_TRANSFORM;
import static org.apache.gravitino.utils.NameIdentifierUtil.getCatalogIdentifier;
import static org.apache.gravitino.utils.NameIdentifierUtil.getSchemaIdentifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Audit;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.MaskAndOmitKeys;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.dto.rel.expressions.FunctionArg;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.secret.SecretManager;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.gravitino.utils.WritableDataTypeValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableOperationDispatcher extends OperationDispatcher implements TableDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(TableOperationDispatcher.class);

  private final Supplier<SchemaDispatcher> schemaDispatcherSupplier;

  /**
   * Creates a new TableOperationDispatcher instance.
   *
   * @param catalogManager The CatalogManager instance to be used for table operations.
   * @param store The EntityStore instance to be used for table operations.
   * @param idGenerator The IdGenerator instance to be used for table operations.
   * @param secretManager The SecretManager instance to be used for secret operations.
   */
  public TableOperationDispatcher(
      CatalogManager catalogManager,
      EntityStore store,
      IdGenerator idGenerator,
      SecretManager secretManager) {
    this(
        catalogManager,
        store,
        idGenerator,
        () -> GravitinoEnv.getInstance().internalSchemaDispatcher(),
        secretManager);
  }

  /**
   * Creates a new TableOperationDispatcher instance.
   *
   * @param catalogManager The CatalogManager instance to be used for table operations.
   * @param store The EntityStore instance to be used for table operations.
   * @param idGenerator The IdGenerator instance to be used for table operations.
   * @param schemaDispatcherSupplier The SchemaDispatcher supplier to ensure schemas are imported.
   * @param secretManager The SecretManager instance to be used for secret operations.
   */
  public TableOperationDispatcher(
      CatalogManager catalogManager,
      EntityStore store,
      IdGenerator idGenerator,
      Supplier<SchemaDispatcher> schemaDispatcherSupplier,
      SecretManager secretManager) {
    super(catalogManager, store, idGenerator, secretManager);
    this.schemaDispatcherSupplier =
        Preconditions.checkNotNull(
            schemaDispatcherSupplier, "schemaDispatcherSupplier must not be null");
  }

  /**
   * Checks whether a table exists, treating catalogs without table operations as empty.
   *
   * @param ident the table identifier
   * @return whether the table exists
   */
  @Override
  public boolean tableExists(NameIdentifier ident) {
    boolean supportsTables =
        doWithCatalog(
            getCatalogIdentifier(ident),
            wrapper -> wrapper.doWithCatalog(catalog -> catalog.ops() instanceof TableCatalog),
            RuntimeException.class);
    return supportsTables && TableDispatcher.super.tableExists(ident);
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
   * @throws NoSuchSchemaException If the containing schema does not exist.
   */
  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    return loadAndImportTable(ident, AuditInfo.EMPTY);
  }

  /** {@inheritDoc} */
  @Override
  public Table importTable(NameIdentifier ident, AuditInfo auditInfo) throws NoSuchTableException {
    Preconditions.checkArgument(auditInfo != null, "auditInfo must not be null");
    return loadAndImportTable(ident, auditInfo);
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
   * @throws GravitinoRuntimeException If external creation succeeds but registration fails; resolve
   *     the entity-store error, verify the source identity, and use loadTable to import it.
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

    TableEntity.NAME.validate(ident.name(), TABLE);
    WritableDataTypeValidator.validateTable(columns, partitions, distribution, sortOrders);
    validateColumns(columns);

    // Load the schema to make sure the schema exists.
    SchemaDispatcher schemaDispatcher = getSchemaDispatcher();
    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    schemaDispatcher.loadSchema(schemaIdent);

    // Lock the table node, not the schema, so tables in the same schema can be created
    // concurrently. The connector call and the entity-store write otherwise run under one
    // schema-wide lock, which serialized all creates in a schema. Ancestors are still read-locked,
    // so create keeps excluding dropSchema/createSchema (catalog WRITE) and rename/drop
    // (schema WRITE); a same-name create, load/import or alter contends on the table node.
    // Trade-off: listTables may briefly observe a table whose creation has not committed yet.
    return TreeLockUtils.doWithTreeLock(
        ident,
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
   * @throws GravitinoRuntimeException If a rename succeeds in the external catalog but Gravitino
   *     cannot update the stored table registration consistently.
   */
  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    boolean isRenameTable =
        Arrays.stream(changes).anyMatch(change -> change instanceof TableChange.RenameTable);

    // use the read lock on the table if there does not exist TableChange.RenameTable in the
    // changes, or:
    // 1. if the TableChange.RenameTable change the schema name, we need to acquire write lock on
    // the catalog,
    //    because the treeLock tool currently does not support acquiring write locks on two tables
    // at the same time.
    // 2. if the TableChange.RenameTable only change the table name, we need to acquire write lock
    // to on the schema.
    //
    // The read lock on the table does not order this alter against a concurrent load or another
    // alter of the same table, and no tree lock orders them across servers. Either of them can
    // write a stale column list over this alter and turn a column rename into a drop plus an add,
    // which removes the renamed column's tags. See updateColumnsIfNecessaryWhenLoad and #14279.
    NameIdentifier nameIdentifierForLock = ident;
    String schemaName = ident.namespace().level(2);
    Arrays.stream(changes).forEach(TableOperationDispatcher::validateColumnChange);
    for (int i = 0; i < changes.length; i++) {
      WritableDataTypeValidator.validateTableChange(changes[i], "updates[" + i + "]");
    }
    for (TableChange change : changes) {
      if (change instanceof TableChange.RenameTable) {
        TableChange.RenameTable rename = (TableChange.RenameTable) change;
        TableEntity.NAME.validate(rename.getNewName(), TABLE);
        if (rename.getNewSchemaName().isPresent()
            && !rename.getNewSchemaName().get().equals(schemaName)) {
          nameIdentifierForLock = getCatalogIdentifier(ident);
          break;
        }
        nameIdentifierForLock = getSchemaIdentifier(ident);
      }
    }

    return TreeLockUtils.doWithTreeLock(
        nameIdentifierForLock,
        nameIdentifierForLock.equals(ident) ? LockType.READ : LockType.WRITE,
        () -> {
          NameIdentifier catalogIdent = getCatalogIdentifier(ident);
          AlterTableCatalogResult catalogResult =
              doWithCatalog(
                  catalogIdent,
                  catalog -> {
                    validateAlterProperties(
                        catalog, HasPropertyMetadata::tablePropertiesMetadata, changes);
                    boolean managed = isManagedEntity(catalog, Capability.Scope.TABLE);
                    Optional<TableEntity> tableEntityBeforeRename =
                        isRenameTable && !managed
                            ? getTableEntityBeforeRename(ident)
                            : Optional.empty();
                    // TableNormalizeDispatcher already normalized the changes. Normalization can
                    // depend on quoting and must not be repeated on canonical physical names.
                    Table table =
                        catalog.doWithTableOps(tableOps -> tableOps.alterTable(ident, changes));
                    return new AlterTableCatalogResult(
                        snapshotTable(catalog, table, managed),
                        tableEntityBeforeRename,
                        resolveColumnNameChanges(changes));
                  },
                  NoSuchTableException.class,
                  IllegalArgumentException.class);
          Table alteredTable = catalogResult.table;

          // The change is already applied in the catalog. If the result has no column list and
          // Gravitino has no stored columns to report instead, the alter fails, because clients
          // require one. The hook's authorization rename and the success event are then skipped,
          // and a retried rename fails because the table already has its new name.
          if (catalogResult.managed) {
            return checkColumnsReported(
                ident,
                EntityCombinedTable.of(alteredTable)
                    .withHiddenProperties(catalogResult.hiddenProperties));
          }

          StringIdentifier stringId = getStringIdFromProperties(alteredTable.properties());
          // Case 1: The table is not created by Gravitino and this table is never imported.
          TableEntity te = catalogResult.tableEntityBeforeRename.orElse(null);
          if (stringId == null) {
            if (te == null) {
              te = getEntity(ident, TABLE, TableEntity.class);
            }
            if (te == null) {
              return checkColumnsReported(
                  ident,
                  EntityCombinedTable.of(alteredTable)
                      .withHiddenProperties(catalogResult.hiddenProperties));
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
                            Namespace newNamespace = getNewNamespace(ident, changes);

                            // Update the columns
                            Pair<Boolean, List<ColumnEntity>> columnsUpdateResult =
                                updateColumnsIfNecessary(
                                    alteredTable,
                                    tableEntity,
                                    alignNameChangesWithCatalogColumns(
                                        alignNameChangesWithStoredColumns(
                                            catalogResult.columnNameChanges, tableEntity.columns()),
                                        alteredTable.columns()));

                            return TableEntity.builder()
                                .withId(tableEntity.id())
                                .withName(alteredTable.name())
                                .withNamespace(newNamespace)
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

          // operateOnEntity returns null if the store update fails or if the returned entity ID
          // does not match the ID from the external catalog.
          if (isRenameTable && updatedTableEntity == null) {
            NameIdentifier newIdent =
                NameIdentifier.of(getNewNamespace(ident, changes), alteredTable.name());
            throw new GravitinoRuntimeException(
                "Table %s was renamed to %s in the external catalog, but its registration in "
                    + "Gravitino could not be updated consistently",
                ident, newIdent);
          }

          EntityCombinedTable result =
              EntityCombinedTable.of(alteredTable, updatedTableEntity)
                  .withHiddenProperties(catalogResult.hiddenProperties);
          // The store kept the columns it had before this alter (see updateColumnsIfNecessary).
          // Report them, so the alter completes, including the hook's authorization rename,
          // instead of failing after the catalog has already applied it. A column drop or rename
          // in this alter is not reflected in them.
          if (alteredTable.columns() == null && updatedTableEntity != null) {
            LOG.warn(
                "Catalog {} returned altered table {} without a column list, reporting the stored "
                    + "columns",
                getCatalogIdentifier(ident),
                ident);
            result.withColumns(toColumns(updatedTableEntity.columns()));
          }
          return checkColumnsReported(ident, result);
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
    NameIdentifier schemaIdentifier = getSchemaIdentifier(ident);
    Pair<Boolean, Boolean> result =
        TreeLockUtils.doWithTreeLock(
            ident,
            LockType.WRITE,
            () -> {
              NameIdentifier catalogIdent = getCatalogIdentifier(ident);
              boolean droppedFromCatalog =
                  doWithCatalog(
                      catalogIdent,
                      c -> c.doWithTableOps(t -> t.dropTable(ident)),
                      RuntimeException.class);

              boolean isManagedTable = isManagedEntity(catalogIdent, Capability.Scope.TABLE);
              if (isManagedTable) {
                return Pair.of(droppedFromCatalog, isManagedTable);
              }

              // A false result is ambiguous: the external table may have been renamed or dropped
              // out of band. Preserve the registration because deleting it after a rename would
              // lose Gravitino-only metadata. A true out-of-band drop can therefore leave a stale
              // registration that requires separate cleanup.
              if (droppedFromCatalog) {
                try {
                  store.delete(ident, TABLE);
                } catch (OptimisticLockException e) {
                  throw e;
                } catch (NoSuchEntityException e) {
                  LOG.warn("The table to be dropped does not exist in the store: {}", ident, e);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
              return Pair.of(droppedFromCatalog, isManagedTable);
            });
    if (!result.getRight()) {
      // Run even after a false drop result: out-of-band drops can leave orphaned schemas.
      // Release the table lock before cleanup to avoid a lock upgrade.
      cleanUpOrphanedSchema(getCatalogIdentifier(ident), schemaIdentifier);
    }
    return result.getLeft();
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
    NameIdentifier schemaIdentifier = getSchemaIdentifier(ident);
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

          boolean isManagedTable = isManagedEntity(catalogIdent, Capability.Scope.TABLE);
          if (isManagedTable) {
            return droppedFromCatalog;
          }

          // A false result is ambiguous: the external table may have been renamed or dropped out of
          // band. Preserve the registration because deleting it after a rename would lose
          // Gravitino-only metadata. A true out-of-band purge can therefore leave a stale
          // registration that requires separate cleanup.
          if (droppedFromCatalog) {
            try {
              store.delete(ident, TABLE);
            } catch (OptimisticLockException e) {
              throw e;
            } catch (NoSuchEntityException e) {
              LOG.warn("The table to be purged does not exist in the store: {}", ident, e);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
          // Run unconditionally: an out-of-band purge may have left orphaned schema entities. The
          // cleanup is best-effort and stops as soon as a schema still exists.
          OrphanedSchemaCleanup.cleanUp(this, catalogIdent, schemaIdentifier);
          return droppedFromCatalog;
        });
  }

  private Namespace getNewNamespace(NameIdentifier tableIdent, TableChange... changes) {
    String schemaName = tableIdent.namespace().level(2);
    return Arrays.stream(changes)
        .filter(
            c ->
                c instanceof TableChange.RenameTable
                    && ((TableChange.RenameTable) c).getNewSchemaName().isPresent()
                    && !((TableChange.RenameTable) c).getNewSchemaName().get().equals(schemaName))
        .map(
            c ->
                NamespaceUtil.ofTable(
                    tableIdent.namespace().level(0),
                    tableIdent.namespace().level(1),
                    ((TableChange.RenameTable) c).getNewSchemaName().get()))
        .reduce((c1, c2) -> c2)
        .orElse(tableIdent.namespace());
  }

  private Optional<TableEntity> getTableEntityBeforeRename(NameIdentifier ident) {
    try {
      return Optional.of(store.get(ident, TABLE, TableEntity.class));
    } catch (NoSuchEntityException e) {
      return Optional.empty();
    } catch (Exception e) {
      throw new GravitinoRuntimeException(
          e, "Failed to read the stored registration for table %s before renaming it", ident);
    }
  }

  private Table loadAndImportTable(NameIdentifier ident, AuditInfo importAudit)
      throws NoSuchTableException {
    EntityCombinedTable entityCombinedTable =
        TreeLockUtils.doWithTreeLock(
            ident,
            LockType.READ,
            () -> {
              // A cold external table will be loaded under its own WRITE lock below. Do not
              // fetch it here only to discard the snapshot and fetch it again during import.
              // Managed catalogs do not need the external import path, even without a store row.
              if (getEntity(ident, TABLE, TableEntity.class) == null
                  && !isManagedEntity(getCatalogIdentifier(ident), Capability.Scope.TABLE)) {
                return null;
              }
              return internalLoadTable(ident);
            });

    if (entityCombinedTable == null || !entityCombinedTable.imported()) {
      // Load the schema to make sure the schema is imported.
      SchemaDispatcher schemaDispatcher = getSchemaDispatcher();
      NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
      schemaDispatcher.loadSchema(schemaIdent);

      // Import under the table lock: the source snapshot and store write exclude local changes
      // to this table, while sibling tables remain available. Ancestor READ locks still exclude
      // schema drop/rename. Never reuse a snapshot read before acquiring this lock.
      try {
        entityCombinedTable =
            TreeLockUtils.doWithTreeLock(
                ident, LockType.WRITE, () -> importTableFromCatalog(ident, importAudit));
      } catch (EntityAlreadyExistsException e) {
        // HA race: another Gravitino node concurrently imported this table. Reload from the
        // entity store to pick up the entity stored by the winning node.
        LOG.info(
            "Table {} was concurrently imported by another node; reloading from store.", ident);
        entityCombinedTable =
            TreeLockUtils.doWithTreeLock(ident, LockType.READ, () -> internalLoadTable(ident));
        if (!entityCombinedTable.imported()) {
          throw new UnsupportedOperationException(
              "Table managed by multiple catalogs. This may cause unexpected issues such as "
                  + "privilege conflicts. To resolve: Remove all catalogs managing this table, "
                  + "then recreate one catalog to ensure single-catalog management.");
        }
      }
    }

    // Update the column entities in Gravitino store if the columns are different from the ones
    // fetching from the underlying source.
    TableEntity updatedEntity = updateColumnsIfNecessaryWhenLoad(ident, entityCombinedTable);
    updatedEntity =
        fillMissingCreationAudit(
            updatedEntity,
            TableEntity.class,
            entityCombinedTable.tableFromCatalog().auditInfo(),
            importAudit,
            (current, audit) ->
                TableEntity.builder()
                    .withId(current.id())
                    .withName(current.name())
                    .withNamespace(current.namespace())
                    .withComment(current.comment())
                    .withProperties(current.properties())
                    .withColumns(fillMissingColumnCreationAudit(current.columns(), audit))
                    .withPartitioning(current.partitioning())
                    .withDistribution(current.distribution())
                    .withSortOrders(current.sortOrders())
                    .withIndexes(current.indexes())
                    .withAuditInfo(audit)
                    .build());

    return EntityCombinedTable.of(entityCombinedTable.tableFromCatalog(), updatedEntity)
        .withHiddenProperties(entityCombinedTable.hiddenProperties())
        .withImported(
            entityCombinedTable.imported()
                && (entityCombinedTable.tableFromGravitino() == null || updatedEntity != null));
  }

  private static List<ColumnEntity> fillMissingColumnCreationAudit(
      List<ColumnEntity> columns, AuditInfo tableAudit) {
    return columns.stream()
        .map(
            column -> {
              Audit audit = column.auditInfo();
              if (audit.creator() != null) {
                return column;
              }
              return ColumnEntity.builder()
                  .withId(column.id())
                  .withName(column.name())
                  .withPosition(column.position())
                  .withDataType(column.dataType())
                  .withComment(column.comment())
                  .withNullable(column.nullable())
                  .withAutoIncrement(column.autoIncrement())
                  .withDefaultValue(column.defaultValue())
                  .withAuditInfo(
                      AuditInfo.builder()
                          .withCreator(tableAudit.creator())
                          .withCreateTime(
                              audit.createTime() == null
                                  ? tableAudit.createTime()
                                  : audit.createTime())
                          .withLastModifier(audit.lastModifier())
                          .withLastModifiedTime(audit.lastModifiedTime())
                          .build())
                  .build();
            })
        .collect(Collectors.toList());
  }

  private EntityCombinedTable importTableFromCatalog(
      NameIdentifier identifier, AuditInfo importAudit) {
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
    if (audit.creator() == null) {
      audit = audit.merge(importAudit, false /* overwrite */);
    }
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
      // Overwrite only with the id stored in the catalog: it identifies the table, so a row under
      // the same name with another id is stale and gets replaced. A generated id identifies
      // nothing, so a row that appeared meanwhile, e.g. from a concurrent import on another node,
      // must win. The plain insert then conflicts, and loadTable reloads that row.
      store.put(tableEntity, stringId != null);
    } catch (EntityAlreadyExistsException e) {
      throw e;
    } catch (OptimisticLockException e) {
      // A conflict with a concurrent writer, such as a lock wait timeout; the load can be retried.
      throw e;
    } catch (Exception e) {
      LOG.error(FormattedErrorMessages.STORE_OP_FAILURE, "put", identifier, e);
      throw new RuntimeException("Failed to import the table entity to the store", e);
    }

    return EntityCombinedTable.of(table.tableFromCatalog(), tableEntity)
        .withHiddenProperties(table.hiddenProperties());
  }

  private SchemaDispatcher getSchemaDispatcher() {
    SchemaDispatcher schemaDispatcher = schemaDispatcherSupplier.get();
    Preconditions.checkArgument(
        schemaDispatcher != null,
        "schemaDispatcherSupplier returned null. "
            + "SchemaDispatcher must be available for table operations.");
    return schemaDispatcher;
  }

  private EntityCombinedTable internalLoadTable(NameIdentifier ident) {
    NameIdentifier catalogIdentifier = getCatalogIdentifier(ident);
    TableCatalogResult catalogResult =
        doWithCatalog(
            catalogIdentifier,
            catalog -> {
              Table table = catalog.doWithTableOps(tableOps -> tableOps.loadTable(ident));
              return snapshotTable(catalog, table);
            },
            NoSuchTableException.class);
    Table table = catalogResult.table;
    // Fail before importing the table or updating its stored columns.
    checkColumnsReported(ident, table);

    if (catalogResult.managed) {
      return EntityCombinedTable.of(table)
          .withHiddenProperties(catalogResult.hiddenProperties)
          // The metadata of managed table is stored by Gravitino, so it is always imported.
          .withImported(true /* imported */);
    }

    StringIdentifier stringId = getStringIdFromProperties(table.properties());
    // Case 1: The table is not created by Gravitino or the external system does not support storing
    // string identifier.
    if (stringId == null) {
      TableEntity tableEntity = getEntity(ident, TABLE, TableEntity.class);
      if (tableEntity == null) {
        return EntityCombinedTable.of(table)
            .withHiddenProperties(catalogResult.hiddenProperties)
            // Some tables don't have properties or are not created by Gravitino,
            // we can't use stringIdentifier to judge whether schema is ever imported or not.
            // We need to check whether the entity exists.
            .withImported(false);
      }

      return EntityCombinedTable.of(table, tableEntity)
          .withHiddenProperties(catalogResult.hiddenProperties)
          // For some catalogs like PG, the identifier information is not stored in the table's
          // metadata, we need to check if this table exists in the store, if so we don't
          // need to import.
          .withImported(true);
    }

    TableEntity tableEntity =
        operateOnEntity(
            ident,
            identifier -> store.get(identifier, TABLE, TableEntity.class),
            "GET",
            stringId.id());

    return EntityCombinedTable.of(table, tableEntity)
        .withHiddenProperties(catalogResult.hiddenProperties)
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
    CreateTableCatalogResult catalogResult =
        doWithCatalog(
            catalogIdent,
            catalog -> {
              catalog.doWithPropertiesMeta(
                  metadata -> {
                    validatePropertyForCreate(metadata.tablePropertiesMetadata(), properties);
                    return null;
                  });

              long uid = idGenerator.nextId();
              // Add StringIdentifier to the properties, the specific catalog will handle this
              // StringIdentifier to make sure only when the operation is successful, the related
              // TableEntity will be visible.
              StringIdentifier stringId = StringIdentifier.fromId(uid);
              Map<String, String> updatedProperties =
                  StringIdentifier.newPropertiesWithId(stringId, properties);

              // We do not retrieve the table again to obtain values generated by the underlying
              // catalog because some catalog APIs are asynchronous.
              Table table =
                  catalog.doWithTableOps(
                      tableOps ->
                          tableOps.createTable(
                              ident,
                              columns,
                              comment,
                              updatedProperties,
                              partitions == null ? EMPTY_TRANSFORM : partitions,
                              distribution == null ? Distributions.NONE : distribution,
                              sortOrders == null ? new SortOrder[0] : sortOrders,
                              indexes == null ? Indexes.EMPTY_INDEXES : indexes));
              return new CreateTableCatalogResult(snapshotTable(catalog, table), uid);
            },
            NoSuchSchemaException.class,
            TableAlreadyExistsException.class);
    Table table = catalogResult.table;
    // Fail before storing the table, as a missing column list failed the snapshot before.
    checkColumnsReported(ident, table);

    // If the table is managed by Gravitino, we don't need to create TableEntity and store it again.
    if (catalogResult.managed) {
      // ManagedTableOperations persists the server-issued ID on insertion and preserves the old
      // ID when returning an existing table. Inspect the unmasked snapshot: the public properties
      // omit this reserved identifier. Do not infer creation from the presence of an owner.
      StringIdentifier persistedId = StringIdentifier.fromProperties(table.properties());
      EntityCombinedTable result =
          EntityCombinedTable.of(table).withHiddenProperties(catalogResult.hiddenProperties);
      long persistedTableId;
      if (persistedId != null) {
        persistedTableId = persistedId.id();
      } else {
        // Custom managed connectors may omit reserved properties from their returned Table. Read
        // the stored identity in that case; absence or a replacement cannot prove this request
        // created the object. Never require an extra connector read after external creation.
        try {
          persistedTableId = store.get(ident, TABLE, TableEntity.class).id();
        } catch (NoSuchEntityException e) {
          return result;
        } catch (IOException e) {
          throw new GravitinoRuntimeException(
              e,
              "Table %s was returned by the catalog, but its stored creation identity could not be read. "
                  + "Check the entity store before retrying or assigning ownership.",
              ident);
        }
      }
      return persistedTableId == catalogResult.id
          ? result.withCreatedTableId(catalogResult.id)
          : result;
    }

    AuditInfo audit =
        AuditInfo.builder()
            .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
            .withCreateTime(Instant.now())
            .build();
    List<ColumnEntity> columnEntityList =
        toColumnEntities(withCatalogColumnNames(columns, table.columns()), audit);

    TableEntity tableEntity =
        TableEntity.builder()
            .withId(catalogResult.id)
            .withName(ident.name())
            .withNamespace(ident.namespace())
            .withColumns(columnEntityList)
            .withAuditInfo(audit)
            .build();

    try {
      store.put(tableEntity, true /* overwrite */);
    } catch (EntityAlreadyExistsException e) {
      throw new TableAlreadyExistsException(
          e,
          "Table %s was created in the external catalog, but its registration conflicts with an "
              + "existing entity. Verify and reconcile the registered and source identities before "
              + "continuing; loading the table does not resolve this conflict.",
          ident);
    } catch (Exception e) {
      LOG.error(FormattedErrorMessages.STORE_OP_FAILURE, "put", ident, e);
      throw new GravitinoRuntimeException(
          e,
          "Table %s was created in the external catalog, but its registration in Gravitino "
              + "failed. Resolve the entity-store error, verify the source identity, and use "
              + "loadTable to import it. Do not repeat createTable; it will report already exists.",
          ident);
    }

    // Merge both the metadata from catalog operation and the metadata from entity store.
    return EntityCombinedTable.of(table, tableEntity)
        .withCreatedTableId(tableEntity.id())
        .withHiddenProperties(catalogResult.hiddenProperties);
  }

  /**
   * Returns the requested columns under the names the catalog reports for them. A catalog may store
   * a name in another case than requested, and the stored entity has to carry the name the catalog
   * reports on later loads. A column whose name differs by more than case keeps the requested name.
   *
   * @param columns the columns of the create request
   * @param catalogColumns the columns the catalog reports for the created table
   * @return the columns to store
   */
  @VisibleForTesting
  static Column[] withCatalogColumnNames(Column[] columns, Column[] catalogColumns) {
    if (columns.length != catalogColumns.length) {
      return columns;
    }

    Column[] named = columns.clone();
    for (int i = 0; i < columns.length; i++) {
      Column column = columns[i];
      String catalogName = catalogColumns[i].name();
      if (!catalogName.equals(column.name()) && catalogName.equalsIgnoreCase(column.name())) {
        named[i] =
            Column.of(
                catalogName,
                column.dataType(),
                column.comment(),
                column.nullable(),
                column.autoIncrement(),
                column.defaultValue());
      }
    }
    return named;
  }

  private TableCatalogResult snapshotTable(
      CatalogManager.CatalogWrapper catalog, Table table, boolean managed) throws Exception {
    Table snapshot = catalog.detachConnectorResult(table);
    MaskAndOmitKeys hiddenProperties =
        getMaskAndOmitKeys(
            catalog, HasPropertyMetadata::tablePropertiesMetadata, snapshot.properties());
    return new TableCatalogResult(snapshot, managed, hiddenProperties);
  }

  private TableCatalogResult snapshotTable(CatalogManager.CatalogWrapper catalog, Table table)
      throws Exception {
    return snapshotTable(catalog, table, isManagedEntity(catalog, Capability.Scope.TABLE));
  }

  private <T extends Table> T checkColumnsReported(NameIdentifier ident, T table) {
    if (table.columns() == null) {
      throw new IllegalStateException(
          String.format(
              "Catalog %s returned table %s without a column list",
              getCatalogIdentifier(ident), ident));
    }
    return table;
  }

  private static Column[] toColumns(@Nullable List<ColumnEntity> columnEntities) {
    if (columnEntities == null) {
      return new Column[0];
    }
    return columnEntities.stream()
        .sorted(Comparator.comparing(ColumnEntity::position))
        .map(
            c ->
                Column.of(
                    c.name(),
                    c.dataType(),
                    c.comment(),
                    c.nullable(),
                    c.autoIncrement(),
                    c.defaultValue()))
        .toArray(Column[]::new);
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
        && Objects.equal(ColumnEntity.commentForStorage(left.comment()), right.comment())
        && left.nullable() == right.nullable()
        && left.autoIncrement() == right.autoIncrement()
        && isSameDefaultValue(left.defaultValue(), right.defaultValue());
  }

  @VisibleForTesting
  static boolean isSameDefaultValue(@Nullable Expression left, @Nullable Expression right) {
    if (Objects.equal(left, right)) {
      return true;
    }

    // Connector snapshots use DTOs, while relational storage restores API expressions. Compare
    // their persisted representation rather than implementation-specific equals methods.
    try {
      return Objects.equal(toDefaultValueArg(left), toDefaultValueArg(right));
    } catch (IllegalArgumentException e) {
      // A default value that cannot be converted is treated as changed instead of failing the
      // comparison.
      LOG.debug("Failed to compare column default values {} and {}", left, right, e);
      return false;
    }
  }

  @Nullable
  private static FunctionArg toDefaultValueArg(@Nullable Expression expression) {
    return expression == null || expression.equals(Column.DEFAULT_VALUE_NOT_SET)
        ? null
        : DTOConverters.toFunctionArg(expression);
  }

  private String columnDifferenceContent(
      Column catalogColumn, int catalogPosition, ColumnEntity entityColumn) {
    List<String> differences = Lists.newArrayList();
    if (!Objects.equal(catalogColumn.name(), entityColumn.name())) {
      differences.add(
          String.format("name[catalog=%s, store=%s]", catalogColumn.name(), entityColumn.name()));
    }
    if (catalogPosition != entityColumn.position()) {
      differences.add(
          String.format(
              "position[catalog=%s, store=%s]", catalogPosition, entityColumn.position()));
    }
    if (!Objects.equal(catalogColumn.dataType(), entityColumn.dataType())) {
      differences.add(
          String.format(
              "type[catalog=%s, store=%s]", catalogColumn.dataType(), entityColumn.dataType()));
    }
    String storedComment = ColumnEntity.commentForStorage(catalogColumn.comment());
    if (!Objects.equal(storedComment, entityColumn.comment())) {
      differences.add(
          String.format("comment[catalog=%s, store=%s]", storedComment, entityColumn.comment()));
    }
    if (catalogColumn.nullable() != entityColumn.nullable()) {
      differences.add(
          String.format(
              "nullable[catalog=%s, store=%s]", catalogColumn.nullable(), entityColumn.nullable()));
    }
    if (catalogColumn.autoIncrement() != entityColumn.autoIncrement()) {
      differences.add(
          String.format(
              "autoIncrement[catalog=%s, store=%s]",
              catalogColumn.autoIncrement(), entityColumn.autoIncrement()));
    }
    if (!isSameDefaultValue(catalogColumn.defaultValue(), entityColumn.defaultValue())) {
      differences.add(
          String.format(
              "defaultValue[catalog=%s, store=%s]",
              catalogColumn.defaultValue(), entityColumn.defaultValue()));
    }
    return String.join(", ", differences);
  }

  /**
   * Works out what each top-level column that existed before an alter is called after it.
   *
   * <p>Stored columns are matched to the catalog's columns by name, so without this a renamed
   * column would look like a dropped column plus a new one, and lose its id and everything attached
   * to it. The changes are replayed in order, so chained renames resolve to the final name. A
   * column that is not in the returned map keeps its name, and a column mapped to {@code null} was
   * dropped, so a new column that reuses its name is not mistaken for it.
   *
   * @param changes the changes applied to the table, after capability normalization
   * @return the new name of each renamed column, or {@code null} for each dropped column, keyed by
   *     the column's name before the alter
   */
  @VisibleForTesting
  static Map<String, String> resolveColumnNameChanges(TableChange... changes) {
    // Original name -> current name, or null once the original column is dropped.
    Map<String, String> originalToCurrent = new HashMap<>();
    // Current names of columns added by these changes.
    Set<String> addedColumns = new HashSet<>();

    for (TableChange change : changes) {
      if (change instanceof TableChange.AddColumn) {
        String[] fieldName = ((TableChange.AddColumn) change).fieldName();
        if (fieldName.length == 1) {
          addedColumns.add(fieldName[0]);
        }

      } else if (change instanceof TableChange.RenameColumn) {
        TableChange.RenameColumn rename = (TableChange.RenameColumn) change;
        if (rename.fieldName().length != 1) {
          continue;
        }
        String from = rename.fieldName()[0];
        String to = rename.getNewName();
        String original = originalColumnNamed(originalToCurrent, addedColumns, from);
        if (original != null) {
          originalToCurrent.put(original, to);
        } else if (addedColumns.remove(from)) {
          addedColumns.add(to);
        }

      } else if (change instanceof TableChange.DeleteColumn) {
        String[] fieldName = ((TableChange.DeleteColumn) change).fieldName();
        if (fieldName.length != 1) {
          continue;
        }
        String original = originalColumnNamed(originalToCurrent, addedColumns, fieldName[0]);
        if (original != null) {
          originalToCurrent.put(original, null);
        } else {
          addedColumns.remove(fieldName[0]);
        }
      }
    }

    originalToCurrent.entrySet().removeIf(e -> e.getKey().equals(e.getValue()));
    return originalToCurrent;
  }

  /**
   * Keys the column renames and drops by the names of the stored columns.
   *
   * <p>Some catalogs, such as MySQL, match column names case-insensitively while reporting
   * case-sensitive names, so they accept a change whose column name differs in case from the stored
   * one. Such a name is mapped to the stored column it matches when exactly one does, so a rename
   * keeps the column id instead of being stored as a drop plus an add.
   *
   * @param nameChanges the renames and drops returned by {@link #resolveColumnNameChanges}
   * @param storedColumns the columns of the stored table entity, or {@code null} if it has none
   * @return the renames and drops keyed by the stored column names
   */
  @VisibleForTesting
  static Map<String, String> alignNameChangesWithStoredColumns(
      Map<String, String> nameChanges, @Nullable List<ColumnEntity> storedColumns) {
    if (nameChanges.isEmpty() || storedColumns == null) {
      return nameChanges;
    }

    Set<String> storedNames =
        storedColumns.stream().map(ColumnEntity::name).collect(Collectors.toSet());
    Map<String, String> aligned = new HashMap<>();
    for (Map.Entry<String, String> change : nameChanges.entrySet()) {
      String name = change.getKey();
      if (!storedNames.contains(name)) {
        List<String> matches =
            storedNames.stream()
                .filter(stored -> stored.equalsIgnoreCase(change.getKey()))
                .filter(stored -> !nameChanges.containsKey(stored))
                .collect(Collectors.toList());
        if (matches.size() == 1 && !aligned.containsKey(matches.get(0))) {
          name = matches.get(0);
        }
      }
      aligned.put(name, change.getValue());
    }
    return aligned;
  }

  /**
   * Points column renames at the names the catalog reports.
   *
   * <p>A catalog may store a name in another case than requested, such as Glue for Hive-format
   * tables. A rename target the catalog does not report is mapped to the catalog column it matches
   * when exactly one unclaimed column does, so the rename keeps the column id instead of being
   * stored as a drop plus an add.
   *
   * @param nameChanges the renames and drops keyed by the stored column names
   * @param catalogColumns the columns the catalog reports, or {@code null} if it reports none
   * @return the renames and drops with targets the catalog reports
   */
  @VisibleForTesting
  static Map<String, String> alignNameChangesWithCatalogColumns(
      Map<String, String> nameChanges, @Nullable Column[] catalogColumns) {
    if (nameChanges.isEmpty() || catalogColumns == null) {
      return nameChanges;
    }

    Set<String> catalogNames =
        Arrays.stream(catalogColumns).map(Column::name).collect(Collectors.toSet());
    Set<String> claimed = new HashSet<>(nameChanges.values());
    Map<String, String> aligned = new HashMap<>();
    for (Map.Entry<String, String> change : nameChanges.entrySet()) {
      String target = change.getValue();
      if (target != null && !catalogNames.contains(target)) {
        List<String> matches =
            catalogNames.stream()
                .filter(target::equalsIgnoreCase)
                .filter(name -> !claimed.contains(name))
                .collect(Collectors.toList());
        if (matches.size() == 1) {
          target = matches.get(0);
          claimed.add(target);
        }
      }
      aligned.put(change.getKey(), target);
    }
    return aligned;
  }

  /** Returns the original name of the pre-existing column currently called {@code name}. */
  @Nullable
  private static String originalColumnNamed(
      Map<String, String> originalToCurrent, Set<String> addedColumns, String name) {
    for (Map.Entry<String, String> entry : originalToCurrent.entrySet()) {
      if (name.equals(entry.getValue())) {
        return entry.getKey();
      }
    }
    // A name no change has touched yet still belongs to the pre-existing column of that name.
    if (!originalToCurrent.containsKey(name) && !addedColumns.contains(name)) {
      return name;
    }
    return null;
  }

  private Pair<Boolean, List<ColumnEntity>> updateColumnsIfNecessary(
      Table tableFromCatalog, TableEntity tableFromGravitino, Map<String, String> nameChanges) {
    if (tableFromCatalog == null || tableFromGravitino == null) {
      LOG.warn(
          "Cannot update columns for table when altering because table or table entity is "
              + "null");
      return Pair.of(false, Collections.emptyList());
    }

    // A catalog that reports no column list says nothing about the columns, so keep the stored
    // ones. Treating it as "all columns dropped" would delete their tags, policies and owners. Load
    // rejects such a table before getting here, and alter keeps the columns as they were before it.
    if (tableFromCatalog.columns() == null) {
      LOG.warn(
          "Table {} from the underlying source has no column list, keeping the stored columns",
          tableFromCatalog.name());
      return Pair.of(
          false,
          tableFromGravitino.columns() == null
              ? Collections.emptyList()
              : tableFromGravitino.columns());
    }

    Map<String, Pair<Integer, Column>> columnsFromCatalogTable =
        IntStream.range(0, tableFromCatalog.columns().length)
            .mapToObj(i -> Pair.of(i, tableFromCatalog.columns()[i]))
            .collect(Collectors.toMap(p -> p.getRight().name(), Function.identity()));
    Map<String, ColumnEntity> columnsFromTableEntity =
        tableFromGravitino.columns() == null
            ? Collections.emptyMap()
            : tableFromGravitino.columns().stream()
                .collect(Collectors.toMap(ColumnEntity::name, Function.identity()));

    // Check if columns need to be updated in Gravitino store
    List<ColumnEntity> columnsToInsert = Lists.newArrayList();
    Set<String> matchedCatalogColumns = new HashSet<>();
    boolean columnsNeedsUpdate = false;
    // Renamed and dropped columns claim their catalog names first. Otherwise a stale stored column
    // (e.g. dropped outside Gravitino) that has the same name as a rename target could be visited
    // first and take that name, and the renamed column would lose its id.
    List<Map.Entry<String, ColumnEntity>> storedColumns =
        new ArrayList<>(columnsFromTableEntity.entrySet());
    storedColumns.sort(Comparator.comparing(e -> !nameChanges.containsKey(e.getKey())));
    for (Map.Entry<String, ColumnEntity> entry : storedColumns) {
      // Follow renames so the stored column keeps its id under its new name.
      String catalogColumnName =
          nameChanges.containsKey(entry.getKey())
              ? nameChanges.get(entry.getKey())
              : entry.getKey();
      Pair<Integer, Column> columnPair =
          catalogColumnName == null || matchedCatalogColumns.contains(catalogColumnName)
              ? null
              : columnsFromCatalogTable.get(catalogColumnName);
      if (columnPair != null) {
        matchedCatalogColumns.add(catalogColumnName);
      }

      if (columnPair == null) {
        LOG.debug(
            "Column {} is not found in the table from underlying source, it will be removed"
                + " from the table entity",
            entry.getKey());
        columnsNeedsUpdate = true;

      } else if (!isSameColumn(columnPair.getRight(), columnPair.getLeft(), entry.getValue())) {
        // Add debug log to print the difference between the two columns
        LOG.debug(
            "Column {} is found in the table from underlying source, but it is different "
                + "from the one in the table entity, it will be updated, differences: {}",
            entry.getKey(),
            columnDifferenceContent(columnPair.getRight(), columnPair.getLeft(), entry.getValue()));

        Column column = columnPair.getRight();
        ColumnEntity updatedColumnEntity =
            ColumnEntity.builder()
                .withId(entry.getValue().id())
                .withName(column.name())
                .withPosition(columnPair.getLeft())
                .withDataType(column.dataType())
                .withComment(ColumnEntity.commentForStorage(column.comment()))
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
      if (!matchedCatalogColumns.contains(entry.getKey())) {
        LOG.debug(
            "Column {} of table: {} is found in the table from underlying source but not in the table "
                + "entity, it will be added to the table entity",
            entry.getKey(),
            tableFromGravitino.id());
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
            combinedTable.tableFromCatalog(),
            combinedTable.tableFromGravitino(),
            Collections.emptyMap());

    // No need to update the columns
    if (!columnsUpdateResult.getLeft()) {
      return combinedTable.tableFromGravitino();
    }

    // Update the columns in the Gravitino store. The diff above only decides whether a write is
    // needed: it was computed before taking the lock, and a concurrent alter may have changed the
    // stored columns since, e.g. renamed a column while keeping its id. So the columns are matched
    // again against the entity being updated, which is the latest stored one. This narrows the
    // load/alter race but does not close it: if this load read the catalog before a concurrent
    // alter and the store after it, the catalog snapshot is the stale side, and a column that the
    // alter renamed is stored as dropped plus added, so it loses its id and its tags.
    //
    // A tree lock cannot close this, because tree locks are local to one server. Closing it needs
    // the alter to record in the store that it is in progress before it calls the catalog, and
    // the load to read the stored version, operation epoch and in-progress mark before it reads
    // the catalog, skipping the store write while the mark is set. The load may write only if the
    // version and epoch are unchanged and no in-progress mark is set. That needs a new EntityStore
    // API and is tracked in #14279.
    Table tableFromCatalog = combinedTable.tableFromCatalog();
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
                                .withComment(entity.comment())
                                .withProperties(entity.properties())
                                .withColumns(
                                    updateColumnsIfNecessary(
                                            tableFromCatalog, entity, Collections.emptyMap())
                                        .getRight())
                                .withPartitioning(entity.partitioning())
                                .withDistribution(entity.distribution())
                                .withSortOrders(entity.sortOrders())
                                .withIndexes(entity.indexes())
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

  private void cleanUpOrphanedSchema(NameIdentifier catalogIdent, NameIdentifier schemaIdent) {
    try {
      // A queued schema writer also blocks new readers. Avoid queuing one behind another table's
      // source DDL when the schema still exists and there is nothing to clean up.
      boolean schemaExists =
          TreeLockUtils.doWithTreeLock(
              schemaIdent,
              LockType.READ,
              () ->
                  doWithCatalog(
                      catalogIdent,
                      c -> c.doWithSchemaOps(s -> s.schemaExists(schemaIdent)),
                      RuntimeException.class));
      if (schemaExists) {
        return;
      }
      TreeLockUtils.doWithTreeLock(
          schemaIdent,
          LockType.WRITE,
          () -> {
            // Recheck under the write lock before deleting any registration.
            OrphanedSchemaCleanup.cleanUp(this, catalogIdent, schemaIdent);
            return null;
          });
    } catch (Exception e) {
      LOG.warn("Failed to clean up orphaned schema entities starting from {}", schemaIdent, e);
    }
  }

  private static void validateColumns(Column[] columns) {
    for (Column column : columns) {
      ColumnEntity.NAME.validate(column.name(), COLUMN);
      ColumnEntity.validateCommentForWrite(column.name(), column.comment());
    }
  }

  private static void validateColumnChange(TableChange change) {
    if (change instanceof TableChange.AddColumn) {
      TableChange.AddColumn addColumn = (TableChange.AddColumn) change;
      if (addColumn.getFieldName().length == 1) {
        ColumnEntity.NAME.validate(addColumn.getFieldName()[0], COLUMN);
        ColumnEntity.validateCommentForWrite(addColumn.getFieldName()[0], addColumn.getComment());
      }
    } else if (change instanceof TableChange.RenameColumn) {
      TableChange.RenameColumn renameColumn = (TableChange.RenameColumn) change;
      if (renameColumn.getFieldName().length == 1) {
        ColumnEntity.NAME.validate(renameColumn.getNewName(), COLUMN);
      }
    } else if (change instanceof TableChange.UpdateColumnComment) {
      TableChange.UpdateColumnComment updateComment = (TableChange.UpdateColumnComment) change;
      if (updateComment.getFieldName().length == 1) {
        ColumnEntity.validateCommentForWrite(
            updateComment.getFieldName()[0], updateComment.getNewComment());
      }
    }
  }

  private static class TableCatalogResult {

    final Table table;
    final boolean managed;
    final MaskAndOmitKeys hiddenProperties;

    private TableCatalogResult(Table table, boolean managed, MaskAndOmitKeys hiddenProperties) {
      this.table = table;
      this.managed = managed;
      this.hiddenProperties = hiddenProperties;
    }
  }

  private static final class AlterTableCatalogResult extends TableCatalogResult {

    private final Optional<TableEntity> tableEntityBeforeRename;
    private final Map<String, String> columnNameChanges;

    private AlterTableCatalogResult(
        TableCatalogResult tableResult,
        Optional<TableEntity> tableEntityBeforeRename,
        Map<String, String> columnNameChanges) {
      super(tableResult.table, tableResult.managed, tableResult.hiddenProperties);
      this.tableEntityBeforeRename = tableEntityBeforeRename;
      this.columnNameChanges = columnNameChanges;
    }
  }

  private static final class CreateTableCatalogResult extends TableCatalogResult {

    private final long id;

    private CreateTableCatalogResult(TableCatalogResult tableResult, long id) {
      super(tableResult.table, tableResult.managed, tableResult.hiddenProperties);
      this.id = id;
    }
  }
}
