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
package org.apache.gravitino.storage.relational.service;

import static org.apache.gravitino.metrics.source.MetricsSource.GRAVITINO_RELATIONAL_STORE_METRIC_NAME;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.mapper.SecurableObjectMapper;
import org.apache.gravitino.storage.relational.mapper.StatisticMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableVersionMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.po.ColumnPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;

/** The service class for table metadata. It provides the basic database operations for table. */
public class TableMetaService {

  private static final TableMetaService INSTANCE = new TableMetaService();
  private BasePOStorageOps<TablePO, TableMetaMapper> ops;

  public static TableMetaService getInstance() {
    return INSTANCE;
  }

  private TableMetaService() {
    this.ops = new HierarchicalConversionPOStorageOps<>(new TablePOStorageOps());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getTableIdBySchemaIdAndName")
  public Long getTableIdBySchemaIdAndName(Long schemaId, String tableName) {
    TablePO tablePO =
        SessionUtils.getWithoutCommit(
            TableMetaMapper.class, mapper -> ops.getPO(mapper, schemaId, tableName));

    if (tablePO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.TABLE.name().toLowerCase(),
          tableName);
    }
    return tablePO.getTableId();
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getTableByIdentifier")
  public TableEntity getTableByIdentifier(NameIdentifier identifier) {
    TablePO tablePO = getTablePOByIdentifier(identifier);

    List<ColumnPO> columnPOs =
        TableColumnMetaService.getInstance()
            .getColumnsByTableIdAndVersion(tablePO.getTableId(), tablePO.getCurrentVersion());

    return POConverters.fromTableAndColumnPOs(tablePO, columnPOs, identifier.namespace());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listTablesByNamespace")
  public List<TableEntity> listTablesByNamespace(Namespace namespace) {
    NamespaceUtil.checkTable(namespace);

    List<TablePO> tablePOs = listTablePOs(namespace);
    return POConverters.fromTablePOs(tablePOs, namespace);
  }

  @Monitored(metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME, baseMetricName = "insertTable")
  public void insertTable(TableEntity tableEntity, boolean overwrite) throws IOException {
    try {
      NameIdentifierUtil.checkTable(tableEntity.nameIdentifier());

      TablePO.Builder builder = TablePO.builder();
      fillTablePOBuilderParentEntityId(builder, tableEntity.namespace());

      TablePO po = POConverters.initializeTablePOWithVersion(tableEntity, builder);
      AtomicReference<TablePO> persistedPO = new AtomicReference<>(po);
      // The schema lock, table row, version row, and columns share one transaction. If any later
      // step fails, the earlier inserts are rolled back as well.
      SchemaMetaService.getInstance()
          .doWithSchemaWriteLock(
              tableEntity.nameIdentifier(),
              po.getSchemaId(),
              po.getCatalogId(),
              po.getMetalakeId(),
              () ->
                  SessionUtils.doWithoutCommit(
                      TableMetaMapper.class,
                      mapper -> {
                        ops.insertPO(mapper, po, overwrite);
                        if (overwrite) {
                          // MySQL may preserve the existing table ID during an upsert. Read the
                          // stored identity and database-generated version while the row is locked.
                          TablePO storedPO =
                              mapper.selectTableMetaBySchemaIdAndName(
                                  po.getSchemaId(), po.getTableName());
                          Preconditions.checkState(
                              storedPO != null,
                              "The overwritten table %s in schema %s does not exist",
                              po.getTableName(),
                              po.getSchemaId());
                          persistedPO.set(tablePOWithPersistedIdentityAndVersions(po, storedPO));
                        }
                      }),
              () ->
                  SessionUtils.doWithoutCommit(
                      TableVersionMapper.class,
                      mapper -> {
                        if (overwrite) {
                          TablePO storedPO = persistedPO.get();
                          // An existing table advances from N to N + 1 during the upsert, so retire
                          // N before recording the new current version. A new table has no N row.
                          if (storedPO.getCurrentVersion() > POConverters.INIT_VERSION) {
                            mapper.softDeleteTableVersionByTableIdAndVersion(
                                storedPO.getTableId(), storedPO.getCurrentVersion() - 1);
                          }
                          mapper.insertTableVersionOnDuplicateKeyUpdate(storedPO);
                        } else {
                          mapper.insertTableVersion(po);
                        }
                      }),
              () -> {
                // We need to delete the columns first if we want to overwrite the table.
                if (overwrite) {
                  TableColumnMetaService.getInstance()
                      .deleteColumnsByTableId(persistedPO.get().getTableId());
                }
              },
              () -> {
                if (tableEntity.columns() != null && !tableEntity.columns().isEmpty()) {
                  TableColumnMetaService.getInstance()
                      .insertColumnPOs(persistedPO.get(), tableEntity.columns());
                }
              });

    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.TABLE, tableEntity.nameIdentifier().toString());
      throw re;
    }
  }

  @Monitored(metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME, baseMetricName = "updateTable")
  public <E extends Entity & HasIdentifier> TableEntity updateTable(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
    TablePO oldTablePO = getTablePOByIdentifier(identifier);
    List<ColumnPO> oldTableColumns =
        TableColumnMetaService.getInstance()
            .getColumnsByTableIdAndVersion(oldTablePO.getTableId(), oldTablePO.getCurrentVersion());
    TableEntity oldTableEntity =
        POConverters.fromTableAndColumnPOs(oldTablePO, oldTableColumns, identifier.namespace());

    TableEntity newTableEntity = (TableEntity) updater.apply((E) oldTableEntity);
    Preconditions.checkArgument(
        Objects.equals(oldTableEntity.id(), newTableEntity.id()),
        "The updated table entity id: %s should be same with the table entity id before: %s",
        newTableEntity.id(),
        oldTableEntity.id());

    boolean isSchemaChanged = !newTableEntity.namespace().equals(oldTableEntity.namespace());
    Long newSchemaId =
        isSchemaChanged
            ? EntityIdService.getEntityId(
                NameIdentifier.of(newTableEntity.namespace().levels()), Entity.EntityType.SCHEMA)
            : oldTablePO.getSchemaId();

    TablePO newTablePO =
        POConverters.updateTablePOWithVersionAndSchemaId(oldTablePO, newTableEntity, newSchemaId);

    try {
      // For a cross-schema rename, the new schema is the parent that must remain alive. For a
      // regular update, newSchemaId is the existing parent, so the same entry point covers both.
      SchemaMetaService.getInstance()
          .doWithSchemaWriteLock(
              newTableEntity.nameIdentifier(),
              newSchemaId,
              oldTablePO.getCatalogId(),
              oldTablePO.getMetalakeId(),
              () -> {
                // current_version is the table's OCC token. A zero-row update means another
                // writer won, so stop before touching version history or columns.
                int updated =
                    SessionUtils.getWithoutCommit(
                        TableMetaMapper.class,
                        mapper -> ops.updatePO(mapper, newTablePO, oldTablePO));
                if (updated == 0) {
                  throw tableWriteFailure(identifier, oldTablePO);
                }
              },
              () ->
                  SessionUtils.doWithoutCommit(
                      TableVersionMapper.class,
                      mapper -> {
                        // Only the CAS winner can reach this step, so it is safe to replace the
                        // details stored under the next table version.
                        mapper.softDeleteTableVersionByTableIdAndVersion(
                            oldTablePO.getTableId(), oldTablePO.getCurrentVersion());
                        mapper.insertTableVersionOnDuplicateKeyUpdate(newTablePO);
                      }),
              () -> {
                // A column failure rolls back the table row and version row in the same
                // transaction.
                TableColumnMetaService.getInstance()
                    .updateColumnPOsFromTableDiff(oldTableEntity, newTableEntity, newTablePO);
              });

    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.TABLE, newTableEntity.nameIdentifier().toString());
      throw re;
    }

    return newTableEntity;
  }

  @Monitored(metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME, baseMetricName = "deleteTable")
  public boolean deleteTable(NameIdentifier identifier) {
    TablePO tablePO = getTablePOByIdentifier(identifier);

    // Delete the table row first and only if it still has the version we read. A stale drop stops
    // there, before it can remove columns, tags, policies, or any other related data.
    SessionUtils.doMultipleWithCommit(
        () -> deleteTableWithVersion(identifier, tablePO), () -> deleteTableDependents(tablePO));

    return true;
  }

  /**
   * Deletes the table root row only when it still has the version observed by the caller.
   *
   * <p>This method deliberately does not start or commit a transaction. Its caller must include it
   * in the same transaction as dependent-row cleanup, so a later cleanup failure restores the root
   * row too. Package-private access also lets concurrency tests submit a deliberately stale
   * snapshot without copying the production CAS logic.
   */
  void deleteTableWithVersion(NameIdentifier identifier, TablePO observedTablePO) {
    int deleted =
        SessionUtils.getWithoutCommit(
            TableMetaMapper.class,
            mapper ->
                mapper.softDeleteTableMetasByTableId(
                    observedTablePO.getTableId(), observedTablePO.getCurrentVersion()));
    if (deleted == 0) {
      throw tableWriteFailure(identifier, observedTablePO);
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteTableMetasByLegacyTimeline")
  public int deleteTableMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    return SessionUtils.doWithCommitAndFetchResult(
            TableMetaMapper.class,
            mapper -> mapper.deleteTableMetasByLegacyTimeline(legacyTimeline, limit))
        + deleteTableVersionByLegacyTimeline(legacyTimeline, limit);
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteTableVersionByLegacyTimeline")
  public int deleteTableVersionByLegacyTimeline(Long legacyTimeline, int limit) {
    return SessionUtils.doWithCommitAndFetchResult(
        TableVersionMapper.class,
        mapper -> mapper.deleteTableVersionByLegacyTimeline(legacyTimeline, limit));
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "batchGetTableByIdentifier")
  public List<TableEntity> batchGetTableByIdentifier(List<NameIdentifier> identifiers) {
    NameIdentifier firstIdent = identifiers.get(0);
    NameIdentifier schemaIdent = NameIdentifierUtil.getSchemaIdentifier(firstIdent);
    List<String> tableNames = new ArrayList<>(identifiers.size());
    tableNames.add(identifiers.get(0).name());
    for (int i = 1; i < identifiers.size(); i++) {
      NameIdentifier ident = identifiers.get(i);
      Preconditions.checkArgument(
          Objects.equals(schemaIdent, NameIdentifierUtil.getSchemaIdentifier(ident)));
      tableNames.add(ident.name());
    }
    return SessionUtils.doWithCommitAndFetchResult(
        TableMetaMapper.class,
        mapper -> {
          List<TablePO> tableList = ops.listPOs(mapper, firstIdent.namespace(), tableNames);
          return POConverters.fromTablePOs(tableList, firstIdent.namespace());
        });
  }

  public BasePOStorageOps<TablePO, TableMetaMapper> ops() {
    return ops;
  }

  private TablePO getTablePOByIdentifier(NameIdentifier identifier) {
    NameIdentifierUtil.checkTable(identifier);
    TablePO tablePO =
        SessionUtils.getWithoutCommit(
            TableMetaMapper.class,
            mapper -> POStorageReadRouting.getPO(mapper, identifier, ops, Entity.EntityType.TABLE));
    if (tablePO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.TABLE.name().toLowerCase(),
          identifier.name());
    }

    return tablePO;
  }

  private List<TablePO> listTablePOs(Namespace namespace) {
    return SessionUtils.getWithoutCommit(
        TableMetaMapper.class,
        mapper -> POStorageReadRouting.listPOs(mapper, namespace, ops, Entity.EntityType.TABLE));
  }

  private void fillTablePOBuilderParentEntityId(TablePO.Builder builder, Namespace namespace) {
    NamespaceUtil.checkTable(namespace);
    NamespacedEntityId namespacedEntityId =
        EntityIdService.getEntityIds(
            NameIdentifier.of(namespace.levels()), Entity.EntityType.SCHEMA);
    builder.withMetalakeId(namespacedEntityId.namespaceIds()[0]);
    builder.withCatalogId(namespacedEntityId.namespaceIds()[1]);
    builder.withSchemaId(namespacedEntityId.entityId());
  }

  private TablePO tablePOWithPersistedIdentityAndVersions(TablePO incomingPO, TablePO persistedPO) {
    // The upsert derives the version inside the database and may preserve an existing table ID, so
    // its dependent rows must carry the identity and versions the database ended up with.
    return TablePO.builder(incomingPO)
        .withTableId(persistedPO.getTableId())
        .withCurrentVersion(persistedPO.getCurrentVersion())
        .withLastVersion(persistedPO.getLastVersion())
        .build();
  }

  private void deleteTableDependents(TablePO tablePO) {
    // The table row has already passed its version check. All cleanup below uses the same database
    // transaction, so either the table and every related row are deleted together, or none are.
    SessionUtils.doWithoutCommit(
        OwnerMetaMapper.class,
        mapper ->
            mapper.softDeleteOwnerRelByMetadataObjectIdAndType(
                tablePO.getTableId(), MetadataObject.Type.TABLE.name()));
    TableColumnMetaService.getInstance().deleteColumnsByTableId(tablePO.getTableId());
    SessionUtils.doWithoutCommit(
        SecurableObjectMapper.class,
        mapper ->
            mapper.softDeleteObjectRelsByMetadataObject(
                tablePO.getTableId(), MetadataObject.Type.TABLE.name()));
    SessionUtils.doWithoutCommit(
        TagMetadataObjectRelMapper.class,
        mapper ->
            mapper.softDeleteTagMetadataObjectRelsByMetadataObject(
                tablePO.getTableId(), MetadataObject.Type.TABLE.name()));
    SessionUtils.doWithoutCommit(
        TagMetadataObjectRelMapper.class,
        mapper -> mapper.softDeleteTagMetadataObjectRelsByTableId(tablePO.getTableId()));
    SessionUtils.doWithoutCommit(
        StatisticMetaMapper.class,
        mapper -> mapper.softDeleteStatisticsByEntityId(tablePO.getTableId()));
    SessionUtils.doWithoutCommit(
        PolicyMetadataObjectRelMapper.class,
        mapper -> mapper.softDeletePolicyMetadataObjectRelsByTableId(tablePO.getTableId()));
    SessionUtils.doWithoutCommit(
        TableVersionMapper.class,
        mapper ->
            mapper.softDeleteTableVersionByTableIdAndVersion(
                tablePO.getTableId(), tablePO.getCurrentVersion()));
  }

  private RuntimeException tableWriteFailure(NameIdentifier identifier, TablePO observedTablePO) {
    // A zero-row CAS has two different meanings:
    // 1. The same table is still here, but another writer changed its version. The caller should
    //    retry, so return OptimisticLockException.
    // 2. The table ID was deleted, renamed, or moved away from the requested name. From the
    //    caller's point of view the requested table no longer exists, so return NoSuchEntity.
    //
    // Read by the stable table ID and lock the row. The lock waits for an in-flight writer to
    // finish, which lets us classify the failure using committed data instead of guessing while
    // the other transaction is still running.
    TablePO currentTablePO =
        SessionUtils.getWithoutCommit(
            TableMetaMapper.class,
            mapper -> mapper.selectTableMetaByIdForUpdate(observedTablePO.getTableId()));
    if (currentTablePO == null
        || !Objects.equals(currentTablePO.getTableName(), observedTablePO.getTableName())
        || !Objects.equals(currentTablePO.getSchemaId(), observedTablePO.getSchemaId())
        || !Objects.equals(currentTablePO.getCatalogId(), observedTablePO.getCatalogId())
        || !Objects.equals(currentTablePO.getMetalakeId(), observedTablePO.getMetalakeId())) {
      return new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.TABLE.name().toLowerCase(),
          identifier.name());
    }
    return ExceptionUtils.concurrentModification(Entity.EntityType.TABLE, identifier);
  }
}
