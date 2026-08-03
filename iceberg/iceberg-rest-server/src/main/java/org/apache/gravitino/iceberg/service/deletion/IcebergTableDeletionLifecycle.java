/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.iceberg.service.deletion;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.utils.IcebergIdentifierUtils;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.po.EntityDeletionPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.apache.gravitino.storage.relational.service.EntityDeletionService;
import org.apache.gravitino.storage.relational.service.EntityIdService;
import org.apache.gravitino.storage.relational.service.TableDeletionService;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;

/** Transactional Iceberg REST table deletion lifecycle. */
public class IcebergTableDeletionLifecycle {

  /** Persisted lifecycle state for a retained table. */
  public static final String DELETED = "DELETED";

  /** Irreversible cleanup-owned lifecycle state. */
  public static final String PURGING = "PURGING";

  private final boolean available;
  private final boolean softDeleteEnabled;
  private final long retentionMs;
  private final IcebergTableCacheInvalidator cacheInvalidator;

  /**
   * Creates an available lifecycle coordinator.
   *
   * @param config Iceberg REST configuration
   */
  public IcebergTableDeletionLifecycle(IcebergConfig config) {
    this(config, true);
  }

  /**
   * Creates a lifecycle coordinator with explicit relational-storage availability.
   *
   * @param config Iceberg REST configuration
   * @param available whether the shared relational metadata store is available
   */
  public IcebergTableDeletionLifecycle(IcebergConfig config, boolean available) {
    this(config, available, new IcebergTableCacheInvalidator());
  }

  IcebergTableDeletionLifecycle(
      IcebergConfig config, boolean available, IcebergTableCacheInvalidator cacheInvalidator) {
    Objects.requireNonNull(config, "config must not be null");
    this.available = available;
    this.softDeleteEnabled = config.get(IcebergConfig.SOFT_DELETE_ENABLED);
    this.retentionMs = config.get(IcebergConfig.SOFT_DELETE_RETENTION_MS);
    this.cacheInvalidator =
        Objects.requireNonNull(cacheInvalidator, "cacheInvalidator must not be null");
  }

  /**
   * Returns whether soft delete uses the durable lifecycle instead of the legacy drop path.
   *
   * @param purgeRequested original Iceberg REST purge flag, retained for compatibility
   * @return whether the lifecycle owns the request
   */
  public boolean manages(boolean purgeRequested) {
    return available && softDeleteEnabled;
  }

  /**
   * Retains and hides one live table in a single relational transaction.
   *
   * <p>This method never unregisters the table and never deletes files.
   *
   * @param context request context
   * @param identifier Iceberg table identifier
   * @param purgeRequested original Iceberg REST purge flag
   */
  public void delete(
      IcebergRequestContext context, TableIdentifier identifier, boolean purgeRequested) {
    if (!manages(purgeRequested)) {
      throw new IllegalStateException("The Iceberg table deletion lifecycle is not enabled");
    }

    String metalake = IcebergRESTServerContext.getInstance().metalakeName();
    NameIdentifier gravitinoIdentifier =
        IcebergIdentifierUtils.toGravitinoTableIdentifier(
            metalake, context.catalogName(), identifier, HierarchicalSchemaUtil.schemaSeparator());
    long deletedAt = System.currentTimeMillis();
    EntityDeletionPO deletion =
        EntityDeletionPO.builder()
            .deletionId(UUID.randomUUID().toString())
            .state(DELETED)
            .retentionExpiresAt(Math.addExact(deletedAt, retentionMs))
            .build();

    try {
      TablePO table = TableDeletionService.getInstance().getLiveTable(gravitinoIdentifier);
      SessionUtils.doMultipleWithCommit(
          () -> TableDeletionService.getInstance().delete(table, deletedAt, deletion),
          () -> appendChange(metalake, gravitinoIdentifier, OperateType.DROP));
    } catch (NoSuchEntityException | IllegalStateException e) {
      if (findRetainedTable(context.catalogName(), identifier) == null) {
        if (e instanceof NoSuchEntityException) {
          throw new NoSuchTableException("Table does not exist: %s", identifier);
        }
        throw e;
      }
    }
    cacheInvalidator.invalidate(gravitinoIdentifier);
  }

  /** Returns whether an exact schema ID and table name is occupied by a retained root. */
  public boolean isNameReserved(String catalogName, TableIdentifier identifier) {
    return findRetainedTable(catalogName, identifier) != null;
  }

  /** Returns all table names occupied by retained roots in one namespace. */
  public Set<String> reservedTableNames(String catalogName, Namespace namespace) {
    if (!available) {
      return Collections.emptySet();
    }
    String metalake = IcebergRESTServerContext.getInstance().metalakeName();
    try {
      return TableDeletionService.getInstance()
          .getReservedTableNames(schemaId(metalake, catalogName, namespace));
    } catch (NoSuchEntityException e) {
      return Collections.emptySet();
    }
  }

  /** Returns retained table roots in one namespace. */
  public List<TablePO> listDeleted(String catalogName, Namespace namespace) {
    if (!available) {
      return new ArrayList<>();
    }
    String metalake = IcebergRESTServerContext.getInstance().metalakeName();
    try {
      return TableDeletionService.getInstance()
          .listRetainedTables(schemaId(metalake, catalogName, namespace));
    } catch (NoSuchEntityException e) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
  }

  /** Returns the retained root and action occupying one routed table name, if complete. */
  @Nullable
  public IcebergRetainedTableDeletion findActive(String catalogName, TableIdentifier identifier) {
    TablePO table = findRetainedTable(catalogName, identifier);
    return table == null ? null : join(table);
  }

  /**
   * Loads the retained root and action for one exact routed table name.
   *
   * @param catalogName routed catalog name
   * @param identifier routed table identifier
   * @return retained table root and deletion action
   */
  public IcebergRetainedTableDeletion getDeleted(String catalogName, TableIdentifier identifier) {
    IcebergRetainedTableDeletion retained = findActive(catalogName, identifier);
    if (retained == null) {
      throw IcebergDeletionException.notFound();
    }
    return retained;
  }

  /**
   * Loads one internal deletion ID and verifies that its retained root matches the routed table.
   *
   * @param catalogName routed catalog name
   * @param identifier routed table identifier
   * @param deletionId internal deletion identifier
   * @return retained table root and deletion action
   */
  public IcebergRetainedTableDeletion getUndropAction(
      String catalogName, TableIdentifier identifier, String deletionId) {
    if (!available || deletionId == null || deletionId.trim().isEmpty()) {
      throw IcebergDeletionException.notFound();
    }
    EntityDeletionPO deletion = EntityDeletionService.getInstance().get(deletionId);
    TablePO table = TableDeletionService.getInstance().getRetainedTable(deletionId);
    if (deletion == null || table == null || !routeMatches(table, catalogName, identifier)) {
      throw IcebergDeletionException.notFound();
    }
    return IcebergRetainedTableDeletion.builder().table(table).deletion(deletion).build();
  }

  /**
   * Reactivates the exact internal deletion generation currently routed by a table name.
   *
   * @param context request context
   * @param identifier exact routed table identifier
   * @param deletionId internal deletion identifier resolved before authorization
   */
  public void undrop(IcebergRequestContext context, TableIdentifier identifier, String deletionId) {
    undropInternal(context, identifier, deletionId, null);
  }

  /**
   * Reactivates the exact deletion generation authorized for an immutable source table ID.
   *
   * @param context request context
   * @param identifier exact routed table identifier
   * @param deletionId exact deletion generation authorized for this request
   * @param expectedTableId immutable source table ID authorized for this request
   */
  public void undrop(
      IcebergRequestContext context,
      TableIdentifier identifier,
      String deletionId,
      long expectedTableId) {
    undropInternal(context, identifier, deletionId, expectedTableId);
  }

  private void undropInternal(
      IcebergRequestContext context,
      TableIdentifier identifier,
      String deletionId,
      @Nullable Long expectedTableId) {
    if (!available) {
      throw IcebergDeletionException.notFound();
    }

    IcebergRetainedTableDeletion retained =
        getUndropAction(context.catalogName(), identifier, deletionId);
    if (expectedTableId != null
        && retained.getTable().getTableId().longValue() != expectedTableId.longValue()) {
      throw IcebergDeletionException.notFound();
    }
    validateRecoverable(retained.getDeletion(), System.currentTimeMillis());

    String metalake = IcebergRESTServerContext.getInstance().metalakeName();
    NameIdentifier gravitinoIdentifier =
        IcebergIdentifierUtils.toGravitinoTableIdentifier(
            metalake, context.catalogName(), identifier, HierarchicalSchemaUtil.schemaSeparator());
    try {
      SessionUtils.doMultipleWithCommit(
          () -> TableDeletionService.getInstance().restore(deletionId),
          () -> appendChange(metalake, gravitinoIdentifier, OperateType.ALTER));
    } catch (IllegalStateException e) {
      EntityDeletionPO current = EntityDeletionService.getInstance().get(deletionId);
      if (current != null && !isRecoverable(current, System.currentTimeMillis())) {
        throw failure(
            IcebergDeletionException.Outcome.GONE,
            "Deletion action has crossed the UNDROP boundary");
      }
      throw failure(
          IcebergDeletionException.Outcome.CONFLICT, "Table generation cannot be restored");
    }
    cacheInvalidator.invalidate(gravitinoIdentifier);
  }

  @Nullable
  private TablePO findRetainedTable(String catalogName, TableIdentifier identifier) {
    if (!available) {
      return null;
    }
    String metalake = IcebergRESTServerContext.getInstance().metalakeName();
    try {
      long schemaId = schemaId(metalake, catalogName, identifier.namespace());
      return TableDeletionService.getInstance().getRetainedTable(schemaId, identifier.name());
    } catch (NoSuchEntityException e) {
      return null;
    }
  }

  @Nullable
  private static IcebergRetainedTableDeletion join(TablePO table) {
    EntityDeletionPO deletion = EntityDeletionService.getInstance().get(table.getDeletionId());
    return deletion == null
        ? null
        : IcebergRetainedTableDeletion.builder().table(table).deletion(deletion).build();
  }

  private boolean routeMatches(TablePO table, String catalogName, TableIdentifier identifier) {
    String metalake = IcebergRESTServerContext.getInstance().metalakeName();
    try {
      return Objects.equals(
              table.getSchemaId(), schemaId(metalake, catalogName, identifier.namespace()))
          && Objects.equals(table.getTableName(), identifier.name());
    } catch (NoSuchEntityException e) {
      return false;
    }
  }

  private static boolean isRecoverable(EntityDeletionPO deletion, long serverNow) {
    return DELETED.equals(deletion.getState())
        && deletion.getRetentionExpiresAt() != null
        && deletion.getRetentionExpiresAt() > serverNow
        && deletion.getPurgeJobId() == null;
  }

  private static void validateRecoverable(EntityDeletionPO deletion, long serverNow) {
    if (PURGING.equals(deletion.getState())
        || deletion.getPurgeJobId() != null
        || deletion.getRetentionExpiresAt() == null
        || deletion.getRetentionExpiresAt() <= serverNow) {
      throw failure(
          IcebergDeletionException.Outcome.GONE, "Deletion action has crossed the UNDROP boundary");
    }
    if (!DELETED.equals(deletion.getState())) {
      throw failure(
          IcebergDeletionException.Outcome.CONFLICT, "Deletion action is not recoverable");
    }
  }

  private static void appendChange(
      String metalake, NameIdentifier identifier, OperateType operateType) {
    SessionUtils.doWithoutCommit(
        EntityChangeLogMapper.class,
        mapper ->
            mapper.insertEntityChange(
                metalake, Entity.EntityType.TABLE.name(), identifier.toString(), operateType));
  }

  private static long schemaId(String metalake, String catalogName, Namespace namespace) {
    String schemaName =
        IcebergIdentifierUtils.icebergNamespaceToSchemaName(
            namespace, HierarchicalSchemaUtil.schemaSeparator());
    return EntityIdService.getEntityId(
        NameIdentifier.of(metalake, catalogName, schemaName), Entity.EntityType.SCHEMA);
  }

  private static IcebergDeletionException failure(
      IcebergDeletionException.Outcome outcome, String message) {
    return new IcebergDeletionException(outcome, message);
  }
}
