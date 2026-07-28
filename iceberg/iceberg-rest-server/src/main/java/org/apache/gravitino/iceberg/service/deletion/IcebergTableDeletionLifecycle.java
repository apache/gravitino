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

import java.util.Collections;
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
import org.apache.iceberg.exceptions.NoSuchTableException;

/** Transactional Iceberg REST table deletion lifecycle. */
public class IcebergTableDeletionLifecycle {

  /** Persisted lifecycle state for a retained table. */
  public static final String DELETED = "DELETED";

  private final boolean available;
  private final boolean softDeleteEnabled;
  private final long retentionMs;

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
    Objects.requireNonNull(config, "config must not be null");
    this.available = available;
    this.softDeleteEnabled = config.get(IcebergConfig.SOFT_DELETE_ENABLED);
    this.retentionMs = config.get(IcebergConfig.SOFT_DELETE_RETENTION_MS);
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

  /** Returns the retained root and action occupying one routed table name, if complete. */
  @Nullable
  public IcebergRetainedTableDeletion findActive(String catalogName, TableIdentifier identifier) {
    TablePO table = findRetainedTable(catalogName, identifier);
    return table == null ? null : join(table);
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
}
