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
package org.apache.gravitino.iceberg.service.cleanup;

import java.util.Objects;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.po.CatalogPO;
import org.apache.gravitino.storage.relational.po.EntityDeletionPO;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.service.EntityDeletionService;
import org.apache.gravitino.storage.relational.service.TableDeletionService;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;

/** Removes the Iceberg registration owned by one retained table-deletion cleanup job. */
public class IcebergRetainedDeletionRegistrationCleaner {

  private static final String PURGING = "PURGING";

  private final IcebergCleanupJobStore cleanupJobStore;
  private final IcebergCatalogWrapperManager catalogWrapperManager;

  /**
   * Creates a retained-deletion registration cleaner.
   *
   * @param cleanupJobStore durable cleanup-job store
   * @param catalogWrapperManager Iceberg catalog wrapper manager
   */
  public IcebergRetainedDeletionRegistrationCleaner(
      IcebergCleanupJobStore cleanupJobStore, IcebergCatalogWrapperManager catalogWrapperManager) {
    this.cleanupJobStore =
        Objects.requireNonNull(cleanupJobStore, "cleanupJobStore must not be null");
    this.catalogWrapperManager =
        Objects.requireNonNull(catalogWrapperManager, "catalogWrapperManager must not be null");
  }

  /**
   * Removes the catalog registration reserved by the retained deletion.
   *
   * <p>The retained table row reserves its schema-id/table-name route through {@code PURGING}, so a
   * different Gravitino table cannot occupy it. This method revalidates the exact job, action, and
   * retained table identities before making the metadata-only external call. Catalog and schema
   * names are resolved from their immutable ids so parent renames do not redirect cleanup. A
   * missing registration is already complete.
   *
   * @param job claimed cleanup job linked to a retained deletion
   */
  public void removeRegistration(IcebergCleanupJob job) {
    RegistrationTarget target = validateTarget(job);
    CatalogWrapperForREST wrapper;
    try {
      wrapper = catalogWrapperManager.getCatalogWrapper(target.catalogName);
    } catch (RuntimeException failure) {
      throw externalFailure(job.id());
    }

    try {
      wrapper.dropTable(target.identifier);
    } catch (NoSuchTableException alreadyRemoved) {
      // A prior attempt may have removed the registration before losing its response.
    } catch (RuntimeException failure) {
      throw externalFailure(job.id());
    }
  }

  private RegistrationTarget validateTarget(IcebergCleanupJob job) {
    Objects.requireNonNull(job, "job must not be null");
    if (job.id() <= 0 || job.tableId() == null || job.deletionId() == null) {
      throw invalidLink(job.id());
    }

    IcebergCleanupJobStatus status = cleanupJobStore.getStatus(job.id()).orElse(null);
    EntityDeletionPO deletion = EntityDeletionService.getInstance().get(job.deletionId());
    TablePO table = TableDeletionService.getInstance().getRetainedTable(job.deletionId());
    if (status == null
        || status.state() != IcebergCleanupJob.State.RUNNING
        || deletion == null
        || !PURGING.equals(deletion.getState())
        || !Long.toString(job.id()).equals(deletion.getPurgeJobId())
        || table == null
        || !Objects.equals(job.tableId(), table.getTableId())
        || !Objects.equals(job.deletionId(), table.getDeletionId())
        || job.catalogId() != table.getCatalogId()
        || !Objects.equals(job.tableName(), table.getTableName())
        || table.getDeletedAt() == null
        || table.getDeletedAt() <= 0) {
      throw invalidLink(job.id());
    }

    CatalogPO catalog =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class, mapper -> mapper.selectCatalogMetaById(table.getCatalogId()));
    SchemaPO schema =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class, mapper -> mapper.selectSchemaMetaById(table.getSchemaId()));
    if (catalog == null
        || schema == null
        || !Objects.equals(catalog.getCatalogId(), table.getCatalogId())
        || !Objects.equals(catalog.getMetalakeId(), table.getMetalakeId())
        || !Objects.equals(schema.getSchemaId(), table.getSchemaId())
        || !Objects.equals(schema.getCatalogId(), table.getCatalogId())
        || !Objects.equals(schema.getMetalakeId(), table.getMetalakeId())
        || !isLive(catalog.getDeletedAt())
        || !isLive(schema.getDeletedAt())) {
      throw new IllegalStateException(
          "Cleanup job " + job.id() + " cannot resolve its retained table route");
    }

    String[] namespaceLevels =
        HierarchicalSchemaUtil.splitSchemaName(
            schema.getSchemaName(), HierarchicalSchemaUtil.physicalSeparator());
    for (String level : namespaceLevels) {
      if (level.isEmpty()) {
        throw new IllegalStateException(
            "Cleanup job " + job.id() + " cannot resolve its retained table route");
      }
    }
    Namespace namespace = Namespace.of(namespaceLevels);
    return new RegistrationTarget(
        catalog.getCatalogName(), TableIdentifier.of(namespace, table.getTableName()));
  }

  private static boolean isLive(Long deletedAt) {
    return deletedAt != null && deletedAt == 0L;
  }

  private static IllegalStateException invalidLink(long jobId) {
    return new IllegalStateException(
        "Cleanup job " + jobId + " is not linked to an active retained deletion");
  }

  private static IllegalStateException externalFailure(long jobId) {
    // Provider exceptions can contain metadata locations or credential-bearing configuration.
    // Do not attach the original failure: the caller persists and logs this exception.
    return new IllegalStateException(
        "Cleanup job " + jobId + " could not remove the retained table registration");
  }

  private static final class RegistrationTarget {
    private final String catalogName;
    private final TableIdentifier identifier;

    private RegistrationTarget(String catalogName, TableIdentifier identifier) {
      this.catalogName = catalogName;
      this.identifier = identifier;
    }
  }
}
