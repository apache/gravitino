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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import javax.annotation.Nullable;
import org.apache.gravitino.iceberg.common.ops.IcebergTableCleanupContext;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.po.CatalogPO;
import org.apache.gravitino.storage.relational.po.EntityDeletionPO;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.gravitino.storage.relational.po.TableDeletionEntryPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Discovers expired retained Iceberg tables and atomically hands them to async cleanup. */
public class IcebergRetainedDeletionPurgeCoordinator {

  private static final Logger LOG =
      LoggerFactory.getLogger(IcebergRetainedDeletionPurgeCoordinator.class);
  private static final String SYSTEM_ACTOR = "iceberg-retained-deletion-purge";
  private static final Set<String> EXPIRING_CREDENTIAL_KEYS =
      ImmutableSet.of(
          "s3.session-token",
          "s3.session-token-expires-at-ms",
          "client.security-token",
          "client.security-token-expires-at-ms",
          "gcs.oauth2.token",
          "gcs.oauth2.token-expires-at",
          "client.refresh-credentials-endpoint",
          "adls.refresh-credentials-endpoint",
          "gcs.oauth2.refresh-credentials-endpoint");
  private static final String ADLS_EXPIRY_KEY_PREFIX = "adls.sas-token-expires-at-ms.";
  private static final String ADLS_TOKEN_KEY_PREFIX = "adls.sas-token.";

  private final IcebergCleanupJobStore cleanupJobStore;
  private final IcebergDeletionPurgeStore purgeStore;
  private final IcebergCatalogWrapperManager catalogWrapperManager;
  private final ParentMetadataLookup parentMetadataLookup;
  private final int maxInflightJobs;
  private final int enqueueBatchSize;
  @Nullable private EligibleDeletionCursor nextCandidateCursor;

  /**
   * Creates a coordinator for one Iceberg REST server.
   *
   * <p>The capacity observation is advisory across servers. Correctness comes from the exact
   * deletion-generation claim in {@link IcebergDeletionPurgeStore}, not from a globally serialized
   * capacity counter.
   *
   * @param cleanupJobStore durable cleanup-job store
   * @param purgeStore atomic retained-deletion handoff store
   * @param catalogWrapperManager Iceberg catalog wrapper manager
   * @param maxInflightJobs advisory ceiling for PENDING and RUNNING cleanup jobs
   * @param enqueueBatchSize hard maximum candidates considered by one invocation
   */
  public IcebergRetainedDeletionPurgeCoordinator(
      IcebergCleanupJobStore cleanupJobStore,
      IcebergDeletionPurgeStore purgeStore,
      IcebergCatalogWrapperManager catalogWrapperManager,
      int maxInflightJobs,
      int enqueueBatchSize) {
    this(
        cleanupJobStore,
        purgeStore,
        catalogWrapperManager,
        new RelationalParentMetadataLookup(),
        maxInflightJobs,
        enqueueBatchSize);
  }

  @VisibleForTesting
  IcebergRetainedDeletionPurgeCoordinator(
      IcebergCleanupJobStore cleanupJobStore,
      IcebergDeletionPurgeStore purgeStore,
      IcebergCatalogWrapperManager catalogWrapperManager,
      ParentMetadataLookup parentMetadataLookup,
      int maxInflightJobs,
      int enqueueBatchSize) {
    if (maxInflightJobs <= 0) {
      throw new IllegalArgumentException("maxInflightJobs must be positive");
    }
    if (enqueueBatchSize <= 0) {
      throw new IllegalArgumentException("enqueueBatchSize must be positive");
    }
    this.cleanupJobStore =
        Objects.requireNonNull(cleanupJobStore, "cleanupJobStore must not be null");
    this.purgeStore = Objects.requireNonNull(purgeStore, "purgeStore must not be null");
    this.catalogWrapperManager =
        Objects.requireNonNull(catalogWrapperManager, "catalogWrapperManager must not be null");
    this.parentMetadataLookup =
        Objects.requireNonNull(parentMetadataLookup, "parentMetadataLookup must not be null");
    this.maxInflightJobs = maxInflightJobs;
    this.enqueueBatchSize = enqueueBatchSize;
  }

  /**
   * Claims a bounded set of retained deletions that have reached their retention deadline.
   *
   * <p>The retained table row reserves its schema-id/table-name route while the action is active.
   * Parent names and the Iceberg cleanup snapshot are resolved before the claim transaction, so no
   * database row lock is held during an external catalog call. The claim transaction then
   * revalidates the exact table-deletion generation. Failure to prepare one candidate leaves it
   * {@code DELETED} and does not prevent later candidates from being considered.
   *
   * @param serverNow authoritative server time in epoch milliseconds
   * @return number of deletion generations atomically claimed and enqueued
   */
  public int enqueueEligibleDeletions(long serverNow) {
    if (Thread.currentThread().isInterrupted()) {
      return 0;
    }
    int observedInflight = Math.max(0, cleanupJobStore.countInflightJobs());
    int availableCapacity = (int) Math.max(0L, (long) maxInflightJobs - (long) observedInflight);
    int claimLimit = Math.min(enqueueBatchSize, availableCapacity);
    if (claimLimit == 0) {
      return 0;
    }

    // Scan the configured batch even when only a smaller number of job slots remain. Otherwise one
    // candidate whose external context cannot currently be prepared can permanently hide a healthy
    // candidate immediately behind it whenever available capacity is one.
    List<TableDeletionEntryPO> candidates = findCandidateWindow(serverNow);
    int claimed = 0;
    for (int index = 0; index < candidates.size() && claimed < claimLimit; index++) {
      TableDeletionEntryPO candidate = candidates.get(index);
      try {
        nextCandidateCursor = cursorOf(candidate);
        IcebergCleanupJob job = prepareCleanupJob(candidate);
        if (purgeStore.claimAndEnqueue(candidate, job, serverNow).isPresent()) {
          claimed++;
        }
      } catch (CancellationException cancelled) {
        Thread.currentThread().interrupt();
        break;
      } catch (RuntimeException failure) {
        // Provider failures may contain credentials or storage locations. Keep the candidate
        // retryable and log only its opaque deletion id, never the provider exception.
        LOG.warn(
            "Unable to enqueue retained Iceberg deletion {} for purge; it remains eligible",
            deletionIdForLog(candidate));
      }
    }
    return claimed;
  }

  private List<TableDeletionEntryPO> findCandidateWindow(long serverNow) {
    EligibleDeletionCursor cursor = nextCandidateCursor;
    if (cursor == null) {
      return purgeStore.findEligibleDeletions(serverNow, enqueueBatchSize);
    }

    List<TableDeletionEntryPO> candidates =
        purgeStore.findEligibleDeletionsAfter(
            serverNow, cursor.retentionExpiresAt, cursor.deletionId, enqueueBatchSize);
    if (!candidates.isEmpty()) {
      return candidates;
    }

    // Reaching the end wraps to the oldest eligible action. The cursor is advisory and local to
    // this server; exact ownership still comes from the relational claim transaction.
    nextCandidateCursor = null;
    return purgeStore.findEligibleDeletions(serverNow, enqueueBatchSize);
  }

  private static EligibleDeletionCursor cursorOf(TableDeletionEntryPO candidate) {
    EntityDeletionPO deletion =
        Objects.requireNonNull(candidate.getDeletion(), "candidate deletion must not be null");
    Long retentionExpiresAt =
        Objects.requireNonNull(
            deletion.getRetentionExpiresAt(), "retention expiry must not be null");
    String deletionId =
        Objects.requireNonNull(deletion.getDeletionId(), "deletion ID must not be null");
    return new EligibleDeletionCursor(retentionExpiresAt, deletionId);
  }

  private IcebergCleanupJob prepareCleanupJob(TableDeletionEntryPO candidate) {
    TablePO table =
        Objects.requireNonNull(candidate.getTable(), "candidate table must not be null");
    EntityDeletionPO deletion =
        Objects.requireNonNull(candidate.getDeletion(), "candidate deletion must not be null");
    String deletionId =
        Objects.requireNonNull(deletion.getDeletionId(), "deletion ID must not be null");

    CatalogPO catalog = parentMetadataLookup.catalog(table.getCatalogId());
    SchemaPO schema = parentMetadataLookup.schema(table.getSchemaId());
    validateParents(table, catalog, schema);
    abortIfInterrupted();

    String[] namespaceLevels =
        HierarchicalSchemaUtil.splitSchemaName(
            schema.getSchemaName(), HierarchicalSchemaUtil.physicalSeparator());
    for (String level : namespaceLevels) {
      if (level.isEmpty()) {
        throw new IllegalStateException("Retained table has an invalid physical schema route");
      }
    }
    Namespace namespace = Namespace.of(namespaceLevels);
    TableIdentifier identifier = TableIdentifier.of(namespace, table.getTableName());
    CatalogWrapperForREST wrapper =
        catalogWrapperManager.getCatalogWrapper(catalog.getCatalogName());
    IcebergTableCleanupContext context = wrapper.loadTableCleanupContext(identifier);
    abortIfInterrupted();
    validateDurableFileIOContext(context);

    return IcebergCleanupJob.forRetainedDeletion(
        0L,
        table.getTableId(),
        deletionId,
        table.getCatalogId(),
        namespace.toString(),
        table.getTableName(),
        context.metadataLocation(),
        context.fileIOImpl(),
        context.fileIOProperties(),
        SYSTEM_ACTOR);
  }

  private static void abortIfInterrupted() {
    if (Thread.currentThread().isInterrupted()) {
      throw new CancellationException("Retained purge collection was interrupted");
    }
  }

  private static void validateDurableFileIOContext(IcebergTableCleanupContext context) {
    boolean expires =
        context.fileIOProperties().keySet().stream()
            .anyMatch(
                key ->
                    EXPIRING_CREDENTIAL_KEYS.contains(key)
                        || key.startsWith(ADLS_EXPIRY_KEY_PREFIX)
                        || key.startsWith(ADLS_TOKEN_KEY_PREFIX));
    if (expires) {
      // Once the worker removes the catalog registration it cannot safely refresh a table-scoped
      // credential after a crash. Leave the deletion unclaimed until a refreshable worker
      // credential design is available instead of creating a PURGING action that can wedge.
      throw new IllegalStateException(
          "Retained purge requires durable or reconstructible FileIO credentials");
    }
  }

  private static void validateParents(TablePO table, CatalogPO catalog, SchemaPO schema) {
    if (catalog == null
        || schema == null
        || !Objects.equals(catalog.getCatalogId(), table.getCatalogId())
        || !Objects.equals(catalog.getMetalakeId(), table.getMetalakeId())
        || !Objects.equals(schema.getSchemaId(), table.getSchemaId())
        || !Objects.equals(schema.getCatalogId(), table.getCatalogId())
        || !Objects.equals(schema.getMetalakeId(), table.getMetalakeId())
        || !isLive(catalog.getDeletedAt())
        || !isLive(schema.getDeletedAt())) {
      throw new IllegalStateException("Retained table parent metadata is unavailable");
    }
  }

  private static boolean isLive(Long deletedAt) {
    return deletedAt != null && deletedAt == 0L;
  }

  private static String deletionIdForLog(TableDeletionEntryPO candidate) {
    if (candidate == null || candidate.getDeletion() == null) {
      return "<unknown>";
    }
    String deletionId = candidate.getDeletion().getDeletionId();
    return deletionId == null ? "<unknown>" : deletionId;
  }

  @VisibleForTesting
  interface ParentMetadataLookup {
    CatalogPO catalog(long catalogId);

    SchemaPO schema(long schemaId);
  }

  private static class RelationalParentMetadataLookup implements ParentMetadataLookup {

    @Override
    public CatalogPO catalog(long catalogId) {
      return SessionUtils.getWithoutCommit(
          CatalogMetaMapper.class, mapper -> mapper.selectCatalogMetaById(catalogId));
    }

    @Override
    public SchemaPO schema(long schemaId) {
      return SessionUtils.getWithoutCommit(
          SchemaMetaMapper.class, mapper -> mapper.selectSchemaMetaById(schemaId));
    }
  }

  private static final class EligibleDeletionCursor {
    private final long retentionExpiresAt;
    private final String deletionId;

    private EligibleDeletionCursor(long retentionExpiresAt, String deletionId) {
      this.retentionExpiresAt = retentionExpiresAt;
      this.deletionId = deletionId;
    }
  }
}
