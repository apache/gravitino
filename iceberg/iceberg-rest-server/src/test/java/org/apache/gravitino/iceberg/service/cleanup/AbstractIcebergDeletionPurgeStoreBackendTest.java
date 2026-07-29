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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.EntityDeletionMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.po.EntityDeletionPO;
import org.apache.gravitino.storage.relational.po.TableDeletionEntryPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.service.EntityDeletionService;
import org.apache.gravitino.storage.relational.service.EntityIdService;
import org.apache.gravitino.storage.relational.service.TableDeletionService;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/** Cross-backend tests for the retained-deletion to cleanup-job handoff. */
abstract class AbstractIcebergDeletionPurgeStoreBackendTest extends TestJDBCBackend {

  private static final String METALAKE = "purge_metalake";
  private static final String CATALOG = "purge_catalog";
  private static final String SCHEMA = "purge_schema";
  private static final String TABLE = "orders";
  private static final long DELETED_AT = 1_784_800_000_000L;

  private NameIdentifier tableIdentifier;
  private TablePO liveTable;
  private IcebergCleanupJobStore cleanupJobStore;
  private IcebergDeletionPurgeStore purgeStore;

  @BeforeEach
  void prepareTableAndStores() throws IOException {
    createParentEntities(METALAKE, CATALOG, SCHEMA, AUDIT_INFO);
    Namespace namespace = NamespaceUtil.ofTable(METALAKE, CATALOG, SCHEMA);
    tableIdentifier = NameIdentifier.of(namespace, TABLE);
    ColumnEntity column =
        ColumnEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("order_id")
            .withPosition(0)
            .withDataType(Types.LongType.get())
            .withNullable(false)
            .withAutoIncrement(false)
            .withAuditInfo(AUDIT_INFO)
            .build();
    TableEntity table =
        TableEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(TABLE)
            .withNamespace(namespace)
            .withColumns(List.of(column))
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.insert(table, false);
    liveTable = loadLiveTable();
    cleanupJobStore = new IcebergCleanupJobStore(new RandomIdGenerator());
    purgeStore = new IcebergDeletionPurgeStore(cleanupJobStore);
  }

  @TestTemplate
  void testExpiryBoundaryAndAtomicClaim() {
    long deadline = DELETED_AT + 10_000;
    delete("D1", deadline);

    assertTrue(purgeStore.findEligibleDeletions(deadline - 1, 10).isEmpty());
    TableDeletionEntryPO candidate = onlyCandidate(deadline);
    assertTrue(
        purgeStore
            .claimAndEnqueue(candidate, cleanupJob(candidate, "alice"), deadline - 1)
            .isEmpty());

    long jobId =
        purgeStore
            .claimAndEnqueue(candidate, cleanupJob(candidate, "alice"), deadline)
            .orElseThrow();
    EntityDeletionPO deletion = EntityDeletionService.getInstance().get("D1");
    assertNotNull(deletion);
    assertEquals("PURGING", deletion.getState());
    assertEquals(Long.toString(jobId), deletion.getPurgeJobId());
    assertNotNull(TableDeletionService.getInstance().getRetainedTable("D1"));
    assertEquals(IcebergCleanupJob.State.PENDING, cleanupJobStore.stateOf(jobId));

    IcebergCleanupJob queued =
        cleanupJobStore.takePendingJob(deadline + 1, 300_000L, 10).orElseThrow();
    assertEquals(liveTable.getTableId(), queued.tableId());
    assertEquals("D1", queued.deletionId());
  }

  @TestTemplate
  void testEligibleCandidateCursorIsExclusive() {
    long deadline = DELETED_AT + 10_000;
    delete("D1", deadline);

    assertEquals(1, purgeStore.findEligibleDeletionsAfter(deadline, deadline, "D0", 10).size());
    assertTrue(purgeStore.findEligibleDeletionsAfter(deadline, deadline, "D1", 10).isEmpty());
    assertTrue(purgeStore.findEligibleDeletionsAfter(deadline, deadline + 1, "D0", 10).isEmpty());
  }

  @TestTemplate
  void testInsertFailureRollsBackPurgeOwnership() {
    long deadline = DELETED_AT + 10_000;
    delete("D1", deadline);
    TableDeletionEntryPO candidate = onlyCandidate(deadline);

    assertThrows(
        RuntimeException.class,
        () -> purgeStore.claimAndEnqueue(candidate, cleanupJob(candidate, null), deadline));

    EntityDeletionPO deletion = EntityDeletionService.getInstance().get("D1");
    assertNotNull(deletion);
    assertEquals("DELETED", deletion.getState());
    assertNull(deletion.getPurgeJobId());
    assertNotNull(TableDeletionService.getInstance().getRetainedTable("D1"));
    assertFalse(
        cleanupJobStore.findUnfinishedJobId(liveTable.getCatalogId(), SCHEMA, TABLE).isPresent());
  }

  @TestTemplate
  void testGuardedTransitionFailureRollsBackInsertedJobAndPurgeOwnership() {
    long deadline = DELETED_AT + 10_000;
    delete("D1", deadline);
    TableDeletionEntryPO candidate = onlyCandidate(deadline);
    AtomicInteger interferingClaimCount = new AtomicInteger();
    long expectedJobId = RandomIdGenerator.INSTANCE.nextId();
    IcebergCleanupJobStore failAfterInsertStore =
        new IcebergCleanupJobStore(() -> expectedJobId) {
          @Override
          void insertJobWithoutCommit(IcebergCleanupJob job, long id, long now) {
            super.insertJobWithoutCommit(job, id, now);
            interferingClaimCount.set(
                SessionUtils.getWithoutCommit(
                    EntityDeletionMapper.class,
                    mapper ->
                        mapper.claimEntityDeletionForPurge(
                            job.deletionId(), "competing-job", now)));
          }
        };
    IcebergDeletionPurgeStore failAfterInsertPurgeStore =
        new IcebergDeletionPurgeStore(failAfterInsertStore);

    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class,
            () ->
                failAfterInsertPurgeStore.claimAndEnqueue(
                    candidate, cleanupJob(candidate, "alice"), deadline));

    assertEquals(1, interferingClaimCount.get());
    assertTrue(failure.getMessage().contains("changed during purge claim"));
    EntityDeletionPO deletion = EntityDeletionService.getInstance().get("D1");
    assertNotNull(deletion);
    assertEquals("DELETED", deletion.getState());
    assertNull(deletion.getPurgeJobId());
    assertNotNull(TableDeletionService.getInstance().getRetainedTable("D1"));
    assertThrows(IllegalStateException.class, () -> failAfterInsertStore.stateOf(expectedJobId));
    assertFalse(
        failAfterInsertStore
            .findUnfinishedJobId(liveTable.getCatalogId(), SCHEMA, TABLE)
            .isPresent());
  }

  @TestTemplate
  void testOwnedCompletionAtomicallyConsumesRetainedGeneration() {
    long deadline = DELETED_AT + 10_000;
    delete("D1", deadline);
    TableDeletionEntryPO candidate = onlyCandidate(deadline);
    long jobId =
        purgeStore
            .claimAndEnqueue(candidate, cleanupJob(candidate, "alice"), deadline)
            .orElseThrow();
    long heartbeat = deadline + 1;
    IcebergCleanupJob claimed =
        cleanupJobStore.takePendingJob(heartbeat, 300_000L, 10).orElseThrow();

    assertTrue(cleanupJobStore.finalizeRetainedDeletion(claimed, heartbeat));
    assertEquals(IcebergCleanupJob.State.SUCCEEDED, cleanupJobStore.stateOf(jobId));
    assertNull(EntityDeletionService.getInstance().get("D1"));
    assertNull(TableDeletionService.getInstance().getRetainedTable("D1"));
  }

  @TestTemplate
  void testFinalizationMismatchRollsBackJobAndMetadata() {
    long deadline = DELETED_AT + 10_000;
    delete("D1", deadline);
    TableDeletionEntryPO candidate = onlyCandidate(deadline);
    long jobId =
        purgeStore
            .claimAndEnqueue(candidate, cleanupJob(candidate, "alice"), deadline)
            .orElseThrow();
    long heartbeat = deadline + 1;
    IcebergCleanupJob claimed =
        cleanupJobStore.takePendingJob(heartbeat, 300_000L, 10).orElseThrow();
    IcebergCleanupJob wrongTable =
        IcebergCleanupJob.forRetainedDeletion(
            claimed.id(),
            claimed.tableId() + 1,
            claimed.deletionId(),
            claimed.catalogId(),
            claimed.namespace(),
            claimed.tableName(),
            claimed.metadataLocation(),
            claimed.fileIOImpl(),
            claimed.fileIOProperties(),
            claimed.createdBy());

    assertThrows(
        IllegalStateException.class,
        () -> cleanupJobStore.finalizeRetainedDeletion(wrongTable, heartbeat));
    assertEquals(IcebergCleanupJob.State.RUNNING, cleanupJobStore.stateOf(jobId));
    assertNotNull(EntityDeletionService.getInstance().get("D1"));
    assertNotNull(TableDeletionService.getInstance().getRetainedTable("D1"));

    assertTrue(cleanupJobStore.finalizeRetainedDeletion(claimed, heartbeat));
  }

  @TestTemplate
  void testStaleCandidateCannotClaimLaterGeneration() {
    delete("D1", Long.MAX_VALUE);
    TableDeletionEntryPO staleCandidate = onlyCandidate(Long.MAX_VALUE);
    TableDeletionService.getInstance().restore("D1");

    liveTable = loadLiveTable();
    delete("D2", Long.MAX_VALUE);

    assertTrue(
        purgeStore
            .claimAndEnqueue(staleCandidate, cleanupJob(staleCandidate, "alice"), Long.MAX_VALUE)
            .isEmpty());
    EntityDeletionPO current = EntityDeletionService.getInstance().get("D2");
    assertNotNull(current);
    assertEquals("DELETED", current.getState());
    assertNull(current.getPurgeJobId());
  }

  @TestTemplate
  void testConcurrentClaimsProduceOneJob() throws Exception {
    long deadline = DELETED_AT + 10_000;
    delete("D1", deadline);
    TableDeletionEntryPO candidate = onlyCandidate(deadline);
    IcebergCleanupJob cleanupJob = cleanupJob(candidate, "alice");
    CyclicBarrier start = new CyclicBarrier(2);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<Optional<Long>> first =
          executor.submit(
              () -> {
                start.await();
                return purgeStore.claimAndEnqueue(candidate, cleanupJob, deadline);
              });
      Future<Optional<Long>> second =
          executor.submit(
              () -> {
                start.await();
                return purgeStore.claimAndEnqueue(candidate, cleanupJob, deadline);
              });

      Optional<Long> firstResult = first.get(30, TimeUnit.SECONDS);
      Optional<Long> secondResult = second.get(30, TimeUnit.SECONDS);
      assertEquals(1, (firstResult.isPresent() ? 1 : 0) + (secondResult.isPresent() ? 1 : 0));
      long winner = firstResult.orElseGet(secondResult::orElseThrow);
      assertEquals(
          Long.toString(winner), EntityDeletionService.getInstance().get("D1").getPurgeJobId());
      assertEquals(IcebergCleanupJob.State.PENDING, cleanupJobStore.stateOf(winner));
    } finally {
      executor.shutdownNow();
    }
  }

  @TestTemplate
  void testClaimAndRestoreHaveOneWinner() throws Exception {
    long deadline = System.currentTimeMillis() + 120_000L;
    delete("D1", deadline);
    TableDeletionEntryPO candidate = onlyCandidate(deadline);
    CyclicBarrier start = new CyclicBarrier(2);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<Optional<Long>> claim =
          executor.submit(
              () -> {
                start.await();
                return purgeStore.claimAndEnqueue(
                    candidate, cleanupJob(candidate, "alice"), deadline);
              });
      Future<Boolean> restore =
          executor.submit(
              () -> {
                start.await();
                try {
                  TableDeletionService.getInstance().restore("D1");
                  return true;
                } catch (IllegalStateException expected) {
                  return false;
                }
              });

      Optional<Long> claimedJob = claim.get(30, TimeUnit.SECONDS);
      boolean restored = restore.get(30, TimeUnit.SECONDS);
      assertTrue(claimedJob.isPresent() ^ restored);
      if (claimedJob.isPresent()) {
        EntityDeletionPO deletion = EntityDeletionService.getInstance().get("D1");
        assertNotNull(deletion);
        assertEquals("PURGING", deletion.getState());
        assertNotNull(TableDeletionService.getInstance().getRetainedTable("D1"));
      } else {
        assertNull(EntityDeletionService.getInstance().get("D1"));
        assertTrue(backend.exists(tableIdentifier, Entity.EntityType.TABLE));
      }
    } finally {
      executor.shutdownNow();
    }
  }

  private void delete(String deletionId, long deadline) {
    EntityDeletionPO deletion =
        EntityDeletionPO.builder()
            .deletionId(deletionId)
            .state("DELETED")
            .retentionExpiresAt(deadline)
            .build();
    TableDeletionService.getInstance().delete(liveTable, DELETED_AT, deletion);
  }

  private TableDeletionEntryPO onlyCandidate(long serverNow) {
    List<TableDeletionEntryPO> candidates = purgeStore.findEligibleDeletions(serverNow, 10);
    assertEquals(1, candidates.size());
    return candidates.get(0);
  }

  private IcebergCleanupJob cleanupJob(TableDeletionEntryPO candidate, String createdBy) {
    return IcebergCleanupJob.forRetainedDeletion(
        0L,
        candidate.getTable().getTableId(),
        candidate.getDeletion().getDeletionId(),
        candidate.getTable().getCatalogId(),
        SCHEMA,
        candidate.getTable().getTableName(),
        "s3://bucket/orders/metadata/00001.json",
        "org.apache.iceberg.aws.s3.S3FileIO",
        ImmutableMap.of("region", "us-west-2"),
        createdBy);
  }

  private TablePO loadLiveTable() {
    long schemaId =
        EntityIdService.getEntityId(
            NameIdentifier.of(METALAKE, CATALOG, SCHEMA), Entity.EntityType.SCHEMA);
    return SessionUtils.doWithCommitAndFetchResult(
        TableMetaMapper.class, mapper -> mapper.selectTableMetaBySchemaIdAndName(schemaId, TABLE));
  }
}

class TestIcebergDeletionPurgeStoreBackend extends AbstractIcebergDeletionPurgeStoreBackendTest {}
