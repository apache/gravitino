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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.iceberg.common.ops.IcebergTableCleanupContext;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.storage.relational.po.CatalogPO;
import org.apache.gravitino.storage.relational.po.EntityDeletionPO;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.gravitino.storage.relational.po.TableDeletionEntryPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

/** Unit tests for bounded retained-deletion discovery and context preparation. */
class TestIcebergRetainedDeletionPurgeCoordinator {

  private static final long NOW = 1_785_000_000_000L;
  private static final long METALAKE_ID = 11L;
  private static final long CATALOG_ID = 22L;
  private static final long SCHEMA_ID = 33L;
  private static final String CATALOG_NAME = "iceberg_catalog";
  private static final String SCHEMA_NAME = "outer";
  private static final String METADATA_LOCATION = "s3://cleanup-bucket/table/metadata/00001.json";
  private static final String FILE_IO_IMPL = "org.apache.iceberg.aws.s3.S3FileIO";

  private IcebergCleanupJobStore cleanupJobStore;
  private IcebergDeletionPurgeStore purgeStore;
  private IcebergCatalogWrapperManager catalogWrapperManager;
  private IcebergRetainedDeletionPurgeCoordinator.ParentMetadataLookup parentMetadataLookup;
  private CatalogWrapperForREST wrapper;
  private CatalogPO catalog;
  private SchemaPO schema;
  private IcebergTableCleanupContext cleanupContext;

  @BeforeEach
  void setUp() {
    cleanupJobStore = mock(IcebergCleanupJobStore.class);
    purgeStore = mock(IcebergDeletionPurgeStore.class);
    catalogWrapperManager = mock(IcebergCatalogWrapperManager.class);
    parentMetadataLookup = mock(IcebergRetainedDeletionPurgeCoordinator.ParentMetadataLookup.class);
    wrapper = mock(CatalogWrapperForREST.class);
    catalog = mock(CatalogPO.class);
    schema = mock(SchemaPO.class);
    cleanupContext =
        new IcebergTableCleanupContext(
            METADATA_LOCATION, FILE_IO_IMPL, ImmutableMap.of("client.region", "us-west-2"));

    when(catalog.getCatalogId()).thenReturn(CATALOG_ID);
    when(catalog.getMetalakeId()).thenReturn(METALAKE_ID);
    when(catalog.getCatalogName()).thenReturn(CATALOG_NAME);
    when(catalog.getDeletedAt()).thenReturn(0L);
    when(schema.getSchemaId()).thenReturn(SCHEMA_ID);
    when(schema.getMetalakeId()).thenReturn(METALAKE_ID);
    when(schema.getCatalogId()).thenReturn(CATALOG_ID);
    when(schema.getSchemaName()).thenReturn(SCHEMA_NAME);
    when(schema.getDeletedAt()).thenReturn(0L);
    when(parentMetadataLookup.catalog(CATALOG_ID)).thenReturn(catalog);
    when(parentMetadataLookup.schema(SCHEMA_ID)).thenReturn(schema);
    when(catalogWrapperManager.getCatalogWrapper(CATALOG_NAME)).thenReturn(wrapper);
    when(wrapper.loadTableCleanupContext(any())).thenReturn(cleanupContext);
    when(cleanupJobStore.countInflightJobs()).thenReturn(0);
    when(purgeStore.claimAndEnqueue(any(), any(), eq(NOW))).thenReturn(Optional.of(101L));
  }

  @Test
  void testFullCapacitySkipsCandidateScan() {
    when(cleanupJobStore.countInflightJobs()).thenReturn(10);
    IcebergRetainedDeletionPurgeCoordinator coordinator = coordinator(10, 4);

    assertEquals(0, coordinator.enqueueEligibleDeletions(NOW));

    verifyNoInteractions(purgeStore);
    verifyNoInteractions(catalogWrapperManager);
  }

  @Test
  void testCapacityAndBatchBoundCandidateClaims() {
    when(cleanupJobStore.countInflightJobs()).thenReturn(8);
    List<TableDeletionEntryPO> candidates =
        List.of(
            candidate(1L, "D1", "table_1"),
            candidate(2L, "D2", "table_2"),
            candidate(3L, "D3", "table_3"));
    when(purgeStore.findEligibleDeletions(NOW, 5)).thenReturn(candidates);
    IcebergRetainedDeletionPurgeCoordinator coordinator = coordinator(10, 5);

    assertEquals(2, coordinator.enqueueEligibleDeletions(NOW));

    verify(purgeStore).findEligibleDeletions(NOW, 5);
    verify(purgeStore, times(2)).claimAndEnqueue(any(), any(), eq(NOW));
    verify(wrapper, times(2)).loadTableCleanupContext(any());
  }

  @Test
  void testUsesImmutableParentIdsAndPhysicalNestedNamespace() {
    String physicalSchema = "outer" + HierarchicalSchemaUtil.physicalSeparator() + "inner";
    when(schema.getSchemaName()).thenReturn(physicalSchema);
    TableDeletionEntryPO candidate = candidate(7L, "D7", "orders");
    when(purgeStore.findEligibleDeletions(NOW, 1)).thenReturn(List.of(candidate));
    IcebergRetainedDeletionPurgeCoordinator coordinator = coordinator(10, 1);

    assertEquals(1, coordinator.enqueueEligibleDeletions(NOW));

    TableIdentifier expectedIdentifier =
        TableIdentifier.of(Namespace.of("outer", "inner"), "orders");
    verify(parentMetadataLookup).catalog(CATALOG_ID);
    verify(parentMetadataLookup).schema(SCHEMA_ID);
    verify(catalogWrapperManager).getCatalogWrapper(CATALOG_NAME);
    verify(wrapper).loadTableCleanupContext(expectedIdentifier);

    ArgumentCaptor<IcebergCleanupJob> jobCaptor = ArgumentCaptor.forClass(IcebergCleanupJob.class);
    verify(purgeStore).claimAndEnqueue(eq(candidate), jobCaptor.capture(), eq(NOW));
    IcebergCleanupJob job = jobCaptor.getValue();
    assertEquals(7L, job.tableId());
    assertEquals("D7", job.deletionId());
    assertEquals(CATALOG_ID, job.catalogId());
    assertEquals(Namespace.of("outer", "inner").toString(), job.namespace());
    assertEquals("orders", job.tableName());
    assertEquals(METADATA_LOCATION, job.metadataLocation());
    assertEquals(FILE_IO_IMPL, job.fileIOImpl());
    assertEquals(ImmutableMap.of("client.region", "us-west-2"), job.fileIOProperties());
    assertEquals("iceberg-retained-deletion-purge", job.createdBy());
  }

  @Test
  void testCandidatePreparationFailureDoesNotStopTick() {
    TableDeletionEntryPO failed = candidate(1L, "D1", "failed_table");
    TableDeletionEntryPO healthy = candidate(2L, "D2", "healthy_table");
    when(purgeStore.findEligibleDeletions(NOW, 2)).thenReturn(List.of(failed, healthy));
    TableIdentifier failedIdentifier =
        TableIdentifier.of(Namespace.of(SCHEMA_NAME), "failed_table");
    when(wrapper.loadTableCleanupContext(failedIdentifier))
        .thenThrow(new IllegalStateException("provider token=secret"));
    IcebergRetainedDeletionPurgeCoordinator coordinator = coordinator(10, 2);

    assertEquals(1, coordinator.enqueueEligibleDeletions(NOW));

    verify(purgeStore, never()).claimAndEnqueue(eq(failed), any(), eq(NOW));
    verify(purgeStore).claimAndEnqueue(eq(healthy), any(), eq(NOW));
  }

  @Test
  void testPreparationFailureDoesNotConsumeAvailableCapacity() {
    TableDeletionEntryPO failed = candidate(1L, "D1", "failed_table");
    TableDeletionEntryPO healthy = candidate(2L, "D2", "healthy_table");
    when(purgeStore.findEligibleDeletions(NOW, 2)).thenReturn(List.of(failed, healthy));
    TableIdentifier failedIdentifier =
        TableIdentifier.of(Namespace.of(SCHEMA_NAME), "failed_table");
    when(wrapper.loadTableCleanupContext(failedIdentifier))
        .thenThrow(new IllegalStateException("provider token=secret"));
    IcebergRetainedDeletionPurgeCoordinator coordinator = coordinator(1, 2);

    assertEquals(1, coordinator.enqueueEligibleDeletions(NOW));

    verify(purgeStore, never()).claimAndEnqueue(eq(failed), any(), eq(NOW));
    verify(purgeStore).claimAndEnqueue(eq(healthy), any(), eq(NOW));
  }

  @Test
  void testCursorAdvancesPastPoisonCandidateWindow() {
    TableDeletionEntryPO first = candidate(1L, "D1", "failed_table_1");
    TableDeletionEntryPO second = candidate(2L, "D2", "failed_table_2");
    TableDeletionEntryPO healthy = candidate(3L, "D3", "healthy_table");
    when(purgeStore.findEligibleDeletions(NOW, 2)).thenReturn(List.of(first, second));
    when(purgeStore.findEligibleDeletionsAfter(NOW, NOW, "D2", 2)).thenReturn(List.of(healthy));
    when(wrapper.loadTableCleanupContext(
            TableIdentifier.of(Namespace.of(SCHEMA_NAME), "failed_table_1")))
        .thenThrow(new IllegalStateException("provider failure"));
    when(wrapper.loadTableCleanupContext(
            TableIdentifier.of(Namespace.of(SCHEMA_NAME), "failed_table_2")))
        .thenThrow(new IllegalStateException("provider failure"));
    IcebergRetainedDeletionPurgeCoordinator coordinator = coordinator(1, 2);

    assertEquals(0, coordinator.enqueueEligibleDeletions(NOW));
    assertEquals(1, coordinator.enqueueEligibleDeletions(NOW));

    verify(purgeStore).findEligibleDeletionsAfter(NOW, NOW, "D2", 2);
    verify(purgeStore).claimAndEnqueue(eq(healthy), any(), eq(NOW));
  }

  @Test
  void testLostClaimIsNotCountedOrRetriedInSameTick() {
    TableDeletionEntryPO candidate = candidate(9L, "D9", "restored_table");
    when(purgeStore.findEligibleDeletions(NOW, 1)).thenReturn(List.of(candidate));
    when(purgeStore.claimAndEnqueue(eq(candidate), any(), eq(NOW))).thenReturn(Optional.empty());
    IcebergRetainedDeletionPurgeCoordinator coordinator = coordinator(10, 1);

    assertEquals(0, coordinator.enqueueEligibleDeletions(NOW));

    verify(purgeStore).claimAndEnqueue(eq(candidate), any(), eq(NOW));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "s3.session-token",
        "client.security-token",
        "gcs.oauth2.token",
        "adls.sas-token.account"
      })
  void testTemporaryFileIOCredentialsRemainUnclaimed(String temporaryCredentialKey) {
    TableDeletionEntryPO candidate = candidate(10L, "D10", "temporary_credentials");
    when(purgeStore.findEligibleDeletions(NOW, 1)).thenReturn(List.of(candidate));
    when(wrapper.loadTableCleanupContext(any()))
        .thenReturn(
            new IcebergTableCleanupContext(
                METADATA_LOCATION,
                FILE_IO_IMPL,
                ImmutableMap.of(temporaryCredentialKey, "secret")));
    IcebergRetainedDeletionPurgeCoordinator coordinator = coordinator(10, 1);

    assertEquals(0, coordinator.enqueueEligibleDeletions(NOW));

    verify(purgeStore, never()).claimAndEnqueue(eq(candidate), any(), eq(NOW));
  }

  @Test
  void testInterruptedContextDiscoveryDoesNotClaim() {
    TableDeletionEntryPO candidate = candidate(11L, "D11", "interrupted");
    when(purgeStore.findEligibleDeletions(NOW, 1)).thenReturn(List.of(candidate));
    when(wrapper.loadTableCleanupContext(any()))
        .thenAnswer(
            ignored -> {
              Thread.currentThread().interrupt();
              return cleanupContext;
            });
    IcebergRetainedDeletionPurgeCoordinator coordinator = coordinator(10, 1);

    try {
      assertEquals(0, coordinator.enqueueEligibleDeletions(NOW));
      verify(purgeStore, never()).claimAndEnqueue(eq(candidate), any(), eq(NOW));
    } finally {
      assertTrue(Thread.interrupted());
    }
  }

  private IcebergRetainedDeletionPurgeCoordinator coordinator(
      int maxInflightJobs, int enqueueBatchSize) {
    return new IcebergRetainedDeletionPurgeCoordinator(
        cleanupJobStore,
        purgeStore,
        catalogWrapperManager,
        parentMetadataLookup,
        maxInflightJobs,
        enqueueBatchSize);
  }

  private static TableDeletionEntryPO candidate(long tableId, String deletionId, String tableName) {
    TablePO table =
        TablePO.builder()
            .withTableId(tableId)
            .withTableName(tableName)
            .withMetalakeId(METALAKE_ID)
            .withCatalogId(CATALOG_ID)
            .withSchemaId(SCHEMA_ID)
            .withAuditInfo("{}")
            .withCurrentVersion(1L)
            .withLastVersion(1L)
            .withDeletedAt(NOW - 1)
            .withDeletionId(deletionId)
            .build();
    EntityDeletionPO deletion =
        EntityDeletionPO.builder()
            .deletionId(deletionId)
            .state("DELETED")
            .retentionExpiresAt(NOW)
            .build();
    TableDeletionEntryPO candidate = mock(TableDeletionEntryPO.class);
    when(candidate.getTable()).thenReturn(table);
    when(candidate.getDeletion()).thenReturn(deletion);
    return candidate;
  }
}
