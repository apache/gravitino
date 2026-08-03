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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.List;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.po.EntityDeletionPO;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.gravitino.storage.relational.po.TableDeletionEntryPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.service.EntityIdService;
import org.apache.gravitino.storage.relational.service.TableDeletionService;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/** Cross-backend coverage for metadata-only removal of a retained table registration. */
class TestIcebergRetainedDeletionRegistrationCleaner extends TestJDBCBackend {

  private static final String METALAKE = "cleanup_metalake";
  private static final String CATALOG = "cleanup_catalog";
  private static final String SCHEMA = "outer:inner";
  private static final String TABLE = "orders";
  private static final String DELETION_ID = "D1";
  private static final String METADATA_LOCATION = "s3://cleanup-bucket/orders/metadata/00001.json";
  private static final long DELETED_AT = 1_784_800_000_000L;

  private Object originalConfig;
  private Object originalIdGenerator;
  private TablePO retainedTable;
  private IcebergCleanupJob claimedJob;
  private IcebergCleanupJobStore cleanupJobStore;
  private IcebergCatalogWrapperManager wrapperManager;
  private CatalogWrapperForREST wrapper;
  private IcebergRetainedDeletionRegistrationCleaner cleaner;

  @BeforeAll
  public void snapshotGravitinoEnv() throws IllegalAccessException {
    originalConfig = FieldUtils.readField(GravitinoEnv.getInstance(), "config", true);
    originalIdGenerator = FieldUtils.readField(GravitinoEnv.getInstance(), "idGenerator", true);
  }

  @AfterAll
  public void restoreGravitinoEnv() throws IllegalAccessException {
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", originalConfig, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "idGenerator", originalIdGenerator, true);
  }

  @BeforeEach
  void prepareRetainedDeletion() throws IOException {
    createParentEntities(METALAKE, CATALOG, SCHEMA, AUDIT_INFO);
    org.apache.gravitino.Namespace namespace = NamespaceUtil.ofTable(METALAKE, CATALOG, SCHEMA);
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
    backend.insert(
        TableEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(TABLE)
            .withNamespace(namespace)
            .withColumns(List.of(column))
            .withAuditInfo(AUDIT_INFO)
            .build(),
        false);

    long schemaId =
        EntityIdService.getEntityId(
            NameIdentifier.of(METALAKE, CATALOG, SCHEMA), Entity.EntityType.SCHEMA);
    retainedTable =
        SessionUtils.getWithoutCommit(
            TableMetaMapper.class,
            mapper -> mapper.selectTableMetaBySchemaIdAndName(schemaId, TABLE));
    EntityDeletionPO deletion =
        EntityDeletionPO.builder()
            .deletionId(DELETION_ID)
            .state("DELETED")
            .retentionExpiresAt(DELETED_AT)
            .build();
    TableDeletionService.getInstance().delete(retainedTable, DELETED_AT, deletion);

    cleanupJobStore = new IcebergCleanupJobStore(new RandomIdGenerator());
    IcebergDeletionPurgeStore purgeStore = new IcebergDeletionPurgeStore(cleanupJobStore);
    TableDeletionEntryPO candidate =
        purgeStore.findEligibleDeletions(DELETED_AT, 1).stream().findFirst().orElseThrow();
    IcebergCleanupJob pending =
        IcebergCleanupJob.forRetainedDeletion(
            0L,
            retainedTable.getTableId(),
            DELETION_ID,
            retainedTable.getCatalogId(),
            Namespace.of("outer", "inner").toString(),
            TABLE,
            METADATA_LOCATION,
            "org.apache.iceberg.aws.s3.S3FileIO",
            ImmutableMap.of(),
            "system");
    purgeStore.claimAndEnqueue(candidate, pending, DELETED_AT).orElseThrow();
    claimedJob = cleanupJobStore.takePendingJob(DELETED_AT + 1, 300_000L, 1).orElseThrow();

    wrapperManager = mock(IcebergCatalogWrapperManager.class);
    wrapper = mock(CatalogWrapperForREST.class);
    when(wrapperManager.getCatalogWrapper(CATALOG)).thenReturn(wrapper);
    cleaner = new IcebergRetainedDeletionRegistrationCleaner(cleanupJobStore, wrapperManager);
  }

  @TestTemplate
  void testRemovesReservedRegistrationUsingStableParentIds() {
    SchemaPO storedSchema =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper -> mapper.selectSchemaMetaById(retainedTable.getSchemaId()));
    assertEquals(
        "outer" + HierarchicalSchemaUtil.physicalSeparator() + "inner",
        storedSchema.getSchemaName());

    cleaner.removeRegistration(claimedJob);

    TableIdentifier expected = TableIdentifier.of(Namespace.of("outer", "inner"), TABLE);
    verify(wrapperManager).getCatalogWrapper(CATALOG);
    verify(wrapper, never()).loadTableMetadata(any());
    verify(wrapper).dropTable(expected);
  }

  @TestTemplate
  void testMissingRegistrationDuringDropIsAlreadyDone() {
    doThrow(new NoSuchTableException("already absent")).when(wrapper).dropTable(any());

    cleaner.removeRegistration(claimedJob);

    verify(wrapper).dropTable(TableIdentifier.of(Namespace.of("outer", "inner"), TABLE));
  }

  @TestTemplate
  void testMismatchedDeletionGenerationFailsBeforeCatalogCall() {
    IcebergCleanupJob wrongGeneration =
        IcebergCleanupJob.forRetainedDeletion(
            claimedJob.id(),
            retainedTable.getTableId() + 1,
            "D2",
            retainedTable.getCatalogId(),
            claimedJob.namespace(),
            TABLE,
            METADATA_LOCATION,
            claimedJob.fileIOImpl(),
            ImmutableMap.of(),
            "system");

    assertThrows(IllegalStateException.class, () -> cleaner.removeRegistration(wrongGeneration));
    verifyNoInteractions(wrapperManager);
  }

  @TestTemplate
  void testProviderFailureIsSanitized() {
    doThrow(
            new IllegalStateException(
                "s3://user:password@bucket/key?token=secret-provider-credential"))
        .when(wrapper)
        .dropTable(any());

    IllegalStateException failure =
        assertThrows(IllegalStateException.class, () -> cleaner.removeRegistration(claimedJob));

    assertFalse(failure.getMessage().contains("password"));
    assertFalse(failure.getMessage().contains("secret-provider-credential"));
    assertNull(failure.getCause());
  }
}
