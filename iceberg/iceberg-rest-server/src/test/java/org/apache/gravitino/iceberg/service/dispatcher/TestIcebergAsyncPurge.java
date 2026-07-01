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

package org.apache.gravitino.iceberg.service.dispatcher;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.cleanup.IcebergCleanupJob;
import org.apache.gravitino.iceberg.service.cleanup.IcebergCleanupManager;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.ImmutableRegisterTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedStatic;

/**
 * Covers the async purge request path in the table and namespace executors: how {@code dropTable}
 * routes on the {@code X-Gravitino-Async-Purge} header, and how {@code createTable} / {@code
 * registerTable} are blocked while a cleanup job holds the identifier.
 */
class TestIcebergAsyncPurge {

  private static final long CATALOG_ID = 42L;
  private static final Namespace DB = Namespace.of("db");
  private static final TableIdentifier TABLE = TableIdentifier.of("db", "t");
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  @BeforeEach
  void setUp() {
    IcebergConfigProvider provider = mock(IcebergConfigProvider.class);
    when(provider.getMetalakeName()).thenReturn("metalake");
    when(provider.getDefaultCatalogName()).thenReturn("cat");
    IcebergRESTServerContext.create(provider, false, false, true, null);
  }

  // --- dropTable routing ---

  @Test
  void testAsyncDropEnqueuesJob() {
    CatalogWrapperForREST wrapper = mock(CatalogWrapperForREST.class);
    IcebergCleanupManager cleanup = mock(IcebergCleanupManager.class);
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.metadataFileLocation()).thenReturn("s3://b/db/t/metadata/0.json");
    when(wrapper.loadTableMetadata(any())).thenReturn(metadata);
    when(wrapper.fileIOImpl()).thenReturn("io");
    when(wrapper.fileIOProperties()).thenReturn(Collections.emptyMap());

    try (MockedStatic<GravitinoEnv> ignored = mockCatalogId()) {
      tableExecutor(wrapper, Optional.of(cleanup)).dropTable(context(true), TABLE, true);
    }

    InOrder ordered = inOrder(wrapper, cleanup);
    ordered.verify(wrapper).loadTableMetadata(TABLE);
    ordered.verify(wrapper).dropTable(TABLE);
    ArgumentCaptor<IcebergCleanupJob> captor = ArgumentCaptor.forClass(IcebergCleanupJob.class);
    ordered.verify(cleanup).addJob(captor.capture());
    IcebergCleanupJob job = captor.getValue();
    Assertions.assertEquals(CATALOG_ID, job.catalogId());
    Assertions.assertEquals("db", job.namespace());
    Assertions.assertEquals("t", job.tableName());
    Assertions.assertEquals("s3://b/db/t/metadata/0.json", job.metadataLocation());
    Assertions.assertEquals("io", job.fileIOImpl());
    Assertions.assertEquals("alice", job.createdBy());
  }

  @Test
  void testSyncPurgeByDefault() {
    CatalogWrapperForREST wrapper = mock(CatalogWrapperForREST.class);
    IcebergCleanupManager cleanup = mock(IcebergCleanupManager.class);

    tableExecutor(wrapper, Optional.of(cleanup)).dropTable(context(false), TABLE, true);

    verify(wrapper).purgeTable(TABLE);
    verify(cleanup, never()).addJob(any());
  }

  @Test
  void testFallbackToSyncWhenDisabled() {
    CatalogWrapperForREST wrapper = mock(CatalogWrapperForREST.class);

    tableExecutor(wrapper, Optional.empty()).dropTable(context(true), TABLE, true);

    verify(wrapper).purgeTable(TABLE);
    verify(wrapper, never()).loadTableMetadata(any());
  }

  @Test
  void testPlainDropWhenNotPurge() {
    CatalogWrapperForREST wrapper = mock(CatalogWrapperForREST.class);
    IcebergCleanupManager cleanup = mock(IcebergCleanupManager.class);

    tableExecutor(wrapper, Optional.of(cleanup)).dropTable(context(false), TABLE, false);

    verify(wrapper).dropTable(TABLE);
    verify(cleanup, never()).addJob(any());
  }

  // --- create / register tombstone ---

  @Test
  void testCreateRejectedWhilePurging() {
    CatalogWrapperForREST wrapper = mock(CatalogWrapperForREST.class);
    IcebergCleanupManager cleanup = mock(IcebergCleanupManager.class);
    when(cleanup.isNameOccupied(CATALOG_ID, "db", "t")).thenReturn(true);

    try (MockedStatic<GravitinoEnv> ignored = mockCatalogId()) {
      AlreadyExistsException e =
          Assertions.assertThrows(
              AlreadyExistsException.class,
              () ->
                  tableExecutor(wrapper, Optional.of(cleanup))
                      .createTable(context(false), DB, createReq()));
      Assertions.assertTrue(e.getMessage().contains("being purged"), e.getMessage());
    }
    verify(wrapper, never()).createTable(any(), any(), anyBoolean(), anyBoolean());
  }

  @Test
  void testCreateAllowedWhenNotPurging() {
    CatalogWrapperForREST wrapper = mock(CatalogWrapperForREST.class);
    IcebergCleanupManager cleanup = mock(IcebergCleanupManager.class);
    when(cleanup.isNameOccupied(CATALOG_ID, "db", "t")).thenReturn(false);

    try (MockedStatic<GravitinoEnv> ignored = mockCatalogId()) {
      tableExecutor(wrapper, Optional.of(cleanup)).createTable(context(false), DB, createReq());
    }
    verify(wrapper).createTable(any(), any(), anyBoolean(), anyBoolean());
  }

  @Test
  void testRegisterRejectedWhilePurging() {
    CatalogWrapperForREST wrapper = mock(CatalogWrapperForREST.class);
    IcebergCleanupManager cleanup = mock(IcebergCleanupManager.class);
    when(cleanup.isNameOccupied(CATALOG_ID, "db", "t")).thenReturn(true);

    try (MockedStatic<GravitinoEnv> ignored = mockCatalogId()) {
      AlreadyExistsException e =
          Assertions.assertThrows(
              AlreadyExistsException.class,
              () ->
                  namespaceExecutor(wrapper, Optional.of(cleanup))
                      .registerTable(context(false), DB, registerReq()));
      Assertions.assertTrue(e.getMessage().contains("being purged"), e.getMessage());
    }
    verify(wrapper, never()).registerTable(any(), any(), anyBoolean(), anyBoolean());
  }

  @Test
  void testRegisterAllowedWhenNotPurging() {
    CatalogWrapperForREST wrapper = mock(CatalogWrapperForREST.class);
    IcebergCleanupManager cleanup = mock(IcebergCleanupManager.class);
    when(cleanup.isNameOccupied(CATALOG_ID, "db", "t")).thenReturn(false);

    try (MockedStatic<GravitinoEnv> ignored = mockCatalogId()) {
      namespaceExecutor(wrapper, Optional.of(cleanup))
          .registerTable(context(false), DB, registerReq());
    }
    verify(wrapper).registerTable(any(), any(), anyBoolean(), anyBoolean());
  }

  // --- helpers ---

  private IcebergTableOperationExecutor tableExecutor(
      CatalogWrapperForREST wrapper, Optional<IcebergCleanupManager> cleanup) {
    return new IcebergTableOperationExecutor(wrapperManager(wrapper), cleanup);
  }

  private IcebergNamespaceOperationExecutor namespaceExecutor(
      CatalogWrapperForREST wrapper, Optional<IcebergCleanupManager> cleanup) {
    return new IcebergNamespaceOperationExecutor(wrapperManager(wrapper), cleanup);
  }

  private static IcebergCatalogWrapperManager wrapperManager(CatalogWrapperForREST wrapper) {
    IcebergCatalogWrapperManager manager = mock(IcebergCatalogWrapperManager.class);
    when(manager.getCatalogWrapper("cat")).thenReturn(wrapper);
    return manager;
  }

  private static IcebergRequestContext context(boolean asyncPurge) {
    IcebergRequestContext context = mock(IcebergRequestContext.class);
    when(context.catalogName()).thenReturn("cat");
    when(context.userName()).thenReturn(asyncPurge ? "alice" : AuthConstants.ANONYMOUS_USER);
    when(context.asyncPurge()).thenReturn(asyncPurge);
    return context;
  }

  /** Stubs the request-thread catalog-id resolution to {@link #CATALOG_ID}. */
  private static MockedStatic<GravitinoEnv> mockCatalogId() {
    MockedStatic<GravitinoEnv> envStatic = mockStatic(GravitinoEnv.class);
    GravitinoEnv env = mock(GravitinoEnv.class);
    CatalogManager catalogManager = mock(CatalogManager.class);
    CatalogManager.CatalogWrapper wrapper = mock(CatalogManager.CatalogWrapper.class);
    BaseCatalog<?> catalog = mock(BaseCatalog.class);
    CatalogEntity entity = mock(CatalogEntity.class);
    envStatic.when(GravitinoEnv::getInstance).thenReturn(env);
    when(env.catalogManager()).thenReturn(catalogManager);
    when(catalogManager.loadCatalogAndWrap(any())).thenReturn(wrapper);
    when(wrapper.catalog()).thenReturn(catalog);
    when(catalog.entity()).thenReturn(entity);
    when(entity.id()).thenReturn(CATALOG_ID);
    return envStatic;
  }

  private static CreateTableRequest createReq() {
    return CreateTableRequest.builder().withName("t").withSchema(SCHEMA).build();
  }

  private static RegisterTableRequest registerReq() {
    return ImmutableRegisterTableRequest.builder()
        .name("t")
        .metadataLocation("s3://b/db/t/metadata/0.json")
        .build();
  }
}
