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
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.gravitino.iceberg.service.purge.IcebergPurgeJob;
import org.apache.gravitino.iceberg.service.purge.IcebergPurgeService;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

class TestIcebergTableOperationExecutorAsyncPurge {

  @BeforeEach
  void setUp() {
    IcebergConfigProvider configProvider = mock(IcebergConfigProvider.class);
    when(configProvider.getMetalakeName()).thenReturn("metalake");
    when(configProvider.getDefaultCatalogName()).thenReturn("cat");
    IcebergRESTServerContext.create(configProvider, false, false, true, null);
  }

  private IcebergTableOperationExecutor newExec(
      CatalogWrapperForREST wrapper, IcebergPurgeService purgeService) {
    IcebergCatalogWrapperManager manager = mock(IcebergCatalogWrapperManager.class);
    when(manager.getCatalogWrapper("cat")).thenReturn(wrapper);
    return new IcebergTableOperationExecutor(manager, purgeService);
  }

  @Test
  void testAsyncWhenHeaderTrueEnqueuesOneJob() {
    CatalogWrapperForREST wrapper = mock(CatalogWrapperForREST.class);
    IcebergPurgeService purgeService = mock(IcebergPurgeService.class);
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.metadataFileLocation()).thenReturn("s3://bucket/db/t/metadata/00000.json");
    when(wrapper.loadTableMetadata(any())).thenReturn(metadata);
    when(wrapper.fileIoImpl()).thenReturn("io");
    when(wrapper.fileIoProperties()).thenReturn(Collections.emptyMap());

    IcebergRequestContext context = mock(IcebergRequestContext.class);
    when(context.catalogName()).thenReturn("cat");
    when(context.userName()).thenReturn("alice");
    when(context.asyncPurge()).thenReturn(true);

    TableIdentifier identifier = TableIdentifier.of("db", "t");
    newExec(wrapper, purgeService).dropTable(context, identifier, true);

    InOrder ordered = inOrder(wrapper, purgeService);
    ordered.verify(wrapper).loadTableMetadata(identifier);
    ordered.verify(wrapper).dropTable(identifier);
    ordered.verify(purgeService, times(1)).enqueue(any());

    ArgumentCaptor<IcebergPurgeJob> jobCaptor = ArgumentCaptor.forClass(IcebergPurgeJob.class);
    verify(purgeService).enqueue(jobCaptor.capture());
    IcebergPurgeJob job = jobCaptor.getValue();
    Assertions.assertEquals("metalake", job.metalakeName());
    Assertions.assertEquals("cat", job.catalogName());
    Assertions.assertEquals("db", job.namespace());
    Assertions.assertEquals("t", job.tableName());
    Assertions.assertEquals("s3://bucket/db/t/metadata/00000.json", job.metadataLocation());
    Assertions.assertEquals("io", job.fileIoImpl());
    Assertions.assertEquals("alice", job.createdBy());
  }

  @Test
  void testDefaultIsSynchronousPurge() {
    CatalogWrapperForREST wrapper = mock(CatalogWrapperForREST.class);
    IcebergPurgeService purgeService = mock(IcebergPurgeService.class);
    IcebergRequestContext context = mock(IcebergRequestContext.class);
    when(context.catalogName()).thenReturn("cat");
    when(context.asyncPurge()).thenReturn(false);

    TableIdentifier identifier = TableIdentifier.of("db", "t");
    newExec(wrapper, purgeService).dropTable(context, identifier, true);

    verify(wrapper).purgeTable(identifier);
    verify(purgeService, never()).enqueue(any());
  }

  @Test
  void testNonPurgeDropIsPlainDrop() {
    CatalogWrapperForREST wrapper = mock(CatalogWrapperForREST.class);
    IcebergPurgeService purgeService = mock(IcebergPurgeService.class);
    IcebergRequestContext context = mock(IcebergRequestContext.class);
    when(context.catalogName()).thenReturn("cat");

    TableIdentifier identifier = TableIdentifier.of("db", "t");
    newExec(wrapper, purgeService).dropTable(context, identifier, false);

    verify(wrapper).dropTable(identifier);
    verify(purgeService, never()).enqueue(any());
  }
}
