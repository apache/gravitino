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
package org.apache.gravitino.maintenance.listener;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.OperationStatus;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.maintenance.scheduler.job.commit.CommitJobClient;
import org.apache.gravitino.maintenance.scheduler.job.commit.CommitJobEnqueueResult;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestPostCommitMaintenanceListener {

  @Test
  void testExpectedEventClassNameConstant() {
    Assertions.assertEquals(
        "org.apache.gravitino.listener.api.event.IcebergUpdateTableEvent",
        PostCommitMaintenanceListener.ICEBERG_UPDATE_TABLE_EVENT);
  }

  @Test
  void testIgnoresUnrelatedEvents() {
    PostCommitMaintenanceListener listener = new PostCommitMaintenanceListener();
    CommitJobClient scheduler = mock(CommitJobClient.class);
    listener.setCommitJobClient(scheduler);

    listener.onPostEvent(new OtherEvent("u", NameIdentifier.of("ml", "c", "s", "t")));

    verify(scheduler, never()).enqueueNow(anyLong(), any());
  }

  @Test
  void testEnqueueUpsertsCommitJob() {
    PostCommitMaintenanceListener listener = new PostCommitMaintenanceListener();
    CommitJobClient scheduler = mock(CommitJobClient.class);
    NameIdentifier identifier = NameIdentifier.of("ml", "c", "s", "t");
    when(scheduler.enqueueNow(eq(42L), eq(identifier))).thenReturn(CommitJobEnqueueResult.INSERTED);
    listener.setCommitJobClient(scheduler);
    listener.setTableLookup(
        ident ->
            TableEntity.builder()
                .withId(42L)
                .withName("t")
                .withNamespace(NamespaceUtil.ofTable("ml", "c", "s"))
                .withAuditInfo(
                    AuditInfo.builder().withCreator("test").withCreateTime(Instant.EPOCH).build())
                .build());

    listener.enqueueCommitJob(identifier);

    verify(scheduler).enqueueNow(42L, identifier);
  }

  @Test
  void testEnqueueSkippedWhenSchedulerNotStarted() {
    PostCommitMaintenanceListener listener = new PostCommitMaintenanceListener();
    listener.setTableLookup(
        ident ->
            TableEntity.builder()
                .withId(1L)
                .withName("t")
                .withNamespace(NamespaceUtil.ofTable("ml", "c", "s"))
                .withAuditInfo(
                    AuditInfo.builder().withCreator("test").withCreateTime(Instant.EPOCH).build())
                .build());

    Assertions.assertDoesNotThrow(
        () -> listener.enqueueCommitJob(NameIdentifier.of("ml", "c", "s", "t")));
  }

  private static final class OtherEvent extends Event {
    OtherEvent(String user, NameIdentifier identifier) {
      super(user, identifier);
    }

    @Override
    public OperationType operationType() {
      return OperationType.CREATE_TABLE;
    }

    @Override
    public OperationStatus operationStatus() {
      return OperationStatus.SUCCESS;
    }
  }
}
