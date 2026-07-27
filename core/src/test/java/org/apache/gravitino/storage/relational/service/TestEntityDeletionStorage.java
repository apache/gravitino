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
package org.apache.gravitino.storage.relational.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.po.EntityDeletionAuditPO;
import org.apache.gravitino.storage.relational.po.EntityDeletionPO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.junit.jupiter.api.TestTemplate;

/** Cross-backend tests for deletion-generation and append-only audit storage. */
public class TestEntityDeletionStorage extends TestJDBCBackend {

  private static final long DELETED_AT = 1_784_800_000_000L;

  @TestTemplate
  public void testInsertAndGetDeletionAndAudit() {
    EntityDeletionPO deletion = newDeletion("deletion-1");
    EntityDeletionAuditPO audit = newAudit("audit-1", deletion.getDeletionId());

    SessionUtils.doMultipleWithCommit(
        () -> EntityDeletionService.getInstance().insert(deletion),
        () -> EntityDeletionAuditService.getInstance().insert(audit));

    EntityDeletionPO loaded = EntityDeletionService.getInstance().get(deletion.getDeletionId());
    assertNotNull(loaded);
    assertEquals("TABLE", loaded.getEntityType());
    assertEquals(984273L, loaded.getEntityId());
    assertEquals(17L, loaded.getEntityVersion());
    assertEquals("demo.sales", loaded.getNamespaceSnapshot());
    assertEquals("orders", loaded.getEntityNameSnapshot());
    assertEquals("DELETED", loaded.getState());
    assertEquals(DELETED_AT + 86_400_000L, loaded.getRetentionExpiresAt());
    assertFalse(loaded.getPurgeRequested());
    assertEquals("ICEBERG_REST_PURGE", loaded.getPurgeJobType());
    assertEquals("PENDING", loaded.getCleanupStatus());
    assertEquals(0, loaded.getCleanupAttemptCount());
    assertNull(loaded.getPurgeJobId());
    assertNull(loaded.getCleanupLastError());
    assertNull(loaded.getAcceptedRestoreEtag());

    EntityDeletionAuditPO loadedAudit =
        EntityDeletionAuditService.getInstance().get(audit.getAuditId());
    assertNotNull(loadedAudit);
    assertEquals(deletion.getDeletionId(), loadedAudit.getDeletionId());
    assertEquals("DELETE_ACCEPTED", loadedAudit.getEventType());
    assertEquals("DELETED", loadedAudit.getNewState());
    assertEquals("PENDING", loadedAudit.getNewCleanupStatus());
    assertEquals("alice", loadedAudit.getActor());
    assertEquals("correlation-1", loadedAudit.getCorrelationId());
  }

  @TestTemplate
  public void testServicesJoinOuterTransactionRollback() {
    EntityDeletionPO deletion = newDeletion("deletion-rollback");
    EntityDeletionAuditPO audit = newAudit("audit-rollback", deletion.getDeletionId());

    assertThrows(
        IllegalStateException.class,
        () ->
            SessionUtils.doMultipleWithCommit(
                () -> EntityDeletionService.getInstance().insert(deletion),
                () -> EntityDeletionAuditService.getInstance().insert(audit),
                () -> {
                  throw new IllegalStateException("force rollback");
                }));

    assertNull(EntityDeletionService.getInstance().get(deletion.getDeletionId()));
    assertNull(EntityDeletionAuditService.getInstance().get(audit.getAuditId()));
  }

  @TestTemplate
  public void testNullableRetentionRoundTrip() {
    EntityDeletionPO deletion = newDeletion("deletion-immediate");
    deletion.setRetentionExpiresAt(null);
    deletion.setCleanupStatus(null);

    EntityDeletionService.getInstance().insert(deletion);

    EntityDeletionPO loaded = EntityDeletionService.getInstance().get(deletion.getDeletionId());
    assertNotNull(loaded);
    assertNull(loaded.getRetentionExpiresAt());
    assertNull(loaded.getCleanupStatus());
  }

  private static EntityDeletionPO newDeletion(String deletionId) {
    return EntityDeletionPO.builder()
        .deletionId(deletionId)
        .entityType("TABLE")
        .entityId(984273L)
        .entityVersion(17L)
        .metalakeId(100L)
        .catalogId(200L)
        .parentId(300L)
        .namespaceSnapshot("demo.sales")
        .entityNameSnapshot("orders")
        .activeNameKey("0123456789abcdef")
        .state("DELETED")
        .revision(0L)
        .deletedAt(DELETED_AT)
        .retentionExpiresAt(DELETED_AT + 86_400_000L)
        .deletedBy("alice")
        .purgeRequested(false)
        .purgeJobType("ICEBERG_REST_PURGE")
        .cleanupStatus("PENDING")
        .cleanupAttemptCount(0)
        .requestId("request-1")
        .correlationId("correlation-1")
        .updatedAt(DELETED_AT)
        .build();
  }

  private static EntityDeletionAuditPO newAudit(String auditId, String deletionId) {
    return EntityDeletionAuditPO.builder()
        .auditId(auditId)
        .deletionId(deletionId)
        .entityType("TABLE")
        .entityId(984273L)
        .eventType("DELETE_ACCEPTED")
        .actionRevision(0L)
        .newState("DELETED")
        .newCleanupStatus("PENDING")
        .actor("alice")
        .requestId("request-1")
        .correlationId("correlation-1")
        .createdAt(DELETED_AT)
        .build();
  }
}
