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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.po.EntityDeletionPO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.junit.jupiter.api.TestTemplate;

/** Cross-backend tests for active deletion-generation storage. */
public class TestEntityDeletionStorage extends TestJDBCBackend {

  private static final long RETENTION_EXPIRES_AT = 1_784_886_400_000L;

  @TestTemplate
  public void testInsertAndGetDeletion() {
    EntityDeletionPO deletion = newDeletion("deletion-1");
    insert(deletion);

    EntityDeletionPO loaded = EntityDeletionService.getInstance().get(deletion.getDeletionId());
    assertNotNull(loaded);
    assertEquals(deletion.getDeletionId(), loaded.getDeletionId());
    assertEquals("DELETED", loaded.getState());
    assertEquals(RETENTION_EXPIRES_AT, loaded.getRetentionExpiresAt());
    assertNull(loaded.getPurgeJobId());
  }

  @TestTemplate
  public void testPurgeJobIdRoundTrip() {
    EntityDeletionPO deletion = newDeletion("deletion-job");
    deletion.setState("PURGING");
    deletion.setPurgeJobId("purge-job-1");

    insert(deletion);

    EntityDeletionPO loaded = EntityDeletionService.getInstance().get(deletion.getDeletionId());
    assertNotNull(loaded);
    assertEquals("PURGING", loaded.getState());
    assertEquals("purge-job-1", loaded.getPurgeJobId());
  }

  @TestTemplate
  public void testMissingDeletionReturnsNull() {
    assertNull(EntityDeletionService.getInstance().get("missing-deletion"));
  }

  @TestTemplate
  public void testInsertRollsBackWithCallerTransaction() {
    EntityDeletionPO deletion = newDeletion("deletion-rollback");

    assertThrows(
        IllegalStateException.class,
        () ->
            SessionUtils.doMultipleWithCommit(
                () -> EntityDeletionService.getInstance().insertWithoutCommit(deletion),
                () -> {
                  throw new IllegalStateException("force rollback");
                }));

    assertNull(EntityDeletionService.getInstance().get(deletion.getDeletionId()));
  }

  @TestTemplate
  public void testDuplicateDeletionIdRejected() {
    EntityDeletionPO deletion = newDeletion("duplicate-deletion");
    insert(deletion);

    assertThrows(RuntimeException.class, () -> insert(deletion));
  }

  private static void insert(EntityDeletionPO deletion) {
    SessionUtils.doMultipleWithCommit(
        () -> EntityDeletionService.getInstance().insertWithoutCommit(deletion));
  }

  private static EntityDeletionPO newDeletion(String deletionId) {
    return EntityDeletionPO.builder()
        .deletionId(deletionId)
        .state("DELETED")
        .retentionExpiresAt(RETENTION_EXPIRES_AT)
        .build();
  }
}
