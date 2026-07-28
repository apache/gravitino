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
package org.apache.gravitino.iceberg.service.deletion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.apache.gravitino.storage.relational.po.EntityDeletionPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.junit.jupiter.api.Test;

/** Tests strong validators for public deletion-action representations. */
public class TestIcebergDeletionETags {

  @Test
  public void testNonPublicRootFieldsDoNotChangePublicEtag() {
    IcebergRetainedTableDeletion retained = retained("created-by-alice", "{}");
    IcebergRetainedTableDeletion changedStorageFields =
        retained("created-by-bob", "{\"storage-only\":true}");

    assertEquals(
        IcebergDeletionETags.strongTag(retained, 100L),
        IcebergDeletionETags.strongTag(changedStorageFields, 100L));
  }

  @Test
  public void testEveryMutablePublicLifecycleFieldChangesEtag() {
    IcebergRetainedTableDeletion retained = retained();
    String original = IcebergDeletionETags.strongTag(retained, 100L);

    retained.getDeletion().setState(IcebergTableDeletionLifecycle.PURGING);
    assertNotEquals(original, IcebergDeletionETags.strongTag(retained, 100L));

    retained = retained();
    retained.getDeletion().setPurgeJobId("job-1");
    assertNotEquals(original, IcebergDeletionETags.strongTag(retained, 100L));

    retained = retained(42L, "renamed-corrupt-root", 3L, 50L);
    assertNotEquals(original, IcebergDeletionETags.strongTag(retained, 100L));
  }

  @Test
  public void testJoinedTableRootParticipatesInEtag() {
    IcebergRetainedTableDeletion retained = retained();
    String original = IcebergDeletionETags.strongTag(retained, 100L);

    assertNotEquals(
        original, IcebergDeletionETags.strongTag(retained(43L, "orders", 3L, 50L), 100L));
    assertNotEquals(
        original, IcebergDeletionETags.strongTag(retained(42L, "orders", 4L, 50L), 100L));
    assertNotEquals(
        original, IcebergDeletionETags.strongTag(retained(42L, "orders", 3L, 51L), 100L));
  }

  @Test
  public void testRetentionBoundaryChangesRecoverableRepresentationAndEtag() {
    IcebergRetainedTableDeletion retained = retained();

    assertNotEquals(
        IcebergDeletionETags.strongTag(retained, 199L),
        IcebergDeletionETags.strongTag(retained, 200L));
  }

  @Test
  public void testNullRetentionDoesNotAcquireATimeBoundary() {
    IcebergRetainedTableDeletion retained = retained();
    retained.getDeletion().setRetentionExpiresAt(null);

    assertEquals(
        IcebergDeletionETags.strongTag(retained, 100L),
        IcebergDeletionETags.strongTag(retained, Long.MAX_VALUE));
  }

  private static IcebergRetainedTableDeletion retained() {
    return retained(42L, "orders", 3L, 50L);
  }

  private static IcebergRetainedTableDeletion retained(
      long tableId, String tableName, long version, long deletedAt) {
    return retained(tableId, tableName, version, deletedAt, "created-by-alice", "{}");
  }

  private static IcebergRetainedTableDeletion retained(String auditInfo, String properties) {
    return retained(42L, "orders", 3L, 50L, auditInfo, properties);
  }

  private static IcebergRetainedTableDeletion retained(
      long tableId,
      String tableName,
      long version,
      long deletedAt,
      String auditInfo,
      String properties) {
    TablePO table =
        TablePO.builder()
            .withTableId(tableId)
            .withTableName(tableName)
            .withMetalakeId(1L)
            .withCatalogId(2L)
            .withSchemaId(3L)
            .withAuditInfo(auditInfo)
            .withCurrentVersion(version)
            .withLastVersion(version)
            .withDeletedAt(deletedAt)
            .withDeletionId("D1")
            .withProperties(properties)
            .build();
    EntityDeletionPO deletion =
        EntityDeletionPO.builder()
            .deletionId("D1")
            .state(IcebergTableDeletionLifecycle.DELETED)
            .retentionExpiresAt(200L)
            .build();
    return IcebergRetainedTableDeletion.builder().table(table).deletion(deletion).build();
  }
}
