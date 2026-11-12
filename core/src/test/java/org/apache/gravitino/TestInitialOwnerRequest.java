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
package org.apache.gravitino;

import java.time.Instant;
import java.util.Map;
import org.apache.gravitino.catalog.EntityCombinedTable;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Tests stable creation identity and ownership request copying. */
public class TestInitialOwnerRequest {
  @Test
  void testConditionSurvivesAuditCopyAndCannotBeUsedForOtherRelations() {
    RelationUpdate initial =
        RelationUpdate.of(
                SupportsRelationOperations.Type.OWNER_REL,
                NameIdentifier.of("m", "c", "s", "t"),
                Entity.EntityType.TABLE,
                new RelationEdgeTarget[0],
                new RelationEdgeTarget[0])
            .withExpectedSourceId(42L);
    Assertions.assertEquals(
        42L,
        initial
            .withAuditInfo(
                AuditInfo.builder().withCreator("u").withCreateTime(Instant.now()).build())
            .expectedSourceId()
            .get());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            RelationUpdate.of(
                    SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
                    initial.sourceIdentifier(),
                    initial.sourceEntityType(),
                    new RelationEdgeTarget[0],
                    new RelationEdgeTarget[0])
                .withExpectedSourceId(42L));
  }

  @Test
  void testCreationSnapshotUsesStoredIdentityRatherThanExternalProperties() {
    Table raw = Mockito.mock(Table.class);
    Mockito.when(raw.properties())
        .thenReturn(StringIdentifier.newPropertiesWithId(StringIdentifier.fromId(99L), Map.of()));
    TableEntity entity =
        TableEntity.builder()
            .withId(42L)
            .withName("t")
            .withNamespace(Namespace.of("m", "c", "s"))
            .withAuditInfo(
                AuditInfo.builder().withCreator("u").withCreateTime(Instant.now()).build())
            .build();
    Assertions.assertEquals(42L, EntityCombinedTable.of(raw, entity).id());
    Assertions.assertEquals(99L, EntityCombinedTable.of(raw).id());
    Mockito.when(raw.properties()).thenReturn(Map.of());
    Assertions.assertThrows(IllegalStateException.class, () -> EntityCombinedTable.of(raw).id());
  }
}
