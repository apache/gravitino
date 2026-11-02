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
package org.apache.gravitino.maintenance.scheduler.policy.expand;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.ObjectPolicyResolver;
import org.apache.gravitino.policy.ObjectPolicyResolver.Match;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestNearestMaintenancePolicyResolver {

  private static final String METALAKE = "metalake";
  private static final MetadataObject TABLE =
      MetadataObjects.of(
          java.util.Arrays.asList("catalog", "schema", "table"), MetadataObject.Type.TABLE);
  private static final MetadataObject CATALOG =
      MetadataObjects.of(
          java.util.Collections.singletonList("catalog"), MetadataObject.Type.CATALOG);

  private ObjectPolicyResolver objectPolicyResolver;
  private NearestMaintenancePolicyResolver resolver;

  @BeforeEach
  void setUp() {
    objectPolicyResolver = mock(ObjectPolicyResolver.class);
    resolver = new NearestMaintenancePolicyResolver(objectPolicyResolver);
  }

  @Test
  void testTableLevelBeatsCatalogLevel() {
    PolicyEntity tablePolicy = policy(10L, "table-compaction");
    PolicyEntity catalogPolicy = policy(20L, "catalog-compaction");
    when(objectPolicyResolver.resolveMatches(METALAKE, TABLE))
        .thenReturn(
            new Match[] {
              new Match(tablePolicy, List.of(TABLE)), new Match(catalogPolicy, List.of(CATALOG))
            });

    NearestMaintenancePolicyResolver.Decision decision =
        resolver.resolve(METALAKE, TABLE, Policy.BuiltInType.ICEBERG_COMPACTION);
    Assertions.assertEquals(NearestMaintenancePolicyResolver.Decision.Kind.WINNER, decision.kind());
    Assertions.assertEquals(java.util.Optional.of(10L), decision.winnerPolicyId());
    Assertions.assertEquals(TABLE, decision.winnerLevel());
  }

  @Test
  void testFallsBackToAncestorWhenTableHasNoMatch() {
    PolicyEntity catalogPolicy = policy(20L, "catalog-compaction");
    when(objectPolicyResolver.resolveMatches(METALAKE, TABLE))
        .thenReturn(new Match[] {new Match(catalogPolicy, List.of(CATALOG))});

    NearestMaintenancePolicyResolver.Decision decision =
        resolver.resolve(METALAKE, TABLE, Policy.BuiltInType.ICEBERG_COMPACTION);
    Assertions.assertEquals(NearestMaintenancePolicyResolver.Decision.Kind.WINNER, decision.kind());
    Assertions.assertEquals(java.util.Optional.of(20L), decision.winnerPolicyId());
    Assertions.assertEquals(CATALOG, decision.winnerLevel());
  }

  @Test
  void testEmptyWhenNoMatchingType() {
    when(objectPolicyResolver.resolveMatches(METALAKE, TABLE)).thenReturn(new Match[0]);

    NearestMaintenancePolicyResolver.Decision decision =
        resolver.resolve(METALAKE, TABLE, Policy.BuiltInType.ICEBERG_COMPACTION);
    Assertions.assertEquals(NearestMaintenancePolicyResolver.Decision.Kind.NONE, decision.kind());
  }

  @Test
  void testSameLevelConflict() {
    PolicyEntity p1 = policy(10L, "compaction-a");
    PolicyEntity p2 = policy(11L, "compaction-b");
    when(objectPolicyResolver.resolveMatches(METALAKE, TABLE))
        .thenReturn(new Match[] {new Match(p1, List.of(TABLE)), new Match(p2, List.of(TABLE))});

    NearestMaintenancePolicyResolver.Decision decision =
        resolver.resolve(METALAKE, TABLE, Policy.BuiltInType.ICEBERG_COMPACTION);
    Assertions.assertEquals(
        NearestMaintenancePolicyResolver.Decision.Kind.CONFLICT, decision.kind());
    Assertions.assertEquals(TABLE, decision.conflictLevel());
    Assertions.assertEquals(List.of(10L, 11L), decision.conflictingPolicyIds());
    Assertions.assertTrue(decision.winnerPolicyId().isEmpty());
  }

  @Test
  void testIgnoresNonMatchingBuiltInType() {
    PolicyEntity custom =
        PolicyEntity.builder()
            .withId(99L)
            .withName("custom")
            .withNamespace(NamespaceUtil.ofPolicy(METALAKE))
            .withPolicyType(Policy.BuiltInType.CUSTOM)
            .withEnabled(true)
            .withContent(
                PolicyContents.custom(
                    java.util.Map.of("k", "v"), java.util.Set.of(MetadataObject.Type.TABLE), null))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();
    when(objectPolicyResolver.resolveMatches(METALAKE, TABLE))
        .thenReturn(new Match[] {new Match(custom, List.of(TABLE))});

    NearestMaintenancePolicyResolver.Decision decision =
        resolver.resolve(METALAKE, TABLE, Policy.BuiltInType.ICEBERG_COMPACTION);
    Assertions.assertEquals(NearestMaintenancePolicyResolver.Decision.Kind.NONE, decision.kind());
  }

  private static PolicyEntity policy(long id, String name) {
    return PolicyEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(NamespaceUtil.ofPolicy(METALAKE))
        .withPolicyType(Policy.BuiltInType.ICEBERG_COMPACTION)
        .withEnabled(true)
        .withContent(
            PolicyContents.icebergDataCompaction(
                IcebergDataCompactionContent.DEFAULT_MIN_DATA_FILE_MSE,
                IcebergDataCompactionContent.DEFAULT_MIN_DELETE_FILE_NUMBER,
                IcebergDataCompactionContent.DEFAULT_DATA_FILE_MSE_WEIGHT,
                IcebergDataCompactionContent.DEFAULT_DELETE_FILE_NUMBER_WEIGHT,
                IcebergDataCompactionContent.DEFAULT_MAX_PARTITION_NUM,
                IcebergDataCompactionContent.DEFAULT_REWRITE_STRATEGY,
                IcebergDataCompactionContent.DEFAULT_SORT_ORDER,
                null,
                "0 0 2 * * ?",
                null,
                null,
                null,
                null))
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
        .build();
  }
}
