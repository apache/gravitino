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
package org.apache.gravitino.maintenance.injob;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
import java.util.function.Function;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link TypeDispatchingInJobRecommender}. */
class TestTypeDispatchingInJobRecommender {

  @Test
  void testDispatchesCompaction() {
    PolicyEntity policy = mock(PolicyEntity.class);
    when(policy.policyType()).thenReturn(Policy.BuiltInType.ICEBERG_COMPACTION);
    when(policy.content()).thenReturn(PolicyContents.icebergDataCompaction());
    Function<Long, Optional<PolicyEntity>> policies = policiesReturning(policy);
    InJobRecommender compaction = mock(InJobRecommender.class);
    when(compaction.recommend(eq(2L), eq(1L), eq(9L), anyString(), eq(policy)))
        .thenReturn(RecommendDecision.SKIP);
    InJobRecommender triggerExpr = mock(InJobRecommender.class);
    InJobRecommender placeholder = mock(InJobRecommender.class);

    TypeDispatchingInJobRecommender dispatcher =
        new TypeDispatchingInJobRecommender(policies, compaction, triggerExpr, placeholder);
    Assertions.assertEquals(RecommendDecision.SKIP, dispatcher.recommend(2L, 1L, 9L, "{\"x\":1}"));
    verify(compaction).recommend(2L, 1L, 9L, "{\"x\":1}", policy);
    verify(triggerExpr, never())
        .recommend(anyLong(), anyLong(), anyLong(), anyString(), any(PolicyEntity.class));
    verify(placeholder, never())
        .recommend(anyLong(), anyLong(), anyLong(), anyString(), any(PolicyEntity.class));
  }

  @Test
  void testDispatchesAlwaysMaintainForExpirePlaceholder() {
    PolicyEntity policy = mock(PolicyEntity.class);
    when(policy.policyType()).thenReturn(Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION);
    when(policy.content()).thenReturn(PolicyContents.icebergSnapshotExpiration());
    Function<Long, Optional<PolicyEntity>> policies = policiesReturning(policy);
    InJobRecommender compaction = mock(InJobRecommender.class);
    InJobRecommender triggerExpr = mock(InJobRecommender.class);
    InJobRecommender placeholder = mock(InJobRecommender.class);
    when(placeholder.recommend(eq(2L), eq(1L), eq(9L), anyString(), eq(policy)))
        .thenReturn(RecommendDecision.MAINTAIN);

    TypeDispatchingInJobRecommender dispatcher =
        new TypeDispatchingInJobRecommender(policies, compaction, triggerExpr, placeholder);
    Assertions.assertEquals(RecommendDecision.MAINTAIN, dispatcher.recommend(2L, 1L, 9L, "{}"));
    verify(placeholder).recommend(2L, 1L, 9L, "{}", policy);
    verify(triggerExpr, never())
        .recommend(anyLong(), anyLong(), anyLong(), anyString(), any(PolicyEntity.class));
    verify(compaction, never())
        .recommend(anyLong(), anyLong(), anyLong(), anyString(), any(PolicyEntity.class));
  }

  @Test
  void testProductionWiringExpirePlaceholderMaintainsDespiteTriggerFalse() {
    PolicyEntity policy = mock(PolicyEntity.class);
    when(policy.policyType()).thenReturn(Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION);
    when(policy.content()).thenReturn(PolicyContents.icebergSnapshotExpiration());
    Function<Long, Optional<PolicyEntity>> policies = policiesReturning(policy);

    TypeDispatchingInJobRecommender dispatcher =
        new TypeDispatchingInJobRecommender(
            policies,
            new CompactionInJobRecommender(policies),
            new TriggerExprInJobRecommender(policies),
            AlwaysMaintainInJobRecommender.INSTANCE);

    Assertions.assertEquals(RecommendDecision.MAINTAIN, dispatcher.recommend(2L, 1L, 9L, "{}"));
  }

  @Test
  void testDispatchesTriggerExprForOrphan() {
    PolicyEntity policy = mock(PolicyEntity.class);
    when(policy.policyType()).thenReturn(Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL);
    when(policy.content()).thenReturn(PolicyContents.icebergOrphanFileRemoval());
    Function<Long, Optional<PolicyEntity>> policies = policiesReturning(policy);

    TriggerExprInJobRecommender triggerExpr = new TriggerExprInJobRecommender(policies);
    TypeDispatchingInJobRecommender dispatcher =
        new TypeDispatchingInJobRecommender(policies, mock(InJobRecommender.class), triggerExpr);

    Assertions.assertEquals(RecommendDecision.MAINTAIN, dispatcher.recommend(2L, 1L, 9L, "{}"));
  }

  @Test
  void testCompactionRecommenderReusesLoadedPolicy() {
    PolicyEntity policy = mock(PolicyEntity.class);
    when(policy.policyType()).thenReturn(Policy.BuiltInType.ICEBERG_COMPACTION);
    when(policy.content())
        .thenReturn(PolicyContents.icebergDataCompaction(500L, 3L, Collections.emptyMap()));
    @SuppressWarnings("unchecked")
    Function<Long, Optional<PolicyEntity>> policies = mock(Function.class);
    when(policies.apply(7L)).thenReturn(Optional.of(policy));

    CompactionInJobRecommender compaction = new CompactionInJobRecommender(policies);
    TypeDispatchingInJobRecommender dispatcher =
        new TypeDispatchingInJobRecommender(
            policies, compaction, new TriggerExprInJobRecommender(policies));

    String metrics =
        "{\"custom-data-file-mse\":600,\"custom-delete-file-number\":0,\"snapshotId\":1}";
    Assertions.assertEquals(RecommendDecision.MAINTAIN, dispatcher.recommend(1L, 7L, 99L, metrics));
    // Type-dispatch loads once; compaction must not look up again.
    verify(policies, times(1)).apply(7L);
  }

  @SuppressWarnings("unchecked")
  private static Function<Long, Optional<PolicyEntity>> policiesReturning(PolicyEntity policy) {
    Function<Long, Optional<PolicyEntity>> policies = mock(Function.class);
    when(policies.apply(anyLong())).thenReturn(Optional.of(policy));
    return policies;
  }
}
