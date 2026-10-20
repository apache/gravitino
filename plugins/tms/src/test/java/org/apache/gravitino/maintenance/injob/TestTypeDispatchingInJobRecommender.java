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
import org.apache.gravitino.maintenance.scheduler.PolicyLookup;
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
    PolicyLookup lookup = mock(PolicyLookup.class);
    when(lookup.findById(1L)).thenReturn(Optional.of(policy));
    InJobRecommender compaction = mock(InJobRecommender.class);
    when(compaction.recommend(eq(2L), eq(1L), eq(9L), anyString(), eq(policy)))
        .thenReturn(RecommendDecision.SKIP);
    InJobRecommender triggerExpr = mock(InJobRecommender.class);

    TypeDispatchingInJobRecommender dispatcher =
        new TypeDispatchingInJobRecommender(lookup, compaction, triggerExpr);
    Assertions.assertEquals(RecommendDecision.SKIP, dispatcher.recommend(2L, 1L, 9L, "{\"x\":1}"));
    verify(compaction).recommend(2L, 1L, 9L, "{\"x\":1}", policy);
    verify(triggerExpr, never())
        .recommend(anyLong(), anyLong(), anyLong(), anyString(), any(PolicyEntity.class));
    verify(triggerExpr, never()).recommend(anyLong(), anyLong(), anyLong(), anyString());
  }

  @Test
  void testDispatchesTriggerExprForExpire() {
    PolicyEntity policy = mock(PolicyEntity.class);
    when(policy.policyType()).thenReturn(Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION);
    when(policy.content()).thenReturn(PolicyContents.icebergSnapshotExpiration());
    PolicyLookup lookup = mock(PolicyLookup.class);
    when(lookup.findById(1L)).thenReturn(Optional.of(policy));
    InJobRecommender compaction = mock(InJobRecommender.class);
    InJobRecommender triggerExpr = mock(InJobRecommender.class);
    when(triggerExpr.recommend(eq(2L), eq(1L), eq(9L), anyString(), eq(policy)))
        .thenReturn(RecommendDecision.SKIP);

    TypeDispatchingInJobRecommender dispatcher =
        new TypeDispatchingInJobRecommender(lookup, compaction, triggerExpr);
    Assertions.assertEquals(RecommendDecision.SKIP, dispatcher.recommend(2L, 1L, 9L, "{}"));
    verify(triggerExpr).recommend(2L, 1L, 9L, "{}", policy);
    verify(compaction, never())
        .recommend(anyLong(), anyLong(), anyLong(), anyString(), any(PolicyEntity.class));
    verify(compaction, never()).recommend(anyLong(), anyLong(), anyLong(), anyString());
  }

  @Test
  void testDispatchesTriggerExprForOrphan() {
    PolicyEntity policy = mock(PolicyEntity.class);
    when(policy.policyType()).thenReturn(Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL);
    when(policy.content()).thenReturn(PolicyContents.icebergOrphanFileRemoval());
    PolicyLookup lookup = mock(PolicyLookup.class);
    when(lookup.findById(1L)).thenReturn(Optional.of(policy));

    TriggerExprInJobRecommender triggerExpr = new TriggerExprInJobRecommender(lookup);
    TypeDispatchingInJobRecommender dispatcher =
        new TypeDispatchingInJobRecommender(lookup, mock(InJobRecommender.class), triggerExpr);

    Assertions.assertEquals(RecommendDecision.MAINTAIN, dispatcher.recommend(2L, 1L, 9L, "{}"));
  }

  @Test
  void testCompactionRecommenderReusesLoadedPolicy() {
    PolicyEntity policy = mock(PolicyEntity.class);
    when(policy.policyType()).thenReturn(Policy.BuiltInType.ICEBERG_COMPACTION);
    when(policy.content())
        .thenReturn(PolicyContents.icebergDataCompaction(500L, 3L, Collections.emptyMap()));
    PolicyLookup lookup = mock(PolicyLookup.class);
    when(lookup.findById(7L)).thenReturn(Optional.of(policy));

    CompactionInJobRecommender compaction = new CompactionInJobRecommender(lookup);
    TypeDispatchingInJobRecommender dispatcher =
        new TypeDispatchingInJobRecommender(
            lookup, compaction, new TriggerExprInJobRecommender(lookup));

    String metrics =
        "{\"custom-data-file-mse\":600,\"custom-delete-file-number\":0,\"snapshotId\":1}";
    Assertions.assertEquals(RecommendDecision.MAINTAIN, dispatcher.recommend(1L, 7L, 99L, metrics));
    // Type-dispatch loads once; compaction must not look up again.
    verify(lookup, times(1)).findById(7L);
  }
}
