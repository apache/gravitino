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

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    when(compaction.recommend(eq(2L), eq(1L), eq(9L), anyString()))
        .thenReturn(RecommendDecision.SKIP);
    InJobRecommender fallback = mock(InJobRecommender.class);

    TypeDispatchingInJobRecommender dispatcher =
        new TypeDispatchingInJobRecommender(lookup, compaction, fallback);
    Assertions.assertEquals(RecommendDecision.SKIP, dispatcher.recommend(2L, 1L, 9L, "{\"x\":1}"));
    verify(compaction).recommend(2L, 1L, 9L, "{\"x\":1}");
    verify(fallback, never()).recommend(anyLong(), anyLong(), anyLong(), anyString());
  }

  @Test
  void testFallsBackForExpire() {
    PolicyEntity policy = mock(PolicyEntity.class);
    when(policy.policyType()).thenReturn(Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION);
    when(policy.content()).thenReturn(PolicyContents.icebergSnapshotExpiration());
    PolicyLookup lookup = mock(PolicyLookup.class);
    when(lookup.findById(1L)).thenReturn(Optional.of(policy));
    InJobRecommender compaction = mock(InJobRecommender.class);
    InJobRecommender fallback = mock(InJobRecommender.class);
    when(fallback.recommend(eq(2L), eq(1L), eq(9L), anyString()))
        .thenReturn(RecommendDecision.MAINTAIN);

    TypeDispatchingInJobRecommender dispatcher =
        new TypeDispatchingInJobRecommender(lookup, compaction, fallback);
    Assertions.assertEquals(RecommendDecision.MAINTAIN, dispatcher.recommend(2L, 1L, 9L, "{}"));
    verify(fallback).recommend(2L, 1L, 9L, "{}");
    verify(compaction, never()).recommend(anyLong(), anyLong(), anyLong(), anyString());
  }
}
