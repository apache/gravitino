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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.apache.gravitino.maintenance.scheduler.PolicyLookup;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.PolicyContents;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link TriggerExprInJobRecommender}. */
class TestTriggerExprInJobRecommender {

  @Test
  void testOrphanTriggerTrueMaintains() {
    PolicyEntity policy = mock(PolicyEntity.class);
    when(policy.content()).thenReturn(PolicyContents.icebergOrphanFileRemoval());
    PolicyLookup lookup = mock(PolicyLookup.class);
    when(lookup.findById(1L)).thenReturn(Optional.of(policy));

    TriggerExprInJobRecommender recommender = new TriggerExprInJobRecommender(lookup);
    Assertions.assertEquals(
        RecommendDecision.MAINTAIN, recommender.recommend(2L, 1L, 9L, "{}", policy));
  }

  @Test
  void testExpirePlaceholderTriggerFalseSkips() {
    PolicyEntity policy = mock(PolicyEntity.class);
    when(policy.content()).thenReturn(PolicyContents.icebergSnapshotExpiration());
    PolicyLookup lookup = mock(PolicyLookup.class);
    when(lookup.findById(1L)).thenReturn(Optional.of(policy));

    TriggerExprInJobRecommender recommender = new TriggerExprInJobRecommender(lookup);
    Assertions.assertEquals(
        RecommendDecision.SKIP, recommender.recommend(2L, 1L, 9L, "{}", policy));
  }

  @Test
  void testManifestPlaceholderTriggerFalseSkips() {
    PolicyEntity policy = mock(PolicyEntity.class);
    when(policy.content()).thenReturn(PolicyContents.icebergRewriteManifests());
    PolicyLookup lookup = mock(PolicyLookup.class);

    TriggerExprInJobRecommender recommender = new TriggerExprInJobRecommender(lookup);
    Assertions.assertEquals(
        RecommendDecision.SKIP, recommender.recommend(2L, 1L, 9L, "{}", policy));
  }
}
