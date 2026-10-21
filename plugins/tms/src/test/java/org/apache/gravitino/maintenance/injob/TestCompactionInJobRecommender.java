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

import java.util.Collections;
import java.util.Optional;
import org.apache.gravitino.maintenance.scheduler.PolicyLookup;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.PolicyContents;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link CompactionInJobRecommender} (delegates to CompactionTriggerEvaluator). */
class TestCompactionInJobRecommender {

  @Test
  void testRecommendUsesTriggerExpr() {
    PolicyEntity policy = mock(PolicyEntity.class);
    when(policy.content())
        .thenReturn(PolicyContents.icebergDataCompaction(500L, 3L, Collections.emptyMap()));
    PolicyLookup lookup = mock(PolicyLookup.class);
    when(lookup.findById(7L)).thenReturn(Optional.of(policy));

    CompactionInJobRecommender recommender = new CompactionInJobRecommender(lookup);
    String metrics =
        "{\"custom-data-file-mse\":600,\"custom-delete-file-number\":0,\"snapshotId\":1}";
    Assertions.assertEquals(
        RecommendDecision.MAINTAIN, recommender.recommend(1L, 7L, 99L, metrics));

    String below =
        "{\"custom-data-file-mse\":100,\"custom-delete-file-number\":1,\"snapshotId\":1}";
    Assertions.assertEquals(RecommendDecision.SKIP, recommender.recommend(1L, 7L, 99L, below));
  }

  @Test
  void testMissingMetricsSkips() {
    PolicyEntity policy = mock(PolicyEntity.class);
    when(policy.content()).thenReturn(PolicyContents.icebergDataCompaction());
    PolicyLookup lookup = mock(PolicyLookup.class);
    when(lookup.findById(7L)).thenReturn(Optional.of(policy));

    CompactionInJobRecommender recommender = new CompactionInJobRecommender(lookup);
    Assertions.assertEquals(
        RecommendDecision.SKIP,
        recommender.recommend(
            1L, 7L, 99L, "{\"snapshotId\":1,\"maintenanceType\":\"rewrite_data_files\"}"));
  }
}
