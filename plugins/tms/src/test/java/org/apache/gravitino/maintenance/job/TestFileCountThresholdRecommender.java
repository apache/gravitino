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

package org.apache.gravitino.maintenance.job;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestFileCountThresholdRecommender {

  @Test
  void testMaintainWhenFileCountMeetsThreshold() {
    FileCountThresholdRecommender recommender = new FileCountThresholdRecommender(10);
    Assertions.assertEquals(
        MaintenanceRecommendDecision.MAINTAIN,
        recommender.recommend(1L, 2L, 3L, "{\"fileCount\":10}"));
    Assertions.assertEquals(
        MaintenanceRecommendDecision.MAINTAIN,
        recommender.recommend(1L, 2L, 3L, "{\"fileCount\":11}"));
  }

  @Test
  void testSkipWhenFileCountBelowThreshold() {
    FileCountThresholdRecommender recommender = new FileCountThresholdRecommender(10);
    Assertions.assertEquals(
        MaintenanceRecommendDecision.SKIP, recommender.recommend(1L, 2L, 3L, "{\"fileCount\":9}"));
  }

  @Test
  void testSkipWhenFileCountMissingOrInvalidJson() {
    FileCountThresholdRecommender recommender = new FileCountThresholdRecommender(1);
    Assertions.assertEquals(
        MaintenanceRecommendDecision.SKIP, recommender.recommend(1L, 2L, 3L, "{}"));
    Assertions.assertEquals(
        MaintenanceRecommendDecision.SKIP, recommender.recommend(1L, 2L, 3L, "not-json"));
  }
}
