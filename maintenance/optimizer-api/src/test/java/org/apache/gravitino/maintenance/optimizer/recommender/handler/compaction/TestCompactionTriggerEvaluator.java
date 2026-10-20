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
package org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.PolicyContents;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link CompactionTriggerEvaluator}. */
class TestCompactionTriggerEvaluator {

  @Test
  void testTriggerExprFromPolicyContent() {
    IcebergDataCompactionContent content =
        (IcebergDataCompactionContent)
            PolicyContents.icebergDataCompaction(1000L, 10L, Collections.emptyMap());

    Map<String, Object> aboveMse = new HashMap<>();
    aboveMse.put(IcebergDataCompactionContent.DATA_FILE_MSE_METRIC, 1000D);
    aboveMse.put(IcebergDataCompactionContent.DELETE_FILE_NUMBER_METRIC, 0L);
    Assertions.assertTrue(CompactionTriggerEvaluator.shouldMaintain(content, aboveMse));

    Map<String, Object> aboveDeletes = new HashMap<>();
    aboveDeletes.put(IcebergDataCompactionContent.DATA_FILE_MSE_METRIC, 0D);
    aboveDeletes.put(IcebergDataCompactionContent.DELETE_FILE_NUMBER_METRIC, 10L);
    Assertions.assertTrue(CompactionTriggerEvaluator.shouldMaintain(content, aboveDeletes));

    Map<String, Object> below = new HashMap<>();
    below.put(IcebergDataCompactionContent.DATA_FILE_MSE_METRIC, 999D);
    below.put(IcebergDataCompactionContent.DELETE_FILE_NUMBER_METRIC, 9L);
    Assertions.assertFalse(CompactionTriggerEvaluator.shouldMaintain(content, below));
  }
}
