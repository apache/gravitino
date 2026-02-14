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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.maintenance.optimizer.api.common.Strategy;
import org.apache.gravitino.maintenance.optimizer.recommender.util.StrategyUtils;

public class CompactionStrategyForTest implements Strategy {

  static final String TARGET_FILE_SIZE_BYTES = "target-file-size-bytes";

  @Override
  public String name() {
    return "compaction-policy-for-test";
  }

  @Override
  public String strategyType() {
    return "compaction";
  }

  @Override
  public Map<String, Object> rules() {
    return ImmutableMap.of(
        "min_datafile_mse",
        1000,
        StrategyUtils.JOB_ROLE_PREFIX + TARGET_FILE_SIZE_BYTES,
        1024,
        StrategyUtils.TRIGGER_EXPR,
        "datafile_mse > min_datafile_mse",
        StrategyUtils.SCORE_EXPR,
        "datafile_mse * delete_file_num");
  }

  @Override
  public Map<String, String> properties() {
    return Map.of();
  }

  @Override
  public Map<String, String> jobOptions() {
    return Map.of(TARGET_FILE_SIZE_BYTES, "1024");
  }

  @Override
  public String jobTemplateName() {
    return "compaction-template";
  }
}
